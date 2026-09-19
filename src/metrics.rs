//! Metric storage: OTLP `MetricsData` writes and the
//! `otelview.storage.v1.MetricReader` query side. Every data point is stored
//! as its own row with a singleton protobuf payload (resource, scope and the
//! metric descriptor with just that point), so reads stream original OTLP
//! data losslessly.

use std::collections::{BTreeMap, HashMap};

use anyhow::{Context, Result, anyhow};
use opentelemetry_proto::tonic::metrics::v1::{
    ExponentialHistogram, Gauge, Histogram, Metric, MetricsData, ResourceMetrics, ScopeMetrics,
    Sum, Summary, metric, number_data_point,
};
use prost::Message;
use sea_query::{Alias, Expr, ExprTrait, Iden, Order, PostgresQueryBuilder, Query};
use sea_query_sqlx::SqlxBinder;
use serde_json::Value;
use sqlx::{AssertSqlSafe, Postgres, Row, Transaction};

use crate::proto::otelview::{MetricInfo, MetricQueryParameters};
use crate::store::{Store, attributes_to_json, timestamp_to_nanos, u64_to_i64};

/// Hard bound on rows examined by one FindMetrics query, protecting memory
/// before per-series downsampling kicks in.
const MAX_METRIC_ROWS: u64 = 100_000;

/// Series cap applied when the query leaves `max_points` at zero.
const DEFAULT_MAX_POINTS: usize = 500;

#[derive(Clone, Copy, Iden)]
enum MetricPoints {
    Table,
    MetricName,
    Description,
    Unit,
    MetricType,
    ServiceName,
    TimeUnixNano,
    Value,
    Count,
    Attributes,
    ResourceAttributes,
    Payload,
}

/// One data point split out of a metric, plus the scalar columns indexed
/// alongside its payload.
struct FlatPoint {
    metric: Metric,
    time: u64,
    value: f64,
    count: u64,
    attributes: Value,
}

impl Store {
    pub async fn write_metrics(&self, data: MetricsData) -> Result<u64> {
        let mut tx = self.pool.begin().await?;
        let mut written = 0u64;
        for resource_metrics in data.resource_metrics {
            let resource_attrs = attributes_to_json(
                resource_metrics
                    .resource
                    .as_ref()
                    .map(|r| r.attributes.as_slice())
                    .unwrap_or(&[]),
            );
            let service = resource_attrs
                .get("service.name")
                .and_then(Value::as_str)
                .unwrap_or("unknown_service")
                .to_owned();
            for scope_metrics in &resource_metrics.scope_metrics {
                for metric in &scope_metrics.metrics {
                    for point in flatten_metric(metric) {
                        self.write_point(
                            &mut tx,
                            &resource_metrics,
                            scope_metrics,
                            point,
                            &service,
                            &resource_attrs,
                        )
                        .await?;
                        written += 1;
                    }
                }
            }
        }
        tx.commit().await?;
        Ok(written)
    }

    async fn write_point(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        resource_metrics: &ResourceMetrics,
        scope_metrics: &ScopeMetrics,
        point: FlatPoint,
        service: &str,
        resource_attrs: &Value,
    ) -> Result<()> {
        let time = u64_to_i64(point.time, "metric timestamp")?;
        let count = u64_to_i64(point.count, "metric count")?;
        let metric_type = metric_type_str(&point.metric)
            .ok_or_else(|| anyhow!("metric {} has no data", point.metric.name))?;
        let payload =
            singleton_payload(resource_metrics, scope_metrics, &point.metric).encode_to_vec();
        let (sql, values) = Query::insert()
            .into_table(MetricPoints::Table)
            .columns([
                MetricPoints::MetricName,
                MetricPoints::Description,
                MetricPoints::Unit,
                MetricPoints::MetricType,
                MetricPoints::ServiceName,
                MetricPoints::TimeUnixNano,
                MetricPoints::Value,
                MetricPoints::Count,
                MetricPoints::Attributes,
                MetricPoints::ResourceAttributes,
                MetricPoints::Payload,
            ])
            .values_panic([
                point.metric.name.clone().into(),
                point.metric.description.clone().into(),
                point.metric.unit.clone().into(),
                metric_type.into(),
                service.into(),
                time.into(),
                point.value.into(),
                count.into(),
                point.attributes.into(),
                resource_attrs.clone().into(),
                payload.into(),
            ])
            .build_sqlx(PostgresQueryBuilder);
        sqlx::query_with(AssertSqlSafe(sql), values)
            .execute(&mut **tx)
            .await?;
        Ok(())
    }

    /// Matching data-point payloads in time order, downsampled per series
    /// (service + point attributes) to the requested budget.
    pub async fn find_metrics(&self, query: &MetricQueryParameters) -> Result<Vec<Vec<u8>>> {
        if query.metric_name.is_empty() {
            return Err(anyhow!("metric_name is required"));
        }
        let mut select = Query::select();
        select
            .columns([MetricPoints::Payload, MetricPoints::ServiceName])
            .expr_as(
                Expr::col(MetricPoints::Attributes).cast_as(Alias::new("TEXT")),
                Alias::new("series_attrs"),
            )
            .from(MetricPoints::Table)
            .and_where(Expr::col(MetricPoints::MetricName).eq(&query.metric_name))
            .order_by(MetricPoints::TimeUnixNano, Order::Asc)
            .limit(MAX_METRIC_ROWS);
        if !query.service_name.is_empty() {
            select.and_where(Expr::col(MetricPoints::ServiceName).eq(&query.service_name));
        }
        if let Some(ts) = &query.time_min {
            select.and_where(Expr::col(MetricPoints::TimeUnixNano).gte(timestamp_to_nanos(ts)?));
        }
        if let Some(ts) = &query.time_max {
            select.and_where(Expr::col(MetricPoints::TimeUnixNano).lt(timestamp_to_nanos(ts)?));
        }
        let (sql, values) = select.build_sqlx(PostgresQueryBuilder);
        let rows = sqlx::query_with(AssertSqlSafe(sql), values)
            .fetch_all(&self.pool)
            .await
            .context("query metric points")?;

        let mut payloads = Vec::with_capacity(rows.len());
        let mut series: HashMap<(String, String), Vec<usize>> = HashMap::new();
        for (index, row) in rows.iter().enumerate() {
            payloads.push(row.try_get::<Vec<u8>, _>("payload")?);
            let key = (
                row.try_get::<String, _>("service_name")?,
                row.try_get::<String, _>("series_attrs")?,
            );
            series.entry(key).or_default().push(index);
        }

        let max_points = if query.max_points <= 0 {
            DEFAULT_MAX_POINTS
        } else {
            query.max_points as usize
        };
        let mut keep = vec![false; payloads.len()];
        for indices in series.into_values() {
            for index in downsample(&indices, max_points) {
                keep[index] = true;
            }
        }
        Ok(payloads
            .into_iter()
            .zip(keep)
            .filter_map(|(payload, keep)| keep.then_some(payload))
            .collect())
    }

    /// One descriptor per metric name, with the services reporting it.
    pub async fn list_metrics(&self) -> Result<Vec<MetricInfo>> {
        let (sql, values) = Query::select()
            .distinct_on([MetricPoints::MetricName])
            .columns([
                MetricPoints::MetricName,
                MetricPoints::Description,
                MetricPoints::Unit,
                MetricPoints::MetricType,
            ])
            .from(MetricPoints::Table)
            .order_by(MetricPoints::MetricName, Order::Asc)
            .order_by(Alias::new("id"), Order::Desc)
            .build_sqlx(PostgresQueryBuilder);
        let descriptors = sqlx::query_with(AssertSqlSafe(sql), values)
            .fetch_all(&self.pool)
            .await
            .context("list metric descriptors")?;

        let (sql, values) = Query::select()
            .columns([MetricPoints::MetricName, MetricPoints::ServiceName])
            .distinct()
            .from(MetricPoints::Table)
            .order_by(MetricPoints::MetricName, Order::Asc)
            .order_by(MetricPoints::ServiceName, Order::Asc)
            .build_sqlx(PostgresQueryBuilder);
        let pairs = sqlx::query_with(AssertSqlSafe(sql), values)
            .fetch_all(&self.pool)
            .await
            .context("list metric services")?;
        let mut services: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for row in pairs {
            services
                .entry(row.try_get("metric_name")?)
                .or_default()
                .push(row.try_get("service_name")?);
        }

        descriptors
            .into_iter()
            .map(|row| {
                let name: String = row.try_get("metric_name")?;
                let info = MetricInfo {
                    services: services.remove(&name).unwrap_or_default(),
                    name,
                    description: row.try_get("description")?,
                    unit: row.try_get("unit")?,
                    metric_type: row.try_get("metric_type")?,
                };
                Ok(info)
            })
            .collect()
    }

    pub async fn metric_services(&self) -> Result<Vec<String>> {
        let (sql, values) = Query::select()
            .column(MetricPoints::ServiceName)
            .distinct()
            .from(MetricPoints::Table)
            .order_by(MetricPoints::ServiceName, Order::Asc)
            .build_sqlx(PostgresQueryBuilder);
        Ok(sqlx::query_with(AssertSqlSafe(sql), values)
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|r| r.get("service_name"))
            .collect())
    }
}

/// Evenly spaced sample of `indices`, keeping first and last, when the series
/// exceeds the point budget.
fn downsample(indices: &[usize], max_points: usize) -> Vec<usize> {
    if indices.len() <= max_points || max_points == 0 {
        return indices.to_vec();
    }
    if max_points == 1 {
        return vec![*indices.last().expect("non-empty series")];
    }
    (0..max_points)
        .map(|i| indices[i * (indices.len() - 1) / (max_points - 1)])
        .collect()
}

/// Split a metric into per-point copies of itself: same descriptor, one data
/// point each, so a row's payload round-trips through OTLP untouched
/// (buckets, temporality, quantiles and exemplars included).
fn flatten_metric(metric: &Metric) -> Vec<FlatPoint> {
    let number_value = |v: Option<&number_data_point::Value>| match v {
        Some(number_data_point::Value::AsDouble(d)) => *d,
        Some(number_data_point::Value::AsInt(i)) => *i as f64,
        None => 0.0,
    };
    let with_data = |data: metric::Data| Metric {
        data: Some(data),
        ..metric.clone()
    };
    match &metric.data {
        Some(metric::Data::Gauge(g)) => g
            .data_points
            .iter()
            .map(|dp| FlatPoint {
                metric: with_data(metric::Data::Gauge(Gauge {
                    data_points: vec![dp.clone()],
                })),
                time: dp.time_unix_nano,
                value: number_value(dp.value.as_ref()),
                count: 0,
                attributes: attributes_to_json(&dp.attributes),
            })
            .collect(),
        Some(metric::Data::Sum(s)) => s
            .data_points
            .iter()
            .map(|dp| FlatPoint {
                metric: with_data(metric::Data::Sum(Sum {
                    data_points: vec![dp.clone()],
                    aggregation_temporality: s.aggregation_temporality,
                    is_monotonic: s.is_monotonic,
                })),
                time: dp.time_unix_nano,
                value: number_value(dp.value.as_ref()),
                count: 0,
                attributes: attributes_to_json(&dp.attributes),
            })
            .collect(),
        Some(metric::Data::Histogram(h)) => h
            .data_points
            .iter()
            .map(|dp| FlatPoint {
                metric: with_data(metric::Data::Histogram(Histogram {
                    data_points: vec![dp.clone()],
                    aggregation_temporality: h.aggregation_temporality,
                })),
                time: dp.time_unix_nano,
                value: dp.sum.unwrap_or(0.0),
                count: dp.count,
                attributes: attributes_to_json(&dp.attributes),
            })
            .collect(),
        Some(metric::Data::ExponentialHistogram(h)) => h
            .data_points
            .iter()
            .map(|dp| FlatPoint {
                metric: with_data(metric::Data::ExponentialHistogram(ExponentialHistogram {
                    data_points: vec![dp.clone()],
                    aggregation_temporality: h.aggregation_temporality,
                })),
                time: dp.time_unix_nano,
                value: dp.sum.unwrap_or(0.0),
                count: dp.count,
                attributes: attributes_to_json(&dp.attributes),
            })
            .collect(),
        Some(metric::Data::Summary(s)) => s
            .data_points
            .iter()
            .map(|dp| FlatPoint {
                metric: with_data(metric::Data::Summary(Summary {
                    data_points: vec![dp.clone()],
                })),
                time: dp.time_unix_nano,
                value: dp.sum,
                count: dp.count,
                attributes: attributes_to_json(&dp.attributes),
            })
            .collect(),
        None => Vec::new(),
    }
}

fn metric_type_str(metric: &Metric) -> Option<&'static str> {
    Some(match metric.data.as_ref()? {
        metric::Data::Gauge(_) => "gauge",
        metric::Data::Sum(_) => "sum",
        metric::Data::Histogram(_) => "histogram",
        metric::Data::ExponentialHistogram(_) => "exponential_histogram",
        metric::Data::Summary(_) => "summary",
    })
}

fn singleton_payload(
    resource: &ResourceMetrics,
    scope: &ScopeMetrics,
    metric: &Metric,
) -> MetricsData {
    MetricsData {
        resource_metrics: vec![ResourceMetrics {
            resource: resource.resource.clone(),
            scope_metrics: vec![ScopeMetrics {
                scope: scope.scope.clone(),
                metrics: vec![metric.clone()],
                schema_url: scope.schema_url.clone(),
            }],
            schema_url: resource.schema_url.clone(),
        }],
    }
}

pub fn decode_metrics_payload(bytes: &[u8]) -> Result<MetricsData> {
    Ok(MetricsData::decode(bytes)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::metrics::v1::{HistogramDataPoint, NumberDataPoint};

    fn gauge_metric() -> Metric {
        Metric {
            name: "cpu.usage".into(),
            unit: "1".into(),
            data: Some(metric::Data::Gauge(Gauge {
                data_points: vec![
                    NumberDataPoint {
                        time_unix_nano: 100,
                        value: Some(number_data_point::Value::AsDouble(0.5)),
                        ..Default::default()
                    },
                    NumberDataPoint {
                        time_unix_nano: 200,
                        value: Some(number_data_point::Value::AsInt(2)),
                        ..Default::default()
                    },
                ],
            })),
            ..Default::default()
        }
    }

    #[test]
    fn flatten_splits_points_and_extracts_values() {
        let points = flatten_metric(&gauge_metric());
        assert_eq!(points.len(), 2);
        assert_eq!(points[0].time, 100);
        assert_eq!(points[0].value, 0.5);
        assert_eq!(points[1].value, 2.0);
    }

    #[test]
    fn histogram_payload_round_trips_buckets() {
        let metric = Metric {
            name: "latency".into(),
            data: Some(metric::Data::Histogram(Histogram {
                data_points: vec![HistogramDataPoint {
                    time_unix_nano: 5,
                    count: 3,
                    sum: Some(12.0),
                    bucket_counts: vec![1, 2],
                    explicit_bounds: vec![10.0],
                    ..Default::default()
                }],
                aggregation_temporality: 2,
            })),
            ..Default::default()
        };
        let points = flatten_metric(&metric);
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].count, 3);
        let payload = singleton_payload(
            &ResourceMetrics::default(),
            &ScopeMetrics::default(),
            &points[0].metric,
        )
        .encode_to_vec();
        let decoded = decode_metrics_payload(&payload).unwrap();
        let round = &decoded.resource_metrics[0].scope_metrics[0].metrics[0];
        match round.data.as_ref().unwrap() {
            metric::Data::Histogram(h) => {
                assert_eq!(h.data_points[0].bucket_counts, vec![1, 2]);
                assert_eq!(h.aggregation_temporality, 2);
            }
            other => panic!("unexpected data: {other:?}"),
        }
    }

    #[test]
    fn downsample_keeps_endpoints_and_budget() {
        let indices: Vec<usize> = (0..10).collect();
        assert_eq!(downsample(&indices, 20), indices);
        let sampled = downsample(&indices, 4);
        assert_eq!(sampled.len(), 4);
        assert_eq!(*sampled.first().unwrap(), 0);
        assert_eq!(*sampled.last().unwrap(), 9);
        assert_eq!(downsample(&indices, 1), vec![9]);
    }

    #[test]
    fn metric_types_map_to_strings() {
        assert_eq!(metric_type_str(&gauge_metric()), Some("gauge"));
        assert_eq!(metric_type_str(&Metric::default()), None);
    }
}
