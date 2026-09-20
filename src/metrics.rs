//! Metric storage: OTLP `MetricsData` writes and the
//! `otelview.storage.v1.MetricReader` query side. Every data point is stored
//! as its own row with a singleton protobuf payload (resource, scope and the
//! metric descriptor with just that point), so reads stream original OTLP
//! data losslessly.

use std::collections::HashMap;

use anyhow::{Context, Result, anyhow};
use opentelemetry_proto::tonic::metrics::v1::{
    ExponentialHistogram, Gauge, Histogram, Metric, MetricsData, ResourceMetrics, ScopeMetrics,
    Sum, Summary, metric, number_data_point,
};
use prost::Message;
use sea_query::{Expr, ExprTrait, Iden, Order, PostgresQueryBuilder, Query};
use sea_query_sqlx::SqlxBinder;
use serde_json::Value;
use sqlx::{AssertSqlSafe, Row};

use crate::proto::otelview::{MetricInfo, MetricQueryParameters};
use crate::store::{INSERT_CHUNK_ROWS, Store, attributes_to_json, timestamp_to_nanos, u64_to_i64};

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

/// A `metric_points` row, materialised before the insert so a whole export can
/// be written with a handful of multi-row statements.
struct MetricRow {
    name: String,
    description: String,
    unit: String,
    metric_type: String,
    service: String,
    time: i64,
    value: f64,
    count: i64,
    attributes: Value,
    resource_attributes: Value,
    payload: Vec<u8>,
}

impl Store {
    pub async fn write_metrics(&self, data: MetricsData) -> Result<u64> {
        // Flatten every point up front, then write in batches. A statement per
        // point costs a network round-trip each, and a single export from an
        // instrumented service routinely carries well over a thousand points:
        // against a remote database that takes minutes, so the client times out
        // and the surrounding transaction rolls back with nothing written.
        let mut rows = Vec::new();
        for resource_metrics in &data.resource_metrics {
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
                        rows.push(metric_row(
                            resource_metrics,
                            scope_metrics,
                            point,
                            &service,
                            &resource_attrs,
                        )?);
                    }
                }
            }
        }
        if rows.is_empty() {
            return Ok(0);
        }

        let mut tx = self.primary.begin().await?;
        for chunk in rows.chunks(INSERT_CHUNK_ROWS) {
            let mut statement = Query::insert();
            statement.into_table(MetricPoints::Table).columns([
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
            ]);
            for row in chunk {
                statement.values_panic([
                    row.name.clone().into(),
                    row.description.clone().into(),
                    row.unit.clone().into(),
                    row.metric_type.clone().into(),
                    row.service.clone().into(),
                    row.time.into(),
                    row.value.into(),
                    row.count.into(),
                    row.attributes.clone().into(),
                    row.resource_attributes.clone().into(),
                    row.payload.clone().into(),
                ]);
            }
            let (sql, values) = statement.build_sqlx(PostgresQueryBuilder);
            sqlx::query_with(AssertSqlSafe(sql), values)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(rows.len() as u64)
    }

    /// Matching data-point payloads in time order, downsampled per series
    /// (service + point attributes) to the requested budget.
    pub async fn find_metrics(&self, query: &MetricQueryParameters) -> Result<Vec<Vec<u8>>> {
        if query.metric_name.is_empty() {
            return Err(anyhow!("metric_name is required"));
        }
        let requested_points = if query.max_points <= 0 {
            DEFAULT_MAX_POINTS
        } else {
            query.max_points as usize
        };

        // The downsampling is pushed into SQL: shipping every raw point of
        // the window and thinning afterwards moved megabytes of payload per
        // chart — the metric graph spent seconds in transfer for points that
        // were immediately discarded. Every k-th row per series (plus each
        // series' last) survives, sized so a series stays a little above the
        // requested budget; the exact evenly-spaced pass below then trims to
        // it. Filters and identifiers are bound; the SQL itself is static.
        let mut conditions = String::from("metric_name = $1");
        let mut bind_index = 2;
        let service_bind = if query.service_name.is_empty() {
            0
        } else {
            conditions.push_str(&format!(" AND service_name = ${bind_index}"));
            bind_index += 1;
            bind_index - 1
        };
        let time_min = query.time_min.as_ref().map(timestamp_to_nanos).transpose()?;
        let min_bind = if time_min.is_some() {
            conditions.push_str(&format!(" AND time_unix_nano >= ${bind_index}"));
            bind_index += 1;
            bind_index - 1
        } else {
            0
        };
        let time_max = query.time_max.as_ref().map(timestamp_to_nanos).transpose()?;
        let max_bind = if time_max.is_some() {
            conditions.push_str(&format!(" AND time_unix_nano < ${bind_index}"));
            bind_index += 1;
            bind_index - 1
        } else {
            0
        };
        let points_bind = bind_index;

        let sql = format!(
            r#"SELECT payload, service_name, series_attrs FROM (
                 SELECT payload, service_name, attributes::TEXT AS series_attrs,
                        time_unix_nano,
                        row_number() OVER w AS rn,
                        count(*) OVER (PARTITION BY service_name, attributes) AS cnt
                 FROM metric_points
                 WHERE {conditions}
                 WINDOW w AS (PARTITION BY service_name, attributes
                              ORDER BY time_unix_nano)
               ) sampled
               WHERE cnt <= ${points_bind}
                  OR rn % (cnt / ${points_bind} + 1) = 1
                  OR rn = cnt
               ORDER BY time_unix_nano ASC
               LIMIT {MAX_METRIC_ROWS}"#,
        );
        let mut db_query = sqlx::query(AssertSqlSafe(sql)).bind(&query.metric_name);
        // Bound in the same order the placeholders were appended.
        for index in 2..bind_index {
            if index == service_bind {
                db_query = db_query.bind(&query.service_name);
            } else if index == min_bind {
                db_query = db_query.bind(time_min.unwrap());
            } else if index == max_bind {
                db_query = db_query.bind(time_max.unwrap());
            }
        }
        db_query = db_query.bind(requested_points as i64);
        let rows = db_query
            .fetch_all(&self.reader)
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

        let mut keep = vec![false; payloads.len()];
        for indices in series.into_values() {
            for index in downsample(&indices, requested_points) {
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
    ///
    /// Both halves used whole-table DISTINCTs, and this is the largest table
    /// by an order of magnitude — millions of points within a day of normal
    /// ingest — so the metric list alone could take the better part of a
    /// minute. Instead: walk the distinct names off the (metric_name, …)
    /// index, then per name probe one descriptor row and skip-scan its
    /// services off the (metric_name, service_name, …) index prefix. The
    /// work is now proportional to metrics × services, dozens of index
    /// probes, not to the point count.
    pub async fn list_metrics(&self) -> Result<Vec<MetricInfo>> {
        let names = self
            .distinct_indexed(MetricPoints::Table, MetricPoints::MetricName)
            .await
            .context("list metric names")?;

        // Per-name lookups run concurrently: each is index-cheap but costs a
        // round-trip, and against a remote database ~50 sequential
        // round-trips added up to ~10s for the metric list on their own.
        // JoinSet rather than a semaphore — the pool's max_connections is
        // already the concurrency ceiling.
        let mut tasks = tokio::task::JoinSet::new();
        for (index, name) in names.into_iter().enumerate() {
            let store = self.clone();
            tasks.spawn(async move {
                let info = store.metric_info(name).await;
                (index, info)
            });
        }
        let mut out: Vec<Option<MetricInfo>> = std::iter::repeat_with(|| None)
            .take(tasks.len())
            .collect();
        while let Some(joined) = tasks.join_next().await {
            let (index, info) = joined.context("metric lookup task")?;
            out[index] = Some(info?);
        }
        Ok(out.into_iter().flatten().collect())
    }

    /// Descriptor and reporting services for one metric name.
    async fn metric_info(&self, name: String) -> Result<MetricInfo> {
        {
            let (sql, values) = Query::select()
                .columns([
                    MetricPoints::Description,
                    MetricPoints::Unit,
                    MetricPoints::MetricType,
                ])
                .from(MetricPoints::Table)
                .and_where(Expr::col(MetricPoints::MetricName).eq(&name))
                // Newest by time, not by id: (metric_name, time_unix_nano)
                // is indexed so this is one probe, where ordering by id made
                // the planner sort every point of the metric.
                .order_by(MetricPoints::TimeUnixNano, Order::Desc)
                .limit(1)
                .build_sqlx(PostgresQueryBuilder);
            let row = sqlx::query_with(AssertSqlSafe(sql), values)
                .fetch_one(&self.reader)
                .await
                .context("fetch metric descriptor")?;

            // Distinct services for this one metric: skip along the
            // (metric_name, service_name) index prefix, one probe each.
            let services: Vec<String> = sqlx::query(AssertSqlSafe(
                r#"WITH RECURSIVE walk AS (
                     (SELECT service_name AS v FROM metric_points
                      WHERE metric_name = $1 ORDER BY service_name LIMIT 1)
                     UNION ALL
                     SELECT (SELECT service_name FROM metric_points
                             WHERE metric_name = $1 AND service_name > walk.v
                             ORDER BY service_name LIMIT 1)
                     FROM walk WHERE walk.v IS NOT NULL
                   )
                   SELECT v FROM walk WHERE v IS NOT NULL ORDER BY v"#
                    .to_string(),
            ))
            .bind(&name)
            .fetch_all(&self.reader)
            .await
            .context("list metric services")?
            .into_iter()
            .map(|r| r.get("v"))
            .collect();

            Ok(MetricInfo {
                name,
                description: row.try_get("description")?,
                unit: row.try_get("unit")?,
                metric_type: row.try_get("metric_type")?,
                services,
            })
        }
    }


    pub async fn metric_services(&self) -> Result<Vec<String>> {
        // Loose index scan over the service_name index: a plain DISTINCT
        // reads every point, and this is the biggest table by far. See
        // `Store::distinct_indexed`.
        self.distinct_indexed(MetricPoints::Table, MetricPoints::ServiceName)
            .await
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

/// Build the row for one flattened point, including its singleton OTLP payload.
fn metric_row(
    resource_metrics: &ResourceMetrics,
    scope_metrics: &ScopeMetrics,
    point: FlatPoint,
    service: &str,
    resource_attrs: &Value,
) -> Result<MetricRow> {
    let time = u64_to_i64(point.time, "metric timestamp")?;
    let count = u64_to_i64(point.count, "metric count")?;
    let metric_type = metric_type_str(&point.metric)
        .ok_or_else(|| anyhow!("metric {} has no data", point.metric.name))?
        .to_owned();
    let payload = singleton_payload(resource_metrics, scope_metrics, &point.metric).encode_to_vec();
    Ok(MetricRow {
        name: point.metric.name.clone(),
        description: point.metric.description.clone(),
        unit: point.metric.unit.clone(),
        metric_type,
        service: service.to_owned(),
        time,
        value: point.value,
        count,
        attributes: point.attributes,
        resource_attributes: resource_attrs.clone(),
        payload,
    })
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
