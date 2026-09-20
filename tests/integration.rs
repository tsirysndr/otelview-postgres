//! Integration tests against a real PostgreSQL.
//!
//! Gated on `TEST_DATABASE_URL`: without it every test is skipped, so plain
//! `cargo test` stays green on machines without a database. CI provides a
//! service container and sets the variable.
//!
//! Everything runs inside one sequential test against freshly dropped
//! tables — the scenarios share state deliberately (the stats and retention
//! steps count rows written by earlier steps), so ordering must be fixed.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, LogsData, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Histogram, HistogramDataPoint, Metric, MetricsData, NumberDataPoint, ResourceMetrics,
    ScopeMetrics, metric, number_data_point,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{
    ResourceSpans, ScopeSpans, Span, Status, TracesData, status,
};
use otelview_postgres::logs::decode_logs_payload;
use otelview_postgres::metrics::decode_metrics_payload;
use otelview_postgres::proto::otelview::{LogQueryParameters, MetricQueryParameters};
use otelview_postgres::proto::storage::TraceQueryParameters;
use otelview_postgres::store::Store;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

fn now_nanos() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64
}

fn attr(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.into())),
        }),
        ..Default::default()
    }
}

fn resource(service: &str) -> Resource {
    Resource {
        attributes: vec![attr("service.name", service)],
        ..Default::default()
    }
}

fn span(trace: u8, span_id: u8, parent: Option<u8>, name: &str, start: u64, end: u64) -> Span {
    Span {
        trace_id: vec![trace; 16],
        span_id: vec![span_id; 8],
        parent_span_id: parent.map(|p| vec![p; 8]).unwrap_or_default(),
        name: name.into(),
        kind: 2,
        start_time_unix_nano: start,
        end_time_unix_nano: end,
        attributes: vec![attr("http.method", "GET")],
        ..Default::default()
    }
}

fn traces_data(service: &str, spans: Vec<Span>) -> TracesData {
    TracesData {
        resource_spans: vec![ResourceSpans {
            resource: Some(resource(service)),
            scope_spans: vec![ScopeSpans {
                spans,
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn log(time: u64, severity: i32, text: &str, body: &str, trace: Option<u8>) -> LogRecord {
    LogRecord {
        time_unix_nano: time,
        observed_time_unix_nano: time,
        severity_number: severity,
        severity_text: text.into(),
        body: Some(AnyValue {
            value: Some(any_value::Value::StringValue(body.into())),
        }),
        attributes: vec![attr("db.system", "postgres")],
        trace_id: trace.map(|t| vec![t; 16]).unwrap_or_default(),
        span_id: trace.map(|t| vec![t; 8]).unwrap_or_default(),
        ..Default::default()
    }
}

fn logs_data(service: &str, records: Vec<LogRecord>) -> LogsData {
    LogsData {
        resource_logs: vec![ResourceLogs {
            resource: Some(resource(service)),
            scope_logs: vec![ScopeLogs {
                log_records: records,
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn gauge_point(time: u64, value: f64) -> NumberDataPoint {
    NumberDataPoint {
        time_unix_nano: time,
        value: Some(number_data_point::Value::AsDouble(value)),
        attributes: vec![attr("core", "0")],
        ..Default::default()
    }
}

fn metrics_data(service: &str, metrics: Vec<Metric>) -> MetricsData {
    MetricsData {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(resource(service)),
            scope_metrics: vec![ScopeMetrics {
                metrics,
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

async fn connect() -> Option<Store> {
    let Ok(url) = std::env::var("TEST_DATABASE_URL") else {
        eprintln!("TEST_DATABASE_URL not set; skipping integration tests");
        return None;
    };
    let primary = PgPoolOptions::new()
        .max_connections(5)
        .connect(&url)
        .await
        .expect("connect to TEST_DATABASE_URL");
    // Same split as main.rs: reads through a session-enforced read-only pool.
    let options = url
        .parse::<PgConnectOptions>()
        .unwrap()
        .options([("default_transaction_read_only", "on")]);
    let reader = PgPoolOptions::new()
        .max_connections(5)
        .connect_with(options)
        .await
        .expect("connect read-only pool");
    sqlx::raw_sql("DROP TABLE IF EXISTS spans, logs, metric_points CASCADE")
        .execute(&primary)
        .await
        .expect("drop tables");
    let store = Store::new(primary, Some(reader), 1000);
    store.migrate().await.expect("migrate");
    // Idempotency: a restart re-runs every migration.
    store.migrate().await.expect("migrate twice");
    Some(store)
}

#[tokio::test]
async fn end_to_end() {
    let Some(store) = connect().await else { return };
    let now = now_nanos();

    spans_roundtrip(&store, now).await;
    logs_roundtrip(&store, now).await;
    metrics_roundtrip(&store, now).await;
    stats(&store).await;
    read_pool_rejects_writes(&store).await;
    retention(&store, now).await;
}

async fn spans_roundtrip(store: &Store, now: u64) {
    let data = traces_data(
        "frontend",
        vec![span(1, 1, None, "GET /checkout", now - 2_000_000_000, now)],
    );
    let (rejected, errors) = store.write(data).await.expect("write spans");
    assert_eq!((rejected, errors.len()), (0, 0));
    let child = traces_data(
        "backend",
        vec![Span {
            status: Some(Status {
                code: status::StatusCode::Error as i32,
                message: "timeout".into(),
            }),
            ..span(1, 2, Some(1), "SELECT orders", now - 1_500_000_000, now)
        }],
    );
    store.write(child).await.expect("write child span");

    // Malformed ids are rejected, not stored.
    let bad = traces_data("frontend", vec![Span { trace_id: vec![9; 4], ..span(3, 3, None, "bad", now, now) }]);
    let (rejected, errors) = store.write(bad).await.expect("write bad span");
    assert_eq!(rejected, 1);
    assert!(!errors.is_empty());

    // Upsert: rewriting a span replaces it instead of duplicating.
    let renamed = traces_data(
        "frontend",
        vec![span(1, 1, None, "GET /checkout-v2", now - 2_000_000_000, now)],
    );
    store.write(renamed).await.expect("rewrite span");

    let spans = store.spans_for_ids(&[vec![1; 16]]).await.expect("spans_for_ids");
    assert_eq!(spans.len(), 2, "upsert must not duplicate");
    assert!(spans.iter().any(|s| s.operation_name == "GET /checkout-v2"));

    let found = store
        .find_ids(&TraceQueryParameters {
            service_name: "backend".into(),
            ..Default::default()
        })
        .await
        .expect("find_ids by service");
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].trace_id, vec![1; 16]);

    let none = store
        .find_ids(&TraceQueryParameters {
            service_name: "nobody".into(),
            ..Default::default()
        })
        .await
        .expect("find_ids no match");
    assert!(none.is_empty());

    assert_eq!(store.services().await.expect("services"), vec!["backend", "frontend"]);
    let ops = store.operations("frontend", "").await.expect("operations");
    assert!(ops.iter().any(|o| o.name == "GET /checkout-v2"));

    let deps = store
        .dependencies(0, i64::MAX)
        .await
        .expect("dependencies");
    assert_eq!(deps, vec![("frontend".into(), "backend".into(), 1)]);
}

async fn logs_roundtrip(store: &Store, now: u64) {
    let written = store
        .write_logs(logs_data(
            "backend",
            vec![
                log(now, 17, "ERROR", "query timeout while selecting orders", Some(1)),
                log(now - 1_000_000_000, 9, "INFO", "pool ready, 100% healthy", None),
            ],
        ))
        .await
        .expect("write logs");
    assert_eq!(written, 2);

    // Newest first, payloads decode back to OTLP.
    let all = store
        .find_logs(&LogQueryParameters::default())
        .await
        .expect("find all logs");
    assert_eq!(all.len(), 2);
    let first = decode_logs_payload(&all[0]).expect("decode log payload");
    let record = &first.resource_logs[0].scope_logs[0].log_records[0];
    assert_eq!(record.severity_text, "ERROR");

    // Substring search over the body.
    let hits = store
        .find_logs(&LogQueryParameters {
            search: "timeout".into(),
            ..Default::default()
        })
        .await
        .expect("search logs");
    assert_eq!(hits.len(), 1);

    // LIKE wildcards in user input match literally, not as wildcards.
    let literal = store
        .find_logs(&LogQueryParameters {
            search: "100%".into(),
            ..Default::default()
        })
        .await
        .expect("wildcard search");
    assert_eq!(literal.len(), 1);
    let miss = store
        .find_logs(&LogQueryParameters {
            search: "1%y".into(),
            ..Default::default()
        })
        .await
        .expect("wildcard miss");
    assert!(miss.is_empty(), "% must not act as a wildcard");

    // Severity floor and trace correlation.
    let errors = store
        .find_logs(&LogQueryParameters {
            min_severity: 17,
            ..Default::default()
        })
        .await
        .expect("severity filter");
    assert_eq!(errors.len(), 1);
    let correlated = store
        .find_logs(&LogQueryParameters {
            trace_id: "01".repeat(16),
            ..Default::default()
        })
        .await
        .expect("trace filter");
    assert_eq!(correlated.len(), 1);
    assert!(
        store
            .find_logs(&LogQueryParameters { trace_id: "zz".into(), ..Default::default() })
            .await
            .is_err(),
        "invalid hex trace_id must be refused"
    );

    assert_eq!(store.log_services().await.expect("log services"), vec!["backend"]);
}

async fn metrics_roundtrip(store: &Store, now: u64) {
    let points: Vec<NumberDataPoint> = (0..10)
        .map(|i| gauge_point(now - (10 - i) * 1_000_000_000, i as f64))
        .collect();
    let written = store
        .write_metrics(metrics_data(
            "backend",
            vec![
                Metric {
                    name: "cpu.usage".into(),
                    unit: "1".into(),
                    data: Some(metric::Data::Gauge(Gauge {
                        data_points: points,
                    })),
                    ..Default::default()
                },
                Metric {
                    name: "http.duration".into(),
                    unit: "ms".into(),
                    data: Some(metric::Data::Histogram(Histogram {
                        data_points: vec![HistogramDataPoint {
                            time_unix_nano: now,
                            count: 3,
                            sum: Some(42.5),
                            bucket_counts: vec![1, 2],
                            explicit_bounds: vec![10.0],
                            ..Default::default()
                        }],
                        aggregation_temporality: 2,
                    })),
                    ..Default::default()
                },
            ],
        ))
        .await
        .expect("write metrics");
    assert_eq!(written, 11);

    // Downsampling: 10 gauge points, budget 4.
    let sampled = store
        .find_metrics(&MetricQueryParameters {
            metric_name: "cpu.usage".into(),
            max_points: 4,
            ..Default::default()
        })
        .await
        .expect("find metrics downsampled");
    assert_eq!(sampled.len(), 4);

    // Histogram payloads survive the roundtrip with buckets intact.
    let hist = store
        .find_metrics(&MetricQueryParameters {
            metric_name: "http.duration".into(),
            ..Default::default()
        })
        .await
        .expect("find histogram");
    assert_eq!(hist.len(), 1);
    let decoded = decode_metrics_payload(&hist[0]).expect("decode metric payload");
    match decoded.resource_metrics[0].scope_metrics[0].metrics[0]
        .data
        .as_ref()
        .expect("metric data")
    {
        metric::Data::Histogram(h) => {
            assert_eq!(h.data_points[0].bucket_counts, vec![1, 2]);
            assert_eq!(h.data_points[0].count, 3);
        }
        other => panic!("expected histogram, got {other:?}"),
    }

    assert!(
        store
            .find_metrics(&MetricQueryParameters::default())
            .await
            .is_err(),
        "metric_name is required"
    );

    let infos = store.list_metrics().await.expect("list metrics");
    let names: Vec<&str> = infos.iter().map(|m| m.name.as_str()).collect();
    assert_eq!(names, vec!["cpu.usage", "http.duration"]);
    assert_eq!(infos[0].metric_type, "gauge");
    assert_eq!(infos[1].metric_type, "histogram");
    assert_eq!(infos[1].services, vec!["backend"]);

    assert_eq!(store.metric_services().await.expect("metric services"), vec!["backend"]);
}

async fn stats(store: &Store) {
    // Row counts come from pg_stat_user_tables.n_live_tup, which the stats
    // machinery updates asynchronously: ANALYZE trues it up, and on older
    // servers the collector may still lag by a moment, hence the retry.
    sqlx::raw_sql("ANALYZE spans, logs, metric_points")
        .execute(store.primary_pool())
        .await
        .expect("analyze");
    let mut stats = store.stats().await.expect("stats");
    for _ in 0..50 {
        if (stats.spans, stats.logs, stats.metric_points) == (2, 2, 11) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        stats = store.stats().await.expect("stats retry");
    }
    assert_eq!(
        (stats.spans, stats.logs, stats.metric_points),
        (2, 2, 11),
        "estimated row counts converge after ANALYZE"
    );
    assert_eq!(stats.services, 2, "distinct services across all signals");
}

async fn read_pool_rejects_writes(store: &Store) {
    // The reader sessions run with default_transaction_read_only=on; if a
    // query were ever routed through them as a write it must fail loudly
    // rather than silently write to a replica.
    let denied = sqlx::raw_sql("INSERT INTO logs (time_unix_nano, observed_time_unix_nano, severity_number, severity_text, service_name, trace_id, span_id, body_text, payload) VALUES (0,0,0,'','x','','','','')")
        .execute(store.reader_pool())
        .await;
    assert!(denied.is_err(), "read pool accepted a write");
}

async fn retention(store: &Store, now: u64) {
    // Age one signal artificially, then sweep with a 1h retention: old rows
    // vanish, recent rows survive.
    let old = now - 3 * 3600 * 1_000_000_000;
    store
        .write_logs(logs_data("backend", vec![log(old, 5, "DEBUG", "ancient", None)]))
        .await
        .expect("write old log");
    let deleted = store
        .sweep_expired(Duration::from_secs(3600))
        .await
        .expect("sweep");
    let logs_deleted = deleted.iter().find(|(t, _)| *t == "logs").unwrap().1;
    assert_eq!(logs_deleted, 1, "only the aged row is swept");
    let survivors = store
        .find_logs(&LogQueryParameters::default())
        .await
        .expect("logs after sweep");
    assert_eq!(survivors.len(), 2, "recent logs survive the sweep");
}
