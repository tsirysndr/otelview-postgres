use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, anyhow};
use opentelemetry_proto::tonic::{
    common::v1::{AnyValue as OtelAnyValue, KeyValue as OtelKeyValue, any_value},
    trace::v1::{ResourceSpans, ScopeSpans, Span, TracesData},
};
use prost::Message;
use sea_query::{
    Alias, Condition, Expr, ExprTrait, Iden, OnConflict, Order, PostgresQueryBuilder, Query,
    SimpleExpr,
};
use sea_query_sqlx::SqlxBinder;
use serde_json::{Value, json};
use sqlx::{AssertSqlSafe, PgPool, Row};

use crate::proto::storage::{
    AnyValue, FoundTraceId, KeyValue, Operation, ServiceSummary, TraceQueryParameters,
    TraceSummary, any_value as query_any_value,
};

#[derive(Clone, Copy, Iden)]
enum Spans {
    Table,
    TraceId,
    SpanId,
    ParentSpanId,
    ServiceName,
    OperationName,
    SpanKind,
    StartTimeUnixNano,
    EndTimeUnixNano,
    DurationNano,
    StatusCode,
    SpanAttributes,
    ResourceAttributes,
    ScopeAttributes,
    Payload,
}

/// Rows per multi-row `INSERT`. Postgres caps a statement at 65535 bind
/// parameters and the widest table here binds 14 per row, so this stays well
/// inside the limit while collapsing a whole export into a few round-trips.
pub(crate) const INSERT_CHUNK_ROWS: usize = 1_000;

const SPAN_COLUMNS: [Spans; 14] = [
    Spans::TraceId,
    Spans::SpanId,
    Spans::ParentSpanId,
    Spans::ServiceName,
    Spans::OperationName,
    Spans::SpanKind,
    Spans::StartTimeUnixNano,
    Spans::EndTimeUnixNano,
    Spans::DurationNano,
    Spans::StatusCode,
    Spans::SpanAttributes,
    Spans::ResourceAttributes,
    Spans::ScopeAttributes,
    Spans::Payload,
];

/// A `spans` row, materialised before the insert so a whole export can be
/// written with a handful of multi-row statements.
struct SpanRow {
    trace_id: Vec<u8>,
    span_id: Vec<u8>,
    parent_span_id: Vec<u8>,
    service: String,
    operation: String,
    kind: String,
    start: i64,
    end: i64,
    duration: i64,
    status: i16,
    span_attributes: Value,
    resource_attributes: Value,
    scope_attributes: Value,
    payload: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct StoredSpan {
    pub trace_id: Vec<u8>,
    pub span_id: Vec<u8>,
    pub parent_span_id: Vec<u8>,
    pub service_name: String,
    pub operation_name: String,
    pub start: i64,
    pub end: i64,
    pub status_code: i16,
    pub payload: Vec<u8>,
}

#[derive(Clone)]
pub struct Store {
    pub(crate) pool: PgPool,
    pub(crate) max_search_depth: u64,
}

impl Store {
    pub fn new(pool: PgPool, max_search_depth: u64) -> Self {
        Self {
            pool,
            max_search_depth,
        }
    }

    pub async fn migrate(&self) -> Result<()> {
        for (name, sql) in [
            (
                "0001_create_spans",
                include_str!("../migrations/0001_create_spans.sql"),
            ),
            (
                "0002_create_logs",
                include_str!("../migrations/0002_create_logs.sql"),
            ),
            (
                "0003_create_metric_points",
                include_str!("../migrations/0003_create_metric_points.sql"),
            ),
        ] {
            sqlx::raw_sql(sql)
                .execute(&self.pool)
                .await
                .with_context(|| format!("run database migration {name}"))?;
        }
        Ok(())
    }

    pub async fn write(&self, data: TracesData) -> Result<(u64, Vec<String>)> {
        let mut rejected = 0;
        let mut errors = Vec::new();
        // Collected first, then written in batches: a statement per span costs
        // a network round-trip each, which does not keep up with a busy
        // exporter talking to a remote database.
        let mut rows: Vec<SpanRow> = Vec::new();

        for resource_spans in data.resource_spans {
            let resource_attrs = attributes_to_json(
                resource_spans
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

            for scope_spans in &resource_spans.scope_spans {
                let scope_attrs = attributes_to_json(
                    scope_spans
                        .scope
                        .as_ref()
                        .map(|s| s.attributes.as_slice())
                        .unwrap_or(&[]),
                );
                for span in &scope_spans.spans {
                    if span.trace_id.len() != 16 || span.span_id.len() != 8 {
                        rejected += 1;
                        if errors.len() < 5 {
                            errors.push(format!(
                                "span {:?} has trace/span ID lengths {}/{}",
                                span.name,
                                span.trace_id.len(),
                                span.span_id.len()
                            ));
                        }
                        continue;
                    }
                    rows.push(span_row(
                        &resource_spans,
                        scope_spans,
                        span,
                        &service,
                        &resource_attrs,
                        &scope_attrs,
                    )?);
                }
            }
        }

        let deduped = dedupe_spans(&rows);

        let mut tx = self.pool.begin().await?;
        for chunk in deduped.chunks(INSERT_CHUNK_ROWS) {
            let mut statement = Query::insert();
            statement.into_table(Spans::Table).columns(SPAN_COLUMNS);
            for row in chunk {
                statement.values_panic([
                    row.trace_id.clone().into(),
                    row.span_id.clone().into(),
                    row.parent_span_id.clone().into(),
                    row.service.clone().into(),
                    row.operation.clone().into(),
                    row.kind.clone().into(),
                    row.start.into(),
                    row.end.into(),
                    row.duration.into(),
                    row.status.into(),
                    row.span_attributes.clone().into(),
                    row.resource_attributes.clone().into(),
                    row.scope_attributes.clone().into(),
                    row.payload.clone().into(),
                ]);
            }
            statement.on_conflict(
                OnConflict::columns([Spans::TraceId, Spans::SpanId])
                    .update_columns(SPAN_COLUMNS.into_iter().skip(2))
                    .to_owned(),
            );
            let (sql, values) = statement.build_sqlx(PostgresQueryBuilder);
            sqlx::query_with(AssertSqlSafe(sql), values)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok((rejected, errors))
    }

    pub async fn spans_for_ids(&self, trace_ids: &[Vec<u8>]) -> Result<Vec<StoredSpan>> {
        if trace_ids.is_empty() {
            return Ok(Vec::new());
        }
        let (sql, values) = Query::select()
            .columns([
                Spans::TraceId,
                Spans::SpanId,
                Spans::ParentSpanId,
                Spans::ServiceName,
                Spans::OperationName,
                Spans::StartTimeUnixNano,
                Spans::EndTimeUnixNano,
                Spans::StatusCode,
                Spans::Payload,
            ])
            .from(Spans::Table)
            .and_where(Expr::col(Spans::TraceId).is_in(trace_ids.iter().cloned()))
            .order_by(Spans::TraceId, Order::Asc)
            .order_by(Spans::StartTimeUnixNano, Order::Asc)
            .build_sqlx(PostgresQueryBuilder);
        let rows = sqlx::query_with(AssertSqlSafe(sql), values)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(row_to_span).collect()
    }

    pub async fn find_ids(&self, query: &TraceQueryParameters) -> Result<Vec<FoundTraceId>> {
        let mut select = Query::select();
        select
            .column(Spans::TraceId)
            .expr_as(
                Expr::col(Spans::StartTimeUnixNano).min(),
                Alias::new("start"),
            )
            .expr_as(Expr::col(Spans::EndTimeUnixNano).max(), Alias::new("end"))
            .from(Spans::Table)
            .group_by_col(Spans::TraceId)
            .order_by_expr(Expr::col(Spans::StartTimeUnixNano).max(), Order::Desc)
            .limit(self.search_depth(query.search_depth));
        apply_query(&mut select, query)?;
        let (sql, values) = select.build_sqlx(PostgresQueryBuilder);
        let rows = sqlx::query_with(AssertSqlSafe(sql), values)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter()
            .map(|row| {
                let start: i64 = row.try_get("start")?;
                let end: i64 = row.try_get("end")?;
                Ok(FoundTraceId {
                    trace_id: row.try_get("trace_id")?,
                    start: Some(nanos_to_timestamp(start)),
                    end: Some(nanos_to_timestamp(end)),
                })
            })
            .collect::<std::result::Result<_, sqlx::Error>>()
            .map_err(Into::into)
    }

    pub async fn services(&self) -> Result<Vec<String>> {
        let (sql, values) = Query::select()
            .column(Spans::ServiceName)
            .distinct()
            .from(Spans::Table)
            .order_by(Spans::ServiceName, Order::Asc)
            .build_sqlx(PostgresQueryBuilder);
        Ok(sqlx::query_with(AssertSqlSafe(sql), values)
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|r| r.get("service_name"))
            .collect())
    }

    pub async fn operations(&self, service: &str, kind: &str) -> Result<Vec<Operation>> {
        let mut select = Query::select();
        select
            .columns([Spans::OperationName, Spans::SpanKind])
            .distinct()
            .from(Spans::Table)
            .and_where(Expr::col(Spans::ServiceName).eq(service))
            .order_by(Spans::OperationName, Order::Asc);
        if !kind.is_empty() {
            select.and_where(Expr::col(Spans::SpanKind).eq(kind));
        }
        let (sql, values) = select.build_sqlx(PostgresQueryBuilder);
        Ok(sqlx::query_with(AssertSqlSafe(sql), values)
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|r| Operation {
                name: r.get("operation_name"),
                span_kind: r.get("span_kind"),
            })
            .collect())
    }

    pub async fn dependencies(&self, start: i64, end: i64) -> Result<Vec<(String, String, i64)>> {
        // The parent/child self-join is deliberately static; values remain bound parameters.
        let rows = sqlx::query(
            "SELECT p.service_name AS parent, c.service_name AS child, COUNT(*)::BIGINT AS calls \
             FROM spans c JOIN spans p ON p.trace_id = c.trace_id AND p.span_id = c.parent_span_id \
             WHERE c.start_time_unix_nano >= $1 AND c.start_time_unix_nano < $2 \
               AND p.service_name <> c.service_name \
             GROUP BY p.service_name, c.service_name ORDER BY calls DESC",
        )
        .bind(start)
        .bind(end)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| (r.get("parent"), r.get("child"), r.get("calls")))
            .collect())
    }

    pub fn summaries(&self, spans: Vec<StoredSpan>) -> Vec<TraceSummary> {
        let mut traces: HashMap<Vec<u8>, Vec<StoredSpan>> = HashMap::new();
        for span in spans {
            traces.entry(span.trace_id.clone()).or_default().push(span);
        }
        traces.into_values().map(summarize).collect()
    }

    pub(crate) fn search_depth(&self, requested: i32) -> u64 {
        let requested = if requested <= 0 { 20 } else { requested as u64 };
        std::cmp::min(requested, std::cmp::max(self.max_search_depth, 1))
    }
}

fn apply_query(
    select: &mut sea_query::SelectStatement,
    query: &TraceQueryParameters,
) -> Result<()> {
    if !query.service_name.is_empty() {
        select.and_where(Expr::col(Spans::ServiceName).eq(&query.service_name));
    }
    if !query.operation_name.is_empty() {
        select.and_where(Expr::col(Spans::OperationName).eq(&query.operation_name));
    }
    if let Some(ts) = &query.start_time_min {
        select.and_where(Expr::col(Spans::StartTimeUnixNano).gte(timestamp_to_nanos(ts)?));
    }
    if let Some(ts) = &query.start_time_max {
        select.and_where(Expr::col(Spans::StartTimeUnixNano).lt(timestamp_to_nanos(ts)?));
    }
    // Jaeger sets both duration bounds on every query, leaving them at zero when
    // the user asked for neither, so a present-but-zero bound has to mean unset:
    // read literally, `duration_nano <= 0` matches no span and every search comes
    // back empty.
    if let Some(nanos) = duration_bound(query.duration_min.as_ref())? {
        select.and_where(Expr::col(Spans::DurationNano).gte(nanos));
    }
    if let Some(nanos) = duration_bound(query.duration_max.as_ref())? {
        select.and_where(Expr::col(Spans::DurationNano).lte(nanos));
    }
    for attribute in &query.attributes {
        let needle =
            json!({ attribute.key.clone(): query_value_to_json(attribute.value.as_ref()) });
        let condition = Condition::any()
            .add(json_contains(Spans::SpanAttributes, needle.clone()))
            .add(json_contains(Spans::ResourceAttributes, needle.clone()))
            .add(json_contains(Spans::ScopeAttributes, needle));
        select.cond_where(condition);
    }
    Ok(())
}

fn json_contains(column: Spans, value: Value) -> SimpleExpr {
    Expr::cust_with_values(format!("{} @> $1", column.to_string()), [value])
}

fn row_to_span(row: sqlx::postgres::PgRow) -> Result<StoredSpan> {
    Ok(StoredSpan {
        trace_id: row.try_get("trace_id")?,
        span_id: row.try_get("span_id")?,
        parent_span_id: row.try_get("parent_span_id")?,
        service_name: row.try_get("service_name")?,
        operation_name: row.try_get("operation_name")?,
        start: row.try_get("start_time_unix_nano")?,
        end: row.try_get("end_time_unix_nano")?,
        status_code: row.try_get("status_code")?,
        payload: row.try_get("payload")?,
    })
}

fn summarize(mut spans: Vec<StoredSpan>) -> TraceSummary {
    spans.sort_by_key(|s| s.start);
    let trace_id = spans
        .first()
        .map(|s| s.trace_id.clone())
        .unwrap_or_default();
    let ids: HashSet<&[u8]> = spans.iter().map(|s| s.span_id.as_slice()).collect();
    let root = spans.iter().find(|s| s.parent_span_id.is_empty());
    let mut services: HashMap<String, (i32, i32)> = HashMap::new();
    for span in &spans {
        let entry = services.entry(span.service_name.clone()).or_default();
        entry.0 += 1;
        if span.status_code == 2 {
            entry.1 += 1;
        }
    }
    TraceSummary {
        trace_id,
        root_service_name: root.map(|s| s.service_name.clone()).unwrap_or_default(),
        root_operation_name: root.map(|s| s.operation_name.clone()).unwrap_or_default(),
        min_start_time_unix_nano: spans
            .iter()
            .map(|s| std::cmp::max(s.start, 0) as u64)
            .min()
            .unwrap_or(0),
        max_end_time_unix_nano: spans
            .iter()
            .map(|s| std::cmp::max(s.end, 0) as u64)
            .max()
            .unwrap_or(0),
        span_count: spans.len() as i32,
        error_span_count: spans.iter().filter(|s| s.status_code == 2).count() as i32,
        orphan_span_count: spans
            .iter()
            .filter(|s| !s.parent_span_id.is_empty() && !ids.contains(s.parent_span_id.as_slice()))
            .count() as i32,
        services: services
            .into_iter()
            .map(|(name, (span_count, error_span_count))| ServiceSummary {
                name,
                span_count,
                error_span_count,
            })
            .collect(),
    }
}

/// Keep one row per `(trace_id, span_id)`, in arrival order, preferring the
/// last occurrence.
///
/// `ON CONFLICT DO UPDATE` refuses to touch the same row twice within a single
/// statement, so batching spans is only safe once duplicates are collapsed.
/// Keeping the last one matches the row-at-a-time behaviour this replaced,
/// where a later write simply overwrote an earlier one.
fn dedupe_spans(rows: &[SpanRow]) -> Vec<&SpanRow> {
    let mut last_index: HashMap<(&[u8], &[u8]), usize> = HashMap::new();
    for (index, row) in rows.iter().enumerate() {
        last_index.insert((&row.trace_id, &row.span_id), index);
    }
    let mut keep: Vec<usize> = last_index.into_values().collect();
    keep.sort_unstable();
    keep.into_iter().map(|i| &rows[i]).collect()
}

/// Build the row for one span, including its singleton OTLP payload.
fn span_row(
    resource_spans: &ResourceSpans,
    scope_spans: &ScopeSpans,
    span: &Span,
    service: &str,
    resource_attrs: &Value,
    scope_attrs: &Value,
) -> Result<SpanRow> {
    let start = u64_to_i64(span.start_time_unix_nano, "start timestamp")?;
    let end = u64_to_i64(span.end_time_unix_nano, "end timestamp")?;
    Ok(SpanRow {
        trace_id: span.trace_id.clone(),
        span_id: span.span_id.clone(),
        parent_span_id: span.parent_span_id.clone(),
        service: service.to_owned(),
        operation: span.name.clone(),
        kind: span_kind(span.kind).to_owned(),
        start,
        end,
        duration: end.saturating_sub(start),
        status: span
            .status
            .as_ref()
            .map(|s| s.code as i16)
            .unwrap_or_default(),
        span_attributes: attributes_to_json(&span.attributes),
        resource_attributes: resource_attrs.clone(),
        scope_attributes: scope_attrs.clone(),
        payload: singleton_payload(resource_spans, scope_spans, span).encode_to_vec(),
    })
}

fn singleton_payload(resource: &ResourceSpans, scope: &ScopeSpans, span: &Span) -> TracesData {
    TracesData {
        resource_spans: vec![ResourceSpans {
            resource: resource.resource.clone(),
            scope_spans: vec![ScopeSpans {
                scope: scope.scope.clone(),
                spans: vec![span.clone()],
                schema_url: scope.schema_url.clone(),
            }],
            schema_url: resource.schema_url.clone(),
        }],
    }
}

pub(crate) fn attributes_to_json(attributes: &[OtelKeyValue]) -> Value {
    Value::Object(
        attributes
            .iter()
            .map(|kv| (kv.key.clone(), otel_value_to_json(kv.value.as_ref())))
            .collect(),
    )
}

pub(crate) fn otel_value_to_json(value: Option<&OtelAnyValue>) -> Value {
    match value.and_then(|v| v.value.as_ref()) {
        Some(any_value::Value::StringValue(v)) => json!(v),
        Some(any_value::Value::StringValueStrindex(v)) => json!(v),
        Some(any_value::Value::BoolValue(v)) => json!(v),
        Some(any_value::Value::IntValue(v)) => json!(v),
        Some(any_value::Value::DoubleValue(v)) => json!(v),
        Some(any_value::Value::BytesValue(v)) => json!(hex(v)),
        Some(any_value::Value::ArrayValue(v)) => Value::Array(
            v.values
                .iter()
                .map(|v| otel_value_to_json(Some(v)))
                .collect(),
        ),
        Some(any_value::Value::KvlistValue(v)) => Value::Object(
            v.values
                .iter()
                .map(|kv| (kv.key.clone(), otel_value_to_json(kv.value.as_ref())))
                .collect(),
        ),
        None => Value::Null,
    }
}

fn query_value_to_json(value: Option<&AnyValue>) -> Value {
    match value.and_then(|v| v.value.as_ref()) {
        Some(query_any_value::Value::StringValue(v)) => json!(v),
        Some(query_any_value::Value::BoolValue(v)) => json!(v),
        Some(query_any_value::Value::IntValue(v)) => json!(v),
        Some(query_any_value::Value::DoubleValue(v)) => json!(v),
        Some(query_any_value::Value::BytesValue(v)) => json!(hex(v)),
        Some(query_any_value::Value::ArrayValue(v)) => Value::Array(
            v.values
                .iter()
                .map(|v| query_value_to_json(Some(v)))
                .collect(),
        ),
        Some(query_any_value::Value::KvlistValue(v)) => Value::Object(
            v.values
                .iter()
                .map(|kv: &KeyValue| (kv.key.clone(), query_value_to_json(kv.value.as_ref())))
                .collect(),
        ),
        None => Value::Null,
    }
}

fn span_kind(kind: i32) -> &'static str {
    match kind {
        1 => "internal",
        2 => "server",
        3 => "client",
        4 => "producer",
        5 => "consumer",
        _ => "unspecified",
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

pub(crate) fn u64_to_i64(value: u64, name: &str) -> Result<i64> {
    i64::try_from(value).map_err(|_| anyhow!("{name} exceeds PostgreSQL BIGINT"))
}

pub fn timestamp_to_nanos(ts: &prost_types::Timestamp) -> Result<i64> {
    ts.seconds
        .checked_mul(1_000_000_000)
        .and_then(|v| v.checked_add(ts.nanos as i64))
        .ok_or_else(|| anyhow!("timestamp is outside nanosecond range"))
}
fn duration_bound(d: Option<&prost_types::Duration>) -> Result<Option<i64>> {
    match d {
        Some(d) => Ok(match duration_to_nanos(d)? {
            0 => None,
            nanos => Some(nanos),
        }),
        None => Ok(None),
    }
}

fn duration_to_nanos(d: &prost_types::Duration) -> Result<i64> {
    d.seconds
        .checked_mul(1_000_000_000)
        .and_then(|v| v.checked_add(d.nanos as i64))
        .ok_or_else(|| anyhow!("duration is outside nanosecond range"))
}
fn nanos_to_timestamp(value: i64) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: value.div_euclid(1_000_000_000),
        nanos: value.rem_euclid(1_000_000_000) as i32,
    }
}

pub fn decode_payload(bytes: &[u8]) -> Result<TracesData> {
    Ok(TracesData::decode(bytes)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};

    fn span_row_for(trace: u8, span: u8, operation: &str) -> SpanRow {
        SpanRow {
            trace_id: vec![trace; 16],
            span_id: vec![span; 8],
            parent_span_id: Vec::new(),
            service: "svc".into(),
            operation: operation.into(),
            kind: "server".into(),
            start: 0,
            end: 1,
            duration: 1,
            status: 0,
            span_attributes: json!({}),
            resource_attributes: json!({}),
            scope_attributes: json!({}),
            payload: Vec::new(),
        }
    }

    #[test]
    fn dedupe_spans_keeps_last_write_per_id_in_order() {
        let rows = vec![
            span_row_for(1, 1, "first"),
            span_row_for(2, 2, "other"),
            // same (trace_id, span_id) as the first: a batch may only carry it
            // once or Postgres rejects the whole statement.
            span_row_for(1, 1, "second"),
            span_row_for(1, 3, "different span, same trace"),
        ];

        let kept = dedupe_spans(&rows);

        assert_eq!(kept.len(), 3);
        // The duplicate collapses to its latest version, and the surviving
        // rows stay in arrival order.
        assert_eq!(kept[0].operation, "other");
        assert_eq!(kept[1].operation, "second");
        assert_eq!(kept[2].operation, "different span, same trace");
    }

    #[test]
    fn dedupe_spans_preserves_distinct_rows() {
        let rows = vec![span_row_for(1, 1, "a"), span_row_for(1, 2, "b")];
        assert_eq!(dedupe_spans(&rows).len(), 2);
    }

    #[test]
    fn negative_timestamp_round_trips() {
        let ts = nanos_to_timestamp(-1);
        assert_eq!(timestamp_to_nanos(&ts).unwrap(), -1);
        assert_eq!((ts.seconds, ts.nanos), (-1, 999_999_999));
    }

    #[test]
    fn attributes_keep_scalar_types_and_nested_values() {
        let attributes = vec![
            KeyValue {
                key: "ok".into(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::BoolValue(true)),
                }),
                ..Default::default()
            },
            KeyValue {
                key: "attempt".into(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::IntValue(3)),
                }),
                ..Default::default()
            },
        ];
        assert_eq!(
            attributes_to_json(&attributes),
            json!({"ok": true, "attempt": 3})
        );
    }

    #[test]
    fn singleton_payload_is_lossless() {
        let span = Span {
            trace_id: vec![1; 16],
            span_id: vec![2; 8],
            name: "request".into(),
            ..Default::default()
        };
        let scope = ScopeSpans {
            spans: vec![span.clone()],
            ..Default::default()
        };
        let resource = ResourceSpans {
            scope_spans: vec![scope.clone()],
            ..Default::default()
        };
        let encoded = singleton_payload(&resource, &scope, &span).encode_to_vec();
        let decoded = decode_payload(&encoded).unwrap();
        assert_eq!(decoded.resource_spans[0].scope_spans[0].spans[0], span);
    }

    #[test]
    fn summary_counts_errors_and_orphans() {
        let trace_id = vec![1; 16];
        let spans = vec![
            StoredSpan {
                trace_id: trace_id.clone(),
                span_id: vec![1; 8],
                parent_span_id: vec![],
                service_name: "frontend".into(),
                operation_name: "GET /".into(),
                start: 10,
                end: 50,
                status_code: 0,
                payload: vec![],
            },
            StoredSpan {
                trace_id,
                span_id: vec![2; 8],
                parent_span_id: vec![9; 8],
                service_name: "backend".into(),
                operation_name: "query".into(),
                start: 20,
                end: 40,
                status_code: 2,
                payload: vec![],
            },
        ];
        let summary = summarize(spans);
        assert_eq!(summary.root_service_name, "frontend");
        assert_eq!(summary.span_count, 2);
        assert_eq!(summary.error_span_count, 1);
        assert_eq!(summary.orphan_span_count, 1);
        assert_eq!(summary.min_start_time_unix_nano, 10);
        assert_eq!(summary.max_end_time_unix_nano, 50);
    }

    #[test]
    fn query_uses_bound_values_for_attribute_search() {
        let mut select = Query::select();
        select.column(Spans::TraceId).from(Spans::Table);
        let query = TraceQueryParameters {
            service_name: "api' OR true --".into(),
            attributes: vec![crate::proto::storage::KeyValue {
                key: "http.method".into(),
                value: Some(crate::proto::storage::AnyValue {
                    value: Some(query_any_value::Value::StringValue("GET".into())),
                }),
            }],
            ..Default::default()
        };
        apply_query(&mut select, &query).unwrap();
        let (sql, values) = select.build_sqlx(PostgresQueryBuilder);
        assert!(!sql.contains("OR true"));
        assert!(sql.contains("span_attributes @>"));
        assert_eq!(values.0.0.len(), 4);
    }

    fn duration_sql(
        min: Option<prost_types::Duration>,
        max: Option<prost_types::Duration>,
    ) -> String {
        let mut select = Query::select();
        select.column(Spans::TraceId).from(Spans::Table);
        let query = TraceQueryParameters {
            duration_min: min,
            duration_max: max,
            ..Default::default()
        };
        apply_query(&mut select, &query).unwrap();
        select.build_sqlx(PostgresQueryBuilder).0
    }

    #[test]
    fn zero_duration_bounds_do_not_filter() {
        let zero = prost_types::Duration::default();
        let sql = duration_sql(Some(zero), Some(zero));
        assert!(!sql.contains("duration_nano"), "got: {sql}");
    }

    #[test]
    fn real_duration_bounds_still_filter() {
        let sql = duration_sql(
            None,
            Some(prost_types::Duration {
                seconds: 10,
                nanos: 0,
            }),
        );
        assert!(sql.contains("duration_nano"), "got: {sql}");
    }
}
