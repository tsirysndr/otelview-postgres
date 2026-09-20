//! Log storage: OTLP `LogsData` writes and the `otelview.storage.v1.LogReader`
//! query side. Each record keeps its original protobuf payload (resource,
//! scope and record) alongside indexed search columns, mirroring the spans
//! table.

use anyhow::{Context, Result, anyhow};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, LogsData, ResourceLogs, ScopeLogs};
use prost::Message;
use sea_query::extension::postgres::PgExpr;
use sea_query::{
    Alias, Condition, Expr, ExprTrait, Iden, LikeExpr, Order, PostgresQueryBuilder, Query,
};
use sea_query_sqlx::SqlxBinder;
use serde_json::Value;
use sqlx::{AssertSqlSafe, Row};

use crate::proto::otelview::LogQueryParameters;
use crate::store::{
    INSERT_CHUNK_ROWS, Store, attributes_to_json, otel_value_to_json, timestamp_to_nanos,
    u64_to_i64,
};

#[derive(Clone, Copy, Iden)]
enum Logs {
    Table,
    TimeUnixNano,
    ObservedTimeUnixNano,
    SeverityNumber,
    SeverityText,
    ServiceName,
    TraceId,
    SpanId,
    BodyText,
    Attributes,
    ResourceAttributes,
    Payload,
}

/// A `logs` row, materialised before the insert so a whole export can be
/// written with a handful of multi-row statements.
struct LogRow {
    time: i64,
    observed: i64,
    severity_number: i32,
    severity_text: String,
    service: String,
    trace_id: Vec<u8>,
    span_id: Vec<u8>,
    body: String,
    attributes: Value,
    resource_attributes: Value,
    payload: Vec<u8>,
}

impl Store {
    pub async fn write_logs(&self, data: LogsData) -> Result<u64> {
        // Collected first, then written in batches: a statement per record
        // costs a network round-trip each, which does not keep up with a busy
        // exporter talking to a remote database.
        let mut rows = Vec::new();
        for resource_logs in &data.resource_logs {
            let resource_attrs = attributes_to_json(
                resource_logs
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
            for scope_logs in &resource_logs.scope_logs {
                for record in &scope_logs.log_records {
                    rows.push(log_row(
                        resource_logs,
                        scope_logs,
                        record,
                        &service,
                        &resource_attrs,
                    )?);
                }
            }
        }
        if rows.is_empty() {
            return Ok(0);
        }

        let mut tx = self.primary.begin().await?;
        for chunk in rows.chunks(INSERT_CHUNK_ROWS) {
            let mut statement = Query::insert();
            statement.into_table(Logs::Table).columns([
                Logs::TimeUnixNano,
                Logs::ObservedTimeUnixNano,
                Logs::SeverityNumber,
                Logs::SeverityText,
                Logs::ServiceName,
                Logs::TraceId,
                Logs::SpanId,
                Logs::BodyText,
                Logs::Attributes,
                Logs::ResourceAttributes,
                Logs::Payload,
            ]);
            for row in chunk {
                statement.values_panic([
                    row.time.into(),
                    row.observed.into(),
                    row.severity_number.into(),
                    row.severity_text.clone().into(),
                    row.service.clone().into(),
                    row.trace_id.clone().into(),
                    row.span_id.clone().into(),
                    row.body.clone().into(),
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

    /// Matching log payloads, newest first, capped by the search depth.
    pub async fn find_logs(&self, query: &LogQueryParameters) -> Result<Vec<Vec<u8>>> {
        let mut select = Query::select();
        select
            .column(Logs::Payload)
            .from(Logs::Table)
            .order_by(Logs::TimeUnixNano, Order::Desc)
            .limit(self.search_depth(query.search_depth));
        if !query.service_name.is_empty() {
            select.and_where(Expr::col(Logs::ServiceName).eq(&query.service_name));
        }
        if query.min_severity > 0 {
            select.and_where(Expr::col(Logs::SeverityNumber).gte(query.min_severity));
        }
        if !query.trace_id.is_empty() {
            let trace_id = hex_decode(&query.trace_id)
                .ok_or_else(|| anyhow!("trace_id must be a hex string"))?;
            select.and_where(Expr::col(Logs::TraceId).eq(trace_id));
            // Redundant with the equality above, but matches the partial
            // index predicate so the planner can use logs_trace_idx.
            select.and_where(Expr::cust("octet_length(trace_id) = 16"));
        }
        if let Some(ts) = &query.time_min {
            select.and_where(Expr::col(Logs::TimeUnixNano).gte(timestamp_to_nanos(ts)?));
        }
        if let Some(ts) = &query.time_max {
            select.and_where(Expr::col(Logs::TimeUnixNano).lt(timestamp_to_nanos(ts)?));
        }
        if !query.search.is_empty() {
            // Backslash is PostgreSQL's default LIKE escape character; an
            // explicit ESCAPE clause is avoided because sea-query renders it
            // parenthesized, which PostgreSQL rejects.
            let needle = LikeExpr::new(format!("%{}%", escape_like(&query.search)));
            select.cond_where(
                Condition::any()
                    .add(Expr::col(Logs::BodyText).ilike(needle.clone()))
                    .add(Expr::col(Logs::SeverityText).ilike(needle.clone()))
                    .add(
                        Expr::col(Logs::Attributes)
                            .cast_as(Alias::new("TEXT"))
                            .ilike(needle),
                    ),
            );
        }
        let (sql, values) = select.build_sqlx(PostgresQueryBuilder);
        let rows = sqlx::query_with(AssertSqlSafe(sql), values)
            .fetch_all(&self.reader)
            .await
            .context("query logs")?;
        rows.into_iter()
            .map(|row| row.try_get("payload").map_err(Into::into))
            .collect()
    }

    pub async fn log_services(&self) -> Result<Vec<String>> {
        let (sql, values) = Query::select()
            .column(Logs::ServiceName)
            .distinct()
            .from(Logs::Table)
            .order_by(Logs::ServiceName, Order::Asc)
            .build_sqlx(PostgresQueryBuilder);
        Ok(sqlx::query_with(AssertSqlSafe(sql), values)
            .fetch_all(&self.reader)
            .await?
            .into_iter()
            .map(|r| r.get("service_name"))
            .collect())
    }
}

/// Build the row for one log record, including its singleton OTLP payload.
fn log_row(
    resource_logs: &ResourceLogs,
    scope_logs: &ScopeLogs,
    record: &LogRecord,
    service: &str,
    resource_attrs: &Value,
) -> Result<LogRow> {
    let time = if record.time_unix_nano != 0 {
        record.time_unix_nano
    } else {
        record.observed_time_unix_nano
    };
    Ok(LogRow {
        time: u64_to_i64(time, "log timestamp")?,
        observed: u64_to_i64(record.observed_time_unix_nano, "observed timestamp")?,
        severity_number: record.severity_number,
        severity_text: record.severity_text.clone(),
        service: service.to_owned(),
        trace_id: record.trace_id.clone(),
        span_id: record.span_id.clone(),
        body: body_text(record),
        attributes: attributes_to_json(&record.attributes),
        resource_attributes: resource_attrs.clone(),
        payload: singleton_payload(resource_logs, scope_logs, record).encode_to_vec(),
    })
}

fn singleton_payload(resource: &ResourceLogs, scope: &ScopeLogs, record: &LogRecord) -> LogsData {
    LogsData {
        resource_logs: vec![ResourceLogs {
            resource: resource.resource.clone(),
            scope_logs: vec![ScopeLogs {
                scope: scope.scope.clone(),
                log_records: vec![record.clone()],
                schema_url: scope.schema_url.clone(),
            }],
            schema_url: resource.schema_url.clone(),
        }],
    }
}

/// Render the body for substring search: plain strings stay as-is, structured
/// bodies become their JSON text.
fn body_text(record: &LogRecord) -> String {
    match otel_value_to_json(record.body.as_ref()) {
        Value::String(s) => s,
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn escape_like(needle: &str) -> String {
    needle
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

pub(crate) fn hex_decode(hex: &str) -> Option<Vec<u8>> {
    if !hex.len().is_multiple_of(2) {
        return None;
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).ok())
        .collect()
}

pub fn decode_logs_payload(bytes: &[u8]) -> Result<LogsData> {
    Ok(LogsData::decode(bytes)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, any_value};

    #[test]
    fn hex_decode_round_trips() {
        assert_eq!(hex_decode("0aff"), Some(vec![0x0a, 0xff]));
        assert_eq!(hex_decode("0a f"), None);
        assert_eq!(hex_decode("abc"), None);
        assert_eq!(hex_decode(""), Some(vec![]));
    }

    #[test]
    fn body_text_keeps_strings_and_serializes_structures() {
        let record = LogRecord {
            body: Some(AnyValue {
                value: Some(any_value::Value::StringValue("plain message".into())),
            }),
            ..Default::default()
        };
        assert_eq!(body_text(&record), "plain message");
        let record = LogRecord {
            body: None,
            ..Default::default()
        };
        assert_eq!(body_text(&record), "");
    }

    #[test]
    fn like_escaping_neutralizes_wildcards() {
        assert_eq!(escape_like("100%_done\\"), "100\\%\\_done\\\\");
    }

    #[test]
    fn singleton_payload_is_lossless() {
        let record = LogRecord {
            severity_number: 9,
            severity_text: "INFO".into(),
            time_unix_nano: 42,
            ..Default::default()
        };
        let scope = ScopeLogs {
            log_records: vec![record.clone()],
            ..Default::default()
        };
        let resource = ResourceLogs {
            scope_logs: vec![scope.clone()],
            ..Default::default()
        };
        let encoded = singleton_payload(&resource, &scope, &record).encode_to_vec();
        let decoded = decode_logs_payload(&encoded).unwrap();
        assert_eq!(decoded.resource_logs[0].scope_logs[0].log_records[0], record);
    }

    #[test]
    fn find_logs_query_binds_search_values() {
        let mut select = Query::select();
        select
            .column(Logs::Payload)
            .from(Logs::Table)
            .and_where(Expr::col(Logs::ServiceName).eq("api' OR true --"));
        let (sql, _) = select.build_sqlx(PostgresQueryBuilder);
        assert!(!sql.as_str().contains("OR true"));
    }
}

