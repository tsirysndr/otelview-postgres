//! Storage counters for `otelview.storage.v1.Diagnostics`.

use anyhow::{Context, Result};
use sqlx::Row;

use crate::store::Store;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Stats {
    pub spans: u64,
    pub logs: u64,
    pub metric_points: u64,
    pub services: u64,
}

impl Store {
    pub async fn stats(&self) -> Result<Stats> {
        // Static SQL, no user input.
        let row = sqlx::query(
            "SELECT (SELECT COUNT(*) FROM spans) AS spans, \
                    (SELECT COUNT(*) FROM logs) AS logs, \
                    (SELECT COUNT(*) FROM metric_points) AS metric_points, \
                    (SELECT COUNT(*) FROM (SELECT service_name FROM spans \
                                           UNION SELECT service_name FROM logs \
                                           UNION SELECT service_name FROM metric_points) s) \
                        AS services",
        )
        .fetch_one(&self.reader)
        .await
        .context("query storage stats")?;
        let get = |name: &str| -> Result<u64> {
            let value: i64 = row.try_get(name)?;
            Ok(value.max(0) as u64)
        };
        Ok(Stats {
            spans: get("spans")?,
            logs: get("logs")?,
            metric_points: get("metric_points")?,
            services: get("services")?,
        })
    }
}
