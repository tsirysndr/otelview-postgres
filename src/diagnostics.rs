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
        // Planner estimates, not COUNT(*): exact counts scan every row of
        // every table, and with millions of points that took the better part
        // of two minutes — for a diagnostics tile. n_live_tup tracks inserts
        // as they happen and autovacuum/ANALYZE trues it up, so it is within
        // a few percent on an append-only workload, which is all a counter in
        // a status view needs. Static SQL, no user input.
        let row = sqlx::query(
            "SELECT COALESCE((SELECT n_live_tup FROM pg_stat_user_tables \
                              WHERE relname = 'spans'), 0) AS spans, \
                    COALESCE((SELECT n_live_tup FROM pg_stat_user_tables \
                              WHERE relname = 'logs'), 0) AS logs, \
                    COALESCE((SELECT n_live_tup FROM pg_stat_user_tables \
                              WHERE relname = 'metric_points'), 0) AS metric_points",
        )
        .fetch_one(&self.reader)
        .await
        .context("query storage stats")?;
        let get = |name: &str| -> Result<u64> {
            let value: i64 = row.try_get(name)?;
            Ok(value.max(0) as u64)
        };

        // Exact, but cheap: each list is a loose index scan, a handful of
        // probes per table rather than a walk over it.
        let mut services: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        services.extend(self.services().await?);
        services.extend(self.log_services().await?);
        services.extend(self.metric_services().await?);

        Ok(Stats {
            spans: get("spans")?,
            logs: get("logs")?,
            metric_points: get("metric_points")?,
            services: services.len() as u64,
        })
    }
}
