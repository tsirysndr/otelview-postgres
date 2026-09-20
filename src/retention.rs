//! Time-based retention for all three signals.
//!
//! Telemetry accumulates fast — a day of ordinary ingest here put millions of
//! metric points and gigabytes on disk — and none of it is worth keeping
//! forever. `RETENTION` names how long a row may live ("7d", "2w", "1mo",
//! "36h"); a background sweep deletes older rows from `spans`, `logs` and
//! `metric_points` on every pass. Unset means what it meant before: keep
//! everything.
//!
//! Deletes run in bounded batches by `ctid` so a first sweep over a large
//! backlog holds no long transaction and no lock anyone would notice; space
//! is returned to Postgres by autovacuum as usual.

use std::time::Duration;

use anyhow::{Context, Result, bail};

use crate::store::Store;

/// Rows deleted per statement. Small enough that each batch is a short
/// transaction even over a slow link, large enough that a backlog drains in
/// few round-trips.
const BATCH_ROWS: usize = 10_000;

/// Parse a humane duration: `<n><unit>` with unit `h`, `d`, `w`, `m`/`mo`
/// (months, as 30 days). "1week, 1 month, x weeks, x days" all reduce to
/// these; whitespace and a trailing `s` are tolerated so `2 weeks` works.
pub fn parse_retention(raw: &str) -> Result<Duration> {
    let s = raw.trim().to_lowercase().replace(' ', "");
    let split = s
        .find(|c: char| !c.is_ascii_digit())
        .with_context(|| format!("retention {raw:?} has no unit (try 7d, 2w, 1mo)"))?;
    let (digits, unit) = s.split_at(split);
    let n: u64 = digits
        .parse()
        .with_context(|| format!("retention {raw:?} has no number (try 7d, 2w, 1mo)"))?;
    if n == 0 {
        bail!("retention {raw:?} is zero, which would delete everything on arrival");
    }
    let hours = match unit.trim_end_matches('s') {
        "h" | "hour" => n,
        "d" | "day" => n * 24,
        "w" | "week" => n * 24 * 7,
        "m" | "mo" | "month" => n * 24 * 30,
        other => bail!("retention unit {other:?} not understood (h, d, w, m/mo)"),
    };
    Ok(Duration::from_secs(hours * 3600))
}

impl Store {
    /// One pass: delete everything older than `retention` from all three
    /// tables. Returns rows deleted per table, for the log line.
    pub async fn sweep_expired(&self, retention: Duration) -> Result<[(&'static str, u64); 3]> {
        let now_nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .context("system clock before the epoch")?
            .as_nanos();
        let cutoff = (now_nanos.saturating_sub(retention.as_nanos())).min(i64::MAX as u128) as i64;

        let mut out = [("spans", 0u64), ("logs", 0u64), ("metric_points", 0u64)];
        for (table, column, slot) in [
            ("spans", "start_time_unix_nano", 0usize),
            ("logs", "time_unix_nano", 1),
            ("metric_points", "time_unix_nano", 2),
        ] {
            // Batched by ctid: `DELETE .. WHERE time < $1` on a large backlog
            // is one giant transaction; this keeps each one small. The inner
            // select is driven by the time index, so a quiet pass costs one
            // probe per table. Identifiers are the literals above, not input.
            let sql = format!(
                "DELETE FROM {table} WHERE ctid IN \
                 (SELECT ctid FROM {table} WHERE {column} < $1 LIMIT {BATCH_ROWS})",
            );
            loop {
                let deleted = sqlx::query(sqlx::AssertSqlSafe(sql.clone()))
                    .bind(cutoff)
                    .execute(&self.primary)
                    .await
                    .with_context(|| format!("sweep expired rows from {table}"))?
                    .rows_affected();
                out[slot].1 += deleted;
                if (deleted as usize) < BATCH_ROWS {
                    break;
                }
            }
        }
        Ok(out)
    }

    /// The retention loop: an immediate sweep, then one per interval, forever.
    /// Sweep failures are logged and retried next round rather than taking
    /// the storage down — losing a sweep is recoverable, losing ingest is not.
    pub async fn run_retention(self, retention: Duration, every: Duration) {
        tracing::info!(
            retention_hours = retention.as_secs() / 3600,
            sweep_interval_secs = every.as_secs(),
            "retention enabled"
        );
        let mut ticker = tokio::time::interval(every);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            match self.sweep_expired(retention).await {
                Ok(counts) => {
                    let total: u64 = counts.iter().map(|(_, n)| n).sum();
                    if total > 0 {
                        tracing::info!(
                            spans = counts[0].1,
                            logs = counts[1].1,
                            metric_points = counts[2].1,
                            "retention sweep deleted expired rows"
                        );
                    } else {
                        tracing::debug!("retention sweep: nothing expired");
                    }
                }
                Err(error) => {
                    tracing::warn!(%error, "retention sweep failed; retrying next interval");
                }
            }
        }
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn humane_durations_parse() {
        let hours = |s: &str| parse_retention(s).unwrap().as_secs() / 3600;
        assert_eq!(hours("36h"), 36);
        assert_eq!(hours("7d"), 7 * 24);
        assert_eq!(hours("1w"), 7 * 24);
        assert_eq!(hours("2 weeks"), 14 * 24);
        assert_eq!(hours("1mo"), 30 * 24);
        assert_eq!(hours("3 months"), 90 * 24);
        assert_eq!(hours("1m"), 30 * 24, "m is months, not minutes");
        assert_eq!(hours("90 days"), 90 * 24);
    }

    #[test]
    fn nonsense_is_refused() {
        for bad in ["", "d", "7", "0d", "7 fortnights", "-3d", "1.5d"] {
            assert!(parse_retention(bad).is_err(), "{bad:?} should not parse");
        }
    }
}
