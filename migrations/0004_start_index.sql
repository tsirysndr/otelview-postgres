-- An un-filtered trace search is "the newest N spans": ORDER BY
-- start_time_unix_nano DESC LIMIT n with no service to narrow it. Every
-- existing index leads with trace_id or service_name, so that query sorted
-- the whole table. This gives it a direct walk.
CREATE INDEX IF NOT EXISTS spans_start_idx
    ON spans (start_time_unix_nano DESC);
