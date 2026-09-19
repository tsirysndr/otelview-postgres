CREATE TABLE IF NOT EXISTS logs (
    id                      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    time_unix_nano          BIGINT NOT NULL,
    observed_time_unix_nano BIGINT NOT NULL,
    severity_number         INTEGER NOT NULL,
    severity_text           TEXT NOT NULL,
    service_name            TEXT NOT NULL,
    trace_id                BYTEA NOT NULL,
    span_id                 BYTEA NOT NULL,
    body_text               TEXT NOT NULL,
    attributes              JSONB NOT NULL DEFAULT '{}',
    resource_attributes     JSONB NOT NULL DEFAULT '{}',
    payload                 BYTEA NOT NULL,
    inserted_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- FindLogs always sorts newest first with an optional time window.
CREATE INDEX IF NOT EXISTS logs_time_idx
    ON logs (time_unix_nano DESC);
-- Service filter plus the same ordering.
CREATE INDEX IF NOT EXISTS logs_service_time_idx
    ON logs (service_name, time_unix_nano DESC);
-- Severity floor plus ordering (min_severity queries).
CREATE INDEX IF NOT EXISTS logs_severity_time_idx
    ON logs (severity_number, time_unix_nano DESC);
-- Trace correlation lookups; most records have no trace id, so keep the
-- index partial.
CREATE INDEX IF NOT EXISTS logs_trace_idx
    ON logs (trace_id, time_unix_nano DESC)
    WHERE octet_length(trace_id) = 16;
-- Attribute containment queries (parity with the spans table).
CREATE INDEX IF NOT EXISTS logs_attributes_gin
    ON logs USING GIN (attributes jsonb_path_ops);
CREATE INDEX IF NOT EXISTS logs_resource_attributes_gin
    ON logs USING GIN (resource_attributes jsonb_path_ops);

-- Substring search over the rendered body benefits from a trigram index.
-- pg_trgm needs privileges we may not have, so it is best-effort: without it
-- ILIKE falls back to a scan bounded by the time indexes above.
DO $$
BEGIN
    BEGIN
        CREATE EXTENSION IF NOT EXISTS pg_trgm;
    EXCEPTION WHEN insufficient_privilege THEN
        NULL;
    END;
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_trgm') THEN
        CREATE INDEX IF NOT EXISTS logs_body_trgm_idx
            ON logs USING GIN (body_text gin_trgm_ops);
    END IF;
END $$;
