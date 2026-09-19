CREATE TABLE IF NOT EXISTS spans (
    trace_id             BYTEA NOT NULL CHECK (octet_length(trace_id) = 16),
    span_id              BYTEA NOT NULL CHECK (octet_length(span_id) = 8),
    parent_span_id       BYTEA NOT NULL,
    service_name         TEXT NOT NULL,
    operation_name       TEXT NOT NULL,
    span_kind            TEXT NOT NULL,
    start_time_unix_nano BIGINT NOT NULL,
    end_time_unix_nano   BIGINT NOT NULL,
    duration_nano        BIGINT NOT NULL,
    status_code          SMALLINT NOT NULL,
    span_attributes      JSONB NOT NULL DEFAULT '{}',
    resource_attributes  JSONB NOT NULL DEFAULT '{}',
    scope_attributes     JSONB NOT NULL DEFAULT '{}',
    payload              BYTEA NOT NULL,
    inserted_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (trace_id, span_id)
);

CREATE INDEX IF NOT EXISTS spans_trace_start_idx
    ON spans (trace_id, start_time_unix_nano);
CREATE INDEX IF NOT EXISTS spans_service_start_idx
    ON spans (service_name, start_time_unix_nano DESC);
CREATE INDEX IF NOT EXISTS spans_service_operation_start_idx
    ON spans (service_name, operation_name, start_time_unix_nano DESC);
CREATE INDEX IF NOT EXISTS spans_parent_idx
    ON spans (trace_id, parent_span_id);
CREATE INDEX IF NOT EXISTS spans_span_attributes_gin
    ON spans USING GIN (span_attributes jsonb_path_ops);
CREATE INDEX IF NOT EXISTS spans_resource_attributes_gin
    ON spans USING GIN (resource_attributes jsonb_path_ops);
CREATE INDEX IF NOT EXISTS spans_scope_attributes_gin
    ON spans USING GIN (scope_attributes jsonb_path_ops);
