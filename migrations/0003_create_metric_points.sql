CREATE TABLE IF NOT EXISTS metric_points (
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    metric_name         TEXT NOT NULL,
    description         TEXT NOT NULL,
    unit                TEXT NOT NULL,
    metric_type         TEXT NOT NULL,
    service_name        TEXT NOT NULL,
    time_unix_nano      BIGINT NOT NULL,
    value               DOUBLE PRECISION NOT NULL,
    count               BIGINT NOT NULL DEFAULT 0,
    attributes          JSONB NOT NULL DEFAULT '{}',
    resource_attributes JSONB NOT NULL DEFAULT '{}',
    payload             BYTEA NOT NULL,
    inserted_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- FindMetrics: exact metric name, optional service, time window, time-ordered.
-- The (name, service, time) index also serves ListMetrics' distinct
-- (metric_name, service_name) scan through its prefix.
CREATE INDEX IF NOT EXISTS metric_points_name_time_idx
    ON metric_points (metric_name, time_unix_nano);
CREATE INDEX IF NOT EXISTS metric_points_name_service_time_idx
    ON metric_points (metric_name, service_name, time_unix_nano);
-- GetServices: distinct service names.
CREATE INDEX IF NOT EXISTS metric_points_service_idx
    ON metric_points (service_name);
