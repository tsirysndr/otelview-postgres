# otelview-postgres

PostgreSQL remote storage for all three OpenTelemetry signals — traces, logs
and metrics — implemented in Rust with Tonic, SQLx, and SeaQuery. Started as a
copy of [jaeger-postgres](https://github.com/tsirysndr/jaeger-postgres) and
extended with the [otelview](https://github.com/tsirysndr/otelview) storage
API, so a single gRPC listener is a complete `remote` backend for an otelview
instance and a Jaeger v2 remote trace storage at the same time.

One gRPC listener provides:

- OTLP `TraceService`, `LogsService` and `MetricsService` `Export` for writes
- Jaeger `jaeger.storage.v2.TraceReader` for trace queries
- Jaeger `jaeger.storage.v2.DependencyReader` for the dependency graph
- otelview `otelview.storage.v1.LogReader` for log search
- otelview `otelview.storage.v1.MetricReader` for metric listing and series
- otelview `otelview.storage.v1.Diagnostics` for storage counters

The original OTLP payloads (resource, scope, span/log record/metric data
point) are stored as protobuf, so reads stream back exactly what was
ingested — histogram buckets, exemplars, quantiles and schema URLs included.
Indexed columns and JSONB attribute copies support service, operation, time,
duration, severity, trace-correlation and attribute searches.

## Run

`DATABASE_URL` is required. The schema and indexes are created automatically at
startup.

```sh
export DATABASE_URL='postgres://postgres:postgres@localhost:5432/otelview'
cargo build --release
./target/release/otelview-postgres
```

After connecting to PostgreSQL and applying the schema, startup prints an ASCII
banner with the configured gRPC address and the services sharing its port.

Optional environment variables:

| Variable | Default | Meaning |
| --- | --- | --- |
| `DATABASE_READ_URL` | unset | Optional read-only replica connection string (see below) |
| `LISTEN_ADDR` | `0.0.0.0:17271` | Combined OTLP, Jaeger and otelview gRPC address |
| `DATABASE_MAX_CONNECTIONS` | `20` | SQLx pool size (applied to each pool) |
| `MAX_SEARCH_DEPTH` | `1000` | Upper bound for trace and log searches |
| `RETENTION` | unset | Delete telemetry older than this: `36h`, `7d`, `2w`, `1mo` (months are 30 days). Unset keeps everything |
| `RETENTION_SWEEP_INTERVAL` | `1h` | How often the retention sweep runs |
| `RUST_LOG` | `otelview_postgres=info` | Log filter |

Every gRPC request emits a start event, a completion event with busy/idle
timings, and an error event when the RPC fails. Request payloads are not logged.

### Read/write split

Set `DATABASE_READ_URL` to a read-only replica and connections are split:
every query (trace search, log search, metric series, stats, dependency
graph) runs against the replica while OTLP writes and migrations stay on the
primary `DATABASE_URL`. The read pool's sessions run with
`default_transaction_read_only = on`, so they cannot write even if the URL
points at the primary. When `DATABASE_READ_URL` is unset, the primary serves
reads and writes through a single pool — no replica required.

### Use as an otelview backend

Point otelview's `remote` storage backend at this server and every signal is
read from and written to PostgreSQL:

```yaml
storage:
  backend: remote
  remote:
    endpoint: "http://127.0.0.1:17271"
```

### Use as a Jaeger backend

Point the Jaeger v2 remote-storage backend at port `17271` with insecure TLS
for a local deployment. The backend implements the standard OTLP writer on
that same endpoint, as required by the Jaeger storage v2 contract.

## Schema and indexes

Three tables, one per signal: `spans`, `logs` and `metric_points`. Indexes are
matched to the query shapes each reader issues:

- `spans`: primary key `(trace_id, span_id)`, B-tree indexes on
  service/operation/time/parent, GIN (`jsonb_path_ops`) on span, resource and
  scope attributes.
- `logs`: B-tree indexes on time, `(service, time)` and `(severity, time)`;
  a partial index on `trace_id` for trace correlation; GIN on attributes; and
  a `pg_trgm` trigram index on the rendered body for substring search. The
  trigram index is best-effort — created only when the `pg_trgm` extension is
  available or installable.
- `metric_points`: B-tree indexes on `(metric_name, time)` and
  `(metric_name, service_name, time)` (the latter also serves `ListMetrics`
  through its prefix), plus `service_name` for `GetServices`.

The migrations are also available under `migrations/` for operators that
manage DDL separately.

## Test

```sh
cargo test
cargo build --release
```

## Install with Nix

The flake builds the server with [crane](https://github.com/ipetkov/crane)
for `aarch64-darwin`, `x86_64-linux` and `aarch64-linux`, with pre-built
artifacts served from the `otelview` Cachix cache (advertised through the
flake's `nixConfig`, so Nix offers it automatically):

```sh
# Run without installing
nix run github:tsirysndr/otelview-postgres

# Install into your profile
nix profile install github:tsirysndr/otelview-postgres

# Or from a checkout
nix build .#otelview-postgres
./result/bin/otelview-postgres
```

`nix develop` drops into a shell with the Rust toolchain, `protoc` and
PostgreSQL client tools. The `nix` GitHub workflow (manually triggered)
builds all three systems and pushes the results to Cachix.

## Install a release

The installer detects macOS ARM64, Linux x86_64, or Linux ARM64 and verifies the
download against the release checksums:

```sh
curl --proto '=https' --tlsv1.2 -LsSf \
  https://raw.githubusercontent.com/tsirysndr/otelview-postgres/main/install.sh | sh
```

A root Linux installation also places the unit at
`/etc/systemd/system/otelview-postgres.service` and creates a private
environment file. Install system-wide, configure, and start it with:

```sh
curl --proto '=https' --tlsv1.2 -LsSf \
  https://raw.githubusercontent.com/tsirysndr/otelview-postgres/main/install.sh | sudo sh
sudo editor /etc/otelview-postgres/env
sudo systemctl enable --now otelview-postgres
sudo systemctl status otelview-postgres
```

The installer intentionally does not start the service until `DATABASE_URL` is
configured. Set `OTELVIEW_POSTGRES_VERSION=v0.1.2` to install a particular
tag, `INSTALL_DIR` to change the binary destination, or `INSTALL_SYSTEMD=0` to
skip the unit.

Tags matching `v*` trigger release builds for Darwin ARM64, Linux x86_64, and
Linux ARM64. The workflow publishes all three archives and `SHA256SUMS` to the
corresponding GitHub release.
