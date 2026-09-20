use clap::Parser;

#[derive(Debug, Clone, Parser)]
#[command(author, version, about)]
pub struct Config {
    /// PostgreSQL connection string (primary, read + write).
    #[arg(long, env = "DATABASE_URL")]
    pub database_url: String,

    /// Optional connection string for a read-only replica. When set, every
    /// query runs against it (with read-only transactions enforced) while
    /// writes and migrations stay on the primary; when unset, the primary
    /// serves both.
    #[arg(long, env = "DATABASE_READ_URL")]
    pub database_read_url: Option<String>,

    /// Address exposed to otelview, Jaeger and OTLP exporters.
    #[arg(long, env = "LISTEN_ADDR", default_value = "0.0.0.0:17271")]
    pub listen_addr: String,

    #[arg(long, env = "DATABASE_MAX_CONNECTIONS", default_value_t = 20)]
    pub database_max_connections: u32,

    /// Safety ceiling for Jaeger search depth.
    #[arg(long, env = "MAX_SEARCH_DEPTH", default_value_t = 1000)]
    pub max_search_depth: u64,
}
