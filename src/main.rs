use std::{net::SocketAddr, time::Duration};

use anyhow::Context;
use clap::Parser;
use opentelemetry_proto::tonic::collector::{
    logs::v1::logs_service_server::LogsServiceServer,
    metrics::v1::metrics_service_server::MetricsServiceServer,
    trace::v1::trace_service_server::TraceServiceServer,
};
use otelview_postgres::{
    banner::startup_banner,
    config::Config,
    proto::otelview::{
        diagnostics_server::DiagnosticsServer, log_reader_server::LogReaderServer,
        metric_reader_server::MetricReaderServer,
    },
    proto::storage::{
        dependency_reader_server::DependencyReaderServer, trace_reader_server::TraceReaderServer,
    },
    server::StorageServer,
    store::Store,
};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use tonic::transport::Server;
use tracing_subscriber::{EnvFilter, fmt::format::FmtSpan};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| "otelview_postgres=info".into()),
        )
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .init();
    let config = Config::parse();
    let address: SocketAddr = config.listen_addr.parse().context("parse LISTEN_ADDR")?;
    let primary = PgPoolOptions::new()
        .max_connections(config.database_max_connections)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&config.database_url)
        .await
        .context("connect to PostgreSQL from DATABASE_URL")?;
    let reader = match config.database_read_url.as_deref().filter(|u| !u.is_empty()) {
        Some(read_url) => {
            // Belt and braces: even if the URL points at the primary, the
            // read pool's sessions cannot write.
            let options = read_url
                .parse::<PgConnectOptions>()
                .context("parse DATABASE_READ_URL")?
                .options([("default_transaction_read_only", "on")]);
            let pool = PgPoolOptions::new()
                .max_connections(config.database_max_connections)
                .acquire_timeout(Duration::from_secs(10))
                .connect_with(options)
                .await
                .context("connect to PostgreSQL from DATABASE_READ_URL")?;
            tracing::info!("read/write split enabled: queries use DATABASE_READ_URL");
            Some(pool)
        }
        None => {
            tracing::info!("DATABASE_READ_URL not set: primary serves reads and writes");
            None
        }
    };
    let store = Store::new(primary, reader, config.max_search_depth);
    store.migrate().await?;
    let service = StorageServer::new(store);

    println!("{}", startup_banner(address));
    tracing::info!(%address, "otelview PostgreSQL storage ready");
    Server::builder()
        .add_service(TraceServiceServer::new(service.clone()))
        .add_service(LogsServiceServer::new(service.clone()))
        .add_service(MetricsServiceServer::new(service.clone()))
        .add_service(TraceReaderServer::new(service.clone()))
        .add_service(DependencyReaderServer::new(service.clone()))
        .add_service(LogReaderServer::new(service.clone()))
        .add_service(MetricReaderServer::new(service.clone()))
        .add_service(DiagnosticsServer::new(service))
        .serve_with_shutdown(address, shutdown())
        .await?;
    Ok(())
}

async fn shutdown() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("install Ctrl-C handler")
    };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! { _ = ctrl_c => {}, _ = terminate => {} }
}
