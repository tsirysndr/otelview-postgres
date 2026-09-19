use std::net::SocketAddr;

const LOGO: &str = r#"        _       _       _
   ___ | |_ ___| |_   _(_) _____      _____ _ __      _ __   __ _
  / _ \| __/ _ \ \ \ / / |/ _ \ \ /\ / / _ \ '__|____| '_ \ / _` |
 | (_) | ||  __/ |\ V /| |  __/\ V  V /  __/ | |_____| |_) | (_| |
  \___/ \__\___|_| \_/ |_|\___| \_/\_/ \___|_|       | .__/ \__, |
                                                     |_|    |___/    "#;

pub fn startup_banner(address: SocketAddr) -> String {
    format!(
        r#"{LOGO}

  PostgreSQL storage for traces, logs and metrics is ready
  ------------------------------------------------------------
  gRPC bind       {address}
  OTLP writes     opentelemetry.proto.collector.trace.v1.TraceService
                  opentelemetry.proto.collector.logs.v1.LogsService
                  opentelemetry.proto.collector.metrics.v1.MetricsService
  Trace reads     jaeger.storage.v2.TraceReader
  Dependencies    jaeger.storage.v2.DependencyReader
  Log reads       otelview.storage.v1.LogReader
  Metric reads    otelview.storage.v1.MetricReader
  Diagnostics     otelview.storage.v1.Diagnostics
  ------------------------------------------------------------
  All gRPC services share port {}.
"#,
        address.port()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn banner_displays_bind_address_and_services() {
        let banner = startup_banner("127.0.0.1:17271".parse().unwrap());
        assert!(banner.is_ascii());
        assert!(banner.contains("gRPC bind       127.0.0.1:17271"));
        assert!(banner.contains("OTLP writes"));
        assert!(banner.contains("TraceReader"));
        assert!(banner.contains("DependencyReader"));
        assert!(banner.contains("LogReader"));
        assert!(banner.contains("MetricReader"));
        assert!(banner.contains("Diagnostics"));
        assert!(banner.contains("share port 17271"));
    }

    #[test]
    fn banner_formats_ipv6_addresses() {
        let banner = startup_banner("[::1]:4317".parse().unwrap());
        assert!(banner.contains("[::1]:4317"));
        assert!(banner.contains("share port 4317"));
    }
}
