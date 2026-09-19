pub mod storage {
    tonic::include_proto!("jaeger.storage.v2");
}

/// Generated code for the otelview remote-storage API (logs, metrics and
/// diagnostics readers). Writes for those signals reuse the standard OTLP
/// collector Export services.
pub mod otelview {
    tonic::include_proto!("otelview.storage.v1");
}
