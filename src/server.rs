use std::{pin::Pin, sync::Arc};

use futures_core::Stream;
use opentelemetry_proto::tonic::{
    collector::{
        logs::v1::{
            ExportLogsServiceRequest, ExportLogsServiceResponse, logs_service_server::LogsService,
        },
        metrics::v1::{
            ExportMetricsServiceRequest, ExportMetricsServiceResponse,
            metrics_service_server::MetricsService,
        },
        trace::v1::{
            ExportTracePartialSuccess, ExportTraceServiceRequest, ExportTraceServiceResponse,
            trace_service_server::TraceService,
        },
    },
    logs::v1::LogsData,
    metrics::v1::MetricsData,
    trace::v1::TracesData,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::{
    logs::decode_logs_payload,
    metrics::decode_metrics_payload,
    proto::otelview::{
        FindLogsRequest, FindMetricsRequest, GetServicesRequest as OvGetServicesRequest,
        GetServicesResponse as OvGetServicesResponse, GetStatsRequest, GetStatsResponse,
        ListMetricsRequest, ListMetricsResponse, diagnostics_server::Diagnostics,
        log_reader_server::LogReader, metric_reader_server::MetricReader,
    },
    proto::storage::{
        Dependency, FindTraceIDsRequest, FindTraceIDsResponse, FindTraceSummariesRequest,
        FindTraceSummariesResponse, FindTracesRequest, GetDependenciesRequest,
        GetDependenciesResponse, GetOperationsRequest, GetOperationsResponse, GetServicesRequest,
        GetServicesResponse, GetTracesRequest, dependency_reader_server::DependencyReader,
        trace_reader_server::TraceReader,
    },
    store::{Store, decode_payload, timestamp_to_nanos},
};

type TraceStream = Pin<Box<dyn Stream<Item = Result<TracesData, Status>> + Send + 'static>>;
type SummaryStream =
    Pin<Box<dyn Stream<Item = Result<FindTraceSummariesResponse, Status>> + Send + 'static>>;
type LogsStream = Pin<Box<dyn Stream<Item = Result<LogsData, Status>> + Send + 'static>>;
type MetricsStream = Pin<Box<dyn Stream<Item = Result<MetricsData, Status>> + Send + 'static>>;

/// Payloads per streamed OTLP chunk.
const CHUNK_SIZE: usize = 256;

#[derive(Clone)]
pub struct StorageServer {
    store: Arc<Store>,
}

impl StorageServer {
    pub fn new(store: Store) -> Self {
        Self {
            store: Arc::new(store),
        }
    }

    async fn stream_ids(&self, ids: Vec<Vec<u8>>) -> Result<Response<TraceStream>, Status> {
        let spans = self.store.spans_for_ids(&ids).await.map_err(internal)?;
        let (tx, rx) = mpsc::channel(16);
        tokio::spawn(async move {
            for span in spans {
                if tx
                    .send(decode_payload(&span.payload).map_err(internal))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

#[tonic::async_trait]
impl TraceService for StorageServer {
    #[tracing::instrument(
        name = "grpc_request",
        skip_all,
        fields(rpc = "opentelemetry.proto.collector.trace.v1.TraceService/Export"),
        err
    )]
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        let data = TracesData {
            resource_spans: request.into_inner().resource_spans,
        };
        let (rejected, errors) = self.store.write(data).await.map_err(internal)?;
        let partial_success = (rejected > 0).then(|| ExportTracePartialSuccess {
            rejected_spans: rejected as i64,
            error_message: errors.join("; "),
        });
        Ok(Response::new(ExportTraceServiceResponse {
            partial_success,
        }))
    }
}

#[tonic::async_trait]
impl TraceReader for StorageServer {
    type GetTracesStream = TraceStream;
    type FindTracesStream = TraceStream;
    type FindTraceSummariesStream = SummaryStream;

    #[tracing::instrument(
        name = "grpc_request",
        skip_all,
        fields(rpc = "jaeger.storage.v2.TraceReader/GetTraces"),
        err
    )]
    async fn get_traces(
        &self,
        request: Request<GetTracesRequest>,
    ) -> Result<Response<Self::GetTracesStream>, Status> {
        let query = request.into_inner().query;
        validate_trace_ids(query.iter().map(|item| item.trace_id.as_slice()))?;
        self.stream_ids(query.into_iter().map(|q| q.trace_id).collect())
            .await
    }

    #[tracing::instrument(
        name = "grpc_request",
        skip_all,
        fields(rpc = "jaeger.storage.v2.TraceReader/GetServices"),
        err
    )]
    async fn get_services(
        &self,
        _: Request<GetServicesRequest>,
    ) -> Result<Response<GetServicesResponse>, Status> {
        Ok(Response::new(GetServicesResponse {
            services: self.store.services().await.map_err(internal)?,
        }))
    }

    #[tracing::instrument(
        name = "grpc_request",
        skip_all,
        fields(rpc = "jaeger.storage.v2.TraceReader/GetOperations"),
        err
    )]
    async fn get_operations(
        &self,
        request: Request<GetOperationsRequest>,
    ) -> Result<Response<GetOperationsResponse>, Status> {
        let request = request.into_inner();
        if request.service.is_empty() {
            return Err(Status::invalid_argument("service is required"));
        }
        Ok(Response::new(GetOperationsResponse {
            operations: self
                .store
                .operations(&request.service, &request.span_kind)
                .await
                .map_err(internal)?,
        }))
    }

    #[tracing::instrument(
        name = "grpc_request",
        skip_all,
        fields(rpc = "jaeger.storage.v2.TraceReader/FindTraces"),
        err
    )]
    async fn find_traces(
        &self,
        request: Request<FindTracesRequest>,
    ) -> Result<Response<Self::FindTracesStream>, Status> {
        let query = request
            .into_inner()
            .query
            .ok_or_else(|| Status::invalid_argument("query is required"))?;
        let ids = self
            .store
            .find_ids(&query)
            .await
            .map_err(invalid_or_internal)?;
        self.stream_ids(ids.into_iter().map(|id| id.trace_id).collect())
            .await
    }

    #[tracing::instrument(
        name = "grpc_request",
        skip_all,
        fields(rpc = "jaeger.storage.v2.TraceReader/FindTraceIDs"),
        err
    )]
    async fn find_trace_i_ds(
        &self,
        request: Request<FindTraceIDsRequest>,
    ) -> Result<Response<FindTraceIDsResponse>, Status> {
        let query = request
            .into_inner()
            .query
            .ok_or_else(|| Status::invalid_argument("query is required"))?;
        Ok(Response::new(FindTraceIDsResponse {
            trace_ids: self
                .store
                .find_ids(&query)
                .await
                .map_err(invalid_or_internal)?,
        }))
    }

    #[tracing::instrument(
        name = "grpc_request",
        skip_all,
        fields(rpc = "jaeger.storage.v2.TraceReader/FindTraceSummaries"),
        err
    )]
    async fn find_trace_summaries(
        &self,
        request: Request<FindTraceSummariesRequest>,
    ) -> Result<Response<Self::FindTraceSummariesStream>, Status> {
        let query = request
            .into_inner()
            .query
            .ok_or_else(|| Status::invalid_argument("query is required"))?;
        let ids = self
            .store
            .find_ids(&query)
            .await
            .map_err(invalid_or_internal)?;
        let ids = ids.into_iter().map(|id| id.trace_id).collect::<Vec<_>>();
        let spans = self.store.spans_for_ids(&ids).await.map_err(internal)?;
        let stream = tokio_stream::once(Ok(FindTraceSummariesResponse {
            summaries: self.store.summaries(spans),
        }));
        Ok(Response::new(Box::pin(stream)))
    }
}

#[tonic::async_trait]
impl DependencyReader for StorageServer {
    #[tracing::instrument(
        name = "grpc_request",
        skip_all,
        fields(rpc = "jaeger.storage.v2.DependencyReader/GetDependencies"),
        err
    )]
    async fn get_dependencies(
        &self,
        request: Request<GetDependenciesRequest>,
    ) -> Result<Response<GetDependenciesResponse>, Status> {
        let request = request.into_inner();
        let start = timestamp_to_nanos(
            request
                .start_time
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("start_time is required"))?,
        )
        .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let end = timestamp_to_nanos(
            request
                .end_time
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("end_time is required"))?,
        )
        .map_err(|e| Status::invalid_argument(e.to_string()))?;
        if start >= end {
            return Err(Status::invalid_argument(
                "start_time must be before end_time",
            ));
        }
        let dependencies = self
            .store
            .dependencies(start, end)
            .await
            .map_err(internal)?
            .into_iter()
            .map(|(parent, child, calls)| Dependency {
                parent,
                child,
                call_count: calls.max(0) as u64,
                source: "postgres".into(),
            })
            .collect();
        Ok(Response::new(GetDependenciesResponse { dependencies }))
    }
}

#[tonic::async_trait]
impl LogsService for StorageServer {
    #[tracing::instrument(
        name = "grpc_request",
        skip_all,
        fields(rpc = "opentelemetry.proto.collector.logs.v1.LogsService/Export"),
        err
    )]
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        let data = LogsData {
            resource_logs: request.into_inner().resource_logs,
        };
        self.store.write_logs(data).await.map_err(internal)?;
        Ok(Response::new(ExportLogsServiceResponse {
            partial_success: None,
        }))
    }
}

#[tonic::async_trait]
impl MetricsService for StorageServer {
    #[tracing::instrument(
        name = "grpc_request",
        skip_all,
        fields(rpc = "opentelemetry.proto.collector.metrics.v1.MetricsService/Export"),
        err
    )]
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        let data = MetricsData {
            resource_metrics: request.into_inner().resource_metrics,
        };
        self.store.write_metrics(data).await.map_err(internal)?;
        Ok(Response::new(ExportMetricsServiceResponse {
            partial_success: None,
        }))
    }
}

#[tonic::async_trait]
impl LogReader for StorageServer {
    type FindLogsStream = LogsStream;

    #[tracing::instrument(
        name = "grpc_request",
        skip_all,
        fields(rpc = "otelview.storage.v1.LogReader/GetServices"),
        err
    )]
    async fn get_services(
        &self,
        _: Request<OvGetServicesRequest>,
    ) -> Result<Response<OvGetServicesResponse>, Status> {
        Ok(Response::new(OvGetServicesResponse {
            services: self.store.log_services().await.map_err(internal)?,
        }))
    }

    #[tracing::instrument(
        name = "grpc_request",
        skip_all,
        fields(rpc = "otelview.storage.v1.LogReader/FindLogs"),
        err
    )]
    async fn find_logs(
        &self,
        request: Request<FindLogsRequest>,
    ) -> Result<Response<Self::FindLogsStream>, Status> {
        let query = request
            .into_inner()
            .query
            .ok_or_else(|| Status::invalid_argument("query is required"))?;
        let payloads = self
            .store
            .find_logs(&query)
            .await
            .map_err(invalid_or_internal)?;
        stream_chunks(payloads, |payloads| {
            let mut chunk = LogsData {
                resource_logs: Vec::new(),
            };
            for payload in payloads {
                chunk
                    .resource_logs
                    .extend(decode_logs_payload(payload)?.resource_logs);
            }
            Ok(chunk)
        })
    }
}

#[tonic::async_trait]
impl MetricReader for StorageServer {
    type FindMetricsStream = MetricsStream;

    #[tracing::instrument(
        name = "grpc_request",
        skip_all,
        fields(rpc = "otelview.storage.v1.MetricReader/GetServices"),
        err
    )]
    async fn get_services(
        &self,
        _: Request<OvGetServicesRequest>,
    ) -> Result<Response<OvGetServicesResponse>, Status> {
        Ok(Response::new(OvGetServicesResponse {
            services: self.store.metric_services().await.map_err(internal)?,
        }))
    }

    #[tracing::instrument(
        name = "grpc_request",
        skip_all,
        fields(rpc = "otelview.storage.v1.MetricReader/ListMetrics"),
        err
    )]
    async fn list_metrics(
        &self,
        _: Request<ListMetricsRequest>,
    ) -> Result<Response<ListMetricsResponse>, Status> {
        Ok(Response::new(ListMetricsResponse {
            metrics: self.store.list_metrics().await.map_err(internal)?,
        }))
    }

    #[tracing::instrument(
        name = "grpc_request",
        skip_all,
        fields(rpc = "otelview.storage.v1.MetricReader/FindMetrics"),
        err
    )]
    async fn find_metrics(
        &self,
        request: Request<FindMetricsRequest>,
    ) -> Result<Response<Self::FindMetricsStream>, Status> {
        let query = request
            .into_inner()
            .query
            .ok_or_else(|| Status::invalid_argument("query is required"))?;
        let payloads = self
            .store
            .find_metrics(&query)
            .await
            .map_err(invalid_or_internal)?;
        stream_chunks(payloads, |payloads| {
            let mut chunk = MetricsData {
                resource_metrics: Vec::new(),
            };
            for payload in payloads {
                chunk
                    .resource_metrics
                    .extend(decode_metrics_payload(payload)?.resource_metrics);
            }
            Ok(chunk)
        })
    }
}

#[tonic::async_trait]
impl Diagnostics for StorageServer {
    #[tracing::instrument(
        name = "grpc_request",
        skip_all,
        fields(rpc = "otelview.storage.v1.Diagnostics/GetStats"),
        err
    )]
    async fn get_stats(
        &self,
        _: Request<GetStatsRequest>,
    ) -> Result<Response<GetStatsResponse>, Status> {
        let stats = self.store.stats().await.map_err(internal)?;
        Ok(Response::new(GetStatsResponse {
            spans: stats.spans,
            logs: stats.logs,
            metric_points: stats.metric_points,
            services: stats.services,
            backend: "postgres".into(),
        }))
    }
}

/// Stream stored payloads as merged OTLP chunks of up to [`CHUNK_SIZE`]
/// records each. Chunks are never empty, per the storage contract.
type ChunkStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

fn stream_chunks<T: Send + 'static>(
    payloads: Vec<Vec<u8>>,
    merge: impl Fn(&[Vec<u8>]) -> anyhow::Result<T> + Send + 'static,
) -> Result<Response<ChunkStream<T>>, Status> {
    let (tx, rx) = mpsc::channel(4);
    tokio::spawn(async move {
        for chunk in payloads.chunks(CHUNK_SIZE) {
            let item = merge(chunk).map_err(internal);
            if tx.send(item).await.is_err() {
                break;
            }
        }
    });
    Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
}

fn validate_trace_ids<'a>(ids: impl IntoIterator<Item = &'a [u8]>) -> Result<(), Status> {
    if ids.into_iter().any(|id| id.len() != 16) {
        Err(Status::invalid_argument(
            "trace_id must contain exactly 16 bytes",
        ))
    } else {
        Ok(())
    }
}

fn internal(error: impl std::fmt::Display) -> Status {
    tracing::error!(%error, "storage request failed");
    Status::internal("storage request failed")
}

fn invalid_or_internal(error: anyhow::Error) -> Status {
    let message = error.to_string();
    if message.contains("timestamp")
        || message.contains("duration")
        || message.contains("trace_id")
        || message.contains("metric_name")
    {
        Status::invalid_argument(message)
    } else {
        internal(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_invalid_trace_ids() {
        let ids = [vec![0; 16], vec![0; 8]];
        let status = validate_trace_ids(ids.iter().map(Vec::as_slice)).unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }
}
