use std::{future::Future, net::SocketAddr, sync::Arc};

use arrow_array::RecordBatch;
use arrow_flight::{
    flight_descriptor::DescriptorType,
    flight_service_server::{FlightService, FlightServiceServer},
    utils::{batches_to_flight_data, flight_data_to_batches},
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, IpcMessage, PollInfo, PutResult, SchemaAsIpc,
    SchemaResult, Ticket,
};
use arrow_ipc::writer::IpcWriteOptions;
use arrow_schema::Schema;
use ehdb_core::{
    ChunkId, ConsumerName, DocumentId, EhdbError, EmbeddingModelId, NamespaceName, PrincipalId,
    Result, StreamName, TableName, TenantId, TransactionId,
};
use ehdb_reference::{
    ArrowEqualityPredicate, LocalArrowSnapshotScanner, LocalReferenceRuntime, ScanArrowSnapshot,
};
use ehdb_retrieval::{HybridSearch, TextSearch, VectorSearch};
use ehdb_storage::ImmutableObjectStore;
use ehdb_stream::{
    DurableConsumer, InMemoryStreamLog, LocalJsonlStreamLog, RetentionPolicy, StreamConfig,
    StreamRecord, StreamSequence, Subject,
};
use futures_util::stream::{self, BoxStream, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tonic::metadata::{AsciiMetadataKey, MetadataMap};
use tonic::transport::{server::TcpIncoming, Server};
use tonic::{Code, Request, Response, Status, Streaming};

pub const SCAN_FLIGHT_TICKET_VERSION: &str = "ehdb.arrow.scan.v1";
pub const DEFAULT_FLIGHT_MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;
pub const DEFAULT_FLIGHT_MAX_CONCURRENT_REQUESTS: usize = 64;
pub const DEFAULT_FLIGHT_TENANT_SCOPE_HEADER: &str = "x-ehdb-tenant";
pub const DEFAULT_FLIGHT_NAMESPACE_SCOPE_HEADER: &str = "x-ehdb-namespace";
pub const DEFAULT_FLIGHT_PRINCIPAL_HEADER: &str = "x-ehdb-principal";
pub const RETRIEVAL_CONTEXT_REQUEST_VERSION: &str = "ehdb.retrieval.context.request.v1";
pub const RETRIEVAL_CONTEXT_RESULT_VERSION: &str = "ehdb.retrieval.context.result.v1";
pub const RETRIEVAL_CONTEXT_EXECUTION_RECEIPT_VERSION: &str =
    "ehdb.retrieval.context.execution.receipt.v1";
pub const RETRIEVAL_CONTEXT_EXECUTION_RECEIPT_EVENT_VERSION: &str =
    "ehdb.retrieval.context.execution.receipt.event.v1";
pub const RETRIEVAL_CONTEXT_EXECUTION_RECEIPT_EVENT_SUBJECT: &str =
    "ehdb.retrieval.context.execution.receipt";
pub const DEFAULT_RETRIEVAL_CONTEXT_MAX_REQUEST_PAYLOAD_BYTES: usize = 1024 * 1024;
pub const DEFAULT_RETRIEVAL_CONTEXT_MAX_RESULT_PAYLOAD_BYTES: usize = 4 * 1024 * 1024;
pub const DEFAULT_RETRIEVAL_CONTEXT_MAX_RECEIPT_PAYLOAD_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlightAuthPolicy {
    DisabledForLocalReference,
    HeaderToken { header_name: String, token: String },
    ExternalRequired,
}

impl FlightAuthPolicy {
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::DisabledForLocalReference | Self::ExternalRequired => Ok(()),
            Self::HeaderToken { header_name, token } => {
                Self::header_key(header_name).map_err(|err| {
                    EhdbError::InvalidState(format!("invalid Flight auth header name: {err}"))
                })?;
                if token.is_empty() {
                    return Err(EhdbError::InvalidState(
                        "Flight auth token must not be empty".to_string(),
                    ));
                }
                if token.len() > 512 {
                    return Err(EhdbError::InvalidState(
                        "Flight auth token must not exceed 512 bytes".to_string(),
                    ));
                }
                if token.bytes().any(|byte| byte.is_ascii_control()) {
                    return Err(EhdbError::InvalidState(
                        "Flight auth token must not contain control characters".to_string(),
                    ));
                }
                Ok(())
            }
        }
    }

    pub fn authorize_metadata(&self, metadata: &MetadataMap) -> Option<Status> {
        match self {
            Self::DisabledForLocalReference => None,
            Self::HeaderToken { header_name, token } => {
                let key = match Self::header_key(header_name) {
                    Ok(key) => key,
                    Err(_) => return Some(Status::internal("EHDB Flight auth policy is invalid")),
                };
                let Some(value) = metadata.get(&key) else {
                    return Some(Status::unauthenticated(
                        "EHDB Flight auth header is missing",
                    ));
                };
                if value.to_str().ok() == Some(token.as_str()) {
                    None
                } else {
                    Some(Status::unauthenticated(
                        "EHDB Flight auth header is invalid",
                    ))
                }
            }
            Self::ExternalRequired => Some(Status::unimplemented(
                "EHDB Flight external auth is not implemented",
            )),
        }
    }

    pub fn requires_request_metadata(&self) -> bool {
        !matches!(self, Self::DisabledForLocalReference)
    }

    fn header_key(header_name: &str) -> std::result::Result<AsciiMetadataKey, String> {
        if header_name.is_empty() {
            return Err("header name must not be empty".to_string());
        }
        if header_name.ends_with("-bin") {
            return Err("binary metadata headers are not supported".to_string());
        }
        AsciiMetadataKey::from_bytes(header_name.as_bytes()).map_err(|err| err.to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlightScanScopePolicy {
    DisabledForLocalReference,
    RequireTenantNamespace {
        tenant_header_name: String,
        namespace_header_name: String,
    },
}

impl FlightScanScopePolicy {
    pub fn require_default_tenant_namespace() -> Self {
        Self::RequireTenantNamespace {
            tenant_header_name: DEFAULT_FLIGHT_TENANT_SCOPE_HEADER.to_string(),
            namespace_header_name: DEFAULT_FLIGHT_NAMESPACE_SCOPE_HEADER.to_string(),
        }
    }

    pub fn validate(&self) -> Result<()> {
        match self {
            Self::DisabledForLocalReference => Ok(()),
            Self::RequireTenantNamespace {
                tenant_header_name,
                namespace_header_name,
            } => {
                FlightAuthPolicy::header_key(tenant_header_name).map_err(|err| {
                    EhdbError::InvalidState(format!(
                        "invalid Flight tenant scope header name: {err}"
                    ))
                })?;
                FlightAuthPolicy::header_key(namespace_header_name).map_err(|err| {
                    EhdbError::InvalidState(format!(
                        "invalid Flight namespace scope header name: {err}"
                    ))
                })?;
                if tenant_header_name == namespace_header_name {
                    return Err(EhdbError::InvalidState(
                        "Flight tenant and namespace scope headers must differ".to_string(),
                    ));
                }
                Ok(())
            }
        }
    }

    pub fn authorize_scan_metadata(
        &self,
        metadata: &MetadataMap,
        request: &ScanLatestTableRequest,
    ) -> Option<Status> {
        match self {
            Self::DisabledForLocalReference => None,
            Self::RequireTenantNamespace {
                tenant_header_name,
                namespace_header_name,
            } => {
                let tenant_key = match FlightAuthPolicy::header_key(tenant_header_name) {
                    Ok(key) => key,
                    Err(_) => {
                        return Some(Status::internal(
                            "EHDB Flight tenant scope policy is invalid",
                        ))
                    }
                };
                let namespace_key = match FlightAuthPolicy::header_key(namespace_header_name) {
                    Ok(key) => key,
                    Err(_) => {
                        return Some(Status::internal(
                            "EHDB Flight namespace scope policy is invalid",
                        ))
                    }
                };

                Self::match_metadata_scope(metadata, &tenant_key, "tenant", request.tenant.as_str())
                    .or_else(|| {
                        Self::match_metadata_scope(
                            metadata,
                            &namespace_key,
                            "namespace",
                            request.namespace.as_str(),
                        )
                    })
            }
        }
    }

    pub fn requires_request_metadata(&self) -> bool {
        !matches!(self, Self::DisabledForLocalReference)
    }

    fn match_metadata_scope(
        metadata: &MetadataMap,
        key: &AsciiMetadataKey,
        label: &str,
        expected: &str,
    ) -> Option<Status> {
        let Some(value) = metadata.get(key) else {
            return Some(Status::unauthenticated(format!(
                "EHDB Flight {label} scope header is missing"
            )));
        };
        if value.to_str().ok() == Some(expected) {
            None
        } else {
            Some(Status::permission_denied(format!(
                "EHDB Flight {label} scope does not match scan request"
            )))
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlightScanGrantPolicy {
    DisabledForLocalReference,
    RequireCatalogGrant { principal_header_name: String },
}

impl FlightScanGrantPolicy {
    pub fn require_default_principal() -> Self {
        Self::RequireCatalogGrant {
            principal_header_name: DEFAULT_FLIGHT_PRINCIPAL_HEADER.to_string(),
        }
    }

    pub fn validate(&self) -> Result<()> {
        match self {
            Self::DisabledForLocalReference => Ok(()),
            Self::RequireCatalogGrant {
                principal_header_name,
            } => FlightAuthPolicy::header_key(principal_header_name)
                .map(|_| ())
                .map_err(|err| {
                    EhdbError::InvalidState(format!("invalid Flight principal header name: {err}"))
                }),
        }
    }

    pub fn authorize_catalog_grant(
        &self,
        metadata: &MetadataMap,
        runtime: &LocalReferenceRuntime,
        request: &ScanLatestTableRequest,
    ) -> Option<Status> {
        match self {
            Self::DisabledForLocalReference => None,
            Self::RequireCatalogGrant {
                principal_header_name,
            } => {
                let principal_key = match FlightAuthPolicy::header_key(principal_header_name) {
                    Ok(key) => key,
                    Err(_) => {
                        return Some(Status::internal(
                            "EHDB Flight principal grant policy is invalid",
                        ))
                    }
                };
                let Some(value) = metadata.get(&principal_key) else {
                    return Some(Status::unauthenticated(
                        "EHDB Flight principal header is missing",
                    ));
                };
                let Some(principal) = value
                    .to_str()
                    .ok()
                    .and_then(|value| PrincipalId::new(value).ok())
                else {
                    return Some(Status::unauthenticated(
                        "EHDB Flight principal header is invalid",
                    ));
                };

                let table = match runtime.state().catalog.get_table(
                    &request.tenant,
                    &request.namespace,
                    &request.table_name,
                ) {
                    Ok(table) => table,
                    Err(error) => return Some(error_to_status(error)),
                };
                if runtime.state().catalog.can_scan(
                    &request.tenant,
                    &request.namespace,
                    &table.id,
                    &principal,
                ) {
                    None
                } else {
                    Some(Status::permission_denied(
                        "EHDB Flight principal is not granted scan access",
                    ))
                }
            }
        }
    }

    pub fn requires_request_metadata(&self) -> bool {
        !matches!(self, Self::DisabledForLocalReference)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlightAccessLogPolicy {
    Disabled,
    DebugOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlightScanCall {
    GetFlightInfo,
    GetSchema,
    DoGet,
}

impl FlightScanCall {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::GetFlightInfo => "get_flight_info",
            Self::GetSchema => "get_schema",
            Self::DoGet => "do_get",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlightScanAccessLogEntry {
    pub call: FlightScanCall,
    pub grpc_code: Code,
    pub row_count: Option<i64>,
    pub flight_data_message_count: Option<usize>,
    pub projection_count: Option<usize>,
    pub predicate_present: bool,
    pub auth_required: bool,
    pub scan_scope_required: bool,
    pub scan_grant_required: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct FlightScanAccessLogInput<'a> {
    pub call: FlightScanCall,
    pub request: &'a ScanLatestTableRequest,
    pub grpc_code: Code,
    pub row_count: Option<i64>,
    pub flight_data_message_count: Option<usize>,
    pub auth_policy: &'a FlightAuthPolicy,
    pub scan_scope_policy: &'a FlightScanScopePolicy,
    pub scan_grant_policy: &'a FlightScanGrantPolicy,
}

impl FlightAccessLogPolicy {
    pub fn scan_access_entry(
        &self,
        input: FlightScanAccessLogInput<'_>,
    ) -> Option<FlightScanAccessLogEntry> {
        match self {
            Self::Disabled => None,
            Self::DebugOnly => Some(FlightScanAccessLogEntry {
                call: input.call,
                grpc_code: input.grpc_code,
                row_count: input.row_count,
                flight_data_message_count: input.flight_data_message_count,
                projection_count: input.request.projection.as_ref().map(Vec::len),
                predicate_present: input.request.predicate.is_some(),
                auth_required: input.auth_policy.requires_request_metadata(),
                scan_scope_required: input.scan_scope_policy.requires_request_metadata(),
                scan_grant_required: input.scan_grant_policy.requires_request_metadata(),
            }),
        }
    }
}

impl FlightScanAccessLogEntry {
    pub fn emit_debug(&self) {
        tracing::debug!(
            target: "ehdb_service::flight_access",
            call = self.call.as_str(),
            grpc_code = ?self.grpc_code,
            row_count = self.row_count,
            flight_data_message_count = self.flight_data_message_count,
            projection_count = self.projection_count,
            predicate_present = self.predicate_present,
            auth_required = self.auth_required,
            scan_scope_required = self.scan_scope_required,
            scan_grant_required = self.scan_grant_required,
            "EHDB local Arrow Flight scan access"
        );
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalArrowFlightServerConfig {
    pub bind_addr: SocketAddr,
    pub max_decoding_message_size: usize,
    pub max_encoding_message_size: usize,
    pub max_concurrent_requests: usize,
    pub auth_policy: FlightAuthPolicy,
    pub scan_scope_policy: FlightScanScopePolicy,
    pub scan_grant_policy: FlightScanGrantPolicy,
    pub access_log_policy: FlightAccessLogPolicy,
}

impl Default for LocalArrowFlightServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            max_decoding_message_size: DEFAULT_FLIGHT_MAX_MESSAGE_SIZE,
            max_encoding_message_size: DEFAULT_FLIGHT_MAX_MESSAGE_SIZE,
            max_concurrent_requests: DEFAULT_FLIGHT_MAX_CONCURRENT_REQUESTS,
            auth_policy: FlightAuthPolicy::DisabledForLocalReference,
            scan_scope_policy: FlightScanScopePolicy::DisabledForLocalReference,
            scan_grant_policy: FlightScanGrantPolicy::DisabledForLocalReference,
            access_log_policy: FlightAccessLogPolicy::DebugOnly,
        }
    }
}

impl LocalArrowFlightServerConfig {
    pub fn validate(&self) -> Result<()> {
        if self.max_decoding_message_size == 0 {
            return Err(EhdbError::InvalidState(
                "Flight max decoding message size must be greater than zero".to_string(),
            ));
        }
        if self.max_encoding_message_size == 0 {
            return Err(EhdbError::InvalidState(
                "Flight max encoding message size must be greater than zero".to_string(),
            ));
        }
        if self.max_concurrent_requests == 0 {
            return Err(EhdbError::InvalidState(
                "Flight max concurrent requests must be greater than zero".to_string(),
            ));
        }
        self.auth_policy.validate()?;
        self.scan_scope_policy.validate()?;
        self.scan_grant_policy.validate()?;
        if self.auth_policy == FlightAuthPolicy::DisabledForLocalReference
            && !self.bind_addr.ip().is_loopback()
        {
            return Err(EhdbError::InvalidState(
                "unauthenticated Flight service config must bind to loopback".to_string(),
            ));
        }
        Ok(())
    }

    pub fn build_service<S>(
        &self,
        runtime: Arc<LocalReferenceRuntime>,
        store: Arc<S>,
    ) -> Result<FlightServiceServer<LocalArrowFlightServer<S>>>
    where
        S: ImmutableObjectStore + Send + Sync + 'static,
    {
        self.validate()?;
        Ok(LocalArrowFlightServer::new_with_runtime_limits(
            runtime,
            store,
            self.auth_policy.clone(),
            self.scan_scope_policy.clone(),
            self.scan_grant_policy.clone(),
            self.access_log_policy.clone(),
            self.max_concurrent_requests,
        )
        .into_server_with_config(self))
    }

    pub async fn bind_loopback_listener<S>(
        &self,
        runtime: Arc<LocalReferenceRuntime>,
        store: Arc<S>,
    ) -> Result<LocalArrowFlightListener<S>>
    where
        S: ImmutableObjectStore + Send + Sync + 'static,
    {
        self.validate()?;
        if !self.bind_addr.ip().is_loopback() {
            return Err(EhdbError::InvalidState(
                "Flight listener harness only supports loopback binds".to_string(),
            ));
        }

        let listener = TcpListener::bind(self.bind_addr)
            .await
            .map_err(|err| EhdbError::Storage(format!("bind Flight listener: {err}")))?;
        let local_addr = listener
            .local_addr()
            .map_err(|err| EhdbError::Storage(format!("read Flight listener address: {err}")))?;
        let incoming = TcpIncoming::from_listener(listener, true, None)
            .map_err(|err| EhdbError::Storage(format!("create Flight incoming stream: {err}")))?;
        let service = self.build_service(runtime, store)?;

        Ok(LocalArrowFlightListener {
            local_addr,
            incoming,
            service,
        })
    }
}

#[derive(Debug)]
pub struct LocalArrowFlightListener<S> {
    local_addr: SocketAddr,
    incoming: TcpIncoming,
    service: FlightServiceServer<LocalArrowFlightServer<S>>,
}

impl<S> LocalArrowFlightListener<S>
where
    S: ImmutableObjectStore + Send + Sync + 'static,
{
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub async fn serve_with_shutdown<F>(self, shutdown: F) -> Result<()>
    where
        F: Future<Output = ()>,
    {
        Server::builder()
            .add_service(self.service)
            .serve_with_incoming_shutdown(self.incoming, shutdown)
            .await
            .map_err(|err| EhdbError::Storage(format!("serve Flight listener: {err}")))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScanLatestTableRequest {
    pub tenant: TenantId,
    pub namespace: NamespaceName,
    pub table_name: TableName,
    pub projection: Option<Vec<String>>,
    pub predicate: Option<ArrowEqualityPredicate>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScanFlightTicket {
    pub version: String,
    pub request: ScanLatestTableRequest,
}

impl ScanFlightTicket {
    pub fn new(request: ScanLatestTableRequest) -> Self {
        Self {
            version: SCAN_FLIGHT_TICKET_VERSION.to_string(),
            request,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate_version()?;
        serde_json::to_vec(self)
            .map_err(|err| EhdbError::InvalidState(format!("encode scan ticket: {err}")))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let ticket: Self = serde_json::from_slice(bytes)
            .map_err(|err| EhdbError::InvalidState(format!("decode scan ticket: {err}")))?;
        ticket.validate_version()?;
        Ok(ticket)
    }

    pub fn to_arrow_ticket(&self) -> Result<Ticket> {
        Ok(Ticket {
            ticket: self.encode()?.into(),
        })
    }

    pub fn from_arrow_ticket(ticket: &Ticket) -> Result<Self> {
        Self::decode(ticket.ticket.as_ref())
    }

    pub fn command_descriptor(&self) -> Result<FlightDescriptor> {
        Ok(FlightDescriptor {
            r#type: DescriptorType::Cmd as i32,
            cmd: self.encode()?.into(),
            path: Vec::new(),
        })
    }

    pub fn into_request(self) -> ScanLatestTableRequest {
        self.request
    }

    fn validate_version(&self) -> Result<()> {
        if self.version == SCAN_FLIGHT_TICKET_VERSION {
            Ok(())
        } else {
            Err(EhdbError::InvalidState(format!(
                "unsupported scan ticket version: {}",
                self.version
            )))
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ArrowScanResult {
    pub schema: Arc<Schema>,
    pub batches: Vec<RecordBatch>,
    pub row_count: usize,
}

impl ArrowScanResult {
    pub fn from_batches(batches: Vec<RecordBatch>) -> Result<Self> {
        let schema = batches.first().map(|batch| batch.schema()).ok_or_else(|| {
            EhdbError::InvalidState("scan returned no record batches".to_string())
        })?;

        for batch in &batches {
            if batch.schema().as_ref() != schema.as_ref() {
                return Err(EhdbError::InvalidState(
                    "scan returned mixed Arrow schemas".to_string(),
                ));
            }
        }

        let row_count = batches.iter().map(RecordBatch::num_rows).sum();
        Ok(Self {
            schema,
            batches,
            row_count,
        })
    }

    pub fn to_flight_data(&self) -> Result<Vec<FlightData>> {
        batches_to_flight_data(self.schema.as_ref(), self.batches.clone()).map_err(|err| {
            EhdbError::InvalidState(format!("encode scan result flight data: {err}"))
        })
    }

    pub fn from_flight_data(flight_data: &[FlightData]) -> Result<Self> {
        let batches = flight_data_to_batches(flight_data).map_err(|err| {
            EhdbError::InvalidState(format!("decode scan result flight data: {err}"))
        })?;
        Self::from_batches(batches)
    }

    pub fn to_flight_info(&self, ticket: &ScanFlightTicket) -> Result<FlightInfo> {
        let stream = self.to_flight_data()?;
        let schema = schema_ipc_bytes(self.schema.as_ref())?.into();
        let total_records = i64::try_from(self.row_count).map_err(|_| {
            EhdbError::InvalidState(format!("scan row count too large: {}", self.row_count))
        })?;
        let total_bytes = total_flight_data_bytes(&stream)?;

        Ok(FlightInfo {
            schema,
            flight_descriptor: Some(ticket.command_descriptor()?),
            endpoint: vec![FlightEndpoint {
                ticket: Some(ticket.to_arrow_ticket()?),
                location: Vec::new(),
                expiration_time: None,
                app_metadata: Vec::new().into(),
            }],
            total_records,
            total_bytes,
            ordered: true,
            app_metadata: SCAN_FLIGHT_TICKET_VERSION.as_bytes().to_vec().into(),
        })
    }
}

fn schema_ipc_bytes(schema: &Schema) -> Result<Vec<u8>> {
    let options = IpcWriteOptions::default();
    let message: IpcMessage = SchemaAsIpc::new(schema, &options)
        .try_into()
        .map_err(|err| EhdbError::InvalidState(format!("encode flight info schema: {err}")))?;
    Ok(message.0.to_vec())
}

fn schema_result_from_schema(schema: &Schema) -> Result<SchemaResult> {
    let options = IpcWriteOptions::default();
    SchemaAsIpc::new(schema, &options)
        .try_into()
        .map_err(|err| EhdbError::InvalidState(format!("encode flight schema result: {err}")))
}

fn total_flight_data_bytes(stream: &[FlightData]) -> Result<i64> {
    let total: usize = stream
        .iter()
        .map(|data| data.data_header.len() + data.data_body.len() + data.app_metadata.len())
        .sum();
    i64::try_from(total)
        .map_err(|_| EhdbError::InvalidState(format!("flight data byte count too large: {total}")))
}

#[derive(Debug, Default)]
pub struct LocalArrowScanService {
    scanner: LocalArrowSnapshotScanner,
}

impl LocalArrowScanService {
    pub fn scan_latest<S: ImmutableObjectStore>(
        &self,
        runtime: &LocalReferenceRuntime,
        store: &S,
        request: ScanLatestTableRequest,
    ) -> Result<ArrowScanResult> {
        let batches = self.scanner.scan_latest(
            runtime,
            store,
            ScanArrowSnapshot {
                tenant: request.tenant,
                namespace: request.namespace,
                table_name: request.table_name,
                projection: request.projection,
                predicate: request.predicate,
            },
        )?;
        ArrowScanResult::from_batches(batches)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchSimilarChunksRequest {
    pub tenant: TenantId,
    pub namespace: NamespaceName,
    pub model_id: EmbeddingModelId,
    pub query: Vec<f32>,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchSimilarChunksHit {
    pub chunk_id: ChunkId,
    pub document_id: DocumentId,
    pub ordinal: u32,
    pub text: String,
    pub checksum: String,
    pub model_id: EmbeddingModelId,
    pub dimensions: usize,
    pub score: f32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchTextChunksRequest {
    pub tenant: TenantId,
    pub namespace: NamespaceName,
    pub query: String,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchTextChunksHit {
    pub chunk_id: ChunkId,
    pub document_id: DocumentId,
    pub ordinal: u32,
    pub text: String,
    pub checksum: String,
    pub match_count: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchHybridChunksRequest {
    pub tenant: TenantId,
    pub namespace: NamespaceName,
    pub model_id: EmbeddingModelId,
    pub query: Vec<f32>,
    pub text_query: String,
    pub limit: usize,
    pub vector_weight: f32,
    pub text_weight: f32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchHybridChunksHit {
    pub chunk_id: ChunkId,
    pub document_id: DocumentId,
    pub ordinal: u32,
    pub text: String,
    pub checksum: String,
    pub model_id: EmbeddingModelId,
    pub dimensions: usize,
    pub vector_score: f32,
    pub text_match_count: usize,
    pub combined_score: f32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AssembleRetrievalContextRequest {
    pub tenant: TenantId,
    pub namespace: NamespaceName,
    pub model_id: EmbeddingModelId,
    pub query: Vec<f32>,
    pub text_query: String,
    pub hit_limit: usize,
    pub max_block_chars: usize,
    pub max_total_chars: usize,
    pub vector_weight: f32,
    pub text_weight: f32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RetrievalContextBlock {
    pub chunk_id: ChunkId,
    pub document_id: DocumentId,
    pub ordinal: u32,
    pub checksum: String,
    pub text: String,
    pub original_text_chars: usize,
    pub clipped: bool,
    pub model_id: EmbeddingModelId,
    pub dimensions: usize,
    pub vector_score: f32,
    pub text_match_count: usize,
    pub combined_score: f32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RetrievalContext {
    pub blocks: Vec<RetrievalContextBlock>,
    pub total_text_chars: usize,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RetrievalContextRequestPayload {
    pub version: String,
    pub request: AssembleRetrievalContextRequest,
}

impl RetrievalContextRequestPayload {
    pub fn new(request: AssembleRetrievalContextRequest) -> Self {
        Self {
            version: RETRIEVAL_CONTEXT_REQUEST_VERSION.to_string(),
            request,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate_version()?;
        serde_json::to_vec(self).map_err(|err| {
            EhdbError::InvalidState(format!("encode retrieval context request: {err}"))
        })
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let payload: Self = serde_json::from_slice(bytes).map_err(|err| {
            EhdbError::InvalidState(format!("decode retrieval context request: {err}"))
        })?;
        payload.validate_version()?;
        Ok(payload)
    }

    pub fn into_request(self) -> AssembleRetrievalContextRequest {
        self.request
    }

    fn validate_version(&self) -> Result<()> {
        if self.version == RETRIEVAL_CONTEXT_REQUEST_VERSION {
            Ok(())
        } else {
            Err(EhdbError::InvalidState(format!(
                "unsupported retrieval context request version: {}",
                self.version
            )))
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RetrievalContextResultPayload {
    pub version: String,
    pub context: RetrievalContext,
}

impl RetrievalContextResultPayload {
    pub fn new(context: RetrievalContext) -> Self {
        Self {
            version: RETRIEVAL_CONTEXT_RESULT_VERSION.to_string(),
            context,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate_version()?;
        serde_json::to_vec(self).map_err(|err| {
            EhdbError::InvalidState(format!("encode retrieval context result: {err}"))
        })
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let payload: Self = serde_json::from_slice(bytes).map_err(|err| {
            EhdbError::InvalidState(format!("decode retrieval context result: {err}"))
        })?;
        payload.validate_version()?;
        Ok(payload)
    }

    pub fn into_context(self) -> RetrievalContext {
        self.context
    }

    fn validate_version(&self) -> Result<()> {
        if self.version == RETRIEVAL_CONTEXT_RESULT_VERSION {
            Ok(())
        } else {
            Err(EhdbError::InvalidState(format!(
                "unsupported retrieval context result version: {}",
                self.version
            )))
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetrievalContextPayloadExecutorConfig {
    pub max_request_payload_bytes: usize,
    pub max_result_payload_bytes: usize,
    pub max_receipt_payload_bytes: usize,
}

impl Default for RetrievalContextPayloadExecutorConfig {
    fn default() -> Self {
        Self {
            max_request_payload_bytes: DEFAULT_RETRIEVAL_CONTEXT_MAX_REQUEST_PAYLOAD_BYTES,
            max_result_payload_bytes: DEFAULT_RETRIEVAL_CONTEXT_MAX_RESULT_PAYLOAD_BYTES,
            max_receipt_payload_bytes: DEFAULT_RETRIEVAL_CONTEXT_MAX_RECEIPT_PAYLOAD_BYTES,
        }
    }
}

impl RetrievalContextPayloadExecutorConfig {
    pub fn validate(&self) -> Result<()> {
        validate_payload_limit(
            "max retrieval context request payload bytes",
            self.max_request_payload_bytes,
        )?;
        validate_payload_limit(
            "max retrieval context result payload bytes",
            self.max_result_payload_bytes,
        )?;
        validate_payload_limit(
            "max retrieval context receipt payload bytes",
            self.max_receipt_payload_bytes,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetrievalContextPayloadScope {
    pub tenant: TenantId,
    pub namespace: NamespaceName,
}

impl RetrievalContextPayloadScope {
    pub fn validate_request(&self, request: &AssembleRetrievalContextRequest) -> Result<()> {
        if request.tenant != self.tenant {
            return Err(EhdbError::InvalidState(
                "retrieval context request tenant does not match execution scope".to_string(),
            ));
        }
        if request.namespace != self.namespace {
            return Err(EhdbError::InvalidState(
                "retrieval context request namespace does not match execution scope".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetrievalContextPayloadExecutionSummary {
    pub request_payload_bytes: usize,
    pub result_payload_bytes: usize,
    pub context_block_count: usize,
    pub total_text_chars: usize,
    pub truncated: bool,
    pub scope_required: bool,
}

impl RetrievalContextPayloadExecutionSummary {
    pub fn validate(&self) -> Result<()> {
        validate_payload_limit(
            "retrieval context receipt request payload bytes",
            self.request_payload_bytes,
        )?;
        validate_payload_limit(
            "retrieval context receipt result payload bytes",
            self.result_payload_bytes,
        )?;
        if self.context_block_count == 0 && self.total_text_chars != 0 {
            return Err(EhdbError::InvalidState(
                "retrieval context receipt total text chars require at least one context block"
                    .to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetrievalContextPayloadExecution {
    pub result_payload: Vec<u8>,
    pub summary: RetrievalContextPayloadExecutionSummary,
}

impl RetrievalContextPayloadExecution {
    pub fn encode_receipt_payload(&self) -> Result<Vec<u8>> {
        RetrievalContextPayloadExecutionReceiptPayload::new(self.summary.clone()).encode()
    }

    pub fn into_artifacts_with_config(
        self,
        config: RetrievalContextPayloadExecutorConfig,
    ) -> Result<RetrievalContextPayloadExecutionArtifacts> {
        config.validate()?;
        let receipt_payload = self.encode_receipt_payload()?;
        if receipt_payload.len() > config.max_receipt_payload_bytes {
            return Err(EhdbError::InvalidState(format!(
                "retrieval context receipt payload exceeds {} bytes",
                config.max_receipt_payload_bytes
            )));
        }
        let artifacts = RetrievalContextPayloadExecutionArtifacts {
            result_payload: self.result_payload,
            receipt_payload,
        };
        artifacts.validate()?;
        Ok(artifacts)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetrievalContextPayloadExecutionArtifacts {
    pub result_payload: Vec<u8>,
    pub receipt_payload: Vec<u8>,
}

impl RetrievalContextPayloadExecutionArtifacts {
    pub fn receipt_summary(&self) -> Result<RetrievalContextPayloadExecutionSummary> {
        Ok(RetrievalContextPayloadExecutionReceiptPayload::decode(&self.receipt_payload)?.summary)
    }

    pub fn receipt_event_payload(
        &self,
    ) -> Result<RetrievalContextPayloadExecutionReceiptEventPayload> {
        RetrievalContextPayloadExecutionReceiptEventPayload::from_artifacts(self)
    }

    pub fn encode_receipt_event_payload(&self) -> Result<Vec<u8>> {
        self.receipt_event_payload()?.encode()
    }

    pub fn publish_receipt_event<L: RetrievalContextReceiptEventStreamLog>(
        &self,
        stream_log: &mut L,
        target: &RetrievalContextReceiptEventStreamTarget,
        transaction_id: TransactionId,
    ) -> Result<StreamRecord> {
        target.publish_artifacts(stream_log, self, transaction_id)
    }

    pub fn validate(&self) -> Result<()> {
        validate_payload_limit(
            "retrieval context artifact result payload bytes",
            self.result_payload.len(),
        )?;
        validate_payload_limit(
            "retrieval context artifact receipt payload bytes",
            self.receipt_payload.len(),
        )?;
        let summary = self.receipt_summary()?;
        if summary.result_payload_bytes != self.result_payload.len() {
            return Err(EhdbError::InvalidState(
                "retrieval context artifact result payload bytes do not match receipt summary"
                    .to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetrievalContextReceiptEventStreamTarget {
    pub tenant: TenantId,
    pub namespace: NamespaceName,
    pub stream: StreamName,
}

impl RetrievalContextReceiptEventStreamTarget {
    pub fn stream_config(&self, retention: RetentionPolicy) -> StreamConfig {
        StreamConfig {
            tenant: self.tenant.clone(),
            namespace: self.namespace.clone(),
            name: self.stream.clone(),
            retention,
        }
    }

    pub fn create_stream<L: RetrievalContextReceiptEventStreamSetupLog>(
        &self,
        stream_log: &mut L,
        retention: RetentionPolicy,
    ) -> Result<()> {
        stream_log.create_receipt_event_stream(self.stream_config(retention))
    }

    pub fn create_keep_all_stream<L: RetrievalContextReceiptEventStreamSetupLog>(
        &self,
        stream_log: &mut L,
    ) -> Result<()> {
        self.create_stream(stream_log, RetentionPolicy::KeepAll)
    }

    pub fn create_bounded_stream<L: RetrievalContextReceiptEventStreamSetupLog>(
        &self,
        stream_log: &mut L,
        max_records: usize,
    ) -> Result<()> {
        validate_payload_limit(
            "retrieval context receipt event stream max records",
            max_records,
        )?;
        self.create_stream(stream_log, RetentionPolicy::MaxRecords(max_records))
    }

    pub fn publish_artifacts<L: RetrievalContextReceiptEventStreamLog>(
        &self,
        stream_log: &mut L,
        artifacts: &RetrievalContextPayloadExecutionArtifacts,
        transaction_id: TransactionId,
    ) -> Result<StreamRecord> {
        let event = artifacts.receipt_event_payload()?;
        stream_log.publish_receipt_event(
            &self.tenant,
            &self.namespace,
            &self.stream,
            event,
            transaction_id,
        )
    }

    pub fn replay_events<L: RetrievalContextReceiptEventStreamReadLog>(
        &self,
        stream_log: &L,
        after: Option<StreamSequence>,
    ) -> Result<Vec<RetrievalContextReceiptEventStreamRecord>> {
        stream_log
            .replay_receipt_event_records(&self.tenant, &self.namespace, &self.stream, after)?
            .iter()
            .map(RetrievalContextReceiptEventStreamRecord::from_stream_record)
            .collect()
    }

    pub fn create_consumer<L: RetrievalContextReceiptEventDurableConsumerLog>(
        &self,
        stream_log: &mut L,
        consumer: ConsumerName,
    ) -> Result<DurableConsumer> {
        stream_log.create_receipt_event_consumer(
            &self.tenant,
            &self.namespace,
            &self.stream,
            consumer,
        )
    }

    pub fn replay_events_for_consumer<L: RetrievalContextReceiptEventDurableConsumerLog>(
        &self,
        stream_log: &L,
        consumer: &ConsumerName,
    ) -> Result<Vec<RetrievalContextReceiptEventStreamRecord>> {
        stream_log
            .replay_receipt_event_records_for_consumer(
                &self.tenant,
                &self.namespace,
                &self.stream,
                consumer,
            )?
            .iter()
            .map(RetrievalContextReceiptEventStreamRecord::from_stream_record)
            .collect()
    }

    pub fn ack_event<L: RetrievalContextReceiptEventDurableConsumerLog>(
        &self,
        stream_log: &mut L,
        consumer: &ConsumerName,
        sequence: StreamSequence,
    ) -> Result<DurableConsumer> {
        stream_log.ack_receipt_event(
            &self.tenant,
            &self.namespace,
            &self.stream,
            consumer,
            sequence,
        )
    }
}

pub trait RetrievalContextReceiptEventStreamSetupLog {
    fn create_receipt_event_stream(&mut self, config: StreamConfig) -> Result<()>;
}

impl RetrievalContextReceiptEventStreamSetupLog for InMemoryStreamLog {
    fn create_receipt_event_stream(&mut self, config: StreamConfig) -> Result<()> {
        self.create_stream(config)
    }
}

impl RetrievalContextReceiptEventStreamSetupLog for LocalJsonlStreamLog {
    fn create_receipt_event_stream(&mut self, config: StreamConfig) -> Result<()> {
        self.create_stream(config)
    }
}

pub trait RetrievalContextReceiptEventStreamLog {
    fn publish_receipt_event(
        &mut self,
        tenant: &TenantId,
        namespace: &NamespaceName,
        stream: &StreamName,
        event: RetrievalContextPayloadExecutionReceiptEventPayload,
        transaction_id: TransactionId,
    ) -> Result<StreamRecord>;
}

impl RetrievalContextReceiptEventStreamLog for InMemoryStreamLog {
    fn publish_receipt_event(
        &mut self,
        tenant: &TenantId,
        namespace: &NamespaceName,
        stream: &StreamName,
        event: RetrievalContextPayloadExecutionReceiptEventPayload,
        transaction_id: TransactionId,
    ) -> Result<StreamRecord> {
        let subject = Subject::new(event.subject())?;
        let payload = event.encode()?;
        self.publish(tenant, namespace, stream, subject, payload, transaction_id)
    }
}

impl RetrievalContextReceiptEventStreamLog for LocalJsonlStreamLog {
    fn publish_receipt_event(
        &mut self,
        tenant: &TenantId,
        namespace: &NamespaceName,
        stream: &StreamName,
        event: RetrievalContextPayloadExecutionReceiptEventPayload,
        transaction_id: TransactionId,
    ) -> Result<StreamRecord> {
        let subject = Subject::new(event.subject())?;
        let payload = event.encode()?;
        self.publish(tenant, namespace, stream, subject, payload, transaction_id)
    }
}

pub trait RetrievalContextReceiptEventStreamReadLog {
    fn replay_receipt_event_records(
        &self,
        tenant: &TenantId,
        namespace: &NamespaceName,
        stream: &StreamName,
        after: Option<StreamSequence>,
    ) -> Result<Vec<StreamRecord>>;
}

impl RetrievalContextReceiptEventStreamReadLog for InMemoryStreamLog {
    fn replay_receipt_event_records(
        &self,
        tenant: &TenantId,
        namespace: &NamespaceName,
        stream: &StreamName,
        after: Option<StreamSequence>,
    ) -> Result<Vec<StreamRecord>> {
        self.replay(tenant, namespace, stream, after)
    }
}

impl RetrievalContextReceiptEventStreamReadLog for LocalJsonlStreamLog {
    fn replay_receipt_event_records(
        &self,
        tenant: &TenantId,
        namespace: &NamespaceName,
        stream: &StreamName,
        after: Option<StreamSequence>,
    ) -> Result<Vec<StreamRecord>> {
        self.replay(tenant, namespace, stream, after)
    }
}

pub trait RetrievalContextReceiptEventDurableConsumerLog:
    RetrievalContextReceiptEventStreamReadLog
{
    fn create_receipt_event_consumer(
        &mut self,
        tenant: &TenantId,
        namespace: &NamespaceName,
        stream: &StreamName,
        consumer: ConsumerName,
    ) -> Result<DurableConsumer>;

    fn replay_receipt_event_records_for_consumer(
        &self,
        tenant: &TenantId,
        namespace: &NamespaceName,
        stream: &StreamName,
        consumer: &ConsumerName,
    ) -> Result<Vec<StreamRecord>>;

    fn ack_receipt_event(
        &mut self,
        tenant: &TenantId,
        namespace: &NamespaceName,
        stream: &StreamName,
        consumer: &ConsumerName,
        sequence: StreamSequence,
    ) -> Result<DurableConsumer>;
}

impl RetrievalContextReceiptEventDurableConsumerLog for InMemoryStreamLog {
    fn create_receipt_event_consumer(
        &mut self,
        tenant: &TenantId,
        namespace: &NamespaceName,
        stream: &StreamName,
        consumer: ConsumerName,
    ) -> Result<DurableConsumer> {
        self.create_consumer(tenant, namespace, stream, consumer)
    }

    fn replay_receipt_event_records_for_consumer(
        &self,
        tenant: &TenantId,
        namespace: &NamespaceName,
        stream: &StreamName,
        consumer: &ConsumerName,
    ) -> Result<Vec<StreamRecord>> {
        self.replay_for_consumer(tenant, namespace, stream, consumer)
    }

    fn ack_receipt_event(
        &mut self,
        tenant: &TenantId,
        namespace: &NamespaceName,
        stream: &StreamName,
        consumer: &ConsumerName,
        sequence: StreamSequence,
    ) -> Result<DurableConsumer> {
        self.ack(tenant, namespace, stream, consumer, sequence)
    }
}

impl RetrievalContextReceiptEventDurableConsumerLog for LocalJsonlStreamLog {
    fn create_receipt_event_consumer(
        &mut self,
        tenant: &TenantId,
        namespace: &NamespaceName,
        stream: &StreamName,
        consumer: ConsumerName,
    ) -> Result<DurableConsumer> {
        self.create_consumer(tenant, namespace, stream, consumer)
    }

    fn replay_receipt_event_records_for_consumer(
        &self,
        tenant: &TenantId,
        namespace: &NamespaceName,
        stream: &StreamName,
        consumer: &ConsumerName,
    ) -> Result<Vec<StreamRecord>> {
        self.replay_for_consumer(tenant, namespace, stream, consumer)
    }

    fn ack_receipt_event(
        &mut self,
        tenant: &TenantId,
        namespace: &NamespaceName,
        stream: &StreamName,
        consumer: &ConsumerName,
        sequence: StreamSequence,
    ) -> Result<DurableConsumer> {
        self.ack(tenant, namespace, stream, consumer, sequence)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetrievalContextReceiptEventStreamRecord {
    pub sequence: StreamSequence,
    pub transaction_id: TransactionId,
    pub event: RetrievalContextPayloadExecutionReceiptEventPayload,
}

impl RetrievalContextReceiptEventStreamRecord {
    pub fn from_stream_record(record: &StreamRecord) -> Result<Self> {
        if record.subject.as_str() != RETRIEVAL_CONTEXT_EXECUTION_RECEIPT_EVENT_SUBJECT {
            return Err(EhdbError::InvalidState(format!(
                "unexpected retrieval context receipt event subject: {}",
                record.subject.as_str()
            )));
        }
        Ok(Self {
            sequence: record.sequence,
            transaction_id: record.transaction_id.clone(),
            event: RetrievalContextPayloadExecutionReceiptEventPayload::decode(&record.payload)?,
        })
    }

    pub fn receipt_summary(&self) -> Result<RetrievalContextPayloadExecutionSummary> {
        self.event.receipt_summary()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetrievalContextPayloadExecutionReceiptEventPayload {
    pub version: String,
    pub receipt_payload: Vec<u8>,
}

impl RetrievalContextPayloadExecutionReceiptEventPayload {
    pub fn new(receipt_payload: Vec<u8>) -> Result<Self> {
        let payload = Self {
            version: RETRIEVAL_CONTEXT_EXECUTION_RECEIPT_EVENT_VERSION.to_string(),
            receipt_payload,
        };
        payload.validate()?;
        Ok(payload)
    }

    pub fn from_artifacts(artifacts: &RetrievalContextPayloadExecutionArtifacts) -> Result<Self> {
        artifacts.validate()?;
        Self::new(artifacts.receipt_payload.clone())
    }

    pub fn subject(&self) -> &'static str {
        RETRIEVAL_CONTEXT_EXECUTION_RECEIPT_EVENT_SUBJECT
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        serde_json::to_vec(self).map_err(|err| {
            EhdbError::InvalidState(format!(
                "encode retrieval context execution receipt event: {err}"
            ))
        })
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let payload: Self = serde_json::from_slice(bytes).map_err(|err| {
            EhdbError::InvalidState(format!(
                "decode retrieval context execution receipt event: {err}"
            ))
        })?;
        payload.validate()?;
        Ok(payload)
    }

    pub fn receipt_summary(&self) -> Result<RetrievalContextPayloadExecutionSummary> {
        Ok(RetrievalContextPayloadExecutionReceiptPayload::decode(&self.receipt_payload)?.summary)
    }

    pub fn into_receipt_payload(self) -> Vec<u8> {
        self.receipt_payload
    }

    fn validate(&self) -> Result<()> {
        self.validate_version()?;
        validate_payload_limit(
            "retrieval context receipt event receipt payload bytes",
            self.receipt_payload.len(),
        )?;
        RetrievalContextPayloadExecutionReceiptPayload::decode(&self.receipt_payload)?;
        Ok(())
    }

    fn validate_version(&self) -> Result<()> {
        if self.version == RETRIEVAL_CONTEXT_EXECUTION_RECEIPT_EVENT_VERSION {
            Ok(())
        } else {
            Err(EhdbError::InvalidState(format!(
                "unsupported retrieval context execution receipt event version: {}",
                self.version
            )))
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetrievalContextPayloadExecutionReceiptPayload {
    pub version: String,
    pub summary: RetrievalContextPayloadExecutionSummary,
}

impl RetrievalContextPayloadExecutionReceiptPayload {
    pub fn new(summary: RetrievalContextPayloadExecutionSummary) -> Self {
        Self {
            version: RETRIEVAL_CONTEXT_EXECUTION_RECEIPT_VERSION.to_string(),
            summary,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate_version()?;
        self.summary.validate()?;
        serde_json::to_vec(self).map_err(|err| {
            EhdbError::InvalidState(format!("encode retrieval context execution receipt: {err}"))
        })
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let payload: Self = serde_json::from_slice(bytes).map_err(|err| {
            EhdbError::InvalidState(format!("decode retrieval context execution receipt: {err}"))
        })?;
        payload.validate_version()?;
        payload.summary.validate()?;
        Ok(payload)
    }

    pub fn into_summary(self) -> RetrievalContextPayloadExecutionSummary {
        self.summary
    }

    fn validate_version(&self) -> Result<()> {
        if self.version == RETRIEVAL_CONTEXT_EXECUTION_RECEIPT_VERSION {
            Ok(())
        } else {
            Err(EhdbError::InvalidState(format!(
                "unsupported retrieval context execution receipt version: {}",
                self.version
            )))
        }
    }
}

#[derive(Debug, Default)]
pub struct LocalRetrievalSearchService;

impl LocalRetrievalSearchService {
    pub fn search_similar(
        &self,
        runtime: &LocalReferenceRuntime,
        request: SearchSimilarChunksRequest,
    ) -> Result<Vec<SearchSimilarChunksHit>> {
        let hits = runtime
            .state()
            .retrieval
            .search_similar(VectorSearch {
                tenant: request.tenant,
                namespace: request.namespace,
                model_id: request.model_id,
                query: request.query,
                limit: request.limit,
            })?
            .into_iter()
            .map(|hit| SearchSimilarChunksHit {
                chunk_id: hit.chunk.id,
                document_id: hit.chunk.document_id,
                ordinal: hit.chunk.ordinal,
                text: hit.chunk.text,
                checksum: hit.chunk.checksum,
                model_id: hit.embedding.model_id,
                dimensions: hit.embedding.dimensions,
                score: hit.score,
            })
            .collect();
        Ok(hits)
    }

    pub fn search_hybrid(
        &self,
        runtime: &LocalReferenceRuntime,
        request: SearchHybridChunksRequest,
    ) -> Result<Vec<SearchHybridChunksHit>> {
        let hits = runtime
            .state()
            .retrieval
            .search_hybrid(HybridSearch {
                tenant: request.tenant,
                namespace: request.namespace,
                model_id: request.model_id,
                query: request.query,
                text_query: request.text_query,
                limit: request.limit,
                vector_weight: request.vector_weight,
                text_weight: request.text_weight,
            })?
            .into_iter()
            .map(|hit| SearchHybridChunksHit {
                chunk_id: hit.chunk.id,
                document_id: hit.chunk.document_id,
                ordinal: hit.chunk.ordinal,
                text: hit.chunk.text,
                checksum: hit.chunk.checksum,
                model_id: hit.embedding.model_id,
                dimensions: hit.embedding.dimensions,
                vector_score: hit.vector_score,
                text_match_count: hit.text_match_count,
                combined_score: hit.combined_score,
            })
            .collect();
        Ok(hits)
    }

    pub fn assemble_context(
        &self,
        runtime: &LocalReferenceRuntime,
        request: AssembleRetrievalContextRequest,
    ) -> Result<RetrievalContext> {
        validate_context_budget("max block chars", request.max_block_chars)?;
        validate_context_budget("max total chars", request.max_total_chars)?;

        let hits = self.search_hybrid(
            runtime,
            SearchHybridChunksRequest {
                tenant: request.tenant,
                namespace: request.namespace,
                model_id: request.model_id,
                query: request.query,
                text_query: request.text_query,
                limit: request.hit_limit,
                vector_weight: request.vector_weight,
                text_weight: request.text_weight,
            },
        )?;

        let mut blocks = Vec::new();
        let mut total_text_chars = 0;
        let mut truncated = false;

        for hit in hits {
            if total_text_chars >= request.max_total_chars {
                truncated = true;
                break;
            }

            let remaining_total = request.max_total_chars - total_text_chars;
            let block_budget = request.max_block_chars.min(remaining_total);
            let original_text_chars = hit.text.chars().count();
            let text = take_char_prefix(&hit.text, block_budget);
            let text_chars = text.chars().count();
            let clipped = original_text_chars > text_chars;
            truncated |= clipped;
            total_text_chars += text_chars;

            blocks.push(RetrievalContextBlock {
                chunk_id: hit.chunk_id,
                document_id: hit.document_id,
                ordinal: hit.ordinal,
                checksum: hit.checksum,
                text,
                original_text_chars,
                clipped,
                model_id: hit.model_id,
                dimensions: hit.dimensions,
                vector_score: hit.vector_score,
                text_match_count: hit.text_match_count,
                combined_score: hit.combined_score,
            });
        }

        Ok(RetrievalContext {
            blocks,
            total_text_chars,
            truncated,
        })
    }

    pub fn execute_context_payload(
        &self,
        runtime: &LocalReferenceRuntime,
        request_payload: &[u8],
    ) -> Result<Vec<u8>> {
        self.execute_context_payload_with_summary(runtime, request_payload)
            .map(|execution| execution.result_payload)
    }

    pub fn execute_context_payload_with_summary(
        &self,
        runtime: &LocalReferenceRuntime,
        request_payload: &[u8],
    ) -> Result<RetrievalContextPayloadExecution> {
        self.execute_context_payload_with_config_and_summary(
            runtime,
            request_payload,
            RetrievalContextPayloadExecutorConfig::default(),
        )
    }

    pub fn execute_context_payload_artifacts(
        &self,
        runtime: &LocalReferenceRuntime,
        request_payload: &[u8],
    ) -> Result<RetrievalContextPayloadExecutionArtifacts> {
        self.execute_context_payload_artifacts_with_config(
            runtime,
            request_payload,
            RetrievalContextPayloadExecutorConfig::default(),
        )
    }

    pub fn execute_context_payload_with_config(
        &self,
        runtime: &LocalReferenceRuntime,
        request_payload: &[u8],
        config: RetrievalContextPayloadExecutorConfig,
    ) -> Result<Vec<u8>> {
        self.execute_context_payload_with_config_and_summary(runtime, request_payload, config)
            .map(|execution| execution.result_payload)
    }

    pub fn execute_context_payload_artifacts_with_config(
        &self,
        runtime: &LocalReferenceRuntime,
        request_payload: &[u8],
        config: RetrievalContextPayloadExecutorConfig,
    ) -> Result<RetrievalContextPayloadExecutionArtifacts> {
        self.execute_context_payload_with_config_and_summary(runtime, request_payload, config)?
            .into_artifacts_with_config(config)
    }

    pub fn execute_context_payload_with_config_and_summary(
        &self,
        runtime: &LocalReferenceRuntime,
        request_payload: &[u8],
        config: RetrievalContextPayloadExecutorConfig,
    ) -> Result<RetrievalContextPayloadExecution> {
        config.validate()?;
        if request_payload.len() > config.max_request_payload_bytes {
            return Err(EhdbError::InvalidState(format!(
                "retrieval context request payload exceeds {} bytes",
                config.max_request_payload_bytes
            )));
        }
        let request = RetrievalContextRequestPayload::decode(request_payload)?.into_request();
        self.execute_context_request_with_config_and_summary(
            runtime,
            request,
            config,
            request_payload.len(),
            false,
        )
    }

    pub fn execute_context_payload_with_scope(
        &self,
        runtime: &LocalReferenceRuntime,
        request_payload: &[u8],
        config: RetrievalContextPayloadExecutorConfig,
        scope: &RetrievalContextPayloadScope,
    ) -> Result<Vec<u8>> {
        self.execute_context_payload_with_scope_and_summary(runtime, request_payload, config, scope)
            .map(|execution| execution.result_payload)
    }

    pub fn execute_context_payload_artifacts_with_scope(
        &self,
        runtime: &LocalReferenceRuntime,
        request_payload: &[u8],
        config: RetrievalContextPayloadExecutorConfig,
        scope: &RetrievalContextPayloadScope,
    ) -> Result<RetrievalContextPayloadExecutionArtifacts> {
        self.execute_context_payload_with_scope_and_summary(
            runtime,
            request_payload,
            config,
            scope,
        )?
        .into_artifacts_with_config(config)
    }

    pub fn execute_context_payload_with_scope_and_summary(
        &self,
        runtime: &LocalReferenceRuntime,
        request_payload: &[u8],
        config: RetrievalContextPayloadExecutorConfig,
        scope: &RetrievalContextPayloadScope,
    ) -> Result<RetrievalContextPayloadExecution> {
        config.validate()?;
        if request_payload.len() > config.max_request_payload_bytes {
            return Err(EhdbError::InvalidState(format!(
                "retrieval context request payload exceeds {} bytes",
                config.max_request_payload_bytes
            )));
        }
        let request = RetrievalContextRequestPayload::decode(request_payload)?.into_request();
        scope.validate_request(&request)?;
        self.execute_context_request_with_config_and_summary(
            runtime,
            request,
            config,
            request_payload.len(),
            true,
        )
    }

    fn execute_context_request_with_config_and_summary(
        &self,
        runtime: &LocalReferenceRuntime,
        request: AssembleRetrievalContextRequest,
        config: RetrievalContextPayloadExecutorConfig,
        request_payload_bytes: usize,
        scope_required: bool,
    ) -> Result<RetrievalContextPayloadExecution> {
        let context = self.assemble_context(runtime, request)?;
        let context_block_count = context.blocks.len();
        let total_text_chars = context.total_text_chars;
        let truncated = context.truncated;
        let result = RetrievalContextResultPayload::new(context).encode()?;
        if result.len() > config.max_result_payload_bytes {
            return Err(EhdbError::InvalidState(format!(
                "retrieval context result payload exceeds {} bytes",
                config.max_result_payload_bytes
            )));
        }
        Ok(RetrievalContextPayloadExecution {
            summary: RetrievalContextPayloadExecutionSummary {
                request_payload_bytes,
                result_payload_bytes: result.len(),
                context_block_count,
                total_text_chars,
                truncated,
                scope_required,
            },
            result_payload: result,
        })
    }

    pub fn search_text(
        &self,
        runtime: &LocalReferenceRuntime,
        request: SearchTextChunksRequest,
    ) -> Result<Vec<SearchTextChunksHit>> {
        let hits = runtime
            .state()
            .retrieval
            .search_text(TextSearch {
                tenant: request.tenant,
                namespace: request.namespace,
                query: request.query,
                limit: request.limit,
            })?
            .into_iter()
            .map(|hit| SearchTextChunksHit {
                chunk_id: hit.chunk.id,
                document_id: hit.chunk.document_id,
                ordinal: hit.chunk.ordinal,
                text: hit.chunk.text,
                checksum: hit.chunk.checksum,
                match_count: hit.match_count,
            })
            .collect();
        Ok(hits)
    }
}

fn validate_context_budget(label: &str, budget: usize) -> Result<()> {
    if budget == 0 {
        return Err(EhdbError::InvalidState(format!(
            "{label} must be greater than zero"
        )));
    }
    Ok(())
}

fn take_char_prefix(text: &str, max_chars: usize) -> String {
    text.chars().take(max_chars).collect()
}

fn validate_payload_limit(label: &str, limit: usize) -> Result<()> {
    if limit == 0 {
        return Err(EhdbError::InvalidState(format!(
            "{label} must be greater than zero"
        )));
    }
    Ok(())
}

#[derive(Debug, Default)]
pub struct LocalArrowFlightService {
    scan: LocalArrowScanService,
}

impl LocalArrowFlightService {
    pub fn get_flight_info<S: ImmutableObjectStore>(
        &self,
        runtime: &LocalReferenceRuntime,
        store: &S,
        request: ScanLatestTableRequest,
    ) -> Result<FlightInfo> {
        let ticket = ScanFlightTicket::new(request.clone());
        let result = self.scan.scan_latest(runtime, store, request)?;
        result.to_flight_info(&ticket)
    }

    pub fn get_schema<S: ImmutableObjectStore>(
        &self,
        runtime: &LocalReferenceRuntime,
        store: &S,
        request: ScanLatestTableRequest,
    ) -> Result<SchemaResult> {
        let result = self.scan.scan_latest(runtime, store, request)?;
        schema_result_from_schema(result.schema.as_ref())
    }

    pub fn do_get<S: ImmutableObjectStore>(
        &self,
        runtime: &LocalReferenceRuntime,
        store: &S,
        ticket: &Ticket,
    ) -> Result<Vec<FlightData>> {
        let request = ScanFlightTicket::from_arrow_ticket(ticket)?.into_request();
        self.scan
            .scan_latest(runtime, store, request)?
            .to_flight_data()
    }
}

#[derive(Debug)]
pub struct LocalArrowFlightServer<S> {
    runtime: Arc<LocalReferenceRuntime>,
    store: Arc<S>,
    service: LocalArrowFlightService,
    auth_policy: FlightAuthPolicy,
    scan_scope_policy: FlightScanScopePolicy,
    scan_grant_policy: FlightScanGrantPolicy,
    access_log_policy: FlightAccessLogPolicy,
    request_slots: Arc<Semaphore>,
}

impl<S> LocalArrowFlightServer<S>
where
    S: ImmutableObjectStore + Send + Sync + 'static,
{
    pub fn new(runtime: Arc<LocalReferenceRuntime>, store: Arc<S>) -> Self {
        Self::new_with_auth(runtime, store, FlightAuthPolicy::DisabledForLocalReference)
    }

    pub fn new_with_auth(
        runtime: Arc<LocalReferenceRuntime>,
        store: Arc<S>,
        auth_policy: FlightAuthPolicy,
    ) -> Self {
        Self::new_with_policies(
            runtime,
            store,
            auth_policy,
            FlightScanScopePolicy::DisabledForLocalReference,
        )
    }

    pub fn new_with_policies(
        runtime: Arc<LocalReferenceRuntime>,
        store: Arc<S>,
        auth_policy: FlightAuthPolicy,
        scan_scope_policy: FlightScanScopePolicy,
    ) -> Self {
        Self::new_with_authorization_policies(
            runtime,
            store,
            auth_policy,
            scan_scope_policy,
            FlightScanGrantPolicy::DisabledForLocalReference,
        )
    }

    pub fn new_with_authorization_policies(
        runtime: Arc<LocalReferenceRuntime>,
        store: Arc<S>,
        auth_policy: FlightAuthPolicy,
        scan_scope_policy: FlightScanScopePolicy,
        scan_grant_policy: FlightScanGrantPolicy,
    ) -> Self {
        Self::new_with_runtime_policies(
            runtime,
            store,
            auth_policy,
            scan_scope_policy,
            scan_grant_policy,
            FlightAccessLogPolicy::DebugOnly,
        )
    }

    pub fn new_with_runtime_policies(
        runtime: Arc<LocalReferenceRuntime>,
        store: Arc<S>,
        auth_policy: FlightAuthPolicy,
        scan_scope_policy: FlightScanScopePolicy,
        scan_grant_policy: FlightScanGrantPolicy,
        access_log_policy: FlightAccessLogPolicy,
    ) -> Self {
        Self::new_with_runtime_limits(
            runtime,
            store,
            auth_policy,
            scan_scope_policy,
            scan_grant_policy,
            access_log_policy,
            DEFAULT_FLIGHT_MAX_CONCURRENT_REQUESTS,
        )
    }

    pub fn new_with_runtime_limits(
        runtime: Arc<LocalReferenceRuntime>,
        store: Arc<S>,
        auth_policy: FlightAuthPolicy,
        scan_scope_policy: FlightScanScopePolicy,
        scan_grant_policy: FlightScanGrantPolicy,
        access_log_policy: FlightAccessLogPolicy,
        max_concurrent_requests: usize,
    ) -> Self {
        Self {
            runtime,
            store,
            service: LocalArrowFlightService::default(),
            auth_policy,
            scan_scope_policy,
            scan_grant_policy,
            access_log_policy,
            request_slots: Arc::new(Semaphore::new(max_concurrent_requests)),
        }
    }

    pub fn into_server(self) -> FlightServiceServer<Self> {
        FlightServiceServer::new(self)
    }

    pub fn into_server_with_config(
        self,
        config: &LocalArrowFlightServerConfig,
    ) -> FlightServiceServer<Self> {
        FlightServiceServer::new(self)
            .max_decoding_message_size(config.max_decoding_message_size)
            .max_encoding_message_size(config.max_encoding_message_size)
    }

    fn log_scan_access(
        &self,
        call: FlightScanCall,
        request: &ScanLatestTableRequest,
        grpc_code: Code,
        row_count: Option<i64>,
        flight_data_message_count: Option<usize>,
    ) {
        if let Some(entry) = self
            .access_log_policy
            .scan_access_entry(FlightScanAccessLogInput {
                call,
                request,
                grpc_code,
                row_count,
                flight_data_message_count,
                auth_policy: &self.auth_policy,
                scan_scope_policy: &self.scan_scope_policy,
                scan_grant_policy: &self.scan_grant_policy,
            })
        {
            entry.emit_debug();
        }
    }

    fn try_acquire_request_slot(&self) -> (Option<OwnedSemaphorePermit>, Option<Status>) {
        match self.request_slots.clone().try_acquire_owned() {
            Ok(permit) => (Some(permit), None),
            Err(tokio::sync::TryAcquireError::NoPermits) => (
                None,
                Some(Status::resource_exhausted(
                    "EHDB Flight concurrent request limit exceeded",
                )),
            ),
            Err(tokio::sync::TryAcquireError::Closed) => (
                None,
                Some(Status::unavailable("EHDB Flight request limiter is closed")),
            ),
        }
    }

    fn request_from_descriptor(descriptor: FlightDescriptor) -> Result<ScanLatestTableRequest> {
        if descriptor.r#type != DescriptorType::Cmd as i32 {
            return Err(EhdbError::InvalidState(
                "EHDB scan Flight descriptor requires a command descriptor".to_string(),
            ));
        }

        ScanFlightTicket::decode(descriptor.cmd.as_ref()).map(ScanFlightTicket::into_request)
    }
}

type FlightResponseStream<T> = BoxStream<'static, std::result::Result<T, Status>>;

#[tonic::async_trait]
impl<S> FlightService for LocalArrowFlightServer<S>
where
    S: ImmutableObjectStore + Send + Sync + 'static,
{
    type HandshakeStream = FlightResponseStream<HandshakeResponse>;
    type ListFlightsStream = FlightResponseStream<FlightInfo>;
    type DoGetStream = FlightResponseStream<FlightData>;
    type DoPutStream = FlightResponseStream<PutResult>;
    type DoActionStream = FlightResponseStream<arrow_flight::Result>;
    type ListActionsStream = FlightResponseStream<ActionType>;
    type DoExchangeStream = FlightResponseStream<FlightData>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> std::result::Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented(
            "EHDB Flight handshake is not implemented",
        ))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> std::result::Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented(
            "EHDB Flight list_flights is not implemented",
        ))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        let (_slot, slot_error) = self.try_acquire_request_slot();
        if let Some(status) = slot_error {
            return Err(status);
        }
        let (metadata, _extensions, descriptor) = request.into_parts();
        if let Some(status) = self.auth_policy.authorize_metadata(&metadata) {
            return Err(status);
        }
        let scan_request = Self::request_from_descriptor(descriptor).map_err(error_to_status)?;
        if let Some(status) = self
            .scan_scope_policy
            .authorize_scan_metadata(&metadata, &scan_request)
        {
            self.log_scan_access(
                FlightScanCall::GetFlightInfo,
                &scan_request,
                status.code(),
                None,
                None,
            );
            return Err(status);
        }
        if let Some(status) =
            self.scan_grant_policy
                .authorize_catalog_grant(&metadata, &self.runtime, &scan_request)
        {
            self.log_scan_access(
                FlightScanCall::GetFlightInfo,
                &scan_request,
                status.code(),
                None,
                None,
            );
            return Err(status);
        }
        let info = match self.service.get_flight_info(
            &self.runtime,
            self.store.as_ref(),
            scan_request.clone(),
        ) {
            Ok(info) => info,
            Err(error) => {
                let status = error_to_status(error);
                self.log_scan_access(
                    FlightScanCall::GetFlightInfo,
                    &scan_request,
                    status.code(),
                    None,
                    None,
                );
                return Err(status);
            }
        };
        self.log_scan_access(
            FlightScanCall::GetFlightInfo,
            &scan_request,
            Code::Ok,
            Some(info.total_records),
            None,
        );
        Ok(Response::new(info))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented(
            "EHDB Flight poll_flight_info is not implemented",
        ))
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<SchemaResult>, Status> {
        let (_slot, slot_error) = self.try_acquire_request_slot();
        if let Some(status) = slot_error {
            return Err(status);
        }
        let (metadata, _extensions, descriptor) = request.into_parts();
        if let Some(status) = self.auth_policy.authorize_metadata(&metadata) {
            return Err(status);
        }
        let scan_request = Self::request_from_descriptor(descriptor).map_err(error_to_status)?;
        if let Some(status) = self
            .scan_scope_policy
            .authorize_scan_metadata(&metadata, &scan_request)
        {
            self.log_scan_access(
                FlightScanCall::GetSchema,
                &scan_request,
                status.code(),
                None,
                None,
            );
            return Err(status);
        }
        if let Some(status) =
            self.scan_grant_policy
                .authorize_catalog_grant(&metadata, &self.runtime, &scan_request)
        {
            self.log_scan_access(
                FlightScanCall::GetSchema,
                &scan_request,
                status.code(),
                None,
                None,
            );
            return Err(status);
        }
        let schema =
            match self
                .service
                .get_schema(&self.runtime, self.store.as_ref(), scan_request.clone())
            {
                Ok(schema) => schema,
                Err(error) => {
                    let status = error_to_status(error);
                    self.log_scan_access(
                        FlightScanCall::GetSchema,
                        &scan_request,
                        status.code(),
                        None,
                        None,
                    );
                    return Err(status);
                }
            };
        self.log_scan_access(
            FlightScanCall::GetSchema,
            &scan_request,
            Code::Ok,
            None,
            None,
        );
        Ok(Response::new(schema))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> std::result::Result<Response<Self::DoGetStream>, Status> {
        let (_slot, slot_error) = self.try_acquire_request_slot();
        if let Some(status) = slot_error {
            return Err(status);
        }
        let (metadata, _extensions, ticket) = request.into_parts();
        if let Some(status) = self.auth_policy.authorize_metadata(&metadata) {
            return Err(status);
        }
        let scan_request = ScanFlightTicket::from_arrow_ticket(&ticket)
            .map(ScanFlightTicket::into_request)
            .map_err(error_to_status)?;
        if let Some(status) = self
            .scan_scope_policy
            .authorize_scan_metadata(&metadata, &scan_request)
        {
            self.log_scan_access(
                FlightScanCall::DoGet,
                &scan_request,
                status.code(),
                None,
                None,
            );
            return Err(status);
        }
        if let Some(status) =
            self.scan_grant_policy
                .authorize_catalog_grant(&metadata, &self.runtime, &scan_request)
        {
            self.log_scan_access(
                FlightScanCall::DoGet,
                &scan_request,
                status.code(),
                None,
                None,
            );
            return Err(status);
        }
        let data = match self
            .service
            .do_get(&self.runtime, self.store.as_ref(), &ticket)
        {
            Ok(data) => data,
            Err(error) => {
                let status = error_to_status(error);
                self.log_scan_access(
                    FlightScanCall::DoGet,
                    &scan_request,
                    status.code(),
                    None,
                    None,
                );
                return Err(status);
            }
        };
        self.log_scan_access(
            FlightScanCall::DoGet,
            &scan_request,
            Code::Ok,
            None,
            Some(data.len()),
        );
        Ok(Response::new(
            stream::iter(data.into_iter().map(Ok)).boxed(),
        ))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented(
            "EHDB Flight do_put is not implemented",
        ))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented(
            "EHDB Flight do_exchange is not implemented",
        ))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> std::result::Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented(
            "EHDB Flight do_action is not implemented",
        ))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> std::result::Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented(
            "EHDB Flight list_actions is not implemented",
        ))
    }
}

fn error_to_status(error: EhdbError) -> Status {
    match error {
        EhdbError::InvalidIdentifier(_) | EhdbError::InvalidState(_) => {
            Status::invalid_argument(error.to_string())
        }
        EhdbError::NotFound(_) => Status::not_found(error.to_string()),
        EhdbError::AlreadyExists(_) => Status::already_exists(error.to_string()),
        EhdbError::Storage(_) => Status::internal(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc,
        },
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_flight::FlightClient;
    use arrow_schema::{DataType, Field, Schema};
    use ehdb_core::{
        ChunkId, DocumentId, EhdbError, EmbeddingModelId, NamespaceName, PrincipalId, SnapshotId,
        TableId, TableName, TenantId, TransactionId,
    };
    use ehdb_reference::{
        ArrowScalarValue, LocalArrowIpcTableStore, LocalReferenceRuntime, WriteArrowIpcTable,
    };
    use ehdb_storage::LocalObjectStore;
    use ehdb_transaction::{CatalogMutation, CommitTransaction, Mutation, RetrievalMutation};
    use futures_util::TryStreamExt;
    use tokio::sync::oneshot;
    use tonic::transport::Channel;

    use super::*;

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn local_scan_service_returns_schema_batches_and_row_count() {
        let (log_path, object_root, runtime, store, tenant, namespace, table_name) =
            seeded_table("service-full-scan");

        let result = LocalArrowScanService::default()
            .scan_latest(
                &runtime,
                &store,
                ScanLatestTableRequest {
                    tenant,
                    namespace,
                    table_name,
                    projection: None,
                    predicate: None,
                },
            )
            .unwrap();

        assert_eq!(result.batches.len(), 1);
        assert_eq!(result.row_count, 3);
        assert_eq!(result.schema.field(0).name(), "execution_id");
        assert_eq!(result.schema.field(1).name(), "attempt");

        fs::remove_file(log_path).unwrap();
        fs::remove_dir_all(object_root).unwrap();
    }

    #[test]
    fn local_scan_service_passes_projection_and_filter_to_scanner() {
        let (log_path, object_root, runtime, store, tenant, namespace, table_name) =
            seeded_table("service-filter-projection");

        let result = LocalArrowScanService::default()
            .scan_latest(
                &runtime,
                &store,
                ScanLatestTableRequest {
                    tenant,
                    namespace,
                    table_name,
                    projection: Some(vec!["execution_id".to_string()]),
                    predicate: Some(ArrowEqualityPredicate {
                        column: "attempt".to_string(),
                        value: ArrowScalarValue::Int64(2),
                    }),
                },
            )
            .unwrap();

        assert_eq!(result.row_count, 1);
        assert_eq!(result.schema.fields().len(), 1);
        assert_eq!(result.schema.field(0).name(), "execution_id");
        let execution_ids = result.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(execution_ids.value(0), "exec-2");

        fs::remove_file(log_path).unwrap();
        fs::remove_dir_all(object_root).unwrap();
    }

    #[test]
    fn local_scan_service_propagates_missing_table_errors() {
        let log_path = temp_log_path("service-missing-table");
        let object_root = temp_object_root("service-missing-table");
        let runtime = LocalReferenceRuntime::open(&log_path).unwrap();
        let store = LocalObjectStore::new(&object_root);

        let error = LocalArrowScanService::default()
            .scan_latest(
                &runtime,
                &store,
                ScanLatestTableRequest {
                    tenant: TenantId::new("tenant-a").unwrap(),
                    namespace: NamespaceName::new("system").unwrap(),
                    table_name: TableName::new("missing").unwrap(),
                    projection: None,
                    predicate: None,
                },
            )
            .unwrap_err();

        assert!(matches!(error, EhdbError::NotFound(_)));
        assert!(!log_path.exists());
        if object_root.exists() {
            fs::remove_dir_all(object_root).unwrap();
        }
    }

    #[test]
    fn local_retrieval_search_service_returns_ranked_hits_from_replay() {
        let log_path = temp_log_path("local-retrieval-search-service-ranked");
        let mut runtime = LocalReferenceRuntime::open(&log_path).unwrap();
        seed_retrieval_vectors(&mut runtime).unwrap();
        let reopened = LocalReferenceRuntime::open(&log_path).unwrap();
        let service = LocalRetrievalSearchService;

        let hits = service
            .search_similar(
                &reopened,
                SearchSimilarChunksRequest {
                    tenant: TenantId::new("tenant-a").unwrap(),
                    namespace: NamespaceName::new("knowledge").unwrap(),
                    model_id: EmbeddingModelId::new("text-embedding-local").unwrap(),
                    query: vec![1.0, 0.0],
                    limit: 10,
                },
            )
            .unwrap();

        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].chunk_id, ChunkId::new("chunk-close").unwrap());
        assert_eq!(hits[0].document_id, DocumentId::new("doc-a").unwrap());
        assert_eq!(hits[0].ordinal, 0);
        assert_eq!(hits[0].text, "close local retrieval hit");
        assert_eq!(hits[0].checksum, "sha256-close");
        assert_eq!(
            hits[0].model_id,
            EmbeddingModelId::new("text-embedding-local").unwrap()
        );
        assert_eq!(hits[0].dimensions, 2);
        assert_eq!(hits[0].score, 1.0);
        assert_eq!(hits[1].chunk_id, ChunkId::new("chunk-farther").unwrap());
        assert!(hits[1].score < hits[0].score);

        fs::remove_file(log_path).unwrap();
    }

    #[test]
    fn local_retrieval_search_service_handles_empty_and_invalid_queries() {
        let log_path = temp_log_path("local-retrieval-search-service-validation");
        let mut runtime = LocalReferenceRuntime::open(&log_path).unwrap();
        seed_retrieval_vectors(&mut runtime).unwrap();
        let service = LocalRetrievalSearchService;

        let empty = service
            .search_similar(
                &runtime,
                SearchSimilarChunksRequest {
                    tenant: TenantId::new("tenant-a").unwrap(),
                    namespace: NamespaceName::new("missing").unwrap(),
                    model_id: EmbeddingModelId::new("text-embedding-local").unwrap(),
                    query: vec![1.0, 0.0],
                    limit: 10,
                },
            )
            .unwrap();
        assert!(empty.is_empty());

        let invalid = service
            .search_similar(
                &runtime,
                SearchSimilarChunksRequest {
                    tenant: TenantId::new("tenant-a").unwrap(),
                    namespace: NamespaceName::new("knowledge").unwrap(),
                    model_id: EmbeddingModelId::new("text-embedding-local").unwrap(),
                    query: vec![0.0, 0.0],
                    limit: 10,
                },
            )
            .unwrap_err();
        assert!(matches!(invalid, EhdbError::InvalidState(_)));

        fs::remove_file(log_path).unwrap();
    }

    #[test]
    fn local_retrieval_search_service_returns_text_hits_from_replay() {
        let log_path = temp_log_path("local-retrieval-text-search-service-ranked");
        let mut runtime = LocalReferenceRuntime::open(&log_path).unwrap();
        seed_retrieval_vectors(&mut runtime).unwrap();
        let reopened = LocalReferenceRuntime::open(&log_path).unwrap();
        let service = LocalRetrievalSearchService;

        let hits = service
            .search_text(
                &reopened,
                SearchTextChunksRequest {
                    tenant: TenantId::new("tenant-a").unwrap(),
                    namespace: NamespaceName::new("knowledge").unwrap(),
                    query: "LOCAL".to_string(),
                    limit: 1,
                },
            )
            .unwrap();

        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].chunk_id, ChunkId::new("chunk-close").unwrap());
        assert_eq!(hits[0].document_id, DocumentId::new("doc-a").unwrap());
        assert_eq!(hits[0].ordinal, 0);
        assert_eq!(hits[0].text, "close local retrieval hit");
        assert_eq!(hits[0].checksum, "sha256-close");
        assert_eq!(hits[0].match_count, 1);

        fs::remove_file(log_path).unwrap();
    }

    #[test]
    fn local_retrieval_search_service_handles_empty_and_invalid_text_queries() {
        let log_path = temp_log_path("local-retrieval-text-search-service-validation");
        let mut runtime = LocalReferenceRuntime::open(&log_path).unwrap();
        seed_retrieval_vectors(&mut runtime).unwrap();
        let service = LocalRetrievalSearchService;

        let empty = service
            .search_text(
                &runtime,
                SearchTextChunksRequest {
                    tenant: TenantId::new("tenant-a").unwrap(),
                    namespace: NamespaceName::new("knowledge").unwrap(),
                    query: "missing".to_string(),
                    limit: 10,
                },
            )
            .unwrap();
        assert!(empty.is_empty());

        for request in [
            SearchTextChunksRequest {
                tenant: TenantId::new("tenant-a").unwrap(),
                namespace: NamespaceName::new("knowledge").unwrap(),
                query: " ".to_string(),
                limit: 10,
            },
            SearchTextChunksRequest {
                tenant: TenantId::new("tenant-a").unwrap(),
                namespace: NamespaceName::new("knowledge").unwrap(),
                query: "local".to_string(),
                limit: 0,
            },
        ] {
            assert!(matches!(
                service.search_text(&runtime, request).unwrap_err(),
                EhdbError::InvalidState(_)
            ));
        }

        fs::remove_file(log_path).unwrap();
    }

    #[test]
    fn local_retrieval_search_service_returns_hybrid_hits_from_replay() {
        let log_path = temp_log_path("local-retrieval-hybrid-search-service-ranked");
        let mut runtime = LocalReferenceRuntime::open(&log_path).unwrap();
        seed_retrieval_vectors(&mut runtime).unwrap();
        let reopened = LocalReferenceRuntime::open(&log_path).unwrap();
        let service = LocalRetrievalSearchService;

        let hits = service
            .search_hybrid(
                &reopened,
                SearchHybridChunksRequest {
                    tenant: TenantId::new("tenant-a").unwrap(),
                    namespace: NamespaceName::new("knowledge").unwrap(),
                    model_id: EmbeddingModelId::new("text-embedding-local").unwrap(),
                    query: vec![1.0, 0.0],
                    text_query: "local".to_string(),
                    limit: 10,
                    vector_weight: 1.0,
                    text_weight: 1.0,
                },
            )
            .unwrap();

        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].chunk_id, ChunkId::new("chunk-close").unwrap());
        assert_eq!(hits[0].document_id, DocumentId::new("doc-a").unwrap());
        assert_eq!(hits[0].ordinal, 0);
        assert_eq!(hits[0].text, "close local retrieval hit");
        assert_eq!(hits[0].checksum, "sha256-close");
        assert_eq!(
            hits[0].model_id,
            EmbeddingModelId::new("text-embedding-local").unwrap()
        );
        assert_eq!(hits[0].dimensions, 2);
        assert_eq!(hits[0].text_match_count, 1);
        assert!(hits[0].combined_score > hits[1].combined_score);

        fs::remove_file(log_path).unwrap();
    }

    #[test]
    fn local_retrieval_search_service_handles_empty_and_invalid_hybrid_queries() {
        let log_path = temp_log_path("local-retrieval-hybrid-search-service-validation");
        let mut runtime = LocalReferenceRuntime::open(&log_path).unwrap();
        seed_retrieval_vectors(&mut runtime).unwrap();
        let service = LocalRetrievalSearchService;

        let empty = service
            .search_hybrid(
                &runtime,
                SearchHybridChunksRequest {
                    tenant: TenantId::new("tenant-a").unwrap(),
                    namespace: NamespaceName::new("knowledge").unwrap(),
                    model_id: EmbeddingModelId::new("text-embedding-local").unwrap(),
                    query: vec![1.0, 0.0],
                    text_query: "missing".to_string(),
                    limit: 10,
                    vector_weight: 0.0,
                    text_weight: 1.0,
                },
            )
            .unwrap();
        assert!(empty.is_empty());

        for request in [
            SearchHybridChunksRequest {
                tenant: TenantId::new("tenant-a").unwrap(),
                namespace: NamespaceName::new("knowledge").unwrap(),
                model_id: EmbeddingModelId::new("text-embedding-local").unwrap(),
                query: vec![1.0, 0.0],
                text_query: " ".to_string(),
                limit: 10,
                vector_weight: 1.0,
                text_weight: 1.0,
            },
            SearchHybridChunksRequest {
                tenant: TenantId::new("tenant-a").unwrap(),
                namespace: NamespaceName::new("knowledge").unwrap(),
                model_id: EmbeddingModelId::new("text-embedding-local").unwrap(),
                query: vec![0.0, 0.0],
                text_query: "local".to_string(),
                limit: 10,
                vector_weight: 1.0,
                text_weight: 1.0,
            },
            SearchHybridChunksRequest {
                tenant: TenantId::new("tenant-a").unwrap(),
                namespace: NamespaceName::new("knowledge").unwrap(),
                model_id: EmbeddingModelId::new("text-embedding-local").unwrap(),
                query: vec![1.0, 0.0],
                text_query: "local".to_string(),
                limit: 10,
                vector_weight: -1.0,
                text_weight: 1.0,
            },
        ] {
            assert!(matches!(
                service.search_hybrid(&runtime, request).unwrap_err(),
                EhdbError::InvalidState(_)
            ));
        }

        fs::remove_file(log_path).unwrap();
    }

    #[test]
    fn local_retrieval_context_assembly_returns_ordered_blocks_from_replay() {
        let log_path = temp_log_path("local-retrieval-context-assembly-ranked");
        let mut runtime = LocalReferenceRuntime::open(&log_path).unwrap();
        seed_retrieval_vectors(&mut runtime).unwrap();
        let reopened = LocalReferenceRuntime::open(&log_path).unwrap();
        let service = LocalRetrievalSearchService;

        let context = service
            .assemble_context(
                &reopened,
                AssembleRetrievalContextRequest {
                    tenant: TenantId::new("tenant-a").unwrap(),
                    namespace: NamespaceName::new("knowledge").unwrap(),
                    model_id: EmbeddingModelId::new("text-embedding-local").unwrap(),
                    query: vec![1.0, 0.0],
                    text_query: "local".to_string(),
                    hit_limit: 10,
                    max_block_chars: 64,
                    max_total_chars: 128,
                    vector_weight: 1.0,
                    text_weight: 1.0,
                },
            )
            .unwrap();

        assert_eq!(context.blocks.len(), 2);
        assert!(!context.truncated);
        assert_eq!(
            context.total_text_chars,
            context
                .blocks
                .iter()
                .map(|block| block.text.chars().count())
                .sum::<usize>()
        );
        assert_eq!(
            context.blocks[0].chunk_id,
            ChunkId::new("chunk-close").unwrap()
        );
        assert_eq!(
            context.blocks[0].document_id,
            DocumentId::new("doc-a").unwrap()
        );
        assert_eq!(context.blocks[0].ordinal, 0);
        assert_eq!(context.blocks[0].checksum, "sha256-close");
        assert_eq!(context.blocks[0].text, "close local retrieval hit");
        assert_eq!(
            context.blocks[0].original_text_chars,
            "close local retrieval hit".chars().count()
        );
        assert!(!context.blocks[0].clipped);
        assert_eq!(
            context.blocks[0].model_id,
            EmbeddingModelId::new("text-embedding-local").unwrap()
        );
        assert_eq!(context.blocks[0].dimensions, 2);
        assert_eq!(context.blocks[0].text_match_count, 1);
        assert!(context.blocks[0].combined_score > context.blocks[1].combined_score);

        fs::remove_file(log_path).unwrap();
    }

    #[test]
    fn local_retrieval_context_assembly_clips_blocks_to_text_budgets() {
        let log_path = temp_log_path("local-retrieval-context-assembly-budget");
        let mut runtime = LocalReferenceRuntime::open(&log_path).unwrap();
        seed_retrieval_vectors(&mut runtime).unwrap();
        let service = LocalRetrievalSearchService;

        let context = service
            .assemble_context(
                &runtime,
                AssembleRetrievalContextRequest {
                    tenant: TenantId::new("tenant-a").unwrap(),
                    namespace: NamespaceName::new("knowledge").unwrap(),
                    model_id: EmbeddingModelId::new("text-embedding-local").unwrap(),
                    query: vec![1.0, 0.0],
                    text_query: "local".to_string(),
                    hit_limit: 10,
                    max_block_chars: 5,
                    max_total_chars: 8,
                    vector_weight: 1.0,
                    text_weight: 1.0,
                },
            )
            .unwrap();

        assert_eq!(context.blocks.len(), 2);
        assert_eq!(context.blocks[0].text, "close");
        assert_eq!(context.blocks[1].text, "far");
        assert_eq!(context.total_text_chars, 8);
        assert!(context.truncated);
        assert!(context.blocks.iter().all(|block| block.clipped));

        fs::remove_file(log_path).unwrap();
    }

    #[test]
    fn local_retrieval_context_assembly_scopes_candidates_and_handles_empty_results() {
        let log_path = temp_log_path("local-retrieval-context-assembly-scope");
        let mut runtime = LocalReferenceRuntime::open(&log_path).unwrap();
        seed_retrieval_vectors(&mut runtime).unwrap();
        let service = LocalRetrievalSearchService;

        let scoped = service
            .assemble_context(
                &runtime,
                AssembleRetrievalContextRequest {
                    tenant: TenantId::new("tenant-b").unwrap(),
                    namespace: NamespaceName::new("knowledge").unwrap(),
                    model_id: EmbeddingModelId::new("text-embedding-local").unwrap(),
                    query: vec![1.0, 0.0],
                    text_query: "other".to_string(),
                    hit_limit: 10,
                    max_block_chars: 64,
                    max_total_chars: 128,
                    vector_weight: 1.0,
                    text_weight: 1.0,
                },
            )
            .unwrap();
        assert_eq!(scoped.blocks.len(), 1);
        assert_eq!(
            scoped.blocks[0].chunk_id,
            ChunkId::new("chunk-other-tenant").unwrap()
        );

        let empty = service
            .assemble_context(
                &runtime,
                AssembleRetrievalContextRequest {
                    tenant: TenantId::new("tenant-a").unwrap(),
                    namespace: NamespaceName::new("missing").unwrap(),
                    model_id: EmbeddingModelId::new("text-embedding-local").unwrap(),
                    query: vec![1.0, 0.0],
                    text_query: "local".to_string(),
                    hit_limit: 10,
                    max_block_chars: 64,
                    max_total_chars: 128,
                    vector_weight: 1.0,
                    text_weight: 1.0,
                },
            )
            .unwrap();
        assert!(empty.blocks.is_empty());
        assert_eq!(empty.total_text_chars, 0);
        assert!(!empty.truncated);

        fs::remove_file(log_path).unwrap();
    }

    #[test]
    fn local_retrieval_context_assembly_validates_budgets_and_search_inputs() {
        let log_path = temp_log_path("local-retrieval-context-assembly-validation");
        let mut runtime = LocalReferenceRuntime::open(&log_path).unwrap();
        seed_retrieval_vectors(&mut runtime).unwrap();
        let service = LocalRetrievalSearchService;

        for request in [
            AssembleRetrievalContextRequest {
                tenant: TenantId::new("tenant-a").unwrap(),
                namespace: NamespaceName::new("knowledge").unwrap(),
                model_id: EmbeddingModelId::new("text-embedding-local").unwrap(),
                query: vec![1.0, 0.0],
                text_query: "local".to_string(),
                hit_limit: 10,
                max_block_chars: 0,
                max_total_chars: 128,
                vector_weight: 1.0,
                text_weight: 1.0,
            },
            AssembleRetrievalContextRequest {
                tenant: TenantId::new("tenant-a").unwrap(),
                namespace: NamespaceName::new("knowledge").unwrap(),
                model_id: EmbeddingModelId::new("text-embedding-local").unwrap(),
                query: vec![1.0, 0.0],
                text_query: "local".to_string(),
                hit_limit: 10,
                max_block_chars: 64,
                max_total_chars: 0,
                vector_weight: 1.0,
                text_weight: 1.0,
            },
            AssembleRetrievalContextRequest {
                tenant: TenantId::new("tenant-a").unwrap(),
                namespace: NamespaceName::new("knowledge").unwrap(),
                model_id: EmbeddingModelId::new("text-embedding-local").unwrap(),
                query: vec![1.0, 0.0],
                text_query: "local".to_string(),
                hit_limit: 0,
                max_block_chars: 64,
                max_total_chars: 128,
                vector_weight: 1.0,
                text_weight: 1.0,
            },
            AssembleRetrievalContextRequest {
                tenant: TenantId::new("tenant-a").unwrap(),
                namespace: NamespaceName::new("knowledge").unwrap(),
                model_id: EmbeddingModelId::new("text-embedding-local").unwrap(),
                query: vec![1.0, 0.0],
                text_query: " ".to_string(),
                hit_limit: 10,
                max_block_chars: 64,
                max_total_chars: 128,
                vector_weight: 1.0,
                text_weight: 1.0,
            },
        ] {
            assert!(matches!(
                service.assemble_context(&runtime, request).unwrap_err(),
                EhdbError::InvalidState(_)
            ));
        }

        fs::remove_file(log_path).unwrap();
    }

    #[test]
    fn retrieval_context_request_payload_round_trips() {
        let request = AssembleRetrievalContextRequest {
            tenant: TenantId::new("tenant-a").unwrap(),
            namespace: NamespaceName::new("knowledge").unwrap(),
            model_id: EmbeddingModelId::new("text-embedding-local").unwrap(),
            query: vec![1.0, 0.0],
            text_query: "local".to_string(),
            hit_limit: 10,
            max_block_chars: 64,
            max_total_chars: 128,
            vector_weight: 1.0,
            text_weight: 1.0,
        };

        let encoded = RetrievalContextRequestPayload::new(request.clone())
            .encode()
            .unwrap();
        let decoded = RetrievalContextRequestPayload::decode(&encoded).unwrap();

        assert_eq!(decoded.version, RETRIEVAL_CONTEXT_REQUEST_VERSION);
        assert_eq!(decoded.into_request(), request);
    }

    #[test]
    fn retrieval_context_result_payload_round_trips_assembled_context() {
        let log_path = temp_log_path("retrieval-context-result-payload");
        let mut runtime = LocalReferenceRuntime::open(&log_path).unwrap();
        seed_retrieval_vectors(&mut runtime).unwrap();
        let context = LocalRetrievalSearchService
            .assemble_context(
                &runtime,
                AssembleRetrievalContextRequest {
                    tenant: TenantId::new("tenant-a").unwrap(),
                    namespace: NamespaceName::new("knowledge").unwrap(),
                    model_id: EmbeddingModelId::new("text-embedding-local").unwrap(),
                    query: vec![1.0, 0.0],
                    text_query: "local".to_string(),
                    hit_limit: 10,
                    max_block_chars: 64,
                    max_total_chars: 128,
                    vector_weight: 1.0,
                    text_weight: 1.0,
                },
            )
            .unwrap();

        let encoded = RetrievalContextResultPayload::new(context.clone())
            .encode()
            .unwrap();
        let decoded = RetrievalContextResultPayload::decode(&encoded).unwrap();

        assert_eq!(decoded.version, RETRIEVAL_CONTEXT_RESULT_VERSION);
        assert_eq!(decoded.into_context(), context);

        fs::remove_file(log_path).unwrap();
    }

    #[test]
    fn retrieval_context_payloads_reject_malformed_json() {
        for error in [
            RetrievalContextRequestPayload::decode(b"not-json").unwrap_err(),
            RetrievalContextResultPayload::decode(b"not-json").unwrap_err(),
        ] {
            assert!(matches!(error, EhdbError::InvalidState(_)));
        }
    }

    #[test]
    fn retrieval_context_payloads_reject_unsupported_versions() {
        let request = AssembleRetrievalContextRequest {
            tenant: TenantId::new("tenant-a").unwrap(),
            namespace: NamespaceName::new("knowledge").unwrap(),
            model_id: EmbeddingModelId::new("text-embedding-local").unwrap(),
            query: vec![1.0, 0.0],
            text_query: "local".to_string(),
            hit_limit: 10,
            max_block_chars: 64,
            max_total_chars: 128,
            vector_weight: 1.0,
            text_weight: 1.0,
        };
        let mut request_payload = RetrievalContextRequestPayload::new(request);
        request_payload.version = "ehdb.retrieval.context.request.v0".to_string();
        assert!(matches!(
            request_payload.encode().unwrap_err(),
            EhdbError::InvalidState(_)
        ));
        let unsupported_request = serde_json::to_vec(&request_payload).unwrap();
        assert!(matches!(
            RetrievalContextRequestPayload::decode(&unsupported_request).unwrap_err(),
            EhdbError::InvalidState(_)
        ));

        let context = RetrievalContext {
            blocks: Vec::new(),
            total_text_chars: 0,
            truncated: false,
        };
        let mut result_payload = RetrievalContextResultPayload::new(context);
        result_payload.version = "ehdb.retrieval.context.result.v0".to_string();
        assert!(matches!(
            result_payload.encode().unwrap_err(),
            EhdbError::InvalidState(_)
        ));
        let unsupported_result = serde_json::to_vec(&result_payload).unwrap();
        assert!(matches!(
            RetrievalContextResultPayload::decode(&unsupported_result).unwrap_err(),
            EhdbError::InvalidState(_)
        ));
    }

    #[test]
    fn retrieval_context_payload_executor_returns_result_payload() {
        let log_path = temp_log_path("retrieval-context-payload-executor");
        let mut runtime = LocalReferenceRuntime::open(&log_path).unwrap();
        seed_retrieval_vectors(&mut runtime).unwrap();
        let request_payload =
            RetrievalContextRequestPayload::new(AssembleRetrievalContextRequest {
                tenant: TenantId::new("tenant-a").unwrap(),
                namespace: NamespaceName::new("knowledge").unwrap(),
                model_id: EmbeddingModelId::new("text-embedding-local").unwrap(),
                query: vec![1.0, 0.0],
                text_query: "local".to_string(),
                hit_limit: 10,
                max_block_chars: 64,
                max_total_chars: 128,
                vector_weight: 1.0,
                text_weight: 1.0,
            })
            .encode()
            .unwrap();

        let response_payload = LocalRetrievalSearchService
            .execute_context_payload(&runtime, &request_payload)
            .unwrap();
        let response = RetrievalContextResultPayload::decode(&response_payload).unwrap();
        let context = response.into_context();

        assert_eq!(context.blocks.len(), 2);
        assert_eq!(
            context.blocks[0].chunk_id,
            ChunkId::new("chunk-close").unwrap()
        );
        assert_eq!(context.blocks[0].text, "close local retrieval hit");
        assert!(!context.truncated);

        fs::remove_file(log_path).unwrap();
    }

    #[test]
    fn retrieval_context_payload_executor_returns_empty_result_payload() {
        let log_path = temp_log_path("retrieval-context-payload-executor-empty");
        let mut runtime = LocalReferenceRuntime::open(&log_path).unwrap();
        seed_retrieval_vectors(&mut runtime).unwrap();
        let request_payload =
            RetrievalContextRequestPayload::new(AssembleRetrievalContextRequest {
                tenant: TenantId::new("tenant-a").unwrap(),
                namespace: NamespaceName::new("missing").unwrap(),
                model_id: EmbeddingModelId::new("text-embedding-local").unwrap(),
                query: vec![1.0, 0.0],
                text_query: "local".to_string(),
                hit_limit: 10,
                max_block_chars: 64,
                max_total_chars: 128,
                vector_weight: 1.0,
                text_weight: 1.0,
            })
            .encode()
            .unwrap();

        let response_payload = LocalRetrievalSearchService
            .execute_context_payload(&runtime, &request_payload)
            .unwrap();
        let context = RetrievalContextResultPayload::decode(&response_payload)
            .unwrap()
            .into_context();

        assert!(context.blocks.is_empty());
        assert_eq!(context.total_text_chars, 0);
        assert!(!context.truncated);

        fs::remove_file(log_path).unwrap();
    }

    #[test]
    fn retrieval_context_payload_executor_propagates_payload_and_validation_errors() {
        let log_path = temp_log_path("retrieval-context-payload-executor-errors");
        let mut runtime = LocalReferenceRuntime::open(&log_path).unwrap();
        seed_retrieval_vectors(&mut runtime).unwrap();
        let service = LocalRetrievalSearchService;

        assert!(matches!(
            service
                .execute_context_payload(&runtime, b"not-json")
                .unwrap_err(),
            EhdbError::InvalidState(_)
        ));

        let mut unsupported =
            RetrievalContextRequestPayload::new(AssembleRetrievalContextRequest {
                tenant: TenantId::new("tenant-a").unwrap(),
                namespace: NamespaceName::new("knowledge").unwrap(),
                model_id: EmbeddingModelId::new("text-embedding-local").unwrap(),
                query: vec![1.0, 0.0],
                text_query: "local".to_string(),
                hit_limit: 10,
                max_block_chars: 64,
                max_total_chars: 128,
                vector_weight: 1.0,
                text_weight: 1.0,
            });
        unsupported.version = "ehdb.retrieval.context.request.v0".to_string();
        let unsupported_payload = serde_json::to_vec(&unsupported).unwrap();
        assert!(matches!(
            service
                .execute_context_payload(&runtime, &unsupported_payload)
                .unwrap_err(),
            EhdbError::InvalidState(_)
        ));

        let invalid_request =
            RetrievalContextRequestPayload::new(AssembleRetrievalContextRequest {
                tenant: TenantId::new("tenant-a").unwrap(),
                namespace: NamespaceName::new("knowledge").unwrap(),
                model_id: EmbeddingModelId::new("text-embedding-local").unwrap(),
                query: vec![1.0, 0.0],
                text_query: "local".to_string(),
                hit_limit: 10,
                max_block_chars: 0,
                max_total_chars: 128,
                vector_weight: 1.0,
                text_weight: 1.0,
            })
            .encode()
            .unwrap();
        assert!(matches!(
            service
                .execute_context_payload(&runtime, &invalid_request)
                .unwrap_err(),
            EhdbError::InvalidState(_)
        ));

        fs::remove_file(log_path).unwrap();
    }

    #[test]
    fn retrieval_context_payload_executor_config_defaults_and_validates() {
        let config = RetrievalContextPayloadExecutorConfig::default();
        assert_eq!(
            config.max_request_payload_bytes,
            DEFAULT_RETRIEVAL_CONTEXT_MAX_REQUEST_PAYLOAD_BYTES
        );
        assert_eq!(
            config.max_result_payload_bytes,
            DEFAULT_RETRIEVAL_CONTEXT_MAX_RESULT_PAYLOAD_BYTES
        );
        assert_eq!(
            config.max_receipt_payload_bytes,
            DEFAULT_RETRIEVAL_CONTEXT_MAX_RECEIPT_PAYLOAD_BYTES
        );
        config.validate().unwrap();

        for config in [
            RetrievalContextPayloadExecutorConfig {
                max_request_payload_bytes: 0,
                max_result_payload_bytes: 1,
                max_receipt_payload_bytes: 1,
            },
            RetrievalContextPayloadExecutorConfig {
                max_request_payload_bytes: 1,
                max_result_payload_bytes: 0,
                max_receipt_payload_bytes: 1,
            },
            RetrievalContextPayloadExecutorConfig {
                max_request_payload_bytes: 1,
                max_result_payload_bytes: 1,
                max_receipt_payload_bytes: 0,
            },
        ] {
            assert!(matches!(
                config.validate().unwrap_err(),
                EhdbError::InvalidState(_)
            ));
        }
    }

    #[test]
    fn retrieval_context_payload_executor_rejects_oversized_request_payloads() {
        let log_path = temp_log_path("retrieval-context-payload-executor-request-bound");
        let runtime = LocalReferenceRuntime::open(&log_path).unwrap();
        let request_payload =
            RetrievalContextRequestPayload::new(AssembleRetrievalContextRequest {
                tenant: TenantId::new("tenant-a").unwrap(),
                namespace: NamespaceName::new("knowledge").unwrap(),
                model_id: EmbeddingModelId::new("text-embedding-local").unwrap(),
                query: vec![1.0, 0.0],
                text_query: "local".to_string(),
                hit_limit: 10,
                max_block_chars: 64,
                max_total_chars: 128,
                vector_weight: 1.0,
                text_weight: 1.0,
            })
            .encode()
            .unwrap();

        let config = RetrievalContextPayloadExecutorConfig {
            max_request_payload_bytes: request_payload.len() - 1,
            max_result_payload_bytes: DEFAULT_RETRIEVAL_CONTEXT_MAX_RESULT_PAYLOAD_BYTES,
            max_receipt_payload_bytes: DEFAULT_RETRIEVAL_CONTEXT_MAX_RECEIPT_PAYLOAD_BYTES,
        };

        assert!(matches!(
            LocalRetrievalSearchService
                .execute_context_payload_with_config(&runtime, &request_payload, config)
                .unwrap_err(),
            EhdbError::InvalidState(_)
        ));

        if log_path.exists() {
            fs::remove_file(log_path).unwrap();
        }
    }

    #[test]
    fn retrieval_context_payload_executor_rejects_oversized_result_payloads() {
        let log_path = temp_log_path("retrieval-context-payload-executor-result-bound");
        let mut runtime = LocalReferenceRuntime::open(&log_path).unwrap();
        seed_retrieval_vectors(&mut runtime).unwrap();
        let request_payload =
            RetrievalContextRequestPayload::new(AssembleRetrievalContextRequest {
                tenant: TenantId::new("tenant-a").unwrap(),
                namespace: NamespaceName::new("knowledge").unwrap(),
                model_id: EmbeddingModelId::new("text-embedding-local").unwrap(),
                query: vec![1.0, 0.0],
                text_query: "local".to_string(),
                hit_limit: 10,
                max_block_chars: 64,
                max_total_chars: 128,
                vector_weight: 1.0,
                text_weight: 1.0,
            })
            .encode()
            .unwrap();
        let config = RetrievalContextPayloadExecutorConfig {
            max_request_payload_bytes: request_payload.len(),
            max_result_payload_bytes: 1,
            max_receipt_payload_bytes: DEFAULT_RETRIEVAL_CONTEXT_MAX_RECEIPT_PAYLOAD_BYTES,
        };

        assert!(matches!(
            LocalRetrievalSearchService
                .execute_context_payload_with_config(&runtime, &request_payload, config)
                .unwrap_err(),
            EhdbError::InvalidState(_)
        ));

        fs::remove_file(log_path).unwrap();
    }

    #[test]
    fn retrieval_context_payload_executor_accepts_configured_bounds() {
        let log_path = temp_log_path("retrieval-context-payload-executor-configured");
        let mut runtime = LocalReferenceRuntime::open(&log_path).unwrap();
        seed_retrieval_vectors(&mut runtime).unwrap();
        let request_payload =
            RetrievalContextRequestPayload::new(AssembleRetrievalContextRequest {
                tenant: TenantId::new("tenant-a").unwrap(),
                namespace: NamespaceName::new("knowledge").unwrap(),
                model_id: EmbeddingModelId::new("text-embedding-local").unwrap(),
                query: vec![1.0, 0.0],
                text_query: "local".to_string(),
                hit_limit: 10,
                max_block_chars: 64,
                max_total_chars: 128,
                vector_weight: 1.0,
                text_weight: 1.0,
            })
            .encode()
            .unwrap();
        let config = RetrievalContextPayloadExecutorConfig {
            max_request_payload_bytes: request_payload.len(),
            max_result_payload_bytes: DEFAULT_RETRIEVAL_CONTEXT_MAX_RESULT_PAYLOAD_BYTES,
            max_receipt_payload_bytes: DEFAULT_RETRIEVAL_CONTEXT_MAX_RECEIPT_PAYLOAD_BYTES,
        };

        let result_payload = LocalRetrievalSearchService
            .execute_context_payload_with_config(&runtime, &request_payload, config)
            .unwrap();
        let context = RetrievalContextResultPayload::decode(&result_payload)
            .unwrap()
            .into_context();
        assert_eq!(context.blocks.len(), 2);

        fs::remove_file(log_path).unwrap();
    }

    #[test]
    fn retrieval_context_payload_executor_returns_redacted_summary() {
        let log_path = temp_log_path("retrieval-context-payload-executor-summary");
        let mut runtime = LocalReferenceRuntime::open(&log_path).unwrap();
        seed_retrieval_vectors(&mut runtime).unwrap();
        let request_payload =
            RetrievalContextRequestPayload::new(AssembleRetrievalContextRequest {
                tenant: TenantId::new("tenant-a").unwrap(),
                namespace: NamespaceName::new("knowledge").unwrap(),
                model_id: EmbeddingModelId::new("text-embedding-local").unwrap(),
                query: vec![1.0, 0.0],
                text_query: "local".to_string(),
                hit_limit: 10,
                max_block_chars: 64,
                max_total_chars: 128,
                vector_weight: 1.0,
                text_weight: 1.0,
            })
            .encode()
            .unwrap();

        let execution = LocalRetrievalSearchService
            .execute_context_payload_with_config_and_summary(
                &runtime,
                &request_payload,
                RetrievalContextPayloadExecutorConfig::default(),
            )
            .unwrap();
        let context = RetrievalContextResultPayload::decode(&execution.result_payload)
            .unwrap()
            .into_context();

        assert_eq!(
            execution.summary.request_payload_bytes,
            request_payload.len()
        );
        assert_eq!(
            execution.summary.result_payload_bytes,
            execution.result_payload.len()
        );
        assert_eq!(execution.summary.context_block_count, context.blocks.len());
        assert_eq!(execution.summary.total_text_chars, context.total_text_chars);
        assert!(!execution.summary.truncated);
        assert!(!execution.summary.scope_required);

        let debug_summary = format!("{:?}", execution.summary);
        for sensitive in [
            "tenant-a",
            "knowledge",
            "text-embedding-local",
            "local",
            "close local retrieval hit",
            "chunk-close",
            "doc-a",
            "sha256-close",
        ] {
            assert!(!debug_summary.contains(sensitive));
        }

        fs::remove_file(log_path).unwrap();
    }

    #[test]
    fn retrieval_context_execution_receipt_payload_round_trips_summary() {
        let summary = RetrievalContextPayloadExecutionSummary {
            request_payload_bytes: 128,
            result_payload_bytes: 512,
            context_block_count: 3,
            total_text_chars: 256,
            truncated: true,
            scope_required: true,
        };

        let encoded = RetrievalContextPayloadExecutionReceiptPayload::new(summary.clone())
            .encode()
            .unwrap();
        let decoded = RetrievalContextPayloadExecutionReceiptPayload::decode(&encoded).unwrap();

        assert_eq!(decoded.version, RETRIEVAL_CONTEXT_EXECUTION_RECEIPT_VERSION);
        assert_eq!(decoded.into_summary(), summary);
    }

    #[test]
    fn retrieval_context_execution_receipt_payload_accepts_empty_context_summary() {
        let summary = RetrievalContextPayloadExecutionSummary {
            request_payload_bytes: 128,
            result_payload_bytes: 64,
            context_block_count: 0,
            total_text_chars: 0,
            truncated: false,
            scope_required: true,
        };

        let encoded = RetrievalContextPayloadExecutionReceiptPayload::new(summary.clone())
            .encode()
            .unwrap();
        let decoded = RetrievalContextPayloadExecutionReceiptPayload::decode(&encoded)
            .unwrap()
            .into_summary();

        assert_eq!(decoded, summary);
    }

    #[test]
    fn retrieval_context_execution_receipt_payloads_reject_invalid_bytes() {
        assert!(matches!(
            RetrievalContextPayloadExecutionReceiptPayload::decode(b"not-json").unwrap_err(),
            EhdbError::InvalidState(_)
        ));

        let mut payload = RetrievalContextPayloadExecutionReceiptPayload::new(
            RetrievalContextPayloadExecutionSummary {
                request_payload_bytes: 128,
                result_payload_bytes: 512,
                context_block_count: 3,
                total_text_chars: 256,
                truncated: true,
                scope_required: false,
            },
        );
        payload.version = "ehdb.retrieval.context.execution.receipt.v0".to_string();
        assert!(matches!(
            payload.encode().unwrap_err(),
            EhdbError::InvalidState(_)
        ));

        let unsupported_payload = serde_json::to_vec(&payload).unwrap();
        assert!(matches!(
            RetrievalContextPayloadExecutionReceiptPayload::decode(&unsupported_payload)
                .unwrap_err(),
            EhdbError::InvalidState(_)
        ));
    }

    #[test]
    fn retrieval_context_execution_receipt_payloads_reject_invalid_summaries() {
        for summary in [
            RetrievalContextPayloadExecutionSummary {
                request_payload_bytes: 0,
                result_payload_bytes: 512,
                context_block_count: 3,
                total_text_chars: 256,
                truncated: true,
                scope_required: false,
            },
            RetrievalContextPayloadExecutionSummary {
                request_payload_bytes: 128,
                result_payload_bytes: 0,
                context_block_count: 3,
                total_text_chars: 256,
                truncated: true,
                scope_required: false,
            },
            RetrievalContextPayloadExecutionSummary {
                request_payload_bytes: 128,
                result_payload_bytes: 512,
                context_block_count: 0,
                total_text_chars: 1,
                truncated: true,
                scope_required: false,
            },
        ] {
            let payload = RetrievalContextPayloadExecutionReceiptPayload::new(summary.clone());
            assert!(matches!(
                payload.encode().unwrap_err(),
                EhdbError::InvalidState(_)
            ));

            let invalid_bytes = serde_json::to_vec(&payload).unwrap();
            assert!(matches!(
                RetrievalContextPayloadExecutionReceiptPayload::decode(&invalid_bytes).unwrap_err(),
                EhdbError::InvalidState(_)
            ));
        }
    }

    #[test]
    fn retrieval_context_execution_receipt_payload_excludes_sensitive_context() {
        let log_path = temp_log_path("retrieval-context-execution-receipt-redaction");
        let mut runtime = LocalReferenceRuntime::open(&log_path).unwrap();
        seed_retrieval_vectors(&mut runtime).unwrap();
        let request_payload =
            RetrievalContextRequestPayload::new(AssembleRetrievalContextRequest {
                tenant: TenantId::new("tenant-a").unwrap(),
                namespace: NamespaceName::new("knowledge").unwrap(),
                model_id: EmbeddingModelId::new("text-embedding-local").unwrap(),
                query: vec![1.0, 0.0],
                text_query: "local".to_string(),
                hit_limit: 10,
                max_block_chars: 64,
                max_total_chars: 128,
                vector_weight: 1.0,
                text_weight: 1.0,
            })
            .encode()
            .unwrap();

        let execution = LocalRetrievalSearchService
            .execute_context_payload_with_summary(&runtime, &request_payload)
            .unwrap();
        let receipt_payload =
            RetrievalContextPayloadExecutionReceiptPayload::new(execution.summary.clone())
                .encode()
                .unwrap();
        let receipt_text = String::from_utf8(receipt_payload.clone()).unwrap();

        for sensitive in [
            "tenant-a",
            "knowledge",
            "text-embedding-local",
            "local",
            "close local retrieval hit",
            "chunk-close",
            "doc-a",
            "sha256-close",
        ] {
            assert!(!receipt_text.contains(sensitive));
        }
        assert_eq!(
            RetrievalContextPayloadExecutionReceiptPayload::decode(&receipt_payload)
                .unwrap()
                .into_summary(),
            execution.summary
        );

        fs::remove_file(log_path).unwrap();
    }

    #[test]
    fn retrieval_context_execution_encodes_matching_receipt_payload() {
        let log_path = temp_log_path("retrieval-context-execution-receipt-helper");
        let mut runtime = LocalReferenceRuntime::open(&log_path).unwrap();
        seed_retrieval_vectors(&mut runtime).unwrap();
        let request_payload =
            RetrievalContextRequestPayload::new(AssembleRetrievalContextRequest {
                tenant: TenantId::new("tenant-a").unwrap(),
                namespace: NamespaceName::new("knowledge").unwrap(),
                model_id: EmbeddingModelId::new("text-embedding-local").unwrap(),
                query: vec![1.0, 0.0],
                text_query: "local".to_string(),
                hit_limit: 10,
                max_block_chars: 64,
                max_total_chars: 128,
                vector_weight: 1.0,
                text_weight: 1.0,
            })
            .encode()
            .unwrap();

        let execution = LocalRetrievalSearchService
            .execute_context_payload_with_summary(&runtime, &request_payload)
            .unwrap();
        let receipt_payload = execution.encode_receipt_payload().unwrap();
        let receipt =
            RetrievalContextPayloadExecutionReceiptPayload::decode(&receipt_payload).unwrap();

        assert_eq!(receipt.version, RETRIEVAL_CONTEXT_EXECUTION_RECEIPT_VERSION);
        assert_eq!(receipt.into_summary(), execution.summary);

        fs::remove_file(log_path).unwrap();
    }

    #[test]
    fn retrieval_context_execution_receipt_helper_keeps_redaction_boundary() {
        let log_path = temp_log_path("retrieval-context-execution-receipt-helper-redaction");
        let mut runtime = LocalReferenceRuntime::open(&log_path).unwrap();
        seed_retrieval_vectors(&mut runtime).unwrap();
        let request_payload =
            RetrievalContextRequestPayload::new(AssembleRetrievalContextRequest {
                tenant: TenantId::new("tenant-a").unwrap(),
                namespace: NamespaceName::new("knowledge").unwrap(),
                model_id: EmbeddingModelId::new("text-embedding-local").unwrap(),
                query: vec![1.0, 0.0],
                text_query: "local".to_string(),
                hit_limit: 10,
                max_block_chars: 64,
                max_total_chars: 128,
                vector_weight: 1.0,
                text_weight: 1.0,
            })
            .encode()
            .unwrap();

        let receipt_payload = LocalRetrievalSearchService
            .execute_context_payload_with_summary(&runtime, &request_payload)
            .unwrap()
            .encode_receipt_payload()
            .unwrap();
        let receipt_text = String::from_utf8(receipt_payload).unwrap();

        for sensitive in [
            "tenant-a",
            "knowledge",
            "text-embedding-local",
            "local",
            "close local retrieval hit",
            "chunk-close",
            "doc-a",
            "sha256-close",
        ] {
            assert!(!receipt_text.contains(sensitive));
        }

        fs::remove_file(log_path).unwrap();
    }

    #[test]
    fn retrieval_context_payload_artifacts_return_result_and_receipt_payloads() {
        let log_path = temp_log_path("retrieval-context-payload-artifacts");
        let mut runtime = LocalReferenceRuntime::open(&log_path).unwrap();
        seed_retrieval_vectors(&mut runtime).unwrap();
        let request_payload =
            RetrievalContextRequestPayload::new(AssembleRetrievalContextRequest {
                tenant: TenantId::new("tenant-a").unwrap(),
                namespace: NamespaceName::new("knowledge").unwrap(),
                model_id: EmbeddingModelId::new("text-embedding-local").unwrap(),
                query: vec![1.0, 0.0],
                text_query: "local".to_string(),
                hit_limit: 10,
                max_block_chars: 64,
                max_total_chars: 128,
                vector_weight: 1.0,
                text_weight: 1.0,
            })
            .encode()
            .unwrap();

        let artifacts = LocalRetrievalSearchService
            .execute_context_payload_artifacts(&runtime, &request_payload)
            .unwrap();
        let context = RetrievalContextResultPayload::decode(&artifacts.result_payload)
            .unwrap()
            .into_context();
        let receipt =
            RetrievalContextPayloadExecutionReceiptPayload::decode(&artifacts.receipt_payload)
                .unwrap();

        assert_eq!(context.blocks.len(), 2);
        assert_eq!(receipt.summary.request_payload_bytes, request_payload.len());
        assert_eq!(
            receipt.summary.result_payload_bytes,
            artifacts.result_payload.len()
        );
        assert_eq!(receipt.summary.context_block_count, context.blocks.len());
        assert_eq!(artifacts.receipt_summary().unwrap(), receipt.summary);
        artifacts.validate().unwrap();

        fs::remove_file(log_path).unwrap();
    }

    #[test]
    fn retrieval_context_payload_artifacts_mark_scope_required() {
        let log_path = temp_log_path("retrieval-context-payload-artifacts-scope");
        let mut runtime = LocalReferenceRuntime::open(&log_path).unwrap();
        seed_retrieval_vectors(&mut runtime).unwrap();
        let request_payload =
            RetrievalContextRequestPayload::new(AssembleRetrievalContextRequest {
                tenant: TenantId::new("tenant-a").unwrap(),
                namespace: NamespaceName::new("knowledge").unwrap(),
                model_id: EmbeddingModelId::new("text-embedding-local").unwrap(),
                query: vec![1.0, 0.0],
                text_query: "local".to_string(),
                hit_limit: 10,
                max_block_chars: 64,
                max_total_chars: 128,
                vector_weight: 1.0,
                text_weight: 1.0,
            })
            .encode()
            .unwrap();
        let scope = RetrievalContextPayloadScope {
            tenant: TenantId::new("tenant-a").unwrap(),
            namespace: NamespaceName::new("knowledge").unwrap(),
        };

        let artifacts = LocalRetrievalSearchService
            .execute_context_payload_artifacts_with_scope(
                &runtime,
                &request_payload,
                RetrievalContextPayloadExecutorConfig::default(),
                &scope,
            )
            .unwrap();
        let receipt =
            RetrievalContextPayloadExecutionReceiptPayload::decode(&artifacts.receipt_payload)
                .unwrap();

        assert!(receipt.summary.scope_required);
        assert_eq!(receipt.summary.context_block_count, 2);

        fs::remove_file(log_path).unwrap();
    }

    #[test]
    fn retrieval_context_payload_artifacts_reject_oversized_receipts() {
        let log_path = temp_log_path("retrieval-context-payload-artifacts-receipt-bound");
        let mut runtime = LocalReferenceRuntime::open(&log_path).unwrap();
        seed_retrieval_vectors(&mut runtime).unwrap();
        let request_payload =
            RetrievalContextRequestPayload::new(AssembleRetrievalContextRequest {
                tenant: TenantId::new("tenant-a").unwrap(),
                namespace: NamespaceName::new("knowledge").unwrap(),
                model_id: EmbeddingModelId::new("text-embedding-local").unwrap(),
                query: vec![1.0, 0.0],
                text_query: "local".to_string(),
                hit_limit: 10,
                max_block_chars: 64,
                max_total_chars: 128,
                vector_weight: 1.0,
                text_weight: 1.0,
            })
            .encode()
            .unwrap();
        let config = RetrievalContextPayloadExecutorConfig {
            max_request_payload_bytes: request_payload.len(),
            max_result_payload_bytes: DEFAULT_RETRIEVAL_CONTEXT_MAX_RESULT_PAYLOAD_BYTES,
            max_receipt_payload_bytes: 1,
        };

        assert!(matches!(
            LocalRetrievalSearchService
                .execute_context_payload_artifacts_with_config(&runtime, &request_payload, config)
                .unwrap_err(),
            EhdbError::InvalidState(_)
        ));

        fs::remove_file(log_path).unwrap();
    }

    #[test]
    fn retrieval_context_payload_artifacts_reject_empty_payloads() {
        for artifacts in [
            RetrievalContextPayloadExecutionArtifacts {
                result_payload: Vec::new(),
                receipt_payload: b"not-json".to_vec(),
            },
            RetrievalContextPayloadExecutionArtifacts {
                result_payload: b"result".to_vec(),
                receipt_payload: Vec::new(),
            },
        ] {
            assert!(matches!(
                artifacts.validate().unwrap_err(),
                EhdbError::InvalidState(_)
            ));
        }
    }

    #[test]
    fn retrieval_context_payload_artifacts_reject_malformed_receipts() {
        let artifacts = RetrievalContextPayloadExecutionArtifacts {
            result_payload: b"result".to_vec(),
            receipt_payload: b"not-json".to_vec(),
        };

        assert!(matches!(
            artifacts.receipt_summary().unwrap_err(),
            EhdbError::InvalidState(_)
        ));
        assert!(matches!(
            artifacts.validate().unwrap_err(),
            EhdbError::InvalidState(_)
        ));
    }

    #[test]
    fn retrieval_context_payload_artifacts_reject_result_length_mismatch() {
        let receipt_payload = RetrievalContextPayloadExecutionReceiptPayload::new(
            RetrievalContextPayloadExecutionSummary {
                request_payload_bytes: 128,
                result_payload_bytes: 256,
                context_block_count: 1,
                total_text_chars: 32,
                truncated: false,
                scope_required: false,
            },
        )
        .encode()
        .unwrap();
        let artifacts = RetrievalContextPayloadExecutionArtifacts {
            result_payload: b"short".to_vec(),
            receipt_payload,
        };

        assert!(matches!(
            artifacts.validate().unwrap_err(),
            EhdbError::InvalidState(_)
        ));
    }

    #[test]
    fn retrieval_context_receipt_event_payload_round_trips_from_artifacts() {
        let log_path = temp_log_path("retrieval-context-receipt-event-payload");
        let mut runtime = LocalReferenceRuntime::open(&log_path).unwrap();
        seed_retrieval_vectors(&mut runtime).unwrap();
        let request_payload =
            RetrievalContextRequestPayload::new(AssembleRetrievalContextRequest {
                tenant: TenantId::new("tenant-a").unwrap(),
                namespace: NamespaceName::new("knowledge").unwrap(),
                model_id: EmbeddingModelId::new("text-embedding-local").unwrap(),
                query: vec![1.0, 0.0],
                text_query: "local".to_string(),
                hit_limit: 10,
                max_block_chars: 64,
                max_total_chars: 128,
                vector_weight: 1.0,
                text_weight: 1.0,
            })
            .encode()
            .unwrap();
        let artifacts = LocalRetrievalSearchService
            .execute_context_payload_artifacts(&runtime, &request_payload)
            .unwrap();

        let event = artifacts.receipt_event_payload().unwrap();
        let encoded = artifacts.encode_receipt_event_payload().unwrap();
        let decoded =
            RetrievalContextPayloadExecutionReceiptEventPayload::decode(&encoded).unwrap();

        assert_eq!(
            event.version,
            RETRIEVAL_CONTEXT_EXECUTION_RECEIPT_EVENT_VERSION
        );
        assert_eq!(
            event.subject(),
            RETRIEVAL_CONTEXT_EXECUTION_RECEIPT_EVENT_SUBJECT
        );
        assert_eq!(decoded, event);
        assert_eq!(
            decoded.receipt_summary().unwrap(),
            artifacts.receipt_summary().unwrap()
        );
        assert_eq!(decoded.into_receipt_payload(), artifacts.receipt_payload);

        fs::remove_file(log_path).unwrap();
    }

    #[test]
    fn retrieval_context_receipt_event_payload_rejects_invalid_payloads() {
        assert!(matches!(
            RetrievalContextPayloadExecutionReceiptEventPayload::new(Vec::new()).unwrap_err(),
            EhdbError::InvalidState(_)
        ));
        assert!(matches!(
            RetrievalContextPayloadExecutionReceiptEventPayload::new(b"not-json".to_vec())
                .unwrap_err(),
            EhdbError::InvalidState(_)
        ));

        let receipt_payload = RetrievalContextPayloadExecutionReceiptPayload::new(
            RetrievalContextPayloadExecutionSummary {
                request_payload_bytes: 128,
                result_payload_bytes: 256,
                context_block_count: 1,
                total_text_chars: 32,
                truncated: false,
                scope_required: false,
            },
        )
        .encode()
        .unwrap();
        let mut event =
            RetrievalContextPayloadExecutionReceiptEventPayload::new(receipt_payload).unwrap();
        event.version = "ehdb.retrieval.context.execution.receipt.event.v0".to_string();

        let unsupported_event = serde_json::to_vec(&event).unwrap();
        assert!(matches!(
            RetrievalContextPayloadExecutionReceiptEventPayload::decode(&unsupported_event)
                .unwrap_err(),
            EhdbError::InvalidState(_)
        ));
    }

    #[test]
    fn retrieval_context_receipt_event_payload_keeps_redaction_boundary() {
        let log_path = temp_log_path("retrieval-context-receipt-event-redaction");
        let mut runtime = LocalReferenceRuntime::open(&log_path).unwrap();
        seed_retrieval_vectors(&mut runtime).unwrap();
        let request_payload =
            RetrievalContextRequestPayload::new(AssembleRetrievalContextRequest {
                tenant: TenantId::new("tenant-a").unwrap(),
                namespace: NamespaceName::new("knowledge").unwrap(),
                model_id: EmbeddingModelId::new("text-embedding-local").unwrap(),
                query: vec![1.0, 0.0],
                text_query: "local".to_string(),
                hit_limit: 10,
                max_block_chars: 64,
                max_total_chars: 128,
                vector_weight: 1.0,
                text_weight: 1.0,
            })
            .encode()
            .unwrap();
        let event_payload = LocalRetrievalSearchService
            .execute_context_payload_artifacts(&runtime, &request_payload)
            .unwrap()
            .encode_receipt_event_payload()
            .unwrap();
        let event_text = String::from_utf8(event_payload).unwrap();

        for sensitive in [
            "tenant-a",
            "knowledge",
            "text-embedding-local",
            "local",
            "close local retrieval hit",
            "chunk-close",
            "doc-a",
            "sha256-close",
        ] {
            assert!(!event_text.contains(sensitive));
        }

        fs::remove_file(log_path).unwrap();
    }

    #[test]
    fn retrieval_context_receipt_event_stream_setup_creates_publishable_stream() {
        let log_path = temp_log_path("retrieval-context-receipt-event-stream-setup");
        let mut runtime = LocalReferenceRuntime::open(&log_path).unwrap();
        seed_retrieval_vectors(&mut runtime).unwrap();
        let request_payload =
            RetrievalContextRequestPayload::new(AssembleRetrievalContextRequest {
                tenant: TenantId::new("tenant-a").unwrap(),
                namespace: NamespaceName::new("knowledge").unwrap(),
                model_id: EmbeddingModelId::new("text-embedding-local").unwrap(),
                query: vec![1.0, 0.0],
                text_query: "local".to_string(),
                hit_limit: 10,
                max_block_chars: 64,
                max_total_chars: 128,
                vector_weight: 1.0,
                text_weight: 1.0,
            })
            .encode()
            .unwrap();
        let artifacts = LocalRetrievalSearchService
            .execute_context_payload_artifacts(&runtime, &request_payload)
            .unwrap();
        let target = RetrievalContextReceiptEventStreamTarget {
            tenant: TenantId::new("tenant-a").unwrap(),
            namespace: NamespaceName::new("knowledge").unwrap(),
            stream: StreamName::new("retrieval-receipts").unwrap(),
        };
        let mut stream_log = InMemoryStreamLog::default();
        let config = target.stream_config(RetentionPolicy::KeepAll);

        assert_eq!(config.tenant, target.tenant);
        assert_eq!(config.namespace, target.namespace);
        assert_eq!(config.name, target.stream);
        assert_eq!(config.retention, RetentionPolicy::KeepAll);

        target.create_keep_all_stream(&mut stream_log).unwrap();
        let record = target
            .publish_artifacts(
                &mut stream_log,
                &artifacts,
                TransactionId::new("txn-retrieval-receipt-stream-setup").unwrap(),
            )
            .unwrap();
        let replayed = target.replay_events(&stream_log, None).unwrap();

        assert_eq!(replayed.len(), 1);
        assert_eq!(replayed[0].sequence, record.sequence);
        assert_eq!(
            replayed[0].receipt_summary().unwrap(),
            artifacts.receipt_summary().unwrap()
        );

        fs::remove_file(log_path).unwrap();
    }

    #[test]
    fn retrieval_context_receipt_event_stream_setup_bounded_retention_keeps_latest_records() {
        let log_path = temp_log_path("retrieval-context-receipt-stream-setup-bounded");
        let mut runtime = LocalReferenceRuntime::open(&log_path).unwrap();
        seed_retrieval_vectors(&mut runtime).unwrap();
        let request_payload =
            RetrievalContextRequestPayload::new(AssembleRetrievalContextRequest {
                tenant: TenantId::new("tenant-a").unwrap(),
                namespace: NamespaceName::new("knowledge").unwrap(),
                model_id: EmbeddingModelId::new("text-embedding-local").unwrap(),
                query: vec![1.0, 0.0],
                text_query: "local".to_string(),
                hit_limit: 10,
                max_block_chars: 64,
                max_total_chars: 128,
                vector_weight: 1.0,
                text_weight: 1.0,
            })
            .encode()
            .unwrap();
        let artifacts = LocalRetrievalSearchService
            .execute_context_payload_artifacts(&runtime, &request_payload)
            .unwrap();
        let target = RetrievalContextReceiptEventStreamTarget {
            tenant: TenantId::new("tenant-a").unwrap(),
            namespace: NamespaceName::new("knowledge").unwrap(),
            stream: StreamName::new("retrieval-receipts").unwrap(),
        };
        let mut stream_log = InMemoryStreamLog::default();

        target.create_bounded_stream(&mut stream_log, 1).unwrap();
        target
            .publish_artifacts(
                &mut stream_log,
                &artifacts,
                TransactionId::new("txn-retrieval-receipt-stream-bounded-1").unwrap(),
            )
            .unwrap();
        let latest_record = target
            .publish_artifacts(
                &mut stream_log,
                &artifacts,
                TransactionId::new("txn-retrieval-receipt-stream-bounded-2").unwrap(),
            )
            .unwrap();
        let replayed = target.replay_events(&stream_log, None).unwrap();

        assert_eq!(replayed.len(), 1);
        assert_eq!(replayed[0].sequence, latest_record.sequence);

        fs::remove_file(log_path).unwrap();
    }

    #[test]
    fn retrieval_context_receipt_event_stream_setup_rejects_zero_bound() {
        let target = RetrievalContextReceiptEventStreamTarget {
            tenant: TenantId::new("tenant-a").unwrap(),
            namespace: NamespaceName::new("knowledge").unwrap(),
            stream: StreamName::new("retrieval-receipts").unwrap(),
        };
        let mut stream_log = InMemoryStreamLog::default();

        assert!(matches!(
            target
                .create_bounded_stream(&mut stream_log, 0)
                .unwrap_err(),
            EhdbError::InvalidState(_)
        ));
    }

    #[test]
    fn retrieval_context_receipt_event_stream_setup_rejects_duplicate_stream() {
        let target = RetrievalContextReceiptEventStreamTarget {
            tenant: TenantId::new("tenant-a").unwrap(),
            namespace: NamespaceName::new("knowledge").unwrap(),
            stream: StreamName::new("retrieval-receipts").unwrap(),
        };
        let mut stream_log = InMemoryStreamLog::default();

        target
            .create_stream(&mut stream_log, RetentionPolicy::KeepAll)
            .unwrap();

        assert!(matches!(
            target
                .create_stream(&mut stream_log, RetentionPolicy::KeepAll)
                .unwrap_err(),
            EhdbError::AlreadyExists(_)
        ));
    }

    #[test]
    fn retrieval_context_receipt_event_stream_setup_persists_jsonl_stream() {
        let runtime_log_path = temp_log_path("retrieval-context-receipt-stream-setup-runtime");
        let stream_log_path = temp_log_path("retrieval-context-receipt-stream-setup-jsonl");
        let mut runtime = LocalReferenceRuntime::open(&runtime_log_path).unwrap();
        seed_retrieval_vectors(&mut runtime).unwrap();
        let request_payload =
            RetrievalContextRequestPayload::new(AssembleRetrievalContextRequest {
                tenant: TenantId::new("tenant-a").unwrap(),
                namespace: NamespaceName::new("knowledge").unwrap(),
                model_id: EmbeddingModelId::new("text-embedding-local").unwrap(),
                query: vec![1.0, 0.0],
                text_query: "local".to_string(),
                hit_limit: 10,
                max_block_chars: 64,
                max_total_chars: 128,
                vector_weight: 1.0,
                text_weight: 1.0,
            })
            .encode()
            .unwrap();
        let artifacts = LocalRetrievalSearchService
            .execute_context_payload_artifacts(&runtime, &request_payload)
            .unwrap();
        let target = RetrievalContextReceiptEventStreamTarget {
            tenant: TenantId::new("tenant-a").unwrap(),
            namespace: NamespaceName::new("knowledge").unwrap(),
            stream: StreamName::new("retrieval-receipts").unwrap(),
        };
        let mut stream_log = LocalJsonlStreamLog::open(&stream_log_path).unwrap();
        target.create_keep_all_stream(&mut stream_log).unwrap();
        drop(stream_log);

        let mut reopened = LocalJsonlStreamLog::open(&stream_log_path).unwrap();
        let record = target
            .publish_artifacts(
                &mut reopened,
                &artifacts,
                TransactionId::new("txn-retrieval-receipt-stream-setup-jsonl").unwrap(),
            )
            .unwrap();
        let replayed = target.replay_events(&reopened, None).unwrap();

        assert_eq!(replayed.len(), 1);
        assert_eq!(replayed[0].sequence, record.sequence);

        fs::remove_file(runtime_log_path).unwrap();
        fs::remove_file(stream_log_path).unwrap();
    }

    #[test]
    fn retrieval_context_receipt_event_stream_setup_persists_bounded_jsonl_stream() {
        let runtime_log_path =
            temp_log_path("retrieval-context-receipt-stream-setup-bounded-runtime");
        let stream_log_path = temp_log_path("retrieval-context-receipt-stream-setup-bounded-jsonl");
        let mut runtime = LocalReferenceRuntime::open(&runtime_log_path).unwrap();
        seed_retrieval_vectors(&mut runtime).unwrap();
        let request_payload =
            RetrievalContextRequestPayload::new(AssembleRetrievalContextRequest {
                tenant: TenantId::new("tenant-a").unwrap(),
                namespace: NamespaceName::new("knowledge").unwrap(),
                model_id: EmbeddingModelId::new("text-embedding-local").unwrap(),
                query: vec![1.0, 0.0],
                text_query: "local".to_string(),
                hit_limit: 10,
                max_block_chars: 64,
                max_total_chars: 128,
                vector_weight: 1.0,
                text_weight: 1.0,
            })
            .encode()
            .unwrap();
        let artifacts = LocalRetrievalSearchService
            .execute_context_payload_artifacts(&runtime, &request_payload)
            .unwrap();
        let target = RetrievalContextReceiptEventStreamTarget {
            tenant: TenantId::new("tenant-a").unwrap(),
            namespace: NamespaceName::new("knowledge").unwrap(),
            stream: StreamName::new("retrieval-receipts").unwrap(),
        };
        let mut stream_log = LocalJsonlStreamLog::open(&stream_log_path).unwrap();
        target.create_bounded_stream(&mut stream_log, 1).unwrap();
        drop(stream_log);

        let mut reopened = LocalJsonlStreamLog::open(&stream_log_path).unwrap();
        target
            .publish_artifacts(
                &mut reopened,
                &artifacts,
                TransactionId::new("txn-retrieval-receipt-stream-setup-bounded-jsonl-1").unwrap(),
            )
            .unwrap();
        let latest_record = target
            .publish_artifacts(
                &mut reopened,
                &artifacts,
                TransactionId::new("txn-retrieval-receipt-stream-setup-bounded-jsonl-2").unwrap(),
            )
            .unwrap();
        let replayed = target.replay_events(&reopened, None).unwrap();

        assert_eq!(replayed.len(), 1);
        assert_eq!(replayed[0].sequence, latest_record.sequence);

        fs::remove_file(runtime_log_path).unwrap();
        fs::remove_file(stream_log_path).unwrap();
    }

    #[test]
    fn retrieval_context_receipt_event_publisher_writes_and_replays_stream_record() {
        let log_path = temp_log_path("retrieval-context-receipt-event-publisher");
        let mut runtime = LocalReferenceRuntime::open(&log_path).unwrap();
        seed_retrieval_vectors(&mut runtime).unwrap();
        let request_payload =
            RetrievalContextRequestPayload::new(AssembleRetrievalContextRequest {
                tenant: TenantId::new("tenant-a").unwrap(),
                namespace: NamespaceName::new("knowledge").unwrap(),
                model_id: EmbeddingModelId::new("text-embedding-local").unwrap(),
                query: vec![1.0, 0.0],
                text_query: "local".to_string(),
                hit_limit: 10,
                max_block_chars: 64,
                max_total_chars: 128,
                vector_weight: 1.0,
                text_weight: 1.0,
            })
            .encode()
            .unwrap();
        let artifacts = LocalRetrievalSearchService
            .execute_context_payload_artifacts(&runtime, &request_payload)
            .unwrap();
        let target = RetrievalContextReceiptEventStreamTarget {
            tenant: TenantId::new("tenant-a").unwrap(),
            namespace: NamespaceName::new("knowledge").unwrap(),
            stream: StreamName::new("retrieval-receipts").unwrap(),
        };
        let mut stream_log = InMemoryStreamLog::default();
        stream_log
            .create_stream(ehdb_stream::StreamConfig {
                tenant: target.tenant.clone(),
                namespace: target.namespace.clone(),
                name: target.stream.clone(),
                retention: ehdb_stream::RetentionPolicy::KeepAll,
            })
            .unwrap();

        let record = artifacts
            .publish_receipt_event(
                &mut stream_log,
                &target,
                TransactionId::new("txn-retrieval-receipt-event").unwrap(),
            )
            .unwrap();
        let event =
            RetrievalContextPayloadExecutionReceiptEventPayload::decode(&record.payload).unwrap();
        let replayed = stream_log
            .replay(&target.tenant, &target.namespace, &target.stream, None)
            .unwrap();

        assert_eq!(
            record.subject.as_str(),
            RETRIEVAL_CONTEXT_EXECUTION_RECEIPT_EVENT_SUBJECT
        );
        assert_eq!(
            event.receipt_summary().unwrap(),
            artifacts.receipt_summary().unwrap()
        );
        assert_eq!(replayed, vec![record]);

        fs::remove_file(log_path).unwrap();
    }

    #[test]
    fn retrieval_context_receipt_event_publisher_persists_jsonl_stream_record() {
        let runtime_log_path =
            temp_log_path("retrieval-context-receipt-event-publisher-jsonl-runtime");
        let stream_log_path =
            temp_log_path("retrieval-context-receipt-event-publisher-jsonl-stream");
        let mut runtime = LocalReferenceRuntime::open(&runtime_log_path).unwrap();
        seed_retrieval_vectors(&mut runtime).unwrap();
        let request_payload =
            RetrievalContextRequestPayload::new(AssembleRetrievalContextRequest {
                tenant: TenantId::new("tenant-a").unwrap(),
                namespace: NamespaceName::new("knowledge").unwrap(),
                model_id: EmbeddingModelId::new("text-embedding-local").unwrap(),
                query: vec![1.0, 0.0],
                text_query: "local".to_string(),
                hit_limit: 10,
                max_block_chars: 64,
                max_total_chars: 128,
                vector_weight: 1.0,
                text_weight: 1.0,
            })
            .encode()
            .unwrap();
        let artifacts = LocalRetrievalSearchService
            .execute_context_payload_artifacts(&runtime, &request_payload)
            .unwrap();
        let target = RetrievalContextReceiptEventStreamTarget {
            tenant: TenantId::new("tenant-a").unwrap(),
            namespace: NamespaceName::new("knowledge").unwrap(),
            stream: StreamName::new("retrieval-receipts").unwrap(),
        };
        let mut stream_log = LocalJsonlStreamLog::open(&stream_log_path).unwrap();
        stream_log
            .create_stream(ehdb_stream::StreamConfig {
                tenant: target.tenant.clone(),
                namespace: target.namespace.clone(),
                name: target.stream.clone(),
                retention: ehdb_stream::RetentionPolicy::KeepAll,
            })
            .unwrap();

        let record = target
            .publish_artifacts(
                &mut stream_log,
                &artifacts,
                TransactionId::new("txn-retrieval-receipt-event-jsonl").unwrap(),
            )
            .unwrap();
        drop(stream_log);

        let reopened = LocalJsonlStreamLog::open(&stream_log_path).unwrap();
        let replayed = reopened
            .replay(&target.tenant, &target.namespace, &target.stream, None)
            .unwrap();

        assert_eq!(replayed, vec![record]);

        fs::remove_file(runtime_log_path).unwrap();
        fs::remove_file(stream_log_path).unwrap();
    }

    #[test]
    fn retrieval_context_receipt_event_publisher_rejects_missing_stream_and_bad_artifacts() {
        let target = RetrievalContextReceiptEventStreamTarget {
            tenant: TenantId::new("tenant-a").unwrap(),
            namespace: NamespaceName::new("knowledge").unwrap(),
            stream: StreamName::new("retrieval-receipts").unwrap(),
        };
        let receipt_payload = RetrievalContextPayloadExecutionReceiptPayload::new(
            RetrievalContextPayloadExecutionSummary {
                request_payload_bytes: 128,
                result_payload_bytes: 5,
                context_block_count: 1,
                total_text_chars: 32,
                truncated: false,
                scope_required: false,
            },
        )
        .encode()
        .unwrap();
        let artifacts = RetrievalContextPayloadExecutionArtifacts {
            result_payload: b"valid".to_vec(),
            receipt_payload,
        };
        let mut missing_stream_log = InMemoryStreamLog::default();

        assert!(matches!(
            artifacts
                .publish_receipt_event(
                    &mut missing_stream_log,
                    &target,
                    TransactionId::new("txn-missing-stream").unwrap(),
                )
                .unwrap_err(),
            EhdbError::NotFound(_)
        ));

        let mut stream_log = InMemoryStreamLog::default();
        stream_log
            .create_stream(ehdb_stream::StreamConfig {
                tenant: target.tenant.clone(),
                namespace: target.namespace.clone(),
                name: target.stream.clone(),
                retention: ehdb_stream::RetentionPolicy::KeepAll,
            })
            .unwrap();
        let bad_artifacts = RetrievalContextPayloadExecutionArtifacts {
            result_payload: b"valid".to_vec(),
            receipt_payload: b"not-json".to_vec(),
        };

        assert!(matches!(
            target
                .publish_artifacts(
                    &mut stream_log,
                    &bad_artifacts,
                    TransactionId::new("txn-bad-artifacts").unwrap(),
                )
                .unwrap_err(),
            EhdbError::InvalidState(_)
        ));
        assert!(stream_log
            .replay(&target.tenant, &target.namespace, &target.stream, None)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn retrieval_context_receipt_event_replay_decodes_order_and_cursor() {
        let log_path = temp_log_path("retrieval-context-receipt-event-replay");
        let mut runtime = LocalReferenceRuntime::open(&log_path).unwrap();
        seed_retrieval_vectors(&mut runtime).unwrap();
        let request_payload =
            RetrievalContextRequestPayload::new(AssembleRetrievalContextRequest {
                tenant: TenantId::new("tenant-a").unwrap(),
                namespace: NamespaceName::new("knowledge").unwrap(),
                model_id: EmbeddingModelId::new("text-embedding-local").unwrap(),
                query: vec![1.0, 0.0],
                text_query: "local".to_string(),
                hit_limit: 10,
                max_block_chars: 64,
                max_total_chars: 128,
                vector_weight: 1.0,
                text_weight: 1.0,
            })
            .encode()
            .unwrap();
        let artifacts = LocalRetrievalSearchService
            .execute_context_payload_artifacts(&runtime, &request_payload)
            .unwrap();
        let target = RetrievalContextReceiptEventStreamTarget {
            tenant: TenantId::new("tenant-a").unwrap(),
            namespace: NamespaceName::new("knowledge").unwrap(),
            stream: StreamName::new("retrieval-receipts").unwrap(),
        };
        let mut stream_log = InMemoryStreamLog::default();
        stream_log
            .create_stream(ehdb_stream::StreamConfig {
                tenant: target.tenant.clone(),
                namespace: target.namespace.clone(),
                name: target.stream.clone(),
                retention: ehdb_stream::RetentionPolicy::KeepAll,
            })
            .unwrap();

        let first = target
            .publish_artifacts(
                &mut stream_log,
                &artifacts,
                TransactionId::new("txn-retrieval-receipt-event-1").unwrap(),
            )
            .unwrap();
        let second = target
            .publish_artifacts(
                &mut stream_log,
                &artifacts,
                TransactionId::new("txn-retrieval-receipt-event-2").unwrap(),
            )
            .unwrap();

        let decoded = target.replay_events(&stream_log, None).unwrap();
        let after_first = target
            .replay_events(&stream_log, Some(first.sequence))
            .unwrap();

        assert_eq!(
            decoded
                .iter()
                .map(|record| record.sequence)
                .collect::<Vec<_>>(),
            vec![first.sequence, second.sequence]
        );
        assert_eq!(
            decoded
                .iter()
                .map(|record| record.transaction_id.to_string())
                .collect::<Vec<_>>(),
            vec![
                "txn-retrieval-receipt-event-1".to_string(),
                "txn-retrieval-receipt-event-2".to_string()
            ]
        );
        assert_eq!(after_first.len(), 1);
        assert_eq!(after_first[0].sequence, second.sequence);
        assert_eq!(
            decoded[0].receipt_summary().unwrap(),
            artifacts.receipt_summary().unwrap()
        );

        fs::remove_file(log_path).unwrap();
    }

    #[test]
    fn retrieval_context_receipt_event_replay_decodes_jsonl_after_reopen() {
        let runtime_log_path =
            temp_log_path("retrieval-context-receipt-event-replay-jsonl-runtime");
        let stream_log_path = temp_log_path("retrieval-context-receipt-event-replay-jsonl-stream");
        let mut runtime = LocalReferenceRuntime::open(&runtime_log_path).unwrap();
        seed_retrieval_vectors(&mut runtime).unwrap();
        let request_payload =
            RetrievalContextRequestPayload::new(AssembleRetrievalContextRequest {
                tenant: TenantId::new("tenant-a").unwrap(),
                namespace: NamespaceName::new("knowledge").unwrap(),
                model_id: EmbeddingModelId::new("text-embedding-local").unwrap(),
                query: vec![1.0, 0.0],
                text_query: "local".to_string(),
                hit_limit: 10,
                max_block_chars: 64,
                max_total_chars: 128,
                vector_weight: 1.0,
                text_weight: 1.0,
            })
            .encode()
            .unwrap();
        let artifacts = LocalRetrievalSearchService
            .execute_context_payload_artifacts(&runtime, &request_payload)
            .unwrap();
        let target = RetrievalContextReceiptEventStreamTarget {
            tenant: TenantId::new("tenant-a").unwrap(),
            namespace: NamespaceName::new("knowledge").unwrap(),
            stream: StreamName::new("retrieval-receipts").unwrap(),
        };
        let mut stream_log = LocalJsonlStreamLog::open(&stream_log_path).unwrap();
        stream_log
            .create_stream(ehdb_stream::StreamConfig {
                tenant: target.tenant.clone(),
                namespace: target.namespace.clone(),
                name: target.stream.clone(),
                retention: ehdb_stream::RetentionPolicy::KeepAll,
            })
            .unwrap();
        let published = target
            .publish_artifacts(
                &mut stream_log,
                &artifacts,
                TransactionId::new("txn-retrieval-receipt-replay-jsonl").unwrap(),
            )
            .unwrap();
        drop(stream_log);

        let reopened = LocalJsonlStreamLog::open(&stream_log_path).unwrap();
        let decoded = target.replay_events(&reopened, None).unwrap();

        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].sequence, published.sequence);
        assert_eq!(
            decoded[0].receipt_summary().unwrap(),
            artifacts.receipt_summary().unwrap()
        );

        fs::remove_file(runtime_log_path).unwrap();
        fs::remove_file(stream_log_path).unwrap();
    }

    #[test]
    fn retrieval_context_receipt_event_replay_rejects_wrong_subject_and_malformed_payload() {
        let target = RetrievalContextReceiptEventStreamTarget {
            tenant: TenantId::new("tenant-a").unwrap(),
            namespace: NamespaceName::new("knowledge").unwrap(),
            stream: StreamName::new("retrieval-receipts").unwrap(),
        };
        let receipt_payload = RetrievalContextPayloadExecutionReceiptPayload::new(
            RetrievalContextPayloadExecutionSummary {
                request_payload_bytes: 128,
                result_payload_bytes: 5,
                context_block_count: 1,
                total_text_chars: 32,
                truncated: false,
                scope_required: false,
            },
        )
        .encode()
        .unwrap();
        let event_payload =
            RetrievalContextPayloadExecutionReceiptEventPayload::new(receipt_payload)
                .unwrap()
                .encode()
                .unwrap();
        let mut wrong_subject_log = InMemoryStreamLog::default();
        wrong_subject_log
            .create_stream(ehdb_stream::StreamConfig {
                tenant: target.tenant.clone(),
                namespace: target.namespace.clone(),
                name: target.stream.clone(),
                retention: ehdb_stream::RetentionPolicy::KeepAll,
            })
            .unwrap();
        wrong_subject_log
            .publish(
                &target.tenant,
                &target.namespace,
                &target.stream,
                Subject::new("ehdb.retrieval.context.other").unwrap(),
                event_payload,
                TransactionId::new("txn-wrong-subject").unwrap(),
            )
            .unwrap();

        assert!(matches!(
            target.replay_events(&wrong_subject_log, None).unwrap_err(),
            EhdbError::InvalidState(_)
        ));

        let mut malformed_log = InMemoryStreamLog::default();
        malformed_log
            .create_stream(ehdb_stream::StreamConfig {
                tenant: target.tenant.clone(),
                namespace: target.namespace.clone(),
                name: target.stream.clone(),
                retention: ehdb_stream::RetentionPolicy::KeepAll,
            })
            .unwrap();
        malformed_log
            .publish(
                &target.tenant,
                &target.namespace,
                &target.stream,
                Subject::new(RETRIEVAL_CONTEXT_EXECUTION_RECEIPT_EVENT_SUBJECT).unwrap(),
                b"not-json".to_vec(),
                TransactionId::new("txn-malformed-payload").unwrap(),
            )
            .unwrap();

        assert!(matches!(
            target.replay_events(&malformed_log, None).unwrap_err(),
            EhdbError::InvalidState(_)
        ));
    }

    #[test]
    fn retrieval_context_receipt_event_consumer_resumes_after_ack() {
        let log_path = temp_log_path("retrieval-context-receipt-event-consumer");
        let mut runtime = LocalReferenceRuntime::open(&log_path).unwrap();
        seed_retrieval_vectors(&mut runtime).unwrap();
        let request_payload =
            RetrievalContextRequestPayload::new(AssembleRetrievalContextRequest {
                tenant: TenantId::new("tenant-a").unwrap(),
                namespace: NamespaceName::new("knowledge").unwrap(),
                model_id: EmbeddingModelId::new("text-embedding-local").unwrap(),
                query: vec![1.0, 0.0],
                text_query: "local".to_string(),
                hit_limit: 10,
                max_block_chars: 64,
                max_total_chars: 128,
                vector_weight: 1.0,
                text_weight: 1.0,
            })
            .encode()
            .unwrap();
        let artifacts = LocalRetrievalSearchService
            .execute_context_payload_artifacts(&runtime, &request_payload)
            .unwrap();
        let target = RetrievalContextReceiptEventStreamTarget {
            tenant: TenantId::new("tenant-a").unwrap(),
            namespace: NamespaceName::new("knowledge").unwrap(),
            stream: StreamName::new("retrieval-receipts").unwrap(),
        };
        let consumer = ConsumerName::new("audit-worker").unwrap();
        let mut stream_log = InMemoryStreamLog::default();
        stream_log
            .create_stream(ehdb_stream::StreamConfig {
                tenant: target.tenant.clone(),
                namespace: target.namespace.clone(),
                name: target.stream.clone(),
                retention: ehdb_stream::RetentionPolicy::KeepAll,
            })
            .unwrap();
        target
            .create_consumer(&mut stream_log, consumer.clone())
            .unwrap();
        let first = target
            .publish_artifacts(
                &mut stream_log,
                &artifacts,
                TransactionId::new("txn-retrieval-receipt-consumer-1").unwrap(),
            )
            .unwrap();
        let second = target
            .publish_artifacts(
                &mut stream_log,
                &artifacts,
                TransactionId::new("txn-retrieval-receipt-consumer-2").unwrap(),
            )
            .unwrap();

        let pending = target
            .replay_events_for_consumer(&stream_log, &consumer)
            .unwrap();
        let durable = target
            .ack_event(&mut stream_log, &consumer, first.sequence)
            .unwrap();
        let resumed = target
            .replay_events_for_consumer(&stream_log, &consumer)
            .unwrap();

        assert_eq!(
            pending
                .iter()
                .map(|record| record.sequence)
                .collect::<Vec<_>>(),
            vec![first.sequence, second.sequence]
        );
        assert_eq!(durable.acked_sequence, Some(first.sequence));
        assert_eq!(resumed.len(), 1);
        assert_eq!(resumed[0].sequence, second.sequence);
        assert_eq!(
            resumed[0].receipt_summary().unwrap(),
            artifacts.receipt_summary().unwrap()
        );

        fs::remove_file(log_path).unwrap();
    }

    #[test]
    fn retrieval_context_receipt_event_consumer_rejects_ack_rollback_and_missing_consumer() {
        let target = RetrievalContextReceiptEventStreamTarget {
            tenant: TenantId::new("tenant-a").unwrap(),
            namespace: NamespaceName::new("knowledge").unwrap(),
            stream: StreamName::new("retrieval-receipts").unwrap(),
        };
        let consumer = ConsumerName::new("audit-worker").unwrap();
        let missing_consumer = ConsumerName::new("missing-audit-worker").unwrap();
        let receipt_payload = RetrievalContextPayloadExecutionReceiptPayload::new(
            RetrievalContextPayloadExecutionSummary {
                request_payload_bytes: 128,
                result_payload_bytes: 5,
                context_block_count: 1,
                total_text_chars: 32,
                truncated: false,
                scope_required: false,
            },
        )
        .encode()
        .unwrap();
        let artifacts = RetrievalContextPayloadExecutionArtifacts {
            result_payload: b"valid".to_vec(),
            receipt_payload,
        };
        let mut stream_log = InMemoryStreamLog::default();
        stream_log
            .create_stream(ehdb_stream::StreamConfig {
                tenant: target.tenant.clone(),
                namespace: target.namespace.clone(),
                name: target.stream.clone(),
                retention: ehdb_stream::RetentionPolicy::KeepAll,
            })
            .unwrap();
        target
            .create_consumer(&mut stream_log, consumer.clone())
            .unwrap();
        let first = target
            .publish_artifacts(
                &mut stream_log,
                &artifacts,
                TransactionId::new("txn-retrieval-receipt-consumer-rollback-1").unwrap(),
            )
            .unwrap();
        let second = target
            .publish_artifacts(
                &mut stream_log,
                &artifacts,
                TransactionId::new("txn-retrieval-receipt-consumer-rollback-2").unwrap(),
            )
            .unwrap();
        target
            .ack_event(&mut stream_log, &consumer, second.sequence)
            .unwrap();

        assert!(matches!(
            target
                .ack_event(&mut stream_log, &consumer, first.sequence)
                .unwrap_err(),
            EhdbError::InvalidState(_)
        ));
        assert!(matches!(
            target
                .replay_events_for_consumer(&stream_log, &missing_consumer)
                .unwrap_err(),
            EhdbError::NotFound(_)
        ));
    }

    #[test]
    fn retrieval_context_receipt_event_consumer_persists_jsonl_cursor_after_reopen() {
        let runtime_log_path =
            temp_log_path("retrieval-context-receipt-event-consumer-jsonl-runtime");
        let stream_log_path =
            temp_log_path("retrieval-context-receipt-event-consumer-jsonl-stream");
        let mut runtime = LocalReferenceRuntime::open(&runtime_log_path).unwrap();
        seed_retrieval_vectors(&mut runtime).unwrap();
        let request_payload =
            RetrievalContextRequestPayload::new(AssembleRetrievalContextRequest {
                tenant: TenantId::new("tenant-a").unwrap(),
                namespace: NamespaceName::new("knowledge").unwrap(),
                model_id: EmbeddingModelId::new("text-embedding-local").unwrap(),
                query: vec![1.0, 0.0],
                text_query: "local".to_string(),
                hit_limit: 10,
                max_block_chars: 64,
                max_total_chars: 128,
                vector_weight: 1.0,
                text_weight: 1.0,
            })
            .encode()
            .unwrap();
        let artifacts = LocalRetrievalSearchService
            .execute_context_payload_artifacts(&runtime, &request_payload)
            .unwrap();
        let target = RetrievalContextReceiptEventStreamTarget {
            tenant: TenantId::new("tenant-a").unwrap(),
            namespace: NamespaceName::new("knowledge").unwrap(),
            stream: StreamName::new("retrieval-receipts").unwrap(),
        };
        let consumer = ConsumerName::new("audit-worker").unwrap();
        let mut stream_log = LocalJsonlStreamLog::open(&stream_log_path).unwrap();
        stream_log
            .create_stream(ehdb_stream::StreamConfig {
                tenant: target.tenant.clone(),
                namespace: target.namespace.clone(),
                name: target.stream.clone(),
                retention: ehdb_stream::RetentionPolicy::KeepAll,
            })
            .unwrap();
        target
            .create_consumer(&mut stream_log, consumer.clone())
            .unwrap();
        let first = target
            .publish_artifacts(
                &mut stream_log,
                &artifacts,
                TransactionId::new("txn-retrieval-receipt-consumer-jsonl-1").unwrap(),
            )
            .unwrap();
        let second = target
            .publish_artifacts(
                &mut stream_log,
                &artifacts,
                TransactionId::new("txn-retrieval-receipt-consumer-jsonl-2").unwrap(),
            )
            .unwrap();
        target
            .ack_event(&mut stream_log, &consumer, first.sequence)
            .unwrap();
        drop(stream_log);

        let reopened = LocalJsonlStreamLog::open(&stream_log_path).unwrap();
        let resumed = target
            .replay_events_for_consumer(&reopened, &consumer)
            .unwrap();

        assert_eq!(resumed.len(), 1);
        assert_eq!(resumed[0].sequence, second.sequence);
        assert_eq!(
            resumed[0].receipt_summary().unwrap(),
            artifacts.receipt_summary().unwrap()
        );

        fs::remove_file(runtime_log_path).unwrap();
        fs::remove_file(stream_log_path).unwrap();
    }

    #[test]
    fn retrieval_context_payload_executor_summary_reports_truncation() {
        let log_path = temp_log_path("retrieval-context-payload-executor-summary-truncated");
        let mut runtime = LocalReferenceRuntime::open(&log_path).unwrap();
        seed_retrieval_vectors(&mut runtime).unwrap();
        let request_payload =
            RetrievalContextRequestPayload::new(AssembleRetrievalContextRequest {
                tenant: TenantId::new("tenant-a").unwrap(),
                namespace: NamespaceName::new("knowledge").unwrap(),
                model_id: EmbeddingModelId::new("text-embedding-local").unwrap(),
                query: vec![1.0, 0.0],
                text_query: "local".to_string(),
                hit_limit: 10,
                max_block_chars: 5,
                max_total_chars: 8,
                vector_weight: 1.0,
                text_weight: 1.0,
            })
            .encode()
            .unwrap();

        let execution = LocalRetrievalSearchService
            .execute_context_payload_with_summary(&runtime, &request_payload)
            .unwrap();

        assert_eq!(execution.summary.context_block_count, 2);
        assert_eq!(execution.summary.total_text_chars, 8);
        assert!(execution.summary.truncated);
        assert!(!execution.summary.scope_required);

        fs::remove_file(log_path).unwrap();
    }

    #[test]
    fn retrieval_context_payload_executor_summary_propagates_bounds() {
        let log_path = temp_log_path("retrieval-context-payload-executor-summary-bounds");
        let mut runtime = LocalReferenceRuntime::open(&log_path).unwrap();
        seed_retrieval_vectors(&mut runtime).unwrap();
        let request_payload =
            RetrievalContextRequestPayload::new(AssembleRetrievalContextRequest {
                tenant: TenantId::new("tenant-a").unwrap(),
                namespace: NamespaceName::new("knowledge").unwrap(),
                model_id: EmbeddingModelId::new("text-embedding-local").unwrap(),
                query: vec![1.0, 0.0],
                text_query: "local".to_string(),
                hit_limit: 10,
                max_block_chars: 64,
                max_total_chars: 128,
                vector_weight: 1.0,
                text_weight: 1.0,
            })
            .encode()
            .unwrap();

        assert!(matches!(
            LocalRetrievalSearchService
                .execute_context_payload_with_config_and_summary(
                    &runtime,
                    &request_payload,
                    RetrievalContextPayloadExecutorConfig {
                        max_request_payload_bytes: request_payload.len() - 1,
                        max_result_payload_bytes:
                            DEFAULT_RETRIEVAL_CONTEXT_MAX_RESULT_PAYLOAD_BYTES,
                        max_receipt_payload_bytes:
                            DEFAULT_RETRIEVAL_CONTEXT_MAX_RECEIPT_PAYLOAD_BYTES,
                    },
                )
                .unwrap_err(),
            EhdbError::InvalidState(_)
        ));
        assert!(matches!(
            LocalRetrievalSearchService
                .execute_context_payload_with_config_and_summary(
                    &runtime,
                    &request_payload,
                    RetrievalContextPayloadExecutorConfig {
                        max_request_payload_bytes: request_payload.len(),
                        max_result_payload_bytes: 1,
                        max_receipt_payload_bytes:
                            DEFAULT_RETRIEVAL_CONTEXT_MAX_RECEIPT_PAYLOAD_BYTES,
                    },
                )
                .unwrap_err(),
            EhdbError::InvalidState(_)
        ));

        fs::remove_file(log_path).unwrap();
    }

    #[test]
    fn retrieval_context_payload_scope_accepts_matching_request() {
        let log_path = temp_log_path("retrieval-context-payload-scope");
        let mut runtime = LocalReferenceRuntime::open(&log_path).unwrap();
        seed_retrieval_vectors(&mut runtime).unwrap();
        let request_payload =
            RetrievalContextRequestPayload::new(AssembleRetrievalContextRequest {
                tenant: TenantId::new("tenant-a").unwrap(),
                namespace: NamespaceName::new("knowledge").unwrap(),
                model_id: EmbeddingModelId::new("text-embedding-local").unwrap(),
                query: vec![1.0, 0.0],
                text_query: "local".to_string(),
                hit_limit: 10,
                max_block_chars: 64,
                max_total_chars: 128,
                vector_weight: 1.0,
                text_weight: 1.0,
            })
            .encode()
            .unwrap();
        let scope = RetrievalContextPayloadScope {
            tenant: TenantId::new("tenant-a").unwrap(),
            namespace: NamespaceName::new("knowledge").unwrap(),
        };

        let result_payload = LocalRetrievalSearchService
            .execute_context_payload_with_scope(
                &runtime,
                &request_payload,
                RetrievalContextPayloadExecutorConfig::default(),
                &scope,
            )
            .unwrap();
        let context = RetrievalContextResultPayload::decode(&result_payload)
            .unwrap()
            .into_context();
        assert_eq!(context.blocks.len(), 2);

        fs::remove_file(log_path).unwrap();
    }

    #[test]
    fn retrieval_context_payload_scope_summary_marks_scope_required() {
        let log_path = temp_log_path("retrieval-context-payload-scope-summary");
        let mut runtime = LocalReferenceRuntime::open(&log_path).unwrap();
        seed_retrieval_vectors(&mut runtime).unwrap();
        let request_payload =
            RetrievalContextRequestPayload::new(AssembleRetrievalContextRequest {
                tenant: TenantId::new("tenant-a").unwrap(),
                namespace: NamespaceName::new("knowledge").unwrap(),
                model_id: EmbeddingModelId::new("text-embedding-local").unwrap(),
                query: vec![1.0, 0.0],
                text_query: "local".to_string(),
                hit_limit: 10,
                max_block_chars: 64,
                max_total_chars: 128,
                vector_weight: 1.0,
                text_weight: 1.0,
            })
            .encode()
            .unwrap();
        let scope = RetrievalContextPayloadScope {
            tenant: TenantId::new("tenant-a").unwrap(),
            namespace: NamespaceName::new("knowledge").unwrap(),
        };

        let execution = LocalRetrievalSearchService
            .execute_context_payload_with_scope_and_summary(
                &runtime,
                &request_payload,
                RetrievalContextPayloadExecutorConfig::default(),
                &scope,
            )
            .unwrap();

        assert_eq!(execution.summary.context_block_count, 2);
        assert!(execution.summary.scope_required);

        fs::remove_file(log_path).unwrap();
    }

    #[test]
    fn retrieval_context_payload_scope_rejects_tenant_and_namespace_mismatches() {
        let log_path = temp_log_path("retrieval-context-payload-scope-mismatch");
        let runtime = LocalReferenceRuntime::open(&log_path).unwrap();
        let request_payload =
            RetrievalContextRequestPayload::new(AssembleRetrievalContextRequest {
                tenant: TenantId::new("tenant-a").unwrap(),
                namespace: NamespaceName::new("knowledge").unwrap(),
                model_id: EmbeddingModelId::new("text-embedding-local").unwrap(),
                query: vec![1.0, 0.0],
                text_query: "local".to_string(),
                hit_limit: 10,
                max_block_chars: 64,
                max_total_chars: 128,
                vector_weight: 1.0,
                text_weight: 1.0,
            })
            .encode()
            .unwrap();

        for scope in [
            RetrievalContextPayloadScope {
                tenant: TenantId::new("tenant-b").unwrap(),
                namespace: NamespaceName::new("knowledge").unwrap(),
            },
            RetrievalContextPayloadScope {
                tenant: TenantId::new("tenant-a").unwrap(),
                namespace: NamespaceName::new("other").unwrap(),
            },
        ] {
            assert!(matches!(
                LocalRetrievalSearchService
                    .execute_context_payload_with_scope(
                        &runtime,
                        &request_payload,
                        RetrievalContextPayloadExecutorConfig::default(),
                        &scope,
                    )
                    .unwrap_err(),
                EhdbError::InvalidState(_)
            ));
        }

        if log_path.exists() {
            fs::remove_file(log_path).unwrap();
        }
    }

    #[test]
    fn retrieval_context_payload_scope_propagates_payload_errors() {
        let log_path = temp_log_path("retrieval-context-payload-scope-errors");
        let runtime = LocalReferenceRuntime::open(&log_path).unwrap();
        let scope = RetrievalContextPayloadScope {
            tenant: TenantId::new("tenant-a").unwrap(),
            namespace: NamespaceName::new("knowledge").unwrap(),
        };

        assert!(matches!(
            LocalRetrievalSearchService
                .execute_context_payload_with_scope(
                    &runtime,
                    b"not-json",
                    RetrievalContextPayloadExecutorConfig::default(),
                    &scope,
                )
                .unwrap_err(),
            EhdbError::InvalidState(_)
        ));

        let request_payload =
            RetrievalContextRequestPayload::new(AssembleRetrievalContextRequest {
                tenant: TenantId::new("tenant-a").unwrap(),
                namespace: NamespaceName::new("knowledge").unwrap(),
                model_id: EmbeddingModelId::new("text-embedding-local").unwrap(),
                query: vec![1.0, 0.0],
                text_query: "local".to_string(),
                hit_limit: 10,
                max_block_chars: 64,
                max_total_chars: 128,
                vector_weight: 1.0,
                text_weight: 1.0,
            })
            .encode()
            .unwrap();
        let config = RetrievalContextPayloadExecutorConfig {
            max_request_payload_bytes: request_payload.len() - 1,
            max_result_payload_bytes: DEFAULT_RETRIEVAL_CONTEXT_MAX_RESULT_PAYLOAD_BYTES,
            max_receipt_payload_bytes: DEFAULT_RETRIEVAL_CONTEXT_MAX_RECEIPT_PAYLOAD_BYTES,
        };
        assert!(matches!(
            LocalRetrievalSearchService
                .execute_context_payload_with_scope(&runtime, &request_payload, config, &scope)
                .unwrap_err(),
            EhdbError::InvalidState(_)
        ));

        if log_path.exists() {
            fs::remove_file(log_path).unwrap();
        }
    }

    #[test]
    fn scan_result_rejects_empty_batch_lists() {
        let error = ArrowScanResult::from_batches(Vec::new()).unwrap_err();
        assert!(matches!(error, EhdbError::InvalidState(_)));
    }

    #[test]
    fn flight_scan_ticket_round_trips_request() {
        let request = filtered_request();
        let decoded =
            ScanFlightTicket::decode(&ScanFlightTicket::new(request.clone()).encode().unwrap())
                .unwrap();

        assert_eq!(decoded.version, SCAN_FLIGHT_TICKET_VERSION);
        assert_eq!(decoded.request, request);
    }

    #[test]
    fn arrow_flight_ticket_round_trips_request() {
        let request = filtered_request();
        let arrow_ticket = ScanFlightTicket::new(request.clone())
            .to_arrow_ticket()
            .unwrap();
        let decoded = ScanFlightTicket::from_arrow_ticket(&arrow_ticket).unwrap();

        assert_eq!(decoded.request, request);
    }

    #[test]
    fn flight_scan_ticket_rejects_unsupported_versions() {
        let mut ticket = ScanFlightTicket::new(filtered_request());
        ticket.version = "ehdb.arrow.scan.v0".to_string();

        let error = ticket.encode().unwrap_err();
        assert!(matches!(error, EhdbError::InvalidState(_)));
    }

    #[test]
    fn flight_scan_ticket_rejects_malformed_payloads() {
        let error = ScanFlightTicket::decode(b"not-json").unwrap_err();
        assert!(matches!(error, EhdbError::InvalidState(_)));
    }

    #[test]
    fn flight_scan_ticket_builds_command_descriptor() {
        let ticket = ScanFlightTicket::new(filtered_request());
        let descriptor = ticket.command_descriptor().unwrap();

        assert_eq!(descriptor.r#type, DescriptorType::Cmd as i32);
        assert!(descriptor.path.is_empty());
        assert_eq!(
            ScanFlightTicket::decode(descriptor.cmd.as_ref())
                .unwrap()
                .version,
            SCAN_FLIGHT_TICKET_VERSION
        );
    }

    #[test]
    fn local_scan_service_executes_decoded_flight_ticket() {
        let (log_path, object_root, runtime, store, _, _, _) =
            seeded_table("service-flight-ticket");
        let request = ScanFlightTicket::decode(
            ScanFlightTicket::new(filtered_request())
                .to_arrow_ticket()
                .unwrap()
                .ticket
                .as_ref(),
        )
        .unwrap()
        .into_request();

        let result = LocalArrowScanService::default()
            .scan_latest(&runtime, &store, request)
            .unwrap();

        assert_eq!(result.row_count, 1);
        assert_eq!(result.schema.field(0).name(), "execution_id");

        fs::remove_file(log_path).unwrap();
        fs::remove_dir_all(object_root).unwrap();
    }

    #[test]
    fn scan_result_round_trips_through_flight_data() {
        let (log_path, object_root, runtime, store, tenant, namespace, table_name) =
            seeded_table("service-flight-data");
        let result = LocalArrowScanService::default()
            .scan_latest(
                &runtime,
                &store,
                ScanLatestTableRequest {
                    tenant,
                    namespace,
                    table_name,
                    projection: None,
                    predicate: None,
                },
            )
            .unwrap();

        let flight_data = result.to_flight_data().unwrap();
        let decoded = ArrowScanResult::from_flight_data(&flight_data).unwrap();

        assert_eq!(flight_data.len(), 2);
        assert_eq!(decoded.row_count, result.row_count);
        assert_eq!(decoded.schema.as_ref(), result.schema.as_ref());
        let execution_ids = decoded.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(execution_ids.value(2), "exec-3");

        fs::remove_file(log_path).unwrap();
        fs::remove_dir_all(object_root).unwrap();
    }

    #[test]
    fn scan_result_flight_data_preserves_projected_schema() {
        let (log_path, object_root, runtime, store, tenant, namespace, table_name) =
            seeded_table("service-flight-data-projection");
        let result = LocalArrowScanService::default()
            .scan_latest(
                &runtime,
                &store,
                ScanLatestTableRequest {
                    tenant,
                    namespace,
                    table_name,
                    projection: Some(vec!["execution_id".to_string()]),
                    predicate: Some(ArrowEqualityPredicate {
                        column: "attempt".to_string(),
                        value: ArrowScalarValue::Int64(2),
                    }),
                },
            )
            .unwrap();

        let decoded = ArrowScanResult::from_flight_data(&result.to_flight_data().unwrap()).unwrap();

        assert_eq!(decoded.row_count, 1);
        assert_eq!(decoded.schema.fields().len(), 1);
        assert_eq!(decoded.schema.field(0).name(), "execution_id");
        let execution_ids = decoded.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(execution_ids.value(0), "exec-2");

        fs::remove_file(log_path).unwrap();
        fs::remove_dir_all(object_root).unwrap();
    }

    #[test]
    fn scan_result_flight_data_rejects_empty_streams() {
        let error = ArrowScanResult::from_flight_data(&[]).unwrap_err();
        assert!(matches!(error, EhdbError::InvalidState(_)));
    }

    #[test]
    fn scan_result_flight_data_rejects_malformed_streams() {
        let error = ArrowScanResult::from_flight_data(&[FlightData::default()]).unwrap_err();
        assert!(matches!(error, EhdbError::InvalidState(_)));
    }

    #[test]
    fn scan_result_builds_flight_info() {
        let (log_path, object_root, runtime, store, tenant, namespace, table_name) =
            seeded_table("service-flight-info");
        let request = ScanLatestTableRequest {
            tenant,
            namespace,
            table_name,
            projection: None,
            predicate: None,
        };
        let ticket = ScanFlightTicket::new(request.clone());
        let result = LocalArrowScanService::default()
            .scan_latest(&runtime, &store, request)
            .unwrap();

        let info = result.to_flight_info(&ticket).unwrap();

        assert!(!info.schema.is_empty());
        assert_eq!(info.total_records, 3);
        assert!(info.total_bytes > 0);
        assert!(info.ordered);
        assert_eq!(info.endpoint.len(), 1);
        assert_eq!(
            info.flight_descriptor.unwrap().r#type,
            DescriptorType::Cmd as i32
        );
        let endpoint_ticket = info.endpoint[0].ticket.as_ref().unwrap();
        assert_eq!(
            ScanFlightTicket::from_arrow_ticket(endpoint_ticket)
                .unwrap()
                .request,
            ticket.request
        );

        fs::remove_file(log_path).unwrap();
        fs::remove_dir_all(object_root).unwrap();
    }

    #[test]
    fn scan_result_flight_info_matches_decodable_result_stream() {
        let (log_path, object_root, runtime, store, tenant, namespace, table_name) =
            seeded_table("service-flight-info-stream");
        let request = ScanLatestTableRequest {
            tenant,
            namespace,
            table_name,
            projection: Some(vec!["execution_id".to_string()]),
            predicate: Some(ArrowEqualityPredicate {
                column: "attempt".to_string(),
                value: ArrowScalarValue::Int64(2),
            }),
        };
        let result = LocalArrowScanService::default()
            .scan_latest(&runtime, &store, request.clone())
            .unwrap();

        let info = result
            .to_flight_info(&ScanFlightTicket::new(request))
            .unwrap();
        let decoded = ArrowScanResult::from_flight_data(&result.to_flight_data().unwrap()).unwrap();

        assert_eq!(info.total_records, decoded.row_count as i64);
        assert_eq!(decoded.schema.field(0).name(), "execution_id");
        assert_eq!(decoded.row_count, 1);

        fs::remove_file(log_path).unwrap();
        fs::remove_dir_all(object_root).unwrap();
    }

    #[test]
    fn local_flight_service_returns_info_schema_and_do_get_stream() {
        let (log_path, object_root, runtime, store, tenant, namespace, table_name) =
            seeded_table("local-flight-service");
        let request = ScanLatestTableRequest {
            tenant,
            namespace,
            table_name,
            projection: Some(vec!["execution_id".to_string()]),
            predicate: Some(ArrowEqualityPredicate {
                column: "attempt".to_string(),
                value: ArrowScalarValue::Int64(2),
            }),
        };
        let service = LocalArrowFlightService::default();

        let schema_result = service
            .get_schema(&runtime, &store, request.clone())
            .unwrap();
        let schema: Schema = schema_result.try_into().unwrap();
        let info = service
            .get_flight_info(&runtime, &store, request.clone())
            .unwrap();
        let endpoint_ticket = info.endpoint[0].ticket.as_ref().unwrap();
        let flight_data = service.do_get(&runtime, &store, endpoint_ticket).unwrap();
        let decoded = ArrowScanResult::from_flight_data(&flight_data).unwrap();

        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "execution_id");
        assert_eq!(info.total_records, 1);
        assert_eq!(decoded.row_count, 1);
        assert_eq!(decoded.schema.field(0).name(), "execution_id");
        let execution_ids = decoded.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(execution_ids.value(0), "exec-2");

        fs::remove_file(log_path).unwrap();
        fs::remove_dir_all(object_root).unwrap();
    }

    #[test]
    fn local_flight_service_rejects_malformed_do_get_ticket() {
        let log_path = temp_log_path("local-flight-service-bad-ticket");
        let object_root = temp_object_root("local-flight-service-bad-ticket");
        let runtime = LocalReferenceRuntime::open(&log_path).unwrap();
        let store = LocalObjectStore::new(&object_root);
        let ticket = Ticket {
            ticket: b"not-json".to_vec().into(),
        };

        let error = LocalArrowFlightService::default()
            .do_get(&runtime, &store, &ticket)
            .unwrap_err();

        assert!(matches!(error, EhdbError::InvalidState(_)));
        assert!(!log_path.exists());
        if object_root.exists() {
            fs::remove_dir_all(object_root).unwrap();
        }
    }

    #[test]
    fn local_flight_service_propagates_missing_table_errors() {
        let log_path = temp_log_path("local-flight-service-missing-table");
        let object_root = temp_object_root("local-flight-service-missing-table");
        let runtime = LocalReferenceRuntime::open(&log_path).unwrap();
        let store = LocalObjectStore::new(&object_root);
        let request = ScanLatestTableRequest {
            tenant: TenantId::new("tenant-a").unwrap(),
            namespace: NamespaceName::new("system").unwrap(),
            table_name: TableName::new("missing").unwrap(),
            projection: None,
            predicate: None,
        };

        let error = LocalArrowFlightService::default()
            .get_flight_info(&runtime, &store, request)
            .unwrap_err();

        assert!(matches!(error, EhdbError::NotFound(_)));
        assert!(!log_path.exists());
        if object_root.exists() {
            fs::remove_dir_all(object_root).unwrap();
        }
    }

    #[tokio::test]
    async fn local_flight_server_get_schema_info_and_do_get_stream() {
        let (log_path, object_root, runtime, store, tenant, namespace, table_name) =
            seeded_table("local-flight-server");
        let request = ScanLatestTableRequest {
            tenant,
            namespace,
            table_name,
            projection: Some(vec!["execution_id".to_string()]),
            predicate: Some(ArrowEqualityPredicate {
                column: "attempt".to_string(),
                value: ArrowScalarValue::Int64(2),
            }),
        };
        let ticket = ScanFlightTicket::new(request);
        let server = LocalArrowFlightServer::new(Arc::new(runtime), Arc::new(store));

        let schema_result = server
            .get_schema(Request::new(ticket.command_descriptor().unwrap()))
            .await
            .unwrap()
            .into_inner();
        let schema: Schema = schema_result.try_into().unwrap();
        let info = server
            .get_flight_info(Request::new(ticket.command_descriptor().unwrap()))
            .await
            .unwrap()
            .into_inner();
        let endpoint_ticket = info.endpoint[0].ticket.clone().unwrap();
        let flight_data = server
            .do_get(Request::new(endpoint_ticket))
            .await
            .unwrap()
            .into_inner()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let decoded = ArrowScanResult::from_flight_data(&flight_data).unwrap();

        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "execution_id");
        assert_eq!(info.total_records, 1);
        assert_eq!(decoded.row_count, 1);
        assert_eq!(decoded.schema.field(0).name(), "execution_id");
        let execution_ids = decoded.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(execution_ids.value(0), "exec-2");

        fs::remove_file(log_path).unwrap();
        fs::remove_dir_all(object_root).unwrap();
    }

    #[tokio::test]
    async fn local_flight_server_enforces_concurrent_request_limit() {
        let (log_path, object_root, runtime, store, tenant, namespace, table_name) =
            seeded_table("local-flight-server-concurrency-limit");
        let request = ScanLatestTableRequest {
            tenant,
            namespace,
            table_name,
            projection: Some(vec!["execution_id".to_string()]),
            predicate: Some(ArrowEqualityPredicate {
                column: "attempt".to_string(),
                value: ArrowScalarValue::Int64(2),
            }),
        };
        let ticket = ScanFlightTicket::new(request);
        let server = LocalArrowFlightServer::new_with_runtime_limits(
            Arc::new(runtime),
            Arc::new(store),
            FlightAuthPolicy::DisabledForLocalReference,
            FlightScanScopePolicy::DisabledForLocalReference,
            FlightScanGrantPolicy::DisabledForLocalReference,
            FlightAccessLogPolicy::Disabled,
            1,
        );
        let _held_slot = server.request_slots.clone().try_acquire_owned().unwrap();

        let info_error = server
            .get_flight_info(Request::new(ticket.command_descriptor().unwrap()))
            .await
            .unwrap_err();
        let schema_error = server
            .get_schema(Request::new(ticket.command_descriptor().unwrap()))
            .await
            .unwrap_err();
        let do_get_error = match server
            .do_get(Request::new(ticket.to_arrow_ticket().unwrap()))
            .await
        {
            Ok(_) => panic!("exhausted Flight request slots must fail do_get"),
            Err(error) => error,
        };

        assert_eq!(info_error.code(), tonic::Code::ResourceExhausted);
        assert_eq!(schema_error.code(), tonic::Code::ResourceExhausted);
        assert_eq!(do_get_error.code(), tonic::Code::ResourceExhausted);

        fs::remove_file(log_path).unwrap();
        fs::remove_dir_all(object_root).unwrap();
    }

    #[tokio::test]
    async fn local_flight_server_enforces_header_token_auth() {
        let (log_path, object_root, runtime, store, tenant, namespace, table_name) =
            seeded_table("local-flight-server-auth");
        let request = ScanLatestTableRequest {
            tenant,
            namespace,
            table_name,
            projection: Some(vec!["execution_id".to_string()]),
            predicate: Some(ArrowEqualityPredicate {
                column: "attempt".to_string(),
                value: ArrowScalarValue::Int64(2),
            }),
        };
        let ticket = ScanFlightTicket::new(request);
        let server = LocalArrowFlightServer::new_with_auth(
            Arc::new(runtime),
            Arc::new(store),
            FlightAuthPolicy::HeaderToken {
                header_name: "x-ehdb-auth".to_string(),
                token: "local-secret".to_string(),
            },
        );

        let missing_error = server
            .get_flight_info(Request::new(ticket.command_descriptor().unwrap()))
            .await
            .unwrap_err();
        assert_eq!(missing_error.code(), tonic::Code::Unauthenticated);
        let missing_schema_error = server
            .get_schema(Request::new(ticket.command_descriptor().unwrap()))
            .await
            .unwrap_err();
        assert_eq!(missing_schema_error.code(), tonic::Code::Unauthenticated);

        let mut wrong_request = Request::new(ticket.command_descriptor().unwrap());
        wrong_request
            .metadata_mut()
            .insert("x-ehdb-auth", "wrong-secret".parse().unwrap());
        let wrong_error = server.get_flight_info(wrong_request).await.unwrap_err();
        assert_eq!(wrong_error.code(), tonic::Code::Unauthenticated);

        let mut schema_request = Request::new(ticket.command_descriptor().unwrap());
        schema_request
            .metadata_mut()
            .insert("x-ehdb-auth", "local-secret".parse().unwrap());
        let schema: Schema = server
            .get_schema(schema_request)
            .await
            .unwrap()
            .into_inner()
            .try_into()
            .unwrap();
        let mut info_request = Request::new(ticket.command_descriptor().unwrap());
        info_request
            .metadata_mut()
            .insert("x-ehdb-auth", "local-secret".parse().unwrap());
        let info = server
            .get_flight_info(info_request)
            .await
            .unwrap()
            .into_inner();
        let endpoint_ticket = info.endpoint[0].ticket.clone().unwrap();

        match server.do_get(Request::new(endpoint_ticket.clone())).await {
            Ok(_) => panic!("missing Flight auth token must fail"),
            Err(error) => assert_eq!(error.code(), tonic::Code::Unauthenticated),
        }

        let mut do_get_request = Request::new(endpoint_ticket);
        do_get_request
            .metadata_mut()
            .insert("x-ehdb-auth", "local-secret".parse().unwrap());
        let flight_data = server
            .do_get(do_get_request)
            .await
            .unwrap()
            .into_inner()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let decoded = ArrowScanResult::from_flight_data(&flight_data).unwrap();

        assert_eq!(schema.field(0).name(), "execution_id");
        assert_eq!(info.total_records, 1);
        assert_eq!(decoded.row_count, 1);

        fs::remove_file(log_path).unwrap();
        fs::remove_dir_all(object_root).unwrap();
    }

    #[tokio::test]
    async fn local_flight_server_enforces_scan_scope_metadata() {
        let (log_path, object_root, runtime, store, tenant, namespace, table_name) =
            seeded_table("local-flight-server-scope");
        let request = ScanLatestTableRequest {
            tenant,
            namespace,
            table_name,
            projection: Some(vec!["execution_id".to_string()]),
            predicate: Some(ArrowEqualityPredicate {
                column: "attempt".to_string(),
                value: ArrowScalarValue::Int64(2),
            }),
        };
        let ticket = ScanFlightTicket::new(request);
        let server = LocalArrowFlightServer::new_with_policies(
            Arc::new(runtime),
            Arc::new(store),
            FlightAuthPolicy::DisabledForLocalReference,
            FlightScanScopePolicy::require_default_tenant_namespace(),
        );

        let missing_error = server
            .get_flight_info(Request::new(ticket.command_descriptor().unwrap()))
            .await
            .unwrap_err();
        assert_eq!(missing_error.code(), tonic::Code::Unauthenticated);
        let missing_schema_error = server
            .get_schema(Request::new(ticket.command_descriptor().unwrap()))
            .await
            .unwrap_err();
        assert_eq!(missing_schema_error.code(), tonic::Code::Unauthenticated);

        let mut wrong_scope = Request::new(ticket.command_descriptor().unwrap());
        wrong_scope.metadata_mut().insert(
            DEFAULT_FLIGHT_TENANT_SCOPE_HEADER,
            "tenant-b".parse().unwrap(),
        );
        wrong_scope.metadata_mut().insert(
            DEFAULT_FLIGHT_NAMESPACE_SCOPE_HEADER,
            "system".parse().unwrap(),
        );
        let wrong_error = server.get_flight_info(wrong_scope).await.unwrap_err();
        assert_eq!(wrong_error.code(), tonic::Code::PermissionDenied);

        let mut schema_request = Request::new(ticket.command_descriptor().unwrap());
        schema_request.metadata_mut().insert(
            DEFAULT_FLIGHT_TENANT_SCOPE_HEADER,
            "tenant-a".parse().unwrap(),
        );
        schema_request.metadata_mut().insert(
            DEFAULT_FLIGHT_NAMESPACE_SCOPE_HEADER,
            "system".parse().unwrap(),
        );
        let schema: Schema = server
            .get_schema(schema_request)
            .await
            .unwrap()
            .into_inner()
            .try_into()
            .unwrap();
        let mut info_request = Request::new(ticket.command_descriptor().unwrap());
        info_request.metadata_mut().insert(
            DEFAULT_FLIGHT_TENANT_SCOPE_HEADER,
            "tenant-a".parse().unwrap(),
        );
        info_request.metadata_mut().insert(
            DEFAULT_FLIGHT_NAMESPACE_SCOPE_HEADER,
            "system".parse().unwrap(),
        );
        let info = server
            .get_flight_info(info_request)
            .await
            .unwrap()
            .into_inner();
        let endpoint_ticket = info.endpoint[0].ticket.clone().unwrap();

        match server.do_get(Request::new(endpoint_ticket.clone())).await {
            Ok(_) => panic!("missing Flight scan scope metadata must fail"),
            Err(error) => assert_eq!(error.code(), tonic::Code::Unauthenticated),
        }

        let mut do_get_request = Request::new(endpoint_ticket);
        do_get_request.metadata_mut().insert(
            DEFAULT_FLIGHT_TENANT_SCOPE_HEADER,
            "tenant-a".parse().unwrap(),
        );
        do_get_request.metadata_mut().insert(
            DEFAULT_FLIGHT_NAMESPACE_SCOPE_HEADER,
            "system".parse().unwrap(),
        );
        let flight_data = server
            .do_get(do_get_request)
            .await
            .unwrap()
            .into_inner()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let decoded = ArrowScanResult::from_flight_data(&flight_data).unwrap();

        assert_eq!(schema.field(0).name(), "execution_id");
        assert_eq!(info.total_records, 1);
        assert_eq!(decoded.row_count, 1);

        fs::remove_file(log_path).unwrap();
        fs::remove_dir_all(object_root).unwrap();
    }

    #[tokio::test]
    async fn local_flight_server_enforces_catalog_scan_grants() {
        let (log_path, object_root, mut runtime, store, tenant, namespace, table_name) =
            seeded_table("local-flight-server-scan-grant");
        grant_scan(&mut runtime, "worker-system", "txn-grant-scan").unwrap();
        let request = ScanLatestTableRequest {
            tenant,
            namespace,
            table_name,
            projection: Some(vec!["execution_id".to_string()]),
            predicate: Some(ArrowEqualityPredicate {
                column: "attempt".to_string(),
                value: ArrowScalarValue::Int64(2),
            }),
        };
        let ticket = ScanFlightTicket::new(request);
        let server = LocalArrowFlightServer::new_with_authorization_policies(
            Arc::new(runtime),
            Arc::new(store),
            FlightAuthPolicy::DisabledForLocalReference,
            FlightScanScopePolicy::DisabledForLocalReference,
            FlightScanGrantPolicy::require_default_principal(),
        );

        let missing_error = server
            .get_flight_info(Request::new(ticket.command_descriptor().unwrap()))
            .await
            .unwrap_err();
        assert_eq!(missing_error.code(), tonic::Code::Unauthenticated);
        let missing_schema_error = server
            .get_schema(Request::new(ticket.command_descriptor().unwrap()))
            .await
            .unwrap_err();
        assert_eq!(missing_schema_error.code(), tonic::Code::Unauthenticated);

        let mut wrong_principal = Request::new(ticket.command_descriptor().unwrap());
        wrong_principal.metadata_mut().insert(
            DEFAULT_FLIGHT_PRINCIPAL_HEADER,
            "worker-other".parse().unwrap(),
        );
        let wrong_error = server.get_flight_info(wrong_principal).await.unwrap_err();
        assert_eq!(wrong_error.code(), tonic::Code::PermissionDenied);

        let mut schema_request = Request::new(ticket.command_descriptor().unwrap());
        schema_request.metadata_mut().insert(
            DEFAULT_FLIGHT_PRINCIPAL_HEADER,
            "worker-system".parse().unwrap(),
        );
        let schema: Schema = server
            .get_schema(schema_request)
            .await
            .unwrap()
            .into_inner()
            .try_into()
            .unwrap();
        let mut info_request = Request::new(ticket.command_descriptor().unwrap());
        info_request.metadata_mut().insert(
            DEFAULT_FLIGHT_PRINCIPAL_HEADER,
            "worker-system".parse().unwrap(),
        );
        let info = server
            .get_flight_info(info_request)
            .await
            .unwrap()
            .into_inner();
        let endpoint_ticket = info.endpoint[0].ticket.clone().unwrap();

        match server.do_get(Request::new(endpoint_ticket.clone())).await {
            Ok(_) => panic!("missing Flight principal metadata must fail"),
            Err(error) => assert_eq!(error.code(), tonic::Code::Unauthenticated),
        }

        let mut do_get_request = Request::new(endpoint_ticket);
        do_get_request.metadata_mut().insert(
            DEFAULT_FLIGHT_PRINCIPAL_HEADER,
            "worker-system".parse().unwrap(),
        );
        let flight_data = server
            .do_get(do_get_request)
            .await
            .unwrap()
            .into_inner()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let decoded = ArrowScanResult::from_flight_data(&flight_data).unwrap();

        assert_eq!(schema.field(0).name(), "execution_id");
        assert_eq!(info.total_records, 1);
        assert_eq!(decoded.row_count, 1);

        fs::remove_file(log_path).unwrap();
        fs::remove_dir_all(object_root).unwrap();
    }

    #[tokio::test]
    async fn local_flight_server_rejects_non_command_descriptors() {
        let log_path = temp_log_path("local-flight-server-path-descriptor");
        let object_root = temp_object_root("local-flight-server-path-descriptor");
        let runtime = LocalReferenceRuntime::open(&log_path).unwrap();
        let store = LocalObjectStore::new(&object_root);
        let descriptor = FlightDescriptor {
            r#type: DescriptorType::Path as i32,
            cmd: Vec::new().into(),
            path: vec!["tenant-a".to_string(), "system".to_string()],
        };
        let server = LocalArrowFlightServer::new(Arc::new(runtime), Arc::new(store));

        let info_error = server
            .get_flight_info(Request::new(descriptor.clone()))
            .await
            .unwrap_err();
        let schema_error = server
            .get_schema(Request::new(descriptor))
            .await
            .unwrap_err();

        assert_eq!(info_error.code(), tonic::Code::InvalidArgument);
        assert_eq!(schema_error.code(), tonic::Code::InvalidArgument);
        assert!(!log_path.exists());
        if object_root.exists() {
            fs::remove_dir_all(object_root).unwrap();
        }
    }

    #[tokio::test]
    async fn local_flight_server_rejects_malformed_do_get_tickets() {
        let log_path = temp_log_path("local-flight-server-bad-ticket");
        let object_root = temp_object_root("local-flight-server-bad-ticket");
        let runtime = LocalReferenceRuntime::open(&log_path).unwrap();
        let store = LocalObjectStore::new(&object_root);
        let ticket = Ticket {
            ticket: b"not-json".to_vec().into(),
        };

        let result = LocalArrowFlightServer::new(Arc::new(runtime), Arc::new(store))
            .do_get(Request::new(ticket))
            .await;

        match result {
            Ok(_) => panic!("malformed tickets must fail"),
            Err(error) => assert_eq!(error.code(), tonic::Code::InvalidArgument),
        }
        assert!(!log_path.exists());
        if object_root.exists() {
            fs::remove_dir_all(object_root).unwrap();
        }
    }

    #[tokio::test]
    async fn local_flight_server_marks_unsupported_methods_unimplemented() {
        let log_path = temp_log_path("local-flight-server-unimplemented");
        let object_root = temp_object_root("local-flight-server-unimplemented");
        let runtime = LocalReferenceRuntime::open(&log_path).unwrap();
        let store = LocalObjectStore::new(&object_root);
        let server = LocalArrowFlightServer::new(Arc::new(runtime), Arc::new(store));

        let error = server
            .poll_flight_info(Request::new(FlightDescriptor::default()))
            .await
            .unwrap_err();

        assert_eq!(error.code(), tonic::Code::Unimplemented);
        assert!(!log_path.exists());
        if object_root.exists() {
            fs::remove_dir_all(object_root).unwrap();
        }
    }

    #[tokio::test]
    async fn flight_listener_binds_loopback_and_shutdown_completes() {
        let log_path = temp_log_path("local-flight-listener");
        let object_root = temp_object_root("local-flight-listener");
        let runtime = LocalReferenceRuntime::open(&log_path).unwrap();
        let store = LocalObjectStore::new(&object_root);
        let listener = LocalArrowFlightServerConfig::default()
            .bind_loopback_listener(Arc::new(runtime), Arc::new(store))
            .await
            .unwrap();
        let local_addr = listener.local_addr();

        assert!(local_addr.ip().is_loopback());
        assert_ne!(local_addr.port(), 0);

        tokio::time::timeout(
            Duration::from_secs(2),
            listener.serve_with_shutdown(async {}),
        )
        .await
        .unwrap()
        .unwrap();

        assert!(!log_path.exists());
        if object_root.exists() {
            fs::remove_dir_all(object_root).unwrap();
        }
    }

    #[tokio::test]
    async fn flight_client_reads_scan_over_loopback_listener() {
        let (log_path, object_root, runtime, store, tenant, namespace, table_name) =
            seeded_table("local-flight-client-smoke");
        let listener = LocalArrowFlightServerConfig::default()
            .bind_loopback_listener(Arc::new(runtime), Arc::new(store))
            .await
            .unwrap();
        let endpoint = format!("http://{}", listener.local_addr());
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server_task = tokio::spawn(listener.serve_with_shutdown(async move {
            let _ = shutdown_rx.await;
        }));
        let channel = Channel::from_shared(endpoint)
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut client = FlightClient::new(channel);
        let request = ScanLatestTableRequest {
            tenant,
            namespace,
            table_name,
            projection: Some(vec!["execution_id".to_string()]),
            predicate: Some(ArrowEqualityPredicate {
                column: "attempt".to_string(),
                value: ArrowScalarValue::Int64(2),
            }),
        };
        let ticket = ScanFlightTicket::new(request);

        let schema = client
            .get_schema(ticket.command_descriptor().unwrap())
            .await
            .unwrap();
        let info = client
            .get_flight_info(ticket.command_descriptor().unwrap())
            .await
            .unwrap();
        let endpoint_ticket = info.endpoint[0].ticket.clone().unwrap();
        let batches = client
            .do_get(endpoint_ticket)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "execution_id");
        assert_eq!(info.total_records, 1);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(batches[0].schema().field(0).name(), "execution_id");
        let execution_ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(execution_ids.value(0), "exec-2");

        drop(client);
        shutdown_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), server_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        fs::remove_file(log_path).unwrap();
        fs::remove_dir_all(object_root).unwrap();
    }

    #[tokio::test]
    async fn flight_client_uses_header_token_auth_over_loopback_listener() {
        let (log_path, object_root, runtime, store, tenant, namespace, table_name) =
            seeded_table("local-flight-client-auth-smoke");
        let config = LocalArrowFlightServerConfig {
            auth_policy: FlightAuthPolicy::HeaderToken {
                header_name: "x-ehdb-auth".to_string(),
                token: "local-secret".to_string(),
            },
            ..LocalArrowFlightServerConfig::default()
        };
        let listener = config
            .bind_loopback_listener(Arc::new(runtime), Arc::new(store))
            .await
            .unwrap();
        let endpoint = format!("http://{}", listener.local_addr());
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server_task = tokio::spawn(listener.serve_with_shutdown(async move {
            let _ = shutdown_rx.await;
        }));
        let channel = Channel::from_shared(endpoint)
            .unwrap()
            .connect()
            .await
            .unwrap();
        let request = ScanLatestTableRequest {
            tenant,
            namespace,
            table_name,
            projection: Some(vec!["execution_id".to_string()]),
            predicate: Some(ArrowEqualityPredicate {
                column: "attempt".to_string(),
                value: ArrowScalarValue::Int64(2),
            }),
        };
        let ticket = ScanFlightTicket::new(request);

        let missing_error = FlightClient::new(channel.clone())
            .get_flight_info(ticket.command_descriptor().unwrap())
            .await
            .unwrap_err();
        match missing_error {
            arrow_flight::error::FlightError::Tonic(status) => {
                assert_eq!(status.code(), tonic::Code::Unauthenticated);
            }
            error => panic!("expected tonic unauthenticated error, got {error:?}"),
        }

        let mut client = FlightClient::new(channel);
        client.add_header("x-ehdb-auth", "local-secret").unwrap();
        let info = client
            .get_flight_info(ticket.command_descriptor().unwrap())
            .await
            .unwrap();
        let endpoint_ticket = info.endpoint[0].ticket.clone().unwrap();
        let batches = client
            .do_get(endpoint_ticket)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(info.total_records, 1);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);

        drop(client);
        shutdown_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), server_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        fs::remove_file(log_path).unwrap();
        fs::remove_dir_all(object_root).unwrap();
    }

    #[tokio::test]
    async fn flight_client_uses_scan_scope_metadata_over_loopback_listener() {
        let (log_path, object_root, runtime, store, tenant, namespace, table_name) =
            seeded_table("local-flight-client-scope-smoke");
        let config = LocalArrowFlightServerConfig {
            scan_scope_policy: FlightScanScopePolicy::require_default_tenant_namespace(),
            ..LocalArrowFlightServerConfig::default()
        };
        let listener = config
            .bind_loopback_listener(Arc::new(runtime), Arc::new(store))
            .await
            .unwrap();
        let endpoint = format!("http://{}", listener.local_addr());
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server_task = tokio::spawn(listener.serve_with_shutdown(async move {
            let _ = shutdown_rx.await;
        }));
        let channel = Channel::from_shared(endpoint)
            .unwrap()
            .connect()
            .await
            .unwrap();
        let request = ScanLatestTableRequest {
            tenant,
            namespace,
            table_name,
            projection: Some(vec!["execution_id".to_string()]),
            predicate: Some(ArrowEqualityPredicate {
                column: "attempt".to_string(),
                value: ArrowScalarValue::Int64(2),
            }),
        };
        let ticket = ScanFlightTicket::new(request);

        let missing_error = FlightClient::new(channel.clone())
            .get_flight_info(ticket.command_descriptor().unwrap())
            .await
            .unwrap_err();
        match missing_error {
            arrow_flight::error::FlightError::Tonic(status) => {
                assert_eq!(status.code(), tonic::Code::Unauthenticated);
            }
            error => panic!("expected tonic unauthenticated error, got {error:?}"),
        }

        let mut wrong_client = FlightClient::new(channel.clone());
        wrong_client
            .add_header(DEFAULT_FLIGHT_TENANT_SCOPE_HEADER, "tenant-b")
            .unwrap();
        wrong_client
            .add_header(DEFAULT_FLIGHT_NAMESPACE_SCOPE_HEADER, "system")
            .unwrap();
        let wrong_error = wrong_client
            .get_flight_info(ticket.command_descriptor().unwrap())
            .await
            .unwrap_err();
        match wrong_error {
            arrow_flight::error::FlightError::Tonic(status) => {
                assert_eq!(status.code(), tonic::Code::PermissionDenied);
            }
            error => panic!("expected tonic permission denied error, got {error:?}"),
        }

        let mut client = FlightClient::new(channel);
        client
            .add_header(DEFAULT_FLIGHT_TENANT_SCOPE_HEADER, "tenant-a")
            .unwrap();
        client
            .add_header(DEFAULT_FLIGHT_NAMESPACE_SCOPE_HEADER, "system")
            .unwrap();
        let info = client
            .get_flight_info(ticket.command_descriptor().unwrap())
            .await
            .unwrap();
        let endpoint_ticket = info.endpoint[0].ticket.clone().unwrap();
        let batches = client
            .do_get(endpoint_ticket)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(info.total_records, 1);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);

        drop(client);
        shutdown_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), server_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        fs::remove_file(log_path).unwrap();
        fs::remove_dir_all(object_root).unwrap();
    }

    #[tokio::test]
    async fn flight_client_uses_catalog_scan_grants_over_loopback_listener() {
        let (log_path, object_root, mut runtime, store, tenant, namespace, table_name) =
            seeded_table("local-flight-client-grant-smoke");
        grant_scan(&mut runtime, "worker-system", "txn-grant-scan").unwrap();
        let config = LocalArrowFlightServerConfig {
            scan_grant_policy: FlightScanGrantPolicy::require_default_principal(),
            ..LocalArrowFlightServerConfig::default()
        };
        let listener = config
            .bind_loopback_listener(Arc::new(runtime), Arc::new(store))
            .await
            .unwrap();
        let endpoint = format!("http://{}", listener.local_addr());
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server_task = tokio::spawn(listener.serve_with_shutdown(async move {
            let _ = shutdown_rx.await;
        }));
        let channel = Channel::from_shared(endpoint)
            .unwrap()
            .connect()
            .await
            .unwrap();
        let request = ScanLatestTableRequest {
            tenant,
            namespace,
            table_name,
            projection: Some(vec!["execution_id".to_string()]),
            predicate: Some(ArrowEqualityPredicate {
                column: "attempt".to_string(),
                value: ArrowScalarValue::Int64(2),
            }),
        };
        let ticket = ScanFlightTicket::new(request);

        let missing_error = FlightClient::new(channel.clone())
            .get_flight_info(ticket.command_descriptor().unwrap())
            .await
            .unwrap_err();
        match missing_error {
            arrow_flight::error::FlightError::Tonic(status) => {
                assert_eq!(status.code(), tonic::Code::Unauthenticated);
            }
            error => panic!("expected tonic unauthenticated error, got {error:?}"),
        }

        let mut wrong_client = FlightClient::new(channel.clone());
        wrong_client
            .add_header(DEFAULT_FLIGHT_PRINCIPAL_HEADER, "worker-other")
            .unwrap();
        let wrong_error = wrong_client
            .get_flight_info(ticket.command_descriptor().unwrap())
            .await
            .unwrap_err();
        match wrong_error {
            arrow_flight::error::FlightError::Tonic(status) => {
                assert_eq!(status.code(), tonic::Code::PermissionDenied);
            }
            error => panic!("expected tonic permission denied error, got {error:?}"),
        }

        let mut client = FlightClient::new(channel);
        client
            .add_header(DEFAULT_FLIGHT_PRINCIPAL_HEADER, "worker-system")
            .unwrap();
        let info = client
            .get_flight_info(ticket.command_descriptor().unwrap())
            .await
            .unwrap();
        let endpoint_ticket = info.endpoint[0].ticket.clone().unwrap();
        let batches = client
            .do_get(endpoint_ticket)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(info.total_records, 1);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);

        drop(client);
        shutdown_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), server_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        fs::remove_file(log_path).unwrap();
        fs::remove_dir_all(object_root).unwrap();
    }

    #[tokio::test]
    async fn flight_listener_rejects_non_loopback_even_with_external_auth_policy() {
        let log_path = temp_log_path("local-flight-listener-non-loopback");
        let object_root = temp_object_root("local-flight-listener-non-loopback");
        let runtime = LocalReferenceRuntime::open(&log_path).unwrap();
        let store = LocalObjectStore::new(&object_root);
        let config = LocalArrowFlightServerConfig {
            bind_addr: "0.0.0.0:0".parse().unwrap(),
            auth_policy: FlightAuthPolicy::ExternalRequired,
            ..LocalArrowFlightServerConfig::default()
        };

        let error = config
            .bind_loopback_listener(Arc::new(runtime), Arc::new(store))
            .await
            .unwrap_err();

        assert!(matches!(error, EhdbError::InvalidState(_)));
        assert!(!log_path.exists());
        if object_root.exists() {
            fs::remove_dir_all(object_root).unwrap();
        }
    }

    #[test]
    fn flight_server_config_defaults_to_bounded_loopback_reference() {
        let config = LocalArrowFlightServerConfig::default();

        config.validate().unwrap();
        assert!(config.bind_addr.ip().is_loopback());
        assert_eq!(
            config.max_decoding_message_size,
            DEFAULT_FLIGHT_MAX_MESSAGE_SIZE
        );
        assert_eq!(
            config.max_encoding_message_size,
            DEFAULT_FLIGHT_MAX_MESSAGE_SIZE
        );
        assert_eq!(
            config.max_concurrent_requests,
            DEFAULT_FLIGHT_MAX_CONCURRENT_REQUESTS
        );
        assert_eq!(
            config.auth_policy,
            FlightAuthPolicy::DisabledForLocalReference
        );
        assert_eq!(
            config.scan_scope_policy,
            FlightScanScopePolicy::DisabledForLocalReference
        );
        assert_eq!(
            config.scan_grant_policy,
            FlightScanGrantPolicy::DisabledForLocalReference
        );
        assert_eq!(config.access_log_policy, FlightAccessLogPolicy::DebugOnly);
    }

    #[test]
    fn flight_server_config_rejects_unbounded_values() {
        let zero_decode = LocalArrowFlightServerConfig {
            max_decoding_message_size: 0,
            ..LocalArrowFlightServerConfig::default()
        };
        assert!(matches!(
            zero_decode.validate(),
            Err(EhdbError::InvalidState(_))
        ));

        let zero_encode = LocalArrowFlightServerConfig {
            max_encoding_message_size: 0,
            ..LocalArrowFlightServerConfig::default()
        };
        assert!(matches!(
            zero_encode.validate(),
            Err(EhdbError::InvalidState(_))
        ));

        let zero_concurrency = LocalArrowFlightServerConfig {
            max_concurrent_requests: 0,
            ..LocalArrowFlightServerConfig::default()
        };
        assert!(matches!(
            zero_concurrency.validate(),
            Err(EhdbError::InvalidState(_))
        ));
    }

    #[test]
    fn flight_server_config_requires_auth_for_non_loopback_binds() {
        let mut config = LocalArrowFlightServerConfig {
            bind_addr: "0.0.0.0:32010".parse().unwrap(),
            ..LocalArrowFlightServerConfig::default()
        };

        assert!(matches!(config.validate(), Err(EhdbError::InvalidState(_))));

        config.auth_policy = FlightAuthPolicy::ExternalRequired;
        config.validate().unwrap();
    }

    #[test]
    fn flight_access_log_policy_builds_bounded_debug_entries() {
        let request = filtered_request();
        let entry = FlightAccessLogPolicy::DebugOnly
            .scan_access_entry(FlightScanAccessLogInput {
                call: FlightScanCall::GetFlightInfo,
                request: &request,
                grpc_code: Code::Ok,
                row_count: Some(1),
                flight_data_message_count: None,
                auth_policy: &FlightAuthPolicy::HeaderToken {
                    header_name: "x-ehdb-auth".to_string(),
                    token: "local-secret".to_string(),
                },
                scan_scope_policy: &FlightScanScopePolicy::require_default_tenant_namespace(),
                scan_grant_policy: &FlightScanGrantPolicy::require_default_principal(),
            })
            .unwrap();

        assert_eq!(entry.call, FlightScanCall::GetFlightInfo);
        assert_eq!(entry.call.as_str(), "get_flight_info");
        assert_eq!(entry.grpc_code, Code::Ok);
        assert_eq!(entry.row_count, Some(1));
        assert_eq!(entry.flight_data_message_count, None);
        assert_eq!(entry.projection_count, Some(1));
        assert!(entry.predicate_present);
        assert!(entry.auth_required);
        assert!(entry.scan_scope_required);
        assert!(entry.scan_grant_required);

        let rendered = format!("{entry:?}");
        assert!(!rendered.contains(request.tenant.as_str()));
        assert!(!rendered.contains(request.namespace.as_str()));
        assert!(!rendered.contains(request.table_name.as_str()));
        assert!(!rendered.contains("local-secret"));
        assert!(!rendered.contains(DEFAULT_FLIGHT_PRINCIPAL_HEADER));
    }

    #[test]
    fn flight_access_log_policy_disabled_emits_no_entries() {
        let request = filtered_request();
        assert!(FlightAccessLogPolicy::Disabled
            .scan_access_entry(FlightScanAccessLogInput {
                call: FlightScanCall::DoGet,
                request: &request,
                grpc_code: Code::PermissionDenied,
                row_count: None,
                flight_data_message_count: Some(2),
                auth_policy: &FlightAuthPolicy::DisabledForLocalReference,
                scan_scope_policy: &FlightScanScopePolicy::DisabledForLocalReference,
                scan_grant_policy: &FlightScanGrantPolicy::DisabledForLocalReference,
            })
            .is_none());
    }

    #[test]
    fn local_flight_server_accepts_disabled_access_log_policy() {
        let log_path = temp_log_path("local-flight-server-disabled-access-log");
        let object_root = temp_object_root("local-flight-server-disabled-access-log");
        let runtime = LocalReferenceRuntime::open(&log_path).unwrap();
        let store = LocalObjectStore::new(&object_root);
        let server = LocalArrowFlightServer::new_with_runtime_policies(
            Arc::new(runtime),
            Arc::new(store),
            FlightAuthPolicy::DisabledForLocalReference,
            FlightScanScopePolicy::DisabledForLocalReference,
            FlightScanGrantPolicy::DisabledForLocalReference,
            FlightAccessLogPolicy::Disabled,
        );

        assert_eq!(server.access_log_policy, FlightAccessLogPolicy::Disabled);

        assert!(!log_path.exists());
        if object_root.exists() {
            fs::remove_dir_all(object_root).unwrap();
        }
    }

    #[test]
    fn flight_header_token_auth_policy_validates_metadata() {
        let policy = FlightAuthPolicy::HeaderToken {
            header_name: "x-ehdb-auth".to_string(),
            token: "local-secret".to_string(),
        };
        let mut metadata = MetadataMap::new();
        metadata.insert("x-ehdb-auth", "local-secret".parse().unwrap());

        policy.validate().unwrap();
        assert!(policy.authorize_metadata(&metadata).is_none());

        let missing = FlightAuthPolicy::HeaderToken {
            header_name: "x-ehdb-auth".to_string(),
            token: "local-secret".to_string(),
        }
        .authorize_metadata(&MetadataMap::new())
        .unwrap();
        assert_eq!(missing.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn flight_header_token_auth_policy_rejects_invalid_contracts() {
        let empty_header = FlightAuthPolicy::HeaderToken {
            header_name: String::new(),
            token: "local-secret".to_string(),
        };
        assert!(matches!(
            empty_header.validate(),
            Err(EhdbError::InvalidState(_))
        ));

        let binary_header = FlightAuthPolicy::HeaderToken {
            header_name: "x-ehdb-auth-bin".to_string(),
            token: "local-secret".to_string(),
        };
        assert!(matches!(
            binary_header.validate(),
            Err(EhdbError::InvalidState(_))
        ));

        let empty_token = FlightAuthPolicy::HeaderToken {
            header_name: "x-ehdb-auth".to_string(),
            token: String::new(),
        };
        assert!(matches!(
            empty_token.validate(),
            Err(EhdbError::InvalidState(_))
        ));

        let control_token = FlightAuthPolicy::HeaderToken {
            header_name: "x-ehdb-auth".to_string(),
            token: "bad\nsecret".to_string(),
        };
        assert!(matches!(
            control_token.validate(),
            Err(EhdbError::InvalidState(_))
        ));
    }

    #[test]
    fn flight_scan_scope_policy_validates_metadata() {
        let policy = FlightScanScopePolicy::require_default_tenant_namespace();
        let request = filtered_request();
        let mut metadata = MetadataMap::new();
        metadata.insert(
            DEFAULT_FLIGHT_TENANT_SCOPE_HEADER,
            "tenant-a".parse().unwrap(),
        );
        metadata.insert(
            DEFAULT_FLIGHT_NAMESPACE_SCOPE_HEADER,
            "system".parse().unwrap(),
        );

        policy.validate().unwrap();
        assert!(policy
            .authorize_scan_metadata(&metadata, &request)
            .is_none());

        let missing = policy
            .authorize_scan_metadata(&MetadataMap::new(), &request)
            .unwrap();
        assert_eq!(missing.code(), tonic::Code::Unauthenticated);

        let mut wrong_namespace = MetadataMap::new();
        wrong_namespace.insert(
            DEFAULT_FLIGHT_TENANT_SCOPE_HEADER,
            "tenant-a".parse().unwrap(),
        );
        wrong_namespace.insert(
            DEFAULT_FLIGHT_NAMESPACE_SCOPE_HEADER,
            "analytics".parse().unwrap(),
        );
        let wrong = policy
            .authorize_scan_metadata(&wrong_namespace, &request)
            .unwrap();
        assert_eq!(wrong.code(), tonic::Code::PermissionDenied);
    }

    #[test]
    fn flight_scan_scope_policy_rejects_invalid_contracts() {
        let empty_tenant_header = FlightScanScopePolicy::RequireTenantNamespace {
            tenant_header_name: String::new(),
            namespace_header_name: DEFAULT_FLIGHT_NAMESPACE_SCOPE_HEADER.to_string(),
        };
        assert!(matches!(
            empty_tenant_header.validate(),
            Err(EhdbError::InvalidState(_))
        ));

        let binary_namespace_header = FlightScanScopePolicy::RequireTenantNamespace {
            tenant_header_name: DEFAULT_FLIGHT_TENANT_SCOPE_HEADER.to_string(),
            namespace_header_name: "x-ehdb-namespace-bin".to_string(),
        };
        assert!(matches!(
            binary_namespace_header.validate(),
            Err(EhdbError::InvalidState(_))
        ));

        let duplicate_headers = FlightScanScopePolicy::RequireTenantNamespace {
            tenant_header_name: DEFAULT_FLIGHT_TENANT_SCOPE_HEADER.to_string(),
            namespace_header_name: DEFAULT_FLIGHT_TENANT_SCOPE_HEADER.to_string(),
        };
        assert!(matches!(
            duplicate_headers.validate(),
            Err(EhdbError::InvalidState(_))
        ));
    }

    #[test]
    fn flight_scan_grant_policy_validates_metadata() {
        let (log_path, object_root, mut runtime, store, tenant, namespace, table_name) =
            seeded_table("local-flight-grant-policy");
        grant_scan(&mut runtime, "worker-system", "txn-grant-scan").unwrap();
        let policy = FlightScanGrantPolicy::require_default_principal();
        let request = ScanLatestTableRequest {
            tenant,
            namespace,
            table_name,
            projection: None,
            predicate: None,
        };
        let mut metadata = MetadataMap::new();
        metadata.insert(
            DEFAULT_FLIGHT_PRINCIPAL_HEADER,
            "worker-system".parse().unwrap(),
        );

        policy.validate().unwrap();
        assert!(policy
            .authorize_catalog_grant(&metadata, &runtime, &request)
            .is_none());

        let missing = policy
            .authorize_catalog_grant(&MetadataMap::new(), &runtime, &request)
            .unwrap();
        assert_eq!(missing.code(), tonic::Code::Unauthenticated);

        let mut wrong = MetadataMap::new();
        wrong.insert(
            DEFAULT_FLIGHT_PRINCIPAL_HEADER,
            "worker-other".parse().unwrap(),
        );
        let denied = policy
            .authorize_catalog_grant(&wrong, &runtime, &request)
            .unwrap();
        assert_eq!(denied.code(), tonic::Code::PermissionDenied);

        drop(store);
        fs::remove_file(log_path).unwrap();
        fs::remove_dir_all(object_root).unwrap();
    }

    #[test]
    fn flight_scan_grant_policy_rejects_invalid_contracts() {
        let empty_header = FlightScanGrantPolicy::RequireCatalogGrant {
            principal_header_name: String::new(),
        };
        assert!(matches!(
            empty_header.validate(),
            Err(EhdbError::InvalidState(_))
        ));

        let binary_header = FlightScanGrantPolicy::RequireCatalogGrant {
            principal_header_name: "x-ehdb-principal-bin".to_string(),
        };
        assert!(matches!(
            binary_header.validate(),
            Err(EhdbError::InvalidState(_))
        ));
    }

    #[test]
    fn flight_server_config_builds_generated_service_without_binding_listener() {
        let log_path = temp_log_path("local-flight-server-config-build");
        let object_root = temp_object_root("local-flight-server-config-build");
        let runtime = LocalReferenceRuntime::open(&log_path).unwrap();
        let store = LocalObjectStore::new(&object_root);
        let config = LocalArrowFlightServerConfig {
            max_decoding_message_size: 1024 * 1024,
            max_encoding_message_size: 2 * 1024 * 1024,
            max_concurrent_requests: 8,
            ..LocalArrowFlightServerConfig::default()
        };

        let _server = config
            .build_service(Arc::new(runtime), Arc::new(store))
            .unwrap();

        assert!(!log_path.exists());
        if object_root.exists() {
            fs::remove_dir_all(object_root).unwrap();
        }
    }

    fn grant_scan(
        runtime: &mut LocalReferenceRuntime,
        principal: &str,
        transaction_id: &str,
    ) -> Result<()> {
        runtime
            .append(CommitTransaction {
                transaction_id: TransactionId::new(transaction_id)?,
                tenant: TenantId::new("tenant-a")?,
                namespace: NamespaceName::new("system")?,
                mutations: vec![Mutation::Catalog(CatalogMutation::GrantScan {
                    table_id: TableId::new("tenant-a_system_executions")?,
                    principal: PrincipalId::new(principal)?,
                })],
            })
            .map(|_| ())
    }

    fn seed_retrieval_vectors(runtime: &mut LocalReferenceRuntime) -> Result<()> {
        runtime
            .append(CommitTransaction {
                transaction_id: TransactionId::new("txn-retrieval-tenant-a")?,
                tenant: TenantId::new("tenant-a")?,
                namespace: NamespaceName::new("knowledge")?,
                mutations: vec![
                    Mutation::Retrieval(RetrievalMutation::RegisterDocument {
                        document_id: DocumentId::new("doc-a")?,
                        source_uri: "artifact://tenant-a/doc-a.md".to_string(),
                        content_type: "text/markdown".to_string(),
                    }),
                    Mutation::Retrieval(RetrievalMutation::RegisterChunk {
                        document_id: DocumentId::new("doc-a")?,
                        chunk_id: ChunkId::new("chunk-close")?,
                        ordinal: 0,
                        text: "close local retrieval hit".to_string(),
                        checksum: "sha256-close".to_string(),
                    }),
                    Mutation::Retrieval(RetrievalMutation::RegisterChunk {
                        document_id: DocumentId::new("doc-a")?,
                        chunk_id: ChunkId::new("chunk-farther")?,
                        ordinal: 1,
                        text: "farther local retrieval hit".to_string(),
                        checksum: "sha256-farther".to_string(),
                    }),
                    Mutation::Retrieval(RetrievalMutation::RegisterEmbedding {
                        chunk_id: ChunkId::new("chunk-close")?,
                        model_id: EmbeddingModelId::new("text-embedding-local")?,
                        dimensions: 2,
                        vector: vec![1.0, 0.0],
                    }),
                    Mutation::Retrieval(RetrievalMutation::RegisterEmbedding {
                        chunk_id: ChunkId::new("chunk-farther")?,
                        model_id: EmbeddingModelId::new("text-embedding-local")?,
                        dimensions: 2,
                        vector: vec![0.5, 0.5],
                    }),
                    Mutation::Retrieval(RetrievalMutation::RegisterEmbedding {
                        chunk_id: ChunkId::new("chunk-farther")?,
                        model_id: EmbeddingModelId::new("text-embedding-other")?,
                        dimensions: 2,
                        vector: vec![1.0, 0.0],
                    }),
                ],
            })
            .map(|_| ())?;

        runtime
            .append(CommitTransaction {
                transaction_id: TransactionId::new("txn-retrieval-tenant-b")?,
                tenant: TenantId::new("tenant-b")?,
                namespace: NamespaceName::new("knowledge")?,
                mutations: vec![
                    Mutation::Retrieval(RetrievalMutation::RegisterDocument {
                        document_id: DocumentId::new("doc-b")?,
                        source_uri: "artifact://tenant-b/doc-b.md".to_string(),
                        content_type: "text/markdown".to_string(),
                    }),
                    Mutation::Retrieval(RetrievalMutation::RegisterChunk {
                        document_id: DocumentId::new("doc-b")?,
                        chunk_id: ChunkId::new("chunk-other-tenant")?,
                        ordinal: 0,
                        text: "other tenant retrieval hit".to_string(),
                        checksum: "sha256-other-tenant".to_string(),
                    }),
                    Mutation::Retrieval(RetrievalMutation::RegisterEmbedding {
                        chunk_id: ChunkId::new("chunk-other-tenant")?,
                        model_id: EmbeddingModelId::new("text-embedding-local")?,
                        dimensions: 2,
                        vector: vec![1.0, 0.0],
                    }),
                ],
            })
            .map(|_| ())
    }

    fn seeded_table(
        name: &str,
    ) -> (
        std::path::PathBuf,
        std::path::PathBuf,
        LocalReferenceRuntime,
        LocalObjectStore,
        TenantId,
        NamespaceName,
        TableName,
    ) {
        let log_path = temp_log_path(name);
        let object_root = temp_object_root(name);
        let tenant = TenantId::new("tenant-a").unwrap();
        let namespace = NamespaceName::new("system").unwrap();
        let table_name = TableName::new("executions").unwrap();
        let store = LocalObjectStore::new(&object_root);
        let mut runtime = LocalReferenceRuntime::open(&log_path).unwrap();

        LocalArrowIpcTableStore
            .write_batch(
                &mut runtime,
                &store,
                WriteArrowIpcTable {
                    tenant: tenant.clone(),
                    namespace: namespace.clone(),
                    table_name: table_name.clone(),
                    snapshot_id: SnapshotId::new("snapshot-0001").unwrap(),
                    create_transaction_id: TransactionId::new("txn-create-table").unwrap(),
                    snapshot_transaction_id: TransactionId::new("txn-commit-snapshot").unwrap(),
                    file_name: "part-000.arrow".to_string(),
                    batch: arrow_batch(),
                },
            )
            .unwrap();

        (
            log_path,
            object_root,
            runtime,
            store,
            tenant,
            namespace,
            table_name,
        )
    }

    fn filtered_request() -> ScanLatestTableRequest {
        ScanLatestTableRequest {
            tenant: TenantId::new("tenant-a").unwrap(),
            namespace: NamespaceName::new("system").unwrap(),
            table_name: TableName::new("executions").unwrap(),
            projection: Some(vec!["execution_id".to_string()]),
            predicate: Some(ArrowEqualityPredicate {
                column: "attempt".to_string(),
                value: ArrowScalarValue::Int64(2),
            }),
        }
    }

    fn temp_log_path(name: &str) -> std::path::PathBuf {
        let suffix = unique_suffix();
        std::env::temp_dir().join(format!("ehdb-service-{name}-{suffix}.jsonl"))
    }

    fn temp_object_root(name: &str) -> std::path::PathBuf {
        let suffix = unique_suffix();
        std::env::temp_dir().join(format!("ehdb-service-objects-{name}-{suffix}"))
    }

    fn unique_suffix() -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        format!("{}-{nanos}-{counter}", std::process::id())
    }

    fn arrow_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("execution_id", DataType::Utf8, false),
            Field::new("attempt", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["exec-1", "exec-2", "exec-3"])),
                Arc::new(Int64Array::from(vec![1, 2, 3])),
            ],
        )
        .unwrap()
    }
}
