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
use ehdb_core::{EhdbError, NamespaceName, PrincipalId, Result, TableName, TenantId};
use ehdb_reference::{
    ArrowEqualityPredicate, LocalArrowSnapshotScanner, LocalReferenceRuntime, ScanArrowSnapshot,
};
use ehdb_storage::ImmutableObjectStore;
use futures_util::stream::{self, BoxStream, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tonic::metadata::{AsciiMetadataKey, MetadataMap};
use tonic::transport::{server::TcpIncoming, Server};
use tonic::{Code, Request, Response, Status, Streaming};

pub const SCAN_FLIGHT_TICKET_VERSION: &str = "ehdb.arrow.scan.v1";
pub const DEFAULT_FLIGHT_MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;
pub const DEFAULT_FLIGHT_MAX_CONCURRENT_REQUESTS: usize = 64;
pub const DEFAULT_FLIGHT_TENANT_SCOPE_HEADER: &str = "x-ehdb-tenant";
pub const DEFAULT_FLIGHT_NAMESPACE_SCOPE_HEADER: &str = "x-ehdb-namespace";
pub const DEFAULT_FLIGHT_PRINCIPAL_HEADER: &str = "x-ehdb-principal";

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
        Ok(LocalArrowFlightServer::new_with_runtime_policies(
            runtime,
            store,
            self.auth_policy.clone(),
            self.scan_scope_policy.clone(),
            self.scan_grant_policy.clone(),
            self.access_log_policy.clone(),
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
        Self {
            runtime,
            store,
            service: LocalArrowFlightService::default(),
            auth_policy,
            scan_scope_policy,
            scan_grant_policy,
            access_log_policy,
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
        EhdbError, NamespaceName, PrincipalId, SnapshotId, TableId, TableName, TenantId,
        TransactionId,
    };
    use ehdb_reference::{
        ArrowScalarValue, LocalArrowIpcTableStore, LocalReferenceRuntime, WriteArrowIpcTable,
    };
    use ehdb_storage::LocalObjectStore;
    use ehdb_transaction::{CatalogMutation, CommitTransaction, Mutation};
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
