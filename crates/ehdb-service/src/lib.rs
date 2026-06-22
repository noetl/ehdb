use std::sync::Arc;

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
use ehdb_core::{EhdbError, NamespaceName, Result, TableName, TenantId};
use ehdb_reference::{
    ArrowEqualityPredicate, LocalArrowSnapshotScanner, LocalReferenceRuntime, ScanArrowSnapshot,
};
use ehdb_storage::ImmutableObjectStore;
use futures_util::stream::{self, BoxStream, StreamExt};
use serde::{Deserialize, Serialize};
use tonic::{Request, Response, Status, Streaming};

pub const SCAN_FLIGHT_TICKET_VERSION: &str = "ehdb.arrow.scan.v1";

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
}

impl<S> LocalArrowFlightServer<S>
where
    S: ImmutableObjectStore + Send + Sync + 'static,
{
    pub fn new(runtime: Arc<LocalReferenceRuntime>, store: Arc<S>) -> Self {
        Self {
            runtime,
            store,
            service: LocalArrowFlightService::default(),
        }
    }

    pub fn into_server(self) -> FlightServiceServer<Self> {
        FlightServiceServer::new(self)
    }

    fn request_from_descriptor(descriptor: FlightDescriptor) -> Result<ScanLatestTableRequest> {
        if descriptor.r#type != DescriptorType::Cmd as i32 {
            return Err(EhdbError::InvalidState(
                "EHDB scan get_flight_info requires a command descriptor".to_string(),
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
        let scan_request =
            Self::request_from_descriptor(request.into_inner()).map_err(error_to_status)?;
        let info = self
            .service
            .get_flight_info(&self.runtime, self.store.as_ref(), scan_request)
            .map_err(error_to_status)?;
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
        _request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented(
            "EHDB Flight get_schema is not implemented",
        ))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> std::result::Result<Response<Self::DoGetStream>, Status> {
        let data = self
            .service
            .do_get(&self.runtime, self.store.as_ref(), request.get_ref())
            .map_err(error_to_status)?;
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
        time::{SystemTime, UNIX_EPOCH},
    };

    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use ehdb_core::{EhdbError, NamespaceName, SnapshotId, TableName, TenantId, TransactionId};
    use ehdb_reference::{
        ArrowScalarValue, LocalArrowIpcTableStore, LocalReferenceRuntime, WriteArrowIpcTable,
    };
    use ehdb_storage::LocalObjectStore;
    use futures_util::TryStreamExt;

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
    fn local_flight_service_returns_info_and_do_get_stream() {
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

        let info = service.get_flight_info(&runtime, &store, request).unwrap();
        let endpoint_ticket = info.endpoint[0].ticket.as_ref().unwrap();
        let flight_data = service.do_get(&runtime, &store, endpoint_ticket).unwrap();
        let decoded = ArrowScanResult::from_flight_data(&flight_data).unwrap();

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
    async fn local_flight_server_get_flight_info_and_do_get_stream() {
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

        let error = LocalArrowFlightServer::new(Arc::new(runtime), Arc::new(store))
            .get_flight_info(Request::new(descriptor))
            .await
            .unwrap_err();

        assert_eq!(error.code(), tonic::Code::InvalidArgument);
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
            .get_schema(Request::new(FlightDescriptor::default()))
            .await
            .unwrap_err();

        assert_eq!(error.code(), tonic::Code::Unimplemented);
        assert!(!log_path.exists());
        if object_root.exists() {
            fs::remove_dir_all(object_root).unwrap();
        }
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
