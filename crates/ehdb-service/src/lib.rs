use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_flight::{
    flight_descriptor::DescriptorType,
    utils::{batches_to_flight_data, flight_data_to_batches},
    FlightData, FlightDescriptor, Ticket,
};
use arrow_schema::Schema;
use ehdb_core::{EhdbError, NamespaceName, Result, TableName, TenantId};
use ehdb_reference::{
    ArrowEqualityPredicate, LocalArrowSnapshotScanner, LocalReferenceRuntime, ScanArrowSnapshot,
};
use ehdb_storage::ImmutableObjectStore;
use serde::{Deserialize, Serialize};

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
