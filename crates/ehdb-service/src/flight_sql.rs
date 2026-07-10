//! Flight SQL projection-tier read surface — the external EHDB driver MVP.
//!
//! This module exposes NoETL's **projection read-model** tier (the
//! execution-state view) to applications *outside* the platform over the
//! standard **Arrow Flight SQL** wire protocol. It is the first slice of the
//! [external EHDB driver RFC](https://github.com/noetl/ehdb/wiki/RFC-External-EHDB-Driver).
//!
//! Load-bearing properties, all structural (not post-hoc):
//!
//! - **Read-only.** The surface reaches the projection tier only through the
//!   [`ProjectionReadModel`] seam, whose sole method is a bounded read. The
//!   write half of [`ProjectionDriver`] is unreachable from here.
//! - **Committed / materialized only.** [`ProjectionReadModel::list_executions`]
//!   returns the folded, materialized execution-state read-model — never
//!   in-flight or dirty state.
//! - **Secret-free.** The Arrow schema is a fixed set of projected,
//!   non-payload columns ([`executions_schema`]). `result` / `error` /
//!   `context` / `workload` bodies are never selectable.
//! - **Bounded.** Every query is clamped to [`MAX_LIMIT`] rows server-side.
//! - **Scoped-token auth.** Token verification is delegated to a
//!   [`ReadTokenVerifier`] the deploying role supplies (resolved from the
//!   keychain / secret manager) — no token value lives in this crate.
//!
//! The projection read is served **on top of** the #178 worker-side read
//! contract: [`ProjectionDriverReadModel`] adapts any [`ProjectionDriver`]
//! (the same trait the `/api/ehdb/tiers/{tier}` handler resolves) into the
//! read-only seam, so this surface reuses that read path rather than forking
//! it. When the #178 worker-side handler lands, the deploying role can supply
//! a [`ProjectionReadModel`] backed by that handler with no change here.

// gRPC handlers return `Result<_, tonic::Status>`; `Status` is ~176 bytes, so
// `result_large_err` fires on every Flight method. Boxing the standard gRPC
// error type would fight the tonic API for no benefit — allow it module-wide.
#![allow(clippy::result_large_err)]

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use arrow_array::{BooleanArray, Int64Array, RecordBatch, StringArray, UInt64Array};
use arrow_flight::{
    encode::FlightDataEncoderBuilder,
    sql::{
        server::FlightSqlService, CommandStatementQuery, ProstMessageExt, SqlInfo,
        TicketStatementQuery,
    },
    FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse, Ticket,
};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use ehdb_core::{EhdbError, Result};
use ehdb_reference::{ExecutionStateView, ProjectionDriver};
use futures_util::{stream, Stream, TryStreamExt};
use prost::Message;
use sqlparser::ast::{
    BinaryOperator, Expr, GroupByExpr, ObjectName, Query, Select, SelectItem, SetExpr, Statement,
    TableFactor, Value,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use tonic::{Request, Response, Status, Streaming};

/// The single virtual table the MVP exposes: the execution-state read-model.
pub const EXECUTIONS_TABLE: &str = "executions";

/// Default row cap when the query omits `LIMIT`.
pub const DEFAULT_LIMIT: usize = 100;

/// Hard server-side row cap. Every query is clamped to this, matching the
/// #178 read contract (`limit ≤ 1000`).
pub const MAX_LIMIT: usize = 1000;

// ---------------------------------------------------------------------------
// Read-only projection seam (the #178 read contract)
// ---------------------------------------------------------------------------

/// The read-only projection-tier seam the Flight SQL surface depends on.
///
/// This is deliberately narrower than [`ProjectionDriver`]: it exposes **only**
/// a bounded list read, so the external surface can never materialize / write
/// through it. The deploying role supplies an implementation — the MVP wraps a
/// [`ProjectionDriver`] via [`ProjectionDriverReadModel`]; once the #178
/// worker-side `tiers/{tier}` handler lands, a handler-backed implementation
/// slots in with no change to this module.
pub trait ProjectionReadModel: Send + Sync + 'static {
    /// Bounded, ordered list of execution-state read-model rows (committed /
    /// materialized only).
    fn list_executions(&self, limit: usize) -> Result<Vec<ExecutionStateView>>;
}

/// Adapts any [`ProjectionDriver`] into the read-only [`ProjectionReadModel`]
/// seam. Only the driver's read method is reachable through this adapter, so
/// the Flight SQL surface is read-only by construction.
pub struct ProjectionDriverReadModel<D>
where
    D: ProjectionDriver + Send + Sync + 'static,
{
    driver: D,
}

impl<D> ProjectionDriverReadModel<D>
where
    D: ProjectionDriver + Send + Sync + 'static,
{
    pub fn new(driver: D) -> Self {
        Self { driver }
    }
}

impl<D> ProjectionReadModel for ProjectionDriverReadModel<D>
where
    D: ProjectionDriver + Send + Sync + 'static,
{
    fn list_executions(&self, limit: usize) -> Result<Vec<ExecutionStateView>> {
        Ok(self.driver.list_executions(limit)?.states)
    }
}

// ---------------------------------------------------------------------------
// Scoped read-only token auth seam
// ---------------------------------------------------------------------------

/// Verifies a presented bearer token is a valid **scoped read-only** external
/// token. The implementation lives in the deploying role and resolves the
/// expected secret from the keychain / secret manager — no token value is ever
/// stored in this crate or surfaced in a response.
pub trait ReadTokenVerifier: Send + Sync + 'static {
    /// Return `true` iff the presented token grants read access.
    fn verify(&self, presented_token: &str) -> bool;
}

/// A verifier that accepts everything — valid **only** for loopback local
/// harnesses / tests, mirroring `FlightAuthPolicy::DisabledForLocalReference`.
/// Never use off loopback.
pub struct AllowAllForLocalReference;

impl ReadTokenVerifier for AllowAllForLocalReference {
    fn verify(&self, _presented_token: &str) -> bool {
        true
    }
}

// ---------------------------------------------------------------------------
// Arrow schema for the `executions` virtual table (secret-free columns)
// ---------------------------------------------------------------------------

/// The fixed, secret-free Arrow schema for the `executions` table. Every
/// column is a projected read-model field; no payload body is present.
pub fn executions_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("execution_id", DataType::Utf8, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("current_node", DataType::Utf8, true),
        Field::new("event_count", DataType::UInt64, false),
        Field::new("first_global_sequence", DataType::UInt64, false),
        Field::new("last_global_sequence", DataType::UInt64, false),
        Field::new("last_event_id", DataType::Int64, false),
        Field::new("terminal", DataType::Boolean, false),
        Field::new("terminal_event_type", DataType::Utf8, true),
    ]))
}

/// The ordered column names of [`executions_schema`].
fn executions_columns() -> Vec<String> {
    executions_schema()
        .fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect()
}

// ---------------------------------------------------------------------------
// Bounded read-only SQL → projection plan
// ---------------------------------------------------------------------------

/// A validated, bounded read query over the `executions` table. Produced by
/// [`plan_projection_sql`]; the only shape the MVP accepts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionQuery {
    /// Resolved output columns, in order (from `*` or an explicit list). Every
    /// name is a column of [`executions_schema`].
    pub columns: Vec<String>,
    /// Optional `status = '…'` equality filter (case-insensitive compare).
    pub status_filter: Option<String>,
    /// Optional `execution_id = '…'` equality filter.
    pub execution_id_filter: Option<String>,
    /// Row cap, already clamped to [`MAX_LIMIT`].
    pub limit: usize,
}

fn ident_lower(name: &ObjectName) -> String {
    name.0
        .iter()
        .map(|i| i.value.to_ascii_lowercase())
        .collect::<Vec<_>>()
        .join(".")
}

/// Parse and validate a bounded read-only `SELECT` over the `executions`
/// table. Rejects everything else (writes, DDL, joins, subqueries, unknown
/// columns, non-equality filters) with a caller-facing reason string.
pub fn plan_projection_sql(sql: &str) -> std::result::Result<ProjectionQuery, String> {
    let statements = Parser::parse_sql(&GenericDialect {}, sql)
        .map_err(|err| format!("failed to parse SQL: {err}"))?;
    if statements.len() != 1 {
        return Err("exactly one statement is allowed".to_string());
    }
    let query = match &statements[0] {
        Statement::Query(query) => query.as_ref(),
        _ => return Err("only read-only SELECT statements are allowed".to_string()),
    };
    plan_query(query)
}

fn plan_query(query: &Query) -> std::result::Result<ProjectionQuery, String> {
    if query.with.is_some() {
        return Err("common table expressions (WITH) are not supported".to_string());
    }
    if query.order_by.is_some() {
        return Err(
            "ORDER BY is not supported (results are ordered by first_global_sequence)".to_string(),
        );
    }
    let select = match query.body.as_ref() {
        SetExpr::Select(select) => select.as_ref(),
        _ => return Err("only a plain SELECT body is supported".to_string()),
    };
    let columns = plan_projection_columns(select)?;
    let (status_filter, execution_id_filter) = plan_selection(select)?;
    let limit = plan_limit(query)?;
    Ok(ProjectionQuery {
        columns,
        status_filter,
        execution_id_filter,
        limit,
    })
}

fn plan_projection_columns(select: &Select) -> std::result::Result<Vec<String>, String> {
    if select.distinct.is_some() {
        return Err("SELECT DISTINCT is not supported".to_string());
    }
    match &select.group_by {
        GroupByExpr::Expressions(exprs, _) if exprs.is_empty() => {}
        _ => return Err("GROUP BY is not supported".to_string()),
    }
    if select.having.is_some() {
        return Err("HAVING is not supported".to_string());
    }
    // FROM must be exactly the `executions` table, no joins.
    if select.from.len() != 1 {
        return Err("exactly one table (executions) may be queried".to_string());
    }
    let twj = &select.from[0];
    if !twj.joins.is_empty() {
        return Err("joins are not supported".to_string());
    }
    match &twj.relation {
        TableFactor::Table { name, .. } => {
            let table = ident_lower(name);
            if table != EXECUTIONS_TABLE {
                return Err(format!(
                    "unknown table '{table}'; only '{EXECUTIONS_TABLE}' is available"
                ));
            }
        }
        _ => return Err("only a plain table reference is supported".to_string()),
    }

    let known = executions_columns();
    let mut resolved = Vec::new();
    for item in &select.projection {
        match item {
            SelectItem::Wildcard(_) => resolved.extend(known.iter().cloned()),
            SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                let col = ident.value.to_ascii_lowercase();
                if !known.contains(&col) {
                    return Err(format!("unknown column '{col}'"));
                }
                resolved.push(col);
            }
            _ => {
                return Err(
                    "only '*' or bare column names are supported in the select list".to_string(),
                )
            }
        }
    }
    if resolved.is_empty() {
        return Err("empty projection".to_string());
    }
    Ok(resolved)
}

fn plan_selection(
    select: &Select,
) -> std::result::Result<(Option<String>, Option<String>), String> {
    let mut status_filter = None;
    let mut execution_id_filter = None;
    if let Some(expr) = &select.selection {
        for eq in flatten_and(expr) {
            let (col, value) = parse_equality(eq)?;
            match col.as_str() {
                "status" => status_filter = Some(value),
                "execution_id" => execution_id_filter = Some(value),
                other => {
                    return Err(format!(
                        "WHERE only supports 'status' or 'execution_id' equality, not '{other}'"
                    ))
                }
            }
        }
    }
    Ok((status_filter, execution_id_filter))
}

/// Flatten an `AND` tree into its leaf predicates. A non-`AND` expression is a
/// single-element list.
fn flatten_and(expr: &Expr) -> Vec<&Expr> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            let mut out = flatten_and(left);
            out.extend(flatten_and(right));
            out
        }
        Expr::Nested(inner) => flatten_and(inner),
        other => vec![other],
    }
}

/// Parse a `<column> = '<string-literal>'` equality; the only WHERE shape
/// allowed.
fn parse_equality(expr: &Expr) -> std::result::Result<(String, String), String> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            let col = match left.as_ref() {
                Expr::Identifier(ident) => ident.value.to_ascii_lowercase(),
                _ => return Err("WHERE left-hand side must be a column name".to_string()),
            };
            let value = match right.as_ref() {
                Expr::Value(Value::SingleQuotedString(s)) => s.clone(),
                _ => return Err("WHERE right-hand side must be a string literal".to_string()),
            };
            Ok((col, value))
        }
        _ => Err("WHERE only supports equality predicates".to_string()),
    }
}

fn plan_limit(query: &Query) -> std::result::Result<usize, String> {
    match &query.limit {
        None => Ok(DEFAULT_LIMIT),
        Some(Expr::Value(Value::Number(n, _))) => {
            let requested: usize = n
                .parse()
                .map_err(|_| format!("invalid LIMIT value '{n}'"))?;
            Ok(requested.min(MAX_LIMIT))
        }
        Some(_) => Err("LIMIT must be an integer literal".to_string()),
    }
}

// ---------------------------------------------------------------------------
// Execute a plan → Arrow RecordBatch
// ---------------------------------------------------------------------------

/// Run a validated [`ProjectionQuery`] against the read model and return the
/// projected Arrow batch. Filters are applied over the bounded list read.
pub fn execute_projection_query(
    model: &dyn ProjectionReadModel,
    query: &ProjectionQuery,
) -> Result<RecordBatch> {
    let states = model.list_executions(query.limit.min(MAX_LIMIT))?;
    let filtered: Vec<&ExecutionStateView> = states
        .iter()
        .filter(|s| {
            query
                .status_filter
                .as_ref()
                .is_none_or(|want| s.status.eq_ignore_ascii_case(want))
                && query
                    .execution_id_filter
                    .as_ref()
                    .is_none_or(|want| &s.execution_id == want)
        })
        .take(query.limit)
        .collect();
    let full = full_executions_batch(&filtered)?;
    let indices = column_indices(&query.columns)?;
    full.project(&indices).map_err(arrow_to_ehdb)
}

fn arrow_to_ehdb(err: ArrowError) -> EhdbError {
    EhdbError::Storage(format!("arrow encode error: {err}"))
}

/// Build the full 9-column `executions` batch from the folded state rows.
fn full_executions_batch(states: &[&ExecutionStateView]) -> Result<RecordBatch> {
    let execution_id = StringArray::from(
        states
            .iter()
            .map(|s| s.execution_id.as_str())
            .collect::<Vec<_>>(),
    );
    let status = StringArray::from(states.iter().map(|s| s.status.as_str()).collect::<Vec<_>>());
    let current_node = StringArray::from(
        states
            .iter()
            .map(|s| s.current_node.as_deref())
            .collect::<Vec<Option<&str>>>(),
    );
    let event_count = UInt64Array::from(
        states
            .iter()
            .map(|s| s.event_count as u64)
            .collect::<Vec<_>>(),
    );
    let first_global_sequence = UInt64Array::from(
        states
            .iter()
            .map(|s| s.first_global_sequence)
            .collect::<Vec<_>>(),
    );
    let last_global_sequence = UInt64Array::from(
        states
            .iter()
            .map(|s| s.last_global_sequence)
            .collect::<Vec<_>>(),
    );
    let last_event_id =
        Int64Array::from(states.iter().map(|s| s.last_event_id).collect::<Vec<_>>());
    let terminal = BooleanArray::from(states.iter().map(|s| s.terminal).collect::<Vec<_>>());
    let terminal_event_type = StringArray::from(
        states
            .iter()
            .map(|s| s.terminal_event_type.as_deref())
            .collect::<Vec<Option<&str>>>(),
    );

    RecordBatch::try_new(
        executions_schema(),
        vec![
            Arc::new(execution_id),
            Arc::new(status),
            Arc::new(current_node),
            Arc::new(event_count),
            Arc::new(first_global_sequence),
            Arc::new(last_global_sequence),
            Arc::new(last_event_id),
            Arc::new(terminal),
            Arc::new(terminal_event_type),
        ],
    )
    .map_err(arrow_to_ehdb)
}

/// Map requested column names to their positions in [`executions_schema`].
fn column_indices(columns: &[String]) -> Result<Vec<usize>> {
    let known = executions_columns();
    columns
        .iter()
        .map(|c| {
            known
                .iter()
                .position(|k| k == c)
                .ok_or_else(|| EhdbError::InvalidState(format!("unknown column '{c}'")))
        })
        .collect()
}

/// The Arrow schema a planned query returns (its projected columns).
pub fn query_result_schema(query: &ProjectionQuery) -> Result<SchemaRef> {
    let indices = column_indices(&query.columns)?;
    let projected = executions_schema()
        .project(&indices)
        .map_err(arrow_to_ehdb)?;
    Ok(Arc::new(projected))
}

// ---------------------------------------------------------------------------
// Flight SQL service
// ---------------------------------------------------------------------------

/// The Flight SQL service serving the projection tier read-only.
pub struct FlightSqlProjectionService {
    model: Arc<dyn ProjectionReadModel>,
    verifier: Arc<dyn ReadTokenVerifier>,
    require_auth: bool,
}

impl FlightSqlProjectionService {
    /// Build a service that requires a valid scoped read token on every call
    /// (the external deployment shape).
    pub fn new(model: Arc<dyn ProjectionReadModel>, verifier: Arc<dyn ReadTokenVerifier>) -> Self {
        Self {
            model,
            verifier,
            require_auth: true,
        }
    }

    /// Build a service with auth disabled — valid **only** for loopback local
    /// harnesses / tests.
    pub fn new_local_reference(model: Arc<dyn ProjectionReadModel>) -> Self {
        Self {
            model,
            verifier: Arc::new(AllowAllForLocalReference),
            require_auth: false,
        }
    }

    fn authorize(
        &self,
        metadata: &tonic::metadata::MetadataMap,
    ) -> std::result::Result<(), Status> {
        if !self.require_auth {
            return Ok(());
        }
        let header = metadata
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| Status::unauthenticated("missing authorization header"))?;
        let token = header
            .strip_prefix("Bearer ")
            .or_else(|| header.strip_prefix("bearer "))
            .unwrap_or(header)
            .trim();
        if token.is_empty() || !self.verifier.verify(token) {
            return Err(Status::unauthenticated("invalid or unauthorized token"));
        }
        Ok(())
    }
}

/// A `TicketStatementQuery` carries the SQL text as its opaque handle so
/// `do_get_statement` can re-plan the same query the `get_flight_info` call
/// validated.
fn statement_ticket(sql: &str) -> Ticket {
    let ticket = TicketStatementQuery {
        statement_handle: sql.as_bytes().to_vec().into(),
    };
    Ticket {
        ticket: ticket.as_any().encode_to_vec().into(),
    }
}

fn plan_and_schema(sql: &str) -> std::result::Result<(ProjectionQuery, SchemaRef), Status> {
    let query = plan_projection_sql(sql).map_err(Status::invalid_argument)?;
    let schema = query_result_schema(&query).map_err(|e| Status::internal(e.to_string()))?;
    Ok((query, schema))
}

type FlightDataStream =
    Pin<Box<dyn Stream<Item = std::result::Result<arrow_flight::FlightData, Status>> + Send>>;

#[tonic::async_trait]
impl FlightSqlService for FlightSqlProjectionService {
    type FlightService = FlightSqlProjectionService;

    async fn do_handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> std::result::Result<
        Response<
            Pin<Box<dyn Stream<Item = std::result::Result<HandshakeResponse, Status>> + Send>>,
        >,
        Status,
    > {
        // The MVP authenticates per-call via the Authorization header (the
        // shape pyarrow.flight / ADBC send). A handshake is not required; we
        // accept an empty one so clients that open one don't fail.
        let output = stream::empty();
        Ok(Response::new(Box::pin(output)))
    }

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        self.authorize(request.metadata())?;
        let sql = query.query;
        let (_, schema) = plan_and_schema(&sql)?;

        let ticket = statement_ticket(&sql);
        let endpoint = FlightEndpoint::new().with_ticket(ticket);
        let info = FlightInfo::new()
            .try_with_schema(schema.as_ref())
            .map_err(|e| Status::internal(format!("encode schema: {e}")))?
            .with_endpoint(endpoint)
            .with_descriptor(request.into_inner());
        Ok(Response::new(info))
    }

    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        request: Request<Ticket>,
    ) -> std::result::Result<Response<FlightDataStream>, Status> {
        self.authorize(request.metadata())?;
        let sql = String::from_utf8(ticket.statement_handle.to_vec())
            .map_err(|_| Status::invalid_argument("statement handle is not valid UTF-8"))?;
        let (query, schema) = plan_and_schema(&sql)?;
        let batch = execute_projection_query(self.model.as_ref(), &query)
            .map_err(|e| Status::internal(e.to_string()))?;

        let batches = vec![Ok(batch)];
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(stream::iter(batches))
            .map_err(Status::from);
        Ok(Response::new(Box::pin(stream) as FlightDataStream))
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

/// Serve a [`FlightSqlProjectionService`] on `addr` until `shutdown` resolves.
///
/// The service is wrapped in the standard `FlightServiceServer` (a
/// [`FlightSqlService`] is a `FlightService` via arrow-flight's blanket impl),
/// so any Flight SQL client (`pyarrow.flight`, ADBC, the Flight SQL JDBC
/// driver) can connect. Keeping the gRPC serving here means the deploying role
/// (the worker endpoint) needs no direct tonic / arrow-flight dependency.
pub async fn serve<F>(
    service: FlightSqlProjectionService,
    addr: SocketAddr,
    shutdown: F,
) -> Result<()>
where
    F: Future<Output = ()> + Send,
{
    use arrow_flight::flight_service_server::FlightServiceServer;
    use tonic::transport::Server;

    Server::builder()
        .add_service(FlightServiceServer::new(service))
        .serve_with_shutdown(addr, shutdown)
        .await
        .map_err(|err| EhdbError::Storage(format!("serve Flight SQL projection service: {err}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FakeModel {
        rows: Vec<ExecutionStateView>,
    }

    impl ProjectionReadModel for FakeModel {
        fn list_executions(&self, limit: usize) -> Result<Vec<ExecutionStateView>> {
            Ok(self.rows.iter().take(limit).cloned().collect())
        }
    }

    fn row(execution_id: &str, status: &str, terminal: bool) -> ExecutionStateView {
        ExecutionStateView {
            execution_id: execution_id.to_string(),
            status: status.to_string(),
            current_node: Some("start".to_string()),
            event_count: 3,
            first_global_sequence: 1,
            last_global_sequence: 3,
            last_event_id: 42,
            terminal,
            terminal_event_type: if terminal {
                Some("playbook.completed".to_string())
            } else {
                None
            },
        }
    }

    #[test]
    fn plan_select_star() {
        let q = plan_projection_sql("SELECT * FROM executions").unwrap();
        assert_eq!(q.columns, executions_columns());
        assert_eq!(q.limit, DEFAULT_LIMIT);
        assert!(q.status_filter.is_none());
    }

    #[test]
    fn plan_projection_and_filters_and_limit() {
        let q = plan_projection_sql(
            "SELECT execution_id, status FROM executions \
             WHERE status = 'COMPLETED' AND execution_id = '99' LIMIT 5",
        )
        .unwrap();
        assert_eq!(q.columns, vec!["execution_id", "status"]);
        assert_eq!(q.status_filter.as_deref(), Some("COMPLETED"));
        assert_eq!(q.execution_id_filter.as_deref(), Some("99"));
        assert_eq!(q.limit, 5);
    }

    #[test]
    fn limit_is_clamped() {
        let q = plan_projection_sql("SELECT * FROM executions LIMIT 100000").unwrap();
        assert_eq!(q.limit, MAX_LIMIT);
    }

    #[test]
    fn rejects_writes_and_ddl() {
        for sql in [
            "INSERT INTO executions VALUES ('x')",
            "UPDATE executions SET status = 'x'",
            "DELETE FROM executions",
            "DROP TABLE executions",
        ] {
            assert!(plan_projection_sql(sql).is_err(), "should reject: {sql}");
        }
    }

    #[test]
    fn rejects_unknown_table_column_and_join() {
        assert!(plan_projection_sql("SELECT * FROM secrets").is_err());
        assert!(plan_projection_sql("SELECT password FROM executions").is_err());
        assert!(plan_projection_sql(
            "SELECT * FROM executions JOIN other ON executions.x = other.y"
        )
        .is_err());
        assert!(plan_projection_sql("SELECT * FROM executions WHERE event_count = 3").is_err());
    }

    #[test]
    fn executes_to_projected_batch() {
        let model = FakeModel {
            rows: vec![row("1", "COMPLETED", true), row("2", "RUNNING", false)],
        };
        let query = plan_projection_sql(
            "SELECT execution_id, status FROM executions WHERE status = 'completed'",
        )
        .unwrap();
        let batch = execute_projection_query(&model, &query).unwrap();
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.num_rows(), 1);
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(ids.value(0), "1");
    }

    #[test]
    fn full_batch_schema_is_secret_free() {
        // The schema is exactly the projected read-model columns — no payload
        // body columns (result/error/context/workload) exist.
        let cols = executions_columns();
        for banned in ["result", "error", "context", "workload", "payload"] {
            assert!(!cols.iter().any(|c| c == banned), "leaked column {banned}");
        }
    }

    #[test]
    fn token_verifier_gate() {
        struct OnlyGood;
        impl ReadTokenVerifier for OnlyGood {
            fn verify(&self, presented_token: &str) -> bool {
                presented_token == "good"
            }
        }
        let model: Arc<dyn ProjectionReadModel> = Arc::new(FakeModel { rows: vec![] });
        let svc = FlightSqlProjectionService::new(model, Arc::new(OnlyGood));

        let mut md = tonic::metadata::MetadataMap::new();
        assert!(svc.authorize(&md).is_err(), "no header → unauthenticated");
        md.insert("authorization", "Bearer bad".parse().unwrap());
        assert!(svc.authorize(&md).is_err(), "bad token → unauthenticated");
        md.insert("authorization", "Bearer good".parse().unwrap());
        assert!(svc.authorize(&md).is_ok(), "good token → ok");
    }
}
