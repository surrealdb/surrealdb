//! Test-only helpers for exercising exec operators and expressions in isolation.
//!
//! The exec module has no general-purpose stub source operator, so the
//! binding-table operators (which all wrap a single input) need one to be
//! driven end-to-end through their `execute()` streams without a datastore.
//! [`ValuesOperator`] replays a fixed list of [`Value`] rows as one batch, and
//! [`root_ctx`] builds a minimal root-level [`ExecutionContext`] that satisfies
//! `buffer_stream`/`monitor_stream` (neither touches the transaction).
//!
//! Two further layers sit on top of that:
//!
//! - [`parse_expr`] / [`physical_expr`] compile a SurrealQL fragment into the runtime AST and into
//!   a [`PhysicalExpr`], so a test states its predicate as source text rather than hand-building
//!   node trees. `physical_expr` uses a txn-less planner, which is exactly the runtime
//!   permission-resolution path.
//! - [`TestDb`] wraps an in-memory datastore and hands out a **database-level**
//!   [`ExecutionContext`]. Anything that reads the catalog (table/field permissions, computed
//!   fields, index metadata) or dereferences a record needs this rather than [`root_ctx`], because
//!   those paths go through the transaction.

use std::collections::HashMap;
use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::catalog::providers::{CatalogProvider, DatabaseProvider, NamespaceProvider};
use crate::ctx::Context;
use crate::dbs::Session;
use crate::exec::context::{DatabaseContext, NamespaceContext, RootContext, SessionInfo};
use crate::exec::function::FunctionRegistry;
use crate::exec::{
	AccessMode, CardinalityHint, ContextLevel, EvalContext, ExecOperator, ExecutionContext,
	FlowResult, OutputOrdering, PhysicalExpr, ValueBatch, ValueBatchStream,
};
use crate::iam::Auth;
use crate::kvs::{Datastore, TransactionType};
use crate::val::Value;

/// A source operator that yields a fixed list of rows as a single batch.
///
/// `cardinality` lets a test pick the buffering strategy `buffer_stream`
/// applies to the consuming operator; the default is `Unbounded`, matching the
/// status-quo path most operators see.
#[derive(Debug)]
pub(crate) struct ValuesOperator {
	values: Vec<Value>,
	cardinality: CardinalityHint,
	ordering: OutputOrdering,
}

impl ValuesOperator {
	/// Build a source over the given rows with conservative `Unbounded`
	/// cardinality and `Unordered` output. Returns a trait object (the operator
	/// builders all hand back `Arc<dyn ExecOperator>`), not `Self`.
	#[allow(clippy::new_ret_no_self)]
	pub(crate) fn new(values: Vec<Value>) -> Arc<dyn ExecOperator> {
		Arc::new(Self {
			values,
			cardinality: CardinalityHint::Unbounded,
			ordering: OutputOrdering::Unordered,
		})
	}
}

impl ExecOperator for ValuesOperator {
	fn name(&self) -> &'static str {
		"Values"
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Root
	}

	fn access_mode(&self) -> AccessMode {
		AccessMode::ReadOnly
	}

	fn cardinality_hint(&self) -> CardinalityHint {
		self.cardinality
	}

	fn output_ordering(&self) -> OutputOrdering {
		self.ordering.clone()
	}

	fn execute(&self, _ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let values = self.values.clone();
		Ok(Box::pin(futures::stream::once(std::future::ready(Ok(ValueBatch::new(values))))))
	}
}

/// Build a minimal root-level [`ExecutionContext`] for operator tests.
///
/// It carries no transaction or datastore — only the config (for
/// `operator_buffer_size`) and the bits `buffer_stream`/`monitor_stream` read.
/// Operators that fetch records or evaluate context-bound expressions cannot
/// run under it; Bind/Distinct (this slice) do not.
pub(crate) fn root_ctx() -> ExecutionContext {
	ExecutionContext::Root(RootContext {
		ctx: Context::new_test().freeze(),
		function_registry: Arc::new(FunctionRegistry::with_builtins()),
		options: None,
		datastore: None,
		cancellation: CancellationToken::new(),
		auth: Arc::new(Auth::default()),
		session: None,
		current_value: None,
		skip_fetch_perms: false,
		version_stamp: None,
	})
}

/// As [`root_ctx`], but with the given auth identity instead of anonymous.
///
/// `Context::new_test()` has `auth_enabled` set, so the anonymous identity
/// [`root_ctx`] carries is subject to permission checks
/// (`should_check_perms(View)` is true under it). Pass `Auth::for_root(Role::Owner)`
/// for a context that is exempt, or a narrower identity to exercise a specific
/// gate.
pub(crate) fn root_ctx_with_auth(auth: Auth) -> ExecutionContext {
	let ExecutionContext::Root(mut root) = root_ctx() else {
		unreachable!("root_ctx builds a Root context")
	};
	root.auth = Arc::new(auth);
	ExecutionContext::Root(root)
}

/// Drain an operator's output stream into a flat list of rows.
pub(crate) async fn collect(op: &Arc<dyn ExecOperator>, ctx: &ExecutionContext) -> Vec<Value> {
	use futures::StreamExt;
	let mut stream = op.execute(ctx).expect("execute should succeed");
	let mut out = Vec::new();
	while let Some(batch) = stream.next().await {
		out.extend(batch.expect("batch should be Ok").into_values());
	}
	out
}

/// Drain an operator's output stream, returning the control-flow signal or
/// error instead of panicking on it.
pub(crate) async fn try_collect(
	op: &Arc<dyn ExecOperator>,
	ctx: &ExecutionContext,
) -> FlowResult<Vec<Value>> {
	use futures::StreamExt;
	let mut stream = op.execute(ctx)?;
	let mut out = Vec::new();
	while let Some(batch) = stream.next().await {
		out.extend(batch?.into_values());
	}
	Ok(out)
}

// =============================================================================
// SurrealQL -> Expr -> PhysicalExpr
// =============================================================================

/// Parse a SurrealQL expression fragment into the runtime AST.
///
/// The fragment is wrapped in `RETURN … ;` so it parses as one statement, then
/// the `OutputStatement` wrapper is peeled off. Panics on a parse error — a test
/// fixture that does not parse is a broken fixture, not a failed assertion.
pub(crate) fn parse_expr(src: &str) -> crate::expr::Expr {
	let ast = crate::syn::parse(&format!("RETURN {src};")).expect("fragment should parse");
	let mut exprs = ast.expressions;
	assert_eq!(exprs.len(), 1, "expected exactly one statement in {src:?}");
	let top: crate::expr::TopLevelExpr = exprs.remove(0).into();
	match top {
		crate::expr::TopLevelExpr::Expr(crate::expr::Expr::Return(ret)) => ret.what.clone(),
		other => panic!("unexpected statement shape for {src:?}: {other:?}"),
	}
}

/// Parse a SurrealQL idiom (a field path) into the runtime AST.
pub(crate) fn parse_idiom(src: &str) -> crate::expr::Idiom {
	match parse_expr(src) {
		crate::expr::Expr::Idiom(idiom) => idiom,
		other => panic!("expected an idiom for {src:?}, got {other:?}"),
	}
}

/// Compile a SurrealQL expression fragment into a [`PhysicalExpr`] against
/// `ctx`.
///
/// Uses a txn-less [`crate::exec::planner::Planner`], which is the same path
/// runtime permission resolution takes — no plan-time index resolution, so it
/// works under [`root_ctx`] as well as under [`TestDb`].
pub(crate) async fn physical_expr(src: &str, ctx: &ExecutionContext) -> Arc<dyn PhysicalExpr> {
	crate::exec::planner::expr_to_physical_expr(parse_expr(src), ctx.ctx(), ctx.function_registry())
		.await
		.expect("fragment should compile to a physical expression")
}

/// Compile and evaluate `src` with `value` bound as both the current value and
/// the document root.
pub(crate) async fn eval_on(src: &str, value: &Value, ctx: &ExecutionContext) -> FlowResult<Value> {
	let expr = physical_expr(src, ctx).await;
	expr.evaluate(EvalContext::from_exec_ctx(ctx).with_value_and_doc(value)).await
}

/// Compile and evaluate `src` in scalar context (no current row).
pub(crate) async fn eval(src: &str, ctx: &ExecutionContext) -> FlowResult<Value> {
	let expr = physical_expr(src, ctx).await;
	expr.evaluate(EvalContext::from_exec_ctx(ctx)).await
}

/// Build a `Value` from a SurrealQL literal — `obj("{ a: 1, b: [2, 3] }")`.
///
/// Goes through the planner rather than `syn::value` so that object literals
/// with computed entries are accepted; the result is asserted to need no
/// context, which every literal satisfies.
pub(crate) async fn val(src: &str) -> Value {
	let ctx = root_ctx();
	eval(src, &ctx).await.expect("literal should evaluate")
}

// =============================================================================
// Database-level context
// =============================================================================

/// An in-memory datastore plus the plumbing to hand an operator or expression a
/// **database-level** [`ExecutionContext`].
///
/// Namespace and database are always `test`/`test`. Each [`Self::exec_ctx`] call
/// opens its own transaction, held for the returned context's lifetime, so a
/// test that wants to observe a write must commit through [`Self::run`] before
/// building the context it reads under.
pub(crate) struct TestDb {
	ds: Arc<Datastore>,
}

impl TestDb {
	/// Create `test`/`test` on a fresh in-memory datastore with server auth
	/// disabled, then run `setup` (a `;`-separated SurrealQL script) as root
	/// Owner.
	///
	/// Auth disabled is the embedded default, and it means
	/// [`crate::exec::permission::should_check_perms`] short-circuits for an
	/// anonymous identity — use [`Self::new_with_auth`] to exercise that gate.
	pub(crate) async fn new(setup: &str) -> Self {
		Self::build(setup, false).await
	}

	/// As [`Self::new`], but with server auth enabled, so an anonymous identity
	/// is subject to permission checks.
	pub(crate) async fn new_with_auth(setup: &str) -> Self {
		Self::build(setup, true).await
	}

	async fn build(setup: &str, auth_enabled: bool) -> Self {
		let ds = Datastore::builder()
			.with_capabilities(crate::dbs::Capabilities::all())
			.with_auth(auth_enabled)
			.build_with_path("memory")
			.await
			.expect("in-memory datastore");
		{
			let txn = ds.transaction(TransactionType::Write).await.expect("write transaction");
			txn.ensure_ns_db(None, "test", "test").await.expect("ensure test/test");
			txn.commit().await.expect("commit ns/db");
		}
		let db = Self {
			ds,
		};
		if !setup.trim().is_empty() {
			db.run(setup).await;
		}
		db
	}

	/// The root-Owner session on `test`/`test`.
	pub(crate) fn owner() -> Session {
		Session::owner().with_ns("test").with_db("test")
	}

	/// Run SurrealQL as root Owner, panicking if any statement fails.
	pub(crate) async fn run(&self, sql: &str) {
		self.run_as(&Self::owner(), sql).await
	}

	/// Run SurrealQL as `session`, panicking if any statement fails.
	pub(crate) async fn run_as(&self, session: &Session, sql: &str) {
		for response in self.ds.execute(sql, session, None).await.expect("query should execute") {
			response.result.expect("statement should succeed");
		}
	}

	/// A database-level [`ExecutionContext`] as root Owner, over a fresh
	/// read transaction.
	pub(crate) async fn exec_ctx(&self) -> ExecutionContext {
		self.exec_ctx_as(&Self::owner(), TransactionType::Read).await
	}

	/// A database-level [`ExecutionContext`] for `session`, over a fresh
	/// transaction of the given type.
	///
	/// Mirrors what `Executor::execute_operator_plan` assembles: the datastore's
	/// own `Context` (so index stores, caches and capabilities match
	/// production), the session's params (`$auth`, `$session`, `$access`), the
	/// session's `Auth`, and the namespace/database definitions read through the
	/// transaction.
	pub(crate) async fn exec_ctx_as(
		&self,
		session: &Session,
		mode: TransactionType,
	) -> ExecutionContext {
		let txn = Arc::new(self.ds.transaction(mode).await.expect("transaction"));
		let mut ctx = self.ds.setup_ctx().expect("context from datastore");
		ctx.attach_session(session).expect("attach session");
		ctx.set_transaction(Arc::clone(&txn));

		let ns = txn.expect_ns_by_name("test").await.expect("namespace test");
		let db = txn.expect_db_by_name("test", "test").await.expect("database test");

		let root = RootContext {
			ctx: ctx.freeze(),
			function_registry: Arc::new(FunctionRegistry::with_builtins()),
			options: Some(self.ds.setup_options(session)),
			datastore: Some(Arc::clone(&self.ds)),
			cancellation: CancellationToken::new(),
			auth: Arc::clone(&session.au),
			session: Some(Arc::new(session_info(session))),
			current_value: None,
			skip_fetch_perms: false,
			version_stamp: None,
		};

		ExecutionContext::Database(DatabaseContext {
			ns_ctx: NamespaceContext {
				root,
				ns,
			},
			db,
			field_state_cache: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
			table_def_cache: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
			index_def_cache: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
		})
	}
}

/// Project a [`Session`] onto the pre-extracted [`SessionInfo`] the streaming
/// executor carries, so `$session`-reading expressions see the same fields they
/// would under a real query.
fn session_info(session: &Session) -> SessionInfo {
	use crate::val::convert_public::convert_public_value_to_internal;
	SessionInfo {
		ns: session.ns.as_deref().map(Into::into),
		db: session.db.as_deref().map(Into::into),
		id: session.id,
		ip: session.ip.as_deref().map(Into::into),
		origin: session.or.as_deref().map(Into::into),
		ac: session.ac.as_deref().map(Into::into),
		rd: session.rd.clone().map(convert_public_value_to_internal),
		token: session.tk.clone().map(convert_public_value_to_internal),
		exp: None,
	}
}

/// Run an operator expected to fail, and return the error it raised.
///
/// Covers both places a failure can surface — `execute()` itself and a later
/// stream item — and panics if the operator instead produces rows. Control-flow
/// signals other than [`ControlFlow::Err`] are not errors and panic too.
pub(crate) async fn drain_err(op: &impl ExecOperator, ctx: &ExecutionContext) -> anyhow::Error {
	use futures::StreamExt;

	let unwrap = |ctrl| match ctrl {
		crate::expr::ControlFlow::Err(e) => e,
		other => panic!("expected an error, got the control-flow signal: {other:?}"),
	};

	let mut stream = match op.execute(ctx) {
		Ok(stream) => stream,
		Err(ctrl) => return unwrap(ctrl),
	};
	match stream.next().await {
		Some(Ok(batch)) => panic!("expected an error, got rows: {:?}", batch.values()),
		Some(Err(ctrl)) => unwrap(ctrl),
		None => panic!("expected an error, got an empty stream"),
	}
}
