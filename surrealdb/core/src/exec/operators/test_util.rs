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
//! - [`parse_expr`] lowers a SurrealQL fragment into its logical form, so a test states its
//!   predicate as source text rather than hand-building node trees. [`plan_statement`] does the
//!   same for a whole statement.
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
use crate::exec::{ExecOperator, ExecutionContext, FlowResult};
use crate::iam::Auth;
use crate::kvs::{Datastore, TransactionType};
use crate::val::Value;

/// Build a minimal root-level [`ExecutionContext`] for operator tests.
///
/// It carries no transaction or datastore — only the config (for
/// `operator_buffer_size`) and the bits `buffer_stream`/`monitor_stream` read.
/// Operators that fetch records or evaluate context-bound expressions cannot
/// run under it. Bind and Distinct can.
pub(crate) fn root_ctx() -> ExecutionContext {
	ExecutionContext::Root(RootContext {
		ctx: Context::new_test().freeze(),
		options: None,
		datastore: None,
		cancellation: Arc::new(CancellationToken::new()),
		auth: Arc::new(Auth::default()),
		session: None,
		current_value: None,
		current_row: None,
		forbidden_row: None,
		inherited_knn: None,
		skip_fetch_perms: false,
		perms_suppressed: false,
		caller_perms_suppressed: false,
		computing_record: None,
		no_write: None,
		version_stamp: None,
		guard_verdicts: Default::default(),
		writing: false,
		caller_view_checked: false,
		nested_runs: Default::default(),
		session_effects: None,
	})
}

/// As [`root_ctx`], but carrying the `Options` a real context carries.
///
/// The permission frames mirror themselves into those options, so that code
/// reading the options lands in the frame it was entered in; `root_ctx`'s bare
/// context has none, and the mirror is invisible there.
pub(crate) fn root_ctx_with_options() -> ExecutionContext {
	let ExecutionContext::Root(mut root) = root_ctx() else {
		unreachable!("root_ctx builds a Root context")
	};
	root.options = Some(crate::dbs::Options::new(&crate::exec::config::ExecConfig::default()));
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
// SurrealQL -> LogicalExpr
// =============================================================================

/// Parse a SurrealQL expression fragment into the surface AST.
///
/// Panics on a parse error — a test fixture that does not parse is a broken
/// fixture, not a failed assertion.
pub(crate) fn parse_sql_expr(src: &str) -> crate::expr::Expr {
	crate::syn::expr(src).expect("fragment should parse")
}

/// Parse a SurrealQL expression fragment and lower it to its logical form.
///
/// Panics when the fragment has no logical form (a write or a schema
/// statement), which a scalar fixture never is.
pub(crate) fn parse_expr(src: &str) -> crate::expr::logical::LogicalExpr {
	crate::expr::logical::lower_expr(parse_sql_expr(src)).expect("fragment should lower")
}

/// Parse a SurrealQL idiom (a field path) into its logical form: a path
/// expression (see [`LogicalExpr::is_path`](crate::expr::logical::LogicalExpr::is_path)).
pub(crate) fn parse_path(src: &str) -> crate::expr::logical::LogicalExpr {
	let expr = parse_expr(src);
	assert!(expr.is_path(), "expected a path for {src:?}, got {expr:?}");
	expr
}

/// Parse a SurrealQL idiom into the catalog's runtime idiom, the form a
/// `DEFINE FIELD` name is stored as and the form the value layer's navigation
/// (`Value::pick`, `Value::each`) takes.
pub(crate) fn field_idiom(src: &str) -> crate::expr::Idiom {
	let expr = parse_sql_expr(src);
	let crate::expr::Expr::Idiom(idiom) = expr else {
		panic!("expected an idiom for {src:?}, got {expr:?}");
	};
	idiom
}

/// Lower, bind and plan one SurrealQL statement against `ctx`, the way the
/// executor does for a top-level statement.
///
/// Needs a context with a transaction ([`TestDb`]): binding and planning read
/// the catalog through it. Panics when the statement has no logical form.
pub(crate) async fn plan_statement(src: &str, ctx: &ExecutionContext) -> Arc<dyn ExecOperator> {
	let op = lower_statement(src);
	crate::exec::planner::plan_statement_inner(
		op,
		ctx.ctx(),
		ctx.function_registry(),
		ctx.try_txn().expect("plan_statement needs a context with a transaction"),
		Some(Arc::clone(&ctx.root().auth)),
		0,
		None,
	)
	.await
	.expect("statement should plan")
}

fn lower_statement(src: &str) -> crate::expr::logical::LogicalOp {
	let ast = crate::syn::parse(src).expect("statement should parse");
	let mut exprs = ast.expressions;
	assert_eq!(exprs.len(), 1, "expected exactly one statement in {src:?}");
	let surrealdb_expr::parser::TopLevelExpr::Expr(expr) = exprs.remove(0) else {
		panic!("unexpected statement shape for {src:?}");
	};
	crate::expr::logical::lower(expr).expect("statement should lower")
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
		Self::build(setup, false, surrealdb_cnf::ConfigMap::empty()).await
	}

	/// As [`Self::new`], but with server auth enabled, so an anonymous identity
	/// is subject to permission checks.
	pub(crate) async fn new_with_auth(setup: &str) -> Self {
		Self::build(setup, true, surrealdb_cnf::ConfigMap::empty()).await
	}

	/// As [`Self::new_with_auth`], with `config` applied.
	#[cfg(feature = "kv-mem")]
	pub(crate) async fn new_with_auth_and_config(
		setup: &str,
		config: surrealdb_cnf::ConfigMap,
	) -> Self {
		Self::build(setup, true, config).await
	}

	/// As [`Self::new`], but with `config` applied, for exercising a knob whose
	/// effect is only visible through a built datastore.
	///
	/// Gated to match its caller (`exec::fan_out`'s config test), so a build
	/// without the in-memory backend does not carry it as dead code.
	#[cfg(feature = "kv-mem")]
	pub(crate) async fn new_with_config(setup: &str, config: surrealdb_cnf::ConfigMap) -> Self {
		Self::build(setup, false, config).await
	}

	/// As [`Self::new`], or [`Self::new_with_auth`] when `auth_enabled`, on a
	/// datastore that runs no maintenance tasks: nothing writes to it but the
	/// statements a test runs. A test comparing the keyspace two runs leave
	/// needs it, since index compaction, task leases and the node heartbeat
	/// otherwise write on their own schedule, inside one run's window and not
	/// the other's.
	pub(crate) async fn quiescent(setup: &str, auth_enabled: bool) -> Self {
		let builder = Datastore::builder().without_maintenance_tasks();
		Self::build_from(builder, setup, auth_enabled, surrealdb_cnf::ConfigMap::empty()).await
	}

	/// A test database over `ds`, whose `test`/`test` namespace and database
	/// already exist: for a test that needs a datastore [`Self::new`] does not
	/// build, such as a versioned one.
	#[cfg(feature = "kv-surrealkv")]
	pub(crate) fn over(ds: Arc<Datastore>) -> Self {
		Self {
			ds,
		}
	}

	/// As [`Self::new_with_config`], over a datastore that `builder` prepares, such as one with a
	/// test-owned memory governor or temporary store.
	#[cfg(feature = "kv-mem")]
	pub(crate) async fn new_on(
		builder: crate::kvs::ds::Builder,
		setup: &str,
		config: surrealdb_cnf::ConfigMap,
	) -> Self {
		Self::build_from(builder, setup, false, config).await
	}

	async fn build(setup: &str, auth_enabled: bool, config: surrealdb_cnf::ConfigMap) -> Self {
		Self::build_from(Datastore::builder(), setup, auth_enabled, config).await
	}

	/// As [`Self::new_with_config`], on a SurrealKV datastore in `dir`.
	#[cfg(feature = "kv-surrealkv")]
	pub(crate) async fn new_on_surrealkv(
		setup: &str,
		dir: &std::path::Path,
		config: surrealdb_cnf::ConfigMap,
	) -> Self {
		Self::new_on_surrealkv_over(Datastore::builder(), setup, dir, config).await
	}

	/// As [`Self::new_on_surrealkv`], on a datastore that enforces permissions.
	#[cfg(feature = "kv-surrealkv")]
	pub(crate) async fn new_on_surrealkv_with_auth(
		setup: &str,
		dir: &std::path::Path,
		config: surrealdb_cnf::ConfigMap,
	) -> Self {
		let path = format!("surrealkv://{}", dir.display());
		Self::build_at(
			Datastore::builder(),
			&path,
			setup,
			true,
			config,
			crate::dbs::Capabilities::all(),
		)
		.await
	}

	/// As [`Self::new_on_surrealkv`], over a datastore that `builder` prepares, such as one with a
	/// test-owned memory governor or temporary store.
	#[cfg(feature = "kv-surrealkv")]
	pub(crate) async fn new_on_surrealkv_over(
		builder: crate::kvs::ds::Builder,
		setup: &str,
		dir: &std::path::Path,
		config: surrealdb_cnf::ConfigMap,
	) -> Self {
		let path = format!("surrealkv://{}", dir.display());
		Self::build_at(builder, &path, setup, false, config, crate::dbs::Capabilities::all()).await
	}

	async fn build_from(
		builder: crate::kvs::ds::Builder,
		setup: &str,
		auth_enabled: bool,
		config: surrealdb_cnf::ConfigMap,
	) -> Self {
		Self::build_at(
			builder,
			"memory",
			setup,
			auth_enabled,
			config,
			crate::dbs::Capabilities::all(),
		)
		.await
	}

	/// As [`Self::new`], or [`Self::new_with_auth`] when `auth_enabled`, with
	/// `capabilities` in place of every capability allowed.
	pub(crate) async fn new_with_capabilities(
		setup: &str,
		auth_enabled: bool,
		capabilities: crate::dbs::Capabilities,
	) -> Self {
		Self::build_at(
			Datastore::builder(),
			"memory",
			setup,
			auth_enabled,
			surrealdb_cnf::ConfigMap::empty(),
			capabilities,
		)
		.await
	}

	/// A test database on the datastore at `path` (`"memory"`, or a backend's address), configured
	/// with `config` and allowed `capabilities`.
	pub(crate) async fn build_at(
		builder: crate::kvs::ds::Builder,
		path: &str,
		setup: &str,
		auth_enabled: bool,
		config: surrealdb_cnf::ConfigMap,
		capabilities: crate::dbs::Capabilities,
	) -> Self {
		let ds = builder
			.with_capabilities(capabilities)
			.with_auth(auth_enabled)
			.with_config(config)
			.build_with_path(path)
			.await
			.expect("datastore");
		Self::seeded(ds, setup).await
	}

	/// As [`Self::new_with_config`], over a memory datastore whose transactions give no read view
	/// and declare none ([`Transactable::snapshot`] answers `None`): for a test whose subject is a
	/// datastore that gives workers nothing to read a range through.
	///
	/// [`Transactable::snapshot`]: surrealdb_kvs::Transactable::snapshot
	#[cfg(feature = "kv-mem")]
	pub(crate) async fn new_viewless(setup: &str, config: surrealdb_cnf::ConfigMap) -> Self {
		let ds = Datastore::builder()
			.with_capabilities(crate::dbs::Capabilities::all())
			.with_auth(false)
			.with_config(config)
			.build_with_factory_path("memory", ViewlessComposer)
			.await
			.expect("datastore");
		Self::seeded(ds, setup).await
	}

	/// The test database over `ds`: its `test`/`test` namespace and database created, then `setup`
	/// run as owner.
	async fn seeded(ds: Arc<Datastore>, setup: &str) -> Self {
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

	/// The datastore itself, for a test that needs a statement's result — or
	/// its failure — rather than only the side effect of running it.
	pub(crate) fn datastore(&self) -> &Arc<Datastore> {
		&self.ds
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
		self.exec_ctx_with(session, mode, false).await
	}

	async fn exec_ctx_with(
		&self,
		session: &Session,
		mode: TransactionType,
		broker: bool,
	) -> ExecutionContext {
		self.exec_ctx_full(session, mode, broker, None).await
	}

	async fn exec_ctx_full(
		&self,
		session: &Session,
		mode: TransactionType,
		broker: bool,
		scheduler: Option<Arc<dyn crate::exec::scheduler::Scheduler>>,
	) -> ExecutionContext {
		let txn = Arc::new(self.ds.transaction(mode).await.expect("transaction"));
		let mut ctx = self.ds.setup_ctx().expect("context from datastore");
		if broker {
			ctx.set_broker(
				self.ds.live_query_broker().map(|b| b as Arc<dyn crate::dbs::MessageBroker>),
			);
		}
		if scheduler.is_some() {
			ctx.set_scheduler(scheduler);
		}
		ctx.attach_session(session).expect("attach session");
		ctx.set_transaction(Arc::clone(&txn));

		let ns = txn.expect_ns_by_name("test").await.expect("namespace test");
		let db = txn.expect_db_by_name("test", "test").await.expect("database test");

		let root = RootContext {
			ctx: ctx.freeze(),
			options: Some(self.ds.setup_options(session)),
			datastore: Some(Arc::clone(&self.ds)),
			cancellation: Arc::new(CancellationToken::new()),
			auth: Arc::clone(&session.au),
			session: Some(Arc::new(session_info(session))),
			current_value: None,
			current_row: None,
			forbidden_row: None,
			inherited_knn: None,
			skip_fetch_perms: false,
			perms_suppressed: false,
			caller_perms_suppressed: false,
			computing_record: None,
			no_write: None,
			version_stamp: None,
			guard_verdicts: Default::default(),
			writing: false,
			caller_view_checked: false,
			nested_runs: Default::default(),
			session_effects: None,
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
			select_permission_cache: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
		})
	}
}

/// Project a [`Session`] onto the pre-extracted [`SessionInfo`] the
/// engine carries, so `$session`-reading expressions see the same fields they
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

/// A composer that opens the community backend at the given path and hands its transactions out
/// as [`ViewlessTransaction`](surrealdb_kvs::testing::ViewlessTransaction)s, which give no read
/// view ([`TestDb::new_viewless`]).
#[cfg(feature = "kv-mem")]
struct ViewlessComposer;

#[cfg(feature = "kv-mem")]
impl crate::kvs::ds::TransactionBuilderFactory for ViewlessComposer {
	type RouterState = ();

	async fn new_transaction_builder(
		&self,
		path: &str,
		canceller: tokio_util::sync::CancellationToken,
		config: surrealdb_cnf::ConfigMap,
	) -> anyhow::Result<crate::kvs::ds::TransactionBuilderParts<Self::RouterState>> {
		let parts =
			crate::CommunityComposer().new_transaction_builder(path, canceller, config).await?;
		Ok(crate::kvs::ds::TransactionBuilderParts::without_router_state(Box::new(
			surrealdb_kvs::testing::ViewlessBuilder(parts.builder),
		)))
	}

	fn path_valid(&self, v: &str) -> anyhow::Result<String> {
		crate::CommunityComposer().path_valid(v)
	}
}
