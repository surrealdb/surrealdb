//! `eval::surql` / `eval::gql` for the engine.
//!
//! `eval` runs its query text as a nested query
//! ([`crate::exec::compiles::nested_query`]), the one sanctioned way a running
//! program has code compiled for it, shared with an embedded script's
//! `surrealdb.query()`. The text is parsed into a statement, which is lowered,
//! bound and planned the way the executor plans a top-level one, in the
//! caller's transaction and with the caller's session, principal and options,
//! then evaluated against an isolated child of the current `ExecutionContext`
//! (only the explicit bindings visible — never the call site's scope). A
//! statement the planner refuses fails the call with `PlannerUnsupported`. The
//! program holding the call is never recompiled: the nested statement is
//! planned and run as a program of its own, sealed again by its own stream.
//!
//! The nested program is served from the compile cache
//! ([`crate::exec::vm::serve`]) under the statement the text parses to, the
//! re-entry depth and the names of the bindings, for the frame of the
//! principal's class, and only while the catalog it read is unchanged; it
//! reads the bindings and the session's own values when it runs. A call
//! evaluated once per row therefore compiles once per distinct text.
//!
//! Capability gating, parsing and statement validation live in
//! `crate::fnc::eval::prepare`.
//!
//! Nested-`eval` recursion is bounded by *depth*: the planner counts
//! expression-nesting depth against
//! `max_computation_depth`, and records that depth onto each `eval` node
//! ([`EvalContext::plan_depth`]). When `eval` plans its query string here, it
//! seeds the fresh planner with that recorded depth so the count *continues*
//! across the re-entry instead of resetting — so a self-referential or
//! deeply-nested `eval` reaches the limit and errors cleanly rather than growing
//! the native stack without bound.

use std::sync::Arc;

use anyhow::Result;

use crate::catalog::providers::{DatabaseProvider, NamespaceProvider};
use crate::ctx::Context;
use crate::dbs::Variables;
use crate::exe::FlowResultExt as _;
use crate::exec::function::{ScalarFunction, Signature};
use crate::exec::ir::iface::Fallibility;
use crate::exec::operators::collect_shaped_value;
use crate::exec::{Error as ExecError, EvalContext, ExecOperator, ExecutionContext};
use crate::expr::Kind;
use crate::expr::logical::{EffectSet, lower};
use crate::fnc::args::{FromArgs, Optional};
use crate::fnc::eval::{Prepared, prepare};
use crate::kvs::Dialect;
use crate::val::{Object, Value};

/// Evaluate `query` in `dialect` with `bindings`, isolated from the call
/// site's scope and bound to the current transaction, as a nested query.
///
/// `prepare` yields either the SurrealQL statements (at most one) or, for GQL,
/// the plan its lowering produced. A statement is lowered and planned through
/// [`crate::exec::vm::serve::statement`], a plan through
/// [`crate::exec::vm::serve::front_end`]; lowering that declines fails the
/// call with `PlannerUnsupported`. The plan is compiled at
/// `ctx.plan_depth + 1` so the nesting count continues toward
/// `max_computation_depth`.
pub(crate) async fn evaluate_streaming(
	ctx: &EvalContext<'_>,
	dialect: Dialect,
	query: String,
	bindings: Option<Object>,
) -> Result<Value> {
	crate::exec::compiles::nested_query(evaluate_nested(ctx, dialect, query, bindings)).await
}

async fn evaluate_nested(
	ctx: &EvalContext<'_>,
	dialect: Dialect,
	query: String,
	bindings: Option<Object>,
) -> Result<Value> {
	let exec_ctx = ctx.exec_ctx;
	let caps = exec_ctx.capabilities();

	// Depth at which this `eval` node was planned (recorded by the planner and
	// surfaced via `BuiltinFunctionExec`). The nested query is one re-entry
	// deeper, so it is compiled at `plan_depth + 1`; that continuation is what
	// bounds nested-`eval` recursion against `max_computation_depth`.
	let depth = ctx.plan_depth + 1;

	// Gate, parse and validate.
	let prepared = prepare(&caps, exec_ctx.auth(), dialect, &query)?;

	// Build an isolated child context: the evaluated query sees the transaction,
	// capabilities and the explicit bindings, but not the caller's scope. The
	// `ExecutionContext` is rebuilt around it, preserving auth / ns-db level.
	// The bindings are read when the nested program runs, so one compilation
	// serves every value they hold. The nested query reads the caller
	// statement's timestamp: its context inherits it, and no clock of its own
	// is installed here.
	let mut isolated = Context::new_isolated(exec_ctx.ctx());
	let mut bound: Vec<String> = Vec::new();
	if let Some(bindings) = bindings {
		bound.extend(bindings.keys().map(|key| key.as_str().to_owned()));
		isolated.attach_variables(Variables::from(bindings))?;
	}
	let isolated = isolated.freeze();
	// The statement reaches no row of the call site's, so inside a KNN query
	// vector's evaluation the row being scanned is out of its reach rather than
	// forbidden to it.
	let mut eval_ctx = exec_ctx.with_new_ctx(isolated).without_forbidden_row();

	// The permission-predicate no-write signal lives on the caller's
	// `EvalContext` (set in `exec::permission`), not on the root this builtin
	// rebuilds, so it is copied onto the root here; the re-entered context then
	// refuses writes as the predicate does. The COMPUTED-field signal is already
	// a root flag and needs no copy.
	eval_ctx = eval_ctx.with_skip_fetch_perms(ctx.skip_fetch_perms);

	// Promote to Database level when a database is selected, so nested table /
	// GQL MATCH queries have the context they require. `eval`'s `required_context`
	// stays Root so db-less queries (e.g. `eval::surql("RETURN 1")`) still work;
	// the engine would otherwise apply this promotion itself.
	if let Some(session) = exec_ctx.session()
		&& let (Some(ns), Some(db)) = (session.ns.clone(), session.db.clone())
	{
		let txn = exec_ctx.txn();
		let ns_def = txn.expect_ns_by_name(ns.as_str()).await?;
		let db_def = txn.expect_db_by_name(ns.as_str(), db.as_str()).await?;
		eval_ctx = eval_ctx.with_database(ns_def, db_def);
	}

	let auth = Some(Arc::clone(&eval_ctx.root().auth));

	match prepared {
		Prepared::Statements(statements) => {
			let mut result = Value::None;
			for mut stmt in statements {
				stmt.prepare_credentials();
				let op = lower(stmt).map_err(|reason| ExecError::PlannerUnsupported {
					reason: reason.to_string(),
				})?;
				let plan = crate::exec::vm::serve::statement(
					&eval_ctx,
					op,
					auth.clone(),
					depth,
					bound.clone(),
				)
				.await?;
				result = run_plan(plan, &eval_ctx).await.catch_return()?;
			}
			Ok(result)
		}
		// A GQL query lowers to a plan.
		Prepared::Plan(op) => {
			let plan =
				crate::exec::vm::serve::front_end(&eval_ctx, *op, auth, depth, bound).await?;
			Ok(run_plan(plan, &eval_ctx).await.catch_return()?)
		}
	}
}

/// Runs `statements` in order as nested queries of `exec`, in its transaction and as its principal,
/// answering the value of the last. The statements come from the engine rather than from a
/// caller's text, so no capability gates them: the one who reaches here has already been judged
/// by the statement that runs them.
pub(crate) async fn run_statements(
	exec: &ExecutionContext,
	depth: u32,
	statements: Vec<crate::expr::Expr>,
) -> Result<Value> {
	crate::exec::compiles::nested_query(async {
		let auth = Some(Arc::clone(&exec.root().auth));
		let mut result = Value::None;
		for mut stmt in statements {
			stmt.prepare_credentials();
			let op = lower(stmt).map_err(|reason| ExecError::PlannerUnsupported {
				reason: reason.to_string(),
			})?;
			let plan = crate::exec::vm::serve::statement(exec, op, auth.clone(), depth, Vec::new())
				.await?;
			result = run_plan(plan, exec).await.catch_return()?;
		}
		Ok(result)
	})
	.await
}

/// Run a compiled plan to the single value it evaluates to.
async fn run_plan(
	plan: Arc<dyn ExecOperator>,
	ctx: &ExecutionContext,
) -> crate::expr::FlowResult<Value> {
	// The query runs as a frame of the statement's driver, when there is
	// one, rather than beneath this call on the native stack.
	let ctx = ctx.clone();
	crate::exec::trampoline::nest(async move {
		let shape = plan.output_shape();
		let stream = plan.execute(&ctx)?;
		collect_shaped_value(stream, shape).await
	})
	.await
}

fn eval_signature() -> Signature {
	Signature::new()
		.arg("query", Kind::String)
		.optional("bindings", Kind::Object)
		.returns(Kind::Any)
}

#[derive(Debug, Clone, Copy, Default)]
pub struct EvalSurql;

impl ScalarFunction for EvalSurql {
	fn name(&self) -> &'static str {
		"eval::surql"
	}

	fn fallibility(&self) -> Fallibility {
		crate::declare_fallibility!(
			Scalar,
			"eval::surql",
			Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA)
		)
	}

	fn effects(&self) -> EffectSet {
		EffectSet::OPAQUE_BODY | EffectSet::WRITES_DATA
	}

	fn signature(&self) -> Signature {
		eval_signature()
	}

	fn is_pure(&self) -> bool {
		false
	}

	fn is_async(&self) -> bool {
		true
	}

	fn invoke(&self, _args: Vec<Value>) -> Result<Value> {
		Err(anyhow::anyhow!("Function '{}' requires async execution", self.name()))
	}

	fn invoke_async<'a>(
		&'a self,
		ctx: &'a EvalContext<'_>,
		args: Vec<Value>,
	) -> crate::exec::BoxFut<'a, Result<Value>> {
		Box::pin(async move {
			let (query, Optional(bindings)) = FromArgs::from_args("eval::surql", args)?;
			evaluate_streaming(ctx, Dialect::SurrealQl, query, bindings).await
		})
	}
}

#[derive(Debug, Clone, Copy, Default)]
pub struct EvalGql;

impl ScalarFunction for EvalGql {
	fn name(&self) -> &'static str {
		"eval::gql"
	}

	fn fallibility(&self) -> Fallibility {
		crate::declare_fallibility!(
			Scalar,
			"eval::gql",
			Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA)
		)
	}

	fn effects(&self) -> EffectSet {
		EffectSet::OPAQUE_BODY | EffectSet::WRITES_DATA
	}

	fn signature(&self) -> Signature {
		eval_signature()
	}

	fn is_pure(&self) -> bool {
		false
	}

	fn is_async(&self) -> bool {
		true
	}

	fn invoke(&self, _args: Vec<Value>) -> Result<Value> {
		Err(anyhow::anyhow!("Function '{}' requires async execution", self.name()))
	}

	fn invoke_async<'a>(
		&'a self,
		ctx: &'a EvalContext<'_>,
		args: Vec<Value>,
	) -> crate::exec::BoxFut<'a, Result<Value>> {
		Box::pin(async move {
			let (query, Optional(bindings)) = FromArgs::from_args("eval::gql", args)?;
			evaluate_streaming(ctx, Dialect::Gql, query, bindings).await
		})
	}
}

// Both run the statement they are handed, so the planner must not duplicate,
// drop or fold a call to either. `is_writer_builtin` keeps them out of every
// purity decision, and the effects each function declares say the same.
/// The `eval::*` scalar builtins.
pub(super) const SCALARS: &[&dyn ScalarFunction] = &[&EvalSurql, &EvalGql];

#[cfg(test)]
mod tests {
	use crate::dbs::Session;
	use crate::dbs::capabilities::{Capabilities, Targets};
	use crate::exec::operators::test_util::TestDb;
	use crate::val::Value;

	/// Every capability allowed, `eval` included.
	fn eval_allowed() -> Capabilities {
		Capabilities::all().with_eval_query(Targets::All)
	}

	fn record_user(user: &str) -> Session {
		let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", user));
		Session::for_record("test", "test", "user", rid)
	}

	/// The last statement's value of `sql` run as `session`, or its error's
	/// message.
	async fn last(db: &TestDb, session: &Session, sql: &str) -> Value {
		let mut results = db.datastore().execute(sql, session, None).await.expect("runs");
		let result = results.pop().expect("a result").result;
		result.map(Value::from).unwrap_or_else(|e| Value::from(e.to_string()))
	}

	fn parse(text: &str) -> Value {
		Value::from(crate::syn::value(text).expect("parses"))
	}

	/// How many statements the datastore's compile cache has compiled.
	async fn statement_compiles(db: &TestDb) -> u64 {
		let ctx = db.exec_ctx().await;
		let units = ctx.ctx().compiled_units().expect("the node keeps units");
		let compiles = units.statements().stats().compiles;
		ctx.txn().cancel().await.expect("cancel");
		compiles
	}

	/// A call evaluated once per row compiles its text once, served to every
	/// later row and to a later query, with the bindings read per row; each
	/// distinct text compiles once, and what a compiled text reads of the
	/// catalog answers as the catalog stands when it runs.
	#[tokio::test]
	async fn a_text_compiles_once_whatever_the_rows() {
		let db = TestDb::new_with_capabilities(
			"CREATE t:1 SET n = 1; CREATE t:2 SET n = 2; CREATE t:3 SET n = 3; \
			 CREATE t:4 SET n = 4; CREATE t:5 SET n = 5;",
			false,
			eval_allowed(),
		)
		.await;
		let owner = TestDb::owner();
		let per_row = "SELECT VALUE eval::surql('RETURN $n + 1', { n: n }) FROM t ORDER BY n";

		let before = statement_compiles(&db).await;
		assert_eq!(last(&db, &owner, per_row).await, parse("[2, 3, 4, 5, 6]"));
		assert_eq!(statement_compiles(&db).await - before, 1, "five rows, one text");

		let before = statement_compiles(&db).await;
		assert_eq!(last(&db, &owner, per_row).await, parse("[2, 3, 4, 5, 6]"));
		assert_eq!(statement_compiles(&db).await - before, 0, "a later query is served");

		let before = statement_compiles(&db).await;
		let two_texts = "SELECT VALUE eval::surql(IF n > 2 { 'RETURN 10' } ELSE { 'RETURN 20' }) \
		                 FROM t ORDER BY n";
		assert_eq!(last(&db, &owner, two_texts).await, parse("[20, 20, 10, 10, 10]"));
		assert_eq!(statement_compiles(&db).await - before, 2, "two distinct texts");

		// What the compiled text reads of the catalog answers as the catalog
		// stands when it runs: a field defined, and a function redefined, after
		// it was compiled.
		let reads = "SELECT VALUE eval::surql('SELECT VALUE twice FROM ONLY $r', { r: id }) \
		             FROM t ORDER BY n";
		assert_eq!(last(&db, &owner, reads).await, parse("[NONE, NONE, NONE, NONE, NONE]"));
		db.run("DEFINE FIELD twice ON t COMPUTED n * 2").await;
		assert_eq!(last(&db, &owner, reads).await, parse("[2, 4, 6, 8, 10]"));
		let calls = "SELECT VALUE eval::surql('RETURN fn::f($n)', { n: n }) FROM t ORDER BY n";
		db.run("DEFINE FUNCTION fn::f($x: int) { RETURN $x + 1 }").await;
		assert_eq!(last(&db, &owner, calls).await, parse("[2, 3, 4, 5, 6]"));
		db.run("DEFINE FUNCTION OVERWRITE fn::f($x: int) { RETURN $x - 1 }").await;
		assert_eq!(last(&db, &owner, calls).await, parse("[0, 1, 2, 3, 4]"));
	}

	const OWNED: &str = "
		DEFINE ACCESS user ON DATABASE TYPE RECORD;
		DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
		DEFINE TABLE doc SCHEMALESS PERMISSIONS FOR select WHERE owner = $auth;
		CREATE user:alice, user:bob;
		CREATE doc:1 SET owner = user:alice;
		CREATE doc:2 SET owner = user:bob;
	";

	/// One compiled program serves two record users of one access method, and
	/// each reads their own `$auth` and is judged by their own permissions,
	/// whichever of them compiled it.
	#[tokio::test]
	async fn two_principals_share_a_compiled_text() {
		let db = TestDb::new_with_capabilities(OWNED, true, eval_allowed()).await;
		for sql in [
			"RETURN eval::surql('SELECT VALUE id FROM doc')",
			"RETURN eval::surql('SELECT VALUE id FROM doc WHERE owner = $auth')",
			"RETURN eval::surql('RETURN [$auth, (SELECT VALUE id FROM doc)]')",
		] {
			let alice = last(&db, &record_user("alice"), sql).await;
			let before = statement_compiles(&db).await;
			let bob = last(&db, &record_user("bob"), sql).await;
			assert_eq!(statement_compiles(&db).await - before, 0, "`{sql}`: bob is served");
			let alice_again = last(&db, &record_user("alice"), sql).await;
			let (alice_sees, bob_sees) = match sql.contains("RETURN [") {
				true => ("[user:alice, [doc:1]]", "[user:bob, [doc:2]]"),
				false => ("[doc:1]", "[doc:2]"),
			};
			assert_eq!(alice, parse(alice_sees), "`{sql}` as alice");
			assert_eq!(bob, parse(bob_sees), "`{sql}` as bob");
			assert_eq!(alice_again, parse(alice_sees), "`{sql}` as alice after bob");
		}
	}

	/// The nested query reads the caller statement's timestamp, so `time::now()`
	/// in its text equals the caller's own read within one statement: the
	/// executor installs the clock in the context the nested query inherits.
	#[tokio::test]
	async fn a_nested_query_reads_the_callers_clock() {
		let db = TestDb::new_with_capabilities("", false, eval_allowed()).await;
		let value = last(
			&db,
			&TestDb::owner(),
			"RETURN [time::now(), eval::surql('RETURN time::now()'), eval::surql('time::now()')]",
		)
		.await;
		let Value::Array(reads) = value else {
			panic!("an array of three reads, found {value:?}");
		};
		assert_eq!(reads.len(), 3);
		assert_eq!(reads[0], reads[1], "the nested read is the caller's");
		assert_eq!(reads[0], reads[2], "a bare expression reads the same clock");
	}

	/// A `RETURN` inside the nested text ends only the nested query: the
	/// caller's statement carries on with the nested query's value.
	#[tokio::test]
	async fn a_return_in_nested_text_ends_only_the_nested_query() {
		let db = TestDb::new_with_capabilities("", false, eval_allowed()).await;
		assert_eq!(
			last(&db, &TestDb::owner(), "RETURN [eval::surql('{ RETURN 1; 2 }'), 3]").await,
			parse("[1, 3]"),
		);
	}

	/// With no `allow-eval-query`, every call is refused naming the function,
	/// whoever makes it.
	#[tokio::test]
	async fn eval_is_denied_without_the_capability() {
		let db = TestDb::new_with_capabilities("", false, Capabilities::all()).await;
		for sql in ["RETURN eval::surql('RETURN 1')", "RETURN eval::gql('MATCH (n) RETURN n')"] {
			let name = sql.split('(').next().unwrap_or_default().trim_start_matches("RETURN ");
			assert_eq!(
				last(&db, &TestDb::owner(), sql).await,
				Value::from(format!("Function '{name}' is not allowed to be executed")),
			);
		}
	}
}
