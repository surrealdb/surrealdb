//! Code reached through the operator tree's expressions, served from the
//! compile cache.
//!
//! Some code is reached only once a program runs, through an expression the
//! program did not compile: the body of a user-defined function a compiled
//! expression calls without a body planned with it, an analyzer's
//! `FUNCTION` call, an API's handler, an expression reached through
//! [`crate::exec::entry`] — a schema statement's name, an `ACCESS`
//! statement's subject — and the query text `eval::surql` and `eval::gql`
//! evaluate, run as a nested query ([`crate::exec::compiles::nested_query`]).
//! Each is compiled from text the catalog or the query carries, so the
//! compile cache keeps it by that text, compared whole
//! ([`super::compile_cache::UnitObject::Expr`],
//! [`super::compile_cache::UnitObject::Statement`],
//! [`super::compile_cache::UnitObject::FrontEnd`]), with one compilation per
//! frame — the class of the principal it is compiled for and the permission
//! contract — and checks what else the compilation read, and the context
//! values it folded in, before serving it. A running program therefore
//! compiles such code only on the cache's miss, once per key and frame
//! ([`crate::exec::compiles::serving_fault`]).
//!
//! Code is compiled for the permission frame it is reached in: where `exec`
//! reads with permissions off — a permission clause, an API's chain, an
//! event's body — the planner plants no permission filter and no field guard
//! over what the code reads, as the runtime enforces none there; the frame's
//! key carries the same flag, so the two compilations are kept apart.
//!
//! A compilation is served to every principal of the frame's class, so it
//! reads the session's own values — `$auth`, `$access`, `$token`,
//! `$session` — when it runs and never folds them
//! ([`crate::exec::planner::Planner::reading_session_values`]).
//!
//! An expression reached where no database is selected names no database to
//! key by: it is compiled on each miss, never kept.

use std::sync::Arc;

use super::compile_cache::{BuildId, ObjectVersion, UnitKey, UnitObject};
use super::program::UnitFrame;
use crate::exec::physical_expr::PhysicalExpr;
use crate::exec::{ExecOperator, ExecutionContext};
use crate::expr::logical::{LogicalExpr, LogicalOp};

/// The key `object` is kept under for code compiled in `exec`, and the frame
/// it is compiled for, for `principal`; `None` where `exec` runs in no
/// database.
fn key_of(
	exec: &ExecutionContext,
	object: UnitObject,
	principal: Option<&crate::iam::Auth>,
) -> Option<(UnitKey, UnitFrame)> {
	let db_ctx = exec.database().ok()?;
	let (ns, db) = (db_ctx.ns_name().to_owned(), db_ctx.db_name().to_owned());
	let frame = UnitFrame {
		class_digest: UnitFrame::digest(exec.ctx().auth_enabled(), principal, Some(&ns), Some(&db)),
		ns: Some(ns.clone()),
		db: Some(db.clone()),
		perms_suppressed: exec.root().perms_suppressed,
		checked: true,
	};
	let key = UnitKey {
		ns,
		db,
		ids: (db_ctx.ns_ctx.ns.namespace_id, db_ctx.db.database_id),
		object,
		version: ObjectVersion::Inline,
		build: BuildId::current(exec.ctx().get_capabilities()),
	};
	Some((key, frame))
}

/// The parameters `visit` names, which a compilation may fold the planning
/// context's values of: what the unit it makes depends on besides the
/// catalog.
fn params_of(
	visit: impl FnOnce(&mut dyn FnMut(&LogicalExpr)),
) -> std::collections::BTreeSet<String> {
	let mut names = std::collections::BTreeSet::new();
	visit(&mut |expr| {
		let _ = expr.visit_deep(&mut |node| {
			if let LogicalExpr::Param(param) = node {
				names.insert(param.as_str().to_owned());
			}
			std::ops::ControlFlow::<()>::Continue(())
		});
	});
	names
}

/// The parameters statement `op` names, which its compilation may fold the
/// planning context's values of: those its expressions read, at any depth, and
/// those it reads its rows from, a source of `FROM $name` among them.
fn statement_params(op: &LogicalOp) -> std::collections::BTreeSet<String> {
	let mut names = params_of(|visit| {
		let _ = op.visit_scalars_deep(&mut |scalar| {
			visit(scalar);
			Ok::<(), std::convert::Infallible>(())
		});
	});
	let _ = op.visit_ops_deep(&mut |node| {
		if let LogicalOp::Source(crate::expr::logical::LogicalSource {
			source: crate::expr::logical::SourceRef::Param(param),
			..
		}) = node
		{
			names.insert(param.as_str().to_owned());
		}
		std::ops::ControlFlow::<()>::Continue(())
	});
	names
}

/// `compile`, served from the compile cache under `key` for `frame` in
/// `exec`: what it compiled is kept with the catalog state the compilation
/// read and the values the context held for `params`, and served while they
/// hold. Every compilation is a miss of the cache, counted as one
/// ([`crate::exec::compiles::serving_fault`]).
async fn served<V, Fut>(
	exec: &ExecutionContext,
	cache: &super::compile_cache::UnitCache<V>,
	keyed: Option<(UnitKey, UnitFrame)>,
	params: std::collections::BTreeSet<String>,
	compile: impl FnOnce() -> Fut + Send,
) -> anyhow::Result<V>
where
	V: Clone + Send + Sync + 'static,
	Fut: std::future::Future<Output = anyhow::Result<V>> + Send,
{
	let txn = exec.try_txn();
	let compile = || {
		crate::exec::compiles::serving_fault(async {
			let recording = txn.as_ref().map(|txn| txn.record_catalog_reads());
			let value = compile().await?;
			let reads = recording.map(|recording| recording.finish());
			let deps = match (reads, &txn) {
				(Some(reads), Some(txn)) if !txn.speculating() => {
					match crate::exec::catalog_deps::CatalogDeps::resolve(txn, &reads).await {
						Some(deps) => {
							super::units::ContextDeps::resolve(txn, &reads, exec.ctx(), params)
								.await
								.map(|context| {
									(
										deps,
										context.without(crate::exec::planner::UNIT_RUNTIME_PARAMS),
									)
								})
						}
						None => None,
					}
				}
				_ => None,
			};
			anyhow::Ok((value, deps))
		})
	};
	match (keyed, &txn) {
		(Some((key, frame)), Some(txn)) if !txn.speculating() => {
			crate::exec::compiles::seam(cache.get_or_compile(
				&key,
				&frame,
				txn,
				exec.ctx(),
				compile,
			))
			.await
		}
		_ => Ok(compile().await?.0),
	}
}

/// `expr` compiled for an evaluation in `exec` at re-entry depth `depth`,
/// for `principal`, served from the compile cache; see the module.
pub(crate) async fn expr(
	exec: &ExecutionContext,
	expr: LogicalExpr,
	principal: Option<Arc<crate::iam::Auth>>,
	depth: u32,
) -> anyhow::Result<Arc<dyn PhysicalExpr>> {
	expr_binding(exec, expr, principal, depth, Vec::new()).await
}

/// [`expr`] for an expression evaluated with the names `bound` bound around
/// it, which it reads when it runs: they are neither folded into the
/// compilation nor held as values it depends on, so one compilation serves
/// every binding of them.
pub(crate) async fn expr_binding(
	exec: &ExecutionContext,
	expr: LogicalExpr,
	principal: Option<Arc<crate::iam::Auth>>,
	depth: u32,
	bound: Vec<String>,
) -> anyhow::Result<Arc<dyn PhysicalExpr>> {
	let Some(units) = exec.ctx().compiled_units() else {
		return compile_expr(exec, expr, principal, depth, &bound).await;
	};
	let expr = Arc::new(expr);
	let mut params = params_of(|visit| visit(&expr));
	params.retain(|name| !bound.contains(name));
	let keyed = key_of(
		exec,
		UnitObject::Expr {
			expr: Arc::clone(&expr),
			depth,
			shadowed: bound.clone(),
		},
		principal.as_deref(),
	);
	served(exec, units.exprs(), keyed, params, || {
		compile_expr(exec, (*expr).clone(), principal.clone(), depth, &bound)
	})
	.await
}

async fn compile_expr(
	exec: &ExecutionContext,
	expr: LogicalExpr,
	principal: Option<Arc<crate::iam::Auth>>,
	depth: u32,
	bound: &[String],
) -> anyhow::Result<Arc<dyn PhysicalExpr>> {
	Ok(crate::exec::planner::compile_unit_expr(expr, exec, principal, depth, bound).await?)
}

/// `op`, a statement planned for a run in `exec` at re-entry depth `depth`,
/// for `principal`, with the names `shadowed`, and the session's own values,
/// read when it runs rather than folded, served from the compile cache; see
/// the module. A planner that declines the statement fails it.
pub(crate) async fn statement(
	exec: &ExecutionContext,
	op: LogicalOp,
	principal: Option<Arc<crate::iam::Auth>>,
	depth: u32,
	mut shadowed: Vec<String>,
) -> anyhow::Result<Arc<dyn ExecOperator>> {
	for name in crate::exec::planner::UNIT_RUNTIME_PARAMS {
		if !shadowed.iter().any(|shadow| shadow == name) {
			shadowed.push((*name).to_owned());
		}
	}
	let Some(txn) = exec.try_txn() else {
		anyhow::bail!(crate::exec::Error::InvalidStatement(
			"a statement reached from an expression is planned in a transaction".to_owned()
		));
	};
	let op = Arc::new(op);
	let params = statement_params(&op);
	let params = params.into_iter().filter(|name| !shadowed.contains(name)).collect();
	let compile = || async {
		Ok(crate::exec::planner::plan_statement_scoped(
			(*op).clone(),
			exec.ctx(),
			exec.function_registry(),
			Arc::clone(&txn),
			principal.clone(),
			depth,
			exec.root().perms_suppressed,
			&shadowed,
		)
		.await?)
	};
	let Some(units) = exec.ctx().compiled_units() else {
		return compile().await;
	};
	let keyed = key_of(
		exec,
		UnitObject::Statement {
			op: Arc::clone(&op),
			depth,
			shadowed: shadowed.clone(),
		},
		principal.as_deref(),
	);
	served(exec, units.statements(), keyed, params, compile).await
}

/// `op`, a front end's plan compiled for a run in `exec` at re-entry depth
/// `depth`, for `principal`, with the names `shadowed`, and the session's own
/// values, read when it runs rather than folded, served from the compile
/// cache; see the module.
pub(crate) async fn front_end(
	exec: &ExecutionContext,
	op: LogicalOp,
	principal: Option<Arc<crate::iam::Auth>>,
	depth: u32,
	mut shadowed: Vec<String>,
) -> anyhow::Result<Arc<dyn ExecOperator>> {
	for name in crate::exec::planner::UNIT_RUNTIME_PARAMS {
		if !shadowed.iter().any(|shadow| shadow == name) {
			shadowed.push((*name).to_owned());
		}
	}
	let Some(txn) = exec.try_txn() else {
		anyhow::bail!(crate::exec::Error::InvalidStatement(
			"a query reached from an expression is planned in a transaction".to_owned()
		));
	};
	let op = Arc::new(op);
	let params = statement_params(&op);
	let params = params.into_iter().filter(|name| !shadowed.contains(name)).collect();
	let compile = || async {
		Ok(crate::exec::planner::plan_logical_scoped(
			(*op).clone(),
			exec.ctx(),
			exec.function_registry(),
			Arc::clone(&txn),
			principal.clone(),
			depth,
			exec.root().perms_suppressed,
			&shadowed,
		)
		.await?)
	};
	let Some(units) = exec.ctx().compiled_units() else {
		return compile().await;
	};
	let keyed = key_of(
		exec,
		UnitObject::FrontEnd {
			op: Arc::clone(&op),
			depth,
			shadowed: shadowed.clone(),
		},
		principal.as_deref(),
	);
	served(exec, units.statements(), keyed, params, compile).await
}

/// Run `spec`, a late-bound statement a block reaches, in `frame`: the
/// statement planned for the catalog the frame's transaction reads, served
/// from the compile cache ([`statement`]), for the frame's principal, with
/// the names the block binds around it read from the frame. Answers its rows,
/// how its run ended, and whether its value is its one row.
pub(super) async fn run_served(
	frame: &ExecutionContext,
	op: &LogicalOp,
	spec: &super::program::ServedSpec,
) -> (Vec<crate::val::Value>, crate::expr::FlowResult<()>, bool) {
	use futures::StreamExt;
	let principal = Some(Arc::clone(&frame.root().auth));
	let plan =
		match statement(frame, op.clone(), principal, spec.depth, spec.shadowed.clone()).await {
			Ok(plan) => plan,
			Err(e) => return (Vec::new(), Err(crate::expr::ControlFlow::Err(e)), false),
		};
	let scalar = matches!(plan.output_shape(), crate::exec::OutputShape::Scalar);
	let mut stream = match plan.execute(frame) {
		Ok(stream) => stream,
		Err(signal) => return (Vec::new(), Err(signal), scalar),
	};
	let mut rows = Vec::new();
	while let Some(batch) = stream.next().await {
		match batch {
			Ok(batch) => rows.extend(batch.into_values()),
			Err(signal) => return (rows, Err(signal), scalar),
		}
	}
	(rows, Ok(()), scalar)
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use crate::exec::operators::test_util::TestDb;

	/// An expression reached through the operator tree's expressions is
	/// compiled on the compile cache's miss and served to a later
	/// transaction, while a context value it folds in holds the same value.
	#[tokio::test]
	async fn an_expression_reached_at_run_time_is_served_from_the_cache() {
		let db =
			TestDb::new("DEFINE FUNCTION fn::f($x: int) { RETURN $x + 1 }; CREATE t:1 SET n = 1;")
				.await;
		let expr = || {
			crate::exec::operators::test_util::parse_expr("fn::f((SELECT VALUE n FROM ONLY t:1))")
		};
		for (run, compiled) in [(0, 1), (1, 1)] {
			let ctx = db.exec_ctx().await;
			let units = Arc::clone(ctx.ctx().compiled_units().expect("the node keeps units"));
			let compiled_expr = super::expr(&ctx, expr(), Some(Arc::clone(&ctx.root().auth)), 0)
				.await
				.expect("compiles");
			let value = compiled_expr
				.evaluate(crate::exec::EvalContext::from_exec_ctx(&ctx))
				.await
				.expect("evaluates");
			assert_eq!(value, crate::val::Value::from(2), "run {run}");
			assert_eq!(units.exprs().stats().compiles, compiled, "run {run}");
			ctx.txn().cancel().await.expect("cancel");
		}
	}

	/// A statement reached at run time is planned for the value its source
	/// parameter holds: the same text under another value is not served the
	/// plan made for the first, which folded that value into its source.
	#[tokio::test]
	async fn a_served_statement_reads_the_value_its_source_parameter_holds() {
		let db = TestDb::new("CREATE t:1 SET n = 1; CREATE t:2 SET n = 2;").await;
		let op =
			|| match crate::exec::operators::test_util::parse_expr("(SELECT VALUE n FROM $src)") {
				crate::expr::logical::LogicalExpr::Subquery(op) => (*op).clone(),
				other => panic!("not a statement: {other:?}"),
			};
		for n in [1, 2] {
			let ctx = db.exec_ctx().await;
			let rid = crate::val::RecordId::new("t".into(), crate::val::RecordIdKey::from(n));
			let ctx = ctx.with_params([("src", crate::val::Value::RecordId(rid))]);
			let principal = Some(Arc::clone(&ctx.root().auth));
			let plan = super::statement(&ctx, op(), principal, 0, Vec::new()).await.expect("plans");
			let rows = crate::exec::operators::test_util::collect(&plan, &ctx).await;
			assert_eq!(rows, vec![crate::val::Value::from(n)], "`$src` = t:{n}");
			ctx.txn().cancel().await.expect("cancel");
		}
	}

	fn record_user(user: &str) -> crate::dbs::Session {
		let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", user));
		crate::dbs::Session::for_record("test", "test", "user", rid)
	}

	const OWNED: &str = "
		DEFINE ACCESS user ON DATABASE TYPE RECORD;
		DEFINE TABLE doc SCHEMALESS PERMISSIONS FULL;
		CREATE doc:1 SET owner = user:alice;
		CREATE doc:2 SET owner = user:bob;
	";

	/// A statement reached at run time is served to every record user of the
	/// access method it was planned for, and reads each caller's own `$auth`.
	#[tokio::test]
	async fn a_served_statement_reads_the_callers_auth() {
		let db = TestDb::new_with_auth(OWNED).await;
		let op = || match crate::exec::operators::test_util::parse_expr(
			"(SELECT VALUE id FROM doc WHERE owner = $auth)",
		) {
			crate::expr::logical::LogicalExpr::Subquery(op) => (*op).clone(),
			other => panic!("not a statement: {other:?}"),
		};
		for (user, sees) in [("alice", 1), ("bob", 2), ("alice", 1)] {
			let ctx = db.exec_ctx_as(&record_user(user), crate::kvs::TransactionType::Read).await;
			let principal = Some(Arc::clone(&ctx.root().auth));
			let plan = super::statement(&ctx, op(), principal, 0, Vec::new()).await.expect("plans");
			let rows = crate::exec::operators::test_util::collect(&plan, &ctx).await;
			let expected = crate::val::Value::RecordId(crate::val::RecordId::new(
				"doc".into(),
				crate::val::RecordIdKey::Number(sees),
			));
			assert_eq!(rows, vec![expected], "{user}");
			ctx.txn().cancel().await.expect("cancel");
		}
	}

	/// An expression reached at run time reads each caller's own `$auth`.
	#[tokio::test]
	async fn a_served_expression_reads_the_callers_auth() {
		let db = TestDb::new_with_auth(OWNED).await;
		for user in ["alice", "bob", "alice"] {
			let ctx = db.exec_ctx_as(&record_user(user), crate::kvs::TransactionType::Read).await;
			let principal = Some(Arc::clone(&ctx.root().auth));
			let expr = crate::exec::operators::test_util::parse_expr("$auth");
			let compiled = super::expr(&ctx, expr, principal, 0).await.expect("compiles");
			let value = compiled
				.evaluate(crate::exec::EvalContext::from_exec_ctx(&ctx))
				.await
				.expect("evaluates");
			let expected = crate::val::Value::RecordId(crate::val::RecordId::new(
				"user".into(),
				crate::val::RecordIdKey::String(user.into()),
			));
			assert_eq!(value, expected, "{user}");
			ctx.txn().cancel().await.expect("cancel");
		}
	}
}
