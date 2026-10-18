//! Code a program reaches only at run time, served from the compile cache.
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
//! ([`crate::exec::planner::Planner::reading_session_values`]). The one
//! exception is a request's own top-level statement, which folds them and is
//! kept under the values it folded ([`request_statement`]). A statement the
//! request keeps for later requests also reads the record keys its sources
//! name when it runs ([`kept_request_statement`]).
//!
//! An expression reached where no database is selected names no database to
//! key by: it is compiled on each miss, never kept.

use std::sync::Arc;

use super::compile_cache::{BuildId, ObjectVersion, UnitKey, UnitObject};
use super::program::UnitFrame;
use crate::exec::{Clause, ExecOperator, ExecutionContext};
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
) -> anyhow::Result<Clause> {
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
) -> anyhow::Result<Clause> {
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
) -> anyhow::Result<Clause> {
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
	shadowed: Vec<String>,
) -> anyhow::Result<Arc<dyn ExecOperator>> {
	statement_in(
		exec,
		super::units::CompiledUnits::statements,
		false,
		op,
		principal,
		depth,
		shadowed,
		false,
	)
	.await
}

/// [`statement`], kept in the cache `cache` picks from the node's units, and
/// planned as the statement of a request where `request_root`. `binds_rows` is
/// whether the statement is the value of a top-level `LET`, whose plan states
/// that its rows arrive in the order they were bound. Such a plan differs from
/// the same statement's anywhere else, so the cache keeps it under `binds_rows`
/// as well.
#[expect(
	clippy::too_many_arguments,
	reason = "the statement, the cache and frame it is planned for, and how its rows are read"
)]
async fn statement_in(
	exec: &ExecutionContext,
	cache: fn(
		&super::units::CompiledUnits,
	) -> &super::compile_cache::UnitCache<Arc<dyn ExecOperator>>,
	request_root: bool,
	op: LogicalOp,
	principal: Option<Arc<crate::iam::Auth>>,
	depth: u32,
	mut shadowed: Vec<String>,
	binds_rows: bool,
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
		Ok(match request_root {
			true => {
				super::out_of_line(|| {
					crate::exec::planner::plan_request_statement_scoped(
						(*op).clone(),
						exec.ctx(),
						exec.function_registry(),
						Arc::clone(&txn),
						principal.clone(),
						exec.root().perms_suppressed,
						&shadowed,
						binds_rows,
					)
				})
				.await?
			}
			false => {
				super::out_of_line(|| {
					crate::exec::planner::plan_statement_scoped(
						(*op).clone(),
						exec.ctx(),
						exec.function_registry(),
						Arc::clone(&txn),
						principal.clone(),
						depth,
						exec.root().perms_suppressed,
						&shadowed,
						binds_rows,
					)
				})
				.await?
			}
		})
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
			binds_rows,
		},
		principal.as_deref(),
	);
	served(exec, cache(units), keyed, params, compile).await
}

/// `op`, a top-level statement of a request, planned for its run in `exec`
/// for `principal` and served from the compile cache, when it is reached.
///
/// The request's program is compiled for one session, so a statement of it
/// folds the values that session's own parameters — `$auth`, `$access`,
/// `$token`, `$session` — hold, as one planned in the program does. A
/// statement that reads none of them is served as [`statement`] serves it. One
/// that reads any is kept under the values it folds as well as its text, so a
/// session holding other values is never served it: a specialisation per
/// session, at most `ExecConfig::specialised_body_cache_size` of them. Where
/// those values are larger than `ExecConfig::specialised_body_max_arg_bytes`,
/// or hold a value that has no encoding, the statement reads them when it
/// runs instead.
pub(crate) async fn request_statement(
	exec: &ExecutionContext,
	op: LogicalOp,
	principal: Option<Arc<crate::iam::Auth>>,
	binds_rows: bool,
) -> anyhow::Result<Arc<dyn ExecOperator>> {
	crate::exec::compiles::note_statement_served();
	let params = statement_params(&op);
	let folded: Vec<&str> = crate::exec::planner::UNIT_RUNTIME_PARAMS
		.iter()
		.copied()
		.filter(|name| params.contains(*name))
		.collect();
	let Some(units) = exec.ctx().compiled_units().filter(|_| !folded.is_empty()) else {
		return statement_in(
			exec,
			super::units::CompiledUnits::statements,
			false,
			op,
			principal,
			0,
			Vec::new(),
			binds_rows,
		)
		.await;
	};
	let values: Vec<crate::val::Value> = folded
		.iter()
		.map(|name| exec.ctx().value(name).cloned().unwrap_or(crate::val::Value::None))
		.collect();
	let Some(session) = super::compile_cache::ArgValues::of(&values, units.specialised_arg_bytes())
	else {
		return statement_in(
			exec,
			super::units::CompiledUnits::statements,
			false,
			op,
			principal,
			0,
			Vec::new(),
			binds_rows,
		)
		.await;
	};
	let Some(txn) = exec.try_txn() else {
		anyhow::bail!(crate::exec::Error::InvalidStatement(
			"a statement of a request is planned in a transaction".to_owned()
		));
	};
	let op = Arc::new(op);
	let compile = || async {
		Ok(super::out_of_line(|| {
			crate::exec::planner::plan_statement_scoped(
				(*op).clone(),
				exec.ctx(),
				exec.function_registry(),
				Arc::clone(&txn),
				principal.clone(),
				0,
				exec.root().perms_suppressed,
				&[],
				binds_rows,
			)
		})
		.await?)
	};
	let keyed = key_of(
		exec,
		UnitObject::SessionStatement {
			op: Arc::clone(&op),
			session,
			shadowed: Vec::new(),
			binds_rows,
		},
		principal.as_deref(),
	);
	served(exec, units.session_statements(), keyed, params, compile).await
}

/// `op`, a top-level statement of a request that the request compiles when it
/// arrives and that [`kept_for_requests`] keeps, planned as the statement of a
/// request it is and served from the node's cache of request statements
/// (`ExecConfig::statement_cache_size`). The caller has judged it read-only,
/// changing no catalog and holding no `EXPLAIN`.
///
/// The request variables it reads only as the key of a record its sources
/// name ([`source_only_request_variables`]) are read when it runs, never
/// folded, so one compilation serves every value of them; it is kept for the
/// statement as written, the frame and the state of the catalog it read. The
/// session's own values it reads are folded and keyed as [`request_statement`]
/// folds and keys them.
pub(crate) async fn kept_request_statement(
	exec: &ExecutionContext,
	op: LogicalOp,
	principal: Option<Arc<crate::iam::Auth>>,
) -> anyhow::Result<Arc<dyn ExecOperator>> {
	let params = statement_params(&op);
	let folded: Vec<&str> = crate::exec::planner::UNIT_RUNTIME_PARAMS
		.iter()
		.copied()
		.filter(|name| params.contains(*name))
		.collect();
	let bound = source_only_request_variables(&op, exec.ctx());
	let Some(units) = exec.ctx().compiled_units().filter(|_| !folded.is_empty()) else {
		return statement_in(
			exec,
			super::units::CompiledUnits::request_statements,
			true,
			op,
			principal,
			0,
			bound,
			false,
		)
		.await;
	};
	let values: Vec<crate::val::Value> = folded
		.iter()
		.map(|name| exec.ctx().value(name).cloned().unwrap_or(crate::val::Value::None))
		.collect();
	let Some(session) = super::compile_cache::ArgValues::of(&values, units.specialised_arg_bytes())
	else {
		return statement_in(
			exec,
			super::units::CompiledUnits::request_statements,
			true,
			op,
			principal,
			0,
			bound,
			false,
		)
		.await;
	};
	let Some(txn) = exec.try_txn() else {
		anyhow::bail!(crate::exec::Error::InvalidStatement(
			"a statement of a request is planned in a transaction".to_owned()
		));
	};
	let op = Arc::new(op);
	let compile = || async {
		Ok(super::out_of_line(|| {
			crate::exec::planner::plan_request_statement_scoped(
				(*op).clone(),
				exec.ctx(),
				exec.function_registry(),
				Arc::clone(&txn),
				principal.clone(),
				exec.root().perms_suppressed,
				&bound,
				false,
			)
		})
		.await?)
	};
	let keyed = key_of(
		exec,
		UnitObject::SessionStatement {
			op: Arc::clone(&op),
			session,
			shadowed: bound.clone(),
			binds_rows: false,
		},
		principal.as_deref(),
	);
	let params = params.into_iter().filter(|name| !bound.contains(name)).collect();
	served(exec, units.session_statements(), keyed, params, compile).await
}

/// The request variables `op` reads only as the key of a record its sources
/// name through `type::record(<table>, $name)`, the table a constant. Main plans such a source as a
/// value the statement computes when it runs, whatever the key holds: the record's table is not
/// resolved when the statement is planned, so no decision of the plan (access path, the
/// table's definitions, a `MATCHES` scope, the catalog the plan depends on)
/// depends on the key, and one compilation that reads the name when it runs
/// is the plan main makes for every value of it.
///
/// A name read anywhere else is folded into the plan by its value and is not
/// among them: not a `FROM $name`, which main resolves to the table or record
/// the value names and plans an access path for; not a name an argument of
/// the call only contains, a filter, a subquery or an operator; and not one a
/// statement nested in a source reads.
pub(crate) fn source_only_request_variables(
	op: &LogicalOp,
	ctx: &crate::ctx::FrozenContext,
) -> Vec<String> {
	use std::collections::BTreeMap;

	use crate::expr::logical::{LogicalSource, SourceRef};
	let mut everywhere: BTreeMap<String, usize> = BTreeMap::new();
	let _ = op.visit_scalars_deep(&mut |scalar| {
		let _ = scalar.visit_deep(&mut |node| {
			if let LogicalExpr::Param(param) = node {
				*everywhere.entry(param.as_str().to_owned()).or_default() += 1;
			}
			std::ops::ControlFlow::<()>::Continue(())
		});
		Ok::<(), std::convert::Infallible>(())
	});
	let mut as_keys: BTreeMap<String, usize> = BTreeMap::new();
	let mut as_sources = std::collections::BTreeSet::new();
	let _ = op.visit_ops_deep(&mut |node| {
		if let LogicalOp::Source(LogicalSource {
			source,
			..
		}) = node
		{
			match source {
				SourceRef::Dynamic(expr) => {
					if let Some(name) = record_key_param(expr) {
						*as_keys.entry(name.to_owned()).or_default() += 1;
					}
				}
				SourceRef::Param(param) => {
					as_sources.insert(param.as_str().to_owned());
				}
				_ => {}
			}
		}
		std::ops::ControlFlow::<()>::Continue(())
	});
	as_keys
		.into_iter()
		.filter(|(name, count)| {
			everywhere.get(name).copied().unwrap_or(0) == *count && !as_sources.contains(name)
		})
		.map(|(name, _)| name)
		.filter(|name| {
			!crate::exec::planner::UNIT_RUNTIME_PARAMS.contains(&name.as_str())
				&& ctx.is_request_param(name)
		})
		.collect()
}

/// The name `expr` reads as the key of a record of a constant table:
/// `type::record(<table>, $name)`.
fn record_key_param(expr: &LogicalExpr) -> Option<&str> {
	let LogicalExpr::Call(call) = expr else {
		return None;
	};
	let crate::expr::Function::Normal(name) = &call.receiver else {
		return None;
	};
	if name != "type::record" {
		return None;
	}
	let [table, LogicalExpr::Param(key)] = call.args.as_slice() else {
		return None;
	};
	match crate::exec::planner::static_value(table) {
		Some(crate::val::Value::String(_) | crate::val::Value::Table(_)) => Some(key.as_str()),
		_ => None,
	}
}

/// Whether `op`, a top-level statement of a request, is kept compiled for
/// later requests: it reads a request variable, and reads every one it reads
/// only as the key of a record its sources name
/// ([`source_only_request_variables`]), so a request that binds other values
/// to them is served the compilation. A statement that reads none, or reads
/// one elsewhere (a `FROM $name` among those places), is compiled with its
/// request.
pub(crate) fn kept_for_requests(op: &LogicalOp, ctx: &crate::ctx::FrozenContext) -> bool {
	let source_only = source_only_request_variables(op, ctx);
	if source_only.is_empty() {
		return false;
	}
	let elsewhere =
		|name: &str| ctx.is_request_param(name) && !source_only.iter().any(|kept| kept == name);
	statement_params(op).iter().all(|name| !elsewhere(name))
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

	/// An expression reached at run time is compiled on the compile cache's
	/// miss and served to a later transaction, while a context value it folds
	/// in holds the same value.
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
