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

use super::compile_cache::{BuildId, ObjectVersion, SourceClass, UnitKey, UnitObject};
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
///
/// The statement's value is handed to the caller whole, and every caller reads it as a value: an
/// access method's `SIGNIN` or `AUTHENTICATE` clause takes its first record, `eval::surql` returns
/// it to the expression that called it, and an API handler answers it. Its rows therefore keep the
/// order they are bound in, as a top-level `LET`'s value does.
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
		true,
		Vec::new(),
	)
	.await
}

/// [`statement`], kept in the cache `cache` picks from the node's units, and
/// planned as the statement of a request where `request_root`. `binds_rows` is
/// whether the statement is the value of a top-level `LET`, whose plan states
/// that its rows arrive in the order they were bound. Such a plan differs from
/// the same statement's anywhere else, so the cache keeps it under `binds_rows`
/// as well. `sources` are the names among `shadowed` the statement reads as its
/// source, with the class of each one's value, which the cache keeps it under
/// too.
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
	sources: Vec<(String, SourceClass)>,
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
	let named = named_targets(&sources);
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
						named.clone(),
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
			sources,
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
			Vec::new(),
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
			Vec::new(),
		)
		.await;
	};
	let Some(txn) = exec.try_txn() else {
		anyhow::bail!(crate::exec::Error::InvalidStatement(
			"a statement of a request is planned in a transaction".to_owned()
		));
	};
	let shadowed: Vec<String> = crate::exec::planner::UNIT_RUNTIME_PARAMS
		.iter()
		.filter(|name| !folded.contains(name))
		.map(|name| (*name).to_owned())
		.collect();
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
				&shadowed,
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
			shadowed: shadowed.clone(),
			binds_rows,
			sources: Vec::new(),
		},
		principal.as_deref(),
	);
	let params = params.into_iter().filter(|name| !shadowed.contains(name)).collect();
	served(exec, units.session_statements(), keyed, params, compile).await
}

/// `op`, a top-level statement of a request that the request compiles when it
/// arrives and that [`kept_for_requests`] keeps, planned as the statement of a
/// request it is and served from the node's cache of request statements
/// (`ExecConfig::statement_cache_size`). The caller has judged it a read or a
/// write that changes no catalog and holds no `EXPLAIN`.
///
/// The request variables it reads only as the key of a record its sources
/// name, or only as its source ([`source_only_request_variables`]), and those a
/// write reads only as its target or as a whole value of its data clause
/// ([`write_run_time_reads`]), are read when it runs, never folded, so one
/// compilation serves every value of them; it is kept for the statement as
/// written, the class of each source's or target's value, the frame and the
/// state of the catalog it read. The session's own values it
/// reads are folded and keyed as [`request_statement`] folds and keys them.
///
/// `literals` are the placeholders [`free_literals`] wrote in place of the
/// statement's literals, with their values; the operator answered reads them
/// from the run's context, where [`LiteralBound`] binds them.
pub(crate) async fn kept_request_statement(
	exec: &ExecutionContext,
	op: LogicalOp,
	principal: Option<Arc<crate::iam::Auth>>,
	literals: &[(String, crate::val::Value)],
) -> anyhow::Result<Arc<dyn ExecOperator>> {
	let params = statement_params(&op);
	let folded: Vec<&str> = crate::exec::planner::UNIT_RUNTIME_PARAMS
		.iter()
		.copied()
		.filter(|name| params.contains(*name))
		.collect();
	let RunTimeReads {
		names: bound,
		sources,
	} = source_only_request_variables(&op, exec.ctx(), literals);
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
			sources,
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
			sources,
		)
		.await;
	};
	let Some(txn) = exec.try_txn() else {
		anyhow::bail!(crate::exec::Error::InvalidStatement(
			"a statement of a request is planned in a transaction".to_owned()
		));
	};
	let mut shadowed = bound;
	for name in crate::exec::planner::UNIT_RUNTIME_PARAMS {
		if !folded.contains(name) && !shadowed.iter().any(|shadow| shadow == name) {
			shadowed.push((*name).to_owned());
		}
	}
	let op = Arc::new(op);
	let named = named_targets(&sources);
	let compile = || async {
		Ok(super::out_of_line(|| {
			crate::exec::planner::plan_request_statement_scoped(
				(*op).clone(),
				exec.ctx(),
				exec.function_registry(),
				Arc::clone(&txn),
				principal.clone(),
				exec.root().perms_suppressed,
				&shadowed,
				false,
				named.clone(),
			)
		})
		.await?)
	};
	let keyed = key_of(
		exec,
		UnitObject::SessionStatement {
			op: Arc::clone(&op),
			session,
			shadowed: shadowed.clone(),
			binds_rows: false,
			sources,
		},
		principal.as_deref(),
	);
	let params = params.into_iter().filter(|name| !shadowed.contains(name)).collect();
	served(exec, units.session_statements(), keyed, params, compile).await
}

/// The table each of `sources` names records of, or is, by the variable's
/// name: what the bind of a kept write names its table from
/// ([`crate::exec::planner::Planner::naming_targets`]).
fn named_targets(sources: &[(String, SourceClass)]) -> Vec<(String, crate::val::TableName)> {
	sources
		.iter()
		.filter_map(|(name, class)| class.table().map(|table| (name.clone(), table.clone())))
		.collect()
}

/// The request variables a kept request statement reads when it runs rather
/// than folding them into its plan ([`source_only_request_variables`]).
#[derive(Default)]
pub(crate) struct RunTimeReads {
	/// Every such name: those of [`Self::sources`], those read only as the
	/// key of `type::record(<table>, $name)`, and those a write reads in its
	/// clauses.
	pub(crate) names: Vec<String>,
	/// The names read only as the statement's source, `FROM [ONLY] $name`, or
	/// as the target of its write, with the class of the value each holds now.
	pub(crate) sources: Vec<(String, SourceClass)>,
}

/// The request variables `op` reads only where one compilation serves every
/// value of them, read when it runs rather than folded.
///
/// A name read only as the key of a record its sources name through
/// `type::record(<table>, $name)`, the table a constant. Main plans such a
/// source as a value the statement computes when it runs, whatever the key
/// holds: the record's table is not resolved when the statement is planned,
/// so no decision of the plan (access path, the table's definitions, a
/// `MATCHES` scope, the catalog the plan depends on) depends on the key.
///
/// A name read only as the source of a statement that does nothing with its
/// rows but project, fetch, limit them and take `ONLY` of them, whose value is
/// a record id naming one record, or an array of such ids
/// ([`SourceClass`]). Its plan is the one main makes for `FROM $name` whose
/// value the binder cannot know: the source is read when the statement runs,
/// and the table it names, its permissions and its fields are resolved then.
/// The statement is kept under the class of the value, so a value of another
/// class is compiled afresh. A statement that filters, orders, groups or
/// splits its rows, holds a `MATCHES`, a nearest-neighbour operator, a
/// `search::` call or a subquery, or reads `WITH` an index hint is not among
/// them: main resolves `FROM $name` to the table or record the value names
/// and its plan may choose by the table, so such a statement is compiled
/// with its request, as is one bound to a table, a range or any other value.
///
/// A name a write reads as its target or in its clauses, as
/// [`write_run_time_reads`] admits.
///
/// A name read anywhere else is folded into the plan by its value and is not
/// among them: not a name an argument of the call only contains, a filter, a
/// subquery or an operator; and not one a statement nested in a source reads.
///
/// `literals` are the placeholders [`free_literals`] wrote in place of the
/// statement's literals, read as request variables holding those values.
pub(crate) fn source_only_request_variables(
	op: &LogicalOp,
	ctx: &crate::ctx::FrozenContext,
	literals: &[(String, crate::val::Value)],
) -> RunTimeReads {
	use std::collections::BTreeMap;

	use crate::expr::logical::{LogicalSource, SourceRef};
	if let LogicalOp::Write(write) = op {
		// A write that reads a request variable elsewhere reads none when it runs.
		return write_run_time_reads(op, write, ctx).unwrap_or_default();
	}
	if let LogicalOp::Timeout(timeout) = op
		&& let LogicalOp::Write(write) = timeout.input.as_ref()
	{
		// A write under `TIMEOUT` reads what the write reads; a name its timeout
		// reads is folded into the plan with the timeout, so it reads none of
		// them when it runs.
		let mut reads = write_run_time_reads(&timeout.input, write, ctx).unwrap_or_default();
		let mut in_timeout = std::collections::BTreeSet::new();
		let _ = timeout.timeout.visit_deep(&mut |node| {
			if let LogicalExpr::Param(param) = node {
				in_timeout.insert(param.as_str().to_owned());
			}
			std::ops::ControlFlow::<()>::Continue(())
		});
		reads.names.retain(|name| !in_timeout.contains(name));
		reads.sources.retain(|(name, _)| !in_timeout.contains(name));
		return reads;
	}
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
	let mut as_sources: BTreeMap<String, usize> = BTreeMap::new();
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
					*as_sources.entry(param.as_str().to_owned()).or_default() += 1;
				}
				SourceRef::Table(_) | SourceRef::RecordId(_) | SourceRef::Mock(_) => {}
			}
		}
		std::ops::ControlFlow::<()>::Continue(())
	});
	let literal =
		|name: &str| literals.iter().find(|(held, _)| held == name).map(|(_, value)| value);
	let readable = |name: &str| {
		!crate::exec::planner::UNIT_RUNTIME_PARAMS.contains(&name)
			&& (ctx.is_request_param(name) || literal(name).is_some())
	};
	let mut names: Vec<String> = as_keys
		.iter()
		.filter(|(name, count)| {
			everywhere.get(*name).copied().unwrap_or(0) == **count
				&& !as_sources.contains_key(*name)
				&& readable(name)
		})
		.map(|(name, _)| name.clone())
		.collect();
	let mut sources = Vec::new();
	if as_sources.len() == 1 && reads_only_its_source(op) {
		for (name, count) in &as_sources {
			if *count != 1
				|| everywhere.contains_key(name)
				|| as_keys.contains_key(name)
				|| !readable(name)
			{
				continue;
			}
			let Some(class) = literal(name).or_else(|| ctx.value(name)).and_then(source_class)
			else {
				continue;
			};
			names.push(name.clone());
			sources.push((name.clone(), class));
		}
	}
	RunTimeReads {
		names,
		sources,
	}
}

/// The request variables `write`, the write `op` is, reads where one
/// compilation serves every value of them, or `None` where it reads one
/// anywhere else.
///
/// A target is such a read where it is a variable alone, read nowhere else in
/// the write, whose value names one table: a record id of the table, an array
/// of record ids of that one table, or, for a `CREATE`, the table itself. An
/// `UPDATE` or `DELETE` reads its target as the source of the rows it writes
/// (`UPDATE $id`). The write is kept under that class ([`SourceClass`]) and
/// the bind names the table from it, so the plan is the one main makes for a
/// write that names the table, with the target read when it runs. A `RELATE` keeps its edge
/// table as written; each of its endpoints is a variable of any record class
/// or reads no request variable.
///
/// Every other request variable the write reads — in its data clause, its
/// `WHERE` and its `RETURN` fields — is read when it runs and keys nothing.
/// No choice of the plan depends on its value: the kept compilation is planned
/// with the name shadowed, so the bind substitutes no value for it, and the IR
/// binder answers a request variable no statement binds as read at run time
/// ([`crate::exec::ir::cache::planning::PlanningCatalog`]). What the value
/// decides — the kind a `CONTENT` accepts, a field's coercion, an `ASSERT`, a
/// permission — the stages judge per record, with the errors a compilation
/// that folded the value raises, as the stages of a write that names no one
/// table do for every name of their statement
/// ([`super::units::StageSetUnit`]). A write that holds a statement, a graph lookup or a recursion,
/// an `INSERT`, a write over the rows of a read, and one whose target is written
/// in the statement's text is compiled with its request.
fn write_run_time_reads(
	op: &LogicalOp,
	write: &crate::expr::logical::LogicalWrite,
	ctx: &crate::ctx::FrozenContext,
) -> Option<RunTimeReads> {
	use std::collections::BTreeMap;

	use crate::expr::logical::{
		LogicalField, LogicalFields, LogicalWriteSource, LogicalWriteStatement,
	};
	let readable = |name: &str| {
		!crate::exec::planner::UNIT_RUNTIME_PARAMS.contains(&name) && ctx.is_request_param(name)
	};
	let mut reads: BTreeMap<String, usize> = BTreeMap::new();
	let mut nested = false;
	let mut count = |expr: &LogicalExpr| {
		let _ = expr.visit_deep(&mut |node| {
			match node {
				LogicalExpr::Param(param) => {
					*reads.entry(param.as_str().to_owned()).or_default() += 1;
				}
				LogicalExpr::Subquery(_)
				| LogicalExpr::Lookup {
					..
				}
				| LogicalExpr::Recurse(_)
				| LogicalExpr::RepeatRecurse(_) => nested = true,
				LogicalExpr::Literal(_)
				| LogicalExpr::Array(_)
				| LogicalExpr::Set(_)
				| LogicalExpr::Object(_)
				| LogicalExpr::RecordId(_)
				| LogicalExpr::Table(_)
				| LogicalExpr::Mock(_)
				| LogicalExpr::Constant(_)
				| LogicalExpr::Field(_)
				| LogicalExpr::Doc
				| LogicalExpr::Current
				| LogicalExpr::Access {
					..
				}
				| LogicalExpr::Index {
					..
				}
				| LogicalExpr::First(_)
				| LogicalExpr::Last(_)
				| LogicalExpr::Each(_)
				| LogicalExpr::Flatten {
					..
				}
				| LogicalExpr::Filter {
					..
				}
				| LogicalExpr::Method {
					..
				}
				| LogicalExpr::Destructure {
					..
				}
				| LogicalExpr::Optional {
					..
				}
				| LogicalExpr::Broadcast {
					..
				}
				| LogicalExpr::SetField {
					..
				}
				| LogicalExpr::Prefix {
					..
				}
				| LogicalExpr::Postfix {
					..
				}
				| LogicalExpr::Invoke {
					..
				}
				| LogicalExpr::Binary {
					..
				}
				| LogicalExpr::Call(_)
				| LogicalExpr::Closure(_)
				| LogicalExpr::If(_)
				| LogicalExpr::Throw(_)
				| LogicalExpr::Break
				| LogicalExpr::Continue => {}
			}
			std::ops::ControlFlow::<()>::Continue(())
		});
	};
	let _ = op.visit_scalars(&mut |scalar| {
		count(scalar);
		Ok::<(), std::convert::Infallible>(())
	});
	if let Some(crate::expr::output::Output::Fields(fields)) = write.statement.output() {
		match crate::expr::logical::lower_fields(fields.clone()).ok()? {
			LogicalFields::Value {
				expr,
				..
			} => count(&expr),
			LogicalFields::Select(fields) => {
				for field in &fields {
					if let LogicalField::Single {
						expr,
						..
					} = field
					{
						count(expr);
					}
				}
			}
		}
	}
	if nested {
		return None;
	}
	let reads_no_variable = |expr: &LogicalExpr| {
		expr.visit_deep(&mut |node| match node {
			LogicalExpr::Param(param) if readable(param.as_str()) => {
				std::ops::ControlFlow::Break(())
			}
			LogicalExpr::Literal(_)
			| LogicalExpr::Array(_)
			| LogicalExpr::Set(_)
			| LogicalExpr::Object(_)
			| LogicalExpr::RecordId(_)
			| LogicalExpr::Param(_)
			| LogicalExpr::Table(_)
			| LogicalExpr::Mock(_)
			| LogicalExpr::Constant(_)
			| LogicalExpr::Field(_)
			| LogicalExpr::Doc
			| LogicalExpr::Current
			| LogicalExpr::Access {
				..
			}
			| LogicalExpr::Index {
				..
			}
			| LogicalExpr::First(_)
			| LogicalExpr::Last(_)
			| LogicalExpr::Each(_)
			| LogicalExpr::Flatten {
				..
			}
			| LogicalExpr::Filter {
				..
			}
			| LogicalExpr::Method {
				..
			}
			| LogicalExpr::Destructure {
				..
			}
			| LogicalExpr::Optional {
				..
			}
			| LogicalExpr::Lookup {
				..
			}
			| LogicalExpr::Recurse(_)
			| LogicalExpr::RepeatRecurse(_)
			| LogicalExpr::Broadcast {
				..
			}
			| LogicalExpr::SetField {
				..
			}
			| LogicalExpr::Prefix {
				..
			}
			| LogicalExpr::Postfix {
				..
			}
			| LogicalExpr::Invoke {
				..
			}
			| LogicalExpr::Binary {
				..
			}
			| LogicalExpr::Call(_)
			| LogicalExpr::Closure(_)
			| LogicalExpr::If(_)
			| LogicalExpr::Throw(_)
			| LogicalExpr::Break
			| LogicalExpr::Continue
			| LogicalExpr::Subquery(_) => std::ops::ControlFlow::Continue(()),
		})
		.is_continue()
	};
	let mut sources = Vec::new();
	// A target is read once, as a scalar of the write or as the source of the rows it writes.
	let mut source = |name: &str, scalar: bool, creates: bool, one_table: bool| -> Option<()> {
		if !readable(name) || reads.get(name).copied().unwrap_or(0) != usize::from(scalar) {
			return None;
		}
		let value = ctx.value(name)?;
		let class = match value {
			crate::val::Value::Table(table) if creates => SourceClass::Table(table.clone()),
			value @ (crate::val::Value::None
			| crate::val::Value::Null
			| crate::val::Value::Bool(_)
			| crate::val::Value::Number(_)
			| crate::val::Value::String(_)
			| crate::val::Value::Duration(_)
			| crate::val::Value::Datetime(_)
			| crate::val::Value::Date(_)
			| crate::val::Value::Time(_)
			| crate::val::Value::Timestamp(_)
			| crate::val::Value::Interval(_)
			| crate::val::Value::Uuid(_)
			| crate::val::Value::Array(_)
			| crate::val::Value::Set(_)
			| crate::val::Value::Object(_)
			| crate::val::Value::Geometry(_)
			| crate::val::Value::Bytes(_)
			| crate::val::Value::Table(_)
			| crate::val::Value::RecordId(_)
			| crate::val::Value::File(_)
			| crate::val::Value::Regex(_)
			| crate::val::Value::Range(_)
			| crate::val::Value::Closure(_)) => source_class(value)?,
		};
		if one_table && class.table().is_none() {
			return None;
		}
		sources.push((name.to_owned(), class));
		Some(())
	};
	let param = |expr: &LogicalExpr| match expr {
		LogicalExpr::Param(param) => Some(param.as_str().to_owned()),
		LogicalExpr::Literal(_)
		| LogicalExpr::Array(_)
		| LogicalExpr::Set(_)
		| LogicalExpr::Object(_)
		| LogicalExpr::RecordId(_)
		| LogicalExpr::Table(_)
		| LogicalExpr::Mock(_)
		| LogicalExpr::Constant(_)
		| LogicalExpr::Field(_)
		| LogicalExpr::Doc
		| LogicalExpr::Current
		| LogicalExpr::Access {
			..
		}
		| LogicalExpr::Index {
			..
		}
		| LogicalExpr::First(_)
		| LogicalExpr::Last(_)
		| LogicalExpr::Each(_)
		| LogicalExpr::Flatten {
			..
		}
		| LogicalExpr::Filter {
			..
		}
		| LogicalExpr::Method {
			..
		}
		| LogicalExpr::Destructure {
			..
		}
		| LogicalExpr::Optional {
			..
		}
		| LogicalExpr::Lookup {
			..
		}
		| LogicalExpr::Recurse(_)
		| LogicalExpr::RepeatRecurse(_)
		| LogicalExpr::Broadcast {
			..
		}
		| LogicalExpr::SetField {
			..
		}
		| LogicalExpr::Prefix {
			..
		}
		| LogicalExpr::Postfix {
			..
		}
		| LogicalExpr::Invoke {
			..
		}
		| LogicalExpr::Binary {
			..
		}
		| LogicalExpr::Call(_)
		| LogicalExpr::Closure(_)
		| LogicalExpr::If(_)
		| LogicalExpr::Throw(_)
		| LogicalExpr::Break
		| LogicalExpr::Continue
		| LogicalExpr::Subquery(_) => None,
	};
	match (&write.statement, &write.source) {
		(LogicalWriteStatement::Relate(_), LogicalWriteSource::Targets(targets)) => {
			let [from, through, to] = targets.as_slice() else {
				return None;
			};
			if !reads_no_variable(through) {
				return None;
			}
			for endpoint in [from, to] {
				if !reads_no_variable(endpoint) {
					source(&param(endpoint)?, true, false, false)?;
				}
			}
		}
		// An `UPSERT` under a condition reads its subjects from the table its target names.
		(
			LogicalWriteStatement::Create(_) | LogicalWriteStatement::Upsert(_),
			LogicalWriteSource::Targets(targets),
		) if write.cond.is_none() => {
			let [target] = targets.as_slice() else {
				return None;
			};
			let creates = matches!(write.statement, LogicalWriteStatement::Create(_));
			source(&param(target)?, true, creates, true)?;
		}
		(
			LogicalWriteStatement::Update(_) | LogicalWriteStatement::Delete(_),
			LogicalWriteSource::Rows(rows),
		) => {
			let LogicalOp::Source(crate::expr::logical::LogicalSource {
				source: crate::expr::logical::SourceRef::Param(name),
				with: None,
				for_update: false,
				..
			}) = rows.as_ref()
			else {
				return None;
			};
			source(name.as_str(), false, false, true)?;
		}
		_ => return None,
	}
	let mut names: Vec<String> = reads.into_keys().filter(|name| readable(name)).collect();
	for (name, _) in &sources {
		if !names.contains(name) {
			names.push(name.clone());
		}
	}
	Some(RunTimeReads {
		names,
		sources,
	})
}

/// Whether `op` does nothing with the rows of its one source but project,
/// fetch and limit them and take `ONLY` of them, reading that source with no
/// index hint, and evaluates no `MATCHES`, nearest-neighbour operator,
/// `search::` call or subquery: a statement whose plan, main's for a source
/// whose value the binder cannot know, makes no choice by the table the
/// source names.
fn reads_only_its_source(op: &LogicalOp) -> bool {
	use std::ops::ControlFlow;

	use crate::expr::logical::LogicalSource;
	let shape = op.visit_ops_deep(&mut |node| {
		if matches!(
			node,
			LogicalOp::Source(LogicalSource {
				with: None,
				for_update: false,
				..
			}) | LogicalOp::Project(_)
				| LogicalOp::Fetch(_)
				| LogicalOp::Limit(_)
				| LogicalOp::Only(_)
		) {
			ControlFlow::Continue(())
		} else {
			ControlFlow::Break(())
		}
	});
	if shape.is_break() {
		return false;
	}
	op.visit_scalars_deep(&mut |scalar| match scalar.visit_deep(&mut |node| {
		if matches!(
			node,
			LogicalExpr::Subquery(_)
				| LogicalExpr::Binary {
					op: crate::expr::BinaryOperator::Matches(_)
						| crate::expr::BinaryOperator::NearestNeighbor(_),
					..
				}
		) {
			ControlFlow::Break(())
		} else if let LogicalExpr::Call(call) = node
			&& let crate::expr::Function::Normal(name) = &call.receiver
			&& name.starts_with("search::")
		{
			ControlFlow::Break(())
		} else {
			ControlFlow::Continue(())
		}
	}) {
		ControlFlow::Break(()) => Err(()),
		ControlFlow::Continue(()) => Ok(()),
	})
	.is_ok()
}

/// The class of `value` as the source of a kept statement: a record id whose
/// key names one record, or an array of such ids; `None` for any other value
/// — a table, a range, a record id whose key holds a closure, which has no
/// literal form, a mixed array.
fn source_class(value: &crate::val::Value) -> Option<SourceClass> {
	use crate::val::Value;
	fn one(value: &Value) -> Option<&crate::val::TableName> {
		if let Value::RecordId(id) = value
			&& !id.key.is_range()
		{
			return Some(&id.table);
		}
		None
	}
	if crate::exec::planner::util::substitution_is_lossy(value) {
		return None;
	}
	if let Value::Array(items) = value {
		// Borrowed until the distinct tables are known, so an array of
		// many ids of few tables clones each table once.
		let mut tables = Vec::new();
		for item in items.iter() {
			let table = one(item)?;
			if !tables.contains(&table) {
				tables.push(table);
			}
		}
		tables.sort();
		Some(SourceClass::Records(tables.into_iter().cloned().collect()))
	} else {
		one(value).cloned().map(SourceClass::Record)
	}
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

/// The name of the placeholder variable [`free_literals`] writes in place of
/// the `ordinal`-th literal it frees. A statement can spell it, so
/// [`free_literals`] frees nothing where the name is read or bound.
fn literal_placeholder(ordinal: usize) -> String {
	format!("\0literal {ordinal}")
}

/// `op`, a top-level statement of a request, with each literal one
/// compilation serves every value of written as a placeholder variable, and
/// the value each placeholder holds; `None` where it has no such literal.
///
/// A literal is freed only where a request variable in its place would be
/// read when the statement runs ([`source_only_request_variables`]), in a
/// statement that reads only its source: a record id that is its one source,
/// and the key of `type::record(<table>, <key>)` as that source. A freed
/// literal's value is no part of the key the compilation is kept under, only
/// its class is; every other literal stays in the statement as written, keyed
/// by its value. An array written as the source is a `Values` leaf, not a
/// source, and is never freed. A statement where any candidate would not be
/// freed, that reads a variable of a placeholder's name, or that runs where a
/// value of that name is bound, frees none.
pub(crate) fn free_literals(
	op: &LogicalOp,
	ctx: &crate::ctx::FrozenContext,
) -> Option<(LogicalOp, Vec<(String, crate::val::Value)>)> {
	use crate::expr::logical::{LogicalSource, SourceRef};
	fn rewrite(op: &mut LogicalOp, literals: &mut Vec<(String, crate::val::Value)>) {
		if let LogicalOp::Source(LogicalSource {
			source,
			..
		}) = op
		{
			match source {
				SourceRef::RecordId(rid) => {
					let rid = LogicalExpr::RecordId(rid.clone());
					if let Some(value) = crate::exec::planner::util::try_literal_to_value(&rid) {
						let name = literal_placeholder(literals.len());
						*source = SourceRef::Param(name.clone().into());
						literals.push((name, value));
					}
				}
				SourceRef::Dynamic(LogicalExpr::Call(call))
					if matches!(&call.receiver,
						crate::expr::Function::Normal(name) if name == "type::record") =>
				{
					if let [table, key @ LogicalExpr::Literal(_)] = call.args.as_mut_slice()
						&& matches!(
							crate::exec::planner::static_value(table),
							Some(crate::val::Value::String(_) | crate::val::Value::Table(_))
						) && let Some(value) = crate::exec::planner::util::try_literal_to_value(key)
						&& !crate::exec::planner::util::substitution_is_lossy(&value)
					{
						let name = literal_placeholder(literals.len());
						*key = LogicalExpr::Param(name.clone().into());
						literals.push((name, value));
					}
				}
				SourceRef::Table(_)
				| SourceRef::Mock(_)
				| SourceRef::Param(_)
				| SourceRef::Dynamic(_) => {}
			}
		}
		for child in op.children_mut() {
			rewrite(child, literals);
		}
	}
	let candidate = op.visit_ops_deep(&mut |node| match node {
		LogicalOp::Source(LogicalSource {
			source: SourceRef::RecordId(_) | SourceRef::Dynamic(LogicalExpr::Call(_)),
			..
		}) => std::ops::ControlFlow::Break(()),
		LogicalOp::Source(_)
		| LogicalOp::Values(_)
		| LogicalOp::Match(_)
		| LogicalOp::Subquery(_)
		| LogicalOp::Write(_)
		| LogicalOp::Ddl(_)
		| LogicalOp::UnionAll(_)
		| LogicalOp::Join(_)
		| LogicalOp::Filter(_)
		| LogicalOp::PermissionFilter(_)
		| LogicalOp::Split(_)
		| LogicalOp::Aggregate(_)
		| LogicalOp::Distinct(_)
		| LogicalOp::Sort(_)
		| LogicalOp::Window(_)
		| LogicalOp::Limit(_)
		| LogicalOp::Project(_)
		| LogicalOp::Fetch(_)
		| LogicalOp::Timeout(_)
		| LogicalOp::VersionScope(_)
		| LogicalOp::Only(_)
		| LogicalOp::Explain(_)
		| LogicalOp::Eval(_)
		| LogicalOp::Sequence(_)
		| LogicalOp::Let(_)
		| LogicalOp::Return(_)
		| LogicalOp::Foreach(_)
		| LogicalOp::If(_)
		| LogicalOp::Throw(_)
		| LogicalOp::Break(_)
		| LogicalOp::Continue(_)
		| LogicalOp::Info(_)
		| LogicalOp::Sleep(_) => std::ops::ControlFlow::Continue(()),
	});
	if candidate.is_continue() || !reads_only_its_source(op) {
		return None;
	}
	let mut literals = Vec::new();
	let mut freed = op.clone();
	rewrite(&mut freed, &mut literals);
	if literals.is_empty() {
		return None;
	}
	let written = statement_params(op);
	if literals.iter().any(|(name, _)| written.contains(name) || ctx.value(name).is_some()) {
		return None;
	}
	let names = source_only_request_variables(&freed, ctx, &literals).names;
	if !literals.iter().all(|(name, _)| names.contains(name)) {
		return None;
	}
	Some((freed, literals))
}

/// A kept request statement run with the values of the literals
/// [`free_literals`] freed from it: each run reads them in a scope of its own,
/// seen by this statement alone.
#[derive(Debug)]
pub(crate) struct LiteralBound {
	inner: Arc<dyn ExecOperator>,
	literals: Vec<(String, Arc<crate::val::Value>)>,
}

impl LiteralBound {
	/// `inner`, run with `literals` bound.
	pub(crate) fn new(
		inner: Arc<dyn ExecOperator>,
		literals: Vec<(String, crate::val::Value)>,
	) -> Self {
		Self {
			inner,
			literals: literals.into_iter().map(|(name, value)| (name, Arc::new(value))).collect(),
		}
	}

	fn bound(&self, ctx: &ExecutionContext) -> ExecutionContext {
		let mut child = crate::ctx::Context::new_child(ctx.ctx());
		for (name, value) in &self.literals {
			child.add_value(name.as_str(), Arc::clone(value));
		}
		ctx.with_new_ctx(child.freeze())
	}
}

impl ExecOperator for LiteralBound {
	fn required_context(&self) -> crate::exec::ContextLevel {
		self.inner.required_context()
	}

	fn execute(
		&self,
		ctx: &ExecutionContext,
	) -> crate::expr::FlowResult<crate::exec::ValueBatchStream> {
		self.inner.execute(&self.bound(ctx))
	}

	fn execute_encoded(
		&self,
		ctx: &ExecutionContext,
		format: surrealdb_rpc::RowFormat,
	) -> crate::expr::FlowResult<crate::exec::ValueBatchStream> {
		self.inner.execute_encoded(&self.bound(ctx), format)
	}

	fn access_mode(&self) -> crate::exec::AccessMode {
		self.inner.access_mode()
	}

	fn output_shape(&self) -> crate::exec::OutputShape {
		self.inner.output_shape()
	}
}

/// Whether `op`, a top-level statement of a request, is kept compiled for
/// later requests: it reads a request variable, and reads every one it reads
/// where one compilation serves every value of it
/// ([`source_only_request_variables`]), so a request that binds other values
/// to them is served the compilation. A statement that reads none, or reads
/// one elsewhere, is compiled with its request.
///
/// `literals` are as [`source_only_request_variables`] reads them.
pub(crate) fn kept_for_requests(
	op: &LogicalOp,
	ctx: &crate::ctx::FrozenContext,
	literals: &[(String, crate::val::Value)],
) -> bool {
	let source_only = source_only_request_variables(op, ctx, literals).names;
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
		let op = || {
			let expr = crate::exec::operators::test_util::parse_expr("(SELECT VALUE n FROM $src)");
			let crate::expr::logical::LogicalExpr::Subquery(op) = expr else {
				panic!("not a statement: {expr:?}");
			};
			(*op).clone()
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
		let op = || {
			let expr = crate::exec::operators::test_util::parse_expr(
				"(SELECT VALUE id FROM doc WHERE owner = $auth)",
			);
			let crate::expr::logical::LogicalExpr::Subquery(op) = expr else {
				panic!("not a statement: {expr:?}");
			};
			(*op).clone()
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

	/// A statement cached in `session_statements()` because its surface text
	/// names `$auth` is keyed only by `$auth`; any other session parameter a
	/// table permission clause reads (`$session`, `$token`, `$access`) must be
	/// read at run time rather than folded from the first session that
	/// compiled the plan.
	#[tokio::test]
	async fn a_session_statement_does_not_fold_unmentioned_session_params() {
		let db = TestDb::new_with_auth(
			"
			DEFINE ACCESS user ON DATABASE TYPE RECORD;
			DEFINE TABLE doc SCHEMALESS
				PERMISSIONS FOR select WHERE ip = $session.ip;
			CREATE doc:1 SET owner = user:alice, ip = '10.0.0.1';
			CREATE doc:2 SET owner = user:alice, ip = '10.0.0.2';
			",
		)
		.await;
		let parse_op = |sql: &str| {
			let expr = crate::exec::operators::test_util::parse_expr(sql);
			let crate::expr::logical::LogicalExpr::Subquery(op) = expr else {
				panic!("not a statement: {expr:?}");
			};
			(*op).clone()
		};

		for (ip, sees) in [("10.0.0.1", 1), ("10.0.0.2", 2)] {
			let mut session = record_user("alice");
			session.ip = Some(ip.to_owned());
			let ctx = db.exec_ctx_as(&session, crate::kvs::TransactionType::Read).await;
			let principal = Some(Arc::clone(&ctx.root().auth));
			let op = parse_op("(SELECT VALUE id FROM doc WHERE owner = $auth)");
			let plan = super::request_statement(&ctx, op, principal, false).await.expect("plans");
			let rows = crate::exec::operators::test_util::collect(&plan, &ctx).await;
			let expected = crate::val::Value::RecordId(crate::val::RecordId::new(
				"doc".into(),
				crate::val::RecordIdKey::Number(sees),
			));
			assert_eq!(rows, vec![expected], "request_statement for ip {ip}");
			ctx.txn().cancel().await.expect("cancel");
		}

		for (ip, key, sees) in
			[("10.0.0.1", 1, vec![1]), ("10.0.0.2", 1, vec![]), ("10.0.0.2", 2, vec![2])]
		{
			let mut session = record_user("alice");
			session.ip = Some(ip.to_owned());
			let ctx = db
				.exec_ctx_as(&session, crate::kvs::TransactionType::Read)
				.await
				.with_params([("k", crate::val::Value::from(key))]);
			let principal = Some(Arc::clone(&ctx.root().auth));
			let op = parse_op("(SELECT VALUE id FROM type::record('doc', $k) WHERE owner = $auth)");
			let plan =
				super::kept_request_statement(&ctx, op, principal, &[]).await.expect("plans");
			let rows = crate::exec::operators::test_util::collect(&plan, &ctx).await;
			let expected: Vec<crate::val::Value> = sees
				.into_iter()
				.map(|n| {
					crate::val::Value::RecordId(crate::val::RecordId::new(
						"doc".into(),
						crate::val::RecordIdKey::Number(n),
					))
				})
				.collect();
			assert_eq!(rows, expected, "kept_request_statement for ip {ip}, key {key}");
			ctx.txn().cancel().await.expect("cancel");
		}
	}
}
