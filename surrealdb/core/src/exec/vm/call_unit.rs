//! A call of a user-defined function a late-bound statement makes.
//!
//! A statement that runs after a change to the catalog its plan cannot know
//! inlines no function body and links no function unit: the function it names
//! may be defined, redefined or removed before it runs. Its call is a call of
//! code the host runs ([`super::program::ExternalCallee::Function`]): the host
//! loads the function's call unit, the statement `fn::name($0, …)` compiled
//! for the definition the transaction reads now, and runs it with the
//! arguments bound to its parameters. The unit makes the checks a call makes —
//! the capability, the definition, its `PERMISSIONS` clause under its `AUTH
//! LIMIT` — binds and coerces the arguments, and runs the body.
//!
//! Call units are served from the compile cache ([`super::compile_cache`]),
//! keyed by the function's name and arity, the version of its definition and
//! the build, with one compilation per frame. The load reads the version and
//! checks what else the unit depends on, a seam of the run-time seal
//! ([`crate::exec::compiles::seam`]); a unit the cache does not hold is
//! compiled in a fault ([`crate::exec::compiles::serving_fault`]) and kept.

use std::sync::Arc;

use super::program::Program;
use crate::exec::planner::Planner;
use crate::exec::{EvalContext, ExecutionContext, FlowResult};
use crate::expr::ControlFlow;
use crate::val::Value;

fn internal(why: String) -> ControlFlow {
	ControlFlow::Err(anyhow::Error::new(crate::err::EngineError::Internal(why)))
}

/// The parameter a call unit reads its argument at `at` from.
fn arg_name(at: usize) -> String {
	format!("__call_arg_{at}")
}

/// What a call is made under, apart from its context values: the
/// transaction, how many catalog versions it has stamped, the principal, the
/// permission contract and the database.
#[derive(Clone)]
struct Scope {
	txn: Arc<crate::kvs::Transaction>,
	stamps: (u64, u64),
	auth: Arc<crate::iam::Auth>,
	perms: bool,
	ids: (crate::catalog::NamespaceId, crate::catalog::DatabaseId),
}

impl Scope {
	/// The scope of a call made in `exec`; `None` where it has no transaction
	/// or database.
	fn of(exec: &ExecutionContext) -> Option<Self> {
		let txn = exec.ctx().try_tx()?;
		let db = exec.database().ok()?;
		let root = exec.root();
		Some(Self {
			txn: Arc::clone(txn),
			stamps: (txn.catalog_stamps(), txn.catalog_object_stamps()),
			auth: Arc::clone(&root.auth),
			perms: root.skip_fetch_perms || root.perms_suppressed,
			ids: (db.ns_ctx.ns.namespace_id, db.db.database_id),
		})
	}

	/// Whether a unit linked under `self` serves a call made under `other`.
	fn serves(&self, other: &Self) -> bool {
		Arc::ptr_eq(&self.txn, &other.txn)
			&& self.stamps == other.stamps
			&& Arc::ptr_eq(&self.auth, &other.auth)
			&& self.perms == other.perms
			&& self.ids == other.ids
	}
}

/// The call unit a call site linked, held by the run for the site's next
/// call: it stands while the call is made in the same transaction, which has
/// stamped no catalog version since, for the same principal, permission
/// contract and database, and under context values the unit was lowered
/// against. Every catalog write of the transaction stamps a version, so a
/// call after one loads the unit again.
pub(super) struct Linked {
	scope: Scope,
	context: Arc<super::units::ContextDeps>,
	unit: Arc<Program>,
}

/// Call `fn::name` with `args` from `ctx`, through the call unit of the
/// definition the transaction reads now, continuing the computation-depth
/// count from `plan_depth`. `held` is the unit the call site linked at its
/// last call ([`Linked`]): reused while it stands, and replaced otherwise.
pub(super) async fn call(
	name: &str,
	plan_depth: u32,
	args: Vec<Value>,
	ctx: &EvalContext<'_>,
	held: &mut Option<Linked>,
) -> FlowResult<Value> {
	let exec = ctx.exec_ctx;
	let scope = Scope::of(exec);
	let standing = match (held.as_ref(), &scope) {
		(Some(linked), Some(scope))
			if linked.scope.serves(scope) && linked.context.hold(exec.ctx()) =>
		{
			Some(Arc::clone(&linked.unit))
		}
		_ => None,
	};
	let unit = match standing {
		Some(unit) => unit,
		None => {
			let (unit, context) = load(name, args.len(), plan_depth, exec).await?;
			*held = match (scope, context) {
				(Some(scope), Some(context)) => Some(Linked {
					scope,
					context,
					unit: Arc::clone(&unit),
				}),
				_ => None,
			};
			unit
		}
	};
	let running = exec.with_params(args.into_iter().enumerate().map(|(at, v)| (arg_name(at), v)));
	match crate::exec::trampoline::nest(super::host::gather_program(unit, running)).await {
		Ok(value) | Err(ControlFlow::Return(value)) => Ok(value),
		Err(other) => Err(other),
	}
}

/// The planner a call unit is compiled with for a call made in `exec`: the
/// unit serves every principal of the frame's class, so it reads the
/// arguments and the session's own values when it runs.
fn unit_planner(exec: &ExecutionContext, arity: usize, plan_depth: u32) -> Option<Planner<'_>> {
	let txn = exec.ctx().try_tx().map(Arc::clone)?;
	let (ns, db) = super::closure::names_of(exec);
	// A frame that waives permissions plans with none applied.
	let root = exec.root();
	let planner = Planner::with_txn(exec.ctx(), exec.function_registry(), txn, ns, db)
		.with_auth(Arc::clone(&root.auth))
		.with_perms_suppressed(root.skip_fetch_perms || root.perms_suppressed)
		.with_depth(plan_depth)
		// A call under `VERSION` reads the definitions the stamp holds.
		.at_stamp(exec.version_stamp());
	Some(planner.shadowing((0..arity).map(arg_name)).reading_session_values())
}

/// The call unit of `fn::name` called with `arity` arguments from `exec`,
/// with the context values it was lowered against where the compile cache
/// keeps it.
async fn load(
	name: &str,
	arity: usize,
	plan_depth: u32,
	exec: &ExecutionContext,
) -> FlowResult<(Arc<Program>, Option<Arc<super::units::ContextDeps>>)> {
	use super::compile_cache::UnitObject;
	crate::exec::compiles::seam(async {
		let planner = unit_planner(exec, arity, plan_depth)
			.ok_or_else(|| internal("a function called with no transaction".to_owned()))?;
		let txn = Arc::clone(
			planner
				.txn()
				.ok_or_else(|| internal("a function called with no transaction".to_owned()))?,
		);
		let frame = super::lower::statement_frame(&planner);
		let key = planner.function_unit_key(name).await.map(|mut key| {
			key.object = UnitObject::FunctionCall(name.to_owned(), arity);
			key
		});
		// A call at a `VERSION` stamp is compiled from the definitions the
		// stamp holds, and the compile cache keeps no unit of it.
		let units =
			exec.ctx().compiled_units().map(Arc::clone).filter(|_| exec.version_stamp().is_none());
		let (Some(units), Some(key)) = (units, key) else {
			// A function the catalog does not hold, or no node cache:
			// compiled for this call alone, where the call fails as the
			// catalog says.
			return crate::exec::compiles::serving_fault(async {
				compile(&planner, name, arity).await.map(|unit| (Arc::new(unit), None))
			})
			.await;
		};
		if let Some((kept, context)) =
			units.function_calls().lookup_with_context(&key, &frame, &txn, exec.ctx()).await
		{
			return Ok((kept, Some(context)));
		}
		let compiled = crate::exec::compiles::serving_fault(async {
			tracing::trace!(function = name, "function call fault");
			units
				.function_calls()
				.get_or_compile(&key, &frame, &txn, exec.ctx(), || async {
					let recording = txn.record_catalog_reads();
					let lowered = compile(&planner, name, arity).await;
					let recorded = recording.finish();
					let program = Arc::new(lowered?);
					let deps = match crate::exec::catalog_deps::CatalogDeps::resolve(
						&txn, &recorded,
					)
					.await
					{
						Some(deps) => super::units::ContextDeps::resolve(
							&txn,
							&recorded,
							planner.frozen(),
							std::collections::BTreeSet::new(),
						)
						.await
						.map(|context| {
							(deps, context.without(crate::exec::planner::UNIT_RUNTIME_PARAMS))
						}),
						None => None,
					};
					Ok::<_, ControlFlow>((program, deps))
				})
				.await
		})
		.await?;
		// What the fault compiled is linked under the context values the
		// cache kept it with; one it kept for this request alone is not.
		let context = units
			.function_calls()
			.lookup_with_context(&key, &frame, &txn, exec.ctx())
			.await
			.filter(|(kept, _)| Arc::ptr_eq(kept, &compiled))
			.map(|(_, context)| context);
		Ok((compiled, context))
	})
	.await
}

/// The statement `fn::name($0, …)` with `arity` arguments, bound, mapped and
/// lowered by `planner`.
async fn compile(planner: &Planner<'_>, name: &str, arity: usize) -> FlowResult<Program> {
	use crate::expr::logical::{LogicalCall, LogicalEval, LogicalExpr, LogicalOp};
	let call = LogicalOp::Eval(LogicalEval {
		expr: LogicalExpr::Call(Box::new(LogicalCall {
			receiver: crate::expr::Function::Custom(name.to_owned()),
			args: (0..arity)
				.map(|at| LogicalExpr::Param(crate::expr::Param::from(arg_name(at))))
				.collect(),
		})),
	});
	lower_statement(planner, call).await
}

/// `op`, bound, mapped and lowered by `planner`. A statement holding a schema
/// statement, or calling a body that holds one, whose effect the plan cannot
/// know is lowered again with what follows each schema statement late-bound,
/// as a statement of a query is ([`Planner::binding_late_after_schema`]). An
/// internal refusal is an invariant the lowering broke, and is raised rather
/// than retried.
async fn lower_statement(
	planner: &Planner<'_>,
	op: crate::expr::logical::LogicalOp,
) -> FlowResult<Program> {
	let refused = |why: super::NotCovered| ControlFlow::Err(super::declined(why).into());
	let not_covered = match lower_once(planner, op.clone()).await? {
		Ok(program) => return Ok(program),
		Err(not_covered) => not_covered,
	};
	if !not_covered.internal
		&& !planner.binds_late_after_schema()
		&& (not_covered.reason == super::speculate::DATA_DEPENDENT_DDL
			|| crate::exec::planner::fold::changes_the_catalog(&op))
	{
		let late = planner.binding_late_after_schema();
		return lower_once(&late, op).await?.map_err(refused);
	}
	Err(refused(not_covered))
}

/// `op`, bound, mapped and lowered by `planner`, or why lowering declines it.
async fn lower_once(
	planner: &Planner<'_>,
	op: crate::expr::logical::LogicalOp,
) -> FlowResult<Result<Program, super::NotCovered>> {
	let bound = crate::exec::planner::bind_statement(planner, op)
		.await
		.map_err(|e| ControlFlow::Err(e.into()))?;
	let mapped = match super::map::map(planner, &bound).await {
		Ok(mapped) => mapped,
		Err(not_covered) => return Ok(Err(not_covered)),
	};
	let physical = super::optimize::optimize(mapped);
	Ok(super::lower::lower(planner, &physical).await)
}

/// Run the body of `fn::name` for the call `ctx` is the body's evaluation of:
/// the body compiled for the values the call's arguments hold, which a body
/// reading an argument where only a value answers needs
/// ([`super::program::ExternalCallee::Specialised`]). The unit is kept by the
/// function's version and those values.
pub(super) async fn specialised(name: &str, ctx: &EvalContext<'_>) -> FlowResult<Value> {
	let exec = ctx.exec_ctx;
	let unit = load_specialised(name, exec).await?;
	match crate::exec::trampoline::nest(super::host::gather_program(unit, exec.clone())).await {
		Ok(value) | Err(ControlFlow::Return(value)) => Ok(value),
		Err(other) => Err(other),
	}
}

/// The body of `fn::name` compiled for the arguments `exec` binds.
async fn load_specialised(name: &str, exec: &ExecutionContext) -> FlowResult<Arc<Program>> {
	use super::compile_cache::UnitObject;
	crate::exec::compiles::seam(async {
		let txn = exec
			.ctx()
			.try_tx()
			.map(Arc::clone)
			.ok_or_else(|| internal(format!("fn::{name} run with no transaction")))?;
		let db_ctx = exec.database().map_err(|e| ControlFlow::Err(e.into()))?;
		let (ns, db) = (db_ctx.ns_ctx.ns.namespace_id, db_ctx.db.database_id);
		let cache = exec.ctx().get_cache();
		let snapshot = crate::kvs::cache::ds::FunctionSnapshot::for_function(
			cache.as_deref(),
			&txn,
			ns,
			db,
			name,
		)
		.await
		.map_err(ControlFlow::Err)?
		.ok_or_else(|| internal(format!("fn::{name} is gone while its call runs")))?;
		let args: Vec<Value> = snapshot
			.definition()
			.args
			.iter()
			.map(|(arg, _)| exec.value(arg.as_str()).cloned().unwrap_or(Value::None))
			.collect();
		let (ns_name, db_name) = super::closure::names_of(exec);
		// The call's frame is the definer's narrowed principal, which the
		// arguments are bound in as values for binding to fold. A frame that
		// waives permissions, as a permission predicate's does, plans with
		// none applied: the reads in it return every row, as the frame runs
		// them. The body serves every principal of the frame's class, so it
		// reads the session's own values when it runs.
		let root = exec.root();
		let planner = Planner::with_txn(
			exec.ctx(),
			exec.function_registry(),
			Arc::clone(&txn),
			ns_name,
			db_name,
		)
		.with_auth(Arc::clone(&root.auth))
		.with_perms_suppressed(root.skip_fetch_perms || root.perms_suppressed)
		.with_depth(super::closure::depth_of(exec))
		.reading_session_values();
		let body = snapshot.body().map_err(ControlFlow::Err)?.clone();
		let function = planner.function_unit_key(name).await;
		let units = exec.ctx().compiled_units().map(Arc::clone);
		let (Some(units), Some(function)) = (units, function) else {
			return compile_body(&planner, body).await;
		};
		let frame = super::lower::statement_frame(&planner);
		// The values are keyed type-exact. A body compiled for values too large
		// to keep, or for values with no encoding, is compiled for this call
		// alone.
		let key = super::compile_cache::ArgValues::of(&args, units.specialised_arg_bytes()).map(
			|values| {
				let mut key = function.clone();
				key.object = UnitObject::SpecialisedBody(name.to_owned(), values);
				key
			},
		);
		if let Some(key) = &key
			&& let Some(kept) =
				units.specialised_bodies().lookup(key, &frame, &txn, exec.ctx()).await
		{
			return Ok(kept);
		}
		// A function whose calls brought more distinct values than its body
		// is compiled for runs its body compiled to read the arguments when it
		// runs, where that answers alike ([`reads_arguments_alike`]) and
		// compiles.
		let mut generic = function;
		generic.object = UnitObject::GenericBody(name.to_owned());
		if units.specialised_enough(&generic, false) && reads_arguments_alike(&body) {
			let names = snapshot.definition().args.iter().map(|(arg, _)| arg.as_str().to_owned());
			let reading = planner.shadowing(names);
			let held = units
				.generic_bodies()
				.get_or_compile(&generic, &frame, &txn, exec.ctx(), || async {
					let recording = txn.record_catalog_reads();
					let compiled = compile_body(&reading, body.clone()).await.ok();
					let recorded = recording.finish();
					let deps = kept_deps(&txn, &recorded, &planner).await;
					Ok::<_, ControlFlow>((compiled, deps))
				})
				.await?;
			if let Some(body) = held {
				return Ok(body);
			}
		}
		units.specialised_enough(&generic, true);
		let Some(key) = key else {
			return compile_body(&planner, body).await;
		};
		units
			.specialised_bodies()
			.get_or_compile(&key, &frame, &txn, exec.ctx(), || async {
				let recording = txn.record_catalog_reads();
				let compiled = compile_body(&planner, body.clone()).await;
				let recorded = recording.finish();
				let program = compiled?;
				let deps = kept_deps(&txn, &recorded, &planner).await;
				Ok::<_, ControlFlow>((program, deps))
			})
			.await
	})
	.await
}

/// What else a unit compiled by `planner` depended on, from the catalog reads
/// `recorded` noted: `None` where those cannot be named.
async fn kept_deps(
	txn: &crate::kvs::Transaction,
	recorded: &crate::kvs::catalog_reads::CatalogReads,
	planner: &Planner<'_>,
) -> Option<(crate::exec::catalog_deps::CatalogDeps, super::units::ContextDeps)> {
	let deps = crate::exec::catalog_deps::CatalogDeps::resolve(txn, recorded).await?;
	super::units::ContextDeps::resolve(txn, recorded, planner.frozen(), Default::default())
		.await
		.map(|context| (deps, context.without(crate::exec::planner::UNIT_RUNTIME_PARAMS)))
}

/// Function body `body`, bound, mapped and lowered by `planner` in a fault.
async fn compile_body(
	planner: &Planner<'_>,
	body: crate::expr::logical::LogicalOp,
) -> FlowResult<Arc<Program>> {
	crate::exec::compiles::serving_fault(async {
		lower_statement(planner, body).await.map(Arc::new)
	})
	.await
}

/// Whether function body `body` answers alike compiled for its arguments'
/// values and compiled to read them when it runs: a value there only chooses
/// how a read is made. A full-text or nearest-neighbour operator, and an index
/// function that reads what such an operator's scan found, answer otherwise
/// where the operand is read when the body runs, since no scan serves it.
fn reads_arguments_alike(body: &crate::expr::logical::LogicalOp) -> bool {
	use crate::expr::logical::LogicalExpr;
	use crate::expr::{BinaryOperator, Function};
	let mut alike = true;
	let _ = body.visit_scalars_deep(&mut |scalar| {
		if scalar.any(|expr| match expr {
			LogicalExpr::Binary {
				op: BinaryOperator::Matches(_) | BinaryOperator::NearestNeighbor(_),
				..
			} => true,
			LogicalExpr::Call(call) => matches!(
				&call.receiver,
				Function::Normal(name)
					if name.starts_with("search::") || name.starts_with("vector::distance::knn")
			),
			_ => false,
		}) {
			alike = false;
		}
		Ok::<(), std::convert::Infallible>(())
	});
	alike
}
