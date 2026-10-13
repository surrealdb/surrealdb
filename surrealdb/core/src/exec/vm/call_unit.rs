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
//! A function body is a barrier to `BREAK` and `CONTINUE`: the loop they name
//! is not written in it, so a loop signal that leaves the unit raises
//! [`ExecError::InvalidControlFlow`]. A `RETURN` in the body is the call's
//! value.
//!
//! Call units are served from the compile cache ([`super::compile_cache`]),
//! keyed by the function's name and arity, the version of its definition and
//! the build, with one compilation per frame. The load reads the version and
//! checks what else the unit depends on, a seam of the run-time seal
//! ([`crate::exec::compiles::seam`]). A unit the cache does not hold is
//! compiled under [`crate::exec::compiles::serving_fault`] and then kept.

use std::sync::Arc;

use super::program::Program;
use crate::exec::planner::Planner;
use crate::exec::{Error as ExecError, EvalContext, ExecutionContext, FlowResult};
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
	// The body reads the caller's value in scope as its document, `$this` and
	// `$parent`, as an inlined body reads the row it is written against: a
	// permission judged over a row with its computed fields derived hands the
	// body that row.
	let running = exec
		.entering_statement(ctx.value_in_scope(), true)
		.with_params(args.into_iter().enumerate().map(|(at, v)| (arg_name(at), v)));
	match crate::exec::trampoline::nest(super::host::gather_program(unit, running)).await {
		Ok(value) | Err(ControlFlow::Return(value)) => Ok(value),
		Err(ControlFlow::Break | ControlFlow::Continue) => {
			Err(ExecError::InvalidControlFlow.into())
		}
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
		let frame = super::statement_frame(&planner);
		let key = planner.function_unit_key(name).await.map(|mut key| {
			key.object = UnitObject::FunctionCall(name.to_owned(), arity);
			key
		});
		// A call at a `VERSION` stamp is compiled from the definitions the
		// stamp holds, and the compile cache keeps no unit of it.
		let units =
			exec.ctx().compiled_units().map(Arc::clone).filter(|_| exec.version_stamp().is_none());
		let (Some(units), Some(key)) = (units, key) else {
			// A function the catalog does not hold, or a frame with no unit cache,
			// is compiled for this call alone. The call then fails as the catalog
			// says.
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
		// The unit is linked under the context values the cache stores it with.
		// When the cache does not return the compiled unit under those values, no
		// context is returned and the call site keeps nothing.
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

/// The statement `fn::name($0, …)` with `arity` arguments, bound, planned and
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

/// `op`, bound, planned and lowered by `planner`. A statement holding a schema
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
	if not_covered.kind != super::NotCoveredKind::Internal
		&& !planner.binds_late_after_schema()
		&& (not_covered.kind == super::NotCoveredKind::LateBind
			|| crate::exec::planner::fold::changes_the_catalog(&op))
	{
		let late = planner.binding_late_after_schema();
		return lower_once(&late, op).await?.map_err(refused);
	}
	Err(refused(not_covered))
}

/// `op`, bound, planned and lowered by `planner`, or why the VM declines it.
async fn lower_once(
	planner: &Planner<'_>,
	op: crate::expr::logical::LogicalOp,
) -> FlowResult<Result<Program, super::NotCovered>> {
	let bound = crate::exec::planner::bind_statement(planner, op)
		.await
		.map_err(|e| ControlFlow::Err(e.into()))?;
	// A unit the run loads is cached under its own key and served to any frame that
	// asks for it. Its pipelines are placed in the scope of its own statement alone,
	// whatever statement the call is made from.
	Ok(super::compile::compile_unit(planner, &bound).await)
}
