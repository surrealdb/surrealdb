//! The physical plan and the bytecode VM that runs it.
//!
//! A bound [`LogicalOp`] takes three steps here, each over the representation
//! the previous one produced:
//!
//! 1. [`map`] translates it into an inert [`PhysicalPlan`], reading from the catalog only what a
//!    node needs to exist and recording each read as a dependency.
//! 2. [`optimize`] rewrites the plan in place. Every rule is an equivalence: it changes how the
//!    statement runs, never its rows, their order, its errors or how often anything is evaluated.
//! 3. [`lower`] compiles the plan into a [`Program`](program::Program) and makes no decisions.
//!
//! The program is a sequence of pipelines, each a synchronous loop over the
//! rows of its source, between the nodes that hold rows — sorts and
//! aggregates. Everything that awaits — reading the next rows from storage,
//! evaluating an expression the VM has no instruction for — is a point where
//! the interpreter hands control back to the async host ([`host`]), which does
//! the work and resumes it. A pipeline with nothing to hand back runs inside
//! the storage cursor, on the stored bytes.
//!
//! The VM is the one engine: a statement whose plan holds a node it does not
//! cover fails with an error that names the construct ([`plan_checked`]), and
//! nothing else runs it. The contract every node must keep is
//! `doc/executor/PHYSICAL_PLAN_REQUIREMENTS.md`.

#[macro_use]
pub(crate) mod plain;

mod binding;
mod block;
mod call_unit;
pub(crate) mod closure;
pub(crate) mod compile_cache;
mod count;
pub(crate) mod descriptor;
pub(crate) mod dispatch;
pub(crate) mod footprint;
pub(crate) mod serve;
pub(crate) use dispatch::Deref;
pub(crate) use map::catalog_within;
pub(crate) use program::ClosureDigest;
pub(crate) mod codec;
pub(crate) mod depth;
pub(crate) mod explain;
mod external;
mod fault;
pub(crate) mod fuel;
mod function;
mod graph;
mod host;
pub(crate) use host::out_of_line;
mod info;
mod inline;
mod interp;
mod knn;
mod leaf;
pub(crate) use subroutine::Subroutine;
pub(crate) use write::notify_at;
#[cfg(test)]
mod golden;
#[cfg(test)]
mod layout;
pub(crate) mod live;
mod lower;
pub(crate) mod lower_live;
mod map;
mod match_plan;
mod nested;
mod omit;
mod optimize;
mod param_unit;
mod plan;
pub(crate) mod plan_catalog;
pub(crate) mod program;
pub(crate) mod request;
#[cfg(all(test, feature = "kv-mem"))]
mod tests_body_scope;
#[cfg(all(test, feature = "kv-mem"))]
mod tests_catalog_units;
#[cfg(all(test, feature = "kv-mem"))]
mod tests_counts;
#[cfg(all(test, feature = "kv-mem"))]
mod tests_dispatch;
#[cfg(all(test, feature = "kv-mem"))]
mod tests_late_layers;
#[cfg(all(test, feature = "kv-mem"))]
mod tests_principal;
#[cfg(all(test, feature = "kv-mem"))]
mod tests_running_compiles;
#[cfg(all(test, feature = "kv-mem"))]
mod tests_stack;
// Its golden records a build that runs no script and no model.
#[cfg(all(test, feature = "kv-mem", not(any(feature = "scripting", feature = "ml"))))]
mod tests_external;

/// The units of a root program or a function bundle, which the ids of the
/// programs beneath it index.
pub(crate) type UnitTable = Arc<[Arc<program::Program>]>;

/// The late-bound statements of a program, as written, by
/// [`program::Program::served`] index: kept beside the program by the
/// operator that runs it, since a program holds no logical plan.
pub(crate) type ServedStatements = Arc<[Arc<LogicalOp>]>;
mod backfill;
mod batch;
mod region;
mod row;
mod scalar;
mod schema_host;
mod services;
mod sort;
pub(crate) mod speculate;
#[cfg(all(storage, not(target_family = "wasm")))]
mod spill;
mod store;
mod subroutine;
#[cfg(all(test, feature = "kv-mem"))]
mod tests;
#[cfg(all(test, feature = "kv-mem"))]
mod tests_aggregate;
#[cfg(all(test, feature = "kv-mem"))]
mod tests_census;
#[cfg(all(test, feature = "kv-mem"))]
mod tests_closure;
#[cfg(all(test, feature = "kv-mem"))]
mod tests_ctl;
#[cfg(all(test, feature = "kv-mem"))]
mod tests_enforced;
#[cfg(all(test, feature = "kv-mem"))]
mod tests_explain;
#[cfg(all(test, feature = "kv-mem"))]
mod tests_frames;
#[cfg(all(test, feature = "kv-mem", any(feature = "gql", feature = "postgres")))]
mod tests_front_end;
#[cfg(all(test, feature = "kv-mem"))]
mod tests_graph;
#[cfg(all(test, feature = "kv-mem"))]
mod tests_index_fns;
#[cfg(all(test, feature = "kv-mem"))]
mod tests_invocations;
#[cfg(all(test, feature = "kv-mem"))]
mod tests_join;
#[cfg(all(test, feature = "kv-mem"))]
mod tests_kept;
#[cfg(all(test, feature = "kv-mem"))]
mod tests_leaf;
#[cfg(all(test, feature = "kv-mem"))]
mod tests_lookups;
#[cfg(test)]
mod tests_methods;
#[cfg(all(test, feature = "kv-mem"))]
mod tests_needs;
#[cfg(all(test, feature = "kv-mem"))]
mod tests_overlay;
#[cfg(all(test, feature = "kv-mem"))]
mod tests_paths;
#[cfg(test)]
mod tests_plain;
#[cfg(all(test, feature = "kv-mem"))]
mod tests_plain_data;
#[cfg(all(test, feature = "kv-mem"))]
mod tests_preempt;
#[cfg(all(test, feature = "kv-mem"))]
mod tests_recurse;
#[cfg(all(test, feature = "kv-mem", not(target_family = "wasm")))]
mod tests_scan_parity;
#[cfg(all(test, feature = "kv-mem"))]
mod tests_sources;
#[cfg(all(test, feature = "kv-mem"))]
mod tests_subq;
#[cfg(all(test, feature = "kv-mem"))]
mod tests_units;
#[cfg(all(test, feature = "kv-mem"))]
mod tests_wide;
#[cfg(all(test, feature = "kv-mem"))]
mod tests_write_data;
#[cfg(all(test, feature = "kv-mem"))]
mod tests_write_gate;
#[cfg(all(test, feature = "kv-mem"))]
mod tests_write_observe;
#[cfg(all(test, feature = "kv-mem"))]
mod tests_write_schema;
#[cfg(all(test, feature = "kv-mem"))]
mod tests_write_storage;
pub(crate) mod units;
mod write;

use std::sync::Arc;

use crate::exec::ExecOperator;
use crate::exec::planner::Planner;
use crate::expr::logical::LogicalOp;

/// The error a statement fails with when the VM does not run it.
///
/// A decline is a failure: nothing else executes a statement. The error names
/// the construct the lowering did not cover, and is a statement-level
/// `PlannerUnsupported`, not an internal error. A lowering that broke an
/// invariant of every program ([`NotCovered::internal`]) is an internal error,
/// and a statement the planner refuses as it plans it
/// ([`NotCovered::refusal`]) fails with the refusal.
pub(super) fn declined(not_covered: NotCovered) -> crate::err::Error {
	let NotCovered {
		reason,
		internal,
		refusal,
	} = not_covered;
	if internal {
		return crate::err::Error::Engine(crate::err::EngineError::Internal(reason));
	}
	if let Some(refusal) = refusal {
		return refusal.error();
	}
	tracing::debug!(%reason, "the VM does not run the statement");
	crate::err::Error::Exec(crate::exec::Error::PlannerUnsupported {
		reason,
	})
}

/// Plan `op`, already bound, to run on the VM: `None` where the statement
/// fails to plan, for a test that asks only whether it does.
#[cfg(test)]
pub(crate) async fn plan(planner: &Planner<'_>, op: &LogicalOp) -> Option<Arc<dyn ExecOperator>> {
	plan_checked(planner, op, None).await.ok()
}

/// Plan `op`, already bound, to run on the VM, or fail with the reason the VM
/// does not run it.
///
/// `unbound` is the statement `op` was bound from, when it is a block, `FOR`,
/// `LET` or expression the planning may plan again late-bound after its
/// schema statements: each statement it holds is bound again where it is
/// planned, the ones after a change late-bound, from the statement as
/// written.
pub(crate) fn plan_checked<'a>(
	planner: &'a Planner<'_>,
	op: &'a LogicalOp,
	unbound: Option<&'a LogicalOp>,
) -> crate::exec::BoxFut<'a, Result<Arc<dyn ExecOperator>, crate::err::Error>> {
	// A boxed `dyn` future: what planning awaits nests deeply, and a caller
	// proving a statement's future `Send` stops at the box.
	Box::pin(async move {
		if let LogicalOp::Explain(explain) = op {
			// Nothing of the explained statement runs under a plain `EXPLAIN`: the
			// ids its planning held are released once it is rendered.
			let mark = planner.speculation.as_ref().map(|speculating| speculating.held.mark());
			let planned = match map::explained_plan(planner, &explain.input).await {
				Ok(planned) => planned,
				Err(not_covered) => return Err(declined(not_covered)),
			};
			// Explaining a statement the planner refuses to plan
			// raises the refusal, as planning the statement does.
			if let Some(refusal) = planned.refusal() {
				return Err(refusal.error());
			}
			// The VM explains a statement it runs: one it cannot lower is declined
			// as it is when it is run.
			let inner_exec = match lower_to_program(planner, &planned).await {
				Ok(program) => wrap_as_operator(planner, program, &planned),
				Err(not_covered) => return Err(declined(not_covered)),
			};
			let explain_node = explain::to_explain_operator(
				&explain.input,
				&planned,
				if explain.analyze {
					Some(&inner_exec)
				} else {
					None
				},
				planner.reads_are_checked(),
			);
			if !explain.analyze
				&& let (Some(speculating), Some(mark)) = (&planner.speculation, mark)
			{
				speculating.held.release_since(mark);
			}
			let plan: Arc<dyn ExecOperator> = if explain.analyze {
				Arc::new(crate::exec::operators::AnalyzePlan {
					plan: explain_node,
					format: explain.format,
					redact_volatile_explain_attrs: planner.frozen().redact_volatile_explain_attrs(),
				})
			} else {
				Arc::new(crate::exec::operators::ExplainPlan {
					plan: explain_node,
					format: explain.format,
				})
			};
			return Ok(plan);
		}
		let not_covered = match plan_statement(planner, op).await? {
			Ok(plan) => return Ok(plan),
			Err(not_covered) => not_covered,
		};
		// A statement holding a schema statement, or reaching code that may run
		// one, whose effect the plan cannot know is planned again late-bound
		// after its schema statements: what follows such a statement loads each
		// catalog object it names, and resolves the session's names again, when
		// it runs. Nothing is planned once the program runs. An internal refusal
		// is an invariant the planning broke, and is raised rather than retried.
		if !not_covered.internal
			&& !planner.binds_late_after_schema()
			&& (not_covered.reason == speculate::DATA_DEPENDENT_DDL
				|| crate::exec::planner::fold::changes_the_catalog(op))
		{
			// What the first planning held belongs to no program.
			if let Some(speculating) = &planner.speculation {
				speculating.held.release_all();
			}
			let late = planner.binding_late_after_schema();
			return match plan_statement(&late, unbound.unwrap_or(op)).await? {
				Ok(plan) => Ok(plan),
				Err(not_covered) => Err(declined(not_covered)),
			};
		}
		Err(declined(not_covered))
	})
}

/// The statements of a request lowered so far, and the units they share.
#[derive(Default)]
pub(crate) struct RequestBuilder {
	units: lower::RequestUnits,
	statements: Vec<Arc<program::Program>>,
	/// Each statement's late-bound block statements, as written, which its
	/// blocks serve when they reach them.
	served: Vec<Option<ServedStatements>>,
	/// The catalog ids each statement's planning held, which its run takes.
	held: Vec<Arc<speculate::HeldIds>>,
}

/// Where a statement of a request was compiled to.
pub(crate) enum RequestEntry {
	/// Statement `index` of the request's program.
	Statement(u32),
	/// An operator of its own, planned on a path that lowers no statement
	/// into the request.
	Operator(Arc<dyn ExecOperator>),
}

impl RequestBuilder {
	/// Plan `op`, bound by `planner`, as the next statement of the request,
	/// as [`super::plan_checked`] plans a statement on its own.
	pub(crate) async fn plan(
		&mut self,
		planner: &Planner<'_>,
		op: &LogicalOp,
		unbound: Option<&LogicalOp>,
	) -> Result<RequestEntry, crate::err::Error> {
		// A plain `EXPLAIN` answers with the rendering of another statement's
		// plan, which its own operator makes.
		if matches!(op, LogicalOp::Explain(_)) {
			return plan_checked(planner, op, unbound).await.map(RequestEntry::Operator);
		}
		let not_covered = match self.plan_statement(planner, op).await? {
			Ok(index) => return Ok(RequestEntry::Statement(index)),
			Err(not_covered) => not_covered,
		};
		// As `plan_checked`: a statement holding a schema statement whose
		// effect the plan cannot know is planned again late-bound.
		if !not_covered.internal
			&& !planner.binds_late_after_schema()
			&& (not_covered.reason == speculate::DATA_DEPENDENT_DDL
				|| crate::exec::planner::fold::changes_the_catalog(op))
		{
			if let Some(speculating) = &planner.speculation {
				speculating.held.release_all();
			}
			let late = planner.binding_late_after_schema();
			return match self.plan_statement(&late, unbound.unwrap_or(op)).await? {
				Ok(index) => Ok(RequestEntry::Statement(index)),
				Err(not_covered) => Err(declined(not_covered)),
			};
		}
		Err(declined(not_covered))
	}

	/// Map, optimize and lower `op` into the request, answering its index, or
	/// the reason the VM does not cover it. What the statement lowered into
	/// the request's units is taken back out where it is not covered.
	async fn plan_statement(
		&mut self,
		planner: &Planner<'_>,
		op: &LogicalOp,
	) -> Result<Result<u32, NotCovered>, crate::err::Error> {
		let physical = match map::map(planner, op).await {
			Ok(physical) => physical,
			Err(not_covered) => return Ok(Err(not_covered)),
		};
		if let Some(refusal) = physical.refusal() {
			return Err(refusal.error());
		}
		let physical = optimize::optimize(physical);
		let mark = self.units.len();
		let (program, served) =
			match lower::lower_request_statement(planner, &physical, &mut self.units).await {
				Ok(lowered) => lowered,
				Err(not_covered) => {
					self.units.rewind(mark);
					return Ok(Err(not_covered));
				}
			};
		if leaf::reporting() {
			leaf::report(op, &program, planner.function_registry());
		}
		let Ok(index) = u32::try_from(self.statements.len()) else {
			return Ok(Err(NotCovered::internal("more statements than a request holds")));
		};
		self.statements.push(Arc::new(program));
		self.served.push((!served.is_empty()).then(|| served.into()));
		if let Some(speculating) = &planner.speculation
			&& !speculating.held.is_empty()
		{
			self.held.push(Arc::clone(&speculating.held));
		}
		Ok(Ok(index))
	}

	/// The program of the request: `steps`, and the statements lowered into
	/// it, with the units they share.
	pub(crate) fn finish(
		self,
		planner: &Planner<'_>,
		steps: Vec<program::RequestStep>,
	) -> Result<request::CompiledRequest, crate::err::Error> {
		let request = program::RequestCode {
			steps,
			statements: self.statements,
		};
		let program = lower::request_program(planner, self.units, request).map_err(declined)?;
		#[cfg(test)]
		request::LAST.with(|last| {
			*last.borrow_mut() = program.request.as_ref().map(|request| {
				(request.steps.len(), request.statements.len(), program.units.len())
			});
		});
		Ok(request::CompiledRequest {
			program: Arc::new(program),
			served: self.served,
			held: self.held,
		})
	}
}

/// Whether binding `op` with `planner` folds a call that reads the clock, in
/// `op` or in the body of a user-defined function it reaches: a plan of it
/// holds the time it was bound at.
pub(crate) async fn binds_the_clock(op: &LogicalOp, planner: &Planner<'_>) -> bool {
	map::binds_the_clock(op, planner).await
}

/// Map, optimize and lower the bound statement `op` to an operator, or the
/// reason the VM does not cover it. A statement the planner refuses raises the
/// refusal here, before any of it runs.
async fn plan_statement(
	planner: &Planner<'_>,
	op: &LogicalOp,
) -> Result<Result<Arc<dyn ExecOperator>, NotCovered>, crate::err::Error> {
	let physical = match map::map(planner, op).await {
		Ok(physical) => physical,
		Err(not_covered) => return Ok(Err(not_covered)),
	};
	if let Some(refusal) = physical.refusal() {
		return Err(refusal.error());
	}
	let physical = optimize::optimize(physical);
	match lower_to_program(planner, &physical).await {
		Ok(program) => {
			if leaf::reporting() {
				leaf::report(op, &program.0, planner.function_registry());
			}
			Ok(Ok(wrap_as_operator(planner, program, &physical)))
		}
		Err(not_covered) => Ok(Err(not_covered)),
	}
}

/// Plan `op`, a plan a front end emitted rather than a statement, to run on
/// the VM.
///
/// The plan is compiled with no binding step: the front end owns what it
/// asks for, and its tables are read as an unbound statement's are, each row
/// put through the table's enforcement. A GQL `MATCH` is folded to a program
/// of binding steps of its own ([`plan_match`]); a relational plan (the SQL
/// front ends') is mapped, optimised and lowered as a statement is, its
/// joins, `DISTINCT` and set operations among the pipeline's own nodes. A plan
/// the VM does not cover fails as a statement it does not cover does, and no
/// other engine compiles it. What the planner refuses as it compiles the plan
/// is raised before any of it runs.
pub(crate) async fn plan_front_end(
	planner: &Planner<'_>,
	op: LogicalOp,
) -> Result<Arc<dyn ExecOperator>, crate::err::Error> {
	if let LogicalOp::Match(matched) = &op {
		return plan_match(planner, matched).await;
	}
	match plan_statement(planner, &op).await? {
		Ok(plan) => Ok(plan),
		Err(not_covered) => Err(declined(not_covered)),
	}
}

#[cfg(test)]
thread_local! {
	/// How many `MATCH` plans this thread has compiled to a program of their own.
	pub(crate) static NATIVE_MATCHES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// A GQL `MATCH` plan as a program of its own.
///
/// What the planner refuses is raised as a statement's refusal is; a shape the
/// fold cannot build, or the lowering does not cover, fails with the decline
/// error of a statement.
async fn plan_match(
	planner: &Planner<'_>,
	matched: &crate::expr::logical::LogicalMatch,
) -> Result<Arc<dyn ExecOperator>, crate::err::Error> {
	use crate::exec::planner::match_backend::Planning;
	use crate::exec::planner::match_plan::MatchFold;
	let backend = match_plan::VmBackend::new(planner);
	let root = match (MatchFold {
		backend: &backend,
	})
	.plan((*matched.plan).clone())
	.await
	{
		Ok(root) => root,
		Err(Planning::Fail(error)) => return Err(error),
		Err(Planning::Decline(not_covered)) => return Err(declined(not_covered)),
	};
	let mut physical = optimize::optimize(match_plan::physical(root, backend.dependencies()));
	let matched_op = LogicalOp::Match(matched.clone());
	let scanned: Vec<_> = physical
		.dependencies
		.iter()
		.map(
			|plan::Dependency::Table {
			     table,
			     ..
			 }| table.clone(),
		)
		.collect();
	match plan_catalog::read(planner, &matched_op, scanned.iter()).await {
		Ok(catalog) => physical.catalog = Some(Arc::new(catalog)),
		Err(not_covered) => return Err(declined(not_covered)),
	}
	if let Some(refusal) = physical.refusal() {
		return Err(refusal.error());
	}
	match lower_to_program(planner, &physical).await {
		Ok((program, served)) => {
			#[cfg(test)]
			NATIVE_MATCHES.with(|n| n.set(n.get() + 1));
			Ok(Arc::new(host::VmOperator::new(program).serving(served)))
		}
		Err(not_covered) => Err(declined(not_covered)),
	}
}

/// Whether `plan` runs on the VM.
#[cfg(test)]
pub(crate) fn runs_on_vm(plan: &dyn ExecOperator) -> bool {
	plan.name() == host::VmOperator::NAME
}

/// Lower a statement already bound and mapped to `physical` to a program,
/// compiled in full: nothing it reaches is planned or lowered while it runs.
///
/// The program carries the statement's tail — its `VERSION`, `TIMEOUT` and
/// `ONLY` — and applies it when it runs.
async fn lower_to_program(
	planner: &Planner<'_>,
	physical: &plan::PhysicalPlan,
) -> Result<(program::Program, Vec<Arc<LogicalOp>>), NotCovered> {
	let lowered = lower::lower_serving(planner, physical).await?;
	tracing::debug!("VM planned the statement");
	Ok(lowered)
}

impl Subroutine {
	/// `bound`, bound by `planner`, as a subroutine whose writes keep what
	/// they compile in `contexts`, or in contexts of its own. Fails with the
	/// reason where the VM does not cover `bound`.
	pub(crate) async fn lower(
		planner: &Planner<'_>,
		bound: &LogicalOp,
		contexts: Option<Arc<crate::exec::operators::write::scope::KeptContexts>>,
	) -> Result<Arc<Self>, crate::err::Error> {
		match lower::lower_subroutine(planner, bound).await {
			Ok(program) => Ok(Arc::new(Self::new(program, contexts))),
			Err(not_covered) => Err(declined(not_covered)),
		}
	}
}

/// `program`, lowered by `planner`, as the operator the planner and the
/// executor hold: the one adapter between a program and the operator tree
/// around it. It keeps the catalog ids the statement's planning held for as
/// long as it or a run of it lasts, since the program's draws take them.
fn wrap_as_operator(
	planner: &Planner<'_>,
	(program, served): (program::Program, Vec<Arc<LogicalOp>>),
	physical: &plan::PhysicalPlan,
) -> Arc<dyn ExecOperator> {
	let held = planner
		.speculation
		.as_ref()
		.map(|speculating| &speculating.held)
		.filter(|held| !held.is_empty())
		.cloned();
	Arc::new(
		host::VmOperator::new(program)
			.holding(held)
			.serving(served)
			.displayed_as(explain::root_name(physical)),
	)
}

/// Why the VM does not run a statement. The statement fails with it
/// ([`declined`]); an [`internal`](Self::internal) one fails as an internal
/// error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NotCovered {
	pub(crate) reason: String,
	/// Whether the lowering built a program breaking an invariant every
	/// program keeps, rather than meeting a statement it does not cover: the
	/// statement fails with an internal error and is never declined.
	pub(crate) internal: bool,
	/// The error the planner raises for a statement it refuses to plan: the
	/// statement fails with it, as it does where the refusal is found as the
	/// statement is mapped.
	pub(crate) refusal: Option<plan::Refusal>,
}

impl NotCovered {
	pub(crate) fn new(reason: impl Into<String>) -> Self {
		Self {
			reason: reason.into(),
			internal: false,
			refusal: None,
		}
	}

	/// A statement the planner refuses to plan, for `refusal`.
	pub(crate) fn refused(refusal: plan::Refusal) -> Self {
		Self {
			reason: "a statement the planner refuses to plan".to_owned(),
			internal: false,
			refusal: Some(refusal),
		}
	}

	/// A statement that needs the ids of its database at plan time, where
	/// `planner` could not resolve them: a session that selects no namespace
	/// or no database fails as it would at run time, and any other cause
	/// declines the statement.
	pub(crate) fn unresolved_database(planner: &Planner<'_>) -> Self {
		if planner.ns().is_none() {
			return Self::refused(plan::Refusal::NamespaceEmpty);
		}
		if planner.db().is_none() {
			return Self::refused(plan::Refusal::DatabaseEmpty);
		}
		Self::new("the database is not resolved at plan time")
	}

	/// A lowering that broke an invariant of every program, for `reason`.
	pub(crate) fn internal(reason: impl Into<String>) -> Self {
		Self {
			reason: reason.into(),
			internal: true,
			refusal: None,
		}
	}
}
