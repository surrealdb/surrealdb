//! Compiling a physical plan into a program.
//!
//! Lowering makes no choices. It walks the plan node by node and emits one
//! pipeline per run of streaming nodes: a pipeline's source is the node its
//! run ends at — a table scan, or a sort or aggregate, whose own input is
//! lowered first as an earlier pipeline with that node as its sink — and its
//! body is the run's nodes, innermost first, ending in the sink the pipeline
//! feeds.
//!
//! Every scalar becomes instructions, and a statement holding one the
//! instructions do not answer is not covered: it runs on the operator tree.
//! Operands are evaluated in the order the operator tree evaluates them: left
//! before right, the right-hand side of `AND` and `OR` only when the left
//! does not decide.

use std::collections::HashMap;
use std::sync::Arc;

use super::NotCovered;
use super::plain::ValueData;
use super::plan::{
	IndexScope, Omission, PhysicalPlan, Plan, Projection, SortKeyRead, SortKeySpec, Walk,
	WalkAction, WalkStep, WriteSubjects,
};
use super::program::{
	AggCall, AggExpr, AggSpec, AggregateSink, ArithOp, BlockInstr, CompareOp, Enforce, FrameMode,
	IndexBinding, Instr, IntCmp, KeyOrder, Label, LimitSpec, NavOp, Operand, PermissionSpec,
	Pipeline, Program, ROW, Reg, SortSpec, Source, StoreInstr, TopKScan, UnaryOp, WriteInstr,
	WriteSpec,
};
use super::row::FieldName;
use crate::exec::logical::Needs;
use crate::exec::operators::write::plan::{Branching, WriteForm, WriteOrder, WritePlan};
use crate::exec::operators::write::stage::StageId;
use crate::exec::planner::write::write_facts;
use crate::exec::planner::{AggregationAnalysis, Planner, analyse_aggregation};
use crate::exec::{AccessMode, ContextLevel};
use crate::expr::logical::{LogicalExpr, LogicalFields, LogicalOp, LogicalWrite};
use crate::expr::operator::{BinaryOperator, PrefixOperator};
use crate::val::Value;

#[cfg(test)]
thread_local! {
	/// How many lowerings have begun on this thread, for tests that must know
	/// a run lowered nothing.
	pub(super) static LOWERINGS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

pub(super) type Lowered<T> = Result<T, NotCovered>;

#[path = "lower_block.rs"]
mod block;
pub(super) use block::data_dependent_ddl;
#[path = "lower_closure.rs"]
mod closure;
#[path = "lower_ddl.rs"]
mod ddl;
pub(super) use ddl::{DdlChecks, ddl_checks, plan_time_name};
#[path = "lower_external.rs"]
mod external;
#[path = "lower_function.rs"]
mod function;
#[path = "lower_graph.rs"]
mod graph;
#[path = "lower_info.rs"]
mod info;
#[path = "lower_path.rs"]
mod path;
#[path = "lower_write_cascade.rs"]
mod write_cascade;
#[path = "lower_write_data.rs"]
mod write_data;
#[cfg(all(test, feature = "kv-mem"))]
pub(super) use write_observe::OPEN_EVENT_UNITS;
pub(super) use write_observe::lower_subscription;
#[path = "lower_write_gate.rs"]
mod write_gate;
#[path = "lower_write_observe.rs"]
mod write_observe;
#[path = "lower_write_schema.rs"]
mod write_schema;
#[path = "lower_write_storage.rs"]
mod write_storage;
#[path = "lower_write_view.rs"]
mod write_view;

/// A statement a write performs on its own behalf, `bound` by `planner`, as
/// the one group of a program of its own (group 0), so it runs over a state
/// kept from one run to the next ([`super::subroutine`]).
pub(super) async fn lower_subroutine(
	planner: &Planner<'_>,
	bound: &crate::expr::logical::LogicalOp,
) -> Lowered<Program> {
	let physical = super::optimize::optimize(super::map::map(planner, bound).await?);
	let mut program = crate::kvs::catalog_reads::sealed(true, async {
		let mut l = Lowering::new(planner);
		if let Some(catalog) = &physical.catalog {
			l.catalog = Arc::clone(catalog);
		}
		l.stage_sets = true;
		let (_, access_mode, _) =
			crate::exec::vm::host::out_of_line(|| l.group(planner, &physical)).await?;
		l.scalar = true;
		l.writes =
			access_mode != AccessMode::ReadOnly || l.groups.iter().any(|g| g.write.is_some());
		l.finish()
	})
	.await?;
	seal(planner, &mut program, Default::default()).await?;
	Ok(program)
}

pub(super) async fn lower(planner: &Planner<'_>, plan: &PhysicalPlan) -> Lowered<Program> {
	let mut program = lower_in(planner, plan, None, None).await?;
	seal(planner, &mut program, Default::default()).await?;
	Ok(program)
}

/// [`lower`] for a program an operator runs, which serves its blocks'
/// late-bound statements when they are reached: the program, and those
/// statements as written ([`super::ServedStatements`]).
pub(super) async fn lower_serving(
	planner: &Planner<'_>,
	plan: &PhysicalPlan,
) -> Lowered<(Program, Vec<Arc<LogicalOp>>)> {
	let scoped = match &plan.tail.version {
		Some(version) => {
			Some(crate::kvs::catalog_reads::sealed(false, planner.under_version(version)).await)
		}
		None => None,
	};
	let planner = scoped.as_ref().unwrap_or(planner);
	let (mut program, served) =
		crate::kvs::catalog_reads::sealed(true, lower_serving_sealed(planner, plan)).await?;
	seal(planner, &mut program, Default::default()).await?;
	Ok((program, served))
}

async fn lower_serving_sealed(
	planner: &Planner<'_>,
	plan: &PhysicalPlan,
) -> Lowered<(Program, Vec<Arc<LogicalOp>>)> {
	crate::exec::compiles::note();
	let mut l = Lowering::new(planner);
	l.ahead = true;
	l.stage_sets = true;
	l.serves_late = true;
	crate::exec::vm::host::out_of_line(|| l.root(plan)).await?;
	let tail = crate::exec::vm::host::out_of_line(|| l.program_tail(&plan.tail)).await?;
	let served = std::mem::take(&mut l.served_ops);
	let mut program = l.finish()?;
	program.tail = tail;
	Ok((program, served))
}

/// [`lower`], for a statement whose records by id its caller knows to be of
/// `known` ([`super::dispatch::seal_knowing`]).
pub(super) async fn lower_knowing(
	planner: &Planner<'_>,
	plan: &PhysicalPlan,
	known: std::collections::BTreeSet<surrealdb_strand::TableName>,
) -> Lowered<Program> {
	let mut program = lower_in(planner, plan, None, None).await?;
	seal(planner, &mut program, known).await?;
	Ok(program)
}

/// Give `program` the dispatch over the tables it reads records of by id
/// ([`super::dispatch::seal_knowing`]). The dispatch units are lowered from
/// the catalog of the tables the program's reach names rather than from the
/// plan, so they are read unsealed ([`crate::kvs::catalog_reads::sealed`]).
async fn seal(
	planner: &Planner<'_>,
	program: &mut Program,
	known: std::collections::BTreeSet<surrealdb_strand::TableName>,
) -> Lowered<()> {
	let sealing = async {
		if known.is_empty() {
			super::dispatch::seal(planner, program).await
		} else {
			super::dispatch::seal_knowing(planner, program, known).await
		}
	};
	crate::kvs::catalog_reads::sealed(false, sealing).await
}

/// The frame a statement lowered by `planner` is lowered for.
pub(super) fn statement_frame(planner: &Planner<'_>) -> super::program::UnitFrame {
	Lowering::statement_frame(planner)
}

/// The positions among a method's arguments, the receiver not counted, at
/// which an implementation of `descriptor` invokes the closure it is handed.
/// Whether one of `args`, at a position in `positions` where the callee runs
/// the closure it is handed, may be a closure other than a literal written
/// there: a parameter, a field, the answer of another call.
fn unseen_closure_arg(args: &[LogicalExpr], positions: &[u16]) -> bool {
	positions
		.iter()
		.filter_map(|&at| args.get(at as usize))
		.any(|arg| !matches!(arg, LogicalExpr::Closure(_) | LogicalExpr::Literal(_)))
}

fn method_closure_args(descriptor: &crate::exec::function::MethodDescriptor) -> Vec<u16> {
	let mut positions: Vec<u16> = descriptor
		.type_impls
		.iter()
		.map(|(_, function)| function)
		.chain(descriptor.fallback.iter())
		.flat_map(|function| function.closure_args().iter().copied())
		.filter_map(|at| at.checked_sub(1).and_then(|at| u16::try_from(at).ok()))
		.collect();
	positions.sort_unstable();
	positions.dedup();
	positions
}

/// The body of `closure` for values capturing `captured`, lowered by
/// `planner` as a unit of an arena of its own: the unit with the table its
/// ids index, or why the VM does not lower the body for the planner's frame.
/// The host's compilation for a fault ([`super::fault`]).
pub(super) async fn lower_closure_body(
	planner: &Planner<'_>,
	closure: &Arc<crate::expr::logical::LogicalClosure>,
	captured: Vec<String>,
) -> Lowered<super::units::ClosureUnit> {
	let catalog = super::plan_catalog::read_closure(planner, closure).await?;
	let mut l = Lowering::new(planner);
	l.catalog = Arc::new(catalog);
	let unit = crate::kvs::catalog_reads::sealed(
		true,
		crate::exec::vm::host::out_of_line(|| l.closure_body_unit(closure, captured)),
	);
	let id = unit.await??;
	let table: super::UnitTable = l.take_arena().units.into();
	let Some(unit) = table.get(id.0 as usize).cloned() else {
		return Err(NotCovered::internal("a closure unit outside the arena it was lowered into"));
	};
	Ok(super::units::ClosureUnit {
		unit,
		table,
	})
}

/// [`lower`] for a statement that is a unit of an enclosing program, whose
/// reads of records by id are served by the dispatch of the program that
/// holds it ([`Program::reach`]).
///
/// A unit lowered inside a statement inherits what the statement does not
/// declare of its own from the `enclosing` statement: its index scope, and
/// the catalog of a plan mapped with it.
pub(super) async fn lower_unit(
	planner: &Planner<'_>,
	plan: &PhysicalPlan,
	enclosing: Option<Enclosing<'_>>,
) -> Lowered<Program> {
	lower_in(planner, plan, None, enclosing).await
}

/// What a statement lowered as a unit of an enclosing one inherits from it
/// ([`lower_unit`]).
#[derive(Clone, Copy)]
pub(super) struct Enclosing<'e> {
	scope: &'e IndexScope,
	catalog: &'e Arc<super::plan_catalog::PlanCatalog>,
}

/// [`lower`] as a unit of `arena`, the arena of a function bundle: the units
/// and function bodies the statement reaches are linked into it, and the
/// program, which holds none of its own, runs with the arena's table.
///
/// Lowering takes every definition from the plan, so it runs sealed off the
/// catalog ([`crate::kvs::catalog_reads::sealed`]); a statement it maps for
/// itself is mapped unsealed.
async fn lower_in<'c>(
	planner: &Planner<'c>,
	plan: &PhysicalPlan,
	arena: Option<&mut function::Arena<'c>>,
	enclosing: Option<Enclosing<'_>>,
) -> Lowered<Program> {
	// A versioned statement is lowered as of its stamp: the functions it calls
	// are the definitions stored then. The stamp a parameter names is read
	// from the catalog, so it is resolved unsealed, as the mapping resolves it.
	let scoped = match &plan.tail.version {
		Some(version) => {
			Some(crate::kvs::catalog_reads::sealed(false, planner.under_version(version)).await)
		}
		None => None,
	};
	let planner = scoped.as_ref().unwrap_or(planner);
	crate::kvs::catalog_reads::sealed(true, lower_in_sealed(planner, plan, arena, enclosing)).await
}

async fn lower_in_sealed<'c>(
	planner: &Planner<'c>,
	plan: &PhysicalPlan,
	mut arena: Option<&mut function::Arena<'c>>,
	enclosing: Option<Enclosing<'_>>,
) -> Lowered<Program> {
	crate::exec::compiles::note();
	let mut l = Lowering::new(planner);
	if let Some(enclosing) = enclosing {
		l.index_scope.clone_from(enclosing.scope);
		l.catalog = Arc::clone(enclosing.catalog);
	}
	l.ahead = true;
	match arena.as_deref_mut() {
		Some(arena) => {
			l.bundle = true;
			l.put_arena(std::mem::take(arena));
		}
		None => l.stage_sets = true,
	}
	let rooted = crate::exec::vm::host::out_of_line(|| l.root(plan)).await;
	let tail = match rooted {
		Ok(()) => crate::exec::vm::host::out_of_line(|| l.program_tail(&plan.tail)).await,
		Err(e) => Err(e),
	};
	if let Some(arena) = arena {
		*arena = l.take_arena();
	}
	let tail = tail?;
	let mut program = l.finish()?;
	program.tail = tail;
	Ok(program)
}

/// The units the statements of one request are lowered into, shared by all
/// of them: a statement lowered into it holds none of its own, and the ids it
/// calls index the units of the program holding the request
/// ([`request_program`]).
#[derive(Default)]
pub(super) struct RequestUnits(function::Arena<'static>);

impl RequestUnits {
	/// How many units the statements lowered so far hold.
	pub(super) fn len(&self) -> usize {
		self.0.len()
	}

	/// Drop the units of the statements lowered from `len` on.
	pub(super) fn rewind(&mut self, len: usize) {
		self.0.rewind(len);
	}
}

/// [`lower`] for a statement of a request, its units lowered into `units`.
/// The statement is lowered as a statement at the root of a program is: only
/// where its units go differs.
pub(super) async fn lower_request_statement(
	planner: &Planner<'_>,
	plan: &PhysicalPlan,
	units: &mut RequestUnits,
) -> Lowered<(Program, Vec<Arc<LogicalOp>>)> {
	let scoped = match &plan.tail.version {
		Some(version) => {
			Some(crate::kvs::catalog_reads::sealed(false, planner.under_version(version)).await)
		}
		None => None,
	};
	let planner = scoped.as_ref().unwrap_or(planner);
	let (mut program, served) =
		crate::kvs::catalog_reads::sealed(true, lower_request_sealed(planner, plan, units)).await?;
	seal(planner, &mut program, Default::default()).await?;
	Ok((program, served))
}

async fn lower_request_sealed(
	planner: &Planner<'_>,
	plan: &PhysicalPlan,
	units: &mut RequestUnits,
) -> Lowered<(Program, Vec<Arc<LogicalOp>>)> {
	crate::exec::compiles::note();
	let mut l = Lowering::new(planner);
	l.ahead = true;
	l.stage_sets = true;
	l.serves_late = true;
	l.put_arena(std::mem::take(&mut units.0).moved());
	let rooted = Box::pin(l.root(plan)).await;
	let tail = match rooted {
		Ok(()) => l.program_tail(&plan.tail).await,
		Err(e) => Err(e),
	};
	units.0 = l.take_arena().moved();
	let tail = tail?;
	let served = std::mem::take(&mut l.served_ops);
	let mut program = l.finish()?;
	program.tail = tail;
	Ok((program, served))
}

/// The program of a whole request: `units`, the units its statements were
/// lowered into, and `request`, its steps and the programs of its statements.
pub(super) fn request_program(
	planner: &Planner<'_>,
	units: RequestUnits,
	request: super::program::RequestCode,
) -> Lowered<Program> {
	let mut l = Lowering::new(planner);
	l.put_arena(units.0.moved());
	let mut program = l.finish()?;
	program.request = Some(Box::new(request));
	Ok(program)
}

impl Lowering<'_, '_> {
	/// Lower `plan` as the statement this lowering compiles.
	fn root<'s>(&'s mut self, plan: &'s PhysicalPlan) -> crate::exec::BoxFut<'s, Lowered<()>> {
		// A boxed `dyn` future: lowering recurses through this, and a caller
		// proving a statement's future `Send` stops at the box.
		Box::pin(async move {
			let l = self;
			l.tempfiles = plan.tail.tempfiles;
			// What the statement does not declare, it inherits from the statement
			// that holds it: an index function in a nested `SELECT` reports
			// against the clause or operator that introduced it.
			let inherited = std::mem::take(&mut l.index_scope);
			l.index_scope.clone_from(&plan.scope);
			if !plan.scope.select {
				l.index_scope.select = inherited.select;
				l.index_scope.executor_tables = inherited.executor_tables.clone();
			}
			if plan.scope.matches.is_empty()
				&& plan.scope.registered.is_empty()
				&& !(inherited.matches.is_empty() && inherited.registered.is_empty())
			{
				l.index_scope.table = inherited.table;
				l.index_scope.matches = inherited.matches;
				l.index_scope.registered = inherited.registered;
				l.index_scope.executor_tables = inherited.executor_tables;
			}
			if plan.scope.knn {
				l.knn_scope_slot = Some(l.knn_slot());
			} else {
				l.index_scope.knn = inherited.knn;
			}
			l.versioned |= plan.tail.version.is_some();
			if let Some(catalog) = &plan.catalog {
				l.catalog = Arc::clone(catalog);
			}
			l.nested_plans.clone_from(&plan.nested);
			match &plan.root {
				Plan::Write {
					write,
					subjects,
				} => crate::exec::vm::host::out_of_line(|| l.write(write, subjects)).await?,
				Plan::Eval {
					expr,
					fetches,
				} => crate::exec::vm::host::out_of_line(|| l.eval_statement(expr, fetches)).await?,
				Plan::Ddl {
					statement,
					checks,
				} => {
					l.ddl_checks = Some(Arc::clone(checks));
					crate::exec::vm::host::out_of_line(|| l.ddl(statement)).await?
				}
				Plan::Info {
					info,
				} => crate::exec::vm::host::out_of_line(|| l.info(info)).await?,
				Plan::Refused {
					refusal,
				} if l.refuses_when_run => l.refused_when_run(refusal)?,
				Plan::Refused {
					refusal,
				} => return Err(NotCovered::refused(refusal.clone())),
				Plan::Control {
					control,
				} => {
					crate::exec::vm::host::out_of_line(|| l.control(control, &plan.sources)).await?
				}
				root => {
					crate::exec::vm::host::out_of_line(|| l.pipeline(root, Sink::Output)).await?
				}
			}
			Ok(())
		})
	}
}

/// The program of the stages `sites` names, lowered for records of `table`
/// written by `write`: one pipeline per site, which answers `true` when every
/// stage of the run kept the record and `false` when one abandoned it.
///
/// A lowering made when the statement arrives: the program is a unit of the
/// statement's ([`StageTables`]), never lowered while it runs.
///
/// The stages are lowered from `catalog`, which holds what the stages of
/// `table` consult, sealed off the catalog itself
/// ([`crate::kvs::catalog_reads::sealed`]).
#[allow(clippy::too_many_arguments)]
async fn lower_stages<'c>(
	planner: &Planner<'c>,
	write: &LogicalWrite,
	table: surrealdb_strand::TableName,
	sites: &[Vec<StageId>],
	arena: &mut function::Arena<'c>,
	frame: StageFrame,
	catalog: &Arc<super::plan_catalog::PlanCatalog>,
	scope: &IndexScope,
) -> Lowered<Program> {
	let lowered = lower_stages_sealed(planner, write, table, sites, arena, frame, catalog, scope);
	crate::kvs::catalog_reads::sealed(true, lowered).await
}

#[allow(clippy::too_many_arguments)]
async fn lower_stages_sealed<'c>(
	planner: &Planner<'c>,
	write: &LogicalWrite,
	table: surrealdb_strand::TableName,
	sites: &[Vec<StageId>],
	arena: &mut function::Arena<'c>,
	frame: StageFrame,
	catalog: &Arc<super::plan_catalog::PlanCatalog>,
	scope: &IndexScope,
) -> Lowered<Program> {
	let facts = write_facts(write);
	let mut l = Lowering::new(planner);
	l.catalog = Arc::clone(catalog);
	// The statement's condition reads the `MATCHES` clauses and `KNN`
	// operator the statement declares, wherever its stages are lowered.
	l.index_scope.clone_from(scope);
	l.ahead = frame.ahead;
	l.caller_suppressed = frame.caller_suppressed;
	l.generic_table = frame.generic_table;
	l.edges_enclosing = frame.edges;
	l.write_output = write.statement.output().cloned();
	l.write_table = Some(table);
	l.write_form = Some(facts.form);
	l.write_logical = Some(write.clone());
	// The stages are units of the arena of the statement they belong to, and
	// so are the statements their cascades run.
	l.put_arena(std::mem::take(arena));
	let lowered = crate::exec::vm::host::out_of_line(|| l.stage_sites(sites)).await;
	let shape = match &lowered {
		Ok(()) => crate::exec::vm::host::out_of_line(|| l.write_shape()).await.unwrap_or_default(),
		Err(_) => Default::default(),
	};
	*arena = l.take_arena();
	lowered?;
	l.write = Some(WriteSpec {
		form: facts.form,
		shape,
		output: write.statement.output().cloned(),
		only: write.statement.only(),
		tables: None,
	});
	l.finish()
}

/// The program of the stages `sites` names for records of `table` written by
/// `write` ([`lower_stages`]), headed as a unit lowered for the frame
/// `planner` lowers for, appended to `arena`, whose units the stages' own ids
/// index.
#[allow(clippy::too_many_arguments)]
async fn stage_program<'c>(
	planner: &Planner<'c>,
	write: &LogicalWrite,
	table: surrealdb_strand::TableName,
	sites: &[Vec<StageId>],
	arena: &mut function::Arena<'c>,
	frame: StageFrame,
	catalog: &Arc<super::plan_catalog::PlanCatalog>,
	scope: &IndexScope,
) -> Lowered<super::program::UnitId> {
	let mut program =
		lower_stages(planner, write, table, sites, arena, frame, catalog, scope).await?;
	program.unit = Some(super::program::UnitHead {
		sig: super::program::UnitSig {
			params: Vec::new(),
			takes_row: false,
		},
		frame: statement_frame(planner),
		entry: None,
		closure: None,
	});
	let id = u32::try_from(arena.units.len())
		.map_err(|_| NotCovered::internal("more units than a program holds"))?;
	arena.units.push(Arc::new(program));
	Ok(super::program::UnitId(id))
}

/// The stages `sites` names for records of `table` written by `write`, as a
/// bundle: a unit holding an arena of its own whose entry is the program of
/// the stages ([`stage_program`]), and whose other units are what the stages
/// run. What the bundle holds reaches no unit of another arena, so the
/// compile cache keeps it whole and a run loads it by its table's name.
async fn stage_bundle(
	planner: &Planner<'_>,
	write: &LogicalWrite,
	table: surrealdb_strand::TableName,
	sites: &[Vec<StageId>],
	frame: StageFrame,
	catalog: &Arc<super::plan_catalog::PlanCatalog>,
	scope: &IndexScope,
) -> Lowered<Program> {
	let mut arena = function::Arena::default();
	let entry =
		stage_program(planner, write, table, sites, &mut arena, frame, catalog, scope).await?;
	let mut holder = Lowering::new(planner);
	holder.delegate_context = arena
		.units
		.iter()
		.map(|unit| unit.required_context)
		.fold(holder.delegate_context, Ord::max);
	holder.units = arena.units;
	let mut bundle = holder.finish()?;
	bundle.unit = Some(super::program::UnitHead {
		sig: super::program::UnitSig {
			params: Vec::new(),
			takes_row: false,
		},
		frame: statement_frame(planner),
		entry: Some(entry),
		closure: None,
	});
	Ok(bundle)
}

/// A parameter's `PERMISSIONS` predicate `guard`, lowered by `planner` as
/// a program whose one host pipeline evaluates it ([`super::param_unit`]),
/// against the catalog entries the predicate consults, read first.
pub(super) async fn param_guard_unit(
	planner: &Planner<'_>,
	guard: &LogicalExpr,
) -> Lowered<Program> {
	// The compile cache serves the unit to every principal of the frame's
	// class, so it reads the session's own values when it runs.
	let planner = planner.reading_session_values();
	let catalog = Arc::new(super::plan_catalog::read_guard(&planner, guard).await?);
	crate::kvs::catalog_reads::sealed(
		true,
		crate::exec::vm::host::out_of_line(|| Lowering::param_guard_unit(&planner, guard, catalog)),
	)
	.await
}

/// The bundle of the stages a write that may reach any table, `set`, applies
/// to the records of `table`, lowered for the frame `planner` lowers for —
/// the generic bundle of a table the catalog does not hold where `generic` —
/// and what its lowering read, where that can be named: what the compile
/// cache compiles on a miss when a run writes a record of the table
/// ([`super::fault::serve_stage_bundle`]).
pub(super) async fn stage_bundle_for(
	planner: &Planner<'_>,
	set: &super::units::StageSetUnit,
	table: &surrealdb_strand::TableName,
	generic: bool,
) -> Lowered<(Arc<Program>, Option<SetMemberDeps>)> {
	let Some(txn) = planner.txn().cloned() else {
		return Err(NotCovered::internal("a stage bundle lowered with no transaction"));
	};
	let read = [table.clone()];
	let catalog = Arc::new(
		crate::kvs::catalog_reads::sealed(
			false,
			super::plan_catalog::read_written(planner, &set.write, &read, generic),
		)
		.await?,
	);
	let frame = StageFrame {
		ahead: set.ahead,
		caller_suppressed: set.caller_suppressed,
		generic_table: generic,
		edges: set.edges,
	};
	// The bundle serves every run of the write, so a name its statements read
	// is read when they run rather than folded from this run's context.
	let planner = planner.shadowing(set.param_names());
	let scope = super::map::write_index_scope(&planner, &set.write).await?;
	set_member(&planner, &txn, set, table, frame, true, &catalog, &scope).await
}

/// The bundle of the stages `set` names for records of `table`, lowered for
/// `frame`, and, where `records`, what its lowering read: the catalog state,
/// and the values the planning context held for the names its definitions
/// read; `None` where those cannot be named.
#[allow(clippy::too_many_arguments)]
async fn set_member(
	planner: &Planner<'_>,
	txn: &Arc<crate::kvs::Transaction>,
	set: &super::units::StageSetUnit,
	table: &surrealdb_strand::TableName,
	frame: StageFrame,
	records: bool,
	catalog: &Arc<super::plan_catalog::PlanCatalog>,
	scope: &IndexScope,
) -> Lowered<(Arc<Program>, Option<SetMemberDeps>)> {
	#[cfg(test)]
	SET_MEMBERS_LOWERED.with(|lowered| lowered.borrow_mut().push(table.clone()));
	let recording = records.then(|| txn.record_catalog_reads());
	let lowered = crate::exec::vm::host::out_of_line(|| {
		stage_bundle(planner, &set.write, table.clone(), &set.sites, frame, catalog, scope)
	})
	.await;
	let reads = recording.map(|recording| recording.finish());
	let bundle = Arc::new(lowered?);
	let Some(reads) = reads else {
		return Ok((bundle, None));
	};
	let deps = match crate::exec::catalog_deps::CatalogDeps::resolve(txn, &reads).await {
		Some(deps) => deps,
		None => return Ok((bundle, None)),
	};
	// The names the write's own statements read are bound around the stages
	// ([`stage_bundle_for`]) and read when they run, never folded.
	let context =
		super::units::ContextDeps::resolve(txn, &reads, planner.frozen(), Default::default())
			.await
			.map(|context| context.without(crate::exec::planner::UNIT_RUNTIME_PARAMS));
	Ok((bundle, context.map(|context| (Arc::new(deps), Arc::new(context)))))
}

#[cfg(test)]
thread_local! {
	/// The tables whose stages, loaded by name, this thread compiled, in
	/// order, for tests that must know which a change had compiled again.
	pub(super) static SET_MEMBERS_LOWERED: std::cell::RefCell<Vec<surrealdb_strand::TableName>> =
		const { std::cell::RefCell::new(Vec::new()) };
}

/// What the bundle of a table's stages was lowered against.
type SetMemberDeps = (Arc<crate::exec::catalog_deps::CatalogDeps>, Arc<super::units::ContextDeps>);

/// What a stage unit is lowered for besides its table: whether it is lowered
/// ahead of the statement's run, the actor's frame its cascades restore,
/// whether it stands for any table the catalog does not hold, and whether it
/// runs beneath the edge cascade that loads it.
#[derive(Clone, Copy)]
struct StageFrame {
	ahead: bool,
	caller_suppressed: bool,
	generic_table: bool,
	/// Where the stages run beneath an edge cascade that loads them, or are
	/// lowered into such a bundle's arena: the permission state of the actor
	/// the cascade restores ([`Lowering::edges_enclosing`]).
	edges: Option<bool>,
}

/// The one table whose records scan leaf `node` reads, if it is one.
fn scanned_table(node: &Plan) -> Option<&surrealdb_strand::TableName> {
	match node {
		Plan::TableScan {
			table,
			..
		}
		| Plan::RecordScan {
			table,
			..
		}
		| Plan::IndexScan {
			table,
			..
		}
		| Plan::IndexUnion {
			table,
			..
		}
		| Plan::Bitmap {
			table,
			..
		}
		| Plan::FullTextScan {
			table,
			..
		}
		| Plan::KnnScan {
			table,
			..
		} => Some(table),
		_ => None,
	}
}

/// The tables `write` can write a record of: the tables its targets name
/// where they name them, and any table where a target is evaluated or a
/// subquery supplies the rows.
pub(super) fn write_reach(write: &LogicalWrite) -> super::dispatch::Reach {
	use super::dispatch::Reach;
	use crate::expr::logical::{
		LogicalSource, LogicalWriteSource, LogicalWriteStatement, SourceRef,
	};
	fn read(op: &LogicalOp) -> Reach {
		let own = match op {
			LogicalOp::Source(LogicalSource {
				source,
				..
			}) => match source {
				SourceRef::Table(table) => {
					Reach::Tables(std::collections::BTreeSet::from([table.clone()]))
				}
				SourceRef::RecordId(rid) => {
					Reach::Tables(std::collections::BTreeSet::from([rid.table.clone()]))
				}
				SourceRef::Mock(mock) => {
					Reach::Tables(std::collections::BTreeSet::from([mock.table().clone()]))
				}
				SourceRef::Param(_) | SourceRef::Dynamic(_) => Reach::Any,
			},
			// These keep or order the records their input yields.
			LogicalOp::Filter(_)
			| LogicalOp::PermissionFilter(_)
			| LogicalOp::Sort(_)
			| LogicalOp::Limit(_)
			| LogicalOp::Distinct(_)
			| LogicalOp::Timeout(_)
			| LogicalOp::VersionScope(_)
			| LogicalOp::Only(_) => Reach::none(),
			_ => Reach::Any,
		};
		op.children().iter().fold(own, |reach, child| reach.union(read(child)))
	}
	match &write.source {
		LogicalWriteSource::Targets(targets) => {
			// `RELATE` writes to its `through`, the second of its targets.
			let written: Vec<&LogicalExpr> = match &write.statement {
				LogicalWriteStatement::Relate(_) => targets.get(1).into_iter().collect(),
				_ => targets.iter().collect(),
			};
			written.into_iter().fold(Reach::none(), |reach, target| {
				reach.union(match target {
					LogicalExpr::Table(_) | LogicalExpr::RecordId(_) | LogicalExpr::Mock(_) => {
						Reach::of_expr(target)
					}
					_ => Reach::Any,
				})
			})
		}
		LogicalWriteSource::Inserts(inserts) => match &inserts.into {
			Some(into) => match into {
				LogicalExpr::Table(_) | LogicalExpr::RecordId(_) => Reach::of_expr(into),
				_ => Reach::Any,
			},
			None => Reach::Any,
		},
		LogicalWriteSource::Rows(input) => read(input),
	}
}

/// The order of view table `table`'s rows ([`lower_view_rows`]), lowered
/// ahead of the write as the root of an arena of its own.
pub(super) async fn view_rows_order(
	planner: &Planner<'_>,
	table: surrealdb_strand::TableName,
	catalog: &Arc<super::plan_catalog::PlanCatalog>,
) -> Lowered<Program> {
	Box::pin(lower_view_rows(planner, true, table, None, catalog)).await
}

/// The order of view table `table`'s rows, lowered from the view table's
/// definitions as a write of the view-row form: one pipeline per action a
/// view's maintenance settles — create, update and delete, in that order —
/// each answering whether the order kept the row. With `arena`, the program
/// is a unit of that arena and holds none of its own; without, it is the root
/// of an arena of its own.
async fn lower_view_rows<'c>(
	planner: &Planner<'c>,
	ahead: bool,
	table: surrealdb_strand::TableName,
	mut arena: Option<&mut function::Arena<'c>>,
	catalog: &Arc<super::plan_catalog::PlanCatalog>,
) -> Lowered<Program> {
	use crate::doc::Action;
	use crate::exec::operators::write::plan::WriteFacts;
	let plan = WritePlan::build(WriteFacts {
		form: WriteForm::View,
		has_data: false,
		on_duplicate_key_update: false,
		ignore: false,
		repeatable: false,
	});
	let mut l = Lowering::new(planner);
	l.catalog = Arc::clone(catalog);
	l.ahead = ahead;
	if let Some(arena) = arena.as_deref_mut() {
		l.put_arena(std::mem::take(arena));
	}
	l.write_table = Some(table);
	l.write_form = Some(WriteForm::View);
	let sites: Option<Vec<Vec<StageId>>> = [Action::Create, Action::Update, Action::Delete]
		.into_iter()
		.map(|action| plan.order(WriteOrder::for_view(action)).map(<[StageId]>::to_vec))
		.collect();
	let built = match sites {
		Some(sites) => crate::exec::vm::host::out_of_line(|| l.stage_sites(&sites)).await,
		None => Err(NotCovered::internal("the view-row plan holds no order for an action")),
	};
	if let Some(arena) = arena {
		*arena = l.take_arena();
	}
	built?;
	l.write = Some(WriteSpec {
		form: WriteForm::View,
		shape: super::program::WriteShape::default(),
		output: None,
		only: false,
		tables: None,
	});
	l.finish()
}

fn key_order(key: &SortKeySpec) -> KeyOrder {
	KeyOrder {
		ascending: key.ascending,
		collate: key.collate,
		numeric: key.numeric,
	}
}

/// Where the rows a pipeline's body finishes with go.
enum Sink<'a> {
	/// The statement's output.
	Output,
	/// Sort `id`, keyed by `keys` evaluated against the row.
	Sort {
		id: u16,
		keys: &'a [SortKeySpec],
	},
	/// Sort `id` of a brute-force KNN, keyed by the distance of the vector at
	/// `field` from the query of `rank`.
	Knn {
		id: u16,
		field: &'a LogicalExpr,
		rank: Arc<super::program::KnnRank>,
	},
	/// Aggregate `id`: the row's group keys, then its fields' arguments, as
	/// `analysis` names them.
	Aggregate {
		id: u16,
		analysis: AggregationAnalysis,
	},
	/// The write body, writing each row as a stored record.
	Write(&'a WritePlan),
	/// Split `id`, whose rows the host runs through the pipeline above it.
	Split {
		id: u16,
	},
	/// Join `id`, whose probe rows the body keys by `keys`, evaluated against
	/// the row, and whose pairings the host runs through the pipeline above
	/// it.
	JoinProbe {
		id: u16,
		keys: Vec<&'a LogicalExpr>,
	},
}

/// The document root an index or a method argument of a path step is
/// evaluated against, which is its current value; the operator tree takes the
/// value stepped from where there is none.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum DocumentRoot {
	/// Not known to the lowering: only an argument that reads no row lowers.
	Unknown,
	/// The row: a projection's fields are evaluated with it as their root.
	Row,
	/// The row a host pipeline runs over, the value stepped from when it
	/// runs over none.
	RowElseReceiver,
}

/// The values of one evaluation region that are evaluated once and read again
/// ([`crate::expr::logical::dag`]), each with the register that holds it once
/// its first occurrence is lowered, and the kinds the region proves.
///
/// A region is a straight-line stretch of one pipeline's code over one register
/// file, whose roots the DAG takes in the order they run; sharing never crosses
/// one. The register is pinned: nothing lowered while the scope is on
/// [`Lowering::shared`] is handed it back by [`Lowering::release`], so every
/// read of it sees the value its first occurrence left there. A first
/// occurrence always ends in a load into the pinned register, which no
/// peephole drops while a later root may read it.
#[derive(Default)]
struct SharedScope {
	values: Vec<(crate::expr::logical::dag::SharedValue, Option<Reg>)>,
	/// The kind proven for the base of each field read of the region, by the
	/// address of the base. Addresses are compared, never dereferenced.
	kinds: HashMap<usize, crate::expr::Kind>,
}

/// What the innermost [`SharedScope`] makes of an expression.
enum SharedSlot {
	/// The first occurrence of the value at this index of the scope: lowered
	/// as written, then kept in the value's register.
	First(usize),
	/// A later occurrence of a value already kept: read from its register.
	Reuse(Reg),
	/// Not a shared value, or one with nothing yet to read.
	Unshared,
}

struct Lowering<'p, 'ctx> {
	planner: &'p Planner<'ctx>,
	/// What the plan being lowered read of the catalog: every definition the
	/// lowering takes ([`super::plan_catalog`]).
	catalog: Arc<super::plan_catalog::PlanCatalog>,
	/// The checks of the schema statement being lowered, as its mapping made
	/// them ([`DdlChecks`]).
	ddl_checks: Option<Arc<DdlChecks>>,
	/// While a permission clause's value is lowered: the planner that holds
	/// no transaction, whose scopes judge the index operators
	/// ([`Self::answers_unscoped_operator`]) and graph degrees the clause
	/// holds.
	bare: Option<Planner<'ctx>>,
	/// Whether a statement held in a frame of a write's clause is bound before
	/// it is mapped ([`Self::binds_nested`]).
	binds_in_frames: bool,
	pipelines: Vec<Pipeline>,
	consts: Vec<Value>,
	fields: Vec<String>,
	paths: Vec<crate::exec::field_path::FieldPath>,
	limits: Vec<LimitSpec>,
	sorts: Vec<SortSpec>,
	splits: Vec<super::program::SplitSpec>,
	joins: Vec<super::program::JoinSpec>,
	/// The group that owns each distinct set.
	distincts: Vec<Option<u16>>,
	fetches: Vec<Vec<Vec<super::program::FetchStepSpec>>>,
	/// For each pipeline, the pipeline that fills the sort, aggregate or split
	/// it reads, if any.
	feeders: Vec<Option<usize>>,
	aggregates: Vec<AggregateSink>,
	permissions: Vec<PermissionSpec>,
	/// The names each projection layer computes.
	computed_names: Vec<Vec<surrealdb_strand::Strand>>,
	/// What each call of code the host runs — an embedded script, a model, a
	/// module export — needs of the transaction and the session, which the
	/// program's needs are made of too.
	call_needs: Vec<Needs>,
	/// The pipelines of the host scalars the body answers.
	host_code: Vec<Pipeline>,
	/// What the catalog declares for the scanned rows, for choosing typed
	/// instructions.
	schema: Option<crate::expr::logical::Schema>,
	table: Option<surrealdb_strand::TableName>,
	// The pipeline being built.
	code: Vec<Instr>,
	/// The end of [`Self::code`] where a jump was last pointed at it, before
	/// anything was emitted there ([`Self::emit`]).
	pointed_at: Option<usize>,
	next_reg: Reg,
	uses_eval: bool,
	/// Whether the pipeline being lowered reads the rows of a sort, whose
	/// keys its projection may share.
	reads_sorted_rows: bool,
	/// Whether the pipeline being built runs an instruction that can raise
	/// for some row: arithmetic, a cast, or an evaluated expression.
	may_fail: bool,
	/// The permission frame the expression being lowered is evaluated in,
	/// while a permission predicate is lowered.
	eval_frame: Option<u16>,
	/// Whether the code being lowered runs in a privileged frame that is not
	/// `eval_frame`: a frame's pipelines built beside it, where `eval_frame` is
	/// set aside. See [`Self::in_privileged_frame`].
	privileged: bool,
	/// Whether the expression being lowered is evaluated with reads left
	/// unchecked: a write permission predicate the host judges
	/// ([`super::interp::State::unchecked_reads`]), or the body of a call
	/// inlined where reads are unchecked.
	unchecked_reads: bool,
	pipeline_limits: Vec<u16>,
	/// Whether the rows of the pipeline being built are records, whose fields
	/// the body reads itself; a value source's rows may be any value.
	rows_are_objects: bool,
	/// Whether a row of the pipeline being built may be a record id: a
	/// statement in `FROM` yields its rows as they are.
	rows_may_be_ids: bool,
	/// The register holding the row a `*` projection reads, fetched where the
	/// row is a record id; `None` where it reads the row as it stands.
	fetched_row: Option<Reg>,
	/// The context the value sources, the nested statements and the graph
	/// lookups require.
	delegate_context: ContextLevel,
	/// For a block, [`Self::delegate_context`] and the length of
	/// [`Self::call_needs`] as they stood once its first schema statement was
	/// lowered. The program declares only the context what runs up to that
	/// statement requires: a schema statement may create the namespace or
	/// database the session names, so every statement after it resolves them
	/// when it runs instead ([`super::program::BlockInstr::Resolve`]).
	catalog_changed: Option<(ContextLevel, usize)>,
	/// For a write statement, what its body is run with.
	write: Option<WriteSpec>,
	/// For a write statement, its `RETURN` clause and the one table it
	/// writes to, when the statement names one.
	write_output: Option<crate::expr::output::Output>,
	write_table: Option<surrealdb_strand::TableName>,
	write_form: Option<WriteForm>,
	/// The write statement being lowered, whose clauses its stages evaluate.
	write_logical: Option<LogicalWrite>,
	/// Whether nothing the write being lowered evaluates may change the
	/// catalog while it runs — no function body, closure or statement it
	/// cannot see into — so what its table's catalog held at lowering is
	/// what it holds for every record, and a stage the table has nothing for
	/// is left out rather than guarded.
	write_catalog_fixed: bool,
	/// Whether the statement this lowering lowers, refused by the planner,
	/// raises the refusal when it runs: a block statement the operator tree
	/// compiles when the block reaches it ([`Self::refused_when_run`]).
	refuses_when_run: bool,
	/// Whether the next group lowered is such a statement.
	next_group_refuses: bool,
	/// For a write statement naming no single table, the runs of stages its
	/// body takes through [`WriteInstr::DispatchTable`], by site.
	stage_sites: Option<Vec<Vec<StageId>>>,
	/// Each table's field view as [`Self::view_layers`] answered it, and as
	/// [`Self::image_view`] lowered it, for the stages that read the view
	/// after the first.
	view_layers_of: HashMap<surrealdb_strand::TableName, Option<ViewLayers>>,
	image_view_of: HashMap<surrealdb_strand::TableName, Option<(Option<u16>, bool)>>,
	/// The pre-mutation snapshot the data stages derive, once lowered.
	snapshot_of: Option<super::write::DataInstr>,
	/// The statement's clauses as the body's instructions read them, once
	/// lowered; `None` inside when a place embeds an expression with no
	/// pipeline.
	shape_of: Option<Option<super::program::WriteShape>>,
	/// Why an expression a place embeds has no pipeline, where one has none:
	/// the refusal the operator tree raises for it, or the reason the VM does
	/// not lower it.
	place_refused: Option<NotCovered>,
	/// The views of the one table the write writes to, once lowered.
	views_of: Option<Arc<super::write::LoweredViews>>,
	/// The names bound around the block statement being lowered — a `FOR`
	/// body's parameter, and what the `LET`s before it bind — which it is
	/// planned with as bound rather than read from the frame.
	block_bound: Vec<String>,
	/// The block names in scope that the program's own reads take from a
	/// slot of the running state ([`Instr::LoadSlot`]), innermost last.
	slot_names: Vec<(String, u16)>,
	/// The slots the program's block names have taken.
	slots: u16,
	/// How many reads of a name in [`Self::slot_names`] were lowered to read
	/// the frame: in a host pipeline, which runs over a state of its own, or
	/// in a permission frame.
	frame_reads: u32,
	/// While above zero, a pipeline beside the one in flight is being built,
	/// whose reads of block names take the frame.
	slot_hidden: u32,
	/// The bindings of block names whose scope is being lowered: where each
	/// was bound, and what the program held then.
	slot_scopes: Vec<block::SlotScope>,
	/// A block program's statements as written, each with the names bound
	/// around it, by source ([`super::plan::PhysicalPlan::sources`]).
	sources: Vec<(LogicalOp, Vec<String>)>,
	/// The loops enclosing the block statement being lowered, innermost last.
	loops: Vec<block::LoopTargets>,
	/// The `FOR` iterator slots the program has taken.
	iter_slots: u16,
	/// How many frames the control flow being lowered has saved.
	saved: usize,
	/// The statements of a block compiled into the program.
	groups: Vec<super::program::GroupSpec>,
	/// What `EXPLAIN ANALYZE` of a block's statements renders
	/// ([`Program::analyses`]).
	analyses: Vec<super::program::Analysis>,
	/// The late-bound statements a block runs, served when it reaches them
	/// ([`super::program::Program::served`]).
	served: Vec<super::program::ServedSpec>,
	/// The statements of [`Self::served`], as written, by the same index.
	served_ops: Vec<Arc<LogicalOp>>,
	/// Whether a late-bound statement a block runs is served when the block
	/// reaches it ([`super::program::BlockInstr::RunServed`]): only in a
	/// program an operator runs, which holds the statements as written.
	serves_late: bool,
	/// The group the pipelines and paging being lowered belong to.
	current_group: Option<u16>,
	/// The tables the program reads records of by id where the table is known
	/// only from data ([`Program::reach`]).
	reach: Option<super::dispatch::Reach>,
	/// What of [`Self::reach`] this lowering reads at sites the operator tree
	/// resolves against the catalog as it stands when they run: a value in
	/// `FROM` only the data names, and a `FETCH`. A value written in `FROM`,
	/// and the records it names, are bound with the statement.
	late_reach: Option<super::dispatch::Reach>,
	/// Whether the statement's value is its one row.
	scalar: bool,
	/// Whether the program is lowered before it runs, when the work a stage
	/// needs compiled ahead of it — a table's live subscriptions' units — is
	/// compiled; a lowering made while a program runs compiles none of it.
	ahead: bool,
	/// Whether the program writes whatever its expressions do: a schema
	/// statement writes the catalog.
	writes: bool,
	/// Registers released for reuse.
	released: Vec<Reg>,
	/// While an element body is lowered — a `[WHERE …]` predicate — the
	/// register holding the element, which is its current value and what a
	/// bare field reads.
	element: Option<Reg>,
	/// While a `SELECT VALUE` that omits paths is lowered, the register
	/// holding the row with those paths removed, which is the document root
	/// of the value's expression in place of the row itself.
	omitted_row: Option<Reg>,
	/// Whether the statement asks for `TEMPFILES`, so its sort may spill.
	tempfiles: bool,
	/// The slot the distances of the `KNN` operator an index function reports
	/// against are published in, where an enclosing statement declares the
	/// operator.
	knn_scope_slot: Option<u16>,
	/// Whether the expression being lowered is a schema statement's evaluated
	/// value, whose method calls name their argument errors as code a write
	/// reaches does ([`super::program::MethodSpec::writing`]).
	writing_labels: bool,
	/// The clauses and operator the statement being lowered declares, which
	/// its index functions report against.
	index_scope: IndexScope,
	/// While the input of a sort that publishes a top-k threshold is lowered,
	/// the index of that sort, which the table scan feeding it names.
	topk_sort: Option<u16>,
	/// The kinds `LET` coerces to and casts convert to.
	kinds: Vec<crate::expr::Kind>,
	/// The parameters the bodies read.
	params: Vec<super::program::ParamSpec>,
	/// The builtins the bodies call.
	calls: Vec<super::program::CallSpec>,
	/// The `MATCHES` operators the bodies evaluate.
	matches: Vec<super::program::MatchesSpec>,
	/// The calls of code the host runs that the bodies make.
	externals: Vec<super::program::ExternalSpec>,
	/// The degree counts the bodies answer from the adjacency storage.
	degrees: Vec<super::program::DegreeSpec>,
	/// The methods the bodies call.
	methods: Vec<super::program::MethodSpec>,
	/// The record ids the bodies build.
	record_ids: Vec<super::program::RecordIdSpec>,
	/// The statements the bodies evaluate in scalar position.
	subqueries: Vec<super::program::SubquerySpec>,
	/// The statements the bodies evaluate in scalar position as groups of the
	/// program.
	nested: Vec<super::program::NestedSpec>,
	/// The statements the plan being lowered holds in its expressions, mapped
	/// with it ([`PhysicalPlan::nested`]).
	nested_plans: Vec<Arc<super::plan::Nested>>,
	/// The probes the nested statements read through, each with its owner
	/// ([`super::program::Program::probes`]).
	probes: Vec<Option<u16>>,
	/// How many draws — ids leased, uuids minted — the schema statement
	/// being lowered has emitted.
	id_draws: u16,
	/// What its speculative run drew for each draw of the schema statement
	/// being lowered, taken by the draws as they are emitted.
	reserving: Vec<(u16, u128)>,
	/// [`super::program::Program::reservations`].
	reservations: Vec<u128>,
	/// [`super::program::Program::implicit_tables`].
	pub(super) implicit_tables: Vec<super::program::ImplicitTable>,
	/// The nested programs of the root being lowered, by
	/// [`super::program::UnitId`]; a lowering of a unit holds the root's
	/// table while it lowers, and gives it back before it finishes.
	units: Vec<Arc<Program>>,
	/// While a nested statement's group is lowered: the group evaluating the
	/// statement, which owns the probes it reads through.
	probe_owner: Option<Option<u16>>,
	/// The graph lookups the bodies take.
	lookups: Vec<super::program::LookupSpec>,
	/// The user-defined functions the bodies call.
	functions: Vec<super::program::FunctionSpec>,
	/// The user-defined function bodies linked into [`Self::units`], when the
	/// arena is a bundle's.
	fn_links: Vec<function::FnLink>,
	/// The statements a delete's cascades run, linked into [`Self::units`].
	cascade_links: Vec<write_cascade::CascadeLink>,
	/// The observer units — tables' events, view tables' orders — linked into
	/// [`Self::units`].
	observer_links: Vec<write_observe::ObserverLink>,
	/// The arena's lowerings of tables' events: those open, and those left
	/// for the outermost to lower.
	events_queue: write_observe::EventsQueue<'ctx>,
	/// The closure bodies linked into [`Self::units`], with the closures as
	/// written they are the bodies of.
	closure_links: Vec<closure::ClosureLink>,
	/// Whether the actor's own frame suppresses permission enforcement: what
	/// an edge cascade restores beneath a reference cascade's bypass.
	caller_suppressed: bool,
	/// Whether the stage unit being lowered stands for any table the
	/// statement reaches that the catalog does not hold, whose references are
	/// those of every table.
	generic_table: bool,
	/// The tables the records of the statement being lowered belong to, where
	/// a cascade knows them from the key that named the record.
	reach_override: Option<super::dispatch::Reach>,
	/// Whether the statement being lowered roots an arena of its own, in which
	/// no unit is being lowered that a stage could reach again and close on.
	/// The stages a write that may reach any table loads by name run beneath
	/// an edge cascade as this decides ([`StageFrame::edges`]).
	stage_sets: bool,
	/// Whether the statement being lowered is a delete's edge cascade
	/// ([`super::units::CatalogUnit::PurgeEdges`]), whose stages' own edge
	/// cascades are the cascade that runs them.
	edge_statement: bool,
	/// Where the stages being lowered are those an edge cascade loads: the
	/// permission state of the actor the cascade restores. An edge cascade lowered for that state
	/// is the cascade running the member ([`write_cascade`]), which the member's arena does
	/// not hold.
	edges_enclosing: Option<bool>,
	/// Whether [`Self::units`] is the arena of a function bundle, which every
	/// body lowered beneath this lowering is a unit of.
	bundle: bool,
	/// Whether the statement being lowered is read at a `VERSION`.
	versioned: bool,
	/// The closures whose bodies run in place of a builtin's invocations.
	closures: Vec<super::program::ClosureSpec>,
	/// The closure literals whose values the bodies make.
	closure_values: Vec<super::program::ClosureValueSpec>,
	/// How many invocations of a closure value, and calls of a builtin that
	/// runs statements, the bodies make: code the program cannot read, which
	/// may write.
	invokes: usize,
	/// How many locked reads (`FOR UPDATE`) the program makes, each of which
	/// registers state on the storage engine and needs a writeable
	/// transaction.
	locks: usize,
	/// The `IF` branches the control flow being lowered is in.
	control_ifs: u32,
	/// Whether the control flow being lowered is in a block or a loop.
	control_nested: bool,
	/// Whether the block about to be lowered is an `IF` branch.
	branch_seq: bool,
	/// How many calls the bodies make whose bodies keep row order: of
	/// user-defined functions ([`crate::exec::planner::ResolvedCustomFunction`]),
	/// of closures a receiver may hold under a method's name, and of builtins
	/// and methods handed a closure that is not a literal of the call.
	row_ordered_calls: usize,
	/// The call frames the bodies open, one per call.
	call_frames: u16,
	/// While an inlined function body is lowered: the registers its
	/// parameters are bound into.
	inline_params: Vec<(String, Reg)>,
	/// While an expression that takes a `RETURN` signalled inside it as its
	/// value is lowered: the catch, whose subquery instructions are pointed
	/// at the expression's end.
	return_catch: Option<ReturnCatch>,
	/// The sharing scopes of the projections being lowered, innermost last.
	/// Only the innermost is consulted, and a lowering that builds code in
	/// registers of its own sets an empty one in its place
	/// ([`Self::hide_shared`]).
	shared: Vec<SharedScope>,
	/// Whether a record-id key element is being lowered: a `BREAK` or
	/// `CONTINUE` it raises outside any statement it holds has no loop to
	/// take it, and is an error.
	key_element: bool,
	/// Whether a projection layer's column is being lowered: a statement it
	/// holds reads `$value` from its frame, as the layer leaves it there.
	column_statements: bool,
	/// The register holding the value a statement the expression being
	/// lowered holds takes as its `$this` and `$parent` in place of the row: a
	/// computed column's reduced record, or the element a lookup with clauses
	/// is taken from. A statement over it is a unit of the program, run where
	/// it is evaluated, never a group.
	statement_doc: Option<Reg>,
	/// The document root an index or a method argument of a path step in the
	/// expressions being lowered is evaluated against.
	document: DocumentRoot,
	/// Whether the statement reports rows whatever its value.
	rows: bool,
}

/// The walk that removes what `path`, a plain path of an `OMIT`, names from a
/// value that may be any shape: all but its last part reach the location, and
/// the last is removed there.
fn walk_along(path: &crate::exec::field_path::FieldPath) -> Walk<LogicalExpr> {
	use crate::exec::field_path::FieldPathPart;
	let Some((last, reach)) = path.0.split_last() else {
		return Walk {
			steps: Vec::new(),
			action: WalkAction::Nothing,
		};
	};
	let mut steps = Vec::with_capacity(reach.len());
	for part in reach {
		steps.push(match part {
			FieldPathPart::Field(name) => WalkStep::Field(name.clone()),
			FieldPathPart::Index(index) => WalkStep::Index(*index),
			FieldPathPart::First => WalkStep::First,
			FieldPathPart::Last => WalkStep::Last,
			FieldPathPart::Lookup(_) => WalkStep::Nowhere,
		});
	}
	Walk {
		steps,
		action: match last {
			FieldPathPart::Field(name) => WalkAction::RemoveField(name.clone()),
			FieldPathPart::Index(index) => WalkAction::RemoveIndex(*index),
			FieldPathPart::First => WalkAction::RemoveFirst,
			FieldPathPart::Last => WalkAction::RemoveLast,
			FieldPathPart::Lookup(_) => WalkAction::Nothing,
		},
	}
}

/// Whether every row `source` yields is a record, whose fields the body reads
/// itself: not a value in `FROM`, a statement's rows or a count.
fn yields_records(source: &Source) -> bool {
	match source {
		Source::Values {
			..
		}
		| Source::Nested {
			..
		}
		| Source::Join {
			..
		}
		| Source::Count {
			..
		} => false,
		Source::Union(sources) => sources.iter().all(yields_records),
		_ => true,
	}
}

/// Whether a row `source` yields may be a record id: a statement in `FROM`
/// yields its rows as they are.
fn yields_ids(source: &Source) -> bool {
	match source {
		Source::Nested {
			..
		} => true,
		Source::Union(sources) => sources.iter().any(yields_ids),
		_ => false,
	}
}

/// Whether `expr` holds a statement or a closure, whose body reads its
/// parameters from the frame it is evaluated in.
pub(super) fn holds_statement(expr: &LogicalExpr) -> bool {
	expr.any(|node| matches!(node, LogicalExpr::Subquery(_) | LogicalExpr::Closure(_)))
}

/// Whether `expr` holds something that is lowered for the caller's
/// authority: a statement, a closure, or a call of a user-defined function.
pub(super) fn needs_caller(expr: &LogicalExpr) -> bool {
	holds_statement(expr)
		|| expr.any(|node| {
			matches!(node, LogicalExpr::Call(call)
				if matches!(call.receiver, crate::expr::Function::Custom(_)))
		})
}

/// A table's field view: its layers, top first, and whether the caller's
/// reads are checked.
type ViewLayers = (Vec<(LogicalFields, crate::expr::logical::Schema)>, bool);

/// An index function call and what it reports against.
struct IndexCall<'c> {
	name: String,
	binding: IndexBinding,
	/// The arguments evaluated at the call, less the clause reference.
	args: Vec<&'c LogicalExpr>,
	/// The clause's query, where it is read when the call runs: evaluated
	/// after `args`, as the call's last argument
	/// ([`IndexBinding::FullText::query_arg`]).
	query: Option<LogicalExpr>,
}

/// The state of the pipeline in flight, set aside while a host pipeline is
/// built beside it.
struct Aside {
	code: Vec<Instr>,
	next_reg: Reg,
	released: Vec<Reg>,
	uses_eval: bool,
	may_fail: bool,
	rows_are_objects: bool,
	schema: Option<crate::expr::logical::Schema>,
	eval_frame: Option<u16>,
	document: DocumentRoot,
}

/// Hand every table a program's statements share from `from` to `to`: the
/// pipelines and the tables their instructions index, the block's groups, and
/// the counters that number frames and loop slots.
fn lend<'c>(from: &mut Lowering<'_, 'c>, to: &mut Lowering<'_, 'c>) {
	macro_rules! lend {
		($($field:ident),* $(,)?) => {
			$(std::mem::swap(&mut from.$field, &mut to.$field);)*
		};
	}
	lend!(
		pipelines,
		consts,
		fields,
		paths,
		limits,
		sorts,
		splits,
		joins,
		distincts,
		fetches,
		feeders,
		aggregates,
		permissions,
		computed_names,
		call_needs,
		host_code,
		groups,
		analyses,
		served,
		served_ops,
		kinds,
		params,
		calls,
		matches,
		externals,
		degrees,
		methods,
		record_ids,
		subqueries,
		nested,
		probes,
		reserving,
		reservations,
		implicit_tables,
		units,
		lookups,
		functions,
		fn_links,
		cascade_links,
		observer_links,
		events_queue,
		closure_links,
		closures,
		closure_values,
		invokes,
		locks,
		row_ordered_calls,
		call_frames,
		iter_slots,
		slots,
		reach,
		late_reach,
	);
}

/// How far each shared table of a lowering reached, to undo a statement that
/// did not lower. The default marks the start of every table.
#[derive(Default)]
struct Marks {
	pipelines: usize,
	consts: usize,
	fields: usize,
	paths: usize,
	limits: usize,
	sorts: usize,
	splits: usize,
	joins: usize,
	distincts: usize,
	fetches: usize,
	feeders: usize,
	aggregates: usize,
	permissions: usize,
	computed_names: usize,
	call_needs: usize,
	host_code: usize,
	groups: usize,
	analyses: usize,
	served: usize,
	kinds: usize,
	params: usize,
	calls: usize,
	matches: usize,
	externals: usize,
	degrees: usize,
	methods: usize,
	record_ids: usize,
	subqueries: usize,
	nested: usize,
	probes: usize,
	reservations: usize,
	implicit_tables: usize,
	units: usize,
	lookups: usize,
	functions: usize,
	fn_links: usize,
	cascade_links: usize,
	observer_links: usize,
	closure_links: usize,
	closures: usize,
	closure_values: usize,
	invokes: usize,
	locks: usize,
	row_ordered_calls: usize,
	call_frames: u16,
	iter_slots: u16,
	catalog_changed: Option<(ContextLevel, usize)>,
}

impl Marks {
	fn of(l: &Lowering<'_, '_>) -> Self {
		Self {
			pipelines: l.pipelines.len(),
			consts: l.consts.len(),
			fields: l.fields.len(),
			paths: l.paths.len(),
			limits: l.limits.len(),
			sorts: l.sorts.len(),
			splits: l.splits.len(),
			joins: l.joins.len(),
			distincts: l.distincts.len(),
			fetches: l.fetches.len(),
			feeders: l.feeders.len(),
			aggregates: l.aggregates.len(),
			permissions: l.permissions.len(),
			computed_names: l.computed_names.len(),
			call_needs: l.call_needs.len(),
			host_code: l.host_code.len(),
			groups: l.groups.len(),
			analyses: l.analyses.len(),
			served: l.served.len(),
			kinds: l.kinds.len(),
			params: l.params.len(),
			calls: l.calls.len(),
			matches: l.matches.len(),
			externals: l.externals.len(),
			degrees: l.degrees.len(),
			methods: l.methods.len(),
			record_ids: l.record_ids.len(),
			subqueries: l.subqueries.len(),
			nested: l.nested.len(),
			probes: l.probes.len(),
			reservations: l.reservations.len(),
			implicit_tables: l.implicit_tables.len(),
			units: l.units.len(),
			lookups: l.lookups.len(),
			functions: l.functions.len(),
			fn_links: l.fn_links.len(),
			cascade_links: l.cascade_links.len(),
			observer_links: l.observer_links.len(),
			closure_links: l.closure_links.len(),
			closures: l.closures.len(),
			closure_values: l.closure_values.len(),
			invokes: l.invokes,
			locks: l.locks,
			row_ordered_calls: l.row_ordered_calls,
			call_frames: l.call_frames,
			iter_slots: l.iter_slots,
			catalog_changed: l.catalog_changed,
		}
	}

	fn restore(&self, l: &mut Lowering<'_, '_>) {
		l.pipelines.truncate(self.pipelines);
		l.reservations.truncate(self.reservations);
		l.implicit_tables.truncate(self.implicit_tables);
		l.units.truncate(self.units);
		l.consts.truncate(self.consts);
		l.fields.truncate(self.fields);
		l.paths.truncate(self.paths);
		l.limits.truncate(self.limits);
		l.sorts.truncate(self.sorts);
		l.splits.truncate(self.splits);
		l.joins.truncate(self.joins);
		l.distincts.truncate(self.distincts);
		l.fetches.truncate(self.fetches);
		l.feeders.truncate(self.feeders);
		l.aggregates.truncate(self.aggregates);
		l.permissions.truncate(self.permissions);
		l.computed_names.truncate(self.computed_names);
		l.call_needs.truncate(self.call_needs);
		l.catalog_changed = self.catalog_changed;
		l.host_code.truncate(self.host_code);
		l.groups.truncate(self.groups);
		l.analyses.truncate(self.analyses);
		l.served.truncate(self.served);
		l.served_ops.truncate(self.served);
		l.kinds.truncate(self.kinds);
		l.params.truncate(self.params);
		l.calls.truncate(self.calls);
		l.matches.truncate(self.matches);
		l.externals.truncate(self.externals);
		l.degrees.truncate(self.degrees);
		l.methods.truncate(self.methods);
		l.record_ids.truncate(self.record_ids);
		l.subqueries.truncate(self.subqueries);
		l.nested.truncate(self.nested);
		l.probes.truncate(self.probes);
		l.lookups.truncate(self.lookups);
		l.functions.truncate(self.functions);
		l.fn_links.truncate(self.fn_links);
		l.cascade_links.truncate(self.cascade_links);
		l.observer_links.truncate(self.observer_links);
		l.closure_links.truncate(self.closure_links);
		l.closures.truncate(self.closures);
		l.closure_values.truncate(self.closure_values);
		l.invokes = self.invokes;
		l.locks = self.locks;
		l.row_ordered_calls = self.row_ordered_calls;
		l.call_frames = self.call_frames;
		l.iter_slots = self.iter_slots;
	}
}

/// The registers a projection layer's columns land in: the fields it sets,
/// the fields it cuts, and the reduced record its computed columns read, when
/// they read one.
struct LayerTargets {
	sets: Reg,
	cuts: Reg,
	reduced: Option<Reg>,
}

/// Where a `RETURN` signalled inside an expression lands, as the operator
/// tree takes one at the expression's root.
struct ReturnCatch {
	/// The register the returned value goes to.
	root: Reg,
	/// The subquery instructions to point at the expression's end.
	exits: Vec<Label>,
	/// Whether a signal is taken inside a permission frame too: a record-id
	/// key element takes one in any frame, a statement's root only in its
	/// own.
	any_frame: bool,
	/// Whether a `BREAK` or `CONTINUE` a statement raises is an error: it is
	/// in a record-id key element, which no loop encloses.
	loops_fail: bool,
}

impl<'p, 'ctx> Lowering<'p, 'ctx> {
	fn new(planner: &'p Planner<'ctx>) -> Self {
		#[cfg(test)]
		LOWERINGS.with(|lowerings| lowerings.set(lowerings.get() + 1));
		Self {
			planner,
			catalog: Arc::default(),
			ddl_checks: None,
			bare: None,
			binds_in_frames: false,
			pipelines: Vec::new(),
			consts: Vec::new(),
			fields: Vec::new(),
			paths: Vec::new(),
			limits: Vec::new(),
			sorts: Vec::new(),
			splits: Vec::new(),
			joins: Vec::new(),
			distincts: Vec::new(),
			fetches: Vec::new(),
			feeders: Vec::new(),
			aggregates: Vec::new(),
			permissions: Vec::new(),
			computed_names: Vec::new(),
			call_needs: Vec::new(),
			host_code: Vec::new(),
			schema: None,
			table: None,
			code: Vec::new(),
			pointed_at: None,
			next_reg: ROW + 1,
			uses_eval: false,
			reads_sorted_rows: false,
			may_fail: false,
			eval_frame: None,
			privileged: false,
			unchecked_reads: false,
			pipeline_limits: Vec::new(),
			rows_are_objects: true,
			rows_may_be_ids: false,
			fetched_row: None,
			delegate_context: ContextLevel::Root,
			catalog_changed: None,
			write: None,
			write_output: None,
			write_table: None,
			write_form: None,
			write_logical: None,
			write_catalog_fixed: false,
			refuses_when_run: false,
			next_group_refuses: false,
			stage_sites: None,
			view_layers_of: HashMap::new(),
			image_view_of: HashMap::new(),
			snapshot_of: None,
			shape_of: None,
			place_refused: None,
			views_of: None,
			block_bound: Vec::new(),
			slot_names: Vec::new(),
			slots: 0,
			frame_reads: 0,
			slot_hidden: 0,
			slot_scopes: Vec::new(),
			sources: Vec::new(),
			loops: Vec::new(),
			iter_slots: 0,
			saved: 0,
			groups: Vec::new(),
			analyses: Vec::new(),
			served: Vec::new(),
			served_ops: Vec::new(),
			serves_late: false,
			current_group: None,
			reach: None,
			late_reach: None,
			scalar: false,
			ahead: false,
			writes: false,
			released: Vec::new(),
			knn_scope_slot: None,
			writing_labels: false,
			element: None,
			omitted_row: None,
			tempfiles: false,
			topk_sort: None,
			index_scope: IndexScope::default(),
			kinds: Vec::new(),
			params: Vec::new(),
			calls: Vec::new(),
			matches: Vec::new(),
			externals: Vec::new(),
			degrees: Vec::new(),
			methods: Vec::new(),
			record_ids: Vec::new(),
			subqueries: Vec::new(),
			nested: Vec::new(),
			nested_plans: Vec::new(),
			probes: Vec::new(),
			id_draws: 0,
			reserving: Vec::new(),
			reservations: Vec::new(),
			implicit_tables: Vec::new(),
			units: Vec::new(),
			probe_owner: None,
			lookups: Vec::new(),
			functions: Vec::new(),
			fn_links: Vec::new(),
			cascade_links: Vec::new(),
			observer_links: Vec::new(),
			events_queue: Default::default(),
			closure_links: Vec::new(),
			caller_suppressed: planner.perms_suppressed,
			generic_table: false,
			reach_override: None,
			stage_sets: false,
			edge_statement: false,
			edges_enclosing: None,
			bundle: false,
			versioned: false,
			closures: Vec::new(),
			closure_values: Vec::new(),
			invokes: 0,
			locks: 0,
			row_ordered_calls: 0,
			control_ifs: 0,
			control_nested: false,
			branch_seq: false,
			call_frames: 0,
			inline_params: Vec::new(),
			return_catch: None,
			shared: Vec::new(),
			key_element: false,
			column_statements: false,
			statement_doc: None,
			document: DocumentRoot::Unknown,
			rows: false,
		}
	}

	fn finish(self) -> Lowered<Program> {
		let (delegate_context, needs) = match self.catalog_changed {
			Some(changed) => changed,
			None => (self.delegate_context, self.call_needs.len()),
		};
		let all = self.call_needs[..needs.min(self.call_needs.len())].iter();
		let access_mode = if self.write.is_some() || self.writes {
			AccessMode::ReadWrite
		} else {
			self.call_needs
				.iter()
				.map(|n| n.access_mode)
				.chain(self.subqueries.iter().map(|s| s.access_mode))
				.chain(self.functions.iter().map(|f| f.body_access_mode))
				.chain(self.closure_values.iter().map(|c| c.access_mode))
				.chain((self.invokes > 0).then_some(AccessMode::ReadWrite))
				.chain((self.locks > 0).then_some(AccessMode::ReadWrite))
				.chain(
					self.groups.iter().any(|g| g.write.is_some()).then_some(AccessMode::ReadWrite),
				)
				.fold(AccessMode::ReadOnly, AccessMode::combine)
		};
		let required_context = all.map(|n| n.context).fold(
			if self.write.is_some() || self.table.is_some() {
				ContextLevel::Database
			} else {
				delegate_context
			},
			Ord::max,
		);
		// A program that may write, or that calls something whose answers
		// depend on the calls made before it, runs its rows one after another
		// in the order read, never as batches that overlap. A pipeline of a
		// statement of a block is its statement's to decide ([`Self::group`]);
		// every other is the program's.
		let keeps_row_order = self.keeps_row_order_since(&Marks::default());
		let mut pipelines = self.pipelines;
		if access_mode == AccessMode::ReadWrite || keeps_row_order {
			for pipeline in pipelines.iter_mut().filter(|pipeline| pipeline.group.is_none()) {
				pipeline.region = false;
			}
		}
		Ok(Program {
			pipelines,
			consts: ValueData::new(self.consts, "the constant pool")?,
			fields: self.fields.iter().map(|name| FieldName::new(name)).collect(),
			paths: self.paths,
			limits: self.limits,
			sorts: self.sorts,
			splits: self.splits,
			joins: self.joins,
			distincts: self.distincts,
			fetches: self.fetches,
			aggregates: self.aggregates,
			permissions: self.permissions,
			computed_names: self.computed_names,
			table: self.table,
			access_mode,
			write: self.write,
			scalar: self.scalar,
			rows: self.rows,
			groups: self.groups,
			analyses: self.analyses,
			served: self.served,
			kinds: self.kinds,
			params: self.params,
			calls: self.calls,
			matches: self.matches,
			externals: self.externals,
			degrees: self.degrees,
			methods: self.methods,
			record_ids: self.record_ids,
			subqueries: self.subqueries,
			nested: self.nested,
			probes: self.probes,
			lookups: self.lookups,
			functions: self.functions,
			closures: self.closures,
			closure_values: self.closure_values,
			host_code: self.host_code,
			reservations: self.reservations,
			implicit_tables: self.implicit_tables,
			units: self.units.into(),
			reach: self.reach,
			dispatch: None,
			unit: None,
			tail: None,
			events: None,
			required_context,
			keeps_row_order,
			request: None,
		})
	}

	/// Whether something lowered since `marks` keeps row order: a builtin
	/// or method the registry declares as keeping it
	/// ([`FunctionRegistry::keeps_row_order`](crate::exec::function::FunctionRegistry::keeps_row_order)),
	/// a user-defined function whose reachable bodies call one, a closure
	/// literal whose body is not deterministic, a nested statement that keeps
	/// it, or code the host runs. Rows such a statement evaluates are not
	/// split into batches that run at once, as a write's are not.
	fn keeps_row_order_since(&self, marks: &Marks) -> bool {
		let registry = self.planner.function_registry();
		self.row_ordered_calls > marks.row_ordered_calls
			|| self.calls[marks.calls..].iter().any(|call| registry.keeps_row_order(&call.name))
			|| self.methods[marks.methods..]
				.iter()
				.any(|method| registry.method_keeps_row_order(&method.name))
			|| self.closure_values[marks.closure_values..].iter().any(|closure| {
				!crate::exec::logical::effects_of_body(&closure.closure.body, registry)
					.is_deterministic()
			}) || self.subqueries[marks.subqueries..].iter().any(|subquery| subquery.keeps_row_order)
			|| self.externals.len() > marks.externals
	}

	/// Lower `plan`, a statement of the block this lowering compiles, into the
	/// program as a group of its own ([`super::program::GroupSpec`]) planned
	/// with `planner`, and answer the group with the access mode and context
	/// level the statement gives the program. A statement that does not lower
	/// leaves the program as it was.
	async fn group(
		&mut self,
		planner: &Planner<'ctx>,
		plan: &PhysicalPlan,
	) -> Lowered<(u16, AccessMode, ContextLevel)> {
		let group = u16::try_from(self.groups.len())
			.map_err(|_| NotCovered::internal("more statements than a block holds"))?;
		let marks = Marks::of(self);
		let mut child = Lowering::new(planner);
		child.catalog = Arc::clone(&self.catalog);
		child.refuses_when_run = std::mem::take(&mut self.next_group_refuses);
		lend(self, &mut child);
		child.ahead = self.ahead;
		child.bundle = self.bundle;
		child.versioned = self.versioned;
		child.caller_suppressed = self.caller_suppressed;
		child.reach_override.clone_from(&self.reach_override);
		child.stage_sets = self.stage_sets;
		child.edge_statement = self.edge_statement;
		child.edges_enclosing = self.edges_enclosing;
		child.current_group = Some(group);
		child.index_scope.clone_from(&self.index_scope);
		child.knn_scope_slot = self.knn_scope_slot;
		child.writing_labels = self.writing_labels;
		child.probe_owner = self.probe_owner;
		// A statement held in definer code runs in a privileged frame, or in a
		// call's frame inside one, which holds none of the caller's bindings:
		// of the block's slots, every one the caller's, it inherits none.
		child.slot_names = match self.in_privileged_frame() {
			true => Vec::new(),
			false => self.slot_names.clone(),
		};
		child.privileged = self.in_privileged_frame();
		child.serves_late = self.serves_late;
		// Reserved before the statement lowers, so a group nested in it takes
		// the next index.
		child.groups.push(super::program::GroupSpec {
			write: None,
			scalar: false,
			rowless: false,
			sorts: 0..0,
			aggregates: 0..0,
			permissions: 0..0,
			version: None,
			timeout: None,
			only: false,
		});
		let tail = &plan.tail;
		let mut version = None;
		let mut timeout = None;
		let lowered = match crate::exec::vm::host::out_of_line(|| child.root(plan)).await {
			Ok(()) => {
				crate::exec::vm::host::out_of_line(|| child.tail(tail, &mut version, &mut timeout))
					.await
			}
			Err(why) => Err(why),
		};
		let lowered = match lowered {
			Ok(()) => child.modes(&marks),
			Err(why) => Err(why),
		};
		// A statement whose expressions write, lock what they read, or call
		// something whose answers depend on the calls made before it, runs its
		// rows one after another in the order written, never as batches that
		// overlap.
		if matches!(&lowered, Ok((AccessMode::ReadWrite, _)))
			|| (lowered.is_ok() && child.keeps_row_order_since(&marks))
		{
			for pipeline in &mut child.pipelines[marks.pipelines..] {
				pipeline.region = false;
			}
		}
		let spec = super::program::GroupSpec {
			// The shape the tail gives the statement: `ONLY` unwraps its one
			// row, as a write's `ONLY` does with or without a `TIMEOUT`, and
			// any other statement with a `TIMEOUT` hands on rows.
			scalar: match &child.write {
				_ if tail.only => true,
				_ if child.rows => false,
				Some(spec) if spec.only => true,
				_ if timeout.is_some() => false,
				_ => child.scalar,
			},
			version,
			timeout,
			only: tail.only,
			rowless: child.scalar,
			sorts: marks.sorts as u16..child.sorts.len() as u16,
			aggregates: marks.aggregates as u16..child.aggregates.len() as u16,
			permissions: marks.permissions as u16..child.permissions.len() as u16,
			write: child.write.take(),
		};
		self.frame_reads += child.frame_reads;
		lend(&mut child, self);
		match lowered {
			Ok((access_mode, required_context)) => {
				self.groups[group as usize] = spec;
				Ok((group, access_mode, required_context))
			}
			Err(why) => {
				marks.restore(self);
				Err(why)
			}
		}
	}

	/// The tail of the statement this lowering compiles, for the program to
	/// apply when it runs; `None` for a statement with no clause.
	async fn program_tail(
		&mut self,
		tail: &super::plan::Tail,
	) -> Lowered<Option<super::program::TailSpec>> {
		let mut version = None;
		let mut timeout = None;
		crate::exec::vm::host::out_of_line(|| self.tail(tail, &mut version, &mut timeout)).await?;
		Ok((version.is_some() || timeout.is_some() || tail.only).then_some(
			super::program::TailSpec {
				version,
				timeout,
				only: tail.only,
			},
		))
	}

	/// The clauses of a statement's tail, as host scalars the run evaluates
	/// when the statement starts.
	async fn tail(
		&mut self,
		tail: &super::plan::Tail,
		version: &mut Option<super::program::HostScalar>,
		timeout: &mut Option<super::program::HostScalar>,
	) -> Lowered<()> {
		if let Some(expr) = &tail.version {
			*version = Some(crate::exec::vm::host::out_of_line(|| self.host_scalar(expr)).await?);
		}
		match &tail.timeout {
			None | Some(LogicalExpr::Literal(crate::expr::logical::LogicalLiteral::None)) => {}
			Some(expr) => {
				*timeout =
					Some(crate::exec::vm::host::out_of_line(|| self.host_scalar(expr)).await?);
			}
		}
		Ok(())
	}

	/// The access mode and context level of the statement this lowering has
	/// lowered since `marks`, as [`Self::finish`] declares a program's.
	fn modes(&self, marks: &Marks) -> Lowered<(AccessMode, ContextLevel)> {
		let all = self.call_needs[marks.call_needs..].iter();
		let subqueries = &self.subqueries[marks.subqueries..];
		let functions = &self.functions[marks.functions..];
		let closure_values = &self.closure_values[marks.closure_values..];
		let nested_writes = self.groups[(marks.groups + 1).min(self.groups.len())..]
			.iter()
			.any(|g| g.write.is_some());
		if self.write.is_some() || self.scalar {
			let access_mode = if self.write.is_some() || self.writes {
				AccessMode::ReadWrite
			} else {
				self.call_needs[marks.call_needs..]
					.iter()
					.map(|n| n.access_mode)
					.chain(subqueries.iter().map(|s| s.access_mode))
					.chain(functions.iter().map(|f| f.body_access_mode))
					.chain(closure_values.iter().map(|c| c.access_mode))
					.chain((self.invokes > marks.invokes).then_some(AccessMode::ReadWrite))
					.chain((self.locks > marks.locks).then_some(AccessMode::ReadWrite))
					.chain(nested_writes.then_some(AccessMode::ReadWrite))
					.fold(AccessMode::ReadOnly, AccessMode::combine)
			};
			let required_context = all.map(|n| n.context).fold(
				if self.write.is_some() {
					ContextLevel::Database
				} else {
					self.delegate_context
				},
				Ord::max,
			);
			return Ok((access_mode, required_context));
		}
		let access_mode = all
			.clone()
			.map(|n| n.access_mode)
			.chain(subqueries.iter().map(|s| s.access_mode))
			.chain(functions.iter().map(|f| f.body_access_mode))
			.chain(closure_values.iter().map(|c| c.access_mode))
			.chain((self.invokes > marks.invokes).then_some(AccessMode::ReadWrite))
			.chain(nested_writes.then_some(AccessMode::ReadWrite))
			.fold(AccessMode::ReadOnly, AccessMode::combine);
		// A statement's own locked reads need a writeable transaction without
		// making it a write.
		let access_mode = if self.locks > marks.locks {
			AccessMode::ReadWrite
		} else {
			access_mode
		};
		let required_context = all.map(|n| n.context).fold(self.delegate_context, Ord::max).max(
			if self.table.is_some() {
				ContextLevel::Database
			} else {
				ContextLevel::Root
			},
		);
		Ok((access_mode, required_context))
	}

	/// Lower a mutation: one pipeline over its subjects, whose body runs each
	/// record through the form's orders with the control flow between them
	/// that [`Branching`] describes.
	async fn write(&mut self, write: &LogicalWrite, subjects: &WriteSubjects) -> Lowered<()> {
		let facts = write_facts(write);
		let plan = WritePlan::build(facts);
		self.write_output = write.statement.output().cloned();
		self.write_table = write_table(write);
		self.write_form = Some(facts.form);
		self.write_logical = Some(write.clone());
		// A delete runs the repairs other tables' references declare, whose
		// statements its footprint does not name.
		self.write_catalog_fixed =
			!matches!(write.statement, crate::expr::logical::LogicalWriteStatement::Delete(_))
				&& !crate::exec::logical::footprint::of(
					&LogicalOp::Write(write.clone()),
					self.planner.function_registry(),
				)
				.writes_catalog();
		// A late-bound write runs each record of a table through the bundle of
		// the table's stages for the catalog as it stands when the record is
		// written ([`Self::stage_tables`]).
		if self.write_table.is_none() || self.late_stages() {
			self.stage_sites = Some(Vec::new());
		}
		match subjects {
			WriteSubjects::Targets(targets) => {
				let mut compiled = Vec::with_capacity(targets.len());
				for target in targets {
					compiled.push(match target {
						LogicalExpr::Mock(mock) => super::program::Target::Mock(mock.clone()),
						target => super::program::Target::Scalar(
							crate::exec::vm::host::out_of_line(|| self.host_scalar(target)).await?,
						),
					});
				}
				let source = Source::WriteTargets {
					targets: compiled,
					guarantees: false,
					// `Statement::is_deferable`: a table names a record to
					// invent for these, and the records stored there for the
					// others.
					deferable: matches!(facts.form, WriteForm::Create)
						|| (matches!(facts.form, WriteForm::Upsert) && write.cond.is_none()),
				};
				crate::exec::vm::host::out_of_line(|| self.write_body(&plan, false, true)).await?;
				self.push_write_pipeline(source)?;
			}
			WriteSubjects::Inserts(inserts) => {
				let source = crate::exec::vm::host::out_of_line(|| {
					self.insert_source(&write.statement, inserts)
				})
				.await?;
				// `RELATE` and `INSERT` ingest their subjects without the
				// statement gate: the creating order's own permission check
				// answers for them.
				crate::exec::vm::host::out_of_line(|| self.write_body(&plan, false, false)).await?;
				self.push_write_pipeline(source)?;
			}
			WriteSubjects::Edges {
				from,
				through,
				to,
			} => {
				let source = Source::WriteEdges {
					from: crate::exec::vm::host::out_of_line(|| self.host_scalar(from)).await?,
					through: crate::exec::vm::host::out_of_line(|| self.host_scalar(through))
						.await?,
					to: crate::exec::vm::host::out_of_line(|| self.host_scalar(to)).await?,
				};
				crate::exec::vm::host::out_of_line(|| self.write_body(&plan, false, false)).await?;
				self.push_write_pipeline(source)?;
			}
			WriteSubjects::Rows(input) => {
				crate::exec::vm::host::out_of_line(|| self.write_rows(input, &plan)).await?
			}
			WriteSubjects::Guaranteed {
				rows,
				targets,
			} => {
				crate::exec::vm::host::out_of_line(|| self.write_rows(rows, &plan)).await?;
				// The guaranteed record is written by a pipeline of its own,
				// run once the read's pipeline has judged every record.
				let mut compiled = Vec::with_capacity(targets.len());
				for target in targets {
					compiled.push(super::program::Target::Scalar(
						crate::exec::vm::host::out_of_line(|| self.host_scalar(target)).await?,
					));
				}
				let source = Source::WriteTargets {
					targets: compiled,
					deferable: false,
					guarantees: true,
				};
				crate::exec::vm::host::out_of_line(|| self.write_body(&plan, false, true)).await?;
				self.push_write_pipeline(source)?;
			}
		}
		let sites = self.stage_sites.take().unwrap_or_default();
		let shape =
			crate::exec::vm::host::out_of_line(|| self.write_shape()).await.unwrap_or_default();
		let tables = if sites.is_empty() {
			None
		} else {
			Some(crate::exec::vm::host::out_of_line(|| self.stage_tables(write, &sites)).await?)
		};
		self.write = Some(WriteSpec {
			form: facts.form,
			shape,
			output: write.statement.output().cloned(),
			only: write.statement.only(),
			tables,
		});
		Ok(())
	}

	/// Lower the pipeline that runs the write body `plan` over the rows read
	/// `input` yields. A statement in `FROM` pages and holds its rows in a
	/// group of its own, which the write reads only once it has run.
	async fn write_rows(&mut self, input: &Plan, plan: &WritePlan) -> Lowered<()> {
		let (limits, sorts, aggregates, groups) =
			(self.limits.len(), self.sorts.len(), self.aggregates.len(), self.groups.len());
		crate::exec::vm::host::out_of_line(|| self.pipeline(input, Sink::Write(plan))).await?;
		let nested = &self.groups[groups..];
		let own = |at: usize, of: fn(&super::program::GroupSpec) -> std::ops::Range<u16>| {
			!nested.iter().any(|group| of(group).contains(&(at as u16)))
		};
		let pages = self.limits[limits..].iter().any(|spec| spec.group == self.current_group);
		let holds = (sorts..self.sorts.len()).any(|at| own(at, |g| g.sorts.clone()))
			|| (aggregates..self.aggregates.len()).any(|at| own(at, |g| g.aggregates.clone()));
		if pages || holds {
			return Err(NotCovered::internal("a write over rows a read holds or pages"));
		}
		Ok(())
	}

	/// Whether the write being lowered is late-bound and loads the stages of
	/// each table it writes by name: one a late-bound planner lowers that is
	/// neither a cascade's nor beneath an edge cascade's stage set.
	fn late_stages(&self) -> bool {
		self.planner.is_late_bound() && self.reach_override.is_none() && !self.edge_statement
	}

	/// The units that apply the runs of stages `sites` names to a record of
	/// each table `write` can reach.
	///
	/// A write that names its tables holds, as units of the statement's
	/// arena, a unit for each of them the catalog holds and one for those it
	/// does not; the statement's lowering reads the catalog of each, so a
	/// change to any of them ends the plan's reuse. A write that may reach any
	/// table holds none: it names its stages, and its run loads the bundle of
	/// a table's stages by the table's name from the compile cache the first
	/// time it writes a record of the table, compiled on a miss
	/// ([`stage_bundle_for`]). Planning never lists the database's tables.
	async fn stage_tables(
		&mut self,
		write: &LogicalWrite,
		sites: &[Vec<StageId>],
	) -> Lowered<super::program::StageTables> {
		let reach = match &self.reach_override {
			Some(reach) => reach.clone(),
			None => write_reach(write),
		};
		let frame = StageFrame {
			ahead: self.ahead,
			caller_suppressed: self.caller_suppressed,
			generic_table: false,
			edges: self.edges_enclosing,
		};
		// A write that may reach any table names its stages, not the tables:
		// the run loads the bundle of a table's stages by the table's name the
		// first time it writes a record of it ([`stage_bundle_for`]). So does a
		// late-bound write, whose tables' catalog may change while it runs: it
		// loads the bundle for the catalog version each record's write meets.
		let late = self.late_stages();
		if matches!(reach, super::dispatch::Reach::Any) || late {
			let edges = match self.stage_sets {
				true => self.edge_statement.then_some(self.caller_suppressed),
				false => self.edges_enclosing,
			};
			let named = Arc::new(super::units::StageSetUnit::new(
				write.clone(),
				sites.to_vec(),
				frame.ahead,
				frame.caller_suppressed,
				edges,
			));
			self.delegate_context = self.delegate_context.max(ContextLevel::Database);
			return Ok(super::program::StageTables {
				tables: Vec::new(),
				default: None,
				reach,
				by_name: Some(named),
				revalidates: late,
			});
		}
		// A reach the write names is read by its plan. One that data names,
		// or that a cascade knows from the key that named the record, is read
		// here for the stage units that serve it, unsealed
		// ([`crate::kvs::catalog_reads::sealed`]).
		let (held, catalog) = if self.reach_override.is_none()
			&& let super::dispatch::Reach::Tables(named) = &reach
		{
			let mut held = Vec::with_capacity(named.len());
			if let Some(txn) = self.planner.txn() {
				for table in named {
					if self.catalog.table(txn, table)?.is_some_and(|read| read.definition.is_some())
					{
						held.push(table.clone());
					}
				}
			}
			(held, Arc::clone(&self.catalog))
		} else {
			let planner = self.planner;
			crate::kvs::catalog_reads::sealed(false, async {
				let named = match &reach {
					super::dispatch::Reach::Tables(named) => named.clone(),
					super::dispatch::Reach::Any => Default::default(),
				};
				let held = super::dispatch::candidates(planner, &named).await?;
				let mut read = held.clone();
				read.extend(named.iter().cloned());
				let catalog =
					super::plan_catalog::read_written(planner, write, &read, false).await?;
				Ok::<_, NotCovered>((held, Arc::new(catalog)))
			})
			.await?
		};
		// A table the write reaches that the catalog does not hold, or any
		// table at all where the write reaches them all, falls to the unit of
		// a table with no definitions.
		let absent = match &reach {
			super::dispatch::Reach::Tables(named) => named
				.iter()
				.find(|name| held.binary_search(name).is_err())
				.map(|table| (table.clone(), frame)),
			super::dispatch::Reach::Any => None,
		};
		let mut tables = Vec::with_capacity(held.len());
		for table in held {
			let unit = crate::exec::vm::host::out_of_line(|| {
				self.stage_unit(write, table.clone(), sites, frame, &catalog)
			})
			.await?;
			tables.push((table, unit));
		}
		let default = match absent {
			Some((table, frame)) => Some(
				crate::exec::vm::host::out_of_line(|| {
					self.stage_unit(write, table, sites, frame, &catalog)
				})
				.await?,
			),
			None => None,
		};
		Ok(super::program::StageTables {
			tables,
			default,
			reach,
			by_name: None,
			revalidates: false,
		})
	}

	/// The stages `sites` names lowered for records of `table`, as a unit of
	/// the program being lowered.
	async fn stage_unit(
		&mut self,
		write: &LogicalWrite,
		table: surrealdb_strand::TableName,
		sites: &[Vec<StageId>],
		frame: StageFrame,
		catalog: &Arc<super::plan_catalog::PlanCatalog>,
	) -> Lowered<super::program::UnitId> {
		let mut arena = self.take_arena();
		let lowered = stage_program(
			self.planner,
			write,
			table,
			sites,
			&mut arena,
			frame,
			catalog,
			&self.index_scope,
		)
		.await;
		self.put_arena(arena);
		let id = lowered?;
		if let Some(program) = self.units.get(id.0 as usize) {
			self.delegate_context = self.delegate_context.max(program.required_context);
		}
		Ok(id)
	}

	/// Lower an expression statement: its value, evaluated once, is the
	/// statement's.
	async fn eval_statement(&mut self, expr: &LogicalExpr, fetches: &[LogicalExpr]) -> Lowered<()> {
		let mut value = crate::exec::vm::host::out_of_line(|| self.once_value(expr, false)).await?;
		if !fetches.is_empty() {
			value =
				crate::exec::vm::host::out_of_line(|| self.fetched(fetches, value, None)).await?;
		}
		let value = Operand::Reg(value);
		let dst = self.register()?;
		self.emit(Instr::Load {
			dst,
			src: value,
		});
		self.emit(Instr::Emit {
			src: dst,
		});
		self.emit(Instr::EndRow);
		self.push_write_pipeline(Source::Once)?;
		self.scalar = true;
		Ok(())
	}

	/// A register holding the value in `src` with `fetches` fetched into it,
	/// in order, as `FETCH` fetches into a row. `rows` are the tables `src`
	/// is a record of, as their catalog defines them, where it is known to be
	/// one: the program then reads records of the tables the fetched fields'
	/// kinds name ([`super::dispatch::fetch_reach`]), and of any table
	/// otherwise.
	async fn fetched(
		&mut self,
		fetches: &[LogicalExpr],
		src: Reg,
		rows: Option<&std::collections::BTreeSet<surrealdb_strand::TableName>>,
	) -> Lowered<Reg> {
		let mut paths = Vec::with_capacity(fetches.len());
		for fetch in fetches {
			// A fetch a parameter names is resolved against the parameter's
			// definition, read as the mapping reads it, unsealed.
			let resolving = self.planner.resolve_field_paths(fetch.clone());
			let resolved =
				crate::kvs::catalog_reads::sealed(false, resolving).await.map_err(|e| {
					match super::plan::Refusal::of(&e) {
						Some(refusal) => NotCovered::refused(refusal),
						None => NotCovered::internal(format!("the FETCH did not resolve: {e}")),
					}
				})?;
			paths.extend(resolved);
		}
		let steps = crate::exec::vm::host::out_of_line(|| self.fetch_steps(&paths)).await?;
		let fetch = u16::try_from(self.fetches.len())
			.map_err(|_| NotCovered::internal("more fetches than a program holds"))?;
		self.fetches.push(steps);
		// The tables the fetch reaches are the program's reach, which its
		// dispatch serves ([`seal`]): read as the dispatch is, unsealed.
		let mut reach = super::dispatch::Reach::none();
		for path in &paths {
			let parts = crate::exec::operators::fetch::fetch_parts(path);
			reach = reach.union(match rows {
				Some(rows) => {
					let reached = super::dispatch::fetch_reach(self.planner, rows, &parts);
					crate::kvs::catalog_reads::sealed(false, reached).await
				}
				None => super::dispatch::Reach::Any,
			});
		}
		// What a schema statement ahead of the fetch may change is judged
		// against every table: the fields the reach was read from are the
		// rows' own.
		self.late_reach = Some(super::dispatch::Reach::Any);
		self.reaches(reach);
		let dst = self.register()?;
		self.emit(Instr::Fetch {
			fetch,
			src,
			dst,
		});
		// The fetch reads records, which a body in the storage cursor cannot.
		self.uses_eval = true;
		Ok(dst)
	}

	/// A register holding `expr`'s value in a program that runs once, over
	/// the statement's row when it has one, a `RETURN` inside it answering it
	/// when `returns_value`; not covered where the body does not answer the
	/// expression itself.
	pub(super) async fn once_value(
		&mut self,
		expr: &LogicalExpr,
		returns_value: bool,
	) -> Lowered<Reg> {
		// The row, when there is one, may be any value: a field the
		// expression names is read by the expression that reads it.
		self.rows_are_objects = false;
		// That row, where there is one, is the expression's document root.
		let document = std::mem::replace(&mut self.document, DocumentRoot::RowElseReceiver);
		let value = if !self.is_native(expr) {
			Err(self.uncovered(expr, "a statement's value"))
		} else if returns_value {
			crate::exec::vm::host::out_of_line(|| self.caught_value(expr)).await
		} else {
			crate::exec::vm::host::out_of_line(|| self.value(expr)).await
		};
		self.document = document;
		let value = value?;
		let dst = self.register()?;
		self.emit(Instr::Load {
			dst,
			src: value,
		});
		Ok(dst)
	}

	/// Push the one pipeline of a write over a source that produces its own
	/// records.
	fn push_write_pipeline(&mut self, source: Source) -> Lowered<()> {
		self.pipelines.push(Pipeline {
			in_place: false,
			stored_rows: false,
			enforce: None,
			source,
			code: std::mem::take(&mut self.code),
			registers: self.next_reg as usize,
			limits: Vec::new(),
			region: false,
			group: self.current_group,
		});
		self.feeders.push(None);
		Ok(())
	}

	/// Emit the body that writes the record in flight — made from the
	/// pipeline's row when `from_row`, and judged by the statement gate when
	/// `gated` — through `plan`'s orders.
	async fn write_body(&mut self, plan: &WritePlan, from_row: bool, gated: bool) -> Lowered<()> {
		let mut skips = Vec::new();
		let mut done = Vec::new();
		if from_row {
			self.emit(Instr::Write(WriteInstr::Record));
		}
		if gated {
			skips.push(self.emit(Instr::Write(WriteInstr::Gate {
				skip: 0,
			})));
		}
		self.emit(Instr::Write(WriteInstr::Begin));
		let order = |which: WriteOrder| {
			plan.order(which).ok_or_else(|| {
				NotCovered::internal(format!("the write plan holds no {} order", which.name()))
			})
		};
		match plan.branching() {
			Branching::Sole(which) => {
				self.write_stages(order(which)?, &mut skips).await?;
			}
			Branching::UpFront {
				creating,
				updating,
			} => {
				self.emit(Instr::Write(WriteInstr::LoadNamedSubject));
				let new = self.emit(Instr::Write(WriteInstr::JumpIfNew {
					target: 0,
				}));
				self.write_stages(order(updating)?, &mut skips).await?;
				done.push(self.emit(Instr::Jump {
					target: 0,
				}));
				let at = self.here();
				self.patch(new, at);
				self.write_stages(order(creating)?, &mut skips).await?;
			}
			// A policy that neither retries nor skips raises every failure of
			// the creating attempt, which fails the statement, so the attempt
			// has nothing to be undone for: it runs as a sole order does, with
			// no savepoint, and a failure leaves what it wrote as a failed
			// `CREATE` does.
			Branching::Retry {
				creating,
				updating,
				policy,
			} if !policy.retry_conflict
				&& !policy.skip_failures
				&& !policy.carry_schema_failure =>
			{
				let read = self.emit(Instr::Write(WriteInstr::JumpIfNotIterationInitial {
					target: 0,
				}));
				self.write_stages(order(creating)?, &mut skips).await?;
				done.push(self.emit(Instr::Jump {
					target: 0,
				}));
				// A record its source read has nothing to create.
				let at = self.here();
				self.patch(read, at);
				self.write_stages(order(updating)?, &mut skips).await?;
			}
			Branching::Retry {
				creating,
				updating,
				policy,
			} => {
				let read = self.emit(Instr::Write(WriteInstr::JumpIfNotIterationInitial {
					target: 0,
				}));
				let creating = order(creating)?;
				// Stages ahead of the savepoint are not part of the attempt
				// and are not undone by rolling it back.
				let split = policy.savepoint_after.min(creating.len());
				crate::exec::vm::host::out_of_line(|| {
					self.write_stages(&creating[..split], &mut skips)
				})
				.await?;
				self.emit(Instr::Write(WriteInstr::SavepointPush));
				let attempt = self.emit(Instr::Write(WriteInstr::TryBegin {
					handler: 0,
				}));
				let mut abandoned = Vec::new();
				crate::exec::vm::host::out_of_line(|| {
					self.write_stages(&creating[split..], &mut abandoned)
				})
				.await?;
				self.emit(Instr::Write(WriteInstr::TryEnd));
				self.emit(Instr::Write(WriteInstr::SavepointRelease));
				done.push(self.emit(Instr::Jump {
					target: 0,
				}));
				// An attempt that abandoned the record keeps what it wrote.
				let at = self.here();
				self.patch_all(abandoned, at);
				self.emit(Instr::Write(WriteInstr::TryEnd));
				self.emit(Instr::Write(WriteInstr::SavepointRelease));
				skips.push(self.emit(Instr::Jump {
					target: 0,
				}));
				// A failed attempt is undone before anything else looks at the
				// transaction. A signal leaves once it is; an error is
				// classified, and a retry reads the record it names afresh
				// unless the statement ran out of time in between.
				let at = self.here();
				self.patch(attempt, at);
				self.emit(Instr::Write(WriteInstr::SavepointRollback));
				self.emit(Instr::Write(WriteInstr::ClearMutated));
				self.emit(Instr::Write(WriteInstr::RethrowSignal));
				skips.push(self.emit(Instr::Write(WriteInstr::ClassifyCreateFailure {
					policy,
					skip: 0,
				})));
				skips.push(self.emit(Instr::Write(WriteInstr::JumpIfDone {
					target: 0,
				})));
				self.emit(Instr::Write(WriteInstr::ReloadForUpdate));
				self.emit(Instr::Write(WriteInstr::CarryBegin));
				let mut carried = Vec::new();
				self.write_stages(order(updating)?, &mut carried).await?;
				self.emit(Instr::Write(WriteInstr::CarryEnd));
				done.push(self.emit(Instr::Jump {
					target: 0,
				}));
				// An update that abandoned the record did not satisfy the
				// failure the attempt carried.
				let at = self.here();
				self.patch_all(carried, at);
				self.emit(Instr::Write(WriteInstr::RaiseCarried));
				self.emit(Instr::Write(WriteInstr::CarryEnd));
				skips.push(self.emit(Instr::Jump {
					target: 0,
				}));
				// A record its source read has nothing to create.
				let at = self.here();
				self.patch(read, at);
				self.write_stages(order(updating)?, &mut skips).await?;
			}
			Branching::ByAction => {
				return Err(NotCovered::internal(
					"a view row is written by its view's maintenance",
				));
			}
		}
		let at = self.here();
		self.patch_all(done, at);
		self.emit(Instr::Write(WriteInstr::End {
			skipped: false,
		}));
		self.emit(Instr::EndRow);
		let at = self.here();
		self.patch_all(skips, at);
		self.emit(Instr::Write(WriteInstr::End {
			skipped: true,
		}));
		self.emit(Instr::EndRow);

		Ok(())
	}

	/// Push one write pipeline per run of `sites`, each the run's stages
	/// answering `true` when every stage kept the record and `false` when one
	/// abandoned it.
	async fn stage_sites(&mut self, sites: &[Vec<StageId>]) -> Lowered<()> {
		for ids in sites {
			self.code = Vec::new();
			self.next_reg = ROW + 1;
			self.released.clear();
			self.uses_eval = false;
			self.may_fail = false;
			let mut skips = Vec::new();
			crate::exec::vm::host::out_of_line(|| self.write_stages(ids, &mut skips)).await?;
			for (kept, answer) in [(true, true), (false, false)] {
				if !kept {
					let here = self.here();
					self.patch_all(std::mem::take(&mut skips), here);
				}
				let src = self.constant(Value::Bool(answer));
				let dst = self.register()?;
				self.emit(Instr::Load {
					dst,
					src,
				});
				self.emit(Instr::Emit {
					src: dst,
				});
				self.emit(Instr::EndRow);
			}
			self.push_write_pipeline(Source::Once)?;
		}
		Ok(())
	}

	/// Emit a run of stages, each jumping to a label in `skips` when it
	/// abandons the record. Every stage lowers to instructions.
	#[deny(clippy::wildcard_enum_match_arm)]
	async fn write_stages(&mut self, ids: &[StageId], skips: &mut Vec<Label>) -> Lowered<()> {
		// A statement naming no single table takes the run as it lowers for
		// each record's table.
		if let Some(sites) = &mut self.stage_sites {
			let site = u16::try_from(sites.len())
				.map_err(|_| NotCovered::internal("more runs of stages than a body holds"))?;
			sites.push(ids.to_vec());
			skips.push(self.emit(Instr::Write(WriteInstr::DispatchTable {
				site,
				skip: 0,
			})));
			return Ok(());
		}
		for id in ids {
			match id {
				StageId::OutputWrite => {
					crate::exec::vm::host::out_of_line(|| self.output_stage()).await?
				}
				StageId::CheckWhereCondition
				| StageId::ComputeInputData
				| StageId::CheckDataFields
				| StageId::ProcessRecordData
				| StageId::ProcessMergeData
				| StageId::DefaultRecordData
				| StageId::GenerateRecordId => {
					crate::exec::vm::host::out_of_line(|| self.data_stage(*id, skips)).await?
				}
				StageId::CheckRecordUserAccess
				| StageId::CheckPermissionsQuickCreate
				| StageId::CheckUpdatePermissions
				| StageId::CheckDeletePermissions
				| StageId::CheckCreatePermissions
				| StageId::RecheckUpdatePermissions
				| StageId::CheckSelectPermissions(_) => {
					crate::exec::vm::host::out_of_line(|| self.gate_stage(*id, skips)).await?
				}
				StageId::ProcessTableFields
				| StageId::CleanupTableFields
				| StageId::MaterialiseObservedFields => {
					crate::exec::vm::host::out_of_line(|| self.schema_stage(*id, skips)).await?
				}
				StageId::ProcessTableEvents(action) => {
					crate::exec::vm::host::out_of_line(|| self.events_stage(*action)).await?
				}
				StageId::ProcessChangefeeds => self.changefeed_stage(),
				StageId::ProcessTableLives(action) => self.lives_stage(*action),
				StageId::ProcessTableViews(action) => {
					crate::exec::vm::host::out_of_line(|| self.views_stage(*action)).await?
				}
				StageId::CheckRecordExists
				| StageId::CheckTableNotView
				| StageId::CheckTableNotLightweight(_)
				| StageId::CheckTableType(_)
				| StageId::ClearRecordData
				| StageId::StoreRecordData
				| StageId::StoreEdgesData
				| StageId::StoreInlineAdjacencyData
				| StageId::ProcessTableReferences
				| StageId::CleanupTableReferences
				| StageId::StoreIndexData
				| StageId::ReclaimDocId
				| StageId::PurgeRecordData => {
					crate::exec::vm::host::out_of_line(|| self.storage_stage(*id, skips)).await?
				}
			}
		}
		Ok(())
	}

	/// Emit the output stage as instructions. The
	/// statement writes to one table, and a table whose field view has no
	/// layer form declines it.
	///
	/// The stage dispatches the `RETURN` clause onto the record's images as
	/// the caller is shown them, as the output stage's body does, and writes
	/// nothing under `OPTION IMPORT`.
	async fn output_stage(&mut self) -> Lowered<()> {
		use super::program::WriteOutput;
		use crate::exec::operators::write::stage::RecordImage;
		use crate::expr::output::Output;
		let Some(table) = self.write_table.clone() else {
			return Err(NotCovered::internal("an output stage with no table to show"));
		};
		let (view, checked) =
			crate::exec::vm::host::out_of_line(|| self.shown_view(&table)).await?;
		let output = self.write_output.clone();
		let projection = match &output {
			Some(Output::Fields(fields)) => {
				Some(crate::exec::vm::host::out_of_line(|| self.return_projection(fields)).await?)
			}
			_ => None,
		};
		let show = |image| {
			Instr::Write(WriteInstr::ShowImage {
				image,
				view,
				checked,
			})
		};
		let set = |output| {
			Instr::Write(WriteInstr::SetOutput {
				output,
			})
		};
		let import = self.emit(Instr::Write(WriteInstr::JumpIfImport {
			target: 0,
		}));
		self.emit(Instr::Write(WriteInstr::ClearCurrentReduced));
		match output {
			Some(Output::None) => {
				self.emit(set(WriteOutput::None));
			}
			Some(Output::Null) => {
				self.emit(set(WriteOutput::Null));
			}
			Some(Output::After) => {
				self.emit(show(RecordImage::Current));
				self.emit(set(WriteOutput::Shown(RecordImage::Current)));
			}
			Some(Output::Before) => {
				self.emit(show(RecordImage::Initial));
				self.emit(set(WriteOutput::Shown(RecordImage::Initial)));
			}
			Some(Output::Diff) => {
				self.emit(show(RecordImage::Initial));
				self.emit(show(RecordImage::Current));
				self.emit(set(WriteOutput::Diff));
			}
			Some(Output::Fields(_)) => {
				self.emit(show(RecordImage::Initial));
				self.emit(show(RecordImage::Current));
				// `RETURN *` and nothing else is the post-image itself, which
				// is how it answers for a record that no longer is one.
				self.emit(set(match projection.flatten() {
					Some(pipeline) => WriteOutput::Projection(pipeline),
					None => WriteOutput::Shown(RecordImage::Current),
				}));
			}
			// An unspecified `RETURN` is the post-image, on the orders that
			// leave one.
			None => {
				let absent = self.emit(Instr::Write(WriteInstr::JumpIfNoPostImage {
					target: 0,
				}));
				self.emit(show(RecordImage::Current));
				self.emit(set(WriteOutput::Shown(RecordImage::Current)));
				let over = self.emit(Instr::Jump {
					target: 0,
				});
				let here = self.here();
				self.patch(absent, here);
				self.emit(set(WriteOutput::None));
				let here = self.here();
				self.patch(over, here);
			}
		}
		let here = self.here();
		self.patch(import, here);
		Ok(())
	}

	/// [`Self::image_view`], and for a table whose field view has no layer
	/// form a host pipeline that puts the image through the table's field
	/// declarations at run time, as the operator tree shows a written record.
	pub(super) async fn shown_view(
		&mut self,
		table: &surrealdb_strand::TableName,
	) -> Lowered<(Option<u16>, bool)> {
		if let Some(view) = crate::exec::vm::host::out_of_line(|| self.image_view(table)).await? {
			return Ok(view);
		}
		let checked = !self.planner.perms_suppressed && self.planner.compiles_for_checked_caller();
		let view = crate::exec::vm::host::out_of_line(|| {
			self.enforced_image(Enforce::Image(table.clone()))
		})
		.await?;
		Ok((Some(view), checked))
	}

	/// A host pipeline over an image that puts it through `enforce` and emits
	/// it.
	pub(super) async fn enforced_image(&mut self, enforce: Enforce) -> Lowered<u16> {
		let aside = self.set_aside();
		let built = crate::exec::vm::host::out_of_line(|| self.view_body(&[])).await;
		let pipeline = self.restore(aside, built)?;
		self.host_code[pipeline as usize].enforce = Some(enforce);
		Ok(pipeline)
	}

	/// The field view of `table` for this statement's caller, as a host
	/// pipeline over an image — `None` inside when the view changes nothing
	/// — and whether the caller's reads are checked; `None` when the view has
	/// no layer form.
	///
	/// The view is the layer stack binding `SELECT * FROM table` puts above
	/// the table's source for the caller: every `COMPUTED` field derived, and
	/// for a checked caller each field it may not select cut.
	async fn image_view(
		&mut self,
		table: &surrealdb_strand::TableName,
	) -> Lowered<Option<(Option<u16>, bool)>> {
		if let Some(view) = self.image_view_of.get(table) {
			return Ok(*view);
		}
		let view = match crate::exec::vm::host::out_of_line(|| self.view_layers(table)).await {
			None => None,
			Some((layers, checked)) if layers.is_empty() => Some((None, checked)),
			Some((layers, checked)) => {
				let aside = self.set_aside();
				let built = crate::exec::vm::host::out_of_line(|| self.view_body(&layers)).await;
				Some((Some(self.restore(aside, built)?), checked))
			}
		};
		self.image_view_of.insert(table.clone(), view);
		Ok(view)
	}

	/// The layers of `table`'s field view for this statement's caller, top
	/// first, and whether the caller's reads are checked; `None` when the view
	/// has no layer form. See [`Self::image_view`].
	async fn view_layers(&mut self, table: &surrealdb_strand::TableName) -> Option<ViewLayers> {
		if let Some(view) = self.view_layers_of.get(table) {
			return view.clone();
		}
		let view = crate::exec::vm::host::out_of_line(|| self.bind_view_layers(table)).await;
		self.view_layers_of.insert(table.clone(), view.clone());
		view
	}

	/// [`Self::view_layers`], bound afresh.
	///
	/// A table that declares no `COMPUTED` field, and no field a checked caller
	/// may not select, has no layer: binding it would answer the empty stack,
	/// so that is answered from its field definitions without binding.
	async fn bind_view_layers(
		&mut self,
		table: &surrealdb_strand::TableName,
	) -> Option<ViewLayers> {
		use crate::catalog::Permission;
		use crate::expr::logical::{LogicalField, LogicalProject, LogicalSource, SourceRef};
		let checked = !self.planner.perms_suppressed && self.planner.compiles_for_checked_caller();
		if let Some(txn) = self.planner.txn().cloned() {
			// A database the catalog does not hold has no table, so no field.
			let Some(read) = self.catalog.table(&txn, table).ok()? else {
				return Some((Vec::new(), checked));
			};
			if read.definition.is_some()
				&& read.fields.iter().all(|field| {
					field.computed.is_none()
						&& (!checked || matches!(field.select_permission, Permission::Full))
				}) {
				return Some((Vec::new(), checked));
			}
		}
		let read = LogicalOp::Project(LogicalProject {
			input: Box::new(LogicalOp::Source(LogicalSource {
				source: SourceRef::Table(table.clone()),
				schema: None,
				with: None,
				for_update: false,
			})),
			fields: LogicalFields::Select(vec![LogicalField::All]),
			omit: Vec::new(),
		});
		// Bound apart from the lowering in flight: a table whose events or
		// views are being lowered is held on the cycle guard, and its view is
		// no less the view of a statement that writes to it from there.
		let planner =
			self.planner.shadowing(std::iter::empty()).with_cycle_guard(Default::default());
		let Ok(bound) = crate::exec::vm::host::out_of_line(|| planner.bind(read)).await else {
			return None;
		};
		let mut layers = Vec::new();
		let mut node = &bound;
		loop {
			match node {
				LogicalOp::Project(layer) if node.is_layer() => {
					layers.push((layer.fields.clone(), layer.input.schema()));
					node = &layer.input;
				}
				LogicalOp::Project(project)
					if matches!(&project.fields, LogicalFields::Select(fields)
						if matches!(fields.as_slice(), [LogicalField::All]))
						&& project.omit.is_empty() =>
				{
					node = &project.input;
				}
				LogicalOp::PermissionFilter(filter) => node = &filter.input,
				// A source without a schema is enforced by its scan: its view
				// has no layer form.
				LogicalOp::Source(LogicalSource {
					schema: Some(_),
					..
				}) => break,
				_ => return None,
			}
		}
		let checked = !self.planner.perms_suppressed && self.planner.compiles_for_checked_caller();
		Some((layers, checked))
	}

	/// The host pipeline of a `RETURN <fields>` projection over the shown
	/// post-image, or `None` for `RETURN *` alone, which is the image.
	async fn return_projection(&mut self, fields: &crate::expr::Fields) -> Lowered<Option<u16>> {
		let fields = crate::expr::logical::lower_fields(fields.clone().into()).map_err(|e| {
			NotCovered::internal(format!("the RETURN clause has no logical form: {e}"))
		})?;
		let projection = super::map::projection(&fields, Vec::new())?;
		if matches!(projection, super::plan::Projection::All) {
			return Ok(None);
		}
		let aside = self.set_aside();
		// `DELETE` leaves no post-image, so the row may be `NONE`.
		self.rows_are_objects = !matches!(self.write_form, Some(WriteForm::Delete));
		let built = crate::exec::vm::host::out_of_line(|| self.projection_body(&projection)).await;
		let pipeline = self.restore(aside, built)?;
		Ok(Some(pipeline))
	}

	/// Set the pipeline in flight aside, to build a host pipeline over a
	/// record's image beside it; [`Self::restore`] finishes that pipeline and
	/// returns to this one.
	fn set_aside(&mut self) -> Aside {
		self.slot_hidden += 1;
		self.hide_shared();
		Aside {
			code: std::mem::take(&mut self.code),
			next_reg: std::mem::replace(&mut self.next_reg, ROW + 1),
			released: std::mem::take(&mut self.released),
			uses_eval: std::mem::replace(&mut self.uses_eval, false),
			may_fail: std::mem::replace(&mut self.may_fail, false),
			rows_are_objects: std::mem::replace(&mut self.rows_are_objects, true),
			schema: self.schema.take(),
			eval_frame: self.eval_frame.take(),
			document: std::mem::replace(&mut self.document, DocumentRoot::Unknown),
		}
	}

	/// Return to the pipeline `aside` holds, pushing the host pipeline built
	/// since when `built` succeeded, and answer its index.
	fn restore(&mut self, aside: Aside, built: Lowered<()>) -> Lowered<u16> {
		self.slot_hidden -= 1;
		self.unhide_shared();
		let registers = self.next_reg;
		let code = std::mem::replace(&mut self.code, aside.code);
		self.next_reg = aside.next_reg;
		self.released = aside.released;
		self.uses_eval = aside.uses_eval;
		self.may_fail = aside.may_fail;
		self.rows_are_objects = aside.rows_are_objects;
		self.schema = aside.schema;
		self.eval_frame = aside.eval_frame;
		self.document = aside.document;
		built?;
		let index = u16::try_from(self.host_code.len())
			.map_err(|_| NotCovered::internal("more host pipelines than a program holds"))?;
		self.host_code.push(Pipeline {
			in_place: false,
			stored_rows: false,
			enforce: None,
			source: Source::Once,
			code,
			registers: registers as usize,
			limits: Vec::new(),
			region: false,
			group: None,
		});
		Ok(index)
	}

	/// The body of an image view's host pipeline: `layers`, bottom-up, then
	/// the row they leave.
	async fn view_body(
		&mut self,
		layers: &[(LogicalFields, crate::expr::logical::Schema)],
	) -> Lowered<()> {
		for (fields, schema) in layers.iter().rev() {
			crate::exec::vm::host::out_of_line(|| self.layer(fields, schema)).await?;
		}
		self.emit(Instr::Emit {
			src: ROW,
		});
		self.emit(Instr::EndRow);
		Ok(())
	}

	/// The body of a `RETURN <fields>` host pipeline: the projection, emitted.
	async fn projection_body(&mut self, projection: &super::plan::Projection) -> Lowered<()> {
		// The projection is a region of its own: its fields run against one row.
		let mut roots = Vec::new();
		projection_roots(projection, crate::expr::logical::dag::Frame::new(0), &mut roots);
		let scope = self.shared_scope(&roots);
		self.shared.push(scope);
		let out = crate::exec::vm::host::out_of_line(|| self.projection(projection)).await;
		self.shared.pop();
		let out = out?;
		self.emit(Instr::Emit {
			src: out,
		});
		self.emit(Instr::EndRow);
		Ok(())
	}

	/// Lower the pipeline whose run of streaming nodes starts at `top` and
	/// whose body ends in `sink`, after the pipelines its source depends on.
	async fn pipeline(&mut self, top: &Plan, sink: Sink<'_>) -> Lowered<()> {
		let mut chain = Vec::new();
		let mut node = top;
		// The table whose enforcement the source's rows are put through.
		let mut enforce = None;
		loop {
			match node {
				Plan::Enforced {
					input,
					table,
				} => {
					enforce = Some(Enforce::Rows(table.clone()));
					node = input;
				}
				Plan::Filter {
					input,
					..
				}
				| Plan::PermissionFilter {
					input,
					..
				}
				| Plan::Layer {
					input,
					..
				}
				| Plan::Limit {
					input,
					..
				}
				| Plan::Project {
					input,
					..
				}
				| Plan::Distinct {
					input,
				}
				| Plan::Fetch {
					input,
					..
				} => {
					chain.push(node);
					node = input;
				}
				Plan::Step {
					input,
					step,
				} if matches!(**step, super::binding::BindingStep::Bind { .. }) => {
					chain.push(node);
					node = input;
				}
				_ => break,
			}
		}
		// A source naming one record, whose row is read and judged whatever
		// the paging, as the operator tree's lookup judges it.
		let one_record = matches!(
			node,
			Plan::RecordScan {
				record: LogicalExpr::RecordId(record),
				..
			} if !matches!(record.key, crate::expr::logical::LogicalRecordIdKey::Range(_))
		);
		// The table a `FETCH` of the pipeline fetches into records of, where
		// its rows are the records a scan of one table reads, unprojected.
		let fetch_rows = scanned_table(node)
			.filter(|_| {
				chain.iter().skip_while(|node| !matches!(node, Plan::Fetch { .. })).all(|node| {
					!matches!(
						node,
						Plan::Project {
							projection,
							..
						} if !matches!(projection, Projection::All)
					)
				})
			})
			.map(|table| std::collections::BTreeSet::from([table.clone()]));
		let mut split_rows_are_objects = None;
		// Whether the rows a sort this pipeline reads was handed may be
		// record ids.
		let mut sorted_ids = false;
		// The pipeline that fills the sort, aggregate or split this one reads.
		let mut feeder = None;
		let source = match node {
			Plan::TableScan {
				..
			}
			| Plan::RecordScan {
				..
			}
			| Plan::IndexScan {
				..
			}
			| Plan::IndexUnion {
				..
			}
			| Plan::Bitmap {
				..
			}
			| Plan::FullTextScan {
				..
			}
			| Plan::KnnScan {
				..
			} => {
				let source = crate::exec::vm::host::out_of_line(|| self.leaf(node)).await?;
				// The body's first step judges a record before anything else
				// does, so a scan of the stored rows may spare it the records
				// that filter refuses.
				match (source, chain.last(), &enforce) {
					(
						Source::Scan {
							table,
							direction,
							topk,
							..
						},
						Some(Plan::Filter {
							predicate,
							..
						}),
						None,
					) => Source::Scan {
						table,
						direction,
						topk,
						prefilter: crate::exec::pre_decode_filter::PreDecodeShape::of(predicate)
							.map(|shape| super::plain::ValueData::new(shape, "a scan's prefilter"))
							.transpose()?,
					},
					(source, ..) => source,
				}
			}
			Plan::Count {
				count,
				select,
				deltas,
				ordinary,
			} => {
				crate::exec::vm::host::out_of_line(|| {
					self.count(count, *select, *deltas, ordinary.as_deref())
				})
				.await?
			}
			Plan::Values {
				..
			}
			| Plan::Subquery {
				..
			} => {
				self.schema = None;
				crate::exec::vm::host::out_of_line(|| self.value_source(node)).await?
			}
			Plan::Probe {
				build,
				field,
				outer,
			} => {
				let source =
					crate::exec::vm::host::out_of_line(|| self.probe(build, field, outer)).await?;
				// The rows are the build's records, which no typed read of
				// the table's schema is chosen over.
				self.schema = None;
				source
			}
			Plan::Union {
				inputs,
			} => {
				let mut sources = Vec::with_capacity(inputs.len());
				for input in inputs {
					sources.push(match input {
						Plan::Values {
							..
						}
						| Plan::Subquery {
							..
						} => crate::exec::vm::host::out_of_line(|| self.value_source(input)).await?,
						storage => {
							crate::exec::vm::host::out_of_line(|| self.leaf(storage)).await?
						}
					});
				}
				// The rows come from several tables, so no one schema
				// describes them.
				self.schema = None;
				Source::Union(sources)
			}
			Plan::Sort {
				input,
				keys,
				keep,
				pushdown,
				random,
			} => {
				let id = self.sorts.len() as u16;
				self.sorts.push(SortSpec {
					keys: keys.iter().map(key_order).collect(),
					keep: *keep,
					spill: self.tempfiles && keep.is_none() && !*random,
					pushdown: *pushdown,
					random: *random,
					knn_slot: None,
					knn_query: None,
				});
				let outer = std::mem::replace(&mut self.topk_sort, pushdown.then_some(id));
				let lowered = crate::exec::vm::host::out_of_line(|| {
					self.pipeline(
						input,
						Sink::Sort {
							id,
							keys,
						},
					)
				})
				.await;
				self.topk_sort = outer;
				lowered?;
				sorted_ids = self.rows_may_be_ids;
				feeder = Some(self.pipelines.len() - 1);
				Source::Sorted {
					sort: id,
				}
			}
			Plan::KnnTopK {
				input,
				field,
				vector,
				k,
				distance,
			} => {
				// The rows nearest the query are the first `k` of the rows
				// ordered by their distance from it.
				let id = self.sorts.len() as u16;
				let (vector, knn_query) = match vector {
					super::plan::KnnTarget::Literal(vector) => {
						(super::program::KnnVector::Literal(vector.clone()), None)
					}
					super::plan::KnnTarget::Deferred(expr) => (
						super::program::KnnVector::Query(id),
						Some(super::program::KnnQuery {
							scalar: crate::exec::vm::host::out_of_line(|| self.host_scalar(expr))
								.await?,
							group: self.current_group,
						}),
					),
				};
				self.sorts.push(SortSpec {
					keys: vec![KeyOrder {
						ascending: true,
						collate: false,
						numeric: false,
					}],
					keep: Some(*k as usize),
					spill: false,
					pushdown: false,
					random: false,
					knn_slot: self.index_scope.knn.then(|| self.knn_slot()),
					knn_query,
				});
				crate::exec::vm::host::out_of_line(|| {
					self.pipeline(
						input,
						Sink::Knn {
							id,
							field,
							rank: Arc::new(super::program::KnnRank {
								vector,
								distance: distance.clone(),
							}),
						},
					)
				})
				.await?;
				sorted_ids = self.rows_may_be_ids;
				feeder = Some(self.pipelines.len() - 1);
				Source::Sorted {
					sort: id,
				}
			}
			Plan::Split {
				input,
				targets,
			} => {
				let id = u16::try_from(self.splits.len())
					.map_err(|_| NotCovered::internal("more splits than a program holds"))?;
				let targets = targets
					.iter()
					.map(|target| {
						crate::exec::operators::split_target(target).ok_or_else(|| {
							NotCovered::internal("a SPLIT target with no write position")
						})
					})
					.collect::<Lowered<Vec<_>>>()?;
				self.splits.push(super::program::SplitSpec {
					kind: super::program::SplitKind::Targets(targets),
					pipeline: 0,
					build_before: None,
				});
				crate::exec::vm::host::out_of_line(|| {
					self.pipeline(
						input,
						Sink::Split {
							id,
						},
					)
				})
				.await?;
				// A split rewrites the fields it splits on, so the rows above
				// it are typed by no schema; they are objects when its input's
				// rows are and its input projects no `VALUE`.
				split_rows_are_objects = Some(
					self.rows_are_objects
						&& !matches!(
							&**input,
							Plan::Project {
								projection: super::plan::Projection::Value(_)
									| super::plan::Projection::OmittedValue { .. },
								..
							}
						),
				);
				feeder = Some(self.pipelines.len() - 1);
				self.schema = None;
				Source::Split {
					split: id,
				}
			}
			Plan::Step {
				input,
				step,
			} => {
				let (kind, build_writes) =
					crate::exec::vm::host::out_of_line(|| self.binding_step(step)).await?;
				let writes = matches!(kind, super::binding::BindingStep::Mutate(_));
				let id = u16::try_from(self.splits.len())
					.map_err(|_| NotCovered::internal("more splits than a program holds"))?;
				// Where the rows of the join read what its build side writes,
				// the build side runs before they are read: when the first
				// pipeline that produces them starts.
				self.splits.push(super::program::SplitSpec {
					kind: super::program::SplitKind::Step(kind),
					pipeline: 0,
					build_before: build_writes.then_some(self.pipelines.len()),
				});
				// A step reads adjacency and records through the database; a
				// write stage writes through it.
				self.call_needs.push(Needs {
					access_mode: if writes {
						AccessMode::ReadWrite
					} else {
						AccessMode::ReadOnly
					},
					context: ContextLevel::Database,
				});
				crate::exec::vm::host::out_of_line(|| {
					self.pipeline(
						input,
						Sink::Split {
							id,
						},
					)
				})
				.await?;
				// Every row of a binding table is an object keyed by binding
				// name, which no schema of the program describes.
				split_rows_are_objects = Some(true);
				feeder = Some(self.pipelines.len() - 1);
				self.schema = None;
				Source::Split {
					split: id,
				}
			}
			Plan::Join {
				left,
				right,
				kind,
				keys,
				residual,
				alias,
			} => {
				let id = u16::try_from(self.joins.len())
					.map_err(|_| NotCovered::internal("more joins than a program holds"))?;
				self.joins.push(super::program::JoinSpec {
					kind: *kind,
					alias: alias.clone(),
					build_keys: Vec::new(),
					residual: None,
					build: 0,
					owner: self.current_group,
					pipeline: 0,
				});
				// The build side is a statement of its own, read when the
				// first probe row reaches the join.
				let build = PhysicalPlan {
					catalog: None,
					root: (**right).clone(),
					dependencies: Vec::new(),
					tail: Default::default(),
					sources: Vec::new(),
					nested: self.nested_plans.clone(),
					scope: IndexScope::default(),
				};
				let (build, _, required_context) =
					crate::exec::vm::host::out_of_line(|| self.group(self.planner, &build)).await?;
				self.delegate_context = self.delegate_context.max(required_context);
				let mut build_keys = Vec::with_capacity(keys.len());
				for (_, build_key) in keys {
					build_keys.push(
						crate::exec::vm::host::out_of_line(|| self.host_scalar(build_key)).await?,
					);
				}
				let residual = match residual {
					Some(residual) => Some(
						crate::exec::vm::host::out_of_line(|| self.host_scalar(residual)).await?,
					),
					None => None,
				};
				crate::exec::vm::host::out_of_line(|| {
					self.pipeline(
						left,
						Sink::JoinProbe {
							id,
							keys: keys.iter().map(|(probe_key, _)| probe_key).collect(),
						},
					)
				})
				.await?;
				feeder = Some(self.pipelines.len() - 1);
				let spec = &mut self.joins[id as usize];
				spec.build_keys = build_keys;
				spec.residual = residual;
				spec.build = build;
				// A pairing is its probe row with the build row under the
				// alias, which no schema of the program describes.
				self.schema = None;
				Source::Join {
					join: id,
				}
			}
			Plan::Aggregate {
				input,
				group,
				fields,
				first_value,
			} => {
				let (id, analysis) = crate::exec::vm::host::out_of_line(|| {
					self.aggregate(group, fields, first_value)
				})
				.await?;
				crate::exec::vm::host::out_of_line(|| {
					self.pipeline(
						input,
						Sink::Aggregate {
							id,
							analysis,
						},
					)
				})
				.await?;
				feeder = Some(self.pipelines.len() - 1);
				Source::Groups {
					aggregate: id,
				}
			}
			Plan::Filter {
				..
			}
			| Plan::PermissionFilter {
				..
			}
			| Plan::Layer {
				..
			}
			| Plan::Enforced {
				..
			}
			| Plan::Limit {
				..
			}
			| Plan::Project {
				..
			}
			| Plan::Distinct {
				..
			}
			| Plan::Fetch {
				..
			} => unreachable!("the chain takes every streaming node"),
			Plan::Write {
				..
			}
			| Plan::Eval {
				..
			}
			| Plan::Ddl {
				..
			}
			| Plan::Info {
				..
			}
			| Plan::Control {
				..
			}
			| Plan::Refused {
				..
			} => return Err(NotCovered::internal("a statement beneath a read")),
		};

		self.code = Vec::new();
		self.next_reg = ROW + 1;
		self.released.clear();
		self.uses_eval = false;
		self.reads_sorted_rows = matches!(source, Source::Sorted { .. });
		self.may_fail = false;
		self.pipeline_limits = Vec::new();
		// A value source's rows may be of any kind.
		self.rows_are_objects = split_rows_are_objects.unwrap_or(yields_records(&source));
		self.rows_may_be_ids = sorted_ids || yields_ids(&source);

		let mut end_row = Vec::new();
		let mut limit_checks = Vec::new();
		let mut stop = Vec::new();
		let mut current = ROW;
		let mut writes = false;
		// A binding replaced the row: the source's own rows are no more what
		// the nodes above judge, and the row no longer lies in the stored bytes.
		let mut rebound = false;
		// The streaming nodes' row code is one evaluation region: a filter's
		// predicate and the projection's fields run in written order against
		// one row, and a node that replaces or rebinds the row starts a new
		// frame. The scope is popped on every path out of the loop.
		let mut roots = Vec::new();
		chain_roots(&chain, &mut roots);
		let scope = self.shared_scope(&roots);
		self.shared.push(scope);
		let looped: Lowered<()> = async {
			for node in chain.iter().rev() {
				match node {
					Plan::Filter {
						predicate,
						..
					} => {
						if current != ROW {
							return Err(NotCovered::internal("a filter over a projection"));
						}
						end_row.extend(
							crate::exec::vm::host::out_of_line(|| self.branch(predicate, false))
								.await?,
						);
					}
					Plan::PermissionFilter {
						predicate,
						auth,
						guard,
						computed,
						..
					} => {
						if current != ROW {
							return Err(NotCovered::internal("a permission over a projection"));
						}
						end_row.extend(
							crate::exec::vm::host::out_of_line(|| {
								self.permission(predicate, auth, *guard, computed.as_ref())
							})
							.await?,
						);
					}
					Plan::Layer {
						fields,
						input_schema,
						..
					} => {
						if current != ROW {
							return Err(NotCovered::internal("a layer over a projection"));
						}
						crate::exec::vm::host::out_of_line(|| self.layer(fields, input_schema))
							.await?;
						// The layer derives fields the table's schema does not
						// type, so no typed read is chosen above it.
						self.schema = None;
					}
					Plan::Limit {
						limit,
						start,
						..
					} => {
						let id = self.limits.len() as u16;
						let limit_scalar = match limit {
							Some(e) => Some(
								crate::exec::vm::host::out_of_line(|| self.host_scalar(e)).await?,
							),
							None => None,
						};
						let start_scalar = match start {
							Some(e) => Some(
								crate::exec::vm::host::out_of_line(|| self.host_scalar(e)).await?,
							),
							None => None,
						};
						self.limits.push(LimitSpec {
							limit: limit_scalar,
							start: start_scalar,
							group: self.current_group,
						});
						self.pipeline_limits.push(id);
						if start.is_some() {
							end_row.push(self.emit(Instr::Skip {
								limit: id,
								target: 0,
							}));
						}
						if limit.is_some() {
							if (!self.rows_are_objects && !matches!(source, Source::Join { .. }))
								|| (one_record && !rebound)
							{
								// Past the limit a row is judged and not taken:
								// the pipeline reads every row.
								end_row.push(self.emit(Instr::JumpIfLimitReached {
									limit: id,
									done: 0,
								}));
							} else if !rebound
								&& matches!(
									source,
									Source::Scan { .. }
										| Source::LightweightEdges {
											record: None,
											..
										} | Source::Index { .. } | Source::IndexUnion { .. }
										| Source::Bitmap { .. } | Source::Count { .. }
										| Source::Sorted { .. } | Source::Groups { .. }
										| Source::Probe { .. } | Source::Join { .. }
								) {
								// A scan, sort, aggregate or join is not opened
								// under a spent limit, so only the rows it takes
								// are checked, and the probe it reads stops with
								// the pipeline.
								limit_checks.push(id);
							} else {
								// A range or a union stops at its limit, as a
								// scan does: no row past it is read or judged.
								stop.push(self.emit(Instr::JumpIfLimitReached {
									limit: id,
									done: 0,
								}));
								limit_checks.push(id);
							}
							self.emit(Instr::Take {
								limit: id,
							});
						}
					}
					Plan::Project {
						projection,
						..
					} => {
						if current != ROW {
							return Err(NotCovered::internal("a projection over a projection"));
						}
						current = match projection {
							// `SELECT *` over a row that is a record id is the
							// record, and no row where none answers.
							Projection::All if self.rows_may_be_ids => {
								self.reaches(super::dispatch::Reach::Any);
								let dst = self.register()?;
								end_row.push(self.emit(Instr::FetchRow {
									dst,
									none: 0,
								}));
								self.uses_eval = true;
								dst
							}
							// `*` beside fields over a row that may be a record id
							// reads the record, as `SELECT *` does, and its fields
							// read it too.
							Projection::Star {
								..
							} if !self.rows_are_objects && self.rows_may_be_ids => {
								self.reaches(super::dispatch::Reach::Any);
								let dst = self.register()?;
								end_row.push(self.emit(Instr::FetchRow {
									dst,
									none: 0,
								}));
								self.uses_eval = true;
								self.fetched_row = Some(dst);
								let projected = crate::exec::vm::host::out_of_line(|| {
									self.projection(projection)
								})
								.await;
								self.fetched_row = None;
								projected?
							}
							_ => {
								crate::exec::vm::host::out_of_line(|| self.projection(projection))
									.await?
							}
						};
					}
					Plan::Distinct {
						..
					} => {
						if current != ROW {
							return Err(NotCovered::internal("a distinct over a projection"));
						}
						let distinct = u16::try_from(self.distincts.len()).map_err(|_| {
							NotCovered::internal("more distinct sets than a program holds")
						})?;
						self.distincts.push(self.current_group);
						// A set that outgrows its budget fails the statement.
						self.may_fail = true;
						end_row.push(self.emit(Instr::Distinct {
							distinct,
							target: 0,
						}));
					}
					Plan::Fetch {
						fetches,
						..
					} => {
						current = crate::exec::vm::host::out_of_line(|| {
							self.fetched(fetches, current, fetch_rows.as_ref())
						})
						.await?;
					}
					Plan::Step {
						step,
						..
					} => {
						let super::binding::BindingStep::Bind {
							name,
						} = &**step
						else {
							unreachable!("the chain holds binding steps that are bindings")
						};
						if current != ROW {
							return Err(NotCovered::internal("a binding over a projection"));
						}
						let name = self.field(name);
						self.emit(Instr::BindRow {
							name,
						});
						rebound = true;
						self.rows_are_objects = true;
						self.rows_may_be_ids = false;
						self.schema = None;
					}
					_ => unreachable!("the chain holds streaming nodes"),
				}
			}
			Ok(())
		}
		.await;
		self.shared.pop();
		looped?;

		// A body that hands its row to a split runs where the host can run
		// the split's rows, outside the storage cursor.
		let splits = matches!(sink, Sink::Split { .. } | Sink::JoinProbe { .. });
		let output = matches!(sink, Sink::Output);
		match sink {
			Sink::Output => {
				self.emit(Instr::Emit {
					src: current,
				});
			}
			Sink::Sort {
				id,
				keys,
			} => {
				if current != ROW {
					return Err(NotCovered::internal("a sort over a projection"));
				}
				let first = self.registers(keys.len())?;
				for (i, key) in keys.iter().enumerate() {
					let dst = first + i as Reg;
					match &key.read {
						// Over rows that may be any value, the key is read from
						// the row as the operator tree's sort extracts it.
						SortKeyRead::Path(path) if !self.rows_are_objects => {
							let row = self.current()?;
							self.paths.push(path.clone());
							let path = (self.paths.len() - 1) as u32;
							self.emit(Instr::Extract {
								src: Operand::Reg(row),
								path,
								dst,
							});
						}
						SortKeyRead::Path(path) => match path.0.split_first() {
							Some((crate::exec::field_path::FieldPathPart::Field(name), rest)) => {
								let field = self.field(name);
								if rest.is_empty() {
									self.emit(Instr::Load {
										dst,
										src: Operand::Field(field),
									});
								} else {
									let rest_path = self.paths.len() as u32;
									self.paths
										.push(crate::exec::field_path::FieldPath(rest.to_vec()));
									self.emit(Instr::LoadPath {
										dst,
										field,
										rest: rest_path,
									});
								}
							}
							_ => {
								return Err(NotCovered::new(
									"a sort key path that does not start at a field",
								));
							}
						},
						SortKeyRead::Computed(expr) => {
							let value =
								crate::exec::vm::host::out_of_line(|| self.projected(expr)).await?;
							self.emit(Instr::Load {
								dst,
								src: value,
							});
						}
					}
				}
				self.emit(Instr::SortPush {
					sort: id,
					keys: first,
					count: keys.len() as u16,
				});
			}
			Sink::Knn {
				id,
				field,
				rank,
			} => {
				if current != ROW {
					return Err(NotCovered::internal("a KNN over a projection"));
				}
				let path = crate::exec::planner::output_field_path(&field.output_path());
				self.paths.push(path);
				let path = (self.paths.len() - 1) as u32;
				let row = self.current()?;
				let key = self.register()?;
				// A row with no vector to rank takes no place in the order.
				end_row.push(self.emit(Instr::KnnRank {
					src: Operand::Reg(row),
					path,
					rank,
					dst: key,
					none: 0,
				}));
				self.emit(Instr::SortPush {
					sort: id,
					keys: key,
					count: 1,
				});
			}
			Sink::Aggregate {
				id,
				analysis,
			} => {
				if current != ROW {
					return Err(NotCovered::internal("an aggregate over a projection"));
				}
				crate::exec::vm::host::out_of_line(|| self.fold_into(id, &analysis)).await?;
			}
			Sink::Write(plan) => {
				if current != ROW {
					return Err(NotCovered::internal("a write over a projection"));
				}
				writes = true;
				crate::exec::vm::host::out_of_line(|| self.write_body(plan, true, true)).await?;
			}
			Sink::Split {
				id,
			} => {
				self.emit(Instr::SplitPush {
					split: id,
					src: current,
				});
			}
			Sink::JoinProbe {
				id,
				keys,
			} => {
				if current != ROW {
					return Err(NotCovered::internal("a join over a projection"));
				}
				let first = self.registers(keys.len())?;
				for (i, key) in keys.iter().enumerate() {
					let value = crate::exec::vm::host::out_of_line(|| self.projected(key)).await?;
					self.emit(Instr::Load {
						dst: first + i as Reg,
						src: value,
					});
				}
				self.emit(Instr::JoinProbe {
					join: id,
					keys: first,
					count: keys.len() as u16,
				});
			}
		}

		for id in limit_checks {
			stop.push(self.emit(Instr::JumpIfLimitReached {
				limit: id,
				done: 0,
			}));
		}
		// The rows the filters drop end here, as filters ending the row.
		let end = self.here();
		self.patch_all(end_row, end);
		self.emit(Instr::EndRow);
		let halt = self.emit(Instr::StopPipeline);
		self.patch_all(stop, halt);

		if let Source::Split {
			split,
		} = source
		{
			self.splits[split as usize].pipeline = self.pipelines.len();
		}
		if let Source::Join {
			join,
		} = source
		{
			self.joins[join as usize].pipeline = self.pipelines.len();
		}
		// A limit spent before the first row spares every pipeline feeding
		// this one its records, as it spares a source it reads directly.
		let mut feeding = feeder;
		while let Some(input) = feeding {
			let limits = self.pipeline_limits.clone();
			self.pipelines[input].limits.extend(limits);
			feeding = self.feeders[input];
		}
		self.feeders.push(feeder);
		// Rows put through a table's enforcement are values: a body that runs
		// on the stored bytes would see the row before it. A body run inside
		// the storage cursor cannot yield there, so it holds no backward jump,
		// which is where the interpreter yields within a row: its cursor
		// charges each record's unit itself and yields between chunks.
		let host_free =
			!self.uses_eval
				&& !writes && !splits
				&& !rebound && enforce.is_none()
				&& !jumps_back(&self.code);
		let in_place = source.reads_storage() && host_free;
		let stored_rows = matches!(source, Source::Sorted { .. }) && host_free;
		let region = !in_place
			&& enforce.is_none()
			&& !writes
			&& self.write.is_none()
			&& super::region::source_admits(&source)
			&& match self.pipeline_limits.as_slice() {
				[] => super::region::body_admits(&self.code, &self.aggregates, &self.nested, false),
				[_] => {
					output
						&& super::region::paged_admits(&self.code)
						&& super::region::body_admits(
							&self.code,
							&self.aggregates,
							&self.nested,
							true,
						)
				}
				_ => false,
			};
		self.pipelines.push(Pipeline {
			in_place,
			stored_rows,
			enforce,
			source,
			code: std::mem::take(&mut self.code),
			registers: self.next_reg as usize,
			limits: std::mem::take(&mut self.pipeline_limits),
			region,
			group: self.current_group,
		});
		Ok(())
	}

	/// The source a scan leaf reads.
	/// The source of a value or a statement in `FROM`: the value's host
	/// scalar, evaluated when the pipeline opens, or the statement's group.
	async fn value_source(&mut self, node: &Plan) -> Lowered<Source> {
		match node {
			Plan::Values {
				expr,
				read,
			} => {
				// A record among the values is read through the database.
				self.delegate_context = self.delegate_context.max(ContextLevel::Database);
				let value = crate::exec::vm::host::out_of_line(|| self.host_scalar(expr)).await?;
				self.locks += usize::from(matches!(read, super::plan::ValuesRead::Locked));
				match read {
					super::plan::ValuesRead::Dynamic | super::plan::ValuesRead::Locked => {
						self.reaches_late(super::dispatch::Reach::of_expr(expr))
					}
					super::plan::ValuesRead::Expression {
						..
					} => self.reaches(super::dispatch::Reach::of_expr(expr)),
				}
				Ok(Source::Values {
					value,
					read: *read,
				})
			}
			Plan::Subquery {
				plan,
			} => {
				if matches!(plan.root, Plan::Control { .. } | Plan::Ddl { .. } | Plan::Info { .. })
				{
					return Err(NotCovered::internal("a block or schema statement in FROM"));
				}
				let (group, _, required_context) =
					crate::exec::vm::host::out_of_line(|| self.group(self.planner, plan)).await?;
				self.delegate_context = self.delegate_context.max(required_context);
				Ok(Source::Nested {
					group,
				})
			}
			other => Err(NotCovered::internal(format!("not a value source: {other:?}"))),
		}
	}

	/// The step a binding-table plan holds as the program holds it: its
	/// scalars as host scalars, and a join's build side as a group of the
	/// program.
	///
	/// Also whether the step is a join whose build side writes.
	async fn binding_step(
		&mut self,
		step: &super::binding::PlanStep,
	) -> Lowered<(super::binding::ProgramStep, bool)> {
		use super::binding::{BindingStep, ExpandStep, JoinStep};
		let mut build_writes = false;
		let lowered = match step {
			BindingStep::Bind {
				name,
			} => BindingStep::Bind {
				name: name.clone(),
			},
			BindingStep::Expand(expand) => {
				let predicate = match &expand.predicate {
					Some(predicate) => Some(
						crate::exec::vm::host::out_of_line(|| self.host_scalar(predicate)).await?,
					),
					None => None,
				};
				BindingStep::Expand(ExpandStep {
					source: expand.source.clone(),
					direction: expand.direction,
					edge_tables: expand.edge_tables.clone(),
					edge_binding: expand.edge_binding.clone(),
					target_binding: expand.target_binding.clone(),
					target_label: expand.target_label.clone(),
					predicate,
					optional: expand.optional,
				})
			}
			BindingStep::Endpoint(endpoint) => BindingStep::Endpoint(endpoint.clone()),
			BindingStep::Path(path) => BindingStep::Path(path.clone()),
			BindingStep::Drain => BindingStep::Drain,
			BindingStep::Mutate(mutation) => {
				use super::binding::MutationStep;
				use crate::exec::operators::mutate::{InsertEdgeOp, InsertNodeOp, UpdateOp};
				BindingStep::Mutate(match mutation {
					MutationStep::Update {
						target,
						op,
					} => MutationStep::Update {
						target: target.clone(),
						op: match op {
							UpdateOp::Set(assignments) => {
								let mut lowered = Vec::with_capacity(assignments.len());
								for (property, value) in assignments {
									lowered.push((
										property.clone(),
										crate::exec::vm::host::out_of_line(|| {
											self.host_scalar(value)
										})
										.await?,
									));
								}
								UpdateOp::Set(lowered)
							}
							UpdateOp::Unset(properties) => UpdateOp::Unset(properties.clone()),
							UpdateOp::Content(object) => UpdateOp::Content(
								crate::exec::vm::host::out_of_line(|| self.host_scalar(object))
									.await?,
							),
						},
					},
					MutationStep::Delete {
						target,
						detach,
						is_edge,
					} => MutationStep::Delete {
						target: target.clone(),
						detach: *detach,
						is_edge: *is_edge,
					},
					MutationStep::Insert {
						nodes,
						edges,
					} => {
						let mut lowered_nodes = Vec::with_capacity(nodes.len());
						for node in nodes {
							lowered_nodes.push(InsertNodeOp {
								name: node.name.clone(),
								table: node.table.clone(),
								props: crate::exec::vm::host::out_of_line(|| {
									self.host_scalar(&node.props)
								})
								.await?,
							});
						}
						let mut lowered_edges = Vec::with_capacity(edges.len());
						for edge in edges {
							lowered_edges.push(InsertEdgeOp {
								name: edge.name.clone(),
								table: edge.table.clone(),
								from: edge.from.clone(),
								to: edge.to.clone(),
								props: crate::exec::vm::host::out_of_line(|| {
									self.host_scalar(&edge.props)
								})
								.await?,
							});
						}
						MutationStep::Insert {
							nodes: lowered_nodes,
							edges: lowered_edges,
						}
					}
				})
			}
			BindingStep::DistinctEdges {
				edges,
			} => BindingStep::DistinctEdges {
				edges: edges.clone(),
			},
			BindingStep::Distinct => BindingStep::Distinct,
			BindingStep::Join(join) => {
				let plan = PhysicalPlan {
					catalog: None,
					root: Plan::Project {
						input: join.build.clone(),
						projection: super::plan::Projection::All,
					},
					dependencies: Vec::new(),
					tail: Default::default(),
					sources: Vec::new(),
					nested: self.nested_plans.clone(),
					scope: IndexScope::default(),
				};
				let (build, build_mode, required_context) =
					crate::exec::vm::host::out_of_line(|| self.group(self.planner, &plan)).await?;
				build_writes = matches!(build_mode, AccessMode::ReadWrite);
				self.delegate_context = self.delegate_context.max(required_context);
				let residual = match &join.residual {
					Some(residual) => Some(
						crate::exec::vm::host::out_of_line(|| self.host_scalar(residual)).await?,
					),
					None => None,
				};
				BindingStep::Join(JoinStep {
					build,
					keys: join.keys.clone(),
					join_type: join.join_type,
					null_template: join.null_template.clone(),
					residual,
				})
			}
		};
		Ok((lowered, build_writes))
	}

	/// The source of a [`Plan::Probe`]: `build` as a group of the program,
	/// read as a statement of its own, and `outer` as the probe's key.
	async fn probe(
		&mut self,
		build: &Plan,
		field: &surrealdb_strand::Strand,
		outer: &LogicalExpr,
	) -> Lowered<Source> {
		let owner = self
			.probe_owner
			.ok_or_else(|| NotCovered::internal("a probe outside a nested statement"))?;
		let plan = PhysicalPlan {
			catalog: None,
			root: Plan::Project {
				input: Box::new(build.clone()),
				projection: super::plan::Projection::All,
			},
			dependencies: Vec::new(),
			tail: Default::default(),
			sources: Vec::new(),
			nested: self.nested_plans.clone(),
			scope: IndexScope::default(),
		};
		let (group, _, required_context) =
			crate::exec::vm::host::out_of_line(|| self.group(self.planner, &plan)).await?;
		self.delegate_context = self.delegate_context.max(required_context);
		let mut path = Vec::new();
		let mut step = outer;
		while let LogicalExpr::Access {
			base,
			field,
		} = step
		{
			path.push(field.clone());
			step = base;
		}
		if !matches!(step, LogicalExpr::Param(param) if param.as_str() == "parent") {
			return Err(NotCovered::internal("a probe key that is not a path into $parent"));
		}
		path.reverse();
		let probe = u16::try_from(self.probes.len())
			.map_err(|_| NotCovered::internal("more probes than a program holds"))?;
		self.probes.push(owner);
		Ok(Source::Probe {
			build: group,
			probe,
			field: field.clone(),
			outer: path,
		})
	}

	/// Whether `table` is a lightweight relation in the catalog the statement
	/// is planned against, which stores no record range to scan.
	fn is_lightweight(&self, table: &surrealdb_strand::TableName) -> Lowered<bool> {
		let Some(txn) = self.planner.txn() else {
			return Ok(false);
		};
		Ok(self.catalog.table(txn, table)?.and_then(|read| read.definition.as_ref()).is_some_and(
			|def| crate::kvs::lightweight::lightweight_relation(&def.table_type).is_some(),
		))
	}

	async fn leaf(&mut self, node: &Plan) -> Lowered<Source> {
		match node {
			Plan::TableScan {
				table,
				direction,
				schema,
				topk,
			} => {
				self.schema = schema.as_deref().cloned();
				self.table.get_or_insert_with(|| table.clone());
				if self.is_lightweight(table)? {
					return Ok(Source::LightweightEdges {
						table: table.clone(),
						record: None,
						direction: *direction,
					});
				}
				// A key with no sort to publish its threshold reads none.
				let topk = match (topk, self.topk_sort) {
					(Some(key), Some(sort)) => Some(TopKScan::new(key, sort)),
					_ => None,
				};
				Ok(Source::Scan {
					table: table.clone(),
					direction: *direction,
					topk,
					prefilter: None,
				})
			}
			Plan::RecordScan {
				table,
				record,
				schema,
				for_update,
			} => {
				self.schema = schema.as_deref().cloned();
				self.table.get_or_insert_with(|| table.clone());
				let record =
					crate::exec::vm::host::out_of_line(|| self.host_scalar(record)).await?;
				// A locked read reads the record's key and locks it, a
				// lightweight edge's as any other, as the operator tree's
				// locked read does.
				if !*for_update && self.is_lightweight(table)? {
					return Ok(Source::LightweightEdges {
						table: table.clone(),
						record: Some(record),
						direction: crate::kvs::Direction::Forward,
					});
				}
				self.locks += usize::from(*for_update);
				Ok(Source::Record {
					table: table.clone(),
					record,
					for_update: *for_update,
				})
			}
			Plan::IndexScan {
				table,
				index,
				access,
				direction,
				schema,
				decode,
				..
			} => {
				self.schema = schema.as_deref().cloned();
				self.table.get_or_insert_with(|| table.clone());
				Ok(Source::Index {
					index: index.clone(),
					access: ValueData::new(access.clone(), "an index access")?,
					direction: *direction,
					decode: decode.clone(),
				})
			}
			Plan::IndexUnion {
				table,
				branches,
				merge,
				schema,
				decode,
				..
			} => {
				self.schema = schema.as_deref().cloned();
				self.table.get_or_insert_with(|| table.clone());
				Ok(Source::IndexUnion {
					branches: branches.clone(),
					merge: merge.clone(),
					decode: decode.clone(),
				})
			}
			Plan::Bitmap {
				table,
				root,
				fallback,
				schema,
				decode,
			} => {
				let fallback = match fallback {
					Some(fallback) => Some(Box::new(
						crate::exec::vm::host::out_of_line(|| self.leaf(fallback)).await?,
					)),
					None => None,
				};
				self.schema = schema.as_deref().cloned();
				self.table.get_or_insert_with(|| table.clone());
				Ok(Source::Bitmap {
					table: table.clone(),
					root: ValueData::new(root.clone(), "a bitmap plan")?,
					fallback,
					decode: decode.clone(),
				})
			}
			Plan::FullTextScan {
				table,
				index,
				query,
				operator,
				schema,
				decode,
			} => {
				self.schema = schema.as_deref().cloned();
				self.table.get_or_insert_with(|| table.clone());
				Ok(Source::FullText {
					table: table.clone(),
					index: index.clone(),
					query: query.clone(),
					operator: operator.clone(),
					decode: decode.clone(),
				})
			}
			Plan::KnnScan {
				table,
				index,
				vector,
				k,
				ef,
				filter,
				ids_only,
				schema,
			} => {
				self.schema = schema.as_deref().cloned();
				self.table.get_or_insert_with(|| table.clone());
				let gate = match filter {
					Some(filter) => {
						Some(crate::exec::vm::host::out_of_line(|| self.knn_gate(filter)).await?)
					}
					None => None,
				};
				Ok(Source::Knn {
					table: table.clone(),
					index: index.clone(),
					vector: vector.clone(),
					k: *k,
					ef: *ef,
					slot: self.index_scope.knn.then(|| self.knn_slot()),
					gate,
					ids_only: *ids_only,
				})
			}
			Plan::Union {
				inputs,
			} if inputs.is_empty() => Ok(Source::Union(Vec::new())),
			_ => Err(NotCovered::internal("a union input that is not a scan")),
		}
	}

	/// The source a count decision answers with.
	///
	/// The catalog facts the count rests on were read when the plan was
	/// mapped: the table's permission, the `COUNT` index and the ordinary
	/// pipeline that answers a count no key or index does. That pipeline is a
	/// unit of the program, planned here like any statement in the program.
	async fn count(
		&mut self,
		count: &crate::exec::planner::CountDecision,
		select: super::plan::CountSelect,
		deltas: Option<crate::catalog::IndexId>,
		ordinary: Option<&PhysicalPlan>,
	) -> Lowered<Source> {
		use super::program::{CountSource, CountVia, CountedValue};
		use crate::exec::planner::CountDecision;
		use crate::expr::logical::SourceRef;
		self.schema = None;
		self.delegate_context = self.delegate_context.max(ContextLevel::Database);
		let ordinary = match ordinary {
			Some(plan) => {
				let plan = super::optimize::optimize(plan.clone());
				let planner = self.planner.nested_statement(
					self.control_ifs,
					self.control_nested || self.control_ifs == 0,
				);
				let (unit, _, required_context) =
					crate::exec::vm::host::out_of_line(|| self.statement_unit(&planner, &plan))
						.await?;
				self.delegate_context = self.delegate_context.max(required_context);
				Some(unit)
			}
			None => None,
		};
		Ok(match count {
			CountDecision::All {
				source,
				output,
			} => {
				let (table, value) = match source {
					SourceRef::Table(table) => (table.clone(), CountedValue::Table(table.clone())),
					SourceRef::RecordId(record) => {
						let expr = LogicalExpr::RecordId(record.clone());
						if !self.is_native(&expr) {
							return Err(NotCovered::internal(
								"the counted record is evaluated by the tree",
							));
						}
						(
							record.table.clone(),
							CountedValue::Code(
								crate::exec::vm::host::out_of_line(|| self.host_code(&expr))
									.await?,
							),
						)
					}
					_ => {
						return Err(NotCovered::internal(
							"a count over a source resolved when it runs",
						));
					}
				};
				self.table.get_or_insert_with(|| table.clone());
				Source::Count {
					count: CountSource::All {
						table,
						source: value,
						select,
						deltas,
						ordinary,
					},
					output: output.clone(),
				}
			}
			CountDecision::Indexed {
				table,
				output,
				btree_access,
				bitmap_plan,
				..
			} => {
				self.table.get_or_insert_with(|| table.clone());
				let via = match (deltas, btree_access, bitmap_plan) {
					(Some(index), _, _) => CountVia::Deltas {
						index,
					},
					(None, Some((index, access)), _) => CountVia::BTreeKeys {
						index: index.clone(),
						access: ValueData::new(access.clone(), "a counted index access")?,
					},
					(None, None, Some(plan)) => {
						CountVia::Bitmap(ValueData::new(plan.clone(), "a counted bitmap plan")?)
					}
					(None, None, None) => {
						return Err(NotCovered::internal("an indexed count no index answers"));
					}
				};
				Source::Count {
					count: CountSource::Indexed {
						table: table.clone(),
						select,
						via,
						ordinary,
					},
					output: output.clone(),
				}
			}
		})
	}

	/// A table's permission: jumps taken for a row it does not admit.
	///
	/// A predicate that reads `computed`, `COMPUTED` fields of the table the
	/// row is a record of, is judged by a host pipeline over a copy of the row
	/// with those fields, and those they read in turn, derived onto it in the
	/// permission frame ([`FrameMode::Derived`]): the row as stored carries
	/// none of them, and goes on as it came. Otherwise a predicate that reads
	/// only the row's own fields and literals is answered by instructions:
	/// the permission frame changes how a predicate reads other records and
	/// whether it may write, and such a predicate does neither. Any other is
	/// evaluated by the host in the permission frame. None runs where the
	/// statement's permissions are not checked.
	///
	/// TODO: fold the session values a predicate reads (`$auth`, `$session`,
	/// `$token`, `$access`) into it at plan time, for example
	/// `$auth.admin = true OR owner = $auth` becoming `true` or
	/// `owner = <id>`. Until then nothing folds: the predicate reads those
	/// values at run time through the shared table-read unit, and `EXPLAIN`
	/// renders it as `<permission>`, as main does. Folding changes that
	/// rendering, so the change needs a decision before it lands.
	async fn permission(
		&mut self,
		predicate: &LogicalExpr,
		auth: &Option<surrealdb_iam::AuthLimit>,
		guard: crate::expr::logical::GuardKind,
		computed: Option<&super::plan::ComputedReads>,
	) -> Lowered<Vec<Label>> {
		let derivations = match computed {
			Some(reads) => {
				crate::exec::vm::host::out_of_line(|| {
					self.guard_derivations(&reads.table, reads.fields.as_deref())
				})
				.await?
			}
			None => Vec::new(),
		};
		if !derivations.is_empty() {
			return crate::exec::vm::host::out_of_line(|| {
				self.derived_permission(predicate, auth, guard, &derivations)
			})
			.await;
		}
		let frame = self.guard_frame(auth, guard);
		let bypass = self.emit(Instr::JumpIfPermissionsBypassed {
			target: 0,
		});
		// What the predicate reads of the frame — its parameters — it reads
		// in the permission frame, which also decides what it may call.
		let outer = self.eval_frame.replace(frame);
		let outer_privileged = std::mem::replace(&mut self.privileged, true);
		let rejected = if !self.is_native_predicate(predicate) {
			Err(self.uncovered(predicate, "a permission predicate"))
		} else if crate::exec::logical::holds_write_statement(predicate) {
			crate::exec::vm::host::out_of_line(|| {
				self.checked_verdict(predicate, super::program::PermissionCheck::Table)
			})
			.await
		} else {
			crate::exec::vm::host::out_of_line(|| self.branch(predicate, false)).await
		};
		self.eval_frame = outer;
		self.privileged = outer_privileged;
		let rejected = rejected?;
		self.patch(bypass, self.here());
		Ok(rejected)
	}

	/// Jumps taken where permission predicate `predicate`, which holds a
	/// statement that writes, does not admit the row: its verdict evaluated in
	/// an [`Instr::PermissionRegion`] naming `check`.
	async fn checked_verdict(
		&mut self,
		predicate: &LogicalExpr,
		check: super::program::PermissionCheck,
	) -> Lowered<Vec<Label>> {
		self.emit(Instr::PermissionRegion {
			check,
		});
		self.hide_shared();
		let verdict = crate::exec::vm::host::out_of_line(|| self.caught(predicate, true)).await;
		self.unhide_shared();
		let verdict = verdict?;
		self.emit(Instr::TryEnd);
		self.may_fail = true;
		Ok(vec![self.emit(Instr::JumpIf {
			cond: verdict,
			truthy: false,
			target: 0,
		})])
	}

	/// Whether the code being lowered is a definer's code run in a privileged
	/// frame, which holds none of the caller's bindings. The frames are the
	/// permission, gate, layer and field-clause frames, and every call made
	/// from one.
	pub(super) fn in_privileged_frame(&self) -> bool {
		self.privileged || super::program::is_permission_frame(self.eval_frame)
	}

	/// The value `name` holds in the context the plan is lowered under, as the
	/// code being lowered sees it: none of the caller's own bindings in a
	/// privileged frame ([`Self::in_privileged_frame`]), only the session's
	/// values there.
	pub(super) fn caller_value(&self, name: &str) -> Option<&crate::val::Value> {
		if self.in_privileged_frame() && !crate::exec::planner::UNIT_RUNTIME_PARAMS.contains(&name)
		{
			return None;
		}
		self.planner.frozen().value(name)
	}

	/// The frame a table permission predicate under `auth` is evaluated in.
	fn guard_frame(
		&mut self,
		auth: &Option<surrealdb_iam::AuthLimit>,
		guard: crate::expr::logical::GuardKind,
	) -> u16 {
		self.permissions.push(PermissionSpec {
			mode: FrameMode::Guard(guard),
			auth: auth.clone(),
			column: false,
		});
		(self.permissions.len() - 1) as u16
	}

	/// A table's permission judged by a host pipeline over a copy of the row
	/// with `derivations` derived onto it, as [`Self::permission`] states:
	/// jumps taken for a row it does not admit.
	pub(super) async fn derived_permission(
		&mut self,
		predicate: &LogicalExpr,
		auth: &Option<surrealdb_iam::AuthLimit>,
		guard: crate::expr::logical::GuardKind,
		derivations: &[write_gate::Derivation],
	) -> Lowered<Vec<Label>> {
		let frame = self.guard_frame(auth, guard);
		let pipeline = crate::exec::vm::host::out_of_line(|| {
			self.guard_pipeline(predicate, frame, derivations)
		})
		.await?;
		let bypass = self.emit(Instr::JumpIfPermissionsBypassed {
			target: 0,
		});
		let row = self.register()?;
		self.emit(Instr::LoadRow {
			dst: row,
		});
		let verdict = self.register()?;
		self.emit(Instr::JudgeCopy {
			pipeline,
			src: row,
			dst: verdict,
		});
		self.release(row);
		self.uses_eval = true;
		self.may_fail = true;
		let rejected = self.emit(Instr::JumpIf {
			cond: Operand::Reg(verdict),
			truthy: false,
			target: 0,
		});
		self.release(verdict);
		self.patch(bypass, self.here());
		Ok(vec![rejected])
	}

	/// Apply projection layer `fields` to the row, as
	/// [`ProjectLayer`](crate::exec::operators::ProjectLayer) applies it: the
	/// columns on stored fields, then those on computed fields, each group in
	/// plan order, every one evaluated before any lands on the row.
	///
	/// A column on a computed field reads the row with each stored column's
	/// landing applied and each computed value derived before it set, when a
	/// stored column is present; that record is built in its own register.
	/// Otherwise it reads the row. Every guard reads the row.
	async fn layer(
		&mut self,
		fields: &crate::expr::logical::LogicalFields,
		input_schema: &crate::expr::logical::Schema,
	) -> Lowered<()> {
		use crate::expr::logical::EvalMode;
		let columns = crate::exec::planner::fold::layer_columns(fields.clone())
			.map_err(|e| NotCovered::internal(format!("the layer did not plan: {e}")))?;
		// What the input's schema declares `COMPUTED`, computed here or not.
		let declared: Vec<surrealdb_strand::Strand> = input_schema
			.fields
			.iter()
			.filter(|field| field.computed.is_some())
			.filter_map(|field| field.path.as_field().cloned())
			.collect();
		let mut stored = Vec::new();
		let mut computed = Vec::new();
		for column in columns {
			match column.frame.mode {
				EvalMode::Caller => {
					return Err(NotCovered::internal("a layer column in the caller's frame"));
				}
				EvalMode::Computed => computed.push(column),
				_ if declared.contains(&column.name) => computed.push(column),
				_ => stored.push(column),
			}
		}
		let sets = self.register()?;
		self.emit(Instr::NewObject {
			dst: sets,
		});
		let cuts = self.register()?;
		self.emit(Instr::NewObject {
			dst: cuts,
		});
		// Every column on a stored field is a field's permission, so any is
		// a stored guard the computed columns read the record through.
		let reduced = if !stored.is_empty() && !computed.is_empty() {
			let reduced = self.register()?;
			self.emit(Instr::LoadRow {
				dst: reduced,
			});
			Some(reduced)
		} else {
			None
		};
		let targets = LayerTargets {
			sets,
			cuts,
			reduced,
		};
		for column in &stored {
			crate::exec::vm::host::out_of_line(|| self.layer_column(column, false, &targets))
				.await?;
		}
		if !computed.is_empty() {
			let names = computed
				.iter()
				.map(|column| column.name.clone())
				.chain(declared.iter().cloned())
				.collect();
			self.computed_names.push(names);
			let names = (self.computed_names.len() - 1) as u32;
			let id = self.field("id");
			self.emit(Instr::BeginComputing {
				id,
				doc: reduced,
				names,
			});
			for column in &computed {
				crate::exec::vm::host::out_of_line(|| self.layer_column(column, true, &targets))
					.await?;
			}
		}
		self.emit(Instr::LandRow {
			sets,
			cuts,
		});
		self.release(sets);
		self.release(cuts);
		if let Some(reduced) = reduced {
			self.release(reduced);
		}
		self.uses_eval = true;
		self.may_fail = true;
		Ok(())
	}

	/// Whether what is being lowered is read at a `VERSION`: the statement
	/// names one, or it is lowered beneath one, as a subquery or the body of
	/// a function the versioned statement calls.
	fn reads_versioned(&self) -> bool {
		self.versioned
			|| self.planner.version.is_some()
			|| !matches!(
				self.planner.plan_time_version,
				crate::exec::planner::PlanTimeVersion::Current
			)
	}

	/// Whether the expression being lowered is evaluated with its reads left
	/// unchecked: in a write permission predicate, in a permission predicate's
	/// frame or a gate's, or in a body inlined in any of them.
	pub(super) fn reads_unchecked(&self) -> bool {
		self.unchecked_reads
			|| self.eval_frame.is_some_and(|frame| {
				super::program::is_permission_frame(Some(frame))
					&& matches!(
						self.permissions[frame as usize].mode,
						FrameMode::Guard(_) | FrameMode::Gate(_) | FrameMode::Derived
					)
			})
	}

	/// The frame of [`Program::permissions`](super::program::Program::permissions)
	/// that evaluates under `mode` and `auth`.
	fn layer_frame(&mut self, mode: FrameMode, auth: &Option<surrealdb_iam::AuthLimit>) -> u16 {
		self.frame_of(PermissionSpec {
			mode,
			auth: auth.clone(),
			column: false,
		})
	}

	/// The frame a projection layer's column evaluates in, `mode` under
	/// `auth`, shared by every column that evaluates alike.
	fn column_frame(&mut self, mode: FrameMode, auth: &Option<surrealdb_iam::AuthLimit>) -> u16 {
		self.frame_of(PermissionSpec {
			mode,
			auth: auth.clone(),
			column: true,
		})
	}

	/// The index of frame `spec`, added to the program's frames where no
	/// frame alike is there.
	fn frame_of(&mut self, spec: PermissionSpec) -> u16 {
		match self.permissions.iter().position(|frame| *frame == spec) {
			Some(frame) => frame as u16,
			None => {
				self.permissions.push(spec);
				(self.permissions.len() - 1) as u16
			}
		}
	}

	/// Evaluate one column of a projection layer and record where it lands,
	/// as [`crate::exec::operators::layer`] states. `computed` is whether the
	/// column is on a computed field.
	async fn layer_column(
		&mut self,
		column: &crate::exec::planner::fold::LogicalLayerColumn,
		computed: bool,
		targets: &LayerTargets,
	) -> Lowered<()> {
		use crate::expr::logical::{EvalMode, GuardKind};
		// Nothing a column leaves in a register is read after it lands.
		let next_reg = self.next_reg;
		let released = self.released.clone();
		let mode = match column.frame.mode {
			EvalMode::Permission => FrameMode::Guard(GuardKind::Read),
			EvalMode::MutablePermission => FrameMode::Guard(GuardKind::Write),
			EvalMode::Computed | EvalMode::Caller => FrameMode::Computed,
		};
		let is_guard = matches!(mode, FrameMode::Guard(_));
		let frame = self.column_frame(mode, &column.frame.auth);
		// The value of a permission column on a computed field is the field's
		// re-derivation, evaluated as the field's body is.
		let value_frame = if computed && is_guard {
			self.column_frame(FrameMode::Computed, &column.frame.auth)
		} else {
			frame
		};
		// A computed column reads the reduced record where there is one: its
		// instructions step from it as their element, and a statement it holds
		// takes it as its `$this` and `$parent`, where nothing in it reads the
		// row otherwise.
		let doc = if computed {
			targets.reduced
		} else {
			None
		};
		if !self.is_native(&column.value)
			|| doc.is_some_and(|_| !self.reads_row_as_column_element(&column.value))
		{
			return Err(NotCovered::new("a projection-layer column with no instructions"));
		}
		let value = self.register()?;
		// A `RETURN` a statement in the column signals is the column's value;
		// any other signal out of it is an error of the column frame's own. A
		// statement reading the table whose layer it is a column of, which the
		// bind leaves for its scan to enforce, does not lower.
		let outer = self.eval_frame.replace(value_frame);
		let outer_privileged = std::mem::replace(&mut self.privileged, true);
		let outer_element = match doc {
			Some(reduced) => self.element.replace(reduced),
			None => self.element,
		};
		let outer_column = std::mem::replace(&mut self.column_statements, true);
		let outer_doc = std::mem::replace(&mut self.statement_doc, doc);
		let src = crate::exec::vm::host::out_of_line(|| self.caught(&column.value, true)).await;
		self.column_statements = outer_column;
		self.statement_doc = outer_doc;
		self.element = outer_element;
		self.eval_frame = outer;
		self.privileged = outer_privileged;
		self.emit(Instr::Load {
			dst: value,
			src: src?,
		});
		let name = self.field(column.name.as_str());
		// A permission column whose value is the literal `NONE` is a field the
		// caller is denied outright, as a guard that folds to `false` leaves
		// it. It cuts the field and has no value to hold to the declared kind.
		let denied = is_guard
			&& matches!(
				column.value,
				LogicalExpr::Literal(crate::expr::logical::LogicalLiteral::None)
			);
		if let (Some(kind), false) = (&column.kind, denied) {
			self.kinds.push(kind.clone());
			self.emit(Instr::CoerceField {
				reg: value,
				kind: (self.kinds.len() - 1) as u32,
				field: name,
			});
		}
		self.paths.push(crate::exec::field_path::FieldPath::field(column.name.as_str()));
		let path = (self.paths.len() - 1) as u32;
		// Every later computed body reads the field as this column derived
		// it, whatever its guard says of it.
		if let (true, Some(reduced)) = (computed, targets.reduced) {
			self.emit(Instr::SetField {
				object: reduced,
				path,
				value: Operand::Reg(value),
			});
		}
		// A stored column's landing is the reduced record's too.
		let reduced = if computed {
			None
		} else {
			targets.reduced
		};
		let mut cut = Vec::new();
		let mut unchanged = Vec::new();
		match &column.guard {
			Some(guard) => {
				if !self.is_native_predicate(guard) {
					return Err(NotCovered::new("a projection-layer guard with no instructions"));
				}
				// `$value` is the value the guard judges, which a statement
				// inside it does not read: the statement takes its names from
				// the frame. A `RETURN` the statement signals is the guard's
				// verdict. A statement reading the table whose layer the guard
				// is a column of, which the bind leaves for its scan to
				// enforce, does not lower.
				let outer = self.eval_frame.replace(frame);
				let outer_privileged = std::mem::replace(&mut self.privileged, true);
				let outer_params =
					std::mem::replace(&mut self.inline_params, vec![("value".to_owned(), value)]);
				let rejected = if holds_statement(guard) {
					let outer_column = std::mem::replace(&mut self.column_statements, true);
					let outer_doc = self.statement_doc.take();
					let check = crate::exec::logical::holds_write_statement(guard)
						.then_some(super::program::PermissionCheck::Field);
					if let Some(check) = check {
						self.emit(Instr::PermissionRegion {
							check,
						});
					}
					let verdict =
						crate::exec::vm::host::out_of_line(|| self.caught(guard, true)).await;
					if check.is_some() {
						self.emit(Instr::TryEnd);
					}
					self.column_statements = outer_column;
					self.statement_doc = outer_doc;
					verdict.map(|verdict| {
						vec![self.emit(Instr::JumpIf {
							cond: verdict,
							truthy: false,
							target: 0,
						})]
					})
				} else {
					crate::exec::vm::host::out_of_line(|| self.branch(guard, false)).await
				};
				self.inline_params = outer_params;
				self.eval_frame = outer;
				self.privileged = outer_privileged;
				cut.extend(rejected?);
				// An admitted stored field is as the row has it.
				if !computed {
					unchanged.push(self.emit(Instr::JumpIfNone {
						cond: Operand::Reg(value),
						none: true,
						target: 0,
					}));
				}
			}
			// A permission without a guard cuts the field its `NONE` leaves.
			None if is_guard => {
				cut.push(self.emit(Instr::JumpIfNone {
					cond: Operand::Reg(value),
					none: true,
					target: 0,
				}));
			}
			None => {}
		}
		self.emit(Instr::SetField {
			object: targets.sets,
			path,
			value: Operand::Reg(value),
		});
		self.emit(Instr::RemoveField {
			object: targets.cuts,
			name,
		});
		if let Some(reduced) = reduced {
			self.emit(Instr::SetField {
				object: reduced,
				path,
				value: Operand::Reg(value),
			});
		}
		if !cut.is_empty() {
			unchanged.push(self.emit(Instr::Jump {
				target: 0,
			}));
			let here = self.here();
			self.patch_all(cut, here);
			self.emit(Instr::RemoveField {
				object: targets.sets,
				name,
			});
			let marker = self.constant(Value::Bool(true));
			self.emit(Instr::SetField {
				object: targets.cuts,
				path,
				value: marker,
			});
			if let Some(reduced) = reduced {
				self.emit(Instr::RemoveField {
					object: reduced,
					name,
				});
			}
		}
		let end = self.here();
		self.patch_all(unchanged, end);
		self.released = released;
		self.released.extend(next_reg..self.next_reg);
		Ok(())
	}

	/// Read an aggregate's fields and keys as the aggregate operator plans
	/// them, lower what its groups finish with into pipelines, and register
	/// it. The expressions the body evaluates per row are lowered where the
	/// rows are folded ([`Self::fold_into`]), from the analysis returned.
	async fn aggregate(
		&mut self,
		group: &[LogicalExpr],
		fields: &LogicalFields,
		first_value: &[usize],
	) -> Lowered<(u16, AggregationAnalysis)> {
		let registry = self.planner.function_registry();
		let mut analysis = analyse_aggregation(registry, fields, group)
			.map_err(|e| NotCovered::internal(format!("the aggregate did not plan: {e}")))?;
		// A column the plan answers by its first value reads the row once per
		// group rather than collecting it.
		for &index in first_value {
			use crate::expr::logical::LogicalField;
			let LogicalFields::Select(list) = fields else {
				return Err(NotCovered::internal("a first-value column of a value selection"));
			};
			let (
				Some(LogicalField::Single {
					expr,
					..
				}),
				Some(field),
			) = (list.get(index), analysis.fields.get_mut(index))
			else {
				return Err(NotCovered::internal(
					"a first-value column the aggregate does not hold",
				));
			};
			field.info = None;
			field.fallback = Some(expr.clone());
			field.fallback_is_constant = false;
		}
		let mut specs = Vec::with_capacity(analysis.fields.len());
		for field in &analysis.fields {
			let aggregate = match &field.info {
				Some(info) => {
					let mut calls = Vec::with_capacity(info.calls.len());
					for call in &info.calls {
						if call.id.resolve(registry).is_none() {
							return Err(NotCovered::internal(
								"an aggregate function the registry lacks",
							));
						}
						let mut extra_args = Vec::with_capacity(call.extra_args.len());
						for arg in &call.extra_args {
							extra_args.push(
								crate::exec::vm::host::out_of_line(|| {
									self.aggregate_scalar(arg, true)
								})
								.await?,
							);
						}
						calls.push(AggCall {
							name: call.id.clone(),
							extra_args,
						});
					}
					let post = match &info.post {
						Some(post) => Some(
							crate::exec::vm::host::out_of_line(|| {
								self.aggregate_scalar(post, true)
							})
							.await?,
						),
						None => None,
					};
					Some(AggExpr {
						calls,
						post,
					})
				}
				None => None,
			};
			// `GROUP ALL`'s columns the row cannot influence are evaluated once,
			// as the group finishes; every other column as the group takes rows.
			let finish = match &field.fallback {
				Some(fallback) if field.fallback_is_constant && analysis.keys.is_empty() => Some(
					crate::exec::vm::host::out_of_line(|| self.aggregate_scalar(fallback, false))
						.await?,
				),
				_ => None,
			};
			specs.push(AggSpec {
				output_path: field.output_path.clone(),
				is_group_key: field.is_group_key,
				group_key_index: field.group_key_index,
				aggregate,
				fallback_is_constant: field.fallback_is_constant,
				finish,
			});
		}
		let id = self.aggregates.len() as u16;
		self.aggregates.push(AggregateSink {
			fields: specs,
			keys: analysis.keys.len(),
		});
		Ok((id, analysis))
	}

	/// A pipeline of [`Program::host_code`] that emits the value of `expr`,
	/// which the host runs once over a row that is an object when
	/// `row_is_object` — a group's answers or keys — and over no row
	/// otherwise.
	async fn aggregate_scalar(&mut self, expr: &LogicalExpr, row_is_object: bool) -> Lowered<u16> {
		let aside = self.set_aside();
		self.rows_are_objects = row_is_object;
		let built = async {
			let value = crate::exec::vm::host::out_of_line(|| self.fallible_value(expr)).await?;
			let dst = self.register()?;
			self.emit(Instr::Load {
				dst,
				src: value,
			});
			self.emit(Instr::Emit {
				src: dst,
			});
			self.emit(Instr::EndRow);
			Ok(())
		}
		.await;
		self.restore(aside, built)
	}

	/// Where `expr`'s value is read from: the instructions the body answers
	/// the expression with; not covered where it does not answer it itself.
	async fn fallible_value(&mut self, expr: &LogicalExpr) -> Lowered<Operand> {
		if !self.is_native(expr) {
			return Err(self.uncovered(expr, "an expression"));
		}
		self.hide_shared();
		let value = crate::exec::vm::host::out_of_line(|| self.value(expr)).await;
		self.unhide_shared();
		value
	}

	/// An aggregate's argument or key: `expr`'s value, read as `NONE` when
	/// evaluating it fails only about the shape of the data, as the aggregate
	/// operator evaluates it.
	async fn operand_or_none(&mut self, expr: &LogicalExpr) -> Lowered<Operand> {
		let begin = self.emit(Instr::TryNone {
			catch: 0,
			dst: None,
		});
		let operand = crate::exec::vm::host::out_of_line(|| self.fallible_value(expr)).await?;
		// An operand the body reads where it stands has nothing to fail.
		if self.code.len() == begin as usize + 1 {
			self.code.pop();
			return Ok(operand);
		}
		let dst = self.register()?;
		self.emit(Instr::Load {
			dst,
			src: operand,
		});
		self.emit(Instr::TryEnd);
		let end = self.here();
		self.code[begin as usize] = Instr::TryNone {
			catch: end,
			dst: Some(dst),
		};
		// A pipeline that catches runs on decoded rows, with the host.
		self.uses_eval = true;
		Ok(Operand::Reg(dst))
	}

	/// Give column `field` of the row's group in aggregate `id` the value of
	/// `expr` while it needs one; an error about the shape of the data leaves
	/// the column as it was.
	async fn group_first(&mut self, id: u16, field: u16, expr: &LogicalExpr) -> Lowered<()> {
		let needed = self.emit(Instr::GroupFirstNeeded {
			aggregate: id,
			field,
			done: 0,
		});
		let begin = self.emit(Instr::TryNone {
			catch: 0,
			dst: None,
		});
		self.hide_shared();
		let operand = crate::exec::vm::host::out_of_line(|| self.fallible_value(expr)).await;
		self.unhide_shared();
		let operand = operand?;
		let guarded = self.code.len() != begin as usize + 1;
		if guarded {
			self.emit(Instr::TryEnd);
		} else {
			self.code.pop();
		}
		self.emit(Instr::GroupFirstSet {
			aggregate: id,
			field,
			src: operand,
		});
		let end = self.here();
		if let Instr::GroupFirstNeeded {
			done,
			..
		} = &mut self.code[needed as usize]
		{
			*done = end;
		}
		if guarded {
			self.code[begin as usize] = Instr::TryNone {
				catch: end,
				dst: None,
			};
			self.uses_eval = true;
		}
		Ok(())
	}

	/// The body's end for a row folded into aggregate `id`: its group keys,
	/// its group, then each field's arguments into their accumulators.
	async fn fold_into(&mut self, id: u16, analysis: &AggregationAnalysis) -> Lowered<()> {
		// The operator evaluates a row's keys, then every aggregate's
		// arguments, before it folds the row into its group field by field.
		for (index, key) in analysis.keys.iter().enumerate() {
			let value = crate::exec::vm::host::out_of_line(|| self.operand_or_none(key)).await?;
			self.emit(Instr::GroupKey {
				aggregate: id,
				index: index as u16,
				value,
			});
		}
		let mut arguments = Vec::new();
		for (field_idx, field) in analysis.fields.iter().enumerate() {
			for (agg_idx, call) in field.info.iter().flat_map(|info| info.calls.iter()).enumerate()
			{
				let value =
					crate::exec::vm::host::out_of_line(|| self.operand_or_none(&call.argument))
						.await?;
				arguments.push((field_idx, agg_idx, value));
			}
		}
		let grouped = !analysis.keys.is_empty();
		if grouped {
			self.emit(Instr::GroupSelect {
				aggregate: id,
			});
		}
		for (field_idx, field) in analysis.fields.iter().enumerate() {
			if field.is_group_key {
				continue;
			}
			if field.info.is_some() {
				for &(_, agg_idx, value) in arguments.iter().filter(|(f, ..)| *f == field_idx) {
					self.emit(Instr::AccUpdate {
						aggregate: id,
						field: field_idx as u16,
						agg: agg_idx as u16,
						value,
					});
				}
			} else if let Some(fallback) = &field.fallback
				// `GROUP ALL`'s constant columns are evaluated once, as the
				// group finishes; every other column as the group takes rows.
				&& (grouped || !field.fallback_is_constant)
			{
				crate::exec::vm::host::out_of_line(|| {
					self.group_first(id, field_idx as u16, fallback)
				})
				.await?;
			}
		}
		Ok(())
	}

	fn here(&self) -> Label {
		self.code.len() as Label
	}

	/// Emit `instr` at the end of the pipeline being built, as the
	/// superinstruction it forms with what precedes it where it forms one,
	/// and answer where it stands.
	///
	/// A superinstruction stands for instructions a body runs one after the
	/// other, where nothing jumps to any but the first, so every path runs
	/// what it would run apart:
	///
	/// - a field set on the object built just before it joins its construction
	///   ([`Instr::BuildObject`]);
	/// - a row's end after the row is handed on ([`Instr::Emit`]) is the hand-off
	///   ([`Instr::EmitEnd`]), with the value loaded for it, or the object built for it
	///   ([`Instr::BuildObjectEmit`]), where one was just before;
	/// - a conditional jump to a row's end is a filter that ends the row ([`Instr::Filter`],
	///   [`Instr::FilterCompare`], [`Instr::FilterFieldInt`]), formed when the row's end is emitted
	///   after the jump was pointed at it, or when the jump is pointed at a row's end already
	///   emitted ([`Self::patch`]).
	fn emit(&mut self, instr: Instr) -> Label {
		let end = self.code.len();
		let pointed = self.pointed_at == Some(end);
		self.pointed_at = None;
		match instr {
			Instr::EndRow => {
				for at in 0..end {
					if Self::jump_target(&self.code[at]) == Some(end as Label) {
						Self::to_filter(&mut self.code[at]);
					}
				}
				if !self.targets(end)
					&& let Some(at) = self.end_row_after_emit(end)
				{
					return at;
				}
				self.code.push(Instr::EndRow);
			}
			Instr::SetField {
				object,
				path,
				value,
			} if value != Operand::Reg(object) && !(pointed && self.targets(end)) => {
				match self.code.last_mut() {
					Some(Instr::NewObject {
						dst,
					}) if *dst == object => {
						*self.code.last_mut().expect("matched above") = Instr::BuildObject {
							dst: object,
							fields: Box::new([(path, value)]),
						};
						return (end - 1) as Label;
					}
					Some(Instr::BuildObject {
						dst,
						fields,
					}) if *dst == object => {
						let mut joined = std::mem::take(fields).into_vec();
						joined.push((path, value));
						*fields = joined.into_boxed_slice();
						return (end - 1) as Label;
					}
					_ => self.code.push(instr),
				}
			}
			instr => self.code.push(instr),
		}
		end as Label
	}

	/// The row's end emitted at `end` as the hand-off of the row just before
	/// it ([`Self::emit`]), where the instructions before it form one: where
	/// it stands, or `None`.
	fn end_row_after_emit(&mut self, end: usize) -> Option<Label> {
		let Some(Instr::Emit {
			src,
		}) = end.checked_sub(1).and_then(|at| self.code.get(at))
		else {
			return None;
		};
		let src = *src;
		let at = end - 1;
		// The value loaded for the hand-off, or the object built for it, joins
		// it where nothing jumps to the hand-off itself.
		if let Some(before) = at.checked_sub(1)
			&& !self.targets(at)
		{
			match &mut self.code[before] {
				Instr::Load {
					dst,
					src: loaded,
				} if *dst == src => {
					let loaded = *loaded;
					self.code.truncate(before);
					self.code.push(Instr::EmitEnd {
						src: loaded,
					});
					return Some(before as Label);
				}
				Instr::BuildObject {
					dst,
					fields,
				} if *dst == src => {
					let fields = std::mem::take(fields);
					self.code.truncate(before);
					self.code.push(Instr::BuildObjectEmit {
						fields,
					});
					return Some(before as Label);
				}
				_ => {}
			}
		}
		self.code[at] = Instr::EmitEnd {
			src: Operand::Reg(src),
		};
		Some(at as Label)
	}

	/// Whether a jump of the pipeline being built is pointed at `at`.
	fn targets(&mut self, at: usize) -> bool {
		self.code.iter_mut().any(|instr| instr.labels_mut().into_iter().any(|l| *l as usize == at))
	}

	/// Where `instr` jumps when it is a conditional jump a filter stands for.
	fn jump_target(instr: &Instr) -> Option<Label> {
		match instr {
			Instr::JumpIf {
				target,
				..
			}
			| Instr::JumpIfCompare {
				target,
				..
			}
			| Instr::JumpIfFieldInt {
				target,
				..
			} => Some(*target),
			_ => None,
		}
	}

	/// `instr`, a conditional jump to the row's end, as the filter that ends
	/// the row where it would jump.
	fn to_filter(instr: &mut Instr) {
		match *instr {
			Instr::JumpIfFieldInt {
				field,
				op,
				lit,
				reversed,
				fallback,
				truthy,
				..
			} => {
				*instr = Instr::FilterFieldInt {
					field,
					op,
					lit,
					reversed,
					fallback,
					reject: truthy,
				};
			}
			Instr::JumpIfCompare {
				op,
				left,
				right,
				truthy,
				..
			} => {
				*instr = Instr::FilterCompare {
					op,
					left,
					right,
					reject: truthy,
				};
			}
			Instr::JumpIf {
				cond,
				truthy,
				..
			} => {
				*instr = Instr::Filter {
					cond,
					reject: truthy,
				};
			}
			_ => {}
		}
	}

	/// Point the jump at `at` to `target`. A conditional jump pointed at a
	/// row's end already emitted is the filter that ends the row
	/// ([`Self::emit`]).
	fn patch(&mut self, at: Label, target: Label) {
		if (target as usize) < self.code.len()
			&& matches!(self.code[target as usize], Instr::EndRow)
			&& Self::jump_target(&self.code[at as usize]).is_some()
		{
			Self::to_filter(&mut self.code[at as usize]);
			return;
		}
		if target as usize == self.code.len() {
			self.pointed_at = Some(self.code.len());
		}
		match &mut self.code[at as usize] {
			Instr::JumpIfPermissionsBypassed {
				target: t,
			} => *t = target,
			Instr::JumpIfLimitReached {
				done,
				..
			} => *done = target,
			Instr::Skip {
				target: t,
				..
			}
			| Instr::Distinct {
				target: t,
				..
			}
			| Instr::JumpIf {
				target: t,
				..
			}
			| Instr::JumpIfNullish {
				target: t,
				..
			}
			| Instr::JumpIfFieldInt {
				target: t,
				..
			}
			| Instr::FilterBegin {
				none: t,
				..
			}
			| Instr::MapBegin {
				other: t,
				..
			}
			| Instr::JumpIfCompare {
				target: t,
				..
			}
			| Instr::JumpIfNone {
				target: t,
				..
			}
			| Instr::JumpIfNotObject {
				target: t,
				..
			}
			| Instr::JumpIfNotRecordId {
				target: t,
				..
			}
			| Instr::MethodResolve {
				closure: Some(t),
				..
			}
			| Instr::Subquery {
				caught: Some(super::program::Catch {
					label: t,
					..
				}),
				..
			}
			| Instr::Block(BlockInstr::Nested {
				caught: Some(super::program::Catch {
					label: t,
					..
				}),
				..
			})
			| Instr::ElemNext {
				done: t,
				..
			} => *t = target,
			Instr::Jump {
				target: t,
			}
			| Instr::Write(
				WriteInstr::JumpIfNew {
					target: t,
				}
				| WriteInstr::JumpIfNotIterationInitial {
					target: t,
				}
				| WriteInstr::JumpIfDone {
					target: t,
				}
				| WriteInstr::JumpIfImport {
					target: t,
				}
				| WriteInstr::JumpIfNoPostImage {
					target: t,
				},
			) => *t = target,
			Instr::Write(
				WriteInstr::Gate {
					skip,
				}
				| WriteInstr::ClassifyCreateFailure {
					skip,
					..
				}
				| WriteInstr::DispatchTable {
					skip,
					..
				},
			) => *skip = target,
			Instr::Write(WriteInstr::TryBegin {
				handler,
			}) => *handler = target,
			Instr::Write(WriteInstr::Data(instr)) => match instr.target_mut() {
				Some(t) => *t = target,
				None => unreachable!("patched a data instruction that does not jump: {instr:?}"),
			},
			Instr::Write(WriteInstr::Schema(instr)) => match instr.target_mut() {
				Some(t) => *t = target,
				None => unreachable!("patched a schema instruction that does not jump: {instr:?}"),
			},
			Instr::Write(WriteInstr::Storage(instr)) => match instr.target_mut() {
				Some(t) => *t = target,
				None => unreachable!("patched a storage instruction that does not jump: {instr:?}"),
			},
			Instr::Write(WriteInstr::Permissions(instr)) => match instr.target_mut() {
				Some(t) => *t = target,
				None => unreachable!("patched a gate instruction that does not jump: {instr:?}"),
			},
			Instr::Store(
				StoreInstr::JumpIfImport {
					target: t,
				}
				| StoreInstr::JumpIfNoService {
					target: t,
					..
				}
				| StoreInstr::JumpIfSpeculating {
					target: t,
				}
				| StoreInstr::JumpIfSessionBase {
					target: t,
					..
				}
				| StoreInstr::BackfillGroup {
					empty: t,
					..
				}
				| StoreInstr::Scan {
					unread: Some(t),
					..
				},
			) => *t = target,
			Instr::Next {
				done,
				..
			}
			| Instr::FetchRow {
				none: done,
				..
			}
			| Instr::KnnRank {
				none: done,
				..
			}
			| Instr::Degree {
				done,
				..
			}
			| Instr::Block(BlockInstr::IterNext {
				done,
				..
			}) => *done = target,
			other => unreachable!("patched an instruction that does not jump: {other:?}"),
		}
	}

	fn patch_all(&mut self, at: Vec<Label>, target: Label) {
		for at in at {
			self.patch(at, target);
		}
	}

	fn register(&mut self) -> Lowered<Reg> {
		match self.released.pop() {
			Some(reg) => Ok(reg),
			None => self.registers(1),
		}
	}

	/// Return `reg`, whose value nothing reads again, for a later
	/// [`Self::register`] to reuse.
	fn release(&mut self, reg: Reg) {
		if self.shared.iter().any(|scope| scope.values.iter().any(|(_, held)| *held == Some(reg))) {
			return;
		}
		self.released.push(reg);
	}

	/// The sharing scope of an evaluation region whose roots, in the order the
	/// code runs them, are `roots`.
	fn shared_scope(&self, roots: &[crate::expr::logical::dag::Root<'_>]) -> SharedScope {
		let oracle = crate::exec::logical::effects::Oracle::new(self.planner.function_registry());
		let dag = crate::expr::logical::dag::build(roots, &oracle);
		let mut kinds = HashMap::new();
		for root in roots {
			let _ = root.expr.visit(&mut |node| {
				if let LogicalExpr::Access {
					base,
					..
				} = node && let Some(kind) = dag.kind_at(base)
				{
					kinds.insert(&**base as *const LogicalExpr as usize, kind.clone());
				}
				std::ops::ControlFlow::<()>::Continue(())
			});
		}
		SharedScope {
			values: dag.shared().into_iter().map(|value| (value, None)).collect(),
			kinds,
		}
	}

	/// The kind the base of a field read is proven to evaluate to against the
	/// open schema: the region's, or, where the read is lowered under a hidden
	/// scope, the same proof taken directly.
	fn base_kind(&self, base: &LogicalExpr) -> Option<crate::expr::Kind> {
		let at = base as *const LogicalExpr as usize;
		match self.shared.last().and_then(|scope| scope.kinds.get(&at)) {
			Some(kind) => Some(kind.clone()),
			None => base.checked_kind(&crate::expr::logical::Schema::open()),
		}
	}

	/// What the innermost sharing scope makes of `expr`, by its address. An
	/// occurrence the DAG names as neither first nor reuse, such as a
	/// conditional one before the first, is lowered as written.
	fn shared_slot(&self, expr: &LogicalExpr) -> SharedSlot {
		let at = expr as *const LogicalExpr;
		let Some(scope) = self.shared.last() else {
			return SharedSlot::Unshared;
		};
		for (index, (value, held)) in scope.values.iter().enumerate() {
			if value.first == at {
				return SharedSlot::First(index);
			}
			// A later occurrence with nothing yet to read is lowered as
			// written.
			if let Some(held) = held
				&& value.reuses.contains(&at)
			{
				return SharedSlot::Reuse(*held);
			}
		}
		SharedSlot::Unshared
	}

	/// Stop reading shared values, until [`Self::unhide_shared`], for code
	/// that is lowered into registers other than the projection's.
	fn hide_shared(&mut self) {
		self.shared.push(SharedScope::default());
	}

	fn unhide_shared(&mut self) {
		self.shared.pop();
	}

	fn registers(&mut self, count: usize) -> Lowered<Reg> {
		let first = self.next_reg;
		self.next_reg = u16::try_from(count)
			.ok()
			.and_then(|count| self.next_reg.checked_add(count))
			.ok_or_else(|| NotCovered::internal("more registers than a body holds"))?;
		Ok(first)
	}

	fn field(&mut self, name: &str) -> u32 {
		let index = self.fields.len() as u32;
		self.fields.push(name.to_owned());
		index
	}

	/// What `expr` needs of the transaction and the session, the stored
	/// functions it calls resolved through the call graph as the planner
	/// resolves them. That resolution reads the definitions of the functions
	/// and modules called, so it runs unsealed, as the mapping does.
	async fn needs_of(&self, expr: &LogicalExpr) -> Needs {
		let mut functions = Vec::new();
		// A late-bound statement calls the functions the catalog holds when
		// it runs, any of which may write.
		if !self.planner.is_late_bound() {
			crate::exec::logical::stored_functions_of(expr, &mut functions);
		}
		let mut stored = Vec::with_capacity(functions.len());
		for function in functions {
			let mode = crate::kvs::catalog_reads::sealed(
				false,
				self.planner.resolve_function_access_mode(&function),
			)
			.await;
			stored.push((function, mode));
		}
		crate::exec::logical::needs_of_expr(expr, self.planner.function_registry(), &|function| {
			stored
				.iter()
				.find(|(stored, _)| stored == function)
				.map_or(AccessMode::ReadWrite, |(_, mode)| *mode)
		})
	}

	/// The register holding the projection of the row in [`ROW`], once the
	/// instructions emitted for it have run. [`ROW`] itself for `*`.
	async fn projection(&mut self, projection: &Projection) -> Lowered<Reg> {
		// A projection evaluates its fields with the row as their document
		// root, which an index or a method argument of a path is read against.
		let outer = std::mem::replace(&mut self.document, DocumentRoot::Row);
		let outer_omitted = self.omitted_row.take();
		let projected = crate::exec::vm::host::out_of_line(|| self.projection_of(projection)).await;
		self.omitted_row = outer_omitted;
		self.document = outer;
		projected
	}

	/// [`Self::projection`], with the row the fields' document root.
	async fn projection_of(&mut self, projection: &Projection) -> Lowered<Reg> {
		Ok(match projection {
			Projection::All => ROW,
			Projection::SortedValue(key) => self.sort_key(*key)?,
			Projection::Value(expr) => {
				let value = crate::exec::vm::host::out_of_line(|| self.projected(expr)).await;
				match value? {
					Operand::Reg(reg) => reg,
					other => {
						let dst = self.register()?;
						self.emit(Instr::Load {
							dst,
							src: other,
						});
						dst
					}
				}
			}
			Projection::OmittedValue {
				expr,
				omit,
			} => {
				let object = self.register()?;
				self.emit(Instr::LoadRow {
					dst: object,
				});
				for omission in omit {
					crate::exec::vm::host::out_of_line(|| {
						self.omit(object, omission, self.rows_are_objects)
					})
					.await?;
				}
				// The value reads the row as it stands without the omitted
				// paths, as its current value, its document and the `$this`
				// of a statement it holds.
				let outer = (
					self.element.replace(object),
					self.omitted_row.replace(object),
					self.statement_doc.replace(object),
				);
				let value = crate::exec::vm::host::out_of_line(|| self.projected(expr)).await;
				(self.element, self.omitted_row, self.statement_doc) = outer;
				match value? {
					Operand::Reg(reg) => reg,
					other => {
						let dst = self.register()?;
						self.emit(Instr::Load {
							dst,
							src: other,
						});
						dst
					}
				}
			}
			Projection::Star {
				fields,
				omit,
			} => {
				let fetched = self.fetched_row.take();
				let object = match fetched {
					Some(fetched) => {
						let object = self.register()?;
						self.emit(Instr::Load {
							dst: object,
							src: Operand::Reg(fetched),
						});
						object
					}
					None => {
						let object = self.register()?;
						self.emit(Instr::LoadRow {
							dst: object,
						});
						object
					}
				};
				// Fields over a fetched record read the record.
				let outer_element = match fetched {
					Some(fetched) => self.element.replace(fetched),
					None => self.element,
				};
				// A row that is not an object stands as it is beside fields
				// that only read it, and has its omissions walked. A field that
				// computes a value makes an object of the row (a geometry as
				// its members, any other value as nothing) to write the fields
				// onto, and one the full projection builds (a projection
				// function, or an output path of more than one part) makes an
				// object of the fields alone.
				let not_object = (!self.rows_are_objects).then(|| {
					self.emit(Instr::JumpIfNotObject {
						cond: Operand::Reg(object),
						target: 0,
					})
				});
				crate::exec::vm::host::out_of_line(|| {
					self.write_fields(object, fields, omit, true)
				})
				.await?;
				if let Some(not_object) = not_object {
					let done = self.emit(Instr::Jump {
						target: 0,
					});
					let here = self.here();
					self.patch(not_object, here);
					let builds = fields.iter().any(|field| {
						self.is_projection_call(&field.expr) || field.output.0.len() > 1
					});
					let computes = fields.iter().any(|field| {
						!matches!(field.expr, LogicalExpr::Field(_)) || field.output.0.len() > 1
					});
					if builds {
						self.emit(Instr::NewObject {
							dst: object,
						});
						crate::exec::vm::host::out_of_line(|| {
							self.write_fields(object, fields, omit, true)
						})
						.await?;
					} else if computes {
						self.emit(Instr::SeedObject {
							dst: object,
						});
						crate::exec::vm::host::out_of_line(|| {
							self.write_fields(object, fields, omit, true)
						})
						.await?;
					} else {
						for omission in omit {
							crate::exec::vm::host::out_of_line(|| {
								self.omit(object, omission, false)
							})
							.await?;
						}
					}
					let here = self.here();
					self.patch(done, here);
				}
				self.element = outer_element;
				object
			}
			Projection::Fields {
				fields,
				omit,
			} => {
				let object = self.register()?;
				self.emit(Instr::NewObject {
					dst: object,
				});
				crate::exec::vm::host::out_of_line(|| {
					self.write_fields(object, fields, omit, true)
				})
				.await?;
				object
			}
		})
	}

	/// Remove `omission` from the value in `object`, which is an object when
	/// `object_only`: a plain path is removed from it in place, and any other
	/// is walked by the host ([`Instr::Omit`]).
	async fn omit(&mut self, object: Reg, omission: &Omission, object_only: bool) -> Lowered<()> {
		match omission {
			Omission::Path(path) if object_only => {
				if let [crate::exec::field_path::FieldPathPart::Field(name)] = path.0.as_slice() {
					let name = self.field(name);
					self.emit(Instr::RemoveField {
						object,
						name,
					});
				} else {
					let path_idx = self.paths.len() as u32;
					self.paths.push(path.clone());
					self.emit(Instr::RemovePath {
						object,
						path: path_idx,
					});
				}
				Ok(())
			}
			Omission::Path(path) => self.omit_walk(object, &walk_along(path)).await,
			Omission::Walk {
				walk,
				..
			} => crate::exec::vm::host::out_of_line(|| self.omit_walk(object, walk)).await,
		}
	}

	/// [`Instr::Omit`] of `walk` from the value in `object`, each expression
	/// it evaluates a host pipeline over the value it is applied to.
	async fn omit_walk(&mut self, object: Reg, walk: &Walk<LogicalExpr>) -> Lowered<()> {
		if !walk.exprs().is_empty() && (self.eval_frame.is_some() || !self.inline_params.is_empty())
		{
			return Err(NotCovered::internal("an OMIT path that evaluates in a frame"));
		}
		let mut steps = Vec::with_capacity(walk.steps.len());
		for step in &walk.steps {
			steps.push(match step {
				WalkStep::Field(name) => WalkStep::Field(name.clone()),
				WalkStep::Index(index) => WalkStep::Index(*index),
				WalkStep::IndexAt(expr) => WalkStep::IndexAt(
					crate::exec::vm::host::out_of_line(|| self.omit_pipeline(expr)).await?,
				),
				WalkStep::Matching(expr) => WalkStep::Matching(
					crate::exec::vm::host::out_of_line(|| self.omit_pipeline(expr)).await?,
				),
				WalkStep::Each => WalkStep::Each,
				WalkStep::First => WalkStep::First,
				WalkStep::Last => WalkStep::Last,
				WalkStep::Destructure(fields) => WalkStep::Destructure(fields.clone()),
				WalkStep::Nowhere => WalkStep::Nowhere,
			});
		}
		let action = match &walk.action {
			WalkAction::RemoveField(name) => WalkAction::RemoveField(name.clone()),
			WalkAction::RemoveIndex(index) => WalkAction::RemoveIndex(*index),
			WalkAction::RemoveIndexAt(expr) => WalkAction::RemoveIndexAt(
				crate::exec::vm::host::out_of_line(|| self.omit_pipeline(expr)).await?,
			),
			WalkAction::RemoveMatching(expr) => WalkAction::RemoveMatching(
				crate::exec::vm::host::out_of_line(|| self.omit_pipeline(expr)).await?,
			),
			WalkAction::RemoveFirst => WalkAction::RemoveFirst,
			WalkAction::RemoveLast => WalkAction::RemoveLast,
			WalkAction::Clear => WalkAction::Clear,
			WalkAction::Destructure(fields) => WalkAction::Destructure(fields.clone()),
			WalkAction::Nothing => WalkAction::Nothing,
		};
		self.emit(Instr::Omit {
			object,
			walk: Arc::new(Walk {
				steps,
				action,
			}),
		});
		self.uses_eval = true;
		self.may_fail = true;
		Ok(())
	}

	/// The host pipeline that answers `expr`, an expression of an `OMIT` path
	/// known only once evaluated, over the value the path applies it to.
	async fn omit_pipeline(&mut self, expr: &LogicalExpr) -> Lowered<u16> {
		let element = self.element.take();
		let catch = self.return_catch.take();
		let aside = self.set_aside();
		let built = crate::exec::vm::host::out_of_line(|| async {
			let value = crate::exec::vm::host::out_of_line(|| self.once_value(expr, false)).await?;
			self.emit(Instr::Emit {
				src: value,
			});
			self.emit(Instr::EndRow);
			Ok::<(), NotCovered>(())
		})
		.await;
		let pipeline = self.restore(aside, built);
		self.element = element;
		self.return_catch = catch;
		pipeline
	}

	/// Set each of `fields` on the object in `object`, then remove the
	/// `omit` paths from it; `object_only` when `object` is known to be an
	/// object.
	async fn write_fields(
		&mut self,
		object: Reg,
		fields: &[super::plan::ProjectedField],
		omit: &[Omission],
		object_only: bool,
	) -> Lowered<()> {
		for field in fields {
			let path = self.paths.len() as u32;
			self.paths.push(field.output.clone());
			if let Some(key) = field.sort_key {
				let value = self.sort_key(key)?;
				self.emit(Instr::SetField {
					object,
					path,
					value: Operand::Reg(value),
				});
				continue;
			}
			if self.is_projection_call(&field.expr) {
				crate::exec::vm::host::out_of_line(|| {
					self.write_projection(&field.expr, object, path, field.aliased)
				})
				.await?;
				continue;
			}
			let value = crate::exec::vm::host::out_of_line(|| self.projected(&field.expr)).await?;
			self.emit(Instr::SetField {
				object,
				path,
				value,
			});
		}
		for omission in omit {
			crate::exec::vm::host::out_of_line(|| self.omit(object, omission, object_only)).await?;
		}
		Ok(())
	}

	/// Whether `body`, evaluated with an element as its current value, lowers
	/// to instructions that read the element as the operator tree does: every
	/// part is native and none reads the row where the tree reads the element
	/// ([`Self::reads_frame_of_element`]).
	fn is_native_element_body(&self, body: &LogicalExpr) -> bool {
		self.is_native_predicate(body) && !self.reads_frame_of_element(body)
	}

	/// A register holding the row the body runs over, as a value of its own.
	fn current(&mut self) -> Lowered<Reg> {
		let dst = self.register()?;
		self.emit(Instr::LoadRow {
			dst,
		});
		self.uses_eval = true;
		Ok(dst)
	}

	/// A register holding sort key `key` of the sorted row the pipeline runs.
	fn sort_key(&mut self, key: u16) -> Lowered<Reg> {
		if !self.reads_sorted_rows {
			return Err(NotCovered::internal("a sort key read over rows no sort handed over"));
		}
		let dst = self.register()?;
		self.emit(Instr::SortKey {
			key,
			dst,
		});
		Ok(dst)
	}

	/// Jumps taken when the truthiness of `expr` is `when`, for the caller to
	/// aim; control falls through otherwise.
	async fn branch(&mut self, expr: &LogicalExpr, when: bool) -> Lowered<Vec<Label>> {
		Ok(match (expr, when) {
			(
				LogicalExpr::Binary {
					left,
					op: BinaryOperator::And,
					right,
				},
				false,
			)
			| (
				LogicalExpr::Binary {
					left,
					op: BinaryOperator::Or,
					right,
				},
				true,
			) => {
				let mut jumps =
					crate::exec::vm::host::out_of_line(|| self.branch(left, when)).await?;
				jumps
					.extend(crate::exec::vm::host::out_of_line(|| self.branch(right, when)).await?);
				jumps
			}
			(
				LogicalExpr::Binary {
					left,
					op: BinaryOperator::And | BinaryOperator::Or,
					right,
				},
				_,
			) => {
				// `AND` taken when true, `OR` taken when false: the left side
				// deciding the other way skips the right side.
				let decided =
					crate::exec::vm::host::out_of_line(|| self.branch(left, !when)).await?;
				let jumps = crate::exec::vm::host::out_of_line(|| self.branch(right, when)).await?;
				self.patch_all(decided, self.here());
				jumps
			}
			(
				LogicalExpr::Prefix {
					op: PrefixOperator::Not,
					expr,
				},
				_,
			) => crate::exec::vm::host::out_of_line(|| self.branch(expr, !when)).await?,
			(other, _) => {
				let cond = crate::exec::vm::host::out_of_line(|| self.value(other)).await?;
				// A typed comparison only this branch reads is the branch.
				if let Operand::Reg(reg) = cond
					&& let Some(Instr::CompareFieldInt {
						field,
						op,
						lit,
						reversed,
						fallback,
						dst,
					}) = self.code.last()
					&& *dst == reg
				{
					let fused = Instr::JumpIfFieldInt {
						field: *field,
						op: *op,
						lit: *lit,
						reversed: *reversed,
						fallback: *fallback,
						truthy: when,
						target: 0,
					};
					*self.code.last_mut().expect("checked above") = fused;
					self.release(reg);
					return Ok(vec![self.here() - 1]);
				}
				// So is any other comparison.
				if let Operand::Reg(reg) = cond
					&& let Some(Instr::Compare {
						op,
						left,
						right,
						dst,
					}) = self.code.last()
					&& *dst == reg
				{
					let fused = Instr::JumpIfCompare {
						op: *op,
						left: *left,
						right: *right,
						truthy: when,
						target: 0,
					};
					*self.code.last_mut().expect("checked above") = fused;
					self.release(reg);
					return Ok(vec![self.here() - 1]);
				}
				vec![self.emit(Instr::JumpIf {
					cond,
					truthy: when,
					target: 0,
				})]
			}
		})
	}

	/// Whether [`Lowering::branch`] answers the predicate `expr` with instructions
	/// alone: `AND`, `OR` and `NOT` over what [`is_native`] admits.
	fn is_native_predicate(&self, expr: &LogicalExpr) -> bool {
		match expr {
			LogicalExpr::Binary {
				left,
				op: BinaryOperator::And | BinaryOperator::Or,
				right,
			} => self.is_native_predicate(left) && self.is_native_predicate(right),
			LogicalExpr::Prefix {
				op: PrefixOperator::Not,
				expr,
			} => self.is_native_predicate(expr),
			other => self.is_native(other),
		}
	}

	/// Whether [`Lowering::value`] compiles `expr` into instructions alone, with
	/// no expression the host evaluates: a field of the row, a literal or a
	/// parameter, and operators and collection literals over those.
	fn is_native(&self, expr: &LogicalExpr) -> bool {
		if crate::exec::planner::static_value(expr).is_some() {
			return true;
		}
		match expr {
			LogicalExpr::Field(_)
			| LogicalExpr::Current
			| LogicalExpr::Doc
			| LogicalExpr::Mock(_)
			| LogicalExpr::Literal(_)
			| LogicalExpr::Param(_)
			| LogicalExpr::Constant(_)
			| LogicalExpr::Table(_) => true,
			LogicalExpr::Binary {
				op: BinaryOperator::Matches(_),
				..
			} => self.matches_native() || self.answers_unscoped_operator(expr).is_some(),
			LogicalExpr::Binary {
				op: BinaryOperator::NearestNeighbor(_),
				..
			} => self.answers_unscoped_operator(expr).is_some(),
			LogicalExpr::Binary {
				left,
				op,
				right,
			} => {
				(CompareOp::of(op).is_some()
					|| ArithOp::of(op).is_some()
					|| matches!(
						op,
						BinaryOperator::And
							| BinaryOperator::Or | BinaryOperator::TenaryCondition
							| BinaryOperator::NullCoalescing
					)) && self.is_native(left)
					&& self.is_native(right)
			}
			LogicalExpr::Prefix {
				expr,
				..
			}
			| LogicalExpr::Postfix {
				expr,
				..
			} => self.is_native(expr),
			LogicalExpr::Call(call) => {
				if let Some((_, _, array, _)) = self.fused(call) {
					return self.is_native(array);
				}
				if let Some(index) = self.index_call(call) {
					return index.args.iter().all(|arg| self.is_native(arg));
				}
				// A function called where reads are unchecked runs the body the
				// plan binds without a principal ([`Self::reads_unchecked`]).
				let callable = match &call.receiver {
					crate::expr::Function::Custom(_) => true,
					crate::expr::Function::Normal(_) => self.native_builtin(call).is_some(),
					crate::expr::Function::Script(_)
					| crate::expr::Function::Model(_)
					| crate::expr::Function::Module(..)
					| crate::expr::Function::Silo {
						..
					} => true,
				};
				callable && call.args.iter().all(|arg| self.is_native(arg))
			}
			LogicalExpr::Throw(value) => self.is_native(value),
			LogicalExpr::Break | LogicalExpr::Continue => true,
			LogicalExpr::Lookup {
				..
			} => {
				self.lookup_run(expr).is_some_and(|(_, base)| self.is_native(base))
					|| self.lookup_statement_op(expr).is_some()
			}
			LogicalExpr::Broadcast {
				base,
				body,
				..
			} => self.is_native_broadcast(base, body),
			LogicalExpr::Closure(_) => true,
			LogicalExpr::Destructure {
				base,
				fields,
			} => self.is_native_destructure(base, fields),
			LogicalExpr::Optional {
				base,
				then,
			} => self.is_native_broadcast(base, then),
			LogicalExpr::Recurse(recurse) => self.is_native_recurse(recurse),
			LogicalExpr::RepeatRecurse(base) => self.is_native(base),
			LogicalExpr::SetField {
				base,
				value,
				..
			} => self.is_native(base) && self.is_native(value),
			LogicalExpr::Invoke {
				target,
				args,
			} => self.is_native(target) && args.iter().all(|arg| self.is_native(arg)),
			LogicalExpr::Filter {
				base,
				predicate,
			} => self.is_native(base) && self.is_native_element_body(predicate),
			LogicalExpr::Access {
				base,
				..
			}
			| LogicalExpr::Each(base)
			| LogicalExpr::First(base)
			| LogicalExpr::Last(base)
			| LogicalExpr::Flatten {
				base,
				..
			} => self.is_native(base),
			LogicalExpr::Index {
				base,
				index,
			} => self.is_native(base) && self.is_native(index) && self.in_step_frame(index),
			LogicalExpr::Method {
				base,
				name,
				args,
			} => {
				let _ = name;
				self.is_native(base)
					&& args.iter().all(|arg| self.is_native(arg) && self.in_step_frame(arg))
			}
			LogicalExpr::Array(items) | LogicalExpr::Set(items) => {
				items.iter().all(|item| self.is_native(item))
			}
			LogicalExpr::Object(entries) => entries.iter().all(|(_, value)| self.is_native(value)),
			LogicalExpr::RecordId(rid) => self.is_native_key(&rid.key),
			// A statement in scalar position runs as a plan of its own. `LET`
			// is refused there, by the operator tree's planner.
			LogicalExpr::Subquery(op) => !matches!(**op, LogicalOp::Let(_)),
			LogicalExpr::If(branches) => {
				branches
					.branches
					.iter()
					.all(|(cond, value)| self.is_native(cond) && self.is_native(value))
					&& branches.otherwise.as_ref().is_none_or(|otherwise| self.is_native(otherwise))
			}
		}
	}

	/// Whether [`Lowering::record_key`] builds `key` with instructions alone.
	fn is_native_key(&self, key: &crate::expr::logical::LogicalRecordIdKey) -> bool {
		use crate::expr::logical::LogicalRecordIdKey;
		let bound = |bound: &std::ops::Bound<LogicalRecordIdKey>| match bound {
			std::ops::Bound::Unbounded => true,
			std::ops::Bound::Included(key) | std::ops::Bound::Excluded(key) => {
				self.is_native_key(key)
			}
		};
		match key {
			LogicalRecordIdKey::Number(_)
			| LogicalRecordIdKey::String(_)
			| LogicalRecordIdKey::Uuid(_)
			| LogicalRecordIdKey::Generate(_) => true,
			LogicalRecordIdKey::Array(items) => items.iter().all(|item| self.is_native(item)),
			LogicalRecordIdKey::Object(entries) => {
				entries.iter().all(|(_, value)| self.is_native(value))
			}
			LogicalRecordIdKey::Range(range) => bound(&range.start) && bound(&range.end),
		}
	}

	/// The shape of record-id key `key`, the instructions computing its parts
	/// emitted in the order the operator tree evaluates them.
	async fn record_key(
		&mut self,
		key: &crate::expr::logical::LogicalRecordIdKey,
	) -> Lowered<super::program::KeyShape> {
		use super::program::KeyShape;
		use crate::expr::logical::LogicalRecordIdKey;
		use crate::val::RecordIdKey;
		Ok(match key {
			LogicalRecordIdKey::Number(n) => {
				KeyShape::Fixed(ValueData::new(RecordIdKey::Number(*n), "a record key")?)
			}
			LogicalRecordIdKey::String(s) => {
				KeyShape::Fixed(ValueData::new(RecordIdKey::String(s.clone()), "a record key")?)
			}
			LogicalRecordIdKey::Uuid(u) => {
				KeyShape::Fixed(ValueData::new(RecordIdKey::Uuid(*u), "a record key")?)
			}
			LogicalRecordIdKey::Generate(generator) => KeyShape::Generate(generator.clone()),
			// A `RETURN` signalled in an element is that element's value.
			LogicalRecordIdKey::Array(items) => {
				let array = self.register()?;
				self.emit(Instr::NewArray {
					dst: array,
				});
				for item in items {
					let outer = std::mem::replace(&mut self.key_element, true);
					let value =
						crate::exec::vm::host::out_of_line(|| self.caught(item, true)).await;
					self.key_element = outer;
					let value = value?;
					self.emit(Instr::ArrayPush {
						array,
						value,
					});
				}
				KeyShape::Array(Operand::Reg(array))
			}
			LogicalRecordIdKey::Object(entries) => {
				let object = self.register()?;
				self.emit(Instr::NewObject {
					dst: object,
				});
				for (key, value) in entries {
					let outer = std::mem::replace(&mut self.key_element, true);
					let value =
						crate::exec::vm::host::out_of_line(|| self.caught(value, true)).await;
					self.key_element = outer;
					let value = value?;
					self.paths.push(crate::exec::field_path::FieldPath::field(key.as_str()));
					let path = (self.paths.len() - 1) as u32;
					self.emit(Instr::SetField {
						object,
						path,
						value,
					});
				}
				KeyShape::Object(Operand::Reg(object))
			}
			LogicalRecordIdKey::Range(range) => {
				let start =
					crate::exec::vm::host::out_of_line(|| self.key_bound(&range.start)).await?;
				let end = crate::exec::vm::host::out_of_line(|| self.key_bound(&range.end)).await?;
				KeyShape::Range(Box::new(start), Box::new(end))
			}
		})
	}

	async fn key_bound(
		&mut self,
		bound: &std::ops::Bound<crate::expr::logical::LogicalRecordIdKey>,
	) -> Lowered<std::ops::Bound<super::program::KeyShape>> {
		use std::ops::Bound;
		Ok(match bound {
			Bound::Unbounded => Bound::Unbounded,
			Bound::Included(key) => {
				Bound::Included(crate::exec::vm::host::out_of_line(|| self.record_key(key)).await?)
			}
			Bound::Excluded(key) => {
				Bound::Excluded(crate::exec::vm::host::out_of_line(|| self.record_key(key)).await?)
			}
		})
	}

	/// Whether `expr`, an index or a method argument of a path step, has the
	/// value the body reads for it. The operator tree evaluates one against
	/// the document root, and against the value stepped from when there is
	/// none: rows that are records are their own document roots, so is the
	/// row a projection's fields are evaluated over, inside an element body
	/// too ([`Self::step_arg`]), and an expression that reads no row has the
	/// one value either way.
	fn in_step_frame(&self, expr: &LogicalExpr) -> bool {
		self.rows_are_objects
			|| self.document != DocumentRoot::Unknown
			|| !expr.any(|node| match node {
				LogicalExpr::Field(_) | LogicalExpr::Doc | LogicalExpr::Current => true,
				LogicalExpr::Param(param) => {
					crate::exec::planner::row_scope::RowScopeKind::from_param_name(param.as_str())
						.is_some()
				}
				_ => false,
			})
	}

	/// `arg`, an index or a method argument of a path step taken from `recv`,
	/// evaluated against the document root as the operator tree evaluates it:
	/// with the root as its current value, and with `recv` where there is no
	/// root. Where the root is unknown an element body holds no argument that
	/// reads a row.
	pub(super) async fn step_arg(&mut self, arg: &LogicalExpr, recv: Operand) -> Lowered<Operand> {
		let root = match self.document {
			DocumentRoot::Unknown => {
				return crate::exec::vm::host::out_of_line(|| self.value(arg)).await;
			}
			DocumentRoot::Row => self.omitted_row,
			DocumentRoot::RowElseReceiver => {
				let root = self.register()?;
				self.emit(Instr::DocumentRoot {
					or: recv,
					dst: root,
				});
				Some(root)
			}
		};
		let outer = std::mem::replace(&mut self.element, root);
		let value = crate::exec::vm::host::out_of_line(|| self.value(arg)).await;
		self.element = outer;
		value
	}

	/// A call of a builtin that runs a closure over an array's elements, whose
	/// closure is written in the call with a body that may run in place:
	/// what the builtin does per element, the closure, the array, and the
	/// expression the closure's body is.
	///
	/// The closure declares no more parameters than the builtin passes it, so
	/// none is bound as missing, and its body passes the inlining rule over
	/// its own parameters.
	fn fused<'c>(
		&self,
		call: &'c crate::expr::logical::LogicalCall,
	) -> Option<(Fused, &'c crate::expr::logical::LogicalClosure, &'c LogicalExpr, &'c LogicalExpr)>
	{
		let crate::expr::Function::Normal(name) = &call.receiver else {
			return None;
		};
		let [array, closure] = call.args.as_slice() else {
			return None;
		};
		let (fused, closure, body) = self.fused_closure(name, closure)?;
		Some((fused, closure, array, body))
	}

	/// What builtin `name` does per element when it is fused with `closure`,
	/// the closure, and the expression the closure's body is, when `closure`
	/// is a literal whose body may run in place.
	///
	/// The closure declares no more parameters than the builtin passes it, so
	/// none is bound as missing, and its body passes the inlining rule for a
	/// closure's.
	fn fused_closure<'c>(
		&self,
		name: &str,
		closure: &'c LogicalExpr,
	) -> Option<(Fused, &'c crate::expr::logical::LogicalClosure, &'c LogicalExpr)> {
		let fused = Fused::of(name)?;
		let LogicalExpr::Closure(closure) = closure else {
			return None;
		};
		if closure.args.len() > fused.passes()
			|| crate::exec::function::experimental_target(name).is_some()
		{
			return None;
		}
		let params: Vec<&str> = closure.args.iter().map(|(param, _)| param.as_str()).collect();
		let body =
			super::inline::closure_body(&closure.body, &params, self.planner.function_registry())?;
		self.is_native(body).then_some((fused, closure.as_ref(), body))
	}

	/// The builtin method `name` calls for an array receiver, fused with its
	/// one argument `args`, when it runs in place.
	fn fused_method<'c>(
		&self,
		name: &str,
		args: &'c [LogicalExpr],
	) -> Option<(String, Fused, &'c crate::expr::logical::LogicalClosure, &'c LogicalExpr)> {
		let [closure] = args else {
			return None;
		};
		let descriptor = self.planner.function_registry().get_method(name)?;
		let builtin = descriptor.resolve(&Value::Array(Default::default()))?.name().to_owned();
		let (fused, closure, body) = self.fused_closure(&builtin, closure)?;
		Some((builtin, fused, closure, body))
	}

	/// The loop of builtin `fused` over the array in `items`, invoking
	/// `closure` in place per element as its lowered `body`: the register its
	/// answer is in.
	async fn fused_loop(
		&mut self,
		fused: Fused,
		closure: &crate::expr::logical::LogicalClosure,
		body: &LogicalExpr,
		items: Reg,
	) -> Lowered<Reg> {
		self.closures.push(super::program::ClosureSpec {
			args: closure.args.clone(),
			returns: closure.returns.clone(),
		});
		let closure_id = (self.closures.len() - 1) as u32;
		let dst = self.register()?;
		let init = match fused {
			Fused::Map | Fused::Filter | Fused::FilterIndex => None,
			Fused::Any => Some(Value::Bool(false)),
			Fused::Every => Some(Value::Bool(true)),
			Fused::Find | Fused::FindIndex => Some(Value::None),
		};
		match init {
			None => {
				self.emit(Instr::NewArray {
					dst,
				});
			}
			Some(value) => {
				let src = self.constant(value);
				self.emit(Instr::Load {
					dst,
					src,
				});
			}
		}
		let pos = self.register()?;
		let zero = self.constant(Value::from(0i64));
		self.emit(Instr::Load {
			dst: pos,
			src: zero,
		});
		let elem = self.register()?;
		let index = self.register()?;
		let mut exits = vec![self.emit(Instr::ElemNext {
			array: items,
			pos,
			elem,
			index,
			done: 0,
		})];
		// Each invocation takes a re-entry from the caller's frame,
		// which the loop does not change: the first answers for all.
		self.emit(Instr::Dive {
			frame: self.eval_frame,
		});
		let top = self.here();
		let params = self.registers(closure.args.len())?;
		let passed = [Operand::Reg(elem), Operand::Reg(index)];
		self.emit(Instr::ClosureBind {
			closure: closure_id,
			args: passed[..fused.passes()].to_vec(),
			params,
		});
		let bindings = closure
			.args
			.iter()
			.enumerate()
			.map(|(i, (param, _))| (param.as_str().to_owned(), params + i as Reg))
			.collect();
		// The body reads the caller's document, not an element the call is
		// lowered inside of, and what it captured of the caller's inlined
		// parameters is what their registers hold.
		let mut bindings: Vec<(String, Reg)> = bindings;
		bindings.extend(self.inline_params.iter().cloned());
		let outer_params = std::mem::replace(&mut self.inline_params, bindings);
		let outer_element = self.element.take();
		let body = crate::exec::vm::host::out_of_line(|| self.value(body)).await;
		self.inline_params = outer_params;
		self.element = outer_element;
		let body = body?;
		let returned = self.register()?;
		self.emit(Instr::ClosureReturn {
			closure: closure_id,
			src: body,
			dst: returned,
		});
		let mut found = Vec::new();
		match fused {
			Fused::Map => {
				self.emit(Instr::ArrayPush {
					array: dst,
					value: Operand::Reg(returned),
				});
			}
			Fused::Filter | Fused::FilterIndex => {
				let skip = self.emit(Instr::JumpIf {
					cond: Operand::Reg(returned),
					truthy: false,
					target: 0,
				});
				self.emit(Instr::ArrayPush {
					array: dst,
					value: Operand::Reg(if fused == Fused::Filter {
						elem
					} else {
						index
					}),
				});
				let next = self.here();
				self.patch(skip, next);
			}
			Fused::Every => found.push(self.emit(Instr::JumpIf {
				cond: Operand::Reg(returned),
				truthy: false,
				target: 0,
			})),
			Fused::Any | Fused::Find | Fused::FindIndex => found.push(self.emit(Instr::JumpIf {
				cond: Operand::Reg(returned),
				truthy: true,
				target: 0,
			})),
		}
		exits.push(self.emit(Instr::ElemNext {
			array: items,
			pos,
			elem,
			index,
			done: 0,
		}));
		self.emit(Instr::Jump {
			target: top,
		});
		// The element that decides the answer: no later one is
		// invoked.
		if !found.is_empty() {
			let at = self.here();
			self.patch_all(found, at);
			let src = match fused {
				Fused::Any => self.constant(Value::Bool(true)),
				Fused::Every => self.constant(Value::Bool(false)),
				Fused::Find => Operand::Reg(elem),
				_ => Operand::Reg(index),
			};
			self.emit(Instr::Load {
				dst,
				src,
			});
		}
		let end = self.here();
		self.patch_all(exits, end);
		Ok(dst)
	}

	/// Method `method`, `descriptor`'s, called on the receiver in `recv` with
	/// `args`: resolved for the receiver before its arguments are evaluated.
	async fn method_call(
		&mut self,
		method: u32,
		descriptor: Arc<crate::exec::function::MethodDescriptor>,
		recv: Operand,
		args: &[LogicalExpr],
	) -> Lowered<Operand> {
		let dst = self.register()?;
		if descriptor.fallback.is_some() || descriptor.dispatches_for_an_object() {
			// A method with a generic implementation resolves for any
			// receiver; one with an object implementation leaves no
			// receiver for a closure field to answer.
			if descriptor.fallback.is_none() {
				self.emit(Instr::MethodResolve {
					method,
					recv,
					closure: None,
				});
			}
			let mut call_args = Vec::with_capacity(args.len());
			for arg in args {
				call_args
					.push(crate::exec::vm::host::out_of_line(|| self.step_arg(arg, recv)).await?);
			}
			self.emit(Instr::Method {
				method,
				recv,
				args: call_args,
				dst,
				frame: self.eval_frame,
			});
			self.uses_eval = true;
			self.may_fail = true;
			return Ok(Operand::Reg(dst));
		}
		// The receiver is resolved before the arguments; each branch
		// evaluates them for its own call.
		let resolve = self.here();
		self.emit(Instr::MethodResolve {
			method,
			recv,
			closure: Some(0),
		});
		let mut builtin_args = Vec::with_capacity(args.len());
		for arg in args {
			builtin_args
				.push(crate::exec::vm::host::out_of_line(|| self.step_arg(arg, recv)).await?);
		}
		self.emit(Instr::Method {
			method,
			recv,
			args: builtin_args,
			dst,
			frame: self.eval_frame,
		});
		let done = self.here();
		self.emit(Instr::Jump {
			target: 0,
		});
		let closure = self.here();
		self.patch(resolve, closure);
		let mut closure_args = Vec::with_capacity(args.len());
		for arg in args {
			closure_args
				.push(crate::exec::vm::host::out_of_line(|| self.step_arg(arg, recv)).await?);
		}
		self.emit(Instr::MethodClosure {
			method,
			recv,
			args: closure_args,
			dst,
			frame: self.eval_frame,
		});
		// The closure a receiver holds under the method's name is a body the
		// program cannot read, so the rows that may call it keep their order.
		self.row_ordered_calls += 1;
		let end = self.here();
		self.patch(done, end);
		// A method may read the row it is called on, and a closure
		// field is invoked by the host.
		self.uses_eval = true;
		self.may_fail = true;
		Ok(Operand::Reg(dst))
	}

	/// [`Self::method_call`] for a method whose builtin for an array receiver
	/// is `fused`'s: an array receiver runs the builtin's loop in place, as
	/// the call of the builtin with the closure written in it does, and any
	/// other takes the method's call.
	async fn fused_method_call(
		&mut self,
		method: u32,
		descriptor: Arc<crate::exec::function::MethodDescriptor>,
		recv: Operand,
		args: &[LogicalExpr],
		(builtin, fused, closure, body): (
			String,
			Fused,
			&crate::expr::logical::LogicalClosure,
			&LogicalExpr,
		),
	) -> Lowered<Operand> {
		let held = self.register()?;
		self.emit(Instr::Load {
			dst: held,
			src: recv,
		});
		let items = self.register()?;
		let scratch = self.register()?;
		let begin = self.emit(Instr::MapBegin {
			src: Operand::Reg(held),
			items,
			dst: scratch,
			other: 0,
		});
		self.calls.push(super::program::CallSpec {
			name: builtin,
			plan_depth: self.planner.current_depth(),
			index: None,
			closure_args: Vec::new(),
		});
		let call = (self.calls.len() - 1) as u32;
		self.emit(Instr::CallAllowed {
			call,
			frame: self.eval_frame,
		});
		self.emit(Instr::MethodRegion {
			method,
		});
		let dst =
			crate::exec::vm::host::out_of_line(|| self.fused_loop(fused, closure, body, items))
				.await?;
		self.emit(Instr::TryEnd);
		let over = self.emit(Instr::Jump {
			target: 0,
		});
		let other = self.here();
		self.patch(begin, other);
		let called = crate::exec::vm::host::out_of_line(|| {
			self.method_call(method, descriptor, Operand::Reg(held), args)
		})
		.await?;
		self.emit(Instr::Load {
			dst,
			src: called,
		});
		let end = self.here();
		self.patch(over, end);
		self.uses_eval = true;
		self.may_fail = true;
		Ok(Operand::Reg(dst))
	}

	/// The operands of a user-defined function's arguments, evaluated in
	/// `frame`: the call's, under the definer's auth, or the caller's where
	/// that auth is the caller's own.
	async fn call_args(
		&mut self,
		args: &[LogicalExpr],
		frame: Option<u16>,
	) -> Lowered<Vec<Operand>> {
		let outer = std::mem::replace(&mut self.eval_frame, frame);
		let mut operands = Vec::with_capacity(args.len());
		let mut failed = None;
		for arg in args {
			match crate::exec::vm::host::out_of_line(|| self.value(arg)).await {
				Ok(value) => operands.push(value),
				Err(e) => {
					failed = Some(e);
					break;
				}
			}
		}
		self.eval_frame = outer;
		match failed {
			Some(e) => Err(e),
			None => Ok(operands),
		}
	}

	/// The value index operator `expr` answers for every row where nothing in
	/// scope answers it, as the operator tree's operator answers it without
	/// evaluating either operand: `false` for a `MATCHES` outside the scope of
	/// a `SELECT` that registers its clauses, and for a `KNN` operator with no
	/// `KNN` in scope. `None` for any other expression.
	fn answers_unscoped_operator(&self, expr: &LogicalExpr) -> Option<Value> {
		let planner = self.bare.as_ref().unwrap_or(self.planner);
		match expr {
			LogicalExpr::Binary {
				op: BinaryOperator::Matches(_),
				..
			} if planner.matches_scope.is_none() => Some(Value::Bool(false)),
			LogicalExpr::Binary {
				op: BinaryOperator::NearestNeighbor(_),
				..
			} if !planner.has_knn_scope() => Some(Value::Bool(false)),
			_ => None,
		}
	}

	/// Whether the `MATCHES` operator is evaluated by instructions: in a
	/// `SELECT`, outside an element body and a permission frame, whose rows
	/// carry the record id the clause is judged for.
	fn matches_native(&self) -> bool {
		self.index_scope.select && self.element.is_none() && self.eval_frame.is_none()
	}

	/// The spec of `expr`, a `MATCHES` operator of the statement being lowered,
	/// as the operator tree's planner builds it: whether the `WHERE` registers
	/// it, the probe it makes of an index, and the pipeline that reads its
	/// left operand where the operand can cross a record link.
	async fn matches_spec(
		&mut self,
		expr: &LogicalExpr,
		operator: &crate::expr::operator::MatchesOperator,
		left: &LogicalExpr,
		right: &LogicalExpr,
	) -> Lowered<super::program::MatchesSpec> {
		use crate::exec::physical_expr::MatchProbe;
		// The `WHERE` registers the node as written, and a projection naming a
		// statement parameter as its query is registered by the node the bind
		// passes left in the `WHERE`, with the parameter's value in place.
		let registered = self.index_scope.registered.contains(expr)
			|| match right {
				LogicalExpr::Param(param) if !self.planner.param_is_shadowed(param.as_str()) => {
					self.caller_value(param.as_str())
						.cloned()
						.and_then(crate::exec::planner::util::try_value_to_logical_expr)
						.is_some_and(|value| {
							self.index_scope.registered.contains(&LogicalExpr::Binary {
								left: Box::new(left.clone()),
								op: BinaryOperator::Matches(operator.clone()),
								right: Box::new(value),
							})
						})
				}
				_ => false,
			};
		// A path on the left, and a right operand that names a value, rendered
		// as the analyzer receives it. A
		// parameter bound around the plan holds the binding's value when the
		// operator runs, so its query is read then ([`Instr::Matches`]).
		let probe = |query: &Value| MatchProbe {
			field: left.clone(),
			query: Some(query.to_raw_string()),
		};
		let probe = match right {
			_ if !left.is_path() => None,
			LogicalExpr::Param(param) if self.planner.param_is_shadowed(param.as_str()) => {
				Some(MatchProbe {
					field: left.clone(),
					query: None,
				})
			}
			// A parameter nothing binds is `NONE`.
			LogicalExpr::Param(param) => {
				Some(self.caller_value(param.as_str()).map_or_else(|| probe(&Value::None), probe))
			}
			other => crate::exec::planner::static_value(other).map(|value| probe(&value)),
		};
		// The operand is read only for a record link, which a path of more
		// than one field can cross.
		let crosses_link = probe
			.as_ref()
			.and_then(|probe| probe.field.field_path())
			.is_some_and(|path| path.segments().len() > 1);
		let left_code = if crosses_link {
			Some(crate::exec::vm::host::out_of_line(|| self.host_code(left)).await?)
		} else {
			None
		};
		// The tree's own expression names the operator in the error it raises.
		let sql = self
			.planner
			.physical_expr(expr.clone())
			.await
			.map_err(|e| match super::plan::Refusal::of(&e) {
				Some(refusal) => NotCovered::refused(refusal),
				None => NotCovered::internal(format!("an expression did not compile: {e}")),
			})?
			.to_sql();
		Ok(super::program::MatchesSpec {
			operator: operator.clone(),
			probe,
			registered,
			executor_tables: self.index_scope.executor_tables.clone(),
			sql,
			left: left_code,
		})
	}

	/// The index function `call` calls, with what it reports against and the
	/// arguments it is applied to, when the statement being lowered declares
	/// the `MATCHES` clause or `KNN` operator the call names. The reference
	/// argument is read here, not evaluated: it is the clause's number, a
	/// literal in range. A call in a permission frame or an element body, or
	/// to a clause the statement does not declare, is not covered.
	fn index_call<'c>(&self, call: &'c crate::expr::logical::LogicalCall) -> Option<IndexCall<'c>> {
		use crate::exec::function::IndexContextKind;
		use crate::expr::logical::LogicalLiteral;
		let crate::expr::Function::Normal(name) = &call.receiver else {
			return None;
		};
		if self.element.is_some() || self.eval_frame.is_some() {
			return None;
		}
		let func = self.planner.function_registry().get_index_function(name)?;
		let mut args: Vec<&LogicalExpr> = call.args.iter().collect();
		let mut query = None;
		let binding = match func.index_context_kind() {
			IndexContextKind::FullText => {
				let at = func.index_ref_arg_index()?;
				if at >= args.len() {
					return None;
				}
				let rf = match args.remove(at) {
					LogicalExpr::Literal(LogicalLiteral::Integer(n)) if (0..=255).contains(n) => {
						*n as u8
					}
					LogicalExpr::Literal(LogicalLiteral::Float(n))
						if (0.0..=255.0).contains(n) && n.fract() == 0.0 =>
					{
						*n as u8
					}
					_ => return None,
				};
				let clause = self.index_scope.resolve(rf)?;
				if let Some(param) = &clause.query_param {
					query = Some(LogicalExpr::Param(param.clone()));
				}
				IndexBinding::FullText {
					table: self
						.index_scope
						.table
						.clone()
						.unwrap_or_else(|| surrealdb_strand::TableName::from("unknown".to_owned())),
					path: clause.field.field_path(),
					query: clause.query.clone(),
					query_arg: clause.query_param.is_some(),
				}
			}
			IndexContextKind::Knn => {
				if !self.index_scope.knn {
					return None;
				}
				if let Some(at) = func.index_ref_arg_index()
					&& at < args.len()
				{
					args.remove(at);
				}
				IndexBinding::Knn {
					slot: self.knn_scope_slot.unwrap_or_else(|| self.knn_slot()),
				}
			}
		};
		Some(IndexCall {
			name: name.clone(),
			binding,
			args,
			query,
		})
	}

	/// The slot the distances of the `KNN` scan of the statement being lowered
	/// are published in, and its index functions read: one for each statement
	/// of the program.
	fn knn_slot(&self) -> u16 {
		self.current_group.map_or(0, |group| group.saturating_add(1))
	}

	/// `call`, a call of builtin `name` the body makes itself: the capability
	/// check, its arguments in turn, and the call by name through the function
	/// registry.
	async fn builtin_call(
		&mut self,
		call: &crate::expr::logical::LogicalCall,
		name: String,
	) -> Lowered<Operand> {
		let registry = self.planner.function_registry();
		let func_context = match registry.get_projection(&name) {
			Some(projection) => projection.required_context(),
			None => registry.get(&name).map(|f| f.required_context()).unwrap_or(ContextLevel::Root),
		};
		self.delegate_context = self.delegate_context.max(func_context);
		if crate::expr::method::is_writer_builtin(&name) {
			self.invokes += 1;
		}
		let closure_args: Vec<u16> = registry.get(&name).map_or_else(Vec::new, |function| {
			function.closure_args().iter().filter_map(|&at| u16::try_from(at).ok()).collect()
		});
		// A closure the builtin runs that is not a literal of the call is a
		// body the program cannot read: the rows that call it keep their
		// order. What it writes, the transaction the statement runs in admits
		// or refuses.
		if unseen_closure_arg(&call.args, &closure_args) {
			self.row_ordered_calls += 1;
		}
		self.calls.push(super::program::CallSpec {
			name,
			plan_depth: self.planner.current_depth(),
			index: None,
			closure_args,
		});
		let call_id = (self.calls.len() - 1) as u32;
		self.emit(Instr::CallAllowed {
			call: call_id,
			frame: self.eval_frame,
		});
		let mut args = Vec::with_capacity(call.args.len());
		for arg in &call.args {
			args.push(crate::exec::vm::host::out_of_line(|| self.value(arg)).await?);
		}
		let dst = self.register()?;
		self.emit(Instr::Call {
			call: call_id,
			args,
			dst,
			frame: self.eval_frame,
		});
		// A builtin may read the row it is called on.
		self.uses_eval = true;
		self.may_fail = true;
		Ok(Operand::Reg(dst))
	}

	/// The name of the builtin `call` calls when the body calls it itself: any
	/// name that is not an index function, and a projection function only
	/// outside an element body. A builtin that runs statements is called as
	/// any other, and a name no builtin holds raises, when called, the error
	/// the registry's lookup raises.
	fn native_builtin(&self, call: &crate::expr::logical::LogicalCall) -> Option<String> {
		let crate::expr::Function::Normal(name) = &call.receiver else {
			return None;
		};
		let registry = self.planner.function_registry();
		// A projection function called for its value reads the current value,
		// which is the row only outside an element body.
		let callable = if registry.is_projection(name) {
			self.element.is_none()
		} else {
			!registry.is_index_function(name)
		};
		callable.then(|| name.clone())
	}

	/// Whether every read a projection layer's column `expr` on a computed
	/// field makes of the reduced record it is evaluated against is a field of
	/// it or it itself, so its instructions read it as an element: no part
	/// reads it through `$parent` or a projection function, through a lookup
	/// `WHERE` reading `$parent`, or in the host step of a method, a filter, a
	/// destructure or a closure. A statement it holds takes the reduced record
	/// as its `$this` and `$parent` ([`Self::statement_doc`]).
	fn reads_row_as_column_element(&self, expr: &LogicalExpr) -> bool {
		!expr.any(|node| match node {
			LogicalExpr::Closure(_)
			| LogicalExpr::Invoke {
				..
			}
			| LogicalExpr::Filter {
				..
			}
			| LogicalExpr::Destructure {
				..
			}
			| LogicalExpr::Method {
				..
			} => true,
			LogicalExpr::Lookup {
				lookup,
				..
			} => lookup.cond.is_some(),
			LogicalExpr::Param(param) => param.as_str() == "parent",
			other => self.is_projection_call(other),
		})
	}

	/// Write `expr`, a call of a projection function at a projection's root,
	/// onto the object in `object`: its arguments evaluated in turn over the
	/// row, and its bindings written at [`Program::paths`]`[path]` when
	/// `aliased`, otherwise under the names the function produces.
	async fn write_projection(
		&mut self,
		expr: &LogicalExpr,
		object: Reg,
		path: u32,
		aliased: bool,
	) -> Lowered<()> {
		let LogicalExpr::Call(call) = expr else {
			return Err(NotCovered::internal("a projection function that is not a call"));
		};
		let crate::expr::Function::Normal(name) = &call.receiver else {
			return Err(NotCovered::internal("a projection function that is not a builtin"));
		};
		let registry = self.planner.function_registry();
		let context = registry
			.get_projection(name)
			.map_or(ContextLevel::Database, |projection| projection.required_context());
		self.delegate_context = self.delegate_context.max(context);
		self.calls.push(super::program::CallSpec {
			name: name.clone(),
			plan_depth: self.planner.current_depth(),
			index: None,
			closure_args: Vec::new(),
		});
		let call_id = (self.calls.len() - 1) as u32;
		let mut args = Vec::with_capacity(call.args.len());
		for arg in &call.args {
			args.push(crate::exec::vm::host::out_of_line(|| self.value(arg)).await?);
		}
		self.emit(Instr::WriteProjection {
			call: call_id,
			args,
			object,
			path,
			aliased,
		});
		// The function reads the row it is called on.
		self.uses_eval = true;
		self.may_fail = true;
		Ok(())
	}

	/// Whether `expr` is a call of a projection function, whose output names
	/// at a projection's root are the function's.
	fn is_projection_call(&self, expr: &LogicalExpr) -> bool {
		matches!(expr, LogicalExpr::Call(call)
			if matches!(&call.receiver, crate::expr::Function::Normal(name)
				if self.planner.function_registry().is_projection(name)))
	}

	/// A scalar the host evaluates outside a body: the value it denotes, or a
	/// pipeline of its own that computes it.
	async fn host_scalar(&mut self, expr: &LogicalExpr) -> Lowered<super::program::HostScalar> {
		use super::program::HostScalar;
		if let Some(value) = crate::exec::planner::static_value(expr) {
			return Ok(HostScalar::Const(super::plain::ValueData::new(value, "a host scalar")?));
		}
		Ok(HostScalar::Code(crate::exec::vm::host::out_of_line(|| self.host_code(expr)).await?))
	}

	/// The host pipeline, of [`Program::host_code`], that emits `expr`'s value
	/// once, built from the body's own instructions; not covered where the
	/// body does not answer `expr` ([`Self::is_native`]).
	async fn host_code(&mut self, expr: &LogicalExpr) -> Lowered<u16> {
		// The pipeline runs over the statement's `$this` or over no row,
		// neither of which is a record the body reads fields of.
		let rows_are_objects = std::mem::replace(&mut self.rows_are_objects, false);
		// Its row, where it has one, is the document root of what it
		// evaluates.
		let document = std::mem::replace(&mut self.document, DocumentRoot::RowElseReceiver);
		// The pipeline is built beside the one in flight, whose state
		// is set aside and restored.
		let code = std::mem::take(&mut self.code);
		let next_reg = std::mem::replace(&mut self.next_reg, ROW + 1);
		let released = std::mem::take(&mut self.released);
		let uses_eval = std::mem::replace(&mut self.uses_eval, false);
		let may_fail = std::mem::replace(&mut self.may_fail, false);
		self.slot_hidden += 1;
		self.hide_shared();
		let built = if self.is_native(expr) {
			crate::exec::vm::host::out_of_line(|| self.host_pipeline(expr)).await
		} else {
			Err(self.uncovered(expr, "a host scalar"))
		};
		self.unhide_shared();
		self.slot_hidden -= 1;
		self.code = code;
		self.next_reg = next_reg;
		self.released = released;
		self.rows_are_objects = rows_are_objects;
		self.document = document;
		self.uses_eval = uses_eval;
		self.may_fail = may_fail;
		let pipeline = built?;
		let index = u16::try_from(self.host_code.len())
			.map_err(|_| NotCovered::internal("more host pipelines than a program holds"))?;
		self.host_code.push(pipeline);
		Ok(index)
	}

	/// The pipelines a KNN search judges its candidates with: the table's
	/// permission, then each condition the search can be given.
	async fn knn_gate(
		&mut self,
		filter: &super::plan::KnnFilter,
	) -> Lowered<super::program::KnnGate> {
		use super::program::{KnnGate, KnnPrefilterSpec};
		let permissions = &filter.permissions;
		let permission = if permissions.is_empty() {
			None
		} else {
			Some(crate::exec::vm::host::out_of_line(|| self.gate_code(permissions, None)).await?)
		};
		let residual = match &filter.residual {
			Some(cond) => Some(
				crate::exec::vm::host::out_of_line(|| self.gate_code(permissions, Some(cond)))
					.await?,
			),
			None => None,
		};
		let matches_condition = match &filter.matches_condition {
			Some(cond) => Some(
				crate::exec::vm::host::out_of_line(|| self.gate_code(permissions, Some(cond)))
					.await?,
			),
			None => None,
		};
		let prefilter = match &filter.prefilter {
			Some(prefilter) => Some(KnnPrefilterSpec {
				root: ValueData::new(prefilter.root.clone(), "a KNN prefilter")?,
				residual: match &prefilter.residual {
					Some(cond) => Some(
						crate::exec::vm::host::out_of_line(|| {
							self.gate_code(permissions, Some(cond))
						})
						.await?,
					),
					None => None,
				},
				uncovered_matches: prefilter.uncovered_matches,
			}),
			None => None,
		};
		Ok(KnnGate {
			permission,
			residual,
			matches_condition,
			prefilter,
		})
	}

	/// The host pipeline, of [`Program::host_code`], that judges one record:
	/// it emits a row when the record passes each of `permissions` in turn and
	/// then `cond`, and none when it does not.
	async fn gate_code(
		&mut self,
		permissions: &[(
			LogicalExpr,
			Option<surrealdb_iam::AuthLimit>,
			crate::expr::logical::GuardKind,
			Option<super::plan::ComputedReads>,
		)],
		cond: Option<&LogicalExpr>,
	) -> Lowered<u16> {
		// The pipeline runs over the record's data as the row, set aside from
		// the pipeline in flight as a host scalar's is.
		let rows_are_objects = std::mem::replace(&mut self.rows_are_objects, false);
		let document = std::mem::replace(&mut self.document, DocumentRoot::RowElseReceiver);
		let code = std::mem::take(&mut self.code);
		let next_reg = std::mem::replace(&mut self.next_reg, ROW + 1);
		let released = std::mem::take(&mut self.released);
		let uses_eval = std::mem::replace(&mut self.uses_eval, false);
		let may_fail = std::mem::replace(&mut self.may_fail, false);
		self.slot_hidden += 1;
		self.hide_shared();
		let built =
			crate::exec::vm::host::out_of_line(|| self.knn_gate_pipeline(permissions, cond)).await;
		self.unhide_shared();
		self.slot_hidden -= 1;
		self.code = code;
		self.next_reg = next_reg;
		self.released = released;
		self.rows_are_objects = rows_are_objects;
		self.document = document;
		self.uses_eval = uses_eval;
		self.may_fail = may_fail;
		let pipeline = built?;
		let index = u16::try_from(self.host_code.len())
			.map_err(|_| NotCovered::internal("more host pipelines than a program holds"))?;
		self.host_code.push(pipeline);
		Ok(index)
	}

	/// The pipeline of [`Self::gate_code`].
	async fn knn_gate_pipeline(
		&mut self,
		permissions: &[(
			LogicalExpr,
			Option<surrealdb_iam::AuthLimit>,
			crate::expr::logical::GuardKind,
			Option<super::plan::ComputedReads>,
		)],
		cond: Option<&LogicalExpr>,
	) -> Lowered<Pipeline> {
		let mut rejected = Vec::new();
		for (predicate, auth, guard, computed) in permissions {
			rejected.extend(
				crate::exec::vm::host::out_of_line(|| {
					self.permission(predicate, auth, *guard, computed.as_ref())
				})
				.await?,
			);
		}
		if let Some(cond) = cond {
			if !self.is_native_predicate(cond) {
				return Err(self.uncovered(cond, "a KNN condition"));
			}
			rejected.extend(crate::exec::vm::host::out_of_line(|| self.branch(cond, false)).await?);
		}
		let passed = self.constant(Value::Bool(true));
		let dst = self.register()?;
		self.emit(Instr::Load {
			dst,
			src: passed,
		});
		self.emit(Instr::Emit {
			src: dst,
		});
		let end = self.here();
		self.patch_all(rejected, end);
		self.emit(Instr::EndRow);
		Ok(Pipeline {
			in_place: false,
			stored_rows: false,
			enforce: None,
			source: Source::Once,
			code: std::mem::take(&mut self.code),
			registers: self.next_reg as usize,
			limits: Vec::new(),
			region: false,
			group: None,
		})
	}

	/// The pipeline of a host scalar the body answers: it runs once, over the
	/// statement's `$this` when there is one, and emits `expr`'s value.
	async fn host_pipeline(&mut self, expr: &LogicalExpr) -> Lowered<Pipeline> {
		let value = crate::exec::vm::host::out_of_line(|| self.value(expr)).await?;
		self.value_pipeline(value)
	}

	/// The pipeline that emits `value` once.
	fn value_pipeline(&mut self, value: Operand) -> Lowered<Pipeline> {
		let dst = self.register()?;
		self.emit(Instr::Load {
			dst,
			src: value,
		});
		self.emit(Instr::Emit {
			src: dst,
		});
		self.emit(Instr::EndRow);
		Ok(Pipeline {
			in_place: false,
			stored_rows: false,
			enforce: None,
			source: Source::Once,
			code: std::mem::take(&mut self.code),
			registers: self.next_reg as usize,
			limits: Vec::new(),
			region: false,
			group: None,
		})
	}

	/// Where a projected field's value is read from: the instructions the
	/// body answers it with, a `RETURN` signalled at its root its value, as
	/// `({ RETURN 1 }) = 1` projects `1`; not covered where the body does not
	/// answer it itself.
	async fn projected(&mut self, expr: &LogicalExpr) -> Lowered<Operand> {
		if !self.is_native(expr) {
			return Err(self.uncovered(expr, "a projected field"));
		}
		crate::exec::vm::host::out_of_line(|| self.caught_value(expr)).await
	}

	/// The register a statement in an element body is taken over, as its
	/// `$this` and `$parent`, where the document root is the row or else the
	/// element: the row the pipeline runs over, and the element where it runs
	/// over none, as the operator tree takes the document root and else the
	/// current value. Otherwise the statement document already in force.
	fn element_statement_doc(&mut self) -> Lowered<Option<Reg>> {
		let (DocumentRoot::RowElseReceiver, None, Some(element)) =
			(self.document, self.statement_doc, self.element)
		else {
			return Ok(self.statement_doc);
		};
		let root = self.register()?;
		self.emit(Instr::DocumentRoot {
			or: Operand::Reg(element),
			dst: root,
		});
		Ok(Some(root))
	}

	/// The value of statement `op` held in an expression, run as a group of
	/// the program where it can be, or else as a unit run where it is
	/// evaluated over [`Self::statement_doc`].
	fn subquery_value<'s>(
		&'s mut self,
		op: &'s LogicalOp,
	) -> crate::exec::BoxFut<'s, Lowered<Operand>> {
		// A boxed `dyn` future: lowering recurses through this, and a caller
		// proving a statement's future `Send` stops at the box.
		Box::pin(async move {
			// A statement in a projection layer's column is a unit run
			// where the column is evaluated, never a group of the program.
			if !self.column_statements
				&& self.statement_doc.is_none()
				&& let Some(nested) =
					crate::exec::vm::host::out_of_line(|| self.nested_group(op)).await?
			{
				return Ok(nested);
			}
			let spec = if self.column_statements {
				// The layer binds `$value` for each of its columns, which a
				// statement in a column therefore reads from its frame.
				let base = match self.planner.catalog_within().nested_late {
					true => self.planner.late_binding(),
					false => self.planner.shadowing(std::iter::empty()),
				};
				// A column is definer code, planned against the session's own
				// values alone where it runs in a privileged frame.
				let base = match self.in_privileged_frame() {
					true => base.privileged(),
					false => base,
				};
				let planner = base
					.nested_statement(
						self.control_ifs,
						self.control_nested || self.control_ifs == 0,
					)
					.shadowing(["value".to_owned()]);
				crate::exec::vm::host::out_of_line(|| self.subquery_unit(&planner, op)).await?
			} else {
				crate::exec::vm::host::out_of_line(|| self.subquery(op)).await?
			};
			self.delegate_context = self.delegate_context.max(spec.required_context);
			self.subqueries.push(spec);
			let subquery = (self.subqueries.len() - 1) as u32;
			let dst = self.register()?;
			// Only the statement's own frame takes a `RETURN` as a value.
			let caught = match &self.return_catch {
				Some(catch)
					if catch.any_frame || !super::program::is_permission_frame(self.eval_frame) =>
				{
					Some(super::program::Catch {
						root: catch.root,
						label: 0,
						loops_fail: catch.loops_fail,
					})
				}
				_ => None,
			};
			let at = self.emit(Instr::Subquery {
				subquery,
				dst,
				frame: self.eval_frame,
				caught,
				doc: self.statement_doc,
			});
			if caught.is_some()
				&& let Some(catch) = &mut self.return_catch
			{
				catch.exits.push(at);
			}
			self.uses_eval = true;
			self.may_fail = true;
			Ok(Operand::Reg(dst))
		})
	}

	/// Why `expr`, a `what` the body does not answer itself, is not covered:
	/// a `LET` written as an expression, an index function it calls that the
	/// statement declares no clause for, or a recursion, which the operator
	/// tree refuses too, or else that it has no instructions.
	fn uncovered(&self, expr: &LogicalExpr, what: &str) -> NotCovered {
		if expr.any(
			|node| matches!(node, LogicalExpr::Subquery(op) if matches!(**op, LogicalOp::Let(_))),
		) {
			return NotCovered::refused(super::plan::Refusal::InvalidStatement {
				message: super::map::LET_IN_AN_EXPRESSION.to_owned(),
			});
		}
		let unbound = expr.any(|node| {
			matches!(node, LogicalExpr::Call(call)
				if matches!(&call.receiver, crate::expr::Function::Normal(name)
					if self.planner.function_registry().get_index_function(name).is_some())
					&& self.index_call(call).is_none())
		});
		if let Some(refusal) = self.recurse_refusal(expr) {
			NotCovered::refused(refusal)
		} else if let Some(refusal) = self.index_call_refusal(expr) {
			NotCovered::refused(refusal)
		} else if unbound {
			NotCovered::new(format!(
				"{what} calling an index function the statement declares no clause for"
			))
		} else {
			NotCovered::new(format!("{what} with no instructions"))
		}
	}

	/// The error the operator tree raises as it compiles an index function
	/// call in `expr` that names no clause of its statement: an index
	/// reference that is no literal in range, no `MATCHES` or `KNN` operator in
	/// the statement's `WHERE`, or no `MATCHES` clause with the reference.
	/// `None` where every index function call names a clause, or where one is
	/// written in an element body or a frame.
	fn index_call_refusal(&self, expr: &LogicalExpr) -> Option<super::plan::Refusal> {
		use crate::exec::function::IndexContextKind;
		use crate::expr::logical::LogicalLiteral;
		if self.element.is_some() || self.eval_frame.is_some() {
			return None;
		}
		let registry = self.planner.function_registry();
		let mut refusal = None;
		let _ = expr.visit(&mut |node| {
			let LogicalExpr::Call(call) = node else {
				return std::ops::ControlFlow::Continue(());
			};
			let crate::expr::Function::Normal(name) = &call.receiver else {
				return std::ops::ControlFlow::Continue(());
			};
			let Some(func) = registry.get_index_function(name) else {
				return std::ops::ControlFlow::Continue(());
			};
			if self.index_call(call).is_some() {
				return std::ops::ControlFlow::Continue(());
			}
			let message = match func.index_context_kind() {
				IndexContextKind::Knn => {
					format!("Index function '{name}': no KNN operator found in WHERE condition")
				}
				IndexContextKind::FullText => {
					let rf = func.index_ref_arg_index().and_then(|at| call.args.get(at)).and_then(
						|arg| match arg {
							LogicalExpr::Literal(LogicalLiteral::Integer(n))
								if (0..=255).contains(n) =>
							{
								Some(*n as u8)
							}
							LogicalExpr::Literal(LogicalLiteral::Float(n))
								if (0.0..=255.0).contains(n) && n.fract() == 0.0 =>
							{
								Some(*n as u8)
							}
							_ => None,
						},
					);
					match rf {
						None => format!(
							"Index function '{name}': index_ref argument must be a literal integer in range 0..255"
						),
						Some(_) if self.index_scope.matches.is_empty() => format!(
							"Index function '{name}': no MATCHES clause found in WHERE condition"
						),
						Some(rf) => {
							let mut available: Vec<u8> =
								self.index_scope.matches.iter().map(|(held, _)| *held).collect();
							available.sort_unstable();
							available.dedup();
							format!(
								"Index function '{name}': no MATCHES clause found for match_ref {rf} (available: {:?})",
								available.iter().collect::<Vec<_>>()
							)
						}
					}
				}
			};
			refusal = Some(super::plan::Refusal::Query {
				message,
			});
			std::ops::ControlFlow::Break(())
		});
		refusal
	}

	/// `expr`'s value, where a `RETURN` a subquery inside it signals is the
	/// value of the whole expression, as the operator tree takes one at a
	/// projected field's root and at a statement's.
	async fn caught_value(&mut self, expr: &LogicalExpr) -> Lowered<Operand> {
		crate::exec::vm::host::out_of_line(|| self.caught(expr, false)).await
	}

	/// `expr`'s value, where a `RETURN` a subquery inside it signals is the
	/// value of the whole expression, in any frame when `any_frame`.
	async fn caught(&mut self, expr: &LogicalExpr, any_frame: bool) -> Lowered<Operand> {
		if !expr.any(|node| matches!(node, LogicalExpr::Subquery(_))) {
			return crate::exec::vm::host::out_of_line(|| self.value(expr)).await;
		}
		let root = self.register()?;
		let outer = self.return_catch.replace(ReturnCatch {
			root,
			exits: Vec::new(),
			any_frame,
			loops_fail: self.key_element,
		});
		self.hide_shared();
		let value = crate::exec::vm::host::out_of_line(|| self.value(expr)).await;
		self.unhide_shared();
		let ReturnCatch {
			exits,
			..
		} = std::mem::replace(&mut self.return_catch, outer).expect("the catch set above");
		let value = value?;
		self.emit(Instr::Load {
			dst: root,
			src: value,
		});
		let end = self.here();
		self.patch_all(exits, end);
		Ok(Operand::Reg(root))
	}

	/// The steps `paths`, resolved `FETCH` paths, walk, as the operator tree's
	/// planner reads them off the paths; a computed index is the host
	/// pipeline that evaluates it.
	pub(super) async fn fetch_steps(
		&mut self,
		paths: &[LogicalExpr],
	) -> Lowered<Vec<Vec<super::program::FetchStepSpec>>> {
		use super::program::FetchStepSpec;
		use crate::exec::operators::fetch::{FetchPart, fetch_parts};
		let mut fields = Vec::with_capacity(paths.len());
		for path in paths {
			let mut steps = Vec::new();
			for part in fetch_parts(path) {
				steps.push(match part {
					FetchPart::Field(name) => FetchStepSpec::Field(name.clone()),
					FetchPart::All => FetchStepSpec::All,
					FetchPart::First => FetchStepSpec::First,
					FetchPart::Last => FetchStepSpec::Last,
					FetchPart::Index(index) => {
						if !self.is_native(index) {
							return Err(NotCovered::new("a FETCH index the body does not answer"));
						}
						FetchStepSpec::Index {
							pipeline: crate::exec::vm::host::out_of_line(|| self.host_code(index))
								.await?,
						}
					}
					FetchPart::Unsupported => FetchStepSpec::Unsupported,
				});
			}
			fields.push(steps);
		}
		Ok(fields)
	}

	/// Record that the program reads records of `reach` by id, so that it is
	/// lowered with the units that read them ([`super::dispatch`]).
	pub(super) fn reaches(&mut self, reach: super::dispatch::Reach) {
		self.reach = Some(match self.reach.take() {
			Some(held) => held.union(reach),
			None => reach,
		});
	}

	/// [`Self::reaches`], for a site the operator tree resolves against the
	/// catalog as it stands when the site runs ([`Self::late_reach`]).
	pub(super) fn reaches_late(&mut self, reach: super::dispatch::Reach) {
		self.late_reach = Some(match self.late_reach.take() {
			Some(held) => held.union(reach.clone()),
			None => reach.clone(),
		});
		self.reaches(reach);
	}

	/// Statement `op` in scalar position as a group of the program, run by
	/// [`super::program::BlockInstr::Nested`] as the plan's strategy for it
	/// says, into a fresh register. `None` where the plan holds no mapping of
	/// it, where the expression is lowered into a pipeline that runs over a
	/// state of its own — a host pipeline, a region's — or where its group
	/// does not lower; the statement then runs as a program of its own.
	async fn nested_group(&mut self, op: &LogicalOp) -> Lowered<Option<Operand>> {
		if self.slot_hidden > 0 {
			return Ok(None);
		}
		let Some(nested) = self.nested_plans.iter().find(|nested| nested.op == *op).cloned() else {
			return Ok(None);
		};
		// A block or a schema statement in scalar position runs as a program
		// of its own.
		if matches!(nested.plan.root, Plan::Control { .. } | Plan::Ddl { .. } | Plan::Info { .. }) {
			return Ok(None);
		}
		let probes = self.probes.len();
		let owner = self.probe_owner.replace(self.current_group);
		let planner = self
			.planner
			.nested_statement(self.control_ifs, self.control_nested || self.control_ifs == 0);
		// A statement held in definer code is lowered against the session's
		// own values alone, never a caller's bindings.
		let planner = match self.in_privileged_frame() {
			true => planner.privileged(),
			false => planner,
		};
		let grouped =
			crate::exec::vm::host::out_of_line(|| self.group(&planner, &nested.plan)).await;
		self.probe_owner = owner;
		let Ok((group, access_mode, required_context)) = grouped else {
			return Ok(None);
		};
		let read_only = access_mode == AccessMode::ReadOnly
			&& self.groups.get(group as usize).is_some_and(|spec| spec.write.is_none());
		self.delegate_context = self.delegate_context.max(required_context);
		self.nested.push(super::program::NestedSpec {
			group,
			inherits_row: op.inherits_row(),
			once: nested.strategy == super::plan::Strategy::Once,
			owner: self.current_group,
			hoist: None,
			read_only,
			probed: self.probes.len() > probes,
		});
		let index = (self.nested.len() - 1) as u32;
		let dst = self.register()?;
		// Only the statement's own frame takes a `RETURN` as a value.
		let caught = match &self.return_catch {
			Some(catch)
				if catch.any_frame || !super::program::is_permission_frame(self.eval_frame) =>
			{
				Some(super::program::Catch {
					root: catch.root,
					label: 0,
					loops_fail: catch.loops_fail,
				})
			}
			_ => None,
		};
		let at = self.emit(Instr::Block(BlockInstr::Nested {
			nested: index,
			dst,
			frame: self.eval_frame,
			caught,
		}));
		if caught.is_some()
			&& let Some(catch) = &mut self.return_catch
		{
			catch.exits.push(at);
		}
		self.uses_eval = true;
		self.may_fail = true;
		Ok(Some(Operand::Reg(dst)))
	}

	/// Subquery `op`, bound by `self`'s planner, as a unit of the program: see
	/// [`Self::subquery_unit`].
	async fn subquery(&mut self, op: &LogicalOp) -> Lowered<super::program::SubquerySpec> {
		let base = match self.planner.catalog_within().nested_late {
			true => self.planner.late_binding(),
			false => self.planner.shadowing(std::iter::empty()),
		};
		// A statement held in definer code is planned against the session's
		// own values alone, never a caller's bindings.
		let base = match self.in_privileged_frame() {
			true => base.privileged(),
			false => base,
		};
		let planner =
			base.nested_statement(self.control_ifs, self.control_nested || self.control_ifs == 0);
		crate::exec::vm::host::out_of_line(|| self.subquery_unit(&planner, op)).await
	}

	/// Whether a call of a user-defined function runs the definition the
	/// transaction reads when the call is made, through the function's call
	/// unit: in a late-bound statement, and in one running code that may
	/// redefine it ([`crate::exec::planner::CatalogWithin::calls_current`]).
	/// Whether the reads of the statement being lowered are made at a
	/// `VERSION` stamp only the run knows.
	pub(super) fn version_unknown(&self) -> bool {
		match self.planner.plan_time_version {
			crate::exec::planner::PlanTimeVersion::Current => self.versioned,
			crate::exec::planner::PlanTimeVersion::At(_) => false,
			crate::exec::planner::PlanTimeVersion::Unknown => true,
		}
	}

	pub(super) fn calls_current_definitions(&self) -> bool {
		self.planner.is_late_bound() || self.planner.catalog_within().calls_current
	}

	/// Whether a statement held in the expression being lowered is bound
	/// before it is mapped. In a frame it is only while a clause of a write is
	/// lowered, which holds the table it writes to on the cycle guard
	/// ([`Self::hold_write_table`]): a permission predicate that judges a
	/// read binds sources that resolve the permission being lowered, which
	/// holds the statement again, and a closure's call frame binds none.
	fn binds_nested(&self) -> bool {
		use super::program::CALL_FRAME;
		match self.eval_frame {
			None => true,
			Some(frame) => self.binds_in_frames && frame < CALL_FRAME,
		}
	}

	/// Hold the table the write being lowered writes to on the planner's
	/// cycle guard while a clause of its own is lowered, when the clause holds
	/// a statement: a statement reading the table binds its sources under the
	/// guard, which leaves the table unresolved rather than resolving the
	/// clause being lowered again. Declines where the table is held already.
	pub(super) async fn hold_write_table(
		&self,
		clause: &LogicalExpr,
	) -> Lowered<Option<impl Send + Sync + 'static>> {
		if !holds_statement(clause) {
			return Ok(None);
		}
		let (Ok(Some((ns, db))), Some(table)) = (self.database_ids(), self.write_table.clone())
		else {
			return Ok(None);
		};
		match self.planner.cycle_guard.try_enter(ns, db, table) {
			Some(held) => Ok(Some(held)),
			None => {
				Err(NotCovered::new("a clause holding a statement is lowered under its own table"))
			}
		}
	}

	/// The value of `expr`, a clause of a permission or of a field, lowered
	/// with [`Self::bare`] the planner that holds no transaction. A `RETURN` a
	/// statement in it signals is the clause's value when `returns_value`, in
	/// whatever frame the clause is evaluated.
	pub(super) async fn bare_value(
		&mut self,
		expr: &LogicalExpr,
		returns_value: bool,
	) -> Lowered<Operand> {
		let planner = Planner::new(self.planner.frozen(), self.planner.function_registry());
		let outer = self.bare.replace(planner);
		let value = if returns_value {
			crate::exec::vm::host::out_of_line(|| self.caught(expr, true)).await
		} else {
			crate::exec::vm::host::out_of_line(|| self.value(expr)).await
		};
		self.bare = outer;
		value
	}

	/// Statement `op`, bound by `planner`, lowered as a program of its own and
	/// held as a unit of the program this lowering builds.
	///
	/// A statement the VM does not lower fails the enclosing statement's
	/// lowering, which then runs on the operator tree whole; no subquery is
	/// planned apart from its program. The unit is the root of an arena of its
	/// own, so the host pipelines it runs over states of their own find the
	/// units they call. It runs in the frame it is lowered for and in no
	/// other.
	pub(super) async fn subquery_unit(
		&mut self,
		planner: &Planner<'ctx>,
		op: &LogicalOp,
	) -> Lowered<super::program::SubquerySpec> {
		let mapped = match super::map::map(planner, op).await {
			Ok(mapped) => mapped,
			// A statement held unbound — a schema statement's clause — has
			// sources whose scans enforce permissions per row, which the VM
			// does not; bound, they carry their permissions as nodes the plan
			// maps. Not where [`Self::binds_nested`] says it is not: there a
			// source is left unresolved because the frame's reads skip
			// permissions, and binding it would resolve the table's own
			// permission predicate, which holds the statement again.
			Err(unmapped) if self.binds_nested() => {
				let Ok(bound) = crate::exec::planner::bind_statement(planner, op.clone()).await
				else {
					return Err(unmapped);
				};
				super::map::map(planner, &bound).await.map_err(|_| unmapped)?
			}
			Err(unmapped) => return Err(unmapped),
		};
		let physical = super::optimize::optimize(mapped);
		let (unit, access_mode, required_context) =
			crate::exec::vm::host::out_of_line(|| self.statement_unit(planner, &physical)).await?;
		let keeps_row_order =
			self.units.get(unit.0 as usize).is_none_or(|program| program.keeps_row_order);
		Ok(super::program::SubquerySpec {
			unit,
			inherits_row: op.inherits_row(),
			access_mode,
			required_context,
			keeps_row_order,
		})
	}

	/// The optimized plan `physical`, lowered as a program of its own and
	/// held as a unit of the program this lowering builds, with what the unit
	/// does to the transaction and the context level it requires.
	pub(super) async fn statement_unit(
		&mut self,
		planner: &Planner<'ctx>,
		physical: &PhysicalPlan,
	) -> Lowered<(super::program::UnitId, AccessMode, ContextLevel)> {
		use super::program::{UnitHead, UnitId, UnitSig};
		// A statement a clause holds runs in the clause's frame, whose
		// principal is capped by the `AUTH LIMIT` of the definition that
		// supplied the clause, and is lowered for that principal.
		let limited;
		let planner = match self
			.eval_frame
			.and_then(|frame| self.permissions.get(frame as usize))
			.and_then(|frame| frame.auth.as_ref())
		{
			Some(limit) => {
				limited = planner.limited(limit);
				&limited
			}
			None => planner,
		};
		// Inside a function bundle the statement is a unit of the bundle's
		// arena, so that a call of a function on the path being lowered finds
		// the unit it is.
		let mut program = if self.bundle {
			let mut arena = self.take_arena();
			let enclosing = Enclosing {
				scope: &self.index_scope,
				catalog: &self.catalog,
			};
			let lowered = crate::exec::vm::host::out_of_line(|| {
				lower_in(planner, physical, Some(&mut arena), Some(enclosing))
			})
			.await;
			self.put_arena(arena);
			lowered?
		} else {
			let enclosing = Enclosing {
				scope: &self.index_scope,
				catalog: &self.catalog,
			};
			crate::exec::vm::host::out_of_line(|| lower_unit(planner, physical, Some(enclosing)))
				.await?
		};
		program.unit = Some(UnitHead {
			sig: UnitSig {
				params: Vec::new(),
				takes_row: false,
			},
			frame: Self::statement_frame(planner),
			entry: None,
			closure: None,
		});
		let id = u32::try_from(self.units.len())
			.map_err(|_| NotCovered::internal("more units than a program holds"))?;
		let shape = (UnitId(id), program.access_mode, program.required_context);
		self.units.push(Arc::new(program));
		Ok(shape)
	}

	/// The frame a whole statement lowered by `planner` is lowered for. The
	/// statement is lowered afresh, so its reads are checked or not as the
	/// permission nodes its plan holds say, whatever frame it is evaluated in.
	pub(super) fn statement_frame(planner: &Planner<'_>) -> super::program::UnitFrame {
		super::program::UnitFrame {
			class_digest: planner.class_digest(planner.auth.as_deref()),
			ns: planner.ns.clone(),
			db: planner.db.clone(),
			perms_suppressed: planner.perms_suppressed,
			checked: true,
		}
	}

	/// Whether the schema admits an integer in top-level field `name`. The
	/// declared kind is read as a hint, never as a guarantee: a field whose
	/// kind admits no integer is compared by the general instruction, since
	/// the guard would fail on every row, and a field whose kind admits one
	/// may still hold other values in rows written before the kind changed,
	/// which [`Instr::CompareFieldInt`] answers through its `fallback`.
	fn may_hold_int(&self, name: &str) -> bool {
		let Some(schema) = &self.schema else {
			return true;
		};
		match schema.field(&crate::expr::logical::FieldPath::field(name)) {
			Some(field) => kind_admits_int(&field.kind),
			None => true,
		}
	}

	/// Where `expr`'s value is read from once the instructions emitted for it
	/// have run.
	fn value<'s>(&'s mut self, expr: &'s LogicalExpr) -> crate::exec::BoxFut<'s, Lowered<Operand>> {
		// A boxed `dyn` future: lowering recurses through this, and a caller
		// proving a statement's future `Send` stops at the box.
		Box::pin(async move {
			// A scalar that denotes a value is that value, as the planner folds it.
			if let Some(value) = crate::exec::planner::static_value(expr) {
				let index = self.consts.len() as u32;
				self.consts.push(value);
				return Ok(Operand::Const(index));
			}
			match self.shared_slot(expr) {
				SharedSlot::Reuse(held) => return Ok(Operand::Reg(held)),
				SharedSlot::First(index) => {
					let result =
						crate::exec::vm::host::out_of_line(|| self.value_node(expr)).await?;
					let scope =
						self.shared.last_mut().expect("the scope outlasts its first occurrence");
					let pinned = match scope.values[index].1 {
						Some(held) => held,
						None => {
							let pinned = self.register()?;
							let scope = self
								.shared
								.last_mut()
								.expect("the scope outlasts its first occurrence");
							scope.values[index].1 = Some(pinned);
							pinned
						}
					};
					if result != Operand::Reg(pinned) {
						self.emit(Instr::Load {
							dst: pinned,
							src: result,
						});
					}
					return Ok(Operand::Reg(pinned));
				}
				SharedSlot::Unshared => {}
			}
			crate::exec::vm::host::out_of_line(|| self.value_node(expr)).await
		})
	}

	/// [`Self::value`] for an expression that is not read from a shared
	/// register: where its value is read from once the instructions emitted
	/// for it have run.
	fn value_node<'s>(
		&'s mut self,
		expr: &'s LogicalExpr,
	) -> crate::exec::BoxFut<'s, Lowered<Operand>> {
		Box::pin(async move {
			match expr {
				LogicalExpr::Constant(constant) => {
					self.consts.push(constant.compute());
					Ok(Operand::Const((self.consts.len() - 1) as u32))
				}
				LogicalExpr::Table(table) => {
					self.consts.push(Value::Table(table.clone()));
					Ok(Operand::Const((self.consts.len() - 1) as u32))
				}
				LogicalExpr::Param(param)
					if let Some((_, reg)) =
						self.inline_params.iter().find(|(name, _)| name == param.as_str()) =>
				{
					Ok(Operand::Reg(*reg))
				}
				// `$this` is the current value, which is the element inside an
				// element body.
				LogicalExpr::Param(param)
					if matches!(param.as_str(), "this" | "self")
						&& let Some(element) = self.element =>
				{
					Ok(Operand::Reg(element))
				}
				LogicalExpr::Param(param) => {
					if let Some(&(_, slot)) =
						self.slot_names.iter().rev().find(|(name, _)| name == param.as_str())
					{
						if self.slot_hidden == 0 && self.eval_frame.is_none() {
							let dst = self.register()?;
							self.emit(Instr::LoadSlot {
								slot,
								dst,
							});
							return Ok(Operand::Reg(dst));
						}
						self.frame_reads += 1;
					} else if !self.planner.param_is_shadowed(param.as_str())
						&& let Some(value) = self.caller_value(param.as_str())
					{
						// A closure the parameter holds when the program is planned
						// has its body prepared for the invocations the program
						// makes of it.
						let value = value.clone();
						crate::exec::vm::host::out_of_line(|| self.prepare_closures_in(&value))
							.await;
					}
					let spec =
						crate::exec::vm::host::out_of_line(|| self.param_spec(param)).await?;
					let dst = self.register()?;
					self.emit(Instr::LoadParam {
						param: spec,
						dst,
						frame: self.eval_frame,
					});
					// A parameter can read the row — `$this`, `$parent` — so the
					// pipeline runs on decoded rows, as for an evaluated expression.
					self.uses_eval = true;
					Ok(Operand::Reg(dst))
				}
				LogicalExpr::Binary {
					left,
					op: BinaryOperator::Matches(operator),
					right,
				} if self.matches_native() => {
					let spec = crate::exec::vm::host::out_of_line(|| {
						self.matches_spec(expr, operator, left, right)
					})
					.await?;
					let query = match &spec.probe {
						Some(probe) if probe.query.is_none() => {
							Some(crate::exec::vm::host::out_of_line(|| self.value(right)).await?)
						}
						_ => None,
					};
					self.matches.push(spec);
					self.delegate_context = self.delegate_context.max(ContextLevel::Root);
					let dst = self.register()?;
					self.emit(Instr::Matches {
						matches: (self.matches.len() - 1) as u32,
						query,
						dst,
						frame: self.eval_frame,
					});
					// The operator reads the record the row is.
					self.uses_eval = true;
					self.may_fail = true;
					Ok(Operand::Reg(dst))
				}
				LogicalExpr::Binary {
					op: BinaryOperator::Matches(_) | BinaryOperator::NearestNeighbor(_),
					..
				} if let Some(value) = self.answers_unscoped_operator(expr) => Ok(self.constant(value)),
				// `AND`, `OR`, `?:` and `??` answer an operand, evaluating the
				// right only when the left does not decide. The right operand
				// follows a jump, so `dag::reach` classes it conditional: a
				// value it computes is never the first occurrence a later
				// read relies on.
				LogicalExpr::Binary {
					left,
					op:
						op @ (BinaryOperator::And
						| BinaryOperator::Or
						| BinaryOperator::TenaryCondition
						| BinaryOperator::NullCoalescing),
					right,
				} => {
					let left = crate::exec::vm::host::out_of_line(|| self.value(left)).await?;
					let dst = self.register()?;
					self.emit(Instr::Load {
						dst,
						src: left,
					});
					let decided = match op {
						BinaryOperator::And => self.emit(Instr::JumpIf {
							cond: Operand::Reg(dst),
							truthy: false,
							target: 0,
						}),
						BinaryOperator::NullCoalescing => self.emit(Instr::JumpIfNullish {
							cond: Operand::Reg(dst),
							nullish: false,
							target: 0,
						}),
						_ => self.emit(Instr::JumpIf {
							cond: Operand::Reg(dst),
							truthy: true,
							target: 0,
						}),
					};
					let right = crate::exec::vm::host::out_of_line(|| self.value(right)).await?;
					self.emit(Instr::Load {
						dst,
						src: right,
					});
					let end = self.here();
					self.patch(decided, end);
					Ok(Operand::Reg(dst))
				}
				LogicalExpr::Binary {
					left,
					op,
					right,
				} if let Some(op) = ArithOp::of(op) => {
					let left = crate::exec::vm::host::out_of_line(|| self.value(left)).await?;
					let right = crate::exec::vm::host::out_of_line(|| self.value(right)).await?;
					let dst = self.register()?;
					self.emit(Instr::Arith {
						op,
						left,
						right,
						dst,
					});
					if !matches!(op, ArithOp::Range { .. }) {
						self.may_fail = true;
					}
					Ok(Operand::Reg(dst))
				}
				LogicalExpr::Prefix {
					op,
					expr: inner,
				} => {
					// A prefix chain longer than the computation depth allows is
					// refused when the planner compiles it.
					let mut depth = self.planner.current_depth();
					let mut cur = expr;
					while let LogicalExpr::Prefix {
						expr: next,
						..
					} = cur
					{
						depth += 1;
						if depth > self.planner.frozen().config.exec.max_computation_depth {
							return Err(NotCovered::refused(
								super::plan::Refusal::ComputationDepthExceeded,
							));
						}
						cur = next;
					}
					let op = match op {
						PrefixOperator::Not => UnaryOp::Not,
						PrefixOperator::Negate => UnaryOp::Negate,
						PrefixOperator::Positive => UnaryOp::Positive,
						PrefixOperator::Range => UnaryOp::RangeTo {
							included: false,
						},
						PrefixOperator::RangeInclusive => UnaryOp::RangeTo {
							included: true,
						},
						PrefixOperator::Cast(kind) => {
							self.kinds.push(kind.clone());
							UnaryOp::Cast {
								kind: (self.kinds.len() - 1) as u32,
							}
						}
					};
					let src = crate::exec::vm::host::out_of_line(|| self.value(inner)).await?;
					let dst = self.register()?;
					self.emit(Instr::Unary {
						op,
						src,
						dst,
					});
					if matches!(op, UnaryOp::Not | UnaryOp::Negate | UnaryOp::Cast { .. }) {
						self.may_fail = true;
					}
					Ok(Operand::Reg(dst))
				}
				LogicalExpr::Postfix {
					expr: inner,
					op,
				} => {
					use crate::expr::logical::LogicalPostfixOperator;
					let op = UnaryOp::RangeFrom {
						excluded: matches!(op, LogicalPostfixOperator::RangeSkip),
					};
					let src = crate::exec::vm::host::out_of_line(|| self.value(inner)).await?;
					let dst = self.register()?;
					self.emit(Instr::Unary {
						op,
						src,
						dst,
					});
					Ok(Operand::Reg(dst))
				}
				LogicalExpr::Access {
					base,
					field,
				} => {
					let src = crate::exec::vm::host::out_of_line(|| self.value(base)).await?;
					let stays = self.base_kind(base).is_some_and(|kind| access_stays_in_vm(&kind));
					let field = self.field(field.as_str());
					let dst = self.register()?;
					self.emit(Instr::Access {
						src,
						field,
						dst,
						frame: self.eval_frame,
					});
					// A record id is fetched by the host, unless the base is
					// proven to be a value the interpreter answers itself.
					if !stays {
						self.uses_eval = true;
					}
					Ok(Operand::Reg(dst))
				}
				// The index is lowered by `step_arg`, which rebinds the element
				// to the document root, so `dag::reach` classes it a body.
				LogicalExpr::Index {
					base,
					index,
				} => {
					let src = crate::exec::vm::host::out_of_line(|| self.value(base)).await?;
					let index =
						crate::exec::vm::host::out_of_line(|| self.step_arg(index, src)).await?;
					let dst = self.register()?;
					self.emit(Instr::Nav {
						op: NavOp::Index,
						src,
						index: Some(index),
						dst,
					});
					self.may_fail = true;
					Ok(Operand::Reg(dst))
				}
				LogicalExpr::Each(base) => {
					let src = crate::exec::vm::host::out_of_line(|| self.value(base)).await?;
					self.reaches(super::dispatch::Reach::Any);
					let dst = self.register()?;
					self.emit(Instr::Each {
						src,
						dst,
						frame: self.eval_frame,
					});
					// A record id is fetched by the host.
					self.uses_eval = true;
					Ok(Operand::Reg(dst))
				}
				LogicalExpr::Break | LogicalExpr::Continue if self.key_element => {
					self.emit(Instr::InvalidControlFlow);
					self.may_fail = true;
					// Nothing reads what a raised error leaves.
					Ok(Operand::Reg(self.register()?))
				}
				LogicalExpr::Break | LogicalExpr::Continue => {
					self.emit(Instr::Signal {
						brk: matches!(expr, LogicalExpr::Break),
						frame: self.eval_frame,
					});
					self.may_fail = true;
					// Nothing reads what a signal leaves.
					Ok(Operand::Reg(self.register()?))
				}
				LogicalExpr::Throw(value) => {
					let src = crate::exec::vm::host::out_of_line(|| self.value(value)).await?;
					self.emit(Instr::Throw {
						src,
					});
					self.may_fail = true;
					// Nothing reads what a raised error leaves.
					Ok(Operand::Reg(self.register()?))
				}
				LogicalExpr::First(base)
				| LogicalExpr::Last(base)
				| LogicalExpr::Flatten {
					base,
					..
				} => {
					let op = match expr {
						LogicalExpr::First(_) => NavOp::First,
						LogicalExpr::Last(_) => NavOp::Last,
						_ => NavOp::Flatten,
					};
					let src = crate::exec::vm::host::out_of_line(|| self.value(base)).await?;
					let dst = self.register()?;
					self.emit(Instr::Nav {
						op,
						src,
						index: None,
						dst,
					});
					Ok(Operand::Reg(dst))
				}
				LogicalExpr::Subquery(op) if self.is_native(expr) => {
					let outer = self.statement_doc;
					self.statement_doc = self.element_statement_doc()?;
					let value =
						crate::exec::vm::host::out_of_line(|| self.subquery_value(op)).await;
					self.statement_doc = outer;
					value
				}
				// A branch evaluated by the host could signal `RETURN` through
				// the value; the operator tree's `IfElseExpr` passes it on.
				// Each condition but the first and each value is lowered behind
				// a `JumpIf`, so `dag::reach` classes every child but the first
				// condition conditional.
				LogicalExpr::If(branches) if self.is_native(expr) => {
					let dst = self.register()?;
					let mut done = Vec::with_capacity(branches.branches.len());
					for (cond, value) in &branches.branches {
						let cond = crate::exec::vm::host::out_of_line(|| self.value(cond)).await?;
						let skip = self.emit(Instr::JumpIf {
							cond,
							truthy: false,
							target: 0,
						});
						// A statement in a branch stands one `IF` deeper.
						self.control_ifs += 1;
						let value = crate::exec::vm::host::out_of_line(|| self.value(value)).await;
						self.control_ifs -= 1;
						self.emit(Instr::Load {
							dst,
							src: value?,
						});
						done.push(self.emit(Instr::Jump {
							target: 0,
						}));
						let next = self.here();
						self.patch(skip, next);
					}
					let otherwise = match &branches.otherwise {
						Some(otherwise) => {
							self.control_ifs += 1;
							let value =
								crate::exec::vm::host::out_of_line(|| self.value(otherwise)).await;
							self.control_ifs -= 1;
							value?
						}
						None => self.constant(Value::None),
					};
					self.emit(Instr::Load {
						dst,
						src: otherwise,
					});
					let end = self.here();
					self.patch_all(done, end);
					Ok(Operand::Reg(dst))
				}
				// A key element takes a `RETURN` signalled in it as its value, and
				// a `BREAK` or `CONTINUE` as an error.
				LogicalExpr::RecordId(rid) if self.is_native_key(&rid.key) => {
					let key =
						crate::exec::vm::host::out_of_line(|| self.record_key(&rid.key)).await?;
					self.record_ids.push(super::program::RecordIdSpec {
						table: rid.table.clone(),
						key,
					});
					let record = (self.record_ids.len() - 1) as u32;
					let dst = self.register()?;
					self.emit(Instr::RecordId {
						record,
						dst,
					});
					Ok(Operand::Reg(dst))
				}
				LogicalExpr::Method {
					base,
					name,
					args,
				} => {
					let Some(descriptor) =
						self.planner.function_registry().get_method(name.as_str())
					else {
						return crate::exec::vm::host::out_of_line(|| {
							self.closure_field_call(base, name, args)
						})
						.await;
					};
					let descriptor = Arc::clone(descriptor);
					let closure_args = method_closure_args(&descriptor);
					// A closure the method runs that is not a literal of the call
					// is a body the program cannot read: the rows that call it keep
					// their order.
					if unseen_closure_arg(args, &closure_args) {
						self.row_ordered_calls += 1;
					}
					self.methods.push(super::program::MethodSpec {
						name: descriptor.name.to_owned(),
						kinds: descriptor.kind_mask(),
						has_fallback: descriptor.fallback.is_some(),
						plan_depth: self.planner.current_depth(),
						closure_args,
						writing: self.writing_labels,
					});
					let method = (self.methods.len() - 1) as u32;
					let recv = crate::exec::vm::host::out_of_line(|| self.value(base)).await?;
					match self.fused_method(name.as_str(), args) {
						Some(fused) => {
							crate::exec::vm::host::out_of_line(|| {
								self.fused_method_call(method, descriptor, recv, args, fused)
							})
							.await
						}
						None => {
							crate::exec::vm::host::out_of_line(|| {
								self.method_call(method, descriptor, recv, args)
							})
							.await
						}
					}
				}
				LogicalExpr::Call(call)
					if matches!(call.receiver, crate::expr::Function::Custom(_))
						&& self.calls_current_definitions() =>
				{
					crate::exec::vm::host::out_of_line(|| self.external_call(expr, call)).await
				}
				LogicalExpr::Call(call)
					if let crate::expr::Function::Custom(name) = &call.receiver
						&& self.is_native(expr) =>
				{
					crate::exec::vm::host::out_of_line(|| self.function_call(call, name)).await
				}
				LogicalExpr::Call(call) if Self::external_callee(call).is_some() => {
					crate::exec::vm::host::out_of_line(|| self.external_call(expr, call)).await
				}
				LogicalExpr::Call(call)
					if let Some((fused, closure, array, body)) = self.fused(call)
						&& self.is_native(array) =>
				{
					let crate::expr::Function::Normal(name) = &call.receiver else {
						unreachable!("a fused builtin is a builtin")
					};
					self.calls.push(super::program::CallSpec {
						name: name.clone(),
						plan_depth: self.planner.current_depth(),
						index: None,
						closure_args: Vec::new(),
					});
					let call_id = (self.calls.len() - 1) as u32;
					self.emit(Instr::CallAllowed {
						call: call_id,
						frame: self.eval_frame,
					});
					// The closure written in the call captures nothing its body
					// reads: it reads only its own parameters.
					let src = crate::exec::vm::host::out_of_line(|| self.value(array)).await?;
					let items = self.register()?;
					self.emit(Instr::ArrayArg {
						src,
						dst: items,
						call: call_id,
						position: 1,
					});
					let dst = crate::exec::vm::host::out_of_line(|| {
						self.fused_loop(fused, closure, body, items)
					})
					.await?;
					self.uses_eval = true;
					self.may_fail = true;
					Ok(Operand::Reg(dst))
				}
				LogicalExpr::Call(call) if let Some(index) = self.index_call(call) => {
					self.delegate_context = self.delegate_context.max(ContextLevel::Root);
					self.calls.push(super::program::CallSpec {
						name: index.name,
						plan_depth: self.planner.current_depth(),
						index: Some(index.binding),
						closure_args: Vec::new(),
					});
					let call_id = (self.calls.len() - 1) as u32;
					self.emit(Instr::CallAllowed {
						call: call_id,
						frame: self.eval_frame,
					});
					let mut args = Vec::with_capacity(index.args.len() + 1);
					for arg in index.args.into_iter().chain(index.query.as_ref()) {
						args.push(crate::exec::vm::host::out_of_line(|| self.value(arg)).await?);
					}
					let dst = self.register()?;
					self.emit(Instr::CallIndexFn {
						call: call_id,
						args,
						dst,
						frame: self.eval_frame,
					});
					// An index function reads the row it is called on.
					self.uses_eval = true;
					self.may_fail = true;
					Ok(Operand::Reg(dst))
				}
				LogicalExpr::Call(call)
					if self.counts_degree(call)
						&& let Some(name) = self.native_builtin(call) =>
				{
					crate::exec::vm::host::out_of_line(|| self.degree_count(call, name)).await
				}
				LogicalExpr::Call(call) if let Some(name) = self.native_builtin(call) => {
					crate::exec::vm::host::out_of_line(|| self.builtin_call(call, name)).await
				}
				LogicalExpr::Array(items) => {
					let dst = self.register()?;
					self.emit(Instr::NewArray {
						dst,
					});
					for item in items {
						let value = crate::exec::vm::host::out_of_line(|| self.value(item)).await?;
						self.emit(Instr::ArrayPush {
							array: dst,
							value,
						});
					}
					Ok(Operand::Reg(dst))
				}
				LogicalExpr::Set(items) => {
					let dst = self.register()?;
					self.emit(Instr::NewSet {
						dst,
					});
					for item in items {
						let value = crate::exec::vm::host::out_of_line(|| self.value(item)).await?;
						self.emit(Instr::CollectPush {
							collection: dst,
							value,
						});
					}
					Ok(Operand::Reg(dst))
				}
				LogicalExpr::Object(entries) => {
					let dst = self.register()?;
					self.emit(Instr::NewObject {
						dst,
					});
					for (key, value) in entries {
						let value =
							crate::exec::vm::host::out_of_line(|| self.value(value)).await?;
						self.paths.push(crate::exec::field_path::FieldPath::field(key.as_str()));
						let path = (self.paths.len() - 1) as u32;
						self.emit(Instr::SetField {
							object: dst,
							path,
							value,
						});
					}
					Ok(Operand::Reg(dst))
				}
				LogicalExpr::Field(name) if self.rows_are_objects && self.element.is_none() => {
					Ok(Operand::Field(self.field(name.as_str())))
				}
				// Over rows that may be any value, a field is read as a path step
				// from the row, as the operator tree steps from its current value.
				LogicalExpr::Field(name) => {
					let row = match self.element {
						Some(element) => element,
						None => self.current()?,
					};
					let field = self.field(name.as_str());
					let dst = self.register()?;
					self.emit(Instr::Access {
						src: Operand::Reg(row),
						field,
						dst,
						frame: self.eval_frame,
					});
					// A record id is fetched by the host.
					self.uses_eval = true;
					Ok(Operand::Reg(dst))
				}
				// The current value is the element inside an element body, and
				// otherwise the row the body runs over, `NONE` for a body that runs
				// over none.
				// `@` is the record identity of the current value.
				LogicalExpr::Doc => {
					let src =
						crate::exec::vm::host::out_of_line(|| self.value(&LogicalExpr::Current))
							.await?;
					let dst = self.register()?;
					self.emit(Instr::Nav {
						op: NavOp::Identity,
						src,
						index: None,
						dst,
					});
					Ok(Operand::Reg(dst))
				}
				LogicalExpr::Current if let Some(element) = self.element => {
					Ok(Operand::Reg(element))
				}
				LogicalExpr::Current => Ok(Operand::Reg(self.current()?)),
				LogicalExpr::Filter {
					base,
					predicate,
				} if self.is_native_element_body(predicate) => {
					let src = crate::exec::vm::host::out_of_line(|| self.value(base)).await?;
					let items = self.register()?;
					let dst = self.register()?;
					let begin = self.emit(Instr::FilterBegin {
						src,
						items,
						dst,
						none: 0,
					});
					let pos = self.register()?;
					let zero = self.constant(Value::from(0i64));
					self.emit(Instr::Load {
						dst: pos,
						src: zero,
					});
					let element = self.register()?;
					let index = self.register()?;
					let top = self.here();
					let next = self.emit(Instr::ElemNext {
						array: items,
						pos,
						elem: element,
						index,
						done: 0,
					});
					// The row is `$parent` inside the predicate, where it reads it.
					let parent = if crate::exec::planner::row_scope::references_parent(predicate) {
						let row = self.current()?;
						let bound = [("parent".to_owned(), row)]
							.into_iter()
							.chain(self.inline_params.iter().cloned())
							.collect();
						Some(std::mem::replace(&mut self.inline_params, bound))
					} else {
						None
					};
					let outer = self.element.replace(element);
					let rejected =
						crate::exec::vm::host::out_of_line(|| self.branch(predicate, false)).await;
					self.element = outer;
					if let Some(outer_params) = parent {
						self.inline_params = outer_params;
					}
					let rejected = rejected?;
					self.emit(Instr::CollectPush {
						collection: dst,
						value: Operand::Reg(element),
					});
					self.emit(Instr::Jump {
						target: top,
					});
					self.patch_all(rejected, top);
					let end = self.here();
					self.patch(begin, end);
					self.patch(next, end);
					self.uses_eval = true;
					Ok(Operand::Reg(dst))
				}
				LogicalExpr::Binary {
					left,
					op,
					right,
				} if self.rows_are_objects
					&& self.element.is_none()
					&& let Some((field, lit, reversed)) = int_comparison(left, right)
					&& let Some(int_op) = CompareOp::of(op).and_then(IntCmp::of)
					&& self.may_hold_int(field) =>
				{
					let fallback = CompareOp::of(op).expect("checked above");
					let field = self.field(field);
					let dst = self.register()?;
					self.emit(Instr::CompareFieldInt {
						field,
						op: if reversed {
							int_op.flipped()
						} else {
							int_op
						},
						lit,
						reversed,
						fallback,
						dst,
					});
					Ok(Operand::Reg(dst))
				}
				LogicalExpr::Binary {
					left,
					op,
					right,
				} if CompareOp::of(op).is_some() => {
					let op = CompareOp::of(op).expect("checked above");
					let left = crate::exec::vm::host::out_of_line(|| self.value(left)).await?;
					let right = crate::exec::vm::host::out_of_line(|| self.value(right)).await?;
					let dst = self.register()?;
					self.emit(Instr::Compare {
						op,
						left,
						right,
						dst,
					});
					Ok(Operand::Reg(dst))
				}
				LogicalExpr::Lookup {
					..
				} => match crate::exec::vm::host::out_of_line(|| self.lookup(expr)).await? {
					Some(value) => Ok(value),
					None => {
						match crate::exec::vm::host::out_of_line(|| self.lookup_statement(expr))
							.await
						{
							// Inside an element body the statement is taken over the
							// element, as the operator tree's lookup is.
							Some(statement) => {
								let outer = match self.element {
									Some(element) => self.statement_doc.replace(element),
									None => self.statement_doc,
								};
								let value =
									crate::exec::vm::host::out_of_line(|| self.value(&statement))
										.await;
								self.statement_doc = outer;
								value
							}
							None => Err(NotCovered::new("a lookup with no lowering here")),
						}
					}
				},
				LogicalExpr::Broadcast {
					base,
					body,
					flatten,
				} if self.is_native_broadcast(base, body) => {
					crate::exec::vm::host::out_of_line(|| self.broadcast(base, body, *flatten))
						.await
				}
				LogicalExpr::Destructure {
					base,
					fields,
				} if self.is_native_destructure(base, fields) => {
					crate::exec::vm::host::out_of_line(|| self.destructure(base, fields)).await
				}
				LogicalExpr::Optional {
					base,
					then,
				} if self.is_native_broadcast(base, then) => {
					crate::exec::vm::host::out_of_line(|| self.optional(base, then)).await
				}
				LogicalExpr::Recurse(recurse) if self.is_native_recurse(recurse) => {
					crate::exec::vm::host::out_of_line(|| self.recurse(recurse)).await
				}
				// The object `base` with `field` set to `value`, `NONE` removing
				// it; anything but an object is itself, `value` unevaluated.
				// `value` follows `JumpIfNotObject`, so `dag::reach` classes it
				// conditional.
				LogicalExpr::SetField {
					base,
					field,
					value,
				} => {
					let src = crate::exec::vm::host::out_of_line(|| self.value(base)).await?;
					let dst = self.register()?;
					self.emit(Instr::Load {
						dst,
						src,
					});
					let skip = self.emit(Instr::JumpIfNotObject {
						cond: Operand::Reg(dst),
						target: 0,
					});
					let value = crate::exec::vm::host::out_of_line(|| self.value(value)).await?;
					self.paths.push(crate::exec::field_path::FieldPath::field(field.as_str()));
					let path = (self.paths.len() - 1) as u32;
					self.emit(Instr::SetField {
						object: dst,
						path,
						value,
					});
					let kept = self.emit(Instr::JumpIfNone {
						cond: value,
						none: false,
						target: 0,
					});
					let name = self.field(field.as_str());
					self.emit(Instr::RemoveField {
						object: dst,
						name,
					});
					let end = self.here();
					self.patch(skip, end);
					self.patch(kept, end);
					Ok(Operand::Reg(dst))
				}
				// The `.@` marker answers in the phase of the recursion whose body
				// holds it.
				LogicalExpr::RepeatRecurse(base) => {
					let src = crate::exec::vm::host::out_of_line(|| self.value(base)).await?;
					let dst = self.register()?;
					self.emit(Instr::RepeatRecurse {
						src,
						dst,
					});
					self.uses_eval = true;
					self.may_fail = true;
					Ok(Operand::Reg(dst))
				}
				LogicalExpr::Mock(mock) => {
					let dst = self.register()?;
					self.emit(Instr::Mock {
						mock: Arc::new(mock.clone()),
						limit: self.planner.frozen().config.exec.generation_allocation_limit,
						dst,
					});
					self.may_fail = true;
					Ok(Operand::Reg(dst))
				}
				LogicalExpr::Closure(_) => {
					match crate::exec::vm::host::out_of_line(|| self.closure_value(expr)).await? {
						Some(value) => Ok(value),
						None => Err(NotCovered::new("a closure with no lowering")),
					}
				}
				LogicalExpr::Invoke {
					target,
					args,
				} => crate::exec::vm::host::out_of_line(|| self.invoke(target, args)).await,
				other => Err(self.uncovered(other, "an expression")),
			}
		})
	}
}

/// The field, integer literal and operand order of a comparison between a
/// top-level field of the row and an integer literal, either way round.
fn int_comparison<'e>(
	left: &'e LogicalExpr,
	right: &'e LogicalExpr,
) -> Option<(&'e str, i64, bool)> {
	use crate::expr::logical::LogicalLiteral;
	match (left, right) {
		(LogicalExpr::Field(name), LogicalExpr::Literal(LogicalLiteral::Integer(n))) => {
			Some((name.as_str(), *n, false))
		}
		(LogicalExpr::Literal(LogicalLiteral::Integer(n)), LogicalExpr::Field(name)) => {
			Some((name.as_str(), *n, true))
		}
		_ => None,
	}
}

/// The roots of the evaluation region a projection's fields are: the value of
/// a value projection, or each field that is not a sort key, in written order
/// and against `frame`.
fn projection_roots<'p>(
	projection: &'p super::plan::Projection,
	frame: crate::expr::logical::dag::Frame,
	roots: &mut Vec<crate::expr::logical::dag::Root<'p>>,
) {
	use crate::expr::logical::dag::Root;
	match projection {
		Projection::Value(expr) => roots.push(Root {
			expr,
			frame,
		}),
		Projection::Fields {
			fields,
			..
		} => {
			roots.extend(fields.iter().filter(|field| field.sort_key.is_none()).map(|field| Root {
				expr: &field.expr,
				frame,
			}))
		}
		Projection::All
		| Projection::OmittedValue {
			..
		}
		| Projection::SortedValue(_)
		| Projection::Star {
			..
		} => {}
	}
}

/// The roots of the evaluation region the row code of a pipeline's streaming
/// `chain` is, in the order the code runs them. A node that replaces or
/// rebinds the row starts a new frame, so that a read of the row above it is
/// not the same value as a read beneath it: a layer sets columns, a binding
/// replaces the row, a fetch rebinds fetched fields and a projection builds a
/// new row. A layer's columns are evaluated in frames of their own and are not
/// roots; a permission, a limit and a distinct hold nothing the row code
/// shares.
fn chain_roots<'p>(chain: &[&'p Plan], roots: &mut Vec<crate::expr::logical::dag::Root<'p>>) {
	use crate::expr::logical::dag::{Frame, Root};
	let mut frame = Frame::new(0);
	for node in chain.iter().rev().copied() {
		match node {
			Plan::Filter {
				predicate,
				..
			} => roots.push(Root {
				expr: predicate,
				frame,
			}),
			Plan::Project {
				projection,
				..
			} => {
				projection_roots(projection, frame, roots);
				frame = frame.next();
			}
			Plan::Layer {
				..
			}
			| Plan::Step {
				..
			}
			| Plan::Fetch {
				..
			} => frame = frame.next(),
			_ => {}
		}
	}
}

/// Whether a base proven to be of `kind` never makes [`Instr::Access`] take
/// the host step: the interpreter hands a record id, an array or a geometry to
/// the host to read a field of, and answers every other value itself. A kind
/// that may be any of those, as `any`, an either and a literal may be, does not
/// stay.
fn access_stays_in_vm(kind: &crate::expr::Kind) -> bool {
	use crate::expr::Kind;
	match kind {
		Kind::None
		| Kind::Null
		| Kind::Bool
		| Kind::Bytes
		| Kind::Datetime
		| Kind::Decimal
		| Kind::Duration
		| Kind::Float
		| Kind::Int
		| Kind::Number
		| Kind::Object
		| Kind::String
		| Kind::Uuid
		| Kind::Regex
		| Kind::Range
		| Kind::Table(_)
		| Kind::File(_)
		| Kind::Function(..)
		| Kind::Set(..) => true,
		Kind::Any
		| Kind::Record(_)
		| Kind::Array(..)
		| Kind::Geometry(_)
		| Kind::Either(_)
		| Kind::Literal(_) => false,
	}
}

/// Whether a value of `kind` can be an integer.
fn kind_admits_int(kind: &crate::expr::Kind) -> bool {
	use crate::expr::Kind;
	match kind {
		Kind::Any | Kind::Int | Kind::Number => true,
		Kind::Either(kinds) => kinds.iter().any(kind_admits_int),
		_ => false,
	}
}

/// A builtin that runs a closure over an array's elements, as
/// `surrealdb_runtime::array` runs it, which a call may run as a loop in
/// place when the closure is written in the call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fused {
	/// Each invocation's value (`array::map`).
	Map,
	/// The elements whose invocation is truthy (`array::filter`).
	Filter,
	/// The positions whose invocation is truthy (`array::filter_index`).
	FilterIndex,
	/// The first element whose invocation is truthy, or NONE (`array::find`).
	Find,
	/// The first position whose invocation is truthy, or NONE
	/// (`array::find_index`, `array::index_of`).
	FindIndex,
	/// Whether some invocation is truthy, stopping at the first
	/// (`array::any`, `array::some`, `array::includes`).
	Any,
	/// Whether every invocation is truthy, stopping at the first that is not
	/// (`array::all`, `array::every`).
	Every,
}

impl Fused {
	fn of(name: &str) -> Option<Self> {
		Some(match name {
			"array::map" => Fused::Map,
			"array::filter" => Fused::Filter,
			"array::filter_index" => Fused::FilterIndex,
			"array::find" => Fused::Find,
			"array::find_index" | "array::index_of" => Fused::FindIndex,
			"array::any" | "array::some" | "array::includes" => Fused::Any,
			"array::all" | "array::every" => Fused::Every,
			_ => return None,
		})
	}

	/// The arguments each invocation is passed: the element, and for
	/// `array::map` its position too.
	fn passes(self) -> usize {
		match self {
			Fused::Map => 2,
			_ => 1,
		}
	}
}

/// The one table `write` writes to, when the statement names it: a table or
/// record id target, `INSERT`'s `INTO`, or the table or record the rows a
/// read yields come from. `None` when the targets are evaluated, name more
/// than one table, or come from anything else — a subquery among them, whose
/// rows name records of whatever tables their values name, not of the table
/// the subquery reads.
pub(super) fn write_table(write: &LogicalWrite) -> Option<surrealdb_strand::TableName> {
	use crate::expr::logical::{LogicalSource, LogicalWriteSource, SourceRef};
	fn named(expr: &LogicalExpr) -> Option<&surrealdb_strand::TableName> {
		match expr {
			LogicalExpr::Table(table) => Some(table),
			LogicalExpr::RecordId(rid) => Some(&rid.table),
			_ => None,
		}
	}
	fn read(op: &LogicalOp, tables: &mut Vec<surrealdb_strand::TableName>) -> bool {
		match op {
			LogicalOp::Source(LogicalSource {
				source,
				..
			}) => match source {
				SourceRef::Table(table) => tables.push(table.clone()),
				SourceRef::RecordId(rid) => tables.push(rid.table.clone()),
				SourceRef::Mock(_) | SourceRef::Param(_) | SourceRef::Dynamic(_) => return false,
			},
			LogicalOp::Values(_) | LogicalOp::Subquery(_) => return false,
			_ => {}
		}
		op.children().iter().all(|child| read(child, tables))
	}
	let mut tables: Vec<surrealdb_strand::TableName> = Vec::new();
	let known = match &write.source {
		LogicalWriteSource::Targets(targets) => {
			// `RELATE` writes to its `through`, the second of its targets.
			let targets = match &write.statement {
				crate::expr::logical::LogicalWriteStatement::Relate(_) => {
					targets.get(1).into_iter().collect::<Vec<_>>()
				}
				_ => targets.iter().collect(),
			};
			targets.into_iter().all(|target| match named(target) {
				Some(table) => {
					tables.push(table.clone());
					true
				}
				None => false,
			})
		}
		LogicalWriteSource::Inserts(inserts) => match inserts.into.as_ref().and_then(named) {
			Some(table) => {
				tables.push(table.clone());
				true
			}
			None => false,
		},
		LogicalWriteSource::Rows(input) => read(input, &mut tables),
	};
	let first = tables.first()?.clone();
	(known && tables.iter().all(|table| *table == first)).then_some(first)
}

/// Whether any instruction of `code` may continue at or before itself: a
/// loop, at whose backward jump the interpreter spends fuel and may yield.
pub(super) fn jumps_back(code: &[Instr]) -> bool {
	code.iter().enumerate().any(|(at, instr)| {
		instr.clone().labels_mut().into_iter().any(|label| *label as usize <= at)
	})
}
