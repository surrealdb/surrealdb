//! The compile of a statement: planned by [`plan_request`], lowered natively by `ir_lower`.
//!
//! A bound statement is planned into a container past the physical boundary, and the container is
//! compiled by the [`Compiler`] into a closed program: every unit it needs is planned through the
//! callback here ([`StatementPlanner`]) and lowered before the program runs. Nothing else plans or
//! lowers a statement, and no setting selects another path.
//!
//! The callback answers a region of the requesting container ([`region_unit`]), the body of a
//! closure literal of the requesting container ([`closure_unit`]), the body and the `PERMISSIONS`
//! predicate of a user-defined function, the read unit of a table, the events of a table, the
//! order of a view table's rows, and the recalculation of an aggregated view. A request for a unit
//! whose planning the IR does not build declines the statement with [`NotCovered::not_yet`], which
//! fails it with `PlannerUnsupported` naming the unit.
//!
//! The notification unit of a live subscription is compiled here too ([`compile_subscription`]),
//! planned for its subscriber and closed over the same callback.

use std::collections::BTreeSet;
use std::sync::Arc;

use surrealdb_expr::parser::TopLevelExpr;
use surrealdb_strand::TableName;

use super::ir_compile::{
	Ask, CompileEnv, Compiled, Compiler, PlanSession, Planned, Resolved, SharedIndexes,
	UnitPlanner, UnitScope, closure_unit, region_limit, region_unit,
};
use super::ir_lower::{CascadeKind, UnitRequest};
use super::live::LiveUnit;
use super::program::{Program, UnitFrame, UnitSig};
use super::{Lowered, NotCovered};
use crate::dbs::Options;
use crate::exec::BoxFut;
#[cfg(test)]
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::{Ir, IrScope};
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::error::IrError;
use crate::exec::ir::iface::catalog::BackendCaps;
use crate::exec::ir::iface::ids::RegionId;
use crate::exec::ir::iface::region_kind::RegionKind;
use crate::exec::ir::op_kind::OpKind;
use crate::exec::ir::phys::ops::PhysOp;
use crate::exec::ir::timers::{NestedPlanning, compile_phase, compile_phase_scope};
use crate::exec::ir::{
	PlanningInput, RequestInput, RequestStatement, SubscriptionRequest, UnitKind, plan_request,
	plan_subscription, request_rejection,
};
use crate::exec::planner::{FunctionLookup, PlanTimeVersion, Planner, PrepareError, PreparedBody};
use crate::expr::logical::LogicalOp;
use crate::val::Value;

/// The session `planner` plans under: the options of its speculation where it has them, else the
/// options of its configuration, selecting the namespace and database it reads the catalog in,
/// and the `$session` value of its context.
pub(super) fn session_of(planner: &Planner<'_>) -> PlanSession {
	let mut opt = match &planner.speculation {
		Some(speculating) => (*speculating.options).clone(),
		None => Options::new(&planner.frozen().config.exec),
	};
	opt.set_ns(planner.ns().map(Arc::from));
	opt.set_db(planner.db().map(Arc::from));
	if let Some(auth) = &planner.auth {
		opt = opt.with_auth(Arc::clone(auth));
	}
	PlanSession::new(
		opt,
		planner.frozen().shared_value("session").cloned().unwrap_or_else(|| Arc::new(Value::None)),
	)
}

/// `op`, bound by `planner`, planned as a container of the kind `unit`, with the references of
/// the indexes it opens learnt into `indexes`.
async fn plan(
	planner: &Planner<'_>,
	session: &PlanSession,
	op: &LogicalOp,
	unit: UnitKind,
	held: &[TableName],
	indexes: &SharedIndexes,
) -> Lowered<Planned> {
	plan_serving(planner, session, op, unit, held, indexes, false).await.map(|(planned, _)| planned)
}

/// [`plan`], binding the groups of its lists that follow a possible catalog change as statements
/// the program serves when it reaches them where `serves_late`, and answering those statements as
/// written, by the index their statement op names.
async fn plan_serving(
	planner: &Planner<'_>,
	session: &PlanSession,
	op: &LogicalOp,
	unit: UnitKind,
	held: &[TableName],
	indexes: &SharedIndexes,
	serves_late: bool,
) -> Lowered<(Planned, Vec<Arc<LogicalOp>>)> {
	let statements = [RequestStatement::Logical(op)];
	let input = RequestInput {
		statements: &statements,
		scope: IrScope::Request {
			fragment: None,
		},
		unit,
		serves_late,
		late_bound: planner.is_late_bound(),
	};
	let planning = PlanningInput {
		planner,
		capabilities: planner.frozen().get_capabilities(),
		backend: BackendCaps::default(),
		held: held.to_vec(),
		session,
	};
	let planned = plan_request(&input, &planning).await?;
	compile_phase!("lower", {
		for answers in &planned.outcome.answers {
			indexes.learn(answers, planner.function_registry())?;
		}
	});
	Ok((Planned::new(planned.ir, planned.facts), planned.outcome.served))
}

/// The `Statement` region of the one statement the request container `ir` holds: the region of
/// its `phys.stmt`, reached through the `phys.request` and `phys.txn` containers around it.
///
/// A container that holds no statement, more than one, a statement that stands for a transaction
/// control, or any other op beside the containers is an internal error: the request driver runs
/// those, and the program of a statement is never lowered from them.
fn statement_region(ir: &Ir) -> Lowered<RegionId> {
	fn descend(ir: &Ir, region: RegionId, found: &mut Vec<RegionId>) -> Lowered<()> {
		for op in ops_in(ir, region) {
			let data = ir.op(op).ok_or(IrError::Stale("compile: request op"))?;
			match &data.kind {
				OpKind::Phys(PhysOp::Request {
					..
				})
				| OpKind::Phys(PhysOp::Txn {
					..
				}) => {
					for inner in &data.regions {
						descend(ir, *inner, found)?;
					}
				}
				OpKind::Phys(PhysOp::Stmt {
					control: None,
					..
				}) => found.extend(data.regions.iter().copied().filter(|inner| {
					ir.region(*inner)
						.is_some_and(|held| matches!(held.kind, RegionKind::Statement(_)))
				})),
				OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => {
					return Err(NotCovered::internal(format!(
						"a request container holds {:?} beside its statement",
						data.kind
					)));
				}
			}
		}
		Ok(())
	}
	let mut found = Vec::new();
	descend(ir, ir.top(), &mut found)?;
	match found.as_slice() {
		[region] => Ok(*region),
		_ => Err(NotCovered::internal(format!(
			"a statement compiles from a request container holding {} statements",
			found.len()
		))),
	}
}

/// The body of the one statement `planned`, a request container, holds, as the container whose
/// top region is the statement's region: the program a statement compiles to.
///
/// The request, transaction and statement ops around it are the request driver's, which runs the
/// statement's transaction and session effects; none of them is lowered, so they are freed. A
/// statement's `TIMEOUT` stands in its region and is lowered with the body. The body reads no
/// value defined outside its region but literals, which move into it, since a program at the root
/// is handed no inputs. The ops of the body keep their ids and generations, so the facts planning
/// computed for them stay current; the facts of the freed ops are retired. No lowering reader needs
/// the facts only the planning phases read ([`FactCache::clear_planning_only`]), and placement
/// reads them at the physical boundary, before the body is rerooted, so they are dropped.
fn statement_body(mut planned: Planned) -> Lowered<Planned> {
	if planned.ir.region(planned.ir.top()).map(|held| held.kind) != Some(RegionKind::Request) {
		return Ok(planned);
	}
	let region = statement_region(&planned.ir)?;
	let Ok(freed) = planned.ir.reroot(region)? else {
		return Err(NotCovered::internal(
			"a statement body reads a value its request container defines",
		));
	};
	#[cfg(test)]
	MOVED_LITERALS.with(|moved| moved.set(moved.get() + freed.moved));
	for op in freed.ops {
		planned.facts.retire(op);
	}
	for value in freed.values {
		planned.facts.invalidate_value(value);
	}
	planned.facts.clear_planning_only();
	Ok(planned)
}

#[cfg(test)]
thread_local! {
	/// The literals [`statement_body`] has moved into a body on this thread.
	static MOVED_LITERALS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
	/// The planning facts the lowering of a schema statement has read on this thread.
	pub(super) static LOWERING_FACT_HITS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// [`statement_body`] by copying the statement's region out of the container, which drops the
/// facts: the reference the rerooted body is compared with.
#[cfg(test)]
fn statement_body_by_copy(planned: Planned) -> Lowered<Planned> {
	let ir = &planned.ir;
	if ir.region(ir.top()).map(|held| held.kind) != Some(RegionKind::Request) {
		return Ok(planned);
	}
	let unit = ir.copy_region(statement_region(ir)?)?;
	if !unit.inputs.is_empty() {
		return Err(NotCovered::internal(
			"a statement body reads a value its request container defines",
		));
	}
	Ok(Planned::new(unit.ir, FactCache::new()).over(planned.planned_over))
}

/// `op`, bound by `planner`, planned and compiled as the program an operator runs, with every unit
/// it needs and the statements its blocks serve when they reach them. The program is the body of
/// the statement ([`statement_body`]); the request driver keeps the containers around it.
///
/// A planner that plans late-bound after a block's schema statements
/// ([`Planner::binds_late_after_schema`]) binds the groups of its lists that follow one as
/// statements the program serves: each is planned for the catalog its transaction reads when the
/// list reaches it, served from the compile cache by its text.
pub(super) async fn compile(planner: &Planner<'_>, op: &LogicalOp) -> Lowered<Compiled> {
	match compile_with(planner, op, statement_body).await {
		// A write that is the statement may have linked the bundle of its stages; a compile of
		// one that fails for any reason but a refusal of the statement is planned again with its
		// stages its own ops, so the statement fails as it does inline.
		Err(e) if planner.links_stages() && is_write_statement(op) && e.refusal.is_none() => {
			#[cfg(test)]
			super::units::INLINED_AGAIN.with(|again| again.set(again.get() + 1));
			Box::pin(compile_with(&planner.inlining_stages(), op, statement_body)).await
		}
		compiled => compiled,
	}
}

/// Whether `op` is a write, under its `TIMEOUT` at most: the one statement whose stages a compile
/// links.
fn is_write_statement(op: &LogicalOp) -> bool {
	match op {
		LogicalOp::Write(_) => true,
		LogicalOp::Timeout(timeout) => matches!(*timeout.input, LogicalOp::Write(_)),
		LogicalOp::Source(_)
		| LogicalOp::Values(_)
		| LogicalOp::Match(_)
		| LogicalOp::Subquery(_)
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
		| LogicalOp::Sleep(_) => false,
	}
}

/// [`compile`], with the writes it holds linking their stages as `planner` says, taking the body
/// of the planned statement by `body`.
async fn compile_with(
	planner: &Planner<'_>,
	op: &LogicalOp,
	body: fn(Planned) -> Lowered<Planned>,
) -> Lowered<Compiled> {
	let session = compile_phase!("bind", session_of(planner));
	let indexes = SharedIndexes::new();
	let serves_late = planner.binds_late_after_schema();
	let (planned, served) =
		plan_serving(planner, &session, op, UnitKind::Request, &[], &indexes, serves_late).await?;
	let planned = compile_phase!("lower", body(planned)?);
	compile_planned(planner, &session, &indexes, planned, served, None).await
}

/// The refusal of the region-constraint rejection of `op`, bound by `planner`, as the statement at
/// `position` of its request; `None` when every region admits what it holds.
///
/// The whole statement is bound, the code that follows a schema statement of its lists included,
/// so a construct is judged wherever the statement holds it.
pub(super) async fn rejection(
	planner: &Planner<'_>,
	op: &LogicalOp,
	position: usize,
) -> Lowered<Option<super::payload::Refusal>> {
	let session = session_of(planner);
	let statements = [RequestStatement::Logical(op)];
	let input = RequestInput {
		statements: &statements,
		scope: IrScope::Request {
			fragment: None,
		},
		unit: UnitKind::RequestStatement {
			position,
		},
		serves_late: false,
		late_bound: planner.is_late_bound(),
	};
	let planning = PlanningInput {
		planner,
		capabilities: planner.frozen().get_capabilities(),
		backend: BackendCaps::default(),
		held: Vec::new(),
		session: &session,
	};
	Ok(request_rejection(&input, &planning).await?.map(|rejection| rejection.refusal()))
}

/// The bundle of the stages `write`, a bound write that names no one table, applies to the
/// records of `table`, for the frame `planner` lowers for: the write planned as a
/// [`UnitKind::StageBundle`], which binds it with `table` as the one table it writes, and lowered
/// as one pipeline per site of its body, with every unit those stages need. The action the unit
/// names is the one the statement's form takes a record through first. The stages are lowered on
/// behalf of an actor whose enforcement is `actor` where it differs from the frame's.
pub(super) async fn compile_stage_bundle(
	planner: &Planner<'_>,
	write: &crate::expr::logical::LogicalWrite,
	table: &TableName,
	actor: Option<bool>,
) -> Lowered<Program> {
	use crate::doc::Action;
	use crate::expr::logical::LogicalWriteStatement;
	let action = match &write.statement {
		LogicalWriteStatement::Create(_)
		| LogicalWriteStatement::Insert(_)
		| LogicalWriteStatement::Relate(_) => Action::Create,
		LogicalWriteStatement::Update(_) | LogicalWriteStatement::Upsert(_) => Action::Update,
		LogicalWriteStatement::Delete(_) => Action::Delete,
	};
	let op = LogicalOp::Write(write.clone());
	// The stages reach what the write's own expressions may redefine as the write's statement
	// does: each call runs the definition the transaction holds when it is made.
	let within = super::speculate::catalog_within_writing(planner, &op, table).await;
	let planner = &planner.shadowing(std::iter::empty()).reaching(within);
	let session = session_of(planner);
	let indexes = SharedIndexes::new();
	let unit = UnitKind::StageBundle {
		table: table.clone(),
		action,
	};
	let planned = plan(planner, &session, &op, unit, &[], &indexes).await?;
	let compiled = compile_planned(planner, &session, &indexes, planned, Vec::new(), actor).await?;
	if !compiled.served.is_empty() {
		return Err(NotCovered::not_yet("the stages of a write that serve late-bound statements"));
	}
	Ok(compiled.program)
}

/// The planner of the view frame, in which the order of a view table's rows is planned: `planner`'s
/// principal and database, holding no name a caller binds, with enforcement suppressed.
fn view_planner<'a>(planner: &Planner<'a>) -> Planner<'a> {
	planner.shadowing(std::iter::empty()).with_perms_suppressed(true)
}

/// The statement the write of a view row is bound from: an `UPDATE` with no subject of its own.
/// The binder of a [`UnitKind::ViewRows`] container takes the view table as the table it writes and
/// the orders of a view row as its stages, so nothing of the statement but its being a write
/// reaches the unit.
fn view_row_statement() -> Lowered<LogicalOp> {
	let statement = crate::expr::statements::UpdateStatement {
		what: vec![crate::expr::Expr::Literal(crate::expr::Literal::None)],
		..Default::default()
	};
	crate::expr::logical::lower(crate::expr::Expr::Update(Box::new(statement))).map_err(|e| {
		NotCovered::internal(format!("the write of a view row has no logical form: {e}"))
	})
}

/// The order of the rows of the view table `table`, compiled as a program of its own for the frame
/// `planner` lowers for, planned by the view frame's planner ([`view_planner`]): one pipeline per
/// action a view's maintenance settles (create, update and delete, in that order), each answering
/// whether the order kept the row, with every unit those stages need.
pub(super) async fn compile_view_rows(
	planner: &Planner<'_>,
	table: &TableName,
) -> Lowered<Program> {
	let planner = view_planner(planner);
	let session = session_of(&planner);
	let indexes = SharedIndexes::new();
	let unit = UnitKind::ViewRows {
		table: table.clone(),
	};
	let planned = plan(&planner, &session, &view_row_statement()?, unit, &[], &indexes).await?;
	let compiled = compile_planned(&planner, &session, &indexes, planned, Vec::new(), None).await?;
	if !compiled.served.is_empty() {
		return Err(NotCovered::internal("the order of a view table serves late-bound statements"));
	}
	Ok(compiled.program)
}

/// `planned`, planned by `planner` under `session` with the references of the indexes it opens
/// learnt into `indexes`, compiled as the program an operator runs with every unit it needs, on
/// behalf of an actor whose enforcement is `actor` where it differs from the frame's. `served`
/// are the statements its lists serve when they reach them, as [`plan_serving`] answers them.
async fn compile_planned(
	planner: &Planner<'_>,
	session: &PlanSession,
	indexes: &SharedIndexes,
	planned: Planned,
	served: Vec<Arc<LogicalOp>>,
	actor: Option<bool>,
) -> Lowered<Compiled> {
	// The units the program needs are planned while it lowers, and report their own phases
	let nested = NestedPlanning::default();
	let _lowering = compile_phase_scope!("lower", excluding nested);
	let frozen = planner.frozen();
	let env = CompileEnv {
		registry: planner.function_registry(),
		capabilities: frozen.get_capabilities(),
		frame: super::statement_frame(planner),
		reads_checked: planner.reads_are_checked(),
		depth: planner.current_depth(),
		config: &frozen.config.exec,
		indexes,
		compiled: None,
		session: session.clone(),
		redact_volatile: frozen.redact_volatile_explain_attrs(),
		surface: frozen.surface(),
	};
	let mut compiler = Compiler::over(
		&env,
		StatementPlanner::new(planner, session, indexes)
			.serving(served)
			.timing_units_in(nested.clone()),
	);
	compiler.on_behalf_of(actor);
	crate::kvs::catalog_reads::sealed(false, compiler.program(planned, BTreeSet::new())).await
}

/// Gives `program`, a statement's program `planner` compiled, the seed shape of its rows written
/// shaped ([`shaped_seed`]). Called where a statement is compiled for its request, not for the
/// units a program loads as it runs, whose rows are never written shaped.
#[cfg(feature = "shaped")]
pub(super) async fn seed_statement(planner: &Planner<'_>, program: &mut Program) -> Lowered<()> {
	program.shaped_seed = Box::pin(crate::kvs::catalog_reads::sealed(
		false,
		shaped_seed(planner, program.table.as_ref()),
	))
	.await?;
	Ok(())
}

/// [`seed_statement`] in a build without shaped frames: no seed, since no row is written shaped.
#[cfg(not(feature = "shaped"))]
pub(super) async fn seed_statement(_planner: &Planner<'_>, _program: &mut Program) -> Lowered<()> {
	Ok(())
}

/// The seed shape of the shaped rows of a statement over `table`, from the catalog `planner`
/// plans over and the principal it plans for; `None` without a table or a database, or for a
/// table whose fields pass the limits of a shape (its rows then derive their shapes).
#[cfg(feature = "shaped")]
async fn shaped_seed(
	planner: &Planner<'_>,
	table: Option<&crate::val::TableName>,
) -> Lowered<Option<Arc<surrealdb_types::shaped::Shape>>> {
	use crate::catalog::providers::TableProvider as _;
	let (Some(table), Some(txn)) = (table, planner.txn()) else {
		return Ok(None);
	};
	let Some((ns, db)) = planner.ns_db_ids().await else {
		return Ok(None);
	};
	let fields = txn
		.all_tb_fields(ns, db, table, None)
		.await
		.map_err(|e| NotCovered::internal(format!("the fields of a shaped table: {e}")))?;
	Ok(crate::exec::shaped::seed_shape(&fields, table.as_str(), planner.reads_are_checked()).ok())
}

/// `op`, bound by `planner`, compiled as a program that serves no late-bound statement: the
/// program of a unit the run loads on its own.
pub(super) async fn compile_unit(planner: &Planner<'_>, op: &LogicalOp) -> Lowered<Program> {
	let compiled = compile(planner, op).await?;
	if !compiled.served.is_empty() {
		return Err(NotCovered::not_yet("a unit that serves late-bound statements"));
	}
	Ok(compiled.program)
}

/// The notification unit of the subscription `request`, planned by `planner`, the subscriber's,
/// under `session` ([`plan_subscription`]), and compiled with every unit it needs. The unit is
/// lowered for the frame of the subscriber's principal, which the notification runs it in.
pub(super) async fn compile_subscription(
	planner: &Planner<'_>,
	session: &PlanSession,
	request: &SubscriptionRequest<'_>,
) -> Lowered<LiveUnit> {
	let indexes = SharedIndexes::new();
	let frozen = planner.frozen();
	let planning = PlanningInput {
		planner,
		capabilities: frozen.get_capabilities(),
		backend: BackendCaps::default(),
		held: Vec::new(),
		session,
	};
	let planned = plan_subscription(request, &planning).await?;
	for answers in &planned.outcome.answers {
		indexes.learn(answers, planner.function_registry())?;
	}
	let env = CompileEnv {
		registry: planner.function_registry(),
		capabilities: frozen.get_capabilities(),
		frame: super::statement_frame(planner),
		reads_checked: planner.reads_are_checked(),
		depth: planner.current_depth(),
		config: &frozen.config.exec,
		indexes: &indexes,
		compiled: None,
		session: session.clone(),
		redact_volatile: frozen.redact_volatile_explain_attrs(),
		surface: frozen.surface(),
	};
	let mut compiler = Compiler::over(&env, StatementPlanner::new(planner, session, &indexes));
	let root = Planned::new(planned.ir, planned.facts);
	crate::kvs::catalog_reads::sealed(false, compiler.subscription(root)).await
}

/// The unit of `closure`'s body for values that capture `captured` and whose unit takes the rows
/// `rows`, compiled by `planner` from a statement that makes the literal: a block that binds each
/// captured name as a local of its own, so the literal captures it, then evaluates the literal,
/// whose body takes the invoking row where it reads one. The unit is the body unit the
/// statement's program holds, with the table its ids index.
///
/// A unit whose head names other captures or rows than the values hold is an internal error: a
/// value links only a unit whose head it matches.
pub(super) async fn closure_body(
	planner: &Planner<'_>,
	closure: &Arc<crate::expr::logical::LogicalClosure>,
	captured: &[String],
	rows: super::program::RowBits,
) -> Lowered<super::units::ClosureUnit> {
	use crate::expr::logical::{LogicalEval, LogicalExpr, LogicalLet, LogicalLiteral};
	let made = LogicalOp::Eval(LogicalEval {
		expr: LogicalExpr::Closure(Box::new((**closure).clone())),
	});
	let mut stmts: Vec<LogicalOp> = captured
		.iter()
		.map(|name| {
			LogicalOp::Let(LogicalLet {
				name: name.clone(),
				kind: None,
				value: Box::new(LogicalOp::Eval(LogicalEval {
					expr: LogicalExpr::Literal(LogicalLiteral::None),
				})),
				synthetic: true,
			})
		})
		.collect();
	stmts.push(made);
	let op = LogicalOp::Sequence(crate::expr::logical::LogicalSequence {
		stmts,
	});
	let bound = crate::exec::planner::bind_statement(planner, op).await.map_err(|e| {
		NotCovered::unsupported(format!("the body of a closure value did not bind: {e}"))
	})?;
	let compiled = compile(planner, &bound).await?;
	let digest = super::ClosureDigest::of(closure);
	let table = Arc::clone(&compiled.program.units);
	let unit = table.iter().find(|unit| {
		unit.unit
			.as_ref()
			.and_then(|head| head.closure.as_ref())
			.is_some_and(|head| head.digest == digest && *head.closure == **closure)
	});
	let Some(unit) = unit else {
		return Err(NotCovered::internal("a closure body's statement lowered no unit of its body"));
	};
	let named = unit
		.unit
		.as_ref()
		.and_then(|head| head.closure.as_ref())
		.is_some_and(|head| head.captured.as_slice() == captured && head.rows == rows);
	if !named {
		return Err(NotCovered::internal(
			"a closure body lowered with a head that does not name the captures of its values",
		));
	}
	Ok(super::units::ClosureUnit {
		unit: Arc::clone(unit),
		table,
	})
}

/// The frame a unit requested at `path` from inside a function body runs in, for `caller`, the
/// planner of that body ([`StatementPlanner::caller`]): the frame its [`UnitKind::FunctionBody`]
/// unit is lowered for.
fn requester_frame(caller: &Planner<'_>, path: &[UnitRequest]) -> super::program::UnitFrame {
	let unchecked = path.iter().rev().find_map(|request| {
		if let UnitRequest::FunctionBody {
			unchecked,
			..
		} = request
		{
			Some(*unchecked)
		} else {
			None
		}
	});
	super::program::UnitFrame {
		checked: !unchecked.unwrap_or(false),
		..super::statement_frame(caller)
	}
}

/// `planner` with its principal capped by `limit`, where one is stated.
fn limited_by<'a>(planner: &Planner<'a>, limit: Option<&crate::iam::AuthLimit>) -> Planner<'a> {
	match limit {
		Some(limit) => planner.limited(limit),
		None => planner.shadowing(std::iter::empty()),
	}
}

/// The planning callback of a statement's compile.
struct StatementPlanner<'a> {
	planner: &'a Planner<'a>,
	/// The session the statement is planned under, which a unit's planning starts from.
	session: &'a PlanSession,
	/// The references of the indexes every container of the compile opens.
	indexes: &'a SharedIndexes,
	/// The statements the root container serves when its lists reach them, as written, by the
	/// index their statement op names.
	served: Vec<Arc<LogicalOp>>,
	/// The time spent planning units, for the stretch that lowers them.
	nested: NestedPlanning,
}

impl<'a> StatementPlanner<'a> {
	fn new(planner: &'a Planner<'a>, session: &'a PlanSession, indexes: &'a SharedIndexes) -> Self {
		Self {
			planner,
			session,
			indexes,
			served: Vec::new(),
			nested: NestedPlanning::default(),
		}
	}

	/// This planner adding the time it spends planning each unit to `nested`.
	fn timing_units_in(mut self, nested: NestedPlanning) -> Self {
		self.nested = nested;
		self
	}

	/// This planner answering the root container's late-bound statements from `served`.
	fn serving(mut self, served: Vec<Arc<LogicalOp>>) -> Self {
		self.served = served;
		self
	}

	/// The planner of the code a request at `path` stands in: the statement's, or that of the body
	/// of the innermost function the path enters, bound as [`Self::prepared_body`] binds it for a
	/// call from the code around it, with the principal capped by the `AUTH LIMIT` the call runs
	/// under where it states one. A request made from inside a function's guard answers `NotYet`.
	async fn caller(&self, path: &[UnitRequest]) -> Lowered<Planner<'a>> {
		let mut planner = self.planner.shadowing(std::iter::empty());
		for request in path {
			match request {
				UnitRequest::FunctionGuard {
					..
				} => {
					return Err(NotCovered::not_yet("a unit asked for by the guard of a function"));
				}
				UnitRequest::FunctionBody {
					name,
					unchecked,
					limit,
				} => {
					let calling = limited_by(&planner, limit.as_ref());
					planner = Self::prepared_body(&calling, name, *unchecked).await?.1.planner;
				}
				UnitRequest::Subquery {
					..
				}
				| UnitRequest::TableUnit {
					..
				}
				| UnitRequest::ViewRows {
					..
				}
				| UnitRequest::ViewRecalc {
					..
				}
				| UnitRequest::StageBundle {
					..
				}
				| UnitRequest::LinkedStages {
					..
				}
				| UnitRequest::Events {
					..
				}
				| UnitRequest::Cascade {
					..
				}
				| UnitRequest::ClosureBody {
					..
				}
				| UnitRequest::LoopBody {
					..
				}
				| UnitRequest::Subscription {
					..
				}
				| UnitRequest::ServedStatement {
					..
				} => {}
			}
		}
		Ok(planner)
	}

	/// The definition of `fn::name` and its body, bound by [`Planner::prepare_snapshot`] for a
	/// call from code `caller` plans, at the depth of a statement ([`Planner::unit_root`]), which
	/// caps the principal by the definition's `AUTH LIMIT` and plans one re-entry deeper, waiving
	/// permissions where `unchecked`. A body that does not bind declines the statement.
	async fn prepared_body(
		caller: &Planner<'a>,
		name: &str,
		unchecked: bool,
	) -> Lowered<(Arc<crate::kvs::cache::ds::FunctionSnapshot>, PreparedBody<'a>)> {
		let root = caller.unit_root();
		let root = match unchecked {
			true => root.waiving_permissions(),
			false => root,
		};
		let snapshot = match root.function_snapshot(name).await {
			FunctionLookup::Found(snapshot) => snapshot,
			FunctionLookup::Absent => {
				return Err(NotCovered::internal(format!(
					"a body asked for fn::{name}, which the call cannot enter"
				)));
			}
			FunctionLookup::Unavailable => {
				return Err(NotCovered::unsupported(format!("fn::{name} could not be read")));
			}
		};
		match root.prepare_snapshot(name, &snapshot).await {
			Ok(prepared) => Ok((snapshot, prepared)),
			Err(PrepareError::Bind(e)) => {
				Err(NotCovered::unsupported(format!("the body of fn::{name} did not bind: {e:#}")))
			}
			Err(PrepareError::NoDatabase) => {
				Err(NotCovered::not_yet("the body of a function planned with no database"))
			}
			Err(PrepareError::AuthLimit) => {
				Err(NotCovered::not_yet("the body of a function whose AUTH LIMIT does not parse"))
			}
		}
	}

	/// The body unit of `fn::name` for a call from the code a request at `path` stands in
	/// ([`Self::caller`]): bound by [`Self::prepared_body`], then planned as a
	/// [`UnitKind::FunctionBody`] container under the guard of `held`. The unit takes the
	/// definition's arguments by name over the calling row, and is lowered for the frame its
	/// planner binds for. The units of one compile are keyed by the function's name and the frame
	/// of the code that calls it, so a body called from another body is bound for the principal
	/// that body runs as.
	async fn function_body(
		&self,
		name: &str,
		unchecked: bool,
		limit: Option<&crate::iam::AuthLimit>,
		path: &[UnitRequest],
		held: &[TableName],
	) -> Lowered<Resolved> {
		let caller = limited_by(&self.caller(path).await?, limit);
		let (snapshot, prepared) = Self::prepared_body(&caller, name, unchecked).await?;
		let body = &prepared.planner;
		let planned =
			plan(body, self.session, &prepared.bound, UnitKind::FunctionBody, held, self.indexes)
				.await?;
		Ok(Resolved::Unit {
			planned: Box::new(planned),
			sig: UnitSig {
				params: snapshot
					.definition()
					.args
					.iter()
					.map(|(arg, _)| arg.as_str().into())
					.collect(),
				takes_row: true,
			},
			scope: Some(UnitScope {
				frame: UnitFrame {
					class_digest: body.class_digest(body.auth.as_deref()),
					ns: body.ns().map(str::to_owned),
					db: body.db().map(str::to_owned),
					perms_suppressed: body.perms_suppressed,
					checked: !unchecked,
				},
				reads_checked: body.reads_are_checked(),
				depth: body.current_depth(),
				actor_suppressed: None,
				depth_sites: Some(std::sync::Arc::new(super::depth::ExprDepth::of_body(
					&snapshot.definition().block,
				))),
			}),
		})
	}

	/// The reach of the read unit of `table`, which the compile cache holds, for a caller
	/// whose reads are checked unless `unchecked`; `None` for a table the catalog does not hold.
	async fn table_unit(&self, table: &TableName, unchecked: bool) -> Lowered<Resolved> {
		let planner = match unchecked {
			true => self.planner.waiving_permissions(),
			false => self.planner.unit_root(),
		};
		match super::dispatch::table_read_unit(&planner, table).await {
			Ok(unit) => Ok(Resolved::Table(Some(super::dispatch::reach_of(&unit.program)))),
			Err(refused) => Err(refused),
		}
	}
}

impl StatementPlanner<'_> {
	/// The guard unit of `fn::name`, the predicate its `PERMISSIONS` clause states: planned from
	/// the statement's planner one re-entry deeper, for its principal capped by the definition's
	/// `AUTH LIMIT`, over an isolated view of its context ([`Planner::privileged`]), as a
	/// [`UnitKind::Guard`] container under the guard of `held`. The session's own values are read
	/// when the guard runs, never folded ([`Planner::reading_session_values`]): the program that
	/// holds the unit is served to every principal of its class. The unit takes the row the call is
	/// judged over and no argument, and is lowered for the frame of that capped principal with
	/// reads unchecked, the permission frame the call judges it in.
	///
	/// A request is made only for a definition whose predicate has a logical form and whose
	/// `AUTH LIMIT` parses. A definition that states neither changed after the call was planned,
	/// and the request declines. A guard of a call made from inside a function body unit or another
	/// guard unit declines the statement, as such a body does ([`Self::function_body`]).
	async fn function_guard(
		&self,
		name: &str,
		limit: Option<&crate::iam::AuthLimit>,
		path: &[UnitRequest],
		held: &[TableName],
	) -> Lowered<Resolved> {
		let caller = limited_by(&self.caller(path).await?, limit);
		let snapshot = match caller.function_snapshot(name).await {
			FunctionLookup::Found(snapshot) => snapshot,
			FunctionLookup::Absent => {
				return Err(NotCovered::internal(format!(
					"a guard asked for fn::{name}, which the call cannot enter"
				)));
			}
			FunctionLookup::Unavailable => {
				return Err(NotCovered::unsupported(format!("fn::{name} could not be read")));
			}
		};
		let definition = snapshot.definition();
		let (crate::catalog::Permission::Specific(predicate), Ok(limit)) =
			(&definition.permissions, crate::iam::AuthLimit::try_from(&definition.auth_limit))
		else {
			return Err(NotCovered::internal(format!(
				"a guard asked for fn::{name}, whose definition states no predicate it is judged by"
			)));
		};
		let mut planner = caller.shadowing(std::iter::empty());
		if let Some(auth) = &caller.auth {
			planner = planner.with_auth(Arc::new(auth.new_limited(&limit)));
		}
		let planner =
			planner.with_depth(caller.current_depth() + 1).privileged().reading_session_values();
		let statement = TopLevelExpr::Expr(predicate.clone());
		let statements = [RequestStatement::Ast(&statement)];
		let input = RequestInput {
			statements: &statements,
			scope: IrScope::Request {
				fragment: None,
			},
			unit: UnitKind::Guard,
			serves_late: false,
			late_bound: false,
		};
		let planning = PlanningInput {
			planner: &planner,
			capabilities: planner.frozen().get_capabilities(),
			backend: BackendCaps::default(),
			held: held.to_vec(),
			session: self.session,
		};
		let planned = plan_request(&input, &planning).await?;
		for answers in &planned.outcome.answers {
			self.indexes.learn(answers, self.planner.function_registry())?;
		}
		Ok(Resolved::Unit {
			planned: Box::new(Planned::new(planned.ir, planned.facts)),
			sig: UnitSig {
				params: Vec::new(),
				takes_row: true,
			},
			scope: Some(UnitScope {
				frame: UnitFrame {
					class_digest: planner.class_digest(planner.auth.as_deref()),
					ns: planner.ns().map(str::to_owned),
					db: planner.db().map(str::to_owned),
					perms_suppressed: planner.perms_suppressed,
					checked: false,
				},
				reads_checked: false,
				depth: planner.current_depth(),
				actor_suppressed: None,
				depth_sites: None,
			}),
		})
	}

	/// The events unit of `table` for `action`: a container built from the event definitions the
	/// catalog holds for the table, with no statement ([`UnitKind::Events`]), planned under the
	/// guard of `held`. Each clause is bound as definer code by the IR binder, permissions off
	/// under the event's `AUTH LIMIT`. The unit takes the image an event is about as its row and no
	/// argument, and is lowered for `frame`, the frame of the write that fires the events, which
	/// the run admits the unit against.
	async fn events_unit(
		&self,
		table: &TableName,
		action: crate::doc::Action,
		frame: &UnitFrame,
		held: &[TableName],
	) -> Lowered<Resolved> {
		let input = RequestInput {
			statements: &[],
			scope: IrScope::Request {
				fragment: None,
			},
			unit: UnitKind::Events {
				table: table.clone(),
				action,
			},
			serves_late: false,
			late_bound: false,
		};
		let planning = PlanningInput {
			planner: self.planner,
			capabilities: self.planner.frozen().get_capabilities(),
			backend: BackendCaps::default(),
			held: held.to_vec(),
			session: self.session,
		};
		let planned = plan_request(&input, &planning).await?;
		for answers in &planned.outcome.answers {
			self.indexes.learn(answers, self.planner.function_registry())?;
		}
		Ok(Resolved::Unit {
			planned: Box::new(Planned::new(planned.ir, planned.facts)),
			sig: UnitSig {
				params: Vec::new(),
				takes_row: true,
			},
			scope: Some(UnitScope {
				frame: frame.clone(),
				reads_checked: self.planner.reads_are_checked(),
				depth: self.planner.current_depth(),
				actor_suppressed: None,
				depth_sites: None,
			}),
		})
	}

	/// The recalculation unit of the aggregated view table `view`: the statement
	/// `recalculation_of` builds from the view's stored definition, over its extremes and its group
	/// names, bound by a planner of the view frame (enforcement suppressed) that shadows the group
	/// names, so that they are read from the frame the unit runs in, then planned as a stored
	/// [`UnitKind::Subquery`] container. The unit takes no row and no argument, and is lowered for
	/// the view frame.
	///
	/// A view table the catalog holds as no aggregated view, or one that keeps no extreme, is a
	/// request no lowering makes: its maintenance recalculates nothing.
	async fn view_recalc(&self, view: &TableName, held: &[TableName]) -> Lowered<Resolved> {
		use crate::catalog::aggregation::Recalculation;
		use crate::catalog::providers::TableProvider;
		use crate::exec::operators::write::stages::view::{
			ViewSources, extremes_of, group_names, recalculation_of,
		};

		let (Some(txn), Some((ns, db))) = (self.planner.txn(), self.planner.ns_db_ids().await)
		else {
			return Err(NotCovered::internal(format!(
				"a recalculation of {view} asked for outside a database"
			)));
		};
		let definition = txn.get_tb(ns, db, view, None).await.map_err(|e| {
			NotCovered::internal(format!("the view table {view} could not be read: {e}"))
		})?;
		let Some(crate::catalog::ViewDefinition::Aggregated {
			analysis,
			condition,
			tables,
			..
		}) = definition.as_deref().and_then(|definition| definition.view.as_ref())
		else {
			return Err(NotCovered::internal(format!(
				"a recalculation asked for {view}, which is no aggregated view"
			)));
		};
		let extremes = extremes_of(analysis);
		if extremes.is_empty() {
			return Err(NotCovered::internal(format!(
				"a recalculation asked for {view}, which keeps no extreme"
			)));
		}
		let names = group_names(analysis);
		let recalculations: Vec<Recalculation> = extremes
			.iter()
			.map(|(function, arg)| Recalculation {
				function: function.clone(),
				stat: 0,
				arg: *arg,
			})
			.collect();
		let sources = ViewSources {
			analysis,
			tables,
			condition: condition.as_ref(),
		};
		let op = recalculation_of(&sources, &recalculations, &names).map_err(|_| {
			NotCovered::internal("an aggregated view's recalculation has no logical form")
		})?;
		let planner = self
			.planner
			.shadowing(std::iter::empty())
			.with_perms_suppressed(true)
			.shadowing(names.iter().cloned());
		let bound = crate::exec::planner::bind_statement(&planner, op).await.map_err(|e| {
			NotCovered::internal(format!("an aggregated view's recalculation did not bind: {e}"))
		})?;
		let planned = plan(
			&planner,
			self.session,
			&bound,
			UnitKind::Subquery {
				stored: true,
			},
			held,
			self.indexes,
		)
		.await?;
		Ok(Resolved::Unit {
			planned: Box::new(planned),
			sig: UnitSig {
				params: Vec::new(),
				takes_row: false,
			},
			scope: Some(UnitScope {
				frame: super::statement_frame(&planner),
				reads_checked: planner.reads_are_checked(),
				depth: planner.current_depth(),
				actor_suppressed: None,
				depth_sites: None,
			}),
		})
	}

	/// The order of the rows of the view table `table`: the write of a view row
	/// ([`view_row_statement`]) planned as a [`UnitKind::ViewRows`] container under the guard of
	/// `held`, by a planner of the view frame (enforcement suppressed), whose program runs the view
	/// table's order for each action a view's maintenance settles. The unit takes no row and no
	/// argument: the maintenance hands it the row in the write state it runs the unit over. It is
	/// lowered for the view frame.
	async fn view_rows(&self, table: &TableName, held: &[TableName]) -> Lowered<Resolved> {
		let planner = view_planner(self.planner);
		let planned = plan(
			&planner,
			self.session,
			&view_row_statement()?,
			UnitKind::ViewRows {
				table: table.clone(),
			},
			held,
			self.indexes,
		)
		.await?;
		Ok(Resolved::Unit {
			planned: Box::new(planned),
			sig: UnitSig {
				params: Vec::new(),
				takes_row: false,
			},
			scope: Some(UnitScope {
				frame: super::statement_frame(&planner),
				reads_checked: planner.reads_are_checked(),
				depth: planner.current_depth(),
				actor_suppressed: None,
				depth_sites: None,
			}),
		})
	}

	/// The `WHERE` condition of the `MATERIALIZE INTO` clause of `field` of `table`, as the catalog
	/// holds it.
	async fn materialize_filter(
		&self,
		table: &TableName,
		field: &crate::expr::Idiom,
	) -> Lowered<crate::expr::Expr> {
		use crate::catalog::providers::TableProvider;

		let (Some(txn), Some((ns, db))) = (self.planner.txn(), self.planner.ns_db_ids().await)
		else {
			return Err(NotCovered::internal(format!(
				"the condition of a materialized field of {table} asked for outside a database"
			)));
		};
		let fields = txn.all_tb_fields(ns, db, table, None).await.map_err(|e| {
			NotCovered::internal(format!("the fields of {table} could not be read: {e}"))
		})?;
		fields
			.iter()
			.find(|definition| definition.name == *field)
			.and_then(|definition| definition.reference.as_ref())
			.and_then(|reference| reference.materialize.as_ref())
			.and_then(|materialize| materialize.filter.clone())
			.ok_or_else(|| {
				NotCovered::internal(format!(
					"the materialized field {} of {table} holds no condition",
					surrealdb_types::ToSql::to_sql(field)
				))
			})
	}

	/// The unit of the statement a deletion performs on its own behalf, `kind`: the delete of the
	/// edges joined to the deleted record, or the delete, read, `UNSET` or subtraction of a record
	/// that references it. The statement is built from the engine's own AST
	/// ([`purge`](crate::exec::operators::write::stages::purge)), bound by a planner that shadows
	/// the names the run binds per record (the vertex, the referencing record, the deleted record),
	/// so that they are read from the frame the unit runs in, with enforcement suppressed where
	/// `suppressed`, and planned as a stored [`UnitKind::Subquery`] container whose value is the
	/// statement's: the rows a read answers. The unit takes no row and no argument, and is lowered
	/// for the statement frame of that planner.
	///
	/// The unit's frame holds the statement's permission state; where the actor's own enforcement
	/// (`caller`) differs from it, as the delete of a referencing record by a writer that enforces
	/// permissions does, the unit is lowered on behalf of the actor, so that the cascades the
	/// statement triggers take the actor's state ([`UnitScope::actor_suppressed`]).
	async fn cascade_unit(
		&self,
		kind: &CascadeKind,
		suppressed: bool,
		caller: bool,
		held: &[TableName],
	) -> Lowered<Resolved> {
		use crate::exec::operators::write::stages::purge::{
			self, PURGE_DELETED, PURGE_REFERENCING, PURGE_VERTEX,
		};
		use crate::expr::{Expr, Param};

		let referencing = || Expr::Param(Param::from(PURGE_REFERENCING.to_owned()));
		let filter = match kind {
			CascadeKind::MaterializeFilter {
				table,
				field,
			} => Some(self.materialize_filter(table, field).await?),
			CascadeKind::Edges
			| CascadeKind::Delete {
				..
			}
			| CascadeKind::Read {
				..
			}
			| CascadeKind::Unset {
				..
			}
			| CascadeKind::Subtract {
				..
			}
			| CascadeKind::MaterializeWrite {
				..
			} => None,
		};
		let (op, names) = match kind {
			CascadeKind::Edges => (purge::purge_edges_statement(), vec![PURGE_VERTEX]),
			CascadeKind::Delete {
				..
			} => (purge::delete_of(referencing()), vec![PURGE_REFERENCING]),
			CascadeKind::Read {
				..
			} => match purge::select_all_of(referencing()) {
				LogicalOp::Subquery(read) => (Ok(*read.input), vec![PURGE_REFERENCING]),
				read @ (LogicalOp::Source(_)
				| LogicalOp::Values(_)
				| LogicalOp::Match(_)
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
				| LogicalOp::Sleep(_)) => (Ok(read), vec![PURGE_REFERENCING]),
			},
			CascadeKind::Unset {
				field,
				..
			} => (
				purge::update_of(referencing(), purge::unset_data(field)),
				vec![PURGE_REFERENCING, PURGE_DELETED],
			),
			CascadeKind::Subtract {
				field,
				..
			} => (
				purge::update_of(referencing(), purge::subtract_data(field)),
				vec![PURGE_REFERENCING, PURGE_DELETED],
			),
			CascadeKind::MaterializeWrite {
				dest,
				..
			} => (
				crate::exec::materialize::write_statement(dest),
				vec![
					crate::exec::materialize::PARENT,
					crate::exec::materialize::CHILD,
					crate::exec::materialize::ADD,
				],
			),
			CascadeKind::MaterializeFilter {
				..
			} => (
				match &filter {
					Some(filter) => crate::exec::materialize::filter_statement(filter),
					None => {
						return Err(NotCovered::internal(
							"a materialize filter statement with no condition",
						));
					}
				},
				vec![crate::exec::materialize::IMAGE],
			),
		};
		let op = op.map_err(|_| NotCovered::internal("a cascade statement has no logical form"))?;
		let planner = self
			.planner
			.shadowing(std::iter::empty())
			.with_perms_suppressed(suppressed)
			.shadowing(names.iter().map(|name| (*name).to_owned()));
		let bound = crate::exec::planner::bind_statement(&planner, op)
			.await
			.map_err(|e| NotCovered::internal(format!("a cascade statement did not bind: {e}")))?;
		let planned = plan(
			&planner,
			self.session,
			&bound,
			UnitKind::Subquery {
				stored: true,
			},
			held,
			self.indexes,
		)
		.await?;
		Ok(Resolved::Unit {
			planned: Box::new(planned),
			sig: UnitSig {
				params: Vec::new(),
				takes_row: false,
			},
			scope: Some(UnitScope {
				frame: super::statement_frame(&planner),
				reads_checked: planner.reads_are_checked(),
				depth: planner.current_depth(),
				actor_suppressed: (caller != suppressed).then_some(caller),
				depth_sites: None,
			}),
		})
	}
}

impl StatementPlanner<'_> {
	/// The bundle of the stages `set`, a write whose stages are linked, applies to the records of
	/// `table`, for the frame the statement is planned for, on behalf of an actor whose
	/// enforcement is `actor` where it differs from the frame's: served from the compile cache
	/// under the table's catalog version and the frame, as a write that may reach any table loads
	/// it ([`super::fault::serve_stage_bundle`]), or compiled now ([`compile_stage_bundle`]) and
	/// kept. The bundle reads the request's variables its write reads when it runs, so one
	/// compilation serves every value of them. A table with no catalog version, a speculating
	/// transaction, or a node that keeps no unit compiles the bundle for this statement alone.
	///
	/// The compile is recorded with the statement's own catalog reads, and a bundle served from the
	/// cache notes what it depends on into them as its dependencies are checked
	/// ([`crate::exec::catalog_deps::CatalogDeps::hold`]), so a statement kept with the bundle is
	/// compiled again whenever the bundle would be.
	async fn linked_stages(
		&self,
		table: &TableName,
		set: &Arc<super::units::StageSetUnit>,
		actor: Option<bool>,
	) -> Lowered<Resolved> {
		use super::compile_cache::{BuildId, ObjectVersion, UnitKey, UnitObject};
		let planner = self.planner;
		let (Some(txn), Some((ns, db)), Some(ns_name), Some(db_name)) =
			(planner.txn().cloned(), planner.ns_db_ids().await, planner.ns(), planner.db())
		else {
			return Err(NotCovered::internal("the stages of a write linked outside a database"));
		};
		let version = txn.catalog_version(ns, db, table).await.map_err(|e| {
			NotCovered::internal(format!("the catalog version of {table} could not be read: {e}"))
		})?;
		let frame = super::statement_frame(planner);
		// Every name the stage set reads is shadowed, so the bundle reads each when its stages run
		// and folds no request's value: one key is one compilation whichever request compiles it.
		let mut names: BTreeSet<String> = set.read_names();
		names.insert(super::units::LINKED_SUBJECT.to_owned());
		let compile = || async {
			let bundler = Planner::with_txn(
				planner.frozen(),
				planner.function_registry(),
				Arc::clone(&txn),
				Some(ns_name.to_owned()),
				Some(db_name.to_owned()),
			);
			let bundler = match &planner.auth {
				Some(auth) => bundler.with_auth(Arc::clone(auth)),
				None => bundler,
			}
			.with_perms_suppressed(frame.perms_suppressed)
			.reading_session_values()
			.shadowing(names.iter().cloned());
			let recording = (!txn.speculating()).then(|| txn.record_catalog_reads());
			let program =
				super::out_of_line(|| compile_stage_bundle(&bundler, &set.write, table, actor))
					.await;
			let reads = recording.map(|recording| recording.finish());
			let program = program?;
			// The write's sites index the bundle's pipelines.
			if program.pipelines.len() != set.sites.len() {
				return Err(NotCovered::internal(
					"the linked stages of a table that do not number the sites of their write",
				));
			}
			let deps = match reads {
				Some(reads) => {
					match crate::exec::catalog_deps::CatalogDeps::resolve(&txn, &reads).await {
						Some(deps) => super::units::ContextDeps::resolve(
							&txn,
							&reads,
							bundler.frozen(),
							Default::default(),
						)
						.await
						.map(|context| {
							(deps, context.without(crate::exec::planner::UNIT_RUNTIME_PARAMS))
						}),
						None => None,
					}
				}
				None => None,
			};
			Ok::<_, NotCovered>((Arc::new(program), deps))
		};
		let cache = planner.frozen().compiled_units().filter(|_| !txn.speculating());
		let bundle = match (cache, version) {
			(Some(cache), Some(version)) => {
				let key = UnitKey {
					ns: ns_name.to_owned(),
					db: db_name.to_owned(),
					ids: (ns, db),
					object: UnitObject::StageBundle {
						set: Arc::clone(set),
						table: table.clone(),
						generic: false,
						actor,
					},
					version: ObjectVersion::Stamped(version),
					build: BuildId::current(planner.frozen().get_capabilities()),
				};
				let compiled = std::sync::atomic::AtomicBool::new(false);
				let bundle = cache
					.stage_bundles()
					.get_or_compile(&key, &frame, &txn, planner.frozen(), || {
						compiled.store(true, std::sync::atomic::Ordering::Relaxed);
						compile()
					})
					.await?;
				note_linked(compiled.load(std::sync::atomic::Ordering::Relaxed));
				bundle
			}
			_ => {
				note_linked(true);
				compile().await?.0
			}
		};
		Ok(Resolved::Linked(bundle))
	}
}

/// Count one bundle of linked stages this thread compiled, or took from the compile cache, for
/// tests that count them ([`super::units::LINKED`]).
fn note_linked(compiled: bool) {
	#[cfg(test)]
	super::units::LINKED.with(|units| {
		let (c, l) = units.get();
		units.set(if compiled {
			(c + 1, l)
		} else {
			(c, l + 1)
		});
	});
	#[cfg(not(test))]
	let _ = compiled;
}

impl UnitPlanner for StatementPlanner<'_> {
	fn plan<'a>(&'a mut self, ask: Ask<'a>) -> BoxFut<'a, Result<Resolved, NotCovered>> {
		Box::pin(async move {
			if let Some(mut unit) = region_unit(&ask)? {
				if let Some(limit) = region_limit(&ask)? {
					// The statement's planner is the requester's where every request on the path
					// planned its container with that planner: a region or closure copied out of
					// its requester, or the events of a write. Otherwise the requester is the body
					// of the innermost function the path enters ([`Self::caller`]).
					let inherits = ask.path.iter().all(|request| {
						matches!(
							request,
							UnitRequest::Subquery { .. }
								| UnitRequest::LoopBody { .. }
								| UnitRequest::ClosureBody { .. }
								| UnitRequest::Events { .. }
						)
					});
					// The frame of the requester, from the events of a write where the path holds
					// them, with the principal capped by the limit.
					let (planner, mut frame) = match inherits {
						true => {
							let mut frame = super::statement_frame(self.planner);
							for request in ask.path {
								if let UnitRequest::Events {
									frame: events,
									..
								} = request
								{
									frame = events.clone();
								}
							}
							(self.planner.limited(&limit), frame)
						}
						false => {
							let caller = self.caller(ask.path).await?;
							let frame = requester_frame(&caller, ask.path);
							(caller.limited(&limit), frame)
						}
					};
					frame.class_digest = planner.class_digest(planner.auth.as_deref());
					if let Resolved::Unit {
						scope,
						..
					} = &mut unit
					{
						*scope = Some(UnitScope {
							frame,
							reads_checked: planner.reads_are_checked(),
							depth: planner.current_depth(),
							actor_suppressed: None,
							depth_sites: None,
						});
					}
				}
				return Ok(unit);
			}
			// A closure body names the stamps of the transaction it is planned in, which a run
			// compares before it takes the unit for a value.
			let stamps = self.planner.txn().map_or(0, |txn| txn.catalog_stamps());
			let version = match self.planner.plan_time_version {
				PlanTimeVersion::At(stamp) => Some(stamp),
				PlanTimeVersion::Current => None,
			};
			if let Some(unit) = closure_unit(&ask, stamps, version)? {
				return Ok(unit);
			}
			// A unit is planned while its planning is not open on the planner's cycle guard. One
			// asked for again from a compile its own planning started has no slot to link to (a
			// recursion within one compile links to the unit's slot and never plans it twice), so
			// the cycle is refused with the computation-depth error rather than planned without
			// end.
			let key = crate::exec::planner::UnitKey::new(
				ask.request.clone(),
				super::statement_frame(self.planner),
			);
			let Some(_open) = self.planner.cycle_guard.try_enter_unit(key) else {
				return Err(NotCovered::refused(super::payload::Refusal::ComputationDepthExceeded));
			};
			let _planning = self.nested.timing();
			match ask.request {
				UnitRequest::FunctionBody {
					name,
					unchecked,
					limit,
				} => self.function_body(name, *unchecked, limit.as_ref(), ask.path, ask.held).await,
				UnitRequest::TableUnit {
					table,
					unchecked,
				} => self.table_unit(table, *unchecked).await,
				UnitRequest::FunctionGuard {
					name,
					limit,
				} => self.function_guard(name, limit.as_ref(), ask.path, ask.held).await,
				UnitRequest::ViewRecalc {
					view,
				} => self.view_recalc(view, ask.held).await,
				UnitRequest::ViewRows {
					table,
				} => self.view_rows(table, ask.held).await,
				UnitRequest::StageBundle {
					..
				} => Err(NotCovered::not_yet("the stage bundle of a write")),
				UnitRequest::LinkedStages {
					table,
					set,
					actor,
				} => self.linked_stages(table, set, *actor).await,
				UnitRequest::Events {
					table,
					action,
					frame,
				} => self.events_unit(table, *action, frame, ask.held).await,
				UnitRequest::Cascade {
					kind,
					suppressed,
					caller,
				} => self.cascade_unit(kind, *suppressed, *caller, ask.held).await,
				// A subscription's unit is compiled for its subscriber and held by the node
				// ([`compile_subscription`]); no program holds one in its unit table.
				UnitRequest::Subscription {
					..
				} => {
					Err(NotCovered::internal("a program asked for the unit of a live subscription"))
				}
				UnitRequest::ServedStatement {
					written,
					..
				} => self.served.get(*written as usize).cloned().map(Resolved::Served).ok_or_else(
					|| NotCovered::internal("a late-bound statement the container did not bind"),
				),
				UnitRequest::Subquery {
					..
				}
				| UnitRequest::LoopBody {
					..
				}
				| UnitRequest::ClosureBody {
					..
				} => {
					Err(NotCovered::internal("a region request the region planners did not answer"))
				}
			}
		})
	}
}

#[cfg(all(test, feature = "kv-mem"))]
mod tests {
	use surrealdb_strand::TableName;

	use super::super::ir_lower::UnitRequest;
	use super::super::plain::ValueData;
	use super::super::program::{Program, UnitFrame};
	use super::super::tests_morsel::{bind, planner};
	use super::{
		LOWERING_FACT_HITS, MOVED_LITERALS, compile_with, statement_body, statement_body_by_copy,
	};
	use crate::exec::operators::test_util::TestDb;
	use crate::exec::planner::{CycleGuard, UnitKey};
	use crate::val::{Uuid, Value};

	fn frame(checked: bool) -> UnitFrame {
		UnitFrame {
			class_digest: 0,
			ns: Some("ns".to_owned()),
			db: Some("db".to_owned()),
			perms_suppressed: false,
			checked,
		}
	}

	fn table_unit(table: &str) -> UnitRequest {
		UnitRequest::TableUnit {
			table: TableName::from(table),
			unchecked: false,
		}
	}

	#[test]
	fn a_unit_open_on_the_guard_is_refused_until_its_planning_closes() {
		let guard = CycleGuard::default();
		let open = guard.try_enter_unit(UnitKey::new(table_unit("ring"), frame(true)));
		assert!(open.is_some(), "a unit no planning holds is entered");
		assert!(
			guard.try_enter_unit(UnitKey::new(table_unit("ring"), frame(true))).is_none(),
			"the same unit for the same frame is a cycle"
		);
		assert!(
			guard.try_enter_unit(UnitKey::new(table_unit("ring"), frame(false))).is_some(),
			"the same unit for another frame is another unit"
		);
		assert!(
			guard.try_enter_unit(UnitKey::new(table_unit("other"), frame(true))).is_some(),
			"another unit is entered"
		);
		assert!(guard.is_idle(), "an open unit changes nothing a planner compiles");
		drop(open);
		assert!(
			guard.try_enter_unit(UnitKey::new(table_unit("ring"), frame(true))).is_some(),
			"a closed unit is entered again"
		);
	}

	/// `value` with every uuid it holds replaced by the nil uuid.
	fn zero_uuids(value: &mut Value) {
		match value {
			Value::Uuid(uuid) => *uuid = Uuid::default(),
			Value::Object(object) => object.values_mut().for_each(zero_uuids),
			Value::Array(array) => array.iter_mut().for_each(zero_uuids),
			Value::None
			| Value::Null
			| Value::Bool(_)
			| Value::Number(_)
			| Value::String(_)
			| Value::Duration(_)
			| Value::Datetime(_)
			| Value::Date(_)
			| Value::Time(_)
			| Value::Timestamp(_)
			| Value::Interval(_)
			| Value::Set(_)
			| Value::Geometry(_)
			| Value::Bytes(_)
			| Value::Table(_)
			| Value::RecordId(_)
			| Value::File(_)
			| Value::Regex(_)
			| Value::Range(_)
			| Value::Closure(_) => {}
		}
	}

	/// `program` with the uuids of its constants zeroed. A schema statement's table template
	/// draws fresh cache stamps at every compile, so two compiles of one statement differ by them
	/// alone; no other part of a program holds a drawn uuid.
	fn zeroed_stamps(program: &Program) -> Program {
		let mut zeroed = program.clone();
		let mut consts = (*zeroed.consts).clone();
		consts.iter_mut().for_each(zero_uuids);
		zeroed.consts = ValueData::new(consts, "a test program's constants")
			.expect("constants that held no closure hold none after zeroing");
		zeroed
	}

	/// The statement body taken by rerooting the request container, which keeps the planning
	/// facts, lowers to the program the body copied out of the container lowers to.
	#[tokio::test]
	async fn statement_body_reroot_matches_copy() {
		let db = TestDb::new(
			"DEFINE TABLE t SCHEMALESS; DEFINE TABLE u SCHEMALESS; DEFINE TABLE pk SCHEMALESS;
			 DEFINE TABLE prm SCHEMALESS PERMISSIONS FOR select, update WHERE v > 0;
			 DEFINE EVENT ev ON prm WHEN true THEN (CREATE u SET w = 1);
			 CREATE t:1 SET v = 1; CREATE u:1 SET w = 2; CREATE pk:1 SET a = 1;",
		)
		.await;
		let ctx = db.exec_ctx().await;
		let corpus = [
			"RETURN 1",
			"SELECT * FROM []",
			"SELECT * FROM $id",
			"SELECT * FROM pk WHERE id = $id",
			"UPDATE $id SET a = 1",
			"SELECT * FROM t TIMEOUT 1s",
			"RETURN $x",
			"{ LET $x = 1; RETURN $x }",
			"SELECT *, (SELECT * FROM u) AS s FROM t",
			"FOR $i IN [1, 2] { CREATE t SET v = $i }",
			"DEFINE FIELD f ON t VALUE (|$x| $x)(1)",
			"DEFINE FIELD f ON t DEFAULT (|$x| $x)(1)",
			"DEFINE FIELD f ON t DEFAULT $x",
			"DEFINE TABLE $x",
			"DEFINE TABLE w PERMISSIONS FOR select WHERE v > 1",
			"DEFINE PARAM $p VALUE (|$x| $x)(1)",
			"DEFINE PARAM $p VALUE string::uppercase('a')",
			"DEFINE PARAM $p VALUE $x",
			"DEFINE TABLE w COMMENT string::uppercase('a')",
			"SELECT * FROM prm",
			"UPDATE prm SET v = 2",
			"{ LET $f = |$x| $x; RETURN $f(1) }",
		];
		let (mut moved, mut reroot_hits, mut copy_hits) = (0, 0, 0);
		for sql in corpus {
			let op = bind(&ctx, sql).await;
			let (before, hits_before) = (
				MOVED_LITERALS.with(|held| held.get()),
				LOWERING_FACT_HITS.with(|held| held.get()),
			);
			let rerooted = Box::pin(compile_with(&planner(&ctx), &op, statement_body)).await;
			moved += MOVED_LITERALS.with(|held| held.get()) - before;
			let hits_mid = LOWERING_FACT_HITS.with(|held| held.get());
			reroot_hits += hits_mid - hits_before;
			let copied = Box::pin(compile_with(&planner(&ctx), &op, statement_body_by_copy)).await;
			copy_hits += LOWERING_FACT_HITS.with(|held| held.get()) - hits_mid;
			match (rerooted, copied) {
				(Ok(rerooted), Ok(copied)) => {
					if rerooted.program != copied.program {
						assert_eq!(
							zeroed_stamps(&rerooted.program),
							zeroed_stamps(&copied.program),
							"the program of `{sql}`, beyond its cache stamps"
						);
					}
					assert_eq!(rerooted.served.len(), copied.served.len(), "`{sql}` serves");
				}
				(rerooted, copied) => {
					assert_eq!(
						rerooted.err().map(|error| format!("{error:?}")),
						copied.err().map(|error| format!("{error:?}")),
						"the refusal of `{sql}`"
					);
				}
			}
		}
		assert_eq!(moved, 0, "a bound statement reads no literal its region does not define");
		eprintln!("fact hits: reroot {reroot_hits}, copy {copy_hits}");
		assert!(reroot_hits > 0, "the kept planning facts are read while lowering");
		assert_eq!(copy_hits, 0, "a copied body holds no planning facts");
	}
}
