//! The planning entry: statements in, a container past the physical boundary out.
//!
//! [`plan_request`] runs the phases of planning in one order, and each phase's output is the next
//! phase's input:
//!
//! 1. A memo of the catalog ([`PlanningCatalog`]) is created for the compile and loads the objects
//!    the parsed statements name.
//! 2. The binder builds the container and the region-constraint check judges it. A rejection is the
//!    error the request is answered with ([`IrError::Rejected`]); the check runs only for a unit
//!    the request wrote ([`UnitKind::checked`](super::UnitKind::checked)).
//! 3. The fill reads the answers of every table read the container names, and, for an entry that
//!    serves schema statements, the field statements that stand where the catalog they read is the
//!    one they run against are given their checks
//!    ([`SchemaBinding::fill_field_checks`](crate::exec::ir::build::stmt::SchemaBinding::fill_field_checks)).
//! 4. The container is verified at the logical boundary, and leaves the bound phase.
//! 5. The loop optimiser runs with [`PassConfig::PRODUCTION`] and the plan-time folder of the
//!    request, and the row-independent record paths of the filters' predicates are folded for the
//!    access-path decisions ([`fill_record_folds`]).
//! 6. The e-graph plans each statement of the container. A statement it leaves alone keeps the
//!    default conversion's plan.
//! 7. The fill reads the answers of the table reads the passes introduced, and the container is
//!    converted to the physical dialect, with the placement of its pipelines.
//! 8. The container enters the physical phase, the physical rewrites run over it with the placement
//!    the conversion used, which they place the pipelines again with, and the physical
//!    common-subexpression pass merges the equal values inside the regions of physical ops.
//! 9. The container is verified at the physical boundary.
//!
//! A catalog answer the memo does not hold is answered as unknown and the statement is planned
//! again when it is reached ([`IrError::LateBind`]); an error from the planning pipeline is an
//! internal outcome of the request, and a rejection is the request's own error.

use std::borrow::Cow;
use std::ops::ControlFlow;
use std::sync::Arc;

use surrealdb_expr::parser::TopLevelExpr;
use surrealdb_strand::TableName;

use super::analysis::{loop_legality, tokens};
use super::core::facts::{FactCache, LegalitySnapshotSlot, LogicalLegalities};
use super::core::ir::{Ir, IrScope, Walk, WalkOrder};
use super::core::verify::{VerifyAt, VerifyError, verify};
use super::egraph::{EgraphOutcome, PlanStmtCx, plan_statement};
use super::error::IrError;
use super::fold::evaluate::{CapabilityPolicy, VmFolder};
use super::iface::catalog::{BackendCaps, CatalogAnswer, EffectCx, FieldView, PhysCx};
use super::iface::conversion::{LOGICAL_PHASE, PHYSICAL_PHASE, Phase};
use super::iface::frame::FrameTable;
use super::iface::ids::OpId;
use super::logical::ops::LogOp;
use super::op_kind::OpKind;
use super::passes::driver::{NoObserver, PassConfig, run_lo};
use super::passes::phys_cse;
use super::phys::access::TableAnswers;
use super::phys::placement::Placement;
use super::phys::seed_host::PhysSeedHost;
use super::phys::{plan_physical, rewrite};
use super::timers::{compile_catalog_reads, compile_phase};
use super::unit::UnitKind;
use super::{RequestInput, RequestStatement};
use crate::catalog::CompiledSubscription;
use crate::dbs::Capabilities;
use crate::exec::ir::build::field_paths::resolved;
use crate::exec::ir::build::field_view::bind_field_view;
use crate::exec::ir::build::logical::bind_request_noted;
use crate::exec::ir::build::subscription::{SubscriptionInput, bind_subscription};
use crate::exec::ir::cache::answer::SessionValues;
use crate::exec::ir::cache::deps::ValueDeps;
use crate::exec::ir::cache::planning::PlanningCatalog;
use crate::exec::ir::logical::constraints::RegionRejection;
use crate::exec::ir::logical::seam::{BindNotes, node_identity};
use crate::exec::logical::pipelines::{
	Legality, LegalityIndex, statement_legality_with, write_statement_legality,
};
use crate::exec::logical::summaries::CatalogSummaries;
use crate::exec::planner::Planner;
use crate::exec::vm::PlanSession;
use crate::exec::vm::fold_host::HOST_RUNNER;
use crate::exec::vm::ir_catalog::{PlannerLoader, fill, fill_record_folds, top_up};
use crate::exec::vm::payload::Refusal;
use crate::expr::logical::{LogicalExpr, LogicalOp, Schema, lower, lower_expr};
use crate::expr::{Kind, PrefixOperator};

/// What planning reads of the request's environment besides its statements.
pub(crate) struct PlanningInput<'a> {
	/// The planner of the request: its transaction, frozen context, registry, principal,
	/// namespace and database, options, and late-binding and speculation state. Every catalog read
	/// of the compile goes through its transaction.
	pub(crate) planner: &'a Planner<'a>,
	/// The capabilities the request's plan-time folds are judged under.
	pub(crate) capabilities: Arc<Capabilities>,
	/// What the datastore underneath offers. A caller that does not say passes the default,
	/// which offers nothing, so no loop other than a `SELECT` fans out.
	pub(crate) backend: BackendCaps,
	/// The tables the request is planned under the guard of: a gate that judges a write holds the
	/// table it writes while it plans a statement of its own, and that statement, planned as a
	/// unit, must not plan the table's permission again. Each is on the planner's cycle guard for
	/// as long as [`plan_request`] runs, unless it is held already.
	pub(crate) held: Vec<surrealdb_strand::TableName>,
	/// The session in force at the first statement of the input: the options, the namespace and
	/// database, and the `LET`s the statements before it bound. [`plan_request`] never advances
	/// it, so a caller whose input runs past a `USE`, `OPTION` or session `LET` plans each part
	/// under the session that statement leaves.
	///
	/// Its namespace and database are the ones the planner's catalog reads are made in, and the
	/// two must agree ([`IrError::Invariant`] otherwise). A free name plans as read at run time,
	/// so neither the session's `LET`s nor its options change what is planned.
	pub(crate) session: &'a PlanSession,
}

/// What the compile learned besides the container.
pub(crate) struct PlanOutcome {
	/// The plan-time values the decisions of each statement consumed, by statement position.
	pub(crate) deps: Vec<(usize, ValueDeps)>,
	/// The plan-time values the conversion's decisions consumed, for the container as a whole,
	/// and whether the record names every value they were computed from.
	pub(crate) physical_deps: (ValueDeps, bool),
	/// What the e-graph did for each statement of the container, in statement order.
	pub(crate) egraph: Vec<EgraphOutcome>,
	/// The answers every table read of the container was planned from, which the references of
	/// the indexes the container opens are read from.
	pub(crate) answers: Vec<Arc<TableAnswers>>,
	/// The statements the container serves when its lists reach them, as written, by the index
	/// their statement op names.
	pub(crate) served: Vec<Arc<LogicalOp>>,
}

/// A planned container.
pub(crate) struct PlannedRequest {
	/// The container, in the physical phase and verified at the physical boundary.
	pub(crate) ir: Ir,
	pub(crate) facts: FactCache,
	pub(crate) outcome: PlanOutcome,
}

/// Plans the statements of `input` into a container.
///
/// Answers [`IrError::Rejected`] for a request the region constraints reject, carrying the
/// rejection's message; [`IrError::LateBind`] where the planner holds no transaction or database
/// to read the catalog through; [`IrError::Invariant`] where the session and the planner name
/// different databases; and an error of the pipeline otherwise. Reading the catalog is the only
/// effect, and the session is only read.
pub(crate) async fn plan_request(
	input: &RequestInput<'_>,
	planning: &PlanningInput<'_>,
) -> Result<PlannedRequest, IrError> {
	let planner = planning.planner;
	let (txn, ids) = compile_phase!("bind", request_catalog(planning).await?);
	let _held: Vec<_> = match ids {
		Some((ns, db)) => planning
			.held
			.iter()
			.filter_map(|table| planner.cycle_guard.try_enter(ns, db, table.clone()))
			.collect(),
		None => Vec::new(),
	};
	let (memo, lowered) = compile_phase!("bind", prepare(input, planning, txn, ids).await?);
	let registry = planner.function_registry();
	let (folder, frames, statements) = compile_phase!("bind", {
		let folder = VmFolder::new(
			CapabilityPolicy::Request(Arc::clone(&planning.capabilities)),
			&HOST_RUNNER,
		);
		(folder, FrameTable::default(), bound_statements(input, &lowered))
	});
	charge_prefix_chains(planner, &statements)?;
	let bound = RequestInput {
		statements: &statements,
		scope: input.scope.clone(),
		unit: input.unit.clone(),
		serves_late: input.serves_late,
		late_bound: input.late_bound,
	};
	let (ir, rejection, notes) =
		compile_phase!("bind", bind_loaded(&bound, planner, registry, &memo, &frames).await?);
	if let Some(rejection) = rejection {
		return Err(IrError::Rejected(Box::new(rejection.refusal())));
	}
	plan_bound(ir, &bound, notes, planning, memo, &folder).await
}

/// Refuses `statements` when a chain of prefix operators in one of them is deeper than the
/// computation-depth budget leaves: each operator of a chain is one step of evaluation, charged
/// from the depth the planner was entered at. A cast the binder removes as yielding its operand
/// ([`is_identity_cast`]) is no step. Answers [`Refusal::ComputationDepthExceeded`].
///
/// The charge is read from the logical form, before any fold, so a chain of literals is refused
/// as one of values known only when the statement runs is.
fn charge_prefix_chains(
	planner: &Planner<'_>,
	statements: &[RequestStatement<'_>],
) -> Result<(), IrError> {
	let budget = planner.frozen().config.exec.max_computation_depth;
	let entered = planner.current_depth();
	let mut within = |expr: &LogicalExpr| -> ControlFlow<()> {
		let mut depth = entered;
		let mut node = expr;
		while let LogicalExpr::Prefix {
			expr: next,
			..
		} = node
		{
			if !is_identity_cast(node) {
				depth += 1;
				if depth > budget {
					return ControlFlow::Break(());
				}
			}
			node = next;
		}
		ControlFlow::Continue(())
	};
	for statement in statements {
		let RequestStatement::Logical(op) = statement else {
			continue;
		};
		let charged = op.visit_scalars_deep(&mut |scalar| match scalar.visit_deep(&mut within) {
			ControlFlow::Continue(()) => Ok(()),
			ControlFlow::Break(()) => Err(()),
		});
		if charged.is_err() {
			return Err(IrError::Rejected(Box::new(Refusal::ComputationDepthExceeded)));
		}
	}
	Ok(())
}

/// Whether `expr` is a cast that yields its operand unchanged whatever row it is evaluated
/// against: a cast to a kind that names one representation, of an operand checked to have that
/// kind with no field known. The binder removes such a cast, so it is no step of evaluation.
fn is_identity_cast(expr: &LogicalExpr) -> bool {
	let LogicalExpr::Prefix {
		op: PrefixOperator::Cast(kind),
		expr: inner,
	} = expr
	else {
		return false;
	};
	checks_only(kind) && inner.checked_kind(&Schema::open()).as_ref() == Some(kind)
}

/// Whether a cast to `kind` of a value already of that kind yields the value: the kind names one
/// representation, so the cast has nothing to convert.
fn checks_only(kind: &Kind) -> bool {
	match kind {
		Kind::Any | Kind::Named(_) | Kind::Either(_) | Kind::Literal(_) => false,
		Kind::Array(inner, _) | Kind::Set(inner, _) => checks_only(inner),
		Kind::None
		| Kind::Null
		| Kind::Bool
		| Kind::Bytes
		| Kind::Datetime
		| Kind::Date
		| Kind::Time
		| Kind::Timestamp
		| Kind::Interval
		| Kind::Decimal
		| Kind::BigDecimal
		| Kind::Duration
		| Kind::Float
		| Kind::Int
		| Kind::I8
		| Kind::U8
		| Kind::I16
		| Kind::U16
		| Kind::I32
		| Kind::U32
		| Kind::U64
		| Kind::F16
		| Kind::F32
		| Kind::Number
		| Kind::Object
		| Kind::String
		| Kind::Uuid
		| Kind::Regex
		| Kind::Table(_)
		| Kind::Record(_)
		| Kind::Geometry(_)
		| Kind::Function(..)
		| Kind::Range
		| Kind::File(_)
		| Kind::Enum(_) => true,
	}
}

/// The region-constraint rejection of `input`, bound as [`plan_request`] binds it and planned no
/// further; `None` when every region admits what it holds, and for a unit the check does not
/// judge ([`UnitKind::checked`](super::UnitKind::checked)).
///
/// The bind reads the catalog the planner's transaction holds now, and its errors are
/// [`plan_request`]'s.
pub(crate) async fn request_rejection(
	input: &RequestInput<'_>,
	planning: &PlanningInput<'_>,
) -> Result<Option<RegionRejection>, IrError> {
	bind_only(input, planning).await.map(|(_, rejection, _)| rejection)
}

/// The container `input` binds to, as [`plan_request`] binds it, in the logical dialect and
/// planned no further, with the catalog memo the binder read. Answers the region-constraint
/// rejection as an error, as [`plan_request`] does.
#[cfg(all(test, feature = "kv-mem"))]
pub(crate) async fn bind_request_over(
	input: &RequestInput<'_>,
	planning: &PlanningInput<'_>,
) -> Result<(Ir, PlanningCatalog), IrError> {
	match bind_only(input, planning).await? {
		(_, Some(rejection), _) => Err(IrError::Rejected(Box::new(rejection.refusal()))),
		(ir, None, memo) => Ok((ir, memo)),
	}
}

/// The container `input` binds to over the catalog the planner's transaction holds, its
/// region-constraint rejection, and the memo the binder read the catalog through.
async fn bind_only(
	input: &RequestInput<'_>,
	planning: &PlanningInput<'_>,
) -> Result<(Ir, Option<RegionRejection>, PlanningCatalog), IrError> {
	let planner = planning.planner;
	let (txn, ids) = request_catalog(planning).await?;
	let (memo, lowered) = prepare(input, planning, txn, ids).await?;
	let frames = FrameTable::default();
	let statements = bound_statements(input, &lowered);
	let bound = RequestInput {
		statements: &statements,
		scope: input.scope.clone(),
		unit: input.unit.clone(),
		serves_late: input.serves_late,
		late_bound: input.late_bound,
	};
	let registry = planner.function_registry();
	let (ir, rejection, _) = bind_loaded(&bound, planner, registry, &memo, &frames).await?;
	Ok((ir, rejection, memo))
}

/// The transaction the compile reads the catalog through, and the ids of the session's namespace
/// and database where both are defined.
///
/// A session that selects no namespace or database still plans: statements at the root or
/// namespace level (`DEFINE NAMESPACE`, `REMOVE DATABASE`, `INFO FOR ROOT`, …) need neither, and
/// a statement that does need one raises the missing-namespace or missing-database error when it
/// runs, as on the executor.
async fn request_catalog(
	planning: &PlanningInput<'_>,
) -> Result<
	(
		Arc<crate::kvs::Transaction>,
		Option<(crate::catalog::NamespaceId, crate::catalog::DatabaseId)>,
	),
	IrError,
> {
	let planner = planning.planner;
	let session = &planning.session.opt;
	if planner.ns() != session.ns.as_deref() || planner.db() != session.db.as_deref() {
		return Err(IrError::Invariant {
			what: "plan_request: the session and the planner name different databases",
		});
	}
	let Some(txn) = planner.txn().cloned() else {
		return Err(unresolved_database(planner));
	};
	Ok((txn, planner.ns_db_ids().await))
}

/// The memo of the compile, loaded with the objects the parsed statements of `input` name, and the
/// logical form each statement is bound from, where it is not the statement itself.
async fn prepare(
	input: &RequestInput<'_>,
	planning: &PlanningInput<'_>,
	txn: Arc<crate::kvs::Transaction>,
	ids: Option<(crate::catalog::NamespaceId, crate::catalog::DatabaseId)>,
) -> Result<(PlanningCatalog, Vec<Option<LogicalOp>>), IrError> {
	let planner = planning.planner;
	let mut memo =
		PlanningCatalog::new(txn, Arc::clone(planner.frozen()), ids, SessionValues::Fold)
			.with_backend(planning.backend)
			.planning_unknown_reads(
				input.serves_late
					|| input.late_bound
					|| matches!(input.unit, UnitKind::StageBundle { .. }),
			)
			.with_calls_current(planner.calls_current())
			.with_caller(planner.auth.clone())
			.at_stamp(match planner.plan_time_version {
				crate::exec::planner::PlanTimeVersion::Current => None,
				crate::exec::planner::PlanTimeVersion::At(stamp) => Some(stamp),
			})
			.with_request_root(planner.is_request_root());
	if matches!(input.unit, UnitKind::Request | UnitKind::RequestStatement { .. }) {
		memo = memo.with_targets(planner.named_targets()).with_links(planner.links_stages());
	}
	if let (Some(ns), Some(db)) = (planner.ns(), planner.db()) {
		memo = memo.with_database_names(ns, db);
	}
	if ids.is_some() {
		memo.load_database().await.map_err(|e| unreadable(&e))?;
	}
	preload(&memo, input).await?;
	// A statement the parser wrote is bound from the logical form the binder would lower it to,
	// held here, so that the legality the logical plan states of its pipelines is read for it as
	// it is for a statement handed over bound.
	let mut lowered: Vec<Option<LogicalOp>> =
		input.statements.iter().map(|statement| lowered_form(statement)).collect();
	// The `FETCH` and `OMIT` expressions that are not paths are resolved to the paths they name
	// before anything is bound, so the plans the binder and the legality read are the same ones.
	for (statement, logical) in input.statements.iter().zip(lowered.iter_mut()) {
		let op = match (logical.as_ref(), statement) {
			(Some(op), _) => op,
			(None, RequestStatement::Logical(op)) => *op,
			(None, RequestStatement::Ast(_)) => continue,
		};
		if let Cow::Owned(op) = resolved(planner, planning.session, op).await? {
			*logical = Some(op);
		}
	}
	Ok((memo, lowered))
}

/// The statements of `input` as they are bound: each from its logical form in `lowered` where it
/// has one, and as written otherwise.
fn bound_statements<'s>(
	input: &RequestInput<'s>,
	lowered: &'s [Option<LogicalOp>],
) -> Vec<RequestStatement<'s>> {
	input
		.statements
		.iter()
		.zip(lowered)
		.map(|(statement, logical)| match logical {
			Some(op) => RequestStatement::Logical(op),
			None => *statement,
		})
		.collect()
}

/// What a live subscription's notification unit is planned from.
pub(crate) struct SubscriptionRequest<'a> {
	/// The table the subscription watches.
	pub(crate) table: &'a TableName,
	/// The subscription as it is stored.
	pub(crate) compiled: &'a CompiledSubscription,
	/// The names the subscriber's frame binds when the unit runs: the frame's own names and the
	/// parameters the subscription captured. None is answered at plan time.
	pub(crate) shadowed: Vec<String>,
}

/// Plans the notification unit of `request` into a container planned as
/// [`UnitKind::Subscription`], for the subscriber `planning` plans for.
///
/// The unit is bound from the stored subscription and the catalog's answers for its table
/// ([`bind_subscription`]), never from a statement. No free name is answered at plan time: the
/// unit is held for every later write to the table, whoever makes it, so it reads the
/// subscriber's frame, its captured parameters and stored parameters when it runs. The `FETCH`
/// expressions are resolved to the paths they name, and the table's field view is bound for the
/// planner's principal ([`bind_field_view`]); the binder's `checked` is the same planner's
/// [`Planner::reads_are_checked`]. The container is then planned as [`plan_request`] plans one.
///
/// Answers [`IrError::Unsupported`] for a subscription whose clauses or field view have no
/// logical form, which leaves it without a unit, and errors as [`plan_request`] does otherwise.
pub(crate) async fn plan_subscription(
	request: &SubscriptionRequest<'_>,
	planning: &PlanningInput<'_>,
) -> Result<PlannedRequest, IrError> {
	let planner = planning.planner;
	let session = &planning.session.opt;
	if planner.ns() != session.ns.as_deref() || planner.db() != session.db.as_deref() {
		return Err(IrError::Invariant {
			what: "plan_subscription: the session and the planner name different databases",
		});
	}
	let registry = planner.function_registry();
	let Some(txn) = planner.txn().cloned() else {
		return Err(unresolved_database(planner));
	};
	let (Some(ids), Some(ns), Some(db)) = (planner.ns_db_ids().await, planner.ns(), planner.db())
	else {
		return Err(unresolved_database(planner));
	};
	let memo =
		PlanningCatalog::new(txn, Arc::clone(planner.frozen()), Some(ids), SessionValues::Isolated)
			.with_backend(planning.backend)
			.with_database_names(ns, db)
			.with_shadowed(request.shadowed.iter().cloned());
	memo.load_database().await.map_err(|e| unreadable(&e))?;

	let mut fetch = Vec::new();
	for expr in request.compiled.fetch.iter().flatten() {
		let logical = lower_expr(expr.clone()).map_err(|error| IrError::Unsupported {
			reason: format!("a FETCH path has no logical form: {error}"),
		})?;
		fetch.extend(planner.resolve_field_paths(logical).await.map_err(|error| {
			IrError::Unsupported {
				reason: format!("a FETCH path did not resolve: {error}"),
			}
		})?);
	}
	let folder =
		VmFolder::new(CapabilityPolicy::Request(Arc::clone(&planning.capabilities)), &HOST_RUNNER);
	let frames = FrameTable::default();
	let scope = IrScope::Request {
		fragment: None,
	};
	let mut bound = None;
	for _ in 0..=MAX_BIND_ROUNDS {
		let ir = match memo.field_view(request.table) {
			CatalogAnswer::Found(view) => {
				let FieldView::Layers(view) = view.as_ref() else {
					return Err(IrError::Unsupported {
						reason: "the table's field view has no layer form".to_owned(),
					});
				};
				let input = SubscriptionInput {
					scope: &scope,
					table: request.table,
					compiled: request.compiled,
					fetch: &fetch,
					view,
					checked: planner.reads_are_checked(),
				};
				let effects = EffectCx {
					registry,
					catalog: &memo,
					frames: &frames,
				};
				match bind_subscription(&input, &effects, Some(planner.frozen().surface())) {
					Ok(ir) => Some(ir),
					Err(IrError::NotLoaded {
						..
					}) => None,
					Err(error) => return Err(error),
				}
			}
			CatalogAnswer::Absent => {
				return Err(IrError::Unsupported {
					reason: "the subscription's table is not defined".to_owned(),
				});
			}
			CatalogAnswer::NotLoaded => None,
			CatalogAnswer::Unknown => {
				return Err(IrError::LateBind {
					what: "plan_subscription: the field view of the subscription's table",
				});
			}
		};
		let views = memo.take_pending_field_views();
		let loaded = memo.load_pending().await.map_err(|e| unreadable(&e))?;
		for table in &views {
			memo.insert_field_view(table.clone(), bind_field_view(planner, &memo, table).await);
		}
		if ir.is_some() && !loaded && views.is_empty() {
			bound = ir;
			break;
		}
	}
	let Some(ir) = bound else {
		return Err(IrError::LateBind {
			what: "plan_subscription: the catalog objects of the subscription reach one another too deeply",
		});
	};
	let input = RequestInput {
		statements: &[],
		scope,
		unit: UnitKind::Subscription {
			table: request.table.clone(),
		},
		serves_late: false,
		late_bound: false,
	};
	plan_bound(ir, &input, BindNotes::default(), planning, memo, &folder).await
}

/// Plans `ir`, the container the binder built for `input`, past the physical boundary: the fill,
/// the logical verification and passes, the e-graph, the conversion to the physical dialect and
/// its rewrites, and the physical verification, in the order [`plan_request`] documents. `notes`
/// are what the binder noted of the logical plans it bound, and `memo` the catalog the binder read.
async fn plan_bound(
	mut ir: Ir,
	bound: &RequestInput<'_>,
	notes: BindNotes,
	planning: &PlanningInput<'_>,
	mut memo: PlanningCatalog,
	folder: &VmFolder,
) -> Result<PlannedRequest, IrError> {
	let planner = planning.planner;
	let registry = planner.function_registry();
	let (mut frames, mut facts) = compile_phase!("bind", {
		fill(&ir, planner, &mut memo).await?;
		(ir.frames().clone(), FactCache::new())
	});
	if let Some(schema) = planner.frozen().surface().schema_binding {
		compile_phase!("bind", schema.fill_field_checks(&mut ir, &mut facts, planner).await?);
	}
	compile_phase!("bind", state_legalities(planner, bound, &notes, &mut facts).await);
	{
		let effects = EffectCx {
			registry,
			catalog: &memo,
			frames: &frames,
		};
		// A container the request binder built is threaded already. A unit another binder built
		// is threaded where it stands, under the catalog the fill completed.
		if !ir.is_threaded() {
			tokens::thread(&mut ir, &mut facts, &effects)?;
		}
		compile_phase!(
			"verify",
			verify(
				&ir,
				&mut facts,
				VerifyAt::PhaseBoundary(Phase::Logical),
				&LOGICAL_PHASE,
				&effects
			)
			.map_err(IrError::Verify)?
		);
		compile_phase!("logical", {
			super::core::rewriter::Rewriter::new(&mut ir, &mut facts)
				.advance_phase(Phase::Logical)?;
			run_lo(&mut ir, &mut facts, folder, &effects, &PassConfig::PRODUCTION, &mut NoObserver)
				.await?
		});
		compile_phase!(
			"logical",
			fill_record_folds(&ir, &mut facts, &effects, planner, &memo, &bound.unit).await?
		);
	}
	// The legality of each logical loop, which the e-graph's surgery keeps current and the
	// physical boundary's verification judges each placed pipeline against.
	let mut legality = LegalitySnapshotSlot::new();
	compile_phase!(
		"logical",
		loop_legality::snapshot_into(&ir, &mut facts, registry, ir.top(), &mut legality)?
	);
	facts.set_legality_snapshot(legality);
	frames = ir.frames().clone();

	let egraph = compile_phase!("egraph", {
		let loader = PlannerLoader {
			planner,
			memo: &memo,
		};
		let cx = PhysCx {
			effects: EffectCx {
				registry,
				catalog: &memo,
				frames: &frames,
			},
			physical: &memo,
			config: &planner.frozen().config.exec,
		};
		let seed = PhysSeedHost::new(cx, folder).with_loader(&loader);
		let mut cx = PlanStmtCx::for_request(cx.effects);
		cx.seed = Some(&seed);
		let mut outcomes = Vec::new();
		for stmt in statements_of(&ir) {
			let outcome = plan_statement(&mut ir, &mut facts, stmt, &cx).await;
			#[cfg(feature = "compile-timers")]
			outcome.report();
			outcomes.push(outcome);
		}
		outcomes
	});

	compile_phase!("phys", top_up(&ir, planner, &memo).await?);
	// The physical dialect holds no effect token.
	tokens::strip(&mut ir, &mut facts);
	let physical_deps = {
		let cx = PhysCx {
			effects: EffectCx {
				registry,
				catalog: &memo,
				frames: &frames,
			},
			physical: &memo,
			config: &planner.frozen().config.exec,
		};
		let placement =
			Placement::from_config(cx.config).with_read_views(cx.physical.io_caps().read_views);
		let physical = compile_phase!(
			"phys",
			plan_physical(&mut ir, &mut facts, &cx, folder, &placement).await?
		);
		compile_phase!("phys", {
			super::core::rewriter::Rewriter::new(&mut ir, &mut facts)
				.advance_phase(Phase::Physical)?;
			rewrite::run(&mut ir, &mut facts, folder, &cx, &placement).await?
		});
		compile_phase!("cse", phys_cse::run(&mut ir, &mut facts, &cx.effects)?);
		compile_phase!(
			"verify",
			verify(
				&ir,
				&mut facts,
				VerifyAt::PhaseBoundary(Phase::Physical),
				&PHYSICAL_PHASE,
				&cx.effects,
			)
			.map_err(|error: VerifyError| IrError::Verify(error))?
		);
		(physical.deps, physical.deps_complete)
	};
	let (answers, deps) = compile_phase!("bind", {
		let answers = memo.held_answers();
		compile_catalog_reads!(memo.issued_reads());
		(answers, memo.finish())
	});
	Ok(PlannedRequest {
		ir,
		facts,
		outcome: PlanOutcome {
			deps,
			physical_deps,
			egraph,
			answers,
			served: notes.served,
		},
	})
}

/// How many times a request is bound again because the memo did not hold an object the binder
/// asked for. Each round loads at least one object the memo did not hold, so a request whose
/// objects reach one another to this depth is planned again when it is reached.
const MAX_BIND_ROUNDS: u32 = 4;

/// Binds `input` over `memo`, and binds it again over what the binder's lookups made the memo
/// load, until a bind asks for nothing the memo does not hold.
///
/// A statement handed over bound names no object the memo could load before binding, so the first
/// bind may be answered by objects the memo does not hold yet; its lookups leave their names
/// pending, and the next round reads them. A parsed statement has its names loaded before the
/// first bind, so its first round is its last, unless a write names a table whose field view
/// derives a value or cuts a field: the view is bound by the planner ([`bind_field_view`]), which
/// only a later round can read. Answers [`IrError::LateBind`] when the last round still left names
/// pending.
async fn bind_loaded(
	input: &RequestInput<'_>,
	planner: &Planner<'_>,
	registry: &crate::exec::function::FunctionRegistry,
	memo: &PlanningCatalog,
	frames: &FrameTable,
) -> Result<(Ir, Option<RegionRejection>, BindNotes), IrError> {
	for _ in 0..=MAX_BIND_ROUNDS {
		let bound = {
			let effects = EffectCx {
				registry,
				catalog: memo,
				frames,
			};
			bind_request_noted(input, &effects, Some(planner.frozen().surface())).await?
		};
		let views = memo.take_pending_field_views();
		let loaded = memo.load_pending().await.map_err(|e| unreadable(&e))?;
		for table in &views {
			memo.insert_field_view(table.clone(), bind_field_view(planner, memo, table).await);
		}
		if !loaded && views.is_empty() {
			return Ok(bound);
		}
	}
	Err(IrError::LateBind {
		what: "plan_request: the catalog objects of the request reach one another too deeply",
	})
}

fn unreadable(cause: &anyhow::Error) -> IrError {
	IrError::CatalogUnreadable {
		what: "plan_request",
		cause: format!("{cause:#}"),
	}
}

/// Loads the catalog objects every parsed statement of `input` names into `memo`, at the
/// statement's position in its request. A statement handed over bound names its objects through
/// the memo's lookups, which load them as they are asked for.
async fn preload(memo: &PlanningCatalog, input: &RequestInput<'_>) -> Result<(), IrError> {
	let base = input.unit.position();
	for (index, statement) in input.statements.iter().enumerate() {
		if let RequestStatement::Ast(statement) = statement {
			memo.preload(base + index, statement).await.map_err(|e| unreadable(&e))?;
		}
	}
	Ok(())
}

/// The error for a planner that cannot read the catalog: a session that selects no namespace or no
/// database fails as it would at run time, and any other cause plans the statement again when it
/// is reached.
fn unresolved_database(planner: &Planner<'_>) -> IrError {
	if planner.ns().is_none() {
		return IrError::Rejected(Box::new(Refusal::NamespaceEmpty));
	}
	if planner.db().is_none() {
		return IrError::Rejected(Box::new(Refusal::DatabaseEmpty));
	}
	IrError::LateBind {
		what: "plan_request: the database is not resolved at plan time",
	}
}

/// The logical form of an expression statement the parser wrote, which the binder lowers a parsed
/// statement to; `None` for any other statement, and for one that has none.
fn lowered_form(statement: &RequestStatement<'_>) -> Option<LogicalOp> {
	match statement {
		RequestStatement::Ast(TopLevelExpr::Expr(expr)) => lower(expr.clone()).ok(),
		RequestStatement::Ast(_) | RequestStatement::Logical(_) => None,
	}
}

/// States on `facts` what the logical plans of the bound statements say of the pipelines and the
/// statements the binder noted ([`BindNotes`]): the legality of the pipeline that reads the rows
/// below a node, and what the statement as a whole refuses its pipelines.
///
/// A loop the binder did not note, or whose node the logical plan lists no pipeline for, is left
/// out and is placed over [`Legality::not_listed`]: it runs serially. The plans are read as the
/// request was bound from them, so a node the binder bound from a copy keys nothing.
async fn state_legalities(
	planner: &Planner<'_>,
	input: &RequestInput<'_>,
	notes: &BindNotes,
	facts: &mut FactCache,
) {
	let registry = planner.function_registry();
	let mut stated = LogicalLegalities::default();
	for statement in input.statements {
		let RequestStatement::Logical(plan) = statement else {
			continue;
		};
		let identity = node_identity(plan);
		let holders: Vec<OpId> =
			notes.statements.iter().filter(|(_, at)| *at == identity).map(|(op, _)| *op).collect();
		if holders.is_empty() {
			continue;
		}
		let summaries = CatalogSummaries::of(planner, plan).await;
		let index = match planner.binds_rows() {
			true => LegalityIndex::binding(plan, registry, &summaries),
			false => LegalityIndex::with_summaries(plan, registry, &summaries),
		};
		let refusal = if let LogicalOp::Write(_) = plan {
			write_statement_legality(plan, registry, &summaries)
		} else {
			statement_legality_with(plan, registry, &summaries)
		};
		for holder in holders {
			stated.statements.insert(holder, refusal);
		}
		let _ = plan.visit_ops_deep(&mut |node| {
			let at = node_identity(node);
			for (loop_op, _) in notes.loops.iter().filter(|(_, held)| *held == at) {
				if let Some(legality) = legality_below(&index, node) {
					stated.pipelines.insert(*loop_op, legality.clone());
				}
			}
			ControlFlow::<()>::Continue(())
		});
	}
	if !stated.pipelines.is_empty() || !stated.statements.is_empty() {
		facts.set_logical_legalities(stated);
	}
}

/// The legality the logical plan states of the pipeline that reads the rows below `node`: the
/// pipeline `node` is the source of, or the segment above the breaker `node` is.
///
/// A `LIMIT` above a breaker is in the breaker's segment, so the rows below it are the segment's:
/// the walk passes `LIMIT`s and then accepts the segment of the breaker alone. A `LIMIT` over a
/// source is a step of the source's pipeline, whose legality covers the source and the paging
/// together and not the rows that leave the paging, so it states nothing for them.
fn legality_below<'i>(index: &'i LegalityIndex<'_>, node: &LogicalOp) -> Option<&'i Legality> {
	if let Some(legality) = index.of_source(node).or_else(|| index.of_segment(node)) {
		return Some(legality);
	}
	let mut at = node;
	while let LogicalOp::Limit(limit) = at {
		at = &limit.input;
		if let Some(legality) = index.of_segment(at) {
			return Some(legality);
		}
	}
	None
}

/// The `logical.stmt` ops of `ir`, in program order.
fn statements_of(ir: &Ir) -> Vec<OpId> {
	let mut found = Vec::new();
	ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
		if matches!(ir.op(op).map(|data| &data.kind), Some(OpKind::Logical(LogOp::Stmt { .. }))) {
			found.push(op);
		}
		Walk::Continue
	});
	found
}
