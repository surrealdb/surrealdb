//! The observer stages lowered to instructions: the table's events and the
//! record's change.
//!
//! A table's events are lowered from the definitions the catalog holds when the
//! program is lowered. An event body runs with permissions off and under the
//! definer's authority rather than as the writer, so each event's clauses are
//! bound and lowered through a planner of that event's own: the writer's, with
//! permissions suppressed, the principal capped by the event's `AUTH LIMIT`,
//! and the event's parameters bound per record rather than folded.
//!
//! Each clause is a host pipeline over the image the event is about, lowered
//! from the clause's logical form: a clause that is a statement is a program
//! of the VM's own, lowered with the event's planner, that the pipeline runs as
//! a subquery, its `RETURN` the clause's value; any other clause is the
//! pipeline's own instructions. A clause with no logical form, or a statement
//! that does not lower, fails the lowering of the write that fires the event.
//!
//! A table's events are a unit of the statement's arena, found again by what
//! they are lowered with, so events that write back to their own table close
//! on the unit being lowered and run by id.

use std::sync::Arc;

use super::{Lowered, Lowering, NotCovered, lend};
use crate::catalog::EventDefinition;
use crate::exec::planner::{Planner, PlannerFrameKey};
use crate::exec::vm::depth::ExprDepth;
use crate::exec::vm::plan::Projection;
use crate::exec::vm::program::{Instr, ROW, SubquerySpec, UnitHead, UnitId, UnitSig, WriteInstr};
use crate::exec::vm::write::{ClauseDepth, EventClauses, LoweredEvents, ObserveInstr};
use crate::expr::logical::LogicalExpr;
use crate::val::TableName;

/// The parameters an event's frame binds for each record, which its clauses
/// read per record and binding must never fold.
const EVENT_PARAMS: [&str; 5] = ["before", "after", "event", "value", "input"];

/// The most lowerings of one table's events for one action a chain of event
/// frames holds open, on every arena beneath the statement, before the
/// lowering refuses it. A chain that settles holds one open for each distinct
/// frame it passes through (the writer's, the event's, the event's beneath a
/// delete's purge); one whose frames never repeat opens another at every hop.
pub(in crate::exec::vm) const OPEN_EVENT_UNITS: usize = 4;

/// How many lowerings of tables' events one arena holds open, one inside
/// another, before [`Lowering::events_unit`] leaves the next to the outermost
/// ([`EventsQueue`]).
const NESTED_EVENT_UNITS: usize = 4;

/// The lowerings of tables' events an arena holds open, and the units whose
/// events are linked and left for its outermost lowering of events to lower.
#[derive(Default)]
pub(super) struct EventsQueue<'ctx> {
	/// How many lowerings of events are open in the arena, one inside another.
	open: usize,
	/// The events whose lowerings are open, outermost first.
	path: Vec<EventsOpen>,
	pending: Vec<PendingEvents<'ctx>>,
}

impl EventsQueue<'_> {
	/// Drop the units left to lower from the `len`th unit of the arena on.
	pub(super) fn rewind(&mut self, len: usize) {
		self.pending.retain(|pending| (pending.unit.0 as usize) < len);
	}
}

/// One table's events for one action, as a lowering of them is open.
#[derive(Clone)]
pub(super) struct EventsOpen {
	ns: crate::catalog::NamespaceId,
	db: crate::catalog::DatabaseId,
	table: TableName,
	action: crate::doc::Action,
}

impl EventsOpen {
	/// Count the lowering open on `planner`'s cycle guard; refused once
	/// [`OPEN_EVENT_UNITS`] lowerings of the same events are.
	fn enter(&self, planner: &Planner<'_>) -> Lowered<crate::exec::planner::EventsEntry> {
		planner
			.cycle_guard
			.try_enter_events(self.ns, self.db, self.table.clone(), self.action, OPEN_EVENT_UNITS)
			.ok_or_else(|| {
				NotCovered::new(
					"a table's events reach the table again in frames that never settle",
				)
			})
	}
}

/// A unit of events linked into the arena whose clauses are left to lower,
/// with what the write that reached it lowered with.
struct PendingEvents<'ctx> {
	unit: UnitId,
	planner: Planner<'ctx>,
	open: EventsOpen,
	events: Arc<[EventDefinition]>,
	catalog: Arc<crate::exec::vm::plan_catalog::PlanCatalog>,
	ahead: bool,
	versioned: bool,
	/// The events whose lowerings were open around it, outermost first.
	path: Vec<EventsOpen>,
}

/// What an observer unit runs: a table's events for an action, or a view
/// table's order for the rows its views write.
#[derive(Clone, PartialEq)]
pub(super) enum Observed {
	Events(TableName, crate::doc::Action),
	ViewRows(TableName),
}

/// An observer unit linked into the arena being lowered: what it runs, what
/// it is lowered with, and the unit.
pub(super) struct ObserverLink {
	of: Observed,
	frame: PlannerFrameKey,
	ahead: bool,
	versioned: bool,
	unit: UnitId,
}

impl ObserverLink {
	/// Whether the unit the link names is among the first `len` of the arena.
	pub(super) fn names_before(&self, len: usize) -> bool {
		(self.unit.0 as usize) < len
	}
}

/// What a clause pipeline computes over the value it is about.
pub(super) enum ClauseCode<'e> {
	/// The value of an expression.
	Expr(&'e LogicalExpr),
	/// A `SELECT`-style field list applied to the value.
	Projection(&'e Projection),
}

impl<'ctx> Lowering<'_, 'ctx> {
	/// Emit `ProcessChangefeeds`: the record's change buffered for the change
	/// feed and the live-query capture.
	pub(super) fn changefeed_stage(&mut self) {
		self.emit(Instr::Write(WriteInstr::Observe(ObserveInstr::RecordChange)));
	}

	/// Emit `ProcessTableLives` for `action`: the table's live subscriptions
	/// notified of the record.
	///
	/// The table's subscriptions' units are compiled when the write's plan
	/// reads its catalog ([`crate::exec::vm::plan_catalog`]); the
	/// instruction only looks them up.
	pub(super) fn lives_stage(&mut self, action: crate::doc::Action) {
		self.emit(Instr::Write(WriteInstr::Observe(ObserveInstr::Lives(action))));
	}

	/// Emit `ProcessTableEvents` for `action`: the events of the one table the
	/// statement writes to, run by the unit [`Self::events_unit`] lowers.
	pub(super) async fn events_stage(&mut self, action: crate::doc::Action) -> Lowered<()> {
		if crate::exec::vm::host::out_of_line(|| self.write_shape()).await.is_none() {
			return Err(self.place_refusal());
		}
		let Some(table) = self.write_table.clone() else {
			return Err(NotCovered::internal("the events of a write that names no single table"));
		};
		let Some(txn) = self.planner.txn().cloned() else {
			return Err(NotCovered::internal("the events of a write lowered with no transaction"));
		};
		let Some((ns, db)) = self.database_ids()? else {
			// A database the catalog does not hold has no events to run, and
			// a statement that cannot define one runs none.
			if self.write_catalog_fixed {
				#[cfg(test)]
				crate::exec::vm::speculate::STAGES_LEFT_OUT
					.with(|count| count.set(count.get() + 1));
				return Ok(());
			}
			return Err(NotCovered::new(
				"the events of a write that may define them, lowered with no database",
			));
		};
		let events = match self.catalog.written(&txn, &table)? {
			Some((_, written)) => Arc::clone(&written.events),
			None => Arc::from([]),
		};
		// A table with no events runs none for any record of a write that
		// cannot define one.
		if events.is_empty() && self.write_catalog_fixed {
			#[cfg(test)]
			crate::exec::vm::speculate::STAGES_LEFT_OUT.with(|count| count.set(count.get() + 1));
			return Ok(());
		}
		let unit = crate::exec::vm::host::out_of_line(|| {
			self.events_unit(table, action, events, (ns, db))
		})
		.await?;
		self.emit(Instr::Write(WriteInstr::Observe(ObserveInstr::Events {
			unit,
			depth: self.planner.current_depth() + self.planner.statement_nesting(),
		})));
		Ok(())
	}

	/// The unit that runs `table`'s `events` for `action`, lowered with this
	/// lowering's planner: a program whose host pipelines are the events'
	/// clauses ([`Program::events`](super::Program)).
	///
	/// A unit is found again by its table, its action and what its clauses are
	/// lowered with, so events whose clauses write back to their own table —
	/// directly, or through the events of another table — reach the unit being
	/// lowered and close the cycle on it. Everything the clauses lower is a unit
	/// of the same arena. A chain of event frames that never settles on one
	/// already linked is refused once [`OPEN_EVENT_UNITS`] lowerings of the
	/// table's events for `action` are open.
	///
	/// Once [`NESTED_EVENT_UNITS`] lowerings of events are open in the arena,
	/// the unit is linked and its clauses are lowered after the outermost of
	/// them finishes ([`EventsQueue`]), so a chain of distinct tables' events
	/// holds a bounded depth of lowering on the native stack however long it
	/// is.
	async fn events_unit(
		&mut self,
		table: TableName,
		action: crate::doc::Action,
		events: Arc<[EventDefinition]>,
		(ns, db): (crate::catalog::NamespaceId, crate::catalog::DatabaseId),
	) -> Lowered<UnitId> {
		let of = Observed::Events(table.clone(), action);
		if let Some(unit) = self.observer_unit(&of) {
			return Ok(unit);
		}
		let open = EventsOpen {
			ns,
			db,
			table,
			action,
		};
		let entered = match open.enter(self.planner) {
			Ok(entered) => entered,
			// Frames that never repeat differ only in what does not change
			// what the clauses do; the unit of an alike frame runs them.
			Err(refused) => return self.alike_observer_unit(&of).ok_or(refused),
		};
		let id = self.link_observer(of)?;
		if self.events_queue.open >= NESTED_EVENT_UNITS {
			self.events_queue.pending.push(PendingEvents {
				unit: id,
				planner: self.planner.shadowing(std::iter::empty()),
				open,
				events,
				catalog: Arc::clone(&self.catalog),
				ahead: self.ahead,
				versioned: self.versioned,
				path: self.events_queue.path.clone(),
			});
			// What the clauses read of the frame is known only once they are
			// lowered, so the write is taken to read it.
			self.frame_reads += 1;
			return Ok(id);
		}
		let planner = self.planner;
		let catalog = Arc::clone(&self.catalog);
		let (ahead, versioned) = (self.ahead, self.versioned);
		let lowered = crate::exec::vm::host::out_of_line(|| {
			self.lower_events(planner, id, open, events, catalog, ahead, versioned)
		})
		.await;
		drop(entered);
		lowered?;
		if self.events_queue.open == 0 {
			crate::exec::vm::host::out_of_line(|| self.drain_events()).await?;
		}
		Ok(id)
	}

	/// Lower the clauses of `events`, the events of `open`'s table for its
	/// action, with `planner` into the unit `id` holds, which
	/// [`Self::events_unit`] linked.
	#[allow(clippy::too_many_arguments)]
	async fn lower_events(
		&mut self,
		planner: &Planner<'ctx>,
		id: UnitId,
		open: EventsOpen,
		events: Arc<[EventDefinition]>,
		catalog: Arc<crate::exec::vm::plan_catalog::PlanCatalog>,
		ahead: bool,
		versioned: bool,
	) -> Lowered<()> {
		let action = open.action;
		// The table is held on the planner's cycle guard while its events are
		// lowered, as a compilation of its permissions or computed fields
		// holds it.
		let _held = planner.cycle_guard.try_enter(open.ns, open.db, open.table.clone());
		// Every statement and body the clauses lower is a unit of this arena,
		// so the events a clause reaches again are found here.
		let mut l = Lowering::new(planner);
		l.catalog = catalog;
		l.ahead = ahead;
		l.bundle = true;
		l.versioned = versioned;
		l.writes = true;
		self.events_queue.open += 1;
		self.events_queue.path.push(open);
		l.put_arena(self.take_arena());
		let built = Box::pin(l.event_clauses(&events)).await;
		self.put_arena(l.take_arena());
		self.events_queue.path.pop();
		self.events_queue.open -= 1;
		let clauses = match built {
			Ok(clauses) => clauses,
			Err(refused) => {
				self.rewind_arena(id.0 as usize);
				return Err(refused);
			}
		};
		self.frame_reads += l.frame_reads;
		let mut program = l.finish()?;
		self.delegate_context = self.delegate_context.max(program.required_context);
		program.unit = Some(UnitHead {
			sig: UnitSig {
				params: Vec::new(),
				takes_row: false,
			},
			frame: Self::statement_frame(planner),
			entry: None,
			closure: None,
		});
		program.events = Some(Arc::new(LoweredEvents {
			action,
			events,
			clauses,
		}));
		self.units[id.0 as usize] = Arc::new(program);
		Ok(())
	}

	/// Lower every unit [`Self::events_unit`] left to the arena's outermost
	/// lowering of events, each as it would have been lowered where it was
	/// reached: with the planner and flags of the write that reached it, and
	/// with the lowerings of events open around it held open on the cycle
	/// guard again.
	async fn drain_events(&mut self) -> Lowered<()> {
		while let Some(pending) = self.events_queue.pending.pop() {
			let mut entered = Vec::with_capacity(pending.path.len() + 1);
			for open in pending.path.iter().chain(std::iter::once(&pending.open)) {
				entered.push(open.enter(&pending.planner)?);
			}
			let outer = std::mem::replace(&mut self.events_queue.path, pending.path);
			let lowered = crate::exec::vm::host::out_of_line(|| {
				self.lower_events(
					&pending.planner,
					pending.unit,
					pending.open,
					pending.events,
					pending.catalog,
					pending.ahead,
					pending.versioned,
				)
			})
			.await;
			self.events_queue.path = outer;
			drop(entered);
			lowered?;
		}
		Ok(())
	}

	/// The observer unit of this arena that runs `of`, lowered with a planner
	/// that plans alike this lowering's ([`PlannerFrameKey::plans_alike`]) and
	/// with its flags, when one is linked.
	fn alike_observer_unit(&self, of: &Observed) -> Option<UnitId> {
		let frame = self.planner.frame_key();
		self.observer_links
			.iter()
			.find(|link| {
				link.of == *of
					&& link.frame.plans_alike(&frame)
					&& link.ahead == self.ahead
					&& link.versioned == self.versioned
			})
			.map(|link| link.unit)
	}

	/// The observer unit of this arena that runs `of`, lowered with this
	/// lowering's planner and flags, when one is linked.
	pub(super) fn observer_unit(&self, of: &Observed) -> Option<UnitId> {
		let frame = self.planner.frame_key();
		self.observer_links
			.iter()
			.find(|link| {
				link.of == *of
					&& link.frame == frame
					&& link.ahead == self.ahead
					&& link.versioned == self.versioned
			})
			.map(|link| link.unit)
	}

	/// Hold a slot of the arena for the observer unit that runs `of`, linked
	/// so that a lowering reaching the same unit from inside its own finds it.
	/// The caller puts the unit in the slot, or rewinds the arena to it.
	pub(super) fn link_observer(&mut self, of: Observed) -> Lowered<UnitId> {
		let id = u32::try_from(self.units.len())
			.map_err(|_| NotCovered::internal("more units than a program holds"))?;
		self.units.push(Arc::new(Lowering::new(self.planner).finish()?));
		self.observer_links.push(ObserverLink {
			of,
			frame: self.planner.frame_key(),
			ahead: self.ahead,
			versioned: self.versioned,
			unit: UnitId(id),
		});
		Ok(UnitId(id))
	}

	/// Each of `events`' clauses lowered as a host pipeline of this lowering,
	/// with the event's own planner.
	async fn event_clauses(&mut self, events: &[EventDefinition]) -> Lowered<Vec<EventClauses>> {
		let mut clauses = Vec::with_capacity(events.len());
		for event in events {
			let when_sites = Arc::new(ExprDepth::of_expr(&event.when));
			let when_depth = ClauseDepth::of(&when_sites);
			let planner = event_planner(self.planner, event)?.with_depth_sites(when_sites);
			let when = crate::exec::vm::host::out_of_line(|| {
				self.event_clause(&planner, event.when_logical())
			})
			.await?;
			let mut then = Vec::new();
			let mut then_depths = Vec::new();
			if !event.is_async() {
				for (logical, expr) in event.then_logical().zip(&event.then) {
					let sites = Arc::new(ExprDepth::of_expr(expr));
					then_depths.push(ClauseDepth::of(&sites));
					let planner = event_planner(self.planner, event)?.with_depth_sites(sites);
					then.push(
						crate::exec::vm::host::out_of_line(|| self.event_clause(&planner, logical))
							.await?,
					);
				}
			}
			clauses.push(EventClauses {
				when,
				then,
				when_depth,
				then_depths,
			});
		}
		Ok(clauses)
	}

	/// A host pipeline over the image an event is about that answers `logical`,
	/// lowered with `planner`, the event's. A clause that is a statement is a
	/// program lowered with `planner`, run as a subquery whose `RETURN` is the
	/// clause's value; any other clause is the pipeline's own instructions,
	/// with what the VM has no instruction for compiled by a planner that
	/// resolves no table and carries no principal.
	///
	/// A clause with no logical form is not covered.
	async fn event_clause(
		&mut self,
		planner: &Planner<'ctx>,
		logical: anyhow::Result<&LogicalExpr>,
	) -> Lowered<u16> {
		let expr = logical.map_err(|e| {
			NotCovered::internal(format!("an event clause has no logical form: {e}"))
		})?;
		// An event body is definer code run in a privileged frame: its clause
		// is planned against the session's own values and the database's, and
		// lowered as a privileged frame's code.
		let frame = crate::ctx::Context::new_isolated(planner.frozen_ctx()).freeze();
		let bare = Planner::new(&frame, planner.function_registry());
		let outer_privileged = std::mem::replace(&mut self.privileged, true);
		let lowered = crate::exec::vm::host::out_of_line(|| {
			self.clause_in(planner, Some(bare), ClauseCode::Expr(expr), true)
		})
		.await;
		self.privileged = outer_privileged;
		lowered
	}

	/// A host pipeline over the value a clause is about that computes `code`,
	/// lowered with `planner` in a lowering of its own that shares this one's
	/// tables. The expressions the instructions have no form for are compiled
	/// by `bare` when there is one, and by `planner` otherwise. The value is
	/// known to be an object when `objects`.
	///
	/// A clause that holds a write the planner cannot plan is not covered.
	pub(super) async fn clause_in(
		&mut self,
		planner: &Planner<'ctx>,
		bare: Option<Planner<'ctx>>,
		code: ClauseCode<'_>,
		objects: bool,
	) -> Lowered<u16> {
		if let ClauseCode::Expr(expr) = &code
			&& crate::exec::logical::unplannable_write_in_expr(expr).is_some()
		{
			return Err(NotCovered::internal("a clause holds a write the planner cannot plan"));
		}
		let mut child = Lowering::new(planner);
		child.catalog = Arc::clone(&self.catalog);
		lend(self, &mut child);
		child.ahead = self.ahead;
		child.bundle = self.bundle;
		child.versioned = self.versioned;
		child.bare = bare;
		// The clause runs in this lowering's frame, privileged where it is.
		child.privileged = self.in_privileged_frame();
		let built = Box::pin(child.clause_pipeline(code, objects)).await;
		let context = child.delegate_context;
		let frame_reads = child.frame_reads;
		lend(self, &mut child);
		self.delegate_context = self.delegate_context.max(context);
		self.frame_reads += frame_reads;
		built
	}

	/// [`Self::clause_in`]'s work, on the lowering made for the clause.
	pub(super) async fn clause_pipeline(
		&mut self,
		code: ClauseCode<'_>,
		objects: bool,
	) -> Lowered<u16> {
		let spec = match &code {
			ClauseCode::Expr(LogicalExpr::Subquery(op)) => {
				// A statement in definer code is bound and planned against the
				// session's own values alone, never a caller's bindings.
				let privileged = self.in_privileged_frame().then(|| self.planner.privileged());
				let planner = privileged.as_ref().unwrap_or(self.planner);
				let bound =
					crate::exec::planner::bind_statement(planner, (**op).clone()).await.map_err(
						|e| NotCovered::new(format!("a clause's statement did not bind: {e}")),
					)?;
				Some(
					crate::exec::vm::host::out_of_line(|| self.subquery_unit(planner, &bound))
						.await?,
				)
			}
			_ => None,
		};
		let aside = self.set_aside();
		self.rows_are_objects = objects;
		// The image the clause runs over is its document root.
		self.document = super::DocumentRoot::RowElseReceiver;
		let built = match code {
			ClauseCode::Expr(expr) => {
				crate::exec::vm::host::out_of_line(|| self.event_clause_code(expr, spec)).await
			}
			ClauseCode::Projection(Projection::All) => {
				self.emit(Instr::Emit {
					src: ROW,
				});
				self.emit(Instr::EndRow);
				Ok(())
			}
			ClauseCode::Projection(projection) => {
				crate::exec::vm::host::out_of_line(|| self.projection_body(projection)).await
			}
		};
		self.restore(aside, built)
	}

	/// The body of a clause's pipeline: `spec` run as a subquery when the
	/// clause is a statement, and `expr`'s instructions otherwise.
	async fn event_clause_code(
		&mut self,
		expr: &LogicalExpr,
		spec: Option<SubquerySpec>,
	) -> Lowered<()> {
		let dst = self.register()?;
		match spec {
			Some(spec) => {
				self.delegate_context = self.delegate_context.max(spec.required_context);
				self.subqueries.push(spec);
				let subquery = (self.subqueries.len() - 1) as u32;
				let at = self.emit(Instr::Subquery {
					subquery,
					dst,
					frame: None,
					caught: Some(super::super::program::Catch {
						root: dst,
						label: 0,
						loops_fail: false,
					}),
					doc: None,
				});
				let here = self.here();
				self.patch(at, here);
				self.uses_eval = true;
				self.may_fail = true;
			}
			None => {
				let value = crate::exec::vm::host::out_of_line(|| self.value(expr)).await?;
				self.emit(Instr::Load {
					dst,
					src: value,
				});
			}
		}
		self.emit(Instr::Emit {
			src: dst,
		});
		self.emit(Instr::EndRow);
		Ok(())
	}
}

/// The planner `event`'s clauses are lowered with: `base`'s catalog and
/// transaction, planning a stored clause, with permissions suppressed as the
/// event's frame suppresses them, the principal capped by the event's
/// `AUTH LIMIT`, and the frame's parameters left to each record.
fn event_planner<'c>(base: &Planner<'c>, event: &EventDefinition) -> Lowered<Planner<'c>> {
	let mut planner =
		base.shadowing(EVENT_PARAMS.iter().map(|name| (*name).to_owned())).in_stored_clause();
	planner.perms_suppressed = true;
	let limit = crate::exec::operators::scan::pipeline::narrowing_auth_limit(&event.auth_limit)
		.map_err(|e| NotCovered::internal(format!("an event's AUTH LIMIT did not convert: {e}")))?;
	if let (Some(limit), Some(auth)) = (limit, planner.auth.clone()) {
		planner = planner.with_auth(Arc::new(auth.new_limited(&limit)));
	}
	Ok(planner)
}

/// A live subscription's work on a record lowered as host pipelines of a
/// program of its own, compiled with `planner` — the subscriber's: its
/// principal, the database the subscription watches, and every parameter its
/// frame binds left to the frame — against the catalog `table` holds now.
///
/// The pipelines are the ones the subscriber's view of a record is made of,
/// in the order the table view applies them, and then the subscription's own
/// clauses: the table's `PERMISSIONS FOR select`, judged for a subscriber
/// whose reads are checked against a copy of the record with the `COMPUTED`
/// fields it reads derived in its own frame, whatever the record carries
/// under their names; the table's field view for
/// the subscriber, every `COMPUTED` field derived and each field the
/// subscriber may not select cut; the `WHERE`; and the projection. `FETCH` is
/// walked by the host from the paths the subscription compiles to. A table
/// whose field view has no layer form, a permission or a `WHERE` with no
/// logical form, or a path that does not resolve, is not covered.
///
/// What the work consults of the catalog is read first, and the lowering
/// takes it from there ([`crate::exec::vm::plan_catalog`]).
pub(crate) async fn lower_subscription(
	planner: &Planner<'_>,
	table: surrealdb_strand::TableName,
	compiled: &crate::catalog::CompiledSubscription,
) -> Lowered<crate::exec::vm::live::LiveUnit> {
	let catalog =
		crate::exec::vm::plan_catalog::read_subscription(planner, &table, compiled).await?;
	let lowered = lower_subscription_from(planner, table, compiled, Arc::new(catalog));
	crate::kvs::catalog_reads::sealed(true, lowered).await
}

async fn lower_subscription_from(
	planner: &Planner<'_>,
	table: surrealdb_strand::TableName,
	compiled: &crate::catalog::CompiledSubscription,
	catalog: Arc<crate::exec::vm::plan_catalog::PlanCatalog>,
) -> Lowered<crate::exec::vm::live::LiveUnit> {
	use crate::catalog::{Permission, SubscriptionFields};
	use crate::exec::vm::live::LiveUnit;
	use crate::exec::vm::write::{Admission, LoweredSubscription, SubscriptionFieldsLowered};

	let Some(txn) = planner.txn().cloned() else {
		return Err(NotCovered::new("a subscription lowered with no database"));
	};
	let definition = catalog
		.table(&txn, &table)?
		.and_then(|read| read.definition.clone())
		.ok_or_else(|| NotCovered::new("the subscription's table did not read"))?;
	let mut l = Lowering::new(planner);
	l.catalog = catalog;
	l.scalar = true;
	let checked = !planner.perms_suppressed && planner.compiles_for_checked_caller();
	let admission = if !checked {
		Admission::All
	} else {
		match &definition.permissions.select {
			Permission::Full => Admission::All,
			Permission::None => Admission::Nothing,
			Permission::Specific(stored) => {
				let guard = super::write_gate::gate_guard(definition.select_permission_logical())?;
				let derived = crate::exec::vm::host::out_of_line(|| {
					l.stored_guard_derivations(&table, stored)
				})
				.await?;
				Admission::Judge(
					crate::exec::vm::host::out_of_line(|| l.read_guard_pipeline(&guard, &derived))
						.await?,
				)
			}
		}
	};
	let (view, view_checked) = l
		.image_view(&table)
		.await?
		.ok_or_else(|| NotCovered::new("the table's field view has no layer form"))?;
	if view_checked != checked {
		return Err(NotCovered::new("the field view and the permission disagree on checking"));
	}
	let cond = match &compiled.cond {
		Some(expr) => {
			let logical = crate::expr::logical::lower_expr(expr.clone()).map_err(|e| {
				NotCovered::new(format!("the subscription's WHERE has no logical form: {e}"))
			})?;
			Some(
				crate::exec::vm::host::out_of_line(|| {
					l.clause_pipeline(ClauseCode::Expr(&logical), true)
				})
				.await?,
			)
		}
		None => None,
	};
	let fields = match &compiled.fields {
		SubscriptionFields::Diff => SubscriptionFieldsLowered::Diff,
		SubscriptionFields::Select(fields) => {
			let logical = crate::expr::logical::lower_fields(fields.clone()).map_err(|e| {
				NotCovered::new(format!("the subscription's projection has no logical form: {e}"))
			})?;
			let projection = crate::exec::vm::map::projection(&logical, Vec::new())?;
			if matches!(projection, Projection::All) {
				SubscriptionFieldsLowered::Subject
			} else {
				let aside = l.set_aside();
				let built = Box::pin(l.projection_body(&projection)).await;
				SubscriptionFieldsLowered::Pipeline(l.restore(aside, built)?)
			}
		}
	};
	// A path that names a parameter is answered from the subscription's own
	// captured variables, as the planner on the subscriber's frame reads it.
	let mut paths = Vec::new();
	for expr in compiled.fetch.iter().flatten() {
		let logical = crate::expr::logical::lower_expr(expr.clone())
			.map_err(|e| NotCovered::new(format!("a FETCH path has no logical form: {e}")))?;
		paths.append(
			&mut planner
				.resolve_field_paths(logical)
				.await
				.map_err(|e| NotCovered::new(format!("a FETCH path did not resolve: {e}")))?,
		);
	}
	let fetch = crate::exec::vm::host::out_of_line(|| l.fetch_steps(&paths)).await?;
	let program = l.finish()?;
	Ok(LiveUnit {
		program: Arc::new(program),
		plan: LoweredSubscription {
			admission,
			view,
			cond,
			fields,
			fetch,
		},
	})
}
