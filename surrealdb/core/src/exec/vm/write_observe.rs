//! The host's side of the observer stages run as instructions: the table's
//! events, its views (`vm::write::view`) and the record's change.
//!
//! An event's `WHEN` and `THEN` clauses are host pipelines of the write's
//! program, lowered once when the program is lowered from the event
//! definitions the table held then. Each record binds its event frame and runs
//! them; an `ASYNC` event's firing is queued, and the change feed's entry
//! buffered, as effects the host takes. A record whose table holds other
//! events than the ones the program was lowered from fails: a program does not
//! run against a catalog it was not lowered for.
//!
//! The table's live subscriptions are not the program's to know: another
//! session registers and kills them at any time. A record reads the ones its
//! write context holds, and a write to a table with none does nothing more.
//! Each subscription's work is a unit of its own
//! ([`LoweredSubscription`] over the program of its [`LiveUnit`]), compiled
//! for its principal before any write reaches it and carried by the write
//! context (`vm::live`); its notification is sent only once the transaction
//! commits.

// Runtime that lowered programs call into.
#![allow(dead_code)]

use std::sync::Arc;

use super::data::statement_input;
use super::{State, hide_conflict_record, internal, run_image};
use crate::catalog::EventDefinition;
use crate::doc::Action;
use crate::exec::operators::fetch::fetch_fields_with;
use crate::exec::operators::write::ctx::WriteCtx;
use crate::exec::operators::write::stages::lives::{
	Images, Notify, Subscriber, deliver, deriving_frozen, envelope, subscriber, subscriber_frame,
};
use crate::exec::operators::write::stages::observe::{
	is_observed, queue_async_event, record_change,
};
use crate::exec::permission::{EventImages, event_frame};
use crate::exec::vm::depth::{ExprDepth, STATEMENT_DEPTH};
use crate::exec::vm::live::LiveUnit;
use crate::exec::vm::program::{Label, Program, UnitId};
use crate::exec::{ExecutionContext, FlowResult};
use crate::expr::ControlFlow;
use crate::val::{RecordId, Value};

/// An instruction of the observer stages.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::exec::vm) enum ObserveInstr {
	/// Run the table's events for the record: the events and clauses of unit
	/// `unit` ([`Program::events`]).
	Events {
		unit: UnitId,
		/// The planner's re-entry depth at the write, which a write reached
		/// from code not evaluated one expression at a time spends before its
		/// records' events run ([`crate::exec::vm::depth`]).
		depth: u32,
	},
	/// Buffer the record's change for the change feed and the live-query
	/// capture.
	RecordChange,
	/// Maintain the views the table feeds for the record, for `action`.
	Views {
		action: Action,
		lowered: Arc<super::view::LoweredViews>,
	},
	/// Notify the table's live subscriptions of the record, for `action`.
	Lives(Action),
}

/// A live subscription's work on a record, as the host pipelines of the
/// program of its [`LiveUnit`] the plan names, lowered for the subscription's
/// principal.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LoweredSubscription {
	/// Whether the table's `PERMISSIONS FOR select` shows the subscriber a
	/// record, judged against the record as stored.
	pub(in crate::exec::vm) admission: Admission,
	/// The table's field view for the subscriber, `None` when it changes
	/// nothing.
	pub(in crate::exec::vm) view: Option<u16>,
	/// The `WHERE`, over the record the notification is about.
	pub(in crate::exec::vm) cond: Option<u16>,
	/// What the notification carries.
	pub(in crate::exec::vm) fields: SubscriptionFieldsLowered,
	/// The `FETCH` paths, walked by the host over what the projection made.
	pub(in crate::exec::vm) fetch: Vec<Vec<crate::exec::vm::program::FetchStepSpec>>,
}

assert_plain!(LoweredSubscription);
assert_plain!(ObserveInstr);

/// The table's `PERMISSIONS FOR select` for a subscriber.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(in crate::exec::vm) enum Admission {
	/// Every record: the permission is `FULL`, or the subscriber's reads are
	/// not checked.
	All,
	/// No record: the permission is `NONE`.
	Nothing,
	/// The records host pipeline `.0` judges truthy.
	Judge(u16),
}

/// What a lowered subscription's notification carries.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(in crate::exec::vm) enum SubscriptionFieldsLowered {
	/// `LIVE SELECT DIFF`: the patch from one reduced image to the other.
	Diff,
	/// `LIVE SELECT *`: the record, reduced.
	Subject,
	/// The row host pipeline `.0` projects over the reduced record.
	Pipeline(u16),
}

/// A table's events, as the program was lowered from them.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::exec::vm) struct LoweredEvents {
	/// The action the order is for, which `$event` names and which decides the
	/// image an event is about.
	pub(in crate::exec::vm) action: Action,
	/// The table's event definitions when the program was lowered, in the
	/// order the catalog holds them.
	pub(in crate::exec::vm) events: Arc<[EventDefinition]>,
	/// Each event's clauses, as host pipelines over the image it is about.
	pub(in crate::exec::vm) clauses: Vec<EventClauses>,
}

/// One event's clauses as host pipelines: its `WHEN`, and for a synchronous
/// event its `THEN` clauses in order. An `ASYNC` event's `THEN` clauses run in
/// the drain, not here, and are not lowered.
///
/// A dormant event, whose `WHEN` is never truthy for the action, runs no
/// clause: it holds no `WHEN` pipeline and no `THEN` pipelines, and a record
/// spends of the budget only what entering its `WHEN` would.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::exec::vm) struct EventClauses {
	/// The `WHEN` pipeline, or `None` for a dormant event.
	pub(in crate::exec::vm) when: Option<u16>,
	pub(in crate::exec::vm) then: Vec<u16>,
	/// What evaluating the `WHEN` spends of the computation-depth budget.
	pub(in crate::exec::vm) when_depth: ClauseDepth,
	/// What evaluating each `THEN` clause spends of it, in the order of
	/// [`Self::then`].
	pub(in crate::exec::vm) then_depths: Vec<ClauseDepth>,
}

/// What evaluating one event clause spends of the computation-depth budget
/// ([`super::depth`]).
///
/// A write statement spends [`STATEMENT_DEPTH`] of its own before its
/// records' events run, and a record a clause writes runs its table's events
/// from what is left once the clause has spent [`Self::write`] and the write
/// its own: events that write records whose events fire again spend the
/// budget level by level and fail with
/// [`crate::exec::Error::ComputationDepthExceeded`] once it is gone, rather
/// than recursing without bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::exec::vm) struct ClauseDepth {
	/// The most the clause has spent at once anywhere in it: the budget left
	/// must cover this for the clause to be evaluated.
	pub(in crate::exec::vm) deepest: u8,
	/// What the clause has spent on the way to its shallowest write, the
	/// write's own [`STATEMENT_DEPTH`] not counted, which entering the clause
	/// spends; nothing for a clause that writes nothing itself.
	pub(in crate::exec::vm) write: u8,
}

impl ClauseDepth {
	/// What evaluating a clause of which `depth` is the measure spends.
	pub(in crate::exec::vm) fn of(depth: &ExprDepth) -> Self {
		let clamp = |n: u32| u8::try_from(n).unwrap_or(u8::MAX);
		Self {
			deepest: clamp(depth.deepest),
			write: clamp(depth.charged()),
		}
	}
}

assert_plain!(LoweredEvents);
assert_plain!(EventClauses);

impl ObserveInstr {
	/// The jump target the instruction holds, for the lowering to patch.
	pub(in crate::exec::vm) fn target_mut(&mut self) -> Option<&mut Label> {
		match self {
			ObserveInstr::Events {
				..
			}
			| ObserveInstr::RecordChange
			| ObserveInstr::Lives(_)
			| ObserveInstr::Views {
				..
			} => None,
		}
	}
}

/// Take `instr` for the record in flight, and answer the instruction the body
/// continues at.
pub(super) async fn step(
	program: &Program,
	instr: &ObserveInstr,
	next: usize,
	state: &mut State,
) -> FlowResult<usize> {
	match instr {
		ObserveInstr::RecordChange => {
			let write = state
				.write
				.as_deref_mut()
				.ok_or_else(|| internal("a write body with no write state"))?;
			let (row, ctx) = write.in_flight()?;
			record_change(row, &ctx).await?;
		}
		ObserveInstr::Events {
			unit,
			depth,
		} => events(program, *unit, *depth, state).await?,
		ObserveInstr::Views {
			action,
			lowered,
		} => super::view::views(program, *action, lowered, state).await?,
		ObserveInstr::Lives(action) => lives(*action, state).await?,
	}
	Ok(next)
}

/// Notify the live subscriptions the record in flight's context holds of the
/// record, each by the unit its context holds for it (`vm::live`).
///
/// Nothing runs under the router engine, which delivers off the write path, or
/// the external engine, whose host delivers; for a write no observer sees; or
/// where no broker takes notifications. A record with no id on a table with
/// subscriptions is a broken order, failed once rather than per subscription.
///
/// The record is noted with the broker whatever the table's subscriptions, so
/// that a subscription registered after the write's snapshot is told of it once
/// the write commits ([`crate::dbs::live_recheck`]).
async fn lives(action: Action, state: &mut State) -> FlowResult<()> {
	let write =
		state.write.as_deref_mut().ok_or_else(|| internal("a write body with no write state"))?;
	let (row, ctx) = write.in_flight()?;
	if !ctx.exec().root().ctx.config.datastore.live_query_engine.notifies_inline()
		|| !is_observed(row, &ctx)
	{
		return Ok(());
	}
	let Some(broker) = ctx.exec().ctx().broker().cloned() else {
		return Ok(());
	};
	let Some(rid) = row.id.clone() else {
		// A record no subscription is told of needs no id.
		if ctx.lives.is_empty() {
			return Ok(());
		}
		return Err(ControlFlow::Err(anyhow::anyhow!(crate::err::EngineError::unreachable(
			"notifying a subscription of a record with no record id"
		))));
	};
	let recheck = ctx.exec().ctx().live_recheck();
	let keep = recheck.is_some_and(|recheck| recheck.keeps_images(&ctx));
	if ctx.lives.is_empty() && !keep {
		if let Some(recheck) = recheck {
			recheck.record(&ctx, rid, action, None);
		}
		return Ok(());
	}
	let images = Images {
		initial: Arc::new(row.initial.clone()),
		current: Arc::new(row.current.clone()),
		action,
	};
	if let Some(recheck) = recheck {
		recheck.record(&ctx, rid.clone(), action, keep.then(|| images.clone()));
	}
	if ctx.lives.is_empty() {
		return Ok(());
	}
	#[cfg(test)]
	LIVES_RUN.with(|runs| runs.set(runs.get() + 1));
	for at in 0..ctx.lives.len() {
		let notified = Box::pin(notify_at(&ctx, at, &rid, &images)).await?;
		if let Notify::Deliver(notification) = notified {
			deliver(&broker, &ctx, &ctx.lives[at], notification).await?;
		}
	}
	Ok(())
}

/// Compute the notification the subscription at position `at` of `ctx`'s
/// subscriptions gets for `rid`, by the unit `ctx` holds for it ([`notify_unit`]).
///
/// A subscription with no unit gets none: it is served nothing, or its clauses
/// did not lower and that is traced where the unit was decided
/// ([`crate::exec::vm::live::ensure`]). The context holds one entry per
/// subscription, so a missing entry is an invariant the write broke.
pub(crate) async fn notify_at(
	ctx: &WriteCtx,
	at: usize,
	rid: &RecordId,
	images: &Images,
) -> Result<Notify, ControlFlow> {
	let subscription = ctx
		.lives
		.get(at)
		.ok_or_else(|| internal("a write's live subscriptions are out of range"))?;
	let slot = ctx
		.live_units
		.get(at)
		.ok_or_else(|| internal("a write's live units do not match its subscriptions"))?;
	let Some(unit) = slot else {
		#[cfg(test)]
		if subscriber(subscription).is_some() {
			LIVES_DECLINED.with(|runs| runs.set(runs.get() + 1));
		}
		return Ok(Notify::Skip);
	};
	#[cfg(test)]
	LIVES_LOWERED.with(|runs| runs.set(runs.get() + 1));
	Ok(Box::pin(notify_unit(unit, subscription, ctx, rid, images)).await)
}

#[cfg(test)]
thread_local! {
	/// Records whose live subscriptions the instructions notified, for tests
	/// that check which path delivered them.
	pub(in crate::exec::vm) static LIVES_RUN: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
	/// Subscriptions notified by their lowered programs.
	pub(in crate::exec::vm) static LIVES_LOWERED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
	/// Subscriptions served whose work has no unit, so they were not notified.
	pub(in crate::exec::vm) static LIVES_DECLINED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Compute one subscription's notification by its lowered program, or decide
/// it gets none: the record and its counterpart reduced to the subscriber's
/// view in the frame binding the unreduced records, then the `WHERE`, the
/// projection and `FETCH` in the frame binding the reduced ones. Every failure
/// is the subscription's: it is skipped, and the write is unaffected. A write
/// reaches this through [`notify_at`]; the live-query router's replay calls
/// it with the unit it holds for a subscription it read itself.
pub(crate) async fn notify_unit(
	unit: &LiveUnit,
	subscription: &crate::catalog::SubscriptionDefinition,
	ctx: &WriteCtx,
	rid: &RecordId,
	images: &Images,
) -> Notify {
	macro_rules! skip {
		($reason:expr, $err:expr) => {{
			tracing::debug!(
				target: "surrealdb::core::exec::write::lives",
				subscription_id = %subscription.id,
				error = %$err,
				reason = $reason,
				"LIVE notification skipped",
			);
			return Notify::Skip;
		}};
	}
	let Some(Subscriber {
		session,
		auth,
		..
	}) = subscriber(subscription)
	else {
		return Notify::Skip;
	};
	let lowered = &unit.plan;
	let program = &unit.program;
	let deriving = subscriber_frame(ctx, subscription, session, images, |images| {
		(Arc::clone(&images.initial), Arc::clone(&images.current))
	});
	let deriving = deriving.with_replaced_principal(deriving_frozen(&deriving), &auth);
	let mut state = match State::new(program, Vec::new(), deriving.function_registry()) {
		Ok(state) => state,
		Err(err) => skip!("the subscription's program has no state to run in", err),
	};
	state.frames = program.permissions.iter().map(|spec| spec.context(&deriving)).collect();
	state.permissions_bypassed = deriving.root().skip_fetch_perms;

	// The notification is about `subject` (`current` for `CREATE`/`UPDATE`,
	// `initial` for `DELETE`) and its counterpart (`initial` for `UPDATE`,
	// absent for `CREATE`/`DELETE`). An image the subscriber's table permission
	// or view filter withholds has no reduced document of its own and is bound
	// to `NONE`. If neither image is visible to the subscriber, no notification
	// is delivered.
	let (subject_admitted, subject) =
		match reduce(unit, &deriving, &mut state, images.subject()).await {
			Ok(Some(value)) => (true, value),
			Ok(None) => (false, Value::None),
			Err(err) => skip!("the record could not be reduced for this subscriber", err),
		};
	let (counterpart_admitted, counterpart) = if images.counterpart().is_nullish() {
		(false, Value::None)
	} else {
		match reduce(unit, &deriving, &mut state, images.counterpart()).await {
			Ok(Some(value)) => (true, value),
			Ok(None) => (false, Value::None),
			Err(err) => skip!("the counterpart could not be reduced for this subscriber", err),
		}
	};
	if !subject_admitted && !counterpart_admitted {
		return Notify::Skip;
	}
	if images.action == Action::Update
		&& subject_admitted
		&& counterpart_admitted
		&& lowered.view.is_some()
		&& images.initial != images.current
		&& subject == counterpart
	{
		return Notify::Skip;
	}

	let (before, after) = match images.action {
		Action::Delete => (Arc::new(subject.clone()), Arc::new(counterpart)),
		Action::Create | Action::Update => (Arc::new(counterpart), Arc::new(subject.clone())),
	};
	let frame = subscriber_frame(ctx, subscription, session, images, |_| {
		(Arc::clone(&before), Arc::clone(&after))
	});
	// The subscriber's WHERE, projection and FETCH clauses run in this frame at
	// notification time, inside the triggering write's transaction. Refuse
	// writes so a subscriber-authored clause cannot run a write subquery there
	// (GHSA-pvv9-h693-m8r4). The `deriving` frame above keeps writing enabled,
	// as its only job is the table owner's own reduction of the record.
	let frame = frame
		.with_replaced_principal(deriving_frozen(&frame), &auth)
		.refusing_writes(crate::dbs::NoWriteFrame::LiveClause);
	state.frames = program.permissions.iter().map(|spec| spec.context(&frame)).collect();
	state.permissions_bypassed = frame.root().skip_fetch_perms;

	if let Some(cond) = lowered.cond {
		match answer(program, cond, subject.clone(), &frame, &mut state).await {
			Ok(value) if value.is_truthy() => {}
			Ok(_) => return Notify::Skip,
			Err(err) => skip!("the WHERE clause could not be evaluated", err),
		}
	}

	let mut result = match lowered.fields {
		SubscriptionFieldsLowered::Diff => {
			let (from, to) = match images.action {
				Action::Delete => (subject.clone(), Value::None),
				Action::Create => (Value::None, subject.clone()),
				Action::Update => ((*before).clone(), subject.clone()),
			};
			Value::Array(
				from.diff(&to).into_iter().map(|op| Value::Object(op.into_object())).collect(),
			)
		}
		SubscriptionFieldsLowered::Subject => subject.clone(),
		SubscriptionFieldsLowered::Pipeline(pipeline) => {
			match answer(program, pipeline, subject.clone(), &frame, &mut state).await {
				Ok(value) => value,
				Err(err) => skip!("the projection could not be evaluated", err),
			}
		}
	};

	if !lowered.fetch.is_empty()
		&& let Err(err) = fetch_fields_with(
			&frame,
			std::mem::take(&mut result),
			&lowered.fetch,
			&crate::exec::vm::host::ProgramIndex {
				program,
			},
			crate::exec::operators::fetch::FetchLinks::default(),
		)
		.await
		.map(|value| result = value)
	{
		skip!("a FETCH path could not be resolved", err);
	}

	envelope(subscription, session, rid, images.public_action(), result)
}

/// One image reduced to what the subscriber may see, in `deriving`, the frame
/// binding the unreduced records: `None` when the table's `PERMISSIONS FOR
/// select` hides it, judged against a copy of the image with the `COMPUTED`
/// fields it reads derived, and otherwise the image through the table's field
/// view.
async fn reduce(
	unit: &LiveUnit,
	deriving: &ExecutionContext,
	state: &mut State,
	image: &Arc<Value>,
) -> FlowResult<Option<Value>> {
	let lowered = &unit.plan;
	let program = &unit.program;
	match lowered.admission {
		Admission::All => {}
		Admission::Nothing => return Ok(None),
		Admission::Judge(pipeline) => {
			let verdict = run_image(
				program,
				pipeline,
				(**image).clone(),
				Some(deriving.clone()),
				state,
				deriving,
			)
			.await?;
			if !verdict.is_truthy() {
				return Ok(None);
			}
		}
	}
	match lowered.view {
		None => Ok(Some((**image).clone())),
		Some(view) => {
			run_image(program, view, (**image).clone(), Some(deriving.clone()), state, deriving)
				.await
				.map(Some)
		}
	}
}

/// Host pipeline `pipeline`'s value over `subject` in `frame`, a `RETURN`
/// inside it being its value.
async fn answer(
	program: &Program,
	pipeline: u16,
	subject: Value,
	frame: &ExecutionContext,
	state: &mut State,
) -> FlowResult<Value> {
	match run_image(program, pipeline, subject, Some(frame.clone()), state, frame).await {
		Ok(value) | Err(ControlFlow::Return(value)) => Ok(value),
		Err(failure) => Err(failure),
	}
}

/// Run the table's events for the record in flight, as unit `unit` holds
/// them: each event's `WHEN`, and for one that fires, its `THEN` clauses in
/// order, or its queue entry for an `ASYNC` event.
///
/// Nothing runs for a write no observer sees. The events the table holds when
/// the record is written are the ones the unit was lowered from; a table that
/// holds others fails the record, since the unit's clauses are not theirs.
/// What the statement supplied, `$input`, is materialised from `program`'s
/// data clause before the unit runs.
async fn events(program: &Program, unit: UnitId, depth: u32, state: &mut State) -> FlowResult<()> {
	let write =
		state.write.as_deref_mut().ok_or_else(|| internal("a write body with no write state"))?;
	let levels = write.maintaining.len();
	let (row, ctx) = write.in_flight()?;
	if !is_observed(row, &ctx)
		|| ctx.events.is_empty()
		|| crate::exec::materialize::is_marked(ctx.exec())
	{
		return Ok(());
	}
	// A write evaluated one expression at a time has spent its way to the
	// write already; any other resumes the count from the planner's depth.
	let exec = match ctx.exec().per_expression_depth() {
		true => ctx.exec().clone(),
		false => ctx.exec().entering_stored_code(depth, false),
	};
	let exec = exec.dive_by(STATEMENT_DEPTH).map_err(ControlFlow::Err)?;
	// A view row's write is nested `levels` deep within the statement's, a
	// depth its write context does not carry.
	let exec = super::view::view_row_exec(exec, levels)?;
	let (held, units) = super::write_unit(program, unit, state, &exec)?;
	let lowered =
		held.events.as_ref().ok_or_else(|| internal("an events unit that holds no events"))?;
	let write =
		state.write.as_deref_mut().ok_or_else(|| internal("a write body with no write state"))?;
	if !write.events_hold(&ctx.events, lowered) {
		return Err(internal("the events of the table changed after the write was lowered"));
	}
	// An event may do anything, so it runs only once every create held before it is checked:
	// none runs after a create that fails.
	exec.txn().flush_held().await.map_err(|e| ControlFlow::Err(e.into()))?;
	let input = statement_input(program, &exec, state).await?;
	let input = Value::clone(&input);
	let (mut nested, group) = super::enter_unit(&held, units, state, &exec)?;
	let ran = fire(&held, lowered, input, &mut nested, &exec).await;
	super::leave_unit(&mut nested, group, state);
	ran
}

/// [`events`]' work in the state of the unit `program`, whose clauses
/// `lowered`'s are, with `input` as `$input`.
async fn fire(
	program: &Program,
	lowered: &LoweredEvents,
	input: Value,
	state: &mut State,
	exec: &ExecutionContext,
) -> FlowResult<()> {
	// The images are copied for the first event that runs a clause. A dormant event before it runs
	// nothing that could change them.
	let mut images: Option<(Value, Value)> = None;
	for (event, clauses) in lowered.events.iter().zip(&lowered.clauses) {
		let Some(when) = clauses.when else {
			enter_check(exec, clauses.when_depth)
				.map_err(|e| ControlFlow::Err(event_failure(&event.name, ControlFlow::Err(e))))?;
			continue;
		};
		if images.is_none() {
			let row = state
				.write
				.as_deref_mut()
				.ok_or_else(|| internal("a write body with no write state"))?
				.row()?;
			images = Some((row.initial.clone(), row.current.clone()));
		}
		let (before, after) =
			images.as_ref().ok_or_else(|| internal("the images of a record in flight"))?;
		let (frame, subject) = event_frame(
			exec,
			event,
			lowered.action,
			EventImages {
				before: before.clone(),
				after: after.clone(),
				input: input.clone(),
			},
		)
		.map_err(ControlFlow::Err)?;
		let when =
			clause(program, when, clauses.when_depth, &subject, &frame, state, exec, event).await?;
		if !when.is_truthy() {
			continue;
		}
		if event.is_async() {
			let write = state
				.write
				.as_deref_mut()
				.ok_or_else(|| internal("a write body with no write state"))?;
			let (row, ctx) = write.in_flight()?;
			queue_async_event(row, &ctx, &frame, event, &subject, lowered.action).await?;
			continue;
		}
		for (then, depth) in clauses.then.iter().zip(&clauses.then_depths) {
			clause(program, *then, *depth, &subject, &frame, state, exec, event).await?;
		}
	}
	Ok(())
}

/// The check entering a clause that spends `depth` of the budget makes: the budget must cover the
/// deepest nesting the clause reaches and the dive the clause's own write makes.
///
/// [`clause`] makes it on the event's frame before the dive, and a dormant event, whose frame is
/// not derived, makes it on `exec`, the context the frame derives from. The two answer alike
/// because the frame leaves the budget as `exec` holds it: the privileged frame, `$before`,
/// `$after`, `$event`, `$value` and `$input`, and the event's `AUTH LIMIT` change nothing the
/// budget is read from. It answers the failure a clause raises for a budget too small for it,
/// before the name of the event is added.
fn enter_check(ctx: &ExecutionContext, depth: ClauseDepth) -> anyhow::Result<()> {
	ctx.check_dive_by(depth.deepest).and_then(|()| ctx.check_dive_by(depth.write))
}

/// Evaluate one clause of `event` against the image the event is about, in its
/// frame, spending what `depth` says the clause spends of the
/// computation-depth budget. A `RETURN` in the clause is the clause's value;
/// every other failure, a budget too small for the clause among them, fails
/// the statement under the event's name.
#[allow(clippy::too_many_arguments)]
async fn clause(
	program: &Program,
	pipeline: u16,
	depth: ClauseDepth,
	subject: &Value,
	frame: &ExecutionContext,
	state: &mut State,
	exec: &ExecutionContext,
	event: &EventDefinition,
) -> FlowResult<Value> {
	let entered = enter_check(frame, depth)
		.and_then(|()| frame.dive_by(depth.write))
		.map(|frame| frame.entering_stored_code(0, true));
	let ran = match entered {
		Ok(frame) => run_image(program, pipeline, subject.clone(), Some(frame), state, exec).await,
		Err(e) => Err(ControlFlow::Err(e)),
	};
	match ran {
		Ok(value) | Err(ControlFlow::Return(value)) => Ok(value),
		Err(signal) => {
			// A UNIQUE-index conflict a clause's write raises is redacted before
			// the event's failure stringifies it: once folded into the message
			// `event_failure` builds, the typed `IndexExists` is gone and the
			// stream exit can no longer match it, so the stored record and its
			// indexed value would otherwise leak to a caller whose reads are
			// permission checked (GHSA-cff7-q66j-4hc8). The caller's own status
			// decides it, taken from the write boundary rather than this event's
			// privileged frame; reading the frame would miss a permission-checked
			// caller whose write fired an event that fired this one, since each
			// event frame suppresses the read check.
			let hide = exec.caller_view_checked();
			let signal = hide_conflict_record(signal, hide);
			Err(ControlFlow::Err(event_failure(&event.name, signal)))
		}
	}
}

/// The failure of a clause of event `event` that ended in `signal`, named by
/// the event.
///
/// An event is a barrier: a `BREAK` or `CONTINUE` leaving a clause has no loop
/// to target and is the misplaced-control-flow error, the one text every such
/// site answers. An error keeps its own text. A `RETURN` is a clause's value
/// and never a failure, so one reaching here is an internal error.
///
/// A storage failure under the clause's error stays the failure's source
/// ([`EventFailure`]), so the statement that fired the event classifies it as
/// it would outside one: a retryable conflict is run again.
fn event_failure(event: &str, signal: ControlFlow) -> anyhow::Error {
	let inner = match signal {
		ControlFlow::Break | ControlFlow::Continue => {
			anyhow::Error::new(crate::exec::Error::InvalidControlFlow)
		}
		ControlFlow::Err(e) => e,
		ControlFlow::Return(_) => anyhow::Error::new(crate::err::EngineError::Internal(
			"a RETURN was treated as the failure of an event clause".to_owned(),
		)),
	};
	anyhow::Error::new(EventFailure {
		event: event.to_owned(),
		inner,
	})
}

/// A clause of event `event` failed with `inner`. Its text is the event's name
/// and `inner`'s text. Its source is the storage failure under `inner`, if any,
/// and nothing else: the classifiers of storage failures see through the event,
/// while every other reader of the chain sees the event's failure alone, as a
/// typed error under it belongs to the clause rather than to the statement.
#[derive(Debug)]
struct EventFailure {
	event: String,
	inner: anyhow::Error,
}

impl std::fmt::Display for EventFailure {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "Error while processing event {}: {}", self.event, self.inner)
	}
}

impl std::error::Error for EventFailure {
	fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
		crate::kvs::storage_error(&self.inner).map(|e| e as &(dyn std::error::Error + 'static))
	}
}

#[cfg(test)]
mod tests {
	use super::{ClauseDepth, enter_check, event_failure};
	use crate::catalog::auth::{AuthLevel, AuthLimit};
	use crate::catalog::{EventDefinition, EventKind};
	use crate::doc::Action;
	use crate::exec::operators::test_util::root_ctx_with_options;
	use crate::exec::permission::{EventImages, event_frame};
	use crate::expr::ControlFlow;
	use crate::val::Value;

	fn event(auth: AuthLimit) -> EventDefinition {
		EventDefinition::new(
			"e".into(),
			"t".into(),
			crate::syn::expr("(true)").expect("a clause"),
			Vec::new(),
			EventKind::Sync,
			auth,
			None,
		)
	}

	/// The frame an event's clauses run in holds the budget its caller holds, whether or not the
	/// event narrows the caller. So the entry check `clause` makes on the frame and the one a
	/// dormant event makes on the caller answer alike, and the dive that follows the check in
	/// `clause` succeeds exactly when the check does. A failure is raised under the event's name.
	#[test]
	fn a_dormant_events_depth_check_is_the_one_its_clause_makes() {
		let narrowing =
			AuthLimit::new(AuthLevel::Namespace("test".to_owned()), Some("Viewer".to_owned()));
		let ctx = root_ctx_with_options();
		let budget = ctx.options().expect("the context has options").dive;
		assert!(budget > 0);
		for auth in [AuthLimit::new_no_limit(), narrowing] {
			let definition = event(auth);
			for consumed in 0..=budget.min(300) + 1 {
				let exec = ctx.entering_stored_code(consumed, false);
				let images = || EventImages {
					before: Value::from(1),
					after: Value::from(2),
					input: Value::from(3),
				};
				let (frame, _) =
					event_frame(&exec, &definition, Action::Update, images()).expect("a frame");
				assert_eq!(
					frame.options().map(|options| options.dive),
					exec.options().map(|options| options.dive),
					"the frame holds the budget of its caller"
				);
				for depth in [(0, 0), (1, 0), (1, 1), (4, 2), (9, 9), (250, 3), (255, 255)] {
					let depth = ClauseDepth {
						deepest: depth.0,
						write: depth.1,
					};
					let live = enter_check(&frame, depth);
					let dormant = enter_check(&exec, depth);
					assert_eq!(
						live.as_ref().map_err(ToString::to_string),
						dormant.as_ref().map_err(ToString::to_string),
						"{consumed} consumed, {depth:?}"
					);
					if live.is_ok() {
						assert!(
							frame.dive_by(depth.write).is_ok(),
							"the dive after the check, {consumed} consumed, {depth:?}"
						);
					}
					if let Err(error) = dormant {
						let raised = event_failure("e", ControlFlow::Err(error)).to_string();
						assert!(raised.starts_with("Error while processing event e: "), "{raised}");
					}
				}
			}
		}
	}

	const BARRIER: &str =
		"Invalid control flow statement, break or continue statement found outside of loop.";

	#[test]
	fn a_loop_signal_in_an_event_is_the_misplaced_control_flow_error() {
		for signal in [ControlFlow::Break, ControlFlow::Continue] {
			assert_eq!(
				event_failure("e", signal).to_string(),
				format!("Error while processing event e: {BARRIER}"),
			);
		}
		assert_eq!(
			event_failure("e", ControlFlow::Err(anyhow::anyhow!("boom"))).to_string(),
			"Error while processing event e: boom",
		);
	}

	#[test]
	fn a_storage_failure_in_an_event_keeps_its_classification() {
		let conflict = anyhow::Error::new(crate::kvs::Error::TransactionConflict(
			"written under the transaction".to_owned(),
		));
		let raised = event_failure("e", ControlFlow::Err(conflict));
		assert!(raised.to_string().starts_with("Error while processing event e: "), "{raised}");
		assert!(crate::kvs::is_retryable_transaction_conflict(&raised));
		let typed = anyhow::Error::new(crate::err::EngineError::Internal("x".to_owned()));
		let raised = event_failure("e", ControlFlow::Err(typed));
		assert_eq!(raised.chain().count(), 1, "only a storage failure is a source");
	}
}
