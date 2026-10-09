//! Everything that observes a completed write.
//!
//! Foreign materialised views, event bodies, live-query notifications and the
//! change feed.
//!
//! Every stage here runs after the record reached storage and reads the
//! record *with* its `COMPUTED` values, which `materialise_observed_fields`
//! put back.
//!
//! The authority each body runs under differs by observer, and none of them
//! is the writer's. A view's condition and projection, and an event's `WHEN`
//! and `THEN`, run with table and field permission enforcement disabled for
//! the whole loop; on top of that an event body — alone among the four — is
//! narrowed to the authorisation limit its `DEFINE EVENT` recorded, which is
//! ADR 0001's definer's-rights obligation. A view definition carries no such
//! limit, so disabling enforcement is the whole of its frame. The live-query
//! reduction is neither: it is per subscriber, and resolves one view per
//! subscription auth rather than sharing the context's.
//!
//! # What suppresses an observer
//!
//! Two conditions suppress all four alike, and each stage checks both: an
//! import, and a record whose post-image equals its pre-image. The second is
//! what makes an observer's subject a *change* rather than a write — a
//! statement that stored a record identical to the one it read notifies
//! nobody.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use surrealdb_datastore::values::event_queue::AsyncEventRecord;
use surrealdb_kvs::timestamp::HlcTimeStamp;

use crate::catalog::{EventDefinition, Record};
use crate::doc::{Action, Error as DocError};
use crate::exe::FlowResultExt;
use crate::exec::operators::write::apply::materialise_input;
use crate::exec::operators::write::ctx::WriteCtx;
use crate::exec::operators::write::row::WriteRow;
use crate::exec::operators::write::stage::{StageId, StageOutcome, WriteStage};
use crate::exec::permission::{EventImages, event_frame, event_physical};
use crate::exec::{BoxFut, Error as ExecError, EvalContext, ExecutionContext, PhysicalExpr};
use crate::expr::{ChangeFeed, ControlFlow};
use crate::key::schema::EventQueueKey;
use crate::kvs::{LiveQueryEngine, Transaction};
use crate::val::Value;

/// This module's stages, by id.
///
/// `None` is a stage whose body has not landed:
/// [`WritePlan::resolve`](crate::exec::operators::write::plan::WritePlan)
/// refuses to bind a plan that names one, rather than running a partial order.
// `stages::lookup` is the total router over `StageId` and holds no wildcard, so
// a new stage still has to be given an owner before it compiles. The wildcard
// below is the arm for a stage this module does not own; enumerating every
// other variant here would restate `lookup`'s table in nine places and go stale
// independently of it.
#[allow(clippy::wildcard_enum_match_arm)]
pub(crate) fn stage(id: StageId) -> Option<Arc<dyn WriteStage>> {
	match id {
		StageId::ProcessChangefeeds => Some(Arc::new(ProcessChangefeeds)),
		StageId::ProcessTableEvents(action) => Some(Arc::new(ProcessTableEvents(action))),
		// `super::lookup` routes only this module's stages here, so anything
		// else is a routing mistake rather than a missing body. Answering
		// `None` leaves it to `resolve` to name, as it names any unbound stage.
		_ => None,
	}
}

/// Whether this statement replays a dump.
///
/// An import restores state that was observed when it was first written, so it
/// runs no observer: no view is recomputed, no event body fires, no subscriber
/// is notified, and nothing is appended to a change feed.
fn is_import(ctx: &WriteCtx) -> bool {
	ctx.exec().options().is_some_and(|options| options.import)
}

/// Whether a completed write has anything for an observer to see.
///
/// The record comparison is memoised on the row, so the four observers of one
/// record share a single deep compare.
pub(crate) fn is_observed(row: &WriteRow, ctx: &WriteCtx) -> bool {
	!is_import(ctx) && row.is_modified()
}

/// Run the table's `DEFINE EVENT` bodies for this record.
///
/// The second of the four observers on every order that has them: after the
/// foreign views are recomputed and before the subscriptions are notified,
/// and — like every stage in this module — after the record reached storage,
/// so a body reads the record as a reader of the table would, `COMPUTED`
/// values and all.
///
/// The payload is the action the order was assembled for, which is what
/// `$event` names and which decides the image the event is about.
///
/// # What it guarantees
///
/// Each of the table's events is taken in the order the catalog holds them,
/// and for each:
///
/// - its `WHEN` is evaluated and decides alone whether the event fires. An omitted `WHEN` is stored
///   as the literal `true`, so there is no absent case to special-case;
/// - a synchronous event's `THEN` clauses are then compiled and evaluated in order, for their
///   effects. A clause's value is discarded — an event body is a statement, not a projection.
///   Compiling them is held behind the `WHEN`, so a body the planner declines fails the statement
///   only where it would have run. An `ASYNC` event's are neither compiled nor run here; see below;
/// - both run in the frame [`event_frame`] derives: permission enforcement off, writes allowed, the
///   definer's `AUTH LIMIT` applied, and `$before` / `$after` / `$event` / `$value` / `$input`
///   bound. `$value` is the pre-image on a delete and the post-image otherwise, and is also the
///   document a bare field path in a body reads;
/// - a `RETURN` in either clause is that clause's value rather than the statement's. A failure is
///   the statement's: it aborts the write and every record already written, under the message the
///   event is named in.
///
/// An import and an unmodified record run no event, exactly as they run no
/// other observer — see [`is_observed`].
///
/// # An `ASYNC` event is queued
///
/// An `ASYNC` event's bodies are not run here. A firing one is persisted to the
/// event queue by [`queue_async_event`] and drained out-of-band, in its own
/// transaction and under its own retry and nesting budget.
/// Only the `WHEN` runs on the write path, and it decides whether an entry is
/// queued exactly as it decides whether a synchronous body runs; the `THEN`
/// clauses are never compiled here, because the queued payload carries the
/// definition's stored text and the drain compiles it for itself.
///
/// The entry is written through the statement's own transaction, so a
/// statement that goes on to fail queues nothing — an event is about a change,
/// and a change that was rolled back did not happen.
#[derive(Debug)]
struct ProcessTableEvents(Action);

impl WriteStage for ProcessTableEvents {
	fn name(&self) -> &'static str {
		StageId::ProcessTableEvents(self.0).name()
	}

	fn attrs(&self) -> Vec<(String, String)> {
		vec![("action".to_string(), action_name(self.0).to_string())]
	}

	fn apply<'a>(
		&'a self,
		row: &'a mut WriteRow,
		ctx: &'a WriteCtx,
	) -> BoxFut<'a, Result<StageOutcome, ControlFlow>> {
		Box::pin(async move {
			if !is_observed(row, ctx) || ctx.events.is_empty() {
				return Ok(StageOutcome::Continue);
			}
			// What the statement supplied, materialised once for every event
			// that reads it.
			let input = input_value(row, ctx).await?;
			let exec = ctx.exec();
			// Txn-less, as the write gates compile with: a planner that
			// resolves no table context cannot recurse into a body naming the
			// table the event is defined on, and a stage running per record
			// shares no parent cycle guard that would break the recursion.
			let planner = crate::exec::vm::descriptor::write_unit_planner(
				exec.ctx(),
				exec.function_registry(),
			);
			for event in ctx.events.iter() {
				let (frame, subject) = event_frame(
					exec,
					event,
					self.0,
					EventImages {
						before: row.initial.clone(),
						after: row.current.clone(),
						input: input.clone(),
					},
				)
				.map_err(ControlFlow::Err)?;
				let compiled = event_physical(event, &planner)
					.await
					.map_err(|e| ControlFlow::Err(e.into()))?;
				if !run_clause(&compiled.when, &frame, &subject, event).await?.is_truthy() {
					continue;
				}
				if event.is_async() {
					queue_async_event(row, ctx, &frame, event, &subject, self.0).await?;
					continue;
				}
				// The bodies of a firing event, compiled here rather than
				// alongside the `WHEN`: see `PhysicalEvent::then`.
				let then = compiled.then().await.map_err(|e| ControlFlow::Err(e.into()))?;
				for clause in &then {
					run_clause(clause, &frame, &subject, event).await?;
				}
			}
			Ok(StageOutcome::Continue)
		})
	}
}

/// The value `$input` names for this record: what the *statement* supplied,
/// materialised from the evaluated data clause rather than read off the record
/// the write ended up with. `NONE` for a form that supplies no clause —
/// `DELETE` is one, and its order carries no `compute_input_data` at all — and
/// for one whose clause has not been evaluated, which no observer can reach:
/// every order that evaluates a clause runs `compute_input_data` long before
/// the record is stored.
///
/// Taken by exclusive reference rather than shared: a `&WriteRow` held across
/// an await would require `WriteRow: Sync`, which its memoised comparison cell
/// deliberately is not. Nothing here mutates the row.
pub(crate) async fn input_value(row: &mut WriteRow, ctx: &WriteCtx) -> Result<Value, ControlFlow> {
	let Some(input) = row.input.as_ref() else {
		return Ok(Value::None);
	};
	// `INSERT`'s `ON DUPLICATE KEY UPDATE` is the clause the updating order
	// evaluates, and the only clause it has.
	let data =
		ctx.clauses.data.as_ref().or(ctx.clauses.duplicate_update.as_ref()).ok_or_else(|| {
			ControlFlow::Err(anyhow::anyhow!("an evaluated data clause with no clause behind it"))
		})?;
	let materialised = materialise_input(data, input, ctx.exec()).await?;
	Ok(Value::clone(&materialised))
}

/// The action an observer was built for, as `EXPLAIN` renders it.
fn action_name(action: Action) -> &'static str {
	match action {
		Action::Create => "create",
		Action::Update => "update",
		Action::Delete => "delete",
	}
}

/// Evaluate one clause of `event` against the image the event is about.
///
/// The image is bound as both the evaluated value and the document root, which
/// is how every caller of a stored clause binds a record: a bare field path in
/// the body reads it, and `$parent` inside an idiom-level filter resolves to
/// it.
///
/// A `RETURN` in the clause is the clause's value. Every other failure — a
/// thrown error, a `BREAK` or `CONTINUE` with no loop to leave — fails the
/// statement under the event's name, which is the phrasing the definition's
/// author sees.
async fn run_clause(
	clause: &Arc<dyn PhysicalExpr>,
	frame: &ExecutionContext,
	subject: &Value,
	event: &EventDefinition,
) -> Result<Value, ControlFlow> {
	clause
		.evaluate(EvalContext::from_exec_ctx(frame).with_value_and_doc(subject))
		.await
		.catch_return()
		.map_err(|e| {
			ControlFlow::Err(anyhow::anyhow!("Error while processing event {}: {e}", event.name))
		})
}

/// Persist a firing `ASYNC` event to the event queue, for the drain in
/// [`crate::doc::process_next_events_batch`] to run out-of-band.
///
/// The entry goes through the statement's own transaction, so the queued event
/// becomes visible exactly when the change it is about does: the commit that
/// makes the record readable is the one that makes the entry drainable, and a
/// statement that fails afterwards leaves neither behind.
///
/// The key orders the queue and makes the entry unique: an HLC timestamp and
/// this node's id, under the namespace and database ids the write resolved and
/// the table and event names the definition carries. The table named is the
/// event's own `target_table`, the table the definition is attached to, so the
/// entry addresses the definition rather than the statement that fired it.
///
/// `trigger_async_event` marks the transaction so the drain is woken once the
/// commit publishes the entry, rather than waiting for its next poll.
///
/// The row is taken by exclusive reference for the reason [`input_value`] is:
/// a `&WriteRow` held across an await would require `WriteRow: Sync`, which its
/// memoised comparison cell deliberately is not. Nothing here mutates the row.
pub(crate) async fn queue_async_event(
	row: &mut WriteRow,
	ctx: &WriteCtx,
	frame: &ExecutionContext,
	event: &EventDefinition,
	subject: &Value,
	action: Action,
) -> Result<(), ControlFlow> {
	let queued = queued_payload(row, frame, event, subject, action)?;
	let key = EventQueueKey {
		ns: ctx.db.namespace_id,
		db: ctx.db.database_id,
		tb: Cow::Borrowed(&event.target_table),
		ev: Cow::Borrowed(&event.name),
		ts: HlcTimeStamp::next().0,
		node_id: frame.ctx().node_id(),
	};
	let txn = ctx.txn();
	txn.put_key(&key, &queued).await.map_err(ControlFlow::Err)?;
	txn.trigger_async_event();
	Ok(())
}

/// Everything the drain needs to run this event's bodies without the statement
/// that fired it: the record the event is about, the frame that record was
/// observed in, and the definition as it stood.
///
/// The payload has to be self-contained, because the drain may run it on
/// another node, after a restart, and long after the writing transaction is
/// gone. Each field is the frame this stage evaluates in, frozen:
///
/// - `perms` is `false`, and is a property of an event body rather than of this write: a body runs
///   with table and field enforcement disabled, which is what the `WHEN` above it has just been
///   judged under. The execution frame expresses that as `skip_fetch_perms` rather than in its
///   options, so it is stated here rather than read off them.
/// - `auth_with_limit` is the frame's own auth, which [`event_frame`] has already narrowed to the
///   `AUTH LIMIT` the `DEFINE EVENT` recorded. That narrowing is ADR 0001's definer's-rights
///   obligation, and the drain must not be able to widen it.
/// - `values` is the whole parameter chain the `WHEN` saw — `$before`, `$after`, `$event`, `$value`
///   and `$input`, over the session's own values — because a body reads them and nothing else will
///   rebuild them.
/// - `ns` and `db` are *names*. The drain re-resolves them and refuses the entry if either no
///   longer carries the id its key spells, which is how a queue entry that outlived the database it
///   was written in is discarded rather than run against a reused id.
/// - `event_depth` is one deeper than the frame's, and `max_depth` is checked against the frame's
///   own depth before anything is written. An event queued from inside a queued event is what that
///   budget bounds, and reaching it fails the statement, because a silently dropped event is a
///   write the definition promised and did not make.
///
/// `fields_computed` records whether the snapshot already carries its
/// `COMPUTED` values, so the drain reads the record this write observed rather
/// than deriving the bodies a second time against a different frame. It is
/// read off the image the action makes the subject, each of which carries its
/// own flag: `materialise_observed_fields` sets them where it runs, and an
/// order that reaches this stage without it — view maintenance is one — leaves
/// both at `false`, which is the truthful answer for a snapshot whose bodies
/// were never derived. The table declaring a computed field does not answer
/// this question.
fn queued_payload(
	row: &WriteRow,
	frame: &ExecutionContext,
	event: &EventDefinition,
	subject: &Value,
	action: Action,
) -> Result<AsyncEventRecord, ControlFlow> {
	// Every context a statement runs in carries the options mirror; only a
	// bare test context is built without one, and it reaches no event.
	let options = frame.options().ok_or_else(|| {
		ControlFlow::Err(anyhow::anyhow!(ExecError::Thrown(
			"Options not available to queue an ASYNC event".into()
		)))
	})?;
	let definition = event.stored();
	let depth = options.async_event_depth();
	if let Some(depth) = depth
		&& depth >= definition.max_depth()
	{
		return Err(ControlFlow::Err(anyhow::anyhow!(DocError::EvReachMaxDepth(
			definition.name.to_string(),
			depth
		))));
	}
	let (ns, db) = options.arc_ns_db().map_err(ControlFlow::Err)?;
	let (metadata, fields_computed) = match action {
		Action::Delete => (row.initial_metadata.clone(), row.initial_fields_computed),
		Action::Create | Action::Update => (row.metadata.clone(), row.fields_computed),
	};
	Ok(AsyncEventRecord {
		attempt: 0,
		event_depth: depth.map(|d| d + 1).unwrap_or(0),
		rid: row.id.clone().map(Arc::new),
		cursor_record: Arc::new(Record {
			metadata,
			data: subject.clone(),
		}),
		fields_computed,
		ns,
		db,
		perms: false,
		auth_enabled: frame.auth_enabled(),
		values: frame.ctx().collect_values(HashMap::new()),
		auth_with_limit: Arc::clone(&frame.root().auth),
		event_definition: definition.clone(),
	})
}

/// The change-feed definition a write on this table appends to.
///
/// The database's definition outranks the table's, and a table that declares
/// its own does not narrow it: a database-level `CHANGEFEED` records every
/// table under it on that database's terms.
fn changefeed_definition(ctx: &WriteCtx) -> Option<&ChangeFeed> {
	ctx.db.changefeed.as_ref().or(ctx.table.changefeed.as_ref())
}

/// Append this record's change to the table's change feed, and capture the
/// live-query event the router engine replays from.
///
/// Last of the four observers on every order that has them, and so the last
/// stage before the statement's output gate. Both halves buffer on the
/// transaction and are flushed at commit with the commit versionstamp, so a
/// statement that goes on to fail appends nothing.
///
/// The images are stored as the record stands here — with the `COMPUTED`
/// values `materialise_observed_fields` put back, and each with the
/// record-type marker its own side carried — because a change feed's reader is
/// shown the record a reader of the table would have been shown.
#[derive(Debug)]
struct ProcessChangefeeds;

impl WriteStage for ProcessChangefeeds {
	fn name(&self) -> &'static str {
		StageId::ProcessChangefeeds.name()
	}

	fn apply<'a>(
		&'a self,
		row: &'a mut WriteRow,
		ctx: &'a WriteCtx,
	) -> BoxFut<'a, Result<StageOutcome, ControlFlow>> {
		Box::pin(async move {
			record_change(row, ctx).await?;
			Ok(StageOutcome::Continue)
		})
	}
}

/// Count `values` against the query's memory, which the transaction's
/// buffers hold until it commits. The query waits or overcommits, and fails
/// only where its pinned bytes would pass its ceiling
/// ([`QueryMemory::hold_unspillable`]).
///
/// [`QueryMemory::hold_unspillable`]: crate::exec::memory::QueryMemory::hold_unspillable
async fn held(
	memory: Option<&Arc<crate::exec::memory::QueryMemory>>,
	values: &[&crate::val::Value],
	ctx: &WriteCtx,
) -> Result<(), ControlFlow> {
	if let Some(memory) = memory {
		let bytes = values.iter().map(|v| crate::exec::memory::size::value_bytes(v)).sum();
		memory.hold_unspillable(bytes, &ctx.exec).await?;
	}
	Ok(())
}

/// Buffer this record's change for the table's change feed, and capture the
/// live-query event the router engine replays from: the effect
/// [`ProcessChangefeeds`] takes, and the one the VM's write body asks its host
/// for. Nothing is buffered for a write no observer sees.
pub(crate) async fn record_change(row: &mut WriteRow, ctx: &WriteCtx) -> Result<(), ControlFlow> {
	if !is_observed(row, ctx) {
		return Ok(());
	}
	// A change is keyed by the record it happened to. A row with no id
	// has nothing to key an entry on, and is passed over rather than
	// refused: the write itself already stands.
	let Some(id) = row.id.as_ref() else {
		return Ok(());
	};
	let txn = ctx.txn();
	// What the transaction buffers until it commits counts against the
	// query's memory.
	let memory = ctx.exec.ctx().memory().cloned();
	if let Some(changefeed) = changefeed_definition(ctx) {
		held(memory.as_ref(), &[&row.initial, &row.current], ctx).await?;
		txn.changefeed_buffer_record_change(
			ctx.db.namespace_id,
			ctx.db.database_id,
			&ctx.table.name,
			id,
			Arc::new(Record {
				metadata: row.initial_metadata.clone(),
				data: row.initial.clone(),
			}),
			Arc::new(Record {
				metadata: row.metadata.clone(),
				data: row.current.clone(),
			}),
			changefeed.store_diff,
		);
	}
	// The live-query capture is independent of the feed above and
	// runs whether or not one is defined.
	if captures_live_events(ctx, &txn).await? {
		held(memory.as_ref(), &[&row.initial, &row.current], ctx).await?;
		txn.live_event_buffer_record_change(
			ctx.db.namespace_id,
			ctx.db.database_id,
			&ctx.table.name,
			id,
			row.initial.clone(),
			row.current.clone(),
		);
	}
	Ok(())
}

/// Whether this write captures its record change into the live-query keyspace
/// ([`crate::lq`]), for the router engine to replay off the write path.
///
/// The capture is independent of the change feed: it writes its own keyspace,
/// honours no `CHANGEFEED` clause, retains the full before and after values
/// whatever the feed's `store_diff`, and affects neither changefeed retention
/// nor `SHOW CHANGES`.
///
/// It happens only under the router engine, and only for a table some node is
/// subscribed to — which is what keeps the write path's added cost
/// proportional to demand. The subscription test reads the committed
/// subscription rows within this write's own snapshot rather than a
/// node-local cache, so a subscriber registered on another node counts; it is
/// a limit-one key scan, so its cost is independent of how many subscribers
/// there are.
async fn captures_live_events(ctx: &WriteCtx, txn: &Transaction) -> Result<bool, ControlFlow> {
	if ctx.exec().ctx().config.datastore.live_query_engine != LiveQueryEngine::Router {
		return Ok(false);
	}
	txn.table_has_live_query(ctx.db.namespace_id, ctx.db.database_id, &ctx.table.name)
		.await
		.map_err(ControlFlow::Err)
}

#[cfg(all(test, feature = "kv-mem"))]
#[allow(clippy::unwrap_used)]
mod tests {
	use super::*;
	use crate::catalog::providers::TableProvider;
	use crate::catalog::{Metadata, RecordType};
	use crate::exec::operators::test_util::{TestDb, physical_expr, val};
	use crate::exec::operators::write::clauses::{WriteClauses, WriteData};
	use crate::exec::operators::write::ctx::TableExistence;
	use crate::exec::operators::write::row::ComputedInput;
	use crate::key::schema::EventQueuePrefix;
	use crate::key::{KVKeyDecode, KVValue};
	use crate::kvs::TransactionType;
	use crate::val::{Object, RecordId, RecordIdKey};

	/// A database-level context on `test`/`test` over a write transaction.
	async fn exec_ctx(db: &TestDb) -> ExecutionContext {
		db.exec_ctx_as(&TestDb::owner(), TransactionType::Write).await
	}

	/// The write context for table `t`.
	async fn write_ctx(exec: &ExecutionContext) -> WriteCtx {
		write_ctx_with(exec, WriteClauses::default()).await
	}

	/// The write context for table `t`, over a statement carrying `clauses`.
	async fn write_ctx_with(exec: &ExecutionContext, clauses: WriteClauses) -> WriteCtx {
		WriteCtx::resolve(exec, &"t".into(), TableExistence::Required, None, Arc::new(clauses))
			.await
			.expect("the write context for an existing table resolves")
	}

	/// The same context as a statement replaying a dump.
	fn as_import(exec: &ExecutionContext) -> ExecutionContext {
		let mut patched = exec.clone();
		let ExecutionContext::Database(database) = &mut patched else {
			panic!("TestDb hands out a database-level context");
		};
		let root = &mut database.ns_ctx.root;
		let options = root.options.as_ref().expect("the test context carries options");
		root.options = Some(options.new_with_import(true));
		patched
	}

	/// `t:<n>`, as a record carrying its own id.
	fn record(n: i64) -> Value {
		let id = RecordId {
			table: "t".into(),
			key: RecordIdKey::Number(n),
		};
		let mut object = Object::default();
		object.insert("id", Value::RecordId(id));
		object.insert("n", Value::from(n));
		Value::Object(object)
	}

	/// A row whose post-image differs from its pre-image.
	fn changed_row(n: i64) -> WriteRow {
		let mut row = WriteRow::for_record(record(n)).expect("a record value makes a row");
		row.current = record(n + 1);
		row
	}

	/// The order lists name their stages, and `EXPLAIN` renders those names, so
	/// the body must answer to the name its id carries.
	#[test]
	fn the_stage_answers_to_the_name_its_id_carries() {
		let stage = stage(StageId::ProcessChangefeeds).expect("the change feed stage has a body");
		assert_eq!(stage.name(), "process_changefeeds");
		assert_eq!(stage.name(), StageId::ProcessChangefeeds.name());
	}

	/// A database-level `CHANGEFEED` governs every table under it. A table that
	/// declares one of its own does not narrow it, so a database feed storing
	/// whole records is not turned into a diff feed by the table's clause.
	#[tokio::test]
	async fn a_database_change_feed_outranks_the_table_s() {
		let db = TestDb::new(
			"DEFINE DATABASE OVERWRITE test CHANGEFEED 1h;
			 DEFINE TABLE t SCHEMALESS CHANGEFEED 1h INCLUDE ORIGINAL;",
		)
		.await;
		let exec = exec_ctx(&db).await;
		let ctx = write_ctx(&exec).await;
		assert!(
			ctx.table.changefeed.as_ref().is_some_and(|feed| feed.store_diff),
			"the table's own clause must ask for diffs, or this proves nothing"
		);
		let resolved = changefeed_definition(&ctx).expect("a feed is defined");
		assert!(!resolved.store_diff, "the database's definition must be the one that applies");
	}

	/// With no feed on either the database or the table there is nothing to
	/// append to, and the stage says so rather than inventing one.
	#[tokio::test]
	async fn a_table_with_no_change_feed_has_no_definition() {
		let db = TestDb::new("DEFINE TABLE t SCHEMALESS;").await;
		let exec = exec_ctx(&db).await;
		let ctx = write_ctx(&exec).await;
		assert!(changefeed_definition(&ctx).is_none());
	}

	/// A statement that stored a record identical to the one it read changed
	/// nothing an observer could see.
	#[tokio::test]
	async fn a_record_that_did_not_change_is_not_observed() {
		let db = TestDb::new("DEFINE TABLE t SCHEMALESS CHANGEFEED 1h;").await;
		let exec = exec_ctx(&db).await;
		let ctx = write_ctx(&exec).await;
		let unchanged = WriteRow::for_record(record(1)).expect("a record value makes a row");
		assert!(!is_observed(&unchanged, &ctx));
		assert!(is_observed(&changed_row(1), &ctx));
	}

	/// An import runs no observer, however much the record changed.
	#[tokio::test]
	async fn an_import_runs_no_observer() {
		let db = TestDb::new("DEFINE TABLE t SCHEMALESS CHANGEFEED 1h;").await;
		let exec = exec_ctx(&db).await;
		let ctx = write_ctx(&as_import(&exec)).await;
		let row = changed_row(1);
		assert!(row.is_modified(), "the row must have changed, or this proves nothing");
		assert!(!is_observed(&row, &ctx));
	}

	/// A datastore running the router engine.
	async fn router_db(setup: &str) -> TestDb {
		TestDb::new_with_config(
			setup,
			surrealdb_cnf::ConfigMap::empty().with_key_value("live_query_engine", "router"),
		)
		.await
	}

	/// Register a subscription on `t`, which is what the capture gate scans for.
	async fn subscribe(db: &TestDb) {
		db.run_as(&TestDb::owner().with_rt(true), "LIVE SELECT * FROM t").await;
	}

	/// The live-query capture is gated on the engine *and* on demand: the inline
	/// engine writes the keyspace never, and the router engine writes it only for
	/// a table some node is subscribed to. That second gate is what keeps the
	/// write path's added cost proportional to demand, so a capture on an
	/// unsubscribed table is as wrong as no capture on a subscribed one.
	#[tokio::test]
	async fn only_the_router_engine_captures_and_only_a_subscribed_table() {
		let schema = "DEFINE TABLE t SCHEMALESS;";

		let inline = TestDb::new(schema).await;
		subscribe(&inline).await;
		let exec = exec_ctx(&inline).await;
		let ctx = write_ctx(&exec).await;
		assert!(
			!captures_live_events(&ctx, &ctx.txn()).await.unwrap(),
			"the inline engine captures nothing, however many subscribers a table has"
		);

		let unwatched = router_db(schema).await;
		let exec = exec_ctx(&unwatched).await;
		let ctx = write_ctx(&exec).await;
		assert!(
			!captures_live_events(&ctx, &ctx.txn()).await.unwrap(),
			"a table nobody is subscribed to is not captured"
		);

		let watched = router_db(schema).await;
		subscribe(&watched).await;
		let exec = exec_ctx(&watched).await;
		let ctx = write_ctx(&exec).await;
		assert!(
			captures_live_events(&ctx, &ctx.txn()).await.unwrap(),
			"a subscribed table under the router engine is captured"
		);
	}

	/// Passing over a record is not failing on it. A table with no feed, an
	/// import, and a row with no id each leave the statement running: the
	/// record's write already stands, and refusing here would discard it along
	/// with every record the statement wrote before it.
	#[tokio::test]
	async fn nothing_the_stage_declines_to_append_fails_the_statement() {
		let db = TestDb::new("DEFINE TABLE t SCHEMALESS;").await;
		let exec = exec_ctx(&db).await;
		let ctx = write_ctx(&exec).await;
		let stage = stage(StageId::ProcessChangefeeds).expect("the change feed stage has a body");

		let mut no_feed = changed_row(1);
		assert_eq!(stage.apply(&mut no_feed, &ctx).await.unwrap(), StageOutcome::Continue);

		let import_ctx = write_ctx(&as_import(&exec)).await;
		let mut imported = changed_row(1);
		assert_eq!(stage.apply(&mut imported, &import_ctx).await.unwrap(), StageOutcome::Continue);

		// A creating order whose id derivation has not run yet has a changed
		// image and nothing to key an entry on.
		let mut without_id =
			WriteRow::for_target(Value::Table("t".into())).expect("a table target makes a row");
		without_id.current = record(1);
		assert!(without_id.id.is_none());
		assert_eq!(stage.apply(&mut without_id, &ctx).await.unwrap(), StageOutcome::Continue);
	}

	// =======================================================================
	// process_table_events
	// =======================================================================

	/// The event stage for `action`.
	fn events(action: Action) -> Arc<dyn WriteStage> {
		stage(StageId::ProcessTableEvents(action)).expect("the event stage has a body")
	}

	/// Run the event stage over `row`, returning the failure it raised.
	async fn event_failure(action: Action, row: &mut WriteRow, ctx: &WriteCtx) -> String {
		let failure = events(action).apply(row, ctx).await.expect_err("the event fails");
		let ControlFlow::Err(err) = failure else {
			panic!("an event failure is an error, not a loop signal");
		};
		err.to_string()
	}

	/// What storage holds for a record the event body wrote.
	async fn stored(ctx: &WriteCtx, table: &str, key: i64) -> Value {
		ctx.txn()
			.get_record(
				ctx.ns.namespace_id,
				ctx.db.database_id,
				&table.into(),
				&RecordIdKey::Number(key),
				None,
			)
			.await
			.expect("the record reads")
			.data
			.clone()
	}

	/// Each order names the action it was assembled for, and `EXPLAIN` renders
	/// it: the three stages share one name and differ only in that payload.
	#[test]
	fn the_event_stage_renders_the_action_it_was_built_for() {
		for (action, rendered) in
			[(Action::Create, "create"), (Action::Update, "update"), (Action::Delete, "delete")]
		{
			let stage = events(action);
			assert_eq!(stage.name(), "process_table_events");
			assert_eq!(stage.name(), StageId::ProcessTableEvents(action).name());
			assert_eq!(stage.attrs(), vec![("action".to_string(), rendered.to_string())]);
		}
	}

	/// The `WHEN` clause decides alone whether the event fires, and a body that
	/// throws fails the statement under the event's name.
	#[tokio::test]
	async fn the_when_clause_decides_whether_the_body_runs() {
		let db = TestDb::new(
			"DEFINE TABLE t SCHEMALESS;
			 DEFINE EVENT ev ON t WHEN $after.n = 2 THEN { THROW 'fired' };",
		)
		.await;
		let exec = exec_ctx(&db).await;
		let ctx = write_ctx(&exec).await;

		let mut fires = changed_row(1);
		let failure = event_failure(Action::Update, &mut fires, &ctx).await;
		assert!(
			failure.contains("Error while processing event ev") && failure.contains("fired"),
			"the failure names the event and carries the body's error: {failure}"
		);

		let mut quiet = changed_row(5);
		assert_eq!(
			events(Action::Update).apply(&mut quiet, &ctx).await.unwrap(),
			StageOutcome::Continue
		);
	}

	/// Every `THEN` runs, in the order the definition lists them, and each is
	/// run for its effect — the value it produces is discarded.
	#[tokio::test]
	async fn every_then_clause_runs_in_order() {
		let db = TestDb::new(
			"DEFINE TABLE t SCHEMALESS;
			 DEFINE TABLE log SCHEMALESS;
			 DEFINE EVENT ev ON t WHEN true
			   THEN (CREATE log:1 SET step = 1), (CREATE log:2 SET step = 2);",
		)
		.await;
		let exec = exec_ctx(&db).await;
		let ctx = write_ctx(&exec).await;
		let mut row = changed_row(1);
		assert_eq!(
			events(Action::Update).apply(&mut row, &ctx).await.unwrap(),
			StageOutcome::Continue
		);
		assert_eq!(stored(&ctx, "log", 1).await, val("{ id: log:1, step: 1 }").await);
		assert_eq!(stored(&ctx, "log", 2).await, val("{ id: log:2, step: 2 }").await);

		// Order is what the throwing bodies prove: the first clause is the one
		// that raises, so the second never ran.
		let db = TestDb::new(
			"DEFINE TABLE t SCHEMALESS;
			 DEFINE EVENT ev ON t WHEN true THEN { THROW 'first' }, { THROW 'second' };",
		)
		.await;
		let exec = exec_ctx(&db).await;
		let ctx = write_ctx(&exec).await;
		let mut row = changed_row(1);
		let failure = event_failure(Action::Update, &mut row, &ctx).await;
		assert!(failure.contains("first"), "the first THEN raises: {failure}");
		assert!(!failure.contains("second"), "the second THEN never ran: {failure}");
	}

	/// The five parameters an event body reads are bound, and `$input` is what
	/// the statement supplied rather than what the record ended up holding.
	#[tokio::test]
	async fn the_body_reads_the_five_parameters() {
		let db = TestDb::new(
			"DEFINE TABLE t SCHEMALESS;
			 DEFINE EVENT ev ON t
			   WHEN $event = 'UPDATE' AND $before.n = 1 AND $after.n = 2
			        AND $value.n = 2 AND $input.n = 7
			   THEN { THROW 'bound' };",
		)
		.await;
		let exec = exec_ctx(&db).await;
		let supplied = Arc::new(val("{ n: 7 }").await);
		let clauses = WriteClauses {
			data: Some(WriteData::Content(physical_expr("{ n: 7 }", &exec).await)),
			..Default::default()
		};
		let ctx = write_ctx_with(&exec, clauses).await;
		let mut row = changed_row(1);
		row.input = Some(ComputedInput::Content(supplied));
		let failure = event_failure(Action::Update, &mut row, &ctx).await;
		assert!(failure.contains("bound"), "every parameter resolved: {failure}");
	}

	/// A delete's event is about the pre-image: `$value` names it, and so does
	/// the document a bare field path reads.
	#[tokio::test]
	async fn a_delete_reads_the_pre_image() {
		let db = TestDb::new(
			"DEFINE TABLE t SCHEMALESS;
			 DEFINE EVENT ev ON t WHEN $event = 'DELETE' AND n = 1 AND $value.n = 1
			   THEN { THROW 'deleted' };",
		)
		.await;
		let exec = exec_ctx(&db).await;
		let ctx = write_ctx(&exec).await;
		let mut row = WriteRow::for_record(record(1)).expect("a record value makes a row");
		row.current = Value::None;
		let failure = event_failure(Action::Delete, &mut row, &ctx).await;
		assert!(failure.contains("deleted"), "the pre-image answered: {failure}");
	}

	/// An import and an unchanged record run no event, as they run no other
	/// observer — a body that would fail the statement never runs.
	#[tokio::test]
	async fn an_import_and_an_unchanged_record_run_no_event() {
		let db = TestDb::new(
			"DEFINE TABLE t SCHEMALESS;
			 DEFINE EVENT ev ON t WHEN true THEN { THROW 'fired' };",
		)
		.await;
		let exec = exec_ctx(&db).await;
		let ctx = write_ctx(&exec).await;
		let stage = events(Action::Update);

		let mut unchanged = WriteRow::for_record(record(1)).expect("a record value makes a row");
		assert_eq!(stage.apply(&mut unchanged, &ctx).await.unwrap(), StageOutcome::Continue);

		let import_ctx = write_ctx(&as_import(&exec)).await;
		let mut imported = changed_row(1);
		assert_eq!(stage.apply(&mut imported, &import_ctx).await.unwrap(), StageOutcome::Continue);
	}

	/// Everything the event queue holds, read through the write transaction so
	/// the stage's own uncommitted entries are visible.
	async fn queued(ctx: &WriteCtx) -> Vec<(Vec<u8>, Vec<u8>)> {
		let range = EventQueuePrefix {}.range().expect("the queue range builds");
		ctx.txn().scan_raw(range, 100, 0, None).await.expect("the queue scans")
	}

	/// `t:1` as an update stored it, changed from the record it read.
	async fn updated_row() -> WriteRow {
		let mut row =
			WriteRow::for_record(val("{ id: t:1, n: 1 }").await).expect("a record makes a row");
		row.current = val("{ id: t:1, n: 2 }").await;
		row
	}

	/// An `ASYNC` event whose `WHEN` fires is written to the event queue rather
	/// than run: the body does not execute on the write path, and the entry the
	/// drain will run it from is in the statement's own transaction.
	///
	/// The entry has to carry everything the drain needs without the statement
	/// that queued it — the record the event is about, the parameters the body
	/// reads, the namespace and database by name, and the definition as it
	/// stood — so the payload is read back and checked field by field rather
	/// than merely counted.
	#[tokio::test]
	async fn an_async_event_is_queued_rather_than_run() {
		let db = TestDb::new(
			"DEFINE TABLE t SCHEMALESS;
			 DEFINE TABLE log SCHEMALESS;
			 DEFINE EVENT ev ON t ASYNC WHEN $after.n = 2 THEN (CREATE log:1 SET step = 1);",
		)
		.await;
		let exec = exec_ctx(&db).await;
		let ctx = write_ctx(&exec).await;
		assert!(
			ctx.events.iter().all(|ev| ev.is_async()),
			"the fixture must define an ASYNC event"
		);
		assert!(queued(&ctx).await.is_empty(), "the queue starts empty");

		let mut row = updated_row().await;
		assert_eq!(
			events(Action::Update).apply(&mut row, &ctx).await.unwrap(),
			StageOutcome::Continue,
			"queueing an ASYNC event leaves the statement running"
		);
		assert_eq!(
			stored(&ctx, "log", 1).await,
			Value::None,
			"the body is the drain's to run, not this statement's"
		);

		let entries = queued(&ctx).await;
		assert_eq!(entries.len(), 1, "one firing event queues one entry");
		let (key_bytes, value_bytes) = &entries[0];

		let key = EventQueueKey::decode_key(key_bytes).expect("the queued key decodes");
		assert_eq!(key.ns, ctx.db.namespace_id);
		assert_eq!(key.db, ctx.db.database_id);
		assert_eq!(key.tb.as_ref(), &ctx.table.name);
		assert_eq!(key.ev.as_ref(), "ev");

		let payload: AsyncEventRecord =
			KVValue::kv_decode_value(value_bytes, ()).expect("the queued payload decodes");
		assert_eq!(payload.attempt, 0, "a queued event has not been attempted");
		assert_eq!(payload.event_depth, 0, "a write is the top of the nesting budget");
		assert_eq!(
			payload.rid.as_deref(),
			Some(&RecordId {
				table: "t".into(),
				key: RecordIdKey::Number(1),
			})
		);
		assert_eq!(
			payload.cursor_record.data,
			val("{ id: t:1, n: 2 }").await,
			"an update's event is about the post-image"
		);
		assert_eq!(payload.ns.as_ref(), "test");
		assert_eq!(payload.db.as_ref(), "test");
		assert!(!payload.perms, "an event body runs with enforcement off");
		assert_eq!(payload.event_definition.name.to_string(), "ev");

		let values = &payload.values;
		assert_eq!(
			values.get("event").map(|v| Value::clone(v)),
			Some(Value::from("UPDATE")),
			"the action the event is about is carried"
		);
		assert_eq!(
			values.get("after").map(|v| Value::clone(v)),
			Some(val("{ id: t:1, n: 2 }").await)
		);
		assert_eq!(
			values.get("before").map(|v| Value::clone(v)),
			Some(val("{ id: t:1, n: 1 }").await)
		);
		assert_eq!(
			values.get("value").map(|v| Value::clone(v)),
			Some(val("{ id: t:1, n: 2 }").await)
		);
		assert!(
			values.contains_key("input"),
			"`$input` is bound, even where the form supplies none"
		);
	}

	/// An `ASYNC` event whose `WHEN` does not fire is not an event at all: it
	/// queues nothing, exactly as a synchronous one runs nothing.
	#[tokio::test]
	async fn an_async_event_whose_when_is_false_queues_nothing() {
		let db = TestDb::new(
			"DEFINE TABLE t SCHEMALESS;
			 DEFINE EVENT ev ON t ASYNC WHEN $after.n = 2 THEN (CREATE log:1 SET step = 1);",
		)
		.await;
		let exec = exec_ctx(&db).await;
		let ctx = write_ctx(&exec).await;

		let mut quiet = changed_row(5);
		assert_eq!(
			events(Action::Update).apply(&mut quiet, &ctx).await.unwrap(),
			StageOutcome::Continue
		);
		assert!(queued(&ctx).await.is_empty(), "a WHEN that did not fire queues nothing");
	}

	/// A delete's queued event is about the pre-image, and carries the
	/// record-type marker that image held rather than the cleared one's.
	#[tokio::test]
	async fn a_deletes_queued_event_snapshots_the_pre_image() {
		let db = TestDb::new(
			"DEFINE TABLE t SCHEMALESS;
			 DEFINE EVENT ev ON t ASYNC WHEN $event = 'DELETE' THEN (CREATE log:1);",
		)
		.await;
		let exec = exec_ctx(&db).await;
		let ctx = write_ctx(&exec).await;

		let mut row =
			WriteRow::for_record(val("{ id: t:1, n: 1 }").await).expect("a record makes a row");
		row.current = Value::None;
		// The two slots are given different markers so that the assertion
		// below names the pre-image's rather than passing on either.
		row.initial_metadata = Some(Metadata {
			record_type: RecordType::Edge {
				variant: 2,
			},
			aggregation_stats: Vec::new(),
		});
		row.metadata = None;
		assert_eq!(
			events(Action::Delete).apply(&mut row, &ctx).await.unwrap(),
			StageOutcome::Continue
		);

		let payload = sole_payload(&ctx).await;
		assert_eq!(payload.cursor_record.data, val("{ id: t:1, n: 1 }").await);
		assert_eq!(
			payload.cursor_record.metadata,
			Some(Metadata {
				record_type: RecordType::Edge {
					variant: 2,
				},
				aggregation_stats: Vec::new(),
			}),
			"the snapshot carries the marker the pre-image held"
		);
		assert_eq!(
			payload.values.get("event").map(|v| Value::clone(v)),
			Some(Value::from("DELETE"))
		);
	}

	/// The one entry the queue holds, decoded.
	async fn sole_payload(ctx: &WriteCtx) -> AsyncEventRecord {
		let entries = queued(ctx).await;
		assert_eq!(entries.len(), 1, "one firing event queues one entry");
		KVValue::kv_decode_value(&entries[0].1, ()).expect("the queued payload decodes")
	}

	/// The snapshot records whether it already carries its `COMPUTED` values,
	/// so the drain reads the image this write observed rather than deriving
	/// the bodies a second time in a frame of its own.
	///
	/// Each image carries its own flag, and the payload reports the one
	/// belonging to the image the action makes the subject. The table
	/// declaring a computed field is not the answer: an order that reaches an
	/// observer without running `materialise_observed_fields` leaves the
	/// images without the values the schema names.
	#[tokio::test]
	async fn the_snapshot_says_whether_its_computed_values_are_in_place() {
		const WITH_COMPUTED: &str = "DEFINE TABLE t SCHEMAFULL;
			 DEFINE FIELD n ON t TYPE number;
			 DEFINE FIELD double ON t COMPUTED n * 2;
			 DEFINE EVENT ev ON t ASYNC WHEN true THEN (CREATE log:1);";
		const WITHOUT_COMPUTED: &str = "DEFINE TABLE t SCHEMALESS;
			 DEFINE EVENT ev ON t ASYNC WHEN true THEN (CREATE log:1);";

		let db = TestDb::new(WITH_COMPUTED).await;
		let exec = exec_ctx(&db).await;
		let ctx = write_ctx(&exec).await;
		let mut row = updated_row().await;
		row.fields_computed = true;
		assert_eq!(
			events(Action::Update).apply(&mut row, &ctx).await.unwrap(),
			StageOutcome::Continue
		);
		assert!(
			sole_payload(&ctx).await.fields_computed,
			"a post-image that carries its computed values says so"
		);

		let db = TestDb::new(WITH_COMPUTED).await;
		let exec = exec_ctx(&db).await;
		let ctx = write_ctx(&exec).await;
		let mut row =
			WriteRow::for_record(val("{ id: t:1, n: 1 }").await).expect("a record makes a row");
		row.current = Value::None;
		row.initial_fields_computed = true;
		assert!(!row.fields_computed, "a delete's post-image flag tracks the image it cleared");
		assert_eq!(
			events(Action::Delete).apply(&mut row, &ctx).await.unwrap(),
			StageOutcome::Continue
		);
		assert!(
			sole_payload(&ctx).await.fields_computed,
			"a delete queues the pre-image, and reports the pre-image's flag"
		);

		// The table declares a computed field and something observes the
		// write, but nothing derived the bodies onto this pre-image. Reading
		// the answer off the schema rather than the image reports `true` here
		// and makes the queued entry lie about what it holds.
		let db = TestDb::new(WITH_COMPUTED).await;
		let exec = exec_ctx(&db).await;
		let ctx = write_ctx(&exec).await;
		let mut row =
			WriteRow::for_record(val("{ id: t:1, n: 1 }").await).expect("a record makes a row");
		row.current = Value::None;
		assert!(!row.initial_fields_computed);
		assert_eq!(
			events(Action::Delete).apply(&mut row, &ctx).await.unwrap(),
			StageOutcome::Continue
		);
		assert!(
			!sole_payload(&ctx).await.fields_computed,
			"an image nothing materialised says so, whatever the table declares"
		);

		let db = TestDb::new(WITHOUT_COMPUTED).await;
		let exec = exec_ctx(&db).await;
		let ctx = write_ctx(&exec).await;
		let mut row =
			WriteRow::for_record(val("{ id: t:1, n: 1 }").await).expect("a record makes a row");
		row.current = Value::None;
		assert_eq!(
			events(Action::Delete).apply(&mut row, &ctx).await.unwrap(),
			StageOutcome::Continue
		);
		assert!(
			!sole_payload(&ctx).await.fields_computed,
			"a table with no computed field has none to have materialised"
		);
	}

	/// An import and an unmodified record queue nothing, for the same reason
	/// they run no other observer.
	#[tokio::test]
	async fn an_import_queues_no_async_event() {
		let db = TestDb::new(
			"DEFINE TABLE t SCHEMALESS;
			 DEFINE EVENT ev ON t ASYNC WHEN true THEN (CREATE log:1);",
		)
		.await;
		let exec = exec_ctx(&db).await;
		let import_ctx = write_ctx(&as_import(&exec)).await;
		let mut imported = updated_row().await;
		assert_eq!(
			events(Action::Update).apply(&mut imported, &import_ctx).await.unwrap(),
			StageOutcome::Continue
		);
		assert!(queued(&import_ctx).await.is_empty(), "an import replays a write nobody observes");
	}

	/// A `THEN` is compiled only once its `WHEN` has fired.
	///
	/// The authority reaches a `THEN` inside the gate the `WHEN` opens, so a body the planner
	/// declines must fail the statement exactly where the body would have run
	/// and nowhere else. Compiling the bodies alongside the `WHEN` would fail
	/// every write to the table instead, including the ones the event never
	/// fires on.
	///
	/// `[LET $x = 1]` is a body with a logical form the planner declines: a
	/// `LET` is a statement, and a statement in scalar position is neither
	/// planned nor carried as a write would be.
	#[tokio::test]
	async fn a_then_is_compiled_only_once_its_when_has_fired() {
		let db = TestDb::new(
			"DEFINE TABLE t SCHEMALESS;
			 DEFINE EVENT ev ON t WHEN $after.n = 2 THEN [LET $x = 1];",
		)
		.await;
		let exec = exec_ctx(&db).await;
		let ctx = write_ctx(&exec).await;

		let mut quiet = changed_row(5);
		assert_eq!(
			events(Action::Update).apply(&mut quiet, &ctx).await.unwrap(),
			StageOutcome::Continue,
			"an event whose WHEN is false never compiles its THEN"
		);

		let mut fires = changed_row(1);
		let failure = event_failure(Action::Update, &mut fires, &ctx).await;
		assert!(
			failure.contains("LET statements can only appear"),
			"the same body is declined once the WHEN fires: {failure}"
		);
	}

	/// A table with no events has nothing to run, and a row the statement did
	/// not change is passed over before any clause is compiled.
	#[tokio::test]
	async fn a_table_with_no_events_runs_nothing() {
		let db = TestDb::new("DEFINE TABLE t SCHEMALESS;").await;
		let exec = exec_ctx(&db).await;
		let ctx = write_ctx(&exec).await;
		assert!(ctx.events.is_empty());
		let mut row = changed_row(1);
		assert_eq!(
			events(Action::Create).apply(&mut row, &ctx).await.unwrap(),
			StageOutcome::Continue
		);
	}
}
