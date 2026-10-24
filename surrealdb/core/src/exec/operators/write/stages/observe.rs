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
use crate::exec::operators::write::ctx::WriteCtx;
use crate::exec::operators::write::row::WriteRow;
use crate::exec::{Error as ExecError, ExecutionContext};
use crate::expr::{ChangeFeed, ControlFlow};
use crate::key::schema::NodeEventQueueKey;
use crate::val::Value;

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

/// Persist a firing `ASYNC` event to the event queue, for the drain in
/// [`crate::doc::process_next_events_batch`] to run out-of-band.
///
/// The entry goes through the statement's own transaction, so the queued event
/// becomes visible exactly when the change it is about does: the commit that
/// makes the record readable is the one that makes the entry drainable, and a
/// statement that fails afterwards leaves neither behind.
///
/// The entry goes under this node's own queue, which this node drains on the
/// commit wake without a lease; the HLC timestamp orders the queue and makes
/// the entry unique within it. Beneath the node the key carries the namespace
/// and database ids the write resolved and the table and event names the
/// definition carries. The table named is the event's own `target_table`, the
/// table the definition is attached to, so the entry addresses the definition
/// rather than the statement that fired it. The previous release neither scans
/// this queue nor decodes its entries: a node rolled back to it among peers on
/// this release has the queue adopted by them, and one rolled back with no
/// such peer left keeps what is queued here until a node on this release is
/// back.
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
	let key = NodeEventQueueKey {
		nd: frame.ctx().event_queue_node(),
		ns: ctx.db.namespace_id,
		db: ctx.db.database_id,
		tb: Cow::Borrowed(&event.target_table),
		ev: Cow::Borrowed(&event.name),
		ts: HlcTimeStamp::next().0,
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
		event_definition: definition.clone().into(),
		claim: None,
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

/// Whether the datastore's live-query engine writes change feeds: the external
/// engine leaves the feed to its host.
pub(crate) fn writes_changefeed(ctx: &WriteCtx) -> bool {
	ctx.exec().ctx().config.datastore.live_query_engine.writes_changefeed()
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
	if let Some(changefeed) = changefeed_definition(ctx)
		&& writes_changefeed(ctx)
	{
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
	if captures_live_events(ctx) {
		held(memory.as_ref(), &[&row.initial, &row.current], ctx).await?;
		let captured = txn.timestamp().await.map_err(ControlFlow::Err)?.as_versionstamp();
		txn.live_event_buffer_record_change(
			ctx.db.namespace_id,
			ctx.db.database_id,
			&ctx.table.name,
			id,
			row.initial.clone(),
			row.current.clone(),
			captured,
		);
	}
	Ok(())
}

/// Whether the order a view row of this table gets writes nothing: the table has
/// no index, no view reading it, no event, no live-query subscription and no
/// change feed on it or on its database, and the router engine captures no
/// change of it. Those are what the stages of that order maintain
/// (`view_maintenance_order` in `write/plan.rs`), so a view row deleted from
/// such a table removes its record and nothing else.
pub(crate) fn view_row_reaches_nothing(ctx: &WriteCtx) -> bool {
	ctx.indexes.is_empty()
		&& ctx.views.is_empty()
		&& ctx.events.is_empty()
		&& !ctx.live_observed
		&& changefeed_definition(ctx).is_none()
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
/// proportional to demand. Whether one is subscribed is the context's
/// `live_observed`, read once when the context is resolved from the committed
/// subscription rows within this write's own snapshot rather than a
/// node-local cache, so a subscriber registered on another node counts, and
/// shared by every record of the statement.
///
/// The test is judged by this write's snapshot, not at its commit: a write
/// whose snapshot found no subscription captures nothing, so a table's first
/// subscription, registered while the write runs, misses the write although
/// it commits above the subscription's watermark. A write that found any
/// subscription captures, and such a subscription receives it.
fn captures_live_events(ctx: &WriteCtx) -> bool {
	ctx.live_observed && ctx.exec().ctx().config.datastore.live_query_engine.captures_events()
}

#[cfg(all(test, feature = "kv-mem"))]
#[allow(clippy::unwrap_used)]
mod tests {

	// =======================================================================
	// process_table_events
	// =======================================================================
}
