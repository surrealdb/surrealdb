//! Live-query notifications for the subscriptions a committed write could not see.
//!
//! A write computes its notifications, record by record, from the subscriptions
//! its transaction's snapshot holds. A `LIVE` that commits after that snapshot was
//! taken and before the write commits is not among them, and the two do not
//! conflict, so without more the subscriber never hears of a change committed
//! after it registered.
//!
//! Every registration is entered in the datastore's
//! [`LiveRegistrations`](surrealdb_datastore::triggers::LiveRegistrations) before
//! it commits, and dated once it has. Each write transaction notes here, as it
//! writes, which records of which tables it wrote ([`LiveRecheck::record`]),
//! with the subscriptions it saw. Once it has committed it asks the registry
//! for the registrations on those tables it may have missed; only when there
//! are any does it read the tables' subscriptions afresh and notifies each one
//! it did not see of every record it wrote ([`run`]).
//!
//! A write that runs while a registration on its table is in flight, one it may
//! not see, keeps its images of each record it writes
//! ([`LiveRecheck::keeps_images`]), and a missed subscriber is told of those as a
//! seen one is: `$before`, `$after` and `DIFF` are what the write saw. Asking costs
//! two atomic loads while no registration is in flight. The kept images are told
//! only while the record still stands as the write left it: the recheck runs in
//! the background, after a later commit may have changed the record and told the
//! subscriber so, and the image that commit replaced must not reach it after.
//!
//! A record written before any such registration began has no images kept, nor
//! one a later commit has changed since, and a missed subscriber is told of it
//! as it stands once the write commits. There is then no image from before
//! the write: an update is told as an update with no prior record, and a delete
//! names the record by its id alone. A record a later commit has deleted is not
//! told of as created or updated, and one a later commit has created again is
//! not told of as deleted: the later commit notifies the subscriber, who
//! registered before it. A record written more than once by the transaction is
//! notified once per write.
//!
//! Delivery is at least once: a `LIVE` that commits just after the write does,
//! before the write has asked the registry, may be told of the write too, though
//! the write was committed before it registered.
//!
//! Registrations the transaction itself made are left out, however many
//! requests it spans: the writes it made before them are not owed to them.

use std::sync::Arc;

use parking_lot::Mutex;
use surrealdb_datastore::close::CommitAction;
use surrealdb_datastore::triggers::CommitTriggers;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::catalog::{DatabaseId, NamespaceId, SubscriptionDefinition};
use crate::ctx::Context;
use crate::dbs::MessageBroker;
use crate::doc::Action;
use crate::exec::operators::write::ctx::WriteCtx;
use crate::exec::operators::write::stages::lives::{Images, Notify, deliver};
use crate::kvs::sequences::Sequences;
use crate::kvs::{TransactionFactory, TransactionType};
use crate::val::{RecordId, TableName, Value};

/// What one transaction wrote, per table, for the subscriptions it may have
/// missed to be told of once it commits.
#[derive(Default)]
pub(crate) struct LiveRecheck {
	tables: Mutex<Vec<TableWrites>>,
	/// Set once the transaction has asked, or will never ask: nothing written
	/// after is noted.
	closed: std::sync::atomic::AtomicBool,
	/// Whether each table's write context is detached as the table is first
	/// written ([`TableCtx::Detached`]) rather than once the transaction has
	/// committed. Set for a recheck whose [`RecheckHandle`] the transaction
	/// itself holds, which the transaction's own write contexts must not hold
	/// in turn.
	detach_at_write: std::sync::atomic::AtomicBool,
}

/// The write context a table's missed subscriptions are told through.
enum TableCtx {
	/// The context of the write that first wrote the table, held until the
	/// transaction has asked whether any subscription was missed, and
	/// detached ([`detach`]) only when one was. It reaches the transaction's
	/// context, which holds the recheck, so [`RecheckHandle`] empties the
	/// recheck once the transaction has asked or ended.
	Live(Arc<WriteCtx>),
	/// The context detached from the transaction.
	Detached(Arc<WriteCtx>),
}

impl TableCtx {
	/// The context detached from the transaction.
	fn detached(&self) -> Arc<WriteCtx> {
		match self {
			Self::Live(ctx) => Arc::new(detach(ctx)),
			Self::Detached(ctx) => Arc::clone(ctx),
		}
	}
}

/// The records one transaction wrote to one table.
struct TableWrites {
	ns: NamespaceId,
	db: DatabaseId,
	tb: TableName,
	/// The [`LiveWrites`](surrealdb_datastore::triggers::LiveWrites) epoch the
	/// transaction counted itself into.
	epoch: u64,
	/// The transaction's [`serial`](crate::kvs::Transaction::serial), whose
	/// own registrations are not owed its writes.
	writer: u64,
	/// The subscriptions the transaction's snapshot held, which it notified.
	seen: Vec<Uuid>,
	/// The list of subscriptions last merged into `seen`: a statement hands every
	/// record it writes the same list, which is merged once.
	merged: Option<Arc<[SubscriptionDefinition]>>,
	/// Holds the registrations the transaction may have missed until it has
	/// asked for them.
	_pin: EpochPin,
	/// The table's write context. Once detached it carries no transaction, no
	/// broker and no subscriptions, so holding it keeps nothing of the
	/// transaction alive.
	ctx: TableCtx,
	/// Each record written, with the write's images when they were kept.
	records: Vec<(RecordId, Action, Option<Images>)>,
}

impl std::fmt::Debug for LiveRecheck {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("LiveRecheck").field("tables", &self.tables.lock().len()).finish()
	}
}

impl LiveRecheck {
	/// Whether a write through `ctx` should hand [`Self::record`] its images: a
	/// registration on its table that it may not see is in flight, so a missed
	/// subscriber can be told what the write saw rather than what is there once
	/// it commits.
	pub(crate) fn keeps_images(&self, ctx: &WriteCtx) -> bool {
		let txn = ctx.txn();
		let Some(epoch) = txn.live_write_epoch() else {
			return false;
		};
		txn.live_registrations().any_candidate(
			ctx.ns.namespace_id,
			ctx.db.database_id,
			&ctx.table.name,
			epoch,
			txn.serial(),
		)
	}

	/// Note that the transaction of `ctx` wrote `rid` with `action`, with the
	/// write's `images` where [`Self::keeps_images`] asked for them.
	///
	/// A transaction no longer counted among its datastore's write
	/// transactions has nothing left to write, and is not noted.
	pub(crate) fn record(
		&self,
		ctx: &Arc<WriteCtx>,
		rid: RecordId,
		action: Action,
		images: Option<Images>,
	) {
		use std::sync::atomic::Ordering;
		let Some(epoch) = ctx.txn().live_write_epoch() else {
			return;
		};
		if self.closed.load(Ordering::Acquire) {
			return;
		}
		let ns = ctx.ns.namespace_id;
		let db = ctx.db.database_id;
		let mut tables = self.tables.lock();
		let index = match tables
			.iter()
			.position(|table| table.ns == ns && table.db == db && table.tb == ctx.table.name)
		{
			Some(index) => index,
			None => {
				tables.push(TableWrites {
					ns,
					db,
					tb: ctx.table.name.clone(),
					epoch,
					writer: ctx.txn().serial(),
					_pin: EpochPin::new(ctx.txn().commit_triggers(), epoch, ctx.txn().serial()),
					seen: Vec::new(),
					merged: None,
					ctx: if self.detach_at_write.load(Ordering::Relaxed) {
						TableCtx::Detached(Arc::new(detach(ctx)))
					} else {
						TableCtx::Live(Arc::clone(ctx))
					},
					records: Vec::new(),
				});
				tables.len() - 1
			}
		};
		let table = &mut tables[index];
		if !table.merged.as_ref().is_some_and(|merged| Arc::ptr_eq(merged, &ctx.lives)) {
			for lv in ctx.lives.iter() {
				if !table.seen.contains(&lv.id) {
					table.seen.push(lv.id);
				}
			}
			table.merged = Some(Arc::clone(&ctx.lives));
		}
		table.records.push((rid, action, images));
	}

	/// Whether the transaction wrote anything a missed subscription could be
	/// owed.
	pub(crate) fn is_empty(&self) -> bool {
		self.tables.lock().is_empty()
	}

	/// Note nothing more, and let go of what was noted: its pins, and the
	/// write contexts that reach the transaction.
	fn close(&self) {
		self.closed.store(true, std::sync::atomic::Ordering::Release);
		// Dropped outside the lock: a context dropped here may be the last
		// reference to the transaction's.
		let tables = std::mem::take(&mut *self.tables.lock());
		drop(tables);
	}
}

/// The handle a transaction's recheck is asked through once it commits
/// ([`spawn`]). Dropped unasked, because the transaction was cancelled or
/// failed to commit, it closes the recheck, so the write contexts it holds,
/// which reach the transaction's context and through it the recheck, are let
/// go of.
pub(crate) struct RecheckHandle {
	recheck: Arc<LiveRecheck>,
	source: RecheckSource,
}

impl RecheckHandle {
	pub(crate) fn new(recheck: Arc<LiveRecheck>, source: RecheckSource) -> Self {
		Self {
			recheck,
			source,
		}
	}

	/// Detach each table's write context as the table is first written. For a
	/// handle the transaction itself holds, as a commit action does: the
	/// contexts would otherwise hold the transaction that holds them.
	pub(crate) fn detach_at_write(&self) {
		self.recheck.detach_at_write.store(true, std::sync::atomic::Ordering::Relaxed);
	}
}

impl Drop for RecheckHandle {
	fn drop(&mut self) {
		self.recheck.close();
	}
}

/// `ctx` with the transaction, the broker and the subscriptions taken out. It
/// keeps the statement clock of the statement that first wrote the table, so
/// every CLOCK read a missed subscription makes answers that one instant, as
/// it would for a subscription the write saw. It does not keep the writing
/// request's cancellation: the recheck runs after that request may have ended
/// or its connection closed, and is owed to other subscribers.
fn detach(ctx: &WriteCtx) -> WriteCtx {
	let clock =
		ctx.exec().ctx().statement_clock().copied().unwrap_or_else(crate::val::Datetime::now);
	let mut detached = Context::new_concurrent(ctx.exec().ctx());
	detached.set_broker(None);
	detached.set_statement_clock(clock);
	let exec =
		ctx.exec().with_new_ctx(detached.freeze()).with_cancellation(CancellationToken::new());
	let mut detached = ctx.rebound(&exec);
	detached.lives = Arc::from(Vec::new());
	detached.live_units = Arc::from(Vec::new());
	detached
}

/// Where a committed transaction's recheck opens its read.
#[derive(Clone)]
pub(crate) struct RecheckSource {
	pub(crate) tf: TransactionFactory,
	pub(crate) sequences: Sequences,
}

/// Tell the subscriptions `handle`'s transaction missed of what it wrote,
/// once it has committed. Runs in the background; the commit has answered
/// already. Each table a subscription was missed on has its write context
/// detached here, before the background task takes it.
#[expect(
	clippy::needless_pass_by_value,
	reason = "the handle is taken so that dropping it, however this returns, closes the recheck"
)]
pub(crate) fn spawn(handle: RecheckHandle, delivery: Arc<dyn MessageBroker>) {
	let recheck = &handle.recheck;
	if recheck.is_empty() {
		return;
	}
	let triggers = Arc::clone(handle.source.tf.commit_triggers());
	// The question is cheap and asked at once, so the common answer, that
	// nothing was missed, spawns nothing. Dropping the handle releases the
	// tables' pins and contexts.
	let missed = missed(recheck, &triggers);
	if missed.iter().all(Vec::is_empty) {
		return;
	}
	let tables = std::mem::take(&mut *recheck.tables.lock());
	let owed: Vec<(TableWrites, Vec<Uuid>)> = tables
		.into_iter()
		.zip(missed)
		.filter(|(_, missed)| !missed.is_empty())
		.map(|(table, missed)| {
			let detached = TableCtx::Detached(table.ctx.detached());
			(
				TableWrites {
					ctx: detached,
					..table
				},
				missed,
			)
		})
		.collect();
	let source = handle.source.clone();
	common::rt::spawn_detached(async move {
		run(owed, &source, &delivery).await;
	});
}

/// For each table `recheck` wrote, the registrations on it the transaction
/// may have missed.
fn missed(recheck: &LiveRecheck, triggers: &CommitTriggers) -> Vec<Vec<Uuid>> {
	recheck
		.tables
		.lock()
		.iter()
		.map(|table| {
			triggers
				.live_registrations
				.candidates(
					table.ns,
					table.db,
					&table.tb,
					table.epoch,
					table.writer,
					triggers.live_writes(),
				)
				.into_iter()
				.filter(|id| !table.seen.contains(id))
				.collect()
		})
		.collect()
}

/// Notify each table's missed subscriptions in turn. A table that cannot be
/// read or notified is logged and the rest are still notified.
async fn run(
	owed: Vec<(TableWrites, Vec<Uuid>)>,
	source: &RecheckSource,
	delivery: &Arc<dyn MessageBroker>,
) {
	for (table, missed) in owed {
		if let Err(err) = Box::pin(recheck_table(&table, &missed, source, delivery)).await {
			tracing::warn!(
				target: "surrealdb::core::dbs::live_recheck",
				error = %err,
				table = %table.tb,
				"Could not notify the live queries a write committed unseen by",
			);
		}
	}
}

/// [`notify_table`] in a read transaction of its own, cancelled however it ends.
async fn recheck_table(
	table: &TableWrites,
	missed: &[Uuid],
	source: &RecheckSource,
	delivery: &Arc<dyn MessageBroker>,
) -> anyhow::Result<()> {
	let txn =
		Arc::new(source.tf.transaction(TransactionType::Read, source.sequences.clone()).await?);
	let outcome = Box::pin(notify_table(table, missed, &txn, delivery)).await;
	let _ = txn.cancel().await;
	outcome
}

/// Notify the subscriptions on `table` named in `missed`, as `txn` reads them,
/// of every record the transaction wrote to it.
async fn notify_table(
	table: &TableWrites,
	missed: &[Uuid],
	txn: &Arc<crate::kvs::Transaction>,
	delivery: &Arc<dyn MessageBroker>,
) -> anyhow::Result<()> {
	use crate::catalog::providers::TableProvider;

	let lives: Vec<_> = txn
		.all_tb_lives(table.ns, table.db, &table.tb, None)
		.await?
		.iter()
		.filter(|lv| missed.contains(&lv.id))
		.cloned()
		.collect();
	if lives.is_empty() {
		return Ok(());
	}
	let detached = table.ctx.detached();
	let mut ctx = Context::new_child(detached.exec().ctx());
	ctx.set_transaction(Arc::clone(txn));
	let ctx = ctx.freeze();
	let exec = detached.exec().with_new_ctx(Arc::clone(&ctx));
	let units =
		crate::exec::vm::live::units_for(&ctx, &detached.ns, &detached.db, &table.tb, &lives)
			.await?;
	let mut write = detached.rebound(&exec);
	write.lives = Arc::from(lives);
	write.live_units = units;
	for (rid, action, kept) in &table.records {
		// A record that cannot be read is logged, and the rest are still told.
		let images = match Box::pin(images_for(&write, txn, rid, *action, kept.as_ref())).await {
			Ok(Some(images)) => images,
			Ok(None) => continue,
			Err(err) => {
				tracing::warn!(
					target: "surrealdb::core::dbs::live_recheck",
					error = %err,
					table = %table.tb,
					"Could not read a record a write committed unseen by a live query",
				);
				continue;
			}
		};
		for at in 0..write.lives.len() {
			let notified = Box::pin(crate::exec::vm::notify_at(&write, at, rid, &images))
				.await
				.map_err(crate::exec::migrate::into_error)?;
			if let Notify::Deliver(notification) = notified {
				deliver(delivery, &write, &write.lives[at], notification)
					.await
					.map_err(crate::exec::migrate::into_error)?;
			}
		}
	}
	Ok(())
}

/// The images a missed subscription is told of `rid`, written with `action`:
/// the write's own, `kept`, while the record still stands as the write left
/// it, and otherwise the record as it now stands. A later commit that changed
/// the record may already have notified the subscriber, which must not then be
/// told the image that commit replaced. `None` when a later commit has undone
/// the write.
async fn images_for(
	write: &WriteCtx,
	txn: &crate::kvs::Transaction,
	rid: &RecordId,
	action: Action,
	kept: Option<&Images>,
) -> anyhow::Result<Option<Images>> {
	let (record, stamp) = crate::exec::migrate::stamped_record(
		txn,
		write.ns.namespace_id,
		write.db.database_id,
		&rid.table,
		&rid.key,
		None,
	)
	.await?;
	let mut current = record.data.clone();
	let exists = !current.is_nullish();
	if exists && (kept.is_some() || !matches!(action, Action::Delete)) {
		crate::exec::migrate::replay_record(write.exec(), &rid.table, &mut current, stamp)
			.await
			.map_err(crate::exec::migrate::into_error)?;
	}
	if let Some(kept) = kept
		&& stands_as(write, &kept.current, &current)
	{
		return Ok(Some(kept.clone()));
	}
	match action {
		Action::Create | Action::Update if exists => Ok(Some(Images {
			initial: Arc::new(Value::None),
			current: Arc::new(current),
			action,
		})),
		Action::Delete if !exists => {
			let mut named = crate::val::Object::default();
			named.insert("id".to_owned(), Value::RecordId(rid.clone()));
			Ok(Some(Images {
				initial: Arc::new(Value::Object(named)),
				current: Arc::new(Value::None),
				action,
			}))
		}
		Action::Create | Action::Update | Action::Delete => Ok(None),
	}
}

/// Whether `kept`, the record as a write left it, is `stored`, the record as it
/// now stands. `kept` may carry the `COMPUTED` fields the write derived for its
/// observers, which are top-level and never stored, so a field `kept` alone
/// holds is passed over when it is one of those.
fn stands_as(write: &WriteCtx, kept: &Value, stored: &Value) -> bool {
	match (kept, stored) {
		(Value::Object(kept), Value::Object(stored)) => {
			stored.iter().all(|(name, value)| kept.get(name) == Some(value))
				&& kept.iter().all(|(name, _)| {
					stored.contains_key(name)
						|| write.fields.iter().any(|field| {
							field.computed.is_some() && field.name.to_raw_string() == name.as_str()
						})
				})
		}
		(kept, stored) => kept == stored || (kept.is_nullish() && stored.is_nullish()),
	}
}

/// A pin of a write's epoch in the registry
/// ([`LiveRegistrations::pin`](surrealdb_datastore::triggers::LiveRegistrations::pin)),
/// released when dropped.
struct EpochPin {
	triggers: Arc<CommitTriggers>,
	epoch: u64,
	/// The serial of the transaction that pinned it.
	by: u64,
}

impl EpochPin {
	fn new(triggers: Arc<CommitTriggers>, epoch: u64, by: u64) -> Self {
		triggers.live_registrations.pin(epoch, by);
		Self {
			triggers,
			epoch,
			by,
		}
	}
}

impl Drop for EpochPin {
	fn drop(&mut self) {
		self.triggers.live_registrations.unpin(self.epoch, self.by);
	}
}

/// Settle a registration once its transaction commits, or abandon it if the
/// transaction does not: a commit action that is dropped without running.
pub(crate) struct SettleRegistration {
	pub(crate) triggers: Arc<CommitTriggers>,
	pub(crate) token: Option<u64>,
}

impl CommitAction for SettleRegistration {
	fn run(mut self: Box<Self>) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
		if let Some(token) = self.token.take() {
			self.triggers.live_registrations.settle(token, self.triggers.live_writes());
		}
		Box::pin(async {})
	}

	fn scope(&self) -> surrealdb_datastore::close::ActionScope {
		surrealdb_datastore::close::ActionScope::SavePoint
	}
}

impl Drop for SettleRegistration {
	fn drop(&mut self) {
		if let Some(token) = self.token.take() {
			self.triggers.live_registrations.abandon(token);
		}
	}
}
