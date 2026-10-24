//! Subscriber-side live-query compute.
//!
//! This is the half of the inverted pipeline that runs *off* the mutator's
//! write path. The write path's only job (under the Router engine) is to
//! capture before/after values into the dedicated `lqe` keyspace cheaply
//! ([`crate::lq::event`]). The matching/permission/projection/FETCH work — the
//! part whose cost previously scaled with subscriber count on every write — is
//! performed here instead, reading the captured events back and replaying them
//! through the same per-subscription units the inline engine notifies through
//! ([`crate::exec::vm::notify_unit`]).
//!
//! The per-node router ([`crate::lq::router`]) drives this as it tails the `lqe`
//! keyspace. Equivalence with the inline engine is pinned by an end-to-end test
//! that asserts the notifications the router delivers are identical in content
//! to those the inline engine delivers for the same writes.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context as _, Result};
use surrealdb_datastore::tx::refused_by_release;
use surrealdb_strand::TableName;
use uuid::Uuid;

use crate::catalog::{StoredSubscriptionDefinition, SubscriptionDefinition};
use crate::dbs::{MessageBroker, Session};
use crate::doc::Action;
use crate::exec::operators::write::ctx::WriteCtx;
use crate::exec::operators::write::stages::lives::{Images, Notify, deliver};
use crate::exec::vm::live::LiveUnit;
use crate::expr::ControlFlow;
use crate::key::schema::{SubscriptionKey, SubscriptionPrefix};
use crate::key::{KVKey, KVKeyDecode, Resumable};
use crate::kvs::walk::ListScan;
use crate::kvs::{Datastore, Direction, Transaction};
use crate::lq::event::{LiveAction, LiveEvent};

/// The error a write-context failure carries. Neither resolving a table's
/// context nor delivering a notification can signal a loop or a `RETURN`, so
/// anything but an error is a broken invariant.
pub(crate) fn into_error(flow: ControlFlow) -> anyhow::Error {
	match flow {
		ControlFlow::Err(e) => e,
		ControlFlow::Break | ControlFlow::Continue | ControlFlow::Return(_) => {
			anyhow::Error::new(crate::exec::Error::InvalidControlFlow)
		}
	}
}

/// The write context of `table` that a subscription's notification is built in: a background
/// context over the read transaction `txn`, in `ns_name` and `db_name`, carrying `broker` when
/// the notifications are sent through one.
///
/// Base options are scoped to the namespace and database with an anonymous principal, which is
/// irrelevant: the pipeline applies each subscription's own stored auth snapshot before
/// evaluating any permission. The table must exist.
pub(crate) async fn table_write_ctx(
	ds: &Datastore,
	txn: Arc<Transaction>,
	ns_name: &str,
	db_name: &str,
	table: &TableName,
	broker: Option<Arc<dyn MessageBroker>>,
) -> Result<WriteCtx> {
	let mut ctx = ds.setup_ctx()?;
	ctx.set_transaction(txn);
	ctx.set_broker(broker);
	let ctx = ctx.freeze();
	let opt = ds.setup_options(&Session::default().with_ns(ns_name).with_db(db_name));
	let exec = crate::exec::entry::frame(&ctx, &opt, None).await;
	WriteCtx::resolve_with_lives(&exec, table).await.map_err(into_error)
}

#[cfg(test)]
thread_local! {
	/// Subscriptions the replays of this thread read, for tests of what a
	/// pass reads.
	pub(crate) static SUBSCRIPTIONS_READ: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// A table's subscriptions, read for one router pass as the pass replays the
/// table's captured events to them.
///
/// Read inside the pass's read transaction, a page at a time as the events
/// reach them, so every event of the pass meets the subscriptions committed
/// before the pass began, and a pass reads no more of them than its budget
/// reaches: every subscription an event is replayed to costs one of the
/// budget, whether the event is evaluated for it or passes it over. A
/// subscription another node owns is passed over, whatever the broker routes
/// to: every node runs a router over the same captured events, so a router
/// that delivered to the subscriptions the other nodes own would reach each
/// owner once per node. A subscription registered after an event was
/// captured is passed over for that event: it is stored with the safe
/// watermark its registering transaction observed, at or above the event's
/// versionstamp, and the event was final before the subscription existed.
/// The watermark is read inside that transaction, so an event whose commit
/// interleaves with it is above the watermark and is delivered if it was
/// captured: a write captures only when its own snapshot finds a
/// subscription on the table (see `captures_live_events`), so a write whose
/// snapshot predates the table's first subscription captures nothing, and
/// that subscription misses it although it commits above the watermark. A
/// subscription registered by the transaction that captured the event,
/// after it, commits with it: the entry notes the subscription among its
/// events, and the event is passed over for it too.
///
/// The subscriptions are in id order: they are read from their keys, which
/// end in the id. So the place a pass keeps inside an event is the id of the
/// last subscription it replayed the event to, and the pass that continues
/// the event reads from the key after it rather than from the table's first.
/// A subscription registered between the passes with a smaller id postdates
/// the event and is passed over either way.
///
/// Each event is evaluated against a single catalog and data snapshot across
/// every subscription it reaches: when a pass stops inside an event, the
/// entry retains its [`TableReplay`] and the read transaction it resolved the
/// table's definitions in until that event finishes, so a schema or permission
/// change between passes does not split the event across two catalog versions.
pub(crate) struct TableReplay {
	/// The table's context, resolved without its subscriptions: the replay
	/// reads them itself.
	write_ctx: WriteCtx,
	table: TableName,
	txn: Arc<Transaction>,
	broker: Arc<dyn MessageBroker>,
	/// This node, whose subscriptions alone the replay delivers to; see the
	/// type's documentation.
	node: Uuid,
	/// The subscriptions read since the scan began, in id order, with the
	/// unit of each, and the id the scan began after: `None` for the
	/// table's first. A replay that starts elsewhere begins the scan again
	/// from there.
	lives: Vec<SubscriptionDefinition>,
	units: Vec<Option<Arc<LiveUnit>>>,
	after: Option<Uuid>,
	scan: ListScan<StoredSubscriptionDefinition>,
}

/// How far [`TableReplay::replay`] got with an event.
pub(crate) enum Replayed {
	/// The event was replayed to every subscription.
	Complete,
	/// The budget was spent with subscriptions still to replay to; the
	/// cursor names the last reached or passed.
	Stopped,
}

impl TableReplay {
	/// Resolve `table`'s context in `txn`, for the events of the database
	/// named `ns_name`/`db_name`; its subscriptions are read as the events
	/// reach them.
	///
	/// Returns `None` when the table no longer exists. Removing a table kills
	/// its subscriptions, so events captured for it before the removal have no
	/// one left to notify and are dropped rather than failing the replay.
	///
	/// Returns `None` as well, with a warning, when a definition the replay
	/// evaluates under — the table's, or one of its fields, events, indexes or
	/// views — needs a newer release than this build. This node cannot
	/// evaluate the table's events for its subscriptions, and the router
	/// delivers a database's entries in capture order, so an entry left in
	/// place would hold every event of the database behind it for as long as
	/// the definition stands.
	pub(crate) async fn resolve(
		ds: &Datastore,
		txn: Arc<Transaction>,
		ns_name: &str,
		db_name: &str,
		table: &TableName,
		broker: Arc<dyn MessageBroker>,
	) -> Result<Option<Self>> {
		// A background context carrying the read transaction and the broker
		// the replayed notifications are sent through.
		let mut ctx = ds.setup_ctx()?;
		ctx.set_transaction(Arc::clone(&txn));
		ctx.set_broker(Some(Arc::clone(&broker)));
		let ctx = ctx.freeze();
		// Base options scoped to this namespace/database. The auth is
		// anonymous and irrelevant: the pipeline applies each subscription's
		// own stored auth snapshot before evaluating any permission.
		let opt = ds.setup_options(&Session::default().with_ns(ns_name).with_db(db_name));
		let exec = crate::exec::entry::frame(&ctx, &opt, None).await;
		// The lookup is cached on the frame, so resolving the context below
		// reads the definition again for free.
		let db_ctx = exec.database().context("a live-query replay requires a database context")?;
		let resolved = async {
			if db_ctx.get_table_def(table, None).await?.is_none() {
				return Ok(None);
			}
			// The table's definitions, without the subscriptions: those are
			// read a page at a time as the events reach them.
			WriteCtx::resolve_without_lives(&exec, table).await.map(Some).map_err(into_error)
		}
		.await;
		let write_ctx = match resolved {
			Ok(Some(write_ctx)) => write_ctx,
			Ok(None) => return Ok(None),
			Err(e) if refused_by_release(&e) => {
				warn!(
					"Live-query router skipped the events of table `{table}` in database \
					 `{db_name}`: a definition of the table needs a newer release than this build"
				);
				return Ok(None);
			}
			Err(e) => return Err(e),
		};
		let scan = scan_after(&write_ctx, table, None)?;
		Ok(Some(Self {
			node: ds.id(),
			write_ctx,
			table: table.clone(),
			txn,
			broker,
			lives: Vec::new(),
			units: Vec::new(),
			after: None,
			scan,
		}))
	}

	/// The read transaction this replay resolved its catalog in.
	pub(crate) fn txn(&self) -> &Arc<Transaction> {
		&self.txn
	}

	/// Drops the pages of subscriptions already read so that holding this
	/// replay across passes keeps only its catalog snapshot and scan position,
	/// and the next pass reads from `cursor` rather than holding old pages in
	/// memory.
	pub(crate) fn park(&mut self) {
		self.lives.clear();
		self.units.clear();
	}

	/// Whether the table this replay resolved still exists with the same id in
	/// `current_txn`. Removing a table (or removing and recreating it under
	/// the same name) kills its subscriptions, so an event held across passes
	/// for a table since removed has no one left to notify.
	pub(crate) async fn table_matches(&self, current_txn: &Transaction) -> Result<bool> {
		use crate::catalog::providers::TableProvider;
		match current_txn
			.get_tb(
				self.write_ctx.ns.namespace_id,
				self.write_ctx.db.database_id,
				&self.table,
				None,
			)
			.await
		{
			Ok(Some(tb)) => Ok(tb.table_id == self.write_ctx.table.table_id),
			Ok(None) => Ok(false),
			Err(e) if refused_by_release(&e) => {
				warn!(
					"Live-query router skipped the events of table `{}` in database `{}`: \
					 a definition of the table needs a newer release than this build",
					self.table, self.write_ctx.db.name
				);
				Ok(false)
			}
			Err(e) => Err(e),
		}
	}

	/// Where the replay of an event starts for `cursor`, the last
	/// subscription the event was replayed to: the position after it among
	/// the subscriptions read, when the scan covers it, and otherwise the
	/// first of a scan begun again after it.
	fn position(&mut self, cursor: Option<Uuid>) -> Result<usize> {
		let covered = cursor == self.after
			|| (cursor > self.after
				&& self.lives.last().is_some_and(|last| cursor <= Some(last.id)));
		if covered {
			return Ok(self.lives.partition_point(|s| cursor.is_some_and(|last| s.id <= last)));
		}
		self.lives.clear();
		self.units.clear();
		self.after = cursor;
		self.scan = scan_after(&self.write_ctx, &self.table, cursor)?;
		Ok(0)
	}

	/// Read the next page of the table's subscriptions, with the unit of
	/// each; `false` once the scan has read them all. A page holding only
	/// subscriptions a newer release wrote adds none: its entries are
	/// skipped, and it is reported `true` like any other page, so the caller
	/// charges it and moves its cursor past it, unless it is the scan's last,
	/// which ends the scan as [`ListScan::next`] ends one.
	async fn read_page(&mut self) -> Result<bool> {
		let Some(page) = self.scan.next_page(&self.txn).await? else {
			return Ok(false);
		};
		if page.is_empty() {
			return Ok(self.scan.has_more());
		}
		#[cfg(test)]
		SUBSCRIPTIONS_READ.with(|n| n.set(n.get() + page.len()));
		let lives: Vec<SubscriptionDefinition> =
			page.iter().map(SubscriptionDefinition::compile).collect();
		// Only the subscriptions this node owns are evaluated, so only they
		// are lowered; the others keep their place in the list with no unit.
		let owned: Vec<SubscriptionDefinition> =
			lives.iter().filter(|live| live.node == self.node).cloned().collect();
		let units = crate::exec::vm::live::units_for(
			self.write_ctx.exec.ctx(),
			&self.write_ctx.ns,
			&self.write_ctx.db,
			&self.table,
			&owned,
		)
		.await?;
		debug_assert_eq!(units.len(), owned.len(), "one unit per subscription lowered");
		let mut units = units.iter();
		self.units.extend(lives.iter().map(|live| {
			if live.node == self.node {
				units.next().cloned().flatten()
			} else {
				None
			}
		}));
		self.lives.extend(lives);
		debug_assert!(
			self.lives.windows(2).all(|pair| pair[0].id < pair[1].id),
			"a table's subscriptions are read in id order"
		);
		Ok(true)
	}

	/// The id the page read last ends at, as a cursor names it: the key's,
	/// which this build reads whether or not it reads the definition.
	fn page_end(&self) -> Result<Option<Uuid>> {
		self.scan.last_key().map(|key| Ok(SubscriptionKey::decode_key(key)?.lq)).transpose()
	}

	/// Replay `event`, captured at `vs`, to the table's subscriptions, through
	/// the same matching, permission, projection and FETCH pipeline the inline
	/// engine runs on the write path.
	///
	/// `index` is the event's place in its entry, and `registered` the
	/// subscriptions the entry's transaction registered among its events,
	/// each with the number of them it was registered after: the event is
	/// passed over for a subscription registered after it, found in
	/// constant time.
	///
	/// Charges `budget` one per subscription the event is replayed to,
	/// evaluated or passed over, one per page read that held only
	/// subscriptions a newer release wrote, and one for an event replayed to
	/// none, so an event costs at least one. Stops when the budget is spent,
	/// before a subscription or after such a page, so a pass replays to no
	/// more subscriptions than its budget however many the table has, and
	/// reads at most one page of them more than those reach. `cursor` is the
	/// last subscription the event was replayed to or passed, `None` for
	/// none yet: the replay starts after it and moves it on as each
	/// subscription is reached, and past each page it could not read — those
	/// subscriptions are their owners' routers' to deliver to — so it names
	/// the place reached whether the call returns or fails, and a replay
	/// resumed after such a page never reads it again. Called with a budget
	/// of at least one.
	pub(crate) async fn replay(
		&mut self,
		vs: u128,
		index: usize,
		event: &LiveEvent,
		registered: &HashMap<Uuid, u32>,
		cursor: &mut Option<Uuid>,
		budget: &mut u64,
	) -> Result<Replayed> {
		// The action selects the same source image and emits the same
		// notification action the inline engine does.
		let images = Images {
			initial: Arc::new(event.before.clone()),
			current: Arc::new(event.after.clone()),
			action: match event.action {
				LiveAction::Create => Action::Create,
				LiveAction::Update => Action::Update,
				LiveAction::Delete => Action::Delete,
			},
		};
		// The subscriptions at or before the cursor were reached by an
		// earlier pass; see [`TableReplay`] for the order.
		let mut at = self.position(*cursor)?;
		let mut reached = 0u64;
		loop {
			if at == self.lives.len() {
				if !self.read_page().await? {
					break;
				}
				if at == self.lives.len() {
					// A page holding only subscriptions a newer release wrote
					// reached none: it cost a read, charged as a subscription
					// passed over is, and the cursor moves past it, so a
					// replay stopped here resumes after it.
					*budget = budget.saturating_sub(1);
					*cursor = self.page_end()?;
					if *budget == 0 {
						return Ok(Replayed::Stopped);
					}
				}
				continue;
			}
			if *budget == 0 {
				return Ok(Replayed::Stopped);
			}
			*budget -= 1;
			reached += 1;
			let subscription = &self.lives[at];
			// A subscription another node owns is that node's router's to
			// deliver, whatever the broker routes to, and one the event
			// precedes is passed over; each at the same cost: the pass read
			// it, and keeps its place after it. Both the commit versionstamp
			// and the capture versionstamp must follow the subscription's
			// watermark so a write captured before the subscription was
			// registered is excluded even when its transaction commits after.
			let captured = if event.captured == 0 {
				vs
			} else {
				event.captured
			};
			if subscription.node == self.node
				&& vs > subscription.watermark
				&& captured > subscription.watermark
				&& registered.get(&subscription.id).is_none_or(|&after| after as usize <= index)
				&& let Some(unit) = &self.units[at]
				&& let Notify::Deliver(notification) = crate::exec::vm::notify_unit(
					unit,
					subscription,
					&self.write_ctx,
					&event.id,
					&images,
				)
				.await
			{
				deliver(&self.broker, &self.write_ctx, subscription, notification)
					.await
					.map_err(into_error)?;
			}
			*cursor = Some(subscription.id);
			at += 1;
		}
		if reached == 0 {
			*budget = budget.saturating_sub(1);
		}
		Ok(Replayed::Complete)
	}
}

/// A scan of `table`'s subscriptions in the database `write_ctx` is resolved
/// in, after the one with id `after`, or of all of them.
fn scan_after(
	write_ctx: &WriteCtx,
	table: &TableName,
	after: Option<Uuid>,
) -> Result<ListScan<StoredSubscriptionDefinition>> {
	let (ns, db) = (write_ctx.ns.namespace_id, write_ctx.db.database_id);
	let range = SubscriptionPrefix {
		ns,
		db,
		tb: Cow::Borrowed(table),
	}
	.range()?;
	Ok(ListScan::new(match after {
		Some(lq) => {
			let key = SubscriptionKey {
				ns,
				db,
				tb: Cow::Borrowed(table),
				lq,
			}
			.encode_key()?;
			range.resume_after(&key, Direction::Forward)
		}
		None => range,
	}))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
	use std::sync::Arc;
	use std::time::Duration;

	use surrealdb_cnf::ConfigMap;

	use crate::channel::Receiver;
	use crate::dbs::{Capabilities, Session};
	use crate::kvs::Datastore;
	use crate::types::{PublicNotification, PublicUuid, PublicValue};

	/// Build a datastore with the given live-query engine and a notification
	/// channel, and without maintenance tasks. These tests drive every router
	/// pass themselves: the router task a default build starts runs passes of
	/// its own on a timer, and one that lands between a test's writes and its
	/// draining of the channel delivers notifications the drain then discards.
	async fn new_ds(engine: &str) -> (Receiver<PublicNotification>, Arc<Datastore>) {
		build_ds_with(ConfigMap::empty().with_key_value("live_query_engine", engine)).await
	}

	/// [`new_ds`] for the Router engine with the event retention
	/// window set: `retention` is a duration, of which the test timestamps
	/// count one millisecond per stamp minted.
	async fn new_router_ds_with_retention(
		retention: &str,
	) -> (Receiver<PublicNotification>, Arc<Datastore>) {
		let config = ConfigMap::empty()
			.with_key_value("live_query_engine", "router")
			.with_key_value("live_query_retention", retention);
		build_ds_with(config).await
	}

	async fn build_ds_with(config: ConfigMap) -> (Receiver<PublicNotification>, Arc<Datastore>) {
		let (send, recv) = crate::channel::bounded(1000);
		let ds = Datastore::builder()
			.without_maintenance_tasks()
			.with_capabilities(Capabilities::all())
			.with_auth(false)
			.with_notify(send)
			.with_config(config)
			.build_with_path("memory")
			.await
			.unwrap();
		(recv, ds)
	}

	fn record_session(ns: &str, db: &str, user_key: &str) -> Session {
		use crate::types::{PublicRecordId, PublicRecordIdKey};
		Session::for_record(
			ns,
			db,
			"user",
			PublicValue::RecordId(PublicRecordId {
				table: "user".to_string().into(),
				key: PublicRecordIdKey::String(user_key.to_string()),
			}),
		)
		.with_rt(true)
	}

	/// Drain notifications until the channel is quiet for 200ms. Inline delivery
	/// flushes on a spawned task, so notifications are not all immediately
	/// available.
	async fn drain(recv: &Receiver<PublicNotification>) -> Vec<PublicNotification> {
		let mut out = Vec::new();
		while let Ok(Ok(n)) = tokio::time::timeout(Duration::from_millis(200), recv.recv()).await {
			out.push(n);
		}
		out
	}

	/// Reduce notifications to a sorted multiset of their *content* — action,
	/// record, and result. The per-subscription live-query id and session id are
	/// deliberately excluded: they are routing/identity metadata that legitimately
	/// differ between two independent datastores. What must be identical between
	/// the inline and router engines is the delivered content, which is where
	/// every matching/permission/projection/diff/fetch guarantee lives.
	fn content(notifs: Vec<PublicNotification>) -> Vec<String> {
		let mut keys: Vec<String> = notifs
			.into_iter()
			.map(|n| format!("{:?}|{:?}|{:?}", n.action, n.record, n.result))
			.collect();
		keys.sort();
		keys
	}

	/// Drive an identical schema + subscriptions + write sequence on a datastore
	/// of the given engine and return the notifications it delivers.
	///
	/// For the Router engine, delivery is performed by the router: a baseline
	/// pass is run after subscriptions and before the writes (establishing the
	/// cursor), then a delivery pass after the writes tails the captured events
	/// and emits the notifications. The inline engine delivers on the write path,
	/// and the router passes are cheap no-ops for it.
	async fn deliver(engine: &str) -> Vec<PublicNotification> {
		let (recv, ds) = new_ds(engine).await;
		let (ns, db) = ("test", "test");
		let owner = Session::owner().with_ns(ns).with_db(db);
		ds.execute(
			"DEFINE TABLE doc PERMISSIONS FOR select FULL; \
			 DEFINE ACCESS user ON DATABASE TYPE RECORD; \
			 DEFINE FIELD owner ON doc TYPE record<user>; \
			 DEFINE FIELD name ON doc TYPE string; \
			 DEFINE FIELD secret ON doc TYPE string PERMISSIONS FOR select WHERE false; \
			 DEFINE FIELD derived ON doc TYPE string \
			     COMPUTED string::concat('derived_', name) \
			     PERMISSIONS FOR select NONE; \
			 DEFINE FIELD mine ON doc TYPE string \
			     COMPUTED string::concat('hi_', name) \
			     PERMISSIONS FOR select WHERE owner = $auth",
			&owner,
			None,
		)
		.await
		.unwrap();

		// A mix of subscribers and projection modes: a privileged owner, a
		// record-access user (alice), an owner DIFF subscription, and a WHERE
		// filter that only matches alice's records.
		let owner_live = Session::owner().with_ns(ns).with_db(db).with_rt(true);
		let alice = record_session(ns, db, "alice");
		ds.execute("LIVE SELECT * FROM doc", &owner_live, None).await.unwrap();
		ds.execute("LIVE SELECT * FROM doc", &alice, None).await.unwrap();
		ds.execute("LIVE SELECT DIFF FROM doc", &owner_live, None).await.unwrap();
		ds.execute("LIVE SELECT * FROM doc WHERE owner = user:alice", &owner_live, None)
			.await
			.unwrap();

		// Clear any notifications produced during setup.
		while recv.try_recv().is_ok() {}

		// Router baseline: establish the cursor after subscriptions, before the
		// writes (a no-op under the inline engine).
		ds.live_query_router_process().await.unwrap();

		// Drive a representative sequence of mutations.
		ds.execute(
			"CREATE doc:1 SET owner = user:alice, name = 'one', secret = 's1'",
			&owner,
			None,
		)
		.await
		.unwrap();
		ds.execute("CREATE doc:2 SET owner = user:bob, name = 'two', secret = 's2'", &owner, None)
			.await
			.unwrap();
		ds.execute("UPDATE doc:1 SET name = 'one-updated'", &owner, None).await.unwrap();
		ds.execute("DELETE doc:2", &owner, None).await.unwrap();

		// Router delivery pass: tail the captured events and emit notifications
		// (a no-op under the inline engine, which already delivered on the write
		// path). Run twice to prove the cursor prevents re-delivery.
		ds.live_query_router_process().await.unwrap();
		ds.live_query_router_process().await.unwrap();

		drain(&recv).await
	}

	/// The correctness gate for the inverted engine: with the Router engine the
	/// router is the sole delivery path (the inline path is a no-op), and the
	/// content it delivers off the write path must be identical to what the
	/// inline engine delivers on the write path — across CREATE/UPDATE/DELETE,
	/// computed-field and conditional field permissions, plain SELECT, DIFF, and
	/// WHERE filtering, for both owner and record-access subscribers. Running the
	/// router twice also pins exactly-once delivery (a duplicate would break the
	/// multiset equality).
	#[tokio::test]
	async fn router_delivery_matches_inline_delivery() {
		let inline = deliver("inline").await;
		let router = deliver("router").await;
		assert!(!inline.is_empty(), "the inline engine must deliver notifications");
		assert!(!router.is_empty(), "the router engine must deliver notifications");
		assert_eq!(
			content(inline),
			content(router),
			"router-delivered content must equal inline-delivered content",
		);
	}

	/// Regression for the startup-window drop (review P1): the router baseline is
	/// established eagerly at datastore construction, so a subscription and a
	/// write that both happen *before the first router pass* must still be
	/// delivered — the first pass must not treat them as pre-baseline history.
	/// (With the previous lazy baseline, the first pass set the cursor to "now"
	/// and silently dropped the captured event.)
	#[tokio::test]
	async fn router_delivers_events_captured_before_first_pass() {
		use surrealdb_kvs::TransactionType::Write;

		use crate::catalog::providers::CatalogProvider;

		let (recv, ds) = new_ds("router").await;
		let (ns, db) = ("test", "test");
		let tx = ds.transaction(Write).await.unwrap();
		tx.ensure_ns_db(None, ns, db).await.unwrap();
		tx.commit().await.unwrap();
		let owner = Session::owner().with_ns(ns).with_db(db);
		ds.execute("DEFINE TABLE doc", &owner, None).await.unwrap();
		// Subscribe and write before any router pass has run.
		let live = Session::owner().with_ns(ns).with_db(db).with_rt(true);
		ds.execute("LIVE SELECT * FROM doc", &live, None).await.unwrap();
		while recv.try_recv().is_ok() {}
		ds.execute("CREATE doc:1 SET n = 1", &owner, None).await.unwrap();
		// The very first router pass must deliver the event captured in the
		// startup window (eager baseline was set at construction, before both).
		ds.live_query_router_process().await.unwrap();
		let notifs = drain(&recv).await;
		assert_eq!(notifs.len(), 1, "first router pass must deliver the startup-window event");
	}

	/// A page of a table's subscriptions written by a newer release reaches
	/// none and is charged like a subscription passed over: a pass whose
	/// budget the namespace and the page spend whole stops there, with the
	/// cursor past the page, and the next pass reaches the subscription
	/// after the page without reading the page again.
	///
	/// The replay took the empty page for the end of the table's
	/// subscriptions, marked the event delivered, and the subscriptions
	/// after the page missed it for good.
	#[tokio::test]
	async fn a_page_of_subscriptions_a_newer_release_wrote_is_passed() {
		use std::borrow::Cow;

		use surrealdb_kvs::TransactionType::Write;
		use surrealdb_strand::TableName;
		use uuid::Uuid;

		use crate::catalog::providers::CatalogProvider;
		use crate::key::KVKey;
		use crate::key::schema::SubscriptionKey;
		use crate::kvs::NORMAL_BATCH_SIZE;
		use crate::kvs::tasks::Progress;
		use crate::kvs::test_support::with_future_floor;

		let (recv, ds) = new_ds("router").await;
		let (ns, db) = ("test", "test");
		let tx = ds.transaction(Write).await.unwrap();
		let database = tx.ensure_ns_db(None, ns, db).await.unwrap();
		tx.commit().await.unwrap();
		let owner = Session::owner().with_ns(ns).with_db(db);
		ds.execute("DEFINE TABLE doc", &owner, None).await.unwrap();
		let live = Session::owner().with_ns(ns).with_db(db).with_rt(true);
		let id = Uuid::from(subscribe(&ds, &live).await).as_u128();
		while recv.try_recv().is_ok() {}
		ds.live_query_router_process().await.unwrap();
		ds.execute("CREATE doc:1 SET n = 1", &owner, None).await.unwrap();

		// A page of subscriptions this build cannot read, sorting before the
		// one it can. Written after the event is captured: a statement's own
		// read of the table's subscriptions refuses them, as it must.
		let page = NORMAL_BATCH_SIZE as u128;
		assert!(id > page, "the page sorts before the subscription");
		let tx = ds.transaction(Write).await.unwrap();
		for n in 1..=page {
			let key = SubscriptionKey {
				ns: database.namespace_id,
				db: database.database_id,
				tb: Cow::Owned(TableName::new("doc")),
				lq: Uuid::from_u128(id - n),
			}
			.encode_key()
			.unwrap();
			tx.set(key, with_future_floor(&[0x00])).await.unwrap();
		}
		tx.commit().await.unwrap();

		// A budget of two: the namespace entered costs one and the page the
		// other, and the pass stops with the cursor past the page.
		let first = ds.live_query_router_process_with_budget(2).await.unwrap();
		assert_eq!(first.progress, Progress::More, "the pass stopped inside the entry: {first:?}");
		assert_eq!(first.items, 0);
		assert!(drain(&recv).await.is_empty(), "nothing is delivered yet");

		// A budget of one, for the subscription alone: the page is not read
		// again, and the lap continues inside the entry without re-entering
		// the namespace.
		let second = ds.live_query_router_process_with_budget(1).await.unwrap();
		assert_eq!(second.items, 1, "the entry is delivered: {second:?}");
		let notifs = drain(&recv).await;
		assert_eq!(notifs.len(), 1, "the subscription after the page is notified: {notifs:?}");
		assert!(format!("{:?}", notifs[0].record).contains('1'));
	}

	/// An event on a table a newer release has since defined is skipped, and
	/// the events captured after it are delivered: the router delivers a
	/// database's entries in capture order, so an entry it cannot replay would
	/// hold every one behind it for as long as the definition stands.
	#[tokio::test]
	async fn an_event_on_a_table_a_newer_release_defined_does_not_hold_the_database() {
		use std::borrow::Cow;

		use surrealdb_kvs::TransactionType::Write;
		use surrealdb_strand::TableName;

		use crate::catalog::providers::CatalogProvider;
		use crate::key::KVKey;
		use crate::key::schema::TableKey;
		use crate::kvs::test_support::with_future_floor;

		let (recv, ds) = new_ds("router").await;
		let (ns, db) = ("test", "test");
		let tx = ds.transaction(Write).await.unwrap();
		let database = tx.ensure_ns_db(None, ns, db).await.unwrap();
		tx.commit().await.unwrap();
		let owner = Session::owner().with_ns(ns).with_db(db);
		ds.execute("DEFINE TABLE future; DEFINE TABLE doc;", &owner, None).await.unwrap();
		let live = Session::owner().with_ns(ns).with_db(db).with_rt(true);
		ds.execute("LIVE SELECT * FROM future", &live, None).await.unwrap();
		subscribe(&ds, &live).await;
		while recv.try_recv().is_ok() {}
		ds.live_query_router_process().await.unwrap();
		ds.execute("CREATE future:1", &owner, None).await.unwrap();
		ds.execute("CREATE doc:1 SET n = 1", &owner, None).await.unwrap();

		// The first event's table, redefined by a newer release.
		let tx = ds.transaction(Write).await.unwrap();
		let key = TableKey {
			ns: database.namespace_id,
			db: database.database_id,
			tb: Cow::Owned(TableName::new("future")),
		}
		.encode_key()
		.unwrap();
		tx.set(key, with_future_floor(&[0x00])).await.unwrap();
		tx.commit().await.unwrap();

		ds.live_query_router_process().await.expect("the entry it cannot replay is skipped");
		let notifs = drain(&recv).await;
		assert_eq!(notifs.len(), 1, "the event after it is delivered: {notifs:?}");
		assert!(format!("{:?}", notifs[0].record).contains("doc"), "{notifs:?}");
	}

	/// A router pass delivers at most its budget of captured entries, and the
	/// lap it continues delivers up to the watermark the lap began with.
	///
	/// Every entry is replayed through the subscriber pipeline, of user-defined
	/// cost, so a backlog would otherwise be delivered whole in one pass, in a
	/// slice a host driving maintenance itself waits on. A pass that stops with
	/// entries still to deliver keeps its lap and reports `More`, so the next
	/// pass continues it; the cursor advances only once the lap has delivered
	/// every database, so nothing is delivered twice or skipped, and an event
	/// captured past the lap's watermark is the next lap's.
	#[tokio::test]
	async fn router_delivery_is_paged_across_passes() {
		use surrealdb_kvs::TransactionType::Write;

		use crate::catalog::providers::CatalogProvider;
		use crate::kvs::tasks::Progress;

		// The datastore's own router task would continue a lap the test leaves
		// stopped, so the test drives every pass itself.
		let (recv, ds) = new_ds("router").await;
		let (ns, db) = ("test", "test");
		// The lap lists the databases through the catalog, so the database is
		// defined rather than left to the table definition to imply.
		let tx = ds.transaction(Write).await.unwrap();
		tx.ensure_ns_db(None, ns, db).await.unwrap();
		tx.commit().await.unwrap();
		let owner = Session::owner().with_ns(ns).with_db(db);
		ds.execute("DEFINE TABLE doc", &owner, None).await.unwrap();
		let live = Session::owner().with_ns(ns).with_db(db).with_rt(true);
		ds.execute("LIVE SELECT * FROM doc", &live, None).await.unwrap();
		while recv.try_recv().is_ok() {}
		// Baseline: the cursor is set after the subscription, before the writes.
		ds.live_query_router_process().await.unwrap();
		for n in 1..=3 {
			ds.execute(&format!("CREATE doc:{n} SET n = {n}"), &owner, None).await.unwrap();
		}

		// A budget of three: the namespace entered costs one, and the pass
		// stops with one entry still to deliver.
		let first = ds.live_query_router_process_with_budget(3).await.unwrap();
		assert_eq!(first.progress, Progress::More, "the pass stopped with an entry to deliver");
		assert_eq!(first.items, 2);
		let notifs = drain(&recv).await;
		assert_eq!(notifs.len(), 2, "two entries delivered, and no more: {notifs:?}");

		// An event captured while the lap is stopped is past its watermark.
		ds.execute("CREATE doc:4 SET n = 4", &owner, None).await.unwrap();

		let second = ds.live_query_router_process_with_budget(3).await.unwrap();
		assert_eq!(second.progress, Progress::Done, "the lap ends at the watermark it began with");
		assert_eq!(second.items, 1);
		let notifs = drain(&recv).await;
		assert_eq!(notifs.len(), 1, "the entry the first pass stopped at, and no more");
		let record = format!("{:?}", notifs[0].record);
		assert!(
			record.contains('3') && !record.contains('4'),
			"not the one captured since: {record}"
		);

		// The next lap delivers what was captured since, once.
		let third = ds.live_query_router_process_with_budget(3).await.unwrap();
		assert_eq!(third.progress, Progress::Done);
		assert_eq!(third.items, 1);
		assert_eq!(drain(&recv).await.len(), 1);
		let fourth = ds.live_query_router_process_with_budget(3).await.unwrap();
		assert_eq!(fourth.items, 0, "nothing is delivered twice");
		assert!(drain(&recv).await.is_empty());
	}

	/// A pass stops inside an entry whose events outnumber what its budget has
	/// left, and the next pass continues in it.
	///
	/// An entry holds every change one commit made to a table, so a bulk write
	/// captures in one entry what a pass would otherwise replay whole — every
	/// event through the subscriber pipeline, of user-defined cost — however
	/// small its budget. The budget counts the subscriptions events are
	/// evaluated against, one here per event, and a pass keeps its place in
	/// the entry it stops inside, so the write is delivered over as many
	/// passes as its events fill, each once.
	#[tokio::test]
	async fn router_pass_stops_inside_an_entry_at_its_budget() {
		use surrealdb_kvs::TransactionType::Write;

		use crate::catalog::providers::CatalogProvider;
		use crate::kvs::tasks::Progress;

		let (recv, ds) = new_ds("router").await;
		let (ns, db) = ("test", "test");
		let tx = ds.transaction(Write).await.unwrap();
		tx.ensure_ns_db(None, ns, db).await.unwrap();
		tx.commit().await.unwrap();
		let owner = Session::owner().with_ns(ns).with_db(db);
		ds.execute("DEFINE TABLE doc", &owner, None).await.unwrap();
		let live = Session::owner().with_ns(ns).with_db(db).with_rt(true);
		ds.execute("LIVE SELECT * FROM doc", &live, None).await.unwrap();
		while recv.try_recv().is_ok() {}
		ds.live_query_router_process().await.unwrap();
		// Five records in one commit: one entry of five events.
		ds.execute("CREATE doc:1, doc:2, doc:3, doc:4, doc:5", &owner, None).await.unwrap();

		// A budget of three: the namespace entered costs one, and the pass
		// delivers two of the entry's five events and stops inside it.
		let first = ds.live_query_router_process_with_budget(3).await.unwrap();
		assert_eq!(first.progress, Progress::More, "the pass stopped inside the entry");
		assert_eq!(first.items, 2);
		delivered_records(&drain(&recv).await, &['1', '2']);

		// The next pass continues in the entry, and stops inside it again.
		let second = ds.live_query_router_process_with_budget(2).await.unwrap();
		assert_eq!(second.progress, Progress::More);
		assert_eq!(second.items, 2);
		delivered_records(&drain(&recv).await, &['3', '4']);

		// The last event ends the entry, and the lap.
		let third = ds.live_query_router_process_with_budget(2).await.unwrap();
		assert_eq!(third.progress, Progress::Done, "the lap has delivered the entry");
		assert_eq!(third.items, 1);
		delivered_records(&drain(&recv).await, &['5']);

		// Nothing is delivered twice.
		let fourth = ds.live_query_router_process_with_budget(3).await.unwrap();
		assert_eq!(fourth.items, 0);
		assert!(drain(&recv).await.is_empty());
	}

	/// A pass that continues an entry an earlier pass stopped inside delivers
	/// its events from the copy the lap keeps, without reading the entry
	/// again, so a bulk write is read and decoded once however many passes
	/// its events span. The stored entry removed while the lap waits between
	/// passes, as the collector removes one past the retention window, leaves
	/// the rest of its events still to deliver, each once.
	#[tokio::test]
	async fn an_entry_a_pass_stopped_inside_is_read_once() {
		use surrealdb_kvs::TransactionType::Write;

		use crate::catalog::providers::CatalogProvider;
		use crate::key::schema::LiveEventsPrefix;
		use crate::kvs::tasks::Progress;

		let (recv, ds) = new_ds("router").await;
		let (ns, db) = ("test", "test");
		let tx = ds.transaction(Write).await.unwrap();
		let database = tx.ensure_ns_db(None, ns, db).await.unwrap();
		tx.commit().await.unwrap();
		let owner = Session::owner().with_ns(ns).with_db(db);
		ds.execute("DEFINE TABLE doc", &owner, None).await.unwrap();
		let live = Session::owner().with_ns(ns).with_db(db).with_rt(true);
		ds.execute("LIVE SELECT * FROM doc", &live, None).await.unwrap();
		while recv.try_recv().is_ok() {}
		ds.live_query_router_process().await.unwrap();
		// Five records in one commit: one entry of five events.
		ds.execute("CREATE doc:1, doc:2, doc:3, doc:4, doc:5", &owner, None).await.unwrap();

		// A budget of three: the namespace entered costs one, and the pass
		// delivers two of the entry's five events and stops inside it.
		let first = ds.live_query_router_process_with_budget(3).await.unwrap();
		assert_eq!(first.progress, Progress::More, "the pass stopped inside the entry");
		delivered_records(&drain(&recv).await, &['1', '2']);

		// Every captured entry of the database is removed from the store.
		let tx = ds.transaction(Write).await.unwrap();
		tx.delr(
			LiveEventsPrefix {
				ns: database.namespace_id,
				db: database.database_id,
			}
			.range()
			.unwrap(),
		)
		.await
		.unwrap();
		tx.commit().await.unwrap();

		// The passes that continue the entry deliver the rest of its events.
		let second = ds.live_query_router_process_with_budget(2).await.unwrap();
		assert_eq!(second.progress, Progress::More);
		assert_eq!(second.items, 2, "{second:?}");
		delivered_records(&drain(&recv).await, &['3', '4']);
		let third = ds.live_query_router_process_with_budget(2).await.unwrap();
		assert_eq!(third.progress, Progress::Done, "the lap has delivered the entry");
		assert_eq!(third.items, 1);
		delivered_records(&drain(&recv).await, &['5']);

		// Nothing is delivered twice.
		let fourth = ds.live_query_router_process_with_budget(3).await.unwrap();
		assert_eq!(fourth.items, 0);
		assert!(drain(&recv).await.is_empty());
	}

	/// The collector keeps an event for the retention window from its capture,
	/// and a lap's unreached events are protected from it only by the snapshot
	/// of the pass that reads them. A lap the window has passed — suspended
	/// between passes while it did, or begun behind it — is reported once, by
	/// the pass that finds it so; a lap the window has not reached is not.
	#[tokio::test]
	async fn a_lap_behind_the_retention_window_is_reported_once() {
		use surrealdb_kvs::TransactionType::Write;

		use crate::catalog::providers::CatalogProvider;
		use crate::kvs::tasks::Progress;

		// The window is a hundred stamps, and every captured event mints one.
		// The memory backend's stamp counter is the process's, so the test
		// needs a process of its own, as nextest gives each test: stamps
		// minted by tests running beside it in one process move the window.
		let (recv, ds) = new_router_ds_with_retention("100ms").await;
		let (ns, db) = ("test", "test");
		let tx = ds.transaction(Write).await.unwrap();
		tx.ensure_ns_db(None, ns, db).await.unwrap();
		tx.commit().await.unwrap();
		let owner = Session::owner().with_ns(ns).with_db(db);
		ds.execute("DEFINE TABLE doc", &owner, None).await.unwrap();
		let live = Session::owner().with_ns(ns).with_db(db).with_rt(true);
		ds.execute("LIVE SELECT * FROM doc", &live, None).await.unwrap();
		while recv.try_recv().is_ok() {}
		let router = ds.live_query_router();

		// A lap begun with its cursor inside the window is not reported.
		ds.live_query_router_process().await.unwrap();
		assert_eq!(router.laps_behind_retention(), 0);

		// A lap suspended between passes: ten events captured, one delivered.
		create_each(&ds, &owner, 10).await;
		let first = ds.live_query_router_process_with_budget(2).await.unwrap();
		assert_eq!(first.progress, Progress::More, "the lap is suspended");
		assert_eq!(first.items, 1);
		assert_eq!(router.laps_behind_retention(), 0, "the window has not passed the cursor");

		// Events captured past the lap's watermark are the next lap's, and the
		// stamps they mint move the window past the cursor this lap began
		// after while it waits: the pass that continues it reports the lap,
		// and the passes after it do not.
		create_each(&ds, &owner, 150).await;
		let second = ds.live_query_router_process_with_budget(2).await.unwrap();
		assert_eq!(second.progress, Progress::More);
		assert_eq!(router.laps_behind_retention(), 1, "the continuing pass reports the lap");
		let third = ds.live_query_router_process_with_budget(2).await.unwrap();
		assert_eq!(third.progress, Progress::More);
		assert_eq!(router.laps_behind_retention(), 1, "a lap is reported once");
		let rest = ds.live_query_router_process().await.unwrap();
		assert_eq!(rest.progress, Progress::Done, "the lap ends at its watermark");
		assert_eq!(router.laps_behind_retention(), 1);

		// A lap begun behind the window: the events waiting beyond the last
		// lap's watermark outran it by more than the window, so the pass that
		// begins the lap reports it.
		let behind = ds.live_query_router_process().await.unwrap();
		assert_eq!(behind.progress, Progress::Done);
		assert_eq!(behind.items, 150, "every event still retained is delivered");
		assert_eq!(router.laps_behind_retention(), 2, "the beginning pass reports the lap");

		// A lap begun inside the window again is not.
		let quiet = ds.live_query_router_process().await.unwrap();
		assert_eq!(quiet.items, 0);
		assert_eq!(router.laps_behind_retention(), 2);
		assert_eq!(drain(&recv).await.len(), 160, "every event was delivered once");
	}

	/// What the window bounds: an event the router has yet to deliver when
	/// the window passes it is collected before delivery, and the lap
	/// continues past it. A lap is suspended between passes while the window
	/// passes its cursor and the collector runs; the pass that continues it
	/// reports the lap and finds nothing left of it, and the events are never
	/// delivered.
	#[tokio::test]
	async fn events_the_window_no_longer_retains_are_collected_before_delivery() {
		use surrealdb_kvs::TransactionType::{Read, Write};
		use tokio_util::sync::CancellationToken;

		use crate::catalog::providers::{CatalogProvider, DatabaseProvider};
		use crate::kvs::tasklease::{LeaseHandler, TaskLeaseType};
		use crate::kvs::tasks::Progress;

		let (recv, ds) = new_router_ds_with_retention("100ms").await;
		let (ns, db) = ("test", "test");
		let tx = ds.transaction(Write).await.unwrap();
		tx.ensure_ns_db(None, ns, db).await.unwrap();
		tx.commit().await.unwrap();
		let owner = Session::owner().with_ns(ns).with_db(db);
		ds.execute("DEFINE TABLE doc", &owner, None).await.unwrap();
		let live = Session::owner().with_ns(ns).with_db(db).with_rt(true);
		ds.execute("LIVE SELECT * FROM doc", &live, None).await.unwrap();
		while recv.try_recv().is_ok() {}
		let router = ds.live_query_router();
		ds.live_query_router_process().await.unwrap();

		// A lap suspended inside its backlog: ten events captured, one
		// delivered, nine unreached. The events captured past its watermark
		// are the next lap's.
		create_each(&ds, &owner, 10).await;
		let first = ds.live_query_router_process_with_budget(2).await.unwrap();
		assert_eq!(first.progress, Progress::More);
		assert_eq!(first.items, 1);
		create_each(&ds, &owner, 150).await;

		// The window has passed the lap's cursor, and the collector removes
		// the events behind the window — the lap's nine among them.
		let (ns_id, db_id) = {
			let tx = ds.transaction(Read).await.unwrap();
			let db = tx.get_db_by_name(ns, db, None).await.unwrap().expect("the test database");
			tx.cancel().await.unwrap();
			(db.namespace_id, db.database_id)
		};
		let lh = LeaseHandler::new(
			ds.sequences().clone(),
			uuid::Uuid::now_v7(),
			ds.transaction_factory().clone(),
			TaskLeaseType::ChangeFeedCleanup,
			Duration::from_secs(60),
		)
		.unwrap();
		assert!(lh.has_lease().await.unwrap());
		let mut budget = u64::MAX;
		assert!(
			crate::lq::gc::gc_databases(
				&ds,
				&lh,
				Duration::from_millis(100),
				&CancellationToken::new(),
				&mut budget,
				&[(ns_id, db_id)],
			)
			.await
			.unwrap(),
			"the collector kept the lease"
		);
		let collected = u64::MAX - budget;
		assert!(collected >= 10, "the events behind the window are collected: {collected}");

		// The pass that continues the lap reports it and finds nothing of it
		// left: the nine are never delivered.
		let rest = ds.live_query_router_process().await.unwrap();
		assert_eq!(rest.progress, Progress::Done);
		assert_eq!(rest.items, 0, "the lap's unreached events were collected");
		assert_eq!(router.laps_behind_retention(), 1, "the continuing pass reported the lap");

		// The next lap begins behind the window too, and delivers only what
		// the collector left of the events captured past the last watermark.
		let next = ds.live_query_router_process().await.unwrap();
		assert_eq!(next.progress, Progress::Done);
		assert!(next.items > 0 && next.items < 150, "the survivors are delivered: {}", next.items);
		assert_eq!(router.laps_behind_retention(), 2, "the beginning pass reported the lap");
		assert_eq!(
			drain(&recv).await.len() as u64,
			1 + next.items,
			"nothing collected is delivered"
		);
	}

	/// The collector sweeps the catalog as the changefeed collector does, which
	/// skips a database whole when a table definition in it needs a newer
	/// release, since the database's changefeed retention cannot be known. The live-query window
	/// is the datastore's, not a table's, so such a database's events are
	/// collected all the same: a node on the older release holding the lease
	/// would otherwise let them grow for as long as it held it.
	#[tokio::test]
	async fn events_of_a_database_with_an_unreadable_table_are_still_collected() {
		use std::borrow::Cow;

		use surrealdb_kvs::TransactionType::{Read, Write};
		use surrealdb_strand::TableName;
		use tokio_util::sync::CancellationToken;

		use crate::catalog::providers::CatalogProvider;
		use crate::catalog::{DatabaseId, NamespaceId};
		use crate::key::KVKey;
		use crate::key::schema::{LiveEventsPrefix, TableKey};
		use crate::kvs::test_support::with_future_floor;

		async fn events(ds: &Datastore, ns: NamespaceId, db: DatabaseId) -> usize {
			let range = LiveEventsPrefix {
				ns,
				db,
			}
			.range()
			.unwrap();
			let tx = ds.transaction(Read).await.unwrap();
			let n = tx.keys_raw(range, 1000, 0, None).await.unwrap().len();
			tx.cancel().await.unwrap();
			n
		}

		// A retention of a nanosecond puts every event captured before the
		// pass behind the window.
		let (recv, ds) = new_router_ds_with_retention("1ns").await;
		let (ns, db) = ("test", "test");
		let tx = ds.transaction(Write).await.unwrap();
		let database = tx.ensure_ns_db(None, ns, db).await.unwrap();
		tx.commit().await.unwrap();
		let (ns_id, db_id) = (database.namespace_id, database.database_id);
		let owner = Session::owner().with_ns(ns).with_db(db);
		ds.execute("DEFINE TABLE doc", &owner, None).await.unwrap();
		let live = Session::owner().with_ns(ns).with_db(db).with_rt(true);
		ds.execute("LIVE SELECT * FROM doc", &live, None).await.unwrap();
		while recv.try_recv().is_ok() {}
		create_each(&ds, &owner, 5).await;
		assert_eq!(events(&ds, ns_id, db_id).await, 5, "each create captured an event");

		// A table definition this build cannot read, under the database.
		let tx = ds.transaction(Write).await.unwrap();
		let key = TableKey {
			ns: ns_id,
			db: db_id,
			tb: Cow::Owned(TableName::new("future")),
		}
		.encode_key()
		.unwrap();
		tx.set(key, with_future_floor(&[0x00])).await.unwrap();
		tx.commit().await.unwrap();

		let pass = ds
			.live_query_gc_process(&Duration::from_secs(1), &CancellationToken::new())
			.await
			.unwrap();
		assert!(pass.items >= 5, "the events behind the window are collected: {pass:?}");
		assert_eq!(events(&ds, ns_id, db_id).await, 0, "the database was collected");
	}

	/// The event keyspace is collected whichever engine delivers live queries:
	/// a datastore restarted under the Inline engine captures no new events,
	/// and the ones the Router engine captured before the restart still age
	/// out rather than stay on disk for good.
	#[tokio::test]
	async fn events_captured_before_a_switch_of_engine_are_still_collected() {
		use surrealdb_kvs::TransactionType::{Read, Write};
		use surrealdb_strand::TableName;
		use tokio_util::sync::CancellationToken;

		use crate::catalog::providers::CatalogProvider;
		use crate::catalog::{DatabaseId, NamespaceId};
		use crate::key::schema::LiveEventsPrefix;
		use crate::val::{RecordId, Value};

		async fn events(ds: &Datastore, ns: NamespaceId, db: DatabaseId) -> usize {
			let range = LiveEventsPrefix {
				ns,
				db,
			}
			.range()
			.unwrap();
			let tx = ds.transaction(Read).await.unwrap();
			let n = tx.keys_raw(range, 1000, 0, None).await.unwrap().len();
			tx.cancel().await.unwrap();
			n
		}

		// A retention of a nanosecond puts every event captured before the
		// pass behind the window.
		let config = ConfigMap::empty()
			.with_key_value("live_query_engine", "inline")
			.with_key_value("live_query_retention", "1ns");
		let (_recv, ds) = build_ds_with(config).await;
		let tx = ds.transaction(Write).await.unwrap();
		let database = tx.ensure_ns_db(None, "test", "test").await.unwrap();
		tx.commit().await.unwrap();
		let (ns_id, db_id) = (database.namespace_id, database.database_id);

		// An entry as the Router engine captures one, left from before the
		// datastore ran under the Inline engine.
		let tx = ds.transaction(Write).await.unwrap();
		let tb = TableName::new("doc");
		let id = RecordId::new(tb.clone(), 1);
		tx.live_event_buffer_record_change(ns_id, db_id, &tb, &id, Value::None, Value::None, 0);
		tx.commit().await.unwrap();
		assert_eq!(events(&ds, ns_id, db_id).await, 1, "the entry is stored");

		let pass = ds
			.live_query_gc_process(&Duration::from_secs(1), &CancellationToken::new())
			.await
			.unwrap();
		assert!(pass.items >= 1, "the entry behind the window is collected: {pass:?}");
		assert_eq!(events(&ds, ns_id, db_id).await, 0, "the database was collected");
	}

	/// A namespace or database definition a newer release wrote hides the id
	/// its keys are addressed by, so the collector's sweep walks past the
	/// database without naming it. The sweep then probes the keyspace for
	/// database subtrees, and the collector collects each one's events under
	/// the datastore's window: a node on the older release holding the lease
	/// would otherwise let them grow for as long as it held it.
	#[tokio::test]
	async fn events_of_a_database_whose_definition_is_unreadable_are_still_collected() {
		use std::borrow::Cow;

		use surrealdb_kvs::TransactionType::{Read, Write};
		use tokio_util::sync::CancellationToken;

		use crate::catalog::providers::CatalogProvider;
		use crate::key::KVKey;
		use crate::key::schema::{DatabaseKey, LiveEventsPrefix, NamespaceKey};
		use crate::kvs::test_support::with_future_floor;

		for level in ["namespace", "database"] {
			let (recv, ds) = new_router_ds_with_retention("1ns").await;
			let (ns, db) = ("test", "test");
			let tx = ds.transaction(Write).await.unwrap();
			let database = tx.ensure_ns_db(None, ns, db).await.unwrap();
			tx.commit().await.unwrap();
			let events = LiveEventsPrefix {
				ns: database.namespace_id,
				db: database.database_id,
			}
			.range()
			.unwrap();
			let count = || async {
				let tx = ds.transaction(Read).await.unwrap();
				let n = tx.keys_raw(events.clone(), 1000, 0, None).await.unwrap().len();
				tx.cancel().await.unwrap();
				n
			};
			let owner = Session::owner().with_ns(ns).with_db(db);
			ds.execute("DEFINE TABLE doc", &owner, None).await.unwrap();
			let live = Session::owner().with_ns(ns).with_db(db).with_rt(true);
			ds.execute("LIVE SELECT * FROM doc", &live, None).await.unwrap();
			while recv.try_recv().is_ok() {}
			create_each(&ds, &owner, 5).await;
			assert_eq!(count().await, 5, "each create captured an event");

			let key = match level {
				"namespace" => NamespaceKey::new(Cow::Borrowed(ns)).encode_key(),
				_ => DatabaseKey {
					ns: database.namespace_id,
					db: Cow::Borrowed(db),
				}
				.encode_key(),
			}
			.unwrap();
			let tx = ds.transaction(Write).await.unwrap();
			tx.set(key, with_future_floor(&[0x00])).await.unwrap();
			tx.commit().await.unwrap();

			let pass = ds
				.live_query_gc_process(&Duration::from_secs(1), &CancellationToken::new())
				.await
				.unwrap();
			assert!(
				pass.items >= 5,
				"{level}: the events behind the window are collected: {pass:?}"
			);
			assert_eq!(count().await, 0, "{level}: the database was collected");
		}
	}

	/// `count` records created a commit each, so each captures an event of
	/// its own and mints a stamp of its own.
	async fn create_each(ds: &Datastore, owner: &Session, count: usize) {
		for _ in 0..count {
			ds.execute("CREATE doc", owner, None).await.unwrap();
		}
	}

	/// A pass stops inside an event whose subscriptions outnumber what its
	/// budget has left, and the next pass continues in it.
	///
	/// An event is evaluated against every subscription of its table, each
	/// through the subscriber pipeline, so a table with many subscriptions
	/// would otherwise make one event cost a pass however much. The budget
	/// counts subscriptions evaluated, and a pass keeps its place among them,
	/// so the event reaches each subscription once, over as many passes as
	/// they fill.
	#[tokio::test]
	async fn router_pass_stops_inside_an_event_at_its_subscriptions() {
		use surrealdb_kvs::TransactionType::Write;

		use crate::catalog::providers::CatalogProvider;
		use crate::kvs::tasks::Progress;

		let (recv, ds) = new_ds("router").await;
		let (ns, db) = ("test", "test");
		let tx = ds.transaction(Write).await.unwrap();
		tx.ensure_ns_db(None, ns, db).await.unwrap();
		tx.commit().await.unwrap();
		let owner = Session::owner().with_ns(ns).with_db(db);
		ds.execute("DEFINE TABLE doc", &owner, None).await.unwrap();
		let live = Session::owner().with_ns(ns).with_db(db).with_rt(true);
		let mut subscriptions = vec![
			subscribe(&ds, &live).await,
			subscribe(&ds, &live).await,
			subscribe(&ds, &live).await,
		];
		subscriptions.sort();
		while recv.try_recv().is_ok() {}
		ds.live_query_router_process().await.unwrap();
		// One event, for three subscriptions.
		ds.execute("CREATE doc:1", &owner, None).await.unwrap();

		// A budget of three: the namespace entered costs one, and the pass
		// evaluates two of the event's three subscriptions and stops inside
		// it.
		let first = ds.live_query_router_process_with_budget(3).await.unwrap();
		assert_eq!(first.progress, Progress::More, "the pass stopped inside the event");
		assert_eq!(first.items, 0, "the event is not delivered whole");
		let notifs = drain(&recv).await;
		let first_to = delivered_to(&notifs);
		assert_eq!(first_to.len(), 2, "{notifs:?}");

		// The next pass continues with the subscription left, and no other.
		let second = ds.live_query_router_process_with_budget(2).await.unwrap();
		assert_eq!(second.progress, Progress::Done, "the lap has delivered the event");
		assert_eq!(second.items, 1);
		let notifs = drain(&recv).await;
		let second_to = delivered_to(&notifs);
		assert_eq!(second_to.len(), 1, "{notifs:?}");
		let mut all = first_to;
		all.extend(second_to);
		all.sort();
		assert_eq!(all, subscriptions, "each subscription once");

		// Nothing is delivered twice.
		let third = ds.live_query_router_process_with_budget(3).await.unwrap();
		assert_eq!(third.items, 0);
		assert!(drain(&recv).await.is_empty());
	}

	/// A pass that fails keeps its lap at the place it reached, so the next
	/// pass continues there and nothing the lap delivered is delivered again.
	///
	/// The cursor advances only once a lap has delivered every database, so
	/// a lap begun again after a failure would start at the cursor and
	/// replay the entries the passes before the failure delivered; and a
	/// pass records each event it delivers as it goes, so the events it
	/// delivered before failing are not replayed either.
	#[tokio::test]
	async fn router_pass_that_fails_keeps_its_lap_where_it_reached() {
		use surrealdb_kvs::TransactionType::Write;

		use crate::catalog::providers::CatalogProvider;
		use crate::kvs::tasks::{PassFailure, Progress};
		use crate::kvs::testing::{NonRetryableErrorSite, inject_non_retryable_error};

		let (recv, ds) = new_ds("router").await;
		let (ns, db) = ("test", "test");
		let tx = ds.transaction(Write).await.unwrap();
		tx.ensure_ns_db(None, ns, db).await.unwrap();
		tx.commit().await.unwrap();
		let owner = Session::owner().with_ns(ns).with_db(db);
		ds.execute("DEFINE TABLE doc", &owner, None).await.unwrap();
		let live = Session::owner().with_ns(ns).with_db(db).with_rt(true);
		ds.execute("LIVE SELECT * FROM doc", &live, None).await.unwrap();
		while recv.try_recv().is_ok() {}
		ds.live_query_router_process().await.unwrap();
		// Five records in one commit: one entry of five events.
		ds.execute("CREATE doc:1, doc:2, doc:3, doc:4, doc:5", &owner, None).await.unwrap();

		// A budget of three: the namespace entered costs one, and the pass
		// delivers two of the entry's five events and stops inside it.
		let first = ds.live_query_router_process_with_budget(3).await.unwrap();
		assert_eq!(first.progress, Progress::More, "the pass stopped inside the entry");
		delivered_records(&drain(&recv).await, &['1', '2']);

		// The next pass delivers one more event, then fails.
		let guard =
			inject_non_retryable_error(NonRetryableErrorSite::LiveQueryRouterAfterEvent, ds.id());
		let failed = ds
			.live_query_router_process_with_budget(3)
			.await
			.expect_err("the injected failure ends the pass");
		drop(guard);
		delivered_records(&drain(&recv).await, &['3']);
		// The failure carries what the pass delivered, and the backlog the
		// kept lap holds.
		let PassFailure(pass) = failed.downcast_ref::<PassFailure>().expect("the pass is attached");
		assert_eq!((pass.items, pass.ran), (1, 1));
		assert_eq!(pass.progress, Progress::More);

		// The pass after continues with the events left, and no other: not
		// the two the lap delivered before the failure, nor the one the
		// failing pass delivered.
		let third = ds.live_query_router_process_with_budget(5).await.unwrap();
		assert_eq!(third.progress, Progress::Done, "the lap has delivered the entry");
		assert_eq!(third.items, 2);
		delivered_records(&drain(&recv).await, &['4', '5']);

		// Nothing is delivered twice.
		let fourth = ds.live_query_router_process_with_budget(3).await.unwrap();
		assert_eq!(fourth.items, 0);
		assert!(drain(&recv).await.is_empty());
	}

	/// Registers a subscription to `doc` for `sess`, and returns its id.
	async fn subscribe(ds: &Datastore, sess: &Session) -> PublicUuid {
		let mut res = ds.execute("LIVE SELECT * FROM doc", sess, None).await.unwrap();
		let value = res.remove(0).result.unwrap();
		let PublicValue::Uuid(id) = &value else {
			panic!("expected the subscription's id, got {value:?}");
		};
		*id
	}

	/// Asserts the notifications are about the records whose keys are
	/// `keys`, in that order.
	fn delivered_records(notifs: &[PublicNotification], keys: &[char]) {
		assert_eq!(notifs.len(), keys.len(), "{notifs:?}");
		for (notif, key) in notifs.iter().zip(keys) {
			let record = format!("{:?}", notif.record);
			assert!(record.contains(*key), "expected doc:{key}: {record}");
		}
	}

	/// The ids the notifications were delivered to, sorted.
	fn delivered_to(notifs: &[PublicNotification]) -> Vec<PublicUuid> {
		let mut ids: Vec<_> = notifs.iter().map(|n| n.id).collect();
		ids.sort();
		ids
	}

	/// The id the one `LIVE` statement among `responses` returned; every
	/// other statement must have succeeded.
	fn live_id(responses: Vec<crate::dbs::QueryResult>) -> PublicUuid {
		let mut ids = Vec::new();
		for response in responses {
			if let PublicValue::Uuid(id) = response.result.unwrap() {
				ids.push(id);
			}
		}
		assert_eq!(ids.len(), 1, "one LIVE statement returns one id: {ids:?}");
		ids.remove(0)
	}

	/// A subscription registered part-way through a transaction is notified
	/// of the writes after it in that transaction and of none before it, as
	/// the inline engine notifies it (`kvs/tests/tx_engine_test.rs`). The
	/// write before it is captured for the subscription that already existed,
	/// and the transaction's events all commit above the watermark the new
	/// subscription was registered against, so the watermark alone does not
	/// tell the two writes apart.
	#[tokio::test]
	async fn router_delivers_a_mid_transaction_live_the_writes_after_it_alone() {
		use surrealdb_kvs::TransactionType::Write;

		use crate::catalog::providers::CatalogProvider;

		let (recv, ds) = new_ds("router").await;
		let (ns, db) = ("test", "test");
		let tx = ds.transaction(Write).await.unwrap();
		tx.ensure_ns_db(None, ns, db).await.unwrap();
		tx.commit().await.unwrap();
		let owner = Session::owner().with_ns(ns).with_db(db);
		ds.execute("DEFINE TABLE doc", &owner, None).await.unwrap();
		let live = Session::owner().with_ns(ns).with_db(db).with_rt(true);
		let earlier = subscribe(&ds, &live).await;
		let res = ds
			.execute(
				"BEGIN; CREATE doc:0; LIVE SELECT * FROM doc; CREATE doc:1; COMMIT;",
				&live,
				None,
			)
			.await
			.unwrap();
		let later = live_id(res);
		while recv.try_recv().is_ok() {}

		ds.live_query_router_process().await.unwrap();
		let notifs = drain(&recv).await;
		let to_earlier: Vec<_> = notifs.iter().filter(|n| n.id == earlier).cloned().collect();
		delivered_records(&to_earlier, &['0', '1']);
		let to_later: Vec<_> = notifs.iter().filter(|n| n.id == later).cloned().collect();
		delivered_records(&to_later, &['1']);
	}

	/// With no subscription on the table before the transaction, the write
	/// before its `LIVE` is captured for nobody, and the one after it is
	/// captured for the new subscription.
	#[tokio::test]
	async fn router_captures_a_transactions_writes_from_its_live_on() {
		use surrealdb_kvs::TransactionType::Write;

		use crate::catalog::providers::CatalogProvider;

		let (recv, ds) = new_ds("router").await;
		let (ns, db) = ("test", "test");
		let tx = ds.transaction(Write).await.unwrap();
		tx.ensure_ns_db(None, ns, db).await.unwrap();
		tx.commit().await.unwrap();
		let owner = Session::owner().with_ns(ns).with_db(db);
		ds.execute("DEFINE TABLE doc", &owner, None).await.unwrap();
		let live = Session::owner().with_ns(ns).with_db(db).with_rt(true);
		let res = ds
			.execute(
				"BEGIN; CREATE doc:0; LIVE SELECT * FROM doc; CREATE doc:1; COMMIT;",
				&live,
				None,
			)
			.await
			.unwrap();
		let later = live_id(res);
		while recv.try_recv().is_ok() {}

		ds.live_query_router_process().await.unwrap();
		let notifs = drain(&recv).await;
		assert_eq!(delivered_to(&notifs), vec![later]);
		delivered_records(&notifs, &['1']);
	}

	/// A subscription receives no event captured before it was registered,
	/// however long the router takes to reach that event.
	///
	/// A pass finds every subscription committed before its transaction
	/// began, including one registered after an event was captured but
	/// before a pass delivers it. Without the watermark the subscription was
	/// registered against, that event, which precedes the subscription, would
	/// be delivered to it.
	#[tokio::test]
	async fn router_delivers_nothing_captured_before_a_subscription() {
		use surrealdb_kvs::TransactionType::Write;

		use crate::catalog::providers::CatalogProvider;

		let (recv, ds) = new_ds("router").await;
		let (ns, db) = ("test", "test");
		let tx = ds.transaction(Write).await.unwrap();
		tx.ensure_ns_db(None, ns, db).await.unwrap();
		tx.commit().await.unwrap();
		let owner = Session::owner().with_ns(ns).with_db(db);
		ds.execute("DEFINE TABLE doc", &owner, None).await.unwrap();
		let live = Session::owner().with_ns(ns).with_db(db).with_rt(true);
		let first = subscribe(&ds, &live).await;
		// Captured, and not yet delivered, when the second subscription is
		// registered.
		ds.execute("CREATE doc:1", &owner, None).await.unwrap();
		let second = subscribe(&ds, &live).await;
		while recv.try_recv().is_ok() {}

		ds.live_query_router_process().await.unwrap();
		let notifs = drain(&recv).await;
		assert_eq!(delivered_to(&notifs), vec![first], "{notifs:?}");

		// An event captured after both is delivered to both.
		ds.execute("CREATE doc:2", &owner, None).await.unwrap();
		ds.live_query_router_process().await.unwrap();
		let notifs = drain(&recv).await;
		let mut both = vec![first, second];
		both.sort();
		assert_eq!(delivered_to(&notifs), both, "{notifs:?}");
	}

	/// A broker that routes to every node, as a clustered broker does,
	/// delivering to the one channel.
	#[derive(Debug)]
	struct ClusterBroker(crate::channel::Sender<PublicNotification>);

	impl crate::dbs::MessageBroker for ClusterBroker {
		fn should_emit(&self, _: [u8; 16], _: [u8; 16]) -> anyhow::Result<bool> {
			Ok(true)
		}

		fn send(
			&self,
			item: crate::dbs::RoutedNotification,
		) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
			Box::pin(async move {
				let _ = self.0.send(item.into_notification()).await;
			})
		}
	}

	/// Each subscription is delivered by its owner's router alone, whatever
	/// the broker routes to. Every node runs a router over the same captured
	/// events, so a router that delivered to the subscriptions the other
	/// nodes own — as a broker that routes across nodes would let it — would
	/// reach each owner once per node.
	#[tokio::test]
	async fn each_subscription_is_delivered_by_its_owners_router_alone() {
		use surrealdb_kvs::TransactionType::{Read, Write};

		use crate::catalog::providers::CatalogProvider;

		let (send, recv) = crate::channel::bounded(1000);
		let config = ConfigMap::empty().with_key_value("live_query_engine", "router");
		let ds = Datastore::builder()
			.with_capabilities(Capabilities::all())
			.with_auth(false)
			.with_notify(send.clone())
			.with_live_query_broker(Arc::new(ClusterBroker(send)))
			.with_config(config)
			.without_maintenance_tasks()
			.build_with_path("memory")
			.await
			.unwrap();
		let (ns, db) = ("test", "test");
		let tx = ds.transaction(Write).await.unwrap();
		tx.ensure_ns_db(None, ns, db).await.unwrap();
		tx.commit().await.unwrap();
		let owner = Session::owner().with_ns(ns).with_db(db);
		ds.execute("DEFINE TABLE doc", &owner, None).await.unwrap();
		let live = Session::owner().with_ns(ns).with_db(db).with_rt(true);
		// Another node on the same storage, with the same broker, its router
		// begun at this node's baseline; each node registers a subscription.
		let other = ds.fork_for_test_with_node_id(uuid::Uuid::new_v4());
		let tx = ds.transaction(Read).await.unwrap();
		other
			.live_query_router()
			.set_baseline(tx.safe_timestamp().await.unwrap().as_versionstamp());
		tx.cancel().await.unwrap();
		let ours = subscribe(&ds, &live).await;
		let theirs = subscribe(&other, &live).await;
		while recv.try_recv().is_ok() {}
		ds.execute("CREATE doc:1", &owner, None).await.unwrap();

		// Each router delivers the event to its own node's subscription, and
		// not to the other's, though the broker would route it there.
		ds.live_query_router_process().await.unwrap();
		let notifs = drain(&recv).await;
		assert_eq!(delivered_to(&notifs), vec![ours], "{notifs:?}");
		other.live_query_router_process().await.unwrap();
		let notifs = drain(&recv).await;
		assert_eq!(delivered_to(&notifs), vec![theirs], "{notifs:?}");

		// Delivered, each once: another pass of either delivers nothing.
		ds.live_query_router_process().await.unwrap();
		other.live_query_router_process().await.unwrap();
		assert!(drain(&recv).await.is_empty());
	}

	/// A subscription registered between the passes of a lap receives none
	/// of the events the lap has still to deliver.
	///
	/// Each pass reads a table's subscriptions afresh, so a lap resumed after
	/// the registration finds the subscription, and would deliver it the rest
	/// of an entry captured before it existed.
	#[tokio::test]
	async fn router_lap_delivers_nothing_to_a_subscription_registered_between_its_passes() {
		use surrealdb_kvs::TransactionType::Write;

		use crate::catalog::providers::CatalogProvider;
		use crate::kvs::tasks::Progress;

		let (recv, ds) = new_ds("router").await;
		let (ns, db) = ("test", "test");
		let tx = ds.transaction(Write).await.unwrap();
		tx.ensure_ns_db(None, ns, db).await.unwrap();
		tx.commit().await.unwrap();
		let owner = Session::owner().with_ns(ns).with_db(db);
		ds.execute("DEFINE TABLE doc", &owner, None).await.unwrap();
		let live = Session::owner().with_ns(ns).with_db(db).with_rt(true);
		let first = subscribe(&ds, &live).await;
		while recv.try_recv().is_ok() {}
		// Five records in one commit: one entry of five events.
		ds.execute("CREATE doc:1, doc:2, doc:3, doc:4, doc:5", &owner, None).await.unwrap();
		// A budget of three: the namespace entered costs one, and the pass
		// delivers two of the entry's five events and stops inside it.
		let pass = ds.live_query_router_process_with_budget(3).await.unwrap();
		assert_eq!(pass.progress, Progress::More, "the pass stopped inside the entry");
		let notifs = drain(&recv).await;
		assert_eq!(delivered_to(&notifs), vec![first; 2], "{notifs:?}");

		let second = subscribe(&ds, &live).await;
		while recv.try_recv().is_ok() {}

		// The lap continues inside the entry: the rest of it, three events,
		// is evaluated for the first subscription and passed over for the
		// second, which is reached and charged all the same.
		let pass = ds.live_query_router_process_with_budget(7).await.unwrap();
		assert_eq!(pass.progress, Progress::Done, "the lap has delivered the entry");
		let notifs = drain(&recv).await;
		assert_eq!(delivered_to(&notifs), vec![first; 3], "{notifs:?}");

		// An event captured after both is delivered to both.
		ds.execute("CREATE doc:6", &owner, None).await.unwrap();
		ds.live_query_router_process().await.unwrap();
		let notifs = drain(&recv).await;
		let mut both = vec![first, second];
		both.sort();
		assert_eq!(delivered_to(&notifs), both, "{notifs:?}");
	}

	/// A subscription an event passes over costs the pass as one it evaluates
	/// for, and the pass keeps its place after it.
	///
	/// An event captured before many subscriptions were registered is passed
	/// over for each of them. Reached for free, they would have a pass with a
	/// small budget traverse them all, and a pass that stopped among them
	/// would reach them again.
	#[tokio::test]
	async fn router_pass_charges_the_subscriptions_it_passes_over() {
		use surrealdb_kvs::TransactionType::Write;

		use crate::catalog::providers::CatalogProvider;
		use crate::kvs::tasks::Progress;

		let (recv, ds) = new_ds("router").await;
		let (ns, db) = ("test", "test");
		let tx = ds.transaction(Write).await.unwrap();
		tx.ensure_ns_db(None, ns, db).await.unwrap();
		tx.commit().await.unwrap();
		let owner = Session::owner().with_ns(ns).with_db(db);
		ds.execute("DEFINE TABLE doc", &owner, None).await.unwrap();
		let live = Session::owner().with_ns(ns).with_db(db).with_rt(true);
		let first = subscribe(&ds, &live).await;
		ds.execute("CREATE doc:1", &owner, None).await.unwrap();
		// Three registered after the event, which is passed over for each.
		for _ in 0..3 {
			subscribe(&ds, &live).await;
		}
		while recv.try_recv().is_ok() {}

		// A budget of three: the namespace entered costs one, and the event
		// reaches two of the four subscriptions, evaluated or passed over.
		let pass = ds.live_query_router_process_with_budget(3).await.unwrap();
		assert_eq!(pass.progress, Progress::More, "the pass stopped among the subscriptions");
		let mut notifs = drain(&recv).await;
		// The next pass continues after the last subscription reached, and
		// reaches the other two with budget to spare.
		let pass = ds.live_query_router_process_with_budget(3).await.unwrap();
		assert_eq!(pass.progress, Progress::Done, "the pass reached the end of the subscriptions");
		notifs.extend(drain(&recv).await);
		assert_eq!(delivered_to(&notifs), vec![first], "{notifs:?}");
	}

	/// A pass reads a table's subscriptions a page at a time, and a pass that
	/// continues an event reads from the subscription it stopped at rather
	/// than from the table's first.
	#[tokio::test]
	async fn router_pass_reads_a_tables_subscriptions_a_page_at_a_time() {
		use surrealdb_kvs::TransactionType::Write;

		use super::SUBSCRIPTIONS_READ;
		use crate::catalog::providers::CatalogProvider;
		use crate::kvs::NORMAL_BATCH_SIZE;
		use crate::kvs::tasks::Progress;

		let (recv, ds) = new_ds("router").await;
		let (ns, db) = ("test", "test");
		let tx = ds.transaction(Write).await.unwrap();
		tx.ensure_ns_db(None, ns, db).await.unwrap();
		tx.commit().await.unwrap();
		let owner = Session::owner().with_ns(ns).with_db(db);
		ds.execute("DEFINE TABLE doc", &owner, None).await.unwrap();
		let live = Session::owner().with_ns(ns).with_db(db).with_rt(true);
		// A page of subscriptions and some more.
		let count = NORMAL_BATCH_SIZE as usize + 20;
		let mut ids = Vec::with_capacity(count);
		for _ in 0..count {
			ids.push(subscribe(&ds, &live).await);
		}
		while recv.try_recv().is_ok() {}
		ds.execute("CREATE doc:1", &owner, None).await.unwrap();

		// A budget short of a page: the pass reads the first page and no
		// more, and stops among its subscriptions.
		let budget = 300u64;
		SUBSCRIPTIONS_READ.with(|n| n.set(0));
		let pass = ds.live_query_router_process_with_budget(budget).await.unwrap();
		assert_eq!(pass.progress, Progress::More, "the pass stopped among the subscriptions");
		assert_eq!(
			SUBSCRIPTIONS_READ.with(|n| n.get()),
			NORMAL_BATCH_SIZE as usize,
			"one page read"
		);
		let mut notifs = drain(&recv).await;
		let delivered = notifs.len();
		assert!(0 < delivered && delivered < budget as usize, "{delivered}");

		// The next pass reads the subscriptions after the one it stopped at,
		// and no other.
		SUBSCRIPTIONS_READ.with(|n| n.set(0));
		let pass = ds.live_query_router_process_with_budget(budget).await.unwrap();
		assert_eq!(pass.progress, Progress::Done, "the pass reached the end of the subscriptions");
		assert_eq!(SUBSCRIPTIONS_READ.with(|n| n.get()), count - delivered, "the rest alone");
		notifs.extend(drain(&recv).await);
		ids.sort();
		assert_eq!(delivered_to(&notifs), ids);
	}

	/// A lap resumed after its database was removed and defined again under
	/// the same names delivers nothing from the removed one.
	///
	/// The replay resolves a database's subscriptions by name, while the
	/// entries a lap has still to deliver are keyed by id and stay retained
	/// under the removed database's prefix until its reclaim runs. A lap that
	/// matched the names alone would replay the removed database's changes to
	/// the new database's subscribers.
	#[tokio::test]
	async fn router_lap_skips_a_database_recreated_under_its_names() {
		use surrealdb_kvs::TransactionType::Write;

		use crate::catalog::providers::CatalogProvider;
		use crate::kvs::tasks::Progress;

		let (recv, ds) = new_ds("router").await;
		let (ns, db) = ("test", "test");
		let tx = ds.transaction(Write).await.unwrap();
		tx.ensure_ns_db(None, ns, db).await.unwrap();
		tx.commit().await.unwrap();
		let owner = Session::owner().with_ns(ns).with_db(db);
		let live = Session::owner().with_ns(ns).with_db(db).with_rt(true);
		ds.execute("DEFINE TABLE doc", &owner, None).await.unwrap();
		ds.execute("LIVE SELECT * FROM doc", &live, None).await.unwrap();
		while recv.try_recv().is_ok() {}
		ds.live_query_router_process().await.unwrap();
		ds.execute("CREATE doc:1", &owner, None).await.unwrap();
		ds.execute("CREATE doc:2", &owner, None).await.unwrap();

		// A budget of two: the namespace entered costs one, and the lap stops
		// with the second entry still to deliver.
		let first = ds.live_query_router_process_with_budget(2).await.unwrap();
		assert_eq!(first.progress, Progress::More);
		assert_eq!(drain(&recv).await.len(), 1);

		// The database is removed and defined again under the same names, with
		// a subscriber of its own. The removed database's reclaim is deferred,
		// so its second entry is still retained under the old ids.
		for res in ds
			.execute("REMOVE DATABASE test; DEFINE DATABASE test; DEFINE TABLE doc", &owner, None)
			.await
			.unwrap()
		{
			res.result.unwrap();
		}
		ds.execute("LIVE SELECT * FROM doc", &live, None).await.unwrap();
		drain(&recv).await;

		let second = ds.live_query_router_process_with_budget(2).await.unwrap();
		assert_eq!(second.progress, Progress::Done, "the lap drops the removed database");
		assert_eq!(second.items, 0, "nothing of the removed database is delivered");
		assert!(drain(&recv).await.is_empty(), "the new subscriber hears nothing of the old");

		// The new database's own changes are delivered as usual.
		ds.execute("CREATE doc:3", &owner, None).await.unwrap();
		let third = ds.live_query_router_process_with_budget(2).await.unwrap();
		assert_eq!(third.items, 1);
		assert_eq!(drain(&recv).await.len(), 1);
	}

	/// A lap charges every database it scans to its budget, empty or not, so
	/// a lap over a catalog of databases with nothing to deliver spans passes
	/// rather than scanning every database in one.
	///
	/// A lap listed every namespace and database when it began and charged
	/// only the entries its scans returned, so a pass over a large catalog of
	/// empty databases read and scanned all of it, in a slice a host driving
	/// maintenance itself waits on. The lap now reaches the databases as it
	/// needs them and an empty scan costs an entry.
	#[tokio::test]
	async fn router_lap_charges_an_empty_database_to_its_budget() {
		use surrealdb_kvs::TransactionType::Write;

		use crate::catalog::providers::CatalogProvider;
		use crate::kvs::tasks::Progress;

		let (recv, ds) = new_ds("router").await;
		let ns = "test";
		let tx = ds.transaction(Write).await.unwrap();
		for db in ["a", "b", "c", "d"] {
			tx.ensure_ns_db(None, ns, db).await.unwrap();
		}
		tx.commit().await.unwrap();
		let owner = Session::owner().with_ns(ns).with_db("a");
		let live = Session::owner().with_ns(ns).with_db("a").with_rt(true);
		ds.execute("DEFINE TABLE doc", &owner, None).await.unwrap();
		ds.execute("LIVE SELECT * FROM doc", &live, None).await.unwrap();
		while recv.try_recv().is_ok() {}
		ds.live_query_router_process().await.unwrap();
		ds.execute("CREATE doc:1", &owner, None).await.unwrap();

		// A budget of three: the namespace entered costs one, the first
		// database costs its one entry and the second, empty, costs one too,
		// so the lap stops with two still to scan.
		let first = ds.live_query_router_process_with_budget(3).await.unwrap();
		assert_eq!(first.progress, Progress::More, "two databases are still to scan");
		assert_eq!(first.items, 1);
		assert_eq!(drain(&recv).await.len(), 1);

		let second = ds.live_query_router_process_with_budget(3).await.unwrap();
		assert_eq!(second.progress, Progress::Done, "the lap has scanned every database");
		assert_eq!(second.items, 0);
		assert!(drain(&recv).await.is_empty());

		// Nothing is delivered twice.
		let third = ds.live_query_router_process_with_budget(3).await.unwrap();
		assert_eq!(third.items, 0);
		assert!(drain(&recv).await.is_empty());
	}

	/// A lap charges every namespace it enters to its budget, as it does
	/// every database it scans, so a lap over a catalog of namespaces with
	/// nothing to deliver spans passes rather than reading every namespace in
	/// one.
	///
	/// The walk stepped through every namespace without a database inside one
	/// call, reading page after page of the namespace list, before the lap
	/// could charge anything, so a pass over a large catalog of empty
	/// namespaces read all of it. Entering a namespace is now a step of the
	/// walk, and the lap charges every step.
	#[tokio::test]
	async fn router_lap_charges_an_entered_namespace_to_its_budget() {
		use surrealdb_kvs::TransactionType::Write;

		use crate::catalog::providers::CatalogProvider;
		use crate::kvs::tasks::Progress;

		let (recv, ds) = new_ds("router").await;
		// Three namespaces without a database ahead, in catalog order, of the
		// one with the subscription.
		for ns in ["a", "b", "c"] {
			ds.execute(&format!("DEFINE NAMESPACE {ns}"), &Session::owner(), None).await.unwrap();
		}
		let tx = ds.transaction(Write).await.unwrap();
		tx.ensure_ns_db(None, "test", "test").await.unwrap();
		tx.commit().await.unwrap();
		let owner = Session::owner().with_ns("test").with_db("test");
		let live = Session::owner().with_ns("test").with_db("test").with_rt(true);
		ds.execute("DEFINE TABLE doc", &owner, None).await.unwrap();
		ds.execute("LIVE SELECT * FROM doc", &live, None).await.unwrap();
		while recv.try_recv().is_ok() {}
		ds.live_query_router_process().await.unwrap();
		ds.execute("CREATE doc:1", &owner, None).await.unwrap();

		// A budget of two: the first pass enters two empty namespaces and
		// stops, the second enters the third and the one with the database,
		// and the third scans the database.
		let first = ds.live_query_router_process_with_budget(2).await.unwrap();
		assert_eq!(first.progress, Progress::More, "two namespaces are still to enter");
		assert_eq!(first.items, 0);
		assert!(drain(&recv).await.is_empty());
		let second = ds.live_query_router_process_with_budget(2).await.unwrap();
		assert_eq!(second.progress, Progress::More, "the database is still to scan");
		assert_eq!(second.items, 0);
		let third = ds.live_query_router_process_with_budget(2).await.unwrap();
		assert_eq!(third.progress, Progress::Done, "the lap has scanned every database");
		assert_eq!(third.items, 1);
		assert_eq!(drain(&recv).await.len(), 1);
	}

	/// A captured event whose table is removed before the router replays it is
	/// dropped, and neither blocks nor re-delivers any other database's events.
	/// The databases are named so the removed table's database is visited
	/// between the other two.
	#[tokio::test]
	async fn router_drops_events_for_a_removed_table() {
		use crate::types::PublicAction;

		fn creates(notifs: &[PublicNotification], table: &str) -> usize {
			notifs
				.iter()
				.filter(|n| {
					matches!(n.action, PublicAction::Create)
						&& matches!(&n.record, PublicValue::RecordId(r) if r.table.as_str() == table)
				})
				.count()
		}

		let (recv, ds) = new_ds("router").await;
		ds.execute("DEFINE NAMESPACE test", &Session::owner(), None).await.unwrap();
		for (db, tb) in [("aa", "ta"), ("bb", "tb"), ("cc", "tc")] {
			let sess = Session::owner().with_ns("test").with_db(db);
			ds.execute(&format!("DEFINE DATABASE {db}; DEFINE TABLE {tb}"), &sess, None)
				.await
				.unwrap();
			let live = Session::owner().with_ns("test").with_db(db).with_rt(true);
			ds.execute(&format!("LIVE SELECT * FROM {tb}"), &live, None).await.unwrap();
			ds.execute(&format!("CREATE {tb}:1"), &sess, None).await.unwrap();
		}
		let bb = Session::owner().with_ns("test").with_db("bb");
		ds.execute("REMOVE TABLE tb", &bb, None).await.unwrap();
		// The removal's `Killed` notification is sent directly, not by the router.
		while recv.try_recv().is_ok() {}

		for _ in 0..3 {
			ds.live_query_router_process().await.unwrap();
		}
		let notifs = drain(&recv).await;
		assert_eq!(creates(&notifs, "ta"), 1, "a database before the removed table: {notifs:?}");
		assert_eq!(creates(&notifs, "tb"), 0, "the removed table: {notifs:?}");
		assert_eq!(creates(&notifs, "tc"), 1, "a database after the removed table: {notifs:?}");

		// Delivery continues past the removal on later passes.
		let aa = Session::owner().with_ns("test").with_db("aa");
		ds.execute("CREATE ta:2", &aa, None).await.unwrap();
		ds.live_query_router_process().await.unwrap();
		ds.live_query_router_process().await.unwrap();
		let notifs = drain(&recv).await;
		assert_eq!(creates(&notifs, "ta"), 1, "a later write is delivered once: {notifs:?}");
	}

	/// A write executed in an open transaction before a subscription is
	/// registered in another session is not delivered to that subscription even
	/// when the write's transaction commits after the registration, matching the
	/// Inline engine.
	#[tokio::test]
	async fn router_delivers_nothing_captured_by_an_earlier_write_that_commits_after_subscription()
	{
		use surrealdb_kvs::TransactionType::Write;
		use surrealdb_strand::TableName;

		use crate::catalog::providers::{CatalogProvider, DatabaseProvider};
		use crate::kvs::tasks::Progress;
		use crate::val::{RecordId, Value};

		let (recv, ds) = new_ds("router").await;
		let (ns, db) = ("test", "test");
		let tx = ds.transaction(Write).await.unwrap();
		tx.ensure_ns_db(None, ns, db).await.unwrap();
		tx.commit().await.unwrap();
		let writer = Session::owner().with_ns(ns).with_db(db);
		ds.execute("DEFINE TABLE doc;", &writer, None).await.unwrap();

		// First subscription on `doc`, so writes on `doc` capture live events.
		let first = Session::owner().with_ns(ns).with_db(db).with_rt(true);
		let first_id = subscribe(&ds, &first).await;
		while recv.try_recv().is_ok() {}
		ds.live_query_router_process().await.unwrap();

		// Buffer a change in an open write transaction with a capture stamp
		// minted before the second subscription registers, then commit the write
		// transaction after the registration commits.
		let tx = ds.transaction(Write).await.unwrap();
		let db_def = tx.get_db_by_name(ns, db, None).await.unwrap().unwrap();
		let tb = TableName::new("doc");
		let id = RecordId::new(tb.clone(), 2i64);
		let captured = tx.timestamp().await.unwrap().as_versionstamp();
		tx.live_event_buffer_record_change(
			db_def.namespace_id,
			db_def.database_id,
			&tb,
			&id,
			Value::None,
			Value::from(std::collections::BTreeMap::from([("n".to_string(), Value::from(2i64))])),
			captured,
		);

		let second = Session::owner().with_ns(ns).with_db(db).with_rt(true);
		let second_id = subscribe(&ds, &second).await;
		while recv.try_recv().is_ok() {}

		tx.commit().await.unwrap();
		while ds.live_query_router_process().await.unwrap().progress == Progress::More {}
		let notifs = drain(&recv).await;
		let ids = delivered_to(&notifs);
		assert!(
			ids.contains(&first_id),
			"the subscription registered before the write receives it: {notifs:?}"
		);
		assert!(
			!ids.contains(&second_id),
			"the subscription registered after the write captured receives nothing of it: {notifs:?}"
		);
	}

	/// When a router pass stops inside an event and a schema change (such as a
	/// computed field definition) lands before the next pass continues that
	/// event, the remaining subscriptions of the event are evaluated against the
	/// same catalog snapshot as the subscriptions reached before the stop.
	#[tokio::test]
	async fn router_evaluates_a_split_event_against_one_catalog_snapshot() {
		use surrealdb_kvs::TransactionType::Write;

		use crate::catalog::providers::CatalogProvider;
		use crate::kvs::tasks::Progress;

		let (recv, ds) = new_ds("router").await;
		let (ns, db) = ("test", "test");
		let tx = ds.transaction(Write).await.unwrap();
		tx.ensure_ns_db(None, ns, db).await.unwrap();
		tx.commit().await.unwrap();
		let sess = Session::owner().with_ns(ns).with_db(db);
		ds.execute(
			"DEFINE TABLE doc SCHEMALESS; \
			 DEFINE FIELD tag ON doc COMPUTED 'before';",
			&sess,
			None,
		)
		.await
		.unwrap();
		for _ in 0..4 {
			let sub = Session::owner().with_ns(ns).with_db(db).with_rt(true);
			ds.execute("LIVE SELECT id, tag FROM doc", &sub, None).await.unwrap();
		}
		while recv.try_recv().is_ok() {}
		ds.live_query_router_process().await.unwrap();
		ds.execute("CREATE doc:1", &sess, None).await.unwrap();

		// Budget 3: 1 for the namespace, 2 for the first two subscriptions of
		// the event, stopping inside the event with two subscriptions left.
		let first = ds.live_query_router_process_with_budget(3).await.unwrap();
		assert_eq!(first.progress, Progress::More);
		assert_eq!(first.items, 0, "stopped inside the event");

		// Redefine the computed field between passes.
		ds.execute("DEFINE FIELD OVERWRITE tag ON doc COMPUTED 'after'", &sess, None)
			.await
			.unwrap();

		let second = ds.live_query_router_process_with_budget(10).await.unwrap();
		assert_eq!(second.progress, Progress::Done);
		assert_eq!(second.items, 1);

		let notifs = drain(&recv).await;
		assert_eq!(notifs.len(), 4, "all four subscriptions notified: {notifs:?}");
		for notif in &notifs {
			let PublicValue::Object(obj) = &notif.result else {
				panic!("expected object notification, got {notif:?}");
			};
			assert_eq!(
				obj.get("tag"),
				Some(&PublicValue::String("before".to_string())),
				"every subscription of the split event sees the same catalog snapshot: {notifs:?}"
			);
		}
	}
}
