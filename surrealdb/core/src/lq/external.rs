//! The host-facing half of the External live-query engine.
//!
//! Under [`LiveQueryEngine::External`](crate::kvs::LiveQueryEngine::External) a write
//! neither captures its record change for a router, notifies inline, nor appends to a
//! change feed. `LIVE` and `KILL` still register and remove their subscription and node
//! keys in the statement's own transaction. The host that owns the commit order observes
//! each committed record change itself and asks the datastore, record by record, what each
//! subscription is told:
//!
//! - [`classify_key`] and [`table_record_range`] let it tell record keys from every other key of a
//!   commit and bound a table's records.
//! - [`Datastore::node_subscriptions`] and [`Datastore::table_subscriptions`] list the
//!   subscriptions registered by a node or on a table, as read at a snapshot.
//! - [`needs_before_image`] says whether a subscription needs the record before an `UPDATE` to
//!   build its notification.
//! - [`Datastore::live_notifications`] builds the notifications of one [`RecordChange`] for the
//!   subscriptions it is given, through the pipeline the inline engine notifies through, with
//!   permissions, `COMPUTED` fields and `FETCH` read at the host's snapshot.
//!
//! [`ChangeFeedSource`] is the host's answer to `SHOW CHANGES`, whose keyspace this engine
//! never writes.

use std::borrow::Cow;
use std::future::Future;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::{Result, bail};
pub use surrealdb_catalog::{DatabaseId, NamespaceId, NodeLiveQuery, SubscriptionDefinition};
pub use surrealdb_datastore::values::changefeed::ChangeSet;
pub use surrealdb_datastore::values::live_query::LiveAction;
pub use surrealdb_expr::expr::statements::show::ShowSince;
pub use surrealdb_expr::val::{RecordId, RecordIdKey, TableName};
pub use surrealdb_types::Notification;
use uuid::Uuid;

use crate::catalog::providers::{DatabaseProvider, NamespaceProvider, TableProvider};
use crate::catalog::{Record, SubscriptionFields};
use crate::doc::Action;
use crate::exec::operators::write::stages::lives::{Images, Notify};
use crate::expr::Param;
use crate::expr::visit::{Visit, Visitor};
use crate::key::schema::{NodeLiveQueryKey, NodeLiveQueryPrefix, RecordKey, RecordPrefix};
use crate::key::{KVKeyDecode, KVValue, RawRange};
use crate::kvs::{Datastore, Direction, LayoutSet, NORMAL_BATCH_SIZE, Transaction};
use crate::lq::subscriber::{into_error, table_write_ctx};
use crate::val::Value;

/// A read-only transaction a host opens on its own snapshot: the state a notification's
/// permissions, `COMPUTED` fields and `FETCH` are read from.
///
/// The datastore neither commits nor cancels it; the host owns its lifetime.
pub type SnapshotReader = Arc<Transaction>;

/// What a storage key addresses, as far as live queries are concerned.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KeyClass {
	/// A record document.
	Record {
		/// The record's namespace.
		ns: NamespaceId,
		/// The record's database.
		db: DatabaseId,
		/// The record's table.
		tb: TableName,
		/// The record's id within its table.
		id: RecordIdKey,
	},
	/// Any other key: a catalog definition, an index entry, a graph edge, a change-feed or
	/// subscription row, and the rest of the keyspace.
	Other,
}

/// Classifies a storage key: a record document key, or anything else.
///
/// Total: bytes that spell no key of the layout are [`KeyClass::Other`].
pub fn classify_key(key: &[u8]) -> KeyClass {
	match RecordKey::decode_key(key) {
		Ok(record) => KeyClass::Record {
			ns: record.ns,
			db: record.db,
			tb: record.tb.into_owned(),
			id: record.id.into_owned(),
		},
		Err(_) => KeyClass::Other,
	}
}

/// The keys of every record of a table, as `[start, end)`.
pub fn table_record_range(
	ns: NamespaceId,
	db: DatabaseId,
	tb: &TableName,
) -> Result<Range<Vec<u8>>> {
	let range: RawRange = RecordPrefix {
		ns,
		db,
		tb: Cow::Borrowed(tb),
	}
	.range()?;
	let (start, end) = range.bounds();
	Ok(start.to_vec()..end.to_vec())
}

/// Whether notifying `subscription` of an `UPDATE` needs the record as it was before the
/// write: it is a `DIFF`, or its projection, `WHERE` or `FETCH` reads `$before`.
///
/// A `DELETE` always needs the before image, and a `CREATE` never has one. Only the
/// subscription's own clauses are inspected: a table or field permission or `COMPUTED`
/// expression that reads `$before` is the table's, so a host whose tables hold such an
/// expression supplies the before image of every update.
pub fn needs_before_image(subscription: &SubscriptionDefinition) -> bool {
	let Some(query) = subscription.query.compiled() else {
		return false;
	};
	let reads = match &query.fields {
		SubscriptionFields::Diff => return true,
		SubscriptionFields::Select(fields) => reads_before(fields),
	};
	reads
		|| query.cond.as_ref().is_some_and(reads_before)
		|| query.fetch.iter().flatten().any(reads_before)
}

/// Whether a clause names `$before` anywhere.
fn reads_before<T>(clause: &T) -> bool
where
	T: Visit<ReadsBefore>,
{
	clause.visit(&mut ReadsBefore).is_err()
}

/// Stops at the first `$before`.
struct ReadsBefore;

impl Visitor for ReadsBefore {
	type Error = ();

	fn visit_param(&mut self, param: &Param) -> Result<(), ()> {
		if param.as_str() == "before" {
			Err(())
		} else {
			Ok(())
		}
	}
}

/// One committed change to one record, as the host observed it.
///
/// The images are the record's stored bytes (the value of its record key), so a host hands
/// over what it read from or wrote to storage without decoding it.
#[derive(Clone, Debug)]
pub struct RecordChange {
	/// The record's namespace.
	pub ns: NamespaceId,
	/// The record's database.
	pub db: DatabaseId,
	/// The record's table.
	pub tb: TableName,
	/// The record's id within its table.
	pub id: RecordIdKey,
	/// What the write did.
	pub action: LiveAction,
	/// The record before the write. Required for a delete, and for an update when a
	/// subscription [needs it](needs_before_image); absent for a create.
	pub before: Option<Vec<u8>>,
	/// The record after the write. Required for a create and an update; absent for a delete.
	pub after: Option<Vec<u8>>,
}

impl RecordChange {
	/// The change a write to `key` made, or `None` when `key` is not a record key.
	pub fn from_key(
		key: &[u8],
		action: LiveAction,
		before: Option<Vec<u8>>,
		after: Option<Vec<u8>>,
	) -> Option<Self> {
		match classify_key(key) {
			KeyClass::Record {
				ns,
				db,
				tb,
				id,
			} => Some(Self {
				ns,
				db,
				tb,
				id,
				action,
				before,
				after,
			}),
			KeyClass::Other => None,
		}
	}

	fn record_id(&self) -> RecordId {
		RecordId {
			table: self.tb.clone(),
			key: self.id.clone(),
		}
	}

	/// The stored record in `image`, or `None` for an absent image.
	async fn decode(&self, at: &Transaction, image: Option<&[u8]>) -> Result<Option<Record>> {
		let Some(bytes) = image else {
			return Ok(None);
		};
		// A packed row is read through the layouts of its table, admitted at `at` the way a scan
		// admits it.
		let mut layouts = LayoutSet::new(self.ns, self.db, None);
		let (_, admitted) = layouts.admit_with_layouts(at, &self.tb, bytes).await?;
		Record::decode_admitted(bytes, admitted.map(|l| l.as_ref()), self.record_id()).map(Some)
	}
}

/// A subscription and the table it is registered on.
#[derive(Clone, Debug)]
pub struct TableSubscription {
	/// The table's namespace.
	pub ns: NamespaceId,
	/// The table's database.
	pub db: DatabaseId,
	/// The table.
	pub tb: TableName,
	/// The subscription as stored.
	pub subscription: SubscriptionDefinition,
}

impl Datastore {
	/// The subscriptions registered by `node`, as read in `at`.
	///
	/// A host rebuilds the subscriptions of its node from these after it loses its memory. A
	/// node key whose subscription row is gone is skipped.
	pub async fn node_subscriptions(
		&self,
		node: Uuid,
		at: &SnapshotReader,
	) -> Result<Vec<TableSubscription>> {
		let mut out = Vec::new();
		// Each table's subscriptions, read once and indexed by id.
		let mut tables: std::collections::HashMap<_, std::collections::HashMap<_, _>> =
			std::collections::HashMap::new();
		let mut next = Some(
			NodeLiveQueryPrefix {
				nd: node,
			}
			.range()?,
		);
		while let Some(range) = next {
			let page = at.batch_keys_vals(range.clone(), NORMAL_BATCH_SIZE, None).await?;
			next = match (&page.next, page.result.last()) {
				(Some(_), Some((k, _))) => {
					Some(crate::key::Resumable::resume_after(range, k, Direction::Forward))
				}
				_ => None,
			};
			for (k, v) in page.result.iter() {
				let live: NodeLiveQuery = KVValue::kv_decode_value(v, ())?;
				let key = NodeLiveQueryKey::decode_key(k)?;
				let table = (live.ns, live.db, live.tb.clone());
				if !tables.contains_key(&table) {
					let found = self.table_subscriptions(live.ns, live.db, &live.tb, at).await?;
					tables.insert(table.clone(), found.into_iter().map(|s| (s.id, s)).collect());
				}
				if let Some(subscription) = tables.get(&table).and_then(|t| t.get(&key.lq)).cloned()
				{
					out.push(TableSubscription {
						ns: live.ns,
						db: live.db,
						tb: live.tb,
						subscription,
					});
				}
			}
		}
		Ok(out)
	}

	/// The subscriptions registered on a table, as read in `at`.
	pub async fn table_subscriptions(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		at: &SnapshotReader,
	) -> Result<Vec<SubscriptionDefinition>> {
		Ok(at.all_tb_lives(ns, db, tb, None).await?.to_vec())
	}

	/// The notifications `subscriptions` receive about `change`, in the order the
	/// subscriptions are given.
	///
	/// Each notification is built exactly as the inline engine builds it: the table's
	/// `PERMISSIONS FOR select` and field permissions are evaluated as the subscription's
	/// stored principal, `COMPUTED` fields are derived, the `WHERE` decides whether the
	/// subscriber hears of the record, and the projection, `DIFF` and `FETCH` are applied. All
	/// reads, including `FETCH`, go through `at`, so the host chooses the snapshot (a replay
	/// at the position of the change reads the same state every time).
	///
	/// `subscriptions` must be those registered on the change's table. A subscription that
	/// is denied, filtered out, expired, or whose evaluation fails yields no notification and
	/// does not affect the others. An update whose stored images are equal yields none for any
	/// subscription, as an inline write that changed nothing notifies nobody.
	///
	/// Fails, with nothing built, when the change lacks an image its action needs, when its
	/// namespace, database or table does not exist in `at`, or when a stored image cannot be
	/// decoded.
	pub async fn live_notifications(
		&self,
		subscriptions: &[SubscriptionDefinition],
		change: &RecordChange,
		at: &SnapshotReader,
	) -> Result<Vec<Notification>> {
		let before = change.decode(at, change.before.as_deref()).await?;
		let after = change.decode(at, change.after.as_deref()).await?;
		let (action, subject_missing) = match change.action {
			LiveAction::Create => (Action::Create, after.is_none()),
			LiveAction::Update => (Action::Update, after.is_none()),
			LiveAction::Delete => (Action::Delete, before.is_none()),
		};
		if subject_missing {
			bail!("a {:?} change of {:?} carries no image of the record", change.action, change.id);
		}
		if change.action == LiveAction::Update
			&& before.is_none()
			&& subscriptions.iter().any(needs_before_image)
		{
			bail!("an update of {:?} needs its before image for a subscription", change.id);
		}
		if change.action == LiveAction::Update && before == after {
			return Ok(Vec::new());
		}
		if subscriptions.is_empty() {
			return Ok(Vec::new());
		}
		let (ns_name, db_name) = names(at, change.ns, change.db).await?;
		let write_ctx =
			table_write_ctx(self, Arc::clone(at), &ns_name, &db_name, &change.tb, None).await?;
		let rid = change.record_id();
		let images = Images {
			initial: Arc::new(before.map_or(Value::None, |r| r.data)),
			current: Arc::new(after.map_or(Value::None, |r| r.data)),
			action,
		};
		let slots: std::collections::HashMap<_, usize> =
			write_ctx.lives.iter().enumerate().map(|(slot, s)| (s.id, slot)).collect();
		let mut out = Vec::new();
		for subscription in subscriptions {
			// The context holds the table's subscriptions; the host's subscription is
			// served at its position there, and one the table does not hold is skipped.
			let Some(&slot) = slots.get(&subscription.id) else {
				continue;
			};
			if let Notify::Deliver(notification) =
				crate::exec::vm::notify_at(&write_ctx, slot, &rid, &images)
					.await
					.map_err(into_error)?
			{
				out.push(notification);
			}
		}
		Ok(out)
	}
}

/// The names of a namespace and a database, as `at` holds them.
async fn names(at: &Transaction, ns: NamespaceId, db: DatabaseId) -> Result<(String, String)> {
	let namespaces = at.all_ns(None).await?;
	let Some(namespace) = namespaces.iter().find(|n| n.namespace_id == ns) else {
		bail!("the namespace {} does not exist at this snapshot", ns.0);
	};
	let databases = at.all_db(ns, None).await?;
	let Some(database) = databases.iter().find(|d| d.database_id == db) else {
		bail!("the database {} does not exist at this snapshot", db.0);
	};
	Ok((namespace.name.as_str().to_owned(), database.name.as_str().to_owned()))
}

/// A host-provided answer to `SHOW CHANGES`, read in place of the change-feed keyspace.
///
/// Installed with [`Builder::with_changefeed_source`](crate::kvs::Builder::with_changefeed_source).
/// The datastore calls it for each `SHOW CHANGES` statement of an authorised principal, with
/// the statement's own transaction, and returns what it answers unchanged.
pub trait ChangeFeedSource: Send + Sync {
	/// The change sets of database `db`, or of table `tb` in it, from `since`, at most `limit`
	/// of them, in ascending versionstamp order, as the engine's own reader returns them.
	fn show_changes<'a>(
		&'a self,
		at: &'a Transaction,
		ns: NamespaceId,
		db: DatabaseId,
		tb: Option<&'a TableName>,
		since: ShowSince,
		limit: Option<u32>,
	) -> Pin<Box<dyn Future<Output = Result<Vec<ChangeSet>>> + Send + 'a>>;
}

#[cfg(all(test, feature = "kv-mem"))]
#[allow(clippy::unwrap_used, clippy::wildcard_enum_match_arm)]
mod tests {
	use std::collections::BTreeMap;
	use std::time::Duration;

	use surrealdb_cnf::ConfigMap;
	use surrealdb_kvs::TransactionType::Read;

	use super::*;
	use crate::channel::Receiver;
	use crate::dbs::{Capabilities, Session};
	use crate::key::schema::{ChangeFeedPrefix, LiveEventsPrefix};
	use crate::types::{PublicNotification, PublicValue};

	/// A host source that holds no change sets.
	struct NoChanges;

	impl ChangeFeedSource for NoChanges {
		fn show_changes<'a>(
			&'a self,
			_at: &'a Transaction,
			_ns: NamespaceId,
			_db: DatabaseId,
			_tb: Option<&'a TableName>,
			_since: ShowSince,
			_limit: Option<u32>,
		) -> Pin<Box<dyn Future<Output = Result<Vec<ChangeSet>>> + Send + 'a>> {
			Box::pin(async { Ok(Vec::new()) })
		}
	}

	async fn new_ds(engine: &str) -> (Receiver<PublicNotification>, Arc<Datastore>) {
		let (send, recv) = crate::channel::bounded(1000);
		let config = ConfigMap::empty().with_key_value("live_query_engine", engine);
		let ds = Datastore::builder()
			.with_capabilities(Capabilities::all())
			.with_auth(false)
			.with_notify(send)
			.with_config(config)
			.with_changefeed_source(Arc::new(NoChanges))
			.build_with_path("memory")
			.await
			.unwrap();
		ensure_db(&ds).await;
		(recv, ds)
	}

	/// Creates the `test` namespace and database.
	async fn ensure_db(ds: &Datastore) {
		use surrealdb_kvs::TransactionType::Write;

		use crate::catalog::providers::CatalogProvider;

		let txn = ds.transaction(Write).await.unwrap();
		txn.ensure_ns_db(None, "test", "test").await.unwrap();
		txn.commit().await.unwrap();
	}

	fn record_session(user_key: &str) -> Session {
		use crate::types::{PublicRecordId, PublicRecordIdKey};
		Session::for_record(
			"test",
			"test",
			"user",
			PublicValue::RecordId(PublicRecordId {
				table: "user".to_string().into(),
				key: PublicRecordIdKey::String(user_key.to_string()),
			}),
		)
		.with_rt(true)
	}

	fn owner() -> Session {
		Session::owner().with_ns("test").with_db("test")
	}

	fn owner_live() -> Session {
		owner().with_rt(true)
	}

	/// Runs `sql` and returns the id of the live query its last statement registered.
	async fn live(ds: &Datastore, sql: &str, session: &Session) -> crate::types::PublicUuid {
		let mut res = ds.execute(sql, session, None).await.unwrap();
		match res.remove(0).result.unwrap() {
			PublicValue::Uuid(id) => id,
			other => panic!("expected a live query id, got {other:?}"),
		}
	}

	async fn drain(recv: &Receiver<PublicNotification>) -> Vec<PublicNotification> {
		let mut out = Vec::new();
		while let Ok(Ok(n)) = tokio::time::timeout(Duration::from_millis(200), recv.recv()).await {
			out.push(n);
		}
		out
	}

	/// What a notification says, without the live query id and session, which differ between
	/// two datastores.
	fn content(notifs: &[PublicNotification]) -> Vec<String> {
		let mut keys: Vec<String> = notifs
			.iter()
			.map(|n| format!("{:?}|{:?}|{:?}", n.action, n.record, n.result))
			.collect();
		keys.sort();
		keys
	}

	const SCHEMA: &str = "DEFINE TABLE doc PERMISSIONS FOR select FULL; \
		 DEFINE ACCESS user ON DATABASE TYPE RECORD; \
		 DEFINE FIELD owner ON doc TYPE record<user>; \
		 DEFINE FIELD name ON doc TYPE string; \
		 DEFINE FIELD secret ON doc TYPE string PERMISSIONS FOR select WHERE false; \
		 DEFINE FIELD derived ON doc TYPE string \
		     COMPUTED string::concat('derived_', name) \
		     PERMISSIONS FOR select NONE; \
		 DEFINE FIELD mine ON doc TYPE string \
		     COMPUTED string::concat('hi_', name) \
		     PERMISSIONS FOR select WHERE owner = $auth";

	/// The writes both engines are driven through, one statement at a time.
	const WRITES: &[&str] = &[
		"CREATE doc:1 SET owner = user:alice, name = 'one', secret = 's1'",
		"CREATE doc:2 SET owner = user:bob, name = 'two', secret = 's2'",
		"UPDATE doc:1 SET name = 'one-updated'",
		"UPDATE doc:2 SET owner = user:alice",
		"UPDATE doc:1 SET name = 'one-updated'",
		"DELETE doc:2",
		"DELETE doc:1",
	];

	/// Registers a mix of subscribers and projections on `doc`: an owner, a record user, an
	/// owner `DIFF`, and a `WHERE` filter that only matches alice's records.
	async fn subscribe(ds: &Datastore) -> Vec<crate::types::PublicUuid> {
		ds.execute(SCHEMA, &owner(), None).await.unwrap();
		vec![
			live(ds, "LIVE SELECT * FROM doc", &owner_live()).await,
			live(ds, "LIVE SELECT * FROM doc", &record_session("alice")).await,
			live(ds, "LIVE SELECT DIFF FROM doc", &owner_live()).await,
			live(ds, "LIVE SELECT * FROM doc WHERE owner = user:alice", &owner_live()).await,
		]
	}

	/// The `(ns, db)` ids of `test`/`test`.
	async fn ids(ds: &Datastore) -> (NamespaceId, DatabaseId) {
		let txn = ds.transaction(Read).await.unwrap();
		let ns = txn.get_ns_by_name("test", None).await.unwrap().unwrap().namespace_id;
		let db = txn.get_db_by_name("test", "test", None).await.unwrap().unwrap().database_id;
		txn.cancel().await.unwrap();
		(ns, db)
	}

	/// The stored bytes of every record of `tb`, read through [`table_record_range`].
	async fn images(ds: &Datastore, tb: &str) -> BTreeMap<Vec<u8>, Vec<u8>> {
		let (ns, db) = ids(ds).await;
		let range = table_record_range(ns, db, &TableName::new(tb)).unwrap();
		let txn = ds.transaction(Read).await.unwrap();
		let rows = txn
			.scan_raw(RawRange::of_bytes(&range.start, &range.end), u32::MAX, 0, None)
			.await
			.unwrap();
		txn.cancel().await.unwrap();
		rows.into_iter().collect()
	}

	/// The changes that turn `before` into `after`, as a host observing the write would
	/// build them from the keys it saw.
	fn changes(
		before: &BTreeMap<Vec<u8>, Vec<u8>>,
		after: &BTreeMap<Vec<u8>, Vec<u8>>,
	) -> Vec<RecordChange> {
		let mut keys: Vec<&Vec<u8>> = before.keys().chain(after.keys()).collect();
		keys.sort();
		keys.dedup();
		keys.into_iter()
			.filter_map(|key| {
				let (b, a) = (before.get(key), after.get(key));
				let action = match (b, a) {
					(None, Some(_)) => LiveAction::Create,
					(Some(b), Some(a)) if b != a => LiveAction::Update,
					(Some(_), None) => LiveAction::Delete,
					_ => return None,
				};
				Some(
					RecordChange::from_key(key, action, b.cloned(), a.cloned())
						.expect("every key of a table's record range is a record key"),
				)
			})
			.collect()
	}

	/// A read snapshot of the datastore as it stands.
	async fn snapshot(ds: &Datastore) -> SnapshotReader {
		Arc::new(ds.transaction(Read).await.unwrap())
	}

	/// The subscriptions on `doc`, as listed from storage.
	async fn doc_subscriptions(ds: &Datastore) -> Vec<SubscriptionDefinition> {
		let (ns, db) = ids(ds).await;
		let at = snapshot(ds).await;
		let subs = ds.table_subscriptions(ns, db, &TableName::new("doc"), &at).await.unwrap();
		at.cancel().await.unwrap();
		subs
	}

	/// The notifications the host builds for the write that turned `before` into `after`.
	async fn fed(
		ds: &Datastore,
		before: &BTreeMap<Vec<u8>, Vec<u8>>,
		after: &BTreeMap<Vec<u8>, Vec<u8>>,
	) -> Vec<PublicNotification> {
		let subs = doc_subscriptions(ds).await;
		let at = snapshot(ds).await;
		let mut out = Vec::new();
		for change in changes(before, after) {
			out.extend(ds.live_notifications(&subs, &change, &at).await.unwrap());
		}
		at.cancel().await.unwrap();
		out
	}

	/// External-mode subscriptions fed the committed changes of each write produce the
	/// notifications the inline engine produces for the same write, and the External datastore
	/// itself delivers none.
	#[tokio::test]
	async fn external_feed_matches_inline_delivery() {
		let (inline_recv, inline) = new_ds("inline").await;
		let (external_recv, external) = new_ds("external").await;
		subscribe(&inline).await;
		let live_ids = subscribe(&external).await;
		while inline_recv.try_recv().is_ok() {}
		while external_recv.try_recv().is_ok() {}

		let mut before = images(&external, "doc").await;
		let mut total = 0;
		for sql in WRITES {
			inline.execute(sql, &owner(), None).await.unwrap();
			let expected = drain(&inline_recv).await;

			external.execute(sql, &owner(), None).await.unwrap();
			let after = images(&external, "doc").await;
			let got = fed(&external, &before, &after).await;
			before = after;

			assert_eq!(content(&got), content(&expected), "notifications of `{sql}`");
			for notification in &got {
				assert!(live_ids.contains(&notification.id));
			}
			total += got.len();
		}
		assert!(total > 0, "the writes must notify somebody");
		// The no-op second `UPDATE doc:1` changes nothing and notifies nobody; the engine
		// itself delivered nothing.
		assert!(drain(&external_recv).await.is_empty());
	}

	/// A DIFF subscription's update needs the before image; the others do not.
	#[tokio::test]
	async fn before_image_is_required_only_where_it_is_read() {
		let (_recv, ds) = new_ds("external").await;
		ds.execute("DEFINE TABLE doc PERMISSIONS FULL", &owner(), None).await.unwrap();
		let plain = live(&ds, "LIVE SELECT * FROM doc", &owner_live()).await;
		let diff = live(&ds, "LIVE SELECT DIFF FROM doc", &owner_live()).await;
		let reads = live(&ds, "LIVE SELECT $before.n AS was FROM doc", &owner_live()).await;
		let cond = live(&ds, "LIVE SELECT * FROM doc WHERE $before.n != n", &owner_live()).await;
		let fetched =
			live(&ds, "LIVE SELECT * FROM doc WHERE n > 1 FETCH other", &owner_live()).await;
		let subs = doc_subscriptions(&ds).await;
		let needs = |id: &crate::types::PublicUuid| {
			needs_before_image(subs.iter().find(|s| s.id == Uuid::from(*id)).unwrap())
		};
		assert!(!needs(&plain));
		assert!(needs(&diff));
		assert!(needs(&reads));
		assert!(needs(&cond));
		assert!(!needs(&fetched));

		ds.execute("CREATE doc:1 SET n = 1", &owner(), None).await.unwrap();
		let first = images(&ds, "doc").await;
		ds.execute("UPDATE doc:1 SET n = 2", &owner(), None).await.unwrap();
		let second = images(&ds, "doc").await;
		let key = first.keys().next().unwrap().clone();
		let at = snapshot(&ds).await;
		let without =
			RecordChange::from_key(&key, LiveAction::Update, None, Some(second[&key].clone()))
				.unwrap();
		// Subscriptions that read the before image make the change unservable without it.
		assert!(ds.live_notifications(&subs, &without, &at).await.is_err());
		// The others are served without it, as with it.
		let only_plain: Vec<_> = subs
			.iter()
			.filter(|s| s.id == Uuid::from(plain) || s.id == Uuid::from(fetched))
			.cloned()
			.collect();
		let got = ds.live_notifications(&only_plain, &without, &at).await.unwrap();
		let with = RecordChange {
			before: Some(first[&key].clone()),
			..without.clone()
		};
		let expected = ds.live_notifications(&only_plain, &with, &at).await.unwrap();
		assert_eq!(content(&got), content(&expected));
		assert_eq!(got.len(), 2);
		// A delete needs its image whatever the subscriptions read.
		let delete = RecordChange::from_key(&key, LiveAction::Delete, None, None).unwrap();
		assert!(ds.live_notifications(&only_plain, &delete, &at).await.is_err());
		at.cancel().await.unwrap();
	}

	/// Table and field permissions are evaluated as the subscription's own principal, against
	/// the snapshot the host supplies.
	#[tokio::test]
	async fn permissions_are_checked_at_the_supplied_snapshot() {
		let (_recv, ds) = new_ds("external").await;
		ds.execute(
			"DEFINE ACCESS user ON DATABASE TYPE RECORD; \
			 DEFINE TABLE gate PERMISSIONS FULL; \
			 CREATE gate:1 SET open = true; \
			 DEFINE TABLE doc PERMISSIONS FOR select WHERE (SELECT VALUE open FROM ONLY gate:1) = true; \
			 DEFINE FIELD secret ON doc PERMISSIONS FOR select WHERE $auth = user:admin;",
			&owner(),
			None,
		)
		.await
		.unwrap();
		let root = live(&ds, "LIVE SELECT * FROM doc", &owner_live()).await;
		let alice = live(&ds, "LIVE SELECT * FROM doc", &record_session("alice")).await;
		// A subscriber denied a field cannot use it as a filter either.
		let oracle =
			live(&ds, "LIVE SELECT * FROM doc WHERE secret = 's'", &record_session("alice")).await;
		let admin = live(&ds, "LIVE SELECT * FROM doc", &record_session("admin")).await;

		let before = images(&ds, "doc").await;
		ds.execute("CREATE doc:1 SET secret = 's', n = 1", &owner(), None).await.unwrap();
		let after = images(&ds, "doc").await;
		let open = snapshot(&ds).await;
		ds.execute("UPDATE gate:1 SET open = false", &owner(), None).await.unwrap();
		let closed = snapshot(&ds).await;

		let subs = doc_subscriptions(&ds).await;
		let change = changes(&before, &after).remove(0);

		let notified = |notifs: &[PublicNotification]| {
			let mut ids: Vec<_> = notifs.iter().map(|n| n.id).collect();
			ids.sort();
			ids
		};
		let ids = |wanted: &[&crate::types::PublicUuid]| {
			let mut ids: Vec<_> = wanted.iter().map(|id| **id).collect();
			ids.sort();
			ids
		};

		let got = ds.live_notifications(&subs, &change, &open).await.unwrap();
		// The oracle subscriber never matches, because it cannot see the field it filters on.
		assert_eq!(notified(&got), ids(&[&root, &alice, &admin]));
		assert!(!notified(&got).contains(&oracle));
		let result_of = |id: &crate::types::PublicUuid| {
			got.iter().find(|n| n.id == *id).unwrap().result.clone()
		};
		let has_secret = |value: &PublicValue| match value {
			PublicValue::Object(o) => o.get("secret").is_some(),
			other => panic!("expected an object, got {other:?}"),
		};
		assert!(has_secret(&result_of(&admin)));
		assert!(!has_secret(&result_of(&alice)));

		// The same change read at the snapshot where the gate has closed is hidden from
		// every principal the table's permission applies to; a root session bypasses it.
		let got = ds.live_notifications(&subs, &change, &closed).await.unwrap();
		assert_eq!(notified(&got), ids(&[&root]));
		open.cancel().await.unwrap();
		closed.cancel().await.unwrap();
	}

	/// Under the External engine a write captures no live event, appends to no change feed and
	/// notifies no one inline, while `LIVE` and `KILL` still commit their keys. The other
	/// engines keep the behaviour they had.
	#[tokio::test]
	async fn engines_differ_only_in_what_a_write_does_beyond_the_record() {
		struct Counts {
			subscriptions: usize,
			feed: usize,
			events: usize,
			inline: usize,
		}
		async fn run(engine: &str) -> (Counts, Counts) {
			let (recv, ds) = new_ds(engine).await;
			ds.execute("DEFINE TABLE doc CHANGEFEED 1h", &owner(), None).await.unwrap();
			let id = live(&ds, "LIVE SELECT * FROM doc", &owner_live()).await;
			while recv.try_recv().is_ok() {}
			let (ns, db) = ids(&ds).await;
			let ds = &ds;
			let counts = |inline: usize| async move {
				let txn = ds.transaction(Read).await.unwrap();
				let feed = txn
					.scan_raw(ChangeFeedPrefix::new(ns, db).range().unwrap(), u32::MAX, 0, None)
					.await
					.unwrap()
					.len();
				let events = txn
					.scan_raw(
						LiveEventsPrefix {
							ns,
							db,
						}
						.range()
						.unwrap(),
						u32::MAX,
						0,
						None,
					)
					.await
					.unwrap()
					.len();
				let at: SnapshotReader = Arc::new(txn);
				let subscriptions = ds.node_subscriptions(ds.id(), &at).await.unwrap().len();
				at.cancel().await.unwrap();
				Counts {
					subscriptions,
					feed,
					events,
					inline,
				}
			};
			let baseline = counts(0).await;
			ds.execute("CREATE doc:1 SET n = 1", &owner(), None).await.unwrap();
			let inline = drain(&recv).await.len();
			let mut written = counts(inline).await;
			written.feed -= baseline.feed;
			ds.execute(&format!("KILL u'{}'", id), &owner_live(), None).await.unwrap();
			let killed = counts(0).await;
			(written, killed)
		}

		let (inline, killed) = run("inline").await;
		assert_eq!((inline.subscriptions, inline.feed, inline.events, inline.inline), (1, 1, 0, 1));
		assert_eq!(killed.subscriptions, 0);

		let (router, killed) = run("router").await;
		// The router's own task delivers what the write captured, so only the keys are compared.
		assert_eq!((router.subscriptions, router.feed, router.events), (1, 1, 1));
		assert_eq!(killed.subscriptions, 0);

		let (external, killed) = run("external").await;
		assert_eq!(
			(external.subscriptions, external.feed, external.events, external.inline),
			(1, 0, 0, 0)
		);
		assert_eq!(killed.subscriptions, 0);
	}

	/// A host's `SHOW CHANGES` source answers in place of the keyspace.
	#[tokio::test]
	async fn show_changes_is_answered_by_the_host_source() {
		#[derive(Debug)]
		struct Fixed;
		impl ChangeFeedSource for Fixed {
			fn show_changes<'a>(
				&'a self,
				_at: &'a Transaction,
				_ns: NamespaceId,
				_db: DatabaseId,
				tb: Option<&'a TableName>,
				since: ShowSince,
				limit: Option<u32>,
			) -> Pin<Box<dyn Future<Output = Result<Vec<ChangeSet>>> + Send + 'a>> {
				Box::pin(async move {
					assert_eq!(tb.map(|t| t.as_str()), Some("doc"));
					assert_eq!(since, ShowSince::Versionstamp(0));
					assert_eq!(limit, Some(5));
					let id = RecordId {
						table: TableName::new("doc"),
						key: RecordIdKey::Number(7),
					};
					let value = Value::from("x");
					let mutations = surrealdb_datastore::values::changefeed::TableMutations(
						TableName::new("doc"),
						vec![surrealdb_datastore::values::changefeed::TableMutation::Set(
							id, value,
						)],
					);
					Ok(vec![ChangeSet(
						42,
						surrealdb_datastore::values::changefeed::DatabaseMutation(vec![mutations]),
					)])
				})
			}
		}
		let ds = Datastore::builder()
			.with_capabilities(Capabilities::all())
			.with_auth(false)
			.with_config(ConfigMap::empty().with_key_value("live_query_engine", "external"))
			.with_changefeed_source(Arc::new(Fixed))
			.build_with_path("memory")
			.await
			.unwrap();
		ensure_db(&ds).await;
		ds.execute("DEFINE TABLE doc CHANGEFEED 1h", &owner(), None).await.unwrap();
		ds.execute("CREATE doc:1 SET n = 1", &owner(), None).await.unwrap();
		let mut res =
			ds.execute("SHOW CHANGES FOR TABLE doc SINCE 0 LIMIT 5", &owner(), None).await.unwrap();
		let value = res.remove(0).result.unwrap();
		let PublicValue::Array(sets) = value else {
			panic!("expected an array of change sets");
		};
		assert_eq!(sets.len(), 1);
		assert!(format!("{:?}", sets[0]).contains("42"));
	}

	/// The host-driven engine writes no change feed, so a datastore without the host's source is
	/// refused rather than answering `SHOW CHANGES` from a keyspace nothing writes.
	#[tokio::test]
	async fn the_external_engine_needs_a_change_feed_source() {
		let built = Datastore::builder()
			.with_config(ConfigMap::empty().with_key_value("live_query_engine", "external"))
			.build_with_path("memory")
			.await;
		assert!(built.is_err());
	}

	/// Every key of a populated datastore is classified, and exactly the record keys are
	/// records, each inside its table's range.
	#[tokio::test]
	async fn classify_key_names_exactly_the_record_keys() {
		let (_recv, ds) = new_ds("external").await;
		ds.execute(
			"DEFINE TABLE person SCHEMAFULL; \
			 DEFINE FIELD name ON person TYPE string; \
			 DEFINE INDEX name_ix ON person FIELDS name; \
			 DEFINE TABLE knows TYPE RELATION CHANGEFEED 1h; \
			 CREATE person:a SET name = 'a'; \
			 CREATE person:1 SET name = 'b'; \
			 CREATE person:u'8f5f1d7a-8c1b-4d6c-9d38-3a5f6f4f2c11' SET name = 'c'; \
			 CREATE person:[1, 'x'] SET name = 'd'; \
			 CREATE person:{ a: 1 } SET name = 'e'; \
			 RELATE person:a->knows->person:1; \
			 LIVE SELECT * FROM person",
			&owner_live(),
			None,
		)
		.await
		.unwrap();
		let (ns, db) = ids(&ds).await;
		let txn = ds.transaction(Read).await.unwrap();
		let everything = txn.scan_raw(RawRange::every_key(), u32::MAX, 0, None).await.unwrap();
		txn.cancel().await.unwrap();
		let tables = [TableName::new("person"), TableName::new("knows")];
		let mut records = 0;
		for (key, _) in &everything {
			let class = classify_key(key);
			let in_range = tables.iter().any(|tb| {
				let range = table_record_range(ns, db, tb).unwrap();
				range.start <= *key && *key < range.end
			});
			match class {
				KeyClass::Record {
					ns: n,
					db: d,
					tb,
					id,
				} => {
					records += 1;
					assert!(in_range, "{tb:?}:{id:?} lies outside its table's range");
					assert_eq!((n, d), (ns, db));
				}
				KeyClass::Other => assert!(!in_range, "a key of a table's range is not a record"),
			}
		}
		// Five people and one edge.
		assert_eq!(records, 6);
		assert_eq!(classify_key(b""), KeyClass::Other);
		assert_eq!(classify_key(b"/*"), KeyClass::Other);
		assert_eq!(classify_key(&[0xff; 9]), KeyClass::Other);
	}
}
