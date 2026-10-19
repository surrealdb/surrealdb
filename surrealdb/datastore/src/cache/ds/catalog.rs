//! A table's catalog entry, shared across transactions and validated by one
//! read.
//!
//! Every transaction that plans a statement needs the definitions of the tables
//! the statement names — the table itself, its fields, its indexes — and reads
//! them through its own snapshot, which costs a key read and two range reads
//! per table per transaction. Those reads answer the same way for every
//! transaction that sees the same catalog. The per-table
//! [`CatalogVersion`](crate::values::catalog_version::CatalogVersion) says
//! *which* catalog entry a transaction sees: one read of it identifies the
//! state, and a snapshot another transaction built under the same version can
//! be reused as it stands.
//!
//! The unit of sharing is the table. A statement's [`CatalogSnapshot`] is the
//! set of [`TableSnapshot`]s it has asked for, each validated by its own
//! version and each shared with every other statement on the node that sees
//! the same version of that table, so the cost of the catalog is paid once per
//! version per table on the node rather than once per transaction, and a
//! catalog write to one table leaves every other table's snapshot in place.
//! A table no catalog write has stamped is validated by the content of its
//! definition instead; see [`TableSnapshot`].

use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;

use anyhow::Result;
use surrealdb_catalog::providers::TableProvider;
use surrealdb_catalog::{
	DatabaseId, EventDefinition, FieldDefinition, IndexDefinition, KeyStructure, NamespaceId,
	TableDefinition, TableLayouts, table_key,
};
use surrealdb_expr::val::TableName;
use uuid::Uuid;

use super::{DatastoreCache, Entry, Lookup};
use crate::tx::{Transaction, refused_by_release};

/// One table's definition, fields and indexes, as every transaction that sees
/// one catalog version of the table sees them.
///
/// # Sharing
///
/// A snapshot is keyed in the [`DatastoreCache`] by `(namespace, database,
/// table, version)`, where the version is the table's
/// [`CatalogVersion`](crate::values::catalog_version::CatalogVersion) as read
/// through the requesting transaction. Every catalog write that changes the
/// table's definition, its fields or its indexes stamps a fresh version in the
/// same transaction, so two transactions that read the same version see the
/// same table and one may serve the other's snapshot. The version is the
/// table's own, so a write to another table moves nothing here.
///
/// The version read goes through the transaction, not the node cache, which
/// is what keeps uncommitted DDL private: a transaction that has written DDL
/// on the table reads the version it minted, misses the cache, and loads
/// through itself, seeing its own writes. The entry it inserts is keyed by
/// that version, which no other transaction can read until this one commits —
/// and if it rolls back, the version is gone with it and the entry can never be
/// served to anyone. It ages out of the cache unread.
///
/// A table that no catalog write has stamped has no version, nor has one
/// whose definition a build that predates versions has rewritten since its
/// last stamp ([`Transaction::catalog_version`]). Its snapshot is
/// keyed by the content hash of its definition as read through the
/// requesting transaction instead: every catalog write to a table's fields or
/// indexes rewrites the table's definition — the `cache_fields_ts` and
/// `cache_indexes_ts` stamps it carries — so two transactions that read the
/// same definition see the same fields and indexes, and a write that changes
/// any of the three hashes to a fresh key and misses. A write made by this
/// build also stamps the table, after which the version keys it. A table that
/// is not defined and never stamped has no content to hash, and is loaded by
/// each transaction on its own.
///
/// # What the definition carries
///
/// `definition` is the table as of its last catalog write. The live-query
/// stamp `cache_lives_ts` is rewritten by `LIVE` and `KILL` without a catalog
/// write, so the value held here is the one of the last catalog write, not of
/// the last registration; the live-query cache keys on a definition read
/// through the transaction, never on this one.
#[derive(Debug)]
pub struct TableSnapshot {
	/// The table's definition, or `None` for a table that is not defined. An
	/// undefined table that carries a version — one that has been removed — is
	/// shared like any other, so asking for it again costs nothing; defining
	/// it stamps a new version, under which it is loaded afresh.
	pub definition: Option<Arc<TableDefinition>>,
	/// The table's field definitions, with the virtual definitions of the
	/// components its record id names ([`FieldDefinition::is_key_part`])
	/// placed straight after `id`.
	pub fields: Arc<[FieldDefinition]>,
	/// What the table's record ids are made of, as [`table_key`] reads it
	/// from the definition and the field list; `None` for a table whose ids
	/// are unconstrained. It is a function of the two beside it, so it is
	/// valid under the version that validates them.
	pub key: Option<Arc<KeyStructure>>,
	/// The table's index definitions, empty when [`Self::indexes_refused`].
	pub indexes: Arc<[IndexDefinition]>,
	/// Whether an index entry of the table needs a newer release than this
	/// build and so is missing from [`Self::indexes`].
	///
	/// A read of the table plans without its indexes and scans, which returns
	/// the same rows. A write must not: the index would go unmaintained. The
	/// snapshot therefore never answers a transaction's own read of the
	/// table's indexes while this is set, and that read refuses.
	pub indexes_refused: bool,
	/// The table's generation ring and the row layouts of its generations, which list exactly the
	/// generations and the ring stored under the version the snapshot is keyed by, because every
	/// statement that changes the table's `!lg` or `!mi` keys stamps its version after the
	/// change. Read for an undefined table too: rows may remain under its root until a range
	/// delete reaches them.
	pub layouts: Arc<TableLayouts>,
	/// The table's event definitions. Every statement that defines or removes an event rewrites
	/// the table's definition (its `cache_events_ts` stamp) and stamps its version, so the list is
	/// valid under the version, or the definition content, the snapshot is keyed by.
	pub events: Arc<[EventDefinition]>,
	/// The view tables that read this table, as [`TableProvider::all_tb_views`] lists them. Every
	/// statement that registers or unregisters a view on this table rewrites the table's
	/// definition (its `cache_tables_ts` stamp) and stamps its version, and a view's sources leave
	/// the list only through an unregistration, so the list is valid under the version, or the
	/// definition content, the snapshot is keyed by.
	pub views: Arc<[TableDefinition]>,
	/// The catalog version the table was read under, or `None` for a table
	/// that has never been stamped, whose snapshot is keyed by its
	/// definition's content instead.
	version: Option<Uuid>,
}

impl TableSnapshot {
	/// The snapshot of `table` under the catalog version `txn` sees.
	///
	/// One read of the table's version through `txn`, one cache lookup, and a
	/// load through `txn` on a miss, published under the version for the next
	/// transaction on the node. A table with no version is read through `txn`
	/// and looked up under the content hash of its definition, its fields and
	/// indexes loaded on a miss and published under the hash. Without a
	/// cache, or for an undefined table with no version, the table is loaded
	/// through `txn` and nothing is shared.
	///
	/// A snapshot served from the cache also answers `txn`'s own later reads
	/// of the table's definition, fields and indexes, as a load through `txn`
	/// does, so a reader that asks `txn` rather than the snapshot decodes
	/// nothing again.
	async fn for_table(
		cache: Option<&DatastoreCache>,
		txn: &Transaction,
		ns: NamespaceId,
		db: DatabaseId,
		table: &TableName,
	) -> Result<Arc<Self>> {
		let version = txn.catalog_version(ns, db, table).await?;
		let Some(cache) = cache else {
			let definition = txn.get_tb(ns, db, table, None).await?;
			return Ok(Arc::new(Self::load(txn, ns, db, table, definition, version).await?));
		};
		let lookup = match version {
			Some(version) => Lookup::Catalog(ns, db, table.as_str(), version),
			None => {
				let Some(definition) = txn.get_tb(ns, db, table, None).await? else {
					return Ok(Arc::new(Self::load(txn, ns, db, table, None, None).await?));
				};
				let lookup =
					Lookup::CatalogUnstamped(ns, db, table.as_str(), content_hash(&definition));
				if let Some(entry) = cache.get(&lookup).map(Entry::try_into_catalog).transpose()? {
					entry.adopt_into(txn, ns, db, table);
					return Ok(entry);
				}
				let loaded =
					Arc::new(Self::load(txn, ns, db, table, Some(definition), None).await?);
				cache.insert(lookup, Entry::Catalog(Arc::clone(&loaded)));
				return Ok(loaded);
			}
		};
		if let Some(entry) = cache.get(&lookup).map(Entry::try_into_catalog).transpose()? {
			entry.adopt_into(txn, ns, db, table);
			return Ok(entry);
		}
		let definition = txn.get_tb(ns, db, table, None).await?;
		let loaded = Arc::new(Self::load(txn, ns, db, table, definition, version).await?);
		cache.insert(lookup, Entry::Catalog(Arc::clone(&loaded)));
		Ok(loaded)
	}

	/// Loads the fields and indexes of one table through `txn`, as `txn` sees
	/// them, beside `definition`, the table's definition as `txn` read it.
	async fn load(
		txn: &Transaction,
		ns: NamespaceId,
		db: DatabaseId,
		table: &TableName,
		definition: Option<Arc<TableDefinition>>,
		version: Option<Uuid>,
	) -> Result<Self> {
		let fields = txn.all_tb_fields(ns, db, table, None).await?;
		let (indexes, indexes_refused) = match txn.all_tb_indexes(ns, db, table, None).await {
			Ok(indexes) => (indexes, false),
			Err(e) if refused_by_release(&e) => (Arc::from([]), true),
			Err(e) => return Err(e),
		};
		let layouts = txn.table_layouts(ns, db, table, None).await?;
		let events = txn.all_tb_events(ns, db, table, None).await?;
		let views = txn.all_tb_views(ns, db, table, None).await?;
		let key = definition.as_deref().and_then(|definition| table_key(definition, &fields));
		Ok(Self {
			definition,
			fields,
			key,
			indexes,
			indexes_refused,
			layouts,
			events,
			views,
			version,
		})
	}

	/// The catalog version the table was read under. `None` means the table
	/// has never been stamped, and this snapshot is keyed by the content of
	/// its definition.
	pub fn version(&self) -> Option<Uuid> {
		self.version
	}

	/// Answer `txn`'s reads of the table with this snapshot, which `txn` was
	/// served under the version, or the definition content, it read.
	fn adopt_into(&self, txn: &Transaction, ns: NamespaceId, db: DatabaseId, table: &TableName) {
		txn.adopt_table_catalog(
			ns,
			db,
			table,
			self.definition.as_ref(),
			&self.fields,
			(!self.indexes_refused).then_some(&self.indexes),
		);
		txn.adopt_table_listings(ns, db, table, &self.events, &self.views);
		txn.adopt_table_layouts(&self.layouts);
	}
}

/// The hash of `definition` as stored: every clause of the `DEFINE TABLE`,
/// and the `cache_*_ts` stamps its catalog writes rewrite. Identity is the
/// stored text, so a definition hashes the same whether or not its clauses
/// have been compiled.
fn content_hash(definition: &TableDefinition) -> u64 {
	let mut hasher = DefaultHasher::new();
	definition.hash(&mut hasher);
	hasher.finish()
}

/// The tables one statement has asked for, each as every transaction that sees
/// its catalog version sees it.
///
/// The snapshot is private to the transaction that built it; what is shared
/// across transactions is each [`TableSnapshot`] it holds. A table missing
/// from it is not a table that does not exist — that is recorded as a
/// [`TableSnapshot`] with no definition — but one nothing has asked for yet.
/// [`Self::extended`] obtains the missing tables, and the accessors answer
/// `None` only for a table the snapshot was never asked for.
#[derive(Debug)]
pub struct CatalogSnapshot {
	ns: NamespaceId,
	db: DatabaseId,
	tables: HashMap<TableName, Arc<TableSnapshot>>,
}

impl CatalogSnapshot {
	/// The snapshot of `ns`/`db` as `txn` sees it, holding `tables`.
	///
	/// Per table, one read of its version through `txn`, one cache lookup, and
	/// a load through `txn` on a miss. Without a cache, or for a table with no
	/// version, the table is loaded through `txn` and nothing is shared.
	///
	/// Tables the statement discovers later — through a `record<t>` field kind,
	/// or a body that names another table — are added with [`Self::extended`].
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(
			level = "trace",
			target = "surrealdb::core::kvs::tx",
			skip(cache, txn)
		)
	)]
	pub async fn for_tables(
		cache: Option<&DatastoreCache>,
		txn: &Transaction,
		ns: NamespaceId,
		db: DatabaseId,
		tables: &[TableName],
	) -> Result<Arc<Self>> {
		let empty = Arc::new(Self {
			ns,
			db,
			tables: HashMap::new(),
		});
		empty.extended(cache, txn, tables).await
	}

	/// This snapshot, holding `tables` as well.
	///
	/// Returns `self` when it already holds every one of them. Otherwise each
	/// missing table is obtained as [`Self::for_tables`] obtains one, through
	/// `txn`, which must be the transaction this snapshot was obtained for, and
	/// the grown snapshot is returned; the tables already held are carried
	/// over as they are.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(
			level = "trace",
			target = "surrealdb::core::kvs::tx",
			skip(self, cache, txn)
		)
	)]
	pub async fn extended(
		self: &Arc<Self>,
		cache: Option<&DatastoreCache>,
		txn: &Transaction,
		tables: &[TableName],
	) -> Result<Arc<Self>> {
		let mut missing: Vec<&TableName> =
			tables.iter().filter(|table| !self.tables.contains_key(*table)).collect();
		missing.sort_unstable();
		missing.dedup();
		if missing.is_empty() {
			return Ok(Arc::clone(self));
		}
		let mut tables = self.tables.clone();
		for table in missing {
			let entry = TableSnapshot::for_table(cache, txn, self.ns, self.db, table).await?;
			tables.insert(table.clone(), entry);
		}
		Ok(Arc::new(Self {
			ns: self.ns,
			db: self.db,
			tables,
		}))
	}

	/// The namespace the snapshot describes.
	pub fn namespace(&self) -> NamespaceId {
		self.ns
	}

	/// The database the snapshot describes.
	pub fn database(&self) -> DatabaseId {
		self.db
	}

	/// Everything held for `table`, or `None` if the snapshot was never asked
	/// for it. A table that is defined nowhere is `Some` with no definition.
	pub fn entry(&self, table: &TableName) -> Option<&Arc<TableSnapshot>> {
		self.tables.get(table)
	}

	/// The definition of `table`: `None` both for a table the snapshot was
	/// never asked for and for one that is not defined; [`Self::entry`] tells
	/// the two apart.
	pub fn table(&self, table: &TableName) -> Option<&Arc<TableDefinition>> {
		self.entry(table)?.definition.as_ref()
	}

	/// The field definitions of `table`, or `None` if the snapshot was never
	/// asked for it.
	pub fn fields(&self, table: &TableName) -> Option<&Arc<[FieldDefinition]>> {
		self.entry(table).map(|entry| &entry.fields)
	}

	/// The key structure of the record ids of `table`: `None` both for a
	/// table the snapshot was never asked for and for one whose ids are
	/// unconstrained; [`Self::entry`] tells the two apart.
	pub fn key(&self, table: &TableName) -> Option<&Arc<KeyStructure>> {
		self.entry(table)?.key.as_ref()
	}

	/// The index definitions of `table`, or `None` if the snapshot was never
	/// asked for it.
	pub fn indexes(&self, table: &TableName) -> Option<&Arc<[IndexDefinition]>> {
		self.entry(table).map(|entry| &entry.indexes)
	}

	/// The row layouts of `table`, or `None` if the snapshot was never asked for it.
	pub fn layouts(&self, table: &TableName) -> Option<&Arc<TableLayouts>> {
		self.entry(table).map(|entry| &entry.layouts)
	}

	/// The tables the snapshot holds.
	pub fn tables(&self) -> impl Iterator<Item = (&TableName, &Arc<TableSnapshot>)> {
		self.tables.iter()
	}

	/// How many tables the snapshot holds.
	pub fn len(&self) -> usize {
		self.tables.len()
	}

	/// Whether the snapshot holds no table at all.
	pub fn is_empty(&self) -> bool {
		self.tables.is_empty()
	}
}

#[cfg(test)]
mod tests {
	use std::borrow::Cow;
	use std::sync::Arc;

	use surrealdb_catalog::providers::{DatabaseProvider, NamespaceProvider, TableProvider};
	use surrealdb_catalog::{
		DatabaseDefinition, DatabaseId, FieldDefinition, FromStored, Index, IndexDefinition,
		IndexId, NamespaceDefinition, NamespaceId, StoredTableDefinition, TableDefinition, TableId,
	};
	use surrealdb_cnf::ConfigMap;
	use surrealdb_expr::expr::{Idiom, KeyNames, Kind, KindLiteral};
	use surrealdb_expr::val::TableName;
	use surrealdb_strand::Strand;
	use uuid::Uuid;

	use super::CatalogSnapshot;
	use crate::TransactionType;
	use crate::cache::ds::DatastoreCache;
	use crate::factory::TransactionFactory;
	use crate::key::schema::TableKey;
	use crate::sequences::Sequences;
	use crate::tx::Transaction;

	const NS: NamespaceId = NamespaceId(1);
	const DB: DatabaseId = DatabaseId(2);

	/// An in-memory store with one namespace and one database, and no cache
	/// entries yet.
	struct Store {
		tf: TransactionFactory,
		sequences: Sequences,
		cache: DatastoreCache,
	}

	impl Store {
		async fn new() -> Self {
			let builder = surrealdb_kvs_any::Backends::community()
				.new_transaction_builder("mem://", Default::default(), ConfigMap::default())
				.await
				.unwrap();
			let tf = TransactionFactory::new(
				Arc::new(crate::triggers::CommitTriggers::new()),
				builder,
				Arc::new(Default::default()),
			);
			let sequences = Sequences::new(tf.clone(), Uuid::new_v4());
			let store = Self {
				tf,
				sequences,
				cache: DatastoreCache::new(64),
			};
			let tx = store.write().await;
			tx.put_ns(NamespaceDefinition {
				namespace_id: NS,
				name: "ns".into(),
				comment: None,
			})
			.await
			.unwrap();
			tx.put_db(
				"ns",
				DatabaseDefinition {
					namespace_id: NS,
					database_id: DB,
					name: "db".into(),
					comment: None,
					changefeed: None,
					strict: false,
					compatibility: Default::default(),
				},
			)
			.await
			.unwrap();
			tx.commit().await.unwrap();
			store
		}

		async fn write(&self) -> Transaction {
			self.tf.transaction(TransactionType::Write, self.sequences.clone()).await.unwrap()
		}

		async fn read(&self) -> Transaction {
			self.tf.transaction(TransactionType::Read, self.sequences.clone()).await.unwrap()
		}

		/// `DEFINE TABLE`, committed.
		async fn define_table(&self, name: &str, id: u32) {
			let tx = self.write().await;
			tx.put_tb("ns", "db", &table(name, id)).await.unwrap();
			tx.commit().await.unwrap();
		}

		/// The snapshot a fresh read transaction gets for `tables`.
		async fn snapshot(&self, tables: &[&str]) -> Arc<CatalogSnapshot> {
			let tables: Vec<TableName> = tables.iter().map(|t| TableName::from(*t)).collect();
			let tx = self.read().await;
			let snapshot =
				CatalogSnapshot::for_tables(Some(&self.cache), &tx, NS, DB, &tables).await.unwrap();
			tx.cancel().await.unwrap();
			snapshot
		}
	}

	fn table(name: &str, id: u32) -> TableDefinition {
		TableDefinition::from_stored(&StoredTableDefinition::new(
			NS,
			DB,
			TableId(id),
			TableName::from(name),
		))
		.unwrap()
	}

	fn field(table: &str, name: &str) -> FieldDefinition {
		FieldDefinition {
			name: Idiom::field(name),
			table: TableName::from(table),
			..Default::default()
		}
	}

	fn field_names(snapshot: &CatalogSnapshot, table: &str) -> Vec<String> {
		let mut names: Vec<String> = snapshot
			.fields(&TableName::from(table))
			.unwrap()
			.iter()
			.map(|f| f.name.to_raw_string())
			.collect();
		names.sort();
		names
	}

	/// The entry one transaction builds is the one the next transaction
	/// gets, as the same allocation.
	#[tokio::test]
	async fn a_second_transaction_reuses_the_entry_the_first_built() {
		let store = Store::new().await;
		store.define_table("person", 1).await;
		let person = TableName::from("person");

		let first = store.snapshot(&["person"]).await;
		let entry = first.entry(&person).unwrap();
		assert!(entry.version().is_some(), "a stamped table has a shareable version");
		assert_eq!(entry.definition.as_ref().unwrap().name.as_str(), "person");

		let second = store.snapshot(&["person"]).await;
		assert!(
			Arc::ptr_eq(entry, second.entry(&person).unwrap()),
			"the second transaction served the first's entry"
		);
	}

	/// A transaction served a cached entry answers its own reads of the table
	/// with it: the definition, fields and indexes it reads afterwards are the
	/// entry's own allocations, not a second decode.
	#[tokio::test]
	async fn a_served_entry_answers_the_transactions_own_reads() {
		let store = Store::new().await;
		store.define_table("person", 1).await;
		let person = TableName::from("person");
		let tx = store.write().await;
		tx.put_tb_field(NS, DB, &person, &field("person", "name")).await.unwrap();
		tx.commit().await.unwrap();
		let built = store.snapshot(&["person"]).await;

		let tx = store.read().await;
		let served = CatalogSnapshot::for_tables(
			Some(&store.cache),
			&tx,
			NS,
			DB,
			std::slice::from_ref(&person),
		)
		.await
		.unwrap();
		let entry = served.entry(&person).unwrap();
		assert!(Arc::ptr_eq(entry, built.entry(&person).unwrap()), "served from the cache");
		let definition = tx.get_tb(NS, DB, &person, None).await.unwrap().unwrap();
		assert!(Arc::ptr_eq(&definition, entry.definition.as_ref().unwrap()));
		let fields = tx.all_tb_fields(NS, DB, &person, None).await.unwrap();
		assert!(Arc::ptr_eq(&fields, &entry.fields));
		assert_eq!(fields.len(), 1);
		let indexes = tx.all_tb_indexes(NS, DB, &person, None).await.unwrap();
		assert!(Arc::ptr_eq(&indexes, &entry.indexes));
		tx.cancel().await.unwrap();
	}

	/// A committed catalog write moves the table's version, so the next
	/// transaction misses the old entry and sees the write; another table's
	/// entry is untouched.
	#[tokio::test]
	async fn ddl_changes_the_version_and_misses() {
		let store = Store::new().await;
		store.define_table("person", 1).await;
		store.define_table("company", 2).await;
		let person = TableName::from("person");
		let company = TableName::from("company");
		let before = store.snapshot(&["person", "company"]).await;
		assert!(field_names(&before, "person").is_empty());

		// DEFINE FIELD, DEFINE INDEX and REMOVE TABLE each stamp a new version.
		let tx = store.write().await;
		tx.put_tb_field(NS, DB, &person, &field("person", "name")).await.unwrap();
		tx.commit().await.unwrap();
		let after_field = store.snapshot(&["person", "company"]).await;
		let version = |s: &CatalogSnapshot, t: &TableName| s.entry(t).unwrap().version();
		assert_ne!(version(&before, &person), version(&after_field, &person));
		assert_eq!(field_names(&after_field, "person"), ["name"]);
		assert!(
			Arc::ptr_eq(before.entry(&company).unwrap(), after_field.entry(&company).unwrap()),
			"a write to person leaves company's entry in place"
		);

		let tx = store.write().await;
		tx.put_tb_index(
			NS,
			DB,
			&person,
			&IndexDefinition {
				index_id: IndexId(1),
				name: "by_name".into(),
				table_name: person.clone(),
				cols: vec![Idiom::field("name")],
				index: Index::Idx,
				count_cond: None,
				comment: None,
				prepare_remove: false,
				format_version: 1,
			},
		)
		.await
		.unwrap();
		tx.commit().await.unwrap();
		let after_index = store.snapshot(&["person"]).await;
		assert_ne!(version(&after_field, &person), version(&after_index, &person));
		assert_eq!(after_index.indexes(&person).unwrap().len(), 1);

		let tx = store.write().await;
		tx.del_tb("ns", "db", &person).await.unwrap();
		tx.commit().await.unwrap();
		let after_remove = store.snapshot(&["person"]).await;
		assert_ne!(version(&after_index, &person), version(&after_remove, &person));
		assert!(after_remove.entry(&person).is_some(), "asked for, so held");
		assert!(after_remove.table(&person).is_none(), "but not defined");
		assert!(version(&after_remove, &person).is_some(), "a removed table keeps a version");
		let again = store.snapshot(&["person"]).await;
		assert!(
			Arc::ptr_eq(after_remove.entry(&person).unwrap(), again.entry(&person).unwrap()),
			"so its absence is served from the cache"
		);
	}

	/// A transaction that writes DDL and then plans sees its own version and
	/// its own writes; once it rolls back, nothing it cached is ever served.
	#[tokio::test]
	async fn a_rolled_back_version_is_never_served() {
		let store = Store::new().await;
		store.define_table("person", 1).await;
		let person = TableName::from("person");
		let committed = store.snapshot(&["person"]).await;
		let committed_version = committed.entry(&person).unwrap().version();

		let tx = store.write().await;
		tx.put_tb_field(NS, DB, &person, &field("person", "secret")).await.unwrap();
		let private = CatalogSnapshot::for_tables(
			Some(&store.cache),
			&tx,
			NS,
			DB,
			std::slice::from_ref(&person),
		)
		.await
		.unwrap();
		let private_version = private.entry(&person).unwrap().version();
		assert_ne!(private_version, committed_version, "the writer sees the version it minted");
		assert_eq!(field_names(&private, "person"), ["secret"], "and its own uncommitted field");
		tx.cancel().await.unwrap();

		let later = store.snapshot(&["person"]).await;
		assert!(
			Arc::ptr_eq(committed.entry(&person).unwrap(), later.entry(&person).unwrap()),
			"the committed entry is served, unchanged"
		);
		assert!(field_names(&later, "person").is_empty());
		assert!(
			store
				.cache
				.get(&super::Lookup::Catalog(NS, DB, "person", private_version.unwrap()))
				.is_some(),
			"the private entry is still in the cache, keyed by a version nobody can read"
		);
	}

	/// A table a snapshot does not hold is obtained and added, and the entry
	/// it obtains is what later transactions get.
	#[tokio::test]
	async fn a_missing_table_is_filled_in() {
		let store = Store::new().await;
		store.define_table("person", 1).await;
		store.define_table("company", 2).await;
		let person = TableName::from("person");
		let company = TableName::from("company");

		let partial = store.snapshot(&["person"]).await;
		assert!(partial.entry(&company).is_none(), "never asked for");
		assert_eq!(partial.len(), 1);

		let grown = store.snapshot(&["person", "company"]).await;
		assert_eq!(grown.len(), 2);
		assert!(grown.table(&company).is_some());
		assert!(
			Arc::ptr_eq(grown.entry(&person).unwrap(), partial.entry(&person).unwrap()),
			"the table already cached is served, not reloaded"
		);

		let served = store.snapshot(&["company"]).await;
		assert!(
			Arc::ptr_eq(grown.entry(&company).unwrap(), served.entry(&company).unwrap()),
			"the entry the grown snapshot obtained is served"
		);

		// Extending a snapshot that already holds the tables is the identity.
		let tx = store.read().await;
		let same =
			grown.extended(Some(&store.cache), &tx, std::slice::from_ref(&person)).await.unwrap();
		assert!(Arc::ptr_eq(&grown, &same));
		// Extending with a table nothing defines records its absence.
		let with_ghost =
			grown.extended(Some(&store.cache), &tx, &[TableName::from("ghost")]).await.unwrap();
		tx.cancel().await.unwrap();
		assert_eq!(with_ghost.len(), 3);
		let ghost = with_ghost.entry(&TableName::from("ghost")).unwrap();
		assert!(ghost.definition.is_none());
		assert!(ghost.version().is_none(), "never stamped, so private");
	}

	/// A table no catalog write has stamped has no version: its entry is
	/// shared under the content of its definition, so a second transaction
	/// reading the same definition is served the first's entry, a rewritten
	/// definition misses, and the first catalog write stamps the table, after
	/// which the version keys it. A stamped table beside it is shared as
	/// usual.
	#[tokio::test]
	async fn an_unstamped_table_is_shared_by_the_content_of_its_definition() {
		let store = Store::new().await;
		// A table definition written around the provider, as a datastore
		// written before versions were stamped holds one.
		let legacy = TableName::from("legacy");
		async fn write_definition(store: &Store, table: &TableName, def: TableDefinition) {
			let tx = store.write().await;
			tx.set_key(
				&TableKey {
					ns: NS,
					db: DB,
					tb: Cow::Borrowed(table),
				},
				&def.to_stored(),
			)
			.await
			.unwrap();
			tx.commit().await.unwrap();
		}
		write_definition(&store, &legacy, table("legacy", 1)).await;
		store.define_table("modern", 2).await;
		let modern = TableName::from("modern");

		let first = store.snapshot(&["legacy", "modern"]).await;
		assert!(first.entry(&legacy).unwrap().version().is_none(), "never stamped");
		assert!(first.table(&legacy).is_some());
		assert!(first.entry(&modern).unwrap().version().is_some());
		let second = store.snapshot(&["legacy", "modern"]).await;
		assert!(
			Arc::ptr_eq(first.entry(&legacy).unwrap(), second.entry(&legacy).unwrap()),
			"the same definition is served the same entry"
		);
		assert!(Arc::ptr_eq(first.entry(&modern).unwrap(), second.entry(&modern).unwrap()));

		// A definition rewritten around the provider — as an older build
		// rewrites it, stamping `cache_fields_ts`, for every field it defines
		// — hashes to a fresh key: the entry is reloaded, and the fields it
		// holds are the ones the rewritten table has.
		let tx = store.write().await;
		tx.set_key(
			&crate::key::schema::FieldKey {
				ns: NS,
				db: DB,
				tb: Cow::Borrowed(&legacy),
				fd: Cow::Owned(Idiom::field("name").to_raw_string()),
			},
			&field("legacy", "name").to_stored(),
		)
		.await
		.unwrap();
		tx.commit().await.unwrap();
		let mut rewritten = table("legacy", 1);
		rewritten.cache_fields_ts = Uuid::now_v7();
		write_definition(&store, &legacy, rewritten).await;
		let reloaded = store.snapshot(&["legacy"]).await;
		assert!(!Arc::ptr_eq(first.entry(&legacy).unwrap(), reloaded.entry(&legacy).unwrap()));
		assert_eq!(field_names(&reloaded, "legacy"), ["name"]);
		assert!(reloaded.entry(&legacy).unwrap().version().is_none(), "still never stamped");
		let served = store.snapshot(&["legacy"]).await;
		assert!(Arc::ptr_eq(reloaded.entry(&legacy).unwrap(), served.entry(&legacy).unwrap()));

		// The first catalog write through the provider stamps the table, and
		// the version keys it from then on.
		let tx = store.write().await;
		tx.put_tb_field(NS, DB, &legacy, &field("legacy", "age")).await.unwrap();
		tx.commit().await.unwrap();
		let stamped = store.snapshot(&["legacy"]).await;
		assert!(stamped.entry(&legacy).unwrap().version().is_some());
		assert_eq!(field_names(&stamped, "legacy"), ["age", "name"]);
		let again = store.snapshot(&["legacy"]).await;
		assert!(Arc::ptr_eq(stamped.entry(&legacy).unwrap(), again.entry(&legacy).unwrap()));
	}

	/// A transaction reads a table's version from storage once: later reads,
	/// of a stamped table and of one that has never been stamped alike, are
	/// answered from the transaction cache, and a stamp the transaction
	/// itself writes is what it reads afterwards.
	#[tokio::test]
	async fn a_version_is_read_from_storage_once_per_transaction() {
		let store = Store::new().await;
		store.define_table("person", 1).await;
		let person = TableName::from("person");
		let ghost = TableName::from("ghost");

		let tx = store.write().await;
		let stamped = tx.catalog_version(NS, DB, &person).await.unwrap();
		assert!(stamped.is_some());
		assert!(tx.catalog_version(NS, DB, &ghost).await.unwrap().is_none());
		let reads = tx.metrics_snapshot_for_test().ops_get;
		assert_eq!(tx.catalog_version(NS, DB, &person).await.unwrap(), stamped);
		assert!(tx.catalog_version(NS, DB, &ghost).await.unwrap().is_none());
		assert_eq!(tx.metrics_snapshot_for_test().ops_get, reads, "answered from the cache");

		tx.put_tb_field(NS, DB, &person, &field("person", "name")).await.unwrap();
		let minted = tx.catalog_version(NS, DB, &person).await.unwrap();
		assert!(minted.is_some());
		assert_ne!(minted, stamped, "the stamp this transaction wrote is what it reads");
		assert_eq!(tx.metrics_snapshot_for_test().ops_get, reads);
		tx.cancel().await.unwrap();
	}

	/// Catalog writes to different tables share no key, so two transactions
	/// that each define a field, an index or a table of their own commit
	/// side by side.
	#[tokio::test]
	async fn ddl_on_different_tables_does_not_conflict() {
		let store = Store::new().await;
		store.define_table("person", 1).await;
		store.define_table("company", 2).await;

		let a = store.write().await;
		let b = store.write().await;
		a.put_tb_field(NS, DB, &TableName::from("person"), &field("person", "name")).await.unwrap();
		b.put_tb_field(NS, DB, &TableName::from("company"), &field("company", "name"))
			.await
			.unwrap();
		a.commit().await.unwrap();
		b.commit().await.expect("a field on another table is not a conflicting write");

		let a = store.write().await;
		let b = store.write().await;
		a.put_tb("ns", "db", &table("order", 3)).await.unwrap();
		b.put_tb("ns", "db", &table("invoice", 4)).await.unwrap();
		a.commit().await.unwrap();
		b.commit().await.expect("defining another table is not a conflicting write");
	}

	/// Registering or killing a live query rewrites the table's live-query
	/// stamp and nothing else: two tables' registrations commit side by side,
	/// and the catalog entry of the table is still served unchanged.
	#[tokio::test]
	async fn live_registration_neither_conflicts_nor_moves_the_catalog() {
		let store = Store::new().await;
		store.define_table("person", 1).await;
		store.define_table("order", 2).await;
		let person = TableName::from("person");
		let before = store.snapshot(&["person"]).await;

		let a = store.write().await;
		let b = store.write().await;
		a.bump_table_lives_cache(NS, DB, &person).await.unwrap();
		b.bump_table_lives_cache(NS, DB, &TableName::from("order")).await.unwrap();
		a.commit().await.unwrap();
		b.commit().await.expect("a live query on another table is not a conflicting write");

		let after = store.snapshot(&["person"]).await;
		assert!(
			Arc::ptr_eq(before.entry(&person).unwrap(), after.entry(&person).unwrap()),
			"a live query is not a catalog change"
		);
	}

	/// A live-query registration and a DDL change to the same table cannot
	/// both commit: the stamp is written under a locked read of the
	/// definition, as every DDL write is, so the two conflict instead of the
	/// later commit overwriting the earlier one's definition.
	#[tokio::test]
	async fn live_registration_conflicts_with_ddl_on_the_same_table() {
		let store = Store::new().await;
		store.define_table("person", 1).await;
		let person = TableName::from("person");

		let a = store.write().await;
		let b = store.write().await;
		a.bump_table_lives_cache(NS, DB, &person).await.unwrap();
		let mut altered = (*b.get_tb(NS, DB, &person, None).await.unwrap().unwrap()).clone();
		altered.permissions.select = surrealdb_catalog::Permission::Full;
		b.replace_tb("ns", "db", &altered).await.unwrap();
		a.commit().await.unwrap();
		b.commit().await.expect_err("a DDL write racing a live registration conflicts");

		let tx = store.read().await;
		let stored = tx.get_tb(NS, DB, &person, None).await.unwrap().unwrap();
		tx.cancel().await.unwrap();
		assert!(
			matches!(stored.permissions.select, surrealdb_catalog::Permission::None),
			"the definition the registration rewrote is the one it read"
		);
	}

	/// Without a node cache the API still answers, loading through the
	/// transaction each time.
	#[tokio::test]
	async fn works_without_a_cache() {
		let store = Store::new().await;
		store.define_table("person", 1).await;
		let person = TableName::from("person");
		let tx = store.read().await;
		let snapshot =
			CatalogSnapshot::for_tables(None, &tx, NS, DB, std::slice::from_ref(&person))
				.await
				.unwrap();
		let version = snapshot.entry(&person).unwrap().version();
		assert!(version.is_some());
		assert!(snapshot.table(&person).is_some());
		let grown = snapshot
			.extended(None, &tx, &[person.clone(), TableName::from("other")])
			.await
			.unwrap();
		assert_eq!(grown.len(), 2);
		tx.cancel().await.unwrap();
		assert!(
			store.cache.get(&super::Lookup::Catalog(NS, DB, "person", version.unwrap())).is_none()
		);
	}
	/// `DEFINE FIELD id ON reading TYPE [city: string, time: datetime]`.
	fn named_id() -> FieldDefinition {
		let mut id = field("reading", "id");
		id.set_type_strict(
			Some(Kind::Literal(KindLiteral::Array(vec![Kind::String, Kind::Datetime]))),
			Some(KeyNames::new(vec![Strand::new("city"), Strand::new("time")], 2).unwrap()),
		)
		.unwrap();
		id
	}

	/// An `id` with a type and no names.
	fn plain_id() -> FieldDefinition {
		let mut id = field("reading", "id");
		id.set_type_strict(
			Some(Kind::Literal(KindLiteral::Array(vec![Kind::String, Kind::Datetime]))),
			None,
		)
		.unwrap();
		id
	}

	/// The field names of `table`, in the order the snapshot lists them.
	fn field_order(snapshot: &CatalogSnapshot, table: &str) -> Vec<String> {
		snapshot
			.fields(&TableName::from(table))
			.unwrap()
			.iter()
			.map(|f| f.name.to_raw_string())
			.collect()
	}

	/// The components a record id names are fields of the table's snapshot,
	/// placed straight after `id`, and the snapshot carries the key structure
	/// they were derived from.
	#[tokio::test]
	async fn a_named_id_puts_its_components_in_the_snapshot_with_the_key() {
		let store = Store::new().await;
		store.define_table("reading", 1).await;
		let reading = TableName::from("reading");
		let tx = store.write().await;
		tx.put_tb_field(NS, DB, &reading, &field("reading", "temp")).await.unwrap();
		tx.put_tb_field(NS, DB, &reading, &named_id()).await.unwrap();
		tx.commit().await.unwrap();

		let snapshot = store.snapshot(&["reading"]).await;
		assert_eq!(field_order(&snapshot, "reading"), ["id", "city", "time", "temp"]);
		let fields = snapshot.fields(&reading).unwrap();
		let parts: Vec<Option<u32>> = fields.iter().map(|f| f.key_part).collect();
		assert_eq!(parts, [None, Some(0), Some(1), None]);
		assert!(fields[1].is_key_part() && fields[2].is_key_part());
		assert!(!fields[0].is_key_part() && !fields[3].is_key_part());

		let key = snapshot.key(&reading).expect("a named id gives the table a key structure");
		assert_eq!(key.arity(), 2);
		let id_key = fields[0].live_key().expect("the id holds the structure the snapshot carries");
		assert!(Arc::ptr_eq(key, id_key), "the snapshot carries the id's own structure");

		// The entry is shared like any other: a second transaction is served it.
		let again = store.snapshot(&["reading"]).await;
		assert!(Arc::ptr_eq(snapshot.entry(&reading).unwrap(), again.entry(&reading).unwrap()));
		assert!(Arc::ptr_eq(key, again.key(&reading).unwrap()));
	}

	/// A table whose `id` names nothing has no key structure and no virtual
	/// definition, and its field list is the allocation the transaction holds
	/// for the table.
	#[tokio::test]
	async fn an_unnamed_id_adds_nothing() {
		let store = Store::new().await;
		store.define_table("reading", 1).await;
		let reading = TableName::from("reading");
		let tx = store.write().await;
		tx.put_tb_field(NS, DB, &reading, &plain_id()).await.unwrap();
		tx.put_tb_field(NS, DB, &reading, &field("reading", "temp")).await.unwrap();
		tx.commit().await.unwrap();

		let snapshot = store.snapshot(&["reading"]).await;
		assert_eq!(field_order(&snapshot, "reading"), ["id", "temp"]);
		assert!(snapshot.fields(&reading).unwrap().iter().all(|f| f.key_part.is_none()));
		assert!(snapshot.key(&reading).is_none());
		assert!(snapshot.entry(&reading).unwrap().key.is_none());

		let tx = store.read().await;
		let first = tx.all_tb_fields(NS, DB, &reading, None).await.unwrap();
		let second = tx.all_tb_fields(NS, DB, &reading, None).await.unwrap();
		tx.cancel().await.unwrap();
		assert!(Arc::ptr_eq(&first, &second), "the transaction answers from one list");
		assert_eq!(*first, **snapshot.fields(&reading).unwrap());
	}

	/// A table with no fields at all has none of either.
	#[tokio::test]
	async fn a_table_with_no_fields_has_no_key() {
		let store = Store::new().await;
		store.define_table("person", 1).await;
		let snapshot = store.snapshot(&["person"]).await;
		assert!(snapshot.fields(&TableName::from("person")).unwrap().is_empty());
		assert!(snapshot.key(&TableName::from("person")).is_none());
		assert!(snapshot.key(&TableName::from("never_asked")).is_none());
	}

	/// The derived definition of a component has no stored form: storing one is
	/// refused, nothing is written and the catalog version stays where it was.
	#[tokio::test]
	async fn a_virtual_definition_cannot_be_stored() {
		let store = Store::new().await;
		store.define_table("reading", 1).await;
		let reading = TableName::from("reading");
		let tx = store.write().await;
		tx.put_tb_field(NS, DB, &reading, &named_id()).await.unwrap();
		tx.commit().await.unwrap();
		let before = store.snapshot(&["reading"]).await;
		let version = before.entry(&reading).unwrap().version();
		let derived = before.key(&reading).unwrap().derived_fields(&reading);
		assert_eq!(derived.len(), 2);

		for virtual_field in &derived {
			let tx = store.write().await;
			let error = tx
				.put_tb_field(NS, DB, &reading, virtual_field)
				.await
				.expect_err("a field derived from the record id is not stored");
			let message = error.to_string();
			assert!(message.contains(&virtual_field.name.to_raw_string()), "{message}");
			assert!(message.contains("derived from the record id"), "{message}");
			let own = tx.all_tb_fields(NS, DB, &reading, None).await.unwrap();
			assert_eq!(own.len(), 3, "the transaction still holds the one stored id");
			tx.cancel().await.unwrap();
		}

		let after = store.snapshot(&["reading"]).await;
		assert_eq!(after.entry(&reading).unwrap().version(), version);
		assert!(Arc::ptr_eq(before.entry(&reading).unwrap(), after.entry(&reading).unwrap()));
		let tx = store.read().await;
		let stored = tx.all_tb_fields(NS, DB, &reading, None).await.unwrap();
		tx.cancel().await.unwrap();
		assert_eq!(stored.iter().filter(|f| f.key_part.is_none()).count(), 1);
	}

	/// Redefining `id` without its names takes the components out of the next
	/// snapshot together with the key structure, and the snapshot is read under
	/// a new version.
	#[tokio::test]
	async fn redefining_the_id_without_names_removes_the_components() {
		let store = Store::new().await;
		store.define_table("reading", 1).await;
		let reading = TableName::from("reading");
		let tx = store.write().await;
		tx.put_tb_field(NS, DB, &reading, &named_id()).await.unwrap();
		tx.put_tb_field(NS, DB, &reading, &field("reading", "temp")).await.unwrap();
		tx.commit().await.unwrap();
		let named = store.snapshot(&["reading"]).await;
		assert_eq!(field_order(&named, "reading"), ["id", "city", "time", "temp"]);

		let tx = store.write().await;
		tx.put_tb_field(NS, DB, &reading, &plain_id()).await.unwrap();
		tx.commit().await.unwrap();
		let plain = store.snapshot(&["reading"]).await;
		assert_ne!(
			named.entry(&reading).unwrap().version(),
			plain.entry(&reading).unwrap().version()
		);
		assert_eq!(field_order(&plain, "reading"), ["id", "temp"]);
		assert!(plain.key(&reading).is_none());
		assert!(named.key(&reading).is_some(), "the snapshot already taken keeps what it read");
	}

	/// Uncommitted DDL on a named `id` is private to the transaction that wrote
	/// it, components and key structure included.
	#[tokio::test]
	async fn an_uncommitted_named_id_is_private() {
		let store = Store::new().await;
		store.define_table("reading", 1).await;
		let reading = TableName::from("reading");
		let committed = store.snapshot(&["reading"]).await;
		assert!(committed.key(&reading).is_none());

		let tx = store.write().await;
		tx.put_tb_field(NS, DB, &reading, &named_id()).await.unwrap();
		let private = CatalogSnapshot::for_tables(
			Some(&store.cache),
			&tx,
			NS,
			DB,
			std::slice::from_ref(&reading),
		)
		.await
		.unwrap();
		assert_eq!(field_order(&private, "reading"), ["id", "city", "time"]);
		assert!(private.key(&reading).is_some());
		tx.cancel().await.unwrap();

		let later = store.snapshot(&["reading"]).await;
		assert!(field_order(&later, "reading").is_empty());
		assert!(later.key(&reading).is_none());
	}
}
