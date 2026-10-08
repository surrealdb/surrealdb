//! The catalog state a compilation depended on, checked before its code is
//! reused.
//!
//! A compilation run inside a recording of the transaction's catalog reads
//! ([`crate::kvs::catalog_reads`]) knows every catalog entry it read, by name.
//! [`CatalogDeps::resolve`] reads each of them once more, in the same
//! transaction, and keeps what identifies its state: a table's catalog
//! version — which every write of the table's definition, fields, indexes,
//! events or views mints afresh — or, for a table with no version proven
//! against its definition, the content hash of that definition; a function's content hash; a
//! parameter's and a database's definition; for a database whose tables were
//! listed, the tables it defines and the catalog versions stamped in it,
//! which stand for the state of each of its tables; and, for the `REFERENCE`
//! fields read for the records of a table, those fields. Each is kept as
//! absent where the entry does not exist, so defining it later is a change
//! too.
//!
//! [`CatalogDeps::hold`] reads the same entries through another transaction,
//! or the same one later, and answers whether every one is as it was. A
//! listed database's state is not read again by a transaction under the
//! catalog epoch it was last read under ([`Transaction::catalog_epoch`]),
//! which sees every table the database defines as that read did; it is noted
//! in the transaction's open recordings as a read of the listing all the
//! same, so a compilation that links what these deps guard depends on the
//! listing too. What it answers rests on the identities: a version is
//! answered only while the definition it was stamped beside is unchanged, so
//! one an older build in the same cluster has rewritten since is no version
//! and its table is held by content instead
//! ([`Transaction::catalog_version`]); and a catalog version is
//! minted per write and is visible only to the transaction that minted it
//! until it commits, so a state read through one transaction's uncommitted
//! DDL is never matched by another transaction until that DDL commits, and
//! never at all once it rolls back. The `REFERENCE` fields that can name a
//! record of a table are found again from those read before, reading the
//! fields only of the tables whose catalog version changed
//! ([`Transaction::reference_fields`]), and stand under a catalog epoch as a
//! listing does.
//!
//! A recording that read anything its names cannot identify
//! ([`CatalogReads::opaque`](crate::kvs::catalog_reads::CatalogReads))
//! resolves to nothing: what was compiled under it is not reused.

use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;

use surrealdb_strand::TableName;
use uuid::Uuid;

use crate::catalog::providers::{DatabaseProvider, TableProvider};
use crate::catalog::{
	DatabaseDefinition, DatabaseId, FunctionDefinition, NamespaceId, ParamDefinition,
};
use crate::kvs::Transaction;
use crate::kvs::cache::ds::FunctionSnapshot;
use crate::kvs::catalog_reads::CatalogReads;
use crate::kvs::tx::ReferenceFields;

/// The state one table was read in.
#[derive(Clone, Debug, PartialEq, Eq)]
enum TableState {
	/// Stamped with this catalog version, proven against the definition.
	Version(Uuid),
	/// No proven version, defined as this content hash.
	Content(u64),
	/// No proven version and not defined.
	Absent,
}

/// The definition a function was read as, and its content hash.
#[derive(Clone, Debug)]
struct FunctionState {
	definition: Arc<FunctionDefinition>,
	hash: u64,
}

impl PartialEq for FunctionState {
	fn eq(&self, other: &Self) -> bool {
		self.hash == other.hash
	}
}

impl Eq for FunctionState {}

/// What a compilation read of the catalog, and the state it read it in.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct CatalogDeps {
	tables: Vec<(NamespaceId, DatabaseId, TableName, TableState)>,
	/// Tables read by name whose database did not exist.
	missing_databases: Vec<(String, String)>,
	functions: Vec<(NamespaceId, DatabaseId, String, Option<FunctionState>)>,
	params: Vec<(NamespaceId, DatabaseId, String, Option<Arc<ParamDefinition>>)>,
	databases: Vec<(String, String, Option<Arc<DatabaseDefinition>>)>,
	/// The state of every table of each database whose tables were listed,
	/// which stands for the state of each table of it read by name.
	listings: Vec<(NamespaceId, DatabaseId, DatabaseState)>,
	/// The `REFERENCE` fields of each database read for the records of a
	/// table, or of any table where it is `None`.
	references: Vec<(NamespaceId, DatabaseId, Option<TableName>, ReferencesState)>,
}

/// The `REFERENCE` fields read for the records of one table: those that can
/// name one, by the table that defines them. Compared by those fields alone.
#[derive(Clone, Debug)]
struct ReferencesState {
	fields: Referencing,
	/// The `REFERENCE` fields of every table they were found among, which a
	/// later check reads only the changed tables of again: the latest
	/// found to hold.
	read: Arc<std::sync::Mutex<Arc<ReferenceFields>>>,
	seen: SeenAt,
}

impl PartialEq for ReferencesState {
	fn eq(&self, other: &Self) -> bool {
		self.fields == other.fields
	}
}

impl Eq for ReferencesState {}

/// The `REFERENCE` fields that can name a record of a table, by the table
/// that defines them, in the database's table order.
pub(crate) type Referencing = Arc<[(TableName, Arc<[crate::catalog::FieldDefinition]>)]>;

/// Of the `REFERENCE` fields `fields` holds, those that can name a record of
/// `target`, or every one where it is `None`, by the table that defines them.
pub(crate) fn references_to(fields: &ReferenceFields, target: Option<&TableName>) -> Referencing {
	let Some(target) = target else {
		return fields.tables.iter().cloned().collect();
	};
	fields
		.tables
		.iter()
		.filter_map(|(table, declared)| {
			let can = |field: &crate::catalog::FieldDefinition| {
				field.field_kind.as_ref().is_none_or(|kind| kind.reference_can_target(target))
			};
			if declared.iter().all(can) {
				return Some((table.clone(), Arc::clone(declared)));
			}
			let kept: Arc<[crate::catalog::FieldDefinition]> =
				declared.iter().filter(|field| can(field)).cloned().collect();
			(!kept.is_empty()).then(|| (table.clone(), kept))
		})
		.collect()
}

/// The state of every table of a database: the tables it defines, the
/// catalog versions stamped in it, and the definition of each table it
/// defines that no catalog write has stamped.
#[derive(Clone, Debug, PartialEq, Eq)]
struct DatabaseState {
	catalog: Arc<crate::kvs::tx::DatabaseCatalogState>,
	unstamped: Vec<(TableName, TableState)>,
	/// The catalog epoch of the latest transaction that read the database in
	/// this state ([`Transaction::catalog_epoch`]); every transaction under
	/// that epoch reads it so.
	seen: SeenAt,
}

/// The catalog epoch a state was last read under, or
/// [`crate::kvs::tx::NO_CATALOG_EPOCH`]. It records where a state was seen,
/// not what the state is, and compares equal to every other.
#[derive(Debug)]
struct SeenAt(std::sync::atomic::AtomicU64);

impl SeenAt {
	fn unseen() -> Self {
		Self(std::sync::atomic::AtomicU64::new(crate::kvs::tx::NO_CATALOG_EPOCH))
	}

	fn is(&self, epoch: u64) -> bool {
		self.0.load(std::sync::atomic::Ordering::Acquire) == epoch
	}

	fn set(&self, epoch: u64) {
		self.0.store(epoch, std::sync::atomic::Ordering::Release);
	}
}

impl Clone for SeenAt {
	fn clone(&self) -> Self {
		Self(std::sync::atomic::AtomicU64::new(self.0.load(std::sync::atomic::Ordering::Acquire)))
	}
}

impl PartialEq for SeenAt {
	fn eq(&self, _: &Self) -> bool {
		true
	}
}

impl Eq for SeenAt {}

impl CatalogDeps {
	/// The state `txn` holds each entry `reads` names in; `None` for an
	/// opaque recording, or one naming an entry that cannot be read.
	pub(crate) async fn resolve(txn: &Transaction, reads: &CatalogReads) -> Option<Self> {
		if reads.opaque {
			return None;
		}
		let mut deps = Self::default();
		for (ns, db, tb) in &reads.tables {
			let state = table_state(txn, *ns, *db, tb).await?;
			deps.tables.push((*ns, *db, tb.clone(), state));
		}
		for (ns, db, tb) in &reads.named_tables {
			match txn.get_db_by_name(ns, db, None).await.ok()? {
				Some(def) => {
					let (ns, db) = (def.namespace_id, def.database_id);
					if !deps.tables.iter().any(|(n, d, t, _)| *n == ns && *d == db && t == tb) {
						let state = table_state(txn, ns, db, tb).await?;
						deps.tables.push((ns, db, tb.clone(), state));
					}
				}
				None => deps.missing_databases.push((ns.clone(), db.clone())),
			}
		}
		for (ns, db, name) in &reads.functions {
			deps.functions.push((
				*ns,
				*db,
				name.clone(),
				function_state(txn, *ns, *db, name).await?,
			));
		}
		for (ns, db, name) in &reads.params {
			deps.params.push((*ns, *db, name.clone(), param_state(txn, *ns, *db, name).await?));
		}
		for (ns, db) in &reads.databases {
			deps.databases.push((
				ns.clone(),
				db.clone(),
				txn.get_db_by_name(ns, db, None).await.ok()?,
			));
		}
		for (ns, db) in &reads.listings {
			let state = database_state(txn, *ns, *db).await?;
			// The epoch is asked after the read, which it then covers.
			if let Some(epoch) = txn.catalog_epoch() {
				state.seen.set(epoch);
			}
			deps.listings.push((*ns, *db, state));
		}
		for (ns, db, target) in &reads.references {
			let read = txn.reference_fields(*ns, *db, None).await.ok()?;
			let seen = SeenAt::unseen();
			if let Some(epoch) = txn.catalog_epoch() {
				seen.set(epoch);
			}
			deps.references.push((
				*ns,
				*db,
				target.clone(),
				ReferencesState {
					fields: references_to(&read, target.as_ref()),
					read: Arc::new(std::sync::Mutex::new(read)),
					seen,
				},
			));
		}
		// A listed database's state holds the state of each of its tables.
		let listed = &deps.listings;
		deps.tables.retain(|(ns, db, _, _)| {
			!listed.iter().any(|(listed_ns, listed_db, _)| listed_ns == ns && listed_db == db)
		});
		Some(deps)
	}

	/// Whether every entry is, as `txn` reads it, in the state it was
	/// resolved in.
	pub(crate) async fn hold(&self, txn: &Transaction) -> bool {
		for (ns, db, tb, state) in &self.tables {
			if table_state(txn, *ns, *db, tb).await.as_ref() != Some(state) {
				return false;
			}
		}
		self.hold_beyond_tables(txn).await
	}

	/// Whether every entry but the tables read by name holds.
	async fn hold_beyond_tables(&self, txn: &Transaction) -> bool {
		for (ns, db) in &self.missing_databases {
			if !matches!(txn.get_db_by_name(ns, db, None).await, Ok(None)) {
				return false;
			}
		}
		for (ns, db, name, state) in &self.functions {
			if !function_holds(txn, *ns, *db, name, state.as_ref()).await {
				return false;
			}
		}
		for (ns, db, name, state) in &self.params {
			match param_state(txn, *ns, *db, name).await {
				Some(now) if now.as_deref() == state.as_deref() => {}
				_ => return false,
			}
		}
		for (ns, db, state) in &self.databases {
			match txn.get_db_by_name(ns, db, None).await {
				Ok(now) if now.as_deref() == state.as_deref() => {}
				_ => return false,
			}
		}
		for (ns, db, state) in &self.listings {
			// A transaction under the epoch the state was read under reads
			// it so, with no read; any other reads it, and where it finds it
			// so under an epoch, the state is that epoch's.
			if txn.catalog_epoch().is_some_and(|epoch| state.seen.is(epoch)) {
				// What a compilation holding this one compiles depends on the
				// listing as much as if it had read it.
				txn.note_listing_read(*ns, *db);
				continue;
			}
			if database_state(txn, *ns, *db).await.as_ref() != Some(state) {
				return false;
			}
			if let Some(epoch) = txn.catalog_epoch() {
				state.seen.set(epoch);
			}
		}
		for (ns, db, target, state) in &self.references {
			// What a compilation holding this one compiles depends on the
			// same fields.
			txn.note_references_read(*ns, *db, target.as_ref());
			if txn.catalog_epoch().is_some_and(|epoch| state.seen.is(epoch)) {
				continue;
			}
			let before =
				Arc::clone(&state.read.lock().unwrap_or_else(std::sync::PoisonError::into_inner));
			let Ok(read) = txn.reference_fields(*ns, *db, Some(&before)).await else {
				return false;
			};
			// Fields found again from the same reads of every table are the
			// same fields.
			let same = before.tables.len() == read.tables.len()
				&& before
					.tables
					.iter()
					.zip(&read.tables)
					.all(|((a, x), (b, y))| a == b && Arc::ptr_eq(x, y));
			if !same && references_to(&read, target.as_ref()) != state.fields {
				return false;
			}
			*state.read.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = read;
			if let Some(epoch) = txn.catalog_epoch() {
				state.seen.set(epoch);
			}
		}
		true
	}
}

/// The state of every table `ns`/`db` defines, as `txn` reads it: two
/// scans, and the definition of each table no catalog write has stamped.
async fn database_state(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
) -> Option<DatabaseState> {
	let catalog = txn.database_catalog_state(ns, db).await.ok()?;
	let stamped: std::collections::HashSet<&TableName> =
		catalog.versions.iter().map(|(table, _)| table).collect();
	let mut unstamped = Vec::new();
	for table in catalog.tables.iter().filter(|table| !stamped.contains(table)) {
		unstamped.push((table.clone(), table_state(txn, ns, db, table).await?));
	}
	Some(DatabaseState {
		catalog,
		unstamped,
		seen: SeenAt::unseen(),
	})
}

async fn table_state(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	tb: &TableName,
) -> Option<TableState> {
	if let Some(version) = txn.catalog_version(ns, db, tb).await.ok()? {
		return Some(TableState::Version(version));
	}
	Some(match txn.get_tb(ns, db, tb, None).await.ok()? {
		Some(def) => {
			let mut hasher = DefaultHasher::new();
			def.hash(&mut hasher);
			TableState::Content(hasher.finish())
		}
		None => TableState::Absent,
	})
}

/// Whether `fn::name` is, as `txn` reads it, as `state` records it: the same
/// definition, told by the allocation the transaction cache hands out for as
/// long as the definition stands, or else by its content hash.
async fn function_holds(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	name: &str,
	state: Option<&FunctionState>,
) -> bool {
	match (txn.get_db_function(ns, db, name, None).await, state) {
		(Ok(def), Some(state)) => {
			Arc::ptr_eq(&def, &state.definition) || FunctionSnapshot::hash_of(&def) == state.hash
		}
		(Err(e), None) => matches!(
			e.downcast_ref::<crate::catalog::Error>(),
			Some(crate::catalog::Error::FcNotFound { .. })
		),
		_ => false,
	}
}

/// The definition of `fn::name`, `Some(None)` when it is not defined.
async fn function_state(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	name: &str,
) -> Option<Option<FunctionState>> {
	match txn.get_db_function(ns, db, name, None).await {
		Ok(def) => Some(Some(FunctionState {
			hash: FunctionSnapshot::hash_of(&def),
			definition: def,
		})),
		Err(e)
			if matches!(
				e.downcast_ref::<crate::catalog::Error>(),
				Some(crate::catalog::Error::FcNotFound { .. })
			) =>
		{
			Some(None)
		}
		Err(_) => None,
	}
}

/// The definition of `$name`, `Some(None)` when it is not defined.
async fn param_state(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	name: &str,
) -> Option<Option<Arc<ParamDefinition>>> {
	match txn.get_db_param(ns, db, name, None).await {
		Ok(def) => Some(Some(def)),
		Err(e)
			if matches!(
				e.downcast_ref::<crate::catalog::Error>(),
				Some(crate::catalog::Error::PaNotFound { .. })
			) =>
		{
			Some(None)
		}
		Err(_) => None,
	}
}
