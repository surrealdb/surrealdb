//! Table-level document-ID (surrogate) space shared by all indexes on a table.
//!
//! Every index family used to own a *private* compact-ID space: full-text search
//! allocated per-index `DocId`s, HNSW and DiskANN each kept their own allocator
//! plus a recycle pool. The same record therefore had a different `u64` identity
//! in every index, which makes cross-index candidate composition impossible
//! without falling back to full [`RecordId`](crate::val::RecordId) comparison.
//!
//! [`TableDocIds`] replaces those private spaces with **one monotonic `u64`
//! space per table**, shared by every index on that table. It maintains the
//! table-scoped mappings
//!
//! - `/*{ns}*{db}*{tb}!dj{record_id}` → [`DocId`], the forward mapping, keyed by the record id's
//!   lossless encoding,
//! - `/*{ns}*{db}*{tb}!di{record_id}` → [`DocId`] (see [`crate::key::table::di`]), the same forward
//!   mapping keyed under the index format, which every id with the same decimal expansion shares;
//!   kept for nodes on the previous release, and trusted only once the reverse mapping confirms it,
//! - `/*{ns}*{db}*{tb}!dd{doc_id}` → [`RecordIdKey`] (see [`crate::key::table::dd`]),
//!
//! and allocates fresh ids through the distributed-sequence batching engine
//! ([`surrealdb_datastore::sequences`], the `TableDocIds` domain).
//!
//! # Invariants
//!
//! - **One id per record, shared across indexes.** [`resolve_or_assign`] is idempotent: the first
//!   index to touch a record assigns the id and commits the mapping; every other index (and any
//!   later compaction) reads the same committed id. Assignment is therefore order-independent
//!   across indexes.
//! - **Monotonic, never recycled.** [`remove`] deletes the mapping but never returns the id to a
//!   pool. A delete followed by a re-create of the same record key yields a *new* id, which keeps
//!   bitmap/cache coherence intact (stale cache entries are rejected by generation checks). `u64`
//!   does not exhaust in practice.
//! - **Removal is centralised.** Because the space is shared, no single index may drop a record's
//!   mapping — a sibling index (or a pending vector compaction) might still reference the id. The
//!   mapping is removed once, at record purge, after every index has released the record.
//!
//! [`resolve_or_assign`]: TableDocIds::resolve_or_assign
//! [`remove`]: TableDocIds::remove
//! [`RecordIdKey`]: crate::val::RecordIdKey

use std::borrow::{Borrow, Cow};

use anyhow::Result;
use surrealdb_datastore::values::diskann::DiskAnnDocsState as DiskAnnDocState;
use surrealdb_datastore::values::hnsw::HnswDocsState as HnswDocState;
// A doc-ID is a stored value and a key field, so the number is declared below
// this layer; the allocator that hands them out stays here.
pub use surrealdb_datastore::values::ids::DocId;
use surrealdb_datastore::{Transaction, storage_error};
use surrealdb_kvs::Error as KvsError;
use surrealdb_kvs::key::KVKey;

use crate::catalog::{DatabaseId, NamespaceId};
use crate::env::IndexEnv;
use crate::key::schema::{
	DiskannDocKeyKey, DiskannDocLookupKey, DocKeyKey, DocKeyPrefix, DocLookupIdentityKey,
	DocLookupIdentityPrefix, DocLookupKey, DocLookupPrefix, DocPendingPrefix, FtDocKeyKey,
	FtDocLookupKey, HnswDocKeyKey, HnswDocLookupKey, HnswDocsStateKey,
};
use crate::key::{KVSubspace, KVValue, Key};
use crate::val::{RecordIdKey, RecordIdentity, TableName};

/// The table-level document-ID allocator and record ↔ doc-ID mapping store.
///
/// Cheap to construct (holds only the table scope); all state lives in the
/// key-value store and the distributed sequence, so instances are disposable and
/// carry no in-memory allocation cursor.
pub struct TableDocIds {
	ns: NamespaceId,
	db: DatabaseId,
	tb: TableName,
}

/// A full-text index's private document-ID allocator and record ↔ doc-ID mapping.
///
/// This is the layout written by releases before the shared table doc-ID format.
/// Existing indexes keep using it so a 3.3 node can read and maintain data that
/// was created by 3.2; only new indexes or explicit rebuilds use [`TableDocIds`].
pub struct LegacyFullTextDocIds {
	ns: NamespaceId,
	db: DatabaseId,
	tb: TableName,
	ix: surrealdb_catalog::IndexId,
}

/// A vector index's private document-ID allocator and record ↔ doc-ID mapping.
pub enum LegacyVectorDocIds {
	Hnsw(crate::IndexKeyBase),
	DiskAnn(crate::IndexKeyBase),
}

enum LegacyVectorFamily {
	Hnsw,
	DiskAnn,
}

#[derive(Default)]
struct LegacyVectorDocState {
	available: roaring::RoaringTreemap,
	next_doc_id: DocId,
}

impl From<HnswDocState> for LegacyVectorDocState {
	fn from(state: HnswDocState) -> Self {
		Self {
			available: state.available,
			next_doc_id: state.next_doc_id,
		}
	}
}

impl From<DiskAnnDocState> for LegacyVectorDocState {
	fn from(state: DiskAnnDocState) -> Self {
		Self {
			available: state.available,
			next_doc_id: state.next_doc_id,
		}
	}
}

impl LegacyVectorFamily {
	fn docs_state_key<'a>(
		self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: Cow<'a, TableName>,
		ix: surrealdb_catalog::IndexId,
	) -> LegacyDocsStateKey<'a> {
		match self {
			Self::Hnsw => LegacyDocsStateKey::Hnsw(HnswDocsStateKey::new(ns, db, tb, ix)),
			Self::DiskAnn => LegacyDocsStateKey::DiskAnn(
				crate::key::schema::DiskannDocsStateKey::new(ns, db, tb, ix),
			),
		}
	}
}

#[derive(Debug)]
enum LegacyDocsStateKey<'a> {
	Hnsw(HnswDocsStateKey<'a>),
	DiskAnn(crate::key::schema::DiskannDocsStateKey<'a>),
}

impl KVKey for LegacyDocsStateKey<'_> {
	type Value = HnswDocState;

	fn encode_buffer(&self, buffer: &mut Vec<u8>) -> Result<()> {
		match self {
			Self::Hnsw(key) => key.encode_buffer(buffer),
			Self::DiskAnn(key) => key.encode_buffer(buffer),
		}
	}

	fn value_context(&self) {}
}

impl LegacyVectorDocIds {
	fn family(&self) -> LegacyVectorFamily {
		match self {
			Self::Hnsw(_) => LegacyVectorFamily::Hnsw,
			Self::DiskAnn(_) => LegacyVectorFamily::DiskAnn,
		}
	}

	fn docs_state_key(&self) -> LegacyDocsStateKey<'_> {
		let ikb = match self {
			Self::Hnsw(ikb) | Self::DiskAnn(ikb) => ikb,
		};
		self.family().docs_state_key(ikb.ns(), ikb.db(), Cow::Borrowed(ikb.table()), ikb.index())
	}

	async fn docs_state(&self, tx: &Transaction) -> Result<LegacyVectorDocState> {
		match self.docs_state_key() {
			LegacyDocsStateKey::Hnsw(key) => {
				Ok(tx.get_key(&key, None).await?.unwrap_or_default().into())
			}
			LegacyDocsStateKey::DiskAnn(key) => {
				Ok(tx.get_key(&key, None).await?.unwrap_or_default().into())
			}
		}
	}

	async fn set_docs_state(&self, tx: &Transaction, state: LegacyVectorDocState) -> Result<()> {
		match self.docs_state_key() {
			LegacyDocsStateKey::Hnsw(key) => {
				let concrete = HnswDocState {
					available: state.available,
					next_doc_id: state.next_doc_id,
				};
				tx.set_key(&key, &concrete).await
			}
			LegacyDocsStateKey::DiskAnn(key) => {
				let concrete = DiskAnnDocState {
					available: state.available,
					next_doc_id: state.next_doc_id,
				};
				tx.set_key(&key, &concrete).await
			}
		}
	}

	pub async fn get_doc_id(&self, tx: &Transaction, id: &RecordIdKey) -> Result<Option<DocId>> {
		let ikb = match self {
			Self::Hnsw(ikb) | Self::DiskAnn(ikb) => ikb,
		};
		let key = self.family().lookup_key(
			ikb.ns(),
			ikb.db(),
			Cow::Borrowed(ikb.table()),
			ikb.index(),
			Cow::Borrowed(id),
		);
		tx.get_key(&key, None).await
	}

	pub async fn resolve_or_assign(&self, env: &dyn IndexEnv, id: &RecordIdKey) -> Result<DocId> {
		if let Some(doc_id) = self.get_doc_id(&env.tx(), id).await? {
			return Ok(doc_id);
		}
		let mut state = self.docs_state(&env.tx()).await?;
		let doc_id = if let Some(doc_id) = state.available.min() {
			state.available.remove(doc_id);
			doc_id
		} else {
			state.next_doc_id += 1;
			state.next_doc_id - 1
		};
		self.set_docs_state(&env.tx(), state).await?;
		let ikb = match self {
			Self::Hnsw(ikb) | Self::DiskAnn(ikb) => ikb,
		};
		let lookup = self.family().lookup_key(
			ikb.ns(),
			ikb.db(),
			Cow::Borrowed(ikb.table()),
			ikb.index(),
			Cow::Borrowed(id),
		);
		env.tx().set_key(&lookup, &doc_id).await?;
		let reverse = self.family().doc_key(
			ikb.ns(),
			ikb.db(),
			Cow::Borrowed(ikb.table()),
			ikb.index(),
			doc_id,
		);
		env.tx().set_key(&reverse, id).await?;
		Ok(doc_id)
	}

	pub async fn get_record_ids_batch(
		&self,
		tx: &Transaction,
		doc_ids: &[DocId],
	) -> Result<Vec<Option<RecordIdKey>>> {
		let ikb = match self {
			Self::Hnsw(ikb) | Self::DiskAnn(ikb) => ikb,
		};
		let keys: Vec<LegacyVectorDocKey<'_>> = doc_ids
			.iter()
			.map(|&doc_id| {
				self.family().doc_key(
					ikb.ns(),
					ikb.db(),
					Cow::Borrowed(ikb.table()),
					ikb.index(),
					doc_id,
				)
			})
			.collect();
		tx.get_many_key(keys, None).await
	}

	pub async fn remove(&self, tx: &Transaction, id: &RecordIdKey) -> Result<Option<DocId>> {
		let Some(doc_id) = self.get_doc_id(tx, id).await? else {
			return Ok(None);
		};
		let ikb = match self {
			Self::Hnsw(ikb) | Self::DiskAnn(ikb) => ikb,
		};
		let forward = self.family().lookup_key(
			ikb.ns(),
			ikb.db(),
			Cow::Borrowed(ikb.table()),
			ikb.index(),
			Cow::Borrowed(id),
		);
		tx.del_key(&forward).await?;
		let reverse = self.family().doc_key(
			ikb.ns(),
			ikb.db(),
			Cow::Borrowed(ikb.table()),
			ikb.index(),
			doc_id,
		);
		tx.del_key(&reverse).await?;

		let mut state = self.docs_state(tx).await?;
		state.available.insert(doc_id);
		self.set_docs_state(tx, state).await?;
		Ok(Some(doc_id))
	}

	pub async fn remove_doc_id(
		&self,
		tx: &Transaction,
		doc_id: DocId,
	) -> Result<Option<RecordIdKey>> {
		let ikb = match self {
			Self::Hnsw(ikb) | Self::DiskAnn(ikb) => ikb,
		};
		let reverse = self.family().doc_key(
			ikb.ns(),
			ikb.db(),
			Cow::Borrowed(ikb.table()),
			ikb.index(),
			doc_id,
		);
		let Some(id) = tx.get_key(&reverse, None).await? else {
			return Ok(None);
		};
		let forward = self.family().lookup_key(
			ikb.ns(),
			ikb.db(),
			Cow::Borrowed(ikb.table()),
			ikb.index(),
			Cow::Borrowed(&id),
		);
		tx.del_key(&forward).await?;
		tx.del_key(&reverse).await?;

		let mut state = self.docs_state(tx).await?;
		state.available.insert(doc_id);
		self.set_docs_state(tx, state).await?;
		Ok(Some(id))
	}
}

#[derive(Debug)]
enum LegacyVectorLookup<'a> {
	Hnsw(HnswDocLookupKey<'a>),
	DiskAnn(DiskannDocLookupKey<'a>),
}

#[derive(Debug)]
enum LegacyVectorDocKey<'a> {
	Hnsw(HnswDocKeyKey<'a>),
	DiskAnn(DiskannDocKeyKey<'a>),
}

impl KVKey for LegacyVectorLookup<'_> {
	type Value = DocId;

	fn encode_buffer(&self, buffer: &mut Vec<u8>) -> Result<()> {
		match self {
			Self::Hnsw(key) => key.encode_buffer(buffer),
			Self::DiskAnn(key) => key.encode_buffer(buffer),
		}
	}

	fn value_context(&self) {}
}

impl KVKey for LegacyVectorDocKey<'_> {
	type Value = RecordIdKey;

	fn encode_buffer(&self, buffer: &mut Vec<u8>) -> Result<()> {
		match self {
			Self::Hnsw(key) => key.encode_buffer(buffer),
			Self::DiskAnn(key) => key.encode_buffer(buffer),
		}
	}

	fn value_context(&self) {}
}

impl LegacyVectorFamily {
	fn lookup_key<'a>(
		self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: Cow<'a, TableName>,
		ix: surrealdb_catalog::IndexId,
		id: Cow<'a, RecordIdKey>,
	) -> LegacyVectorLookup<'a> {
		match self {
			Self::Hnsw => LegacyVectorLookup::Hnsw(HnswDocLookupKey::new(ns, db, tb, ix, id)),
			Self::DiskAnn => {
				LegacyVectorLookup::DiskAnn(DiskannDocLookupKey::new(ns, db, tb, ix, id))
			}
		}
	}

	fn doc_key(
		self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: Cow<'_, TableName>,
		ix: surrealdb_catalog::IndexId,
		doc_id: DocId,
	) -> LegacyVectorDocKey<'_> {
		match self {
			Self::Hnsw => LegacyVectorDocKey::Hnsw(HnswDocKeyKey::new(ns, db, tb, ix, doc_id)),
			Self::DiskAnn => {
				LegacyVectorDocKey::DiskAnn(DiskannDocKeyKey::new(ns, db, tb, ix, doc_id))
			}
		}
	}
}

impl LegacyFullTextDocIds {
	pub fn new(
		ns: NamespaceId,
		db: DatabaseId,
		tb: TableName,
		ix: surrealdb_catalog::IndexId,
	) -> Self {
		Self {
			ns,
			db,
			tb,
			ix,
		}
	}

	pub async fn get_doc_id(&self, tx: &Transaction, id: &RecordIdKey) -> Result<Option<DocId>> {
		let key = FtDocLookupKey::new(
			self.ns,
			self.db,
			Cow::Borrowed(&self.tb),
			self.ix,
			Cow::Borrowed(id),
		);
		tx.get_key(&key, None).await
	}

	pub async fn resolve_or_assign(&self, env: &dyn IndexEnv, id: &RecordIdKey) -> Result<DocId> {
		let tx = env.tx();
		let key = FtDocLookupKey::new(
			self.ns,
			self.db,
			Cow::Borrowed(&self.tb),
			self.ix,
			Cow::Borrowed(id),
		);
		if let Some(doc_id) = tx.get_key(&key, None).await? {
			return Ok(doc_id);
		}
		let doc_id = env
			.sequences()?
			.next_legacy_full_text_doc_id(Some(env), self.ns, self.db, self.tb.clone(), self.ix)
			.await?;
		tx.set_key(&key, &doc_id).await?;
		let reverse = FtDocKeyKey::new(self.ns, self.db, Cow::Borrowed(&self.tb), self.ix, doc_id);
		tx.set_key(&reverse, id).await?;
		Ok(doc_id)
	}

	pub async fn get_record_id(
		&self,
		tx: &Transaction,
		doc_id: DocId,
	) -> Result<Option<RecordIdKey>> {
		let key = FtDocKeyKey::new(self.ns, self.db, Cow::Borrowed(&self.tb), self.ix, doc_id);
		tx.get_key(&key, None).await
	}

	pub async fn get_record_ids_batch(
		&self,
		tx: &Transaction,
		doc_ids: &[DocId],
	) -> Result<Vec<Option<RecordIdKey>>> {
		let keys: Vec<FtDocKeyKey<'_>> = doc_ids
			.iter()
			.map(|&doc_id| {
				FtDocKeyKey::new(self.ns, self.db, Cow::Borrowed(&self.tb), self.ix, doc_id)
			})
			.collect();
		tx.get_many_key(keys, None).await
	}

	pub async fn remove_doc_id(&self, tx: &Transaction, doc_id: DocId) -> Result<()> {
		let reverse = FtDocKeyKey::new(self.ns, self.db, Cow::Borrowed(&self.tb), self.ix, doc_id);
		if let Some(id) = tx.get_key(&reverse, None).await? {
			let forward = FtDocLookupKey::new(
				self.ns,
				self.db,
				Cow::Borrowed(&self.tb),
				self.ix,
				Cow::Borrowed(&id),
			);
			tx.del_key(&forward).await?;
			tx.del_key(&reverse).await?;
		}
		Ok(())
	}
}

impl TableDocIds {
	/// Creates a doc-ID store for the given table scope.
	pub fn new(ns: NamespaceId, db: DatabaseId, tb: TableName) -> Self {
		Self {
			ns,
			db,
			tb,
		}
	}

	/// The injective forward key for `id`.
	fn identity_key<'a>(&'a self, id: &'a RecordIdKey) -> DocLookupIdentityKey<'a> {
		DocLookupIdentityKey::new(
			self.ns,
			self.db,
			Cow::Borrowed(&self.tb),
			RecordIdentity(id.clone()),
		)
	}

	/// The type-erased forward key for `id`, whose hit must be verified.
	fn erased_key<'a>(&'a self, id: &'a RecordIdKey) -> DocLookupKey<'a> {
		DocLookupKey::new(self.ns, self.db, Cow::Borrowed(&self.tb), Cow::Borrowed(id))
	}

	/// The record mapped at `spelling`, the bytes the index format encodes a
	/// record id to: the doc-ID the type-erased forward key at those bytes
	/// holds, and the record its reverse mapping names.
	///
	/// For a caller that holds an id only as a key spelled it under the index
	/// format, which drops a nested number's variant, so that decoding it yields
	/// an id naming no record. The bytes are looked up as they are, never decoded
	/// and re-encoded: that round trip turns a float into the nearest decimal,
	/// which need not encode back to the same bytes. Where several records share
	/// the spelling, this is the one holding the key. `None` when no mapping
	/// resolves.
	pub async fn record_spelled_as(
		&self,
		tx: &Transaction,
		spelling: &[u8],
	) -> Result<Option<RecordIdKey>> {
		let mut key = DocLookupPrefix::new(self.ns, self.db, Cow::Borrowed(&self.tb))
			.encode_bound()?
			.to_vec();
		key.extend_from_slice(spelling);
		let Some(raw) = tx.get(Key::from(key), None).await? else {
			return Ok(None);
		};
		let doc_id = DocId::kv_decode_value(&raw, ())?;
		self.get_record_id(tx, doc_id).await
	}

	/// Returns the doc-ID mapped to `id`, or `None` if the record has no id yet.
	///
	/// Reads the injective `!dj` mapping first and falls back to `!di` for a
	/// record mapped before `!dj` existed. **A hit on either is adopted only when
	/// the reverse mapping agrees that the doc-ID names this very record.** The
	/// reverse mapping is the one direction that has always kept a record id
	/// exactly, so it is what decides.
	///
	/// Both directions need the check, for different reasons. `!di` is shared by
	/// every id with the same decimal expansion, so a hit there may belong to a
	/// different record. `!dj` is injective, but a node on the previous release
	/// purges a record by deleting `!di` and `!dd` alone — it has never heard of
	/// `!dj` — so during a rolling upgrade a hit there may name a doc-ID whose
	/// record is gone, or one superseded by a mapping that node has since
	/// written. Adopting either would hand an index a doc-ID that no `!dd`
	/// resolves, which is the failure this key exists to remove.
	///
	/// The `!dj` check costs a second read on the hit path. It can go when the
	/// migration that drops `!di` does: past that point no node writes a doc-ID
	/// mapping this one cannot see.
	pub async fn get_doc_id(&self, tx: &Transaction, id: &RecordIdKey) -> Result<Option<DocId>> {
		Ok(self.resolve_verified(tx, id).await?.map(|(doc_id, _)| doc_id))
	}

	/// [`Self::get_doc_id`], and which direction answered.
	///
	/// The rule itself is `Transaction::resolve_doc_id_mapping`, which lives
	/// beside the three bands it reads. This supplies the table it reads them
	/// for, and reads the current state rather than a snapshot.
	async fn resolve_verified(
		&self,
		tx: &Transaction,
		id: &RecordIdKey,
	) -> Result<Option<(DocId, bool)>> {
		tx.resolve_doc_id_mapping(self.ns, self.db, &self.tb, id, None).await
	}

	/// Whether the reverse mapping for `doc_id` names `id` itself.
	///
	/// Compared on encoded record keys rather than with `==`: a number's equality
	/// is cross-variant, so `[1dec] == [1]` while the two address different
	/// records, and an equality check would accept the very collision this
	/// guards against.
	async fn mapping_names(
		&self,
		tx: &Transaction,
		doc_id: DocId,
		id: &RecordIdKey,
	) -> Result<bool> {
		let Some(mapped) = self.get_record_id(tx, doc_id).await? else {
			return Ok(false);
		};
		Ok(mapped.addresses_same_record(id))
	}

	/// Points the type-erased key for `id` at `doc_id`, so a node still on the
	/// previous release resolves the record, once `doc_id` is decided.
	///
	/// A blind `set`: every id with the same decimal expansion shares the key,
	/// so another record may hold it, and a conditional write would fail for no
	/// reason. Readers on this release verify a hit against `!dd` before
	/// trusting it. The record holding it, though, can be one that resolves
	/// through this key alone: a node on the previous release mapped it, so it
	/// has no `!dj`, or purged and re-created it, so its `!dj` names a doc-ID
	/// whose reverse mapping is gone. Overwriting the key would leave that record
	/// with no mapping on this release, so its injective mapping is published
	/// first, as its own next resolve would publish it, unless one the reverse
	/// mapping confirms is already there. Every caller has read the key already,
	/// so the read here arms no conflict the write did not.
	async fn mirror_erased(&self, tx: &Transaction, id: &RecordIdKey, doc_id: DocId) -> Result<()> {
		let di = self.erased_key(id);
		if let Some(held) = tx.get_key(&di, None).await?
			&& held != doc_id
			&& let Some(owner) = self.get_record_id(tx, held).await?
			&& !owner.addresses_same_record(id)
		{
			let owner_dj = self.identity_key(&owner);
			let confirmed = match tx.get_key(&owner_dj, None).await? {
				Some(claimed) => self.mapping_names(tx, claimed, &owner).await?,
				None => false,
			};
			if !confirmed {
				tx.set_key(&owner_dj, &held).await?;
			}
		}
		tx.set_key(&di, &doc_id).await
	}

	/// Returns the existing doc-ID for `id`, or allocates, persists, and returns a
	/// new one.
	///
	/// Idempotent: concurrent callers on different indexes converge on the same id
	/// because the forward mappings are read first and only a missing mapping
	/// triggers a sequence allocation. The new id is drawn from the table-scoped
	/// distributed sequence (batched via `config.idx.table_doc_ids_batch_size`) and
	/// the forward (`!dj`, mirrored onto `!di`) and reverse (`!dd`) mappings are
	/// written.
	///
	/// The injective forward mapping `!dj` is claimed with a conditional create
	/// (`putc`) rather than a blind `set`. When two doc-ID-consuming indexes first
	/// touch the same record concurrently (e.g. concurrent `DEFINE`/`REBUILD` of
	/// full-text / HNSW / DiskANN), each observes no mapping and allocates a
	/// *different* id from the shared sequence. On last-writer-wins backends
	/// (TiKV) a blind `set` would let the later writer overwrite the forward
	/// mapping — leaving one record with two doc-IDs and breaking the cross-index
	/// identity scans rely on. `putc` reads `!dj` before writing, which arms the
	/// write-conflict check, so only one writer wins; the loser's commit is
	/// rejected and its caller retries, at which point the fast path returns the
	/// committed id (making the operation idempotent). The abandoned speculative
	/// id is never reused, which is fine — the space is monotonic and `u64` does
	/// not exhaust (see the module invariants). The `!di` mirror is written blind
	/// once the claim has decided the id; see [`Self::mirror_erased`].
	pub async fn resolve_or_assign(&self, env: &dyn IndexEnv, id: &RecordIdKey) -> Result<DocId> {
		let tx = env.tx();
		let dj = self.identity_key(id);
		// Fast path: the record already has an id in this table's space, under
		// the injective mapping or — for a record mapped before it existed — the
		// verified type-erased one.
		if let Some((doc_id, from_identity)) = self.resolve_verified(&tx, id).await? {
			if !from_identity {
				// Only the type-erased key had it: a node on the previous release
				// mapped this record, or it was created after the migration ran.
				// Publish the injective mapping now, so the record does not go on
				// depending on a band that is to be dropped once no supported
				// upgrade path starts below this release. Written once — the next
				// resolve answers from `!dj` and takes the branch above.
				tx.set_key(&dj, &doc_id).await?;
			}
			return Ok(doc_id);
		}
		// Allocate a fresh, monotonic id from the table-scoped sequence.
		let doc_id = env
			.sequences()?
			.next_table_doc_id(
				Some(env),
				self.ns,
				self.db,
				self.tb.clone(),
				env.config().table_doc_ids_batch_size,
			)
			.await?;
		// Claim the forward mapping on the injective key. `putc` with no expected
		// value only writes if `!dj` is still absent, and reading it arms the
		// write-conflict check. The claim is made here rather than on `!di`
		// because that key is shared by every id with the same decimal expansion:
		// claiming it would make one such record's arrival deny the others an id
		// of their own.
		match tx.put_key(&dj, &doc_id).await {
			Ok(()) => {
				// We won the race: publish the reverse mapping too. Written only
				// after the forward claim succeeds, so a loser leaves no dangling
				// `!dd` entry to clean up.
				let dd = DocKeyKey::new(self.ns, self.db, Cow::Borrowed(&self.tb), doc_id);
				tx.set_key(&dd, id).await?;
				self.mirror_erased(&tx, id, doc_id).await?;
				Ok(doc_id)
			}
			// Another index published the mapping first and it is already visible
			// in our snapshot: adopt the committed id and drop our speculative
			// one. (Under snapshot isolation the primary defence is the
			// commit-time conflict above plus the caller's retry; this branch
			// covers the mapping becoming visible between our two reads.)
			Err(e) if matches!(storage_error(&e), Some(KvsError::TransactionKeyAlreadyExists)) => {
				let claimed = tx.get_key(&dj, None).await?.ok_or_else(|| {
					anyhow::anyhow!("doc-ID mapping missing after a conditional-create conflict")
				})?;
				if self.mapping_names(&tx, claimed, id).await? {
					// A sibling index resolved the same record first; its id is
					// the one to share.
					return Ok(claimed);
				}
				// The claim is stale: a node on the previous release purged this
				// record by deleting `!di` and `!dd` alone, leaving `!dj` behind,
				// and the reverse mapping no longer names the record. Adopting it
				// would hand every index a doc-ID that resolves to nothing, so the
				// speculative id replaces it instead.
				tx.set_key(&dj, &doc_id).await?;
				let dd = DocKeyKey::new(self.ns, self.db, Cow::Borrowed(&self.tb), doc_id);
				tx.set_key(&dd, id).await?;
				self.mirror_erased(&tx, id, doc_id).await?;
				Ok(doc_id)
			}
			Err(e) => Err(e),
		}
	}

	/// Returns the record key mapped to `doc_id`, or `None` if unmapped.
	pub async fn get_record_id(
		&self,
		tx: &Transaction,
		doc_id: DocId,
	) -> Result<Option<RecordIdKey>> {
		let key = DocKeyKey::new(self.ns, self.db, Cow::Borrowed(&self.tb), doc_id);
		tx.get_key(&key, None).await
	}

	/// Resolves many doc-IDs to their record keys in one batched read, preserving
	/// input order. Unmapped ids yield `None` in their slot.
	pub async fn get_record_ids_batch(
		&self,
		tx: &Transaction,
		doc_ids: &[DocId],
	) -> Result<Vec<Option<RecordIdKey>>> {
		self.get_record_ids_batch_at(tx, doc_ids, None).await
	}

	/// [`Self::get_record_ids_batch`] as of `version`. Mappings are removed
	/// when their record is deleted, so a time-travel read must resolve
	/// through the mapping state its snapshot saw, not the current one.
	pub async fn get_record_ids_batch_at(
		&self,
		tx: &Transaction,
		doc_ids: &[DocId],
		version: Option<u64>,
	) -> Result<Vec<Option<RecordIdKey>>> {
		let keys: Vec<DocKeyKey<'_>> = doc_ids
			.iter()
			.map(|&doc_id| DocKeyKey::new(self.ns, self.db, Cow::Borrowed(&self.tb), doc_id))
			.collect();
		tx.get_many_key(keys, version).await
	}

	/// Resolves many record keys to their doc-IDs in one batched read,
	/// preserving input order. Records with no id in this table's space yield
	/// `None` in their slot. The mirror of [`Self::get_record_ids_batch`] for
	/// the forward (`!dj`) direction.
	///
	/// Reaches the same verdict as [`Self::get_doc_id`], for the same reasons:
	/// a hit is adopted only when the reverse mapping agrees it names the
	/// record asked for. The check is batched, so verifying costs one read for
	/// the whole slice rather than one per hit.
	pub async fn get_doc_ids_batch(
		&self,
		tx: &Transaction,
		ids: &[RecordIdKey],
	) -> Result<Vec<Option<DocId>>> {
		// Both directions are read in batches, and so is the verification of
		// each: four round trips for the whole slice, whatever it holds. The
		// unresolved slots are not a rare tail to mop up one at a time — the
		// adjacency fold asks this about records it has not mapped yet, and
		// resolves every `None` itself.
		let identity: Vec<DocLookupIdentityKey<'_>> =
			ids.iter().map(|id| self.identity_key(id)).collect();
		let hits = tx.get_many_key(identity, None).await?;
		let mut found = self.verify_batch(tx, ids, hits).await?;

		let missing: Vec<&RecordIdKey> =
			found.iter().zip(ids).filter_map(|(slot, id)| slot.is_none().then_some(id)).collect();
		if missing.is_empty() {
			return Ok(found);
		}

		// The type-erased key answers for a record mapped before the injective
		// one existed. Its hits need the same check, and for a second reason:
		// the key is shared by every id with the same decimal expansion, so a
		// hit there may belong to another record entirely.
		let erased: Vec<DocLookupKey<'_>> = missing.iter().map(|id| self.erased_key(id)).collect();
		let fallback = tx.get_many_key(erased, None).await?;
		let mut fallback = self.verify_batch(tx, &missing, fallback).await?.into_iter();
		for slot in found.iter_mut() {
			if slot.is_none() {
				*slot = fallback.next().flatten();
			}
		}
		Ok(found)
	}

	/// Keeps each hit only if the reverse mapping agrees it names the record
	/// asked for, reading every reverse mapping in one batch.
	///
	/// A forward hit is never authoritative on its own. The type-erased key is
	/// shared by every id with the same decimal expansion; the injective one is
	/// not, but a node on the previous release purges a record by deleting `!di`
	/// and `!dd` alone, so a claim can outlive the record it named or the
	/// doc-ID's reassignment to another.
	async fn verify_batch(
		&self,
		tx: &Transaction,
		ids: &[impl Borrow<RecordIdKey>],
		mut found: Vec<Option<DocId>>,
	) -> Result<Vec<Option<DocId>>> {
		let claimed: Vec<DocId> = found.iter().flatten().copied().collect();
		if claimed.is_empty() {
			return Ok(found);
		}
		let mut named = self.get_record_ids_batch(tx, &claimed).await?.into_iter();
		for (slot, id) in found.iter_mut().zip(ids) {
			if slot.is_none() {
				continue;
			}
			// `named` advances in step with the slots that filled `claimed`.
			let agrees =
				named.next().flatten().is_some_and(|m| m.addresses_same_record(id.borrow()));
			if !agrees {
				*slot = None;
			}
		}
		Ok(found)
	}

	/// Removes both directions of the mapping for `id`. The id is *not* recycled.
	///
	/// Must only be called once a record is being purged and every index has
	/// released it (see the module invariants).
	///
	/// Returns the doc-ID the record was mapped to, which the deferred-reclaim
	/// sweep uses to restore the mapping of a record re-created concurrently with
	/// it.
	///
	/// `None` means nothing was removed: no forward key resolved to a doc-ID
	/// whose reverse mapping names `id`. Either the record has no mapping, or
	/// the only hit is the type-erased key held by another record with the same
	/// decimal expansion — declined rather than followed, since following it
	/// would delete that record's reverse mapping — or the only hit is an
	/// injective claim whose reverse mapping is gone. The last is left by a
	/// purge on the previous release, which deletes the erased forward key and
	/// the reverse key but not the injective one. That stranded claim is left in
	/// place: it is bounded — at most one per record id — invisible to readers,
	/// every one of which verifies, and replaced when the id is used again,
	/// where [`resolve_or_assign`](Self::resolve_or_assign) finds its claim
	/// taken by a mapping that no longer names the record and overwrites it.
	pub async fn remove(&self, tx: &Transaction, id: &RecordIdKey) -> Result<Option<DocId>> {
		let Some(doc_id) = self.get_doc_id(tx, id).await? else {
			return Ok(None);
		};
		tx.del_key(&self.identity_key(id)).await?;
		let dd = DocKeyKey::new(self.ns, self.db, Cow::Borrowed(&self.tb), doc_id);
		tx.del_key(&dd).await?;
		// The type-erased key may be owned by a different record with the same
		// decimal expansion, whose mapping must outlive this one; it is this
		// record's to delete only when the reverse mapping named this record.
		//
		// When a colliding record *does* survive, it is left without an erased
		// mapping, and a node on the previous release — which resolves through
		// this key alone — mints it a second doc-ID. That is accepted for as
		// long as a cluster runs both releases at once. Keeping the key
		// instead is worse: it would then name an id whose reverse mapping is
		// gone, and an older node re-creating any id that spells it adopts that
		// dead id and writes nothing, so every later hit on the record is
		// dropped after taking a top-k slot — the defect this release ends,
		// reintroduced on a far commoner shape than the one it would guard.
		let di = self.erased_key(id);
		if tx.get_key(&di, None).await? == Some(doc_id) {
			tx.del_key(&di).await?;
		}
		Ok(Some(doc_id))
	}

	/// Restores the mapping `id ↔ doc_id`, unless the record has already been
	/// handed a fresh id.
	///
	/// Used by the deferred-reclaim sweep to repair the narrow race where a
	/// record is re-created (adopting its still-live id via the
	/// [`resolve_or_assign`](Self::resolve_or_assign) fast path) concurrently
	/// with the sweep reclaiming that id: the re-created record's index entries
	/// reference `doc_id`, so the mapping must be put back. The forward mapping
	/// is written with a put-if-absent claim, so if the record was instead
	/// re-indexed with a freshly allocated id after the reclaim, the restore
	/// yields to it and returns `false`.
	pub async fn restore(&self, tx: &Transaction, id: &RecordIdKey, doc_id: DocId) -> Result<bool> {
		// A node on the previous release re-creates a record by publishing `!di`
		// and `!dd` alone — it has never heard of `!dj`. Its index entries
		// already reference the id it minted, so claiming the injective key,
		// which it never wrote, would succeed and bury a live mapping under the
		// reclaimed one. A mapping the reverse direction confirms wins here.
		if let Some(current) = tx.get_key(&self.erased_key(id), None).await?
			&& current != doc_id
			&& self.mapping_names(tx, current, id).await?
		{
			return Ok(false);
		}
		match tx.put_key(&self.identity_key(id), &doc_id).await {
			Ok(()) => {
				let dd = DocKeyKey::new(self.ns, self.db, Cow::Borrowed(&self.tb), doc_id);
				tx.set_key(&dd, id).await?;
				self.mirror_erased(tx, id, doc_id).await?;
				Ok(true)
			}
			// The record already re-acquired a (fresh) id: leave it in place.
			Err(e) if matches!(storage_error(&e), Some(KvsError::TransactionKeyAlreadyExists)) => {
				Ok(false)
			}
			Err(e) => Err(e),
		}
	}

	/// Removes **all** doc-ID state for the table (`!dj`/`!di`/`!dd` mappings and
	/// `!dp` pending-reclaim markers) in one prefix delete per kind, reclaiming the
	/// shared space.
	///
	/// The write set is proportional to the table, so this is only for a caller
	/// that needs the space gone before its own transaction commits — a space a
	/// background reclaim has already torn, whose surviving mappings are no
	/// longer internally consistent and so cannot be adopted by a new index. A
	/// last-consumer drop instead enqueues the space for background reclaim
	/// (`Transaction::del_tb_doc_ids_deferred`), which is bounded.
	///
	/// The monotonic sequence counter is deliberately left intact, so a doc-ID
	/// index defined later keeps allocating fresh, never-reused ids.
	pub async fn remove_all(&self, tx: &Transaction) -> Result<()> {
		tx.delr(DocLookupIdentityPrefix::new(self.ns, self.db, Cow::Borrowed(&self.tb)).range()?)
			.await?;
		tx.delr(DocLookupPrefix::new(self.ns, self.db, Cow::Borrowed(&self.tb)).range()?).await?;
		tx.delr(DocKeyPrefix::new(self.ns, self.db, Cow::Borrowed(&self.tb)).range()?).await?;
		tx.delr(DocPendingPrefix::new(self.ns, self.db, Cow::Borrowed(&self.tb)).range()?).await?;
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use surrealdb_kvs::TransactionType;
	use surrealdb_kvs::TransactionType::{Read, Write};

	use super::*;
	use crate::IndexKeyBase;
	use crate::test_env::{TestIndexEnv, TestIndexStore};

	const NS: NamespaceId = NamespaceId(1);
	const DB: DatabaseId = DatabaseId(1);

	fn rid(s: &str) -> RecordIdKey {
		RecordIdKey::from(s.to_owned())
	}

	#[tokio::test]
	async fn legacy_full_text_uses_private_mappings() {
		let ds = TestIndexStore::new().await;
		let ix = surrealdb_catalog::IndexId(7);
		{
			let ctx = ds.env(Write).await;
			let d = LegacyFullTextDocIds::new(NS, DB, "t".into(), ix);
			assert_eq!(d.resolve_or_assign(&ctx, &rid("a")).await.unwrap(), 0);
			assert_eq!(d.resolve_or_assign(&ctx, &rid("b")).await.unwrap(), 1);
			finish(ctx).await;
		}

		// Existing private mappings remain readable.
		let ctx = ds.env(Read).await;
		let d = LegacyFullTextDocIds::new(NS, DB, "t".into(), ix);
		assert_eq!(d.get_doc_id(&ctx.tx(), &rid("a")).await.unwrap(), Some(0));
		assert_eq!(d.get_record_id(&ctx.tx(), 1).await.unwrap(), Some(rid("b")));
		assert_eq!(
			d.get_record_ids_batch(&ctx.tx(), &[0, 1]).await.unwrap(),
			vec![Some(rid("a")), Some(rid("b"))]
		);

		// The shared table space must be untouched by legacy writes.
		let shared = TableDocIds::new(NS, DB, "t".into());
		assert_eq!(shared.get_doc_id(&ctx.tx(), &rid("a")).await.unwrap(), None);
	}

	#[tokio::test]
	async fn legacy_vector_allocates_private_space() {
		let ds = TestIndexStore::new().await;
		let ikb = IndexKeyBase::new(NS, DB, "t".into(), surrealdb_catalog::IndexId(8));
		{
			let ctx = ds.env(Write).await;
			LegacyVectorDocIds::Hnsw(ikb.clone()).resolve_or_assign(&ctx, &rid("a")).await.unwrap();
			finish(ctx).await;
		}

		let ctx = ds.env(Write).await;
		let legacy = LegacyVectorDocIds::Hnsw(ikb.clone());
		assert_eq!(legacy.resolve_or_assign(&ctx, &rid("b")).await.unwrap(), 1);
		finish(ctx).await;

		let ctx = ds.env(Read).await;
		assert_eq!(
			legacy.get_record_ids_batch(&ctx.tx(), &[0, 1]).await.unwrap(),
			vec![Some(rid("a")), Some(rid("b"))]
		);
		let shared = TableDocIds::new(NS, DB, "t".into());
		assert_eq!(shared.get_doc_id(&ctx.tx(), &rid("a")).await.unwrap(), None);
	}

	/// Builds an environment (with sequences) plus a `TableDocIds` for table
	/// `t`. `idx` distinguishes independent per-index handles onto the *same*
	/// table-level space.
	async fn new_op(ds: &TestIndexStore, tt: TransactionType) -> (TestIndexEnv, TableDocIds) {
		let ctx = ds.env(tt).await;
		let d = TableDocIds::new(NS, DB, "t".into());
		(ctx, d)
	}

	async fn finish(ctx: TestIndexEnv) {
		ctx.tx().commit().await.unwrap();
	}

	/// A record id holding a number and one holding the same number in another
	/// variant are two records, and must not share a doc-ID.
	///
	/// The type-erased forward key cannot tell them apart, so the injective one
	/// is what keeps them separate. Asserted on encoded record keys rather than
	/// with `==`, which is cross-variant for numbers and would accept the very
	/// collision this guards.
	#[tokio::test]
	async fn ids_differing_only_in_numeric_variant_keep_distinct_doc_ids() {
		use crate::val::{Number, Value};
		let arr = |n: Number| RecordIdKey::Array(vec![Value::Number(n)].into());
		let int = arr(Number::Int(1));
		let dec = arr(Number::Decimal(rust_decimal::Decimal::from(1)));

		let ds = TestIndexStore::new().await;
		let d = TableDocIds::new(NS, DB, "t".into());
		let (a, b) = {
			let ctx = ds.env(Write).await;
			let a = d.resolve_or_assign(&ctx, &int).await.unwrap();
			let b = d.resolve_or_assign(&ctx, &dec).await.unwrap();
			finish(ctx).await;
			(a, b)
		};
		assert_ne!(a, b, "two records must not share a doc-ID");

		let ctx = ds.env(Read).await;
		let tx = ctx.tx();
		assert_eq!(d.get_doc_id(&tx, &int).await.unwrap(), Some(a));
		assert_eq!(d.get_doc_id(&tx, &dec).await.unwrap(), Some(b));
		let encoded = |k: &RecordIdKey| storekey::encode_vec(k).unwrap();
		assert_eq!(
			encoded(&d.get_record_id(&tx, a).await.unwrap().unwrap()),
			encoded(&int),
			"the reverse mapping must name the record that owns the doc-ID"
		);
		assert_eq!(encoded(&d.get_record_id(&tx, b).await.unwrap().unwrap()), encoded(&dec));
	}

	/// A node on the previous release purges a record by deleting `!di` and
	/// `!dd` alone — it has never heard of `!dj` — so during a rolling upgrade
	/// the injective key can outlive the record it named.
	///
	/// Such a claim must not be read back as a mapping, and must not be adopted
	/// when the record is resolved again: both would hand every index a doc-ID
	/// that no reverse mapping resolves, which is the failure `!dj` exists to
	/// remove.
	#[tokio::test]
	async fn a_claim_the_reverse_mapping_no_longer_names_is_replaced() {
		let ds = TestIndexStore::new().await;
		let d = TableDocIds::new(NS, DB, "t".into());
		let id = rid("purged-by-an-older-node");

		let original = {
			let ctx = ds.env(Write).await;
			let doc_id = d.resolve_or_assign(&ctx, &id).await.unwrap();
			finish(ctx).await;
			doc_id
		};
		// Exactly what such a purge leaves behind: `!di` and `!dd` gone, `!dj` not.
		{
			let ctx = ds.env(Write).await;
			let tx = ctx.tx();
			tx.del_key(&d.erased_key(&id)).await.unwrap();
			tx.del_key(&DocKeyKey::new(NS, DB, Cow::Borrowed(&d.tb), original)).await.unwrap();
			finish(ctx).await;
		}

		{
			let ctx = ds.env(Read).await;
			assert_eq!(
				d.get_doc_id(&ctx.tx(), &id).await.unwrap(),
				None,
				"an orphaned claim is not a mapping"
			);
			ctx.tx().cancel().await.unwrap();
		}

		let fresh = {
			let ctx = ds.env(Write).await;
			let doc_id = d.resolve_or_assign(&ctx, &id).await.unwrap();
			finish(ctx).await;
			doc_id
		};
		assert_ne!(fresh, original, "the record must not adopt the id its purge left behind");

		let ctx = ds.env(Read).await;
		let tx = ctx.tx();
		assert_eq!(d.get_doc_id(&tx, &id).await.unwrap(), Some(fresh));
		assert_eq!(d.get_record_id(&tx, fresh).await.unwrap(), Some(id));
		tx.cancel().await.unwrap();
	}

	/// A mapping left under the type-erased key by an older release is adopted
	/// only by the record the reverse mapping names.
	///
	/// Every id with the same decimal expansion shares that key, so a hit on it
	/// says nothing on its own. A record that did not own the mapping must be
	/// handed an id of its own rather than inherit one.
	#[tokio::test]
	async fn a_type_erased_mapping_is_adopted_only_by_the_record_it_names() {
		use crate::val::{Number, Value};
		let arr = |n: Number| RecordIdKey::Array(vec![Value::Number(n)].into());
		let int = arr(Number::Int(1));
		let dec = arr(Number::Decimal(rust_decimal::Decimal::from(1)));

		let ds = TestIndexStore::new().await;
		let d = TableDocIds::new(NS, DB, "t".into());
		// Exactly what a store written before the injective key looks like: the
		// erased forward mapping and the reverse one, and no `!dj`.
		{
			let ctx = ds.env(Write).await;
			let tx = ctx.tx();
			tx.set_key(&d.erased_key(&int), &7u64).await.unwrap();
			tx.set_key(&DocKeyKey::new(NS, DB, Cow::Borrowed(&d.tb), 7), &int).await.unwrap();
			finish(ctx).await;
		}

		let ctx = ds.env(Read).await;
		let tx = ctx.tx();
		assert_eq!(
			d.get_doc_id(&tx, &int).await.unwrap(),
			Some(7),
			"the record the reverse mapping names adopts it"
		);
		assert_eq!(
			d.get_doc_id(&tx, &dec).await.unwrap(),
			None,
			"a different record on the same erased key must not inherit it"
		);
		tx.cancel().await.unwrap();

		// ... and resolving it hands out a fresh id instead.
		let ctx = ds.env(Write).await;
		let fresh = d.resolve_or_assign(&ctx, &dec).await.unwrap();
		finish(ctx).await;
		assert_ne!(fresh, 7);

		// The fresh mapping took over the erased key, which was the owner's only
		// forward mapping; the owner must still resolve to its own id.
		let ctx = ds.env(Read).await;
		let tx = ctx.tx();
		assert_eq!(tx.get_key(&d.erased_key(&int), None).await.unwrap(), Some(fresh));
		assert_eq!(
			d.get_doc_id(&tx, &int).await.unwrap(),
			Some(7),
			"the record the erased key named keeps its id"
		);
		assert_eq!(
			tx.get_key(&d.identity_key(&int), None).await.unwrap(),
			Some(7),
			"through an injective mapping of its own"
		);
		assert_eq!(d.get_doc_id(&tx, &dec).await.unwrap(), Some(fresh));
		tx.cancel().await.unwrap();
	}

	/// The record an erased key names keeps resolving when a colliding record
	/// takes the key over, even if its own injective mapping is stale.
	///
	/// A node on the previous release that purges and re-creates the record
	/// leaves the `!dj` this release wrote naming the purged doc-ID, and maps the
	/// re-created record through `!di` and `!dd` alone. Taking that `!di` for
	/// the colliding record must first point the stale `!dj` at the live doc-ID.
	#[tokio::test]
	async fn a_stale_injective_mapping_is_replaced_before_the_erased_key_moves() {
		use crate::val::{Number, Value};
		let arr = |n: Number| RecordIdKey::Array(vec![Value::Number(n)].into());
		let int = arr(Number::Int(1));
		let dec = arr(Number::Decimal(rust_decimal::Decimal::from(1)));

		let ds = TestIndexStore::new().await;
		let d = TableDocIds::new(NS, DB, "t".into());
		// `!dj` still names doc 3, purged; `!di` and `!dd` name the re-created 7.
		{
			let ctx = ds.env(Write).await;
			let tx = ctx.tx();
			tx.set_key(&d.identity_key(&int), &3u64).await.unwrap();
			tx.set_key(&d.erased_key(&int), &7u64).await.unwrap();
			tx.set_key(&DocKeyKey::new(NS, DB, Cow::Borrowed(&d.tb), 7), &int).await.unwrap();
			finish(ctx).await;
		}

		let ctx = ds.env(Write).await;
		let fresh = d.resolve_or_assign(&ctx, &dec).await.unwrap();
		finish(ctx).await;
		assert_ne!(fresh, 7);

		let ctx = ds.env(Read).await;
		let tx = ctx.tx();
		assert_eq!(tx.get_key(&d.erased_key(&int), None).await.unwrap(), Some(fresh));
		assert_eq!(
			d.get_doc_id(&tx, &int).await.unwrap(),
			Some(7),
			"the re-created record keeps the doc-ID its postings use"
		);
		assert_eq!(tx.get_key(&d.identity_key(&int), None).await.unwrap(), Some(7));
		tx.cancel().await.unwrap();
	}

	#[tokio::test]
	async fn resolve_is_idempotent_and_monotonic() {
		let ds = TestIndexStore::new().await;
		{
			let (ctx, d) = new_op(&ds, Write).await;
			// First record gets id 0.
			assert_eq!(d.resolve_or_assign(&ctx, &rid("a")).await.unwrap(), 0);
			// Same record resolves to the same id (idempotent).
			assert_eq!(d.resolve_or_assign(&ctx, &rid("a")).await.unwrap(), 0);
			// A new record gets the next monotonic id.
			assert_eq!(d.resolve_or_assign(&ctx, &rid("b")).await.unwrap(), 1);
			finish(ctx).await;
		}
		// Both directions of the mapping are persisted.
		let (ctx, d) = new_op(&ds, Read).await;
		assert_eq!(d.get_doc_id(&ctx.tx(), &rid("a")).await.unwrap(), Some(0));
		assert_eq!(d.get_doc_id(&ctx.tx(), &rid("b")).await.unwrap(), Some(1));
		assert_eq!(d.get_record_id(&ctx.tx(), 0).await.unwrap(), Some(rid("a")));
		assert_eq!(d.get_record_id(&ctx.tx(), 1).await.unwrap(), Some(rid("b")));
	}

	#[tokio::test]
	async fn one_doc_id_shared_across_indexes() {
		// Two independent handles model two indexes on the same table. The first
		// to touch a record assigns the id; the second reads the committed value,
		// so both observe the SAME doc-id.
		let ds = TestIndexStore::new().await;
		let assigned = {
			let (ctx, index_a) = new_op(&ds, Write).await;
			let id = index_a.resolve_or_assign(&ctx, &rid("shared")).await.unwrap();
			finish(ctx).await;
			id
		};
		let (ctx, index_b) = new_op(&ds, Write).await;
		assert_eq!(index_b.resolve_or_assign(&ctx, &rid("shared")).await.unwrap(), assigned);
		finish(ctx).await;
	}

	#[tokio::test]
	async fn delete_then_recreate_yields_new_id() {
		let ds = TestIndexStore::new().await;
		{
			let (ctx, d) = new_op(&ds, Write).await;
			assert_eq!(d.resolve_or_assign(&ctx, &rid("a")).await.unwrap(), 0);
			assert_eq!(d.resolve_or_assign(&ctx, &rid("b")).await.unwrap(), 1);
			finish(ctx).await;
		}
		// Remove "a" (doc 0).
		{
			let (ctx, d) = new_op(&ds, Write).await;
			d.remove(&ctx.tx(), &rid("a")).await.unwrap();
			finish(ctx).await;
		}
		// Re-create "a": it must get a fresh id (2), never the recycled 0.
		{
			let (ctx, d) = new_op(&ds, Write).await;
			assert_eq!(d.resolve_or_assign(&ctx, &rid("a")).await.unwrap(), 2);
			finish(ctx).await;
		}
		let (ctx, d) = new_op(&ds, Read).await;
		// The old reverse mapping for 0 is gone.
		assert_eq!(d.get_record_id(&ctx.tx(), 0).await.unwrap(), None);
		assert_eq!(d.get_doc_id(&ctx.tx(), &rid("a")).await.unwrap(), Some(2));
	}

	/// The forward batch answers for a record mapped before the injective key
	/// existed, through the same verified fallback as the single-record read.
	#[tokio::test]
	async fn forward_batch_falls_back_for_a_record_mapped_under_the_erased_key() {
		let ds = TestIndexStore::new().await;
		let mapped = rid("mapped-before-dj");
		let absent = rid("never-mapped");

		// A store as an older release left it: the erased forward key and the
		// reverse one, and no injective key.
		{
			let (ctx, d) = new_op(&ds, Write).await;
			let tx = ctx.tx();
			tx.set_key(&d.erased_key(&mapped), &4u64).await.unwrap();
			tx.set_key(&DocKeyKey::new(NS, DB, Cow::Borrowed(&d.tb), 4), &mapped).await.unwrap();
			finish(ctx).await;
		}

		let (ctx, d) = new_op(&ds, Read).await;
		assert_eq!(
			d.get_doc_ids_batch(&ctx.tx(), &[mapped, absent]).await.unwrap(),
			vec![Some(4), None],
			"order is preserved and the fallback answers for the mapped record"
		);
	}

	/// Removing one of two records that share an erased key leaves the survivor
	/// resolvable on this release, and takes the shared key with it.
	///
	/// Taking the key is deliberate. A node on the previous release resolves
	/// through it alone, so the survivor looks unmapped there and is minted a
	/// second doc-ID for the length of the window, which is accepted. Keeping the
	/// key would be worse: it would name an id
	/// whose reverse mapping is gone, and an older node re-creating any id that
	/// spells it would adopt that dead id and publish nothing, so every later hit
	/// on that record is dropped after taking a top-k slot.
	#[tokio::test]
	async fn removing_a_colliding_record_keeps_the_survivor_resolvable_here() {
		use crate::val::{Number, Value};
		let arr = |n: Number| RecordIdKey::Array(vec![Value::Number(n)].into());

		let ds = TestIndexStore::new().await;
		let d = TableDocIds::new(NS, DB, "t".into());
		let int = arr(Number::Int(1));
		let dec = arr(Number::Decimal(rust_decimal::Decimal::from(1)));

		let (int_id, dec_id) = {
			let ctx = ds.env(Write).await;
			let a = d.resolve_or_assign(&ctx, &int).await.unwrap();
			let b = d.resolve_or_assign(&ctx, &dec).await.unwrap();
			finish(ctx).await;
			(a, b)
		};
		assert_ne!(int_id, dec_id, "they are distinct records");

		{
			let ctx = ds.env(Write).await;
			d.remove(&ctx.tx(), &dec).await.unwrap();
			finish(ctx).await;
		}

		let ctx = ds.env(Read).await;
		let tx = ctx.tx();
		// The survivor keeps its own id, through the injective mapping the
		// removal did not touch.
		assert_eq!(d.get_doc_id(&tx, &int).await.unwrap(), Some(int_id));
		assert_eq!(d.get_doc_id(&tx, &dec).await.unwrap(), None, "the removed record is gone");
		// And nothing is left naming the dead id.
		assert_eq!(
			tx.get_key(&d.erased_key(&dec), None).await.unwrap(),
			None,
			"a shared key naming a removed record must not outlive it"
		);
		tx.cancel().await.unwrap();
	}

	/// Resolving a record that only the type-erased key maps publishes the
	/// injective mapping for it.
	///
	/// Nothing else backfills `!dj`: the migration runs once, at the first
	/// upgraded node's start, and every record a node on the previous release
	/// creates afterwards is `!di`-only. Left that way, such a record would lose
	/// its forward mapping entirely when `!di` is dropped, and the next resolve
	/// would mint it a second id while its postings still reference the first.
	#[tokio::test]
	async fn resolving_an_erased_only_record_publishes_the_injective_mapping() {
		let ds = TestIndexStore::new().await;
		let d = TableDocIds::new(NS, DB, "t".into());
		let id = rid("mapped-by-an-older-node");

		// A store as a node on the previous release leaves it: `!di` and `!dd`,
		// and no `!dj`.
		{
			let ctx = ds.env(Write).await;
			let tx = ctx.tx();
			tx.set_key(&d.erased_key(&id), &9u64).await.unwrap();
			tx.set_key(&DocKeyKey::new(NS, DB, Cow::Borrowed(&d.tb), 9), &id).await.unwrap();
			finish(ctx).await;
		}
		{
			let ctx = ds.env(Read).await;
			let tx = ctx.tx();
			assert_eq!(tx.get_key(&d.identity_key(&id), None).await.unwrap(), None);
			tx.cancel().await.unwrap();
		}

		let resolved = {
			let ctx = ds.env(Write).await;
			let doc_id = d.resolve_or_assign(&ctx, &id).await.unwrap();
			finish(ctx).await;
			doc_id
		};
		assert_eq!(resolved, 9, "the record keeps the id it was already mapped to");

		let ctx = ds.env(Read).await;
		let tx = ctx.tx();
		assert_eq!(
			tx.get_key(&d.identity_key(&id), None).await.unwrap(),
			Some(9),
			"the injective mapping must now name the same id"
		);
		tx.cancel().await.unwrap();
	}

	/// A reclaim repair must not bury a mapping an older node has published.
	///
	/// Such a node re-creates a record by writing `!di` and `!dd` with a fresh
	/// id and no `!dj`, so the injective claim is free for the taking — and
	/// taking it would point the record back at the reclaimed id while that
	/// node's index entries reference the new one.
	#[tokio::test]
	async fn restore_yields_to_a_mapping_an_older_node_published() {
		let ds = TestIndexStore::new().await;
		let d = TableDocIds::new(NS, DB, "t".into());
		let id = rid("recreated-by-an-older-node");

		let reclaimed = {
			let ctx = ds.env(Write).await;
			let doc_id = d.resolve_or_assign(&ctx, &id).await.unwrap();
			d.remove(&ctx.tx(), &id).await.unwrap();
			finish(ctx).await;
			doc_id
		};

		// What such a node leaves behind when it re-creates the record.
		let published = reclaimed + 100;
		{
			let ctx = ds.env(Write).await;
			let tx = ctx.tx();
			tx.set_key(&d.erased_key(&id), &published).await.unwrap();
			tx.set_key(&DocKeyKey::new(NS, DB, Cow::Borrowed(&d.tb), published), &id)
				.await
				.unwrap();
			finish(ctx).await;
		}

		{
			let ctx = ds.env(Write).await;
			assert!(
				!d.restore(&ctx.tx(), &id, reclaimed).await.unwrap(),
				"the repair must yield to the live mapping"
			);
			finish(ctx).await;
		}

		let ctx = ds.env(Read).await;
		let tx = ctx.tx();
		assert_eq!(
			d.get_doc_id(&tx, &id).await.unwrap(),
			Some(published),
			"the record keeps the id the older node minted"
		);
		assert_eq!(tx.get_key(&d.erased_key(&id), None).await.unwrap(), Some(published));
		tx.cancel().await.unwrap();
	}

	/// The batched forward lookup must reach the same verdict as the
	/// single-record one.
	///
	/// A node on the previous release purges a record by deleting `!di` and
	/// `!dd` alone, so a `!dj` hit can name a doc-ID the reverse mapping has
	/// disowned, or one it has since given to another record. A batch that
	/// trusted its hits would hand that orphan to every doc-ID-consuming
	/// caller.
	#[tokio::test]
	async fn the_forward_batch_disowns_a_claim_the_reverse_mapping_no_longer_names() {
		let ds = TestIndexStore::new().await;
		let d = TableDocIds::new(NS, DB, "t".into());
		let purged = rid("purged-by-an-older-node");
		let superseded = rid("remapped-by-an-older-node");
		let live = rid("still-here");

		let (orphan, stale, live_id) = {
			let ctx = ds.env(Write).await;
			let orphan = d.resolve_or_assign(&ctx, &purged).await.unwrap();
			let stale = d.resolve_or_assign(&ctx, &superseded).await.unwrap();
			let live_id = d.resolve_or_assign(&ctx, &live).await.unwrap();
			finish(ctx).await;
			(orphan, stale, live_id)
		};

		{
			let ctx = ds.env(Write).await;
			let tx = ctx.tx();
			// Exactly what such a purge leaves behind: `!di` and `!dd` gone,
			// `!dj` not.
			tx.del_key(&d.erased_key(&purged)).await.unwrap();
			tx.del_key(&DocKeyKey::new(NS, DB, Cow::Borrowed(&d.tb), orphan)).await.unwrap();
			// And what a remap leaves: the reverse mapping now names someone else.
			tx.set_key(&DocKeyKey::new(NS, DB, Cow::Borrowed(&d.tb), stale), &live).await.unwrap();
			finish(ctx).await;
		}

		let ctx = ds.env(Read).await;
		let tx = ctx.tx();
		for (id, what) in [(&purged, "orphaned"), (&superseded, "superseded")] {
			assert_eq!(
				d.get_doc_id(&tx, id).await.unwrap(),
				None,
				"the single-record path already disowns the {what} claim"
			);
		}
		assert_eq!(
			d.get_doc_ids_batch(&tx, &[purged.clone(), superseded.clone(), live.clone()])
				.await
				.unwrap(),
			vec![None, None, Some(live_id)],
			"the batch must agree with it, and must still answer for the record beside them"
		);
		tx.cancel().await.unwrap();
	}

	/// Removing one record must not take the erased mapping another record owns.
	///
	/// That key is shared by every id with the same decimal expansion, so it
	/// belongs to whichever record the reverse mapping names — deleting it
	/// unconditionally would strand the other one on a node that still reads it.
	#[tokio::test]
	async fn removing_one_of_two_colliding_records_leaves_the_other_mapped() {
		use crate::val::{Number, Value};
		let arr = |n: Number| RecordIdKey::Array(vec![Value::Number(n)].into());
		let int = arr(Number::Int(1));
		let dec = arr(Number::Decimal(rust_decimal::Decimal::from(1)));

		let ds = TestIndexStore::new().await;
		let (a, b) = {
			let (ctx, d) = new_op(&ds, Write).await;
			let a = d.resolve_or_assign(&ctx, &int).await.unwrap();
			let b = d.resolve_or_assign(&ctx, &dec).await.unwrap();
			finish(ctx).await;
			(a, b)
		};
		assert_ne!(a, b);

		{
			let (ctx, d) = new_op(&ds, Write).await;
			assert_eq!(d.remove(&ctx.tx(), &int).await.unwrap(), Some(a));
			finish(ctx).await;
		}

		let (ctx, d) = new_op(&ds, Read).await;
		let tx = ctx.tx();
		assert_eq!(d.get_doc_id(&tx, &int).await.unwrap(), None, "the removed record is gone");
		assert_eq!(d.get_doc_id(&tx, &dec).await.unwrap(), Some(b));
		// Asserted on the erased key itself. A reader on this release prefers the
		// injective one, so it cannot see this either way — the record left
		// holding the erased key is the one a node on the *previous* release
		// still resolves through, and deleting it there would strand that record
		// mid-rollout.
		assert_eq!(
			tx.get_key(&d.erased_key(&dec), None).await.unwrap(),
			Some(b),
			"the surviving record keeps the erased mapping an older node reads"
		);
	}

	#[tokio::test]
	async fn batch_resolve_preserves_order() {
		let ds = TestIndexStore::new().await;
		{
			let (ctx, d) = new_op(&ds, Write).await;
			d.resolve_or_assign(&ctx, &rid("a")).await.unwrap(); // 0
			d.resolve_or_assign(&ctx, &rid("b")).await.unwrap(); // 1
			finish(ctx).await;
		}
		let (ctx, d) = new_op(&ds, Read).await;
		// Order is preserved and unmapped ids yield None in their slot.
		let got = d.get_record_ids_batch(&ctx.tx(), &[1, 0, 9]).await.unwrap();
		assert_eq!(got, vec![Some(rid("b")), Some(rid("a")), None]);
	}

	#[tokio::test]
	async fn remove_all_clears_mappings_and_keeps_monotonicity() {
		// Models dropping the last doc-ID-consuming index on a table: every
		// `!di`/`!dd` mapping is reclaimed, but the sequence counter is kept so a
		// later index still allocates fresh, never-reused ids.
		let ds = TestIndexStore::new().await;
		{
			let (ctx, d) = new_op(&ds, Write).await;
			d.resolve_or_assign(&ctx, &rid("a")).await.unwrap(); // 0
			d.resolve_or_assign(&ctx, &rid("b")).await.unwrap(); // 1
			finish(ctx).await;
		}
		// Purge the whole shared space.
		{
			let (ctx, d) = new_op(&ds, Write).await;
			d.remove_all(&ctx.tx()).await.unwrap();
			finish(ctx).await;
		}
		// Both directions are gone for every record. The injective key is
		// asserted directly: `get_doc_id` answers `None` once the reverse mapping
		// is gone, so reading through it would pass even if that band leaked.
		{
			let (ctx, d) = new_op(&ds, Read).await;
			for name in ["a", "b"] {
				assert_eq!(
					ctx.tx().get_key(&d.identity_key(&rid(name)), None).await.unwrap(),
					None,
					"the injective band must be purged too"
				);
			}
			assert_eq!(d.get_doc_id(&ctx.tx(), &rid("a")).await.unwrap(), None);
			assert_eq!(d.get_doc_id(&ctx.tx(), &rid("b")).await.unwrap(), None);
			assert_eq!(d.get_record_id(&ctx.tx(), 0).await.unwrap(), None);
			assert_eq!(d.get_record_id(&ctx.tx(), 1).await.unwrap(), None);
		}
		// A record re-created after the purge gets a fresh id (2), never the
		// reclaimed 0/1 — the never-reuse invariant survives a full purge.
		let (ctx, d) = new_op(&ds, Write).await;
		assert_eq!(d.resolve_or_assign(&ctx, &rid("a")).await.unwrap(), 2);
		finish(ctx).await;
	}
}

/// Cross-index concurrency regression tests. These exercise the last-writer-wins
/// `!di` assignment race (race #2) that only manifests on TiKV, so they are
/// gated to the `kv-tikv` backend and require a running cluster at
/// 127.0.0.1:2379 (the `tikv` CI lane).
#[cfg(test)]
#[cfg(feature = "kv-tikv")]
mod tikv_concurrency {
	use std::borrow::Cow;
	use std::collections::HashSet;
	use std::sync::Arc;

	use surrealdb_datastore::is_retryable_transaction_conflict;
	use surrealdb_kvs::TransactionType;

	use super::{DocId, TableDocIds};
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::key::schema::{DocKeyPrefix, RootRoot, VersionKey};
	use crate::test_env::TestIndexStore;
	use crate::val::RecordIdKey;

	const NS: NamespaceId = NamespaceId(1);
	const DB: DatabaseId = DatabaseId(1);

	async fn fresh_tikv_ds() -> Arc<TestIndexStore> {
		let ds = TestIndexStore::with_path("tikv:127.0.0.1:2379").await;
		let tx = ds.transaction(TransactionType::Write).await.unwrap();
		// Both top-level regions: everything under the root, and the storage
		// version key, which sits outside it.
		tx.delr(RootRoot {}.range_subtree().unwrap()).await.unwrap();
		tx.del_key(&VersionKey {}).await.unwrap();
		tx.commit().await.unwrap();
		Arc::new(ds)
	}

	/// Resolve-or-assign `rid` in its own transaction, retrying on a transaction
	/// conflict until it commits. Returns the doc-ID that was durably committed.
	async fn resolve_committed(ds: &TestIndexStore, rid: &RecordIdKey) -> DocId {
		loop {
			let ctx = ds.env(TransactionType::Write).await;
			let d = TableDocIds::new(NS, DB, "t".into());
			match d.resolve_or_assign(&ctx, rid).await {
				Ok(doc_id) => match ctx.tx().commit().await {
					Ok(()) => return doc_id,
					Err(e) if is_retryable_transaction_conflict(&e) => continue,
					Err(e) => panic!("unexpected commit error: {e}"),
				},
				Err(e) => {
					let _ = ctx.tx().cancel().await;
					if is_retryable_transaction_conflict(&e) {
						continue;
					}
					panic!("unexpected resolve error: {e}");
				}
			}
		}
	}

	/// Race #2: several indexes first-touching the SAME record concurrently must
	/// converge on ONE doc-ID. A blind `set !di` lets a later writer overwrite
	/// the forward mapping on TiKV while every writer's distinct `!dd` reverse
	/// entry survives — leaving a record with several doc-IDs. The conditional
	/// `putc` claim keeps a single winner that all others adopt, so exactly one
	/// `!di`/`!dd` pair remains.
	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	#[serial_test::serial]
	async fn one_doc_id_across_concurrent_indexes() {
		const TASKS: usize = 6;
		let ds = fresh_tikv_ds().await;
		let rid = RecordIdKey::from("shared".to_owned());

		let mut handles = Vec::with_capacity(TASKS);
		for _ in 0..TASKS {
			let ds = Arc::clone(&ds);
			let rid = rid.clone();
			handles.push(tokio::spawn(async move { resolve_committed(&ds, &rid).await }));
		}
		let mut ids = Vec::with_capacity(TASKS);
		for h in handles {
			ids.push(h.await.unwrap());
		}

		// Every task converged on the same committed doc-ID.
		let unique: HashSet<_> = ids.iter().copied().collect();
		assert_eq!(unique.len(), 1, "all indexes must share one doc-ID, got {ids:?}");
		let doc_id = ids[0];

		let tb = "t".into();
		let tx = ds.transaction(TransactionType::Read).await.unwrap();
		let d = TableDocIds::new(NS, DB, "t".into());
		// The bidirectional mapping is consistent...
		assert_eq!(d.get_doc_id(&tx, &rid).await.unwrap(), Some(doc_id));
		assert_eq!(d.get_record_id(&tx, doc_id).await.unwrap(), Some(rid.clone()));
		// ...and no loser leaked a stale reverse mapping: exactly one `!dd` entry.
		let reverse = tx
			.getr(DocKeyPrefix::new(NS, DB, Cow::Borrowed(&tb)).range().unwrap(), None)
			.await
			.unwrap();
		assert_eq!(
			reverse.len(),
			1,
			"exactly one reverse mapping expected, found {}",
			reverse.len()
		);
		tx.cancel().await.unwrap();
	}

	/// Race #2, deterministic (fails before the fix, passes after): drive the
	/// exact last-writer-wins window instead of relying on scheduler timing.
	/// Index B opens its transaction — and pins its snapshot with a read — before
	/// index A assigns and commits the doc-ID. B then resolves on that stale
	/// snapshot: a blind `set !di` would last-writer-wins overwrite A's mapping
	/// (leaving the record with two doc-IDs), whereas the `putc` claim makes B's
	/// commit conflict. Either way the shared space must end with exactly one
	/// mapping, pointing at A's doc-ID.
	#[tokio::test]
	#[serial_test::serial]
	async fn first_touch_never_diverges_in_lww_window() {
		let ds = fresh_tikv_ds().await;
		let rid = RecordIdKey::from("rec".to_owned());

		// B's transaction starts first; the explicit read pins its snapshot to
		// the pre-assignment state (no `!di` yet), even if start timestamps are
		// assigned lazily.
		let ctx_b = ds.env(TransactionType::Write).await;
		let d_b = TableDocIds::new(NS, DB, "t".into());
		assert_eq!(d_b.get_doc_id(&ctx_b.tx(), &rid).await.unwrap(), None);

		// A assigns a doc-ID and commits.
		let a_doc = {
			let ctx_a = ds.env(TransactionType::Write).await;
			let d_a = TableDocIds::new(NS, DB, "t".into());
			let id = d_a.resolve_or_assign(&ctx_a, &rid).await.unwrap();
			ctx_a.tx().commit().await.unwrap();
			id
		};

		// B resolves on its stale snapshot, then tries to commit.
		let b_doc = d_b.resolve_or_assign(&ctx_b, &rid).await.unwrap();
		let b_commit_ok = ctx_b.tx().commit().await.is_ok();

		// Exactly one mapping survives, and it is A's.
		let tx = ds.transaction(TransactionType::Read).await.unwrap();
		let d = TableDocIds::new(NS, DB, "t".into());
		let tb = "t".into();
		let reverse = tx
			.getr(DocKeyPrefix::new(NS, DB, Cow::Borrowed(&tb)).range().unwrap(), None)
			.await
			.unwrap();
		assert_eq!(d.get_doc_id(&tx, &rid).await.unwrap(), Some(a_doc));
		assert_eq!(
			reverse.len(),
			1,
			"record must have exactly one doc-ID (b_doc={b_doc}, b_commit_ok={b_commit_ok})",
		);
		tx.cancel().await.unwrap();
	}
}
