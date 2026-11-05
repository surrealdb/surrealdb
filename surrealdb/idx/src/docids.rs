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
//! - `/*{ns}*{db}*{tb}!dd{doc_id}` → [`RecordIdKey`] (see [`crate::key::table::dd`]),
//!
//! and allocates fresh ids through the distributed-sequence batching engine
//! ([`surrealdb_datastore::sequences`], the `TableDocIds` domain).
//!
//! A space may also hold `!di` rows, the forward mapping keyed under the index
//! format, which a 3.3 node writes alongside `!dj`. Nothing here reads or
//! writes them; they are removed with the space.
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

use std::borrow::Cow;

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
	DocLookupIdentityPrefix, DocLookupPrefix, DocPendingPrefix, FtDocKeyKey, FtDocLookupKey,
	HnswDocKeyKey, HnswDocLookupKey, HnswDocsStateKey,
};
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

	/// Reads `id`'s forward mapping under a commit-time conflict registration.
	///
	/// See [`TableDocIds::lock_unmapped`].
	pub async fn lock_unmapped(&self, tx: &Transaction, id: &RecordIdKey) -> Result<()> {
		let key = FtDocLookupKey::new(
			self.ns,
			self.db,
			Cow::Borrowed(&self.tb),
			self.ix,
			Cow::Borrowed(id),
		);
		tx.getu_key(&key).await.map(drop)
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

/// Whether a claim of a forward mapping may be a blind `set`, given `read_at`, the transaction's
/// count of forward-mapping writes ([`Transaction::doc_lookup_writes`]) before it read the
/// mapping absent. The precondition is that the mapping is absent, and not written by this
/// transaction since the read: the count is known and unchanged, and the backend's commit fails
/// a blind write that overlaps one committed after its snapshot
/// ([`Transaction::blind_write_conflicts`]). The caller writes the claim with no await between
/// this check and the write, and claims one mapping of a transaction at a time; two claims that
/// both checked before either wrote would both be blind.
fn claim_may_be_blind(tx: &Transaction, read_at: Option<u64>) -> bool {
	read_at.is_some() && tx.doc_lookup_writes() == read_at && tx.blind_write_conflicts()
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

	/// The forward key for `id`.
	fn identity_key<'a>(&'a self, id: &'a RecordIdKey) -> DocLookupIdentityKey<'a> {
		DocLookupIdentityKey::new(
			self.ns,
			self.db,
			Cow::Borrowed(&self.tb),
			RecordIdentity(id.clone()),
		)
	}

	/// The reverse key for `doc_id`.
	fn doc_key(&self, doc_id: DocId) -> DocKeyKey<'_> {
		DocKeyKey::new(self.ns, self.db, Cow::Borrowed(&self.tb), doc_id)
	}

	/// Returns the doc-ID mapped to `id`, or `None` if the record has no id yet.
	///
	/// The forward mapping is keyed by the record id's lossless encoding, so ids
	/// differing only in a number's kind each have their own. It is published
	/// and deleted together with the reverse mapping, so a hit is adopted as it
	/// is; see `Transaction::resolve_doc_id`.
	pub async fn get_doc_id(&self, tx: &Transaction, id: &RecordIdKey) -> Result<Option<DocId>> {
		tx.resolve_doc_id(self.ns, self.db, &self.tb, id, None).await
	}

	/// Reads `id`'s forward mapping under a commit-time conflict registration.
	///
	/// For a caller that has found `id` unmapped and acts on that: an index
	/// removing a record's content finds nothing to remove. The answer is judged
	/// on this transaction's snapshot, and a mapping can be written after it — by
	/// an index build replaying the write that created the record, which indexes
	/// its content under a new doc-ID. Registered, that write fails this
	/// transaction's commit as a retryable conflict rather than leaving the
	/// replayed content indexed for a record this transaction changed.
	pub async fn lock_unmapped(&self, tx: &Transaction, id: &RecordIdKey) -> Result<()> {
		tx.getu_key(&self.identity_key(id)).await.map(drop)
	}

	/// Returns the existing doc-ID for `id`, or allocates, persists, and returns a
	/// new one.
	///
	/// Idempotent: concurrent callers on different indexes converge on the same id
	/// because the forward mapping is read first and only a missing mapping
	/// triggers a sequence allocation. The new id is drawn from the table-scoped
	/// distributed sequence (batched via `config.idx.table_doc_ids_batch_size`) and
	/// the forward (`!dj`) and reverse (`!dd`) mappings are written.
	///
	/// The forward mapping is claimed with a conditional create (`put`) rather than a blind
	/// `set`, unless the claim may be blind: the backend's commit fails a transaction whose
	/// blind write overlaps one that committed after its snapshot
	/// ([`Transaction::blind_write_conflicts`]), and the mapping was read absent by this
	/// transaction and not written by it since. Then an existence check adds nothing to the
	/// commit's own conflict check, and the claim is a `set`. The second condition is the
	/// transaction's count of forward-mapping writes ([`Transaction::doc_lookup_writes`]),
	/// taken before the read and known and unchanged at the claim. The allocation between the
	/// read and the claim awaits, and another claim of the mapping made by this transaction
	/// meanwhile would be overwritten by a `set`, which no commit-time check detects, leaving
	/// two reverse mappings for one record.
	///
	/// When two doc-ID-consuming indexes first touch the same record concurrently (e.g.
	/// concurrent `DEFINE`/`REBUILD` of full-text / HNSW / DiskANN), each observes no mapping
	/// and allocates a *different* id from the shared sequence. On last-writer-wins backends
	/// (TiKV) a blind `set` would let the later writer overwrite the forward mapping — leaving
	/// one record with two doc-IDs and breaking the cross-index identity scans rely on. `put`
	/// reads `!dj` before writing, which arms the write-conflict check, so only one writer
	/// wins; the loser's commit is rejected and its caller retries, at which point the fast
	/// path returns the committed id (making the operation idempotent). The abandoned
	/// speculative id is never reused, which is fine — the space is monotonic and `u64` does
	/// not exhaust (see the module invariants).
	pub async fn resolve_or_assign(&self, env: &dyn IndexEnv, id: &RecordIdKey) -> Result<DocId> {
		let tx = env.tx();
		// Counted before the read, so the claim can tell that this transaction wrote no forward
		// mapping since.
		let written = tx.doc_lookup_writes();
		// Fast path: the record already has an id in this table's space.
		if let Some(doc_id) = self.get_doc_id(&tx, id).await? {
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
		// Claim the forward mapping. `put` only writes if `!dj` is still absent, and
		// reading it arms the write-conflict check. The blind `set` is the same claim
		// where the read above answered absent, this transaction has written no
		// forward mapping since, and the backend's commit rejects any overlapping
		// write: it is rejected exactly as the `put` would be.
		let dj = self.identity_key(id);
		let claimed = if claim_may_be_blind(&tx, written) {
			tx.set_key(&dj, &doc_id).await
		} else {
			tx.put_key(&dj, &doc_id).await
		};
		match claimed {
			Ok(()) => {
				// We won the race: publish the reverse mapping too. Written only
				// after the forward claim succeeds, so a loser leaves no dangling
				// `!dd` entry to clean up.
				tx.set_key(&self.doc_key(doc_id), id).await?;
				Ok(doc_id)
			}
			// Reachable only on the `put` path: another index published the mapping
			// first and it is already visible in our snapshot, so adopt the committed
			// id and drop our speculative one. (Under snapshot isolation the primary
			// defence is the commit-time conflict above plus the caller's retry; this
			// branch covers the mapping becoming visible between our two reads.)
			Err(e) if matches!(storage_error(&e), Some(KvsError::TransactionKeyAlreadyExists)) => {
				tx.get_key(&dj, None).await?.ok_or_else(|| {
					anyhow::anyhow!("doc-ID mapping missing after a conditional-create conflict")
				})
			}
			Err(e) => Err(e),
		}
	}

	/// Returns the doc-ID of each record of `ids`, in order, assigning one to
	/// each that has none, as [`Self::resolve_or_assign`] does for one record.
	///
	/// The forward mappings are read in one batched read and the new IDs drawn
	/// from the table's sequence as one block. `ids` must name each record at
	/// most once: the claims are then of distinct mappings, so a blind claim
	/// overwrites no claim this transaction made, and one is blind on the
	/// condition [`Self::resolve_or_assign`] states for its own.
	pub async fn resolve_or_assign_many(
		&self,
		env: &dyn IndexEnv,
		ids: &[RecordIdKey],
	) -> Result<Vec<DocId>> {
		let tx = env.tx();
		let written = tx.doc_lookup_writes();
		let found = self.get_doc_ids_batch(&tx, ids).await?;
		let missing = found.iter().filter(|doc_id| doc_id.is_none()).count();
		if missing == 0 {
			return Ok(found.into_iter().flatten().collect());
		}
		let fresh = env
			.sequences()?
			.next_table_doc_ids(
				Some(env),
				self.ns,
				self.db,
				self.tb.clone(),
				missing,
				env.config().table_doc_ids_batch_size,
			)
			.await?;
		let blind = claim_may_be_blind(&tx, written);
		let mut fresh = fresh.into_iter();
		let mut out = Vec::with_capacity(ids.len());
		for (id, found) in ids.iter().zip(found) {
			if let Some(doc_id) = found {
				out.push(doc_id);
				continue;
			}
			let doc_id = fresh
				.next()
				.ok_or_else(|| anyhow::anyhow!("the sequence drew fewer doc-IDs than asked for"))?;
			let dj = self.identity_key(id);
			let claimed = if blind {
				tx.set_key(&dj, &doc_id).await
			} else {
				tx.put_key(&dj, &doc_id).await
			};
			match claimed {
				Ok(()) => {
					tx.set_key(&self.doc_key(doc_id), id).await?;
					out.push(doc_id);
				}
				Err(e)
					if matches!(storage_error(&e), Some(KvsError::TransactionKeyAlreadyExists)) =>
				{
					out.push(tx.get_key(&dj, None).await?.ok_or_else(|| {
						anyhow::anyhow!(
							"doc-ID mapping missing after a conditional-create conflict"
						)
					})?);
				}
				Err(e) => return Err(e),
			}
		}
		Ok(out)
	}

	/// Returns the record key mapped to `doc_id`, or `None` if unmapped.
	pub async fn get_record_id(
		&self,
		tx: &Transaction,
		doc_id: DocId,
	) -> Result<Option<RecordIdKey>> {
		tx.get_key(&self.doc_key(doc_id), None).await
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
		let keys: Vec<DocKeyKey<'_>> = doc_ids.iter().map(|&doc_id| self.doc_key(doc_id)).collect();
		tx.get_many_key(keys, version).await
	}

	/// Resolves many record keys to their doc-IDs in one batched read,
	/// preserving input order. Records with no id in this table's space yield
	/// `None` in their slot. The mirror of [`Self::get_record_ids_batch`] for
	/// the forward (`!dj`) direction.
	pub async fn get_doc_ids_batch(
		&self,
		tx: &Transaction,
		ids: &[RecordIdKey],
	) -> Result<Vec<Option<DocId>>> {
		let keys: Vec<DocLookupIdentityKey<'_>> =
			ids.iter().map(|id| self.identity_key(id)).collect();
		tx.get_many_key(keys, None).await
	}

	/// Removes both directions of the mapping for `id`. The id is *not* recycled.
	///
	/// Must only be called once a record is being purged and every index has
	/// released it (see the module invariants).
	///
	/// Returns the doc-ID the record was mapped to, which the deferred-reclaim
	/// sweep uses to restore the mapping of a record re-created concurrently with
	/// it, or `None` when the record has no mapping.
	pub async fn remove(&self, tx: &Transaction, id: &RecordIdKey) -> Result<Option<DocId>> {
		let Some(doc_id) = self.get_doc_id(tx, id).await? else {
			return Ok(None);
		};
		tx.del_key(&self.identity_key(id)).await?;
		tx.del_key(&self.doc_key(doc_id)).await?;
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
		match tx.put_key(&self.identity_key(id), &doc_id).await {
			Ok(()) => {
				tx.set_key(&self.doc_key(doc_id), id).await?;
				Ok(true)
			}
			// The record already re-acquired a (fresh) id: leave it in place.
			Err(e) if matches!(storage_error(&e), Some(KvsError::TransactionKeyAlreadyExists)) => {
				Ok(false)
			}
			Err(e) => Err(e),
		}
	}

	/// Removes **all** doc-ID state for the table (`!dj`/`!dd` mappings, any
	/// `!di` rows a 3.3 node wrote, and `!dp` pending-reclaim markers) in one
	/// prefix delete per kind, reclaiming the shared space.
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
	/// The forward key encodes the record id losslessly, which is what keeps
	/// them separate. Asserted on encoded record keys rather than
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

	/// Removing one of two records whose ids differ only in a number's kind
	/// leaves the other mapped, and writes nothing under the type-erased key.
	#[tokio::test]
	async fn removing_a_colliding_record_leaves_the_other_mapped() {
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
		assert_eq!(d.get_doc_id(&tx, &int).await.unwrap(), Some(int_id));
		assert_eq!(d.get_doc_id(&tx, &dec).await.unwrap(), None, "the removed record is gone");
		assert_eq!(
			d.get_doc_ids_batch(&tx, &[dec.clone(), int.clone()]).await.unwrap(),
			vec![None, Some(int_id)]
		);
		let erased = DocLookupPrefix::new(NS, DB, Cow::Borrowed(&d.tb)).range().unwrap();
		assert!(
			tx.keys(erased, u32::MAX, 0, None).await.unwrap().is_empty(),
			"nothing reads the type-erased band, so nothing writes it"
		);
		tx.cancel().await.unwrap();
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
		// `!dj`/`!dd` mapping is reclaimed, but the sequence counter is kept so a
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

	/// A forward mapping, present or absent, is read from the store once per
	/// transaction until the transaction writes a forward mapping.
	#[tokio::test]
	async fn a_forward_mapping_is_read_once_per_transaction() {
		let ds = TestIndexStore::new().await;
		{
			let (ctx, d) = new_op(&ds, Write).await;
			d.resolve_or_assign(&ctx, &rid("a")).await.unwrap();
			finish(ctx).await;
		}
		let (ctx, d) = new_op(&ds, Write).await;
		let tx = ctx.tx();
		let gets = || tx.metrics_snapshot_for_test().ops_get;
		let before = gets();
		for _ in 0..3 {
			assert_eq!(d.get_doc_id(&tx, &rid("a")).await.unwrap(), Some(0));
		}
		assert_eq!(gets() - before, 1, "a mapping is read once");
		let before = gets();
		for _ in 0..3 {
			assert_eq!(d.get_doc_id(&tx, &rid("z")).await.unwrap(), None);
		}
		assert_eq!(gets() - before, 1, "an absent mapping is read once");
		tx.cancel().await.unwrap();
	}

	/// A read after this transaction wrote or removed a forward mapping answers
	/// what it wrote, not what it read before.
	#[tokio::test]
	async fn a_forward_mapping_read_sees_this_transactions_writes() {
		let ds = TestIndexStore::new().await;
		let (ctx, d) = new_op(&ds, Write).await;
		let tx = ctx.tx();
		assert_eq!(d.get_doc_id(&tx, &rid("a")).await.unwrap(), None);
		let first = d.resolve_or_assign(&ctx, &rid("a")).await.unwrap();
		assert_eq!(d.get_doc_id(&tx, &rid("a")).await.unwrap(), Some(first));
		assert_eq!(d.remove(&tx, &rid("a")).await.unwrap(), Some(first));
		assert_eq!(d.get_doc_id(&tx, &rid("a")).await.unwrap(), None, "the removal is read");
		let second = d.resolve_or_assign(&ctx, &rid("a")).await.unwrap();
		assert_ne!(first, second, "an id is never reused");
		assert_eq!(d.get_doc_id(&tx, &rid("a")).await.unwrap(), Some(second));
		tx.cancel().await.unwrap();
	}

	/// A mapping read after a write that a rollback to a save point then undoes
	/// is read again, not answered from what was read in between.
	#[tokio::test]
	async fn a_forward_mapping_read_follows_a_save_point_rollback() {
		let ds = TestIndexStore::new().await;
		let (ctx, d) = new_op(&ds, Write).await;
		let tx = ctx.tx();
		tx.new_save_point().await.unwrap();
		let id = d.resolve_or_assign(&ctx, &rid("a")).await.unwrap();
		assert_eq!(d.get_doc_id(&tx, &rid("a")).await.unwrap(), Some(id));
		tx.rollback_to_save_point().await.unwrap();
		assert_eq!(d.get_doc_id(&tx, &rid("a")).await.unwrap(), None, "the rollback is read");
		tx.cancel().await.unwrap();
	}

	/// A claim of a mapping is blind only while this transaction has written no forward mapping
	/// since it read the mapping absent: its own claim in between makes the claim an
	/// existence-checked `put`, which refuses to overwrite it.
	#[tokio::test]
	async fn a_claim_after_this_transactions_own_claim_is_not_blind() {
		let ds = TestIndexStore::new().await;
		let (ctx, d) = new_op(&ds, Write).await;
		let tx = ctx.tx();
		assert!(tx.blind_write_conflicts(), "the memory backend rejects overlapping writes");
		let read_at = tx.doc_lookup_writes();
		assert!(claim_may_be_blind(&tx, read_at), "nothing written since the read");
		assert!(!claim_may_be_blind(&tx, None), "no count stands");
		let first = d.resolve_or_assign(&ctx, &rid("a")).await.unwrap();
		assert!(!claim_may_be_blind(&tx, read_at), "a forward mapping written since the read");
		let put_before = tx.metrics_snapshot_for_test().ops_put;
		assert_eq!(d.resolve_or_assign(&ctx, &rid("a")).await.unwrap(), first, "idempotent");
		assert_eq!(tx.metrics_snapshot_for_test().ops_put, put_before);
		tx.cancel().await.unwrap();
	}

	/// Two claims of one record made through one transaction at once end with one mapping in
	/// each direction.
	#[tokio::test]
	async fn two_claims_in_one_transaction_leave_one_reverse_mapping() {
		let ds = TestIndexStore::new().await;
		let (ctx, d) = new_op(&ds, Write).await;
		let key = rid("a");
		let (a, b) = tokio::join!(d.resolve_or_assign(&ctx, &key), d.resolve_or_assign(&ctx, &key));
		assert_eq!(a.unwrap(), b.unwrap(), "both claims answer the one id");
		let tx = ctx.tx();
		let tb = "t".into();
		let reverse = tx
			.getr(DocKeyPrefix::new(NS, DB, Cow::Borrowed(&tb)).range().unwrap(), None)
			.await
			.unwrap();
		assert_eq!(reverse.len(), 1, "exactly one reverse mapping");
		tx.cancel().await.unwrap();
	}

	/// Where the backend rejects an overlapping write, the claim of a mapping is a blind `set`,
	/// and a claim made on a snapshot another writer has since claimed fails at commit as the
	/// existence-checked `put` does: one mapping survives, the first committer's.
	#[tokio::test]
	async fn a_blind_claim_on_a_stale_snapshot_fails_at_commit() {
		let ds = TestIndexStore::new().await;
		let rid = rid("rec");
		let ctx_b = ds.env(Write).await;
		assert!(
			ctx_b.tx().blind_write_conflicts(),
			"the memory backend rejects overlapping writes"
		);
		let d = TableDocIds::new(NS, DB, "t".into());
		assert_eq!(d.get_doc_id(&ctx_b.tx(), &rid).await.unwrap(), None);
		let a_doc = {
			let ctx_a = ds.env(Write).await;
			let id = d.resolve_or_assign(&ctx_a, &rid).await.unwrap();
			ctx_a.tx().commit().await.unwrap();
			id
		};
		let sets_before = ctx_b.tx().metrics_snapshot_for_test().ops_set;
		d.resolve_or_assign(&ctx_b, &rid).await.unwrap();
		let tx_b = ctx_b.tx();
		let snapshot = tx_b.metrics_snapshot_for_test();
		assert_eq!(snapshot.ops_put, 0, "the claim is not an existence-checked put");
		assert!(snapshot.ops_set > sets_before, "the claim is a blind set");
		assert!(tx_b.commit().await.is_err(), "the commit overlaps the first claim");
		let tx = ds.transaction(TransactionType::Read).await.unwrap();
		assert_eq!(d.get_doc_id(&tx, &rid).await.unwrap(), Some(a_doc));
		let tb = "t".into();
		let reverse = tx
			.getr(DocKeyPrefix::new(NS, DB, Cow::Borrowed(&tb)).range().unwrap(), None)
			.await
			.unwrap();
		assert_eq!(reverse.len(), 1, "exactly one reverse mapping");
		tx.cancel().await.unwrap();
	}
}
