//! Distributed sequence and ID generation management.
//!
//! This module provides a distributed ID generation system that uses a batch allocation
//! strategy to efficiently generate unique identifiers across multiple nodes. The system
//! maintains both state (per-node tracking) and batch allocations (reserved ID ranges)
//! to ensure uniqueness while minimizing coordination overhead.
//!
//! # Key Components
//!
//! - **Sequences**: Main coordinator for all sequence operations
//! - **SequenceDomain**: Defines different types of sequences (namespace IDs, database IDs, etc.)
//! - **BatchValue**: Represents a batch allocation of IDs owned by a specific node
//! - **SequenceState**: Tracks the next available ID for a node
//!
//! # ID Generation Strategy
//!
//! Each node maintains local state and coordinates with other nodes through batch allocations
//! stored in the key-value store. When a node needs IDs, it allocates a batch and uses those
//! IDs locally until the batch is exhausted, then allocates a new batch.

use std::borrow::Cow;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::Result;
use common::EngineError;
use common::time::sleep;
use rand::Rng;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use surrealdb_catalog::providers::{
	CancellationProbe, DatabaseProvider, NamespaceProvider, TableProvider,
};
use surrealdb_catalog::{DatabaseId, IndexId, NamespaceId, SequenceDefinition, TableId};
use surrealdb_kvs::impl_kv_value_revisioned;
use surrealdb_kvs::key::{KVKey, KVKeyDecode, TypedRange};
use surrealdb_strand::TableName;
use tokio::sync::{Mutex, RwLock};
use uuid::Uuid;
use web_time::Instant;

use crate::factory::TransactionFactory;
use crate::key::schema::{
	DbIdBatchKey, DbIdBatchPrefix, DbIdStateKey, DocIdBatchKey, DocIdBatchPrefix, DocIdStateKey,
	FtDocBatchKey, FtDocBatchPrefix, FtDocStateKey, IndexIdBatchKey, IndexIdBatchPrefix,
	IndexIdStateKey, NsIdBatchKey, NsIdBatchPrefix, NsIdStateKey, SeqBatchKey, SeqBatchPrefix,
	SeqStateKey, SeqStatePrefix, SequenceKey, TbIdBatchKey, TbIdBatchPrefix, TbIdStateKey,
};
use crate::tx::ProvisionalSequence;
use crate::values::ids::DocId;
use crate::{Transaction, TransactionType};

type SequencesMap = Arc<RwLock<HashMap<Arc<SequenceDomain>, Arc<CachedSequence>>>>;

/// An allocator held in a node's map, together with whether it has left it.
///
/// A draw takes an allocator out of the map and locks it afterwards, so it can
/// still be drawn from once it has been removed — by a statement that redefined
/// or removed its sequence, or by a draw that found it reset. Removed, it must
/// not serve again. The next allocator built for the same sequence on this node
/// writes the same cursor row, and can leave it holding exactly the value the
/// removed one last wrote; the conditional write that catches a reset then
/// succeeds, and the removed allocator resumes inside a window the sequence no
/// longer has while other nodes claim the same values from the new run.
struct CachedSequence {
	/// Set when this allocator leaves the map. See [`Sequence::next`].
	evicted: AtomicBool,
	sequence: Mutex<Sequence>,
}

impl CachedSequence {
	fn new(sequence: Sequence) -> Self {
		Self {
			evicted: AtomicBool::new(false),
			sequence: Mutex::new(sequence),
		}
	}

	/// Marks this allocator as no longer the one serving its sequence. Called
	/// with the map's write lock held, so the mark precedes any allocator built
	/// to replace it.
	fn evict(&self) {
		self.evicted.store(true, Ordering::Release);
	}
}

/// The lowest value a user-defined sequence can hand out without repeating one
/// it has already issued.
///
/// A sequence's position is not a single number: every node keeps its own
/// cursor under `!st{nid}`, holding the value it would serve next, and
/// [`SequenceDomain::requires_dense_ids`] makes that cursor durable per
/// allocation for `sequence::nextval`. The highest cursor therefore bounds
/// every value the sequence has issued anywhere in the cluster.
///
/// A node that claimed a window and stopped before its first allocation leaves
/// a reserved batch above every cursor. Those values were never served, so they
/// are deliberately not counted here: the result is the resume point for a
/// sequence being re-established from scratch, not a claim over live windows.
///
/// # Arguments
/// * `tx` - The transaction to read the cursors through
/// * `ns` / `db` - The database owning the sequence
/// * `sq` - The sequence name
/// * `start` - The declared `START`, returned when the sequence has never run
/// * `version` - Read the cursors at this version, as the caller read the definition it passes as
///   `start`. Mixing the two reports a historical definition beside a present-day position.
///
/// # Returns
/// `start` raised to the highest cursor recorded for the sequence.
pub async fn next_unissued_value(
	tx: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	sq: &str,
	start: i64,
	version: Option<u64>,
) -> Result<i64> {
	let range = SeqStatePrefix {
		ns,
		db,
		sq: Cow::Borrowed(sq),
	}
	.range()?;
	let mut next = start;
	for (_, st) in tx.getr(range, version).await? {
		next = next.max(st.next);
	}
	Ok(next)
}

/// What a claim is checked against before it is committed.
///
/// A window only belongs to the sequence if both of these are still in the
/// store: the definition the allocator was built from, and the cursor it last
/// made durable. Every statement that resets a sequence takes at least one of
/// them away — an `OVERWRITE` that rewrites the same definition leaves the
/// first intact and deletes the second — so the pair is what the claim needs,
/// not either alone.
///
/// `None` on either means there is nothing to compare against: a domain with
/// no definition, or an allocator that has not written a cursor yet. A
/// provisional claim — one with a `caller` — is checked against neither: its
/// sequence is defined only inside that uncommitted transaction, where nothing
/// else can reset it.
#[derive(Clone, Copy)]
struct ClaimGuard<'a> {
	definition: Option<&'a SequenceDefinition>,
	cursor: Option<&'a SequenceState>,
	/// The transaction a provisional claim is written through, and read in.
	/// `None` for an allocator that claims in a transaction of its own.
	caller: Option<&'a Transaction>,
}

/// The window a batch claim settled on.
struct BatchAllocation {
	/// The first value to serve.
	from: i64,
	/// The exclusive end of the window.
	to: i64,
	/// The start the window's claim row is keyed by, which
	/// [`Sequence::window`] verifies against. Equal to `from` unless an owned
	/// window is being resumed part-way through.
	window: i64,
}

/// A transaction for one step of the allocator, and who ends it.
///
/// A provisional allocator works inside the transaction that defined its
/// sequence: what it writes has to become durable with that definition and be
/// undone with it, so it must not commit or cancel that transaction itself.
/// Every other allocator opens a transaction of its own — which also keeps it
/// off the parent transaction under strict serialization (e.g. FDB) — and ends
/// it in the same step.
///
/// [`Self::commit`] and [`Self::cancel`] do nothing in the first case, so each
/// step spells its own outcome once and the distinction lives here.
#[expect(
	clippy::large_enum_variant,
	reason = "a single short-lived local per allocator step, never collected or passed by \
	          value in bulk; boxing it would put a heap allocation in the path of every draw"
)]
enum AllocatorTx<'a> {
	/// The transaction that defined the sequence. Ending it belongs to
	/// whoever opened it, not to the allocator.
	Caller(&'a Transaction),
	/// Opened for this step alone, and ended by it.
	Own(Transaction),
}

impl<'a> AllocatorTx<'a> {
	/// Borrows `caller` if there is one, otherwise opens a `kind` transaction.
	async fn open(
		caller: Option<&'a Transaction>,
		tf: &TransactionFactory,
		kind: TransactionType,
		sqs: &Sequences,
	) -> SequenceResult<Self> {
		Ok(match caller {
			Some(tx) => Self::Caller(tx),
			None => Self::Own(tf.transaction(kind, sqs.clone()).await?),
		})
	}

	/// The transaction to run this step's reads and writes in.
	fn tx(&self) -> &Transaction {
		match self {
			Self::Caller(tx) => tx,
			Self::Own(tx) => tx,
		}
	}

	/// Commits, if this transaction is the allocator's to end.
	async fn commit(self) -> SequenceResult<()> {
		if let Self::Own(tx) = self {
			tx.commit().await?;
		}
		Ok(())
	}

	/// Cancels, if this transaction is the allocator's to end.
	async fn cancel(self) -> SequenceResult<()> {
		if let Self::Own(tx) = self {
			tx.cancel().await?;
		}
		Ok(())
	}
}

/// What an allocator's own operations fail with.
///
/// The variant exists so that the one failure a caller must not simply
/// propagate is the one the type makes it name. Everything else is carried
/// through unchanged.
#[derive(Debug)]
enum SequenceError {
	/// The sequence was redefined or removed while this allocator was serving
	/// it, so the allocator is stale and has to be rebuilt.
	///
	/// Only `DEFINE SEQUENCE` and `REMOVE SEQUENCE` reset a sequence, and the
	/// reset reaches an allocator in three shapes that mean the same thing
	/// here: a conditional write finding the value it required already gone,
	/// the commit losing a conflict to the reset on a key both of them wrote,
	/// or a draw from an allocator that has left the node's map (see
	/// [`CachedSequence`]). None of them issued an id — the transaction that
	/// would have made the cursor durable is the one that failed or was
	/// cancelled.
	Reset,
	Other(anyhow::Error),
}

/// The result of an allocator operation that can discover it has been reset.
type SequenceResult<T> = std::result::Result<T, SequenceError>;

impl std::fmt::Display for SequenceError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Reset => f.write_str(
				"the sequence was redefined or removed while this allocator was serving it",
			),
			Self::Other(e) => write!(f, "{e}"),
		}
	}
}

impl From<anyhow::Error> for SequenceError {
	fn from(err: anyhow::Error) -> Self {
		Self::Other(err)
	}
}

impl SequenceError {
	/// The failure to hand to code outside this module.
	///
	/// Deliberately not a [`std::error::Error`] impl and so not an
	/// `anyhow::Error` conversion: nothing outside these functions can name
	/// `SequenceError`, let alone match on it, so letting `?` raise one would
	/// put a private type in front of callers who could only print it. Leaving
	/// this module is an explicit step, and the reset becomes the failure the
	/// datastore already has a name for.
	fn into_public(self) -> anyhow::Error {
		match self {
			Self::Other(err) => err,
			Self::Reset => anyhow::Error::new(crate::error::DatastoreError::SequenceReset),
		}
	}
}

/// Whether an error that has left the allocator is a reset that survived the
/// rebuild [`Sequences::next_val`] performs.
#[cfg(test)]
fn is_sequence_reset(err: &anyhow::Error) -> bool {
	err.chain().any(|e| {
		matches!(
			e.downcast_ref::<crate::error::DatastoreError>(),
			Some(crate::error::DatastoreError::SequenceReset)
		)
	})
}

/// Manager for all sequence operations in the system.
///
/// The Sequences struct coordinates ID generation across different domains
/// (namespaces, databases, tables, indexes, and user sequences) and manages
/// the lifecycle of sequence allocations.
#[derive(Clone)]
pub struct Sequences {
	tf: TransactionFactory,
	nid: Uuid,
	sequences: SequencesMap,
}

/// Defines the different types of sequences supported by the system.
///
/// Each variant represents a distinct ID generation domain with its own
/// namespace and allocation strategy.
#[derive(Hash, PartialEq, Eq)]
enum SequenceDomain {
	/// A user-defined sequence in a database
	UserName(NamespaceId, DatabaseId, String),
	/// A sequence generating table-level DocIds shared by all indexes on a table
	TableDocIds(NamespaceId, DatabaseId, TableName),
	/// A sequence generating IDs for namespaces
	NameSpacesIds,
	/// A sequence generating IDs for databases
	DatabasesIds(NamespaceId),
	/// A sequence generating IDs for tables
	TablesIds(NamespaceId, DatabaseId),
	/// A sequence generating IDs for indexes
	IndexIds(NamespaceId, DatabaseId, TableName),
	/// The private full-text doc-ID allocator used by pre-format-1 indexes.
	LegacyFullTextDocIds(NamespaceId, DatabaseId, TableName, IndexId),
}

impl SequenceDomain {
	fn new_user(ns: NamespaceId, db: DatabaseId, sq: &str) -> Self {
		Self::UserName(ns, db, sq.to_string())
	}

	pub fn new_table_doc_ids(ns: NamespaceId, db: DatabaseId, tb: TableName) -> Self {
		Self::TableDocIds(ns, db, tb)
	}

	pub fn new_namespace_ids() -> Self {
		Self::NameSpacesIds
	}

	pub fn new_database_ids(ns: NamespaceId) -> Self {
		Self::DatabasesIds(ns)
	}

	pub fn new_table_ids(ns: NamespaceId, db: DatabaseId) -> Self {
		Self::TablesIds(ns, db)
	}

	pub fn new_index_ids(ns: NamespaceId, db: DatabaseId, tb: TableName) -> Self {
		Self::IndexIds(ns, db, tb)
	}

	pub fn new_legacy_full_text_doc_ids(
		ns: NamespaceId,
		db: DatabaseId,
		tb: TableName,
		ix: IndexId,
	) -> Self {
		Self::LegacyFullTextDocIds(ns, db, tb, ix)
	}

	fn new_batch_range_keys(&self) -> Result<TypedRange<BatchValue>> {
		match self {
			Self::UserName(ns, db, sq) => SeqBatchPrefix {
				ns: *ns,
				db: *db,
				sq: Cow::Borrowed(sq),
			}
			.range(),
			Self::TableDocIds(ns, db, tb) => {
				DocIdBatchPrefix::new(*ns, *db, Cow::Borrowed(tb)).range()
			}
			Self::NameSpacesIds => NsIdBatchPrefix {}.range(),
			Self::DatabasesIds(ns) => DbIdBatchPrefix {
				ns: *ns,
			}
			.range(),
			Self::TablesIds(ns, db) => TbIdBatchPrefix {
				ns: *ns,
				db: *db,
			}
			.range(),
			Self::IndexIds(ns, db, tb) => IndexIdBatchPrefix {
				ns: *ns,
				db: *db,
				tb: Cow::Borrowed(tb),
			}
			.range(),
			Self::LegacyFullTextDocIds(ns, db, tb, ix) => {
				FtDocBatchPrefix::new(*ns, *db, Cow::Borrowed(tb), *ix).range()
			}
		}
	}

	/// Whether this domain has to hand out a dense, gap-free run of ids.
	///
	/// Only the user-facing sequence behind `sequence::nextval` promises that:
	/// its conformance suite fixes `BATCH` as a reservation size rather than a
	/// per-connection stride, so a caller may rely on consecutive values while
	/// the node stays up. Every other domain allocates surrogate keys that
	/// consumers look up rather than count — the shared doc-ID space says so
	/// where it assigns them, and the catalog domains already seed past ids in
	/// use — so a skipped id costs nothing there.
	///
	/// Density is bought with one durable commit per allocation, because every
	/// id in between has to be recoverable on its own. A domain that does not
	/// need it must not pay for it: an indexed write allocates a doc-ID per
	/// record, so that commit would land on the hot write path.
	///
	/// Density does not survive every crash, and cannot. A node that claims a
	/// window and stops before its first per-allocation commit leaves behind a
	/// cursor that predates the claim, and the resume bound in
	/// [`Sequence::check_batch_allocation`] raises that cursor to the window's
	/// own start rather than trusting it, because the range beneath it may
	/// since have been claimed by a peer. The ids in between are skipped by
	/// this node; the alternative is handing out a peer's ids a second time.
	fn requires_dense_ids(&self) -> bool {
		matches!(self, Self::UserName(..))
	}

	fn new_batch_key(&self, start: i64) -> SequenceBatchKey<'_> {
		match &self {
			Self::UserName(ns, db, sq) => SequenceBatchKey::User(SeqBatchKey {
				ns: *ns,
				db: *db,
				sq: Cow::Borrowed(sq.as_str()),
				start,
			}),
			Self::TableDocIds(ns, db, tb) => SequenceBatchKey::TableDocIds(DocIdBatchKey::new(
				*ns,
				*db,
				Cow::Borrowed(tb),
				start,
			)),
			Self::NameSpacesIds => SequenceBatchKey::Namespaces(NsIdBatchKey {
				start,
			}),
			Self::DatabasesIds(ns) => SequenceBatchKey::Databases(DbIdBatchKey {
				ns: *ns,
				start,
			}),
			Self::TablesIds(ns, db) => SequenceBatchKey::Tables(TbIdBatchKey {
				ns: *ns,
				db: *db,
				start,
			}),
			Self::IndexIds(ns, db, tb) => SequenceBatchKey::Indexes(IndexIdBatchKey {
				ns: *ns,
				db: *db,
				tb: Cow::Borrowed(tb),
				start,
			}),
			Self::LegacyFullTextDocIds(ns, db, tb, ix) => SequenceBatchKey::LegacyFullTextDocIds(
				FtDocBatchKey::new(*ns, *db, Cow::Borrowed(tb), *ix, start),
			),
		}
	}

	/// Reads back a batch key this domain's own range produced.
	///
	/// The domain says which of the six families the bytes belong to, so the
	/// dispatch is the same one [`Self::new_batch_key`] makes, run the other way.
	/// A batch's `start` lives in its key rather than in [`BatchValue`], so this is
	/// also the only way to learn which range a scanned entry reserved.
	fn decode_batch_key<'a>(&self, bytes: &'a [u8]) -> Result<SequenceBatchKey<'a>> {
		Ok(match &self {
			Self::UserName(..) => SequenceBatchKey::User(SeqBatchKey::decode_key(bytes)?),
			Self::TableDocIds(..) => {
				SequenceBatchKey::TableDocIds(DocIdBatchKey::decode_key(bytes)?)
			}
			Self::NameSpacesIds => SequenceBatchKey::Namespaces(NsIdBatchKey::decode_key(bytes)?),
			Self::DatabasesIds(..) => SequenceBatchKey::Databases(DbIdBatchKey::decode_key(bytes)?),
			Self::TablesIds(..) => SequenceBatchKey::Tables(TbIdBatchKey::decode_key(bytes)?),
			Self::IndexIds(..) => SequenceBatchKey::Indexes(IndexIdBatchKey::decode_key(bytes)?),
			Self::LegacyFullTextDocIds(..) => {
				SequenceBatchKey::LegacyFullTextDocIds(FtDocBatchKey::decode_key(bytes)?)
			}
		})
	}

	/// The state key for `nid`, owning its fields so it can be held for the life of
	/// the [`Sequence`] that writes through it.
	fn new_state_key(&self, nid: Uuid) -> SequenceStateKey<'static> {
		match &self {
			Self::UserName(ns, db, sq) => SequenceStateKey::User(SeqStateKey {
				ns: *ns,
				db: *db,
				sq: Cow::Owned(sq.clone()),
				nid,
			}),
			Self::TableDocIds(ns, db, tb) => SequenceStateKey::TableDocIds(DocIdStateKey::new(
				*ns,
				*db,
				Cow::Owned(tb.clone()),
				nid,
			)),
			Self::NameSpacesIds => SequenceStateKey::Namespaces(NsIdStateKey {
				nid,
			}),
			Self::DatabasesIds(ns) => SequenceStateKey::Databases(DbIdStateKey {
				ns: *ns,
				nid,
			}),
			Self::TablesIds(ns, db) => SequenceStateKey::Tables(TbIdStateKey {
				ns: *ns,
				db: *db,
				nid,
			}),
			Self::IndexIds(ns, db, tb) => SequenceStateKey::Indexes(IndexIdStateKey {
				ns: *ns,
				db: *db,
				tb: Cow::Owned(tb.clone()),
				nid,
			}),
			Self::LegacyFullTextDocIds(ns, db, tb, ix) => SequenceStateKey::LegacyFullTextDocIds(
				FtDocStateKey::new(*ns, *db, Cow::Owned(tb.clone()), *ix, nid),
			),
		}
	}
}

/// The batch key of whichever family a [`SequenceDomain`] names.
///
/// A domain chooses between six key families, and one function cannot return six
/// different `impl KVKey`. This enum is that return type. It declares no layout
/// of its own and spells no bytes: every method forwards to the generated key it
/// wraps. What it buys is that the family stays a type all the way to the
/// transaction, so the value written under it is `BatchValue` by the compiler's
/// reckoning rather than by the call site's.
#[derive(Debug)]
enum SequenceBatchKey<'a> {
	User(SeqBatchKey<'a>),
	TableDocIds(DocIdBatchKey<'a>),
	Namespaces(NsIdBatchKey),
	Databases(DbIdBatchKey),
	Tables(TbIdBatchKey),
	Indexes(IndexIdBatchKey<'a>),
	LegacyFullTextDocIds(FtDocBatchKey<'a>),
}

impl SequenceBatchKey<'_> {
	/// The inclusive lower bound of the window this key names.
	///
	/// A window's start is encoded only in its key, never in the
	/// [`BatchValue`] stored under it, so a resume point taken from a scanned
	/// row has to read the bound back from here.
	fn start(&self) -> i64 {
		match self {
			Self::User(k) => k.start,
			Self::TableDocIds(k) => k.start,
			Self::Namespaces(k) => k.start,
			Self::Databases(k) => k.start,
			Self::Tables(k) => k.start,
			Self::Indexes(k) => k.start,
			Self::LegacyFullTextDocIds(k) => k.start,
		}
	}
}

impl KVKey for SequenceBatchKey<'_> {
	type Value = BatchValue;

	fn encode_buffer(&self, buffer: &mut Vec<u8>) -> Result<()> {
		match self {
			Self::User(k) => k.encode_buffer(buffer),
			Self::TableDocIds(k) => k.encode_buffer(buffer),
			Self::Namespaces(k) => k.encode_buffer(buffer),
			Self::Databases(k) => k.encode_buffer(buffer),
			Self::Tables(k) => k.encode_buffer(buffer),
			Self::Indexes(k) => k.encode_buffer(buffer),
			Self::LegacyFullTextDocIds(k) => k.encode_buffer(buffer),
		}
	}

	fn value_context(&self) {}
}

/// The per-node state key of whichever family a [`SequenceDomain`] names. See
/// [`SequenceBatchKey`].
#[derive(Clone, Debug)]
enum SequenceStateKey<'a> {
	User(SeqStateKey<'a>),
	TableDocIds(DocIdStateKey<'a>),
	Namespaces(NsIdStateKey),
	Databases(DbIdStateKey),
	Tables(TbIdStateKey),
	Indexes(IndexIdStateKey<'a>),
	LegacyFullTextDocIds(FtDocStateKey<'a>),
}

impl KVKey for SequenceStateKey<'_> {
	type Value = SequenceState;

	fn encode_buffer(&self, buffer: &mut Vec<u8>) -> Result<()> {
		match self {
			Self::User(k) => k.encode_buffer(buffer),
			Self::TableDocIds(k) => k.encode_buffer(buffer),
			Self::Namespaces(k) => k.encode_buffer(buffer),
			Self::Databases(k) => k.encode_buffer(buffer),
			Self::Tables(k) => k.encode_buffer(buffer),
			Self::Indexes(k) => k.encode_buffer(buffer),
			Self::LegacyFullTextDocIds(k) => k.encode_buffer(buffer),
		}
	}

	fn value_context(&self) {}
}

/// Represents a batch allocation of IDs in the key-value store.
///
/// A batch allocation reserves a range of IDs for a specific node (identified by `owner`).
/// The range is from some starting value (stored in the key) up to (but not including) `to`.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
pub struct BatchValue {
	/// The exclusive upper bound of the batch allocation
	to: i64,
	/// The UUID of the node that owns this batch allocation
	owner: Uuid,
}
impl_kv_value_revisioned!(BatchValue);

impl BatchValue {
	/// Only the allocator builds these in a running store; the crate above
	/// needs the constructor to pin the encoding against frozen fixtures.
	#[cfg(feature = "test-hooks")]
	#[doc(hidden)]
	pub fn new(to: i64, owner: Uuid) -> Self {
		Self {
			to,
			owner,
		}
	}
}

/// Tracks the next available ID for a specific node in a sequence.
///
/// Each node maintains its own `SequenceState` which tracks the next ID it will
/// allocate from its current batch. This state is persisted to coordinate with
/// batch allocations and ensure no ID is used twice.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
pub struct SequenceState {
	/// The next ID to be allocated by this node
	next: i64,
}
impl_kv_value_revisioned!(SequenceState);

impl SequenceState {
	/// Only the allocator builds these in a running store; the crate above
	/// needs the constructor to pin the encoding against frozen fixtures.
	#[cfg(feature = "test-hooks")]
	#[doc(hidden)]
	pub fn new(next: i64) -> Self {
		Self {
			next,
		}
	}
}

impl Sequences {
	pub fn new(tf: TransactionFactory, nid: Uuid) -> Self {
		Self {
			tf,
			sequences: Arc::new(Default::default()),
			nid,
		}
	}

	/// Cleans up all sequences associated with a removed namespace.
	///
	/// This method is called when a namespace is deleted to remove all cached
	/// sequence state for databases within that namespace.
	pub async fn namespace_removed(&self, tx: &Transaction, ns: NamespaceId) -> Result<()> {
		for db in tx.all_db(ns, None).await?.iter() {
			self.database_removed(tx, ns, db.database_id).await?;
		}
		Ok(())
	}

	/// Cleans up all sequences associated with a removed database.
	///
	/// This method is called when a database is deleted to remove all cached
	/// sequence state for user-defined sequences within that database.
	pub async fn database_removed(
		&self,
		tx: &Transaction,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<()> {
		for sqs in tx.all_db_sequences(ns, db, None).await?.iter() {
			self.sequence_removed(ns, db, &sqs.name).await;
		}
		Ok(())
	}

	/// Removes a user-defined sequence's allocator from this node's map, and
	/// marks it evicted.
	///
	/// Called when a statement redefines or removes the sequence. The mark is
	/// what stops a draw that already holds the allocator from serving out of
	/// it once the statement commits — see [`CachedSequence`].
	pub async fn sequence_removed(&self, ns: NamespaceId, db: DatabaseId, sq: &str) {
		let key = SequenceDomain::new_user(ns, db, sq);
		let mut cached = self.sequences.write().await;
		if let Some(s) = cached.remove(&key) {
			s.evict();
		}
	}

	/// Core internal method for retrieving the next value from a sequence.
	///
	/// This method coordinates sequence loading, caching, and value generation.
	/// It ensures that only one Sequence instance exists per domain by checking
	/// the cache first, then loading if needed.
	///
	/// # Arguments
	/// * `ctx` - Optional mutable context for timeout checking
	/// * `seq` - The sequence domain to generate values from
	/// * `start` - The starting value if the sequence hasn't been initialized
	/// * `batch` - The batch size for ID allocations
	/// * `timeout` - Optional timeout for batch allocation operations
	///
	/// # Returns
	/// The next sequential value
	async fn next_val(
		&self,
		ctx: Option<&dyn CancellationProbe>,
		seq: Arc<SequenceDomain>,
		start: i64,
		batch: u32,
		timeout: Option<Duration>,
	) -> Result<i64> {
		let sequence = self.sequences.read().await.get(&seq).cloned();
		if let Some(s) = sequence {
			let res =
				s.sequence.lock().await.next(self, ctx, &seq, batch, None, Some(&s.evicted)).await;
			match res {
				// The sequence was redefined or removed under this allocator,
				// here or on a peer. Drop it and build it again below, which
				// reads the definition as it stands rather than trusting the
				// `start` reaching this call — that one was read before the
				// statement that caused the reset.
				Err(SequenceError::Reset) => {
					self.evict_this_allocator(&seq, &s).await;
				}
				res => return res.map_err(SequenceError::into_public),
			}
			// That was this call's one rebuild.
			return self
				.build_and_next(ctx, &seq, start, batch, timeout)
				.await
				.map_err(SequenceError::into_public);
		}
		// Nothing was cached, so nothing has been rebuilt yet. Building reads
		// the definition and the cursor and then claims a window against them,
		// and a reset committing across that gap fails the claim just as it
		// fails a cached allocator's write. Give it the same single rebuild
		// rather than reporting the first one: a reset hitting the fresh
		// allocator too means another statement landed in the same window, and
		// reporting that beats looping against a sequence somebody is actively
		// redefining.
		match self.build_and_next(ctx, &seq, start, batch, timeout).await {
			Err(SequenceError::Reset) => self
				.build_and_next(ctx, &seq, start, batch, timeout)
				.await
				.map_err(SequenceError::into_public),
			res => res.map_err(SequenceError::into_public),
		}
	}

	/// Builds the allocator for a domain that has none cached, and serves one
	/// value from it.
	///
	/// An allocator that reports a reset is removed again before returning, so
	/// that a caller retrying builds a fresh one rather than finding this one
	/// in the map.
	async fn build_and_next(
		&self,
		ctx: Option<&dyn CancellationProbe>,
		seq: &Arc<SequenceDomain>,
		start: i64,
		batch: u32,
		timeout: Option<Duration>,
	) -> SequenceResult<i64> {
		let s = match self.sequences.write().await.entry(Arc::clone(seq)) {
			// Somebody else built one while this call was waiting for the
			// lock. Serve from theirs rather than replacing it.
			Entry::Occupied(e) => Arc::clone(e.get()),
			Entry::Vacant(e) => {
				let s = Arc::new(CachedSequence::new(
					Sequence::load(ctx, self, seq, start, batch, timeout).await?,
				));
				Arc::clone(e.insert(s))
			}
		};
		let res = s.sequence.lock().await.next(self, ctx, seq, batch, None, Some(&s.evicted)).await;
		if matches!(res, Err(SequenceError::Reset)) {
			self.evict_this_allocator(seq, &s).await;
		}
		res
	}

	/// Removes `s` from the map, and only `s`, and marks it evicted.
	///
	/// Several tasks queue on one allocator, so they meet the same reset one
	/// after another. Removing whatever the map holds would have each of them
	/// evict the allocator its predecessor had just rebuilt, and every task
	/// after the first would pay a failed write, a load and a claim to arrive
	/// back where it started.
	///
	/// The mark is what stops the ones still queued on `s`. The rebuild that
	/// follows writes the same cursor row, and can put back the value `s`
	/// expects there — see [`CachedSequence`].
	async fn evict_this_allocator(&self, seq: &Arc<SequenceDomain>, s: &Arc<CachedSequence>) {
		let mut cached = self.sequences.write().await;
		s.evict();
		if let Entry::Occupied(e) = cached.entry(Arc::clone(seq))
			&& Arc::ptr_eq(e.get(), s)
		{
			e.remove();
		}
	}

	/// Generates the next namespace ID.
	///
	/// # Arguments
	/// * `ctx` - Optional mutable context for transaction operations
	///
	/// # Returns
	/// A new unique namespace ID
	pub async fn next_namespace_id(
		&self,
		ctx: Option<&dyn CancellationProbe>,
	) -> Result<NamespaceId> {
		let domain = Arc::new(SequenceDomain::new_namespace_ids());
		let id = self.next_val(ctx, domain, 0, 100, None).await?;
		Ok(NamespaceId(id as u32))
	}

	/// Generates the next database ID within a namespace.
	///
	/// # Arguments
	/// * `ctx` - Optional mutable context for transaction operations
	/// * `ns` - The namespace ID to generate the database ID within
	///
	/// # Returns
	/// A new unique database ID for the given namespace
	pub async fn next_database_id(
		&self,
		ctx: Option<&dyn CancellationProbe>,
		ns: NamespaceId,
	) -> Result<DatabaseId> {
		let domain = Arc::new(SequenceDomain::new_database_ids(ns));
		let id = self.next_val(ctx, domain, 0, 100, None).await?;
		Ok(DatabaseId(id as u32))
	}

	/// Generates the next table ID within a database.
	///
	/// # Arguments
	/// * `ctx` - Optional mutable context for transaction operations
	/// * `ns` - The namespace ID
	/// * `db` - The database ID to generate the table ID within
	///
	/// # Returns
	/// A new unique table ID for the given database
	pub async fn next_table_id(
		&self,
		ctx: Option<&dyn CancellationProbe>,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<TableId> {
		let domain = Arc::new(SequenceDomain::new_table_ids(ns, db));
		let id = self.next_val(ctx, domain, 0, 100, None).await?;
		Ok(TableId(id as u32))
	}

	/// Generates the next index ID within a table.
	///
	/// # Arguments
	/// * `ctx` - Optional mutable context for transaction operations
	/// * `ns` - The namespace ID
	/// * `db` - The database ID
	/// * `tb` - The table name to generate the index ID within
	///
	/// # Returns
	/// A new unique index ID for the given table
	pub async fn next_index_id(
		&self,
		ctx: Option<&dyn CancellationProbe>,
		ns: NamespaceId,
		db: DatabaseId,
		tb: TableName,
	) -> Result<IndexId> {
		let domain = Arc::new(SequenceDomain::new_index_ids(ns, db, tb));
		let id = self.next_val(ctx, domain, 0, 100, None).await?;
		Ok(IndexId(id as u32))
	}

	/// Generates the next value for a user-defined sequence.
	///
	/// # Arguments
	/// * `ctx` - Optional mutable context for transaction operations
	/// * `tx` - The transaction to use for accessing sequence configuration
	/// * `ns` - The namespace ID
	/// * `db` - The database ID
	/// * `sq` - The sequence name
	///
	/// # Returns
	/// The next value in the user-defined sequence
	pub async fn next_user_sequence_id(
		&self,
		ctx: Option<&dyn CancellationProbe>,
		tx: &Transaction,
		ns: NamespaceId,
		db: DatabaseId,
		sq: &str,
	) -> Result<i64> {
		// Read so a missing sequence is reported before any allocator is built;
		// `Sequence::load` reads it again, and that read is the authoritative
		// one, because this one can be overtaken by a `DEFINE SEQUENCE` before
		// the allocation happens.
		let seq_def = tx.get_db_sequence(ns, db, sq, None).await?;
		// A sequence this transaction defined and has not committed is served
		// by the transaction, from an allocator it owns. Only this transaction
		// can say that: from the store an uncommitted `DEFINE` and a definition
		// a concurrent statement replaced look the same, and they want opposite
		// answers. Nothing else may draw from an allocator built on a
		// definition nobody else can see — and nothing has to unwind it either,
		// because it goes when this transaction goes.
		if let Some(slot) = tx.provisional_sequence(ns, db, sq).await {
			let domain = SequenceDomain::new_user(ns, db, sq);
			return self.next_provisional_val(ctx, tx, &slot, &domain, &seq_def).await;
		}
		let domain = Arc::new(SequenceDomain::new_user(ns, db, sq));
		self.next_val(ctx, domain, seq_def.start, seq_def.batch, seq_def.timeout).await
	}

	/// Serves a draw from a sequence `tx` has defined and not committed, from
	/// an allocator `tx` owns.
	///
	/// The allocator is built on the first draw and kept for the rest of the
	/// transaction, so consecutive draws come out of one window as they do
	/// anywhere else. It is never published to the node's map: it has no
	/// committed definition to check its claims against, which is sound only
	/// while the transaction that wrote that definition is open.
	///
	/// The build happens under the slot's own lock, which the draw then reuses.
	/// Building claims a window and writes it through `tx`, so two draws that
	/// both found the slot empty would both claim, and the second would collide
	/// on the row the first created. See [`ProvisionalSequence`].
	async fn next_provisional_val(
		&self,
		ctx: Option<&dyn CancellationProbe>,
		tx: &Transaction,
		slot: &ProvisionalSequence,
		domain: &SequenceDomain,
		seq_def: &SequenceDefinition,
	) -> Result<i64> {
		let mut slot = slot.lock().await;
		let allocator = match &mut *slot {
			Some(allocator) => allocator,
			none => none.insert(
				Sequence::load_provisional(ctx, self, domain, seq_def, tx)
					.await
					.map_err(SequenceError::into_public)?,
			),
		};
		allocator
			.next(self, ctx, domain, seq_def.batch, Some(tx), None)
			.await
			.map_err(SequenceError::into_public)
	}

	/// Generates the next document ID for a table's shared doc-ID space.
	///
	/// # Arguments
	/// * `ctx` - Optional mutable context for transaction operations
	/// * `ns` - The namespace ID
	/// * `db` - The database ID
	/// * `tb` - The table the doc-ID space belongs to
	/// * `batch` - The batch size for ID allocation
	///
	/// # Returns
	/// A new unique, monotonic document ID for the table
	pub async fn next_table_doc_id(
		&self,
		ctx: Option<&dyn CancellationProbe>,
		ns: NamespaceId,
		db: DatabaseId,
		tb: TableName,
		batch: u32,
	) -> Result<DocId> {
		let domain = Arc::new(SequenceDomain::new_table_doc_ids(ns, db, tb));
		let id = self.next_val(ctx, domain, 0, batch, None).await?;
		Ok(id as DocId)
	}

	/// Generates the next document ID for a legacy full-text index's private space.
	pub async fn next_legacy_full_text_doc_id(
		&self,
		ctx: Option<&dyn CancellationProbe>,
		ns: NamespaceId,
		db: DatabaseId,
		tb: TableName,
		ix: IndexId,
	) -> Result<DocId> {
		let domain = Arc::new(SequenceDomain::new_legacy_full_text_doc_ids(ns, db, tb, ix));
		let id = self.next_val(ctx, domain, 0, 100, None).await?;
		Ok(id as DocId)
	}
}

/// Internal per-node sequence state manager.
///
/// This struct manages the local state for a specific sequence on a specific node.
/// It tracks the current position within an allocated batch and coordinates with
/// the distributed batch allocation system when the current batch is exhausted.
#[derive(Clone)]
pub(crate) struct Sequence {
	/// Transaction factory for creating transactions to persist state
	tf: TransactionFactory,
	/// The current state tracking the next ID to allocate
	st: SequenceState,
	/// Optional timeout for batch allocation operations
	timeout: Option<Duration>,
	/// The exclusive upper bound of the current batch allocation
	to: i64,
	/// The key used to persist this sequence's state
	state_key: SequenceStateKey<'static>,
	/// The cursor this allocator last made durable under [`Self::state_key`],
	/// or `None` if it has not written one yet.
	///
	/// Every write of the cursor is conditional on finding this value still
	/// there, which is how an allocator learns that a `DEFINE`/`REMOVE
	/// SEQUENCE` has cleared its row — on this node or any other, without
	/// anything having to tell it. See [`SequenceError::Reset`].
	///
	/// The condition cannot tell a row still holding this value from one put
	/// back holding it, and the run that follows a reset can do exactly that:
	/// the row belongs to this node, so the next allocator for the sequence
	/// here writes it too. That case is caught by the eviction mark instead —
	/// see [`CachedSequence`].
	durable: Option<SequenceState>,
	/// The definition this allocator was built from, for a user-defined
	/// sequence; `None` for the domains that have none.
	///
	/// Every batch claim is conditional on this still being the definition in
	/// force. Reading it inside the claim's own transaction is what closes the
	/// gap between settling the start and committing the window: a reposition
	/// landing in between clears the claims, so the claim would otherwise
	/// recompute its start from a definition already replaced and re-hand ids
	/// the sequence had issued.
	///
	/// The read settles what to compare against; it does not by itself make a
	/// concurrent `DEFINE SEQUENCE` conflict, because a read joins the conflict
	/// set only under serializable snapshot isolation and these backends do not
	/// use it. The claim writes the definition back unchanged after comparing
	/// it, and that write is what puts the key in both write sets — which is
	/// why an apparently redundant rewrite is there. A provisional allocator
	/// carries none of this: it has no committed definition to compare against,
	/// and nothing outside the transaction that wrote its definition can reset
	/// it.
	definition: Option<SequenceDefinition>,
	/// The start the claim row of this allocator's window is keyed by.
	///
	/// Its existence is what says the window is still the sequence's. Every
	/// statement that resets a sequence deletes the claim rows, whatever it
	/// leaves the definition looking like, so this sees a reset that rewrote
	/// the definition byte-for-byte and a removal followed by a redefinition,
	/// neither of which comparing the definition can. The first cursor write
	/// reads the row and writes it back unchanged, which is what makes that
	/// write contend with a reset committing under it; see [`Self::next`].
	window: i64,
}

impl Sequence {
	/// Loads or initializes a sequence instance for the current node.
	///
	/// This method reads the persisted state for this sequence (if it exists) and
	/// allocates an initial batch of IDs. If no state exists, it starts from the
	/// provided `start` value.
	///
	/// # Arguments
	/// * `ctx` - Optional mutable context for timeout checking
	/// * `sqs` - The sequences manager
	/// * `seq` - The sequence domain identifying which sequence to load
	/// * `start` - The starting value if no state exists
	/// * `batch` - The batch size for ID allocations
	/// * `timeout` - Optional timeout for batch allocation operations
	async fn load(
		ctx: Option<&dyn CancellationProbe>,
		sqs: &Sequences,
		seq: &SequenceDomain,
		start: i64,
		batch: u32,
		timeout: Option<Duration>,
	) -> SequenceResult<Self> {
		Self::build(ctx, sqs, seq, start, batch, timeout, None).await
	}

	/// Builds an allocator for a sequence the caller's transaction has defined
	/// and not committed, from the definition that transaction holds.
	///
	/// Nothing committed describes this sequence yet — the store still holds
	/// whatever the statement is replacing, and building from that would
	/// allocate out of the run being replaced. So the caller's view is the
	/// definition, and no guard is armed against a committed one. That is only
	/// sound while it stays inside the transaction that wrote it, which is why
	/// the transaction owns what this returns rather than the node's map.
	async fn load_provisional(
		ctx: Option<&dyn CancellationProbe>,
		sqs: &Sequences,
		seq: &SequenceDomain,
		def: &SequenceDefinition,
		caller: &Transaction,
	) -> SequenceResult<Self> {
		Self::build(ctx, sqs, seq, def.start, def.batch, def.timeout, Some(caller)).await
	}

	async fn build(
		ctx: Option<&dyn CancellationProbe>,
		sqs: &Sequences,
		seq: &SequenceDomain,
		start: i64,
		batch: u32,
		timeout: Option<Duration>,
		caller: Option<&Transaction>,
	) -> SequenceResult<Self> {
		let state_key = seq.new_state_key(sqs.nid);
		// A provisional allocator reads what the transaction that defined the
		// sequence can see — including the rows that statement has cleared and
		// not committed. Reading committed state instead would build it over a
		// cursor and windows the statement has already taken away, and the
		// claim below, which reads through that same transaction, would then
		// disagree with what was read here.
		let own = AllocatorTx::open(caller, &sqs.tf, TransactionType::Read, sqs).await?;
		let tx = own.tx();
		// What the row holds right now, kept as the condition for this
		// allocator's first write. `None` means there is no row, which the
		// conditional write requires to still be true.
		let durable: Option<SequenceState> = tx.get_key(&state_key, None).await?;
		// Where a user-defined sequence begins is settled here, against the
		// definition as it stands, not against the one the caller read. An
		// absent cursor is how a reset looks *and* how first use looks, so this
		// path cannot tell them apart and must not trust a `start` that may
		// predate a `DEFINE SEQUENCE OVERWRITE`: seeding from it would rebuild
		// the allocator over ids already issued. The conditional write in
		// [`Sequence::next`] covers the allocator once it is cached; this
		// covers the moment it is built. A removed sequence has no definition,
		// and failing here is the right answer to give the caller.
		let (start, batch, timeout, definition) = match seq {
			// See [`Sequence::load_provisional`].
			SequenceDomain::UserName(..) if caller.is_some() => (start, batch, timeout, None),
			SequenceDomain::UserName(ns, db, sq) => {
				match tx
					.get_key(
						&SequenceKey {
							ns: *ns,
							db: *db,
							sq: Cow::Borrowed(sq),
						},
						None,
					)
					.await?
				{
					Some(def) => {
						let def: SequenceDefinition = def;
						(def.start, def.batch, def.timeout, Some(def))
					}
					// The caller read a definition, this transaction did not
					// write it, and nothing is committed under the name: it was
					// removed after the caller read it. Allocating anyway would
					// hand out values for a sequence that no longer exists and
					// leave claim rows for whoever defines the name next. The
					// uncommitted case does not reach here: it is served by
					// [`Sequence::load_provisional`], which the caller's own
					// transaction is the only thing that can select.
					None => {
						return Err(anyhow::Error::new(surrealdb_catalog::Error::SeqNotFound {
							name: sq.clone(),
						})
						.into());
					}
				}
			}
			_ => (start, batch, timeout, None),
		};
		let mut st: SequenceState = if let Some(st) = durable.clone() {
			st
		} else {
			// First boot for this sequence: bump the configured start past any IDs
			// already issued via the catalog so we never reuse live namespace,
			// database, table, or index identifiers.
			let start = Self::seed_start_from_catalog(tx, seq, start).await?;
			SequenceState {
				next: start,
			}
		};
		own.cancel().await?;
		let BatchAllocation {
			from,
			to,
			window,
		} = Self::find_batch_allocation(
			sqs,
			ctx,
			seq,
			st.next,
			batch,
			timeout,
			ClaimGuard {
				definition: definition.as_ref(),
				cursor: durable.as_ref(),
				caller,
			},
		)
		.await?;
		st.next = from;
		Ok(Self {
			tf: sqs.tf.clone(),
			state_key,
			to,
			st,
			timeout,
			durable,
			definition,
			window,
		})
	}

	/// Raises `start` to one past the highest catalog-assigned ID for domains
	/// backed by the namespace, database, table, or index catalogs; other
	/// domains keep `start` unchanged.
	async fn seed_start_from_catalog(
		tx: &Transaction,
		seq: &SequenceDomain,
		start: i64,
	) -> Result<i64> {
		// `start` is a lower bound; the scan only increases it when catalog rows exist.
		let mut seeded = start;
		match seq {
			SequenceDomain::NameSpacesIds => {
				for ns in tx.all_ns(None).await?.iter() {
					seeded = seeded.max(ns.namespace_id.0 as i64 + 1);
				}
			}
			SequenceDomain::DatabasesIds(ns) => {
				for db in tx.all_db(*ns, None).await?.iter() {
					seeded = seeded.max(db.database_id.0 as i64 + 1);
				}
			}
			SequenceDomain::TablesIds(ns, db) => {
				for tb in tx.all_tb(*ns, *db, None).await?.iter() {
					seeded = seeded.max(tb.table_id.0 as i64 + 1);
				}
			}
			SequenceDomain::IndexIds(ns, db, tb) => {
				for ix in tx.all_tb_indexes(*ns, *db, tb, None).await?.iter() {
					seeded = seeded.max(ix.index_id.0 as i64 + 1);
				}
			}
			// Document-ID spaces and user-defined sequences are not backed by the
			// catalog id-allocation scheme, so there are no pre-existing IDs to avoid
			// colliding with.
			SequenceDomain::TableDocIds(..)
			| SequenceDomain::LegacyFullTextDocIds(..)
			| SequenceDomain::UserName(..) => {}
		}
		Ok(seeded)
	}

	/// Gets the next ID from this sequence.
	///
	/// If the current batch is exhausted, this method will allocate a new batch
	/// before returning the next ID.
	///
	/// A dense domain persists its cursor after every allocation, since a
	/// restart has to resume on the exact id it stopped at. A gap-tolerant one
	/// persists nothing here: its window's end was committed with the claim
	/// that reserved it, which already bounds every id the window can yield.
	/// See [`SequenceDomain::requires_dense_ids`].
	///
	/// # Arguments
	/// * `sqs` - The sequences manager
	/// * `ctx` - Optional mutable context for timeout checking
	/// * `seq` - The sequence domain
	/// * `batch` - The batch size for new allocations if needed
	/// * `caller` - The transaction a provisional allocator draws through
	/// * `evicted` - For an allocator held in a node's map, whether it has left it. See
	///   [`CachedSequence`].
	async fn next(
		&mut self,
		sqs: &Sequences,
		ctx: Option<&dyn CancellationProbe>,
		seq: &SequenceDomain,
		batch: u32,
		caller: Option<&Transaction>,
		evicted: Option<&AtomicBool>,
	) -> SequenceResult<i64> {
		// A user-defined sequence sizes its window from the definition this
		// allocator belongs to, which is the one the claim is checked against.
		// `batch` reaching this call was read by the caller and can be a
		// definition already replaced.
		let batch = self.definition.as_ref().map(|d| d.batch).unwrap_or(batch);
		if self.st.next >= self.to {
			BatchAllocation {
				from: self.st.next,
				to: self.to,
				window: self.window,
			} = Self::find_batch_allocation(
				sqs,
				ctx,
				seq,
				self.st.next,
				batch,
				self.timeout,
				ClaimGuard {
					definition: self.definition.as_ref(),
					cursor: self.durable.as_ref(),
					caller,
				},
			)
			.await?;
		}
		let v = self.st.next;
		self.st.next += 1;
		if !seq.requires_dense_ids() {
			// The claim that reserved this window already committed its end, so
			// there is nothing left to make durable per id.
			return Ok(v);
		}
		// Write the cursor, conditional on the row still holding what this
		// allocator last put there. The condition costs nothing extra — the
		// write and its commit happen either way — and it is what makes a reset
		// visible to every node rather than only the one that ran the
		// statement: a peer clearing the row leaves this write no value to
		// match, wherever the peer is. See [`Sequence::durable`].
		//
		// A provisional allocator writes unconditionally, because there is no
		// peer for the condition to catch: its sequence is defined by a
		// transaction that has not committed, so no one else can see it, let
		// alone reset it. A save point rollback inside that transaction restores
		// the allocator along with the rows it wrote, so the two stay in step —
		// see `Transaction::provisional_save_points`.
		let own = AllocatorTx::open(caller, &self.tf, TransactionType::Write, sqs).await?;
		let tx = own.tx();

		// The cursor comparison alone cannot carry the very first write after a
		// load: there is no previous value, so it asserts the row is absent —
		// which is exactly what a reset leaves behind, and the write succeeds
		// against one. What that write checks instead is that the window it is
		// about to serve from is still claimed.
		//
		// The claim row, not the definition: every statement that resets a
		// sequence deletes the claim rows, whatever it leaves the definition
		// looking like. An `OVERWRITE` that rewrites the definition
		// byte-for-byte, and a `REMOVE` followed by a redefinition, both leave
		// a definition that compares equal — or absent, as the caller saw it —
		// while having taken the window away.
		//
		// Only this write needs it. Once a cursor this allocator wrote is
		// durable the comparison stands on its own, so the read stays off the
		// hot path.
		//
		// Read, the row protects this write only against a reset that has
		// already committed. A read joins the conflict set only under
		// serializable snapshot isolation, and the backends this runs against
		// do not use it, so a reset committing between this read and this
		// commit goes unnoticed: it deletes the claim row and every cursor row
		// it can see, this allocator's cursor row does not exist yet, and the
		// two transactions write nothing in common. Both commit. The cursor
		// then lands in a row the reset never saw, every later write compares
		// against it and succeeds, and this node serves out a window the
		// sequence no longer has — while a peer rebuilding from the new
		// `START` claims the same range. Writing the claim row back unchanged
		// puts it in both write sets, so one of the two loses on any backend.
		//
		// A reset to the bound `INFO FOR DB STRUCTURE` reports lives exactly
		// here: that bound is the highest cursor, which is where a node with
		// no cursor yet has its window.
		//
		// Not for a provisional allocator: it claimed inside this same
		// transaction, so there is no window between the claim and this write
		// for a reset to land in, and nothing committed for the claim to be
		// checked against.
		if caller.is_none()
			&& self.durable.is_none()
			&& let SequenceDomain::UserName(ns, db, sq) = seq
		{
			let key = SeqBatchKey {
				ns: *ns,
				db: *db,
				sq: Cow::Borrowed(sq),
				start: self.window,
			};
			let claim = match tx.get_key(&key, None).await {
				Ok(Some(claim)) if claim.owner == sqs.nid && claim.to == self.to => claim,
				Ok(_) => {
					own.cancel().await?;
					return Err(SequenceError::Reset);
				}
				Err(e) => {
					own.cancel().await?;
					return Err(e.into());
				}
			};
			if let Err(e) = tx.set_key(&key, &claim).await {
				own.cancel().await?;
				return Err(e.into());
			}
		}

		// Execute operations and ensure transaction is cancelled on error
		let written = if caller.is_some() {
			tx.set_key(&self.state_key, &self.st).await
		} else {
			tx.put_compare_key(&self.state_key, &self.st, self.durable.as_ref()).await
		};
		match written {
			// Removed from the map on its way here, so another allocator may now
			// be serving this sequence on this node — see [`CachedSequence`].
			//
			// Checked after the write has read the row, not before. Whatever
			// could leave that row holding what this allocator expects writes
			// only after the mark: the map's lock orders every rebuild after the
			// removal, and a statement that redefines the sequence evicts before
			// it draws or commits. So a row read as expected either predates all
			// of them — and this write then contends with theirs on the row
			// itself — or was written by one of them, which the mark precedes.
			Ok(_) if evicted.is_some_and(|e| e.load(Ordering::Acquire)) => {
				own.cancel().await?;
				Err(SequenceError::Reset)
			}
			// A provisional allocator's cursor rides the caller's commit and is
			// undone by the caller's rollback, so this commits nothing.
			Ok(_) => match own.commit().await {
				Ok(()) => {
					self.durable = Some(self.st.clone());
					Ok(v)
				}
				// The write went through and the reset committed under it. The
				// two contend on a key both of them wrote — the claim row
				// above, or the cursor row itself — and this is the commit
				// being told it lost.
				Err(SequenceError::Other(e)) if crate::is_retryable_transaction_conflict(&e) => {
					Err(SequenceError::Reset)
				}
				Err(e) => Err(e),
			},
			Err(e) => {
				own.cancel().await?;
				if crate::is_condition_not_met(&e) {
					return Err(SequenceError::Reset);
				}
				Err(e.into())
			}
		}
	}

	/// Finds and allocates a batch of IDs with retry logic and exponential backoff.
	///
	/// This method repeatedly attempts to allocate a batch until successful or until
	/// a timeout is reached. It uses exponential backoff with jitter to reduce
	/// contention when multiple nodes are competing for batch allocations.
	///
	/// # Arguments
	/// * `sqs` - The sequences manager
	/// * `ctx` - Optional mutable context for timeout checking
	/// * `seq` - The sequence domain
	/// * `next` - The next ID that needs to be allocated
	/// * `batch` - The batch size to allocate
	/// * `to` - Optional timeout duration for the entire operation
	///
	/// # Returns
	/// The window claimed. See [`BatchAllocation`].
	async fn find_batch_allocation(
		sqs: &Sequences,
		ctx: Option<&dyn CancellationProbe>,
		seq: &SequenceDomain,
		next: i64,
		batch: u32,
		to: Option<Duration>,
		guard: ClaimGuard<'_>,
	) -> SequenceResult<BatchAllocation> {
		// Use for exponential backoff
		let mut tempo = 4;
		const MAX_BACKOFF: u64 = 32_768;
		let start = if to.is_some() {
			Some(Instant::now())
		} else {
			None
		};
		// Loop until we have a successful allocation.
		// We check the timeout inherited from the context
		loop {
			if let Some(ctx) = ctx {
				ctx.expect_not_timedout().await?;
			} else {
				yield_now!();
			}
			if let (Some(ref start), Some(ref to)) = (start, to) {
				// We check the time associated with the sequence
				if start.elapsed().ge(to) {
					let timeout = *to;
					return Err(anyhow::Error::new(EngineError::QueryTimedout(timeout)).into());
				}
			}
			match Self::check_batch_allocation(sqs, seq, next, batch, guard).await {
				Ok(r) => return Ok(r),
				// Not transient: the sequence this allocator belongs to is gone,
				// and retrying would claim a window under a definition that no
				// longer exists. The caller rebuilds instead.
				Err(SequenceError::Reset) => return Err(SequenceError::Reset),
				// Also not transient. What the backoff waits for is the state
				// another transaction holds to change, and a provisional claim
				// runs in the caller's own transaction: whatever it failed on —
				// a write budget it has spent, a limit it has crossed — is a
				// property of that transaction, which no amount of waiting
				// alters. Retrying here would spin until the query timeout, or
				// forever where none is set.
				Err(e) if guard.caller.is_some() => return Err(e),
				Err(_) => {}
			}
			// exponential backoff with full jitter
			let sleep_ms = rand::rng().random_range(1..=tempo);
			sleep(Duration::from_millis(sleep_ms)).await;
			if tempo < MAX_BACKOFF {
				tempo *= 2;
			}
		}
	}

	/// Attempts to allocate a batch of IDs in a single transaction.
	///
	/// This method scans existing batch allocations to find the highest allocated ID,
	/// reuses existing batches owned by this node if available, and creates a new
	/// batch allocation if needed. The entire operation is atomic within a transaction.
	///
	/// # Arguments
	/// * `sqs` - The sequences manager
	/// * `seq` - The sequence domain
	/// * `next` - The next ID that needs to be allocated
	/// * `batch` - The batch size to allocate
	///
	/// # Returns
	/// The window claimed. See [`BatchAllocation`].
	async fn check_batch_allocation(
		sqs: &Sequences,
		seq: &SequenceDomain,
		next: i64,
		batch: u32,
		guard: ClaimGuard<'_>,
	) -> SequenceResult<BatchAllocation> {
		// A provisional allocator claims inside the transaction that defined
		// the sequence: the window is as provisional as the definition it was
		// computed from, and must appear and disappear with it.
		let own = AllocatorTx::open(guard.caller, &sqs.tf, TransactionType::Write, sqs).await?;
		let tx = own.tx();
		// Gap-tolerant domains persist the window's end here, in the same
		// transaction that claims it, instead of persisting a cursor per
		// allocation. See `SequenceDomain::requires_dense_ids`.
		let dense = seq.requires_dense_ids();
		let state_key = seq.new_state_key(sqs.nid);

		// Execute operations and ensure transaction is cancelled on error
		let result = async {
			// The window about to be claimed is only meaningful under the
			// definition it was computed from — and reading that definition is
			// not enough to say so. A read joins the conflict set only under
			// serializable snapshot isolation, and the backends this runs
			// against do not use it: `kvs-mem` opens transactions at plain
			// snapshot isolation. So a claim whose snapshot predates a
			// `DEFINE SEQUENCE` commits after it with nothing to stop it,
			// leaving a window the sequence no longer owns. The first-write
			// check cannot object to it either: the row is present, owned by
			// this node and correctly bounded. A peer that claimed after the
			// reset computed its window from the new `START`, and the two
			// overlap.
			//
			// Writing the definition back unchanged is what makes the two
			// contend. Both transactions then write that key, so one of them
			// loses on any backend, whatever it does with reads. One small
			// write per window claimed, not per id.
			//
			// Only where the allocator has a committed definition to hold on
			// to. A provisional allocator has none: the sequence it serves
			// exists only inside the uncommitted transaction it claims through,
			// where nothing else can reset it, so there is nothing here for the
			// check to catch.
			if let (Some(expected), SequenceDomain::UserName(ns, db, sq)) = (guard.definition, seq)
			{
				let key = SequenceKey {
					ns: *ns,
					db: *db,
					sq: Cow::Borrowed(sq),
				};
				let current = tx.get_key(&key, None).await?;
				if current.as_ref() != Some(expected) {
					return Err(SequenceError::Reset);
				}
				tx.set_key(&key, expected).await?;
			}
			// And that the cursor this allocator last made durable is still
			// there. A reset deletes it, and an `OVERWRITE` that rewrote the
			// same definition leaves the check above satisfied — so without
			// this the claim commits a window starting where this allocator
			// left off. The cursor write would then notice the reset, but too
			// late: the window is durable, and the rebuild resumes into it
			// instead of starting from `START`.
			//
			// A provisional claim is unguarded here too, for the same reason as
			// the definition: no one outside the caller's transaction can see
			// this sequence, let alone reset it, so there is nothing to catch.
			if dense
				&& guard.caller.is_none()
				&& let Some(cursor) = guard.cursor
			{
				let current: Option<SequenceState> = tx.get_key(&state_key, None).await?;
				if current.as_ref() != Some(cursor) {
					return Err(SequenceError::Reset);
				}
			}
			let batch_range = seq.new_batch_range_keys()?;
			let batches = tx.getr(batch_range, None).await?;
			let mut next_start = next;
			// Scan every existing batch
			for (key, ba) in batches.iter() {
				next_start = next_start.max(ba.to);
				// The batch belongs to this node
				if ba.owner == sqs.nid {
					// Decoded up front: both the resume bound below and the
					// retirement under it address the row through this key.
					let decoded = seq.decode_batch_key(key);
					// A window this node still owns is resumed into rather than
					// replaced — but never below its own start. The start is
					// encoded only in the key, and a peer may have claimed the
					// range beneath it since this node last persisted, so
					// resuming at `next` alone would hand out the peer's ids a
					// second time. An undecodable key carries no bound, so it
					// cannot be resumed into safely and is retired instead.
					let resume = match &decoded {
						Ok(batch_key) => next.max(batch_key.start()),
						Err(_) => ba.to,
					};
					if resume < ba.to {
						let owned = decoded.as_ref().map(|k| k.start()).unwrap_or(resume);
						if !dense {
							tx.set_key(
								&state_key,
								&SequenceState {
									next: ba.to,
								},
							)
							.await?;
						}
						return Ok(BatchAllocation {
							from: resume,
							to: ba.to,
							window: owned,
						});
					}
					// Otherwise we can remove this old batch and create a new one.
					//
					// Deleted through the decoded key, which re-encodes to the bytes
					// it came from — the round trip every declared key upholds. A key
					// that does not decode has no such guarantee and cannot be
					// addressed any other way, so it is retired by its bytes: the
					// alternative is to fail, and every failure here is swallowed and
					// retried by `find_batch_allocation`, so one unreadable entry
					// would stall this domain's allocation for as long as the caller
					// waits.
					match decoded {
						Ok(batch_key) => tx.del_key(&batch_key).await?,
						Err(e) => {
							warn!("Retiring an undecodable sequence batch key: {e}");
							tx.del(key.as_slice().into()).await?
						}
					}
				}
			}
			// We compute the new batch
			// At least one id, whatever `batch` says. `next` hands out
			// `next_start` as soon as this returns, and for a gap-tolerant
			// domain the watermark recorded below is the only record that it
			// was issued. An empty window would record a mark equal to that
			// id, so a restart would resume on it and hand it out twice.
			// `batch` reaches here straight from configuration, which sets no
			// lower bound of its own.
			let next_to = next_start + batch.max(1) as i64;
			// And store it in the KV store
			let bv = BatchValue {
				to: next_to,
				owner: sqs.nid,
			};
			let batch_key = seq.new_batch_key(next_start);
			// Claim the batch with a conditional create (put-if-absent) rather
			// than a blind `set`. Two nodes exhausting the same domain
			// concurrently observe the same committed state and therefore compute
			// the same `next_start` (a node only reallocates once its current
			// batch is exhausted, so its local `next` never exceeds the highest
			// committed `to`), so they collide on this exact key. On
			// last-writer-wins backends (TiKV) a blind `set` would let both commit
			// and hand out the same range. The conditional create rejects the
			// second one: its comparison is performed by the write, not by a
			// read the commit would have to notice, so only the first committer
			// wins. `find_batch_allocation` then retries, re-scans, and claims
			// the next free range.
			// Conflict-serializing backends (mem/rocksdb/surrealkv) already reject
			// the second writer either way.
			tx.put_key(&batch_key, &bv).await?;
			// For a gap-tolerant domain, persist the window's end in the very
			// transaction that claims it. Committing a high-water mark rather
			// than a cursor is what lets ids be handed out from memory: a
			// restart resumes at or past every id this window can yield, so the
			// unconsumed tail is skipped and none is ever issued twice. Riding
			// the claim's own commit keeps that free — there is no second
			// transaction, and so no window where a claimed range is durable
			// but its bound is not.
			if !dense {
				tx.set_key(
					&state_key,
					&SequenceState {
						next: next_to,
					},
				)
				.await?;
			}
			Ok::<BatchAllocation, SequenceError>(BatchAllocation {
				from: next_start,
				to: next_to,
				window: next_start,
			})
		}
		.await;

		match result {
			Ok(res) => {
				own.commit().await?;
				Ok(res)
			}
			Err(e) => {
				own.cancel().await?;
				Err(e)
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;
	use std::time::Duration;

	use anyhow::Result;
	use surrealdb_catalog::providers::{DatabaseProvider, NamespaceProvider, TableProvider};
	use surrealdb_catalog::{
		DatabaseDefinition, DatabaseId, FromStored, Index, IndexDefinition, IndexId,
		NamespaceDefinition, NamespaceId, StoredTableDefinition, TableDefinition, TableId,
	};
	use surrealdb_cnf::ConfigMap;
	use surrealdb_strand::TableName;
	use uuid::Uuid;

	use crate::TransactionType;
	use crate::factory::TransactionFactory;
	use crate::sequences::{
		Sequence, SequenceDomain, SequenceError, SequenceResult, Sequences, is_sequence_reset,
	};

	/// Every domain addresses the route its own key family spells.
	///
	/// The dispatch enums exist to keep a domain's choice of family a type rather
	/// than bytes, which is only worth anything if each variant wraps the key the
	/// schema declares for that domain. Nothing else checks it: the allocator only
	/// ever round-trips through itself, so a variant wrapping a neighbour's key
	/// type would read and write consistently while silently sharing another
	/// domain's ID space.
	#[test]
	fn each_domain_addresses_its_own_route() {
		use surrealdb_kvs::key::KVKey;

		let ns = NamespaceId(1);
		let db = DatabaseId(2);
		let tb = TableName::from("testtb");
		let nid = Uuid::from_u128(3);

		// `start` and `nid` are the only fields that vary within a family, so the
		// route is what the leading bytes have to prove.
		for (domain, batch, state) in [
			(
				SequenceDomain::UserName(ns, db, "sq".to_string()),
				&b"/*\0\0\0\x01*\0\0\0\x02!sqsq\0!ba"[..],
				&b"/*\0\0\0\x01*\0\0\0\x02!sqsq\0!st"[..],
			),
			(
				SequenceDomain::TableDocIds(ns, db, tb.clone()),
				&b"/*\0\0\0\x01*\0\0\0\x02*testtb\0!dh"[..],
				&b"/*\0\0\0\x01*\0\0\0\x02*testtb\0!ds"[..],
			),
			(SequenceDomain::NameSpacesIds, &b"/!nh"[..], &b"/!ni"[..]),
			(SequenceDomain::DatabasesIds(ns), &b"/*\0\0\0\x01!dh"[..], &b"/*\0\0\0\x01!di"[..]),
			(
				SequenceDomain::TablesIds(ns, db),
				&b"/*\0\0\0\x01*\0\0\0\x02!th"[..],
				&b"/*\0\0\0\x01*\0\0\0\x02!ti"[..],
			),
			(
				SequenceDomain::IndexIds(ns, db, tb),
				&b"/*\0\0\0\x01*\0\0\0\x02*testtb\0!ih"[..],
				&b"/*\0\0\0\x01*\0\0\0\x02*testtb\0!is"[..],
			),
		] {
			let encoded = domain.new_batch_key(7).encode_key().unwrap();
			assert!(
				encoded.starts_with(batch),
				"batch key {} does not sit under {}",
				String::from_utf8_lossy(&encoded),
				String::from_utf8_lossy(batch)
			);
			// The key a scan hands back decodes and re-encodes to the same bytes,
			// which is what lets the write phase delete through the decoded key.
			assert_eq!(
				&*domain.decode_batch_key(&encoded).unwrap().encode_key().unwrap(),
				&*encoded,
				"a batch key did not survive the round trip its delete relies on"
			);

			let encoded = domain.new_state_key(nid).encode_key().unwrap();
			assert!(
				encoded.starts_with(state),
				"state key {} does not sit under {}",
				String::from_utf8_lossy(&encoded),
				String::from_utf8_lossy(state)
			);
		}
	}

	/// A transaction source without a `Datastore`.
	///
	/// These tests are white-box on the allocator - they call
	/// `Sequence::seed_start_from_catalog` directly - so they belong beside it.
	/// Reaching for a `Datastore` to get a transaction would put them above the
	/// thing they test and force its internals public; the factory is what a
	/// datastore uses underneath anyway.
	async fn factory() -> (TransactionFactory, Sequences) {
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
		(tf, sequences)
	}

	/// A claimed window always contains at least one id, whatever `batch` says.
	///
	/// `batch` reaches the allocator straight from configuration, which sets no
	/// lower bound of its own. At zero the window would be empty, and a
	/// gap-tolerant domain records the window's end as the only evidence that
	/// an id was issued — so the recorded mark would equal the id just handed
	/// out, and a reload would resume on it and issue it a second time.
	#[tokio::test]
	async fn a_zero_batch_still_records_past_every_issued_id() {
		let (tf, _) = factory().await;
		// One node id across both allocators. The state key is keyed per node,
		// so this is what a restart of the same node reads back.
		let nid = Uuid::from_u128(9);
		let ns = NamespaceId(1);
		let db = DatabaseId(2);
		let tb = TableName::from("t");

		// Bounded, because an unclamped zero batch does not merely reissue: the
		// window it claims is zero-width, so the next claim deletes that row and
		// re-creates it at the same start, the conditional create rejects it,
		// and `find_batch_allocation` retries that forever. Without a deadline
		// a regression here hangs instead of failing.
		let allocate = |seqs: Sequences, tb: TableName| async move {
			let mut ids = Vec::new();
			for _ in 0..4 {
				ids.push(seqs.next_table_doc_id(None, ns, db, tb.clone(), 0).await.unwrap());
			}
			ids
		};
		let deadline = Duration::from_secs(10);
		let mut ids =
			tokio::time::timeout(deadline, allocate(Sequences::new(tf.clone(), nid), tb.clone()))
				.await
				.expect("a zero batch stalled allocation");
		ids.extend(
			tokio::time::timeout(deadline, allocate(Sequences::new(tf, nid), tb.clone()))
				.await
				.expect("a zero batch stalled allocation after the reload"),
		);

		let unique: std::collections::HashSet<_> = ids.iter().copied().collect();
		assert_eq!(unique.len(), ids.len(), "a zero batch reissued an id across a reload: {ids:?}");
	}

	#[tokio::test]
	async fn seed_start_from_catalog_uses_max_existing_id() {
		let (tf, sequences) = factory().await;
		let ns_id = NamespaceId(7);
		let db_id = DatabaseId(11);
		let tb_name: TableName = "tb".into();

		let tx = tf.transaction(TransactionType::Write, sequences.clone()).await.unwrap();
		tx.put_ns(NamespaceDefinition {
			namespace_id: ns_id,
			name: "ns".into(),
			comment: None,
		})
		.await
		.unwrap();
		tx.put_db(
			"ns",
			DatabaseDefinition {
				namespace_id: ns_id,
				database_id: db_id,
				name: "db".into(),
				comment: None,
				changefeed: None,
				strict: false,
			},
		)
		.await
		.unwrap();
		let tb_def = TableDefinition::from_stored(&StoredTableDefinition::new(
			ns_id,
			db_id,
			TableId(13),
			tb_name.clone(),
		))
		.unwrap();
		tx.put_tb("ns", "db", &tb_def).await.unwrap();
		tx.put_tb_index(
			ns_id,
			db_id,
			&tb_name,
			&IndexDefinition {
				index_id: IndexId(17),
				name: "ix".into(),
				table_name: tb_name.clone(),
				cols: vec![],
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

		let tx = tf.transaction(TransactionType::Read, sequences.clone()).await.unwrap();

		// Seeds past the highest existing ID in each catalog domain.
		assert_eq!(
			Sequence::seed_start_from_catalog(&tx, &SequenceDomain::NameSpacesIds, 0)
				.await
				.unwrap(),
			8
		);
		assert_eq!(
			Sequence::seed_start_from_catalog(&tx, &SequenceDomain::DatabasesIds(ns_id), 0)
				.await
				.unwrap(),
			12
		);
		assert_eq!(
			Sequence::seed_start_from_catalog(&tx, &SequenceDomain::TablesIds(ns_id, db_id), 0)
				.await
				.unwrap(),
			14
		);
		assert_eq!(
			Sequence::seed_start_from_catalog(
				&tx,
				&SequenceDomain::IndexIds(ns_id, db_id, tb_name.clone()),
				0,
			)
			.await
			.unwrap(),
			18
		);

		// `start` is a lower bound: a higher caller-supplied value wins.
		assert_eq!(
			Sequence::seed_start_from_catalog(&tx, &SequenceDomain::NameSpacesIds, 100)
				.await
				.unwrap(),
			100
		);

		// Empty catalog scopes leave `start` unchanged.
		assert_eq!(
			Sequence::seed_start_from_catalog(
				&tx,
				&SequenceDomain::DatabasesIds(NamespaceId(999)),
				3,
			)
			.await
			.unwrap(),
			3
		);

		tx.cancel().await.unwrap();
	}

	#[tokio::test]
	async fn seed_start_from_catalog_returns_start_on_empty_store() {
		let (tf, sequences) = factory().await;
		let tx = tf.transaction(TransactionType::Read, sequences.clone()).await.unwrap();

		assert_eq!(
			Sequence::seed_start_from_catalog(&tx, &SequenceDomain::NameSpacesIds, 0)
				.await
				.unwrap(),
			0
		);
		assert_eq!(
			Sequence::seed_start_from_catalog(
				&tx,
				&SequenceDomain::DatabasesIds(NamespaceId(0)),
				0,
			)
			.await
			.unwrap(),
			0
		);

		tx.cancel().await.unwrap();
	}

	/// Any `Sequences` will do for building a transaction; the factory is what
	/// carries the store.
	fn tf_sequences(tf: &TransactionFactory) -> Sequences {
		Sequences::new(tf.clone(), Uuid::from_u128(99))
	}

	/// Writes a sequence definition the way `DEFINE SEQUENCE` leaves it, and
	/// clears the allocator rows the statement clears, in the given transaction.
	async fn define_sequence_in(
		tx: &crate::Transaction,
		ns: NamespaceId,
		db: DatabaseId,
		name: &str,
		start: i64,
		batch: u32,
	) -> Result<()> {
		use std::borrow::Cow;

		use crate::key::schema::{SeqBatchPrefix, SeqStatePrefix, SequenceKey};

		tx.set_key(
			&SequenceKey {
				ns,
				db,
				sq: Cow::Borrowed(name),
			},
			&surrealdb_catalog::SequenceDefinition {
				name: name.into(),
				batch,
				start,
				timeout: None,
			},
		)
		.await?;
		tx.delr(
			SeqBatchPrefix {
				ns,
				db,
				sq: Cow::Borrowed(name),
			}
			.range()?,
		)
		.await?;
		tx.delr(
			SeqStatePrefix {
				ns,
				db,
				sq: Cow::Borrowed(name),
			}
			.range()?,
		)
		.await?;
		Ok(())
	}

	/// Writes a sequence definition the way `DEFINE SEQUENCE` leaves it, and
	/// clears the allocator rows the statement clears.
	async fn define_sequence(
		tf: &TransactionFactory,
		sqs: &Sequences,
		ns: NamespaceId,
		db: DatabaseId,
		name: &str,
		start: i64,
		batch: u32,
	) -> Result<()> {
		let tx = tf.transaction(TransactionType::Write, sqs.clone()).await?;
		match define_sequence_in(&tx, ns, db, name, start, batch).await {
			Ok(()) => tx.commit().await,
			Err(e) => {
				tx.cancel().await?;
				Err(e)
			}
		}
	}

	/// Repositions a sequence to the bound `INFO FOR DB STRUCTURE` reports as
	/// `resume`, reading that bound and writing the definition in one
	/// transaction.
	///
	/// The two have to share a transaction. Read on its own, the bound is stale
	/// by the time the definition is written, and a reposition below an id
	/// issued in between is defined to reissue it.
	async fn reposition_to_resume(
		tf: &TransactionFactory,
		sqs: &Sequences,
		ns: NamespaceId,
		db: DatabaseId,
		name: &str,
		batch: u32,
	) -> Result<()> {
		let tx = tf.transaction(TransactionType::Write, sqs.clone()).await?;
		let res = async {
			let def = tx.get_db_sequence(ns, db, name, None).await?;
			let start = super::next_unissued_value(&tx, ns, db, name, def.start, None).await?;
			define_sequence_in(&tx, ns, db, name, start, batch).await
		}
		.await;
		match res {
			Ok(()) => tx.commit().await,
			Err(e) => {
				tx.cancel().await?;
				Err(e)
			}
		}
	}

	/// A reset landing between an allocator's batch claim and its first cursor
	/// write is caught by that write.
	///
	/// The claim is guarded and the cursor comparison is guarded, but between
	/// them sits the one write the comparison cannot carry: with no previous
	/// cursor it asserts the row is absent, and a reset leaves it absent. The
	/// definition is checked in that transaction for exactly this moment.
	#[tokio::test]
	async fn a_reset_between_the_claim_and_the_first_write_is_caught() {
		let (tf, _) = factory().await;
		let ns = NamespaceId(1);
		let db = DatabaseId(2);
		let name = "sq";
		let seqs = Sequences::new(tf.clone(), Uuid::from_u128(21));
		define_sequence(&tf, &seqs, ns, db, name, 1, 1000).await.unwrap();

		// Built, so its window is claimed and committed — but it has not
		// written a cursor yet, which is the state this covers.
		let domain = SequenceDomain::new_user(ns, db, name);
		let mut seq = Sequence::load(None, &seqs, &domain, 1, 1000, None).await.unwrap();

		// The reposition lands now.
		define_sequence(&tf, &seqs, ns, db, name, 500, 1000).await.unwrap();

		let err = seq
			.next(&seqs, None, &domain, 1000, None, None)
			.await
			.expect_err("the first write served a window the reposition had replaced");
		assert!(matches!(err, SequenceError::Reset), "expected a reset, got: {err}");
	}

	/// An `OVERWRITE` that rewrites the same definition still resets the
	/// sequence, and an allocator built before it must not survive.
	///
	/// Comparing the definition cannot see this: the bytes are unchanged. What
	/// changed is that every claim and cursor row was deleted.
	#[tokio::test]
	async fn an_identical_overwrite_still_resets_an_allocator() {
		let (tf, _) = factory().await;
		let ns = NamespaceId(1);
		let db = DatabaseId(2);
		let name = "sq";
		let seqs = Sequences::new(tf.clone(), Uuid::from_u128(31));
		define_sequence(&tf, &seqs, ns, db, name, 1, 1000).await.unwrap();

		let domain = SequenceDomain::new_user(ns, db, name);
		let mut seq = Sequence::load(None, &seqs, &domain, 1, 1000, None).await.unwrap();

		// Byte-for-byte the definition it already had, which still clears the
		// claim and cursor rows.
		define_sequence(&tf, &seqs, ns, db, name, 1, 1000).await.unwrap();

		let err = seq
			.next(&seqs, None, &domain, 1000, None, None)
			.await
			.expect_err("an allocator survived a reset that rewrote the same definition");
		assert!(matches!(err, SequenceError::Reset), "expected a reset, got: {err}");
	}

	/// A sequence removed after the caller read it is not allocated from.
	///
	/// The caller holds a definition the store no longer has. That is the same
	/// shape as a caller carrying an uncommitted `DEFINE`, and only the
	/// transaction can tell them apart — so where it says the definition is not
	/// its own, an absent one means removed, and building an allocator over it
	/// would issue values for a sequence that is gone and leave claim rows for
	/// whoever defines the name next.
	#[tokio::test]
	async fn an_allocator_is_not_built_over_a_removed_sequence() {
		use std::borrow::Cow;

		use crate::key::schema::SequenceKey;

		let (tf, _) = factory().await;
		let ns = NamespaceId(1);
		let db = DatabaseId(2);
		let name = "sq";
		let seqs = Sequences::new(tf.clone(), Uuid::from_u128(32));
		define_sequence(&tf, &seqs, ns, db, name, 1, 1000).await.unwrap();

		// Removed out from under the caller, which still holds what it read.
		let tx = tf.transaction(TransactionType::Write, seqs.clone()).await.unwrap();
		tx.del_key(&SequenceKey {
			ns,
			db,
			sq: Cow::Borrowed(name),
		})
		.await
		.unwrap();
		tx.commit().await.unwrap();

		let domain = SequenceDomain::new_user(ns, db, name);
		let err = match Sequence::load(None, &seqs, &domain, 1, 1000, None).await {
			Ok(_) => panic!("an allocator was built over a removed sequence"),
			Err(e) => e,
		};
		assert!(
			err.to_string().contains("does not exist"),
			"expected the sequence to be reported missing, got: {err}"
		);
	}

	/// A peer whose window is exhausted picks up a reset before it claims.
	///
	/// The peer is not the node that ran the statement, so nothing evicted its
	/// allocator. With `BATCH 1` its next call claims before it writes a
	/// cursor, and the claim is the only thing standing between it and a window
	/// starting where it left off — a window the rebuild would then resume
	/// into, carrying the sequence on instead of resetting it. Comparing the
	/// definition cannot help when the overwrite rewrote the same bytes.
	#[tokio::test]
	async fn a_peer_with_an_exhausted_window_picks_up_an_identical_overwrite() {
		let (tf, _) = factory().await;
		let ns = NamespaceId(1);
		let db = DatabaseId(2);
		let name = "sq";
		let peer = Sequences::new(tf.clone(), Uuid::from_u128(41));
		define_sequence(&tf, &peer, ns, db, name, 1, 1).await.unwrap();

		let serve = async |tf: TransactionFactory, peer: Sequences| -> i64 {
			let tx = tf.transaction(TransactionType::Read, peer.clone()).await.unwrap();
			let v = peer.next_user_sequence_id(None, &tx, ns, db, name).await;
			tx.cancel().await.unwrap();
			v.unwrap()
		};
		// `BATCH 1`, so this leaves the window exhausted and a cursor durable.
		assert_eq!(serve(tf.clone(), peer.clone()).await, 1);

		// Another node reruns the definition unchanged. The peer is told
		// nothing, and the bytes it would compare are the same.
		define_sequence(&tf, &peer, ns, db, name, 1, 1).await.unwrap();

		assert_eq!(
			serve(tf.clone(), peer.clone()).await,
			1,
			"the peer carried on from its old window instead of resetting"
		);
	}

	/// A transaction source whose next commit first runs an armed hook.
	///
	/// This is how a straddle is made deterministic: the hook lands other
	/// transactions — a reposition, a peer's claim — inside the commit of the one
	/// under test, so the code path exercised is the real one, not a re-enactment
	/// of it.
	mod hooked {
		use std::future::Future;
		use std::pin::Pin;
		use std::sync::{Arc, Mutex as StdMutex};

		use surrealdb_cnf::ConfigMap;
		use surrealdb_kvs::api::{BoxFut, KeysResult, ScanResult, Transactable};
		use surrealdb_kvs::err::Result as KvsResult;
		use surrealdb_kvs::{
			Key, KeyRange, Metrics, TransactionBuilder, TransactionType as KvsTransactionType, Val,
		};

		use crate::factory::TransactionFactory;

		pub(super) type Hook = Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>;
		pub(super) type HookSlot = Arc<StdMutex<Option<Hook>>>;

		/// Wraps a builder so that the next `commit` of any transaction it hands
		/// out first runs the armed hook. One-shot: the hook is taken before it
		/// runs, so transactions the hook itself opens commit unintercepted.
		struct HookedBuilder {
			inner: Box<dyn TransactionBuilder>,
			hook: HookSlot,
		}
		struct HookedTx {
			inner: Box<dyn Transactable>,
			hook: HookSlot,
		}

		impl TransactionBuilder for HookedBuilder {
			fn name(&self) -> &'static str {
				self.inner.name()
			}
			fn new_transaction(
				&self,
				write: KvsTransactionType,
			) -> BoxFut<'_, KvsResult<(Box<dyn Transactable>, bool)>> {
				Box::pin(async move {
					let (inner, local) = self.inner.new_transaction(write).await?;
					let tx: Box<dyn Transactable> = Box::new(HookedTx {
						inner,
						hook: Arc::clone(&self.hook),
					});
					Ok((tx, local))
				})
			}
			fn shutdown(&self) -> BoxFut<'_, KvsResult<()>> {
				self.inner.shutdown()
			}
			fn register_metrics(&self) -> Option<Metrics> {
				self.inner.register_metrics()
			}
			fn collect_u64_metric(&self, metric: &str) -> Option<u64> {
				self.inner.collect_u64_metric(metric)
			}
		}

		impl Transactable for HookedTx {
			fn kind(&self) -> &'static str {
				self.inner.kind()
			}
			fn closed(&self) -> bool {
				self.inner.closed()
			}
			fn writeable(&self) -> bool {
				self.inner.writeable()
			}
			fn cancel<'a>(&'a self) -> BoxFut<'a, KvsResult<()>> {
				self.inner.cancel()
			}
			fn commit<'a>(&'a self) -> BoxFut<'a, KvsResult<()>> {
				Box::pin(async move {
					let hook = self.hook.lock().unwrap().take();
					if let Some(hook) = hook {
						hook().await;
					}
					self.inner.commit().await
				})
			}
			fn exists<'a>(
				&'a self,
				key: Key<'a>,
				version: Option<u64>,
			) -> BoxFut<'a, KvsResult<bool>> {
				self.inner.exists(key, version)
			}
			fn get<'a>(
				&'a self,
				key: Key<'a>,
				version: Option<u64>,
			) -> BoxFut<'a, KvsResult<Option<Val>>> {
				self.inner.get(key, version)
			}
			fn set<'a>(&'a self, key: Key<'a>, val: Val) -> BoxFut<'a, KvsResult<()>> {
				self.inner.set(key, val)
			}
			fn put<'a>(&'a self, key: Key<'a>, val: Val) -> BoxFut<'a, KvsResult<()>> {
				self.inner.put(key, val)
			}
			fn putc<'a>(
				&'a self,
				key: Key<'a>,
				val: Val,
				chk: Option<Val>,
			) -> BoxFut<'a, KvsResult<()>> {
				self.inner.putc(key, val, chk)
			}
			fn del<'a>(&'a self, key: Key<'a>) -> BoxFut<'a, KvsResult<()>> {
				self.inner.del(key)
			}
			fn delc<'a>(
				&'a self,
				key: Key<'a>,
				chk: Option<&'a [u8]>,
			) -> BoxFut<'a, KvsResult<()>> {
				self.inner.delc(key, chk)
			}
			fn keys<'a>(
				&'a self,
				rng: KeyRange<'a>,
				limit: u32,
				skip: u32,
				version: Option<u64>,
			) -> BoxFut<'a, KvsResult<KeysResult>> {
				self.inner.keys(rng, limit, skip, version)
			}
			fn keysr<'a>(
				&'a self,
				rng: KeyRange<'a>,
				limit: u32,
				skip: u32,
				version: Option<u64>,
			) -> BoxFut<'a, KvsResult<KeysResult>> {
				self.inner.keysr(rng, limit, skip, version)
			}
			fn scan<'a>(
				&'a self,
				rng: KeyRange<'a>,
				limit: u32,
				skip: u32,
				version: Option<u64>,
			) -> BoxFut<'a, KvsResult<ScanResult>> {
				self.inner.scan(rng, limit, skip, version)
			}
			fn scanr<'a>(
				&'a self,
				rng: KeyRange<'a>,
				limit: u32,
				skip: u32,
				version: Option<u64>,
			) -> BoxFut<'a, KvsResult<ScanResult>> {
				self.inner.scanr(rng, limit, skip, version)
			}
			fn new_save_point(&self) -> BoxFut<'_, KvsResult<()>> {
				self.inner.new_save_point()
			}
			fn release_last_save_point(&self) -> BoxFut<'_, KvsResult<()>> {
				self.inner.release_last_save_point()
			}
			fn rollback_to_save_point(&self) -> BoxFut<'_, KvsResult<()>> {
				self.inner.rollback_to_save_point()
			}
		}

		/// A factory over `mem://` whose transactions are intercepted, and the
		/// slot to arm.
		pub(super) async fn hooked_factory() -> (TransactionFactory, HookSlot) {
			let hook: HookSlot = Arc::new(StdMutex::new(None));
			let builder = surrealdb_kvs_any::Backends::community()
				.new_transaction_builder("mem://", Default::default(), ConfigMap::default())
				.await
				.unwrap();
			let tf = TransactionFactory::new(
				Arc::new(crate::triggers::CommitTriggers::new()),
				Box::new(HookedBuilder {
					inner: builder,
					hook: Arc::clone(&hook),
				}),
				Arc::new(Default::default()),
			);
			(tf, hook)
		}
	}

	/// A draw from a definition one transaction has not committed is invisible
	/// to every other transaction.
	///
	/// The definition it comes from does not exist for anyone else, and will not
	/// exist at all if that transaction rolls back, so neither may the values
	/// drawn from it. That holds for the allocator, which the transaction owns
	/// rather than the node's map, and for what the allocator writes, which
	/// rides the same transaction.
	#[tokio::test(flavor = "multi_thread")]
	async fn a_provisional_draw_is_invisible_to_other_transactions() {
		let (tf, seqs) = factory().await;
		let ns = NamespaceId(1);
		let db = DatabaseId(2);
		let name = "sq";
		define_sequence(&tf, &seqs, ns, db, name, 1, 10).await.unwrap();

		// One transaction repositions to 900 and draws, without committing.
		let repositioning = tf.transaction(TransactionType::Write, seqs.clone()).await.unwrap();
		define_sequence_in(&repositioning, ns, db, name, 900, 10).await.unwrap();
		repositioning.sequence_defined_here(ns, db, name.to_string()).await;
		let drawn = seqs.next_user_sequence_id(None, &repositioning, ns, db, name).await.unwrap();
		assert_eq!(drawn, 900, "the defining transaction draws from what it wrote");

		// A transaction that can only see the committed definition is served
		// from that, and from a store the draw above has not touched.
		let other = tf.transaction(TransactionType::Read, seqs.clone()).await.unwrap();
		let elsewhere = seqs.next_user_sequence_id(None, &other, ns, db, name).await.unwrap();
		other.cancel().await.unwrap();
		assert_eq!(
			elsewhere, 1,
			"a concurrent transaction was served a value derived from an uncommitted `START`"
		);

		repositioning.cancel().await.unwrap();
	}

	/// A draw from a definition that never commits leaves nothing behind it.
	///
	/// The values a provisional draw issues are backed by rows — the claimed
	/// window and the node cursor. Those rows record a position in a sequence
	/// that, once the defining transaction rolls back, does not exist. They must
	/// therefore share that transaction's fate rather than being made durable on
	/// their own, which is what a draw served through the node's map would do.
	///
	/// This is the half that `INFO FOR DB` cannot report: the definition is
	/// rolled back either way, so only the rows themselves distinguish the two.
	#[tokio::test(flavor = "multi_thread")]
	async fn a_cancelled_provisional_draw_leaves_no_rows_behind() {
		use std::borrow::Cow;

		use crate::key::schema::{SeqBatchPrefix, SeqStatePrefix};

		let (tf, seqs) = factory().await;
		let ns = NamespaceId(1);
		let db = DatabaseId(2);
		let name = "sq";

		// Define and draw in one transaction, then abandon it. The sequence has
		// never been defined before, so anything left in its key families came
		// from this draw.
		let defining = tf.transaction(TransactionType::Write, seqs.clone()).await.unwrap();
		define_sequence_in(&defining, ns, db, name, 7, 10).await.unwrap();
		defining.sequence_defined_here(ns, db, name.to_string()).await;
		assert_eq!(
			seqs.next_user_sequence_id(None, &defining, ns, db, name).await.unwrap(),
			7,
			"the defining transaction draws from what it wrote"
		);
		defining.cancel().await.unwrap();

		let tx = tf.transaction(TransactionType::Read, seqs.clone()).await.unwrap();
		let batches = tx
			.getr(
				SeqBatchPrefix {
					ns,
					db,
					sq: Cow::Borrowed(name),
				}
				.range()
				.unwrap(),
				None,
			)
			.await
			.unwrap();
		let states = tx
			.getr(
				SeqStatePrefix {
					ns,
					db,
					sq: Cow::Borrowed(name),
				}
				.range()
				.unwrap(),
				None,
			)
			.await
			.unwrap();
		tx.cancel().await.unwrap();

		assert!(
			batches.is_empty(),
			"a rolled-back definition left {} claimed window(s) behind it",
			batches.len()
		);
		assert!(
			states.is_empty(),
			"a rolled-back definition left {} node cursor(s) behind it",
			states.len()
		);
	}

	/// An allocator loaded while a reposition is open cannot outlive it.
	///
	/// The statement evicts this node's allocator when it runs, which covers the
	/// one that existed before it but not one loaded between that eviction and
	/// the commit. Such an allocator comes from the definition being replaced,
	/// and neither check that catches a reset would fire on it: reusing a window
	/// already claimed writes no claim row, and the claim row is read only on the
	/// first write after a load, which this one is past.
	///
	/// What separates the two is the definition every claim writes back
	/// unchanged. It puts the load and the reposition in the same write set, so
	/// they cannot both commit — whichever loses, no allocator serving the old
	/// window outlives a committed reposition.
	#[tokio::test(flavor = "multi_thread")]
	async fn an_allocator_loaded_during_a_reposition_cannot_outlive_it() {
		let (tf, seqs) = factory().await;
		let ns = NamespaceId(1);
		let db = DatabaseId(2);
		let name = "sq";

		// A run this node has served from: cursor 2, window `[1, 101)`.
		define_sequence(&tf, &seqs, ns, db, name, 1, 100).await.unwrap();
		{
			let tx = tf.transaction(TransactionType::Read, seqs.clone()).await.unwrap();
			assert_eq!(seqs.next_user_sequence_id(None, &tx, ns, db, name).await.unwrap(), 1);
			tx.cancel().await.unwrap();
		}

		// The reposition opens and evicts inline, as the statement does.
		let repositioning = tf.transaction(TransactionType::Write, seqs.clone()).await.unwrap();
		define_sequence_in(&repositioning, ns, db, name, 1, 1).await.unwrap();
		repositioning.sequence_defined_here(ns, db, name.to_string()).await;
		seqs.sequence_removed(ns, db, name).await;

		// A load in the window between that eviction and the commit, from the
		// definition still committed. It reuses the window already claimed for
		// it, so the only thing it writes is the definition.
		let domain = SequenceDomain::new_user(ns, db, name);
		let stale = Sequence::load(None, &seqs, &domain, 1, 100, None).await;

		let drawn = seqs.next_user_sequence_id(None, &repositioning, ns, db, name).await.unwrap();
		assert_eq!(drawn, 1, "the reposition draws from what it wrote");

		match repositioning.commit().await {
			// The load and the reposition wrote the definition in common, and
			// this is the reposition being told it lost. Nothing was
			// repositioned, so the allocator that loaded is still current.
			Err(e) => assert!(
				crate::is_retryable_transaction_conflict(&e),
				"the reposition failed for some reason other than the contention \
				 that protects it: {e}"
			),
			// If it did commit, the load must not have produced an allocator
			// that can hand out what the new run is handing out. It draws
			// first: a draw from the new run would move the cursor out from
			// under it and mask the overlap.
			Ok(()) => {
				let from_stale = match stale {
					Err(_) => None,
					Ok(mut stale) => {
						match stale.next(&seqs, None, &domain, 100, None, None).await {
							Err(SequenceError::Reset) => None,
							Err(e) => panic!("unexpected error from the stale allocator: {e:?}"),
							Ok(v) => Some(v),
						}
					}
				};
				let tx = tf.transaction(TransactionType::Read, seqs.clone()).await.unwrap();
				let fresh = seqs.next_user_sequence_id(None, &tx, ns, db, name).await.unwrap();
				tx.cancel().await.unwrap();
				if let Some(from_stale) = from_stale {
					assert_ne!(
						from_stale, fresh,
						"an allocator loaded during the reposition served a value the new \
						 run also served"
					);
				}
			}
		}
	}

	/// Runs a reposition on a node while a draw holds that node's allocator
	/// outside the map, then lets the draw proceed.
	///
	/// A draw clones the allocator out of the map before locking it, so it can
	/// outlast the eviction the reposition performs. The run that follows then
	/// recreates the node's cursor row with the value the old allocator last
	/// wrote — through the reposition's own draw, or the first ordinary draw
	/// after it — and the conditional write that would catch a reset matches.
	///
	/// Returns what the held draw produced, and every value the new run issued:
	/// the node's first draw, a peer's, and the node's next one.
	async fn draw_held_across_a_reposition(
		reposition_draws: bool,
	) -> (SequenceResult<i64>, Vec<i64>) {
		let (tf, seqs) = factory().await;
		let peer = Sequences::new(tf.clone(), Uuid::from_u128(101));
		let ns = NamespaceId(1);
		let db = DatabaseId(2);
		let name = "sq";
		let domain = SequenceDomain::new_user(ns, db, name);

		// The node has served, so its allocator holds `[1, 101)` at cursor 2.
		define_sequence(&tf, &seqs, ns, db, name, 1, 100).await.unwrap();
		let tx = tf.transaction(TransactionType::Read, seqs.clone()).await.unwrap();
		assert_eq!(seqs.next_user_sequence_id(None, &tx, ns, db, name).await.unwrap(), 1);
		tx.cancel().await.unwrap();

		// A draw takes the allocator out of the map, as `next_val` does.
		let held = seqs.sequences.read().await.get(&domain).cloned().expect("a cached allocator");

		// The reposition runs on the same node.
		let mut issued = Vec::new();
		let repositioning = tf.transaction(TransactionType::Write, seqs.clone()).await.unwrap();
		define_sequence_in(&repositioning, ns, db, name, 1, 1).await.unwrap();
		repositioning.sequence_defined_here(ns, db, name.to_string()).await;
		seqs.sequence_removed(ns, db, name).await;
		if reposition_draws {
			issued.push(
				seqs.next_user_sequence_id(None, &repositioning, ns, db, name).await.unwrap(),
			);
		}
		repositioning.commit().await.unwrap();
		if !reposition_draws {
			let tx = tf.transaction(TransactionType::Read, seqs.clone()).await.unwrap();
			issued.push(seqs.next_user_sequence_id(None, &tx, ns, db, name).await.unwrap());
			tx.cancel().await.unwrap();
		}

		// The held draw proceeds.
		let from_held = held
			.sequence
			.lock()
			.await
			.next(&seqs, None, &domain, 100, None, Some(&held.evicted))
			.await;

		// A peer, and the node again, serve from the new run.
		for node in [&peer, &seqs] {
			let tx = tf.transaction(TransactionType::Read, node.clone()).await.unwrap();
			issued.push(node.next_user_sequence_id(None, &tx, ns, db, name).await.unwrap());
			tx.cancel().await.unwrap();
		}
		(from_held, issued)
	}

	/// An allocator a draw held across a reposition that drew in its own
	/// transaction must not serve once the reposition commits.
	#[tokio::test(flavor = "multi_thread")]
	async fn a_draw_held_across_a_reposition_is_refused() {
		let (from_held, issued) = draw_held_across_a_reposition(true).await;
		assert!(
			matches!(from_held, Err(SequenceError::Reset)),
			"an allocator evicted by the reposition served {from_held:?}; the new run issued \
			 {issued:?}"
		);
		let mut distinct = issued.clone();
		distinct.sort_unstable();
		distinct.dedup();
		assert_eq!(distinct.len(), issued.len(), "the new run issued a value twice: {issued:?}");
	}

	/// The same, when what recreates the cursor is an ordinary draw after the
	/// reposition rather than one inside it.
	#[tokio::test(flavor = "multi_thread")]
	async fn a_draw_held_across_a_reposition_and_a_rebuild_is_refused() {
		let (from_held, issued) = draw_held_across_a_reposition(false).await;
		assert!(
			matches!(from_held, Err(SequenceError::Reset)),
			"an allocator evicted by the reposition served {from_held:?}; the new run issued \
			 {issued:?}"
		);
		let mut distinct = issued.clone();
		distinct.sort_unstable();
		distinct.dedup();
		assert_eq!(distinct.len(), issued.len(), "the new run issued a value twice: {issued:?}");
	}

	/// Draws queued on a peer's allocator when a reposition elsewhere resets it
	/// must not resume from it once it has been rebuilt.
	///
	/// The reposition's eviction is local to its own node, so a peer finds out
	/// from its own cursor write: the first queued draw fails, and the peer
	/// rebuilds. The rebuild writes the same cursor row and can put back the
	/// value the old allocator last wrote, so the draws still queued on the old
	/// one would match it — unless evicting it on that reset also marked it.
	#[tokio::test(flavor = "multi_thread")]
	async fn draws_queued_on_a_reset_allocator_do_not_resume_after_its_rebuild() {
		let (tf, repositioner) = factory().await;
		let peer = Sequences::new(tf.clone(), Uuid::from_u128(101));
		let ns = NamespaceId(1);
		let db = DatabaseId(2);
		let name = "sq";
		let domain = SequenceDomain::new_user(ns, db, name);

		// The peer has served, so its allocator holds `[1, 101)` at cursor 2.
		define_sequence(&tf, &peer, ns, db, name, 1, 100).await.unwrap();
		let tx = tf.transaction(TransactionType::Read, peer.clone()).await.unwrap();
		assert_eq!(peer.next_user_sequence_id(None, &tx, ns, db, name).await.unwrap(), 1);
		tx.cancel().await.unwrap();

		// Draws queue on the peer's allocator, outside its map.
		let queued = peer.sequences.read().await.get(&domain).cloned().expect("a cached allocator");

		// A reposition commits on the other node, which the peer's map never hears of.
		let tx = tf.transaction(TransactionType::Write, repositioner.clone()).await.unwrap();
		define_sequence_in(&tx, ns, db, name, 1, 1).await.unwrap();
		tx.sequence_defined_here(ns, db, name.to_string()).await;
		repositioner.sequence_removed(ns, db, name).await;
		tx.commit().await.unwrap();

		// The first queued draw finds its cursor gone, and the peer evicts and
		// rebuilds, as `next_val` does on a reset.
		let first = queued
			.sequence
			.lock()
			.await
			.next(&peer, None, &domain, 100, None, Some(&queued.evicted))
			.await;
		assert!(matches!(first, Err(SequenceError::Reset)), "the reset went unnoticed: {first:?}");
		peer.evict_this_allocator(&Arc::new(SequenceDomain::new_user(ns, db, name)), &queued).await;
		let mut issued = Vec::new();
		let tx = tf.transaction(TransactionType::Read, peer.clone()).await.unwrap();
		issued.push(peer.next_user_sequence_id(None, &tx, ns, db, name).await.unwrap());
		tx.cancel().await.unwrap();

		// The next draw still queued on the old allocator.
		let second = queued
			.sequence
			.lock()
			.await
			.next(&peer, None, &domain, 100, None, Some(&queued.evicted))
			.await;

		for node in [&repositioner, &repositioner, &peer] {
			let tx = tf.transaction(TransactionType::Read, node.clone()).await.unwrap();
			issued.push(node.next_user_sequence_id(None, &tx, ns, db, name).await.unwrap());
			tx.cancel().await.unwrap();
		}
		assert!(
			matches!(second, Err(SequenceError::Reset)),
			"a draw queued on the reset allocator served {second:?} after its rebuild; the new \
			 run issued {issued:?}"
		);
		let mut distinct = issued.clone();
		distinct.sort_unstable();
		distinct.dedup();
		assert_eq!(distinct.len(), issued.len(), "the new run issued a value twice: {issued:?}");
	}

	/// The same, across nodes, where the eviction never reached in the first
	/// place.
	///
	/// A `DEFINE SEQUENCE` evicts only on the node that runs it, so a peer's
	/// allocator is not dropped by either an inline or a commit-time eviction —
	/// a close-time hook would not reach it any more than the inline one does.
	/// What keeps a peer's allocator from outliving the reposition is the same
	/// definition write-back: the peer's claim and the statement both write that
	/// key, so they cannot both commit.
	#[tokio::test(flavor = "multi_thread")]
	async fn a_peers_allocator_cannot_outlive_a_reposition_either() {
		let (tf, _) = factory().await;
		let ns = NamespaceId(1);
		let db = DatabaseId(2);
		let name = "sq";
		let peer = Sequences::new(tf.clone(), Uuid::from_u128(101));
		let repositioner = Sequences::new(tf.clone(), Uuid::from_u128(102));

		// The peer has served, so it holds a window and a cursor of its own.
		define_sequence(&tf, &peer, ns, db, name, 1, 100).await.unwrap();
		{
			let tx = tf.transaction(TransactionType::Read, peer.clone()).await.unwrap();
			assert_eq!(peer.next_user_sequence_id(None, &tx, ns, db, name).await.unwrap(), 1);
			tx.cancel().await.unwrap();
		}

		// The reposition runs on the other node. Its eviction is node-local and
		// never touches the peer.
		let tx = tf.transaction(TransactionType::Write, repositioner.clone()).await.unwrap();
		define_sequence_in(&tx, ns, db, name, 1, 1).await.unwrap();
		tx.sequence_defined_here(ns, db, name.to_string()).await;
		repositioner.sequence_removed(ns, db, name).await;

		// The peer loads afresh inside the window.
		let domain = SequenceDomain::new_user(ns, db, name);
		let loaded = Sequence::load(None, &peer, &domain, 1, 100, None).await;

		let drawn = repositioner.next_user_sequence_id(None, &tx, ns, db, name).await.unwrap();
		assert_eq!(drawn, 1, "the reposition draws from what it wrote");
		let committed = tx.commit().await;

		assert!(
			committed.is_err() || loaded.is_err(),
			"a peer loaded a window inside the reposition and both were allowed to stand"
		);
	}

	/// A `DEFINE SEQUENCE` that a save point rollback undid is not served from
	/// afterwards, in either direction.
	///
	/// A synchronous event can redefine a sequence and draw from it inside a
	/// document save point, then fail in a way `INSERT IGNORE` swallows. The
	/// rollback restores the definition, the claim and the cursor, so the rest of
	/// the transaction must see the sequence as committed: repositioned upward it
	/// would jump over values it never issued, and repositioned downward it would
	/// issue again values it already had.
	#[tokio::test(flavor = "multi_thread")]
	async fn a_definition_rolled_back_by_a_save_point_is_not_served_from() {
		for rolled_back_start in [900, 1] {
			let (tf, seqs) = factory().await;
			let ns = NamespaceId(1);
			let db = DatabaseId(2);
			let name = "sq";
			define_sequence(&tf, &seqs, ns, db, name, 1, 10).await.unwrap();
			for expected in 1..=3 {
				let tx = tf.transaction(TransactionType::Read, seqs.clone()).await.unwrap();
				assert_eq!(
					seqs.next_user_sequence_id(None, &tx, ns, db, name).await.unwrap(),
					expected
				);
				tx.cancel().await.unwrap();
			}

			let tx = tf.transaction(TransactionType::Write, seqs.clone()).await.unwrap();
			tx.new_save_point().await.unwrap();
			define_sequence_in(&tx, ns, db, name, rolled_back_start, 10).await.unwrap();
			tx.sequence_defined_here(ns, db, name.to_string()).await;
			seqs.sequence_removed(ns, db, name).await;
			assert_eq!(
				seqs.next_user_sequence_id(None, &tx, ns, db, name).await.unwrap(),
				rolled_back_start,
				"inside the save point the redefinition is in force"
			);
			tx.rollback_to_save_point().await.unwrap();
			assert_eq!(
				seqs.next_user_sequence_id(None, &tx, ns, db, name).await.unwrap(),
				4,
				"after rolling back a redefinition to START {rolled_back_start}, the transaction \
				 was served from it rather than from the committed run"
			);
			tx.commit().await.unwrap();

			let tx = tf.transaction(TransactionType::Read, seqs.clone()).await.unwrap();
			assert_eq!(
				seqs.next_user_sequence_id(None, &tx, ns, db, name).await.unwrap(),
				5,
				"the committed run did not continue after the transaction"
			);
			tx.cancel().await.unwrap();
		}
	}

	/// A provisional allocator built inside a save point that is rolled back must
	/// not keep serving from the window it claimed there.
	///
	/// The rollback removes the claim row along with the cursor. Another node
	/// sizes its window from the claim rows it can see, so a value served from a
	/// window with no claim row behind it is one that node will issue again.
	#[tokio::test(flavor = "multi_thread")]
	async fn a_window_claimed_inside_a_rolled_back_save_point_is_not_served_from() {
		let (tf, seqs) = factory().await;
		let peer = Sequences::new(tf.clone(), Uuid::from_u128(101));
		let ns = NamespaceId(1);
		let db = DatabaseId(2);
		let name = "sq";

		let tx = tf.transaction(TransactionType::Write, seqs.clone()).await.unwrap();
		define_sequence_in(&tx, ns, db, name, 1, 10).await.unwrap();
		tx.sequence_defined_here(ns, db, name.to_string()).await;
		tx.new_save_point().await.unwrap();
		seqs.next_user_sequence_id(None, &tx, ns, db, name).await.unwrap();
		tx.rollback_to_save_point().await.unwrap();
		let kept = seqs.next_user_sequence_id(None, &tx, ns, db, name).await.unwrap();
		tx.commit().await.unwrap();

		let mut from_peer = Vec::new();
		for _ in 0..3 {
			let tx = tf.transaction(TransactionType::Read, peer.clone()).await.unwrap();
			from_peer.push(peer.next_user_sequence_id(None, &tx, ns, db, name).await.unwrap());
			tx.cancel().await.unwrap();
		}
		assert!(
			!from_peer.contains(&kept),
			"a peer issued {kept} again, which the defining transaction committed; the peer \
			 issued {from_peer:?}"
		);
	}

	/// A window claim whose transaction straddles a committed reposition must
	/// not survive it.
	///
	/// The claim reads the definition and scans the batch rows, computes a
	/// window from them, and commits. If a `DEFINE SEQUENCE OVERWRITE` commits
	/// between that read and that commit, the window was computed from rows the
	/// overwrite deleted, yet nothing the two transactions touch in common is
	/// *written* by both — the overwrite's range delete only saw rows committed
	/// before its snapshot — so under snapshot isolation both commit. The claim
	/// row then survives the reset: the first-write check finds it present, owned
	/// and correctly bounded, and the rebuild resumes into it. A peer that
	/// claimed after the reset computed its window from `START`, and the two
	/// overlap.
	///
	/// The straddle is made deterministic by intercepting the claim
	/// transaction's `commit` and running the reposition — and the peer's own
	/// claim — inside it, through the real `check_batch_allocation`.
	#[tokio::test(flavor = "multi_thread")]
	async fn a_claim_straddling_a_reposition_does_not_survive_it() {
		use std::collections::HashSet;
		use std::sync::Mutex as StdMutex;

		let (tf, hook) = hooked::hooked_factory().await;

		let ns = NamespaceId(1);
		let db = DatabaseId(2);
		let name = "sq";
		const BATCH: u32 = 1000;
		let n1 = Sequences::new(tf.clone(), Uuid::from_u128(101));
		let n2 = Sequences::new(tf.clone(), Uuid::from_u128(102));
		define_sequence(&tf, &n1, ns, db, name, 1, BATCH).await.unwrap();

		// N1 has served, so its window `[1, 1001)` and cursor are committed.
		{
			let tx = tf.transaction(TransactionType::Read, n1.clone()).await.unwrap();
			assert_eq!(n1.next_user_sequence_id(None, &tx, ns, db, name).await.unwrap(), 1);
			tx.cancel().await.unwrap();
		}

		// Inside N2's claim commit: the reposition lands, and N1 — evicted by
		// the statement on its own node — rebuilds and claims from `START 500`
		// through the real allocator. Both commit before the straddling claim
		// does.
		let a1_slot: Arc<StdMutex<Option<Sequence>>> = Arc::new(StdMutex::new(None));
		{
			let tf = tf.clone();
			let n1 = n1.clone();
			let a1_slot = Arc::clone(&a1_slot);
			*hook.lock().unwrap() = Some(Box::new(move || {
				Box::pin(async move {
					define_sequence(&tf, &n1, ns, db, name, 500, BATCH).await.unwrap();
					let domain = SequenceDomain::new_user(ns, db, name);
					let a1 = Sequence::load(None, &n1, &domain, 500, BATCH, None)
						.await
						.expect("N1's claim after the reposition");
					*a1_slot.lock().unwrap() = Some(a1);
				})
			}));
		}

		// N2's first load: its claim transaction reads the definition and the
		// rows before the reposition and commits after it.
		let domain = SequenceDomain::new_user(ns, db, name);
		let a2 = match Sequence::load(None, &n2, &domain, 1, BATCH, None).await {
			// The claim noticed the reset: this is the outcome the hole's fix
			// produces. Rebuild, as `next_val` would.
			Err(SequenceError::Reset) => {
				Sequence::load(None, &n2, &domain, 1, BATCH, None).await.unwrap()
			}
			Ok(a2) => a2,
			Err(e) => panic!("N2's load failed: {e}"),
		};
		assert!(hook.lock().unwrap().is_none(), "the straddle did not run");
		let a1 = a1_slot.lock().unwrap().take().expect("N1's allocator");

		let (w1, w2) = ((a1.st.next, a1.to), (a2.st.next, a2.to));

		// And prove it by serving: N1 walks up through 1001, N2 starts there.
		let mut a1 = a1;
		let mut a2 = a2;
		let mut issued = HashSet::new();
		let mut twice = Vec::new();
		for _ in 0..502 {
			let v = a1.next(&n1, None, &domain, BATCH, None, None).await.unwrap();
			if !issued.insert(v) {
				twice.push(v);
			}
		}
		for _ in 0..3 {
			let v = a2.next(&n2, None, &domain, BATCH, None, None).await.unwrap();
			if !issued.insert(v) {
				twice.push(v);
			}
		}
		assert!(twice.is_empty(), "ids issued twice after a reposition: {twice:?}");
		assert!(
			w1.1 <= w2.0 || w2.1 <= w1.0,
			"the windows overlap: N1 [{}, {}) and N2 [{}, {})",
			w1.0,
			w1.1,
			w2.0,
			w2.1
		);
	}

	/// A reset that lands while the allocator is being built for the first time
	/// is rebuilt through, not reported.
	///
	/// With nothing cached there is no allocator to evict, so the reset is this
	/// call's first sight of one: the load reads the definition and the cursor,
	/// the claim straddles the reposition, and the claim is rejected. Reporting
	/// that would fail the caller's query over a sequence that is perfectly
	/// usable a moment later, and it is the same straddle a cached allocator
	/// survives.
	///
	/// Driven through `next_user_sequence_id` rather than the allocator, because
	/// the failure this pins is one the public path had and the sibling tests
	/// did not see: they rebuild by calling `Sequence::load` themselves.
	#[tokio::test(flavor = "multi_thread")]
	async fn a_reset_while_building_is_rebuilt_not_reported() {
		let (tf, hook) = hooked::hooked_factory().await;

		let ns = NamespaceId(1);
		let db = DatabaseId(2);
		let name = "sq";
		const BATCH: u32 = 1000;
		let n1 = Sequences::new(tf.clone(), Uuid::from_u128(101));
		let n2 = Sequences::new(tf.clone(), Uuid::from_u128(102));
		define_sequence(&tf, &n1, ns, db, name, 1, BATCH).await.unwrap();

		// The reposition lands inside N2's claim commit — the first commit N2
		// makes, since its cache is empty and the load only reads.
		{
			let tf = tf.clone();
			let n1 = n1.clone();
			*hook.lock().unwrap() = Some(Box::new(move || {
				Box::pin(async move {
					define_sequence(&tf, &n1, ns, db, name, 500, BATCH).await.unwrap();
				})
			}));
		}

		let tx = tf.transaction(TransactionType::Read, n2.clone()).await.unwrap();
		let got = n2
			.next_user_sequence_id(None, &tx, ns, db, name)
			.await
			.expect("a reset seen while building must be rebuilt through, not reported");
		tx.cancel().await.unwrap();

		assert!(hook.lock().unwrap().is_none(), "the straddle did not run");
		assert_eq!(got, 500, "the rebuild allocates from the committed `START`");
	}

	/// An allocator's first cursor write, straddling a committed reposition,
	/// must not survive it.
	///
	/// The first write cannot compare cursors — there is no previous one, so it
	/// asserts the row is absent, and a reset leaves it absent. What it checks
	/// instead is that the window it serves from is still claimed. Read under
	/// snapshot isolation, that check protects nothing: the reset deletes the
	/// claim row and every cursor row it can see, and this node's cursor row
	/// does not exist yet, so the two transactions write nothing in common and
	/// both commit. The cursor is then durable in a row the reset never saw, the
	/// node carries on serving the window the reset cleared, and every later
	/// write compares against a cursor of its own making. A peer rebuilding from
	/// the new `START` claims the same range.
	///
	/// This is the regime a reposition to the reported `resume` bound lives in:
	/// that bound is the highest cursor, and a node that has claimed a window
	/// but written no cursor yet has that window right there.
	#[tokio::test(flavor = "multi_thread")]
	async fn a_first_cursor_write_straddling_a_reposition_does_not_survive_it() {
		use std::collections::HashSet;
		use std::sync::Mutex as StdMutex;

		let (tf, hook) = hooked::hooked_factory().await;
		let ns = NamespaceId(1);
		let db = DatabaseId(2);
		let name = "sq";
		const BATCH: u32 = 10;
		let n1 = Sequences::new(tf.clone(), Uuid::from_u128(101));
		let n2 = Sequences::new(tf.clone(), Uuid::from_u128(102));
		define_sequence(&tf, &n1, ns, db, name, 1, BATCH).await.unwrap();

		// N1 has claimed `[1, 11)` and written no cursor: the state this covers.
		let domain = SequenceDomain::new_user(ns, db, name);
		let mut a1 = Sequence::load(None, &n1, &domain, 1, BATCH, None).await.unwrap();
		assert_eq!((a1.st.next, a1.to), (1, 11));

		// Inside N1's first cursor write: the sequence is repositioned to its
		// own `resume` bound — 1, since nothing has been issued — and N2 claims
		// from it through the real allocator. Both commit before the straddling
		// write does.
		let a2_slot: Arc<StdMutex<Option<Sequence>>> = Arc::new(StdMutex::new(None));
		{
			let tf = tf.clone();
			let n2 = n2.clone();
			let a2_slot = Arc::clone(&a2_slot);
			*hook.lock().unwrap() = Some(Box::new(move || {
				Box::pin(async move {
					reposition_to_resume(&tf, &n2, ns, db, name, BATCH).await.unwrap();
					let domain = SequenceDomain::new_user(ns, db, name);
					let a2 = Sequence::load(None, &n2, &domain, 1, BATCH, None)
						.await
						.expect("N2's claim after the reposition");
					*a2_slot.lock().unwrap() = Some(a2);
				})
			}));
		}

		let mut issued = HashSet::new();
		let mut twice = Vec::new();
		// Either the write notices — as a reset, or as the conflict it loses —
		// and N1 rebuilds as `next_val` would, or it commits and N1 serves on
		// from a window the sequence no longer has.
		match a1.next(&n1, None, &domain, BATCH, None, None).await {
			Ok(v) => {
				issued.insert(v);
			}
			Err(SequenceError::Reset) => {
				a1 = Sequence::load(None, &n1, &domain, 1, BATCH, None).await.unwrap();
			}
			Err(e) => panic!("N1's first write failed: {e}"),
		}
		assert!(hook.lock().unwrap().is_none(), "the straddle did not run");
		let mut a2 = a2_slot.lock().unwrap().take().expect("N2's allocator");

		let (w1, w2) = ((a1.st.next, a1.to), (a2.st.next, a2.to));

		// And prove it by serving: each walks the rest of its window.
		for _ in 0..(BATCH as usize - 1) {
			let v = a1.next(&n1, None, &domain, BATCH, None, None).await.unwrap();
			if !issued.insert(v) {
				twice.push(v);
			}
		}
		for _ in 0..BATCH as usize {
			let v = a2.next(&n2, None, &domain, BATCH, None, None).await.unwrap();
			if !issued.insert(v) {
				twice.push(v);
			}
		}
		assert!(twice.is_empty(), "ids issued twice after a reposition: {twice:?}");
		assert!(
			w1.1 <= w2.0 || w2.1 <= w1.0,
			"the windows overlap: N1 [{}, {}) and N2 [{}, {})",
			w1.0,
			w1.1,
			w2.0,
			w2.1
		);
	}

	/// Allocation never issues the same id twice, however `DEFINE SEQUENCE
	/// OVERWRITE` interleaves with it.
	///
	/// The targeted tests each pin one interleaving that was reasoned about.
	/// This one does not choose the interleaving: several nodes allocate
	/// concurrently while the sequence is repositioned underneath them, and the
	/// only thing asserted is the property that has to hold regardless — an id
	/// handed out once is never handed out again.
	///
	/// Repositions move the start strictly forward, past anything the workers
	/// can have reached, which is the repair an operator actually performs and
	/// what makes uniqueness an invariant rather than a coincidence: overwriting
	/// *backwards* is defined to reissue.
	///
	/// A worker may legitimately come away empty. A reset landing on a freshly
	/// rebuilt allocator is reported rather than retried forever, and a write
	/// can lose a transaction conflict; neither issues an id, so neither can
	/// break uniqueness. Anything else is a real failure and fails the test.
	#[tokio::test(flavor = "multi_thread")]
	async fn concurrent_allocation_never_reissues_across_repositions() {
		use std::collections::HashSet;

		let (tf, _) = factory().await;
		let ns = NamespaceId(1);
		let db = DatabaseId(2);
		let name = "sq";
		// Small enough that a node revisits the store often, so the repositions
		// land inside allocation rather than between quiet stretches of it.
		const BATCH: u32 = 10;
		const NODES: u128 = 3;
		const TASKS_PER_NODE: usize = 3;
		const ALLOCATIONS: usize = 40;
		// Each reposition clears everything below it, and the stride is far
		// above what the workers can allocate inside one epoch, so a value from
		// an earlier epoch can never collide with a later one.
		const STRIDE: i64 = 10_000;
		const EPOCHS: i64 = 3;

		define_sequence(&tf, &tf_sequences(&tf), ns, db, name, 1, BATCH).await.unwrap();

		let mut workers = Vec::new();
		for node in 0..NODES {
			// One allocator per node, shared by its tasks — the cache whose
			// staleness is the thing under test.
			let seqs = Sequences::new(tf.clone(), Uuid::from_u128(node));
			for _ in 0..TASKS_PER_NODE {
				let seqs = seqs.clone();
				let tf = tf.clone();
				workers.push(tokio::spawn(async move {
					let mut got = Vec::new();
					for _ in 0..ALLOCATIONS {
						let tx = tf.transaction(TransactionType::Read, seqs.clone()).await.unwrap();
						let res = seqs.next_user_sequence_id(None, &tx, ns, db, name).await;
						tx.cancel().await.unwrap();
						match res {
							Ok(v) => got.push(v),
							Err(e)
								if is_sequence_reset(&e)
									|| crate::is_retryable_transaction_conflict(&e) => {}
							Err(e) => panic!("allocation failed: {e}"),
						}
					}
					got
				}));
			}
		}

		let repositioner = {
			let tf = tf.clone();
			tokio::spawn(async move {
				let sqs = tf_sequences(&tf);
				for epoch in 1..=EPOCHS {
					tokio::time::sleep(Duration::from_millis(5)).await;
					let start = epoch * STRIDE;
					// Competes with the allocators for the same rows, so a lost
					// conflict is expected and retried.
					loop {
						match define_sequence(&tf, &sqs, ns, db, name, start, BATCH).await {
							Ok(()) => break,
							Err(e) if crate::is_retryable_transaction_conflict(&e) => continue,
							Err(e) => panic!("reposition failed: {e}"),
						}
					}
				}
			})
		};

		let mut issued = Vec::new();
		for w in workers {
			issued.extend(
				tokio::time::timeout(Duration::from_secs(60), w)
					.await
					.expect("allocation stalled")
					.expect("a worker panicked"),
			);
		}
		tokio::time::timeout(Duration::from_secs(60), repositioner)
			.await
			.expect("repositioning stalled")
			.expect("the repositioner panicked");

		assert!(!issued.is_empty(), "the fixture issued nothing, so it asserts nothing");
		let unique: HashSet<i64> = issued.iter().copied().collect();
		assert_eq!(
			unique.len(),
			issued.len(),
			"an id was issued twice across a reposition ({} of {} distinct)",
			unique.len(),
			issued.len()
		);

		// Everything settled: the sequence serves from the last reposition, not
		// from a window some node was still holding.
		let seqs = tf_sequences(&tf);
		let tx = tf.transaction(TransactionType::Read, seqs.clone()).await.unwrap();
		let after = seqs.next_user_sequence_id(None, &tx, ns, db, name).await.unwrap();
		tx.cancel().await.unwrap();
		assert!(
			after >= EPOCHS * STRIDE,
			"allocation resumed below the last reposition: {after} < {}",
			EPOCHS * STRIDE
		);
	}

	/// Repositioning a sequence to the bound it reports, while it is being
	/// allocated from, never issues the same id twice.
	///
	/// `resume` is what `INFO FOR DB STRUCTURE` gives an operator, and what the
	/// documentation tells them to reposition to. It is above every id issued,
	/// but it lies *inside* windows that were claimed and not fully served — the
	/// regime the sibling fixture excludes by repositioning far above anything
	/// claimable. Uniqueness holds there only if every node holding such a
	/// window is stopped before it serves from it again, whether or not it has
	/// written a cursor for it yet.
	///
	/// The bound is read in the transaction that writes the definition. Read
	/// separately it goes stale, and a reposition below an id issued in between
	/// is defined to reissue it, which would say nothing about the allocator.
	#[tokio::test(flavor = "multi_thread")]
	async fn repositioning_a_live_sequence_to_resume_never_reissues() {
		use std::collections::HashSet;

		let (tf, _) = factory().await;
		let ns = NamespaceId(1);
		let db = DatabaseId(2);
		let name = "sq";
		// Small enough that a node revisits the store often, so the repositions
		// land inside allocation rather than between quiet stretches of it.
		const BATCH: u32 = 10;
		const NODES: u128 = 3;
		const TASKS_PER_NODE: usize = 3;
		const ALLOCATIONS: usize = 40;
		const EPOCHS: usize = 3;

		define_sequence(&tf, &tf_sequences(&tf), ns, db, name, 1, BATCH).await.unwrap();

		let mut workers = Vec::new();
		for node in 0..NODES {
			// One allocator per node, shared by its tasks — the cache whose
			// staleness is the thing under test.
			let seqs = Sequences::new(tf.clone(), Uuid::from_u128(node));
			for _ in 0..TASKS_PER_NODE {
				let seqs = seqs.clone();
				let tf = tf.clone();
				workers.push(tokio::spawn(async move {
					let mut got = Vec::new();
					for _ in 0..ALLOCATIONS {
						let tx = tf.transaction(TransactionType::Read, seqs.clone()).await.unwrap();
						let res = seqs.next_user_sequence_id(None, &tx, ns, db, name).await;
						tx.cancel().await.unwrap();
						match res {
							Ok(v) => got.push(v),
							Err(e)
								if is_sequence_reset(&e)
									|| crate::is_retryable_transaction_conflict(&e) => {}
							Err(e) => panic!("allocation failed: {e}"),
						}
					}
					got
				}));
			}
		}

		let repositioner = {
			let tf = tf.clone();
			tokio::spawn(async move {
				let sqs = tf_sequences(&tf);
				for _ in 0..EPOCHS {
					tokio::time::sleep(Duration::from_millis(5)).await;
					// Competes with the allocators for the same rows, so a lost
					// conflict is expected and retried.
					loop {
						match reposition_to_resume(&tf, &sqs, ns, db, name, BATCH).await {
							Ok(()) => break,
							Err(e) if crate::is_retryable_transaction_conflict(&e) => continue,
							Err(e) => panic!("reposition failed: {e}"),
						}
					}
				}
			})
		};

		let mut issued = Vec::new();
		for w in workers {
			issued.extend(
				tokio::time::timeout(Duration::from_secs(60), w)
					.await
					.expect("allocation stalled")
					.expect("a worker panicked"),
			);
		}
		tokio::time::timeout(Duration::from_secs(60), repositioner)
			.await
			.expect("repositioning stalled")
			.expect("the repositioner panicked");

		assert!(!issued.is_empty(), "the fixture issued nothing, so it asserts nothing");
		let unique: HashSet<i64> = issued.iter().copied().collect();
		assert_eq!(
			unique.len(),
			issued.len(),
			"an id was issued twice across a reposition ({} of {} distinct)",
			unique.len(),
			issued.len()
		);

		// Everything settled: a node arriving now serves above everything the
		// sequence has issued, not from a window some node was still holding.
		let top = issued.iter().copied().max().unwrap();
		let seqs = tf_sequences(&tf);
		let tx = tf.transaction(TransactionType::Read, seqs.clone()).await.unwrap();
		let after = seqs.next_user_sequence_id(None, &tx, ns, db, name).await.unwrap();
		tx.cancel().await.unwrap();
		assert!(after > top, "allocation resumed at {after}, below an id already issued ({top})");
	}

	/// A peer picks up a reset nobody told it about.
	///
	/// `DEFINE SEQUENCE` drops the allocator on the node that ran it, and
	/// nothing carries that eviction to the others — every node caches its own
	/// allocator, keyed by its own node id. The reset is instead visible in the
	/// store: clearing the cursor rows leaves a peer's next conditional write
	/// with no value to match, which is how it learns to reload.
	///
	/// Without that, the peer keeps serving its cached window and hands out ids
	/// the repositioned sequence has already given to someone else.
	#[tokio::test]
	async fn a_peer_reloads_a_sequence_reset_elsewhere() {
		let (tf, _) = factory().await;
		let ns = NamespaceId(1);
		let db = DatabaseId(2);
		let name = "sq";
		// Two nodes over one store, which is the deployment this covers: the
		// node that reruns the definition and a peer that is only serving.
		let peer = Sequences::new(tf.clone(), Uuid::from_u128(1));

		// A window wide enough that the peer would never revisit the store on
		// its own, so nothing but the conditional write can tell it.
		const BATCH: u32 = 1000;
		define_sequence(&tf, &peer, ns, db, name, 1, BATCH).await.unwrap();

		let serve = async |tf: TransactionFactory, peer: Sequences| -> i64 {
			let tx = tf.transaction(TransactionType::Read, peer.clone()).await.unwrap();
			let v = peer.next_user_sequence_id(None, &tx, ns, db, name).await;
			tx.cancel().await.unwrap();
			v.unwrap()
		};
		for expected in 1..=2 {
			assert_eq!(serve(tf.clone(), peer.clone()).await, expected);
		}

		// What `DEFINE SEQUENCE OVERWRITE ... START 500` leaves behind, applied
		// by another node. The peer is told nothing.
		define_sequence(&tf, &peer, ns, db, name, 500, BATCH).await.unwrap();

		assert_eq!(
			serve(tf.clone(), peer.clone()).await,
			500,
			"a peer kept serving a sequence that was repositioned elsewhere"
		);
	}

	/// A node building its first allocator uses the definition in force, even
	/// though an absent cursor looks the same as first use.
	///
	/// The conditional write catches a reset only once an allocator is cached:
	/// with none, the first write is a put-if-absent, and a reset leaves the
	/// row absent — so it succeeds and reports nothing. Nothing distinguishes
	/// the two states at that point, which is why the start is taken from the
	/// store rather than from what the caller read.
	#[tokio::test]
	async fn a_first_load_ignores_a_start_the_caller_read_before_a_reset() {
		let (tf, _) = factory().await;
		let ns = NamespaceId(1);
		let db = DatabaseId(2);
		let name = "sq";
		const BATCH: u32 = 1000;
		define_sequence(&tf, &tf_sequences(&tf), ns, db, name, 1, BATCH).await.unwrap();

		// A node that has served this sequence, so ids 1.. are in use.
		let first = Sequences::new(tf.clone(), Uuid::from_u128(11));
		{
			let tx = tf.transaction(TransactionType::Read, first.clone()).await.unwrap();
			assert_eq!(first.next_user_sequence_id(None, &tx, ns, db, name).await.unwrap(), 1);
			tx.cancel().await.unwrap();
		}

		// A different node, holding no allocator at all. Its transaction reads
		// the old definition, and the overwrite commits before it allocates.
		let fresh = Sequences::new(tf.clone(), Uuid::from_u128(12));
		let stale = tf.transaction(TransactionType::Read, fresh.clone()).await.unwrap();
		let _ = stale.get_db_sequence(ns, db, name, None).await.unwrap();
		define_sequence(&tf, &fresh, ns, db, name, 500, BATCH).await.unwrap();

		let got = fresh.next_user_sequence_id(None, &stale, ns, db, name).await.unwrap();
		stale.cancel().await.unwrap();
		assert_eq!(got, 500, "a first load reissued from a start the caller read before the reset");
	}

	/// A reset that lands mid-statement allocates from the definition now in
	/// force, not the one the caller had already read.
	///
	/// The caller reads the definition, then the allocator writes its cursor;
	/// a `DEFINE SEQUENCE OVERWRITE` committing in between is detected by that
	/// write. Reloading on what the caller read would rebuild the allocator at
	/// the old start and hand out ids the sequence has already issued, so the
	/// reset is reported to the caller, which reads the definition again.
	#[tokio::test]
	async fn a_reset_mid_statement_allocates_from_the_new_definition() {
		let (tf, _) = factory().await;
		let ns = NamespaceId(1);
		let db = DatabaseId(2);
		let name = "sq";
		let node = Sequences::new(tf.clone(), Uuid::from_u128(7));
		const BATCH: u32 = 1000;
		define_sequence(&tf, &node, ns, db, name, 1, BATCH).await.unwrap();

		{
			let tx = tf.transaction(TransactionType::Read, node.clone()).await.unwrap();
			assert_eq!(node.next_user_sequence_id(None, &tx, ns, db, name).await.unwrap(), 1);
			tx.cancel().await.unwrap();
		}

		// Opened before the overwrite, so it carries the old definition into
		// the call that runs after it — the window this covers.
		let stale = tf.transaction(TransactionType::Read, node.clone()).await.unwrap();
		let _ = stale.get_db_sequence(ns, db, name, None).await.unwrap();
		define_sequence(&tf, &node, ns, db, name, 500, BATCH).await.unwrap();

		let got = node.next_user_sequence_id(None, &stale, ns, db, name).await.unwrap();
		stale.cancel().await.unwrap();
		assert_eq!(got, 500, "the retry allocated from the definition the caller had already read");
	}
}
