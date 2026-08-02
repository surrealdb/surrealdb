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
use std::time::Duration;

use anyhow::Result;
use common::EngineError;
use rand::Rng;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use surrealdb_catalog::providers::{
	CancellationProbe, DatabaseProvider, NamespaceProvider, TableProvider,
};
use surrealdb_catalog::{DatabaseId, IndexId, NamespaceId, TableId};
use surrealdb_kvs::key::{KVKey, TypedRange};
use surrealdb_kvs::{Key, impl_kv_value_revisioned};
use surrealdb_strand::TableName;
use tokio::sync::{Mutex, RwLock};
use tokio::time::sleep;
use uuid::Uuid;
use web_time::Instant;

use crate::factory::TransactionFactory;
use crate::key::schema::{
	DbIdBatchKey, DbIdBatchPrefix, DbIdStateKey, DocIdBatchKey, DocIdBatchPrefix, DocIdStateKey,
	IndexIdBatchKey, IndexIdBatchPrefix, IndexIdStateKey, NsIdBatchKey, NsIdBatchPrefix,
	NsIdStateKey, SeqBatchKey, SeqBatchPrefix, SeqStateKey, TbIdBatchKey, TbIdBatchPrefix,
	TbIdStateKey,
};
use crate::values::ids::DocId;
use crate::{Transaction, TransactionType};

type SequencesMap = Arc<RwLock<HashMap<Arc<SequenceDomain>, Arc<Mutex<Sequence>>>>>;

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
		}
	}

	fn new_batch_key(&self, start: i64) -> Result<Key<'static>> {
		match &self {
			Self::UserName(ns, db, sq) => SeqBatchKey {
				ns: *ns,
				db: *db,
				sq: Cow::Borrowed(sq.as_str()),
				start,
			}
			.encode_key(),
			Self::TableDocIds(ns, db, tb) => {
				DocIdBatchKey::new(*ns, *db, Cow::Borrowed(tb), start).encode_key()
			}
			Self::NameSpacesIds => NsIdBatchKey {
				start,
			}
			.encode_key(),
			Self::DatabasesIds(ns) => DbIdBatchKey {
				ns: *ns,
				start,
			}
			.encode_key(),
			Self::TablesIds(ns, db) => TbIdBatchKey {
				ns: *ns,
				db: *db,
				start,
			}
			.encode_key(),
			Self::IndexIds(ns, db, tb) => IndexIdBatchKey {
				ns: *ns,
				db: *db,
				tb: Cow::Borrowed(tb),
				start,
			}
			.encode_key(),
		}
	}

	fn new_state_key(&self, nid: Uuid) -> Result<Key<'static>> {
		match &self {
			Self::UserName(ns, db, sq) => SeqStateKey {
				ns: *ns,
				db: *db,
				sq: Cow::Borrowed(sq.as_str()),
				nid,
			}
			.encode_key(),
			Self::TableDocIds(ns, db, tb) => {
				DocIdStateKey::new(*ns, *db, Cow::Borrowed(tb), nid).encode_key()
			}
			Self::NameSpacesIds => NsIdStateKey {
				nid,
			}
			.encode_key(),
			Self::DatabasesIds(ns) => DbIdStateKey {
				ns: *ns,
				nid,
			}
			.encode_key(),
			Self::TablesIds(ns, db) => TbIdStateKey {
				ns: *ns,
				db: *db,
				nid,
			}
			.encode_key(),
			Self::IndexIds(ns, db, tb) => IndexIdStateKey {
				ns: *ns,
				db: *db,
				tb: Cow::Borrowed(tb),
				nid,
			}
			.encode_key(),
		}
	}
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

	/// Removes a specific user-defined sequence from the cache.
	///
	/// This method is called when a sequence is deleted to clean up its cached state.
	pub async fn sequence_removed(&self, ns: NamespaceId, db: DatabaseId, sq: &str) {
		let key = SequenceDomain::new_user(ns, db, sq);
		self.sequences.write().await.remove(&key);
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
			return s.lock().await.next(self, ctx, &seq, batch).await;
		}
		let s = match self.sequences.write().await.entry(Arc::clone(&seq)) {
			Entry::Occupied(e) => Arc::clone(e.get()),
			Entry::Vacant(e) => {
				let s = Arc::new(Mutex::new(
					Sequence::load(ctx, self, &seq, start, batch, timeout).await?,
				));
				Arc::clone(e.insert(s))
			}
		};
		s.lock().await.next(self, ctx, &seq, batch).await
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
		let seq = tx.get_db_sequence(ns, db, sq, None).await?;
		let domain = Arc::new(SequenceDomain::new_user(ns, db, sq));
		self.next_val(ctx, domain, seq.start, seq.batch, seq.timeout).await
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
}

/// Internal per-node sequence state manager.
///
/// This struct manages the local state for a specific sequence on a specific node.
/// It tracks the current position within an allocated batch and coordinates with
/// the distributed batch allocation system when the current batch is exhausted.
struct Sequence {
	/// Transaction factory for creating transactions to persist state
	tf: TransactionFactory,
	/// The current state tracking the next ID to allocate
	st: SequenceState,
	/// Optional timeout for batch allocation operations
	timeout: Option<Duration>,
	/// The exclusive upper bound of the current batch allocation
	to: i64,
	/// The key used to persist this sequence's state
	state_key: Key<'static>,
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
	) -> Result<Self> {
		let state_key = seq.new_state_key(sqs.nid)?;
		// Create a separate transaction for reading sequence state to avoid conflicts
		// with the parent transaction in strict serialization mode (e.g., FDB)
		let tx = sqs.tf.transaction(TransactionType::Read, sqs.clone()).await?;
		let mut st: SequenceState = if let Some(v) = tx.get(state_key.as_borrowed(), None).await? {
			revision::from_slice(&v)?
		} else {
			// First boot for this sequence: bump the configured start past any IDs
			// already issued via the catalog so we never reuse live namespace,
			// database, table, or index identifiers.
			let start = Self::seed_start_from_catalog(&tx, seq, start).await?;
			SequenceState {
				next: start,
			}
		};
		tx.cancel().await?;
		let (from, to) =
			Self::find_batch_allocation(sqs, ctx, seq, st.next, batch, timeout).await?;
		st.next = from;
		Ok(Self {
			tf: sqs.tf.clone(),
			state_key,
			to,
			st,
			timeout,
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
			// Table doc IDs and user-defined sequences are not backed by the catalog
			// id-allocation scheme, so there are no pre-existing IDs to avoid
			// colliding with.
			SequenceDomain::TableDocIds(..) | SequenceDomain::UserName(..) => {}
		}
		Ok(seeded)
	}

	/// Gets the next ID from this sequence.
	///
	/// If the current batch is exhausted, this method will allocate a new batch
	/// before returning the next ID. The state is persisted to the key-value store
	/// after each allocation.
	///
	/// # Arguments
	/// * `sqs` - The sequences manager
	/// * `ctx` - Optional mutable context for timeout checking
	/// * `seq` - The sequence domain
	/// * `batch` - The batch size for new allocations if needed
	async fn next(
		&mut self,
		sqs: &Sequences,
		ctx: Option<&dyn CancellationProbe>,
		seq: &SequenceDomain,
		batch: u32,
	) -> Result<i64> {
		if self.st.next >= self.to {
			(self.st.next, self.to) =
				Self::find_batch_allocation(sqs, ctx, seq, self.st.next, batch, self.timeout)
					.await?;
		}
		let v = self.st.next;
		self.st.next += 1;
		// write the state on the KV store
		let tx = self.tf.transaction(TransactionType::Write, sqs.clone()).await?;

		// Execute operations and ensure transaction is cancelled on error
		let data = revision::to_vec(&self.st)?;
		match tx.set(self.state_key.as_borrowed(), &data).await {
			Ok(_) => {
				tx.commit().await?;
				Ok(v)
			}
			Err(e) => {
				tx.cancel().await?;
				Err(e)
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
	/// A tuple of (start, end) representing the allocated batch range [start, end)
	async fn find_batch_allocation(
		sqs: &Sequences,
		ctx: Option<&dyn CancellationProbe>,
		seq: &SequenceDomain,
		next: i64,
		batch: u32,
		to: Option<Duration>,
	) -> Result<(i64, i64)> {
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
					return Err(anyhow::Error::new(EngineError::QueryTimedout(timeout)));
				}
			}
			if let Ok(r) = Self::check_batch_allocation(sqs, seq, next, batch).await {
				return Ok(r);
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
	/// A tuple of (start, end) representing the allocated batch range [start, end)
	async fn check_batch_allocation(
		sqs: &Sequences,
		seq: &SequenceDomain,
		next: i64,
		batch: u32,
	) -> Result<(i64, i64)> {
		let tx = sqs.tf.transaction(TransactionType::Write, sqs.clone()).await?;

		// Execute operations and ensure transaction is cancelled on error
		let result = async {
			let batch_range = seq.new_batch_range_keys()?;
			let batches = tx.getr(batch_range, None).await?;
			let mut next_start = next;
			// Scan every existing batch
			for (key, ba) in batches.iter() {
				next_start = next_start.max(ba.to);
				// The batch belongs to this node
				if ba.owner == sqs.nid {
					// If a previous batch belongs to this node, we can remove it,
					// as we are going to create a new one
					// If the current value is still in the batch range, we return it
					if next < ba.to {
						return Ok((next, ba.to));
					}
					// Otherwise we can remove this old batch and create a new one
					tx.del(key.as_slice().into()).await?;
				}
			}
			// We compute the new batch
			let next_to = next_start + batch as i64;
			// And store it in the KV store
			let bv = revision::to_vec(&BatchValue {
				to: next_to,
				owner: sqs.nid,
			})?;
			let batch_key = seq.new_batch_key(next_start)?;
			// Claim the batch with a conditional create (put-if-absent) rather
			// than a blind `set`. Two nodes exhausting the same domain
			// concurrently observe the same committed state and therefore compute
			// the same `next_start` (a node only reallocates once its current
			// batch is exhausted, so its local `next` never exceeds the highest
			// committed `to`), so they collide on this exact key. On
			// last-writer-wins backends (TiKV) a blind `set` would let both commit
			// and hand out the same range; a conditional create reads the key
			// first, which arms the write-conflict check, so only the first
			// committer wins and the loser is rejected. `find_batch_allocation`
			// then retries, re-scans, and claims the next free range.
			// Conflict-serializing backends (mem/rocksdb/surrealkv) already reject
			// the second writer either way.
			tx.put(batch_key, &bv).await?;
			Ok::<(i64, i64), anyhow::Error>((next_start, next_to))
		}
		.await;

		match result {
			Ok(res) => {
				tx.commit().await?;
				Ok(res)
			}
			Err(e) => {
				tx.cancel().await?;
				Err(e)
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use surrealdb_catalog::providers::{DatabaseProvider, NamespaceProvider, TableProvider};
	use surrealdb_catalog::{
		DatabaseDefinition, DatabaseId, FromStored, Index, IndexDefinition, IndexId,
		NamespaceDefinition, NamespaceId, StoredTableDefinition, TableDefinition, TableId,
	};
	use surrealdb_cnf::ConfigMap;
	use surrealdb_strand::TableName;
	use tokio::sync::Notify;
	use uuid::Uuid;

	use crate::TransactionType;
	use crate::factory::TransactionFactory;
	use crate::sequences::{Sequence, SequenceDomain, Sequences};

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
		let tf =
			TransactionFactory::new(Arc::new(Notify::new()), builder, Arc::new(Default::default()));
		let sequences = Sequences::new(tf.clone(), Uuid::new_v4());
		(tf, sequences)
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
}
