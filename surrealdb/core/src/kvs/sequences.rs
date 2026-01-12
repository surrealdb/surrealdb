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

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::ops::Range;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use rand::{Rng, thread_rng};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};
use tokio::time::sleep;
use uuid::Uuid;

use crate::catalog::providers::DatabaseProvider;
use crate::catalog::{DatabaseId, IndexId, NamespaceId, TableId};
use crate::ctx::Context;
use crate::err::Error;
use crate::idx::IndexKeyBase;
use crate::idx::seqdocids::DocId;
use crate::key::database::th::TableIdGeneratorBatchKey;
use crate::key::database::ti::TableIdGeneratorStateKey;
use crate::key::namespace::dh::DatabaseIdGeneratorBatchKey;
use crate::key::namespace::di::DatabaseIdGeneratorStateKey;
use crate::key::root::nh::NamespaceIdGeneratorBatchKey;
use crate::key::root::ni::NamespaceIdGeneratorStateKey;
use crate::key::sequence::Prefix;
use crate::key::sequence::ba::Ba;
use crate::key::sequence::st::St;
use crate::key::table::ih::IndexIdGeneratorBatchKey;
use crate::key::table::is::IndexIdGeneratorStateKey;
use crate::kvs::ds::TransactionFactory;
use crate::kvs::{KVKey, LockType, Transaction, TransactionType, impl_kv_value_revisioned};
use crate::val::TableName;

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
	/// A sequence generating DocIds for a FullText search index
	FullTextDocIds(IndexKeyBase),
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

	pub(crate) fn new_ft_doc_ids(ikb: IndexKeyBase) -> Self {
		Self::FullTextDocIds(ikb)
	}

	pub(crate) fn new_namespace_ids() -> Self {
		Self::NameSpacesIds
	}

	pub(crate) fn new_database_ids(ns: NamespaceId) -> Self {
		Self::DatabasesIds(ns)
	}

	pub(crate) fn new_table_ids(ns: NamespaceId, db: DatabaseId) -> Self {
		Self::TablesIds(ns, db)
	}

	pub(crate) fn new_index_ids(ns: NamespaceId, db: DatabaseId, tb: TableName) -> Self {
		Self::IndexIds(ns, db, tb)
	}

	fn new_batch_range_keys(&self) -> Result<Range<Vec<u8>>> {
		match self {
			Self::UserName(ns, db, sq) => Prefix::new_ba_range(*ns, *db, sq),
			Self::FullTextDocIds(ibk) => ibk.new_ib_range(),
			Self::NameSpacesIds => NamespaceIdGeneratorBatchKey::range(),
			Self::DatabasesIds(ns) => DatabaseIdGeneratorBatchKey::range(*ns),
			Self::TablesIds(ns, db) => TableIdGeneratorBatchKey::range(*ns, *db),
			Self::IndexIds(ns, db, tb) => IndexIdGeneratorBatchKey::range(*ns, *db, tb),
		}
	}

	fn new_batch_key(&self, start: i64) -> Result<Vec<u8>> {
		match &self {
			Self::UserName(ns, db, sq) => Ba::new(*ns, *db, sq, start).encode_key(),
			Self::FullTextDocIds(ikb) => ikb.new_ib_key(start).encode_key(),
			Self::NameSpacesIds => NamespaceIdGeneratorBatchKey::new(start).encode_key(),
			Self::DatabasesIds(ns) => DatabaseIdGeneratorBatchKey::new(*ns, start).encode_key(),
			Self::TablesIds(ns, db) => TableIdGeneratorBatchKey::new(*ns, *db, start).encode_key(),
			Self::IndexIds(ns, db, tb) => {
				IndexIdGeneratorBatchKey::new(*ns, *db, tb, start).encode_key()
			}
		}
	}

	fn new_state_key(&self, nid: Uuid) -> Result<Vec<u8>> {
		match &self {
			Self::UserName(ns, db, sq) => St::new(*ns, *db, sq, nid).encode_key(),
			Self::FullTextDocIds(ikb) => ikb.new_is_key(nid).encode_key(),
			Self::NameSpacesIds => NamespaceIdGeneratorStateKey::new(nid).encode_key(),
			Self::DatabasesIds(ns) => DatabaseIdGeneratorStateKey::new(*ns, nid).encode_key(),
			Self::TablesIds(ns, db) => TableIdGeneratorStateKey::new(*ns, *db, nid).encode_key(),
			Self::IndexIds(ns, db, tb) => {
				IndexIdGeneratorStateKey::new(*ns, *db, tb, nid).encode_key()
			}
		}
	}
}

/// Represents a batch allocation of IDs in the key-value store.
///
/// A batch allocation reserves a range of IDs for a specific node (identified by `owner`).
/// The range is from some starting value (stored in the key) up to (but not including) `to`.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
pub(crate) struct BatchValue {
	/// The exclusive upper bound of the batch allocation
	to: i64,
	/// The UUID of the node that owns this batch allocation
	owner: Uuid,
}
impl_kv_value_revisioned!(BatchValue);

/// Tracks the next available ID for a specific node in a sequence.
///
/// Each node maintains its own `SequenceState` which tracks the next ID it will
/// allocate from its current batch. This state is persisted to coordinate with
/// batch allocations and ensure no ID is used twice.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
pub(crate) struct SequenceState {
	/// The next ID to be allocated by this node
	next: i64,
}
impl_kv_value_revisioned!(SequenceState);

impl Sequences {
	pub(super) fn new(tf: TransactionFactory, nid: Uuid) -> Self {
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
	pub(crate) async fn namespace_removed(&self, tx: &Transaction, ns: NamespaceId) -> Result<()> {
		for db in tx.all_db(ns).await?.iter() {
			self.database_removed(tx, ns, db.database_id).await?;
		}
		Ok(())
	}

	/// Cleans up all sequences associated with a removed database.
	///
	/// This method is called when a database is deleted to remove all cached
	/// sequence state for user-defined sequences within that database.
	pub(crate) async fn database_removed(
		&self,
		tx: &Transaction,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<()> {
		for sqs in tx.all_db_sequences(ns, db).await?.iter() {
			self.sequence_removed(ns, db, &sqs.name).await;
		}
		Ok(())
	}

	/// Removes a specific user-defined sequence from the cache.
	///
	/// This method is called when a sequence is deleted to clean up its cached state.
	pub(crate) async fn sequence_removed(&self, ns: NamespaceId, db: DatabaseId, sq: &str) {
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
		ctx: Option<&Context>,
		seq: Arc<SequenceDomain>,
		start: i64,
		batch: u32,
		timeout: Option<Duration>,
	) -> Result<i64> {
		let sequence = self.sequences.read().await.get(&seq).cloned();
		if let Some(s) = sequence {
			return s.lock().await.next(self, ctx, &seq, batch).await;
		}
		let s = match self.sequences.write().await.entry(seq.clone()) {
			Entry::Occupied(e) => e.get().clone(),
			Entry::Vacant(e) => {
				let s = Arc::new(Mutex::new(
					Sequence::load(ctx, self, &seq, start, batch, timeout).await?,
				));
				e.insert(s).clone()
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
	pub(crate) async fn next_namespace_id(&self, ctx: Option<&Context>) -> Result<NamespaceId> {
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
	pub(crate) async fn next_database_id(
		&self,
		ctx: Option<&Context>,
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
	pub(crate) async fn next_table_id(
		&self,
		ctx: Option<&Context>,
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
	pub(crate) async fn next_index_id(
		&self,
		ctx: Option<&Context>,
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
	pub(crate) async fn next_user_sequence_id(
		&self,
		ctx: Option<&Context>,
		tx: &Transaction,
		ns: NamespaceId,
		db: DatabaseId,
		sq: &str,
	) -> Result<i64> {
		let seq = tx.get_db_sequence(ns, db, sq).await?;
		let domain = Arc::new(SequenceDomain::new_user(ns, db, sq));
		self.next_val(ctx, domain, seq.start, seq.batch, seq.timeout).await
	}

	/// Generates the next document ID for a full-text search index.
	///
	/// # Arguments
	/// * `ctx` - Optional mutable context for transaction operations
	/// * `ikb` - The index key base identifying the full-text index
	/// * `batch` - The batch size for ID allocation
	///
	/// # Returns
	/// A new unique document ID for the full-text search index
	pub(crate) async fn next_fts_doc_id(
		&self,
		ctx: Option<&Context>,
		ikb: IndexKeyBase,
		batch: u32,
	) -> Result<DocId> {
		let domain = Arc::new(SequenceDomain::new_ft_doc_ids(ikb));
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
	state_key: Vec<u8>,
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
		ctx: Option<&Context>,
		sqs: &Sequences,
		seq: &SequenceDomain,
		start: i64,
		batch: u32,
		timeout: Option<Duration>,
	) -> Result<Self> {
		let state_key = seq.new_state_key(sqs.nid)?;
		// Create a separate transaction for reading sequence state to avoid conflicts
		// with the parent transaction in strict serialization mode (e.g., FDB)
		let tx =
			sqs.tf.transaction(TransactionType::Read, LockType::Optimistic, sqs.clone()).await?;
		let mut st: SequenceState = if let Some(v) = tx.get(&state_key, None).await? {
			revision::from_slice(&v)?
		} else {
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
		ctx: Option<&Context>,
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
		let tx =
			self.tf.transaction(TransactionType::Write, LockType::Optimistic, sqs.clone()).await?;

		// Execute operations and ensure transaction is cancelled on error
		match tx.set(&self.state_key, &revision::to_vec(&self.st)?, None).await {
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
		ctx: Option<&Context>,
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
					let timeout = (*to).into();
					return Err(anyhow::Error::new(Error::QueryTimedout(timeout)));
				}
			}
			if let Ok(r) = Self::check_batch_allocation(sqs, seq, next, batch).await {
				return Ok(r);
			}
			// exponential backoff with full jitter
			let sleep_ms = thread_rng().gen_range(1..=tempo);
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
		let tx =
			sqs.tf.transaction(TransactionType::Write, LockType::Optimistic, sqs.clone()).await?;

		// Execute operations and ensure transaction is cancelled on error
		let result = async {
			let batch_range = seq.new_batch_range_keys()?;
			let val = tx.getr(batch_range, None).await?;
			let mut next_start = next;
			// Scan every existing batch
			for (key, val) in val.iter() {
				let ba: BatchValue = revision::from_slice(val)?;
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
					tx.del(key).await?;
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
			tx.set(&batch_key, &bv, None).await?;
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
