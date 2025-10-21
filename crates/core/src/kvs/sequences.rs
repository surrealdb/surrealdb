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
use crate::ctx::MutableContext;
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

type SequencesMap = Arc<RwLock<HashMap<Arc<SequenceDomain>, Arc<Mutex<Sequence>>>>>;

#[derive(Clone)]
pub struct Sequences {
	tf: TransactionFactory,
	nid: Uuid,
	sequences: SequencesMap,
}

#[derive(Hash, PartialEq, Eq)]
enum SequenceDomain {
	/// A user sequence in a namespace
	UserName(NamespaceId, DatabaseId, String),
	/// A sequence generating DocIds for a FullText search index
	FullTextDocIds(IndexKeyBase),
	/// A sequence generating ids for namespaces
	NameSpacesIds,
	/// A sequence generating ids for databases
	DatabasesIds(NamespaceId),
	/// A sequence generating ids for tables
	TablesIds(NamespaceId, DatabaseId),
	/// A sequence generating ids for tables
	IndexIds(NamespaceId, DatabaseId, String),
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

	pub(crate) fn new_index_ids(ns: NamespaceId, db: DatabaseId, tb: String) -> Self {
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

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
pub(crate) struct BatchValue {
	to: i64,
	owner: Uuid,
}
impl_kv_value_revisioned!(BatchValue);

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
pub(crate) struct SequenceState {
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
	pub(crate) async fn namespace_removed(&self, tx: &Transaction, ns: NamespaceId) -> Result<()> {
		for db in tx.all_db(ns).await?.iter() {
			self.database_removed(tx, ns, db.database_id).await?;
		}
		Ok(())
	}
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

	pub(crate) async fn sequence_removed(&self, ns: NamespaceId, db: DatabaseId, sq: &str) {
		let key = SequenceDomain::new_user(ns, db, sq);
		self.sequences.write().await.remove(&key);
	}

	async fn next_val(
		&self,
		ctx: Option<&MutableContext>,
		tx: &Transaction,
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
					Sequence::load(self, tx, &seq, start, batch, timeout).await?,
				));
				e.insert(s).clone()
			}
		};
		s.lock().await.next(self, ctx, &seq, batch).await
	}

	pub(crate) async fn next_namespace_id(
		&self,
		ctx: Option<&MutableContext>,
		tx: &Transaction,
	) -> Result<NamespaceId> {
		let domain = Arc::new(SequenceDomain::new_namespace_ids());
		let id = self.next_val(ctx, tx, domain, 0, 100, None).await?;
		Ok(NamespaceId(id as u32))
	}

	pub(crate) async fn next_database_id(
		&self,
		ctx: Option<&MutableContext>,
		tx: &Transaction,
		ns: NamespaceId,
	) -> Result<DatabaseId> {
		let domain = Arc::new(SequenceDomain::new_database_ids(ns));
		let id = self.next_val(ctx, tx, domain, 0, 100, None).await?;
		Ok(DatabaseId(id as u32))
	}

	pub(crate) async fn next_table_id(
		&self,
		ctx: Option<&MutableContext>,
		tx: &Transaction,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<TableId> {
		let domain = Arc::new(SequenceDomain::new_table_ids(ns, db));
		let id = self.next_val(ctx, tx, domain, 0, 100, None).await?;
		Ok(TableId(id as u32))
	}

	pub(crate) async fn next_index_id(
		&self,
		ctx: Option<&MutableContext>,
		tx: &Transaction,
		ns: NamespaceId,
		db: DatabaseId,
		tb: String,
	) -> Result<IndexId> {
		let domain = Arc::new(SequenceDomain::new_index_ids(ns, db, tb));
		let id = self.next_val(ctx, tx, domain, 0, 100, None).await?;
		Ok(IndexId(id as u32))
	}

	pub(crate) async fn next_user_sequence_id(
		&self,
		ctx: Option<&MutableContext>,
		tx: &Transaction,
		ns: NamespaceId,
		db: DatabaseId,
		sq: &str,
	) -> Result<i64> {
		let seq = tx.get_db_sequence(ns, db, sq).await?;
		let domain = Arc::new(SequenceDomain::new_user(ns, db, sq));
		self.next_val(ctx, tx, domain, seq.start, seq.batch, seq.timeout).await
	}

	pub(crate) async fn next_fts_doc_id(
		&self,
		ctx: Option<&MutableContext>,
		tx: &Transaction,
		ikb: IndexKeyBase,
		batch: u32,
	) -> Result<DocId> {
		let domain = Arc::new(SequenceDomain::new_ft_doc_ids(ikb));
		let id = self.next_val(ctx, tx, domain, 0, batch, None).await?;
		Ok(id as DocId)
	}
}

struct Sequence {
	tf: TransactionFactory,
	st: SequenceState,
	timeout: Option<Duration>,
	to: i64,
	state_key: Vec<u8>,
}

impl Sequence {
	async fn load(
		sqs: &Sequences,
		tx: &Transaction,
		seq: &SequenceDomain,
		start: i64,
		batch: u32,
		timeout: Option<Duration>,
	) -> Result<Self> {
		let state_key = seq.new_state_key(sqs.nid)?;
		let mut st: SequenceState = if let Some(v) = tx.get(&state_key, None).await? {
			revision::from_slice(&v)?
		} else {
			SequenceState {
				next: start,
			}
		};
		let (from, to) = Self::check_batch_allocation(sqs, seq, st.next, batch).await?;
		st.next = from;
		Ok(Self {
			tf: sqs.tf.clone(),
			state_key,
			to,
			st,
			timeout,
		})
	}

	async fn next(
		&mut self,
		sqs: &Sequences,
		ctx: Option<&MutableContext>,
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
		tx.set(&self.state_key, &revision::to_vec(&self.st)?, None).await?;
		tx.commit().await?;
		Ok(v)
	}

	async fn find_batch_allocation(
		sqs: &Sequences,
		ctx: Option<&MutableContext>,
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
				if ctx.is_timedout().await? {
					break;
				}
			} else {
				yield_now!();
			}
			if let (Some(ref start), Some(ref to)) = (start, to) {
				// We check the time associated with the sequence
				if start.elapsed().ge(to) {
					break;
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
		Err(anyhow::Error::new(Error::QueryTimedout))
	}

	async fn check_batch_allocation(
		sqs: &Sequences,
		seq: &SequenceDomain,
		next: i64,
		batch: u32,
	) -> Result<(i64, i64)> {
		let tx =
			sqs.tf.transaction(TransactionType::Write, LockType::Optimistic, sqs.clone()).await?;
		let batch_range = seq.new_batch_range_keys()?;
		let val = tx.getr(batch_range, None).await?;
		let mut next_start = next;
		// Scan every existing batches
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
		tx.commit().await?;
		Ok((next_start, next_to))
	}
}
