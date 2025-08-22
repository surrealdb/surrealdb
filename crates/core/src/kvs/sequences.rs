use std::ops::Range;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use rand::{Rng, thread_rng};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use tokio::time::sleep;
use uuid::Uuid;

use crate::catalog::{DatabaseId, NamespaceId};
use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::idx::IndexKeyBase;
use crate::key::sequence::Prefix;
use crate::key::sequence::ba::Ba;
use crate::key::sequence::st::St;
use crate::kvs::ds::TransactionFactory;
use crate::kvs::{KVKey, LockType, Transaction, TransactionType, impl_kv_value_revisioned};

#[derive(Clone)]
pub(crate) struct Sequences {
	tf: TransactionFactory,
	sequences: Arc<DashMap<Arc<SequenceDomain>, Sequence>>,
}

#[derive(Hash, PartialEq, Eq)]
pub(crate) enum SequenceDomain {
	/// A user sequence in a namespace
	UserName(NamespaceId, DatabaseId, String),
	/// A sequence generating DocIds for a FullText search index
	FullTextDocIds(IndexKeyBase),
}

impl SequenceDomain {
	fn new_user(ns: NamespaceId, db: DatabaseId, sq: &str) -> Self {
		Self::UserName(ns, db, sq.to_string())
	}

	pub(crate) fn new_ft_doc_ids(ikb: IndexKeyBase) -> Self {
		Self::FullTextDocIds(ikb)
	}

	fn new_batch_range_keys(&self) -> Result<Range<Vec<u8>>> {
		match self {
			Self::UserName(ns, db, sq) => Prefix::new_ba_range(*ns, *db, sq),
			Self::FullTextDocIds(ibk) => ibk.new_ib_range(),
		}
	}

	fn new_batch_key(&self, start: i64) -> Result<Vec<u8>> {
		match &self {
			Self::UserName(ns, db, sq) => Ba::new(*ns, *db, sq, start).encode_key(),
			Self::FullTextDocIds(ikb) => ikb.new_ib_key(start).encode_key(),
		}
	}

	fn new_state_key(&self, nid: Uuid) -> Result<Vec<u8>> {
		match &self {
			Self::UserName(ns, db, sq) => St::new(*ns, *db, sq, nid).encode_key(),
			Self::FullTextDocIds(ikb) => ikb.new_is_key(nid).encode_key(),
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
	pub(super) fn new(tf: TransactionFactory) -> Self {
		Self {
			tf,
			sequences: Arc::new(Default::default()),
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
			self.sequence_removed(ns, db, &sqs.name);
		}
		Ok(())
	}

	pub(crate) fn sequence_removed(&self, ns: NamespaceId, db: DatabaseId, sq: &str) {
		let key = SequenceDomain::new_user(ns, db, sq);
		self.sequences.remove(&key);
	}

	async fn next_val<F>(
		&self,
		ctx: &Context,
		nid: Uuid,
		seq: Arc<SequenceDomain>,
		batch: u32,
		init_params: F,
	) -> Result<i64>
	where
		F: FnOnce() -> (i64, Option<Duration>) + Send + Sync + 'static,
	{
		match self.sequences.entry(seq.clone()) {
			Entry::Occupied(mut e) => e.get_mut().next(ctx, nid, &seq, batch).await,
			Entry::Vacant(e) => {
				let (start, timeout) = init_params();
				let s =
					Sequence::load(self.tf.clone(), &ctx.tx(), nid, &seq, start, batch, timeout)
						.await?;
				e.insert(s).next(ctx, nid, &seq, batch).await
			}
		}
	}

	pub(crate) async fn next_val_user(
		&self,
		ctx: &Context,
		opt: &Options,
		sq: &str,
	) -> Result<i64> {
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
		let seq = ctx.tx().get_db_sequence(ns, db, sq).await?;
		let key = Arc::new(SequenceDomain::new_user(ns, db, sq));
		self.next_val(ctx, opt.id()?, key, seq.batch, move || (seq.start, seq.timeout)).await
	}

	pub(crate) async fn next_val_fts_idx(
		&self,
		ctx: &Context,
		nid: Uuid,
		seq: Arc<SequenceDomain>,
		batch: u32,
	) -> Result<i64> {
		self.next_val(ctx, nid, seq, batch, move || (0, None)).await
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
		tf: TransactionFactory,
		tx: &Transaction,
		nid: Uuid,
		seq: &SequenceDomain,
		start: i64,
		batch: u32,
		timeout: Option<Duration>,
	) -> Result<Self> {
		let state_key = seq.new_state_key(nid)?;
		let mut st: SequenceState = if let Some(v) = tx.get(&state_key, None).await? {
			revision::from_slice(&v)?
		} else {
			SequenceState {
				next: start,
			}
		};
		let (from, to) = Self::check_batch_allocation(&tf, seq, nid, st.next, batch).await?;
		st.next = from;
		Ok(Self {
			tf,
			state_key,
			to,
			st,
			timeout,
		})
	}

	async fn next(
		&mut self,
		ctx: &Context,
		nid: Uuid,
		seq: &SequenceDomain,
		batch: u32,
	) -> Result<i64> {
		if self.st.next >= self.to {
			(self.st.next, self.to) = Self::find_batch_allocation(
				&self.tf,
				ctx,
				nid,
				seq,
				self.st.next,
				batch,
				self.timeout,
			)
			.await?;
		}
		let v = self.st.next;
		self.st.next += 1;
		// write the state on the KV store
		let tx = self.tf.transaction(TransactionType::Write, LockType::Optimistic).await?;
		tx.set(&self.state_key, &revision::to_vec(&self.st)?, None).await?;
		tx.commit().await?;
		Ok(v)
	}

	async fn find_batch_allocation(
		tf: &TransactionFactory,
		ctx: &Context,
		nid: Uuid,
		seq: &SequenceDomain,
		next: i64,
		batch: u32,
		to: Option<Duration>,
	) -> Result<(i64, i64)> {
		// Use for exponential backoff
		let mut rng = thread_rng();
		let mut tempo = 4;
		const MAX_BACKOFF: u64 = 32_768;
		let start = if to.is_some() {
			Some(Instant::now())
		} else {
			None
		};
		// Loop until we have a successful allocation.
		// We check the timeout inherited from the context
		while !ctx.is_timedout().await? {
			if let (Some(ref start), Some(ref to)) = (start, to) {
				// We check the time associated with the sequence
				if start.elapsed().ge(to) {
					break;
				}
			}
			if let Ok(r) = Self::check_batch_allocation(tf, seq, nid, next, batch).await {
				return Ok(r);
			}
			// exponential backoff with full jitter
			let sleep_ms = rng.gen_range(1..=tempo);
			sleep(Duration::from_millis(sleep_ms)).await;
			if tempo < MAX_BACKOFF {
				tempo *= 2;
			}
		}
		Err(anyhow::Error::new(Error::QueryTimedout))
	}

	async fn check_batch_allocation(
		tf: &TransactionFactory,
		seq: &SequenceDomain,
		nid: Uuid,
		next: i64,
		batch: u32,
	) -> Result<(i64, i64)> {
		let tx = tf.transaction(TransactionType::Write, LockType::Optimistic).await?;
		let batch_range = seq.new_batch_range_keys()?;
		let val = tx.getr(batch_range, None).await?;
		let mut next_start = next;
		// Scan every existing batches
		for (key, val) in val.iter() {
			let ba: BatchValue = revision::from_slice(val)?;
			next_start = next_start.max(ba.to);
			// The batch belongs to this node
			if ba.owner == nid {
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
			owner: nid,
		})?;
		let batch_key = seq.new_batch_key(next_start)?;
		tx.set(&batch_key, &bv, None).await?;
		tx.commit().await?;
		Ok((next_start, next_to))
	}
}
