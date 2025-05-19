use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::key::sequence::Prefix;
use crate::key::sequence::ba::Ba;
use crate::key::sequence::st::St;
use crate::kvs::ds::TransactionFactory;
use crate::kvs::{KeyEncode, LockType, Transaction, TransactionType};
use crate::sql::statements::define::DefineSequenceStatement;
use anyhow::Result;
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use rand::{Rng, thread_rng};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use uuid::Uuid;

#[derive(Clone)]
pub(crate) struct Sequences {
	tf: TransactionFactory,
	sequences: Arc<DashMap<SequenceKey, Sequence>>,
}

#[derive(Hash, PartialEq, Eq)]
struct SequenceKey {
	ns: String,
	db: String,
	sq: String,
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[non_exhaustive]
struct BatchValue {
	to: i64,
	owner: Uuid,
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[non_exhaustive]
struct SequenceState {
	next: i64,
}

impl SequenceKey {
	fn new(ns: &str, db: &str, sq: &str) -> Self {
		Self {
			ns: ns.to_string(),
			db: db.to_string(),
			sq: sq.to_string(),
		}
	}
}

impl Sequences {
	pub(super) fn new(tf: TransactionFactory) -> Self {
		Self {
			tf,
			sequences: Arc::new(Default::default()),
		}
	}
	pub(crate) async fn namespace_removed(&self, tx: &Transaction, ns: &str) -> Result<()> {
		for db in tx.all_ns().await?.iter() {
			self.database_removed(tx, ns, &db.name).await?;
		}
		Ok(())
	}
	pub(crate) async fn database_removed(
		&self,
		tx: &Transaction,
		ns: &str,
		db: &str,
	) -> Result<()> {
		for sqs in tx.all_db_sequences(ns, db).await?.iter() {
			self.sequence_removed(ns, db, &sqs.name);
		}
		Ok(())
	}

	pub(crate) fn sequence_removed(&self, ns: &str, db: &str, sq: &str) {
		let key = SequenceKey::new(ns, db, sq);
		self.sequences.remove(&key);
	}

	pub(crate) async fn next_val(&self, ctx: &Context, opt: &Options, sq: &str) -> Result<i64> {
		let (ns, db) = opt.ns_db()?;
		let seq = ctx.tx().get_db_sequence(ns, db, sq).await?;
		let key = SequenceKey::new(ns, db, sq);
		match self.sequences.entry(key) {
			Entry::Occupied(mut e) => e.get_mut().next(ctx, opt, sq, seq.batch).await,
			Entry::Vacant(e) => {
				let s = Sequence::load(self.tf.clone(), ctx, opt, sq, &seq).await?;
				e.insert(s).next(ctx, opt, sq, seq.batch).await
			}
		}
	}
}

struct Sequence {
	tf: TransactionFactory,
	st: SequenceState,
	timeout: Option<Duration>,
	to: i64,
	key: Vec<u8>,
}

impl Sequence {
	async fn load(
		tf: TransactionFactory,
		ctx: &Context,
		opt: &Options,
		sq: &str,
		seq: &DefineSequenceStatement,
	) -> Result<Self> {
		let (ns, db) = opt.ns_db()?;
		let nid = opt.id()?;
		let key = St::new(ns, db, sq, nid).encode()?;
		let mut st: SequenceState = if let Some(v) = ctx.tx().get(&key, None).await? {
			revision::from_slice(&v)?
		} else {
			SequenceState {
				next: seq.start,
			}
		};
		let (from, to) =
			Self::check_batch_allocation(&tf, ns, db, sq, nid, st.next, seq.batch).await?;
		st.next = from;
		Ok(Self {
			tf,
			key,
			to,
			st,
			timeout: seq.timeout.as_ref().map(|d| d.0.0),
		})
	}

	pub(crate) async fn next(
		&mut self,
		ctx: &Context,
		opt: &Options,
		seq: &str,
		batch: u32,
	) -> Result<i64> {
		if self.st.next >= self.to {
			(self.st.next, self.to) = Self::find_batch_allocation(
				&self.tf,
				ctx,
				opt,
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
		tx.set(&self.key, revision::to_vec(&self.st)?, None).await?;
		tx.commit().await?;
		Ok(v)
	}

	async fn find_batch_allocation(
		tf: &TransactionFactory,
		ctx: &Context,
		opt: &Options,
		seq: &str,
		next: i64,
		batch: u32,
		to: Option<Duration>,
	) -> Result<(i64, i64)> {
		let (ns, db) = opt.ns_db()?;
		let nid = opt.id()?;
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
			if let Ok(r) = Self::check_batch_allocation(tf, ns, db, seq, nid, next, batch).await {
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
		ns: &str,
		db: &str,
		seq: &str,
		nid: Uuid,
		next: i64,
		batch: u32,
	) -> Result<(i64, i64)> {
		let tx = tf.transaction(TransactionType::Write, LockType::Optimistic).await?;
		let (beg, end) = Prefix::new_ba_range(ns, db, seq)?;
		let val = tx.getr(beg..end, None).await?;
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
		let ba = Ba::new(ns, db, seq, next_start);
		let bv = revision::to_vec(&BatchValue {
			to: next_to,
			owner: nid,
		})?;
		tx.set(ba, bv, None).await?;
		tx.commit().await?;
		Ok((next_start, next_to))
	}
}
