use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::key::sequence::ba::Ba;
use crate::key::sequence::Prefix;
use crate::kvs::ds::TransactionFactory;
use crate::kvs::{LockType, Transaction, TransactionType};
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
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
	pub(crate) async fn namespace_removed(&self, tx: &Transaction, ns: &str) -> Result<(), Error> {
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
	) -> Result<(), Error> {
		for sqs in tx.all_db_sequences(ns, db).await?.iter() {
			self.sequence_removed(ns, db, &sqs.name);
		}
		Ok(())
	}

	pub(crate) fn sequence_removed(&self, ns: &str, db: &str, sq: &str) {
		let key = SequenceKey::new(ns, db, sq);
		self.sequences.remove(&key);
	}

	pub(crate) async fn next_val(
		&self,
		ctx: &Context,
		opt: &Options,
		sq: &str,
	) -> Result<i64, Error> {
		let (ns, db) = opt.ns_db()?;
		let seq = ctx.tx().get_db_sequence(ns, db, sq).await?;
		let key = SequenceKey::new(ns, db, sq);
		match self.sequences.entry(key) {
			Entry::Occupied(mut e) => e.get_mut().next(ctx, opt, sq, seq.batch).await,
			Entry::Vacant(e) => {
				let s = Sequence::new(self.tf.clone());
				e.insert(s).next(ctx, opt, sq, seq.batch).await
			}
		}
	}
}

struct Sequence {
	tf: TransactionFactory,
	next: i64,
	to: i64,
}

impl Sequence {
	fn new(tf: TransactionFactory) -> Self {
		Self {
			tf,
			next: 0,
			to: 0,
		}
	}
	pub(crate) async fn next(
		&mut self,
		ctx: &Context,
		opt: &Options,
		seq: &str,
		batch: u32,
	) -> Result<i64, Error> {
		if self.next >= self.to {
			(self.next, self.to) =
				Self::check_batch_allocation(&self.tf, ctx, opt, seq, batch).await?;
		}
		let v = self.next;
		self.next += 1;
		// TODO write next on the kv store
		Ok(v)
	}

	async fn check_batch_allocation(
		tf: &TransactionFactory,
		ctx: &Context,
		opt: &Options,
		seq: &str,
		batch: u32,
	) -> Result<(i64, i64), Error> {
		let (ns, db) = opt.ns_db()?;
		let nid = opt.id()?;
		// Use for exponential backoff
		let mut tempo = 5;
		// Loop until we have a successful allocation
		while !ctx.is_timedout().await? {
			if let Ok(r) = Self::new_batch_allocation(tf, ns, db, seq, nid, batch).await {
				return Ok(r);
			}
			// exponential backoff
			sleep(Duration::from_millis(tempo)).await;
			tempo *= 2;
		}
		Err(Error::QueryTimedout)
	}

	async fn new_batch_allocation(
		tf: &TransactionFactory,
		ns: &str,
		db: &str,
		seq: &str,
		nid: Uuid,
		batch: u32,
	) -> Result<(i64, i64), Error> {
		let tx = tf.transaction(TransactionType::Write, LockType::Optimistic).await?;
		let (beg, end) = Prefix::new_ba_range(ns, db, seq)?;
		let val = tx.getr(beg..end, None).await?;
		let mut next_start = 0;
		// Scan every existing batches
		for (key, val) in val.iter() {
			let ba: BatchValue = revision::from_slice(val)?;
			next_start = next_start.max(ba.to);
			if ba.owner == nid {
				// If a previous batch belongs to this node, we can remove it,
				// as we are going to create a new one
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
