use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::kvs::ds::TransactionFactory;
use crate::kvs::Transaction;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use std::sync::atomic::AtomicI64;
use std::sync::Arc;

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
		let key = SequenceKey::new(ns, db, sq);
		match self.sequences.entry(key) {
			Entry::Occupied(e) => e.get().next(ctx).await,
			Entry::Vacant(e) => {
				let sq = Sequence::new(self.tf.clone(), ctx).await?;
				e.insert(sq).next(ctx).await
			}
		}
	}
}

struct Sequence {
	_tf: TransactionFactory,
	seq: AtomicI64,
}

impl Sequence {
	async fn new(tf: TransactionFactory, _ctx: &Context) -> Result<Self, Error> {
		// TODO check pre-allocation
		Ok(Self {
			_tf: tf,
			seq: AtomicI64::new(0),
		})
	}
	pub(crate) async fn next(&self, _ctx: &Context) -> Result<i64, Error> {
		// TODO check pre-allocation
		let v = self.seq.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
		Ok(v)
	}
}
