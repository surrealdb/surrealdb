//! The indexes a transaction cannot read.
//!
//! A `DEFINE INDEX` builds in transactions of its own. A blocking build reads
//! the table as committed and commits the entries it writes before the
//! definition's transaction does, so the transaction that defined the index
//! holds a snapshot that predates them: the index it can read is empty, or
//! lacks every record the build indexed. A `CONCURRENTLY` build reads nothing
//! until that transaction has closed, so there the index is empty for as long
//! as the transaction is open.
//!
//! Nothing durable says so. The definition and the build state are written by
//! different transactions, and the build state the defining transaction would
//! consult is committed past its snapshot, so the one thing that knows is the
//! transaction that ran the statement. It records the index here, and the
//! readers that choose indexes ([`crate::index_state::filter_online_indexes`])
//! and the writers that maintain them ask.
//!
//! A transaction that writes to an index it did not define can be in the same
//! place. Its snapshot may show the index online while the decision how to
//! maintain it, taken on the store as it is now, finds a build running or the
//! index retired: the write is then queued for the build, or skipped, and the
//! entries the snapshot holds lack it. That transaction records the index here
//! as [`Unseen::Queued`] too.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::Transaction;
use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::val::{RecordId, TableName};

/// One index of one table, as the transaction that defined it names it. The
/// id is part of it, so an index replaced under the same name is another entry.
#[derive(Clone, PartialEq, Eq, Hash)]
pub(crate) struct IndexAt {
	pub(crate) ns: NamespaceId,
	pub(crate) db: DatabaseId,
	pub(crate) tb: TableName,
	pub(crate) ix: IndexId,
}

/// Why a transaction cannot read an index it defined.
#[derive(Clone)]
pub enum Unseen {
	/// The build waits for the transaction to close. The index holds nothing
	/// yet, and the transaction's own writes are queued for the build, so
	/// nothing it writes conflicts with an entry. Also an index the
	/// transaction did not define whose entries in its snapshot lack a write
	/// it made, as the write was queued for a build or the index was retired.
	Queued,
	/// The build ran in transactions of its own and indexed records. Its
	/// entries are committed past the transaction's snapshot.
	Built {
		/// The records this transaction created after the definition. Their
		/// entries are its own writes, which it reads and can change; every
		/// other entry of a record that existed when the index was built is
		/// the build's, which it can neither read nor write.
		///
		/// A record stays in the set until the transaction ends, deleted or
		/// not: the set does not roll back with the storage, and a save point
		/// rollback brings back a record the transaction deleted.
		///
		/// A speculation's copy of the registry shares the set until one of them
		/// writes it, so taking the copy costs nothing in the records created.
		created: Arc<HashSet<RecordId>>,
		/// A view of the store opened once the build had returned, which reads
		/// the entries the build wrote. Nothing but the build writes the keys
		/// of an index no other transaction can see yet, so what it reads of
		/// them holds until this transaction closes. Kept for a `UNIQUE` index,
		/// whose writes check their key through it, and `None` for every other
		/// kind.
		view: Option<Arc<Transaction>>,
	},
}

/// What a transaction cannot read of the indexes it defined.
///
/// A speculation of the transaction ([`Transaction::speculate`]) takes a copy,
/// so what it records stays with it and ends with it.
#[derive(Clone, Default)]
pub struct UnseenIndexes {
	entries: HashMap<IndexAt, Unseen>,
}

impl UnseenIndexes {
	pub(crate) fn mark(&mut self, at: IndexAt, unseen: Unseen) {
		self.entries.insert(at, unseen);
	}

	pub(crate) fn forget(&mut self, at: &IndexAt) -> Option<Unseen> {
		self.entries.remove(at)
	}

	pub(crate) fn contains(&self, at: &IndexAt) -> bool {
		self.entries.contains_key(at)
	}

	pub(crate) fn get_mut(&mut self, at: &IndexAt) -> Option<&mut Unseen> {
		self.entries.get_mut(at)
	}

	/// Forget every index, answering the views of the ones a build wrote.
	pub(crate) fn drain_views(&mut self) -> Vec<Arc<Transaction>> {
		std::mem::take(&mut self.entries)
			.into_values()
			.filter_map(|unseen| match unseen {
				Unseen::Built {
					view,
					..
				} => view,
				Unseen::Queued => None,
			})
			.collect()
	}
}

impl Transaction {
	/// Record that this transaction cannot read index `ix` of `tb`, for the
	/// reason `unseen` gives.
	pub fn mark_index_unseen(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		ix: IndexId,
		unseen: Unseen,
	) {
		let registry = self.unseen_indexes.get_or_init(Default::default);
		registry.lock().mark(
			IndexAt {
				ns,
				db,
				tb: tb.clone(),
				ix,
			},
			unseen,
		);
	}

	/// Record that this transaction can read index `ix` of `tb`: it defined it
	/// again, and the build it ran leaves nothing the transaction cannot see.
	///
	/// Answers the view the transaction held of what the earlier build wrote,
	/// which the caller closes.
	pub fn forget_index_unseen(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		ix: IndexId,
	) -> Option<Arc<Transaction>> {
		let registry = self.unseen_indexes.get()?;
		let forgotten = registry.lock().forget(&IndexAt {
			ns,
			db,
			tb: tb.clone(),
			ix,
		});
		match forgotten? {
			Unseen::Built {
				view,
				..
			} => view,
			Unseen::Queued => None,
		}
	}

	/// Whether this transaction has recorded an index it cannot read.
	///
	/// A transaction that defined none answers without taking a lock, which is
	/// every transaction but the one that ran a `DEFINE INDEX`.
	pub fn defined_unseen_indexes(&self) -> bool {
		self.unseen_indexes.get().is_some()
	}

	/// Forget the indexes this transaction cannot read, and close
	/// the views it holds of what their builds wrote.
	///
	/// Run when the transaction closes, by commit or cancel: nothing it reads
	/// or writes afterwards. A speculation holds a copy of the registry whose
	/// views are its transaction's, and leaves them to it.
	pub(crate) async fn release_unseen_indexes(&self) {
		if self.speculating() {
			return;
		}
		let Some(registry) = self.unseen_indexes.get() else {
			return;
		};
		let views = registry.lock().drain_views();
		for view in views {
			if let Err(err) = view.cancel_bare().await {
				tracing::warn!(
					target: "surrealdb::core::kvs::tx",
					"closing the view of an index this transaction defined failed: {err}"
				);
			}
		}
	}

	/// Whether this transaction recorded index `ix` of `tb` as one it cannot read.
	pub fn index_unseen(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		ix: IndexId,
	) -> bool {
		let Some(registry) = self.unseen_indexes.get() else {
			return false;
		};
		registry.lock().contains(&IndexAt {
			ns,
			db,
			tb: tb.clone(),
			ix,
		})
	}

	/// Run `f` over what this transaction cannot read of index `ix` of `tb`,
	/// answering `None` when it can read it.
	///
	/// The registry is held for the call, so `f` is synchronous and short.
	pub fn with_unseen_index<R>(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		ix: IndexId,
		f: impl FnOnce(&mut Unseen) -> R,
	) -> Option<R> {
		let registry = self.unseen_indexes.get()?;
		let mut registry = registry.lock();
		registry
			.get_mut(&IndexAt {
				ns,
				db,
				tb: tb.clone(),
				ix,
			})
			.map(f)
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use surrealdb_cnf::ConfigMap;
	use surrealdb_kvs::TransactionType;

	use super::*;
	use crate::catalog::providers::TableProvider;
	use crate::catalog::{Index, IndexDefinition};
	use crate::factory::TransactionFactory;
	use crate::index_state::filter_online_indexes;
	use crate::sequences::Sequences;
	use crate::speculation::Overlay;

	const NS: NamespaceId = NamespaceId(1);
	const DB: DatabaseId = DatabaseId(1);

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
		let sequences = Sequences::new(tf.clone(), uuid::Uuid::new_v4());
		(tf, sequences)
	}

	fn definition(id: u32, tb: &TableName) -> IndexDefinition {
		IndexDefinition {
			index_id: IndexId(id),
			name: format!("ix{id}").into(),
			table_name: tb.clone(),
			cols: vec![],
			index: Index::Idx,
			count_cond: None,
			comment: None,
			prepare_remove: false,
			format_version: 1,
			nulls_not_distinct: false,
		}
	}

	async fn view(tf: &TransactionFactory, sq: &Sequences) -> Arc<Transaction> {
		Arc::new(tf.transaction(TransactionType::Read, sq.clone()).await.unwrap())
	}

	/// A transaction that defined no index it cannot read reads them all, and
	/// answers so without a registry.
	#[tokio::test]
	async fn a_transaction_that_defined_nothing_reads_every_index() {
		let (tf, sq) = factory().await;
		let tx = tf.transaction(TransactionType::Write, sq).await.unwrap();
		let tb = TableName::new("item");
		assert!(!tx.defined_unseen_indexes());
		assert!(!tx.index_unseen(NS, DB, &tb, IndexId(1)));
		assert!(tx.with_unseen_index(NS, DB, &tb, IndexId(1), |_| ()).is_none());
		tx.cancel().await.unwrap();
	}

	/// An index is unseen by its namespace, database, table and id: another
	/// index of the table, the same id on another table and the same id in
	/// another database are read as before.
	#[tokio::test]
	async fn an_unseen_index_is_named_by_where_it_is_and_its_id() {
		let (tf, sq) = factory().await;
		let tx = tf.transaction(TransactionType::Write, sq).await.unwrap();
		let (item, other) = (TableName::new("item"), TableName::new("other"));
		tx.mark_index_unseen(NS, DB, &item, IndexId(1), Unseen::Queued);
		assert!(tx.defined_unseen_indexes());
		assert!(tx.index_unseen(NS, DB, &item, IndexId(1)));
		assert!(!tx.index_unseen(NS, DB, &item, IndexId(2)));
		assert!(!tx.index_unseen(NS, DB, &other, IndexId(1)));
		assert!(!tx.index_unseen(NS, DatabaseId(2), &item, IndexId(1)));
		assert!(!tx.index_unseen(NamespaceId(2), DB, &item, IndexId(1)));
		tx.forget_index_unseen(NS, DB, &item, IndexId(1));
		assert!(!tx.index_unseen(NS, DB, &item, IndexId(1)));
		tx.cancel().await.unwrap();
	}

	/// The planner's filter leaves out an index its transaction cannot read,
	/// whatever the catalog and the durable state say of it, and nothing else.
	#[tokio::test]
	async fn the_online_filter_leaves_out_an_unseen_index_and_keeps_the_rest() {
		let (tf, sq) = factory().await;
		let tx = tf.transaction(TransactionType::Write, sq).await.unwrap();
		let tb = TableName::new("item");
		let (first, second) = (definition(1, &tb), definition(2, &tb));
		tx.put_tb_index(NS, DB, &tb, &first).await.unwrap();
		tx.put_tb_index(NS, DB, &tb, &second).await.unwrap();
		let both: Arc<[IndexDefinition]> = Arc::from(vec![first.clone(), second.clone()]);

		let ids =
			|kept: &Arc<[IndexDefinition]>| kept.iter().map(|ix| ix.index_id.0).collect::<Vec<_>>();
		assert_eq!(
			ids(&filter_online_indexes(&tx, NS, DB, Arc::clone(&both)).await.unwrap()),
			[1, 2]
		);

		tx.mark_index_unseen(NS, DB, &tb, IndexId(1), Unseen::Queued);
		assert_eq!(ids(&filter_online_indexes(&tx, NS, DB, Arc::clone(&both)).await.unwrap()), [2]);

		tx.forget_index_unseen(NS, DB, &tb, IndexId(1));
		assert_eq!(ids(&filter_online_indexes(&tx, NS, DB, both).await.unwrap()), [1, 2]);
		tx.cancel().await.unwrap();
	}

	/// What a transaction holds of a built index it cannot read changes in
	/// place, so the records it creates are remembered for the statements after.
	#[tokio::test]
	async fn the_records_a_transaction_creates_are_kept_with_the_index() {
		let (tf, sq) = factory().await;
		let tx = tf.transaction(TransactionType::Write, sq.clone()).await.unwrap();
		let tb = TableName::new("item");
		let created = RecordId::new(tb.clone(), 7_i64);
		tx.mark_index_unseen(
			NS,
			DB,
			&tb,
			IndexId(1),
			Unseen::Built {
				created: Arc::default(),
				view: Some(view(&tf, &sq).await),
			},
		);
		let held = |tx: &Transaction| {
			tx.with_unseen_index(NS, DB, &tb, IndexId(1), |unseen| match unseen {
				Unseen::Built {
					created: held,
					..
				} => held.contains(&created),
				Unseen::Queued => false,
			})
		};
		assert_eq!(held(&tx), Some(false));
		tx.with_unseen_index(NS, DB, &tb, IndexId(1), |unseen| {
			if let Unseen::Built {
				created: held,
				..
			} = unseen
			{
				Arc::make_mut(held).insert(created.clone());
			}
		});
		assert_eq!(held(&tx), Some(true));
		tx.cancel().await.unwrap();
	}

	/// A speculation chooses indexes as its transaction does, so it cannot
	/// read what the transaction cannot.
	#[tokio::test]
	async fn a_speculation_cannot_read_what_its_transaction_cannot() {
		let (tf, sq) = factory().await;
		let tx = Arc::new(tf.transaction(TransactionType::Write, sq).await.unwrap());
		let tb = TableName::new("item");
		tx.mark_index_unseen(NS, DB, &tb, IndexId(1), Unseen::Queued);

		let speculation = tx.speculate(Overlay::default()).expect("not a view");
		assert!(speculation.txn().index_unseen(NS, DB, &tb, IndexId(1)));
		assert!(!speculation.txn().index_unseen(NS, DB, &tb, IndexId(2)));
		drop(speculation);
		tx.cancel().await.unwrap();
	}

	/// What a speculation records of the indexes its transaction defined stays
	/// with it: an index it marks or forgets and a record it creates are its
	/// own, and its transaction's registry is as it was when the speculation
	/// ends.
	#[tokio::test]
	async fn a_speculation_records_into_a_copy_of_the_registry() {
		let (tf, sq) = factory().await;
		let tx = Arc::new(tf.transaction(TransactionType::Write, sq).await.unwrap());
		let tb = TableName::new("item");
		let created = RecordId::new(tb.clone(), 7_i64);
		tx.mark_index_unseen(
			NS,
			DB,
			&tb,
			IndexId(1),
			Unseen::Built {
				created: Arc::default(),
				view: None,
			},
		);
		tx.mark_index_unseen(NS, DB, &tb, IndexId(2), Unseen::Queued);
		let holds = |tx: &Transaction| {
			tx.with_unseen_index(NS, DB, &tb, IndexId(1), |unseen| match unseen {
				Unseen::Built {
					created: held,
					..
				} => held.contains(&created),
				Unseen::Queued => false,
			})
		};

		let speculation = tx.speculate(Overlay::default()).expect("not a view");
		let view = speculation.txn();
		view.with_unseen_index(NS, DB, &tb, IndexId(1), |unseen| {
			if let Unseen::Built {
				created: held,
				..
			} = unseen
			{
				Arc::make_mut(held).insert(created.clone());
			}
		});
		view.mark_index_unseen(NS, DB, &tb, IndexId(3), Unseen::Queued);
		assert!(view.forget_index_unseen(NS, DB, &tb, IndexId(2)).is_none());
		assert_eq!(holds(view), Some(true));
		assert!(view.index_unseen(NS, DB, &tb, IndexId(3)));
		assert!(!view.index_unseen(NS, DB, &tb, IndexId(2)));
		drop(speculation);

		assert_eq!(holds(&tx), Some(false));
		assert!(!tx.index_unseen(NS, DB, &tb, IndexId(3)));
		assert!(tx.index_unseen(NS, DB, &tb, IndexId(2)));
		tx.cancel().await.unwrap();
	}

	/// A speculation takes its copy of the registry without copying the records
	/// its transaction created: the set is shared until one of them writes it,
	/// so a transaction that created a great many pays nothing for each
	/// speculation.
	#[tokio::test]
	async fn a_speculation_shares_the_created_records_until_it_writes() {
		let (tf, sq) = factory().await;
		let tx = Arc::new(tf.transaction(TransactionType::Write, sq).await.unwrap());
		let tb = TableName::new("item");
		let created = RecordId::new(tb.clone(), 7_i64);
		let written = RecordId::new(tb.clone(), 8_i64);
		tx.mark_index_unseen(
			NS,
			DB,
			&tb,
			IndexId(1),
			Unseen::Built {
				created: Arc::new(HashSet::from([created.clone()])),
				view: None,
			},
		);
		let set = |tx: &Transaction| {
			tx.with_unseen_index(NS, DB, &tb, IndexId(1), |unseen| match unseen {
				Unseen::Built {
					created: held,
					..
				} => Some(Arc::clone(held)),
				Unseen::Queued => None,
			})
			.flatten()
			.expect("a built index")
		};

		let speculation = tx.speculate(Overlay::default()).expect("not a view");
		assert!(Arc::ptr_eq(&set(&tx), &set(speculation.txn())));
		speculation.txn().with_unseen_index(NS, DB, &tb, IndexId(1), |unseen| {
			if let Unseen::Built {
				created: held,
				..
			} = unseen
			{
				Arc::make_mut(held).insert(written.clone());
			}
		});
		assert!(!Arc::ptr_eq(&set(&tx), &set(speculation.txn())));
		assert!(set(&tx).contains(&created));
		assert!(!set(&tx).contains(&written), "the transaction's set is as it was");
		assert!(set(speculation.txn()).contains(&created));
		assert!(set(speculation.txn()).contains(&written));
		drop(speculation);
		tx.cancel().await.unwrap();
	}

	/// A transaction that closes, by commit or by cancel, forgets the indexes it
	/// defined and closes the view it held of what their builds wrote. A
	/// speculation of it ends without doing either.
	#[tokio::test]
	async fn closing_the_transaction_closes_the_views_it_holds() {
		let (tf, sq) = factory().await;
		let tb = TableName::new("item");
		for commit in [false, true] {
			let tx = Arc::new(tf.transaction(TransactionType::Write, sq.clone()).await.unwrap());
			let built = view(&tf, &sq).await;
			tx.mark_index_unseen(
				NS,
				DB,
				&tb,
				IndexId(1),
				Unseen::Built {
					created: Arc::default(),
					view: Some(Arc::clone(&built)),
				},
			);
			tx.mark_index_unseen(NS, DB, &tb, IndexId(2), Unseen::Queued);

			drop(tx.speculate(Overlay::default()).expect("not a view"));
			assert!(!built.closed(), "a speculation leaves the view to its transaction");
			assert!(tx.index_unseen(NS, DB, &tb, IndexId(1)));

			if commit {
				tx.commit().await.unwrap();
			} else {
				tx.cancel().await.unwrap();
			}
			assert!(built.closed(), "the view closes with the transaction (commit: {commit})");
			assert!(!tx.index_unseen(NS, DB, &tb, IndexId(1)));
			assert!(!tx.index_unseen(NS, DB, &tb, IndexId(2)));
		}
	}

	/// A definition drawn again hands back the view of the one it replaces.
	#[tokio::test]
	async fn forgetting_an_index_hands_back_its_view() {
		let (tf, sq) = factory().await;
		let tx = tf.transaction(TransactionType::Write, sq.clone()).await.unwrap();
		let tb = TableName::new("item");
		let built = view(&tf, &sq).await;
		tx.mark_index_unseen(
			NS,
			DB,
			&tb,
			IndexId(1),
			Unseen::Built {
				created: Arc::default(),
				view: Some(Arc::clone(&built)),
			},
		);
		tx.mark_index_unseen(NS, DB, &tb, IndexId(2), Unseen::Queued);
		assert!(tx.forget_index_unseen(NS, DB, &tb, IndexId(2)).is_none());
		let handed = tx.forget_index_unseen(NS, DB, &tb, IndexId(1)).expect("the view");
		assert!(Arc::ptr_eq(&handed, &built));
		assert!(tx.forget_index_unseen(NS, DB, &tb, IndexId(1)).is_none());
		built.cancel_bare().await.unwrap();
		tx.cancel().await.unwrap();
	}

	/// A speculation of a transaction that marked nothing reads every index,
	/// and does not hold a registry the transaction has not made.
	#[tokio::test]
	async fn a_speculation_of_a_transaction_that_defined_nothing_reads_every_index() {
		let (tf, sq) = factory().await;
		let tx = Arc::new(tf.transaction(TransactionType::Write, sq).await.unwrap());
		let speculation = tx.speculate(Overlay::default()).expect("not a view");
		assert!(!speculation.txn().defined_unseen_indexes());
		drop(speculation);
		tx.cancel().await.unwrap();
	}
}
