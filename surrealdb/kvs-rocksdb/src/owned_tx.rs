//! Small wrappers around the rocksdb crate types to remove the borrow and make the types static
//! instead.

use std::mem::ManuallyDrop;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use rocksdb::{
	DBRawIteratorWithThreadMode, OptimisticTransactionDB, OptimisticTransactionOptions,
	ReadOptions, Transaction, WriteOptions,
};
use tokio::sync::Notify;

#[derive(Clone)]
pub struct Db(Arc<OptimisticTransactionDB>);

impl Db {
	pub fn new(db: OptimisticTransactionDB) -> Self {
		Db(Arc::new(db))
	}

	pub fn begin(&self, w_opt: &WriteOptions, tx_opt: &OptimisticTransactionOptions) -> OwnedTx {
		let tx = self.0.transaction_opt(w_opt, tx_opt);

		// Safety: we increase the lifetime of the transaction here to static
		// This is safe as we store the resulting value together with a Arc reference to the
		// database ensuring it remains alive while the transaction is active.
		let tx = unsafe {
			std::mem::transmute::<
				rocksdb::Transaction<'_, OptimisticTransactionDB>,
				rocksdb::Transaction<'static, OptimisticTransactionDB>,
			>(tx)
		};

		#[allow(clippy::arc_with_non_send_sync)]
		OwnedTx(Arc::new(CommitTx {
			tx,
			notify: Arc::new(Notify::new()),
			db: self.clone(),
		}))
	}
}

/// An owned handle on a transaction that can build a database iterator later,
/// without the transaction's mutex being held at that point.
///
/// Building an [`OwnedRawDbIter`] directly needs the [`OwnedTx`] at the moment
/// the iterator is built, which pins iterator construction to wherever the
/// caller holds the guard. A handle carries the same keep-alive —
/// the [`OwnedTxBorrow`] that [`OwnedTx::into_inner`] waits on — but defers
/// the build, so work dispatched to another thread can construct its own
/// iterator while the transaction is still guaranteed to outlive it.
pub struct OwnedTxHandle {
	db: Db,
	keep_alive: OwnedTxBorrow,
}

impl OwnedTxHandle {
	/// Build the database iterator this handle was taken for, moving the
	/// keep-alive into it. Consumes the handle, so the borrow is transferred
	/// rather than duplicated and the transaction is held exactly as long as
	/// the iterator that reads through its snapshot.
	pub fn raw_iter(self, opt: ReadOptions) -> OwnedRawDbIter {
		let iter = self.db.0.raw_iterator_opt(opt);

		// Safety: we increase the lifetime of the iterator here to static
		// This is safe as we store the resulting value together with a Arc reference to the
		// database ensuring it remains alive while the iterator is active.
		let iter = unsafe {
			std::mem::transmute::<
				DBRawIteratorWithThreadMode<'_, OptimisticTransactionDB>,
				DBRawIteratorWithThreadMode<'static, OptimisticTransactionDB>,
			>(iter)
		};

		OwnedRawDbIter {
			iter,
			// The iterator's lifetime does not depend on any transaction, however
			// we only use db iterators with snapshots from transactions which are only
			// alive while the transaction is alive, so we need a pointer to the transaction
			// to keep it alive. The database is then kept alive via the transaction being alive.
			_keep_alive: self.keep_alive,
		}
	}
}

impl Deref for Db {
	type Target = OptimisticTransactionDB;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

/// An owned transaction which can only be used to commit the transaction
pub struct CommitTx {
	// Order important, tx must drop before db.
	/// The transaction.
	tx: Transaction<'static, OptimisticTransactionDB>,
	/// Owned reference to the database to ensure it reains alive while the transaction is alive.
	db: Db,
	// Needs to be an arc as we need to drop the arc to CommitTx before using the notifier.
	/// notification struct to be able to wait until we have sole ownership of the transaction.
	notify: Arc<Notify>,
}

impl CommitTx {
	pub fn set_commit_timestamp(&self, ts: u64) {
		self.tx.set_commit_timestamp(ts);
	}

	pub fn rollback_to_savepoint(&self) -> Result<(), rocksdb::Error> {
		self.tx.rollback_to_savepoint()
	}

	pub fn commit(self) -> Result<(), rocksdb::Error> {
		self.tx.commit()
	}

	pub fn rollback(self) -> Result<(), rocksdb::Error> {
		self.tx.rollback()
	}
}

struct OwnedTxBorrow(ManuallyDrop<Arc<CommitTx>>);

// Safety: CommitTx is not Sync so Arc<CommitTx> is not Send because if it was we could create
// a reference to CommitTx from a different thread. However, this type does not allow any access to
// CommitTx and is solely to ensure the transaction remains alive.
// The OwnedTx struct actually owns the transaction until it is dropped, if there are any
// OwnedTxBorrow left then the transaction is `moved` i.e. possibly send across a thread to be
// dropped which is safe since the transaction implements send.
unsafe impl Send for OwnedTxBorrow {}
// Safety: There is nothing you can do with a borrow to OwnedTxBorrow so it always safe.
unsafe impl Sync for OwnedTxBorrow {}

impl Drop for OwnedTxBorrow {
	fn drop(&mut self) {
		let notify = Arc::clone(&self.0.notify);
		// Drop before notifying to ensure the reference count is decrement before the commit waiter
		// is woken up.
		// Safety: Safe as this and above are the only places this field is accessed.
		// Afterwards it is no longer accessed.
		unsafe { ManuallyDrop::drop(&mut self.0) };

		// Notify a possibly waiting commit that it might be able to continue.
		notify.notify_one();
	}
}

pub struct OwnedTx(Arc<CommitTx>);

// Safety: CommitTx is not Sync so Arc<CommitTx> is not Send because if it was we could create
// a reference to CommitTx from a different thread. However, OwnedTx makes sure no other threads
// can create borrow to CommitTx so it is safe to send across a thread.
unsafe impl Send for OwnedTx {}

impl OwnedTx {
	/// Returns the associated database
	pub fn db(&self) -> &Db {
		&self.0.db
	}

	/// Take an owned handle that keeps this transaction alive and can build a
	/// database iterator later, off the mutex. See [`OwnedTxHandle`].
	pub fn handle(&self) -> OwnedTxHandle {
		OwnedTxHandle {
			db: self.0.db.clone(),
			keep_alive: OwnedTxBorrow(ManuallyDrop::new(Arc::clone(&self.0))),
		}
	}

	/// Create an iterator which owns the transaction thus ensuring it remains valid.
	pub fn owned_raw_iter(&self, opt: ReadOptions) -> OwnedRawTxIter {
		let iter = self.0.tx.raw_iterator_opt(opt);
		// Safety: we increase the lifetime of the iterator here to static
		// This is safe as we store the resulting value together with a Arc reference to the
		// transaction ensuring it remains alive while the iterator is active.
		let iter = unsafe {
			std::mem::transmute::<
				DBRawIteratorWithThreadMode<
					'_,
					rocksdb::Transaction<'static, OptimisticTransactionDB>,
				>,
				DBRawIteratorWithThreadMode<
					'static,
					rocksdb::Transaction<'static, OptimisticTransactionDB>,
				>,
			>(iter)
		};

		OwnedRawTxIter {
			iter,
			_keep_alive: OwnedTxBorrow(ManuallyDrop::new(Arc::clone(&self.0))),
		}
	}

	/// Wait until we can take sole ownership of the transaction and then return it.
	pub async fn into_inner(mut self) -> CommitTx {
		// We are dropping the transaction so we need to wait until all cursors are done.
		loop {
			match Arc::try_unwrap(self.0) {
				Ok(x) => break x,
				Err(e) => {
					self.0 = e;
				}
			}
			self.0.notify.notified().await
		}
	}

	/// Roll back to the most recent savepoint while cursors may be alive.
	///
	/// Rolling back rebuilds the write-batch index that iterators created by
	/// [`Self::owned_raw_iter`] point into, so the caller must guarantee that
	/// no such iterator is read afterwards and none is read concurrently:
	/// writable scan cursors hold this transaction's `inner` lock for every
	/// batch and replace their iterator when the transaction's rollback epoch
	/// moves. Dropping a stale iterator is sound; it only releases its
	/// reference to the database iterator and holds no ownership of the
	/// write-batch entries.
	pub fn rollback_to_savepoint_keeping_cursors(&self) -> Result<(), rocksdb::Error> {
		self.0.rollback_to_savepoint()
	}

	/// Function shadowing `Transaction::<_,_>::rollback` to make it less likely to accidentally
	/// call this function.
	///
	/// Transaction<'static,OptimisticTransactionDB>::rollback clears internal datastructures,
	/// so if a cursor is alive when rollback is called the cursor internal data will still point
	/// into the datastructures, most likely resulting in dangling pointers. This makes it unsound
	/// as the API freely allows you to have cursors around after a call to rollback.
	///
	/// To call rollback you should first call `Self::into_inner` and then call rollback on that.
	#[expect(private_bounds)]
	#[expect(dead_code)]
	pub fn rollback<T: DoNotUseThisMethod>(&self) {
		panic!("Do not use this function")
	}

	/// Function shadowing `Transaction::<_,_>::rollback_to_savepoint` to make it less likely to
	/// accidentally call this function.
	///
	/// See [`OwnedTx::rollback`] for reason
	///
	/// To roll back to a savepoint call [`Self::rollback_to_savepoint_keeping_cursors`].
	#[expect(private_bounds)]
	#[expect(dead_code)]
	pub fn rollback_to_savepoint<T: DoNotUseThisMethod>(&self) {
		panic!("Do not use this function")
	}
}

trait DoNotUseThisMethod {}

impl Deref for OwnedTx {
	type Target = Transaction<'static, OptimisticTransactionDB>;

	fn deref(&self) -> &Self::Target {
		&self.0.tx
	}
}

pub struct OwnedRawTxIter {
	/// Actual iterator.
	iter: DBRawIteratorWithThreadMode<
		'static,
		rocksdb::Transaction<'static, OptimisticTransactionDB>,
	>,
	/// Arc reference to keep the transaction alive.
	_keep_alive: OwnedTxBorrow,
}

impl DerefMut for OwnedRawTxIter {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.iter
	}
}

impl Deref for OwnedRawTxIter {
	type Target = DBRawIteratorWithThreadMode<
		'static,
		rocksdb::Transaction<'static, OptimisticTransactionDB>,
	>;

	fn deref(&self) -> &Self::Target {
		&self.iter
	}
}

pub struct OwnedRawDbIter {
	/// Actual iterator.
	iter: DBRawIteratorWithThreadMode<'static, OptimisticTransactionDB>,
	/// Arc reference to keep the database alive.
	_keep_alive: OwnedTxBorrow,
}

impl DerefMut for OwnedRawDbIter {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.iter
	}
}

impl Deref for OwnedRawDbIter {
	type Target = DBRawIteratorWithThreadMode<'static, OptimisticTransactionDB>;

	fn deref(&self) -> &Self::Target {
		&self.iter
	}
}
