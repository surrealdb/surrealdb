use std::borrow::Cow;

use uuid::Uuid;

use crate::conn::ctx;
use crate::method::live::spawn;
use crate::method::{Api, Cancel, Commit, Create, Delete, Insert, Query, Select, Update, Upsert};
use crate::opt::{CreateResource, IntoResource};
use crate::{Connection, OnceLockExt, Surreal};

/// Transaction handle produced when [`Begin`](crate::method::Begin) completes after
/// [`Surreal::begin`](crate::Surreal::begin).
///
/// Use [`Transaction::query`](Transaction::query) and related methods on this handle, then
/// [`Transaction::commit`](Transaction::commit) or [`Transaction::cancel`](Transaction::cancel).
///
/// Dropping a transaction without doing either rolls it back, on a task the
/// drop cannot await: nothing it wrote is kept, and the rollback may not have
/// finished when the drop returns. Call [`cancel`](Transaction::cancel) to
/// wait for it.
#[derive(Debug)]
#[must_use = "transactions must be committed or cancelled to complete them"]
pub struct Transaction<C: Connection> {
	pub(crate) id: Uuid,
	pub(crate) client: Surreal<C>,
	pub(crate) snapshot: Option<surrealdb_types::Value>,
	pub(crate) guard: RollbackOnDrop<C>,
}

/// Rolls a transaction back when it is dropped while still open.
///
/// Held by the [`Transaction`] and by the [`Commit`] / [`Cancel`] futures it
/// turns into, and disarmed once one of those begins running, because from
/// then on the engine call owns the transaction's fate.
#[derive(Debug)]
pub(crate) struct RollbackOnDrop<C: Connection> {
	/// A handle on the transaction's session, which keeps it open until the
	/// rollback has run. `None` once disarmed.
	client: Option<Surreal<C>>,
	txn: Uuid,
}

impl<C: Connection> RollbackOnDrop<C> {
	pub(crate) fn new(client: &Surreal<C>, txn: Uuid) -> Self {
		Self {
			client: Some(client.share_session()),
			txn,
		}
	}

	pub(crate) fn disarm(&mut self) {
		self.client = None;
	}
}

impl<C: Connection> Drop for RollbackOnDrop<C> {
	fn drop(&mut self) {
		let Some(client) = self.client.take() else {
			return;
		};
		let txn = self.txn;
		// Dropped outside a runtime, there is nothing to run the rollback on.
		#[cfg(not(target_family = "wasm"))]
		if tokio::runtime::Handle::try_current().is_err() {
			return;
		}
		spawn(async move {
			if let Ok(router) = client.inner.router.extract()
				&& let Err(error) = router.engine.rollback(ctx(client.session_id), txn).await
			{
				tracing::debug!("Failed to roll back dropped transaction '{txn}'; {error}");
			}
		});
	}
}

impl<C> Transaction<C>
where
	C: Connection,
{
	/// For a read-only transaction, the snapshot it reads: an object holding
	/// `position` (an opaque token, or `NONE`), `age` and `stale`. `None` for a
	/// read-write transaction.
	pub fn snapshot(&self) -> Option<&surrealdb_types::Value> {
		self.snapshot.as_ref()
	}

	/// Commits this transaction, persisting the changes, and returns a
	/// [`Commit`] future that yields the original [`Surreal`] client on
	/// success.
	pub fn commit(self) -> Commit<C> {
		Commit::from_transaction(self)
	}

	/// Rolls this transaction back and returns a [`Cancel`] future that yields
	/// the original [`Surreal`] client on success, without persisting the
	/// changes from this transaction.
	pub fn cancel(self) -> Cancel<C> {
		Cancel::from_transaction(self)
	}

	/// Returns the underlying [`Surreal`] session handle for this transaction.
	///
	/// Because [`Surreal`] carries only the session ID and not the transaction
	/// ID, any query or data operation executed on `tx.client()` runs **outside**
	/// this transaction on the same session: it is auto-committed immediately,
	/// cannot observe uncommitted writes made through `tx`, and is not rolled
	/// back by [`Transaction::cancel`]. Use [`Transaction::query`] and the other
	/// methods on [`Transaction`] for transactional operations.
	///
	/// Calling `tx.client().clone()` creates a new independent session on the
	/// same connection, inheriting the session's namespace, database,
	/// authentication, and variables.
	pub fn client(&self) -> &Surreal<C> {
		&self.client
	}

	/// See [Surreal::api]. Every call made from the handle runs inside this
	/// transaction.
	pub fn api(&self) -> Api<'_, C> {
		let mut api = self.client.api();
		api.txn = Some(self.id);
		api
	}

	/// See [Surreal::query]
	pub fn query<'client>(&'client self, query: impl Into<Cow<'client, str>>) -> Query<'client, C> {
		self.client.query(query).with_transaction(self.id)
	}

	/// See [Surreal::select]
	pub fn select<O>(&'_ self, resource: impl IntoResource<O>) -> Select<'_, C, O> {
		self.client.select(resource).with_transaction(self.id)
	}

	/// See [Surreal::create]
	pub fn create<R>(&'_ self, resource: impl CreateResource<R>) -> Create<'_, C, R> {
		self.client.create(resource).with_transaction(self.id)
	}

	/// See [Surreal::insert]
	pub fn insert<O>(&'_ self, resource: impl IntoResource<O>) -> Insert<'_, C, O> {
		self.client.insert(resource).with_transaction(self.id)
	}

	/// See [Surreal::upsert]
	pub fn upsert<O>(&'_ self, resource: impl IntoResource<O>) -> Upsert<'_, C, O> {
		self.client.upsert(resource).with_transaction(self.id)
	}

	/// See [Surreal::update]
	pub fn update<O>(&'_ self, resource: impl IntoResource<O>) -> Update<'_, C, O> {
		self.client.update(resource).with_transaction(self.id)
	}

	/// See [Surreal::delete]
	pub fn delete<O>(&'_ self, resource: impl IntoResource<O>) -> Delete<'_, C, O> {
		self.client.delete(resource).with_transaction(self.id)
	}
}

pub(super) trait WithTransaction {
	fn with_transaction(self, id: Uuid) -> Self;
}
