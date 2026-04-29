use std::borrow::Cow;

use uuid::Uuid;

use crate::method::{Cancel, Commit, Create, Delete, Insert, Query, Select, Update, Upsert};
use crate::opt::{CreateResource, IntoResource};
use crate::{Connection, Surreal};

/// Transaction handle produced when [`Begin`](crate::method::Begin) completes after
/// [`Surreal::begin`](crate::Surreal::begin).
///
/// Use [`Transaction::query`](Transaction::query) and related methods on this handle, then
/// [`Transaction::commit`](Transaction::commit) or [`Transaction::cancel`](Transaction::cancel).
#[derive(Debug)]
#[must_use = "transactions must be committed or cancelled to complete them"]
pub struct Transaction<C: Connection> {
	pub(crate) id: Uuid,
	pub(crate) client: Surreal<C>,
}

impl<C> Transaction<C>
where
	C: Connection,
{
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
