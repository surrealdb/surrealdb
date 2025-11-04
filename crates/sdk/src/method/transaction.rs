use std::borrow::Cow;

use uuid::Uuid;

use crate::method::{Cancel, Commit, Create, Delete, Insert, Query, Select, Update, Upsert};
use crate::opt::{CreateResource, IntoResource};
use crate::{Connection, Surreal};

/// An ongoing transaction
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
	/// Creates a commit future
	pub fn commit(self) -> Commit<C> {
		Commit::from_transaction(self)
	}

	/// Creates a cancel future
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
