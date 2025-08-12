use uuid::Uuid;

use crate::api::method::{Cancel, Commit, Create, Delete, Insert, Query, Select, Update, Upsert};
use crate::api::{Connection, Surreal};
use crate::opt::{CreateResource, IntoQuery, IntoResource};

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
		Commit {
			client: self.client,
		}
	}

	/// Creates a cancel future
	pub fn cancel(self) -> Cancel<C> {
		Cancel {
			client: self.client,
		}
	}

	/// See [Surreal::query]
	pub fn query(&self, query: impl IntoQuery) -> Query<C> {
		self.client.query(query).with_transaction(self.id)
	}

	/// See [Surreal::select]
	pub fn select<O>(&self, resource: impl IntoResource<O>) -> Select<C, O> {
		self.client.select(resource).with_transaction(self.id)
	}

	/// See [Surreal::create]
	pub fn create<R>(&self, resource: impl CreateResource<R>) -> Create<C, R> {
		self.client.create(resource).with_transaction(self.id)
	}

	/// See [Surreal::insert]
	pub fn insert<O>(&self, resource: impl IntoResource<O>) -> Insert<C, O> {
		self.client.insert(resource).with_transaction(self.id)
	}

	/// See [Surreal::upsert]
	pub fn upsert<O>(&self, resource: impl IntoResource<O>) -> Upsert<C, O> {
		self.client.upsert(resource).with_transaction(self.id)
	}

	/// See [Surreal::update]
	pub fn update<O>(&self, resource: impl IntoResource<O>) -> Update<C, O> {
		self.client.update(resource).with_transaction(self.id)
	}

	/// See [Surreal::delete]
	pub fn delete<O>(&self, resource: impl IntoResource<O>) -> Delete<C, O> {
		self.client.delete(resource).with_transaction(self.id)
	}
}

pub(super) trait WithTransaction {
	fn with_transaction(self, id: Uuid) -> Self;
}
