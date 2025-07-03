use crate::api::Surreal;
use crate::api::method::Cancel;
use crate::api::method::Commit;
use crate::api::method::Create;
use crate::api::method::Delete;
use crate::api::method::Insert;
use crate::api::method::Query;
use crate::api::method::Select;
use crate::api::method::Update;
use crate::api::method::Upsert;
use crate::opt::CreatableResource;
use crate::opt::InsertableResource;
use crate::opt::IntoQuery;
use crate::opt::Resource;
use uuid::Uuid;

/// An ongoing transaction
#[derive(Debug)]
#[must_use = "transactions must be committed or cancelled to complete them"]
pub struct Transaction {
	pub(crate) id: Uuid,
	pub(crate) client: Surreal,
}

impl Transaction {
	/// Creates a commit future
	pub fn commit(self) -> Commit {
		Commit {
			client: self.client,
		}
	}

	/// Creates a cancel future
	pub fn cancel(self) -> Cancel {
		Cancel {
			client: self.client,
		}
	}

	/// See [Surreal::query]
	pub fn query(&self, query: impl IntoQuery) -> Query {
		self.client.query(query).with_transaction(self.id)
	}

	/// See [Surreal::select]
	pub fn select<R, RT>(&self, resource: R) -> Select<R, RT>
	where
		R: Resource,
	{
		self.client.select(resource).with_transaction(self.id)
	}

	/// See [Surreal::create]
	pub fn create<R, RT>(&self, resource: R) -> Create<R, RT>
	where
		R: CreatableResource,
	{
		self.client.create(resource).with_transaction(self.id)
	}

	/// See [Surreal::insert]
	pub fn insert<R, RT>(&self, resource: R) -> Insert<R, RT>
	where
		R: InsertableResource,
	{
		self.client.insert(resource).with_transaction(self.id)
	}

	/// See [Surreal::upsert]
	pub fn upsert<R, RT>(&self, resource: R) -> Upsert<R, RT>
	where
		R: Resource,
	{
		self.client.upsert(resource).with_transaction(self.id)
	}

	/// See [Surreal::update]
	pub fn update<R, RT>(&self, resource: R) -> Update<R, RT>
	where
		R: Resource,
	{
		self.client.update(resource).with_transaction(self.id)
	}

	/// See [Surreal::delete]
	pub fn delete<R, RT>(&self, resource: R) -> Delete<R, RT>
	where
		R: Resource,
	{
		self.client.delete(resource).with_transaction(self.id)
	}
}

pub(super) trait WithTransaction {
	fn with_transaction(self, id: Uuid) -> Self;
}
