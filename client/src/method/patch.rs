use crate::method::Method;
use crate::param::DbResource;
use crate::param::Param;
use crate::param::PatchOp;
use crate::param::Range;
use crate::Connection;
use crate::Result;
use crate::Router;
use futures::future::BoxFuture;
use serde::de::DeserializeOwned;
use std::future::IntoFuture;
use std::marker::PhantomData;
use surrealdb::sql::Array;
use surrealdb::sql::Id;
use surrealdb::sql::Value;

/// A patch future
#[derive(Debug)]
pub struct Patch<'r, C: Connection, R> {
	pub(super) router: Result<&'r Router<C>>,
	pub(super) resource: Result<DbResource>,
	pub(super) range: Option<Range<Id>>,
	pub(super) patches: Vec<Value>,
	pub(super) response_type: PhantomData<R>,
}

impl<'r, C, R> Patch<'r, C, R>
where
	C: Connection,
{
	/// Applies JSON Patch changes to all records, or a specific record, in the database.
	pub fn patch(mut self, PatchOp(patch): PatchOp) -> Patch<'r, C, R> {
		self.patches.push(patch);
		self
	}
}

impl<'r, Client, R> IntoFuture for Patch<'r, Client, R>
where
	Client: Connection,
	R: DeserializeOwned + Send,
{
	type Output = Result<R>;
	type IntoFuture = BoxFuture<'r, Result<R>>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let resource = self.resource?;
			let param = match self.range {
				Some(range) => resource.with_range(range)?,
				None => resource.into(),
			};
			let patches = Value::Array(Array(self.patches));
			let mut conn = Client::new(Method::Patch);
			conn.execute(self.router?, Param::new(vec![param, patches])).await
		})
	}
}
