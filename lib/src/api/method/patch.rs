use crate::api::conn::Method;
use crate::api::conn::Param;
use crate::api::conn::Router;
use crate::api::opt::PatchOp;
use crate::api::opt::Range;
use crate::api::opt::Resource;
use crate::api::Connection;
use crate::api::Result;
use crate::sql::Array;
use crate::sql::Id;
use crate::sql::Value;
use serde::de::DeserializeOwned;
use std::future::Future;
use std::future::IntoFuture;
use std::marker::PhantomData;
use std::pin::Pin;

/// A patch future
#[derive(Debug)]
pub struct Patch<'r, C: Connection, R> {
	pub(super) router: Result<&'r Router<C>>,
	pub(super) resource: Result<Resource>,
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
	R: DeserializeOwned + Send + Sync,
{
	type Output = Result<R>;
	type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'r>>;

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
