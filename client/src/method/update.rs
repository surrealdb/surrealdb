use crate::method::Content;
use crate::method::Merge;
use crate::method::Method;
use crate::method::Patch;
use crate::param::DbResource;
use crate::param::Param;
use crate::param::PatchOp;
use crate::param::Range;
use crate::Connection;
use crate::Result;
use crate::Router;
use futures::future::BoxFuture;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::future::IntoFuture;
use std::marker::PhantomData;
use surrealdb::sql::Id;

/// An update future
#[derive(Debug)]
pub struct Update<'r, C: Connection, R> {
	pub(super) router: Result<&'r Router<C>>,
	pub(super) resource: Result<DbResource>,
	pub(super) range: Option<Range<Id>>,
	pub(super) response_type: PhantomData<R>,
}

impl<'r, Client, R> Update<'r, Client, R>
where
	Client: Connection,
{
	async fn execute<T>(self) -> Result<T>
	where
		T: DeserializeOwned,
	{
		let resource = self.resource?;
		let param = match self.range {
			Some(range) => resource.with_range(range)?,
			None => resource.into(),
		};
		let mut conn = Client::new(Method::Update);
		conn.execute(self.router?, Param::new(vec![param])).await
	}
}

impl<'r, Client, R> IntoFuture for Update<'r, Client, Option<R>>
where
	Client: Connection,
	R: DeserializeOwned + Send + 'r,
{
	type Output = Result<R>;
	type IntoFuture = BoxFuture<'r, Result<R>>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(self.execute())
	}
}

impl<'r, Client, R> IntoFuture for Update<'r, Client, Vec<R>>
where
	Client: Connection,
	R: DeserializeOwned + Send + 'r,
{
	type Output = Result<Vec<R>>;
	type IntoFuture = BoxFuture<'r, Result<Vec<R>>>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(self.execute())
	}
}

impl<C, R> Update<'_, C, Vec<R>>
where
	C: Connection,
{
	/// Restricts the records to update to those in the specified range
	pub fn range(mut self, bounds: impl Into<Range<Id>>) -> Self {
		self.range = Some(bounds.into());
		self
	}
}

macro_rules! update_methods {
	($this:ty, $res:ty) => {
		impl<'r, C, R> Update<'r, C, $this>
		where
			C: Connection,
			R: DeserializeOwned + Send,
		{
			/// Replaces the current document / record data with the specified data
			pub fn content<D>(self, data: D) -> Content<'r, C, D, $res>
			where
				D: Serialize,
			{
				Content {
					router: self.router,
					method: Method::Update,
					resource: self.resource,
					range: self.range,
					content: data,
					response_type: PhantomData,
				}
			}

			/// Merges the current document / record data with the specified data
			pub fn merge<D>(self, data: D) -> Merge<'r, C, D, $res>
			where
				D: Serialize,
			{
				Merge {
					router: self.router,
					resource: self.resource,
					range: self.range,
					content: data,
					response_type: PhantomData,
				}
			}

			/// Patches the current document / record data with the specified JSON Patch data
			pub fn patch(self, PatchOp(patch): PatchOp) -> Patch<'r, C, $res> {
				Patch {
					router: self.router,
					resource: self.resource,
					range: self.range,
					patches: vec![patch],
					response_type: PhantomData,
				}
			}
		}
	};
}

update_methods!(Option<R>, R);
update_methods!(Vec<R>, Vec<R>);
