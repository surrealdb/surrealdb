use crate::api::conn::Method;
use crate::api::conn::Param;
use crate::api::conn::Router;
use crate::api::method::Content;
use crate::api::method::Merge;
use crate::api::method::Patch;
use crate::api::opt::PatchOp;
use crate::api::opt::Range;
use crate::api::opt::Resource;
use crate::api::Connection;
use crate::api::Result;
use crate::sql::Id;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::future::Future;
use std::future::IntoFuture;
use std::marker::PhantomData;
use std::pin::Pin;

/// An update future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Update<'r, C: Connection, R> {
	pub(super) router: Result<&'r Router<C>>,
	pub(super) resource: Result<Resource>,
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
	R: DeserializeOwned + Send + Sync + 'r,
{
	type Output = Result<R>;
	type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'r>>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(self.execute())
	}
}

impl<'r, Client, R> IntoFuture for Update<'r, Client, Vec<R>>
where
	Client: Connection,
	R: DeserializeOwned + Send + Sync + 'r,
{
	type Output = Result<Vec<R>>;
	type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'r>>;

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
			R: DeserializeOwned + Send + Sync,
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
