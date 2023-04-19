use crate::api::conn::Method;
use crate::api::conn::Param;
use crate::api::conn::Router;
use crate::api::opt::Range;
use crate::api::opt::Resource;
use crate::api::Connection;
use crate::api::Result;
use crate::sql::Id;
use serde::de::DeserializeOwned;
use std::future::Future;
use std::future::IntoFuture;
use std::marker::PhantomData;
use std::pin::Pin;

/// A record delete future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Delete<'r, C: Connection, R> {
	pub(super) router: Result<&'r Router<C>>,
	pub(super) resource: Result<Resource>,
	pub(super) range: Option<Range<Id>>,
	pub(super) response_type: PhantomData<R>,
}

impl<'r, Client, R> Delete<'r, Client, R>
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
		let mut conn = Client::new(Method::Delete);
		conn.execute(self.router?, Param::new(vec![param])).await
	}
}

impl<'r, Client, R> IntoFuture for Delete<'r, Client, Option<R>>
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

impl<'r, Client, R> IntoFuture for Delete<'r, Client, Vec<R>>
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

impl<C, R> Delete<'_, C, Vec<R>>
where
	C: Connection,
{
	/// Restricts a range of records to delete
	pub fn range(mut self, bounds: impl Into<Range<Id>>) -> Self {
		self.range = Some(bounds.into());
		self
	}
}
