use crate::api::conn::Method;
use crate::api::conn::Param;
use crate::api::conn::Router;
use crate::api::opt::Range;
use crate::api::opt::Resource;
use crate::api::Connection;
use crate::api::Result;
use crate::sql::Id;
use std::future::Future;
use std::future::IntoFuture;
use std::marker::PhantomData;
use std::pin::Pin;

/// A record delete future
#[derive(Debug)]
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
	async fn execute(self) -> Result<()> {
		let resource = self.resource?;
		let param = match self.range {
			Some(range) => resource.with_range(range)?,
			None => resource.into(),
		};
		let mut conn = Client::new(Method::Delete);
		conn.execute(self.router?, Param::new(vec![param])).await
	}
}

impl<'r, Client> IntoFuture for Delete<'r, Client, Option<()>>
where
	Client: Connection,
{
	type Output = Result<()>;
	type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'r>>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(self.execute())
	}
}

impl<'r, Client> IntoFuture for Delete<'r, Client, Vec<()>>
where
	Client: Connection,
{
	type Output = Result<()>;
	type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'r>>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(self.execute())
	}
}

impl<C> Delete<'_, C, Vec<()>>
where
	C: Connection,
{
	/// Restricts a range of records to delete
	pub fn range(mut self, bounds: impl Into<Range<Id>>) -> Self {
		self.range = Some(bounds.into());
		self
	}
}
