use crate::api::conn::Method;
use crate::api::conn::Param;
use crate::api::conn::Router;
use crate::api::Connection;
use crate::api::Result;
use std::future::Future;
use std::future::IntoFuture;
use std::pin::Pin;

/// A tick-at-specified-timestamp future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Tick<'r, C: Connection> {
	pub(super) router: Result<&'r Router<C>>,
	pub(super) ts: u64,
}

impl<'r, Client> IntoFuture for Tick<'r, Client>
where
	Client: Connection,
{
	type Output = Result<()>;
	type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'r>>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let mut conn = Client::new(Method::Tick);
			conn.execute_unit(self.router?, Param::new(vec![self.ts.into()])).await
		})
	}
}
