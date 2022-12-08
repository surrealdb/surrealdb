use crate::api::method::Method;
use crate::api::param::Param;
use crate::api::Connection;
use crate::api::Result;
use crate::api::Router;
use std::future::Future;
use std::future::IntoFuture;
use std::pin::Pin;

/// A health check future
#[derive(Debug)]
pub struct Health<'r, C: Connection> {
	pub(super) router: Result<&'r Router<C>>,
}

impl<'r, Client> IntoFuture for Health<'r, Client>
where
	Client: Connection,
{
	type Output = Result<()>;
	type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'r>>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async {
			let mut conn = Client::new(Method::Health);
			conn.execute(self.router?, Param::new(Vec::new())).await
		})
	}
}
