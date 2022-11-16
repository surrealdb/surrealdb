use crate::method::Method;
use crate::param::Param;
use crate::Connection;
use crate::Result;
use crate::Router;
use futures::future::BoxFuture;
use std::future::IntoFuture;

/// An unset future
#[derive(Debug)]
pub struct Unset<'r, C: Connection> {
	pub(super) router: Result<&'r Router<C>>,
	pub(super) key: String,
}

impl<'r, Client> IntoFuture for Unset<'r, Client>
where
	Client: Connection,
{
	type Output = Result<()>;
	type IntoFuture = BoxFuture<'r, Result<()>>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let mut conn = Client::new(Method::Unset);
			conn.execute(self.router?, Param::new(vec![self.key.into()])).await
		})
	}
}
