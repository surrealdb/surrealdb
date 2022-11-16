use crate::method::Method;
use crate::param::Jwt;
use crate::param::Param;
use crate::Connection;
use crate::Result;
use crate::Router;
use futures::future::BoxFuture;
use std::future::IntoFuture;

/// An authentication future
#[derive(Debug)]
pub struct Authenticate<'r, C: Connection> {
	pub(super) router: Result<&'r Router<C>>,
	pub(super) token: Jwt,
}

impl<'r, Client> IntoFuture for Authenticate<'r, Client>
where
	Client: Connection,
{
	type Output = Result<()>;
	type IntoFuture = BoxFuture<'r, Result<()>>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let mut conn = Client::new(Method::Authenticate);
			conn.execute(self.router?, Param::new(vec![self.token.into()])).await
		})
	}
}
