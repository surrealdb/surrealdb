use crate::api::conn::Method;
use crate::api::conn::Param;
use crate::api::conn::Router;
use crate::api::opt::auth::Jwt;
use crate::api::Connection;
use crate::api::Error;
use crate::api::ExtraFeatures;
use crate::api::Result;
use std::future::Future;
use std::future::IntoFuture;
use std::pin::Pin;

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
	type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'r>>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let router = self.router?;
			if !router.features.contains(&ExtraFeatures::Auth) {
				return Err(Error::AuthNotSupported.into());
			}
			let mut conn = Client::new(Method::Authenticate);
			conn.execute(router, Param::new(vec![self.token.into()])).await
		})
	}
}
