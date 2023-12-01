use crate::api::conn::Method;
use crate::api::conn::Param;
use crate::api::method::OnceLockExt;
use crate::api::opt::auth::Jwt;
use crate::api::Connection;
use crate::api::Result;
use crate::Surreal;
use std::borrow::Cow;
use std::future::Future;
use std::future::IntoFuture;
use std::pin::Pin;

/// An authentication future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Authenticate<'r, C: Connection> {
	pub(super) client: Cow<'r, Surreal<C>>,
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
			let router = self.client.router.extract()?;
			let mut conn = Client::new(Method::Authenticate);
			conn.execute_unit(router, Param::new(vec![self.token.0.into()])).await
		})
	}
}
