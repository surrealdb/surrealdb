use std::borrow::Cow;
use std::future::IntoFuture;

use crate::Surreal;
use crate::api::conn::Command;
use crate::api::method::{BoxFuture, OnceLockExt};
use crate::api::opt::auth::Jwt;
use crate::api::{Connection, Result};

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
	type IntoFuture = BoxFuture<'r, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let router = self.client.inner.router.extract()?;
			router
				.execute_unit(Command::Authenticate {
					token: self.token.0,
				})
				.await
		})
	}
}
