use std::borrow::Cow;
use std::future::IntoFuture;

use crate::conn::Command;
use crate::method::{BoxFuture, OnceLockExt};
use crate::opt::auth::Token;
use crate::{Connection, Result, Surreal};

/// An authentication future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Authenticate<'r, C: Connection> {
	pub(super) client: Cow<'r, Surreal<C>>,
	pub(super) token: Token,
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
