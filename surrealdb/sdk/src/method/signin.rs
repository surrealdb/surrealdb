use std::borrow::Cow;
use std::future::IntoFuture;

use surrealdb_types::Value;

use crate::conn::Command;
use crate::method::{BoxFuture, OnceLockExt};
use crate::opt::auth::Token;
use crate::{Connection, Result, Surreal};

/// A signin future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Signin<'r, C: Connection> {
	pub(super) client: Cow<'r, Surreal<C>>,
	pub(super) credentials: Value,
}

impl<C> Signin<'_, C>
where
	C: Connection,
{
	/// Converts to an owned type which can easily be moved to a different
	/// thread
	pub fn into_owned(self) -> Signin<'static, C> {
		Signin {
			client: Cow::Owned(self.client.into_owned()),
			..self
		}
	}
}

impl<'r, Client> IntoFuture for Signin<'r, Client>
where
	Client: Connection,
{
	type Output = Result<Token>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		let Signin {
			client,
			credentials,
			..
		} = self;
		Box::pin(async move {
			let router = client.inner.router.extract()?;
			router
				.execute(Command::Signin {
					credentials: credentials.into_object()?,
				})
				.await
		})
	}
}
