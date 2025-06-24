use surrealdb_core::iam::SigninParams;

use crate::Surreal;
use crate::api::Connection;
use crate::api::Result;
use crate::api::conn::Command;
use crate::api::method::BoxFuture;
use crate::method::OnceLockExt;
use crate::opt::auth::Jwt;
use std::borrow::Cow;
use std::future::IntoFuture;

/// A signin future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Signin<'r, C: Connection> {
	pub(super) client: Cow<'r, Surreal<C>>,
	pub(super) params: SigninParams,
}

impl<C> Signin<'_, C>
where
	C: Connection,
{
	/// Converts to an owned type which can easily be moved to a different thread
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
	type Output = Result<Jwt>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		let Signin {
			client,
			params,
		} = self;
		Box::pin(async move {
			let router = client.inner.router.extract()?;
			let value = router.execute(Command::Signin(params)).await?;

			Ok(value)
		})
	}
}
