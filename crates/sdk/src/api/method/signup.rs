use crate::Surreal;
use crate::api::Connection;
use crate::api::Result;
use crate::api::conn::Command;
use crate::api::method::BoxFuture;
use crate::method::OnceLockExt;
use crate::opt::auth::Jwt;
use anyhow::Context;
use surrealdb_core::iam::SignupParams;
use std::borrow::Cow;
use std::future::IntoFuture;

/// A signup future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Signup<'r, C: Connection> {
	pub(super) client: Cow<'r, Surreal<C>>,
	pub(super) params: SignupParams,
}

impl<C> Signup<'_, C>
where
	C: Connection,
{
	/// Converts to an owned type which can easily be moved to a different thread
	pub fn into_owned(self) -> Signup<'static, C> {
		Signup {
			client: Cow::Owned(self.client.into_owned()),
			..self
		}
	}
}

impl<'r, Client> IntoFuture for Signup<'r, Client>
where
	Client: Connection,
{
	type Output = Result<Jwt>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		let Signup {
			client,
			params,
		} = self;

		Box::pin(async move {
			let router = client.inner.router.extract()?;
			let value = router.execute(Command::Signup(params)).await?;

			Ok(value)
		})
	}
}
