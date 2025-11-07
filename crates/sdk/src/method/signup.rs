use std::borrow::Cow;
use std::future::IntoFuture;

use surrealdb_types::Value;

use crate::conn::Command;
use crate::method::{BoxFuture, OnceLockExt};
use crate::opt::auth::Token;
use crate::{Connection, Result, Surreal};

/// A signup future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Signup<'r, C: Connection> {
	pub(super) client: Cow<'r, Surreal<C>>,
	pub(super) credentials: Value,
}

impl<C> Signup<'_, C>
where
	C: Connection,
{
	/// Converts to an owned type which can easily be moved to a different
	/// thread
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
	type Output = Result<Token>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		let Signup {
			client,
			credentials,
			..
		} = self;
		Box::pin(async move {
			let router = client.inner.router.extract()?;
			router
				.execute(
					Command::Signup {
						credentials: credentials.into_object()?,
					},
					client.session_id,
				)
				.await
		})
	}
}
