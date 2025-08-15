use std::borrow::Cow;
use std::future::IntoFuture;

use crate::Surreal;
use crate::api::conn::Command;
use crate::api::method::BoxFuture;
use crate::api::{Connection, Result};
use crate::method::OnceLockExt;

/// An unset future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Unset<'r, C: Connection> {
	pub(super) client: Cow<'r, Surreal<C>>,
	pub(super) key: String,
}

impl<C> Unset<'_, C>
where
	C: Connection,
{
	/// Converts to an owned type which can easily be moved to a different
	/// thread
	pub fn into_owned(self) -> Unset<'static, C> {
		Unset {
			client: Cow::Owned(self.client.into_owned()),
			..self
		}
	}
}

impl<'r, Client> IntoFuture for Unset<'r, Client>
where
	Client: Connection,
{
	type Output = Result<()>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let router = self.client.inner.router.extract()?;
			router
				.execute_unit(Command::Unset {
					key: self.key,
				})
				.await
		})
	}
}
