use std::borrow::Cow;
use std::future::IntoFuture;

use crate::Surreal;
use crate::api::conn::Command;
use crate::api::method::BoxFuture;
use crate::api::{Connection, Result};
use crate::method::OnceLockExt;

/// A health check future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Health<'r, C: Connection> {
	pub(super) client: Cow<'r, Surreal<C>>,
}

impl<C> Health<'_, C>
where
	C: Connection,
{
	/// Converts to an owned type which can easily be moved to a different
	/// thread
	pub fn into_owned(self) -> Health<'static, C> {
		Health {
			client: Cow::Owned(self.client.into_owned()),
		}
	}
}

impl<'r, Client> IntoFuture for Health<'r, Client>
where
	Client: Connection,
{
	type Output = Result<()>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let router = self.client.inner.router.extract()?;
			router.execute_unit(Command::Health).await
		})
	}
}
