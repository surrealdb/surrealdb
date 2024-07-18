use crate::api::method::BoxFuture;

use crate::api::conn::Method;
use crate::api::conn::Param;
use crate::api::Connection;
use crate::api::Result;
use crate::method::OnceLockExt;
use crate::Surreal;
use std::borrow::Cow;
use std::future::IntoFuture;

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
	/// Converts to an owned type which can easily be moved to a different thread
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
			let router = self.client.router.extract()?;
			router.execute_unit(Method::Health, Param::new(Vec::new())).await
		})
	}
}
