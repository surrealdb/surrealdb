use std::borrow::Cow;
use std::future::IntoFuture;

use crate::conn::ctx;
use crate::method::{BoxFuture, OnceLockExt};
use crate::{Connection, Result, Surreal};

/// Returned by [`Surreal::use_defaults`](crate::Surreal::use_defaults), used to apply the server's
/// default namespace/database.
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct UseDefaults<'r, C: Connection> {
	pub(super) client: Cow<'r, Surreal<C>>,
}

impl<C> UseDefaults<'_, C>
where
	C: Connection,
{
	/// Converts to an owned type which can easily be moved to a different
	/// thread
	pub fn into_owned(self) -> UseDefaults<'static, C> {
		UseDefaults {
			client: Cow::Owned(self.client.into_owned()),
		}
	}
}

impl<'r, Client> IntoFuture for UseDefaults<'r, Client>
where
	Client: Connection,
{
	type Output = Result<(Option<String>, Option<String>)>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let router = self.client.inner.router.extract()?;
			let result = router.engine.use_ns_db(ctx(self.client.session_id), None, None).await?;

			Ok(result)
		})
	}
}
