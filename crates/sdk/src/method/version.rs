use std::borrow::Cow;
use std::future::IntoFuture;

use crate::conn::Command;
use crate::err::Error;
use crate::method::{BoxFuture, OnceLockExt};
use crate::{Connection, Result, Surreal};

/// A version future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Version<'r, C: Connection> {
	pub(super) client: Cow<'r, Surreal<C>>,
}

impl<C> Version<'_, C>
where
	C: Connection,
{
	/// Converts to an owned type which can easily be moved to a different
	/// thread
	pub fn into_owned(self) -> Version<'static, C> {
		Version {
			client: Cow::Owned(self.client.into_owned()),
		}
	}
}

impl<'r, Client> IntoFuture for Version<'r, Client>
where
	Client: Connection,
{
	type Output = Result<semver::Version>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let router = self.client.inner.router.extract()?;
			let version = router.execute_value(Command::Version).await?;
			let version = version.into_string()?;
			let semantic = version.trim_start_matches("surrealdb-");
			semantic.parse().map_err(|_| Error::InvalidSemanticVersion(format!("\"{version}\"")))
		})
	}
}
