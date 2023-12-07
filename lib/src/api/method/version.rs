use crate::api::conn::Method;
use crate::api::conn::Param;
use crate::api::err::Error;
use crate::api::Connection;
use crate::api::Result;
use crate::method::OnceLockExt;
use crate::Surreal;
use std::borrow::Cow;
use std::future::Future;
use std::future::IntoFuture;
use std::pin::Pin;

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
	/// Converts to an owned type which can easily be moved to a different thread
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
	type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'r>>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let mut conn = Client::new(Method::Version);
			let version = conn
				.execute_value(self.client.router.extract()?, Param::new(Vec::new()))
				.await?
				.convert_to_string()?;
			let semantic = version.trim_start_matches("surrealdb-");
			semantic.parse().map_err(|_| Error::InvalidSemanticVersion(semantic.to_string()).into())
		})
	}
}
