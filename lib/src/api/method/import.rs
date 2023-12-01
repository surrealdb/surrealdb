use crate::api::conn::Method;
use crate::api::conn::Param;
use crate::api::Connection;
use crate::api::Error;
use crate::api::ExtraFeatures;
use crate::api::Result;
use crate::method::OnceLockExt;
use crate::Surreal;
use std::borrow::Cow;
use std::future::Future;
use std::future::IntoFuture;
use std::path::PathBuf;
use std::pin::Pin;

/// An database import future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Import<'r, C: Connection> {
	pub(super) client: Cow<'r, Surreal<C>>,
	pub(super) file: PathBuf,
}

impl<C> Import<'_, C>
where
	C: Connection,
{
	/// Converts to an owned type which can easily be moved to a different thread
	pub fn into_owned(self) -> Import<'static, C> {
		Import {
			client: Cow::Owned(self.client.into_owned()),
			..self
		}
	}
}

impl<'r, Client> IntoFuture for Import<'r, Client>
where
	Client: Connection,
{
	type Output = Result<()>;
	type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'r>>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let router = self.client.router.extract()?;
			if !router.features.contains(&ExtraFeatures::Backup) {
				return Err(Error::BackupsNotSupported.into());
			}
			let mut conn = Client::new(Method::Import);
			conn.execute_unit(router, Param::file(self.file)).await
		})
	}
}
