use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;
use std::path::PathBuf;

use crate::conn::ctx;
use crate::method::{BoxFuture, OnceLockExt};
use crate::{Connection, Error, ExtraFeatures, Result, Surreal};

/// Returned by [`Surreal::import`](crate::Surreal::import) to restore from a `.surql` backup file.
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Import<'r, C: Connection, T = ()> {
	pub(super) client: Cow<'r, Surreal<C>>,
	pub(super) file: PathBuf,
	pub(super) import_type: PhantomData<T>,
}

impl<C, T> Import<'_, C, T>
where
	C: Connection,
{
	/// Converts to an owned type which can easily be moved to a different
	/// thread
	pub fn into_owned(self) -> Import<'static, C, T> {
		Import {
			client: Cow::Owned(self.client.into_owned()),
			..self
		}
	}
}

impl<'r, Client, T> IntoFuture for Import<'r, Client, T>
where
	Client: Connection,
{
	type Output = Result<()>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let router = self.client.inner.router.extract()?;
			if !router.features.contains(&ExtraFeatures::Backup) {
				return Err(Error::internal(
					"The protocol or storage engine does not support backups on this architecture"
						.to_string(),
				));
			}

			router.engine.import_file(ctx(self.client.session_id), self.file).await
		})
	}
}
