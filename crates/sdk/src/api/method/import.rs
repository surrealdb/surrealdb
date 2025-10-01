use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;
use std::path::PathBuf;

use crate::Surreal;
use crate::api::conn::Command;
use crate::api::method::BoxFuture;
use crate::api::{Connection, Error, ExtraFeatures, Result};
use crate::method::{Model, OnceLockExt};

/// An database import future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Import<'r, C: Connection, T = ()> {
	pub(super) client: Cow<'r, Surreal<C>>,
	pub(super) file: PathBuf,
	pub(super) is_ml: bool,
	pub(super) import_type: PhantomData<T>,
}

impl<'r, C> Import<'r, C>
where
	C: Connection,
{
	/// Import machine learning model
	pub fn ml(self) -> Import<'r, C, Model> {
		Import {
			client: self.client,
			file: self.file,
			is_ml: true,
			import_type: PhantomData,
		}
	}
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
				return Err(Error::BackupsNotSupported.into());
			}

			if self.is_ml {
				return router
					.execute_unit(Command::ImportMl {
						path: self.file,
					})
					.await;
			}

			router
				.execute_unit(Command::ImportFile {
					path: self.file,
				})
				.await
		})
	}
}
