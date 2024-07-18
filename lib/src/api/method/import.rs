use crate::api::method::BoxFuture;

use crate::api::conn::Method;
use crate::api::conn::MlConfig;
use crate::api::conn::Param;
use crate::api::Connection;
use crate::api::Error;
use crate::api::ExtraFeatures;
use crate::api::Result;
use crate::method::Model;
use crate::method::OnceLockExt;
use crate::Surreal;
use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;
use std::path::PathBuf;

/// An database import future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Import<'r, C: Connection, T = ()> {
	pub(super) client: Cow<'r, Surreal<C>>,
	pub(super) file: PathBuf,
	pub(super) ml_config: Option<MlConfig>,
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
			ml_config: Some(MlConfig::Import),
			import_type: PhantomData,
		}
	}
}

impl<'r, C, T> Import<'r, C, T>
where
	C: Connection,
{
	/// Converts to an owned type which can easily be moved to a different thread
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
			let router = self.client.router.extract()?;
			if !router.features.contains(&ExtraFeatures::Backup) {
				return Err(Error::BackupsNotSupported.into());
			}
			let mut param = Param::file(self.file);
			param.ml_config = self.ml_config;
			router.execute_unit(Method::Import, param).await
		})
	}
}
