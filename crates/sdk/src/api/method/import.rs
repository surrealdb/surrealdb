use crate::Surreal;
use crate::api::Error;
use crate::api::ExtraFeatures;
use crate::api::Result;
use crate::api::conn::Command;
use crate::api::method::BoxFuture;
use crate::method::Model;

use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;
use std::path::PathBuf;

/// An database import future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Import<T = ()> {
	pub(super) client: Surreal,
	pub(super) file: PathBuf,
	pub(super) is_ml: bool,
	pub(super) import_type: PhantomData<T>,
}

impl Import
{
	/// Import machine learning model
	pub fn ml(self) -> Import<Model> {
		Import {
			client: self.client,
			file: self.file,
			is_ml: true,
			import_type: PhantomData,
		}
	}
}

impl<T> IntoFuture for Import<T>
{
	type Output = Result<()>;
	type IntoFuture = BoxFuture<'static, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			todo!("STU: Implement Import");
			// let router = self.client.inner.router.extract()?;
			// if !router.features.contains(&ExtraFeatures::Backup) {
			// 	return Err(Error::BackupsNotSupported.into());
			// }

			// if self.is_ml {
			// 	return router
			// 		.execute_unit(Command::ImportMl {
			// 			path: self.file,
			// 		})
			// 		.await;
			// }

			// router
			// 	.execute_unit(Command::ImportFile {
			// 		path: self.file,
			// 	})
			// 	.await
		})
	}
}
