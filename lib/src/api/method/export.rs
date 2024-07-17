use crate::api::conn::Method;
use crate::api::conn::MlConfig;
use crate::api::conn::Param;
use crate::api::Connection;
use crate::api::Error;
use crate::api::ExtraFeatures;
use crate::api::Result;
use crate::method::Model;
use crate::method::OnceLockExt;
use crate::opt::ExportDestination;
use crate::Surreal;
use channel::Receiver;
use futures::future::BoxFuture;
use futures::Stream;
use futures::StreamExt;
use semver::Version;
use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

/// A database export future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Export<'r, C: Connection, R, T = ()> {
	pub(super) client: Cow<'r, Surreal<C>>,
	pub(super) target: ExportDestination,
	pub(super) ml_config: Option<MlConfig>,
	pub(super) response: PhantomData<R>,
	pub(super) export_type: PhantomData<T>,
}

impl<'r, C, R> Export<'r, C, R>
where
	C: Connection,
{
	/// Export machine learning model
	pub fn ml(self, name: &str, version: Version) -> Export<'r, C, R, Model> {
		Export {
			client: self.client,
			target: self.target,
			ml_config: Some(MlConfig::Export {
				name: name.to_owned(),
				version: version.to_string(),
			}),
			response: self.response,
			export_type: PhantomData,
		}
	}
}

impl<C, R, T> Export<'_, C, R, T>
where
	C: Connection,
{
	/// Converts to an owned type which can easily be moved to a different thread
	pub fn into_owned(self) -> Export<'static, C, R, T> {
		Export {
			client: Cow::Owned(self.client.into_owned()),
			..self
		}
	}
}

impl<'r, Client, T> IntoFuture for Export<'r, Client, PathBuf, T>
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
			let mut param = match self.target {
				ExportDestination::File(path) => Param::file(path),
				ExportDestination::Memory => unreachable!(),
			};
			param.ml_config = self.ml_config;
			router.execute_unit(Method::Export, param).await
		})
	}
}

impl<'r, Client, T> IntoFuture for Export<'r, Client, (), T>
where
	Client: Connection,
{
	type Output = Result<Backup>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let router = self.client.router.extract()?;
			if !router.features.contains(&ExtraFeatures::Backup) {
				return Err(Error::BackupsNotSupported.into());
			}
			let (tx, rx) = crate::channel::bounded(1);
			let ExportDestination::Memory = self.target else {
				unreachable!();
			};
			let mut param = Param::bytes_sender(tx);
			param.ml_config = self.ml_config;
			router.execute_unit(Method::Export, param).await?;
			Ok(Backup {
				rx,
			})
		})
	}
}

/// A stream of exported data
#[derive(Debug, Clone)]
#[must_use = "streams do nothing unless you poll them"]
pub struct Backup {
	rx: Receiver<Result<Vec<u8>>>,
}

impl Stream for Backup {
	type Item = Result<Vec<u8>>;

	fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		self.as_mut().rx.poll_next_unpin(cx)
	}
}
