use crate::api::conn::Method;
use crate::api::conn::Param;
use crate::api::conn::Router;
use crate::api::Connection;
use crate::api::Error;
use crate::api::ExtraFeatures;
use crate::api::Result;
use crate::opt::ExportDestination;
use channel::Receiver;
use futures::Stream;
use futures::StreamExt;
use std::future::Future;
use std::future::IntoFuture;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

/// A database export future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Export<'r, C: Connection, R> {
	pub(super) router: Result<&'r Router<C>>,
	pub(super) target: ExportDestination,
	pub(super) response: PhantomData<R>,
}

impl<'r, Client> IntoFuture for Export<'r, Client, PathBuf>
where
	Client: Connection,
{
	type Output = Result<()>;
	type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'r>>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async {
			let router = self.router?;
			if !router.features.contains(&ExtraFeatures::Backup) {
				return Err(Error::BackupsNotSupported.into());
			}
			let mut conn = Client::new(Method::Export);
			match self.target {
				ExportDestination::File(path) => conn.execute_unit(router, Param::file(path)).await,
				ExportDestination::Memory => unreachable!(),
			}
		})
	}
}

impl<'r, Client> IntoFuture for Export<'r, Client, ()>
where
	Client: Connection,
{
	type Output = Result<Backup>;
	type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'r>>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let router = self.router?;
			if !router.features.contains(&ExtraFeatures::Backup) {
				return Err(Error::BackupsNotSupported.into());
			}
			let (tx, rx) = crate::channel::bounded(1);
			let mut conn = Client::new(Method::Export);
			let ExportDestination::Memory = self.target else {
				unreachable!();
			};
			conn.execute_unit(router, Param::bytes_sender(tx)).await?;
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
