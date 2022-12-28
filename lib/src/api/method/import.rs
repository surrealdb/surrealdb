use crate::api::conn::Method;
use crate::api::conn::Param;
use crate::api::conn::Router;
use crate::api::Connection;
use crate::api::Error;
use crate::api::ExtraFeatures;
use crate::api::Result;
use std::future::Future;
use std::future::IntoFuture;
use std::path::PathBuf;
use std::pin::Pin;

/// An database import future
#[derive(Debug)]
pub struct Import<'r, C: Connection> {
	pub(super) router: Result<&'r Router<C>>,
	pub(super) file: PathBuf,
}

impl<'r, Client> IntoFuture for Import<'r, Client>
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
			let mut conn = Client::new(Method::Import);
			conn.execute(router, Param::file(self.file)).await
		})
	}
}
