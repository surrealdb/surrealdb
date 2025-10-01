use std::borrow::Cow;
use std::future::IntoFuture;

use crate::Surreal;
use crate::api::conn::Command;
use crate::api::method::{BoxFuture, UseDb};
use crate::api::{Connection, Result};
use crate::method::OnceLockExt;

/// Stores the namespace to use
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct UseNs<'r, C: Connection> {
	pub(super) client: Cow<'r, Surreal<C>>,
	pub(super) ns: String,
}

impl<C> UseNs<'_, C>
where
	C: Connection,
{
	/// Converts to an owned type which can easily be moved to a different
	/// thread
	pub fn into_owned(self) -> UseNs<'static, C> {
		UseNs {
			client: Cow::Owned(self.client.into_owned()),
			..self
		}
	}
}

impl<'r, C> UseNs<'r, C>
where
	C: Connection,
{
	/// Switch to a specific database
	pub fn use_db(self, db: impl Into<String>) -> UseDb<'r, C> {
		UseDb {
			ns: self.ns.into(),
			db: db.into(),
			client: self.client,
		}
	}
}

impl<'r, Client> IntoFuture for UseNs<'r, Client>
where
	Client: Connection,
{
	type Output = Result<()>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let router = self.client.inner.router.extract()?;
			router
				.execute_unit(Command::Use {
					namespace: Some(self.ns),
					database: None,
				})
				.await
		})
	}
}
