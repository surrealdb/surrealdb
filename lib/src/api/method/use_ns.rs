use crate::api::conn::Method;
use crate::api::conn::Param;
use crate::api::conn::Router;
use crate::api::Connection;
use crate::api::Result;
use std::future::Future;
use std::future::IntoFuture;
use std::pin::Pin;

/// Stores the namespace to use
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct UseNs<'r, C: Connection> {
	pub(super) router: Result<&'r Router<C>>,
	pub(super) ns: String,
}

/// A use NS and DB future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct UseNsDb<'r, C: Connection> {
	pub(super) router: Result<&'r Router<C>>,
	pub(super) ns: String,
	pub(super) db: String,
}

impl<'r, C> UseNs<'r, C>
where
	C: Connection,
{
	/// Switch to a specific database
	pub fn use_db(self, db: impl Into<String>) -> UseNsDb<'r, C> {
		UseNsDb {
			db: db.into(),
			ns: self.ns,
			router: self.router,
		}
	}
}

impl<'r, Client> IntoFuture for UseNsDb<'r, Client>
where
	Client: Connection,
{
	type Output = Result<()>;
	type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'r>>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let mut conn = Client::new(Method::Use);
			conn.execute_unit(self.router?, Param::new(vec![self.ns.into(), self.db.into()])).await
		})
	}
}
