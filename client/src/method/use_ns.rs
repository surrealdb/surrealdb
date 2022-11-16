use crate::method::Method;
use crate::param::Param;
use crate::Connection;
use crate::Result;
use crate::Router;
use futures::future::BoxFuture;
use std::future::IntoFuture;

/// Stores the namespace to use
#[derive(Debug)]
pub struct UseNs<'r, C: Connection> {
	pub(super) router: Result<&'r Router<C>>,
	pub(super) ns: String,
}

/// A use NS and DB future
#[derive(Debug)]
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
	type IntoFuture = BoxFuture<'r, Result<()>>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let mut conn = Client::new(Method::Use);
			conn.execute(self.router?, Param::new(vec![self.ns.into(), self.db.into()])).await
		})
	}
}
