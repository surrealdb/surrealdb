use std::borrow::Cow;
use std::future::IntoFuture;

use surrealdb_types::Value;

use crate::conn::Command;
use crate::method::{BoxFuture, OnceLockExt};
use crate::opt::WaitFor;
use crate::{Connection, Result, Surreal};

#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct UseDb<'r, C: Connection> {
	pub(super) client: Cow<'r, Surreal<C>>,
	pub(super) ns: Option<String>,
	pub(super) db: String,
}

impl<C> UseDb<'_, C>
where
	C: Connection,
{
	/// Converts to an owned type which can easily be moved to a different
	/// thread
	pub fn into_owned(self) -> UseDb<'static, C> {
		UseDb {
			client: Cow::Owned(self.client.into_owned()),
			..self
		}
	}
}

impl<'r, Client> IntoFuture for UseDb<'r, Client>
where
	Client: Connection,
{
	type Output = Result<(Option<String>, Option<String>)>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let router = self.client.inner.router.extract()?;
			let result = router
				.execute_value(Command::Use {
					namespace: self.ns,
					database: Some(self.db),
				})
				.await?;
			self.client.inner.waiter.0.send(Some(WaitFor::Database)).ok();

			let Value::Object(obj) = result else {
				return Ok((None, None));
			};

			let namespace = obj.get("namespace").and_then(|v| v.as_string()).map(String::from);
			let database = obj.get("database").and_then(|v| v.as_string()).map(String::from);

			Ok((namespace, database))
		})
	}
}
