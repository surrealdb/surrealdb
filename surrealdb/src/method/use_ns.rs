use std::borrow::Cow;
use std::future::IntoFuture;

use surrealdb_types::Value;

use crate::conn::Command;
use crate::method::{BoxFuture, OnceLockExt, UseDb};
use crate::{Connection, Result, Surreal};

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
	type Output = Result<(Option<String>, Option<String>)>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let router = self.client.inner.router.extract()?;
			let result = router
				.execute_value(
					self.client.session_id,
					Command::Use {
						namespace: Some(self.ns),
						database: None,
					},
				)
				.await?;

			let Value::Object(obj) = result else {
				return Ok((None, None));
			};

			let namespace = obj.get("namespace").and_then(|v| v.as_string()).map(String::from);
			let database = obj.get("database").and_then(|v| v.as_string()).map(String::from);

			Ok((namespace, database))
		})
	}
}
