use std::borrow::Cow;
use std::future::IntoFuture;

use surrealdb_types::Value;

use crate::conn::Command;
use crate::method::{BoxFuture, OnceLockExt};
use crate::{Connection, Result, Surreal};

/// Stores the namespace to use
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct UseDefaults<'r, C: Connection> {
	pub(super) client: Cow<'r, Surreal<C>>,
}

impl<C> UseDefaults<'_, C>
where
	C: Connection,
{
	/// Converts to an owned type which can easily be moved to a different
	/// thread
	pub fn into_owned(self) -> UseDefaults<'static, C> {
		UseDefaults {
			client: Cow::Owned(self.client.into_owned()),
		}
	}
}

impl<'r, Client> IntoFuture for UseDefaults<'r, Client>
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
						namespace: None,
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
