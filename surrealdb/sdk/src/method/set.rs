use std::borrow::Cow;
use std::future::IntoFuture;

use surrealdb_types::Value;

use crate::conn::Command;
use crate::method::{BoxFuture, OnceLockExt};
use crate::{Connection, Result, Surreal};

/// A set future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Set<'r, C: Connection> {
	pub(super) client: Cow<'r, Surreal<C>>,
	pub(super) key: String,
	pub(super) value: Value,
}

impl<C> Set<'_, C>
where
	C: Connection,
{
	/// Converts to an owned type which can easily be moved to a different
	/// thread
	pub fn into_owned(self) -> Set<'static, C> {
		Set {
			client: Cow::Owned(self.client.into_owned()),
			..self
		}
	}
}

impl<'r, Client> IntoFuture for Set<'r, Client>
where
	Client: Connection,
{
	type Output = Result<()>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let router = self.client.inner.router.extract()?;
			router
				.execute_unit(Command::Set {
					key: self.key,
					value: self.value,
				})
				.await
		})
	}
}
