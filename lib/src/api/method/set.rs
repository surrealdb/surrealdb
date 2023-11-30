use crate::api::conn::Method;
use crate::api::conn::Param;
use crate::api::Connection;
use crate::api::Result;
use crate::method::OnceLockExt;
use crate::sql::Value;
use crate::Surreal;
use std::borrow::Cow;
use std::future::Future;
use std::future::IntoFuture;
use std::pin::Pin;

/// A set future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Set<'r, C: Connection> {
	pub(super) client: Cow<'r, Surreal<C>>,
	pub(super) key: String,
	pub(super) value: Result<Value>,
}

impl<C> Set<'_, C>
where
	C: Connection,
{
	/// Converts to an owned type which can easily be moved to a different thread
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
	type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'r>>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let mut conn = Client::new(Method::Set);
			conn.execute_unit(
				self.client.router.extract()?,
				Param::new(vec![self.key.into(), self.value?]),
			)
			.await
		})
	}
}
