use crate::api::conn::Method;
use crate::api::conn::Param;
use crate::api::Connection;
use crate::api::Result;
use crate::method::OnceLockExt;
use crate::opt::WaitFor;
use crate::sql::Value;
use crate::Surreal;
use std::borrow::Cow;
use std::future::Future;
use std::future::IntoFuture;
use std::pin::Pin;

#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct UseDb<'r, C: Connection> {
	pub(super) client: Cow<'r, Surreal<C>>,
	pub(super) ns: Value,
	pub(super) db: String,
}

impl<C> UseDb<'_, C>
where
	C: Connection,
{
	/// Converts to an owned type which can easily be moved to a different thread
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
	type Output = Result<()>;
	type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'r>>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let mut conn = Client::new(Method::Use);
			conn.execute_unit(
				self.client.router.extract()?,
				Param::new(vec![self.ns, self.db.into()]),
			)
			.await?;
			self.client.waiter.0.send(Some(WaitFor::Database)).ok();
			Ok(())
		})
	}
}
