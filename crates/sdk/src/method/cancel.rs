use std::future::IntoFuture;

use crate::method::BoxFuture;
use crate::{Connection, Result, Surreal};

/// A transaction cancellation future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Cancel<C: Connection> {
	pub(crate) client: Surreal<C>,
}

impl<C> IntoFuture for Cancel<C>
where
	C: Connection,
{
	type Output = Result<Surreal<C>>;
	type IntoFuture = BoxFuture<'static, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			self.client.query("CANCEL").await?;
			Ok(self.client)
		})
	}
}
