use std::future::IntoFuture;

use crate::api::method::BoxFuture;
use crate::api::{Connection, Result, Surreal};
use crate::core::expr::TopLevelExpr;

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
			self.client.query(TopLevelExpr::Cancel).await?;
			Ok(self.client)
		})
	}
}
