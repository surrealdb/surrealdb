use std::future::IntoFuture;

use surrealdb_core::sql::statements::CancelStatement;

use crate::api::method::BoxFuture;
use crate::api::{Connection, Result, Surreal};

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
	type IntoFuture = BoxFuture<'static, Self::Output>;
	type Output = Result<Surreal<C>>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			self.client.query(CancelStatement::default()).await?;
			Ok(self.client)
		})
	}
}
