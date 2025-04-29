use std::future::IntoFuture;

use surrealdb_core::sql::statements::CommitStatement;

use crate::api::method::BoxFuture;
use crate::api::{Connection, Result, Surreal};

/// A transaction commit future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Commit<C: Connection> {
	pub(crate) client: Surreal<C>,
}

impl<C> IntoFuture for Commit<C>
where
	C: Connection,
{
	type IntoFuture = BoxFuture<'static, Self::Output>;
	type Output = Result<Surreal<C>>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			self.client.query(CommitStatement::default()).await?;
			Ok(self.client)
		})
	}
}
