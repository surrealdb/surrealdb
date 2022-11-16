use crate::Connection;
use crate::Result;
use crate::Surreal;
use futures::future::BoxFuture;
use std::future::IntoFuture;
use surrealdb::sql::statements::CommitStatement;

/// A transaction commit future
#[derive(Debug)]
pub struct Commit<C: Connection> {
	pub(crate) client: Surreal<C>,
}

impl<C> IntoFuture for Commit<C>
where
	C: Connection,
{
	type Output = Result<Surreal<C>>;
	type IntoFuture = BoxFuture<'static, Result<Surreal<C>>>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			self.client.query(CommitStatement).await?;
			Ok(self.client)
		})
	}
}
