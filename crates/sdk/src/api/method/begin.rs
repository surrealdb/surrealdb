use crate::api::Connection;
use crate::api::Result;
use crate::api::Surreal;
use crate::api::method::BoxFuture;
use crate::api::method::Transaction;
use std::future::IntoFuture;
use surrealdb_core::sql::statements::BeginStatement;
use uuid::Uuid;

/// A beginning of a transaction
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Begin<C: Connection> {
	pub(super) client: Surreal<C>,
}

impl<C> IntoFuture for Begin<C>
where
	C: Connection,
{
	type Output = Result<Transaction<C>>;
	type IntoFuture = BoxFuture<'static, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			self.client.query(BeginStatement::default()).await?;
			Ok(Transaction {
				id: Uuid::new_v4(),
				client: self.client,
			})
		})
	}
}
