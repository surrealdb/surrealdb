use std::future::IntoFuture;

use crate::conn::Command;
use crate::method::{BoxFuture, Transaction};
use crate::{Connection, OnceLockExt, Result, Surreal};

/// A transaction commit future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Commit<C: Connection> {
	pub(crate) client: Surreal<C>,
	pub(crate) txn: uuid::Uuid,
}

impl<C> Commit<C>
where
	C: Connection,
{
	pub(crate) fn from_transaction(transaction: Transaction<C>) -> Self {
		Self {
			client: transaction.client,
			txn: transaction.id,
		}
	}
}

impl<C> IntoFuture for Commit<C>
where
	C: Connection,
{
	type Output = Result<Surreal<C>>;
	type IntoFuture = BoxFuture<'static, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let router = self.client.inner.router.extract()?;
			let _: crate::types::Value = router
				.execute(
					self.client.session_id,
					Command::Commit {
						txn: self.txn,
					},
				)
				.await?;
			Ok(self.client)
		})
	}
}
