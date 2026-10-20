use std::future::IntoFuture;

use crate::conn::ctx;
use crate::method::transaction::RollbackOnDrop;
use crate::method::{BoxFuture, Transaction};
use crate::{Connection, OnceLockExt, Result, Surreal};

/// From [`Transaction::cancel`](crate::method::Transaction::cancel); rolls back the open
/// transaction and yields the [`Surreal`](crate::Surreal) client again.
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Cancel<C: Connection> {
	pub(crate) client: Surreal<C>,
	pub(crate) txn: uuid::Uuid,
	guard: RollbackOnDrop<C>,
}

impl<C> Cancel<C>
where
	C: Connection,
{
	pub(crate) fn from_transaction(transaction: Transaction<C>) -> Self {
		Self {
			client: transaction.client,
			txn: transaction.id,
			guard: transaction.guard,
		}
	}
}

impl<C> IntoFuture for Cancel<C>
where
	C: Connection,
{
	type Output = Result<Surreal<C>>;
	type IntoFuture = BoxFuture<'static, Self::Output>;

	fn into_future(mut self) -> Self::IntoFuture {
		Box::pin(async move {
			let router = self.client.inner.router.extract()?;
			// The guard stays armed until the engine has answered, so a caller
			// that cancels this future mid-call still leaves the transaction
			// rolled back. Only a successful answer settles the transaction: an
			// error may be a refusal before dispatch that leaves it open, and a
			// rollback after an error is harmless.
			let outcome = router.engine.rollback(ctx(self.client.session_id), self.txn).await;
			if outcome.is_ok() {
				self.guard.disarm();
			}
			outcome?;
			Ok(self.client)
		})
	}
}
