use std::future::IntoFuture;

use crate::conn::Command;
use crate::method::{BoxFuture, Transaction};
use crate::{Connection, OnceLockExt, Result, Surreal};

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
		let client = self.client;
		Box::pin(async move {
			let router = client.inner.router.extract()?;
			let result: crate::types::Value =
				router.execute(client.session_id, Command::Begin).await?;
			// Extract the UUID from the result
			let uuid = result.into_uuid()?;
			Ok(Transaction {
				id: uuid.into(),
				client,
			})
		})
	}
}
