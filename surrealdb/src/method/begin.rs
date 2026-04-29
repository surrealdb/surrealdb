use std::future::IntoFuture;

use crate::conn::Command;
use crate::method::{BoxFuture, Transaction};
use crate::{Connection, OnceLockExt, Result, Surreal};

/// From [`Surreal::begin`](crate::Surreal::begin). Yields a
/// [`Transaction`](crate::method::Transaction) that can then take
/// [queries](crate::method::Transaction::query), then be
/// [committed](crate::method::Transaction::commit) or
/// [cancelled](crate::method::Transaction::cancel).
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
			let uuid = result.into_uuid().map_err(|e| crate::Error::internal(e.to_string()))?;
			Ok(Transaction {
				id: uuid.into(),
				client,
			})
		})
	}
}
