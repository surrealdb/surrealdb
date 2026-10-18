use std::future::IntoFuture;
use std::time::Duration;

use surrealdb_engine_api::ReadOnlyBegin;

use crate::conn::ctx;
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
	pub(super) read_only: Option<ReadOnlyBegin>,
}

impl<C: Connection> Begin<C> {
	/// Opens a read-only transaction that reads the current state. Any write
	/// on it fails.
	pub fn read_only(mut self) -> Self {
		self.read_only.get_or_insert(ReadOnlyBegin {
			max_staleness: Duration::ZERO,
			strict: false,
		});
		self
	}

	/// Opens a read-only transaction over a snapshot no older than `bound`.
	///
	/// A datastore with no stale tier reads the current state, which meets any
	/// bound. See [`Self::strict`] for what happens when no snapshot does.
	pub fn max_staleness(mut self, bound: Duration) -> Self {
		let strict = self.read_only.is_some_and(|read_only| read_only.strict);
		self.read_only = Some(ReadOnlyBegin {
			max_staleness: bound,
			strict,
		});
		self
	}

	/// Fails, instead of reading the current state, when no snapshot meets the
	/// staleness bound. Implies a read-only transaction.
	pub fn strict(mut self) -> Self {
		let max_staleness = self.read_only.map_or(Duration::ZERO, |r| r.max_staleness);
		self.read_only = Some(ReadOnlyBegin {
			max_staleness,
			strict: true,
		});
		self
	}
}

impl<C> Begin<C>
where
	C: Connection,
{
	/// Returns the underlying [`Surreal`] session handle that this [`Begin`]
	/// future will start a transaction on when awaited.
	///
	/// Operations run on `begin.client()` execute on that session outside any
	/// transaction. Calling `begin.client().clone()` creates a new independent
	/// session on the same connection, inheriting the session's namespace,
	/// database, authentication, and variables.
	pub fn client(&self) -> &Surreal<C> {
		&self.client
	}
}

impl<C> IntoFuture for Begin<C>
where
	C: Connection,
{
	type Output = Result<Transaction<C>>;
	type IntoFuture = BoxFuture<'static, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		let client = self.client;
		let read_only = self.read_only;
		Box::pin(async move {
			let router = client.inner.router.extract()?;
			let opened = router.engine.begin(ctx(client.session_id), read_only).await?;
			Ok(Transaction {
				id: opened.id,
				client,
				snapshot: opened.snapshot,
			})
		})
	}
}
