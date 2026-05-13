use std::borrow::Cow;
use std::future::IntoFuture;

use crate::conn::Command;
use crate::method::{BoxFuture, OnceLockExt};
use crate::{Connection, Result, Surreal};

/// A graceful-shutdown future.
///
/// Awaiting it asks the underlying engine to release its resources.
/// For embedded engines (e.g. SurrealKV, RocksDB, in-memory) this
/// awaits the datastore's `shutdown()` so that file handles and OS
/// locks are released before the future resolves — important on
/// Windows, where per-handle file-lock semantics make immediate
/// re-opens fail until the previous instance has closed. For remote
/// engines this is a no-op acknowledgement; callers can simply drop
/// the connection.
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Shutdown<'r, C: Connection> {
	pub(super) client: Cow<'r, Surreal<C>>,
}

impl<C> Shutdown<'_, C>
where
	C: Connection,
{
	/// Convert to an owned type that can be moved across threads.
	pub fn into_owned(self) -> Shutdown<'static, C> {
		Shutdown {
			client: Cow::Owned(self.client.into_owned()),
		}
	}
}

impl<'r, Client> IntoFuture for Shutdown<'r, Client>
where
	Client: Connection,
{
	type Output = Result<()>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let router = self.client.inner.router.extract()?;
			router.execute_unit(self.client.session_id, Command::Shutdown).await
		})
	}
}
