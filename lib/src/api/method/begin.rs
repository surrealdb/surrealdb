use crate::api::method::Cancel;
use crate::api::method::Commit;
use crate::api::Connection;
use crate::api::Result;
use crate::api::Surreal;
use crate::sql::statements::BeginStatement;
use std::future::Future;
use std::future::IntoFuture;
use std::ops::Deref;
use std::pin::Pin;

/// A beginning of a transaction
#[derive(Debug)]
pub struct Begin<C: Connection> {
	pub(super) client: Surreal<C>,
}

impl<C> IntoFuture for Begin<C>
where
	C: Connection,
{
	type Output = Result<Transaction<C>>;
	type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'static>>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			self.client.query(BeginStatement).await?;
			Ok(Transaction {
				client: self.client,
			})
		})
	}
}

/// An ongoing transaction
#[derive(Debug)]
pub struct Transaction<C: Connection> {
	client: Surreal<C>,
}

impl<C> Transaction<C>
where
	C: Connection,
{
	/// Creates a commit future
	pub fn commit(self) -> Commit<C> {
		Commit {
			client: self.client,
		}
	}

	/// Creates a cancel future
	pub fn cancel(self) -> Cancel<C> {
		Cancel {
			client: self.client,
		}
	}
}

impl<C> Deref for Transaction<C>
where
	C: Connection,
{
	type Target = Surreal<C>;

	fn deref(&self) -> &Self::Target {
		&self.client
	}
}
