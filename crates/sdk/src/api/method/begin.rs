use crate::api::Result;
use crate::api::Surreal;
use crate::api::method::BoxFuture;
use crate::api::method::Cancel;
use crate::api::method::Commit;
use crate::method::Query;
use crate::opt;
use std::borrow::Cow;
use std::future::IntoFuture;
use std::ops::Deref;
use surrealdb_core::dbs::Variables;
use surrealdb_core::sql::statements::BeginStatement;

/// A beginning of a transaction
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Begin {
	pub(super) client: Surreal,
}

impl IntoFuture for Begin {
	type Output = Result<Transaction>;
	type IntoFuture = BoxFuture<'static, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			self.client.query(BeginStatement::default().to_string()).await?;
			Ok(Transaction {
				client: self.client,
			})
		})
	}
}

/// An ongoing transaction
#[derive(Debug)]
#[must_use = "transactions must be committed or cancelled to complete them"]
pub struct Transaction {
	client: Surreal,
}

impl Transaction {
	/// Creates a commit future
	pub fn commit(self) -> Commit {
		Commit {
			client: self.client,
		}
	}

	/// Creates a cancel future
	pub fn cancel(self) -> Cancel {
		Cancel {
			client: self.client,
		}
	}
}

impl Deref for Transaction {
	type Target = Surreal;

	fn deref(&self) -> &Self::Target {
		&self.client
	}
}
