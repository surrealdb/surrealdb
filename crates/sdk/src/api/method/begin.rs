use crate::api::Connection;
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
pub struct Begin<'req, C: Connection> {
	pub(super) client: Cow<'req, Surreal<C>>,
}

impl<'req, C> IntoFuture for Begin<'req, C>
where
	C: Connection,
{
	type Output = Result<Transaction<'req, C>>;
	type IntoFuture = BoxFuture<'req, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			self.client.query(BeginStatement::default().to_string()).await?;
			Ok(Transaction {
				client: Cow::Borrowed(&self.client),
			})
		})
	}
}

impl<'req, C> Begin<'req, C>
where
	C: Connection,
{
	pub fn query(&self, query: impl opt::IntoQuery) -> Query<C> {
		Query {
			queries: vec![query.into_query()],
			variables: Variables::default(),
			client: Cow::Borrowed(&self.client),
		}
	}
}

/// An ongoing transaction
#[derive(Debug)]
#[must_use = "transactions must be committed or cancelled to complete them"]
pub struct Transaction<'req, C: Connection> {
	client: Cow<'req, Surreal<C>>,
}

impl<'req, C> Transaction<'req, C>
where
	C: Connection,
{
	/// Creates a commit future
	pub fn commit(self) -> Commit<'req, C> {
		Commit {
			client: Cow::Borrowed(&self.client),
		}
	}

	/// Creates a cancel future
	pub fn cancel(self) -> Cancel<'req, C> {
		Cancel {
			client: Cow::Borrowed(&self.client),
		}
	}
}

impl<'req, C> Deref for Transaction<'req, C>
where
	C: Connection,
{
	type Target = Surreal<C>;

	fn deref(&self) -> &Self::Target {
		&self.client
	}
}
