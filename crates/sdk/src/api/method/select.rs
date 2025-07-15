use super::transaction::WithTransaction;
use crate::Surreal;
use crate::api::method::live::Subscribe;
use crate::opt::{KeyRange, RangeableResource};

use crate::api::Result;
use crate::api::method::BoxFuture;
use crate::api::opt::Resource;
use std::future::IntoFuture;
use std::marker::PhantomData;
use surrealdb_core::expr::Thing as RecordId;
use surrealdb_core::sql::statements::{LiveStatement, SelectStatement};
use surrealdb_protocol::proto::rpc::v1::QueryRequest;
use surrealdb_protocol::{TryFromQueryStream, TryFromValue};
use uuid::Uuid;

/// A select future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Select<R: Resource, RT> {
	pub(super) client: Surreal,
	pub(super) txn: Option<Uuid>,
	pub(super) resource: R,
	pub(super) response_type: PhantomData<RT>,
	// pub(super) response_item_type: PhantomData<RTItem>,
}

impl<R, RT> WithTransaction for Select<R, RT>
where
	R: Resource,
{
	fn with_transaction(mut self, id: Uuid) -> Self {
		self.txn = Some(id);
		self
	}
}

impl<R, RT> Select<R, RT> where R: Resource {}

impl<R, RT> IntoFuture for Select<R, RT>
where
	R: Resource,
	RT: TryFromQueryStream,
{
	type Output = Result<RT>;
	type IntoFuture = BoxFuture<'static, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		let Select {
			txn,
			client,
			resource,
			..
		} = self;

		let what = resource.into_values();

		Box::pin(async move {
			let mut client = client.client.clone();

			let mut stmt = SelectStatement::default();
			stmt.what = what.into();

			let response = client
				.query(QueryRequest {
					txn_id: txn.map(|id| id.into()),
					query: stmt.to_string(),
					variables: None,
				})
				.await
				.map_err(anyhow::Error::from)?;

			Ok(RT::try_from_query_stream(response.into_inner()).await?)
		})
	}
}

// impl<R> Select<R, Value>
// where
// 	R: RangeableResource,
// {
// 	/// Restricts the records selected to those in the specified range
// 	pub fn range(self, range: impl Into<KeyRange>) -> Select<RecordId, Value> {

// 		Select {
// 			resource: self.resource.with_range(range.into()),
// 			client: self.client,
// 			txn: self.txn,
// 			response_type: PhantomData,
// 		}
// 	}
// }

impl<R, RT> Select<R, RT>
where
	R: RangeableResource,
{
	/// Restricts the records selected to those in the specified range
	pub fn range(self, range: impl Into<KeyRange>) -> Select<RecordId, RT> {
		Select {
			resource: self.resource.with_range(range.into()),
			client: self.client,
			txn: self.txn,
			response_type: PhantomData,
		}
	}
}

impl<R, RT> Select<R, RT>
where
	R: Resource,
	RT: TryFromValue,
{
	/// Turns a normal select query into a live query
	///
	/// # Examples
	///
	/// ```no_run
	/// # use futures::StreamExt;
	/// # use surrealdb::opt::Resource;
	/// # use surrealdb::Result;
	/// # use surrealdb::Notification;
	/// # #[derive(Debug, serde::Deserialize)]
	/// # struct Person;
	/// #
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engine::any::connect("mem://").await?;
	/// #
	/// // Select the namespace/database to use
	/// db.use_ns("namespace").use_db("database").await?;
	///
	/// // Listen to all updates on a table
	/// let mut stream = db.select("person").live().await?;
	/// # let _: Option<Result<Notification<Person>>> = stream.next().await;
	///
	/// // Listen to updates on a range of records
	/// let mut stream = db.select("person").range("jane".."john").live().await?;
	/// # let _: Option<Result<Notification<Person>>> = stream.next().await;
	///
	/// // Listen to updates on a specific record
	/// let mut stream = db.select(("person", "h5wxrf2ewk8xjxosxtyc")).live().await?;
	///
	/// // The returned stream implements `futures::Stream` so we can
	/// // use it with `futures::StreamExt`, for example.
	/// while let Some(result) = stream.next().await {
	///     handle(result);
	/// }
	///
	/// // Handle the result of the live query notification
	/// fn handle(result: Result<Notification<Person>>) {
	///     match result {
	///         Ok(notification) => println!("{notification:?}"),
	///         Err(error) => eprintln!("{error}"),
	///     }
	/// }
	/// #
	/// # Ok(())
	/// # }
	/// ```
	pub fn live(self) -> Subscribe<RT> {
		let what = self.resource.into_values();
		let what = what.0[0].clone();
		let mut live_stmt = LiveStatement::default();
		live_stmt.what = what.into();

		Subscribe {
			txn: self.txn,
			client: self.client,
			live_query: live_stmt.to_string(),
			response_type: self.response_type,
		}
	}
}
