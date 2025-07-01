use super::transaction::WithTransaction;
use crate::Surreal;
use crate::opt::RangeableResource;


use crate::api::Result;
use crate::api::conn::Command;
use crate::api::method::BoxFuture;
use crate::api::opt::Resource;
use crate::method::Live;
use crate::opt::KeyRange;
use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;
use surrealdb_core::expr::TryFromValue;
use surrealdb_core::expr::Value;
use uuid::Uuid;

/// A select future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Select<R: Resource, RT, T = ()> {
	pub(super) client: Surreal,
	pub(super) txn: Option<Uuid>,
	pub(super) resource: R,
	pub(super) response_type: PhantomData<RT>,
	pub(super) query_type: PhantomData<T>,
}

impl<R, RT, T> WithTransaction for Select<R, RT, T>
where 	R: Resource,
{
	fn with_transaction(mut self, id: Uuid) -> Self {
		self.txn = Some(id);
		self
	}
}

impl<R, RT, T> Select<R, RT, T>
where R: Resource,
{
}

macro_rules! into_future {
	($method:ident) => {
		
	};
}

impl<R, RT> IntoFuture for Select<R, RT>
where
	R: Resource,
	RT: TryFromValue,
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
		Box::pin(async move {
			todo!("STUB: Select<R, RT> future");
		})
	}
}

// impl<C, R> Select<'_, C, R, Value>
// where
// 	C
// 	R: RangeableResource,
// {
// 	/// Restricts the records selected to those in the specified range
// 	pub fn range(mut self, range: impl Into<KeyRange>) -> Self {
// 		self.resource = self.resource.with_range(range.into());
// 		self
// 	}
// }

// impl<C, R, RT> Select<'_, C, R, Vec<RT>>
// where
// 	C
// 	R: RangeableResource,
// {
// 	/// Restricts the records selected to those in the specified range
// 	pub fn range(mut self, range: impl Into<KeyRange>) -> Self {
// 		self.resource = self.resource.with_range(range.into());
// 		self
// 	}
// }

impl<R, RT> Select<R, RT>
where R: Resource,
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
	pub fn live(self) -> Select<R, RT, Live> {
		Select {
			txn: self.txn,
			client: self.client,
			resource: self.resource,
			response_type: self.response_type,
			query_type: PhantomData,
		}
	}
}
