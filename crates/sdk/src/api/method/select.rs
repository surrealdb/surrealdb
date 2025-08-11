use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;

use serde::de::DeserializeOwned;
use uuid::Uuid;

use super::transaction::WithTransaction;
use crate::api::conn::Command;
use crate::api::method::{BoxFuture, OnceLockExt};
use crate::api::opt::Resource;
use crate::api::{Connection, Result};
use crate::method::Live;
use crate::opt::KeyRange;
use crate::{Surreal, Value};

/// A select future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Select<'r, C: Connection, R, T = ()> {
	pub(super) txn: Option<Uuid>,
	pub(super) client: Cow<'r, Surreal<C>>,
	pub(super) resource: Result<Resource>,
	pub(super) response_type: PhantomData<R>,
	pub(super) query_type: PhantomData<T>,
}

impl<C, R, T> WithTransaction for Select<'_, C, R, T>
where
	C: Connection,
{
	fn with_transaction(mut self, id: Uuid) -> Self {
		self.txn = Some(id);
		self
	}
}

impl<C, R, T> Select<'_, C, R, T>
where
	C: Connection,
{
	/// Converts to an owned type which can easily be moved to a different
	/// thread
	pub fn into_owned(self) -> Select<'static, C, R, T> {
		Select {
			client: Cow::Owned(self.client.into_owned()),
			..self
		}
	}
}

macro_rules! into_future {
	($method:ident) => {
		fn into_future(self) -> Self::IntoFuture {
			let Select {
				txn,
				client,
				resource,
				..
			} = self;
			Box::pin(async move {
				let router = client.inner.router.extract()?;
				router
					.$method(Command::Select {
						txn,
						what: resource?,
					})
					.await
			})
		}
	};
}

impl<'r, Client> IntoFuture for Select<'r, Client, Value>
where
	Client: Connection,
{
	type Output = Result<Value>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_value}
}

impl<'r, Client, R> IntoFuture for Select<'r, Client, Option<R>>
where
	Client: Connection,
	R: DeserializeOwned,
{
	type Output = Result<Option<R>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_opt}
}

impl<'r, Client, R> IntoFuture for Select<'r, Client, Vec<R>>
where
	Client: Connection,
	R: DeserializeOwned,
{
	type Output = Result<Vec<R>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_vec}
}

impl<C> Select<'_, C, Value>
where
	C: Connection,
{
	/// Restricts the records selected to those in the specified range
	pub fn range(mut self, range: impl Into<KeyRange>) -> Self {
		self.resource = self.resource.and_then(|x| x.with_range(range.into()));
		self
	}
}

impl<C, R> Select<'_, C, Vec<R>>
where
	C: Connection,
{
	/// Restricts the records selected to those in the specified range
	pub fn range(mut self, range: impl Into<KeyRange>) -> Self {
		self.resource = self.resource.and_then(|x| x.with_range(range.into()));
		self
	}
}

impl<'r, C, R> Select<'r, C, R>
where
	C: Connection,
	R: DeserializeOwned,
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
	pub fn live(self) -> Select<'r, C, R, Live> {
		Select {
			txn: self.txn,
			client: self.client,
			resource: self.resource,
			response_type: self.response_type,
			query_type: PhantomData,
		}
	}
}
