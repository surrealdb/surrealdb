use std::borrow::Cow;
use std::collections::HashMap;
use std::future::IntoFuture;
use std::pin::Pin;
use std::task::{Context, Poll};

use anyhow::bail;
use futures::StreamExt;
use futures::future::Either;
use futures::stream::SelectAll;
use indexmap::IndexMap;
use serde::Serialize;
use serde::de::DeserializeOwned;
use uuid::Uuid;

use super::transaction::WithTransaction;
use super::{Stream, live};
use crate::api::conn::Command;
use crate::api::err::Error;
use crate::api::method::BoxFuture;
use crate::api::{self, Connection, ExtraFeatures, Result, opt};
use crate::core::expr::{LogicalPlan, TopLevelExpr};
use crate::core::val;
use crate::method::{OnceLockExt, Stats, WithStats};
use crate::value::Notification;
use crate::{Surreal, Value};

/// A query future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Query<'r, C: Connection> {
	pub(crate) txn: Option<Uuid>,
	pub(crate) client: Cow<'r, Surreal<C>>,
	pub(crate) inner: Result<ValidQuery>,
}

impl<C> WithTransaction for Query<'_, C>
where
	C: Connection,
{
	fn with_transaction(mut self, id: Uuid) -> Self {
		self.txn = Some(id);
		self
	}
}

#[derive(Debug)]
pub(crate) enum ValidQuery {
	Raw {
		query: Cow<'static, str>,
		bindings: val::Object,
	},
	Normal {
		query: Vec<TopLevelExpr>,
		register_live_queries: bool,
		bindings: val::Object,
	},
}

impl<'r, C> Query<'r, C>
where
	C: Connection,
{
	pub(crate) fn normal(
		client: Cow<'r, Surreal<C>>,
		query: Vec<TopLevelExpr>,
		bindings: val::Object,
		register_live_queries: bool,
	) -> Self {
		Query {
			txn: None,
			client,
			inner: Ok(ValidQuery::Normal {
				query,
				bindings,
				register_live_queries,
			}),
		}
	}

	pub(crate) fn map_valid<F>(self, f: F) -> Self
	where
		F: FnOnce(ValidQuery) -> Result<ValidQuery>,
	{
		match self.inner {
			Ok(x) => Query {
				txn: self.txn,
				client: self.client,
				inner: f(x),
			},
			x => Query {
				txn: self.txn,
				client: self.client,
				inner: x,
			},
		}
	}

	/// Converts to an owned type which can easily be moved to a different
	/// thread
	pub fn into_owned(self) -> Query<'static, C> {
		Query {
			txn: self.txn,
			client: Cow::Owned(self.client.into_owned()),
			inner: self.inner,
		}
	}
}

impl<'r, Client> IntoFuture for Query<'r, Client>
where
	Client: Connection,
{
	type Output = Result<Response>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			// Extract the router from the client
			let router = self.client.inner.router.extract()?;

			match self.inner? {
				ValidQuery::Raw {
					query,
					bindings,
				} => {
					router
						.execute_query(Command::RawQuery {
							query,
							txn: self.txn,
							variables: bindings,
						})
						.await
				}
				ValidQuery::Normal {
					query,
					register_live_queries,
					bindings,
				} => {
					// Collect the indexes of the live queries which should be registerd.
					let query_indicies = if register_live_queries {
						query
							.iter()
							// BEGIN, COMMIT, and CANCEL don't return a result.
							.filter(|x| {
								!matches!(
									x,
									TopLevelExpr::Begin
										| TopLevelExpr::Commit | TopLevelExpr::Cancel
								)
							})
							.enumerate()
							.filter(|(_, x)| matches!(x, TopLevelExpr::Live(_)))
							.map(|(i, _)| i)
							.collect()
					} else {
						Vec::new()
					};

					// If there are live queries and it is not supported, return an error.
					if !query_indicies.is_empty()
						&& !router.features.contains(&ExtraFeatures::LiveQueries)
					{
						return Err(Error::LiveQueriesNotSupported.into());
					}

					let query = LogicalPlan {
						expressions: query,
					};

					let mut response = router
						.execute_query(Command::Query {
							txn: self.txn,
							query,
							variables: bindings,
						})
						.await?;

					for idx in query_indicies {
						let Some((_, result)) = response.results.get(&idx) else {
							continue;
						};

						// This is a live query. We are using this as a workaround to avoid
						// creating another public error variant for this internal error.
						let res = match result {
							Ok(id) => {
								let val::Value::Uuid(uuid) = id else {
									bail!(Error::InternalError(
										"successfull live query did not return a uuid".to_string(),
									));
								};
								live::register(router, uuid.0).await.map(|rx| {
									Stream::new(self.client.inner.clone().into(), uuid.0, Some(rx))
								})
							}
							Err(_) => Err(anyhow::Error::new(Error::NotLiveQuery(idx))),
						};
						response.live_queries.insert(idx, res);
					}

					Ok(response)
				}
			}
		})
	}
}

impl<'r, Client> IntoFuture for WithStats<Query<'r, Client>>
where
	Client: Connection,
{
	type Output = Result<WithStats<Response>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let response = self.0.await?;
			Ok(WithStats(response))
		})
	}
}

impl<C> Query<'_, C>
where
	C: Connection,
{
	/// Chains a query onto an existing query
	pub fn query(self, surql: impl opt::IntoQuery) -> Self {
		let client = self.client.clone();
		self.map_valid(move |valid| match valid {
			ValidQuery::Raw {
				..
			} => {
				Err(Error::InvalidParams("Appending to raw queries is not supported".to_owned())
					.into())
			}
			ValidQuery::Normal {
				mut query,
				register_live_queries,
				bindings,
			} => match client.query(surql).inner {
				Ok(ValidQuery::Normal {
					query: stmts,
					..
				}) => {
					query.extend(stmts);
					Ok(ValidQuery::Normal {
						query,
						register_live_queries,
						bindings,
					})
				}
				Ok(ValidQuery::Raw {
					..
				}) => Err(Error::InvalidParams("Appending raw queries is not supported".to_owned())
					.into()),
				Err(error) => Err(error),
			},
		})
	}

	/// Return query statistics along with its results
	pub const fn with_stats(self) -> WithStats<Self> {
		WithStats(self)
	}

	/// Binds a parameter or parameters to a query
	///
	/// # Examples
	///
	/// Binding a key/value tuple
	///
	/// ```no_run
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engine::any::connect("mem://").await?;
	/// let response = db.query("CREATE user SET name = $name")
	///     .bind(("name", "John Doe"))
	///     .await?;
	/// # Ok(())
	/// # }
	/// ```
	///
	/// Binding an object
	///
	/// ```no_run
	/// use serde::Serialize;
	///
	/// #[derive(Serialize)]
	/// struct User<'a> {
	///     name: &'a str,
	/// }
	///
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engine::any::connect("mem://").await?;
	/// let response = db.query("CREATE user SET name = $name")
	///     .bind(User {
	///         name: "John Doe",
	///     })
	///     .await?;
	/// # Ok(())
	/// # }
	/// ```
	pub fn bind(self, bindings: impl Serialize + 'static) -> Self {
		self.map_valid(move |mut valid| {
			let current_bindings = match &mut valid {
				ValidQuery::Raw {
					bindings,
					..
				} => bindings,
				ValidQuery::Normal {
					bindings,
					..
				} => bindings,
			};
			let bindings = api::value::to_core_value(bindings)?;
			match bindings {
				val::Value::Object(mut map) => current_bindings.append(&mut map.0),
				val::Value::Array(array) => {
					if array.len() != 2 || !matches!(array[0], val::Value::Strand(_)) {
						let bindings = val::Value::Array(array);
						let bindings = Value::from_inner(bindings);
						return Err(Error::InvalidBindings(bindings).into());
					}

					let mut iter = array.into_iter();
					let Some(val::Value::Strand(key)) = iter.next() else {
						unreachable!()
					};
					let Some(value) = iter.next() else {
						unreachable!()
					};

					current_bindings.insert(key.into_string(), value);
				}
				_ => {
					let bindings = Value::from_inner(bindings);
					return Err(Error::InvalidBindings(bindings).into());
				}
			}

			Ok(valid)
		})
	}
}

pub(crate) type QueryResult = Result<val::Value>;

/// The response type of a `Surreal::query` request
#[derive(Debug)]
pub struct Response {
	pub(crate) results: IndexMap<usize, (Stats, QueryResult)>,
	pub(crate) live_queries: IndexMap<usize, Result<Stream<Value>>>,
}

/// A `LIVE SELECT` stream from the `query` method
#[derive(Debug)]
#[must_use = "streams do nothing unless you poll them"]
pub struct QueryStream<R>(pub(crate) Either<Stream<R>, SelectAll<Stream<R>>>);

impl futures::Stream for QueryStream<Value> {
	type Item = Notification<Value>;

	fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		self.as_mut().0.poll_next_unpin(cx)
	}
}

impl<R> futures::Stream for QueryStream<Notification<R>>
where
	R: DeserializeOwned + Unpin,
{
	type Item = Result<Notification<R>>;

	fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		self.as_mut().0.poll_next_unpin(cx)
	}
}

impl Response {
	pub(crate) fn new() -> Self {
		Self {
			results: Default::default(),
			live_queries: Default::default(),
		}
	}

	/// Takes and returns records returned from the database
	///
	/// A query that only returns one result can be deserialized into an
	/// `Option<T>`, while those that return multiple results should be
	/// deserialized into a `Vec<T>`.
	///
	/// # Examples
	///
	/// ```no_run
	/// use serde::Deserialize;
	/// use surrealdb::RecordId;
	///
	/// #[derive(Debug, Deserialize)]
	/// struct User {
	///     id: RecordId,
	///     balance: String
	/// }
	///
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engine::any::connect("mem://").await?;
	/// #
	/// let mut response = db
	///     // Get `john`'s details
	///     .query("SELECT * FROM user:john")
	///     // List all users whose first name is John
	///     .query("SELECT * FROM user WHERE name.first = 'John'")
	///     // Get John's address
	///     .query("SELECT address FROM user:john")
	///     // Get all users' addresses
	///     .query("SELECT address FROM user")
	///     .await?;
	///
	/// // Get the first (and only) user from the first query
	/// let user: Option<User> = response.take(0)?;
	///
	/// // Get all users from the second query
	/// let users: Vec<User> = response.take(1)?;
	///
	/// // Retrieve John's address without making a special struct for it
	/// let address: Option<String> = response.take((2, "address"))?;
	///
	/// // Get all users' addresses
	/// let addresses: Vec<String> = response.take((3, "address"))?;
	///
	/// // You can continue taking more fields on the same response
	/// // object when extracting individual fields
	/// let mut response = db.query("SELECT * FROM user").await?;
	///
	/// // Since the query we want to access is at index 0, we can use
	/// // a shortcut instead of `response.take((0, "field"))`
	/// let ids: Vec<String> = response.take("id")?;
	/// let names: Vec<String> = response.take("name")?;
	/// let addresses: Vec<String> = response.take("address")?;
	/// #
	/// # Ok(())
	/// # }
	/// ```
	///
	/// The indices are stable. Taking one index doesn't affect the numbering
	/// of the other indices, so you can take them in any order you see fit.
	pub fn take<R>(&mut self, index: impl opt::QueryResult<R>) -> Result<R>
	where
		R: DeserializeOwned,
	{
		index.query_result(self)
	}

	/// Takes and streams records returned from a `LIVE SELECT` query
	///
	/// This is the counterpart to [Response::take] used to stream the results
	/// of a live query.
	///
	/// # Examples
	///
	/// ```no_run
	/// use serde::Deserialize;
	/// use surrealdb::Notification;
	/// use surrealdb::RecordId;
	/// use surrealdb::Value;
	///
	/// #[derive(Debug, Deserialize)]
	/// struct User {
	///     id: RecordId,
	///     balance: String
	/// }
	///
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engine::any::connect("mem://").await?;
	/// #
	/// let mut response = db
	///     // Stream all changes to the user table
	///     .query("LIVE SELECT * FROM user")
	///     .await?;
	///
	/// // Stream the result of the live query at the given index
	/// // while deserialising into the User type
	/// let mut stream = response.stream::<Notification<User>>(0)?;
	///
	/// // Stream raw values instead
	/// let mut stream = response.stream::<Value>(0)?;
	///
	/// // Combine and stream all `LIVE SELECT` statements in this query
	/// let mut stream = response.stream::<Value>(())?;
	/// #
	/// # Ok(())
	/// # }
	/// ```
	///
	/// Consume the stream the same way you would any other type that implements
	/// `futures::Stream`.
	pub fn stream<R>(&mut self, index: impl opt::QueryStream<R>) -> Result<QueryStream<R>> {
		index.query_stream(self)
	}

	/// Take all errors from the query response
	///
	/// The errors are keyed by the corresponding index of the statement that
	/// failed. Afterwards the response is left with only statements that did
	/// not produce any errors.
	///
	/// # Examples
	///
	/// ```no_run
	///
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engine::any::connect("mem://").await?;
	/// # let mut response = db.query("SELECT * FROM user").await?;
	/// let errors = response.take_errors();
	/// # Ok(())
	/// # }
	/// ```
	pub fn take_errors(&mut self) -> HashMap<usize, anyhow::Error> {
		let mut keys = Vec::new();
		for (key, result) in &self.results {
			if result.1.is_err() {
				keys.push(*key);
			}
		}
		let mut errors = HashMap::with_capacity(keys.len());
		for key in keys {
			if let Some((_, Err(error))) = self.results.swap_remove(&key) {
				errors.insert(key, error);
			}
		}
		errors
	}

	/// Check query response for errors and return the first error, if any, or
	/// the response
	///
	/// # Examples
	///
	/// ```no_run
	///
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engine::any::connect("mem://").await?;
	/// # let response = db.query("SELECT * FROM user").await?;
	/// response.check()?;
	/// # Ok(())
	/// # }
	/// ```
	pub fn check(mut self) -> Result<Self> {
		let mut first_error = None;
		for (key, result) in &self.results {
			if result.1.is_err() {
				first_error = Some(*key);
				break;
			}
		}
		if let Some(key) = first_error {
			if let Some((_, Err(error))) = self.results.swap_remove(&key) {
				return Err(error);
			}
		}
		Ok(self)
	}

	/// Returns the number of statements in the query
	///
	/// # Examples
	///
	/// ```no_run
	///
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engine::any::connect("mem://").await?;
	/// let response = db.query("SELECT * FROM user:john; SELECT * FROM user;").await?;
	///
	/// assert_eq!(response.num_statements(), 2);
	/// #
	/// # Ok(())
	/// # }
	pub fn num_statements(&self) -> usize {
		self.results.len()
	}
}

impl WithStats<Response> {
	/// Takes and returns records returned from the database
	///
	/// Similar to [Response::take] but this method returns `None` when
	/// you try taking an index that doesn't correspond to a query
	/// statement.
	///
	/// # Examples
	///
	/// ```no_run
	/// use serde::Deserialize;
	/// use surrealdb::RecordId;
	///
	/// #[derive(Debug, Deserialize)]
	/// struct User {
	///     id: RecordId,
	///     balance: String
	/// }
	///
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engine::any::connect("mem://").await?;
	/// #
	/// let mut response = db
	///     // Get `john`'s details
	///     .query("SELECT * FROM user:john")
	///     // List all users whose first name is John
	///     .query("SELECT * FROM user WHERE name.first = 'John'")
	///     // Get John's address
	///     .query("SELECT address FROM user:john")
	///     // Get all users' addresses
	///     .query("SELECT address FROM user")
	///     // Return stats along with query results
	///     .with_stats()
	///     .await?;
	///
	/// // Get the first (and only) user from the first query
	/// if let Some((stats, result)) = response.take(0) {
	///     let execution_time = stats.execution_time;
	///     let user: Option<User> = result?;
	/// }
	///
	/// // Get all users from the second query
	/// if let Some((stats, result)) = response.take(1) {
	///     let execution_time = stats.execution_time;
	///     let users: Vec<User> = result?;
	/// }
	///
	/// // Retrieve John's address without making a special struct for it
	/// if let Some((stats, result)) = response.take((2, "address")) {
	///     let execution_time = stats.execution_time;
	///     let address: Option<String> = result?;
	/// }
	///
	/// // Get all users' addresses
	/// if let Some((stats, result)) = response.take((3, "address")) {
	///     let execution_time = stats.execution_time;
	///     let addresses: Vec<String> = result?;
	/// }
	/// #
	/// # Ok(())
	/// # }
	/// ```
	pub fn take<R>(&mut self, index: impl opt::QueryResult<R>) -> Option<(Stats, Result<R>)>
	where
		R: DeserializeOwned,
	{
		let stats = index.stats(&self.0)?;
		let result = index.query_result(&mut self.0);
		Some((stats, result))
	}

	/// Take all errors from the query response
	///
	/// The errors are keyed by the corresponding index of the statement that
	/// failed. Afterwards the response is left with only statements that did
	/// not produce any errors.
	///
	/// # Examples
	///
	/// ```no_run
	///
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engine::any::connect("mem://").await?;
	/// # let mut response = db.query("SELECT * FROM user").await?;
	/// let errors = response.take_errors();
	/// # Ok(())
	/// # }
	/// ```
	pub fn take_errors(&mut self) -> HashMap<usize, (Stats, anyhow::Error)> {
		let mut keys = Vec::new();
		for (key, result) in &self.0.results {
			if result.1.is_err() {
				keys.push(*key);
			}
		}
		let mut errors = HashMap::with_capacity(keys.len());
		for key in keys {
			if let Some((stats, Err(error))) = self.0.results.swap_remove(&key) {
				errors.insert(key, (stats, error));
			}
		}
		errors
	}

	/// Check query response for errors and return the first error, if any, or
	/// the response
	///
	/// # Examples
	///
	/// ```no_run
	///
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engine::any::connect("mem://").await?;
	/// # let response = db.query("SELECT * FROM user").await?;
	/// response.check()?;
	/// # Ok(())
	/// # }
	/// ```
	pub fn check(self) -> Result<Self> {
		let response = self.0.check()?;
		Ok(Self(response))
	}

	/// Returns the number of statements in the query
	///
	/// # Examples
	///
	/// ```no_run
	///
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engine::any::connect("mem://").await?;
	/// let response = db.query("SELECT * FROM user:john; SELECT * FROM user;").await?;
	///
	/// assert_eq!(response.num_statements(), 2);
	/// #
	/// # Ok(())
	/// # }
	pub fn num_statements(&self) -> usize {
		self.0.num_statements()
	}

	/// Returns the unwrapped response
	pub fn into_inner(self) -> Response {
		self.0
	}
}

#[cfg(test)]
mod tests {
	use serde::Deserialize;

	use super::*;
	use crate::value::to_value;

	#[derive(Debug, Clone, Serialize, Deserialize)]
	struct Summary {
		title: String,
	}

	#[derive(Debug, Clone, Serialize, Deserialize)]
	struct Article {
		title: String,
		body: String,
	}

	fn to_map(vec: Vec<QueryResult>) -> IndexMap<usize, (Stats, QueryResult)> {
		vec.into_iter()
			.map(|result| {
				let stats = Stats {
					execution_time: Default::default(),
				};
				(stats, result)
			})
			.enumerate()
			.collect()
	}

	#[test]
	fn take_from_an_empty_response() {
		let mut response = Response::new();
		let value: Value = response.take(0).unwrap();
		assert!(value.into_inner().is_none());

		let mut response = Response::new();
		let option: Option<String> = response.take(0).unwrap();
		assert!(option.is_none());

		let mut response = Response::new();
		let vec: Vec<String> = response.take(0).unwrap();
		assert!(vec.is_empty());
	}

	#[test]
	fn take_from_an_errored_query() {
		let mut response = Response {
			results: to_map(vec![Err(Error::ConnectionUninitialised.into())]),
			..Response::new()
		};
		response.take::<Option<()>>(0).unwrap_err();
	}

	#[test]
	fn take_from_empty_records() {
		let mut response = Response {
			results: to_map(vec![]),
			..Response::new()
		};
		let value: Value = response.take(0).unwrap();
		assert_eq!(value, Default::default());

		let mut response = Response {
			results: to_map(vec![]),
			..Response::new()
		};
		let option: Option<String> = response.take(0).unwrap();
		assert!(option.is_none());

		let mut response = Response {
			results: to_map(vec![]),
			..Response::new()
		};
		let vec: Vec<String> = response.take(0).unwrap();
		assert!(vec.is_empty());
	}

	#[test]
	fn take_from_a_scalar_response() {
		let scalar = 265;

		let mut response = Response {
			results: to_map(vec![Ok(scalar.into())]),
			..Response::new()
		};
		let value: Value = response.take(0).unwrap();
		assert_eq!(value.into_inner(), val::Value::from(scalar));

		let mut response = Response {
			results: to_map(vec![Ok(scalar.into())]),
			..Response::new()
		};
		let option: Option<_> = response.take(0).unwrap();
		assert_eq!(option, Some(scalar));

		let mut response = Response {
			results: to_map(vec![Ok(scalar.into())]),
			..Response::new()
		};
		let vec: Vec<i64> = response.take(0).unwrap();
		assert_eq!(vec, vec![scalar]);

		let scalar = true;

		let mut response = Response {
			results: to_map(vec![Ok(scalar.into())]),
			..Response::new()
		};
		let value: Value = response.take(0).unwrap();
		assert_eq!(value.into_inner(), val::Value::from(scalar));

		let mut response = Response {
			results: to_map(vec![Ok(scalar.into())]),
			..Response::new()
		};
		let option: Option<_> = response.take(0).unwrap();
		assert_eq!(option, Some(scalar));

		let mut response = Response {
			results: to_map(vec![Ok(scalar.into())]),
			..Response::new()
		};
		let vec: Vec<bool> = response.take(0).unwrap();
		assert_eq!(vec, vec![scalar]);
	}

	#[test]
	fn take_preserves_order() {
		let mut response = Response {
			results: to_map(vec![
				Ok(0.into()),
				Ok(1.into()),
				Ok(2.into()),
				Ok(3.into()),
				Ok(4.into()),
				Ok(5.into()),
				Ok(6.into()),
				Ok(7.into()),
			]),
			..Response::new()
		};
		let Some(four): Option<i32> = response.take(4).unwrap() else {
			panic!("query not found");
		};
		assert_eq!(four, 4);
		let Some(six): Option<i32> = response.take(6).unwrap() else {
			panic!("query not found");
		};
		assert_eq!(six, 6);
		let Some(zero): Option<i32> = response.take(0).unwrap() else {
			panic!("query not found");
		};
		assert_eq!(zero, 0);
		let one: Value = response.take(1).unwrap();
		assert_eq!(one.into_inner(), val::Value::from(1));
	}

	#[test]
	fn take_key() {
		let summary = Summary {
			title: "Lorem Ipsum".to_owned(),
		};
		let value = to_value(summary.clone()).unwrap();

		let mut response = Response {
			results: to_map(vec![Ok(value.clone().into_inner())]),
			..Response::new()
		};
		let title: Value = response.take("title").unwrap();
		assert_eq!(title.into_inner(), val::Value::from(summary.title.as_str()));

		let mut response = Response {
			results: to_map(vec![Ok(value.clone().into_inner())]),
			..Response::new()
		};
		let Some(title): Option<String> = response.take("title").unwrap() else {
			panic!("title not found");
		};
		assert_eq!(title, summary.title);

		let mut response = Response {
			results: to_map(vec![Ok(value.into_inner())]),
			..Response::new()
		};
		let vec: Vec<String> = response.take("title").unwrap();
		assert_eq!(vec, vec![summary.title]);

		let article = Article {
			title: "Lorem Ipsum".to_owned(),
			body: "Lorem Ipsum Lorem Ipsum".to_owned(),
		};
		let value = to_value(article.clone()).unwrap();

		let mut response = Response {
			results: to_map(vec![Ok(value.clone().into_inner())]),
			..Response::new()
		};
		let Some(title): Option<String> = response.take("title").unwrap() else {
			panic!("title not found");
		};
		assert_eq!(title, article.title);
		let Some(body): Option<String> = response.take("body").unwrap() else {
			panic!("body not found");
		};
		assert_eq!(body, article.body);

		let mut response = Response {
			results: to_map(vec![Ok(value.clone().into_inner())]),
			..Response::new()
		};
		let vec: Vec<String> = response.take("title").unwrap();
		assert_eq!(vec, vec![article.title.clone()]);

		let mut response = Response {
			results: to_map(vec![Ok(value.into_inner())]),
			..Response::new()
		};
		let value: Value = response.take("title").unwrap();
		assert_eq!(value.into_inner(), val::Value::from(article.title));
	}

	#[test]
	fn take_key_multi() {
		let article = Article {
			title: "Lorem Ipsum".to_owned(),
			body: "Lorem Ipsum Lorem Ipsum".to_owned(),
		};
		let value = to_value(article.clone()).unwrap();

		let mut response = Response {
			results: to_map(vec![Ok(value.clone().into_inner())]),
			..Response::new()
		};
		let title: Vec<String> = response.take("title").unwrap();
		assert_eq!(title, vec![article.title.clone()]);
		let body: Vec<String> = response.take("body").unwrap();
		assert_eq!(body, vec![article.body]);

		let mut response = Response {
			results: to_map(vec![Ok(value.clone().into_inner())]),
			..Response::new()
		};
		let vec: Vec<String> = response.take("title").unwrap();
		assert_eq!(vec, vec![article.title]);
	}

	#[test]
	fn take_partial_records() {
		let mut response = Response {
			results: to_map(vec![Ok(vec![val::Value::from(true), val::Value::from(false)].into())]),
			..Response::new()
		};
		let value: Value = response.take(0).unwrap();
		assert_eq!(
			value.into_inner(),
			val::Value::from(vec![val::Value::from(true), val::Value::from(false)])
		);

		let mut response = Response {
			results: to_map(vec![Ok(vec![val::Value::from(true), val::Value::from(false)].into())]),
			..Response::new()
		};
		let vec: Vec<bool> = response.take(0).unwrap();
		assert_eq!(vec, vec![true, false]);

		let mut response = Response {
			results: to_map(vec![Ok(vec![val::Value::from(true), val::Value::from(false)].into())]),
			..Response::new()
		};

		let Err(e) = response.take::<Option<bool>>(0) else {
			panic!("silently dropping records not allowed");
		};
		let Ok(Error::LossyTake(Response {
			results: mut map,
			..
		})) = e.downcast()
		else {
			panic!("silently dropping records not allowed");
		};

		let records = map.swap_remove(&0).unwrap().1.unwrap();
		assert_eq!(
			records,
			val::Value::from(vec![val::Value::from(true), val::Value::from(false)])
		);
	}

	#[test]
	fn check_returns_the_first_error() {
		let response = vec![
			Ok(0.into()),
			Ok(1.into()),
			Ok(2.into()),
			Err(Error::ConnectionUninitialised.into()),
			Ok(3.into()),
			Ok(4.into()),
			Ok(5.into()),
			Err(Error::BackupsNotSupported.into()),
			Ok(6.into()),
			Ok(7.into()),
			Err(Error::DuplicateRequestId(0).into()),
		];
		let response = Response {
			results: to_map(response),
			..Response::new()
		};
		let Some(Error::ConnectionUninitialised) = response.check().unwrap_err().downcast_ref()
		else {
			panic!("check did not return the first error");
		};
	}

	#[test]
	fn take_errors() {
		let response = vec![
			Ok(0.into()),
			Ok(1.into()),
			Ok(2.into()),
			Err(Error::ConnectionUninitialised.into()),
			Ok(3.into()),
			Ok(4.into()),
			Ok(5.into()),
			Err(Error::BackupsNotSupported.into()),
			Ok(6.into()),
			Ok(7.into()),
			Err(Error::DuplicateRequestId(0).into()),
		];
		let mut response = Response {
			results: to_map(response),
			..Response::new()
		};
		let errors = response.take_errors();
		assert_eq!(response.num_statements(), 8);
		assert_eq!(errors.len(), 3);
		let Some(Error::DuplicateRequestId(0)) = errors[&10].downcast_ref() else {
			panic!("index `10` is not `DuplicateRequestId`");
		};
		let Some(Error::BackupsNotSupported) = errors[&7].downcast_ref() else {
			panic!("index `7` is not `BackupsNotSupported`");
		};
		let Some(Error::ConnectionUninitialised) = errors[&3].downcast_ref() else {
			panic!("index `3` is not `ConnectionUninitialised`");
		};
		let Some(value): Option<i32> = response.take(2).unwrap() else {
			panic!("statement not found");
		};
		assert_eq!(value, 2);
		let value: Value = response.take(4).unwrap();
		assert_eq!(value.into_inner(), val::Value::from(3));
	}
}
