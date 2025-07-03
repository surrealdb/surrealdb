use super::live;
use super::transaction::WithTransaction;
use crate::Surreal;
use crate::api::ExtraFeatures;
use crate::api::Result;
use crate::api::conn::Command;
use crate::api::err::Error;
use crate::api::method::BoxFuture;
use crate::api::opt;
use crate::method::Commit;
use crate::method::Stats;
use crate::method::WithStats;
use crate::opt::IntoVariables;
use crate::value::Notification;
use anyhow::bail;
use anyhow::{Context as AnyhowContext, anyhow};
use futures::StreamExt;
use futures::future::Either;
use futures::stream::SelectAll;
use indexmap::IndexMap;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::borrow::Cow;
use std::collections::HashMap;
use std::future::IntoFuture;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use surrealdb_core::dbs::ResponseData;
use surrealdb_core::dbs::Variables;
use surrealdb_core::dbs::{self, Failure};
use surrealdb_core::expr::TryFromValue;
use surrealdb_core::expr::{Object, Value, to_value as to_core_value};
use surrealdb_core::rpc;
use surrealdb_core::sql;
use surrealdb_core::sql::Statement;
use surrealdb_protocol::proto::rpc::v1::QueryRequest;
use uuid::Uuid;

/// A query future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Query {
	pub(crate) txn: Option<Uuid>,
	pub(crate) client: Surreal,
	pub(crate) queries: Vec<String>,
	pub(crate) variables: Variables,
}

impl WithTransaction for Query {
	fn with_transaction(mut self, id: Uuid) -> Self {
		self.txn = Some(id);
		self
	}
}

impl Query {
	pub fn new(client: Surreal) -> Self {
		Query {
			txn: None,
			client,
			queries: Vec::new(),
			variables: Variables::default(),
		}
	}

	/// Sets the variables for the query
	pub fn variables(mut self, variables: impl Into<Variables>) -> Self {
		self.variables = variables.into();
		self
	}
}

impl IntoFuture for Query
where
	Self: Send + Sync + 'static,
{
	type Output = Result<QueryResults>;
	type IntoFuture = BoxFuture<'static, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let mut client = self.client.client.clone();
			let client = &mut client;
			let query = self.queries.join(";");
			let variables =
				self.variables.try_into().context("Failed to convert variables to QueryRequest")?;

			let num_statements = self.queries.len();

			let response = client
				.query(QueryRequest {
					query,
					variables: Some(variables),
				})
				.await?;

			let response = response.into_inner();
			todo!("STU: Query::into_future");

			// let mut stream = response.into_inner();

			// let mut query_results = QueryResults::with_capacity(num_statements);
			// while let Some(result) = stream.next().await {
			// 	let result = result?;
			// 	let index = result.query_index as usize;
			// 	let values = result.values.ok_or_else(|| anyhow::anyhow!("Missing values in query response"))?;
			// 	// If this is the first result for the query index, create a new query result with the stats, otherwise extend the existing query result
			// 	let mut query_result = query_results.results.entry(index).or_insert_with(dbs::QueryResult::default);
			// 	if let Some(stats) = result.stats {
			// 		// TODO: Only do this for the first result of the query index.
			// 		query_result.stats = stats.try_into()?;
			// 	}
			// 	if let Ok(result_values) = &mut query_result.values {
			// 		result_values.extend(values.values.into_iter().map(TryInto::try_into).collect::<Result<Vec<_>>>()?);
			// 	}
			// }

			// Ok(query_results)
		})
	}
}

impl IntoFuture for WithStats<Query>
where
	Self: Send + Sync + 'static,
{
	type Output = Result<WithStats<QueryResults>>;
	type IntoFuture = BoxFuture<'static, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let response = self.0.await?;
			Ok(WithStats(response))
		})
	}
}

impl Query {
	/// Chains a query onto an existing query
	pub fn query(mut self, surql: impl Into<String>) -> Self {
		let client = self.client.clone();

		let surql = surql.into();

		self.queries.push(surql);

		self
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
	pub fn bind(mut self, bindings: impl IntoVariables) -> Self {
		let variables = bindings.into_variables();

		self.variables.extend(variables);

		self
	}
}

/// The response type of a `Surreal::query` request
#[derive(Debug)]
pub struct QueryResults {
	pub(crate) results: IndexMap<usize, dbs::QueryResult>,
}

impl QueryResults {
	pub(crate) fn with_capacity(capacity: usize) -> Self {
		Self {
			results: IndexMap::with_capacity(capacity),
		}
	}
}

#[derive(Debug)]
pub struct QueryResult<R> {
	pub stats: dbs::QueryStats,
	pub result: Result<R>,
}

// impl<R> futures::Stream for QueryStream<Notification<R>>
// where
// 	R: TryFromValue + Unpin,
// {
// 	type Item = Result<Notification<R>>;

// 	fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
// 		self.as_mut().0.poll_next_unpin(cx)
// 	}
// }

impl QueryResults {
	pub(crate) fn new() -> Self {
		Self {
			results: Default::default(),
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
	pub fn take<R>(&mut self, index: impl opt::QueryAccessor<R>) -> Result<R>
	where
		R: TryFromValue,
	{
		index.query_result(self)
	}

	/// Take all errors from the query response
	///
	/// The errors are keyed by the corresponding index of the statement that failed.
	/// Afterwards the response is left with only statements that did not produce any errors.
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
			if result.values.is_err() {
				keys.push(*key);
			}
		}
		let mut errors = HashMap::with_capacity(keys.len());
		for key in keys {
			if let Some(query_result) = self.results.swap_remove(&key) {
				if let Err(error) = query_result.values {
					// If the result is an error, we insert it into the errors map
					errors.insert(key, error.into());
				}
			}
		}
		errors
	}

	/// Check query response for errors and return the first error, if any, or the response
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
			if result.values.is_err() {
				first_error = Some(*key);
				break;
			}
		}
		if let Some(key) = first_error {
			if let Some(query_result) = self.results.swap_remove(&key) {
				if let Err(error) = query_result.values {
					return Err(error.into());
				}
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

impl WithStats<QueryResults> {
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
	pub fn take<R>(&mut self, index: impl opt::QueryAccessor<R>) -> Option<QueryResult<R>>
	where
		R: TryFromValue,
	{
		let stats = index.stats(&self.0)?;
		let result = index.query_result(&mut self.0);
		Some(QueryResult {
			stats,
			result,
		})
	}

	/// Take all errors from the query response
	///
	/// The errors are keyed by the corresponding index of the statement that failed.
	/// Afterwards the response is left with only statements that did not produce any errors.
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
	pub fn take_errors(&mut self) -> HashMap<usize, dbs::QueryResult> {
		let mut keys = Vec::new();
		for (key, result) in &self.0.results {
			if result.values.is_err() {
				keys.push(*key);
			}
		}
		let mut errors = HashMap::with_capacity(keys.len());
		for key in keys {
			if let Some(query_result) = self.0.results.swap_remove(&key) {
				if query_result.values.is_err() {
					errors.insert(key, query_result);
				}
			}
		}
		errors
	}

	/// Check query response for errors and return the first error, if any, or the response
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
	pub fn into_inner(self) -> QueryResults {
		self.0
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use serde::Deserialize;
	use surrealdb_core::expr::Value;
	use surrealdb_core::expr::to_value;

	#[derive(Debug, Clone, Serialize, Deserialize)]
	struct Summary {
		title: String,
	}

	#[derive(Debug, Clone, Serialize, Deserialize)]
	struct Article {
		title: String,
		body: String,
	}

	fn to_map(vec: Vec<dbs::QueryResult>) -> IndexMap<usize, dbs::QueryResult> {
		vec.into_iter().enumerate().collect()
	}

	#[test]
	fn take_from_an_empty_response() {
		let mut response = QueryResults::new();
		let value: Value = response.take(0).unwrap();
		assert!(value.is_none());

		let mut response = QueryResults::new();
		let option: Option<String> = response.take(0).unwrap();
		assert!(option.is_none());

		let mut response = QueryResults::new();
		let vec: Vec<String> = response.take(0).unwrap();
		assert!(vec.is_empty());
	}

	#[test]
	fn take_from_an_errored_query() {
		let mut response = QueryResults {
			results: to_map(vec![dbs::QueryResult {
				stats: dbs::QueryStats::default(),
				values: Err(Error::ConnectionUninitialised.into()),
			}]),
			..QueryResults::new()
		};
		response.take::<Option<()>>(0).unwrap_err();
	}

	#[test]
	fn take_from_empty_records() {
		let mut response = QueryResults {
			results: to_map(vec![]),
			..QueryResults::new()
		};
		let value: Value = response.take(0).unwrap();
		assert_eq!(value, Default::default());

		let mut response = QueryResults {
			results: to_map(vec![]),
			..QueryResults::new()
		};
		let option: Option<String> = response.take(0).unwrap();
		assert!(option.is_none());

		let mut response = QueryResults {
			results: to_map(vec![]),
			..QueryResults::new()
		};
		let vec: Vec<String> = response.take(0).unwrap();
		assert!(vec.is_empty());
	}

	#[test]
	fn take_from_a_scalar_response() {
		let scalar = 265;

		let mut response = QueryResults {
			results: to_map(vec![dbs::QueryResult::ok(scalar.into())]),
			..QueryResults::new()
		};
		let value: Value = response.take(0).unwrap();
		assert_eq!(value, Value::from(scalar));

		let mut response = QueryResults {
			results: to_map(vec![dbs::QueryResult::ok(scalar.into())]),
			..QueryResults::new()
		};
		let option: Option<_> = response.take(0).unwrap();
		assert_eq!(option, Some(scalar));

		let mut response = QueryResults {
			results: to_map(vec![dbs::QueryResult::ok(scalar.into())]),
			..QueryResults::new()
		};
		let vec: Vec<i64> = response.take(0).unwrap();
		assert_eq!(vec, vec![scalar]);

		let scalar = true;

		let mut response = QueryResults {
			results: to_map(vec![dbs::QueryResult::ok(scalar.into())]),
			..QueryResults::new()
		};
		let value: Value = response.take(0).unwrap();
		assert_eq!(value, Value::from(scalar));

		let mut response = QueryResults {
			results: to_map(vec![dbs::QueryResult::ok(scalar.into())]),
			..QueryResults::new()
		};
		let option: Option<_> = response.take(0).unwrap();
		assert_eq!(option, Some(scalar));

		let mut response = QueryResults {
			results: to_map(vec![dbs::QueryResult::ok(scalar.into())]),
			..QueryResults::new()
		};
		let vec: Vec<bool> = response.take(0).unwrap();
		assert_eq!(vec, vec![scalar]);
	}

	#[test]
	fn take_preserves_order() {
		let mut response = QueryResults {
			results: to_map(vec![
				dbs::QueryResult::ok(0.into()),
				dbs::QueryResult::ok(1.into()),
				dbs::QueryResult::ok(2.into()),
				dbs::QueryResult::ok(3.into()),
				dbs::QueryResult::ok(4.into()),
				dbs::QueryResult::ok(5.into()),
				dbs::QueryResult::ok(6.into()),
				dbs::QueryResult::ok(7.into()),
			]),
			..QueryResults::new()
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
		assert_eq!(one, Value::from(1));
	}

	#[test]
	fn take_key() {
		let summary = Summary {
			title: "Lorem Ipsum".to_owned(),
		};
		let value = to_value(summary.clone()).unwrap();

		let mut response = QueryResults {
			results: to_map(vec![dbs::QueryResult::ok(value.clone())]),
			..QueryResults::new()
		};
		let title: Value = response.take("title").unwrap();
		assert_eq!(title, Value::from(summary.title.as_str()));

		let mut response = QueryResults {
			results: to_map(vec![dbs::QueryResult::ok(value.clone())]),
			..QueryResults::new()
		};
		let Some(title): Option<String> = response.take("title").unwrap() else {
			panic!("title not found");
		};
		assert_eq!(title, summary.title);

		let mut response = QueryResults {
			results: to_map(vec![dbs::QueryResult::ok(value)]),
			..QueryResults::new()
		};
		let vec: Vec<String> = response.take("title").unwrap();
		assert_eq!(vec, vec![summary.title]);

		let article = Article {
			title: "Lorem Ipsum".to_owned(),
			body: "Lorem Ipsum Lorem Ipsum".to_owned(),
		};
		let value: Value = to_value(article).unwrap();

		let mut response = QueryResults {
			results: to_map(vec![dbs::QueryResult::ok(value.clone())]),
			..QueryResults::new()
		};
		let Some(title): Option<String> = response.take("title").unwrap() else {
			panic!("title not found");
		};
		assert_eq!(title, article.title);
		let Some(body): Option<String> = response.take("body").unwrap() else {
			panic!("body not found");
		};
		assert_eq!(body, article.body);

		let mut response = QueryResults {
			results: to_map(vec![dbs::QueryResult::ok(value.clone())]),
			..QueryResults::new()
		};
		let vec: Vec<String> = response.take("title").unwrap();
		assert_eq!(vec, vec![article.title.clone()]);

		let mut response = QueryResults {
			results: to_map(vec![dbs::QueryResult::ok(value)]),
			..QueryResults::new()
		};
		let value: Value = response.take("title").unwrap();
		assert_eq!(value, Value::from(article.title));
	}

	#[test]
	fn take_key_multi() {
		let article = Article {
			title: "Lorem Ipsum".to_owned(),
			body: "Lorem Ipsum Lorem Ipsum".to_owned(),
		};
		let value: Value = to_value(article).unwrap();

		let mut response = QueryResults {
			results: to_map(vec![dbs::QueryResult::ok(value.clone())]),
			..QueryResults::new()
		};
		let title: Vec<String> = response.take("title").unwrap();
		assert_eq!(title, vec![article.title.clone()]);
		let body: Vec<String> = response.take("body").unwrap();
		assert_eq!(body, vec![article.body]);

		let mut response = QueryResults {
			results: to_map(vec![dbs::QueryResult::ok(value.clone())]),
			..QueryResults::new()
		};
		let vec: Vec<String> = response.take("title").unwrap();
		assert_eq!(vec, vec![article.title]);
	}

	#[test]
	fn take_partial_records() {
		let mut response = QueryResults {
			results: to_map(vec![dbs::QueryResult::ok(vec![true, false].into())]),
			..QueryResults::new()
		};
		let value: Value = response.take(0).unwrap();
		assert_eq!(value, vec![Value::from(true), Value::from(false)].into());

		let mut response = QueryResults {
			results: to_map(vec![dbs::QueryResult::ok(vec![true, false].into())]),
			..QueryResults::new()
		};
		let vec: Vec<bool> = response.take(0).unwrap();
		assert_eq!(vec, vec![true, false]);

		let mut response = QueryResults {
			results: to_map(vec![dbs::QueryResult::ok(vec![true, false].into())]),
			..QueryResults::new()
		};

		let Err(e) = response.take::<Option<bool>>(0) else {
			panic!("silently dropping records not allowed");
		};
		let Ok(Error::LossyTake(QueryResults {
			results: mut map,
			..
		})) = e.downcast()
		else {
			panic!("silently dropping records not allowed");
		};

		let records = map.swap_remove(&0).unwrap().result.unwrap();
		assert_eq!(records, vec![true, false].into());
	}

	#[test]
	fn check_returns_the_first_error() {
		let response = vec![
			dbs::QueryResult::ok(0.into()),
			dbs::QueryResult::ok(1.into()),
			dbs::QueryResult::ok(2.into()),
			dbs::QueryResult::err(Error::ConnectionUninitialised.into()),
			dbs::QueryResult::ok(3.into()),
			dbs::QueryResult::ok(4.into()),
			dbs::QueryResult::ok(5.into()),
			dbs::QueryResult::err(Error::BackupsNotSupported.into()),
			dbs::QueryResult::ok(6.into()),
			dbs::QueryResult::ok(7.into()),
			dbs::QueryResult::err(Error::DuplicateRequestId("0".to_string()).into()),
		];
		let response = QueryResults {
			results: to_map(response),
			..QueryResults::new()
		};
		let Some(Error::ConnectionUninitialised) = response.check().unwrap_err().downcast_ref()
		else {
			panic!("check did not return the first error");
		};
	}

	#[test]
	fn take_errors() {
		let response = vec![
			dbs::QueryResult::ok(0.into()),
			dbs::QueryResult::ok(1.into()),
			dbs::QueryResult::ok(2.into()),
			dbs::QueryResult::err(Error::ConnectionUninitialised.into()),
			dbs::QueryResult::ok(3.into()),
			dbs::QueryResult::ok(4.into()),
			dbs::QueryResult::ok(5.into()),
			dbs::QueryResult::err(Error::BackupsNotSupported.into()),
			dbs::QueryResult::ok(6.into()),
			dbs::QueryResult::ok(7.into()),
			dbs::QueryResult::err(Error::DuplicateRequestId("0".to_string()).into()),
		];
		let mut response = QueryResults {
			results: to_map(response),
			..QueryResults::new()
		};
		let errors = response.take_errors();
		assert_eq!(response.num_statements(), 8);
		assert_eq!(errors.len(), 3);
		let Some(Error::DuplicateRequestId(duplicate_id)) = errors[&10].downcast_ref() else {
			panic!("index `10` is not `DuplicateRequestId`");
		};
		assert_eq!(duplicate_id, "0");
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
		assert_eq!(value, Value::from(3));
	}
}
