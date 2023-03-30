use crate::api::conn::Method;
use crate::api::conn::Param;
use crate::api::conn::Router;
use crate::api::err::Error;
use crate::api::opt;
use crate::api::Connection;
use crate::api::Result;
use crate::sql;
use crate::sql::to_value;
use crate::sql::Array;
use crate::sql::Object;
use crate::sql::Statement;
use crate::sql::Statements;
use crate::sql::Strand;
use crate::sql::Value;
use indexmap::IndexMap;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::future::Future;
use std::future::IntoFuture;
use std::mem;
use std::pin::Pin;

/// A query future
#[derive(Debug)]
pub struct Query<'r, C: Connection> {
	pub(super) router: Result<&'r Router<C>>,
	pub(super) query: Vec<Result<Vec<Statement>>>,
	pub(super) bindings: Result<BTreeMap<String, Value>>,
}

impl<'r, Client> IntoFuture for Query<'r, Client>
where
	Client: Connection,
{
	type Output = Result<Response>;
	type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'r>>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let mut statements = Vec::with_capacity(self.query.len());
			for query in self.query {
				statements.extend(query?);
			}
			let query = sql::Query(Statements(statements));
			let param = Param::query(query, self.bindings?);
			let mut conn = Client::new(Method::Query);
			conn.execute_query(self.router?, param).await
		})
	}
}

impl<'r, C> Query<'r, C>
where
	C: Connection,
{
	/// Chains a query onto an existing query
	pub fn query(mut self, query: impl opt::IntoQuery) -> Self {
		self.query.push(query.into_query());
		self
	}

	/// Binds a parameter or parameters to a query
	///
	/// # Examples
	///
	/// Binding a key/value tuple
	///
	/// ```no_run
	/// use surrealdb::sql;
	///
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
	/// use surrealdb::sql;
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
	pub fn bind(mut self, bindings: impl Serialize) -> Self {
		if let Ok(current) = &mut self.bindings {
			match to_value(bindings) {
				Ok(mut bindings) => {
					if let Value::Array(Array(array)) = &mut bindings {
						if let [Value::Strand(Strand(key)), value] = &mut array[..] {
							let mut map = BTreeMap::new();
							map.insert(mem::take(key), mem::take(value));
							bindings = map.into();
						}
					}
					match &mut bindings {
						Value::Object(Object(map)) => current.append(map),
						_ => {
							self.bindings = Err(Error::InvalidBindings(bindings).into());
						}
					}
				}
				Err(error) => {
					self.bindings = Err(error.into());
				}
			}
		}
		self
	}
}

pub(crate) type QueryResult = Result<Vec<Value>>;

/// The response type of a `Surreal::query` request
#[derive(Debug)]
pub struct Response(pub(crate) IndexMap<usize, QueryResult>);

impl Response {
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
	/// use surrealdb::sql;
	///
	/// #[derive(Debug, Deserialize)]
	/// # #[allow(dead_code)]
	/// struct User {
	///     id: String,
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

	/// Take all errors from the query response
	///
	/// The errors are keyed by the corresponding index of the statement that failed.
	/// Afterwards the response is left with only statements that did not produce any errors.
	///
	/// # Examples
	///
	/// ```no_run
	/// use surrealdb::sql;
	///
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engine::any::connect("mem://").await?;
	/// # let mut response = db.query("SELECT * FROM user").await?;
	/// let errors = response.take_errors();
	/// # Ok(())
	/// # }
	/// ```
	pub fn take_errors(&mut self) -> HashMap<usize, crate::Error> {
		let mut keys = Vec::new();
		for (key, result) in &self.0 {
			if result.is_err() {
				keys.push(*key);
			}
		}
		let mut errors = HashMap::with_capacity(keys.len());
		for key in keys {
			if let Some(Err(error)) = self.0.remove(&key) {
				errors.insert(key, error);
			}
		}
		errors
	}

	/// Check query response for errors and return the first error, if any, or the response
	///
	/// # Examples
	///
	/// ```no_run
	/// use surrealdb::sql;
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
		for (key, result) in &self.0 {
			if result.is_err() {
				first_error = Some(*key);
				break;
			}
		}
		if let Some(key) = first_error {
			if let Some(Err(error)) = self.0.remove(&key) {
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
	/// use surrealdb::sql;
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
		self.0.len()
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::Error::Api;
	use serde::Deserialize;

	#[derive(Debug, Clone, Serialize, Deserialize)]
	struct Summary {
		title: String,
	}

	#[derive(Debug, Clone, Serialize, Deserialize)]
	struct Article {
		title: String,
		body: String,
	}

	fn to_map(vec: Vec<QueryResult>) -> IndexMap<usize, QueryResult> {
		vec.into_iter().enumerate().collect()
	}

	#[test]
	fn take_from_an_empty_response() {
		let mut response = Response(Default::default());
		let option: Option<String> = response.take(0).unwrap();
		assert!(option.is_none());

		let mut response = Response(Default::default());
		let vec: Vec<String> = response.take(0).unwrap();
		assert!(vec.is_empty());
	}

	#[test]
	fn take_from_an_errored_query() {
		let mut response = Response(to_map(vec![Err(Error::ConnectionUninitialised.into())]));
		response.take::<Option<()>>(0).unwrap_err();
	}

	#[test]
	fn take_from_empty_records() {
		let mut response = Response(to_map(vec![Ok(vec![])]));
		let option: Option<String> = response.take(0).unwrap();
		assert!(option.is_none());

		let mut response = Response(to_map(vec![Ok(vec![])]));
		let vec: Vec<String> = response.take(0).unwrap();
		assert!(vec.is_empty());
	}

	#[test]
	fn take_from_a_scalar_response() {
		let scalar = 265;

		let mut response = Response(to_map(vec![Ok(vec![scalar.into()])]));
		let option: Option<_> = response.take(0).unwrap();
		assert_eq!(option, Some(scalar));

		let mut response = Response(to_map(vec![Ok(vec![scalar.into()])]));
		let vec: Vec<usize> = response.take(0).unwrap();
		assert_eq!(vec, vec![scalar]);

		let scalar = true;

		let mut response = Response(to_map(vec![Ok(vec![scalar.into()])]));
		let option: Option<_> = response.take(0).unwrap();
		assert_eq!(option, Some(scalar));

		let mut response = Response(to_map(vec![Ok(vec![scalar.into()])]));
		let vec: Vec<bool> = response.take(0).unwrap();
		assert_eq!(vec, vec![scalar]);
	}

	#[test]
	fn take_preserves_order() {
		let mut response = Response(to_map(vec![
			Ok(vec![0.into()]),
			Ok(vec![1.into()]),
			Ok(vec![2.into()]),
			Ok(vec![3.into()]),
			Ok(vec![4.into()]),
			Ok(vec![5.into()]),
			Ok(vec![6.into()]),
			Ok(vec![7.into()]),
		]));
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
		let Some(one): Option<i32> = response.take(1).unwrap() else {
            panic!("query not found");
        };
		assert_eq!(one, 1);
	}

	#[test]
	fn take_key() {
		let summary = Summary {
			title: "Lorem Ipsum".to_owned(),
		};
		let value = to_value(summary.clone()).unwrap();

		let mut response = Response(to_map(vec![Ok(vec![value.clone()])]));
		let Some(title): Option<String> = response.take("title").unwrap() else {
            panic!("title not found");
        };
		assert_eq!(title, summary.title);

		let mut response = Response(to_map(vec![Ok(vec![value])]));
		let vec: Vec<String> = response.take("title").unwrap();
		assert_eq!(vec, vec![summary.title]);

		let article = Article {
			title: "Lorem Ipsum".to_owned(),
			body: "Lorem Ipsum Lorem Ipsum".to_owned(),
		};
		let value = to_value(article.clone()).unwrap();

		let mut response = Response(to_map(vec![Ok(vec![value.clone()])]));
		let Some(title): Option<String> = response.take("title").unwrap() else {
            panic!("title not found");
        };
		assert_eq!(title, article.title);
		let Some(body): Option<String> = response.take("body").unwrap() else {
            panic!("body not found");
        };
		assert_eq!(body, article.body);

		let mut response = Response(to_map(vec![Ok(vec![value])]));
		let vec: Vec<String> = response.take("title").unwrap();
		assert_eq!(vec, vec![article.title]);
	}

	#[test]
	fn take_partial_records() {
		let mut response = Response(to_map(vec![Ok(vec![true.into(), false.into()])]));
		let vec: Vec<bool> = response.take(0).unwrap();
		assert_eq!(vec, vec![true, false]);

		let mut response = Response(to_map(vec![Ok(vec![true.into(), false.into()])]));
		let Err(Api(Error::LossyTake(Response(mut map)))): Result<Option<bool>> = response.take(0) else {
            panic!("silently dropping records not allowed");
        };
		let records = map.remove(&0).unwrap().unwrap();
		assert_eq!(records, vec![true.into(), false.into()]);
	}

	#[test]
	fn check_returns_the_first_error() {
		let response = vec![
			Ok(vec![0.into()]),
			Ok(vec![1.into()]),
			Ok(vec![2.into()]),
			Err(Error::ConnectionUninitialised.into()),
			Ok(vec![3.into()]),
			Ok(vec![4.into()]),
			Ok(vec![5.into()]),
			Err(Error::BackupsNotSupported.into()),
			Ok(vec![6.into()]),
			Ok(vec![7.into()]),
			Err(Error::AuthNotSupported.into()),
		];
		let response = Response(to_map(response));
		let crate::Error::Api(Error::ConnectionUninitialised) = response.check().unwrap_err() else {
            panic!("check did not return the first error");
        };
	}

	#[test]
	fn take_errors() {
		let response = vec![
			Ok(vec![0.into()]),
			Ok(vec![1.into()]),
			Ok(vec![2.into()]),
			Err(Error::ConnectionUninitialised.into()),
			Ok(vec![3.into()]),
			Ok(vec![4.into()]),
			Ok(vec![5.into()]),
			Err(Error::BackupsNotSupported.into()),
			Ok(vec![6.into()]),
			Ok(vec![7.into()]),
			Err(Error::AuthNotSupported.into()),
		];
		let mut response = Response(to_map(response));
		let errors = response.take_errors();
		assert_eq!(response.num_statements(), 8);
		assert_eq!(errors.len(), 3);
		let crate::Error::Api(Error::AuthNotSupported) = errors.get(&10).unwrap() else {
            panic!("index `10` is not `AuthNotSupported`");
        };
		let crate::Error::Api(Error::BackupsNotSupported) = errors.get(&7).unwrap() else {
            panic!("index `7` is not `BackupsNotSupported`");
        };
		let crate::Error::Api(Error::ConnectionUninitialised) = errors.get(&3).unwrap() else {
            panic!("index `3` is not `ConnectionUninitialised`");
        };
		let Some(value): Option<i32> = response.take(2).unwrap() else {
            panic!("statement not found");
        };
		assert_eq!(value, 2);
		let Some(value): Option<i32> = response.take(4).unwrap() else {
            panic!("statement not found");
        };
		assert_eq!(value, 3);
	}
}
