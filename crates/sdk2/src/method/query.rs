use std::pin::Pin;

use anyhow::Result;
use futures::StreamExt;
use surrealdb_types::{Array, QueryChunk, Value, Variables};

use crate::method::{Executable, Request};
use crate::utils::{Bindable, QueryFrame, QueryResult, QueryResults, QueryStream};

#[derive(Clone)]
pub struct Query {
	pub(crate) query: String,
	pub(crate) variables: Variables,
}

impl Query {
	pub fn new(query: impl Into<String>) -> Self {
		Self {
			query: query.into(),
			variables: Variables::new(),
		}
	}
}

/// Builder methods for Query requests
impl Request<Query> {
	pub fn bind(mut self, vars: impl Bindable) -> Self {
		vars.bind(&mut self.inner.variables);
		self
	}

	/// Append SQL to the query.
	pub fn append(mut self, sql: impl Into<String>) -> Self {
		self.inner.query.push_str(&sql.into());
		self
	}

	/// Execute the query and return a stream of [`QueryFrame`](crate::utils::QueryFrame)s.
	///
	/// This allows processing results as they arrive, which is useful for:
	/// - Large result sets that shouldn't be loaded entirely into memory
	/// - Multi-statement queries where you want to process each statement's results separately
	///
	/// # Example
	/// ```ignore
	/// use futures::StreamExt;
	///
	/// let mut stream = db.query("SELECT * FROM user; SELECT * FROM post").stream().await?;
	///
	/// while let Some(frame) = stream.next().await {
	///     match frame {
	///         QueryFrame::Value { query, value } => println!("Query {query}: {value:?}"),
	///         QueryFrame::Done { query, .. } => println!("Query {query} complete"),
	///         QueryFrame::Error { error, .. } => eprintln!("Error: {error}"),
	///     }
	/// }
	/// ```
	pub async fn stream(
		self,
	) -> Result<QueryStream<Pin<Box<dyn futures::Stream<Item = QueryChunk> + Send>>>> {
		self.controller.ready().await?;
		let inner = self
			.controller
			.query(self.session_id, self.tx_id, self.inner.query, self.inner.variables)
			.await?;
		Ok(QueryStream::new(inner))
	}

	/// Execute the query and collect all results into [`QueryResults`].
	///
	/// Each statement's results are collected separately, allowing you to:
	/// - Access results by statement index
	/// - Handle errors per-statement
	/// - Access query statistics
	///
	/// # Example
	/// ```ignore
	/// let results = db.query("SELECT * FROM user; SELECT * FROM post").collect().await?;
	///
	/// // Get results for first statement
	/// let users: Vec<User> = results.take(0)?;
	///
	/// // Check if all statements succeeded
	/// results.all_ok()?;
	/// ```
	pub async fn collect(self) -> Result<QueryResults> {
		let mut stream = self.stream().await?;
		let mut values: Vec<Option<Vec<Value>>> = Vec::new();
		let mut results: Vec<QueryResult> = Vec::new();

		while let Some(frame) = stream.next().await {
			let index = frame.query() as usize;

			// Ensure we have enough slots
			while values.len() <= index {
				values.push(None);
				results.push(QueryResult::default());
			}

			match frame {
				QueryFrame::Value { value, is_single, .. } => {
					if is_single {
						// For single values, we need to preserve stats/kind from default
						// They'll be updated when Done frame arrives
						let stats = results[index].stats().clone();
						let kind = results[index].kind().clone();
						results[index] = QueryResult::new(Ok(value), stats, kind);
					} else {
						if let Some(values) = values[index].as_mut() {
							values.push(value);
						} else {
							values[index] = Some(vec![value]);
						}
					}
				}
				QueryFrame::Error { error, stats, r#type, .. } => {
					results[index] = QueryResult::new(Err(error.to_string()), stats, r#type);
				}
				QueryFrame::Done { stats, r#type, .. } => {
					let result = if let Some(values) = values[index].take() {
						Ok(Value::Array(Array::from_values(values)))
					} else {
						// If no values were collected, check if we already have a single value
						match results[index].try_get::<Value>() {
							Some(Ok(v)) if v != Value::None => Ok(v),
							_ => Ok(Value::None),
						}
					};
					results[index] = QueryResult::new(result, stats, r#type);
				}
			}
		}

		Ok(QueryResults::new(results))
	}
}

impl Executable for Query {
	type Output = QueryResults;

	fn execute(req: Request<Self>) -> impl Future<Output = Result<Self::Output>> + Send {
		req.collect()
	}
}
