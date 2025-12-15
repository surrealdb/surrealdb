use std::pin::Pin;

use anyhow::Result;
use futures::StreamExt;
use surrealdb_types::{Array, Duration, QueryChunk, QueryStats, SurrealValue, Value, Variables};

use crate::method::{Executable, Request};
use crate::utils::{Bindable, QueryFrame, QueryStream};

fn empty_stats() -> QueryStats {
	QueryStats {
		records_received: 0,
		bytes_received: 0,
		records_scanned: 0,
		bytes_scanned: 0,
		duration: Duration::default(),
	}
}

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
						results[index].result = Ok(value);
					} else {
						if let Some(values) = values[index].as_mut() {
							values.push(value);
						} else {
							values[index] = Some(vec![value]);
						}
					}
				}
				QueryFrame::Error { error, stats, .. } => {
					results[index].result = Err(error.to_string());
					results[index].stats = stats;
				}
				QueryFrame::Done { stats, .. } => {
					if let Some(values) = values[index].take() {
						results[index].result = Ok(Value::Array(Array::from_values(values)));
					}
					results[index].stats = stats;
				}
			}
		}

		Ok(QueryResults { results })
	}
}

impl Executable for Query {
	type Output = QueryResults;

	fn execute(req: Request<Self>) -> impl Future<Output = Result<Self::Output>> + Send {
		req.collect()
	}
}

/// Result of a single query statement.
#[derive(Debug, Clone)]
pub struct QueryResult<T: SurrealValue = Value> {
	result: Result<T, String>,
	stats: QueryStats,
}

impl Default for QueryResult {
	fn default() -> Self {
		Self {
			result: Ok(Value::None),
			stats: empty_stats(),
		}
	}
}

impl<T: SurrealValue> QueryResult<T> {
	/// Returns true if this query succeeded.
	pub fn is_ok(&self) -> bool {
		self.result.is_ok()
	}

	/// Returns true if this query failed.
	pub fn is_err(&self) -> bool {
		self.result.is_err()
	}

	pub fn take(self) -> Result<T, String> {
		self.result
	}

	/// Returns the query statistics.
	pub fn stats(&self) -> &QueryStats {
		&self.stats
	}
}

/// Collected results from a multi-statement query.
///
/// Each statement's results are stored separately, allowing per-statement
/// error handling and typed access.
#[derive(Debug, Clone, Default)]
pub struct QueryResults {
	results: Vec<QueryResult>,
}

impl QueryResults {
	/// Returns the number of statements in the query.
	pub fn len(&self) -> usize {
		self.results.len()
	}

	/// Returns true if no statements were executed.
	pub fn is_empty(&self) -> bool {
		self.results.is_empty()
	}

	/// Gets a reference to the result at the given index.
	pub fn get(&self, index: usize) -> Option<&QueryResult> {
		self.results.get(index)
	}

	/// Gets a mutable reference to the result at the given index.
	pub fn get_mut(&mut self, index: usize) -> Option<&mut QueryResult> {
		self.results.get_mut(index)
	}

	/// Returns true if all statements succeeded.
	pub fn is_all_ok(&self) -> bool {
		self.results.iter().all(|r| r.is_ok())
	}

	/// Returns an iterator over all results.
	pub fn iter(&self) -> impl Iterator<Item = &QueryResult> {
		self.results.iter()
	}

	/// Returns a mutable iterator over all results.
	pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut QueryResult> {
		self.results.iter_mut()
	}
}

impl std::ops::Index<usize> for QueryResults {
	type Output = QueryResult;

	fn index(&self, index: usize) -> &Self::Output {
		&self.results[index]
	}
}

impl std::ops::IndexMut<usize> for QueryResults {
	fn index_mut(&mut self, index: usize) -> &mut Self::Output {
		&mut self.results[index]
	}
}

impl IntoIterator for QueryResults {
	type Item = QueryResult;
	type IntoIter = std::vec::IntoIter<QueryResult>;

	fn into_iter(self) -> Self::IntoIter {
		self.results.into_iter()
	}
}
