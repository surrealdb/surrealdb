use std::collections::HashMap;

use anyhow::Result;
use surrealdb_types::{Duration, QueryStats, QueryType, SurrealValue, Value};

/// Result of a single query statement.
#[derive(Debug, Clone)]
pub struct QueryResult<T: SurrealValue = Value> {
	result: std::result::Result<T, String>,
	stats: QueryStats,
	kind: QueryType,
}

impl Default for QueryResult {
	fn default() -> Self {
		Self {
			result: Ok(Value::None),
			stats: QueryStats {
                records_received: 0,
                bytes_received: 0,
                records_scanned: 0,
                bytes_scanned: 0,
                duration: Duration::default(),
            },
			kind: QueryType::Other,
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

	/// Takes ownership and returns the result.
	pub fn take(self) -> std::result::Result<T, String> {
		self.result
	}

	/// Returns the query statistics.
	pub fn stats(&self) -> &QueryStats {
		&self.stats
	}

	/// Returns the query type.
	pub fn kind(&self) -> &QueryType {
		&self.kind
	}
}

impl QueryResult {
	/// Converts the result to the specified type.
	///
	/// Returns an error if the query failed or type conversion fails.
	///
	/// # Example
	/// ```ignore
	/// let results = db.query("SELECT * FROM user:1").await?;
	/// let user: User = results.get(0)?.into_t()?;
	/// ```
	pub fn into_t<T: SurrealValue>(self) -> Result<T> {
		match self.result {
			Ok(value) => T::from_value(value),
			Err(e) => Err(anyhow::anyhow!("Query error: {}", e)),
		}
	}

	/// Converts this `QueryResult<Value>` into a `QueryResult<T>`.
	///
	/// The conversion happens at the result level, so if the query failed,
	/// the error is preserved. If type conversion fails, it becomes an error.
	///
	/// # Example
	/// ```ignore
	/// let result = db.select("user").collect().await?;
	/// let typed: QueryResult<Vec<User>> = result.into_typed()?;
	/// ```
	pub fn into_typed<T: SurrealValue>(self) -> Result<QueryResult<T>> {
		let result = match self.result {
			Ok(value) => match T::from_value(value) {
				Ok(typed) => Ok(typed),
				Err(e) => Err(format!("Type conversion failed: {}", e)),
			},
			Err(e) => Err(e),
		};
		Ok(QueryResult {
			result,
			stats: self.stats,
			kind: self.kind,
		})
	}

	/// Attempts to convert the result to the specified type without consuming.
	///
	/// Returns `None` if the query failed, or `Some(Ok(T))` / `Some(Err(e))` for conversion.
	///
	/// # Example
	/// ```ignore
	/// let results = db.query("SELECT * FROM user:1").await?;
	/// if let Some(result) = results.try_get::<User>(0) {
	///     match result {
	///         Ok(user) => println!("User: {user:?}"),
	///         Err(e) => eprintln!("Conversion error: {e}"),
	///     }
	/// }
	/// ```
	pub fn try_get<T: SurrealValue>(&self) -> Option<Result<T>> {
		match &self.result {
			Ok(value) => Some(T::from_value(value.clone())),
			Err(_) => None,
		}
	}

	pub(crate) fn new(result: std::result::Result<Value, String>, stats: QueryStats, kind: QueryType) -> Self {
		Self { result, stats, kind }
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
	///
	/// # Example
	/// ```ignore
	/// let results = db.query("SELECT * FROM user").await?;
	/// let result = results.get(0);
	/// ```
	pub fn get(&self, index: usize) -> Option<&QueryResult> {
		self.results.get(index)
	}

	/// Gets a mutable reference to the result at the given index.
	pub fn get_mut(&mut self, index: usize) -> Option<&mut QueryResult> {
		self.results.get_mut(index)
	}

	/// Takes ownership of the result at the given index and converts it to type `T`.
	///
	/// Returns an error if:
	/// - The index doesn't exist (use `?` to handle)
	/// - The query failed
	/// - Type conversion fails
	///
	/// # Example
	/// ```ignore
	/// let results = db.query("SELECT * FROM user:1").await?;
	///
	/// // Type inference from variable annotation (recommended)
	/// let user: User = results.take(0)?;
	///
	/// // Explicit type parameter (also works)
	/// let user = results.take::<User>(0)?;
	///
	/// // For arrays
	/// let users: Vec<User> = results.take(0)?;
	///
	/// // For scalars
	/// let n: i64 = results.take(0)?;
	/// ```
	pub fn take<T: SurrealValue>(&mut self, index: usize) -> Result<T> {
		match self.results.get(index) {
			Some(result) => result.clone().into_t(),
			None => Err(anyhow::anyhow!("Query index {} not found", index)),
		}
	}

	/// Attempts to get and convert the result at the given index without consuming.
	///
	/// Returns `None` if the index doesn't exist or the query failed.
	/// Returns `Some(Ok(T))` if conversion succeeds, or `Some(Err(e))` if conversion fails.
	///
	/// # Example
	/// ```ignore
	/// let results = db.query("SELECT * FROM user:1").await?;
	/// if let Some(result) = results.try_get::<User>(0) {
	///     match result {
	///         Ok(user) => println!("User: {user:?}"),
	///         Err(e) => eprintln!("Conversion error: {e}"),
	///     }
	/// }
	/// ```
	pub fn try_get<T: SurrealValue>(&self, index: usize) -> Option<Result<T>> {
		self.results.get(index)?.try_get()
	}

	/// Returns true if all statements succeeded.
	pub fn is_all_ok(&self) -> bool {
		self.results.iter().all(|r| r.is_ok())
	}

	/// Checks that all statements succeeded, returning the first error if any failed.
	///
	/// # Example
	/// ```ignore
	/// let results = db.query("SELECT * FROM user; SELECT * FROM post")
	///     .collect().await?
	///     .all_ok()?;
	/// ```
	pub fn all_ok(self) -> Result<Self> {
		for (index, result) in self.results.iter().enumerate() {
			if result.is_err() {
				// Clone to get the error message
				if let Err(err) = result.clone().take() {
					return Err(anyhow::anyhow!("Query {} failed: {}", index, err));
				}
			}
		}
		Ok(self)
	}

	/// Extracts all errors, keyed by statement index.
	///
	/// After calling this, the affected results will have their errors cleared.
	pub fn take_errors(&mut self) -> HashMap<usize, String> {
		let mut errors = HashMap::new();
		for (index, result) in self.results.iter_mut().enumerate() {
			if let Err(err) = &result.result {
				let err = err.clone();
				result.result = Ok(Value::None);
				errors.insert(index, err);
			}
		}
		errors
	}

	/// Returns an iterator over all results.
	pub fn iter(&self) -> impl Iterator<Item = &QueryResult> {
		self.results.iter()
	}

	/// Returns a mutable iterator over all results.
	pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut QueryResult> {
		self.results.iter_mut()
	}

	pub(crate) fn new(results: Vec<QueryResult>) -> Self {
		Self { results }
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

