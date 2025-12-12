use std::collections::{BTreeMap, HashMap};
use std::pin::Pin;

use anyhow::Result;
use futures::StreamExt;
use surrealdb_types::{Object, QueryChunk, SurrealValue, Variables};

use crate::method::{Executable, Request};
use crate::utils::QueryStream;

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
	pub fn bind(mut self, vars: impl QueryVars) -> Self {
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

	/// Execute the query and collect all results into a Vec.
	pub async fn collect(self) -> Result<Vec<surrealdb_types::Value>> {
		self.controller.ready().await?;
		let mut stream = self
			.controller
			.query(self.session_id, self.tx_id, self.inner.query, self.inner.variables)
			.await?;

		let mut results = Vec::new();
		while let Some(chunk) = stream.next().await {
			if let Some(values) = chunk.result {
				results.extend(values);
			}
		}

		Ok(results)
	}
}

impl Executable for Query {
	type Output = Vec<surrealdb_types::Value>;

	fn execute(req: Request<Self>) -> impl Future<Output = Result<Self::Output>> + Send {
		// TODO the default behaviour is collect, but we can also add a stream method.
		// You then end up with the following:
		//   db.query("return 123").await?;            -- collects (default)
		//   db.query("return 123").collect().await?;  -- collects
		//   db.query("return 123").stream().await?;   -- streams
		req.collect()
	}
}

pub trait QueryVars {
	fn bind(self, vars: &mut Variables);
}

impl<K: Into<String>, V: SurrealValue> QueryVars for BTreeMap<K, V> {
	fn bind(self, vars: &mut Variables) {
		vars.extend(self.into_iter().map(|(k, v)| (k.into(), v.into_value())).collect());
	}
}

impl<K: Into<String>, V: SurrealValue> QueryVars for HashMap<K, V> {
	fn bind(self, vars: &mut Variables) {
		vars.extend(self.into_iter().map(|(k, v)| (k.into(), v.into_value())).collect());
	}
}

impl<K: Into<String>, V: SurrealValue> QueryVars for Vec<(K, V)> {
	fn bind(self, vars: &mut Variables) {
		vars.extend(self.into_iter().map(|(k, v)| (k.into(), v.into_value())).collect());
	}
}

impl QueryVars for Variables {
	fn bind(self, vars: &mut Variables) {
		vars.extend(self);
	}
}

impl QueryVars for Object {
	fn bind(self, vars: &mut Variables) {
		vars.extend(Variables::from(self));
	}
}

impl<K: Into<String>, V: SurrealValue> QueryVars for (K, V) {
	fn bind(self, vars: &mut Variables) {
		vars.insert(self.0, self.1);
	}
}