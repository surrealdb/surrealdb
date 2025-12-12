use anyhow::Result;
use surrealdb_types::{SurrealValue, Variables};
use futures::StreamExt;
use crate::method::{Executable, Request};

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
	/// Add a variable to the query.
	pub fn var(mut self, key: impl Into<String>, value: impl SurrealValue) -> Self {
		self.inner.variables.insert(key.into(), value.into_value());
		self
	}

	/// Add multiple variables to the query.
	pub fn vars(mut self, vars: impl Into<Variables>) -> Self {
		self.inner.variables.extend(vars.into());
		self
	}

	/// Append SQL to the query.
	pub fn append(mut self, sql: impl Into<String>) -> Self {
		self.inner.query.push_str(&sql.into());
		self
	}

	pub async fn collect(self) -> Result<Vec<surrealdb_types::Value>> {
		self.controller.ready().await?;
		let mut stream = self.controller
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
