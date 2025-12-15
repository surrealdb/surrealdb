use std::pin::Pin;

use anyhow::Result;
use futures::Stream;
use surrealdb_types::{Datetime, Duration, QueryChunk, RecordId, SurrealValue, Table, Value, Variables};

use crate::method::{Executable, Query, Request};
use crate::utils::{QueryResult, ValueStream};

#[derive(Clone)]
pub struct Select {
	pub(crate) subject: SelectSubject,
	pub(crate) fields: Vec<String>,
	pub(crate) limit: Option<u64>,
	pub(crate) start: Option<u64>,
	// cond
	// fetch
	pub(crate) timeout: Option<Duration>,
	pub(crate) version: Option<Datetime>,
}

impl Select {
	pub fn new(subject: impl Into<SelectSubject>) -> Self {
		Self {
			subject: subject.into(),
			fields: vec![],
			limit: None,
			start: None,
			timeout: None,
			version: None,
		}
	}

	pub fn build(&self) -> (String, Variables) {
		let mut sql = String::new();
		let mut vars = Variables::new();

		sql.push_str("SELECT ");
		if self.fields.is_empty() {
			sql.push_str("*");
		} else {
			sql.push_str("type::fields($fields)");
			vars.insert("fields".to_string(), self.fields.clone());
		}

		sql.push_str(" FROM $subject");
		vars.insert("subject".to_string(), self.subject.clone());

		if let Some(limit) = self.limit {
			sql.push_str(" LIMIT $limit");
			vars.insert("limit".to_string(), limit);
		}

		if let Some(start) = self.start {
			sql.push_str(" START $start");
			vars.insert("start".to_string(), start);
		}

		if let Some(timeout) = self.timeout {
			sql.push_str(" TIMEOUT $timeout");
			vars.insert("timeout".to_string(), timeout);
		}

		if let Some(version) = self.version {
			sql.push_str(" VERSION $version");
			vars.insert("version".to_string(), version);
		}

		(sql, vars)
	}
}

/// Builder methods for Select requests
impl Request<Select> {
	pub fn field(mut self, field: impl Into<String>) -> Self {
		self.inner.fields.push(field.into());
		self
	}

	pub fn fields<T, S>(mut self, fields: T) -> Self
	where
		T: Into<Vec<S>>,
		S: Into<String>,
	{
		let fields: Vec<S> = fields.into();
		self.inner.fields.extend(fields.into_iter().map(Into::into));
		self
	}

	pub fn limit(mut self, limit: u64) -> Self {
		self.inner.limit = Some(limit);
		self
	}

	pub fn start(mut self, start: u64) -> Self {
		self.inner.start = Some(start);
		self
	}

	pub fn timeout(mut self, timeout: Duration) -> Self {
		self.inner.timeout = Some(timeout);
		self
	}

	pub fn version(mut self, version: Datetime) -> Self {
		self.inner.version = Some(version);
		self
	}
}

// Execution methods
impl Request<Select> {

	/// Execute the select and return a typed [`ValueStream`].
	///
	/// # Type Parameters
	/// - `T`: The target type to convert values to. Defaults to [`Value`].
	///
	/// # Example
	/// ```ignore
	/// use futures::StreamExt;
	///
	/// let mut stream = db.select("user").stream::<User>().await?;
	/// while let Some(frame) = stream.next().await {
	///     if let Some(user) = frame.into_value() {
	///         println!("{user:?}");
	///     }
	/// }
	/// ```
	pub async fn stream<T: SurrealValue>(
		self,
	) -> Result<ValueStream<Pin<Box<dyn Stream<Item = QueryChunk> + Send>>, T>> {
		let (sql, vars) = self.inner.build();
		let stream = Request::new(&self, Query::new(sql)).bind(vars).stream().await?;
		Ok(stream.into_value_stream::<T>(0))
	}

	/// Execute the select and collect all results into a typed [`QueryResult`].
	///
	/// # Type Parameters
	/// - `T`: The target type to convert values to. Use type inference or specify explicitly.
	///
	/// # Example
	/// ```ignore
	/// // Type inference (recommended)
	/// let result: QueryResult<Vec<User>> = db.select("user").collect().await?;
	/// let users: Vec<User> = result.take()?;
	///
	/// // Explicit type parameter
	/// let result = db.select("user").collect::<Vec<User>>().await?;
	///
	/// // Default to Value
	/// let result = db.select("user").collect::<Value>().await?;
	///
	/// // Access stats
	/// let stats = result.stats();
	/// ```
	pub async fn collect<T: SurrealValue>(self) -> Result<QueryResult<T>> {
		let (sql, vars) = self.inner.build();
		let results = Request::new(&self, Query::new(sql)).bind(vars).collect().await?;
		let result = results
			.into_iter()
			.next()
			.ok_or_else(|| anyhow::anyhow!("No result returned from select query"))?;
		result.into_typed()
	}
}

impl Executable for Select {
	type Output = QueryResult;

	fn execute(req: Request<Self>) -> impl Future<Output = Result<Self::Output>> + Send {
		req.collect::<Value>()
	}
}

#[derive(Clone, SurrealValue)]
#[surreal(untagged)]
pub enum SelectSubject {
	Table(Table),
	RecordId(RecordId),
}

impl From<Table> for SelectSubject {
	fn from(table: Table) -> Self {
		Self::Table(table)
	}
}

impl From<RecordId> for SelectSubject {
	fn from(record_id: RecordId) -> Self {
		Self::RecordId(record_id)
	}
}
