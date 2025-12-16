use std::pin::Pin;

use anyhow::Result;
use futures::Stream;
use surrealdb_types::{QueryChunk, SurrealValue, Value, Variables};

use crate::method::{
	Executable,
	Query,
	Request,
};
use crate::sql::{BuildSqlContext, Condition, ConditionBuilder, Fields, IntoCondition, IntoFields, IntoTimeout, IntoVersion, Subject, Timeout, Version};
use crate::utils::{QueryResult, ValueStream};

#[derive(Clone)]
pub struct Select {
	pub(crate) subject: Subject,
	pub(crate) fields: Fields,
	pub(crate) limit: Option<u64>,
	pub(crate) start: Option<u64>,
	pub(crate) cond: Condition,
	pub(crate) fetch: Fields,
	pub(crate) timeout: Timeout,
	pub(crate) version: Version,
}

impl Select {
	pub fn new(subject: impl Into<Subject>) -> Self {
		Self {
			subject: subject.into(),
			fields: Fields::default(),
			limit: None,
			start: None,
			cond: Condition::default(),
			fetch: Fields::default(),
			timeout: Timeout::default(),
			version: Version::default(),
		}
	}

	pub fn build(self) -> (String, Variables) {
		let mut ctx = BuildSqlContext::default();

		ctx.push("SELECT ");
		ctx.push(self.fields);
		ctx.push(" FROM ");
		ctx.push(self.subject);

		if !self.cond.is_empty() {
			ctx.push(" WHERE ");
			ctx.push(self.cond);
		}

		if let Some(limit) = self.limit {
			ctx.push(format!(" LIMIT {limit}"));
		}

		if let Some(start) = self.start {
			ctx.push(format!(" START {start}"));
		}

		if !self.fetch.is_empty() {
			ctx.push(" FETCH ");
			ctx.push(self.fetch);
		}

		ctx.push(self.version);
		ctx.push(self.timeout);

		ctx.output()
	}
}

/// Builder methods for Select requests
impl Request<Select> {
	pub fn field(mut self, field: impl Into<String>) -> Self {
		self.inner.fields.0.push(field.into());
		self
	}

	pub fn fields<T: IntoFields>(mut self, fields: T) -> Self {
		fields.build(&mut self.inner.fields);
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

	pub fn timeout<T: IntoTimeout>(mut self, timeout: T) -> Self {
		timeout.build(&mut self.inner.timeout);
		self
	}

	pub fn version<T: IntoVersion>(mut self, version: T) -> Self {
		version.build(&mut self.inner.version);
		self
	}

	/// Add a WHERE clause using a raw SQL expression string.
	///
	/// # Example
	/// ```ignore
	/// db.select("user").cond("age > 18").collect().await?;
	/// ```
	pub fn cond(mut self, condition: impl IntoCondition) -> Self {
		condition.build(&mut self.inner.cond);
		self
	}

	/// Add a WHERE clause using a closure-based condition builder.
	///
	/// # Example
	/// ```ignore
	/// db.select("user")
	///     .where(|w| {
	///         w.field("age").gt(18)
	///          .and()
	///          .field("status").eq("active")
	///     })
	///     .collect().await?;
	/// ```
	pub fn r#where<F>(mut self, condition: F) -> Self
	where
		F: FnOnce(ConditionBuilder) -> ConditionBuilder,
	{
		let builder = ConditionBuilder::new();
		let builder = condition(builder);
		self.inner.cond.0.push(builder);
		self
	}

	/// Add a FETCH clause to eagerly load related records.
	///
	/// # Example
	/// ```ignore
	/// db.select("user").fetch("profile").collect().await?;
	/// db.select("user").fetch(["profile", "settings"]).collect().await?;
	/// ```
	pub fn fetch<T: IntoFields>(mut self, fields: T) -> Self {
		fields.build(&mut self.inner.fetch);
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
		let (inner, ctx) = self.split();
		let (sql, vars) = inner.build();
		let stream = ctx.into_request(Query::new(sql)).bind(vars).stream().await?;
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
		let (inner, ctx) = self.split();
		let (sql, vars) = inner.build();
		let results = ctx.into_request(Query::new(sql)).bind(vars).collect().await?;
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