use surrealdb_types::Variables;

use crate::{method::{QueryExecutable, Request}, sql::{BuildSqlContext, Condition, ConditionBuilder, Fields, IntoCondition, IntoFields, IntoTimeout, IntoVersion, Subject, Timeout, Version}};

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

impl QueryExecutable for Select {
	fn query(self) -> (String, Variables) {
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