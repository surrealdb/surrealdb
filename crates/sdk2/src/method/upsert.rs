use surrealdb_types::{SurrealValue, Variables};

use crate::{method::{QueryExecutable, Request}, sql::{BuildSqlContext, Condition, ConditionBuilder, IntoCondition, IntoTimeout, IntoVersion, Subject, Timeout, Version}};

#[derive(Clone)]
pub struct Upsert {
	pub(crate) subject: Subject,
	pub(crate) cond: Condition,
	pub(crate) content: Option<surrealdb_types::Value>,
	pub(crate) timeout: Timeout,
	pub(crate) version: Version,
}

impl Upsert {
	pub fn new(subject: impl Into<Subject>) -> Self {
		Self {
			subject: subject.into(),
			cond: Condition::default(),
			content: None,
			timeout: Timeout::default(),
			version: Version::default(),
		}
	}
}

/// Builder methods for Upsert requests
impl Request<Upsert> {
	/// Sets the content/data for the record to upsert (create if not exists, update if exists).
	///
	/// # Example
	/// ```ignore
	/// db.upsert("person")
	///     .content(User { name: "Tobie", age: 30 })
	///     .collect().await?;
	/// ```
	pub fn content<T: SurrealValue>(mut self, data: T) -> Self {
		self.inner.content = Some(data.into_value());
		self
	}

	/// Add a WHERE clause using a raw SQL expression string.
	///
	/// # Example
	/// ```ignore
	/// db.upsert("person").cond("age > 100").collect().await?;
	/// ```
	pub fn cond(mut self, condition: impl IntoCondition) -> Self {
		condition.build(&mut self.inner.cond);
		self
	}

	/// Add a WHERE clause using a closure-based condition builder.
	///
	/// # Example
	/// ```ignore
	/// db.upsert("person")
	///     .where(|w| w.field("age").gt(100))
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

	pub fn timeout<T: IntoTimeout>(mut self, timeout: T) -> Self {
		timeout.build(&mut self.inner.timeout);
		self
	}

	pub fn version<T: IntoVersion>(mut self, version: T) -> Self {
		version.build(&mut self.inner.version);
		self
	}
}

impl QueryExecutable for Upsert {
	fn query(self) -> (String, Variables) {
		let mut ctx = BuildSqlContext::default();

		ctx.push("UPSERT ");
		ctx.push(self.subject);

		if let Some(content) = self.content {
			let var = ctx.var(content);
			ctx.push(" CONTENT ");
			ctx.push(var);
		}

		if !self.cond.is_empty() {
			ctx.push(" WHERE ");
			ctx.push(self.cond);
		}

		ctx.push(self.version);
		ctx.push(self.timeout);

		ctx.output()
	}
}
