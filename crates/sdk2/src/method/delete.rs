use surrealdb_types::Variables;

use crate::{method::{QueryExecutable, Request}, sql::{BuildSqlContext, Condition, ConditionBuilder, IntoCondition, IntoTimeout, IntoVersion, Subject, Timeout, Version}};

#[derive(Clone)]
pub struct Delete {
	pub(crate) subject: Subject,
	pub(crate) cond: Condition,
	pub(crate) timeout: Timeout,
	pub(crate) version: Version,
}

impl Delete {
	pub fn new(subject: impl Into<Subject>) -> Self {
		Self {
			subject: subject.into(),
			cond: Condition::default(),
			timeout: Timeout::default(),
			version: Version::default(),
		}
	}
}

/// Builder methods for Delete requests
impl Request<Delete> {
	/// Add a WHERE clause using a raw SQL expression string.
	///
	/// # Example
	/// ```ignore
	/// db.delete("person").cond("age > 100").collect().await?;
	/// ```
	pub fn cond(mut self, condition: impl IntoCondition) -> Self {
		condition.build(&mut self.inner.cond);
		self
	}

	/// Add a WHERE clause using a closure-based condition builder.
	///
	/// # Example
	/// ```ignore
	/// db.delete("person")
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

impl QueryExecutable for Delete {
	fn query(self) -> (String, Variables) {
		let mut ctx = BuildSqlContext::default();

		// Check if subject is a specific record ID or a table
		match &self.subject {
			crate::sql::Subject::RecordId(_) => {
				// For specific record IDs, use "DELETE record_id"
				ctx.push("DELETE ");
				ctx.push(self.subject);
			}
			crate::sql::Subject::Table(_) => {
				// For tables, use "DELETE FROM table"
				ctx.push("DELETE FROM ");
				ctx.push(self.subject);
				
				if !self.cond.is_empty() {
					ctx.push(" WHERE ");
					ctx.push(self.cond);
				}
			}
		}

		ctx.push(" RETURN BEFORE");

		ctx.push(self.version);
		ctx.push(self.timeout);

		ctx.output()
	}
}
