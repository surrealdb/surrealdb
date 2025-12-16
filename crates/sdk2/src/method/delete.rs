use surrealdb_types::Variables;

use crate::{method::{QueryExecutable, Request}, sql::{BuildSqlContext, Condition, ConditionBuilder, IntoCondition, IntoTimeout, Return, ReturnBuilder, Subject, Timeout}};

#[derive(Clone)]
pub struct Delete {
	pub(crate) subject: Subject,
	pub(crate) cond: Condition,
	pub(crate) timeout: Timeout,
	pub(crate) only: bool,
	pub(crate) r#return: Return,
}

impl Delete {
	pub fn new(subject: impl Into<Subject>) -> Self {
		Self {
			subject: subject.into(),
			cond: Condition::default(),
			timeout: Timeout::default(),
			only: false,
			r#return: Return::Before,
		}
	}
}

/// Builder methods for Delete requests
impl Request<Delete> {
	pub fn only(mut self) -> Self {
		self.inner.only = true;
		self
	}

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

	pub fn r#return<F>(mut self, r#return: F) -> Self
	where
		F: FnOnce(ReturnBuilder) -> Return,
	{
		let builder = ReturnBuilder::new();
		let r#return = r#return(builder);
		self.inner.r#return = r#return;
		self
	}
}

impl QueryExecutable for Delete {
	fn query(self) -> (String, Variables) {
		let mut ctx = BuildSqlContext::default();

		ctx.push("DELETE ");
		if self.only {
			ctx.push("ONLY ");
		}
		ctx.push(self.subject);

		if !self.cond.is_empty() {
			ctx.push(" WHERE ");
			ctx.push(self.cond);
		}

		ctx.push(self.r#return);
		ctx.push(self.timeout);

		ctx.output()
	}
}
