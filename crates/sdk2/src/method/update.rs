use surrealdb_types::{SurrealValue, Variables};

use crate::{method::{QueryExecutable, Request}, sql::{BuildSqlContext, Condition, ConditionBuilder, IntoCondition, IntoTimeout, Return, Subject, Timeout}};
use crate::sql::ReturnBuilder;

#[derive(Clone)]
pub struct Update {
	pub(crate) subject: Subject,
	pub(crate) cond: Condition,
	pub(crate) content: Option<surrealdb_types::Value>,
	pub(crate) timeout: Timeout,
	pub(crate) r#return: Return,
}

impl Update {
	pub fn new(subject: impl Into<Subject>) -> Self {
		Self {
			subject: subject.into(),
			cond: Condition::default(),
			content: None,
			timeout: Timeout::default(),
			r#return: Return::Before,
		}
	}
}

/// Builder methods for Update requests
impl Request<Update> {
	/// Sets the content/data to replace the record with.
	///
	/// # Example
	/// ```ignore
	/// db.update("person")
	///     .content(User { name: "Tobie", age: 31 })
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
	/// db.update("person").cond("age > 100").collect().await?;
	/// ```
	pub fn cond(mut self, condition: impl IntoCondition) -> Self {
		condition.build(&mut self.inner.cond);
		self
	}

	/// Add a WHERE clause using a closure-based condition builder.
	///
	/// # Example
	/// ```ignore
	/// db.update("person")
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

impl QueryExecutable for Update {
	fn query(self) -> (String, Variables) {
		let mut ctx = BuildSqlContext::default();

		ctx.push("UPDATE ");
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

		ctx.push(self.r#return);
		ctx.push(self.timeout);

		ctx.output()
	}
}
