use surrealdb_types::{SurrealValue, Variables};

use crate::{method::{QueryExecutable, Request}, sql::{BuildSqlContext, IntoTimeout, Return, ReturnBuilder, Subject, Timeout}};

#[derive(Clone)]
pub struct Create {
	pub(crate) subject: Subject,
	pub(crate) content: Option<surrealdb_types::Value>,
	pub(crate) timeout: Timeout,
	pub(crate) only: bool,
	pub(crate) r#return: Return,
}

impl Create {
	pub fn new(subject: impl Into<Subject>) -> Self {
		Self {
			subject: subject.into(),
			content: None,
			timeout: Timeout::default(),
			only: false,
			r#return: Return::Before,
		}
	}
}

/// Builder methods for Create requests
impl Request<Create> {
	pub fn only(mut self) -> Self {
		self.inner.only = true;
		self
	}

	/// Sets the content/data for the record to create.
	///
	/// # Example
	/// ```ignore
	/// db.create("person")
	///     .content(User { name: "Tobie", age: 30 })
	///     .collect().await?;
	/// ```
	pub fn content<T: SurrealValue>(mut self, data: T) -> Self {
		self.inner.content = Some(data.into_value());
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

impl QueryExecutable for Create {
	fn query(self) -> (String, Variables) {
		let mut ctx = BuildSqlContext::default();

		ctx.push("CREATE ");
		if self.only {
			ctx.push("ONLY ");
		}
		ctx.push(self.subject);

		if let Some(content) = self.content {
			let var = ctx.var(content);
			ctx.push(" CONTENT ");
			ctx.push(var);
		}

		ctx.push(self.r#return);
		ctx.push(self.timeout);

		ctx.output()
	}
}
