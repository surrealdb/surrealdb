use surrealdb_types::{SurrealValue, Variables};

use crate::{method::{QueryExecutable, Request}, sql::{BuildSqlContext, IntoTimeout, IntoVersion, Subject, Timeout, Version}};

#[derive(Clone)]
pub struct Create {
	pub(crate) subject: Subject,
	pub(crate) content: Option<surrealdb_types::Value>,
	pub(crate) timeout: Timeout,
	pub(crate) version: Version,
}

impl Create {
	pub fn new(subject: impl Into<Subject>) -> Self {
		Self {
			subject: subject.into(),
			content: None,
			timeout: Timeout::default(),
			version: Version::default(),
		}
	}
}

/// Builder methods for Create requests
impl Request<Create> {
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

	pub fn version<T: IntoVersion>(mut self, version: T) -> Self {
		version.build(&mut self.inner.version);
		self
	}
}

impl QueryExecutable for Create {
	fn query(self) -> (String, Variables) {
		let mut ctx = BuildSqlContext::default();

		ctx.push("CREATE ");
		ctx.push(self.subject);

		if let Some(content) = self.content {
			let var = ctx.var(content);
			ctx.push(" CONTENT ");
			ctx.push(var);
		}

		ctx.push(self.version);
		ctx.push(self.timeout);

		ctx.output()
	}
}
