use surrealdb_types::{SurrealValue, Variables};

use crate::{method::{QueryExecutable, Request}, sql::{BuildSqlContext, IntoTimeout, IntoVersion, Subject, Timeout, Version}};

#[derive(Clone)]
pub struct Insert {
	pub(crate) subject: Subject,
	pub(crate) content: Option<surrealdb_types::Value>,
	pub(crate) timeout: Timeout,
	pub(crate) version: Version,
}

impl Insert {
	pub fn new(subject: impl Into<Subject>) -> Self {
		Self {
			subject: subject.into(),
			content: None,
			timeout: Timeout::default(),
			version: Version::default(),
		}
	}
}

/// Builder methods for Insert requests
impl Request<Insert> {
	/// Sets the content/data for the record(s) to insert.
	///
	/// # Example
	/// ```ignore
	/// // Insert a single record
	/// db.insert("person")
	///     .content(User { name: "Tobie", age: 30 })
	///     .collect().await?;
	///
	/// // Insert multiple records
	/// db.insert("person")
	///     .content(vec![
	///         User { name: "Tobie", age: 30 },
	///         User { name: "Jaime", age: 28 },
	///     ])
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

impl QueryExecutable for Insert {
	fn query(self) -> (String, Variables) {
		let mut ctx = BuildSqlContext::default();

		let content = self.content;
		let is_array = content.as_ref().map(|v| v.is_array()).unwrap_or(false);

		if is_array {
			// For arrays, use INSERT INTO
			ctx.push("INSERT INTO ");
			ctx.push(self.subject);
			if let Some(content) = content {
				let var = ctx.var(content);
				ctx.push(" ");
				ctx.push(var);
			}
		} else {
			// For single objects or no content, use CREATE
			ctx.push("CREATE ");
			ctx.push(self.subject);
			if let Some(content) = content {
				let var = ctx.var(content);
				ctx.push(" CONTENT ");
				ctx.push(var);
			}
		}

		ctx.push(self.version);
		ctx.push(self.timeout);

		ctx.output()
	}
}
