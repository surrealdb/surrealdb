use surrealdb_types::{SqlFormat, SurrealValue, Table, ToSql, Variables};

use crate::{method::{QueryExecutable, Request}, sql::{BuildSqlContext, IntoTimeout, IntoVersion, Subject, Timeout, Version}};

#[derive(Clone)]
pub struct Relate {
	pub(crate) from: Subject,
	pub(crate) through: Table,
	pub(crate) to: Subject,
	pub(crate) content: Option<surrealdb_types::Value>,
	pub(crate) timeout: Timeout,
	pub(crate) version: Version,
}

impl Relate {
	pub fn new(from: impl Into<Subject>, through: impl Into<Table>, to: impl Into<Subject>) -> Self {
		Self {
			from: from.into(),
			through: through.into(),
			to: to.into(),
			content: None,
			timeout: Timeout::default(),
			version: Version::default(),
		}
	}
}

/// Builder methods for Relate requests
impl Request<Relate> {
	/// Sets the content/data for the relation edge.
	///
	/// # Example
	/// ```ignore
	/// db.relate("person:tobie", "founded", "company:surrealdb")
	///     .content(RelationData { since: "2021" })
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

impl QueryExecutable for Relate {
	fn query(self) -> (String, Variables) {
		let mut ctx = BuildSqlContext::default();

		ctx.push("RELATE ");
		
		// Format FROM - need to handle parentheses for complex expressions
		let from_sql = {
			let mut sql = String::new();
			match &self.from {
				Subject::Table(table) => {
					table.fmt_sql(&mut sql, SqlFormat::SingleLine);
				}
				Subject::RecordId(record_id) => {
					record_id.fmt_sql(&mut sql, SqlFormat::SingleLine);
				}
			}
			sql
		};
		ctx.push(from_sql);
		
		ctx.push(" -> ");
		
		// Format THROUGH (table)
		let through_sql = {
			let mut sql = String::new();
			self.through.fmt_sql(&mut sql, SqlFormat::SingleLine);
			sql
		};
		ctx.push(through_sql);
		
		ctx.push(" -> ");
		
		// Format TO
		let to_sql = {
			let mut sql = String::new();
			match &self.to {
				Subject::Table(table) => {
					table.fmt_sql(&mut sql, SqlFormat::SingleLine);
				}
				Subject::RecordId(record_id) => {
					record_id.fmt_sql(&mut sql, SqlFormat::SingleLine);
				}
			}
			sql
		};
		ctx.push(to_sql);

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
