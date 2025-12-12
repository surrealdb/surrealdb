use anyhow::Result;
use surrealdb_types::{RecordId, SurrealValue, Table, Variables};

use crate::method::{Executable, Query, Request};

#[derive(Clone)]
pub struct Select {
	pub(crate) subject: SelectSubject,
	pub(crate) fields: Vec<String>,
	pub(crate) limit: Option<u64>,
	pub(crate) start: Option<u64>,
	// add cond, fetch, timeout, version
}

impl Select {
	pub fn new(subject: impl Into<SelectSubject>) -> Self {
		Self {
			subject: subject.into(),
			fields: vec![],
			limit: None,
			start: None,
		}
	}

	pub fn build(&self) -> (String, Variables) {
		let mut sql = String::new();
		let mut vars = Variables::new();

		sql.push_str("SELECT ");
		if self.fields.is_empty() {
			sql.push_str("*");
		} else {
			sql.push_str("type::fields($fields)");
			vars.insert("fields".to_string(), self.fields.clone());
		}

		sql.push_str(" FROM $subject");
		vars.insert("subject".to_string(), self.subject.clone());

		if let Some(limit) = self.limit {
			sql.push_str(" LIMIT $limit");
			vars.insert("limit".to_string(), limit);
		}

		if let Some(start) = self.start {
			sql.push_str(" START $start");
			vars.insert("start".to_string(), start);
		}

		(sql, vars)
	}
}

/// Builder methods for Query requests
impl Request<Select> {
	pub fn field(mut self, field: impl Into<String>) -> Self {
		self.inner.fields.push(field.into());
		self
	}

	pub fn fields<T, S>(mut self, fields: T) -> Self
	where
		T: Into<Vec<S>>,
		S: Into<String>,
	{
		let fields: Vec<S> = fields.into();
		self.inner.fields.extend(fields.into_iter().map(Into::into));
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

	pub async fn collect(self) -> Result<Vec<surrealdb_types::Value>> {
		let (sql, vars) = self.inner.build();
		Request::new(&self, Query::new(sql)).bind(vars).collect().await
	}
}

impl Executable for Select {
	type Output = Vec<surrealdb_types::Value>;

	fn execute(req: Request<Self>) -> impl Future<Output = Result<Self::Output>> + Send {
		req.collect()
	}
}

#[derive(Clone, SurrealValue)]
#[surreal(untagged)]
pub enum SelectSubject {
	Table(Table),
	RecordId(RecordId),
}

impl From<Table> for SelectSubject {
	fn from(table: Table) -> Self {
		Self::Table(table)
	}
}

impl From<RecordId> for SelectSubject {
	fn from(record_id: RecordId) -> Self {
		Self::RecordId(record_id)
	}
}
