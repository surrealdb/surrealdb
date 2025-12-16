use crate::sql::{BuildSql, BuildSqlContext};
use surrealdb_types::{RecordId, SqlFormat, SurrealValue, Table, ToSql};

#[derive(Clone, SurrealValue)]
#[surreal(untagged)]
pub enum Subject {
	Table(Table),
	RecordId(RecordId),
}

impl From<&str> for Subject {
	fn from(table: &str) -> Self {
		Self::Table(Table::new(table))
	}
}

impl From<String> for Subject {
	fn from(table: String) -> Self {
		Self::Table(Table::new(table))
	}
}

impl From<Table> for Subject {
	fn from(table: Table) -> Self {
		Self::Table(table)
	}
}

impl From<RecordId> for Subject {
	fn from(record_id: RecordId) -> Self {
		Self::RecordId(record_id)
	}
}

impl BuildSql for Subject {
	fn build(self, ctx: &mut BuildSqlContext) {
		match self {
			Subject::Table(table) => {
				let mut sql = String::new();
				table.fmt_sql(&mut sql, SqlFormat::SingleLine);
				ctx.push(sql);
			}
			Subject::RecordId(record_id) => {
				let mut sql = String::new();
				record_id.fmt_sql(&mut sql, SqlFormat::SingleLine);
				ctx.push(sql);
			}
		}
	}
}