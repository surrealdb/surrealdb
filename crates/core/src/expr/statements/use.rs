use surrealdb_types::{SqlFormat, ToSql};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum UseStatement {
	Ns(String),
	Db(String),
	NsDb(String, String),
}

impl ToSql for UseStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::r#use::UseStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
