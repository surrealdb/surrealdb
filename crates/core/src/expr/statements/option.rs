use surrealdb_types::{SqlFormat, ToSql};

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) struct OptionStatement {
	pub name: String,
	pub what: bool,
}

impl ToSql for OptionStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::option::OptionStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
