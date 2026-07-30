use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct RemoveFunctionStatement {
	pub name: Strand,
	pub if_exists: bool,
}

impl ToSql for RemoveFunctionStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::remove::RemoveFunctionStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
