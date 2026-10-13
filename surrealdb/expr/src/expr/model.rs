use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

pub const ARGUMENTS: &str = "The model expects 1 argument. The argument can be either a number, an object, or an array of numbers.";

pub fn get_model_path(ns: &str, db: &str, name: &str, version: &str, hash: &str) -> String {
	format!("ml/{ns}/{db}/{name}-{version}-{hash}.surml")
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct Model {
	pub name: Strand,
	pub version: Strand,
}

impl ToSql for Model {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		let stmt: crate::sql::model::Model = self.clone().into();
		stmt.fmt_sql(f, sql_fmt);
	}
}
