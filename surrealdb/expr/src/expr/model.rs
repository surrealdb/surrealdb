use common::fmt::EscapeKwFreeIdent;
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

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
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push_str("ml");
		for s in self.name.as_str().split("::") {
			f.push_str("::");
			write_sql!(f, fmt, "{}", EscapeKwFreeIdent(s));
		}

		write_sql!(f, fmt, "<{}>", self.version.as_str());
	}
}
