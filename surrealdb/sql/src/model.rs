use common::fmt::EscapeKwFreeIdent;
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
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
