use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::fmt::EscapeKwFreeIdent;

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct Model {
	pub name: String,
	pub version: String,
}

impl ToSql for Model {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push_str("ml");
		for s in self.name.split("::") {
			f.push_str("::");
			write_sql!(f, fmt, "{}", EscapeKwFreeIdent(s));
		}

		write_sql!(f, fmt, "<{}>", self.version);
	}
}

impl From<Model> for crate::expr::Model {
	fn from(v: Model) -> Self {
		Self {
			name: v.name,
			version: v.version,
		}
	}
}
impl From<crate::expr::Model> for Model {
	fn from(v: crate::expr::Model) -> Self {
		Self {
			name: v.name,
			version: v.version,
		}
	}
}
