use common::fmt::EscapeKwFreeIdent;
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct OptionStatement {
	pub name: Strand,
	pub what: bool,
}

impl OptionStatement {
	pub fn import() -> Self {
		Self {
			name: Strand::new_static("IMPORT"),
			what: true,
		}
	}
}

impl ToSql for OptionStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		if self.what {
			write_sql!(f, fmt, "OPTION {}", EscapeKwFreeIdent(self.name.as_str()))
		} else {
			write_sql!(f, fmt, "OPTION {} = FALSE", EscapeKwFreeIdent(self.name.as_str()))
		}
	}
}
