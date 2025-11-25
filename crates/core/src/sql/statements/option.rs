use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::fmt::EscapeIdent;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct OptionStatement {
	pub name: String,
	pub what: bool,
}

impl OptionStatement {
	pub(crate) fn import() -> Self {
		Self {
			name: "IMPORT".to_string(),
			what: true,
		}
	}
}

impl ToSql for OptionStatement {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		if self.what {
			write_sql!(f, sql_fmt, "OPTION {}", EscapeIdent(&self.name))
		} else {
			write_sql!(f, sql_fmt, "OPTION {} = FALSE", EscapeIdent(&self.name))
		}
	}
}

impl From<OptionStatement> for crate::expr::statements::OptionStatement {
	fn from(v: OptionStatement) -> Self {
		crate::expr::statements::OptionStatement {
			name: v.name,
			what: v.what,
		}
	}
}

impl From<crate::expr::statements::OptionStatement> for OptionStatement {
	fn from(v: crate::expr::statements::OptionStatement) -> Self {
		OptionStatement {
			name: v.name,
			what: v.what,
		}
	}
}
