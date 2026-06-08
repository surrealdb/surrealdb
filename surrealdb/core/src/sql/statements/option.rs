use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::fmt::EscapeKwFreeIdent;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct OptionStatement {
	pub name: Strand,
	pub what: bool,
}

impl OptionStatement {
	pub(crate) fn import() -> Self {
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
