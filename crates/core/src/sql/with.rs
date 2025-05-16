use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{Formatter, Result};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum With {
	NoIndex,
	Index(Vec<String>),
}

impl From<With> for crate::expr::With {
	fn from(v: With) -> Self {
		match v {
			With::NoIndex => Self::NoIndex,
			With::Index(i) => Self::Index(i),
		}
	}
}
impl From<crate::expr::With> for With {
	fn from(v: crate::expr::With) -> Self {
		match v {
			crate::expr::With::NoIndex => Self::NoIndex,
			crate::expr::With::Index(i) => Self::Index(i),
		}
	}
}

crate::sql::impl_display_from_sql!(With);

impl crate::sql::DisplaySql for With {
	fn fmt_sql(&self, f: &mut Formatter) -> Result {
		f.write_str("WITH")?;
		match self {
			With::NoIndex => f.write_str(" NOINDEX"),
			With::Index(i) => {
				f.write_str(" INDEX ")?;
				f.write_str(&i.join(","))
			}
		}
	}
}
