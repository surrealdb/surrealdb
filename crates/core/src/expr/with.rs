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

crate::expr::impl_display_from_sql!(With);

impl crate::expr::DisplaySql for With {
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
