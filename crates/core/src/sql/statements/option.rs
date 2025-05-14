use crate::sql::ident::Ident;

use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct OptionStatement {
	pub name: Ident,
	pub what: bool,
}

crate::sql::impl_display_from_sql!(OptionStatement);

impl crate::sql::DisplaySql for OptionStatement {
	fn fmt_sql(&self, f: &mut fmt::Formatter) -> fmt::Result {
		if self.what {
			write!(f, "OPTION {}", self.name)
		} else {
			write!(f, "OPTION {} = FALSE", self.name)
		}
	}
}
