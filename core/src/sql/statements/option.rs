use crate::sql::ident::Ident;
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[revisioned(revision = 1)]
#[non_exhaustive]
pub struct OptionStatement {
	pub name: Ident,
	pub what: bool,
}

impl fmt::Display for OptionStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		if self.what {
			write!(f, "OPTION {}", self.name)
		} else {
			write!(f, "OPTION {} = FALSE", self.name)
		}
	}
}
