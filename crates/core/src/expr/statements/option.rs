use std::fmt;

use revision::revisioned;
use serde::{Deserialize, Serialize};

use crate::expr::ident::Ident;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Hash)]
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
