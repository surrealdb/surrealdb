use std::fmt;

use crate::fmt::EscapeIdent;

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct OptionStatement {
	pub name: String,
	pub what: bool,
}

impl fmt::Display for OptionStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		if self.what {
			write!(f, "OPTION {}", EscapeIdent(&self.name))
		} else {
			write!(f, "OPTION {} = FALSE", EscapeIdent(&self.name))
		}
	}
}
