use std::fmt;

use crate::fmt::EscapeKwFreeIdent;

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) struct OptionStatement {
	pub name: String,
	pub what: bool,
}

impl fmt::Display for OptionStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		if self.what {
			write!(f, "OPTION {}", EscapeKwFreeIdent(&self.name))
		} else {
			write!(f, "OPTION {} = FALSE", EscapeKwFreeIdent(&self.name))
		}
	}
}
