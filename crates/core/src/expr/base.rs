use std::fmt;

use crate::expr::Value;
use crate::expr::statements::info::InfoStructure;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Base {
	Root,
	Ns,
	Db,
}

impl Default for Base {
	fn default() -> Self {
		Self::Root
	}
}

impl fmt::Display for Base {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Ns => f.write_str("NAMESPACE"),
			Self::Db => f.write_str("DATABASE"),
			Self::Root => f.write_str("ROOT"),
		}
	}
}

impl InfoStructure for Base {
	fn structure(self) -> Value {
		self.to_string().into()
	}
}
