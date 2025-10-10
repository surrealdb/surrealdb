use std::fmt;

use revision::revisioned;

use crate::expr::statements::info::InfoStructure;
use crate::val::Value;

#[revisioned(revision = 1)]
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

impl From<Base> for crate::expr::Base {
	fn from(v: Base) -> Self {
		match v {
			Base::Root => Self::Root,
			Base::Ns => Self::Ns,
			Base::Db => Self::Db,
		}
	}
}

impl From<crate::expr::Base> for Base {
	fn from(v: crate::expr::Base) -> Self {
		match v {
			crate::expr::Base::Root => Self::Root,
			crate::expr::Base::Ns => Self::Ns,
			crate::expr::Base::Db => Self::Db,
		}
	}
}
