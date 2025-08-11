use std::fmt;

use revision::revisioned;
use serde::{Deserialize, Serialize};

use crate::expr::statements::info::InfoStructure;
use crate::expr::{Ident, Value};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum Base {
	Root,
	Ns,
	Db,
	// TODO(gguillemas): This variant is kept in 2.0.0 for backward compatibility. Drop in 3.0.0.
	Sc(Ident),
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
			// TODO(gguillemas): This variant is kept in 2.0.0 for backward compatibility. Drop in
			// 3.0.0.
			Self::Sc(sc) => write!(f, "SCOPE {sc}"),
			Self::Root => f.write_str("ROOT"),
		}
	}
}

impl InfoStructure for Base {
	fn structure(self) -> Value {
		self.to_string().into()
	}
}
