use std::fmt;

use crate::sql::Ident;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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

impl From<Base> for crate::expr::Base {
	fn from(v: Base) -> Self {
		match v {
			Base::Root => Self::Root,
			Base::Ns => Self::Ns,
			Base::Db => Self::Db,
			Base::Sc(sc) => Self::Sc(sc.into()),
		}
	}
}

impl From<crate::expr::Base> for Base {
	fn from(v: crate::expr::Base) -> Self {
		match v {
			crate::expr::Base::Root => Self::Root,
			crate::expr::Base::Ns => Self::Ns,
			crate::expr::Base::Db => Self::Db,
			crate::expr::Base::Sc(sc) => Self::Sc(sc.into()),
		}
	}
}
