
use crate::sql::Ident;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
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

crate::sql::impl_display_from_sql!(Base);

impl crate::sql::DisplaySql for Base {
	fn fmt_sql(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Ns => f.write_str("NAMESPACE"),
			Self::Db => f.write_str("DATABASE"),
			// TODO(gguillemas): This variant is kept in 2.0.0 for backward compatibility. Drop in 3.0.0.
			Self::Sc(sc) => write!(f, "SCOPE {sc}"),
			Self::Root => f.write_str("ROOT"),
		}
	}
}


