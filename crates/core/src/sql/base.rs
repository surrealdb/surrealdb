use std::fmt;

use surrealdb_types::{SqlFormat, ToSql, write_sql};

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Base {
	#[default]
	Root,
	Ns,
	Db,
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

impl ToSql for Base {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		match self {
			Self::Ns => write_sql!(f, sql_fmt, "NAMESPACE"),
			Self::Db => write_sql!(f, sql_fmt, "DATABASE"),
			Self::Root => write_sql!(f, sql_fmt, "ROOT"),
		}
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
