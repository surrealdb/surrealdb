use std::fmt;

use crate::sql::{Base, Expr, Ident};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum InfoStatement {
	// revision discriminant override accounting for previous behavior when adding variants and
	// removing not at the end of the enum definition.
	Root(bool),
	Ns(bool),
	Db(bool, Option<Expr>),
	Tb(Ident, bool, Option<Expr>),
	User(Ident, Option<Base>, bool),
	Index(Ident, Ident, bool),
}

impl fmt::Display for InfoStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Root(false) => f.write_str("INFO FOR ROOT"),
			Self::Root(true) => f.write_str("INFO FOR ROOT STRUCTURE"),
			Self::Ns(false) => f.write_str("INFO FOR NAMESPACE"),
			Self::Ns(true) => f.write_str("INFO FOR NAMESPACE STRUCTURE"),
			Self::Db(false, v) => match v {
				Some(v) => write!(f, "INFO FOR DATABASE VERSION {v}"),
				None => f.write_str("INFO FOR DATABASE"),
			},
			Self::Db(true, v) => match v {
				Some(v) => write!(f, "INFO FOR DATABASE VERSION {v} STRUCTURE"),
				None => f.write_str("INFO FOR DATABASE STRUCTURE"),
			},
			Self::Tb(t, false, v) => match v {
				Some(v) => write!(f, "INFO FOR TABLE {t} VERSION {v}"),
				None => write!(f, "INFO FOR TABLE {t}"),
			},

			Self::Tb(t, true, v) => match v {
				Some(v) => write!(f, "INFO FOR TABLE {t} VERSION {v} STRUCTURE"),
				None => write!(f, "INFO FOR TABLE {t} STRUCTURE"),
			},
			Self::User(u, b, false) => match b {
				Some(b) => write!(f, "INFO FOR USER {u} ON {b}"),
				None => write!(f, "INFO FOR USER {u}"),
			},
			Self::User(u, b, true) => match b {
				Some(b) => write!(f, "INFO FOR USER {u} ON {b} STRUCTURE"),
				None => write!(f, "INFO FOR USER {u} STRUCTURE"),
			},
			Self::Index(i, t, false) => write!(f, "INFO FOR INDEX {i} ON {t}"),
			Self::Index(i, t, true) => write!(f, "INFO FOR INDEX {i} ON {t} STRUCTURE"),
		}
	}
}

impl From<InfoStatement> for crate::expr::statements::InfoStatement {
	fn from(v: InfoStatement) -> Self {
		match v {
			InfoStatement::Root(v) => Self::Root(v),
			InfoStatement::Ns(v) => Self::Ns(v),
			InfoStatement::Db(v, ver) => Self::Db(v, ver.map(From::from)),
			InfoStatement::Tb(t, v, ver) => Self::Tb(t.into(), v, ver.map(From::from)),
			InfoStatement::User(u, b, v) => Self::User(u.into(), b.map(Into::into), v),
			InfoStatement::Index(i, t, v) => Self::Index(i.into(), t.into(), v),
		}
	}
}

impl From<crate::expr::statements::InfoStatement> for InfoStatement {
	fn from(v: crate::expr::statements::InfoStatement) -> Self {
		match v {
			crate::expr::statements::InfoStatement::Root(v) => Self::Root(v),
			crate::expr::statements::InfoStatement::Ns(v) => Self::Ns(v),
			crate::expr::statements::InfoStatement::Db(v, ver) => Self::Db(v, ver.map(From::from)),
			crate::expr::statements::InfoStatement::Tb(t, v, ver) => {
				Self::Tb(t.into(), v, ver.map(From::from))
			}
			crate::expr::statements::InfoStatement::User(u, b, v) => {
				Self::User(u.into(), b.map(Into::into), v)
			}
			crate::expr::statements::InfoStatement::Index(i, t, v) => {
				Self::Index(i.into(), t.into(), v)
			}
		}
	}
}
