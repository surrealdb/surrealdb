use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::sql::{Base, Expr};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum InfoStatement {
	// revision discriminant override accounting for previous behavior when adding variants and
	// removing not at the end of the enum definition.
	Root(bool),
	Ns(bool),
	Db(bool, Option<Expr>),
	Tb(Expr, bool, Option<Expr>),
	User(Expr, Option<Base>, bool),
	Index(Expr, Expr, bool),
}

impl ToSql for InfoStatement {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		match self {
			Self::Root(false) => f.push_str("INFO FOR ROOT"),
			Self::Root(true) => f.push_str("INFO FOR ROOT STRUCTURE"),
			Self::Ns(false) => f.push_str("INFO FOR NAMESPACE"),
			Self::Ns(true) => f.push_str("INFO FOR NAMESPACE STRUCTURE"),
			Self::Db(false, v) => match v {
				Some(v) => write_sql!(f, sql_fmt, "INFO FOR DATABASE VERSION {v}"),
				None => f.push_str("INFO FOR DATABASE"),
			},
			Self::Db(true, v) => match v {
				Some(v) => write_sql!(f, sql_fmt, "INFO FOR DATABASE VERSION {v} STRUCTURE"),
				None => f.push_str("INFO FOR DATABASE STRUCTURE"),
			},
			Self::Tb(t, false, v) => match v {
				Some(v) => write_sql!(f, sql_fmt, "INFO FOR TABLE {} VERSION {v}", t),
				None => write_sql!(f, sql_fmt, "INFO FOR TABLE {}", t),
			},
			Self::Tb(t, true, v) => match v {
				Some(v) => write_sql!(f, sql_fmt, "INFO FOR TABLE {} VERSION {v} STRUCTURE", t),
				None => write_sql!(f, sql_fmt, "INFO FOR TABLE {} STRUCTURE", t),
			},
			Self::User(u, b, false) => match b {
				Some(b) => write_sql!(f, sql_fmt, "INFO FOR USER {} ON {b}", u),
				None => write_sql!(f, sql_fmt, "INFO FOR USER {}", u),
			},
			Self::User(u, b, true) => match b {
				Some(b) => write_sql!(f, sql_fmt, "INFO FOR USER {} ON {b} STRUCTURE", u),
				None => write_sql!(f, sql_fmt, "INFO FOR USER {} STRUCTURE", u),
			},
			Self::Index(i, t, false) => {
				write_sql!(f, sql_fmt, "INFO FOR INDEX {} ON {}", i, t)
			}
			Self::Index(i, t, true) => {
				write_sql!(f, sql_fmt, "INFO FOR INDEX {} ON {} STRUCTURE", i, t)
			}
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
