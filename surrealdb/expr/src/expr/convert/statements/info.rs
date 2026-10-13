//! `sql` -> `expr` conversions for [`crate::sql::statements::info`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::info::*;

impl From<InfoStatement> for crate::expr::statements::InfoStatement {
	fn from(v: InfoStatement) -> Self {
		match v {
			InfoStatement::Root(v, ver) => Self::Root(v, ver.map(From::from)),
			InfoStatement::Ns(v, ver) => Self::Ns(v, ver.map(From::from)),
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
			crate::expr::statements::InfoStatement::Root(v, ver) => {
				Self::Root(v, ver.map(From::from))
			}
			crate::expr::statements::InfoStatement::Ns(v, ver) => Self::Ns(v, ver.map(From::from)),
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
