use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::sql::Expr;

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum UseStatement {
	Ns(Expr),
	Db(Expr),
	NsDb(Expr, Expr),
	Default,
}

impl ToSql for UseStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push_str("USE");
		match self {
			UseStatement::Ns(ns) => write_sql!(f, fmt, " NS {ns}"),
			UseStatement::Db(db) => write_sql!(f, fmt, " DB {db}"),
			UseStatement::NsDb(ns, db) => {
				write_sql!(f, fmt, " NS {ns} DB {db}")
			}
			UseStatement::Default => {
				write_sql!(f, fmt, " DEFAULT")
			}
		}
	}
}

impl From<UseStatement> for crate::expr::statements::UseStatement {
	fn from(v: UseStatement) -> Self {
		match v {
			UseStatement::Ns(ns) => crate::expr::statements::UseStatement::Ns(ns.into()),
			UseStatement::Db(db) => crate::expr::statements::UseStatement::Db(db.into()),
			UseStatement::NsDb(ns, db) => {
				crate::expr::statements::UseStatement::NsDb(ns.into(), db.into())
			}
			UseStatement::Default => crate::expr::statements::UseStatement::Default,
		}
	}
}

impl From<crate::expr::statements::UseStatement> for UseStatement {
	fn from(v: crate::expr::statements::UseStatement) -> Self {
		match v {
			crate::expr::statements::UseStatement::Ns(ns) => UseStatement::Ns(ns.into()),
			crate::expr::statements::UseStatement::Db(db) => UseStatement::Db(db.into()),
			crate::expr::statements::UseStatement::NsDb(ns, db) => {
				UseStatement::NsDb(ns.into(), db.into())
			}
			crate::expr::statements::UseStatement::Default => UseStatement::Default,
		}
	}
}
