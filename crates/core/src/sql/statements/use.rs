use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::fmt::EscapeKwFreeIdent;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum UseStatement {
	Ns(String),
	Db(String),
	NsDb(String, String),
}

impl ToSql for UseStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push_str("USE");
		match self {
			UseStatement::Ns(ns) => write_sql!(f, fmt, " NS {}", EscapeKwFreeIdent(ns)),
			UseStatement::Db(ns) => write_sql!(f, fmt, " DB {}", EscapeKwFreeIdent(ns)),
			UseStatement::NsDb(ns, db) => {
				write_sql!(f, fmt, " NS {} DB {}", EscapeKwFreeIdent(ns), EscapeKwFreeIdent(db))
			}
		}
	}
}

impl From<UseStatement> for crate::expr::statements::UseStatement {
	fn from(v: UseStatement) -> Self {
		match v {
			UseStatement::Ns(ns) => crate::expr::statements::UseStatement::Ns(ns),
			UseStatement::Db(db) => crate::expr::statements::UseStatement::Db(db),
			UseStatement::NsDb(ns, db) => crate::expr::statements::UseStatement::NsDb(ns, db),
		}
	}
}

impl From<crate::expr::statements::UseStatement> for UseStatement {
	fn from(v: crate::expr::statements::UseStatement) -> Self {
		match v {
			crate::expr::statements::UseStatement::Ns(ns) => UseStatement::Ns(ns),
			crate::expr::statements::UseStatement::Db(db) => UseStatement::Db(db),
			crate::expr::statements::UseStatement::NsDb(ns, db) => UseStatement::NsDb(ns, db),
		}
	}
}
