use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::expr::{CoverStmts, Expr};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
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
			UseStatement::Ns(ns) => write_sql!(f, fmt, " NS {}", CoverStmts(ns)),
			UseStatement::Db(db) => write_sql!(f, fmt, " DB {}", CoverStmts(db)),
			UseStatement::NsDb(ns, db) => {
				write_sql!(f, fmt, " NS {} DB {}", CoverStmts(ns), CoverStmts(db))
			}
			UseStatement::Default => {
				write_sql!(f, fmt, " DEFAULT")
			}
		}
	}
}
