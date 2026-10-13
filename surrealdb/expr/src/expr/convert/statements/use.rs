//! `sql` -> `expr` conversions for [`crate::sql::statements::use`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::r#use::*;

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
