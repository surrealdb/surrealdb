use std::fmt;

use crate::sql::Expr;

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum UseStatement {
	Ns(Expr),
	Db(Expr),
	NsDb(Expr, Expr),
	Defaults,
}

impl fmt::Display for UseStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str("USE")?;
		match self {
			UseStatement::Ns(ns) => write!(f, " NS {ns}")?,
			UseStatement::Db(db) => write!(f, " DB {db}")?,
			UseStatement::NsDb(ns, db) => write!(f, " NS {ns} DB {db}")?,
			UseStatement::Defaults => write!(f, " DEFAULTS")?,
		}
		Ok(())
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
			UseStatement::Defaults => crate::expr::statements::UseStatement::Defaults,
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
			crate::expr::statements::UseStatement::Defaults => UseStatement::Defaults,
		}
	}
}
