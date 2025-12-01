use std::fmt;

use crate::expr::Expr;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum UseStatement {
	Ns(Expr),
	Db(Expr),
	NsDb(Expr, Expr),
	Default,
}

impl fmt::Display for UseStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str("USE")?;

		match self {
			UseStatement::Ns(ns) => write!(f, " NS {ns}")?,
			UseStatement::Db(db) => write!(f, " DB {db}")?,
			UseStatement::NsDb(ns, db) => write!(f, " NS {ns} DB {db}")?,
			UseStatement::Default => write!(f, " DEFAULT")?,
		}
		Ok(())
	}
}
