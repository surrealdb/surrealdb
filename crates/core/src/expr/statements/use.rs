use std::fmt;

use crate::fmt::EscapeKwFreeIdent;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum UseStatement {
	Ns(String),
	Db(String),
	NsDb(String, String),
}

impl fmt::Display for UseStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str("USE")?;
		match self {
			UseStatement::Ns(ns) => write!(f, " NS {}", EscapeKwFreeIdent(ns))?,
			UseStatement::Db(ns) => write!(f, " DB {}", EscapeKwFreeIdent(ns))?,
			UseStatement::NsDb(ns, db) => {
				write!(f, " NS {} DB {}", EscapeKwFreeIdent(ns), EscapeKwFreeIdent(db))?
			}
		}
		Ok(())
	}
}
