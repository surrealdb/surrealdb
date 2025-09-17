use std::fmt;

use crate::fmt::EscapeIdent;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct UseStatement {
	pub ns: Option<String>,
	pub db: Option<String>,
}

impl fmt::Display for UseStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str("USE")?;
		if let Some(ref ns) = self.ns {
			write!(f, " NS {}", EscapeIdent(ns))?;
		}
		if let Some(ref db) = self.db {
			write!(f, " DB {}", EscapeIdent(db))?;
		}
		Ok(())
	}
}

impl From<UseStatement> for crate::expr::statements::UseStatement {
	fn from(v: UseStatement) -> Self {
		crate::expr::statements::UseStatement {
			ns: v.ns,
			db: v.db,
		}
	}
}

impl From<crate::expr::statements::UseStatement> for UseStatement {
	fn from(v: crate::expr::statements::UseStatement) -> Self {
		UseStatement {
			ns: v.ns,
			db: v.db,
		}
	}
}
