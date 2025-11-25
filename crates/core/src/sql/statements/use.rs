use std::fmt;

use crate::sql::Expr;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct UseStatement {
	pub ns: Option<Expr>,
	pub db: Option<Expr>,
}

impl fmt::Display for UseStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str("USE")?;
		if let Some(ref ns) = self.ns {
			write!(f, " NS {ns}")?;
		}
		if let Some(ref db) = self.db {
			write!(f, " DB {db}")?;
		}
		Ok(())
	}
}

impl From<UseStatement> for crate::expr::statements::UseStatement {
	fn from(v: UseStatement) -> Self {
		crate::expr::statements::UseStatement {
			ns: v.ns.map(Into::into),
			db: v.db.map(Into::into),
		}
	}
}

impl From<crate::expr::statements::UseStatement> for UseStatement {
	fn from(v: crate::expr::statements::UseStatement) -> Self {
		UseStatement {
			ns: v.ns.map(Into::into),
			db: v.db.map(Into::into),
		}
	}
}
