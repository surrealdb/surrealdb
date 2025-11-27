use std::fmt;

use crate::expr::Expr;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct UseStatement {
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
		if self.ns.is_none() && self.db.is_none() {
			write!(f, " DEFAULTS")?;
		}
		Ok(())
	}
}
