use std::fmt;

use crate::fmt::EscapeIdent;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct UseStatement {
	pub ns: Option<String>,
	pub db: Option<String>,
}

impl fmt::Display for UseStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str("USE")?;
		if let Some(ref ns) = self.ns {
			let ns = EscapeIdent(ns);
			write!(f, " NS {ns}")?;
		}
		if let Some(ref db) = self.db {
			let db = EscapeIdent(db);
			write!(f, " DB {db}")?;
		}
		Ok(())
	}
}
