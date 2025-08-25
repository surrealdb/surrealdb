use std::fmt;

use crate::expr::Ident;
use crate::expr::escape::EscapeIdent;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct UseStatement {
	pub ns: Option<Ident>,
	pub db: Option<Ident>,
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
