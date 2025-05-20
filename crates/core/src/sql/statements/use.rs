use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

use crate::sql::escape::EscapeIdent;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct UseStatement {
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
