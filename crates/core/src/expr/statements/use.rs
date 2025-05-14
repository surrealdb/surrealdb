use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

use crate::expr::escape::EscapeIdent;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct UseStatement {
	pub ns: Option<String>,
	pub db: Option<String>,
}

crate::expr::impl_display_from_sql!(UseStatement);

impl crate::expr::DisplaySql for UseStatement {
	fn fmt_sql(&self, f: &mut fmt::Formatter) -> fmt::Result {
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
