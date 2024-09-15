use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

use crate::sql::escape::escape_ident;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
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
			let ns = escape_ident(ns);
			write!(f, " NS {ns}")?;
		}
		if let Some(ref db) = self.db {
			let db = escape_ident(db);
			write!(f, " DB {db}")?;
		}
		Ok(())
	}
}
