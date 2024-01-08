use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 1)]
pub struct UseStatement {
	pub ns: Option<String>,
	pub db: Option<String>,
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
