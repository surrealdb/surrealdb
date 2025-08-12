use std::fmt::{Display, Formatter, Result};

use revision::revisioned;
use serde::{Deserialize, Serialize};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum With {
	NoIndex,
	Index(Vec<String>),
}

impl Display for With {
	fn fmt(&self, f: &mut Formatter) -> Result {
		f.write_str("WITH")?;
		match self {
			With::NoIndex => f.write_str(" NOINDEX"),
			With::Index(i) => {
				f.write_str(" INDEX ")?;
				f.write_str(&i.join(","))
			}
		}
	}
}
