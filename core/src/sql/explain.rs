use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[revisioned(revision = 1)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Explain(pub bool);

impl fmt::Display for Explain {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str("EXPLAIN")?;
		if self.0 {
			f.write_str(" FULL")?;
		}
		Ok(())
	}
}
