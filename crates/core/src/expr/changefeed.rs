use crate::expr::statements::info::InfoStructure;
use crate::val::{Duration, Value};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};
use std::{str, time};

#[revisioned(revision = 2)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[non_exhaustive]
pub struct ChangeFeed {
	pub expiry: time::Duration,
	#[revision(start = 2)]
	pub store_diff: bool,
}
impl Display for ChangeFeed {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "CHANGEFEED {}", Duration(self.expiry))?;
		if self.store_diff {
			write!(f, " INCLUDE ORIGINAL")?;
		};
		Ok(())
	}
}

impl Default for ChangeFeed {
	fn default() -> Self {
		Self {
			expiry: time::Duration::from_secs(0),
			store_diff: false,
		}
	}
}

impl InfoStructure for ChangeFeed {
	fn structure(self) -> Value {
		Value::from(map! {
			"expiry".to_string() => Duration(self.expiry).structure(),
			"original".to_string() => self.store_diff.into(),
		})
	}
}
