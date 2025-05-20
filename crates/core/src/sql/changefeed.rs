use crate::sql::SqlValue;
use crate::sql::duration::Duration;

use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};
use std::str;
use std::time;

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

impl From<ChangeFeed> for crate::expr::ChangeFeed {
	fn from(v: ChangeFeed) -> Self {
		crate::expr::ChangeFeed {
			expiry: v.expiry,
			store_diff: v.store_diff,
		}
	}
}

impl From<crate::expr::ChangeFeed> for ChangeFeed {
	fn from(v: crate::expr::ChangeFeed) -> Self {
		ChangeFeed {
			expiry: v.expiry,
			store_diff: v.store_diff,
		}
	}
}
