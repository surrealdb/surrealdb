use std::fmt::{self, Display, Formatter};
use std::time;

use crate::val::Duration;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct ChangeFeed {
	pub expiry: time::Duration,
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
