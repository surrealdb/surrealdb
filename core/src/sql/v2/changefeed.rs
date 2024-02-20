use crate::sql::duration::Duration;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};
use std::str;
use std::time;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[revisioned(revision = 2)]
pub struct ChangeFeed {
	pub expiry: time::Duration,
	#[revisioned(start = 2)]
	pub store_original: bool,
}

impl Display for ChangeFeed {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		let diff = match self.store_original {
			true => "STORE ORIGINAL ",
			false => "",
		};
		write!(f, "CHANGEFEED {}{}", Duration(self.expiry), diff)?;
		Ok(())
	}
}

impl Default for ChangeFeed {
	fn default() -> Self {
		Self {
			expiry: time::Duration::from_secs(0),
			store_original: false,
		}
	}
}
