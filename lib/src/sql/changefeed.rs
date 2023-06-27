use crate::sql::comment::shouldbespace;
use crate::sql::duration::{duration, Duration};
use crate::sql::error::IResult;
use nom::bytes::complete::tag_no_case;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};
use std::str;
use std::time;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct ChangeFeed {
	pub expiry: time::Duration,
}

impl Display for ChangeFeed {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "CHANGEFEED {}", Duration(self.expiry))?;
		Ok(())
	}
}

pub fn changefeed(i: &str) -> IResult<&str, ChangeFeed> {
	let (i, _) = tag_no_case("CHANGEFEED")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = duration(i)?;
	Ok((
		i,
		ChangeFeed {
			expiry: v.0,
		},
	))
}

impl Default for ChangeFeed {
	fn default() -> Self {
		Self {
			expiry: time::Duration::from_secs(0),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn changefeed_missing() {
		let sql: &str = "";
		let res = changefeed(sql);
		assert!(res.is_err());
	}

	#[test]
	fn changefeed_enabled() {
		let sql = "CHANGEFEED 1h";
		let res = changefeed(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("CHANGEFEED 1h", format!("{}", out));
		assert_eq!(
			out,
			ChangeFeed {
				expiry: time::Duration::from_secs(3600)
			}
		);
	}
}
