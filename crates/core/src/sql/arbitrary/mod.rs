mod idiom;
mod parts;
mod statements;
mod utils;
pub(crate) use idiom::*;
pub(crate) use parts::*;
pub(crate) use utils::*;

use std::time;

use arbitrary::{Arbitrary, Result, Unstructured};
use surrealdb_types::Duration;

use crate::sql::changefeed::ChangeFeed;
use crate::sql::statements::SleepStatement;

impl<'a> Arbitrary<'a> for ChangeFeed {
	fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self> {
		Ok(Self {
			expiry: time::Duration::new(u64::arbitrary(u)?, u32::arbitrary(u)?).into(),
			store_diff: bool::arbitrary(u)?,
		})
	}
}

impl<'a> Arbitrary<'a> for SleepStatement {
	fn arbitrary(_u: &mut Unstructured<'a>) -> Result<Self> {
		Ok(Self {
			// When fuzzing we don't want to sleep, that's slow... we want insomnia.
			duration: Duration::from_std(time::Duration::new(0, 0)),
		})
	}
}
