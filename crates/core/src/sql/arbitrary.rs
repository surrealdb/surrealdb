use crate::sql::{
	changefeed::ChangeFeed, datetime::Datetime, duration::Duration, regex::Regex,
	statements::SleepStatement,
};
use arbitrary::{Arbitrary, Result, Unstructured};
use regex_syntax::ast::Ast;
use std::time;

impl<'a> Arbitrary<'a> for Duration {
	fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self> {
		Ok(Self::from(time::Duration::new(u64::arbitrary(u)?, u32::arbitrary(u)?)))
	}
}

impl<'a> Arbitrary<'a> for Datetime {
	fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self> {
		let result = chrono::DateTime::UNIX_EPOCH + chrono::Duration::seconds(i64::arbitrary(u)?);
		Ok(Self(result))
	}
}

impl<'a> Arbitrary<'a> for Regex {
	fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self> {
		let ast = Ast::arbitrary(u)?;
		Ok(Self(
			regex::Regex::new(&format!("{ast}")).map_err(|_| arbitrary::Error::IncorrectFormat)?,
		))
	}
}

impl<'a> Arbitrary<'a> for ChangeFeed {
	fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self> {
		Ok(Self {
			expiry: time::Duration::new(u64::arbitrary(u)?, u32::arbitrary(u)?),
			store_diff: bool::arbitrary(u)?,
		})
	}
}

impl<'a> Arbitrary<'a> for SleepStatement {
	fn arbitrary(_u: &mut Unstructured<'a>) -> Result<Self> {
		Ok(Self {
			// When fuzzing we don't want to sleep, that's slow... we want insomnia.
			duration: Duration(time::Duration::new(0, 0)),
		})
	}
}
