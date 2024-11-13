use crate::{ctx::Context, dbs::Options, doc::CursorDoc, err::Error, sql::datetime::Datetime};
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::{fmt, ops::Range};

use super::Value;

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Version(
	#[revision(end = 2, convert_fn = "convert_version_datetime")] pub Datetime,
	#[revision(start = 2)] pub Value,
);

#[derive(Clone, Debug)]
pub enum ComputedVersion {
	Fixed(u64),
	Range(Range<u64>),
}

impl Version {
	fn convert_version_datetime(
		&mut self,
		_revision: u16,
		old: Datetime,
	) -> Result<(), revision::Error> {
		self.0 = Value::Datetime(old);
		Ok(())
	}

	pub async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<ComputedVersion, Error> {
		match self.0.compute(stk, ctx, opt, doc).await? {
			Value::Datetime(v) => Ok(ComputedVersion::Fixed(v.to_u64())),
			Value::Range(r) => {
				let r: Range<Datetime> = (*r).try_into()?;
				Ok(ComputedVersion::Range(r.start.to_u64()..r.end.to_u64()))
			}
			found => Err(Error::InvalidVersion {
				found,
			}),
		}
	}
}

impl fmt::Display for Version {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "VERSION {}", self.0)
	}
}
