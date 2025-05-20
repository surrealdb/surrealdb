use super::FlowResultExt;
use crate::{ctx::Context, dbs::Options, doc::CursorDoc, err::Error, sql::datetime::Datetime};
use anyhow::Result;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

use super::SqlValue;

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Version(
	#[revision(end = 2, convert_fn = "convert_version_datetime")] pub Datetime,
	#[revision(start = 2)] pub SqlValue,
);

impl Version {
	fn convert_version_datetime(
		&mut self,
		_revision: u16,
		old: Datetime,
	) -> Result<(), revision::Error> {
		self.0 = SqlValue::Datetime(old);
		Ok(())
	}
}

impl fmt::Display for Version {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "VERSION {}", self.0)
	}
}

impl From<Version> for crate::expr::Version {
	fn from(v: Version) -> Self {
		Self(v.0.into())
	}
}

impl From<crate::expr::Version> for Version {
	fn from(v: crate::expr::Version) -> Self {
		Self(v.0.into())
	}
}
