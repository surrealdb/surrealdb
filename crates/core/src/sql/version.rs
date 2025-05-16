use crate::sql::datetime::Datetime;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

use super::Value;

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Version(
	#[revision(end = 2, convert_fn = "convert_version_datetime")] pub Datetime,
	#[revision(start = 2)] pub Value,
);

impl Version {
	fn convert_version_datetime(
		&mut self,
		_revision: u16,
		old: Datetime,
	) -> Result<(), revision::Error> {
		self.0 = Value::Datetime(old);
		Ok(())
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

crate::sql::impl_display_from_sql!(Version);

impl crate::sql::DisplaySql for Version {
	fn fmt_sql(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "VERSION {}", self.0)
	}
}
