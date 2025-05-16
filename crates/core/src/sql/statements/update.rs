use crate::sql::{Cond, Data, Explain, Output, Timeout, Values, With};

use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[revisioned(revision = 3)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct UpdateStatement {
	#[revision(start = 2)]
	pub only: bool,
	pub what: Values,
	#[revision(start = 3)]
	pub with: Option<With>,
	pub data: Option<Data>,
	pub cond: Option<Cond>,
	pub output: Option<Output>,
	pub timeout: Option<Timeout>,
	pub parallel: bool,
	#[revision(start = 3)]
	pub explain: Option<Explain>,
}

impl UpdateStatement {
	/// Check if we require a writeable transaction
	pub(crate) fn writeable(&self) -> bool {
		true
	}
}

impl From<UpdateStatement> for crate::expr::statements::UpdateStatement {
	fn from(v: UpdateStatement) -> Self {
		crate::expr::statements::UpdateStatement {
			only: v.only,
			what: v.what.into(),
			with: v.with.map(Into::into),
			data: v.data.map(Into::into),
			cond: v.cond.map(Into::into),
			output: v.output.map(Into::into),
			timeout: v.timeout.map(Into::into),
			parallel: v.parallel,
			explain: v.explain.map(Into::into),
		}
	}
}

impl From<crate::expr::statements::UpdateStatement> for UpdateStatement {
	fn from(v: crate::expr::statements::UpdateStatement) -> Self {
		UpdateStatement {
			only: v.only,
			what: v.what.into(),
			with: v.with.map(Into::into),
			data: v.data.map(Into::into),
			cond: v.cond.map(Into::into),
			output: v.output.map(Into::into),
			timeout: v.timeout.map(Into::into),
			parallel: v.parallel,
			explain: v.explain.map(Into::into),
		}
	}
}

crate::sql::impl_display_from_sql!(UpdateStatement);

impl crate::sql::DisplaySql for UpdateStatement {
	fn fmt_sql(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "UPDATE")?;
		if self.only {
			f.write_str(" ONLY")?
		}
		write!(f, " {}", self.what)?;
		if let Some(ref v) = self.with {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.data {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.cond {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.output {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.timeout {
			write!(f, " {v}")?
		}
		if self.parallel {
			f.write_str(" PARALLEL")?
		}
		if let Some(ref v) = self.explain {
			write!(f, " {v}")?
		}
		Ok(())
	}
}
