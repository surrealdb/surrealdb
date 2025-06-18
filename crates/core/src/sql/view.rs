use crate::sql::{cond::Cond, field::Fields, group::Groups, table::Tables};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct View {
	pub expr: Fields,
	pub what: Tables,
	pub cond: Option<Cond>,
	pub group: Option<Groups>,
}

impl fmt::Display for View {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "AS SELECT {} FROM {}", self.expr, self.what)?;
		if let Some(ref v) = self.cond {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.group {
			write!(f, " {v}")?
		}
		Ok(())
	}
}

impl From<View> for crate::expr::View {
	fn from(v: View) -> Self {
		crate::expr::View {
			expr: v.expr.into(),
			what: v.what.into(),
			cond: v.cond.map(Into::into),
			group: v.group.map(Into::into),
		}
	}
}

impl From<crate::expr::View> for View {
	fn from(v: crate::expr::View) -> Self {
		View {
			expr: v.expr.into(),
			what: v.what.into(),
			cond: v.cond.map(Into::into),
			group: v.group.map(Into::into),
		}
	}
}
