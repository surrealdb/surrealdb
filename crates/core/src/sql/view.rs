use std::fmt;

use crate::sql::fmt::Fmt;
use crate::sql::{Cond, Fields, Groups, Ident};

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct View {
	pub expr: Fields,
	pub what: Vec<Ident>,
	pub cond: Option<Cond>,
	pub group: Option<Groups>,
}

impl fmt::Display for View {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "AS SELECT {} FROM {}", self.expr, Fmt::comma_separated(self.what.iter()))?;
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
			what: v.what.into_iter().map(Into::into).collect(),
			cond: v.cond.map(Into::into),
			group: v.group.map(Into::into),
		}
	}
}

impl From<crate::expr::View> for View {
	fn from(v: crate::expr::View) -> Self {
		View {
			expr: v.expr.into(),
			what: v.what.into_iter().map(Into::into).collect(),
			cond: v.cond.map(Into::into),
			group: v.group.map(Into::into),
		}
	}
}
