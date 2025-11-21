use std::fmt;

use crate::fmt::{EscapeKwFreeIdent, Fmt};
use crate::sql::{Cond, Fields, Groups};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct View {
	pub expr: Fields,
	pub what: Vec<String>,
	pub cond: Option<Cond>,
	pub group: Option<Groups>,
}

impl fmt::Display for View {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(
			f,
			"AS SELECT {} FROM {}",
			self.expr,
			Fmt::comma_separated(self.what.iter().map(|x| EscapeKwFreeIdent(x.as_ref())))
		)?;
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
			materialize: true,
			expr: v.expr.into(),
			what: v.what.clone(),
			cond: v.cond.map(Into::into),
			group: v.group.map(Into::into),
		}
	}
}

impl From<crate::expr::View> for View {
	fn from(v: crate::expr::View) -> Self {
		View {
			expr: v.expr.into(),
			what: v.what.clone(),
			cond: v.cond.map(Into::into),
			group: v.group.map(Into::into),
		}
	}
}
