use crate::catalog::ViewDefinition;
use crate::expr::statements::info::InfoStructure;
use crate::expr::{Value, cond::Cond, field::Fields, group::Groups, table::Tables};
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

impl View {
	pub(crate) fn to_definition(&self) -> ViewDefinition {
		ViewDefinition {
			expr: self.expr.clone(),
			what: self.what.clone(),
			cond: self.cond.clone(),
			group: self.group.clone(),
		}
	}
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
impl InfoStructure for View {
	fn structure(self) -> Value {
		self.to_string().into()
	}
}
