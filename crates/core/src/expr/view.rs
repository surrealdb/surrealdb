use std::fmt;

use crate::catalog::ViewDefinition;
use crate::expr::fmt::Fmt;
use crate::expr::statements::info::InfoStructure;
use crate::expr::{Cond, Fields, Groups, Ident, Value};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct View {
	pub expr: Fields,
	pub what: Vec<Ident>,
	pub cond: Option<Cond>,
	pub group: Option<Groups>,
}

impl View {
	pub(crate) fn to_definition(&self) -> ViewDefinition {
		ViewDefinition {
			fields: self.expr.clone(),
			what: self.what.iter().map(|s| s.to_raw_string()).collect(),
			cond: self.cond.clone().map(|c| c.0),
			groups: self.group.clone(),
		}
	}
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
impl InfoStructure for View {
	fn structure(self) -> Value {
		self.to_string().into()
	}
}
