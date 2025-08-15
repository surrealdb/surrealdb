use std::fmt;

use revision::revisioned;
use serde::{Deserialize, Serialize};

use crate::catalog::ViewDefinition;
use crate::expr::fmt::Fmt;
use crate::expr::statements::info::InfoStructure;
use crate::expr::{Cond, Fields, Groups, Ident, Value};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct View {
	pub expr: Fields,
	pub what: Vec<Ident>,
	pub cond: Option<Cond>,
	pub group: Option<Groups>,
}

impl View {
	pub(crate) fn to_definition(&self) -> ViewDefinition {
		ViewDefinition {
			expr: self.expr.clone(),
			what: self.what.iter().map(|s| s.as_raw_string()).collect(),
			cond: self.cond.clone(),
			group: self.group.clone(),
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
