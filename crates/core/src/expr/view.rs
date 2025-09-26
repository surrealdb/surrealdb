use crate::catalog::ViewDefinition;
use crate::expr::expression::VisitExpression;
use crate::expr::statements::info::InfoStructure;
use crate::expr::{Cond, Expr, Fields, Groups, Value};
use crate::fmt::{EscapeIdent, Fmt};
use std::fmt;
use std::fmt::Debug;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct View {
	pub expr: Fields,
	pub what: Vec<String>,
	pub cond: Option<Cond>,
	pub group: Option<Groups>,
}

impl VisitExpression for View {
	fn visit<F>(&self, visitor: &mut F)
	where
		F: FnMut(&Expr),
	{
		self.expr.visit(visitor);
		self.cond.iter().for_each(|cond| cond.0.visit(visitor));
		self.group.iter().for_each(|groups| groups.visit(visitor));
	}
}

impl View {
	pub(crate) fn to_definition(&self) -> ViewDefinition {
		ViewDefinition {
			fields: self.expr.clone(),
			what: self.what.clone(),
			cond: self.cond.clone().map(|c| c.0),
			groups: self.group.clone(),
		}
	}
}

impl fmt::Display for View {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(
			f,
			"AS SELECT {} FROM {}",
			self.expr,
			Fmt::comma_separated(self.what.iter().map(EscapeIdent))
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
impl InfoStructure for View {
	fn structure(self) -> Value {
		self.to_string().into()
	}
}
