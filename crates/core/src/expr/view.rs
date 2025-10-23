use ahash::HashMap;
use anyhow::Result;
use std::fmt;
use std::fmt::Debug;

use crate::catalog::{Aggregation, ViewDefinition, ViewDefinition2};

use crate::expr::statements::info::InfoStructure;
use crate::expr::{Cond, Expr, Fields, Groups, Value};
use crate::fmt::{EscapeIdent, Fmt};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct View {
	pub(crate) materialize: bool,
	pub(crate) expr: Fields,
	pub(crate) what: Vec<String>,
	pub(crate) cond: Option<Cond>,
	pub(crate) group: Option<Groups>,
}

struct ViewAggregateCollector<'a> {
	within_aggregate_argument: bool,
	exprs_map: &'a mut HashMap<Expr, usize>,
	aggregates: &'a mut Vec<Aggregation>,
	groups: &'a Groups,
}

impl View {
	pub(crate) fn to_definition(&self) -> Result<ViewDefinition> {
		Ok(ViewDefinition {
			_tmp: self.compute_definition()?,
			fields: self.expr.clone(),
			what: self.what.clone(),
			cond: self.cond.clone().map(|c| c.0),
			groups: self.group.clone(),
		})
	}

	fn compute_definition(&self) -> Result<ViewDefinition2> {
		if !self.materialize {
			return Ok(ViewDefinition2::Select {
				fields: self.expr.clone(),
				tables: self.what.clone(),
				cond: self.cond.clone().map(|x| x.0),
				groups: self.group.clone(),
			});
		}

		let Some(group) = self.group.as_ref() else {
			// No group, nothing to aggregate.
			return Ok(ViewDefinition2::Materialized {
				fields: self.expr.clone(),
				target_tables: self.what.clone(),
				condition: self.cond.clone().map(|x| x.0),
			});
		};

		todo!()
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
