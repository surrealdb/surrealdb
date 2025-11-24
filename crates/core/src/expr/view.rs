use std::fmt;
use std::fmt::Debug;

use anyhow::{Result, bail};

use crate::catalog::ViewDefinition;
use crate::catalog::aggregation::{AggregateFields, AggregationAnalysis};
use crate::err::Error;
use crate::expr::statements::info::InfoStructure;
use crate::expr::{Cond, Fields, Groups, Value};
use crate::fmt::{EscapeKwFreeIdent, Fmt};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct View {
	pub(crate) materialize: bool,
	pub(crate) expr: Fields,
	pub(crate) what: Vec<String>,
	pub(crate) cond: Option<Cond>,
	pub(crate) group: Option<Groups>,
}

impl View {
	pub(crate) fn to_definition(&self) -> Result<ViewDefinition> {
		if !self.materialize {
			return Ok(ViewDefinition::Select {
				fields: self.expr.clone(),
				tables: self.what.clone(),
				condition: self.cond.clone().map(|x| x.0),
				groups: self.group.clone(),
			});
		}

		let Some(group) = self.group.as_ref() else {
			// No group, nothing to aggregate.
			return Ok(ViewDefinition::Materialized {
				fields: self.expr.clone(),
				tables: self.what.clone(),
				condition: self.cond.clone().map(|x| x.0),
			});
		};

		let analysis = AggregationAnalysis::analyze_fields_groups(&self.expr, group, true)?;
		if let AggregateFields::Value(_) = analysis.fields {
			bail!(Error::InvalidAggregation {
				message: "the selector `VALUE` clause is not supported on DEFINE TABLE .. AS SELECT .. GROUP .. aggregates"
					.to_string()
			})
		}

		Ok(ViewDefinition::Aggregated {
			groups: group.clone(),
			fields: self.expr.clone(),

			analysis,
			tables: self.what.clone(),
			condition: self.cond.clone().map(|x| x.0),
		})
	}
}

impl fmt::Display for View {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(
			f,
			"AS SELECT {} FROM {}",
			self.expr,
			Fmt::comma_separated(self.what.iter().map(|x| EscapeKwFreeIdent(x)))
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
