use crate::sql::statements::info::InfoStructure;
use crate::sql::{cond::Cond, field::Fields, group::Groups, table::Tables, Value};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

use super::paths::ID;
use super::statements::SelectStatement;
use super::{Field, Idiom};

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

impl From<View> for SelectStatement {
	fn from(
		View {
			mut expr,
			what,
			cond,
			group,
		}: View,
	) -> Self {
		let id_field = if let Some(ref group) = group {
			Field::Single {
				expr: Value::Array(group.clone().into()),
				alias: Some(Idiom(ID.clone().to_vec())),
			}
		} else {
			Field::Single {
				expr: Idiom(ID.clone().to_vec()).into(),
				alias: None,
			}
		};
		expr.0.push(id_field);

		SelectStatement {
			expr,
			what: what.into(),
			cond,
			group,
			..Default::default()
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
