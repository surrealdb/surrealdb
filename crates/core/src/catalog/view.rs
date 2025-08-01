use revision::revisioned;
use crate::expr::{Fields, Tables, Cond, Groups, Value};
use crate::expr::statements::info::InfoStructure;
use crate::map;
use crate::sql::ToSql;

use serde::{Deserialize, Serialize};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct ViewDefinition {
	pub expr: Fields,
	pub what: Tables,
	pub cond: Option<Cond>,
	pub group: Option<Groups>,
}

impl ToSql for ViewDefinition {
	fn to_sql(&self) -> String {
		let mut sql = format!("AS SELECT {} FROM {}", self.expr, self.what);
		if let Some(ref v) = self.cond {
			sql.push_str(&format!(" {}", v.to_string()));
		}
		if let Some(ref v) = self.group {
			sql.push_str(&format!(" {}", v.to_string()));
		}
		sql
	}
}
impl InfoStructure for ViewDefinition {
	fn structure(self) -> Value {
		self.to_sql().into()
	}
}

