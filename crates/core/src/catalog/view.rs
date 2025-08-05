use crate::expr::statements::info::InfoStructure;
use crate::expr::{Cond, Fields, Groups, Tables, Value};
use crate::sql::{ToSql, View};
use revision::revisioned;

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

impl ViewDefinition {
    pub(crate) fn to_sql_definition(&self) -> View {
        View {
            expr: self.expr.clone().into(),
            what: self.what.clone().into(),
            cond: self.cond.clone().map(Into::into),
            group: self.group.clone().map(Into::into),
        }
    }
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
