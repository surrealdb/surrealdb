use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::sql::{Cond, Fields, Groups};

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct View {
	pub expr: Fields,
	pub what: Vec<String>,
	pub cond: Option<Cond>,
	pub group: Option<Groups>,
}

impl ToSql for View {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, "AS SELECT {}", self.expr);
		if !self.what.is_empty() {
			f.push_str(" FROM ");
			for (i, expr) in self.what.iter().enumerate() {
				if i > 0 {
					f.push_str(", ");
				}
				expr.fmt_sql(f, fmt);
			}
		}
		if let Some(ref v) = self.cond {
			write_sql!(f, " {}", v);
		}
		if let Some(ref v) = self.group {
			write_sql!(f, " {}", v);
		}
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
