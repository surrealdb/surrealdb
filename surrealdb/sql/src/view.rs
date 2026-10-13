use common::fmt::{EscapeKwFreeIdent, Fmt};
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::{Cond, Fields, Groups, TableName};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct View {
	pub expr: Fields,
	pub what: Vec<TableName>,
	pub cond: Option<Cond>,
	pub group: Option<Groups>,
}

impl ToSql for View {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(
			f,
			fmt,
			"AS SELECT {} FROM {}",
			self.expr,
			Fmt::comma_separated(self.what.iter().map(|x| EscapeKwFreeIdent(x.as_str())))
		);
		if let Some(ref v) = self.cond {
			write_sql!(f, fmt, " {v}");
		}
		if let Some(ref v) = self.group {
			write_sql!(f, fmt, " {v}");
		}
	}
}
