use std::fmt::Debug;

use common::fmt::Fmt;
use surrealdb_strand::TableName;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::expr::statements::info::InfoStructure;
use crate::expr::{Cond, Fields, Groups, Value};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
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
			Fmt::comma_separated(self.what.iter().map(|x| common::fmt::EscapeIdent(x.as_str())))
		);
		if let Some(ref v) = self.cond {
			write_sql!(f, fmt, " {v}");
		}
		if let Some(ref v) = self.group {
			write_sql!(f, fmt, " {v}");
		}
	}
}
impl InfoStructure for View {
	fn structure(self) -> Value {
		self.to_sql().into()
	}
}
