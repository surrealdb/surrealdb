use surrealdb_types::ToSql;

use super::Value;
use super::statements::info::InfoStructure;
use crate::expr::{CoverStmts, Expr};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Reference {
	pub on_delete: ReferenceDeleteStrategy,
}

impl surrealdb_types::ToSql for Reference {
	fn fmt_sql(&self, f: &mut String, fmt: surrealdb_types::SqlFormat) {
		f.push_str("ON DELETE ");
		self.on_delete.fmt_sql(f, fmt);
	}
}

impl InfoStructure for Reference {
	fn structure(self) -> Value {
		map! {
			"on_delete" => self.on_delete.structure(),
		}
		.into()
	}
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum ReferenceDeleteStrategy {
	Reject,
	Ignore,
	Cascade,
	Unset,
	Custom(Expr),
}

impl surrealdb_types::ToSql for ReferenceDeleteStrategy {
	fn fmt_sql(&self, f: &mut String, fmt: surrealdb_types::SqlFormat) {
		match self {
			ReferenceDeleteStrategy::Reject => f.push_str("REJECT"),
			ReferenceDeleteStrategy::Ignore => f.push_str("IGNORE"),
			ReferenceDeleteStrategy::Cascade => f.push_str("CASCADE"),
			ReferenceDeleteStrategy::Unset => f.push_str("UNSET"),
			ReferenceDeleteStrategy::Custom(v) => {
				f.push_str("THEN ");
				CoverStmts(v).fmt_sql(f, fmt);
			}
		}
	}
}

impl InfoStructure for ReferenceDeleteStrategy {
	fn structure(self) -> Value {
		self.to_sql().into()
	}
}
