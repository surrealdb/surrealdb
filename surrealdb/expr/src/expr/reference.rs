use common::fmt::EscapeKwFreeIdent;
use surrealdb_strand::TableName;
use surrealdb_types::{ToSql, write_sql};

use super::statements::info::InfoStructure;
use super::{Idiom, Value};
use crate::expr::{CoverStmts, Expr};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Reference {
	pub on_delete: ReferenceDeleteStrategy,
	/// The `MATERIALIZE INTO` clause, when the field keeps a stored list of the
	/// records that reference each record.
	pub materialize: Option<Materialize>,
}

/// The `MATERIALIZE INTO table.field [WHERE filter]` clause of a `REFERENCE`
/// field: the field of the referenced table that holds the ids of the records
/// referencing each record, and the condition a referencing record must meet to
/// be listed.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Materialize {
	/// The referenced table that holds the list.
	pub table: TableName,
	/// The top-level field of `table` that holds the list.
	pub field: Idiom,
	/// The condition, evaluated on the referencing record.
	pub filter: Option<Expr>,
}

impl surrealdb_types::ToSql for Materialize {
	fn fmt_sql(&self, f: &mut String, fmt: surrealdb_types::SqlFormat) {
		write_sql!(
			f,
			fmt,
			"MATERIALIZE INTO {}.{}",
			EscapeKwFreeIdent(self.table.as_str()),
			self.field
		);
		if let Some(filter) = &self.filter {
			write_sql!(f, fmt, " WHERE {}", CoverStmts(filter));
		}
	}
}

impl surrealdb_types::ToSql for Reference {
	fn fmt_sql(&self, f: &mut String, fmt: surrealdb_types::SqlFormat) {
		f.push_str("ON DELETE ");
		self.on_delete.fmt_sql(f, fmt);
		if let Some(materialize) = &self.materialize {
			f.push(' ');
			materialize.fmt_sql(f, fmt);
		}
	}
}

impl InfoStructure for Reference {
	fn structure(self) -> Value {
		let materialize = self.materialize.map(|m| {
			Value::from(map! {
				"table" => Value::String(m.table.into()),
				"field" => Value::String(m.field.to_raw_string().into()),
				"filter", if let Some(filter) = m.filter => Value::from(filter.to_sql()),
			})
		});
		map! {
			"on_delete" => self.on_delete.structure(),
			"materialize", if let Some(m) = materialize => m,
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
