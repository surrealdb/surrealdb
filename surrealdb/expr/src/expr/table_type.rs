//! The kind of a table: schemafull document, relation, or any.
//!
//! `TableType` and its `Relation` payload are shared vocabulary between the
//! DEFINE/ALTER TABLE statements and the catalog's table definitions; both
//! planes persist them, so the revisioned shapes here are storage-stable.

use common::fmt::EscapeKwFreeIdent;
use revision::revisioned;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::expr::Kind;
use crate::expr::statements::info::InfoStructure;
use crate::sql;
use crate::val::{TableName, Value};

/// The type of records stored by a table
#[revisioned(revision = 1)]
#[derive(Debug, Default, Hash, Clone, Eq, PartialEq)]
pub enum TableType {
	#[default]
	Any,
	Normal,
	Relation(Relation),
}

impl ToSql for TableType {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		match self {
			TableType::Any => f.push_str("ANY"),
			TableType::Normal => f.push_str("NORMAL"),
			TableType::Relation(rel) => {
				f.push_str("RELATION");
				if !rel.from.is_empty() {
					f.push_str(" IN ");
					for (idx, k) in rel.from.iter().enumerate() {
						if idx != 0 {
							f.push_str(" | ");
						}
						write_sql!(f, sql_fmt, "{}", EscapeKwFreeIdent(k.as_str()));
					}
				}
				if !rel.to.is_empty() {
					f.push_str(" OUT ");
					for (idx, k) in rel.to.iter().enumerate() {
						if idx != 0 {
							f.push_str(" | ");
						}
						write_sql!(f, sql_fmt, "{}", EscapeKwFreeIdent(k.as_str()));
					}
				}
				if rel.enforced {
					f.push_str(" ENFORCED");
				}
			}
		}
	}
}

impl InfoStructure for TableType {
	fn structure(self) -> Value {
		match self {
			Self::Any => Value::from(map! {
				"kind" => "ANY".into(),
			}),
			Self::Normal => Value::from(map! {
				"kind" => "NORMAL".into(),
			}),
			Self::Relation(rel) => Value::from(map! {
				"kind" => "RELATION".into(),
				"in", if !rel.from.is_empty() =>
					rel.from.into_iter().map(Value::Table).collect::<Vec<_>>().into(),
				"out", if !rel.to.is_empty() =>
					rel.to.into_iter().map(Value::Table).collect::<Vec<_>>().into(),
				"enforced" => rel.enforced.into()
			}),
		}
	}
}

impl From<sql::table_type::TableType> for TableType {
	fn from(v: sql::table_type::TableType) -> Self {
		match v {
			sql::table_type::TableType::Any => Self::Any,
			sql::table_type::TableType::Normal => Self::Normal,
			sql::table_type::TableType::Relation(rel) => Self::Relation(rel.into()),
		}
	}
}

impl From<TableType> for sql::table_type::TableType {
	fn from(v: TableType) -> Self {
		match v {
			TableType::Any => Self::Any,
			TableType::Normal => Self::Normal,
			TableType::Relation(rel) => Self::Relation(rel.into()),
		}
	}
}

#[revisioned(revision = 2)]
#[derive(Debug, Hash, Clone, Eq, PartialEq)]
pub struct Relation {
	#[revision(end = 2, convert_fn = "rev_convert_from")]
	pub old_from: Option<Kind>,
	/// Contains the tables the relation originates from,
	/// if empty then there was no `IN` clause
	#[revision(start = 2)]
	pub from: Vec<TableName>,
	#[revision(end = 2, convert_fn = "rev_convert_to")]
	pub old_to: Option<Kind>,
	/// Contains the tables the relation goes to,
	/// if empty then there was no `OUT` clause
	#[revision(start = 2)]
	pub to: Vec<TableName>,
	pub enforced: bool,
}

impl Relation {
	fn rev_convert_from(&mut self, _rev: u16, value: Option<Kind>) -> Result<(), revision::Error> {
		if let Some(x) = value {
			let Kind::Record(x) = x else {
				return Err(revision::Error::Conversion(format!(
					"Invalid kind within table relation, should have been a record, found: {:#?}",
					x,
				)));
			};
			self.from = x
		}
		Ok(())
	}
	fn rev_convert_to(&mut self, _rev: u16, value: Option<Kind>) -> Result<(), revision::Error> {
		if let Some(x) = value {
			let Kind::Record(x) = x else {
				return Err(revision::Error::Conversion(format!(
					"Invalid kind within table relation, should have been a record, found: {:#?}",
					x,
				)));
			};
			self.to = x
		}
		Ok(())
	}
}

impl From<sql::table_type::Relation> for Relation {
	fn from(v: sql::table_type::Relation) -> Self {
		Self {
			from: v.from.into_iter().map(Into::into).collect(),
			to: v.to.into_iter().map(Into::into).collect(),
			enforced: v.enforced,
		}
	}
}

impl From<Relation> for sql::table_type::Relation {
	fn from(v: Relation) -> Self {
		Self {
			from: v.from.into_iter().map(Into::into).collect(),
			to: v.to.into_iter().map(Into::into).collect(),
			enforced: v.enforced,
		}
	}
}
