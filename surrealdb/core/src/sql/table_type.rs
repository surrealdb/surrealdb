use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::fmt::EscapeKwFreeIdent;
use crate::val::TableName;

/// The type of records stored by a table
#[derive(Debug, Default, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum TableType {
	#[default]
	Any,
	Normal,
	Relation(Relation),
}

impl ToSql for TableType {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		match self {
			TableType::Normal => {
				write_sql!(f, sql_fmt, " NORMAL");
			}
			TableType::Relation(rel) => {
				write_sql!(f, sql_fmt, " RELATION");
				if !rel.from.is_empty() {
					f.push_str(" IN ");
					for (idx, k) in rel.from.iter().enumerate() {
						if idx != 0 {
							f.push_str(" | ");
						}
						write_sql!(f, sql_fmt, "{}", EscapeKwFreeIdent(k.as_str()))
					}
				}
				if !rel.to.is_empty() {
					f.push_str(" OUT ");
					for (idx, k) in rel.to.iter().enumerate() {
						if idx != 0 {
							f.push_str(" | ");
						}
						write_sql!(f, sql_fmt, "{}", EscapeKwFreeIdent(k.as_str()))
					}
				}
				if rel.enforced {
					write_sql!(f, sql_fmt, " ENFORCED");
				}
			}
			TableType::Any => {
				write_sql!(f, sql_fmt, " ANY");
			}
		}
	}
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Relation {
	#[cfg_attr(feature = "arbitrary", arbitrary(with = crate::sql::arbitrary::atleast_one))]
	pub from: Vec<TableName>,
	#[cfg_attr(feature = "arbitrary", arbitrary(with = crate::sql::arbitrary::atleast_one))]
	pub to: Vec<TableName>,
	pub enforced: bool,
}
