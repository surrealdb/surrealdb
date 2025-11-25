use std::fmt::Write;

use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::fmt::EscapeIdent;
use crate::sql::order::Ordering;
use crate::sql::{Cond, Dir, Fields, Groups, Idiom, Limit, RecordIdKeyRangeLit, Splits, Start};

/// A lookup is a unified way of looking up graph edges and record references.
/// Since they both work very similarly, they also both support the same operations
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct Lookup {
	pub kind: LookupKind,
	pub expr: Option<Fields>,
	pub what: Vec<LookupSubject>,
	pub cond: Option<Cond>,
	pub split: Option<Splits>,
	pub group: Option<Groups>,
	pub order: Option<Ordering>,
	pub limit: Option<Limit>,
	pub start: Option<Start>,
	pub alias: Option<Idiom>,
}

impl surrealdb_types::ToSql for Lookup {
	fn fmt_sql(&self, f: &mut String, fmt: surrealdb_types::SqlFormat) {
		use surrealdb_types::ToSql;
		if self.what.len() <= 1
			&& self.what.iter().all(|v| v.referencing_field().is_none())
			&& self.cond.is_none()
			&& self.alias.is_none()
			&& self.expr.is_none()
		{
			self.kind.fmt_sql(f, fmt);
			if self.what.is_empty() {
				f.push('?');
			} else {
				for (i, item) in self.what.iter().enumerate() {
					if i > 0 {
						fmt.write_separator(f);
					}
					item.fmt_sql(f, fmt);
				}
			}
		} else {
			self.kind.fmt_sql(f, fmt);
			f.push('(');
			if let Some(ref expr) = self.expr {
				f.push_str("SELECT ");
				expr.fmt_sql(f, fmt);
				f.push_str(" FROM ");
			}
			if self.what.is_empty() {
				f.push('?');
			} else {
				for (i, item) in self.what.iter().enumerate() {
					if i > 0 {
						fmt.write_separator(f);
					}
					item.fmt_sql(f, fmt);
				}
			}
			if let Some(ref v) = self.cond {
				f.push(' ');
				v.fmt_sql(f, fmt);
			}
			if let Some(ref v) = self.split {
				f.push(' ');
				v.fmt_sql(f, fmt);
			}
			if let Some(ref v) = self.group {
				f.push(' ');
				v.fmt_sql(f, fmt);
			}
			if let Some(ref v) = self.order {
				f.push(' ');
				v.fmt_sql(f, fmt);
			}
			if let Some(ref v) = self.limit {
				f.push(' ');
				v.fmt_sql(f, fmt);
			}
			if let Some(ref v) = self.start {
				f.push(' ');
				v.fmt_sql(f, fmt);
			}
			if let Some(ref v) = self.alias {
				f.push_str(" AS ");
				v.fmt_sql(f, fmt);
			}
			f.push(')');
		}
	}
}

impl From<Lookup> for crate::expr::Lookup {
	fn from(v: Lookup) -> Self {
		Self {
			kind: v.kind.into(),
			expr: v.expr.map(From::from),
			what: v.what.into_iter().map(From::from).collect(),
			cond: v.cond.map(Into::into),
			split: v.split.map(Into::into),
			group: v.group.map(Into::into),
			order: v.order.map(Into::into),
			limit: v.limit.map(Into::into),
			start: v.start.map(Into::into),
			alias: v.alias.map(Into::into),
		}
	}
}

impl From<crate::expr::Lookup> for Lookup {
	fn from(v: crate::expr::Lookup) -> Self {
		Lookup {
			kind: v.kind.into(),
			expr: v.expr.map(Into::into),
			what: v.what.into_iter().map(From::from).collect(),
			cond: v.cond.map(Into::into),
			split: v.split.map(Into::into),
			group: v.group.map(Into::into),
			order: v.order.map(Into::into),
			limit: v.limit.map(Into::into),
			start: v.start.map(Into::into),
			alias: v.alias.map(Into::into),
		}
	}
}

/// This enum instructs whether the lookup is a graph edge or a record reference
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum LookupKind {
	Graph(Dir),
	Reference,
}

impl Default for LookupKind {
	fn default() -> Self {
		Self::Graph(Dir::Both)
	}
}

impl ToSql for LookupKind {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		match self {
			Self::Graph(dir) => dir.fmt_sql(f, sql_fmt),
			Self::Reference => f.push_str("<~"),
		}
	}
}

impl From<LookupKind> for crate::expr::lookup::LookupKind {
	fn from(v: LookupKind) -> Self {
		match v {
			LookupKind::Graph(dir) => Self::Graph(dir.into()),
			LookupKind::Reference => Self::Reference,
		}
	}
}

impl From<crate::expr::lookup::LookupKind> for LookupKind {
	fn from(v: crate::expr::lookup::LookupKind) -> Self {
		match v {
			crate::expr::lookup::LookupKind::Graph(dir) => Self::Graph(dir.into()),
			crate::expr::lookup::LookupKind::Reference => Self::Reference,
		}
	}
}

/// This enum instructs whether we scan all edges on a table or just a specific range
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum LookupSubject {
	Table {
		table: String,
		referencing_field: Option<String>,
	},
	Range {
		table: String,
		range: RecordIdKeyRangeLit,
		referencing_field: Option<String>,
	},
}

impl LookupSubject {
	pub fn referencing_field(&self) -> Option<&String> {
		match self {
			LookupSubject::Table {
				referencing_field,
				..
			} => referencing_field.as_ref(),
			LookupSubject::Range {
				referencing_field,
				..
			} => referencing_field.as_ref(),
		}
	}
}

impl ToSql for LookupSubject {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		match self {
			Self::Table {
				table,
				referencing_field,
			} => {
				EscapeIdent(table).fmt_sql(f, sql_fmt);
				if let Some(referencing_field) = referencing_field {
					write_sql!(f, sql_fmt, " FIELD {}", EscapeIdent(referencing_field));
				}
			}
			Self::Range {
				table,
				range,
				referencing_field,
			} => {
				write_sql!(f, sql_fmt, "{}:{range}", EscapeIdent(table));
				if let Some(referencing_field) = referencing_field {
					write_sql!(f, sql_fmt, " FIELD {}", EscapeIdent(referencing_field));
				}
			}
		}
	}
}

impl From<LookupSubject> for crate::expr::lookup::LookupSubject {
	fn from(v: LookupSubject) -> Self {
		match v {
			LookupSubject::Table {
				table,
				referencing_field,
			} => Self::Table {
				table: table.clone(),
				referencing_field,
			},
			LookupSubject::Range {
				table,
				range,
				referencing_field,
			} => Self::Range {
				table,
				range: range.into(),
				referencing_field,
			},
		}
	}
}

impl From<crate::expr::lookup::LookupSubject> for LookupSubject {
	fn from(v: crate::expr::lookup::LookupSubject) -> Self {
		match v {
			crate::expr::lookup::LookupSubject::Table {
				table,
				referencing_field,
			} => Self::Table {
				table: table.clone(),
				referencing_field,
			},
			crate::expr::lookup::LookupSubject::Range {
				table,
				range,
				referencing_field,
			} => Self::Range {
				table,
				range: range.into(),
				referencing_field,
			},
		}
	}
}
