use std::ops::Bound;

use common::fmt::{EscapeKwFreeIdent, Fmt};
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::expr::order::Ordering;
use crate::expr::start::Start;
use crate::expr::{Cond, Dir, Fields, Groups, Idiom, Limit, RecordIdKeyRangeLit, Splits};
use crate::val::{RecordIdKeyRange, TableName};

/// A lookup is a unified way of looking up graph edges and record references.
/// Since they both work very similarly, they also both support the same operations
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct Lookup {
	pub kind: LookupKind,
	pub expr: Option<Fields>,
	pub only: bool,
	pub what: Vec<LookupSubject>,
	pub cond: Option<Cond>,
	pub split: Option<Splits>,
	pub group: Option<Groups>,
	pub order: Option<Ordering>,
	pub limit: Option<Limit>,
	pub start: Option<Start>,
	pub alias: Option<Idiom>,
}

impl Lookup {
	/// Whether evaluating this graph lookup can be done on a read-only
	/// transaction. Every clause the lookup carries can hold a user
	/// expression, so each is inspected; the subject (`what`) names tables and
	/// ranges only.
	pub fn read_only(&self) -> bool {
		self.what.iter().all(LookupSubject::read_only)
			&& self.expr.as_ref().map(|x| x.read_only()).unwrap_or(true)
			&& self.cond.as_ref().map(|x| x.0.read_only()).unwrap_or(true)
			&& self.split.as_ref().map(|x| x.read_only()).unwrap_or(true)
			&& self.group.as_ref().map(|x| x.read_only()).unwrap_or(true)
			&& self.order.as_ref().map(|x| x.read_only()).unwrap_or(true)
			&& self.limit.as_ref().map(|x| x.read_only()).unwrap_or(true)
			&& self.start.as_ref().map(|x| x.read_only()).unwrap_or(true)
			&& self.alias.as_ref().map(|x| x.read_only()).unwrap_or(true)
	}
}

impl LookupSubject {
	pub fn referencing_field(&self) -> Option<&str> {
		match self {
			Self::Table {
				referencing_field,
				..
			}
			| Self::Range {
				referencing_field,
				..
			} => referencing_field.as_deref(),
		}
	}
	/// A range subject's bounds are record-id keys, which carry arbitrary
	/// expressions in their array and object forms.
	pub fn read_only(&self) -> bool {
		match self {
			LookupSubject::Table {
				..
			} => true,
			LookupSubject::Range {
				range,
				..
			} => range.read_only(),
		}
	}
}

impl ToSql for Lookup {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		if self.what.len() <= 1
			// When the singular lookup subject has a referencing field, it needs to be wrapped in parentheses
			// Otherwise <~table.field will be parsed as [Lookup(<~table), Field(.field)]
			// Whereas <~(table.field) will be parsed as [Lookup(<~table.field)]
			//
			// Further more `<-foo:a..` can lead to issues when the next part of the idiom starts
			// with a `.`
			&& self.what.iter().all(|v| {
				if v.referencing_field().is_some() {
					return false
				}
				if let LookupSubject::Range { range: RecordIdKeyRangeLit{ end: Bound::Unbounded, .. }, ..} = v {
					return false
				}
				true
			})
			&& self.cond.is_none()
			&& self.alias.is_none()
			&& self.expr.is_none()
		{
			self.kind.fmt_sql(f, fmt);
			if self.what.is_empty() {
				f.push('?');
			} else {
				write_sql!(f, fmt, "{}", Fmt::comma_separated(self.what.iter()));
			}
		} else {
			write_sql!(f, fmt, "{}(", self.kind);
			if let Some(ref expr) = self.expr {
				write_sql!(f, fmt, "SELECT {} FROM ", expr);
				if self.only {
					f.push_str("ONLY ");
				}
			}
			if self.what.is_empty() {
				f.push('?');
			} else {
				write_sql!(f, fmt, "{}", Fmt::comma_separated(&self.what));
			}
			if let Some(ref v) = self.cond {
				write_sql!(f, fmt, " {v}");
			}
			if let Some(ref v) = self.split {
				write_sql!(f, fmt, " {v}");
			}
			if let Some(ref v) = self.group {
				write_sql!(f, fmt, " {v}");
			}
			if let Some(ref v) = self.order {
				write_sql!(f, fmt, " {v}");
			}
			if let Some(ref v) = self.limit {
				write_sql!(f, fmt, " {v}");
			}
			if let Some(ref v) = self.start {
				write_sql!(f, fmt, " {v}");
			}
			if let Some(ref v) = self.alias {
				write_sql!(f, fmt, " AS {v}");
			}
			f.push(')');
		}
	}
}

/// This enum instructs whether the lookup is a graph edge or a record reference
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum LookupKind {
	Graph(Dir),
	Reference,
}

impl Default for LookupKind {
	fn default() -> Self {
		Self::Graph(Dir::default())
	}
}

impl ToSql for LookupKind {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			Self::Graph(dir) => dir.fmt_sql(f, fmt),
			Self::Reference => f.push_str("<~"),
		}
	}
}

/// This enum instructs whether we scan all edges on a table or just a specific range
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum LookupSubject {
	Table {
		table: TableName,
		referencing_field: Option<String>,
	},
	Range {
		table: TableName,
		range: RecordIdKeyRangeLit,
		referencing_field: Option<String>,
	},
}

/// This enum instructs whether we scan all edges on a table or just a specific range
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum ComputedLookupSubject {
	Table {
		table: TableName,
		referencing_field: Option<String>,
	},
	Range {
		table: TableName,
		range: RecordIdKeyRange,
		referencing_field: Option<String>,
	},
}

impl ComputedLookupSubject {
	pub fn into_literal(self) -> LookupSubject {
		match self {
			ComputedLookupSubject::Table {
				table,
				referencing_field,
			} => LookupSubject::Table {
				table,
				referencing_field,
			},
			ComputedLookupSubject::Range {
				table,
				range,
				referencing_field,
			} => LookupSubject::Range {
				table,
				range: range.into_literal(),
				referencing_field,
			},
		}
	}
}

impl ToSql for LookupSubject {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			Self::Table {
				table,
				referencing_field,
			} => {
				write_sql!(f, fmt, "{}", EscapeKwFreeIdent(table.as_str()));
				if let Some(referencing_field) = referencing_field {
					write_sql!(f, fmt, " FIELD {}", EscapeKwFreeIdent(referencing_field));
				}
			}
			Self::Range {
				table,
				range,
				referencing_field,
			} => {
				write_sql!(f, fmt, "{}:{range}", EscapeKwFreeIdent(table.as_str()));
				if let Some(referencing_field) = referencing_field {
					write_sql!(f, fmt, " FIELD {}", EscapeKwFreeIdent(referencing_field));
				}
			}
		}
	}
}
