use surrealdb_types::{SqlFormat, ToSql};

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
		self.expr.as_ref().map(|x| x.read_only()).unwrap_or(true)
			&& self.cond.as_ref().map(|x| x.0.read_only()).unwrap_or(true)
			&& self.split.as_ref().map(|x| x.read_only()).unwrap_or(true)
			&& self.group.as_ref().map(|x| x.read_only()).unwrap_or(true)
			&& self.order.as_ref().map(|x| x.read_only()).unwrap_or(true)
			&& self.limit.as_ref().map(|x| x.read_only()).unwrap_or(true)
			&& self.start.as_ref().map(|x| x.read_only()).unwrap_or(true)
			&& self.alias.as_ref().map(|x| x.read_only()).unwrap_or(true)
	}
}

impl ToSql for Lookup {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::lookup::Lookup = self.clone().into();
		stmt.fmt_sql(f, fmt);
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
		let stmt: crate::sql::lookup::LookupSubject = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
