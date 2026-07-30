//! `sql` -> `expr` conversions for [`crate::sql::lookup`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::lookup::*;

impl From<Lookup> for crate::expr::Lookup {
	fn from(v: Lookup) -> Self {
		Self {
			kind: v.kind.into(),
			expr: v.expr.map(From::from),
			only: v.only,
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
			only: v.only,
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

impl From<LookupSubject> for crate::expr::lookup::LookupSubject {
	fn from(v: LookupSubject) -> Self {
		match v {
			LookupSubject::Table {
				table,
				referencing_field,
			} => Self::Table {
				table: table.into(),
				referencing_field,
			},
			LookupSubject::Range {
				table,
				range,
				referencing_field,
			} => Self::Range {
				table: table.into(),
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
				table: table.into(),
				referencing_field,
			},
			crate::expr::lookup::LookupSubject::Range {
				table,
				range,
				referencing_field,
			} => Self::Range {
				table: table.into(),
				range: range.into(),
				referencing_field,
			},
		}
	}
}
