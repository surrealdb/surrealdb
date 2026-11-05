//! `DEFINE TYPE`: a named type of the selected database.
//!
//! A type's body is a kind. A body that is a union of string literals (`'sad' | 'ok' | 'happy'`)
//! is an ordered set of labels, which `ALTER TYPE` edits and by whose declaration order the
//! `enum::*` functions and an `ORDER BY` over a field of the type rank a value. Any other body is
//! a kind a value is coerced to. Each `ASSERT` judges the coerced value as `$value`, in
//! declaration order; `DEFAULT` is the value a field of the type takes when a write gives it none
//! and the field declares no default of its own.

use common::fmt::{EscapeKwFreeIdent, QuoteStr};
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::DefineKind;
use crate::expr::permission::Permission;
use crate::expr::{CoverStmts, Expr, Kind, KindLiteral, Literal};

/// `DEFINE TYPE name AS kind [[CONSTRAINT name] ASSERT expr]… [DEFAULT expr]
/// [SQL TYPE POSTGRESQL 'text'] [COMMENT expr] [PERMISSIONS …]`.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DefineTypeStatement {
	pub kind: DefineKind,
	pub name: Strand,
	/// The kind a value of the type has.
	pub body: Kind,
	/// The checks, in declaration order.
	pub asserts: Vec<TypeAssert>,
	/// The value a field of the type takes when a write gives it none.
	pub default: Option<Expr>,
	/// The type as the PostgreSQL front end declares it, opaque to SurrealQL.
	pub sql_type: Option<String>,
	pub comment: Expr,
	pub permissions: Permission,
}

impl Default for DefineTypeStatement {
	fn default() -> Self {
		Self {
			kind: DefineKind::Default,
			name: Strand::default(),
			body: Kind::Any,
			asserts: Vec::new(),
			default: None,
			sql_type: None,
			comment: Expr::Literal(Literal::None),
			permissions: Permission::default(),
		}
	}
}

/// One check of a type: an expression over `$value` that holds unless it is false, with the
/// constraint name it was declared with, when it was.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct TypeAssert {
	pub name: Option<String>,
	pub check: Expr,
}

/// The labels of `body`, in declaration order, when it is a union of string literals; `None` for
/// any other kind.
pub fn union_labels(body: &Kind) -> Option<Vec<String>> {
	let label = |kind: &Kind| {
		if let Kind::Literal(KindLiteral::String(label)) = kind {
			Some(label.as_str().to_owned())
		} else {
			None
		}
	};
	if let Kind::Either(members) = body {
		members.iter().map(label).collect()
	} else {
		label(body).map(|label| vec![label])
	}
}

/// The kind a union of `labels` is: each label a string literal kind, in declaration order.
pub fn union_kind<'a>(labels: impl IntoIterator<Item = &'a str>) -> Kind {
	Kind::either(
		labels
			.into_iter()
			.map(|label| Kind::Literal(KindLiteral::String(Strand::new(label))))
			.collect(),
	)
}

impl ToSql for DefineTypeStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "DEFINE TYPE");
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write_sql!(f, fmt, " OVERWRITE"),
			DefineKind::IfNotExists => write_sql!(f, fmt, " IF NOT EXISTS"),
		}
		write_sql!(f, fmt, " {} AS {}", EscapeKwFreeIdent(self.name.as_str()), self.body);
		for assert in &self.asserts {
			if let Some(name) = &assert.name {
				write_sql!(f, fmt, " CONSTRAINT {}", EscapeKwFreeIdent(name));
			}
			write_sql!(f, fmt, " ASSERT {}", CoverStmts(&assert.check));
		}
		if let Some(default) = &self.default {
			write_sql!(f, fmt, " DEFAULT {}", CoverStmts(default));
		}
		if let Some(sql_type) = &self.sql_type {
			write_sql!(f, fmt, " SQL TYPE POSTGRESQL {}", QuoteStr(sql_type));
		}
		if !matches!(self.comment, Expr::Literal(Literal::None)) {
			write_sql!(f, fmt, " COMMENT {}", CoverStmts(&self.comment));
		}
		write_sql!(f, fmt, " PERMISSIONS {}", self.permissions);
	}
}
