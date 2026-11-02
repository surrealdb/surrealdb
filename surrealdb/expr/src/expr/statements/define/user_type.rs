//! A user-defined SQL type of a database: a PostgreSQL domain or composite type.
//!
//! The SQL front ends define these types; SurrealQL has no syntax for them. A column declared with
//! one is a field whose kind, `ASSERT` and `DEFAULT` the front end derives from the type, and
//! whose `SQL TYPE` names the type, so the catalog keeps the type itself only for what reads it by
//! name: the lowering of a later column, the catalog views, and the wire types.

use common::fmt::{EscapeKwFreeIdent, Fmt, QuoteStr};
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::DefineKind;
use crate::expr::{CoverStmts, Expr, Kind};

/// Defines a user-defined SQL type of the selected database.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DefineUserTypeStatement {
	pub kind: DefineKind,
	pub name: Strand,
	pub body: UserTypeBody,
}

/// What a user-defined SQL type is.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum UserTypeBody {
	Domain(DomainBody),
	Composite(CompositeBody),
}

impl UserTypeBody {
	/// The kind of type, as the statements that name it say it.
	pub fn type_kind(&self) -> UserTypeKind {
		match self {
			Self::Domain(_) => UserTypeKind::Domain,
			Self::Composite(_) => UserTypeKind::Composite,
		}
	}
}

/// Which kind of user-defined SQL type a statement names.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum UserTypeKind {
	Domain,
	Composite,
}

/// A domain: a base type with the `NOT NULL`, `DEFAULT` and checks that a column of the domain
/// takes.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DomainBody {
	/// The kind of the base type, which a column of the domain holds.
	pub base: Kind,
	/// The base type as the PostgreSQL front end declares it, in its canonical text.
	pub base_type: String,
	/// Whether a column of the domain refuses NULL.
	pub not_null: bool,
	/// The value a column of the domain takes when a write gives it none.
	pub default: Option<Expr>,
	/// The checks, in declaration order. Each reads the value under test as `$value` and holds
	/// unless it is false.
	pub checks: Vec<DomainCheck>,
}

/// One check of a domain, with the constraint name it was declared with, when it was.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DomainCheck {
	pub name: Option<String>,
	pub check: Expr,
}

/// A composite type: named attributes in declaration order.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct CompositeBody {
	pub fields: Vec<CompositeField>,
}

/// One attribute of a composite type.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct CompositeField {
	pub name: String,
	/// The kind a value of the attribute has; it admits NULL, as every attribute does.
	pub kind: Kind,
	/// The attribute's type as the PostgreSQL front end declares it, in its canonical text.
	pub sql_type: String,
}

/// The text of a user-defined type for people: what `INFO` and the logs show. No SurrealQL
/// statement reads it back.
impl ToSql for DefineUserTypeStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let word = match self.body {
			UserTypeBody::Domain(_) => "DOMAIN",
			UserTypeBody::Composite(_) => "COMPOSITE",
		};
		write_sql!(f, fmt, "DEFINE {word}");
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write_sql!(f, fmt, " OVERWRITE"),
			DefineKind::IfNotExists => write_sql!(f, fmt, " IF NOT EXISTS"),
		}
		write_sql!(f, fmt, " {}", EscapeKwFreeIdent(self.name.as_str()));
		match &self.body {
			UserTypeBody::Domain(domain) => {
				write_sql!(
					f,
					fmt,
					" TYPE {} SQL TYPE pgsql {}",
					domain.base,
					QuoteStr(&domain.base_type)
				);
				if domain.not_null {
					write_sql!(f, fmt, " NOT NULL");
				}
				if let Some(default) = &domain.default {
					write_sql!(f, fmt, " DEFAULT {}", CoverStmts(default));
				}
				for check in &domain.checks {
					write_sql!(f, fmt, " CHECK");
					if let Some(name) = &check.name {
						write_sql!(f, fmt, " {}", EscapeKwFreeIdent(name));
					}
					write_sql!(f, fmt, " {}", CoverStmts(&check.check));
				}
			}
			UserTypeBody::Composite(composite) => {
				write_sql!(
					f,
					fmt,
					" FIELDS {}",
					Fmt::comma_separated(composite.fields.iter().map(|field| {
						Fmt::new(field, |field, f, fmt| {
							write_sql!(
								f,
								fmt,
								"{}: {} SQL TYPE pgsql {}",
								EscapeKwFreeIdent(&field.name),
								field.kind,
								QuoteStr(&field.sql_type)
							)
						})
					}))
				);
			}
		}
	}
}
