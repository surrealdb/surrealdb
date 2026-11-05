use common::fmt::QuoteStr;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::AlterKind;
use crate::expr::permission::{FieldPermissions, Permissions};
use crate::expr::reference::Reference;
use crate::expr::statements::define::SqlType;
use crate::expr::{CoverStmts, Expr, KeyNames, KeyNamesError, Kind, KindLiteral, Literal};

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub enum AlterDefault {
	#[default]
	None,
	Drop,
	Always(Expr),
	Set(Expr),
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct AlterFieldStatement {
	pub name: Expr,
	pub what: Expr,
	pub if_exists: bool,
	pub kind: AlterKind<Kind>,
	/// The names of the components of the tuple type of an `id` field: `Set` with
	/// a named `TYPE`, `Drop` with an unnamed `TYPE` or `DROP TYPE`, `None` when
	/// the statement leaves the type alone. Change it through
	/// [`Self::set_type`].
	pub key_names: AlterKind<KeyNames>,
	pub flexible: AlterKind<()>,
	pub readonly: AlterKind<()>,
	/// Set or drop the `INLINE` clause.
	pub inline: AlterKind<()>,
	pub value: AlterKind<Expr>,
	pub assert: AlterKind<Expr>,
	pub default: AlterDefault,
	pub permissions: Option<Permissions>,
	pub comment: AlterKind<String>,
	pub reference: AlterKind<Reference>,
	/// `MIGRATE EXISTING ROWS …`: what the alteration does to stored rows.
	pub migrate: Option<crate::expr::statements::define::MigrateClause>,
	/// Set or drop the `SQL TYPE` clause.
	pub sql_type: AlterKind<SqlType>,
}

impl Default for AlterFieldStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			what: Expr::Literal(Literal::None),
			if_exists: false,
			kind: AlterKind::None,
			key_names: AlterKind::None,
			flexible: AlterKind::None,
			readonly: AlterKind::None,
			inline: AlterKind::None,
			value: AlterKind::None,
			assert: AlterKind::None,
			default: AlterDefault::None,
			permissions: None,
			comment: AlterKind::None,
			reference: AlterKind::None,
			migrate: None,
			sql_type: AlterKind::None,
		}
	}
}

impl AlterFieldStatement {
	/// Sets the type and the names of the components of the record id.
	///
	/// `Set` names must come with a `Set` tuple type of one element per name.
	/// The type of an unnamed `TYPE` or `DROP TYPE` carries `Drop` names, and a
	/// statement without a `TYPE` clause carries `None`.
	///
	/// # Errors
	///
	/// [`KeyNamesError::Arity`] when the names do not match the tuple. The
	/// statement is unchanged.
	pub fn set_type(
		&mut self,
		kind: AlterKind<Kind>,
		names: AlterKind<KeyNames>,
	) -> Result<(), KeyNamesError> {
		if let AlterKind::Set(names) = &names {
			let kinds = if let AlterKind::Set(Kind::Literal(KindLiteral::Array(kinds))) = &kind {
				kinds.len()
			} else {
				0
			};
			if kinds != names.len() {
				return Err(KeyNamesError::Arity {
					names: names.len(),
					kinds,
				});
			}
		}
		self.kind = kind;
		self.key_names = names;
		Ok(())
	}
}

impl ToSql for AlterFieldStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "ALTER FIELD");
		if self.if_exists {
			write_sql!(f, fmt, " IF EXISTS");
		}
		write_sql!(f, fmt, " {} ON {}", CoverStmts(&self.name), CoverStmts(&self.what));
		match self.kind {
			AlterKind::Set(ref x) => match (&self.key_names, x) {
				(AlterKind::Set(names), Kind::Literal(KindLiteral::Array(kinds)))
					if kinds.len() == names.len() =>
				{
					write_sql!(f, fmt, " TYPE {}", names.fmt_keyed(kinds));
				}
				(AlterKind::Set(_), _) => {
					debug_assert!(false, "component names do not match the type of the field");
					tracing::error!("component names do not match the type of the field");
					write_sql!(f, fmt, " TYPE {x}");
				}
				_ => write_sql!(f, fmt, " TYPE {x}"),
			},
			AlterKind::Drop => write_sql!(f, fmt, " DROP TYPE"),
			AlterKind::None => {}
		}
		match self.flexible {
			AlterKind::Set(_) => write_sql!(f, fmt, " FLEXIBLE"),
			AlterKind::Drop => write_sql!(f, fmt, " DROP FLEXIBLE"),
			AlterKind::None => {}
		}
		match self.readonly {
			AlterKind::Set(_) => write_sql!(f, fmt, " READONLY"),
			AlterKind::Drop => write_sql!(f, fmt, " DROP READONLY"),
			AlterKind::None => {}
		}
		match self.inline {
			AlterKind::Set(_) => write_sql!(f, fmt, " INLINE"),
			AlterKind::Drop => write_sql!(f, fmt, " DROP INLINE"),
			AlterKind::None => {}
		}
		match self.value {
			AlterKind::Set(ref x) => write_sql!(f, fmt, " VALUE {}", CoverStmts(x)),
			AlterKind::Drop => write_sql!(f, fmt, " DROP VALUE"),
			AlterKind::None => {}
		}
		match self.assert {
			AlterKind::Set(ref x) => write_sql!(f, fmt, " ASSERT {}", CoverStmts(x)),
			AlterKind::Drop => write_sql!(f, fmt, " DROP ASSERT"),
			AlterKind::None => {}
		}

		match self.default {
			AlterDefault::None => {}
			AlterDefault::Drop => write_sql!(f, fmt, " DROP DEFAULT"),
			AlterDefault::Always(ref d) => write_sql!(f, fmt, " DEFAULT ALWAYS {}", CoverStmts(d)),
			AlterDefault::Set(ref d) => write_sql!(f, fmt, " DEFAULT {}", CoverStmts(d)),
		}

		if let Some(permissions) = &self.permissions {
			write_sql!(f, fmt, " {}", FieldPermissions(permissions));
		}

		match self.comment {
			AlterKind::Set(ref x) => write_sql!(f, fmt, " COMMENT {}", QuoteStr(x)),
			AlterKind::Drop => write_sql!(f, fmt, " DROP COMMENT"),
			AlterKind::None => {}
		}
		match self.reference {
			AlterKind::Set(ref x) => write_sql!(f, fmt, " REFERENCE {x}"),
			AlterKind::Drop => write_sql!(f, fmt, " DROP REFERENCE"),
			AlterKind::None => {}
		}
		match self.sql_type {
			AlterKind::Set(ref x) => write_sql!(f, fmt, " {x}"),
			AlterKind::Drop => write_sql!(f, fmt, " DROP SQL TYPE"),
			AlterKind::None => {}
		}
		if let Some(ref migrate) = self.migrate {
			migrate.fmt_sql(f, fmt);
		}
	}
}
