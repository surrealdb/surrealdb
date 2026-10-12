use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::DefineKind;
use crate::expr::permission::{FieldPermissions, Permissions};
use crate::expr::reference::Reference;
use crate::expr::{CoverStmts, Expr, KeyNames, KeyNamesError, Kind, KindLiteral, Literal};

/// Returns true if this type contains an `object` anywhere (including literal
/// object types, `array<object>` and `option<object>`).
pub fn kind_contains_object(kind: &Kind) -> bool {
	match kind {
		Kind::Object => true,
		Kind::Either(kinds) => kinds.iter().any(kind_contains_object),
		Kind::Array(inner, _) | Kind::Set(inner, _) => kind_contains_object(inner),
		Kind::Literal(KindLiteral::Object(_)) => true,
		Kind::Literal(KindLiteral::Array(kinds)) => kinds.iter().any(kind_contains_object),
		_ => false,
	}
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub enum DefineDefault {
	#[default]
	None,
	Always(Expr),
	Set(Expr),
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DefineFieldStatement {
	pub kind: DefineKind,
	pub name: Expr,
	pub what: Expr,
	pub field_kind: Option<Kind>,
	/// The names of the components of the tuple type of an `id` field. Some only
	/// when `field_kind` is a tuple of as many elements; set it through
	/// [`Self::set_type`].
	pub key_names: Option<KeyNames>,
	pub flexible: bool,
	pub readonly: bool,
	/// Inline the field's value into the edge's adjacency (the `INLINE` clause).
	pub inline: bool,
	pub value: Option<Expr>,
	pub assert: Option<Expr>,
	pub computed: Option<Expr>,
	pub default: DefineDefault,
	pub permissions: Permissions,
	pub comment: Expr,
	pub reference: Option<Reference>,
	pub graphql_alias: Option<String>,
	pub graphql_deprecated: Option<String>,
}

impl Default for DefineFieldStatement {
	fn default() -> Self {
		Self {
			kind: DefineKind::Default,
			name: Expr::Literal(Literal::None),
			what: Expr::Literal(Literal::None),
			field_kind: None,
			key_names: None,
			flexible: false,
			readonly: false,
			inline: false,
			value: None,
			assert: None,
			computed: None,
			default: DefineDefault::None,
			permissions: Permissions::default(),
			comment: Expr::Literal(Literal::None),
			reference: None,
			graphql_alias: None,
			graphql_deprecated: None,
		}
	}
}

impl DefineFieldStatement {
	/// Sets the type of the field and the names of its record id components.
	///
	/// `names` is Some only for the whole type of an `id` field, which must then
	/// be a tuple with one element per name.
	///
	/// # Errors
	///
	/// [`KeyNamesError::Arity`] when `names` is Some and `kind` is not a tuple of
	/// as many elements. The statement is unchanged.
	pub fn set_type(
		&mut self,
		kind: Option<Kind>,
		names: Option<KeyNames>,
	) -> Result<(), KeyNamesError> {
		if let Some(names) = &names {
			let kinds = match &kind {
				Some(Kind::Literal(KindLiteral::Array(kinds))) => kinds.len(),
				_ => 0,
			};
			if kinds != names.len() {
				return Err(KeyNamesError::Arity {
					names: names.len(),
					kinds,
				});
			}
		}
		self.field_kind = kind;
		self.key_names = names;
		Ok(())
	}
}

impl ToSql for DefineFieldStatement {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		f.push_str("DEFINE FIELD");
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => f.push_str(" OVERWRITE"),
			DefineKind::IfNotExists => f.push_str(" IF NOT EXISTS"),
		}
		write_sql!(f, sql_fmt, " {} ON {}", CoverStmts(&self.name), CoverStmts(&self.what));
		if let Some(ref v) = self.field_kind {
			match (&self.key_names, v) {
				(Some(names), Kind::Literal(KindLiteral::Array(kinds)))
					if kinds.len() == names.len() =>
				{
					write_sql!(f, sql_fmt, " TYPE {}", names.fmt_keyed(kinds));
				}
				(Some(_), _) => {
					debug_assert!(false, "component names do not match the type of the field");
					tracing::error!("component names do not match the type of the field");
					write_sql!(f, sql_fmt, " TYPE {}", v);
				}
				(None, _) => write_sql!(f, sql_fmt, " TYPE {}", v),
			}
			if self.flexible {
				f.push_str(" FLEXIBLE");
			}
		}
		match self.default {
			DefineDefault::None => {}
			DefineDefault::Always(ref expr) => {
				write_sql!(f, sql_fmt, " DEFAULT ALWAYS {}", CoverStmts(expr));
			}
			DefineDefault::Set(ref expr) => {
				write_sql!(f, sql_fmt, " DEFAULT {}", CoverStmts(expr));
			}
		}
		if self.readonly {
			f.push_str(" READONLY");
		}
		if self.inline {
			f.push_str(" INLINE");
		}
		if let Some(ref v) = self.value {
			write_sql!(f, sql_fmt, " VALUE {}", CoverStmts(v))
		}
		if let Some(ref v) = self.assert {
			write_sql!(f, sql_fmt, " ASSERT {}", CoverStmts(v))
		}
		if let Some(ref v) = self.computed {
			write_sql!(f, sql_fmt, " COMPUTED {}", CoverStmts(v))
		}
		if let Some(ref v) = self.reference {
			write_sql!(f, sql_fmt, " REFERENCE {v}");
		}
		if !matches!(self.comment, Expr::Literal(Literal::None)) {
			write_sql!(f, sql_fmt, " COMMENT {}", CoverStmts(&self.comment));
		}
		if let Some(ref alias) = self.graphql_alias {
			write_sql!(f, sql_fmt, " GRAPHQL_ALIAS {}", common::fmt::QuoteStr(alias));
		}
		if let Some(ref reason) = self.graphql_deprecated {
			write_sql!(f, sql_fmt, " GRAPHQL_DEPRECATED {}", common::fmt::QuoteStr(reason));
		}
		if sql_fmt.is_pretty() {
			f.push('\n');
			sql_fmt.write_indent(f);
		} else {
			f.push(' ');
		}
		FieldPermissions(&self.permissions).fmt_sql(f, sql_fmt);
	}
}
