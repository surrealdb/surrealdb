#![deny(clippy::wildcard_enum_match_arm)]

use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::DefineKind;
use crate::expr::permission::{FieldPermissions, Permissions};
use crate::expr::reference::Reference;
use crate::expr::{CoverStmts, Expr, KeyNames, KeyNamesError, Kind, KindLiteral, Literal};

/// Returns true if this type contains an `object` anywhere (including literal
/// object types, `array<object>` and `option<object>`).
pub fn kind_contains_object(kind: &Kind) -> bool {
	match kind {
		// What a named type holds is the catalog's to say, which this does not read.
		Kind::Named(_) => false,
		Kind::Object | Kind::Literal(KindLiteral::Object(_)) => true,
		Kind::Either(kinds) | Kind::Literal(KindLiteral::Array(kinds)) => {
			kinds.iter().any(kind_contains_object)
		}
		Kind::Array(inner, _) | Kind::Set(inner, _) => kind_contains_object(inner),
		Kind::Any
		| Kind::None
		| Kind::Null
		| Kind::Bool
		| Kind::Bytes
		| Kind::Datetime
		| Kind::Date
		| Kind::Time
		| Kind::Timestamp
		| Kind::Interval
		| Kind::Decimal
		| Kind::BigDecimal
		| Kind::Duration
		| Kind::Float
		| Kind::Int
		| Kind::I8
		| Kind::U8
		| Kind::I16
		| Kind::U16
		| Kind::I32
		| Kind::U32
		| Kind::U64
		| Kind::F16
		| Kind::F32
		| Kind::Number
		| Kind::String
		| Kind::Uuid
		| Kind::Regex
		| Kind::Table(_)
		| Kind::Record(_)
		| Kind::Geometry(_)
		| Kind::Function(_, _)
		| Kind::Range
		| Kind::Literal(
			KindLiteral::String(_)
			| KindLiteral::Integer(_)
			| KindLiteral::Float(_)
			| KindLiteral::Decimal(_)
			| KindLiteral::Duration(_)
			| KindLiteral::Bool(_),
		)
		| Kind::File(_)
		| Kind::Enum(_) => false,
	}
}

/// The SQL dialect a `SQL TYPE` clause names.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum SqlTypeDialect {
	PostgreSql,
	MySql,
	DynamoDb,
}

impl SqlTypeDialect {
	/// The keyword that spells the dialect in SurrealQL.
	pub fn keyword(self) -> &'static str {
		match self {
			SqlTypeDialect::PostgreSql => "POSTGRESQL",
			SqlTypeDialect::MySql => "MYSQL",
			SqlTypeDialect::DynamoDb => "DYNAMODB",
		}
	}
}

/// The declared SQL type of a field: the type as the named dialect spells it,
/// such as `numeric(10,2)` for PostgreSQL. The text is stored as written; the
/// declaration does not change the field's kind or what a write accepts.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct SqlType {
	pub dialect: SqlTypeDialect,
	pub text: String,
}

impl ToSql for SqlType {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		write_sql!(
			f,
			sql_fmt,
			"SQL TYPE {} {}",
			self.dialect.keyword(),
			common::fmt::QuoteStr(&self.text)
		);
	}
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub enum DefineDefault {
	#[default]
	None,
	Always(Expr),
	Set(Expr),
}

/// `MIGRATE EXISTING ROWS AS <name> USING <expr> [FALLBACK <expr>]`: what a
/// field definition does to the rows stored before it.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct MigrateClause {
	/// The migration's name, unique per table.
	pub name: String,
	/// The transform, over `$value`.
	pub using: Expr,
	/// The constant stored where the transform fails or its result does not
	/// pass the definition.
	pub fallback: Option<Expr>,
}

impl ToSql for MigrateClause {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		write_sql!(
			f,
			sql_fmt,
			" MIGRATE EXISTING ROWS AS {} USING {}",
			common::fmt::EscapeIdent(&self.name),
			CoverStmts(&self.using)
		);
		if let Some(ref fallback) = self.fallback {
			write_sql!(f, sql_fmt, " FALLBACK {}", CoverStmts(fallback));
		}
	}
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
	/// `MIGRATE EXISTING ROWS …`: what the definition does to stored rows.
	pub migrate: Option<MigrateClause>,
	/// The declared SQL type, the `SQL TYPE` clause.
	pub sql_type: Option<SqlType>,
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
			migrate: None,
			sql_type: None,
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
		if let Some(ref sql_type) = self.sql_type {
			write_sql!(f, sql_fmt, " {sql_type}");
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
		if let Some(ref migrate) = self.migrate {
			migrate.fmt_sql(f, sql_fmt);
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
