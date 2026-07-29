use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::DefineKind;
use crate::reference::Reference;
use crate::{CoverStmts, Expr, Kind, Literal, Permissions};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum DefineDefault {
	#[default]
	None,
	Always(Expr),
	Set(Expr),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DefineFieldStatement {
	pub kind: DefineKind,
	pub name: Expr,
	pub what: Expr,
	pub field_kind: Option<Kind>,
	pub flexible: bool,
	pub readonly: bool,
	pub value: Option<Expr>,
	pub assert: Option<Expr>,
	pub computed: Option<Expr>,
	pub default: DefineDefault,
	pub permissions: Permissions,
	pub comment: Expr,
	pub reference: Option<Reference>,
	/// Optional GraphQL alias declared via `GRAPHQL_ALIAS "..."`.
	pub graphql_alias: Option<String>,
	/// Optional GraphQL deprecation reason declared via
	/// `GRAPHQL_DEPRECATED "..."`.
	pub graphql_deprecated: Option<String>,
}

impl Default for DefineFieldStatement {
	fn default() -> Self {
		Self {
			kind: DefineKind::Default,
			name: Expr::Literal(Literal::None),
			what: Expr::Literal(Literal::None),
			field_kind: None,
			flexible: false,
			readonly: false,
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
			write_sql!(f, sql_fmt, " TYPE {}", v);
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
		self.permissions.fmt_sql(f, sql_fmt);
	}
}
