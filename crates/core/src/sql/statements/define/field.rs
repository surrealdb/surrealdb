use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::DefineKind;
use crate::fmt::CoverStmts;
use crate::sql::reference::Reference;
use crate::sql::{Expr, Kind, Literal, Permissions};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) enum DefineDefault {
	#[default]
	None,
	Always(Expr),
	Set(Expr),
}

impl From<DefineDefault> for crate::expr::statements::define::DefineDefault {
	fn from(value: DefineDefault) -> Self {
		match value {
			DefineDefault::None => crate::expr::statements::define::DefineDefault::None,
			DefineDefault::Always(expr) => {
				crate::expr::statements::define::DefineDefault::Always(expr.into())
			}
			DefineDefault::Set(expr) => {
				crate::expr::statements::define::DefineDefault::Set(expr.into())
			}
		}
	}
}

impl From<crate::expr::statements::define::DefineDefault> for DefineDefault {
	fn from(value: crate::expr::statements::define::DefineDefault) -> Self {
		match value {
			crate::expr::statements::define::DefineDefault::None => DefineDefault::None,
			crate::expr::statements::define::DefineDefault::Always(expr) => {
				DefineDefault::Always(expr.into())
			}
			crate::expr::statements::define::DefineDefault::Set(expr) => {
				DefineDefault::Set(expr.into())
			}
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DefineFieldStatement {
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
		if sql_fmt.is_pretty() {
			f.push('\n');
			sql_fmt.write_indent(f);
		} else {
			f.push(' ');
		}
		self.permissions.fmt_sql(f, sql_fmt);
	}
}

impl From<DefineFieldStatement> for crate::expr::statements::DefineFieldStatement {
	fn from(v: DefineFieldStatement) -> Self {
		Self {
			kind: v.kind.into(),
			name: v.name.into(),
			what: v.what.into(),
			readonly: v.readonly,
			field_kind: v.field_kind.map(Into::into),
			flexible: v.flexible,
			value: v.value.map(Into::into),
			assert: v.assert.map(Into::into),
			computed: v.computed.map(Into::into),
			default: v.default.into(),
			permissions: v.permissions.into(),
			comment: v.comment.into(),
			reference: v.reference.map(Into::into),
		}
	}
}

#[allow(clippy::fallible_impl_from)]
impl From<crate::expr::statements::DefineFieldStatement> for DefineFieldStatement {
	fn from(v: crate::expr::statements::DefineFieldStatement) -> Self {
		Self {
			kind: v.kind.into(),
			name: v.name.into(),
			what: v.what.into(),
			readonly: v.readonly,
			field_kind: v.field_kind.map(Into::into),
			flexible: v.flexible,
			value: v.value.map(Into::into),
			assert: v.assert.map(Into::into),
			computed: v.computed.map(Into::into),
			default: v.default.into(),
			permissions: v.permissions.into(),
			comment: v.comment.into(),
			reference: v.reference.map(Into::into),
		}
	}
}
