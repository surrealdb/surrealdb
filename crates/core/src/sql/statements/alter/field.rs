use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::AlterKind;
use crate::fmt::{EscapeIdent, QuoteStr};
use crate::sql::reference::Reference;
use crate::sql::{Expr, Idiom, Kind, Permissions};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum AlterDefault {
	#[default]
	None,
	Drop,
	Always(Expr),
	Set(Expr),
}

impl From<crate::expr::statements::alter::AlterDefault> for AlterDefault {
	fn from(value: crate::expr::statements::alter::AlterDefault) -> Self {
		match value {
			crate::expr::statements::alter::AlterDefault::None => AlterDefault::None,
			crate::expr::statements::alter::AlterDefault::Drop => AlterDefault::Drop,
			crate::expr::statements::alter::AlterDefault::Always(expr) => {
				AlterDefault::Always(expr.into())
			}
			crate::expr::statements::alter::AlterDefault::Set(expr) => {
				AlterDefault::Set(expr.into())
			}
		}
	}
}

impl From<AlterDefault> for crate::expr::statements::alter::AlterDefault {
	fn from(value: AlterDefault) -> Self {
		match value {
			AlterDefault::None => crate::expr::statements::alter::AlterDefault::None,
			AlterDefault::Drop => crate::expr::statements::alter::AlterDefault::Drop,
			AlterDefault::Always(expr) => {
				crate::expr::statements::alter::AlterDefault::Always(expr.into())
			}
			AlterDefault::Set(expr) => {
				crate::expr::statements::alter::AlterDefault::Set(expr.into())
			}
		}
	}
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct AlterFieldStatement {
	pub name: Idiom,
	pub what: String,
	pub if_exists: bool,
	pub kind: AlterKind<Kind>,
	pub flexible: AlterKind<()>,
	pub readonly: AlterKind<()>,
	pub value: AlterKind<Expr>,
	pub assert: AlterKind<Expr>,
	pub default: AlterDefault,
	pub permissions: Option<Permissions>,
	pub comment: AlterKind<String>,
	pub reference: AlterKind<Reference>,
}

impl ToSql for AlterFieldStatement {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		f.push_str("ALTER FIELD");
		if self.if_exists {
			f.push_str(" IF EXISTS");
		}
		write_sql!(f, sql_fmt, " {} ON {}", self.name, EscapeIdent(&self.what));
		match self.kind {
			AlterKind::Set(ref x) => write_sql!(f, sql_fmt, " TYPE {}", x),
			AlterKind::Drop => f.push_str(" DROP TYPE"),
			AlterKind::None => {}
		}
		match self.flexible {
			AlterKind::Set(_) => f.push_str(" FLEXIBLE"),
			AlterKind::Drop => f.push_str(" DROP FLEXIBLE"),
			AlterKind::None => {}
		}
		match self.readonly {
			AlterKind::Set(_) => f.push_str(" READONLY"),
			AlterKind::Drop => f.push_str(" DROP READONLY"),
			AlterKind::None => {}
		}
		match self.value {
			AlterKind::Set(ref x) => {
				write_sql!(f, sql_fmt, "VALUE {x}");
			}
			AlterKind::Drop => f.push_str(" DROP VALUE"),
			AlterKind::None => {}
		}
		match self.assert {
			AlterKind::Set(ref x) => {
				write_sql!(f, sql_fmt, "ASSERT {x}");
			}
			AlterKind::Drop => f.push_str(" DROP ASSERT"),
			AlterKind::None => {}
		}

		match self.default {
			AlterDefault::None => {}
			AlterDefault::Drop => f.push_str("DROP DEFAULT"),
			AlterDefault::Always(ref d) => {
				write_sql!(f, sql_fmt, "DEFAULT ALWAYS {d}");
			}
			AlterDefault::Set(ref d) => {
				write_sql!(f, sql_fmt, "DEFAULT {d}");
			}
		}

		if let Some(permissions) = &self.permissions {
			write_sql!(f, sql_fmt, " {permissions}");
		}

		match self.comment {
			AlterKind::Set(ref x) => write_sql!(f, sql_fmt, " COMMENT {}", QuoteStr(x)),
			AlterKind::Drop => f.push_str(" DROP COMMENT"),
			AlterKind::None => {}
		}
		match self.reference {
			AlterKind::Set(ref x) => {
				write_sql!(f, sql_fmt, "REFERENCE {x}");
			}
			AlterKind::Drop => f.push_str(" DROP REFERENCE"),
			AlterKind::None => {}
		}
	}
}

impl From<AlterFieldStatement> for crate::expr::statements::alter::AlterFieldStatement {
	fn from(v: AlterFieldStatement) -> Self {
		crate::expr::statements::alter::AlterFieldStatement {
			name: v.name.into(),
			what: v.what,
			if_exists: v.if_exists,
			kind: v.kind.into(),
			flexible: v.flexible.into(),
			readonly: v.readonly.into(),
			value: v.value.into(),
			assert: v.assert.into(),
			default: v.default.into(),
			permissions: v.permissions.map(Into::into),
			comment: v.comment.into(),
			reference: v.reference.into(),
		}
	}
}

impl From<crate::expr::statements::alter::AlterFieldStatement> for AlterFieldStatement {
	fn from(v: crate::expr::statements::alter::AlterFieldStatement) -> Self {
		AlterFieldStatement {
			name: v.name.into(),
			what: v.what,
			if_exists: v.if_exists,
			kind: v.kind.into(),
			flexible: v.flexible.into(),
			readonly: v.readonly.into(),
			value: v.value.into(),
			assert: v.assert.into(),
			default: v.default.into(),
			permissions: v.permissions.map(Into::into),
			comment: v.comment.into(),
			reference: v.reference.into(),
		}
	}
}
