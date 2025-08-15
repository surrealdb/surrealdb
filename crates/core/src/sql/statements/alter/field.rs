use std::fmt::{self, Display};

use super::AlterKind;
use crate::sql::reference::Reference;
use crate::sql::{Expr, Ident, Idiom, Kind, Permissions};
use crate::val::Strand;

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
	pub what: Ident,
	pub if_exists: bool,
	pub flex: AlterKind<()>,
	pub kind: AlterKind<Kind>,
	pub readonly: AlterKind<()>,
	pub value: AlterKind<Expr>,
	pub assert: AlterKind<Expr>,
	pub default: AlterDefault,
	pub permissions: Option<Permissions>,
	pub comment: AlterKind<Strand>,
	pub reference: AlterKind<Reference>,
}

impl Display for AlterFieldStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "ALTER FIELD")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {} ON {}", self.name, self.what)?;
		match self.flex {
			AlterKind::Set(_) => write!(f, " FLEXIBLE")?,
			AlterKind::Drop => write!(f, " DROP FLEXIBLE")?,
			AlterKind::None => {}
		}
		match self.kind {
			AlterKind::Set(ref x) => write!(f, " TYPE {x}")?,
			AlterKind::Drop => write!(f, " DROP TYPE")?,
			AlterKind::None => {}
		}
		match self.readonly {
			AlterKind::Set(_) => write!(f, " READONLY")?,
			AlterKind::Drop => write!(f, " DROP READONLY")?,
			AlterKind::None => {}
		}
		match self.value {
			AlterKind::Set(ref x) => write!(f, " VALUE {x}")?,
			AlterKind::Drop => write!(f, " DROP VALUE")?,
			AlterKind::None => {}
		}
		match self.assert {
			AlterKind::Set(ref x) => write!(f, " ASSERT {x}")?,
			AlterKind::Drop => write!(f, " DROP ASSERT")?,
			AlterKind::None => {}
		}

		match self.default {
			AlterDefault::None => {}
			AlterDefault::Drop => write!(f, "DROP DEFAULT")?,
			AlterDefault::Always(ref d) => write!(f, "DEFAULT ALWAYS {d}")?,
			AlterDefault::Set(ref d) => write!(f, "DEFAULT {d}")?,
		}

		if let Some(permissions) = &self.permissions {
			write!(f, "{permissions}")?;
		}

		match self.comment {
			AlterKind::Set(ref x) => write!(f, " COMMENT {x}")?,
			AlterKind::Drop => write!(f, " DROP COMMENT")?,
			AlterKind::None => {}
		}
		match self.reference {
			AlterKind::Set(ref x) => write!(f, " REFERENCE {x}")?,
			AlterKind::Drop => write!(f, " DROP REFERENCE")?,
			AlterKind::None => {}
		}
		Ok(())
	}
}

impl From<AlterFieldStatement> for crate::expr::statements::alter::AlterFieldStatement {
	fn from(v: AlterFieldStatement) -> Self {
		crate::expr::statements::alter::AlterFieldStatement {
			name: v.name.into(),
			what: v.what.into(),
			if_exists: v.if_exists,
			flex: v.flex.into(),
			kind: v.kind.into(),
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
			what: v.what.into(),
			if_exists: v.if_exists,
			flex: v.flex.into(),
			kind: v.kind.into(),
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
