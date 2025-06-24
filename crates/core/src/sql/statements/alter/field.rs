use crate::sql::reference::Reference;
use crate::sql::{Expr, Ident, Idiom, Kind, Permissions, Strand};

use std::fmt::{self, Display};

use super::AlterKind;

pub enum AlterDefault {
	None,
	Drop,
	Always(Expr),
	Set(Expr),
}

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Hash)]
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
		if let Some(flex) = self.flex {
			if flex {
				write!(f, " FLEXIBLE")?;
			} else {
				write!(f, " DROP FLEXIBLE")?;
			}
		}
		if let Some(kind) = &self.kind {
			if let Some(kind) = kind {
				write!(f, " TYPE {kind}")?;
			} else {
				write!(f, " DROP TYPE")?;
			}
		}
		if let Some(readonly) = self.readonly {
			if readonly {
				write!(f, " READONLY")?;
			} else {
				write!(f, " DROP READONLY")?;
			}
		}
		if let Some(value) = &self.value {
			if let Some(value) = value {
				write!(f, " VALUE {value}")?;
			} else {
				write!(f, " DROP VALUE")?;
			}
		}
		if let Some(assert) = &self.assert {
			if let Some(assert) = assert {
				write!(f, " ASSERT {assert}")?;
			} else {
				write!(f, " DROP ASSERT")?;
			}
		}
		if let Some(default) = &self.default {
			if let Some(default) = default {
				write!(f, " DEFAULT")?;
				if self.default_always.is_some_and(|x| x) {
					write!(f, " ALWAYS")?;
				}

				write!(f, " {default}")?;
			} else {
				write!(f, " DROP DEFAULT")?;
			}
		}
		if let Some(permissions) = &self.permissions {
			write!(f, "{permissions}")?;
		}
		if let Some(comment) = &self.comment {
			if let Some(comment) = comment {
				write!(f, " COMMENT {comment}")?;
			} else {
				write!(f, " DROP COMMENT")?;
			}
		}
		if let Some(reference) = &self.reference {
			if let Some(reference) = reference {
				write!(f, " REFERENCE {reference}")?;
			} else {
				write!(f, " DROP REFERENCE")?;
			}
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
			flex: v.flex,
			kind: v.kind.map(|opt| opt.map(Into::into)),
			readonly: v.readonly,
			value: v.value.map(|opt| opt.map(Into::into)),
			assert: v.assert.map(|opt| opt.map(Into::into)),
			default: v.default.map(|opt| opt.map(Into::into)),
			permissions: v.permissions.map(Into::into),
			comment: v.comment.map(|opt| opt.map(Into::into)),
			reference: v.reference.map(|opt| opt.map(Into::into)),
		}
	}
}

impl From<crate::expr::statements::alter::AlterFieldStatement> for AlterFieldStatement {
	fn from(v: crate::expr::statements::alter::AlterFieldStatement) -> Self {
		AlterFieldStatement {
			name: v.name.into(),
			what: v.what.into(),
			if_exists: v.if_exists,
			flex: v.flex,
			kind: v.kind.map(|opt| opt.map(Into::into)),
			readonly: v.readonly,
			value: v.value.map(|opt| opt.map(Into::into)),
			assert: v.assert.map(|opt| opt.map(Into::into)),
			default: v.default.map(|opt| opt.map(Into::into)),
			permissions: v.permissions.map(Into::into),
			comment: v.comment.map(|opt| opt.map(Into::into)),
			reference: v.reference.map(|opt| opt.map(Into::into)),
		}
	}
}
