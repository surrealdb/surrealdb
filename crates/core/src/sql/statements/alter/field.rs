use crate::sql::reference::Reference;
use crate::sql::{Ident, Permissions, Strand, Value};
use crate::sql::{Idiom, Kind};

use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct AlterFieldStatement {
	pub name: Idiom,
	pub what: Ident,
	pub if_exists: bool,
	pub flex: Option<bool>,
	pub kind: Option<Option<Kind>>,
	pub readonly: Option<bool>,
	pub value: Option<Option<Value>>,
	pub assert: Option<Option<Value>>,
	pub default: Option<Option<Value>>,
	pub permissions: Option<Permissions>,
	pub comment: Option<Option<Strand>>,
	pub reference: Option<Option<Reference>>,
	pub default_always: Option<bool>,
}

impl From<AlterFieldStatement> for crate::expr::statements::alter::AlterFieldStatement {
	fn from(v: AlterFieldStatement) -> Self {
		crate::expr::statements::alter::AlterFieldStatement {
			name: v.name.into(),
			what: v.what.into(),
			if_exists: v.if_exists,
			flex: v.flex.map(Into::into),
			kind: v.kind.map(|opt| opt.map(Into::into)),
			readonly: v.readonly,
			value: v.value.map(|opt| opt.map(Into::into)),
			assert: v.assert.map(|opt| opt.map(Into::into)),
			default: v.default.map(|opt| opt.map(Into::into)),
			permissions: v.permissions.map(Into::into),
			comment: v.comment.map(|opt| opt.map(Into::into)),
			reference: v.reference.map(|opt| opt.map(Into::into)),
			default_always: v.default_always,
		}
	}
}

impl From<crate::expr::statements::alter::AlterFieldStatement> for AlterFieldStatement {
	fn from(v: crate::expr::statements::alter::AlterFieldStatement) -> Self {
		AlterFieldStatement {
			name: v.name.into(),
			what: v.what.into(),
			if_exists: v.if_exists,
			flex: v.flex.map(Into::into),
			kind: v.kind.map(|opt| opt.map(Into::into)),
			readonly: v.readonly,
			value: v.value.map(|opt| opt.map(Into::into)),
			assert: v.assert.map(|opt| opt.map(Into::into)),
			default: v.default.map(|opt| opt.map(Into::into)),
			permissions: v.permissions.map(Into::into),
			comment: v.comment.map(|opt| opt.map(Into::into)),
			reference: v.reference.map(|opt| opt.map(Into::into)),
			default_always: v.default_always,
		}
	}
}

crate::sql::impl_display_from_sql!(AlterFieldStatement);

impl crate::sql::DisplaySql for AlterFieldStatement {
	fn fmt_sql(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "ALTER FIELD")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {} ON {}", self.name, self.what)?;
		if let Some(ref flex) = self.flex {
			if *flex {
				write!(f, " FLEXIBLE")?;
			} else {
				write!(f, " DROP FLEXIBLE")?;
			}
		}
		if let Some(ref kind) = self.kind {
			if let Some(ref kind) = kind {
				write!(f, " TYPE {kind}")?;
			} else {
				write!(f, " DROP TYPE")?;
			}
		}
		if let Some(ref readonly) = self.readonly {
			if *readonly {
				write!(f, " READONLY")?;
			} else {
				write!(f, " DROP READONLY")?;
			}
		}
		if let Some(ref value) = self.value {
			if let Some(ref value) = value {
				write!(f, " VALUE {value}")?;
			} else {
				write!(f, " DROP VALUE")?;
			}
		}
		if let Some(ref assert) = self.assert {
			if let Some(ref assert) = assert {
				write!(f, " ASSERT {assert}")?;
			} else {
				write!(f, " DROP ASSERT")?;
			}
		}
		if let Some(ref default) = self.default {
			if let Some(ref default) = default {
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
			if let Some(ref comment) = comment {
				write!(f, " COMMENT {comment}")?;
			} else {
				write!(f, " DROP COMMENT")?;
			}
		}
		if let Some(reference) = &self.reference {
			if let Some(ref reference) = reference {
				write!(f, " REFERENCE {reference}")?;
			} else {
				write!(f, " DROP REFERENCE")?;
			}
		}
		Ok(())
	}
}
