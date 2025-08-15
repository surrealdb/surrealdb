use std::fmt::{self, Display, Write};

use super::DefineKind;
use crate::sql::fmt::{is_pretty, pretty_indent};
use crate::sql::reference::Reference;
use crate::sql::{Expr, Ident, Idiom, Kind, Permissions};
use crate::val::Strand;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum DefineDefault {
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

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct DefineFieldStatement {
	pub kind: DefineKind,
	pub name: Idiom,
	pub what: Ident,
	/// Whether the field is marked as flexible.
	/// Flexible allows the field to be schemaless even if the table is marked
	/// as schemafull.
	pub flex: bool,
	pub field_kind: Option<Kind>,
	pub readonly: bool,
	pub value: Option<Expr>,
	pub assert: Option<Expr>,
	pub computed: Option<Expr>,
	pub default: DefineDefault,
	pub permissions: Permissions,
	pub comment: Option<Strand>,
	pub reference: Option<Reference>,
}

impl Display for DefineFieldStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE FIELD")?;
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write!(f, " OVERWRITE")?,
			DefineKind::IfNotExists => write!(f, " IF NOT EXISTS")?,
		}
		write!(f, " {} ON {}", self.name, self.what)?;
		if self.flex {
			write!(f, " FLEXIBLE")?
		}
		if let Some(ref v) = self.field_kind {
			write!(f, " TYPE {v}")?
		}

		match self.default {
			DefineDefault::None => {}
			DefineDefault::Always(ref expr) => {
				write!(f, " DEFAULT ALWAYS {expr}")?;
			}
			DefineDefault::Set(ref expr) => {
				write!(f, " DEFAULT {expr}")?;
			}
		}

		if self.readonly {
			write!(f, " READONLY")?
		}
		if let Some(ref v) = self.value {
			write!(f, " VALUE {v}")?
		}
		if let Some(ref v) = self.assert {
			write!(f, " ASSERT {v}")?
		}
		if let Some(ref v) = self.computed {
			write!(f, " COMPUTED {v}")?
		}
		if let Some(ref v) = self.reference {
			write!(f, " REFERENCE {v}")?
		}
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {v}")?
		}
		let _indent = if is_pretty() {
			Some(pretty_indent())
		} else {
			f.write_char(' ')?;
			None
		};
		// Alternate permissions display implementation ignores delete permission
		// This display is used to show field permissions, where delete has no effect
		// Displaying the permission could mislead users into thinking it has an effect
		// Additionally, including the permission will cause a parsing error in 3.0.0
		write!(f, "{:#}", self.permissions)?;
		Ok(())
	}
}

impl From<DefineFieldStatement> for crate::expr::statements::DefineFieldStatement {
	fn from(v: DefineFieldStatement) -> Self {
		Self {
			kind: v.kind.into(),
			name: v.name.into(),
			what: v.what.into(),
			flex: v.flex,
			readonly: v.readonly,
			field_kind: v.field_kind.map(Into::into),
			value: v.value.map(Into::into),
			assert: v.assert.map(Into::into),
			computed: v.computed.map(Into::into),
			default: v.default.into(),
			permissions: v.permissions.into(),
			comment: v.comment,
			reference: v.reference.map(Into::into),
		}
	}
}

impl From<crate::expr::statements::DefineFieldStatement> for DefineFieldStatement {
	fn from(v: crate::expr::statements::DefineFieldStatement) -> Self {
		Self {
			kind: v.kind.into(),
			name: v.name.into(),
			what: v.what.into(),
			flex: v.flex,
			readonly: v.readonly,
			field_kind: v.field_kind.map(Into::into),
			value: v.value.map(Into::into),
			assert: v.assert.map(Into::into),
			computed: v.computed.map(Into::into),
			default: v.default.into(),
			permissions: v.permissions.into(),
			comment: v.comment,
			reference: v.reference.map(Into::into),
		}
	}
}
