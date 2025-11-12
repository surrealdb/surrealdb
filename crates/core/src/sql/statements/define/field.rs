use std::fmt::{self, Display, Write};

use super::DefineKind;
use crate::fmt::{is_pretty, pretty_indent};
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
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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
	pub comment: Option<Expr>,
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
			comment: None,
			reference: None,
		}
	}
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
		if let Some(ref v) = self.field_kind {
			write!(f, " TYPE {v}")?;
			if self.flexible {
				write!(f, " FLEXIBLE")?;
			}
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
			readonly: v.readonly,
			field_kind: v.field_kind.map(Into::into),
			flexible: v.flexible,
			value: v.value.map(Into::into),
			assert: v.assert.map(Into::into),
			computed: v.computed.map(Into::into),
			default: v.default.into(),
			permissions: v.permissions.into(),
			comment: v.comment.map(|x| x.into()),
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
			comment: v.comment.map(|x| x.into()),
			reference: v.reference.map(Into::into),
		}
	}
}
