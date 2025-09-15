use std::fmt::{self, Display, Write};

use crate::sql::fmt::{is_pretty, pretty_indent};
use crate::sql::{Ident, Timeout};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct AlterSequenceStatement {
	pub name: Ident,
	pub if_exists: bool,
	pub timeout: Option<Timeout>,
}

impl Display for AlterSequenceStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "ALTER SEQUENCE")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {}", self.name)?;
		if let Some(ref timeout) = self.timeout {
			write!(f, " TIMEOUT {timeout}")?;
		}
		let _indent = if is_pretty() {
			Some(pretty_indent())
		} else {
			f.write_char(' ')?;
			None
		};
		Ok(())
	}
}

impl From<AlterSequenceStatement> for crate::expr::statements::alter::AlterSequenceStatement {
	fn from(v: AlterSequenceStatement) -> Self {
		crate::expr::statements::alter::AlterSequenceStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			timeout: v.timeout.map(Into::into),
		}
	}
}
impl From<crate::expr::statements::alter::AlterSequenceStatement> for AlterSequenceStatement {
	fn from(v: crate::expr::statements::alter::AlterSequenceStatement) -> Self {
		AlterSequenceStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			timeout: v.timeout.map(Into::into),
		}
	}
}
