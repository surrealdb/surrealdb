use crate::sql::fmt::{is_pretty, pretty_indent};
use crate::sql::{Ident, Timeout};

use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Write};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct AlterSequenceStatement {
	pub name: Ident,
	pub if_exists: bool,
	pub timeout: Option<Timeout>,
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

crate::sql::impl_display_from_sql!(AlterSequenceStatement);

impl crate::sql::DisplaySql for AlterSequenceStatement {
	fn fmt_sql(&self, f: &mut fmt::Formatter) -> fmt::Result {
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
