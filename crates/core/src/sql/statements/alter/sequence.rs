use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::fmt::EscapeIdent;
use crate::sql::Timeout;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct AlterSequenceStatement {
	pub name: String,
	pub if_exists: bool,
	pub timeout: Option<Timeout>,
}

impl ToSql for AlterSequenceStatement {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str("ALTER SEQUENCE");
		if self.if_exists {
			f.push_str(" IF EXISTS");
		}
		write_sql!(f, " {}", EscapeIdent(&self.name));
		if let Some(ref timeout) = self.timeout {
			write_sql!(f, " TIMEOUT {}", timeout);
		}
	}
}

impl From<AlterSequenceStatement> for crate::expr::statements::alter::AlterSequenceStatement {
	fn from(v: AlterSequenceStatement) -> Self {
		crate::expr::statements::alter::AlterSequenceStatement {
			name: v.name,
			if_exists: v.if_exists,
			timeout: v.timeout.map(Into::into),
		}
	}
}
impl From<crate::expr::statements::alter::AlterSequenceStatement> for AlterSequenceStatement {
	fn from(v: crate::expr::statements::alter::AlterSequenceStatement) -> Self {
		AlterSequenceStatement {
			name: v.name,
			if_exists: v.if_exists,
			timeout: v.timeout.map(Into::into),
		}
	}
}
