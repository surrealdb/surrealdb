use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::sql::Timeout;
use crate::sql::statements::alter::AlterKind;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct AlterSystemStatement {
	pub query_timeout: AlterKind<Timeout>,
	pub compact: bool,
}

impl ToSql for AlterSystemStatement {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		write_sql!(f, _fmt, "ALTER SYSTEM");
		match &self.query_timeout {
			AlterKind::None => {}
			AlterKind::Set(time_out) => {
				write_sql!(f, _fmt, " QUERY_TIMEOUT {}", time_out.0);
			}
			AlterKind::Drop => {
				write_sql!(f, _fmt, " DROP QUERY_TIMEOUT");
			}
		}
		if self.compact {
			write_sql!(f, _fmt, " COMPACT");
		}
	}
}

impl From<AlterSystemStatement> for crate::expr::statements::alter::AlterSystemStatement {
	fn from(v: AlterSystemStatement) -> Self {
		crate::expr::statements::alter::AlterSystemStatement {
			query_timeout: v.query_timeout.into(),
			compact: v.compact,
		}
	}
}
impl From<crate::expr::statements::alter::AlterSystemStatement> for AlterSystemStatement {
	fn from(v: crate::expr::statements::alter::AlterSystemStatement) -> Self {
		AlterSystemStatement {
			query_timeout: v.query_timeout.into(),
			compact: v.compact,
		}
	}
}
