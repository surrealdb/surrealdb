use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::types::PublicDuration;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Hash)]
pub struct SleepStatement {
	pub(crate) duration: PublicDuration,
}

impl ToSql for SleepStatement {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		write_sql!(f, sql_fmt, "SLEEP {}", self.duration);
	}
}

impl From<SleepStatement> for crate::expr::statements::SleepStatement {
	fn from(v: SleepStatement) -> Self {
		crate::expr::statements::SleepStatement {
			duration: v.duration.into(),
		}
	}
}

impl From<crate::expr::statements::SleepStatement> for SleepStatement {
	fn from(v: crate::expr::statements::SleepStatement) -> Self {
		SleepStatement {
			duration: v.duration.into(),
		}
	}
}
