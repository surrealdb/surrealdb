use std::fmt::{self, Display, Formatter};

use crate::types::PublicDuration;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct ChangeFeed {
	pub expiry: PublicDuration,
	pub store_diff: bool,
}
impl Display for ChangeFeed {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "CHANGEFEED {}", self.expiry)?;
		if self.store_diff {
			write!(f, " INCLUDE ORIGINAL")?;
		};
		Ok(())
	}
}

impl surrealdb_types::ToSql for ChangeFeed {
	fn fmt_sql(&self, f: &mut String, _fmt: surrealdb_types::SqlFormat) {
		use surrealdb_types::write_sql;
		write_sql!(f, "CHANGEFEED {}", self.expiry);
		if self.store_diff {
			f.push_str(" INCLUDE ORIGINAL");
		}
	}
}

impl From<ChangeFeed> for crate::expr::ChangeFeed {
	fn from(v: ChangeFeed) -> Self {
		crate::expr::ChangeFeed {
			expiry: v.expiry.into(),
			store_diff: v.store_diff,
		}
	}
}

impl From<crate::expr::ChangeFeed> for ChangeFeed {
	fn from(v: crate::expr::ChangeFeed) -> Self {
		ChangeFeed {
			expiry: v.expiry.into(),
			store_diff: v.store_diff,
		}
	}
}
