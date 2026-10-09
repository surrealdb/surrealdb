use std::time;

use revision::revisioned;

use crate::expr::statements::info::InfoStructure;
use crate::val::{Duration, Value};

#[revisioned(revision = 1)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct ChangeFeed {
	pub expiry: time::Duration,
	pub store_diff: bool,
}

impl Default for ChangeFeed {
	fn default() -> Self {
		Self {
			expiry: time::Duration::from_secs(0),
			store_diff: false,
		}
	}
}

impl InfoStructure for ChangeFeed {
	fn structure(self) -> Value {
		Value::from(map! {
			"expiry" => Duration(self.expiry).into(),
			"original" => self.store_diff.into(),
		})
	}
}

impl surrealdb_types::ToSql for ChangeFeed {
	fn fmt_sql(&self, f: &mut String, sql_fmt: surrealdb_types::SqlFormat) {
		use surrealdb_types::write_sql;
		write_sql!(f, sql_fmt, "CHANGEFEED {}", common::fmt::SqlDuration(self.expiry));
		if self.store_diff {
			f.push_str(" INCLUDE ORIGINAL");
		}
	}
}
