use std::time;

use revision::revisioned;

use crate::expr::statements::info::InfoStructure;
use crate::val::stored::schema::Stored;
use crate::val::{BoundedDuration, Duration, Value};

#[revisioned(revision = 1)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Stored)]
#[stored(since = "4.0")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct ChangeFeed {
	#[stored(n = 2)]
	pub expiry: BoundedDuration,
	#[stored(n = 4)]
	pub store_diff: bool,
}

impl Default for ChangeFeed {
	fn default() -> Self {
		Self {
			expiry: BoundedDuration(time::Duration::ZERO),
			store_diff: false,
		}
	}
}

impl InfoStructure for ChangeFeed {
	fn structure(self) -> Value {
		Value::from(map! {
			"expiry" => Duration::from(self.expiry).into(),
			"original" => self.store_diff.into(),
		})
	}
}

impl surrealdb_types::ToSql for ChangeFeed {
	fn fmt_sql(&self, f: &mut String, sql_fmt: surrealdb_types::SqlFormat) {
		use surrealdb_types::write_sql;
		write_sql!(f, sql_fmt, "CHANGEFEED {}", common::fmt::SqlDuration(self.expiry.0));
		if self.store_diff {
			f.push_str(" INCLUDE ORIGINAL");
		}
	}
}
