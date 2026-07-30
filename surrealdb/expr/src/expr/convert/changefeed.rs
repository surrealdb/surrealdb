//! `sql` -> `expr` conversions for [`crate::sql::changefeed`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::changefeed::*;

impl From<ChangeFeed> for crate::expr::ChangeFeed {
	fn from(v: ChangeFeed) -> Self {
		crate::expr::ChangeFeed {
			expiry: v.expiry,
			store_diff: v.store_diff,
		}
	}
}

impl From<crate::expr::ChangeFeed> for ChangeFeed {
	fn from(v: crate::expr::ChangeFeed) -> Self {
		ChangeFeed {
			expiry: v.expiry,
			store_diff: v.store_diff,
		}
	}
}
