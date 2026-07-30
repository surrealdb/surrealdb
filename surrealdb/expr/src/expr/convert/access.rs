//! `sql` -> `expr` conversions for [`crate::sql::access`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::access::*;

impl From<AccessDuration> for crate::expr::access::AccessDuration {
	fn from(v: AccessDuration) -> Self {
		Self {
			grant: v.grant.into(),
			token: v.token.into(),
			session: v.session.into(),
		}
	}
}

impl From<crate::expr::access::AccessDuration> for AccessDuration {
	fn from(v: crate::expr::access::AccessDuration) -> Self {
		Self {
			grant: v.grant.into(),
			token: v.token.into(),
			session: v.session.into(),
		}
	}
}
