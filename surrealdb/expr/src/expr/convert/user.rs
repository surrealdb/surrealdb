//! `sql` -> `expr` conversions for [`crate::sql::user`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::user::*;

impl From<UserDuration> for crate::expr::user::UserDuration {
	fn from(v: UserDuration) -> Self {
		crate::expr::user::UserDuration {
			token: v.token.into(),
			session: v.session.into(),
		}
	}
}

impl From<crate::expr::user::UserDuration> for UserDuration {
	fn from(v: crate::expr::user::UserDuration) -> Self {
		UserDuration {
			token: v.token.into(),
			session: v.session.into(),
		}
	}
}
