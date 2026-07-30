//! `sql` -> `expr` conversions for [`crate::sql::statements::define::config::defaults`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::define::config::defaults::*;

impl From<DefaultConfig> for crate::expr::statements::define::config::defaults::DefaultConfig {
	fn from(v: DefaultConfig) -> Self {
		crate::expr::statements::define::config::defaults::DefaultConfig {
			namespace: v.namespace.into(),
			database: v.database.into(),
		}
	}
}

impl From<crate::expr::statements::define::config::defaults::DefaultConfig> for DefaultConfig {
	fn from(v: crate::expr::statements::define::config::defaults::DefaultConfig) -> Self {
		DefaultConfig {
			namespace: v.namespace.into(),
			database: v.database.into(),
		}
	}
}
