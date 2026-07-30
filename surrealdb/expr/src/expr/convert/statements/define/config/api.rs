//! `sql` -> `expr` conversions for [`crate::sql::statements::define::config::api`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::define::config::api::*;

impl From<ApiConfig> for crate::expr::statements::define::config::api::ApiConfig {
	fn from(v: ApiConfig) -> Self {
		crate::expr::statements::define::config::api::ApiConfig {
			middleware: v.middleware.into_iter().map(From::from).collect(),
			permissions: v.permissions.into(),
		}
	}
}

impl From<crate::expr::statements::define::config::api::ApiConfig> for ApiConfig {
	fn from(v: crate::expr::statements::define::config::api::ApiConfig) -> Self {
		ApiConfig {
			middleware: v.middleware.into_iter().map(From::from).collect(),
			permissions: v.permissions.into(),
		}
	}
}

impl From<Middleware> for crate::expr::statements::define::config::api::Middleware {
	fn from(v: Middleware) -> Self {
		crate::expr::statements::define::config::api::Middleware {
			name: v.name,
			args: v.args.into_iter().map(From::from).collect(),
		}
	}
}

impl From<crate::expr::statements::define::config::api::Middleware> for Middleware {
	fn from(v: crate::expr::statements::define::config::api::Middleware) -> Self {
		Middleware {
			name: v.name,
			args: v.args.into_iter().map(From::from).collect(),
		}
	}
}
