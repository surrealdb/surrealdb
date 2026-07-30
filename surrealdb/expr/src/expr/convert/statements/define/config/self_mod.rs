//! `sql` -> `expr` conversions declared directly in [`crate::sql::statements::define::config`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::define::config::*;

impl From<DefineConfigStatement> for crate::expr::statements::define::DefineConfigStatement {
	fn from(v: DefineConfigStatement) -> Self {
		crate::expr::statements::define::DefineConfigStatement {
			kind: v.kind.into(),
			inner: v.inner.into(),
		}
	}
}

impl From<crate::expr::statements::define::DefineConfigStatement> for DefineConfigStatement {
	fn from(v: crate::expr::statements::define::DefineConfigStatement) -> Self {
		DefineConfigStatement {
			inner: v.inner.into(),
			kind: v.kind.into(),
		}
	}
}

impl From<ConfigInner> for crate::expr::statements::define::config::ConfigInner {
	fn from(v: ConfigInner) -> Self {
		match v {
			ConfigInner::GraphQL(v) => {
				crate::expr::statements::define::config::ConfigInner::GraphQL(v.into())
			}
			ConfigInner::Default(v) => {
				crate::expr::statements::define::config::ConfigInner::Default(v.into())
			}
			ConfigInner::Api(v) => {
				crate::expr::statements::define::config::ConfigInner::Api(v.into())
			}
		}
	}
}

impl From<crate::expr::statements::define::config::ConfigInner> for ConfigInner {
	fn from(v: crate::expr::statements::define::config::ConfigInner) -> Self {
		match v {
			crate::expr::statements::define::config::ConfigInner::GraphQL(v) => {
				ConfigInner::GraphQL(v.into())
			}
			crate::expr::statements::define::config::ConfigInner::Default(v) => {
				ConfigInner::Default(v.into())
			}
			crate::expr::statements::define::config::ConfigInner::Api(v) => {
				ConfigInner::Api(v.into())
			}
		}
	}
}
