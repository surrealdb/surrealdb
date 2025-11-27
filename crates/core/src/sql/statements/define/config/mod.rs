pub mod api;
use surrealdb_types::{SqlFormat, ToSql, write_sql};
pub mod graphql;

use api::ApiConfig;
pub use graphql::GraphQLConfig;

use super::DefineKind;

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct DefineConfigStatement {
	pub kind: DefineKind,
	pub inner: ConfigInner,
}

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

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) enum ConfigInner {
	GraphQL(GraphQLConfig),
	Api(ApiConfig),
}

impl ToSql for DefineConfigStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push_str("DEFINE CONFIG");
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => f.push_str(" OVERWRITE"),
			DefineKind::IfNotExists => f.push_str(" IF NOT EXISTS"),
		}

		f.push(' ');
		self.inner.fmt_sql(f, fmt);
	}
}

impl ToSql for ConfigInner {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match &self {
			ConfigInner::GraphQL(v) => v.fmt_sql(f, fmt),
			ConfigInner::Api(v) => {
				f.push_str("API");
				v.fmt_sql(f, fmt);
			}
		}
	}
}

impl From<ConfigInner> for crate::expr::statements::define::config::ConfigInner {
	fn from(v: ConfigInner) -> Self {
		match v {
			ConfigInner::GraphQL(v) => {
				crate::expr::statements::define::config::ConfigInner::GraphQL(v.into())
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
			crate::expr::statements::define::config::ConfigInner::Api(v) => {
				ConfigInner::Api(v.into())
			}
		}
	}
}
