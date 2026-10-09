use surrealdb_types::{SqlFormat, ToSql};
pub mod api;
pub mod defaults;

use api::ApiConfig;
use defaults::DefaultConfig;

use crate::expr::graphql_config::GraphQLConfig;
use crate::expr::statements::define::DefineKind;
use crate::iam::ConfigKind;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DefineConfigStatement {
	pub kind: DefineKind,
	pub inner: ConfigInner,
}

/// The config struct as a computation target.

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum ConfigInner {
	GraphQL(GraphQLConfig),
	Api(ApiConfig),
	Default(DefaultConfig),
}

impl ConfigInner {
	pub fn kind(&self) -> ConfigKind {
		match self {
			ConfigInner::Default(_) => ConfigKind::Default,
			ConfigInner::GraphQL(_) => ConfigKind::GraphQL,
			ConfigInner::Api(_) => ConfigKind::Api,
		}
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
			ConfigInner::Default(v) => v.fmt_sql(f, fmt),
		}
	}
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
