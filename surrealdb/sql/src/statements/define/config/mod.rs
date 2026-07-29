pub mod api;
pub mod defaults;
use surrealdb_types::{SqlFormat, ToSql};
pub mod graphql;

use api::ApiConfig;
use defaults::DefaultConfig;
pub use graphql::GraphQLConfig;

use super::DefineKind;

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct DefineConfigStatement {
	pub kind: DefineKind,
	pub inner: ConfigInner,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum ConfigInner {
	GraphQL(GraphQLConfig),
	Api(ApiConfig),
	Default(DefaultConfig),
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
			ConfigInner::Default(v) => v.fmt_sql(f, fmt),
		}
	}
}
