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
