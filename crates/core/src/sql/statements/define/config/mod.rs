pub mod api;
pub mod graphql;

use crate::iam::ConfigKind;

use anyhow::Result;
use api::ApiConfig;
use graphql::GraphQLConfig;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct DefineConfigStatement {
	pub inner: ConfigInner,
	pub if_not_exists: bool,
	pub overwrite: bool,
}

impl From<DefineConfigStatement> for crate::expr::statements::define::DefineConfigStatement {
	fn from(v: DefineConfigStatement) -> Self {
		crate::expr::statements::define::DefineConfigStatement {
			inner: v.inner.into(),
			if_not_exists: v.if_not_exists,
			overwrite: v.overwrite,
		}
	}
}

impl From<crate::expr::statements::define::DefineConfigStatement> for DefineConfigStatement {
	fn from(v: crate::expr::statements::define::DefineConfigStatement) -> Self {
		DefineConfigStatement {
			inner: v.inner.into(),
			if_not_exists: v.if_not_exists,
			overwrite: v.overwrite,
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum ConfigInner {
	GraphQL(GraphQLConfig),
	Api(ApiConfig),
}

impl DefineConfigStatement {}

impl ConfigInner {
	pub fn name(&self) -> String {
		ConfigKind::from(self).to_string()
	}

	pub fn try_into_graphql(self) -> Result<GraphQLConfig> {
		match self {
			ConfigInner::GraphQL(g) => Ok(g),
			c => fail!("found {c} when a graphql config was expected"),
		}
	}

	pub fn try_into_api(&self) -> Result<&ApiConfig> {
		match self {
			ConfigInner::Api(a) => Ok(a),
			c => fail!("found {c} when a api config was expected"),
		}
	}
}

impl From<ConfigInner> for ConfigKind {
	fn from(value: ConfigInner) -> Self {
		(&value).into()
	}
}

impl From<&ConfigInner> for ConfigKind {
	fn from(value: &ConfigInner) -> Self {
		match value {
			ConfigInner::GraphQL(_) => ConfigKind::GraphQL,
			ConfigInner::Api(_) => ConfigKind::Api,
		}
	}
}

impl Display for DefineConfigStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE CONFIG")?;
		if self.if_not_exists {
			write!(f, " IF NOT EXISTS")?
		}
		if self.overwrite {
			write!(f, " OVERWRITE")?
		}

		write!(f, "{}", self.inner)?;

		Ok(())
	}
}

impl Display for ConfigInner {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match &self {
			ConfigInner::GraphQL(v) => Display::fmt(v, f),
			ConfigInner::Api(v) => Display::fmt(v, f),
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
