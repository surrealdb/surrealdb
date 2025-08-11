pub mod api;
pub mod graphql;

use std::fmt::{self, Display};

use api::ApiConfig;
use graphql::GraphQLConfig;

use super::DefineKind;

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct DefineConfigStatement {
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
pub enum ConfigInner {
	GraphQL(GraphQLConfig),
	Api(ApiConfig),
}

impl Display for DefineConfigStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE CONFIG")?;
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write!(f, " OVERWRITE")?,
			DefineKind::IfNotExists => write!(f, " IF NOT EXISTS")?,
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
