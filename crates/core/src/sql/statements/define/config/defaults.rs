use std::fmt::{self, Display};

use crate::sql::Expr;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct DefaultsConfig {
	pub namespace: Option<Expr>,
	pub database: Option<Expr>,
}

impl Display for DefaultsConfig {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, " DEFAULTS")?;
		if let Some(namespace) = &self.namespace {
			write!(f, " NAMESPACE {}", namespace)?;
		}
		if let Some(database) = &self.database {
			write!(f, " DATABASE {}", database)?;
		}
		Ok(())
	}
}

impl From<DefaultsConfig> for crate::expr::statements::define::config::defaults::DefaultsConfig {
	fn from(v: DefaultsConfig) -> Self {
		crate::expr::statements::define::config::defaults::DefaultsConfig {
			namespace: v.namespace.map(Into::into),
			database: v.database.map(Into::into),
		}
	}
}
impl From<crate::expr::statements::define::config::defaults::DefaultsConfig> for DefaultsConfig {
	fn from(v: crate::expr::statements::define::config::defaults::DefaultsConfig) -> Self {
		DefaultsConfig {
			namespace: v.namespace.map(Into::into),
			database: v.database.map(Into::into),
		}
	}
}