use std::fmt::{self, Display};

use crate::sql::Expr;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct DefaultConfig {
	pub namespace: Option<Expr>,
	pub database: Option<Expr>,
}

impl Display for DefaultConfig {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, " DEFAULT")?;
		if let Some(namespace) = &self.namespace {
			write!(f, " NAMESPACE {}", namespace)?;
		}
		if let Some(database) = &self.database {
			write!(f, " DATABASE {}", database)?;
		}
		Ok(())
	}
}

impl From<DefaultConfig> for crate::expr::statements::define::config::defaults::DefaultConfig {
	fn from(v: DefaultConfig) -> Self {
		crate::expr::statements::define::config::defaults::DefaultConfig {
			namespace: v.namespace.map(Into::into),
			database: v.database.map(Into::into),
		}
	}
}
impl From<crate::expr::statements::define::config::defaults::DefaultConfig> for DefaultConfig {
	fn from(v: crate::expr::statements::define::config::defaults::DefaultConfig) -> Self {
		DefaultConfig {
			namespace: v.namespace.map(Into::into),
			database: v.database.map(Into::into),
		}
	}
}
