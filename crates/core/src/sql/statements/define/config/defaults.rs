use std::fmt::{self, Display};

use crate::sql::{Expr, Literal};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct DefaultConfig {
	pub namespace: Expr,
	pub database: Expr,
}

impl Default for DefaultConfig {
	fn default() -> Self {
		Self {
			namespace: Expr::Literal(Literal::None),
			database: Expr::Literal(Literal::None),
		}
	}
}

impl Display for DefaultConfig {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, " DEFAULT")?;
		write!(f, " NAMESPACE {}", self.namespace)?;
		write!(f, " DATABASE {}", self.database)?;
		Ok(())
	}
}

impl From<DefaultConfig> for crate::expr::statements::define::config::defaults::DefaultConfig {
	fn from(v: DefaultConfig) -> Self {
		crate::expr::statements::define::config::defaults::DefaultConfig {
			namespace: v.namespace.into(),
			database: v.database.into(),
		}
	}
}
impl From<crate::expr::statements::define::config::defaults::DefaultConfig> for DefaultConfig {
	fn from(v: crate::expr::statements::define::config::defaults::DefaultConfig) -> Self {
		DefaultConfig {
			namespace: v.namespace.into(),
			database: v.database.into(),
		}
	}
}
