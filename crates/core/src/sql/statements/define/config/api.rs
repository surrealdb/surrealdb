use std::fmt::{self, Display};

use crate::sql::fmt::Fmt;
use crate::sql::{Expr, Permission};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct ApiConfig {
	pub middleware: Vec<Middleware>,
	pub permissions: Permission,
}

impl Display for ApiConfig {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, " API")?;

		if !self.middleware.is_empty() {
			write!(f, " MIDDLEWARE ")?;
			write!(
				f,
				"{}",
				Fmt::pretty_comma_separated(self.middleware.iter().map(|m| format!(
					"{}({})",
					m.name,
					Fmt::pretty_comma_separated(m.args.iter())
				)))
			)?
		}

		write!(f, " PERMISSIONS {}", self.permissions)?;
		Ok(())
	}
}

impl From<ApiConfig> for crate::expr::statements::define::config::api::ApiConfig {
	fn from(v: ApiConfig) -> Self {
		crate::expr::statements::define::config::api::ApiConfig {
			middleware: v.middleware.into_iter().map(From::from).collect(),
			permissions: v.permissions.into(),
		}
	}
}
impl From<crate::expr::statements::define::config::api::ApiConfig> for ApiConfig {
	fn from(v: crate::expr::statements::define::config::api::ApiConfig) -> Self {
		ApiConfig {
			middleware: v.middleware.into_iter().map(From::from).collect(),
			permissions: v.permissions.into(),
		}
	}
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Middleware {
	pub name: String,
	pub args: Vec<Expr>,
}

impl From<Middleware> for crate::expr::statements::define::config::api::Middleware {
	fn from(v: Middleware) -> Self {
		crate::expr::statements::define::config::api::Middleware {
			name: v.name,
			args: v.args.into_iter().map(From::from).collect(),
		}
	}
}
impl From<crate::expr::statements::define::config::api::Middleware> for Middleware {
	fn from(v: crate::expr::statements::define::config::api::Middleware) -> Self {
		Middleware {
			name: v.name,
			args: v.args.into_iter().map(From::from).collect(),
		}
	}
}
