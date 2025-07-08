use std::fmt::{self, Display};

use crate::sql::fmt::Fmt;

use crate::sql::{Expr, Permission};
use revision::revisioned;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct RequestMiddleware(pub Vec<(String, Vec<Expr>)>);

impl From<RequestMiddleware> for crate::api::middleware::RequestMiddleware {
	fn from(v: RequestMiddleware) -> Self {
		crate::api::middleware::RequestMiddleware(
			v.0.into_iter().map(|(k, v)| (k, v.into_iter().map(Into::into).collect())).collect(),
		)
	}
}

impl From<crate::api::middleware::RequestMiddleware> for RequestMiddleware {
	fn from(v: crate::api::middleware::RequestMiddleware) -> Self {
		RequestMiddleware(
			v.0.into_iter().map(|(k, v)| (k, v.into_iter().map(Into::into).collect())).collect(),
		)
	}
}

#[derive(Clone, Debug, Default)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct ApiConfig {
	pub middleware: Option<RequestMiddleware>,
	pub permissions: Option<Permission>,
}

impl ApiConfig {
	pub fn is_empty(&self) -> bool {
		self.middleware.is_none() && self.permissions.is_none()
	}
}

impl Display for ApiConfig {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, " API")?;

		if let Some(mw) = &self.middleware {
			write!(f, " MIDDLEWARE ")?;
			write!(
				f,
				"{}",
				Fmt::pretty_comma_separated(
					mw.0.iter().map(|(k, v)| format!("{k}({})", Fmt::pretty_comma_separated(v)))
				)
			)?
		}

		if let Some(p) = &self.permissions {
			write!(f, " PERMISSIONS {}", p)?;
		}
		Ok(())
	}
}

impl From<ApiConfig> for crate::expr::statements::define::config::api::ApiConfig {
	fn from(v: ApiConfig) -> Self {
		crate::expr::statements::define::config::api::ApiConfig {
			middleware: v.middleware.map(Into::into),
			permissions: v.permissions.map(Into::into),
		}
	}
}
impl From<crate::expr::statements::define::config::api::ApiConfig> for ApiConfig {
	fn from(v: crate::expr::statements::define::config::api::ApiConfig) -> Self {
		ApiConfig {
			middleware: v.middleware.map(Into::into),
			permissions: v.permissions.map(Into::into),
		}
	}
}
