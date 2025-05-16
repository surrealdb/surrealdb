use std::fmt::{self};

use crate::api::middleware::RequestMiddleware;
use crate::sql::fmt::Fmt;
use crate::sql::statements::info::InfoStructure;
use crate::sql::{Permission, Value};
use revision::revisioned;
use serde::{Deserialize, Serialize};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct ApiConfig {
	pub middleware: Option<RequestMiddleware>,
	pub permissions: Option<Permission>,
}

impl ApiConfig {
	pub fn is_empty(&self) -> bool {
		self.middleware.is_none() && self.permissions.is_none()
	}
}

crate::sql::impl_display_from_sql!(ApiConfig);

impl crate::sql::DisplaySql for ApiConfig {
	fn fmt_sql(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, " API")?;

		if let Some(mw) = &self.middleware {
			write!(f, " MIDDLEWARE ")?;
			write!(
				f,
				"{}",
				Fmt::pretty_comma_separated(
					mw.iter().map(|(k, v)| format!("{k}({})", Fmt::pretty_comma_separated(v)))
				)
			)?
		}

		if let Some(p) = &self.permissions {
			write!(f, " PERMISSIONS {}", p)?;
		}
		Ok(())
	}
}

impl InfoStructure for ApiConfig {
	fn structure(self) -> Value {
		Value::from(map!(
			"permissions", if let Some(v) = self.permissions => v.structure(),
			"middleware", if let Some(v) = self.middleware => v.structure(),
		))
	}
}
