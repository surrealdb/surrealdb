use std::fmt::{self, Display};

use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::sql::{Expr, Permission};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct ApiConfig {
	pub middleware: Vec<Middleware>,
	pub permissions: Permission,
}

impl ToSql for ApiConfig {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		if !self.middleware.is_empty() {
			f.push_str(" MIDDLEWARE ");
			let middleware_strs: Vec<String> = self
				.middleware
				.iter()
				.map(|m| {
					let args_str: Vec<String> =
						m.args.iter().map(|arg| surrealdb_types::ToSql::to_sql(arg)).collect();
					format!("{}({})", m.name, args_str.join(", "))
				})
				.collect();
			f.push_str(&middleware_strs.join(", "));
		}

		write_sql!(f, " PERMISSIONS {}", self.permissions);
	}
}

impl Display for ApiConfig {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		use surrealdb_types::ToSql;
		write!(f, "{}", self.to_sql())
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
pub(crate) struct Middleware {
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
