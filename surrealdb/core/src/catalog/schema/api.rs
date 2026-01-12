use std::fmt::{self, Display};

use revision::revisioned;
use surrealdb_types::{SqlFormat, SurrealValue, ToSql, write_sql};

use crate::api::path::Path;
use crate::catalog::Permission;
use crate::expr::Expr;
use crate::expr::statements::info::InfoStructure;
use crate::fmt::Fmt;
use crate::kvs::impl_kv_value_revisioned;
use crate::sql;
use crate::val::{Array, Object, Value};

/// The API definition.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub struct ApiDefinition {
	/// The URL path of the API.
	pub(crate) path: Path,
	/// The actions of the API.
	pub(crate) actions: Vec<ApiActionDefinition>,
	/// The fallback expression of the API.
	pub(crate) fallback: Option<Expr>,
	/// The config of the API.
	pub(crate) config: ApiConfigDefinition,
	/// An optional comment for the definition.
	pub(crate) comment: Option<String>,
}

impl_kv_value_revisioned!(ApiDefinition);

impl ApiDefinition {
	/// Finds the api definition which most closely matches the segments of the
	/// path.
	pub(crate) fn find_definition<'a>(
		definitions: &'a [ApiDefinition],
		segments: Vec<&str>,
		method: ApiMethod,
	) -> Option<(&'a ApiDefinition, Object)> {
		let mut specificity = 0;
		let mut res = None;
		for api in definitions.iter() {
			if let Some(params) = api.path.fit(segments.as_slice())
				&& (api.fallback.is_some()
					|| api.actions.iter().any(|x| x.methods.contains(&method)))
			{
				let s = api.path.specificity();
				if s > specificity {
					specificity = s;
					res = Some((api, params));
				}
			}
		}

		res
	}

	fn to_sql_definition(&self) -> sql::statements::DefineApiStatement {
		sql::statements::DefineApiStatement {
			kind: sql::statements::define::DefineKind::Default,
			path: sql::Expr::Literal(sql::Literal::String(self.path.to_string())),
			actions: self.actions.iter().map(|x| x.to_sql_action()).collect(),
			fallback: self.fallback.clone().map(|x| x.into()),
			config: self.config.to_sql_config(),
			comment: self
				.comment
				.clone()
				.map(|x| sql::Expr::Literal(sql::Literal::String(x)))
				.unwrap_or(sql::Expr::Literal(sql::Literal::None)),
		}
	}
}

impl ToSql for ApiDefinition {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.to_sql_definition().fmt_sql(f, fmt)
	}
}

impl InfoStructure for ApiDefinition {
	fn structure(self) -> Value {
		Value::from(Object(map! {
			"path".to_string() => self.path.to_string().into(),
			"config".to_string() => self.config.structure(),
			"fallback".to_string(), if let Some(fallback) = self.fallback => fallback.structure(),
			"actions".to_string() => Value::from(self.actions.into_iter().map(InfoStructure::structure).collect::<Vec<Value>>()),
			"comment".to_string(), if let Some(comment) = self.comment => comment.into(),
		}))
	}
}

/// REST API method.
#[revisioned(revision = 1)]
#[derive(SurrealValue, Clone, Copy, Debug, Default, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[surreal(untagged, lowercase)]
pub enum ApiMethod {
	/// REST DELETE method.
	Delete,
	/// REST GET method.
	#[default]
	Get,
	/// REST PATCH method.
	Patch,
	/// REST POST method.
	Post,
	/// REST PUT method.
	Put,
	/// REST TRACE method.
	Trace,
}

impl Display for ApiMethod {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Delete => write!(f, "delete"),
			Self::Get => write!(f, "get"),
			Self::Patch => write!(f, "patch"),
			Self::Post => write!(f, "post"),
			Self::Put => write!(f, "put"),
			Self::Trace => write!(f, "trace"),
		}
	}
}

impl ToSql for ApiMethod {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.to_string().fmt_sql(f, fmt)
	}
}

impl InfoStructure for ApiMethod {
	fn structure(self) -> Value {
		Value::from(self.to_string())
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ApiActionDefinition {
	pub methods: Vec<ApiMethod>,
	pub action: Expr,
	pub config: ApiConfigDefinition,
}

impl_kv_value_revisioned!(ApiActionDefinition);

impl ApiActionDefinition {
	pub fn to_sql_action(&self) -> sql::statements::define::ApiAction {
		sql::statements::define::ApiAction {
			methods: self.methods.clone(),
			action: self.action.clone().into(),
			config: self.config.to_sql_config(),
		}
	}
}

impl InfoStructure for ApiActionDefinition {
	fn structure(self) -> Value {
		Value::from(map!(
			"methods" => Value::from(self.methods.into_iter().map(InfoStructure::structure).collect::<Vec<Value>>()),
			"action" => Value::from(self.action.to_sql()),
			"config" => self.config.structure(),
		))
	}
}

/// The API config definition.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct ApiConfigDefinition {
	/// The middleware of the API.
	pub(crate) middleware: Vec<MiddlewareDefinition>,
	/// The permissions of the API.
	pub(crate) permissions: Permission,
}

impl ApiConfigDefinition {
	/// Convert the API config definition into a SQL config.
	pub fn to_sql_config(&self) -> sql::statements::define::config::api::ApiConfig {
		sql::statements::define::config::api::ApiConfig {
			middleware: self.middleware.iter().map(|mw| mw.to_sql_middleware()).collect(),
			permissions: self.permissions.clone().into(),
		}
	}
}

impl InfoStructure for ApiConfigDefinition {
	fn structure(self) -> Value {
		Value::from(map!(
			"permissions" => self.permissions.structure(),
			"middleware", if !self.middleware.is_empty() => {
				Value::Object(Object(
						self.middleware
						.into_iter()
						.map(|m| {
							let value = m.args
								.iter()
								.map(|x| Value::String(x.to_sql()))
								.collect();

							(m.name, Value::Array(Array(value)))
						})
						.collect(),
				))
			}
		))
	}
}

impl ToSql for ApiConfigDefinition {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push_str("API");
		if !self.middleware.is_empty() {
			write_sql!(f, fmt, " MIDDLEWARE ");
			write_sql!(
				f,
				fmt,
				"{}",
				Fmt::pretty_comma_separated(self.middleware.iter().map(|m| {
					let args = Fmt::pretty_comma_separated(m.args.iter()).to_sql();
					format!("{}({})", m.name, args)
				}))
			);
		}

		write_sql!(f, fmt, " PERMISSIONS {}", self.permissions);
	}
}

/// API Middleware definition.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) struct MiddlewareDefinition {
	/// The name of function to invoke.
	pub name: String,
	/// The arguments to pass to the function.
	pub args: Vec<Value>,
}

impl MiddlewareDefinition {
	fn to_sql_middleware(&self) -> sql::statements::define::config::api::Middleware {
		sql::statements::define::config::api::Middleware {
			name: self.name.clone(),
			args: self
				.args
				.clone()
				.into_iter()
				.map(|v| {
					let public_val: crate::types::PublicValue =
						v.try_into().expect("value conversion should succeed");
					sql::Expr::from_public_value(public_val)
				})
				.collect(),
		}
	}
}
