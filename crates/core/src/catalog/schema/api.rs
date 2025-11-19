use std::fmt::{self, Display};

use revision::revisioned;
use surrealdb_types::{SqlFormat, SurrealValue, ToSql};

use crate::api::path::Path;
use crate::catalog::Permission;
use crate::expr::Expr;
use crate::expr::statements::info::InfoStructure;
use crate::kvs::impl_kv_value_revisioned;
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
			if let Some(params) = api.path.fit(segments.as_slice()) {
				if api.fallback.is_some() || api.actions.iter().any(|x| x.methods.contains(&method))
				{
					let s = api.path.specificity();
					if s > specificity {
						specificity = s;
						res = Some((api, params));
					}
				}
			}
		}

		res
	}

	fn to_sql_definition(&self) -> crate::sql::statements::DefineApiStatement {
		crate::sql::statements::DefineApiStatement {
			kind: crate::sql::statements::define::DefineKind::Default,
			path: crate::sql::Expr::Literal(crate::sql::Literal::String(self.path.to_string())),
			actions: self.actions.iter().map(|x| x.to_sql_action()).collect(),
			fallback: self.fallback.clone().map(|x| x.into()),
			config: self.config.to_sql_config(),
			comment: self
				.comment
				.clone()
				.map(|x| crate::sql::Expr::Literal(crate::sql::Literal::String(x))),
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
#[derive(SurrealValue, Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[surreal(untagged, lowercase)]
pub enum ApiMethod {
	/// REST DELETE method.
	Delete,
	/// REST GET method.
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
	pub fn to_sql_action(&self) -> crate::sql::statements::define::ApiAction {
		crate::sql::statements::define::ApiAction {
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
			"action" => Value::from(self.action.to_string()),
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
	pub fn to_sql_config(&self) -> crate::sql::statements::define::config::api::ApiConfig {
		crate::sql::statements::define::config::api::ApiConfig {
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
								.map(|x| Value::String(x.to_string()))
								.collect();

							(m.name.clone(), Value::Array(Array(value)))
						})
						.collect(),
				))
			}
		))
	}
}

impl Display for ApiConfigDefinition {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		use surrealdb_types::ToSql;
		write!(f, "{}", self.to_sql_config().to_sql())
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
	fn to_sql_middleware(&self) -> crate::sql::statements::define::config::api::Middleware {
		crate::sql::statements::define::config::api::Middleware {
			name: self.name.clone(),
			args: self
				.args
				.clone()
				.into_iter()
				.map(|v| {
					let public_val: crate::types::PublicValue =
						v.try_into().expect("value conversion should succeed");
					crate::sql::Expr::from_public_value(public_val)
				})
				.collect(),
		}
	}
}
