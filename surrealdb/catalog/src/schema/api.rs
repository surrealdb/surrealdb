use std::fmt;

use anyhow::{Context as _, Result};
use common::fmt::Fmt;
use revision::revisioned;
use surrealdb_kvs::impl_kv_value_revisioned;
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, SurrealValue, ToSql, write_sql};

use crate::catalog::auth::AuthLimit;
use crate::catalog::{ExprText, FromStored, PathText, Permission, StoredPermission};
use crate::expr::Expr;
use crate::expr::statements::info::InfoStructure;
use crate::sql;
use crate::val::{Array, Object, Value};

/// REST API method.
///
/// The stored twin of [`sql::ApiMethod`]. The AST form renders back to
/// SurrealQL; this form is persisted inside API definitions and is exposed to
/// handlers as `$request.method`, so it owns both the revision encoding and
/// the `SurrealValue` mapping and must not change shape or variant order
/// without a revision bump.
#[revisioned(revision = 1)]
#[derive(SurrealValue, Clone, Copy, Debug, Default, Eq, PartialEq, PartialOrd, Hash)]
#[surreal(crate = "surrealdb_types")]
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

impl fmt::Display for ApiMethod {
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

impl From<sql::ApiMethod> for ApiMethod {
	fn from(v: sql::ApiMethod) -> Self {
		match v {
			sql::ApiMethod::Delete => Self::Delete,
			sql::ApiMethod::Get => Self::Get,
			sql::ApiMethod::Patch => Self::Patch,
			sql::ApiMethod::Post => Self::Post,
			sql::ApiMethod::Put => Self::Put,
			sql::ApiMethod::Trace => Self::Trace,
		}
	}
}

impl From<ApiMethod> for sql::ApiMethod {
	fn from(v: ApiMethod) -> Self {
		match v {
			ApiMethod::Delete => Self::Delete,
			ApiMethod::Get => Self::Get,
			ApiMethod::Patch => Self::Patch,
			ApiMethod::Post => Self::Post,
			ApiMethod::Put => Self::Put,
			ApiMethod::Trace => Self::Trace,
		}
	}
}

/// The API definition.
///
/// Not `Default`: a route path has no meaningful empty value, and every
/// construction site supplies one.
#[revisioned(revision = 2)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct StoredApiDefinition {
	/// The API's route path, as the canonical text `Path` renders to.
	pub path: PathText,
	/// The actions of the API.
	pub actions: Vec<StoredApiActionDefinition>,
	/// Canonical SurrealQL text of the fallback (`THEN`) clause's expression.
	pub fallback: Option<ExprText>,
	/// The config of the API.
	pub config: StoredApiConfigDefinition,
	/// An optional comment for the definition.
	pub comment: Option<String>,
	/// The auth limit of the API.
	#[revision(start = 2, default_fn = "default_auth_limit")]
	pub auth_limit: AuthLimit,
}

// This was pushed in after the first beta, so we need to add auth_limit to structs in a
// non-breaking way
impl StoredApiDefinition {
	/// The routing half of [`ApiDefinition::find_definition`], answered on the
	/// stored form.
	///
	/// Routing reads only the path, whether a fallback exists, and each
	/// action's methods. Compiling the whole definition to answer it would
	/// parse every handler body in the database on every request, of which at
	/// most one is then executed; compiling just the route costs what decoding
	/// it used to, since the stored text was parsed during decode before it
	/// became text.
	///
	/// Returns the matched path parameters and the path's specificity, so the
	/// caller can pick the most specific match before compiling anything.
	pub fn route(
		&self,
		segments: &[&str],
		method: ApiMethod,
	) -> Result<Option<(crate::val::Object, u8)>> {
		let path = self.path.compile().with_context(|| {
			format!("the stored route of API `{}` no longer compiles", self.path)
		})?;
		let Some(params) = path.fit(segments) else {
			return Ok(None);
		};
		let handled =
			self.fallback.is_some() || self.actions.iter().any(|x| x.methods.contains(&method));
		Ok(handled.then(|| (params, path.specificity())))
	}

	fn default_auth_limit(_revision: u16) -> Result<AuthLimit, revision::Error> {
		Ok(AuthLimit::new_no_limit())
	}
}

impl_kv_value_revisioned!(StoredApiDefinition);

impl ApiDefinition {
	fn to_sql_definition(&self) -> sql::statements::DefineApiStatement {
		sql::statements::DefineApiStatement {
			kind: sql::statements::define::DefineKind::Default,
			path: sql::Expr::Literal(sql::Literal::String(self.path.to_string().into())),
			actions: self.actions.iter().map(|x| x.to_sql_action()).collect(),
			fallback: self.fallback.as_ref().map(|e| sql::Expr::from(e.clone())),
			config: self.config.to_sql_config(),
			comment: self
				.comment
				.clone()
				.map(|x| sql::Expr::Literal(sql::Literal::String(x.into())))
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
		let object = map! {
			"path" => self.path.to_string().into(),
			"config" => self.config.structure(),
			"fallback", if let Some(fallback) = self.fallback => Value::from(fallback.to_stored_sql()),
			"actions" => Value::from(self.actions.into_iter().map(InfoStructure::structure).collect::<Vec<Value>>()),
			"comment", if let Some(comment) = self.comment => comment.into(),
		};
		Value::from(Object::from(object))
	}
}

impl InfoStructure for ApiMethod {
	fn structure(self) -> Value {
		Value::from(self.to_string())
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct StoredApiActionDefinition {
	pub methods: Vec<ApiMethod>,
	/// Canonical SurrealQL text of the action's (`THEN`) expression.
	pub action: ExprText,
	pub config: StoredApiConfigDefinition,
}

impl_kv_value_revisioned!(StoredApiActionDefinition);

impl ApiAction {
	fn to_sql_action(&self) -> sql::statements::define::ApiAction {
		sql::statements::define::ApiAction {
			methods: self.methods.iter().copied().map(Into::into).collect(),
			action: sql::Expr::from(self.action.clone()),
			config: self.config.to_sql_config(),
		}
	}
}

impl InfoStructure for ApiAction {
	fn structure(self) -> Value {
		Value::from(map!(
			"methods" => Value::from(self.methods.into_iter().map(InfoStructure::structure).collect::<Vec<Value>>()),
			"action" => Value::from(self.action.to_stored_sql()),
			"config" => self.config.structure(),
		))
	}
}

/// The API config definition.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct StoredApiConfigDefinition {
	/// The middleware of the API.
	pub middleware: Vec<MiddlewareDefinition>,
	/// The permissions of the API.
	pub permissions: StoredPermission,
}

impl ApiConfig {
	/// Convert the API config into a SQL config.
	pub fn to_sql_config(&self) -> sql::statements::define::config::api::ApiConfig {
		sql::statements::define::config::api::ApiConfig {
			middleware: self.middleware.iter().map(|mw| mw.to_sql_middleware()).collect(),
			permissions: self.permissions.to_sql_permission(),
		}
	}
}

impl InfoStructure for ApiConfig {
	fn structure(self) -> Value {
		Value::from(map!(
			"permissions" => self.permissions.structure(),
			"middleware", if !self.middleware.is_empty() => {
				Value::Object(
					self.middleware
						.into_iter()
						.map(|m| {
							let value = m.args
								.iter()
								.map(|x| Value::String(x.to_sql().into()))
								.collect();

							(m.name, Value::Array(Array(value)))
						})
						.collect::<Object>(),
				)
			}
		))
	}
}

impl ToSql for StoredApiConfigDefinition {
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
					format!("{}({})", m.name.as_str(), args)
				}))
			);
		}

		write_sql!(f, fmt, " PERMISSIONS {}", self.permissions);
	}
}

impl ToSql for ApiConfig {
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
					format!("{}({})", m.name.as_str(), args)
				}))
			);
		}

		write_sql!(f, fmt, " PERMISSIONS {}", self.permissions);
	}
}

/// API Middleware definition.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct MiddlewareDefinition {
	/// The name of function to invoke.
	pub name: Strand,
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

/// Runtime form of [`StoredApiConfigDefinition`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApiConfig {
	pub middleware: Vec<MiddlewareDefinition>,
	pub permissions: Permission,
}

impl FromStored for ApiConfig {
	type Stored = StoredApiConfigDefinition;

	fn from_stored(stored: &StoredApiConfigDefinition) -> anyhow::Result<ApiConfig> {
		Ok(ApiConfig {
			middleware: stored.middleware.clone(),
			permissions: Permission::from_stored(&stored.permissions)?,
		})
	}
}

impl ApiConfig {
	pub fn to_stored(&self) -> StoredApiConfigDefinition {
		StoredApiConfigDefinition {
			middleware: self.middleware.clone(),
			permissions: crate::catalog::StoredPermission::from_runtime(&self.permissions),
		}
	}
}

/// Runtime form of [`StoredApiActionDefinition`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApiAction {
	pub methods: Vec<ApiMethod>,
	pub action: Expr,
	pub config: ApiConfig,
}

impl FromStored for ApiAction {
	type Stored = StoredApiActionDefinition;

	fn from_stored(stored: &StoredApiActionDefinition) -> anyhow::Result<ApiAction> {
		Ok(ApiAction {
			methods: stored.methods.clone(),
			action: stored.action.compile()?,
			config: ApiConfig::from_stored(&stored.config)?,
		})
	}
}

impl ApiAction {
	pub fn to_stored(&self) -> StoredApiActionDefinition {
		StoredApiActionDefinition {
			methods: self.methods.clone(),
			action: ExprText::new(&self.action),
			config: self.config.to_stored(),
		}
	}
}

/// Runtime form of [`StoredApiDefinition`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApiDefinition {
	pub path: crate::catalog::api_path::Path,
	pub actions: Vec<ApiAction>,
	pub fallback: Option<Expr>,
	pub config: ApiConfig,
	pub auth_limit: AuthLimit,
	pub comment: Option<String>,
}

impl FromStored for ApiDefinition {
	type Stored = StoredApiDefinition;

	fn from_stored(stored: &StoredApiDefinition) -> anyhow::Result<ApiDefinition> {
		fn build(stored: &StoredApiDefinition) -> anyhow::Result<ApiDefinition> {
			Ok(ApiDefinition {
				path: stored.path.compile()?,
				actions: stored
					.actions
					.iter()
					.map(ApiAction::from_stored)
					.collect::<anyhow::Result<_>>()?,
				fallback: stored.fallback.as_ref().map(|t| t.compile()).transpose()?,
				config: ApiConfig::from_stored(&stored.config)?,
				auth_limit: stored.auth_limit.clone(),
				comment: stored.comment.clone(),
			})
		}
		build(stored).with_context(|| {
			format!("the stored definition of API `{}` no longer compiles", stored.path)
		})
	}
}

impl ApiDefinition {
	/// Finds the most specific API definition matching the given path
	/// segments and method, together with the captured path parameters.
	pub fn to_stored(&self) -> StoredApiDefinition {
		StoredApiDefinition {
			path: PathText::new(&self.path),
			actions: self.actions.iter().map(ApiAction::to_stored).collect(),
			fallback: self.fallback.as_ref().map(ExprText::new),
			config: self.config.to_stored(),
			comment: self.comment.clone(),
			auth_limit: self.auth_limit.clone(),
		}
	}
}
