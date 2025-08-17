use std::fmt::{self, Display};

use revision::revisioned;

use crate::api::path::Path;
use crate::catalog::Permission;
use crate::expr::Expr;
use crate::expr::fmt::Fmt;
use crate::expr::statements::info::InfoStructure;
use crate::kvs::impl_kv_value_revisioned;
use crate::sql::ToSql;
use crate::val::{Array, Object, Strand, Value};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct ApiDefinition {
	pub path: Path,
	pub actions: Vec<ApiActionStore>,
	pub fallback: Option<Expr>,
	pub config: ApiConfigStore,
	pub comment: Option<Strand>,
}

impl_kv_value_revisioned!(ApiDefinition);

impl ApiDefinition {
	/// Finds the api definition which most closely matches the segments of the
	/// path.
	pub fn find_definition<'a>(
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
		todo!("STU")
		// crate::sql::statements::DefineApiStatement {
		// 	kind: DefineKind::Default,
		// 	path: self.path.into(),
		// 	actions: self.actions.iter().map(|x| x.to_sql_action()).collect(),
		// 	fallback: self.fallback.clone(),
		// 	config: self.config.to_sql_config(),
		// 	comment: self.comment.clone(),
		// }
	}
}

impl ToSql for ApiDefinition {
	fn to_sql(&self) -> String {
		self.to_sql_definition().to_string()
	}
}

impl InfoStructure for ApiDefinition {
	fn structure(self) -> Value {
		Value::from(Object(map! {
			// TODO: Null byte validity
			"path".to_string() => Strand::new(self.path.to_string()).unwrap().into(),
			"config".to_string() => self.config.structure(),
			"fallback".to_string(), if let Some(fallback) = self.fallback => fallback.structure(),
			"actions".to_string() => Value::from(self.actions.into_iter().map(InfoStructure::structure).collect::<Vec<Value>>()),
			"comment".to_string(), if let Some(comment) = self.comment => comment.into(),
		}))
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum ApiMethod {
	Delete,
	Get,
	Patch,
	Post,
	Put,
	Trace,
}

impl TryFrom<&Value> for ApiMethod {
	type Error = anyhow::Error;
	fn try_from(value: &Value) -> Result<Self, Self::Error> {
		match value {
			Value::Strand(s) => match s.to_ascii_lowercase().as_str() {
				"delete" => Ok(Self::Delete),
				"get" => Ok(Self::Get),
				"patch" => Ok(Self::Patch),
				"post" => Ok(Self::Post),
				"put" => Ok(Self::Put),
				"trace" => Ok(Self::Trace),
				unexpected => Err(anyhow::anyhow!("method does not match: {unexpected}")),
			},
			_ => Err(anyhow::anyhow!("method does not match: {value}")),
		}
	}
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
pub struct ApiActionStore {
	pub methods: Vec<ApiMethod>,
	pub action: Expr,
	pub config: ApiConfigStore,
}

impl_kv_value_revisioned!(ApiActionStore);

impl InfoStructure for ApiActionStore {
	fn structure(self) -> Value {
		Value::from(map!(
			"methods" => Value::from(self.methods.into_iter().map(InfoStructure::structure).collect::<Vec<Value>>()),
			"action" => Value::from(self.action.to_string()),
			"config" => self.config.structure(),
		))
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct ApiConfigStore {
	pub middleware: Vec<MiddlewareStore>,
	pub permissions: Permission,
}

impl InfoStructure for ApiConfigStore {
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
								.map(|x| Value::Strand(Strand::new(x.to_string()).unwrap()))
								.collect();

							(m.name.clone(), Value::Array(Array(value)))
						})
						.collect(),
				))
			}
		))
	}
}

impl Display for ApiConfigStore {
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

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct MiddlewareStore {
	pub name: String,
	pub args: Vec<Value>,
}
