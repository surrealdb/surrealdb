use std::collections::BTreeMap;

use crate::{
	dbs::Capabilities,
	sql::{Cond, Data, Fields, Limit, Number, Output, Start, Timeout, Value, Version},
	syn::{
		condition_with_capabilities, fields_with_capabilities, output_with_capabilities,
		value_with_capabilities,
	},
};

use super::RpcError;

#[derive(Clone, Debug)]
pub(crate) enum RpcData {
	Patch(Value),
	Merge(Value),
	Replace(Value),
	Content(Value),
}

impl Into<Data> for RpcData {
	fn into(self) -> Data {
		match self {
			RpcData::Patch(v) => Data::PatchExpression(v),
			RpcData::Merge(v) => Data::MergeExpression(v),
			RpcData::Replace(v) => Data::ReplaceExpression(v),
			RpcData::Content(v) => Data::ContentExpression(v),
		}
	}
}

impl RpcData {
	pub(crate) fn value(&self) -> &Value {
		match self {
			RpcData::Patch(v) => v,
			RpcData::Merge(v) => v,
			RpcData::Replace(v) => v,
			RpcData::Content(v) => v,
		}
	}

	pub(crate) fn from_string(str: String, v: Value) -> Result<RpcData, RpcError> {
		match str.to_lowercase().as_str() {
			"patch" => Ok(RpcData::Patch(v)),
			"merge" => Ok(RpcData::Merge(v)),
			"replace" => Ok(RpcData::Replace(v)),
			"content" => Ok(RpcData::Content(v)),
			_ => Err(RpcError::InvalidParams),
		}
	}
}

#[derive(Clone, Debug, Default)]
pub(crate) struct StatementOptions {
	pub data: Option<RpcData>,
	pub fields: Option<Fields>,
	pub output: Option<Output>,
	pub limit: Option<Limit>,
	pub start: Option<Start>,
	pub cond: Option<Cond>,
	pub only: bool,
	pub relation: bool,
	pub unique: bool,
	pub timeout: Option<Timeout>,
	pub version: Option<Version>,
	pub vars: Option<BTreeMap<String, Value>>,
}

impl StatementOptions {
	pub(crate) fn with_data_content(&mut self, v: Value) -> &mut Self {
		self.data = Some(RpcData::Content(v));
		self
	}

	pub(crate) fn with_output(&mut self, output: Output) -> &mut Self {
		self.output = Some(output);
		self
	}

	pub(crate) fn process_options(
		&mut self,
		opts: Value,
		capabilities: &Capabilities,
	) -> Result<&mut Self, RpcError> {
		if let Value::Object(mut obj) = opts {
			// Process "data_expr" option
			if let Some(data) = &self.data {
				if let Some(v) = obj.remove("data_expr") {
					if let Value::Strand(v) = v {
						self.data =
							Some(RpcData::from_string(v.to_string(), data.value().to_owned())?);
					} else {
						return Err(RpcError::InvalidParams);
					}
				}
			}

			// Process "fields" option
			if let Some(v) = obj.remove("fields") {
				if let Value::Strand(v) = v {
					self.fields = Some(fields_with_capabilities(v.as_str(), capabilities)?)
				} else {
					return Err(RpcError::InvalidParams);
				}
			}

			// Process "return" option
			if let Some(v) = obj.remove("return") {
				if let Value::Strand(v) = v {
					self.output = Some(output_with_capabilities(v.as_str(), capabilities)?)
				} else {
					return Err(RpcError::InvalidParams);
				}
			}

			// Process "limit" option
			if let Some(v) = obj.remove("limit") {
				if let Value::Number(Number::Int(_)) = v {
					self.limit = Some(Limit(v))
				} else {
					return Err(RpcError::InvalidParams);
				}
			}

			// Process "start" option
			if let Some(v) = obj.remove("start") {
				if let Value::Number(Number::Int(_)) = v {
					self.start = Some(Start(v))
				} else {
					return Err(RpcError::InvalidParams);
				}
			}

			// Process "cond" option
			if let Some(v) = obj.remove("cond") {
				if let Value::Strand(v) = v {
					self.cond = Some(condition_with_capabilities(v.as_str(), capabilities)?)
				} else {
					return Err(RpcError::InvalidParams);
				}
			}

			// Process "version" option
			if let Some(v) = obj.remove("version") {
				let v = match v {
					v @ Value::Datetime(_) => v,
					Value::Strand(v) => value_with_capabilities(v.as_str(), capabilities)?,
					_ => {
						return Err(RpcError::InvalidParams);
					}
				};

				self.version = Some(Version(v))
			}

			// Process "timeout" option
			if let Some(v) = obj.remove("timeout") {
				if let Value::Duration(v) = v {
					self.timeout = Some(Timeout(v))
				} else {
					return Err(RpcError::InvalidParams);
				}
			}

			// Process "only" option
			if let Some(v) = obj.remove("only") {
				if let Value::Bool(v) = v {
					self.only = v;
				} else {
					return Err(RpcError::InvalidParams);
				}
			}

			// Process "relation" option
			if let Some(v) = obj.remove("relation") {
				if let Value::Bool(v) = v {
					self.relation = v;
				} else {
					return Err(RpcError::InvalidParams);
				}
			}

			// Process "unique" option
			if let Some(v) = obj.remove("unique") {
				if let Value::Bool(v) = v {
					self.unique = v;
				} else {
					return Err(RpcError::InvalidParams);
				}
			}

			// Process "vars" option
			if let Some(v) = obj.remove("vars") {
				if let Value::Object(v) = v {
					self.vars = Some(v.0)
				} else {
					return Err(RpcError::InvalidParams);
				}
			}

			Ok(self)
		} else {
			Err(RpcError::InvalidParams)
		}
	}

	pub(crate) fn data_expr(&self) -> Option<Data> {
		self.data.to_owned().map(|v| v.into())
	}

	pub(crate) fn merge_vars(&self, v: &BTreeMap<String, Value>) -> BTreeMap<String, Value> {
		match &self.vars {
			Some(vars) => mrg! {vars.clone(), v},
			None => v.clone(),
		}
	}
}
