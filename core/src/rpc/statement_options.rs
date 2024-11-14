use std::collections::BTreeMap;

use crate::{sql::{Cond, Data, Limit, Number, Output, Start, Value}, syn::condition};

use super::RpcError;

#[derive(Clone, Debug)]
pub enum RpcData {
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
	pub fn value(&self) -> &Value {
		match self {
			RpcData::Patch(v) => v,
			RpcData::Merge(v) => v,
			RpcData::Replace(v) => v,
			RpcData::Content(v) => v,
		}
	}

	pub fn from_string(str: String, v: Value) -> Result<RpcData, RpcError> {
		match str.to_lowercase().as_str() {
			"patch" => Ok(RpcData::Patch(v)),
			"merge" => Ok(RpcData::Merge(v)),
			"replace" => Ok(RpcData::Replace(v)),
			"content" => Ok(RpcData::Content(v)),
			_ => Err(RpcError::InvalidParams),
		}
	}
}

#[derive(Clone, Debug)]
pub struct StatementOptions {
	pub data: Option<RpcData>,
	pub output: Output,
	pub limit: Option<Limit>,
	pub start: Option<Start>,
    pub cond: Option<Cond>,
    pub vars: Option<BTreeMap<String, Value>>,
}

impl Default for StatementOptions {
	fn default() -> Self {
		StatementOptions {
			data: None,
			output: Output::After,
            limit: None,
            start: None,
            cond: None,
            vars: None,
		}
	}
}

impl StatementOptions {
	pub fn with_data_content(&mut self, v: Value) -> &mut Self {
		self.data = Some(RpcData::Content(v));
		self
	}

	pub fn process_options(&mut self, opts: Value) -> Result<&mut Self, RpcError> {
		if let Value::Object(obj) = opts {
			// Process "data_expr" option
			if let Some(data) = &self.data {
				if let Some(v) = obj.get("data_expr") {
					if let Value::Strand(v) = v {
						self.data =
							Some(RpcData::from_string(v.to_string(), data.value().to_owned())?);
					} else {
						return Err(RpcError::InvalidParams);
					}
				}
			}

			// Process "output" option
			if let Some(v) = obj.get("return") {
				if let Value::Strand(v) = v {
					self.output = match v.to_lowercase().as_str() {
						"none" => Output::None,
						"null" => Output::Null,
						"diff" => Output::Diff,
						"before" => Output::Before,
						"after" => Output::After,
						_ => return Err(RpcError::InvalidParams),
					}
				} else {
					return Err(RpcError::InvalidParams);
				}
			}

			// Process "limit" option
			if let Some(v) = obj.get("limit") {
				if let Value::Number(Number::Int(_)) = v {
					self.limit = Some(Limit(v.to_owned()))
				} else {
					return Err(RpcError::InvalidParams);
				}
			}

			// Process "start" option
			if let Some(v) = obj.get("start") {
				if let Value::Number(Number::Int(_)) = v {
					self.start = Some(Start(v.to_owned()))
				} else {
					return Err(RpcError::InvalidParams);
				}
			}

			// Process "cond" option
			if let Some(v) = obj.get("cond") {
				if let Value::Strand(v) = v {
					self.cond = Some(condition(v.as_str())?)
				} else {
					return Err(RpcError::InvalidParams);
				}
			}

			// Process "vars" option
			if let Some(v) = obj.get("vars") {
				if let Value::Object(v) = v {
					self.vars = Some(v.0.to_owned())
				} else {
					return Err(RpcError::InvalidParams);
				}
			}

			Ok(self)
		} else {
			Err(RpcError::InvalidParams)
		}
	}

	pub fn data_expr(&self) -> Option<Data> {
		self.data.to_owned().map(|v| v.into())
	}

    pub fn merge_vars(&self, v: &BTreeMap<String, Value>) -> BTreeMap<String, Value> {
        match &self.vars {
			Some(vars) => mrg! {vars.clone(), v},
			None => v.clone(),
		}
    }
}
