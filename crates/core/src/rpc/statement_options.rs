use super::RpcError;
use crate::dbs::{Capabilities, Variables};
use crate::sql::{Cond, Data, Expr, Fetchs, Fields, Limit, Literal, Output, Start, Timeout};
use crate::syn;
use crate::val::{Number, Object, Value};

#[derive(Clone, Debug)]
pub(crate) enum RpcData {
	Patch(Value),
	Merge(Value),
	Replace(Value),
	Content(Value),
	Single(Value),
}

impl From<RpcData> for Data {
	fn from(data: RpcData) -> Self {
		match data {
			RpcData::Patch(v) => Data::PatchExpression(v.into_literal().into()),
			RpcData::Merge(v) => Data::MergeExpression(v.into_literal().into()),
			RpcData::Replace(v) => Data::ReplaceExpression(v.into_literal().into()),
			RpcData::Content(v) => Data::ContentExpression(v.into_literal().into()),
			RpcData::Single(v) => Data::SingleExpression(v.into_literal().into()),
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
			RpcData::Single(v) => v,
		}
	}

	pub(crate) fn from_string(str: String, v: Value) -> Result<RpcData, RpcError> {
		match str.to_lowercase().as_str() {
			"patch" => Ok(RpcData::Patch(v)),
			"merge" => Ok(RpcData::Merge(v)),
			"replace" => Ok(RpcData::Replace(v)),
			"content" => Ok(RpcData::Content(v)),
			"single" => Ok(RpcData::Single(v)),
			unexpected => Err(RpcError::InvalidParams(format!(
				"Expected 'patch', 'merge', 'replace', 'content', or 'single', got {unexpected}"
			))),
		}
	}
}

/// Statement Options for the `select`, `insert`, `create`, `upsert`, `update`,
/// `relate` and `delete` methods.
#[derive(Clone, Debug, Default)]
pub(crate) struct StatementOptions {
	/// - One of: `"content"`, `"replace"`, `"merge"`, `"patch"` or `"single"`.
	/// - For the `insert`, `create`, `upsert`, `update` and `relate` methods
	pub data: Option<RpcData>,
	/// - A string, containing fields to select. Also works with the `VALUE` keyword.
	/// - For the `select` method
	pub fields: Option<Fields>,
	/// - One of: `"none"`, `"null"`, `"diff"`, `"before"`, `"after"` or a list of fields
	/// - For the `insert`, `create`, `upsert`, `update`, `relate` and `delete` methods
	pub output: Option<Output>,
	/// - A number, stating how many records can be selected or affected
	/// - For the `select` method
	pub limit: Option<Limit>,
	/// - A number, stating how many records to skip in a selection
	/// - For the `select` method
	pub start: Option<Start>,
	/// - A string, containing an expression for a `WHERE` clause
	/// - For the `select`, `upsert`, `update` and `delete` methods
	pub cond: Option<Cond>,
	/// - A boolean, stating where we want to select or affect only a single record.
	/// - For the `select`, `create`, `upsert`, `update`, `relate` and `delete` methods
	pub only: bool,
	/// - A boolean, stating wether we are inserting relations.
	/// - For the `insert` method
	pub relation: bool,
	/// - A boolean, stating wether the relation we are inserting needs to be unique
	/// - For the `relate` method
	pub unique: bool,
	/// - Can contain either:
	///    - A datetime
	///    - A string, containing an expression which computes into a datetime
	/// - For the `select`, `insert` and `create` methods
	pub version: Option<Expr>,
	/// - A duration, stating how long execution can last
	/// - For all (`select`, `insert`, `create`, `upsert`, `update`, `relate` and `delete`) methods
	pub timeout: Option<Timeout>,
	/// - An object, containing variables to define during execution of the method
	/// - For all (`select`, `insert`, `create`, `upsert`, `update`, `relate` and `delete`) methods
	pub vars: Option<Variables>,
	/// - A boolean, stating wether the LQ notifications should contain diffs
	/// - For the `live` method
	pub diff: bool,
	/// - A string, containing fields to fetch.
	/// - For the `select` and `live` methods
	pub fetch: Option<Fetchs>,
}

impl StatementOptions {
	pub const fn new() -> Self {
		StatementOptions {
			data: None,
			fields: None,
			output: None,
			limit: None,
			start: None,
			cond: None,
			only: false,
			relation: false,
			unique: false,
			version: None,
			timeout: None,
			vars: None,
			diff: false,
			fetch: None,
		}
	}

	pub(crate) fn with_data_content(&mut self, v: Value) -> &mut Self {
		self.data = Some(RpcData::Content(v));
		self
	}

	pub(crate) fn with_output(&mut self, output: Output) -> &mut Self {
		self.output = Some(output);
		self
	}

	pub(crate) fn extract_options_rpc_object(
		&mut self,
		mut obj: Object,
		capabilities: &Capabilities,
	) -> Result<&mut Self, RpcError> {
		// Process "data_expr" option
		if let Some(data) = &self.data {
			if let Some(v) = obj.remove("data_expr") {
				if let Value::Strand(v) = v {
					self.data = Some(RpcData::from_string(v.to_string(), data.value().to_owned())?);
				} else {
					return Err(RpcError::InvalidParams(
						"Expected 'data_expr' to be string".to_string(),
					));
				}
			}
		}

		// Process "fields" option
		if let Some(v) = obj.remove("fields") {
			if let Value::Strand(v) = v {
				self.fields = Some(syn::fields_with_capabilities(v.as_str(), capabilities)?)
			} else {
				return Err(RpcError::InvalidParams("Expected 'fields' to be string".to_string()));
			}
		}

		// Process "return" option
		if let Some(v) = obj.remove("return") {
			if let Value::Strand(v) = v {
				self.output = Some(syn::output_with_capabilities(v.as_str(), capabilities)?)
			} else {
				return Err(RpcError::InvalidParams("Expected 'return' to be string".to_string()));
			}
		}

		// Process "limit" option
		if let Some(v) = obj.remove("limit") {
			if let Value::Number(Number::Int(i)) = v {
				self.limit = Some(Limit(Expr::Literal(Literal::Integer(i))))
			} else {
				return Err(RpcError::InvalidParams("Expected 'limit' to be number".to_string()));
			}
		}

		// Process "start" option
		if let Some(v) = obj.remove("start") {
			if let Value::Number(Number::Int(i)) = v {
				self.start = Some(Start(Expr::Literal(Literal::Integer(i))))
			} else {
				return Err(RpcError::InvalidParams("Expected 'start' to be number".to_string()));
			}
		}

		// Process "cond" option
		if let Some(v) = obj.remove("cond") {
			if let Value::Strand(v) = v {
				let v = syn::expr_with_capabilities(v.as_str(), capabilities)?;
				self.cond = Some(Cond(v))
			} else {
				return Err(RpcError::InvalidParams("Expected 'cond' to be string".to_string()));
			}
		}

		// Process "version" option
		if let Some(v) = obj.remove("version") {
			let v = match v {
				Value::Datetime(d) => Expr::Literal(Literal::Datetime(d)),
				Value::Strand(v) => syn::expr_with_capabilities(v.as_str(), capabilities)?,
				_ => {
					return Err(RpcError::InvalidParams(
						"Expected 'version' to be string or datetime".to_string(),
					));
				}
			};

			self.version = Some(v)
		}

		// Process "timeout" option
		if let Some(v) = obj.remove("timeout") {
			if let Value::Duration(v) = v {
				self.timeout = Some(Timeout(v))
			} else {
				return Err(RpcError::InvalidParams(
					"Expected 'timeout' to be duration".to_string(),
				));
			}
		}

		// Process "only" option
		if let Some(v) = obj.remove("only") {
			if let Value::Bool(v) = v {
				self.only = v;
			} else {
				return Err(RpcError::InvalidParams("Expected 'only' to be boolean".to_string()));
			}
		}

		// Process "relation" option
		if let Some(v) = obj.remove("relation") {
			if let Value::Bool(v) = v {
				self.relation = v;
			} else {
				return Err(RpcError::InvalidParams(
					"Expected 'relation' to be boolean".to_string(),
				));
			}
		}

		// Process "unique" option
		if let Some(v) = obj.remove("unique") {
			if let Value::Bool(v) = v {
				self.unique = v;
			} else {
				return Err(RpcError::InvalidParams("Expected 'unique' to be boolean".to_string()));
			}
		}

		// Process "vars" option
		if let Some(v) = obj.remove("vars") {
			if let Value::Object(v) = v {
				self.vars = Some(v.into())
			} else {
				return Err(RpcError::InvalidParams("Expected 'vars' to be object".to_string()));
			}
		}

		// Process "diff" option
		if let Some(v) = obj.remove("diff") {
			if self.fields.is_some() {
				// diff and fields cannot co-exist, as diff overwrites the fields
				return Err(RpcError::InvalidParams(
					"'diff' and 'fields' cannot co-exist".to_string(),
				));
			}

			if let Value::Bool(v) = v {
				self.diff = v;
			} else {
				return Err(RpcError::InvalidParams("Expected 'diff' to be boolean".to_string()));
			}
		}

		// Process "fetch" option
		if let Some(v) = obj.remove("fetch") {
			if let Value::Strand(v) = v {
				self.fetch = Some(syn::fetchs_with_capabilities(v.as_str(), capabilities)?)
			} else {
				return Err(RpcError::InvalidParams("Expected 'fetch' to be string".to_string()));
			}
		}

		Ok(self)
	}

	pub(crate) fn data_expr(&self) -> Option<Data> {
		self.data.clone().map(|v| v.into())
	}

	pub(crate) fn merge_vars(&self, v: &Variables) -> Variables {
		match &self.vars {
			Some(vars) => vars.merged(v.clone()),
			None => v.clone(),
		}
	}
}
