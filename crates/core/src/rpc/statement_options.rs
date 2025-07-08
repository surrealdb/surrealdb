use crate::sql::{Data, SqlValue};

use super::RpcError;

#[derive(Clone, Debug)]
pub(crate) enum RpcData {
	Patch(SqlValue),
	Merge(SqlValue),
	Replace(SqlValue),
	Content(SqlValue),
	Single(SqlValue),
}

impl From<RpcData> for Data {
	fn from(data: RpcData) -> Self {
		match data {
			RpcData::Patch(v) => Data::PatchExpression(v),
			RpcData::Merge(v) => Data::MergeExpression(v),
			RpcData::Replace(v) => Data::ReplaceExpression(v),
			RpcData::Content(v) => Data::ContentExpression(v),
			RpcData::Single(v) => Data::SingleExpression(v),
		}
	}
}

impl RpcData {
	pub(crate) fn value(&self) -> &SqlValue {
		match self {
			RpcData::Patch(v) => v,
			RpcData::Merge(v) => v,
			RpcData::Replace(v) => v,
			RpcData::Content(v) => v,
			RpcData::Single(v) => v,
		}
	}

	pub(crate) fn from_string(str: String, v: SqlValue) -> Result<RpcData, RpcError> {
		match str.to_lowercase().as_str() {
			"patch" => Ok(RpcData::Patch(v)),
			"merge" => Ok(RpcData::Merge(v)),
			"replace" => Ok(RpcData::Replace(v)),
			"content" => Ok(RpcData::Content(v)),
			"single" => Ok(RpcData::Single(v)),
			_ => Err(RpcError::InvalidParams),
		}
	}
}
