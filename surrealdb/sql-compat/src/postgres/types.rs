use sqlparser::ast::DataType;

use crate::error::TranslateError;

/// Translate a PostgreSQL data type to a SurrealDB type name string.
pub fn translate_data_type(dt: &DataType) -> Result<String, TranslateError> {
	match dt {
		DataType::Boolean | DataType::Bool => Ok("bool".to_string()),
		DataType::SmallInt(_) | DataType::Int2(_) => Ok("int".to_string()),
		DataType::Integer(_) | DataType::Int4(_) | DataType::Int(_) => Ok("int".to_string()),
		DataType::BigInt(_) | DataType::Int8(_) => Ok("int".to_string()),
		DataType::Real | DataType::Float4 => Ok("float".to_string()),
		DataType::DoublePrecision | DataType::Float8 | DataType::Double(..) => {
			Ok("float".to_string())
		}
		DataType::Float(_) => Ok("float".to_string()),
		DataType::Decimal(..) | DataType::Numeric(..) => Ok("decimal".to_string()),
		DataType::Char(_)
		| DataType::Character(_)
		| DataType::Varchar(_)
		| DataType::CharacterVarying(_)
		| DataType::Text => Ok("string".to_string()),
		DataType::Bytea => Ok("bytes".to_string()),
		DataType::Timestamp(_, _) | DataType::Datetime(_) => Ok("datetime".to_string()),
		DataType::Date => Ok("datetime".to_string()),
		DataType::Uuid => Ok("uuid".to_string()),
		DataType::JSON | DataType::JSONB => Ok("object".to_string()),
		DataType::Array(inner) => match inner {
			sqlparser::ast::ArrayElemTypeDef::AngleBracket(dt)
			| sqlparser::ast::ArrayElemTypeDef::SquareBracket(dt, _)
			| sqlparser::ast::ArrayElemTypeDef::Parenthesis(dt) => {
				let inner_type = translate_data_type(dt)?;
				Ok(format!("array<{inner_type}>"))
			}
			sqlparser::ast::ArrayElemTypeDef::None => Ok("array".to_string()),
		},
		other => Err(TranslateError::unsupported(format!("data type: {other}"))),
	}
}
