
use crate::proto::surrealdb::ast::{
    SqlValue as SqlValueProto,
    Number as NumberProto,

};

impl TryFrom<SqlValueProto> for crate::sql::SqlValue {
	type Error = anyhow::Error;

	fn try_from(proto: SqlValueProto) -> Result<Self, Self::Error> {
        use crate::proto::surrealdb::ast::sql_value;

		let Some(inner) = proto.inner else {
			return Ok(crate::sql::SqlValue::None);
		};

		// Null null = 1;
		// bool bool = 2;
		// Number number = 3;
		// string strand = 4;
		// Duration duration = 5;
		// Datetime datetime = 6;
		// Uuid uuid = 7;
		// Array array = 8;
		// Object object = 9;
		// Geometry geometry = 10;
		// bytes bytes = 11;
		// Thing thing = 12;
		// Ident param = 13;
		// Idiom idiom = 14;
		// Table table = 15;
		// Mock mock = 16;
		// Regex regex = 17;
		// Cast cast = 18;
		// Block block = 19;
		// SqlValueRange range = 20;
		// Edges edges = 21;
		// Block future = 22;
		// Constant constant = 23;
		// Function function = 24;
		// Subquery subquery = 25;
		// Expression expression = 26;
		// Statements query = 27;
		// Model model = 28;
		// Closure closure = 29;
		// Refs refs = 30;
		// File file = 31;
		let sql_value = match inner {
			sql_value::Inner::Null(_) => crate::sql::SqlValue::Null,
			sql_value::Inner::Bool(v) => crate::sql::SqlValue::Bool(v),
			sql_value::Inner::Number(v) => crate::sql::SqlValue::Number(v.try_into()?),
			sql_value::Inner::Strand(v) => crate::sql::SqlValue::Strand(v.into()),
			sql_value::Inner::Duration(v) => crate::sql::SqlValue::Duration(v.into()),
			sql_value::Inner::Datetime(v) => crate::sql::SqlValue::Datetime(v.try_into()?),
			sql_value::Inner::Uuid(v) => crate::sql::SqlValue::Uuid(v.try_into()?),
			_ => todo!("Handle other SqlValue types"),
			// sql_value::Inner::Array(v) => {
			//     crate::sql::SqlValue::Array(v.try_into()?)
			// }
			// sql_value::Inner::Object(v) => {
			//     crate::sql::SqlValue::Object(v.try_into()?)
			// }
			// sql_value::Inner::Geometry(v) => crate::sql::SqlValue::Geometry(v.into()),
			// sql_value::Inner::Bytes(v) => crate::sql::SqlValue::Bytes(v.into()),
			// sql_value::Inner::Thing(v) => crate::sql::SqlValue::Thing(v.into()),
			// sql_value::Inner::Param(v) => crate::sql::SqlValue::Param(v.into()),
			// sql_value::Inner::Idiom(v) => crate::sql::SqlValue::Idiom(v.into()),
			// sql_value::Inner::Table(v) => crate::sql::SqlValue::Table(v.into()),
			// sql_value::Inner::Mock(v) => crate::sql::SqlValue::Mock(v.into()),
			// sql_value::Inner::Regex(v) => crate::sql::SqlValue::Regex(v.into()),
			// sql_value::Inner::Cast(v) => crate::sql::SqlValue::Cast(v.into()),
			// sql_value::Inner::Block(v) => crate::sql::SqlValue::Block(v.into()),
			// sql_value::Inner::Range(v) => crate::sql::SqlValue::Range(v.into()),
			// sql_value::Inner::Edges(v) => crate::sql::SqlValue::Edges(v.into()),
			// sql_value::Inner::Future(v) => crate::sql::SqlValue::Future(v.into()),
			// sql_value::Inner::Constant(v) => crate::sql::SqlValue::Constant(v.into()),
			// sql_value::Inner::Function(v) => crate::sql::SqlValue::Function(v.into()),
			// sql_value::Inner::Subquery(v) => crate::sql::SqlValue::Subquery(v.into()),
			// sql_value::Inner::Expression(v) => crate::sql::SqlValue::Expression(v.into()),
			// sql_value::Inner::Query(v) => crate::sql::SqlValue::Query(v.into()),
			// sql_value::Inner::Model(v) => crate::sql::SqlValue::Model(v.into()),
			// sql_value::Inner::Closure(v) => crate::sql::SqlValue::Closure(v.into()),
			// sql_value::Inner::Refs(v) => crate::sql::SqlValue::Refs(v.into()),
			// sql_value::Inner::File(v) => crate::sql::SqlValue::File(v.into()),
		};

		Ok(sql_value)
	}
}

impl TryFrom<NumberProto> for crate::sql::Number {
	type Error = anyhow::Error;

	fn try_from(proto: NumberProto) -> Result<Self, Self::Error> {
        use crate::proto::surrealdb::ast::number;
		let Some(inner) = proto.inner else {
			return Err(anyhow::anyhow!("Invalid Number: missing value"));
		};

		Ok(match inner {
			number::Inner::Int(v) => crate::sql::Number::Int(v),
			number::Inner::Float(v) => crate::sql::Number::Float(v),
			number::Inner::Decimal(v) => {
				crate::sql::Number::Decimal(crate::sql::DecimalExt::from_str_normalized(&v)?)
			}
		})
	}
}

impl From<super::google::protobuf::Duration> for crate::sql::Duration {
	fn from(proto: super::google::protobuf::Duration) -> Self {
		crate::sql::Duration(std::time::Duration::from_nanos(
			proto.seconds as u64 * 1_000_000_000 + proto.nanos as u64,
		))
	}
}

impl TryFrom<super::google::protobuf::Timestamp> for crate::sql::Datetime {
	type Error = anyhow::Error;
	fn try_from(proto: super::google::protobuf::Timestamp) -> Result<Self, Self::Error> {
		Ok(crate::sql::Datetime(proto.try_into()?))
	}
}
