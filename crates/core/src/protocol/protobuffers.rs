// use std::collections::BTreeMap;

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::str::FromStr;

use surrealdb_protocol::proto::prost_types::{
	self as prost_types, Duration as DurationProto, Timestamp as TimestampProto,
};
use surrealdb_protocol::proto::v1 as proto;

use crate::val::{Number, Value};
use anyhow::Context;
use anyhow::Result;
use surrealdb_protocol::TryFromValue;
use surrealdb_protocol::proto::v1::value::Value as ValueInner;
use surrealdb_protocol::proto::v1::{
	Array as ArrayProto, File as FileProto, Geometry as GeometryProto, NullValue as NullValueProto,
	Object as ObjectProto, RecordId as RecordIdProto, RecordIdKey as RecordIdKeyProto,
	Uuid as UuidProto, Value as ValueProto, geometry as geometry_proto,
	record_id_key as record_id_key_proto, value as value_proto,
};

impl TryFrom<ValueProto> for Value {
	type Error = anyhow::Error;

	fn try_from(proto: ValueProto) -> Result<Self, Self::Error> {
		let Some(inner) = proto.value else {
			return Ok(Value::None);
		};

		let value = match inner {
			ValueInner::Null(_) => Value::Null,
			ValueInner::Bool(v) => Value::Bool(v),
			ValueInner::Int64(v) => Value::Number(v.into()),
			ValueInner::Uint64(v) => Value::Number(v.into()),
			ValueInner::Float64(v) => Value::Number(v.into()),
			ValueInner::Decimal(v) => Value::Number(Number::Decimal(v.try_into()?)),
			ValueInner::String(v) => Value::Strand(v.into()),
			ValueInner::Duration(v) => Value::Duration(v.into()),
			ValueInner::Datetime(v) => Value::Datetime(v.try_into()?),
			ValueInner::Uuid(v) => Value::Uuid(v.try_into()?),
			ValueInner::Array(v) => Value::Array(v.try_into()?),
			ValueInner::Object(v) => Value::Object(v.try_into()?),
			ValueInner::Geometry(v) => Value::Geometry(v.try_into()?),
			ValueInner::Bytes(v) => Value::Bytes(v.into()),
			ValueInner::RecordId(v) => Value::RecordId(v.try_into()?),
			ValueInner::File(v) => Value::File(v.into()),
		};

		Ok(value)
	}
}

impl TryFrom<Value> for ValueProto {
	type Error = anyhow::Error;

	fn try_from(value: Value) -> Result<Self, Self::Error> {
		use value_proto::Value as ValueInner;
		let inner = match value {
			// These value types are simple values which
			// can be used in query responses sent to
			// the client.
			Value::None => {
				return Ok(Self {
					value: None,
				});
			}
			Value::Null => ValueInner::Null(NullValueProto {}),
			Value::Bool(boolean) => ValueInner::Bool(boolean),
			Value::Number(number) => match number {
				Number::Int(int) => ValueInner::Int64(int),
				Number::Float(float) => ValueInner::Float64(float),
				Number::Decimal(decimal) => ValueInner::Decimal(decimal.try_into()?),
			},
			Value::Strand(strand) => ValueInner::String(strand.into()),
			Value::Duration(duration) => ValueInner::Duration(DurationProto {
				seconds: duration.0.as_secs() as i64,
				nanos: duration.0.subsec_nanos() as i32,
			}),
			Value::Datetime(datetime) => ValueInner::Datetime(TimestampProto {
				seconds: datetime.0.timestamp(),
				nanos: datetime.0.timestamp_subsec_nanos() as i32,
			}),
			Value::Uuid(uuid) => ValueInner::Uuid(uuid.try_into()?),
			Value::Array(array) => ValueInner::Array(array.try_into()?),
			Value::Object(object) => ValueInner::Object(object.try_into()?),
			Value::Geometry(geometry) => ValueInner::Geometry(geometry.try_into()?),
			Value::Bytes(bytes) => ValueInner::Bytes(bytes.0.into()),
			Value::RecordId(thing) => ValueInner::RecordId(RecordIdProto {
				id: Some(thing.key.try_into()?),
				table: thing.table,
			}),
			Value::File(file) => ValueInner::File(FileProto {
				bucket: file.bucket,
				key: file.key,
			}),
			Value::Table(_) | Value::Closure(_) | Value::Regex(_) => {
				return Err(anyhow::anyhow!("Value is not network compatible: {:?}", value));
			}
		};

		Ok(Self {
			value: Some(inner),
		})
	}
}

impl From<crate::val::Duration> for DurationProto {
	fn from(duration: crate::val::Duration) -> Self {
		DurationProto {
			seconds: duration.0.as_secs() as i64,
			nanos: duration.0.subsec_nanos() as i32,
		}
	}
}

impl From<DurationProto> for crate::val::Duration {
	fn from(proto: DurationProto) -> Self {
		crate::val::Duration(std::time::Duration::from_nanos(
			proto.seconds as u64 * 1_000_000_000 + proto.nanos as u64,
		))
	}
}

impl From<crate::val::Datetime> for TimestampProto {
	fn from(datetime: crate::val::Datetime) -> Self {
		TimestampProto {
			seconds: datetime.0.timestamp(),
			nanos: datetime.0.timestamp_subsec_nanos() as i32,
		}
	}
}

impl TryFrom<TimestampProto> for crate::val::Datetime {
	type Error = anyhow::Error;

	fn try_from(proto: TimestampProto) -> Result<Self, Self::Error> {
		Ok(crate::val::Datetime(
			chrono::DateTime::from_timestamp(proto.seconds, proto.nanos as u32)
				.context("Invalid timestamp")?,
		))
	}
}

impl TryFrom<UuidProto> for crate::val::Uuid {
	type Error = uuid::Error;

	fn try_from(proto: UuidProto) -> Result<Self, Self::Error> {
		Ok(crate::val::Uuid(uuid::Uuid::from_str(&proto.value)?))
	}
}

impl TryFrom<crate::val::Uuid> for UuidProto {
	type Error = Infallible;

	fn try_from(uuid: crate::val::Uuid) -> Result<Self, Self::Error> {
		Ok(UuidProto {
			value: uuid.0.to_string(),
		})
	}
}

impl TryFrom<crate::val::Array> for ArrayProto {
	type Error = anyhow::Error;

	fn try_from(array: crate::val::Array) -> Result<Self, Self::Error> {
		Ok(ArrayProto {
			values: array.0.into_iter().map(ValueProto::try_from).collect::<Result<Vec<_>, _>>()?,
		})
	}
}

impl TryFrom<ArrayProto> for crate::val::Array {
	type Error = anyhow::Error;

	fn try_from(proto: ArrayProto) -> Result<Self, Self::Error> {
		Ok(crate::val::Array(
			proto.values.into_iter().map(Value::try_from).collect::<Result<Vec<_>, _>>()?,
		))
	}
}

impl TryFrom<ObjectProto> for crate::val::Object {
	type Error = anyhow::Error;

	fn try_from(proto: ObjectProto) -> Result<Self, Self::Error> {
		let mut object = BTreeMap::new();
		for (key, value) in proto.items {
			object.insert(key, Value::try_from(value)?);
		}
		Ok(crate::val::Object(object))
	}
}

impl TryFrom<crate::val::Object> for ObjectProto {
	type Error = anyhow::Error;

	fn try_from(object: crate::val::Object) -> Result<Self, Self::Error> {
		let mut items = BTreeMap::new();
		for (key, value) in object.0 {
			items.insert(key, ValueProto::try_from(value)?);
		}
		Ok(ObjectProto {
			items,
		})
	}
}

impl TryFrom<GeometryProto> for crate::val::Geometry {
	type Error = anyhow::Error;

	fn try_from(proto: GeometryProto) -> Result<Self, Self::Error> {
		let Some(inner) = proto.geometry else {
			return Err(anyhow::anyhow!("Invalid Geometry: missing value"));
		};

		let geometry = match inner {
			geometry_proto::Geometry::Point(v) => crate::val::Geometry::Point(v.into()),
			geometry_proto::Geometry::Line(v) => crate::val::Geometry::Line(v.into()),
			geometry_proto::Geometry::Polygon(v) => crate::val::Geometry::Polygon(v.try_into()?),
			geometry_proto::Geometry::MultiPoint(v) => crate::val::Geometry::MultiPoint(v.into()),
			geometry_proto::Geometry::MultiLine(v) => crate::val::Geometry::MultiLine(v.into()),
			geometry_proto::Geometry::MultiPolygon(v) => {
				crate::val::Geometry::MultiPolygon(v.try_into()?)
			}
			geometry_proto::Geometry::Collection(v) => {
				crate::val::Geometry::Collection(v.try_into()?)
			}
		};

		Ok(geometry)
	}
}

impl TryFrom<crate::val::Geometry> for GeometryProto {
	type Error = anyhow::Error;

	fn try_from(geometry: crate::val::Geometry) -> Result<Self, Self::Error> {
		let inner = match geometry {
			crate::val::Geometry::Point(v) => geometry_proto::Geometry::Point(v.into()),
			crate::val::Geometry::Line(v) => geometry_proto::Geometry::Line(v.into()),
			crate::val::Geometry::Polygon(v) => geometry_proto::Geometry::Polygon(v.into()),
			crate::val::Geometry::MultiPoint(v) => geometry_proto::Geometry::MultiPoint(v.into()),
			crate::val::Geometry::MultiLine(v) => geometry_proto::Geometry::MultiLine(v.into()),
			crate::val::Geometry::MultiPolygon(v) => {
				geometry_proto::Geometry::MultiPolygon(v.into())
			}
			crate::val::Geometry::Collection(v) => {
				geometry_proto::Geometry::Collection(v.try_into()?)
			}
		};

		Ok(Self {
			geometry: Some(inner),
		})
	}
}

impl TryFrom<RecordIdProto> for crate::val::RecordId {
	type Error = anyhow::Error;

	fn try_from(proto: RecordIdProto) -> Result<Self, Self::Error> {
		let Some(id) = proto.id else {
			return Err(anyhow::anyhow!("Invalid RecordId: missing id"));
		};
		Ok(Self {
			table: proto.table,
			key: id.try_into()?,
		})
	}
}

impl TryFrom<crate::val::RecordId> for RecordIdProto {
	type Error = anyhow::Error;

	fn try_from(recordid: crate::val::RecordId) -> Result<Self, Self::Error> {
		Ok(Self {
			table: recordid.table,
			id: Some(recordid.key.try_into()?),
		})
	}
}

impl From<FileProto> for crate::val::File {
	fn from(proto: FileProto) -> Self {
		Self {
			bucket: proto.bucket,
			key: proto.key,
		}
	}
}

impl From<crate::val::File> for FileProto {
	fn from(file: crate::val::File) -> Self {
		Self {
			bucket: file.bucket,
			key: file.key,
		}
	}
}

impl TryFrom<RecordIdKeyProto> for crate::val::RecordIdKey {
	type Error = anyhow::Error;

	fn try_from(proto: RecordIdKeyProto) -> Result<Self, Self::Error> {
		let Some(inner) = proto.id else {
			return Err(anyhow::anyhow!("Invalid Id: missing value"));
		};

		Ok(match inner {
			id_proto::Id::Int64(v) => crate::val::RecordIdKey::Number(v),
			id_proto::Id::String(v) => crate::val::RecordIdKey::String(v),
			id_proto::Id::Uuid(v) => crate::val::RecordIdKey::Uuid(v.try_into()?),
			id_proto::Id::Array(v) => crate::val::RecordIdKey::Array(v.try_into()?),
		})
	}
}

impl TryFrom<crate::val::RecordIdKey> for RecordIdKeyProto {
	type Error = anyhow::Error;

	fn try_from(id: crate::val::RecordIdKey) -> Result<Self, Self::Error> {
		let inner = match id {
			crate::val::RecordIdKey::Number(v) => id_proto::Id::Int64(v),
			crate::val::RecordIdKey::String(v) => id_proto::Id::String(v),
			crate::val::RecordIdKey::Uuid(v) => id_proto::Id::Uuid(v.0.into()),
			crate::val::RecordIdKey::Array(v) => id_proto::Id::Array(v.try_into()?),
			crate::val::RecordIdKey::Object(v) => {
				return Err(anyhow::anyhow!(
					"Id::Object is not supported in proto conversion: {v:?}"
				));
			}
			crate::val::RecordIdKey::Range(v) => {
				return Err(anyhow::anyhow!(
					"Id::Range is not supported in proto conversion: {v:?}"
				));
			}
		};

		Ok(Self {
			id: Some(inner),
		})
	}
}

/*
impl TryFrom<ValueProto> for crate::val::Cond {
	type Error = anyhow::Error;

	fn try_from(proto: ValueProto) -> Result<Self, Self::Error> {
		let value = Value::try_from(proto)?;
		Ok(Self(value))
	}
}

impl TryFrom<crate::val::Cond> for ValueProto {
	type Error = anyhow::Error;

	fn try_from(cond: crate::val::Cond) -> Result<Self, Self::Error> {
		let value = ValueProto::try_from(cond.0)?;
		Ok(value)
	}
}

impl TryFrom<ValueProto> for Version {
	type Error = anyhow::Error;

	fn try_from(proto: ValueProto) -> Result<Self, Self::Error> {
		let value = Value::try_from(proto)?;
		Ok(Version(value))
	}
}

impl TryFrom<Version> for ValueProto {
	type Error = anyhow::Error;

	fn try_from(version: Version) -> Result<Self, Self::Error> {
		let value = ValueProto::try_from(version.0)?;
		Ok(value)
	}
}
*/

impl TryFromValue for Value {
	#[inline]
	fn try_from_value(value: ValueProto) -> Result<Self> {
		Value::try_from(value)
	}
}

impl PartialEq<Value> for ValueProto {
	fn eq(&self, other: &Value) -> bool {
		match Value::try_from(self.clone()) {
			Ok(value) => &value == other,
			Err(_) => false,
		}
	}
}

impl PartialEq<ValueProto> for Value {
	fn eq(&self, other: &ValueProto) -> bool {
		match ValueProto::try_from(self.clone()) {
			Ok(value) => &value == other,
			Err(_) => false,
		}
	}
}

impl PartialEq<crate::sql::SqlValue> for ValueProto {
	fn eq(&self, other: &crate::sql::SqlValue) -> bool {
		let expr_value = crate::expr::Value::try_from(self.clone()).unwrap();
		crate::sql::SqlValue::from(expr_value) == *other
	}
}

impl PartialEq<ValueProto> for crate::sql::SqlValue {
	fn eq(&self, other: &ValueProto) -> bool {
		let expr_value = crate::expr::Value::try_from(other.clone()).unwrap();
		*self == crate::sql::SqlValue::from(expr_value)
	}
}

// Moved from flatbuffers.rs - these are protobuf conversions, not flatbuffer conversions
// Organized with Value first, then other types alphabetically

// === Value-related conversions ===

impl TryFrom<proto::Value> for Fetch {
	type Error = anyhow::Error;

	fn try_from(value: proto::Value) -> Result<Self, Self::Error> {
		let value = Value::try_from(value)?;
		Ok(Fetch(value))
	}
}

// === Data conversions ===

impl TryFrom<proto::Data> for Data {
	type Error = anyhow::Error;

	fn try_from(value: proto::Data) -> Result<Self, Self::Error> {
		use proto::data::Data as DataType;
		let Some(inner) = value.data else {
			return Err(anyhow::anyhow!("data is required"));
		};

		match inner {
			DataType::Empty(_) => Ok(Data::EmptyExpression),
			DataType::Set(set_expr) => {
				Ok(Data::SetExpression(try_from_set_multi_expr_proto(set_expr)?))
			}
			DataType::Unset(unset_expr) => {
				Ok(Data::UnsetExpression(try_from_unset_multi_expr_proto(unset_expr)?))
			}
			DataType::Patch(value) => Ok(Data::PatchExpression(value.try_into()?)),
			DataType::Merge(merge_expr) => Ok(Data::MergeExpression(merge_expr.try_into()?)),
			DataType::Replace(replace_expr) => {
				Ok(Data::ReplaceExpression(replace_expr.try_into()?))
			}
			DataType::Content(content_expr) => {
				Ok(Data::ContentExpression(content_expr.try_into()?))
			}
			DataType::Value(value) => Ok(Data::SingleExpression(value.try_into()?)),
			DataType::Values(values) => {
				Ok(Data::ValuesExpression(try_from_values_multi_expr_proto(values)?))
			}
			DataType::Update(update_expr) => {
				Ok(Data::UpdateExpression(try_from_set_multi_expr_proto(update_expr)?))
			}
		}
	}
}

impl TryFrom<crate::expr::data::Data> for proto::Data {
	type Error = anyhow::Error;

	fn try_from(value: crate::expr::data::Data) -> Result<Self, Self::Error> {
		use proto::data::Data as DataType;
		match value {
			crate::expr::Data::EmptyExpression => Ok(proto::Data {
				data: Some(DataType::Empty(proto::NullValue {})),
			}),
			crate::expr::Data::SetExpression(set_expr) => Ok(proto::Data {
				data: Some(DataType::Set(try_from_set_multi_expr(set_expr)?)),
			}),
			crate::expr::Data::UnsetExpression(unset_expr) => Ok(proto::Data {
				data: Some(DataType::Unset(try_from_unset_multi_expr(unset_expr)?)),
			}),
			crate::expr::Data::PatchExpression(patch_expr) => Ok(proto::Data {
				data: Some(DataType::Patch(patch_expr.try_into()?)),
			}),
			crate::expr::Data::MergeExpression(merge_expr) => Ok(proto::Data {
				data: Some(DataType::Merge(merge_expr.try_into()?)),
			}),
			crate::expr::Data::ReplaceExpression(replace_expr) => Ok(proto::Data {
				data: Some(DataType::Replace(replace_expr.try_into()?)),
			}),
			crate::expr::Data::ContentExpression(content_expr) => Ok(proto::Data {
				data: Some(DataType::Content(content_expr.try_into()?)),
			}),
			crate::expr::Data::SingleExpression(value) => Ok(proto::Data {
				data: Some(DataType::Value(value.try_into()?)),
			}),
			crate::expr::Data::ValuesExpression(values) => Ok(proto::Data {
				data: Some(DataType::Values(try_from_values_multi_expr(values)?)),
			}),
			crate::expr::Data::UpdateExpression(update_expr) => Ok(proto::Data {
				data: Some(DataType::Update(try_from_set_multi_expr(update_expr)?)),
			}),
		}
	}
}

// Data helper functions
fn try_from_set_multi_expr_proto(
	proto: proto::data::SetMultiExpr,
) -> Result<Vec<(Idiom, Operator, Value)>, anyhow::Error> {
	let mut out = Vec::new();
	for item in proto.items {
		out.push(try_from_set_expr_proto(item)?);
	}
	Ok(out)
}

fn try_from_set_expr_proto(
	proto::data::SetExpr {
		idiom,
		operator,
		value,
	}: proto::data::SetExpr,
) -> Result<(Idiom, Operator, Value), anyhow::Error> {
	let idiom = idiom.context("idiom is required")?.try_into()?;
	let operator = Operator::try_from(proto::Operator::try_from(operator)?)?;
	let value = value.context("value is required")?.try_into()?;
	Ok((idiom, operator, value))
}

fn try_from_set_multi_expr(
	expr: Vec<(Idiom, Operator, Value)>,
) -> Result<proto::data::SetMultiExpr, anyhow::Error> {
	let mut out = proto::data::SetMultiExpr::default();
	for item in expr {
		out.items.push(try_from_set_expr(item)?);
	}
	Ok(out)
}

fn try_from_set_expr(
	expr: (Idiom, Operator, Value),
) -> Result<proto::data::SetExpr, anyhow::Error> {
	let (idiom, operator, value) = expr;
	Ok(proto::data::SetExpr {
		idiom: Some(idiom.try_into()?),
		operator: proto::Operator::try_from(operator)? as i32,
		value: Some(value.try_into()?),
	})
}

fn try_from_unset_multi_expr_proto(
	proto: proto::data::UnsetMultiExpr,
) -> Result<Vec<Idiom>, anyhow::Error> {
	let mut out = Vec::new();
	for item in proto.items {
		out.push(item.try_into()?);
	}
	Ok(out)
}

fn try_from_unset_multi_expr(
	expr: Vec<Idiom>,
) -> Result<proto::data::UnsetMultiExpr, anyhow::Error> {
	let mut out = proto::data::UnsetMultiExpr::default();
	for item in expr {
		out.items.push(item.try_into()?);
	}
	Ok(out)
}

fn try_from_values_multi_expr_proto(
	proto: proto::data::ValuesMultiExpr,
) -> Result<Vec<Vec<(Idiom, Value)>>, anyhow::Error> {
	let mut out = Vec::new();
	for item in proto.items {
		out.push(try_from_values_expr_proto(item)?);
	}
	Ok(out)
}

fn try_from_values_expr_proto(
	proto: proto::data::ValuesExpr,
) -> Result<Vec<(Idiom, Value)>, anyhow::Error> {
	let mut out = Vec::new();
	for item in proto.items {
		let idiom = item.idiom.context("idiom is required")?.try_into()?;
		let value = item.value.context("value is required")?.try_into()?;
		out.push((idiom, value));
	}
	Ok(out)
}

fn try_from_values_multi_expr(
	expr: Vec<Vec<(Idiom, Value)>>,
) -> Result<proto::data::ValuesMultiExpr, anyhow::Error> {
	let mut out = proto::data::ValuesMultiExpr::default();
	for item in expr {
		out.items.push(try_from_values_expr(item)?);
	}
	Ok(out)
}

fn try_from_values_expr(
	expr: Vec<(Idiom, Value)>,
) -> Result<proto::data::ValuesExpr, anyhow::Error> {
	let mut out = proto::data::ValuesExpr::default();
	for item in expr {
		let idiom = item.0.try_into()?;
		let value = item.1.try_into()?;
		out.items.push(proto::data::IdiomValuePair {
			idiom: Some(idiom),
			value: Some(value),
		});
	}
	Ok(out)
}

// === Explain conversions ===

impl TryFrom<proto::Explain> for crate::expr::Explain {
	type Error = anyhow::Error;

	fn try_from(value: proto::Explain) -> Result<Self, Self::Error> {
		Ok(Self(value.explain))
	}
}

impl TryFrom<crate::expr::Explain> for proto::Explain {
	type Error = anyhow::Error;

	fn try_from(value: crate::expr::Explain) -> Result<Self, Self::Error> {
		Ok(Self {
			explain: value.0,
		})
	}
}

// === Fetchs conversions ===

impl TryFrom<proto::Fetchs> for Fetchs {
	type Error = anyhow::Error;

	fn try_from(value: proto::Fetchs) -> Result<Self, Self::Error> {
		let items =
			value.items.into_iter().map(TryInto::try_into).collect::<Result<Vec<_>, _>>()?;
		Ok(Fetchs(items))
	}
}

// === Field conversions ===

impl TryFrom<proto::fields::Field> for Field {
	type Error = anyhow::Error;

	fn try_from(proto: proto::fields::Field) -> Result<Self, Self::Error> {
		let Some(inner_field) = proto.field else {
			return Err(anyhow::anyhow!("Missing field"));
		};
		match inner_field {
			proto::fields::field::Field::All(_) => Ok(Field::All),
			proto::fields::field::Field::Single(single) => Ok(Field::Single {
				expr: single.expr.context("Missing expr")?.try_into()?,
				alias: single.alias.map(TryInto::try_into).transpose()?,
			}),
		}
	}
}

impl TryFrom<Field> for proto::fields::Field {
	type Error = anyhow::Error;

	fn try_from(value: Field) -> Result<Self, Self::Error> {
		let field = match value {
			Field::All => proto::fields::field::Field::All(proto::NullValue::default()),
			Field::Single {
				expr,
				alias,
			} => proto::fields::field::Field::Single(proto::fields::SingleField {
				expr: Some(expr.try_into()?),
				alias: alias.map(TryInto::try_into).transpose()?,
			}),
		};

		Ok(proto::fields::Field {
			field: Some(field),
		})
	}
}

// === Fields conversions ===

impl TryFrom<proto::Fields> for Fields {
	type Error = anyhow::Error;

	fn try_from(value: proto::Fields) -> Result<Self, Self::Error> {
		let single = value.single;
		let fields =
			value.fields.into_iter().map(TryInto::try_into).collect::<Result<Vec<_>, _>>()?;
		Ok(Fields(fields, single))
	}
}

impl TryFrom<Fields> for proto::Fields {
	type Error = anyhow::Error;

	fn try_from(value: Fields) -> Result<Self, Self::Error> {
		Ok(proto::Fields {
			single: value.1,
			fields: value.0.into_iter().map(TryInto::try_into).collect::<Result<Vec<_>, _>>()?,
		})
	}
}

// === Idiom conversions ===

impl TryFrom<proto::Idiom> for Idiom {
	type Error = anyhow::Error;

	fn try_from(proto: proto::Idiom) -> Result<Self, Self::Error> {
		idiom(&proto.value)
	}
}

impl TryFrom<Idiom> for proto::Idiom {
	type Error = anyhow::Error;

	fn try_from(value: Idiom) -> Result<Self, Self::Error> {
		Ok(proto::Idiom {
			value: value.to_string(),
		})
	}
}

// === Limit conversions ===

impl TryFrom<proto::Limit> for crate::expr::Limit {
	type Error = anyhow::Error;

	fn try_from(value: proto::Limit) -> Result<Self, Self::Error> {
		Ok(Self(Value::Number(Number::Int(value.limit as i64))))
	}
}

// === Operator conversions ===

impl TryFrom<proto::Operator> for Operator {
	type Error = anyhow::Error;

	fn try_from(value: proto::Operator) -> Result<Self, Self::Error> {
		match value {
			proto::Operator::Unspecified => Err(anyhow::anyhow!("operator is required")),
			proto::Operator::Neg => Ok(Operator::Neg),
			proto::Operator::Not => Ok(Operator::Not),
			proto::Operator::Or => Ok(Operator::Or),
			proto::Operator::And => Ok(Operator::And),
			proto::Operator::Tco => Ok(Operator::Tco),
			proto::Operator::Nco => Ok(Operator::Nco),
			proto::Operator::Add => Ok(Operator::Add),
			proto::Operator::Sub => Ok(Operator::Sub),
			proto::Operator::Mul => Ok(Operator::Mul),
			proto::Operator::Div => Ok(Operator::Div),
			proto::Operator::Rem => Ok(Operator::Rem),
			proto::Operator::Pow => Ok(Operator::Pow),
			proto::Operator::Inc => Ok(Operator::Inc),
			proto::Operator::Dec => Ok(Operator::Dec),
			proto::Operator::Ext => Ok(Operator::Ext),
			proto::Operator::Equal => Ok(Operator::Equal),
			proto::Operator::Exact => Ok(Operator::Exact),
			proto::Operator::NotEqual => Ok(Operator::NotEqual),
			proto::Operator::AllEqual => Ok(Operator::AllEqual),
			proto::Operator::AnyEqual => Ok(Operator::AnyEqual),
			proto::Operator::LessThan => Ok(Operator::LessThan),
			proto::Operator::LessThanOrEqual => Ok(Operator::LessThanOrEqual),
			proto::Operator::GreaterThan => Ok(Operator::MoreThan),
			proto::Operator::GreaterThanOrEqual => Ok(Operator::MoreThanOrEqual),
			proto::Operator::Contain => Ok(Operator::Contain),
			proto::Operator::NotContain => Ok(Operator::NotContain),
			proto::Operator::ContainAll => Ok(Operator::ContainAll),
			proto::Operator::ContainAny => Ok(Operator::ContainAny),
			proto::Operator::ContainNone => Ok(Operator::ContainNone),
			proto::Operator::Inside => Ok(Operator::Inside),
			proto::Operator::NotInside => Ok(Operator::NotInside),
			proto::Operator::AllInside => Ok(Operator::AllInside),
			proto::Operator::AnyInside => Ok(Operator::AnyInside),
			proto::Operator::NoneInside => Ok(Operator::NoneInside),
			proto::Operator::Outside => Ok(Operator::Outside),
			proto::Operator::Intersects => Ok(Operator::Intersects),
		}
	}
}

impl TryFrom<Operator> for proto::Operator {
	type Error = anyhow::Error;

	fn try_from(value: Operator) -> Result<Self, Self::Error> {
		match value {
			Operator::Neg => Ok(proto::Operator::Neg),
			Operator::Not => Ok(proto::Operator::Not),
			Operator::Or => Ok(proto::Operator::Or),
			Operator::And => Ok(proto::Operator::And),
			Operator::Tco => Ok(proto::Operator::Tco),
			Operator::Nco => Ok(proto::Operator::Nco),
			Operator::Add => Ok(proto::Operator::Add),
			Operator::Sub => Ok(proto::Operator::Sub),
			Operator::Mul => Ok(proto::Operator::Mul),
			Operator::Div => Ok(proto::Operator::Div),
			Operator::Rem => Ok(proto::Operator::Rem),
			Operator::Pow => Ok(proto::Operator::Pow),
			Operator::Inc => Ok(proto::Operator::Inc),
			Operator::Dec => Ok(proto::Operator::Dec),
			Operator::Ext => Ok(proto::Operator::Ext),
			Operator::Equal => Ok(proto::Operator::Equal),
			Operator::Exact => Ok(proto::Operator::Exact),
			Operator::NotEqual => Ok(proto::Operator::NotEqual),
			Operator::AllEqual => Ok(proto::Operator::AllEqual),
			Operator::AnyEqual => Ok(proto::Operator::AnyEqual),
			Operator::LessThan => Ok(proto::Operator::LessThan),
			Operator::LessThanOrEqual => Ok(proto::Operator::LessThanOrEqual),
			Operator::MoreThan => Ok(proto::Operator::GreaterThan),
			Operator::MoreThanOrEqual => Ok(proto::Operator::GreaterThanOrEqual),
			Operator::Contain => Ok(proto::Operator::Contain),
			Operator::NotContain => Ok(proto::Operator::NotContain),
			Operator::ContainAll => Ok(proto::Operator::ContainAll),
			Operator::ContainAny => Ok(proto::Operator::ContainAny),
			Operator::ContainNone => Ok(proto::Operator::ContainNone),
			Operator::Inside => Ok(proto::Operator::Inside),
			Operator::NotInside => Ok(proto::Operator::NotInside),
			Operator::AllInside => Ok(proto::Operator::AllInside),
			Operator::AnyInside => Ok(proto::Operator::AnyInside),
			Operator::NoneInside => Ok(proto::Operator::NoneInside),
			Operator::Outside => Ok(proto::Operator::Outside),
			Operator::Intersects => Ok(proto::Operator::Intersects),
			Operator::Matches(_, _) => Err(anyhow::anyhow!("matches is not supported")),
			Operator::Knn(_, _) => Err(anyhow::anyhow!("knn is not supported")),
			Operator::Ann(_, _) => Err(anyhow::anyhow!("ann is not supported")),
		}
	}
}

// === Output conversions ===

impl TryFrom<proto::Output> for crate::expr::output::Output {
	type Error = anyhow::Error;

	fn try_from(value: proto::Output) -> Result<Self, Self::Error> {
		use proto::output::Output as OutputType;
		let Some(inner) = value.output else {
			return Ok(crate::expr::output::Output::None);
		};

		match inner {
			OutputType::Null(_) => Ok(crate::expr::output::Output::Null),
			OutputType::Diff(_) => Ok(crate::expr::output::Output::Diff),
			OutputType::After(_) => Ok(crate::expr::output::Output::After),
			OutputType::Before(_) => Ok(crate::expr::output::Output::Before),
			OutputType::Fields(fields) => {
				Ok(crate::expr::output::Output::Fields(fields.try_into()?))
			}
		}
	}
}

impl TryFrom<crate::expr::output::Output> for proto::Output {
	type Error = anyhow::Error;

	fn try_from(value: crate::expr::output::Output) -> Result<Self, Self::Error> {
		use proto::output::Output as OutputType;
		match value {
			crate::expr::output::Output::None => Ok(proto::Output {
				output: None,
			}),
			crate::expr::output::Output::Null => Ok(proto::Output {
				output: Some(OutputType::Null(proto::NullValue {})),
			}),
			crate::expr::output::Output::Diff => Ok(proto::Output {
				output: Some(OutputType::Diff(proto::NullValue {})),
			}),
			crate::expr::output::Output::After => Ok(proto::Output {
				output: Some(OutputType::After(proto::NullValue {})),
			}),
			crate::expr::output::Output::Before => Ok(proto::Output {
				output: Some(OutputType::Before(proto::NullValue {})),
			}),
			crate::expr::output::Output::Fields(fields) => Ok(proto::Output {
				output: Some(OutputType::Fields(fields.try_into()?)),
			}),
		}
	}
}

// === Start conversions ===

impl TryFrom<proto::Start> for crate::expr::Start {
	type Error = anyhow::Error;

	fn try_from(value: proto::Start) -> Result<Self, Self::Error> {
		Ok(Self(Value::Number(Number::Int(value.start as i64))))
	}
}

impl TryFrom<crate::expr::Start> for proto::Start {
	type Error = anyhow::Error;

	fn try_from(value: crate::expr::Start) -> Result<Self, Self::Error> {
		let start = match value.0 {
			Value::Number(Number::Int(start)) => start as u64,
			_ => return Err(anyhow::anyhow!("Invalid start value")),
		};
		Ok(Self {
			start,
		})
	}
}

// === Timeout conversions ===

impl From<prost_types::Duration> for Timeout {
	fn from(value: prost_types::Duration) -> Self {
		Timeout(Duration::from(value))
	}
}

// === With conversions ===

impl TryFrom<proto::With> for crate::expr::With {
	type Error = anyhow::Error;

	fn try_from(value: proto::With) -> Result<Self, Self::Error> {
		if value.indexes.is_empty() {
			Ok(Self::NoIndex)
		} else {
			Ok(Self::Index(value.indexes.into_iter().map(|s| s.to_string()).collect()))
		}
	}
}

impl TryFrom<crate::expr::With> for proto::With {
	type Error = anyhow::Error;

	fn try_from(value: crate::expr::With) -> Result<Self, Self::Error> {
		let indexes = match value {
			crate::expr::With::Index(indexes) => {
				indexes.into_iter().map(|s| s.to_string()).collect()
			}
			crate::expr::With::NoIndex => vec![],
		};
		Ok(Self {
			indexes,
		})
	}
}
