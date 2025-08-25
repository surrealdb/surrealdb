use std::collections::BTreeMap;

use rust_decimal::Decimal;
use surrealdb_protocol::fb::v1 as proto_fb;

use super::{FromFlatbuffers, ToFlatbuffers};
use crate::{Duration, GeometryKind, Kind, KindLiteral};

impl ToFlatbuffers for Kind {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::Kind<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> anyhow::Result<Self::Output<'bldr>> {
		let args = match self {
			Self::Any => proto_fb::KindArgs {
				kind_type: proto_fb::KindType::Any,
				kind: Some(
					proto_fb::AnyKind::create(builder, &proto_fb::AnyKindArgs {}).as_union_value(),
				),
			},
			Self::None => proto_fb::KindArgs {
				kind_type: proto_fb::KindType::NONE,
				kind: None,
			},
			Self::Null => proto_fb::KindArgs {
				kind_type: proto_fb::KindType::Null,
				kind: Some(
					proto_fb::NullKind::create(builder, &proto_fb::NullKindArgs {})
						.as_union_value(),
				),
			},
			Self::Bool => proto_fb::KindArgs {
				kind_type: proto_fb::KindType::Bool,
				kind: Some(
					proto_fb::BoolKind::create(builder, &proto_fb::BoolKindArgs {})
						.as_union_value(),
				),
			},
			Self::Bytes => proto_fb::KindArgs {
				kind_type: proto_fb::KindType::Bytes,
				kind: Some(
					proto_fb::BytesKind::create(builder, &proto_fb::BytesKindArgs {})
						.as_union_value(),
				),
			},
			Self::Datetime => proto_fb::KindArgs {
				kind_type: proto_fb::KindType::Datetime,
				kind: Some(
					proto_fb::DatetimeKind::create(builder, &proto_fb::DatetimeKindArgs {})
						.as_union_value(),
				),
			},
			Self::Decimal => proto_fb::KindArgs {
				kind_type: proto_fb::KindType::Decimal,
				kind: Some(
					proto_fb::DecimalKind::create(builder, &proto_fb::DecimalKindArgs {})
						.as_union_value(),
				),
			},
			Self::Duration => proto_fb::KindArgs {
				kind_type: proto_fb::KindType::Duration,
				kind: Some(
					proto_fb::DurationKind::create(builder, &proto_fb::DurationKindArgs {})
						.as_union_value(),
				),
			},
			Self::Float => proto_fb::KindArgs {
				kind_type: proto_fb::KindType::Float,
				kind: Some(
					proto_fb::FloatKind::create(builder, &proto_fb::FloatKindArgs {})
						.as_union_value(),
				),
			},
			Self::Int => proto_fb::KindArgs {
				kind_type: proto_fb::KindType::Int,
				kind: Some(
					proto_fb::IntKind::create(builder, &proto_fb::IntKindArgs {}).as_union_value(),
				),
			},
			Self::Number => proto_fb::KindArgs {
				kind_type: proto_fb::KindType::Number,
				kind: Some(
					proto_fb::NumberKind::create(builder, &proto_fb::NumberKindArgs {})
						.as_union_value(),
				),
			},
			Self::Object => proto_fb::KindArgs {
				kind_type: proto_fb::KindType::Object,
				kind: Some(
					proto_fb::ObjectKind::create(builder, &proto_fb::ObjectKindArgs {})
						.as_union_value(),
				),
			},
			Self::String => proto_fb::KindArgs {
				kind_type: proto_fb::KindType::String,
				kind: Some(
					proto_fb::StringKind::create(builder, &proto_fb::StringKindArgs {})
						.as_union_value(),
				),
			},
			Self::Uuid => proto_fb::KindArgs {
				kind_type: proto_fb::KindType::Uuid,
				kind: Some(
					proto_fb::UuidKind::create(builder, &proto_fb::UuidKindArgs {})
						.as_union_value(),
				),
			},
			Self::Regex => proto_fb::KindArgs {
				kind_type: proto_fb::KindType::Regex,
				kind: Some(
					proto_fb::RegexKind::create(builder, &proto_fb::RegexKindArgs {})
						.as_union_value(),
				),
			},
			Self::Record(tables) => {
				let table_offsets: Vec<_> = tables
					.iter()
					.map(|t| {
						let name = builder.create_string(t.as_str());
						proto_fb::TableName::create(
							builder,
							&proto_fb::TableNameArgs {
								name: Some(name),
							},
						)
					})
					.collect::<Vec<_>>();
				let tables = builder.create_vector(&table_offsets);
				proto_fb::KindArgs {
					kind_type: proto_fb::KindType::Record,
					kind: Some(
						proto_fb::RecordKind::create(
							builder,
							&proto_fb::RecordKindArgs {
								tables: Some(tables),
							},
						)
						.as_union_value(),
					),
				}
			}
			Self::Geometry(types) => {
				let type_offsets: Vec<_> =
					types.iter().map(|t| t.to_fb(builder)).collect::<anyhow::Result<Vec<_>>>()?;
				let types = builder.create_vector(&type_offsets);

				proto_fb::KindArgs {
					kind_type: proto_fb::KindType::Geometry,
					kind: Some(
						proto_fb::GeometryKind::create(
							builder,
							&proto_fb::GeometryKindArgs {
								types: Some(types),
							},
						)
						.as_union_value(),
					),
				}
			}
			Self::Option(kind) => {
				let inner = kind.to_fb(builder)?;
				proto_fb::KindArgs {
					kind_type: proto_fb::KindType::Option,
					kind: Some(
						proto_fb::OptionKind::create(
							builder,
							&proto_fb::OptionKindArgs {
								inner: Some(inner),
							},
						)
						.as_union_value(),
					),
				}
			}
			Self::Either(kinds) => {
				let kind_offsets: Vec<_> =
					kinds.iter().map(|k| k.to_fb(builder)).collect::<anyhow::Result<Vec<_>>>()?;
				let kinds = builder.create_vector(&kind_offsets);

				proto_fb::KindArgs {
					kind_type: proto_fb::KindType::Either,
					kind: Some(
						proto_fb::EitherKind::create(
							builder,
							&proto_fb::EitherKindArgs {
								kinds: Some(kinds),
							},
						)
						.as_union_value(),
					),
				}
			}
			Self::Set(inner, size) => {
				let inner = inner.to_fb(builder)?;
				let size = size.map(|len| len.to_fb(builder)).transpose()?;

				proto_fb::KindArgs {
					kind_type: proto_fb::KindType::Set,
					kind: Some(
						proto_fb::SetKind::create(
							builder,
							&proto_fb::SetKindArgs {
								inner: Some(inner),
								size,
							},
						)
						.as_union_value(),
					),
				}
			}
			Self::Array(inner, size) => {
				let inner = inner.to_fb(builder)?;
				let size = size.map(|len| len.to_fb(builder)).transpose()?;

				proto_fb::KindArgs {
					kind_type: proto_fb::KindType::Array,
					kind: Some(
						proto_fb::ArrayKind::create(
							builder,
							&proto_fb::ArrayKindArgs {
								inner: Some(inner),
								size,
							},
						)
						.as_union_value(),
					),
				}
			}
			Self::Function(args, return_type) => {
				let args = args
					.as_ref()
					.map(|args| -> anyhow::Result<_> {
						let arg_offsets: Vec<_> = args
							.iter()
							.map(|arg| arg.to_fb(builder))
							.collect::<anyhow::Result<Vec<_>>>()?;
						Ok(builder.create_vector(&arg_offsets))
					})
					.transpose()?;

				let return_type = return_type
					.as_ref()
					.map(|return_type| return_type.to_fb(builder))
					.transpose()?;

				proto_fb::KindArgs {
					kind_type: proto_fb::KindType::Function,
					kind: Some(
						proto_fb::FunctionKind::create(
							builder,
							&proto_fb::FunctionKindArgs {
								args,
								return_type,
							},
						)
						.as_union_value(),
					),
				}
			}
			Self::Range => proto_fb::KindArgs {
				kind_type: proto_fb::KindType::Range,
				kind: Some(
					proto_fb::RangeKind::create(builder, &proto_fb::RangeKindArgs {})
						.as_union_value(),
				),
			},
			Self::Literal(literal) => {
				let literal = literal.to_fb(builder)?.as_union_value();
				proto_fb::KindArgs {
					kind_type: proto_fb::KindType::Literal,
					kind: Some(literal),
				}
			}
			Self::File(buckets) => {
				let bucket_offsets: Vec<_> =
					buckets.iter().map(|b| builder.create_string(b.as_str())).collect();
				let buckets = builder.create_vector(&bucket_offsets);

				proto_fb::KindArgs {
					kind_type: proto_fb::KindType::File,
					kind: Some(
						proto_fb::FileKind::create(
							builder,
							&proto_fb::FileKindArgs {
								buckets: Some(buckets),
							},
						)
						.as_union_value(),
					),
				}
			}
		};

		Ok(proto_fb::Kind::create(builder, &args))
	}
}

impl ToFlatbuffers for KindLiteral {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::LiteralKind<'bldr>>;

	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> anyhow::Result<flatbuffers::WIPOffset<proto_fb::LiteralKind<'bldr>>> {
		let args = match self {
			Self::Bool(bool) => proto_fb::LiteralKindArgs {
				literal_type: proto_fb::LiteralType::Bool,
				literal: Some(
					proto_fb::BoolValue::create(
						builder,
						&proto_fb::BoolValueArgs {
							value: *bool,
						},
					)
					.as_union_value(),
				),
			},
			Self::String(string) => {
				let string_offset = builder.create_string(string);
				proto_fb::LiteralKindArgs {
					literal_type: proto_fb::LiteralType::String,
					literal: Some(
						proto_fb::StringValue::create(
							builder,
							&proto_fb::StringValueArgs {
								value: Some(string_offset),
							},
						)
						.as_union_value(),
					),
				}
			}
			Self::Integer(i) => proto_fb::LiteralKindArgs {
				literal_type: proto_fb::LiteralType::Int64,
				literal: Some(
					proto_fb::Int64Value::create(
						builder,
						&proto_fb::Int64ValueArgs {
							value: *i,
						},
					)
					.as_union_value(),
				),
			},
			Self::Float(f) => proto_fb::LiteralKindArgs {
				literal_type: proto_fb::LiteralType::Float64,
				literal: Some(
					proto_fb::Float64Value::create(
						builder,
						&proto_fb::Float64ValueArgs {
							value: *f,
						},
					)
					.as_union_value(),
				),
			},
			Self::Decimal(d) => proto_fb::LiteralKindArgs {
				literal_type: proto_fb::LiteralType::Decimal,
				literal: Some(d.to_fb(builder)?.as_union_value()),
			},
			Self::Duration(duration) => proto_fb::LiteralKindArgs {
				literal_type: proto_fb::LiteralType::Duration,
				literal: Some(duration.to_fb(builder)?.as_union_value()),
			},
			Self::Array(array) => {
				let array_items: Vec<_> = array
					.iter()
					.map(|item| item.to_fb(builder))
					.collect::<anyhow::Result<Vec<_>>>()?;
				let kinds_vector = builder.create_vector(&array_items);
				let literal_array = proto_fb::LiteralArray::create(
					builder,
					&proto_fb::LiteralArrayArgs {
						kinds: Some(kinds_vector),
					},
				);
				proto_fb::LiteralKindArgs {
					literal_type: proto_fb::LiteralType::Array,
					literal: Some(literal_array.as_union_value()),
				}
			}
			Self::Object(object) => proto_fb::LiteralKindArgs {
				literal_type: proto_fb::LiteralType::Object,
				literal: Some(object.to_fb(builder)?.as_union_value()),
			},
		};

		Ok(proto_fb::LiteralKind::create(builder, &args))
	}
}

impl FromFlatbuffers for Kind {
	type Input<'a> = proto_fb::Kind<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		use proto_fb::KindType;

		let kind_type = input.kind_type();

		match kind_type {
			KindType::Any => Ok(Kind::Any),
			KindType::NONE => Ok(Kind::None),
			KindType::Null => Ok(Kind::Null),
			KindType::Bool => Ok(Kind::Bool),
			KindType::Int => Ok(Kind::Int),
			KindType::Float => Ok(Kind::Float),
			KindType::Decimal => Ok(Kind::Decimal),
			KindType::Number => Ok(Kind::Number),
			KindType::String => Ok(Kind::String),
			KindType::Duration => Ok(Kind::Duration),
			KindType::Datetime => Ok(Kind::Datetime),
			KindType::Uuid => Ok(Kind::Uuid),
			KindType::Bytes => Ok(Kind::Bytes),
			KindType::Object => Ok(Kind::Object),
			KindType::Record => {
				let Some(record) = input.kind_as_record() else {
					return Err(anyhow::anyhow!("Missing record kind"));
				};
				let tables = if let Some(tables) = record.tables() {
					tables
						.iter()
						.map(|t| {
							let Some(name) = t.name() else {
								return Err(anyhow::anyhow!("Missing table name"));
							};
							Ok(name.to_string())
						})
						.collect::<anyhow::Result<Vec<_>>>()?
				} else {
					Vec::new()
				};
				Ok(Kind::Record(tables))
			}
			KindType::Geometry => {
				let Some(geometry) = input.kind_as_geometry() else {
					return Err(anyhow::anyhow!("Missing geometry kind"));
				};
				let types = if let Some(types) = geometry.types() {
					types.iter().map(GeometryKind::from_fb).collect::<anyhow::Result<Vec<_>>>()?
				} else {
					Vec::new()
				};
				Ok(Kind::Geometry(types))
			}
			KindType::Set => {
				let Some(set) = input.kind_as_set() else {
					return Err(anyhow::anyhow!("Missing set kind"));
				};
				let Some(inner) = set.inner() else {
					return Err(anyhow::anyhow!("Missing set item kind"));
				};
				let size = set.size().map(u64::from_fb).transpose()?;
				Ok(Kind::Set(Box::new(Kind::from_fb(inner)?), size))
			}
			KindType::Array => {
				let Some(array) = input.kind_as_array() else {
					return Err(anyhow::anyhow!("Missing array kind"));
				};
				let Some(inner) = array.inner() else {
					return Err(anyhow::anyhow!("Missing array item kind"));
				};
				let size = array.size().map(u64::from_fb).transpose()?;
				Ok(Kind::Array(Box::new(Kind::from_fb(inner)?), size))
			}
			KindType::Either => {
				let Some(either) = input.kind_as_either() else {
					return Err(anyhow::anyhow!("Missing either kind"));
				};
				let kinds = if let Some(kinds) = either.kinds() {
					kinds.iter().map(Kind::from_fb).collect::<anyhow::Result<Vec<_>>>()?
				} else {
					Vec::new()
				};
				Ok(Kind::Either(kinds))
			}
			KindType::Function => {
				let Some(function) = input.kind_as_function() else {
					return Err(anyhow::anyhow!("Missing function kind"));
				};
				let args = if let Some(args) = function.args() {
					Some(args.iter().map(Kind::from_fb).collect::<anyhow::Result<Vec<_>>>()?)
				} else {
					None
				};
				let return_type = if let Some(return_type) = function.return_type() {
					Some(Box::new(Kind::from_fb(return_type)?))
				} else {
					None
				};
				Ok(Kind::Function(args, return_type))
			}
			KindType::File => {
				let Some(file) = input.kind_as_file() else {
					return Err(anyhow::anyhow!("Missing file kind"));
				};
				let buckets = if let Some(buckets) = file.buckets() {
					buckets.iter().map(|x| x.to_string()).collect::<Vec<_>>()
				} else {
					Vec::new()
				};
				Ok(Kind::File(buckets))
			}
			KindType::Literal => {
				let Some(literal) = input.kind_as_literal() else {
					return Err(anyhow::anyhow!("Missing literal kind"));
				};
				Ok(Kind::Literal(KindLiteral::from_fb(literal)?))
			}
			KindType::Range => Ok(Kind::Range),
			KindType::Option => {
				let Some(option) = input.kind_as_option() else {
					return Err(anyhow::anyhow!("Missing option kind"));
				};
				let Some(inner) = option.inner() else {
					return Err(anyhow::anyhow!("Missing option item kind"));
				};
				Ok(Kind::Option(Box::new(Kind::from_fb(inner)?)))
			}
			KindType::Regex => Ok(Kind::Regex),
			_ => Err(anyhow::anyhow!("Unknown kind type")),
		}
	}
}

impl FromFlatbuffers for KindLiteral {
	type Input<'a> = proto_fb::LiteralKind<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		use proto_fb::LiteralType;

		let literal_type = input.literal_type();

		match literal_type {
			LiteralType::Bool => {
				let Some(bool_val) = input.literal_as_bool() else {
					return Err(anyhow::anyhow!("Missing bool value"));
				};
				Ok(KindLiteral::Bool(bool_val.value()))
			}
			LiteralType::String => {
				let Some(string_val) = input.literal_as_string() else {
					return Err(anyhow::anyhow!("Missing string value"));
				};
				let Some(value) = string_val.value() else {
					return Err(anyhow::anyhow!("Missing string content"));
				};
				Ok(KindLiteral::String(value.to_string()))
			}
			LiteralType::Int64 => {
				let Some(int_val) = input.literal_as_int_64() else {
					return Err(anyhow::anyhow!("Missing int64 value"));
				};
				Ok(KindLiteral::Integer(int_val.value()))
			}
			LiteralType::Float64 => {
				let Some(float_val) = input.literal_as_float_64() else {
					return Err(anyhow::anyhow!("Missing float64 value"));
				};
				Ok(KindLiteral::Float(float_val.value()))
			}
			LiteralType::Decimal => {
				let Some(decimal) = input.literal_as_decimal() else {
					return Err(anyhow::anyhow!("Missing decimal value"));
				};
				Ok(KindLiteral::Decimal(Decimal::from_fb(decimal)?))
			}
			LiteralType::Duration => {
				let Some(duration_val) = input.literal_as_duration() else {
					return Err(anyhow::anyhow!("Missing duration value"));
				};
				Ok(KindLiteral::Duration(Duration::from_fb(duration_val)?))
			}
			LiteralType::Array => {
				let Some(array_val) = input.literal_as_array() else {
					return Err(anyhow::anyhow!("Missing array value"));
				};
				let items = if let Some(items) = array_val.kinds() {
					items.iter().map(Kind::from_fb).collect::<anyhow::Result<Vec<_>>>()?
				} else {
					Vec::new()
				};
				Ok(KindLiteral::Array(items))
			}
			LiteralType::Object => {
				let Some(object_val) = input.literal_as_object() else {
					return Err(anyhow::anyhow!("Missing object value"));
				};
				let map = BTreeMap::<String, Kind>::from_fb(object_val)?;
				Ok(KindLiteral::Object(map))
			}
			_ => Err(anyhow::anyhow!("Unknown literal type")),
		}
	}
}

impl ToFlatbuffers for GeometryKind {
	type Output<'bldr> = proto_fb::GeometryKindType;

	fn to_fb<'bldr>(
		&self,
		_builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> anyhow::Result<Self::Output<'bldr>> {
		Ok(match self {
			GeometryKind::Point => proto_fb::GeometryKindType::Point,
			GeometryKind::Line => proto_fb::GeometryKindType::Line,
			GeometryKind::Polygon => proto_fb::GeometryKindType::Polygon,
			GeometryKind::MultiPoint => proto_fb::GeometryKindType::MultiPoint,
			GeometryKind::MultiLine => proto_fb::GeometryKindType::MultiLineString,
			GeometryKind::MultiPolygon => proto_fb::GeometryKindType::MultiPolygon,
			GeometryKind::Collection => proto_fb::GeometryKindType::Collection,
		})
	}
}

impl FromFlatbuffers for GeometryKind {
	type Input<'a> = proto_fb::GeometryKindType;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		match input {
			proto_fb::GeometryKindType::Point => Ok(GeometryKind::Point),
			proto_fb::GeometryKindType::Line => Ok(GeometryKind::Line),
			proto_fb::GeometryKindType::Polygon => Ok(GeometryKind::Polygon),
			proto_fb::GeometryKindType::MultiPoint => Ok(GeometryKind::MultiPoint),
			proto_fb::GeometryKindType::MultiLineString => Ok(GeometryKind::MultiLine),
			proto_fb::GeometryKindType::MultiPolygon => Ok(GeometryKind::MultiPolygon),
			proto_fb::GeometryKindType::Collection => Ok(GeometryKind::Collection),
			_ => Err(anyhow::anyhow!("Unknown geometry kind type: {:?}", input)),
		}
	}
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;

	use rstest::rstest;
	use rust_decimal::Decimal;
	use surrealdb_protocol::fb::v1 as proto_fb;

	use super::*;
	use crate::{Duration, Kind, KindLiteral};

	#[rstest]
	#[case::any(Kind::Any)]
	#[case::null(Kind::Null)]
	#[case::bool(Kind::Bool)]
	#[case::bytes(Kind::Bytes)]
	#[case::datetime(Kind::Datetime)]
	#[case::decimal(Kind::Decimal)]
	#[case::duration(Kind::Duration)]
	#[case::float(Kind::Float)]
	#[case::int(Kind::Int)]
	#[case::number(Kind::Number)]
	#[case::object(Kind::Object)]
	#[case::string(Kind::String)]
	#[case::uuid(Kind::Uuid)]
	#[case::regex(Kind::Regex)]
	#[case::range(Kind::Range)]
	#[case::record(Kind::Record(vec!["test_table".to_string()]))]
	#[case::geometry(Kind::Geometry(vec![GeometryKind::Point, GeometryKind::Polygon]))]
	#[case::option(Kind::Option(Box::new(Kind::String)))]
	#[case::either(Kind::Either(vec![Kind::String, Kind::Number]))]
	#[case::set(Kind::Set(Box::new(Kind::String), Some(10)))]
	#[case::array(Kind::Array(Box::new(Kind::String), Some(5)))]
	#[case::function(Kind::Function(Some(vec![Kind::String, Kind::Number]), Some(Box::new(Kind::Bool))))]
	#[case::file(Kind::File(vec!["bucket1".to_string(), "bucket2".to_string()]))]
	// KindLiteral variants
	#[case::literal_bool(Kind::Literal(KindLiteral::Bool(true)))]
	#[case::literal_bool_false(Kind::Literal(KindLiteral::Bool(false)))]
	#[case::literal_string(Kind::Literal(KindLiteral::String("test_string".to_string())))]
	#[case::literal_integer(Kind::Literal(KindLiteral::Integer(42)))]
	#[case::literal_integer_min(Kind::Literal(KindLiteral::Integer(i64::MIN)))]
	#[case::literal_integer_max(Kind::Literal(KindLiteral::Integer(i64::MAX)))]
	#[case::literal_float(Kind::Literal(KindLiteral::Float(std::f64::consts::PI)))]
	#[case::literal_float_nan(Kind::Literal(KindLiteral::Float(f64::NAN)))]
	#[case::literal_float_infinity(Kind::Literal(KindLiteral::Float(f64::INFINITY)))]
	#[case::literal_float_neg_infinity(Kind::Literal(KindLiteral::Float(f64::NEG_INFINITY)))]
	#[case::literal_decimal(Kind::Literal(KindLiteral::Decimal(Decimal::new(123, 2))))]
	#[case::literal_duration(Kind::Literal(KindLiteral::Duration(Duration::default())))]
	#[case::literal_array(Kind::Literal(KindLiteral::Array(vec![Kind::String, Kind::Number])))]
	#[case::literal_array_empty(Kind::Literal(KindLiteral::Array(vec![])))]
	#[case::literal_object(Kind::Literal(KindLiteral::Object(BTreeMap::from([
		("key1".to_string(), Kind::String),
		("key2".to_string(), Kind::Number)
	]))))]
	#[case::literal_object_empty(Kind::Literal(KindLiteral::Object(BTreeMap::new())))]
	fn test_flatbuffers_roundtrip_kind(#[case] input: Kind) {
		let mut builder = flatbuffers::FlatBufferBuilder::new();
		let input_fb = input.to_fb(&mut builder).expect("Failed to convert to FlatBuffer");
		builder.finish_minimal(input_fb);
		let buf = builder.finished_data();
		let kind_fb = flatbuffers::root::<proto_fb::Kind>(buf).expect("Failed to read FlatBuffer");
		let kind = Kind::from_fb(kind_fb).expect("Failed to convert from FlatBuffer");
		assert_eq!(input, kind, "Roundtrip conversion failed for input: {:?}", input);
	}
}
