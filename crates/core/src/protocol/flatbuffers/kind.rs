use crate::expr::{Duration, Ident, Kind, Literal, Number, Strand, Table};
use crate::protocol::{FromFlatbuffers, ToFlatbuffers};
use rust_decimal::Decimal;
use std::collections::BTreeMap;
use surrealdb_protocol::fb::v1 as proto_fb;

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
			Self::Point => proto_fb::KindArgs {
				kind_type: proto_fb::KindType::Point,
				kind: Some(
					proto_fb::PointKind::create(builder, &proto_fb::PointKindArgs {})
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
				let table_offsets: Vec<_> =
					tables.iter().map(|t| t.to_fb(builder)).collect::<anyhow::Result<Vec<_>>>()?;
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
					types.iter().map(|t| builder.create_string(t.as_str())).collect();
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
				let kind = kind.to_fb(builder)?.as_union_value();
				proto_fb::KindArgs {
					kind_type: proto_fb::KindType::Option,
					kind: Some(kind),
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
					buckets.iter().map(|b| builder.create_string(&b.0)).collect();
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

			Self::References(_, _) => {
				todo!("The references type will be removed, no need to implement it");
			}
		};

		Ok(proto_fb::Kind::create(builder, &args))
	}
}

impl ToFlatbuffers for Literal {
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
			Self::Number(number) => match number {
				Number::Int(i) => proto_fb::LiteralKindArgs {
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
				Number::Float(f) => proto_fb::LiteralKindArgs {
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
				Number::Decimal(d) => proto_fb::LiteralKindArgs {
					literal_type: proto_fb::LiteralType::Decimal,
					literal: Some(d.to_fb(builder)?.as_union_value()),
				},
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
				proto_fb::LiteralKindArgs {
					literal_type: proto_fb::LiteralType::Array,
					literal: Some(builder.create_vector(&array_items).as_union_value()),
				}
			}
			Self::Object(object) => proto_fb::LiteralKindArgs {
				literal_type: proto_fb::LiteralType::Object,
				literal: Some(object.to_fb(builder)?.as_union_value()),
			},
			Self::DiscriminatedObject(discriminant_key, variants) => {
				let discriminant_key = builder.create_string(discriminant_key);
				let variant_offsets: Vec<_> = variants
					.iter()
					.map(|map| map.to_fb(builder))
					.collect::<anyhow::Result<Vec<_>>>()?;

				let variants_vector = builder.create_vector(&variant_offsets);
				let literal_discriminated_object = proto_fb::LiteralDiscriminatedObject::create(
					builder,
					&proto_fb::LiteralDiscriminatedObjectArgs {
						discriminant_key: Some(discriminant_key),
						variants: Some(variants_vector),
					},
				);

				proto_fb::LiteralKindArgs {
					literal_type: proto_fb::LiteralType::DiscriminatedObject,
					literal: Some(literal_discriminated_object.as_union_value()),
				}
			}
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
							Ok(Table::from(name))
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
					types.iter().map(|t| t.to_string()).collect()
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
					buckets.iter().map(|b| Ident::from(b.to_string())).collect()
				} else {
					Vec::new()
				};
				Ok(Kind::File(buckets))
			}
			KindType::Literal => {
				let Some(literal) = input.kind_as_literal() else {
					return Err(anyhow::anyhow!("Missing literal kind"));
				};
				Ok(Kind::Literal(Literal::from_fb(literal)?))
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

impl FromFlatbuffers for Literal {
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
				Ok(Literal::Bool(bool_val.value()))
			}
			LiteralType::String => {
				let Some(string_val) = input.literal_as_string() else {
					return Err(anyhow::anyhow!("Missing string value"));
				};
				let Some(value) = string_val.value() else {
					return Err(anyhow::anyhow!("Missing string content"));
				};
				Ok(Literal::String(Strand::from(value)))
			}
			LiteralType::Int64 => {
				let Some(int_val) = input.literal_as_int_64() else {
					return Err(anyhow::anyhow!("Missing int64 value"));
				};
				Ok(Literal::Number(Number::Int(int_val.value())))
			}
			LiteralType::Float64 => {
				let Some(float_val) = input.literal_as_float_64() else {
					return Err(anyhow::anyhow!("Missing float64 value"));
				};
				Ok(Literal::Number(Number::Float(float_val.value())))
			}
			LiteralType::Decimal => {
				let Some(decimal) = input.literal_as_decimal() else {
					return Err(anyhow::anyhow!("Missing decimal value"));
				};
				Ok(Literal::Number(Number::Decimal(Decimal::from_fb(decimal)?)))
			}
			LiteralType::Duration => {
				let Some(duration_val) = input.literal_as_duration() else {
					return Err(anyhow::anyhow!("Missing duration value"));
				};
				Ok(Literal::Duration(Duration::from_fb(duration_val)?))
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
				Ok(Literal::Array(items))
			}
			LiteralType::Object => {
				let Some(object_val) = input.literal_as_object() else {
					return Err(anyhow::anyhow!("Missing object value"));
				};
				let map = BTreeMap::<String, Kind>::from_fb(object_val)?;
				Ok(Literal::Object(map))
			}
			LiteralType::DiscriminatedObject => {
				let Some(disc_obj) = input.literal_as_discriminated_object() else {
					return Err(anyhow::anyhow!("Missing discriminated object"));
				};
				let Some(discriminant_key) = disc_obj.discriminant_key() else {
					return Err(anyhow::anyhow!("Missing discriminant key"));
				};
				let variants = if let Some(variants) = disc_obj.variants() {
					variants
						.iter()
						.map(BTreeMap::<String, Kind>::from_fb)
						.collect::<anyhow::Result<Vec<_>>>()?
				} else {
					Vec::new()
				};
				Ok(Literal::DiscriminatedObject(discriminant_key.to_string(), variants))
			}
			_ => Err(anyhow::anyhow!("Unknown literal type")),
		}
	}
}
