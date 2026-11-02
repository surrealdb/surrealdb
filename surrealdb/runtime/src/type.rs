use anyhow::{Result, bail, ensure};
use geo_types::Point;
use rust_decimal::Decimal;
use surrealdb_expr::expr::Error as ExprError;
use surrealdb_expr::val::{
	Array, Bytes, Date, Datetime, Duration, File, Geometry, Interval, Number, Range, RecordId,
	RecordIdKey, RecordIdKeyRange, Set, TableName, Time, Timestamp, Uuid, Value,
};

use super::args::Optional;

/// Returns the type of the value as a string.
pub fn type_of((val,): (Value,)) -> Result<Value> {
	Ok(Value::String(val.kind_of().into()))
}

pub fn array((val,): (Value,)) -> Result<Value> {
	Ok(val.cast_to::<Array>()?.into())
}

pub fn set((val,): (Value,)) -> Result<Value> {
	Ok(val.cast_to::<Set>()?.into())
}

pub fn bool((val,): (Value,)) -> Result<Value> {
	Ok(val.cast_to::<bool>()?.into())
}

pub fn file((bucket, key): (String, String)) -> Result<Value> {
	Ok(Value::File(File::new(bucket, key)))
}

pub fn bytes((val,): (Value,)) -> Result<Value> {
	Ok(val.cast_to::<Bytes>()?.into())
}

pub fn date((val,): (Value,)) -> Result<Value> {
	Ok(val.cast_to::<Date>()?.into())
}

pub fn time((val,): (Value,)) -> Result<Value> {
	Ok(val.cast_to::<Time>()?.into())
}

pub fn timestamp((val,): (Value,)) -> Result<Value> {
	Ok(val.cast_to::<Timestamp>()?.into())
}

pub fn interval((val,): (Value,)) -> Result<Value> {
	Ok(val.cast_to::<Interval>()?.into())
}

pub fn datetime((val,): (Value,)) -> Result<Value> {
	Ok(val.cast_to::<Datetime>()?.into())
}

pub fn decimal((val,): (Value,)) -> Result<Value> {
	Ok(val.cast_to::<Decimal>()?.into())
}

pub fn duration((val,): (Value,)) -> Result<Value> {
	Ok(val.cast_to::<Duration>()?.into())
}

pub fn float((val,): (Value,)) -> Result<Value> {
	Ok(val.cast_to::<f64>()?.into())
}

pub fn f16((val,): (Value,)) -> Result<Value> {
	Ok(Value::Number(Number::F16(val.cast_to::<surrealdb_types::f16>()?)))
}

pub fn f32((val,): (Value,)) -> Result<Value> {
	Ok(Value::Number(Number::F32(val.cast_to::<f32>()?)))
}

pub fn f64(args: (Value,)) -> Result<Value> {
	float(args)
}

pub fn geometry((val,): (Value,)) -> Result<Value> {
	Ok(val.cast_to::<Geometry>()?.into())
}

pub fn int((val,): (Value,)) -> Result<Value> {
	Ok(val.cast_to::<i64>()?.into())
}

pub fn i8((val,): (Value,)) -> Result<Value> {
	Ok(Value::Number(Number::I8(val.cast_to::<i8>()?)))
}

pub fn u8((val,): (Value,)) -> Result<Value> {
	Ok(Value::Number(Number::U8(val.cast_to::<u8>()?)))
}

pub fn i16((val,): (Value,)) -> Result<Value> {
	Ok(Value::Number(Number::I16(val.cast_to::<i16>()?)))
}

pub fn u16((val,): (Value,)) -> Result<Value> {
	Ok(Value::Number(Number::U16(val.cast_to::<u16>()?)))
}

pub fn i32((val,): (Value,)) -> Result<Value> {
	Ok(Value::Number(Number::I32(val.cast_to::<i32>()?)))
}

pub fn u32((val,): (Value,)) -> Result<Value> {
	Ok(Value::Number(Number::U32(val.cast_to::<u32>()?)))
}

pub fn i64(args: (Value,)) -> Result<Value> {
	int(args)
}

pub fn u64((val,): (Value,)) -> Result<Value> {
	Ok(Value::Number(Number::U64(val.cast_to::<u64>()?)))
}

pub fn number((val,): (Value,)) -> Result<Value> {
	Ok(val.cast_to::<Number>()?.into())
}

pub fn point((val,): (Value,)) -> Result<Value> {
	Ok(val.cast_to::<Point<f64>>()?.into())
}

pub fn range((val,): (Value,)) -> Result<Value> {
	Ok(val.cast_to::<Box<Range>>()?.into())
}

pub fn string((val,): (Value,)) -> Result<Value> {
	Ok(val.cast_to::<String>()?.into())
}

pub fn string_lossy((val,): (Value,)) -> Result<Value> {
	if let Value::Bytes(x) = val {
		//TODO: Replace with from_utf8_lossy_owned once stablized.
		Ok(String::from_utf8_lossy(&x).into_owned().into())
	} else {
		val.cast_to::<String>()
			.map(Value::from)
			.map_err(ExprError::from)
			.map_err(anyhow::Error::new)
	}
}

pub fn table((val,): (Value,)) -> Result<Value> {
	let table_name = match val {
		Value::Table(t) => t,
		Value::RecordId(t) => t.table,
		Value::String(t) => TableName::new(t),
		v @ (Value::None
		| Value::Null
		| Value::Bool(_)
		| Value::Number(_)
		| Value::Duration(_)
		| Value::Datetime(_)
		| Value::Date(_)
		| Value::Time(_)
		| Value::Timestamp(_)
		| Value::Interval(_)
		| Value::Uuid(_)
		| Value::Array(_)
		| Value::Set(_)
		| Value::Object(_)
		| Value::Geometry(_)
		| Value::Bytes(_)
		| Value::File(_)
		| Value::Regex(_)
		| Value::Range(_)
		| Value::Closure(_)) => bail!(ExprError::TbInvalid {
			value: v.into_raw_string(),
		}),
	};
	Ok(Value::Table(table_name))
}

pub fn record((arg1, Optional(arg2)): (Value, Optional<Value>)) -> Result<Value> {
	match (arg1, arg2) {
		// Empty table name
		(Value::String(arg1), _) if arg1.is_empty() => bail!(ExprError::TbInvalid {
			value: arg1.into_string(),
		}),

		// SECURITY: the legacy two-argument form `type::record(<value>, "table")`
		// used to validate that the value's record id belonged to the supplied
		// table. The current `type::record(table, key)` semantic flips the
		// roles, so a SIGNIN clause like `SELECT * FROM type::record($id, "user")`
		// no longer rejects an `$id` whose table is not `user` — it just builds
		// `<$id>:user`. When `$id` is already a record id, preserve the old
		// validation: error if the record's table doesn't match the constraint
		// string. Other shapes fall through to the new semantic.
		(Value::RecordId(rid), Some(Value::String(constraint))) => {
			if rid.table.as_str() != constraint.as_str() {
				bail!(ExprError::IdInvalid {
					value: format!(
						"{}: expected a record in table '{}'",
						Value::RecordId(rid).into_raw_string(),
						constraint
					),
				});
			}
			Ok(Value::RecordId(rid))
		}

		// Handle second argument
		(arg1, Some(arg2)) => Ok(Value::RecordId(RecordId {
			key: match arg2 {
				Value::RecordId(v) => v.key,
				Value::Array(v) => v.into(),
				Value::Object(v) => v.into(),
				Value::Number(v) => match v {
					Number::Int(x) => x.into(),
					Number::I8(x) => i64::from(x).into(),
					Number::U8(x) => i64::from(x).into(),
					Number::I16(x) => i64::from(x).into(),
					Number::U16(x) => i64::from(x).into(),
					Number::I32(x) => i64::from(x).into(),
					Number::U32(x) => i64::from(x).into(),
					Number::U64(x) => match i64::try_from(x) {
						Ok(i) => RecordIdKey::Number(i),
						Err(_) => bail!(ExprError::IdInvalid {
							value: v.to_string(),
						}),
					},
					// Safety: float -> string conversion cannot contain a null byte.
					Number::Float(x) => x.to_string().into(),
					Number::F16(x) => x.to_string().into(),
					Number::F32(x) => x.to_string().into(),
					// Safety: decimal -> string conversion cannot contain a null byte.
					Number::Decimal(x) => x.to_string().into(),
					// Safety: bigdecimal -> string conversion cannot contain a null byte.
					Number::BigDecimal(x) => x.to_string().into(),
				},
				Value::Range(v) => {
					let res =
						RecordIdKeyRange::from_value_range((*v).clone()).ok_or_else(|| {
							ExprError::IdInvalid {
								value: Value::Range(v).into_raw_string(),
							}
						})?;
					RecordIdKey::Range(Box::new(res))
				}
				Value::Uuid(u) => u.into(),
				ref v @ (Value::None
				| Value::Null
				| Value::Bool(_)
				| Value::String(_)
				| Value::Duration(_)
				| Value::Datetime(_)
				| Value::Date(_)
				| Value::Time(_)
				| Value::Timestamp(_)
				| Value::Interval(_)
				| Value::Set(_)
				| Value::Geometry(_)
				| Value::Bytes(_)
				| Value::Table(_)
				| Value::File(_)
				| Value::Regex(_)
				| Value::Closure(_)) => {
					let s = v.to_raw_string();
					ensure!(
						!s.is_empty(),
						ExprError::IdInvalid {
							value: arg2.into_raw_string(),
						}
					);
					RecordIdKey::String(s.into())
				}
			},
			table: if let Value::String(s) = arg1 {
				TableName::from(s)
			} else {
				TableName::new(arg1.into_raw_string())
			},
		})),

		(arg1, None) => arg1
			.cast_to::<RecordId>()
			.map(Value::from)
			.map_err(ExprError::from)
			.map_err(anyhow::Error::new),
	}
}

pub fn uuid((val,): (Value,)) -> Result<Value> {
	val.cast_to::<Uuid>().map(Value::from).map_err(ExprError::from).map_err(anyhow::Error::new)
}

pub mod is {
	use anyhow::Result;
	use surrealdb_expr::val::{Geometry, Number, TableName, Value};

	use crate::args::Optional;

	pub fn array((arg,): (Value,)) -> Result<Value> {
		Ok((arg).is_array().into())
	}

	pub fn set((arg,): (Value,)) -> Result<Value> {
		Ok(arg.is_set().into())
	}

	pub fn bool((arg,): (Value,)) -> Result<Value> {
		Ok(arg.is_bool().into())
	}

	pub fn bytes((arg,): (Value,)) -> Result<Value> {
		Ok(arg.is_bytes().into())
	}

	pub fn collection((arg,): (Value,)) -> Result<Value> {
		Ok(matches!(arg, Value::Geometry(Geometry::Collection(_))).into())
	}

	pub fn date((arg,): (Value,)) -> Result<Value> {
		Ok(arg.is_date().into())
	}

	pub fn time((arg,): (Value,)) -> Result<Value> {
		Ok(arg.is_time().into())
	}

	pub fn timestamp((arg,): (Value,)) -> Result<Value> {
		Ok(arg.is_timestamp().into())
	}

	pub fn interval((arg,): (Value,)) -> Result<Value> {
		Ok(arg.is_interval().into())
	}

	pub fn datetime((arg,): (Value,)) -> Result<Value> {
		Ok(arg.is_datetime().into())
	}

	pub fn decimal((arg,): (Value,)) -> Result<Value> {
		Ok(arg.is_decimal().into())
	}

	pub fn duration((arg,): (Value,)) -> Result<Value> {
		Ok(arg.is_duration().into())
	}

	pub fn float((arg,): (Value,)) -> Result<Value> {
		Ok(arg.is_float().into())
	}

	pub fn f16((arg,): (Value,)) -> Result<Value> {
		Ok(matches!(arg, Value::Number(Number::F16(_))).into())
	}

	pub fn f32((arg,): (Value,)) -> Result<Value> {
		Ok(matches!(arg, Value::Number(Number::F32(_))).into())
	}

	pub fn f64((arg,): (Value,)) -> Result<Value> {
		float((arg,))
	}

	pub fn geometry((arg,): (Value,)) -> Result<Value> {
		Ok(arg.is_geometry().into())
	}

	pub fn int((arg,): (Value,)) -> Result<Value> {
		Ok(arg.is_int().into())
	}

	pub fn i8((arg,): (Value,)) -> Result<Value> {
		Ok(matches!(arg, Value::Number(Number::I8(_))).into())
	}

	pub fn u8((arg,): (Value,)) -> Result<Value> {
		Ok(matches!(arg, Value::Number(Number::U8(_))).into())
	}

	pub fn i16((arg,): (Value,)) -> Result<Value> {
		Ok(matches!(arg, Value::Number(Number::I16(_))).into())
	}

	pub fn u16((arg,): (Value,)) -> Result<Value> {
		Ok(matches!(arg, Value::Number(Number::U16(_))).into())
	}

	pub fn i32((arg,): (Value,)) -> Result<Value> {
		Ok(matches!(arg, Value::Number(Number::I32(_))).into())
	}

	pub fn u32((arg,): (Value,)) -> Result<Value> {
		Ok(matches!(arg, Value::Number(Number::U32(_))).into())
	}

	pub fn i64((arg,): (Value,)) -> Result<Value> {
		int((arg,))
	}

	pub fn u64((arg,): (Value,)) -> Result<Value> {
		Ok(matches!(arg, Value::Number(Number::U64(_))).into())
	}

	pub fn line((arg,): (Value,)) -> Result<Value> {
		Ok(matches!(arg, Value::Geometry(Geometry::Line(_))).into())
	}

	pub fn none((arg,): (Value,)) -> Result<Value> {
		Ok(arg.is_none().into())
	}

	pub fn null((arg,): (Value,)) -> Result<Value> {
		Ok(arg.is_null().into())
	}

	pub fn multiline((arg,): (Value,)) -> Result<Value> {
		Ok(matches!(arg, Value::Geometry(Geometry::MultiLine(_))).into())
	}

	pub fn multipoint((arg,): (Value,)) -> Result<Value> {
		Ok(matches!(arg, Value::Geometry(Geometry::MultiPoint(_))).into())
	}

	pub fn multipolygon((arg,): (Value,)) -> Result<Value> {
		Ok(matches!(arg, Value::Geometry(Geometry::MultiPolygon(_))).into())
	}

	pub fn number((arg,): (Value,)) -> Result<Value> {
		Ok(arg.is_number().into())
	}

	pub fn object((arg,): (Value,)) -> Result<Value> {
		Ok(arg.is_object().into())
	}

	pub fn point((arg,): (Value,)) -> Result<Value> {
		Ok(matches!(arg, Value::Geometry(Geometry::Point(_))).into())
	}

	pub fn polygon((arg,): (Value,)) -> Result<Value> {
		Ok(matches!(arg, Value::Geometry(Geometry::Polygon(_))).into())
	}

	pub fn range((arg,): (Value,)) -> Result<Value> {
		Ok(arg.is_range().into())
	}

	pub fn record((arg, Optional(table)): (Value, Optional<String>)) -> Result<Value> {
		let res = match table {
			Some(tb) => {
				let tb = TableName::new(tb);
				arg.is_record_type(&[tb]).into()
			}
			None => arg.is_record().into(),
		};
		Ok(res)
	}

	pub fn string((arg,): (Value,)) -> Result<Value> {
		Ok(arg.is_strand().into())
	}

	pub fn uuid((arg,): (Value,)) -> Result<Value> {
		Ok(arg.is_uuid().into())
	}
}

#[cfg(test)]
mod tests {
	use surrealdb_expr::expr::Error as ExprError;
	use surrealdb_expr::val::{Number, Value};
	use surrealdb_strand::Strand;

	use crate::args::Optional;

	#[test]
	fn is_array() {
		let value = super::is::array((vec![
			Value::String(Strand::new_static("hello")),
			Value::String(Strand::new_static("world")),
		]
		.into(),))
		.unwrap();
		assert_eq!(value, Value::Bool(true));

		let value = super::is::array(("test".into(),)).unwrap();
		assert_eq!(value, Value::Bool(false));
	}

	#[test]
	fn no_empty_record() {
		let value = super::record(("".into(), Optional(None)));
		let _expected = ExprError::TbInvalid {
			value: "".into(),
		};
		if !matches!(value, Err(_expected)) {
			panic!("An empty record tb part should result in an error");
		}

		let value = super::record(("table".into(), Optional(Some("".into()))));
		let _expected = ExprError::IdInvalid {
			value: "".into(),
		};
		if !matches!(value, Err(_expected)) {
			panic!("An empty record id part should result in an error");
		}
	}

	#[test]
	fn sized_number_types() {
		let v = super::i8((Value::from(42),)).unwrap();
		assert_eq!(v, Value::Number(Number::I8(42)));
		assert_eq!(super::is::i8((v.clone(),)).unwrap(), Value::Bool(true));
		assert_eq!(super::is::int((v.clone(),)).unwrap(), Value::Bool(false));

		let v_u64 = super::u64((Value::from(100),)).unwrap();
		assert_eq!(v_u64, Value::Number(Number::U64(100)));
		assert_eq!(super::is::u64((v_u64,)).unwrap(), Value::Bool(true));

		let r = super::record(("person".into(), Optional(Some(v)))).unwrap();
		assert_eq!(r, Value::RecordId(super::RecordId::new("person".into(), 42i64)));
	}
}
