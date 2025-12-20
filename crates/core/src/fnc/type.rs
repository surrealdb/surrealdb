use anyhow::{Result, bail, ensure};
use geo::Point;
use reblessive::tree::Stk;
use rust_decimal::Decimal;

use super::args::Optional;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::{FlowResultExt as _, Idiom};
use crate::syn;
use crate::val::{
	Array, Bytes, Datetime, Duration, File, Geometry, Number, Range, RecordId, RecordIdKey,
	RecordIdKeyRange, Set, TableName, Uuid, Value,
};

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

pub fn datetime((val,): (Value,)) -> Result<Value> {
	Ok(val.cast_to::<Datetime>()?.into())
}

pub fn decimal((val,): (Value,)) -> Result<Value> {
	Ok(val.cast_to::<Decimal>()?.into())
}

pub fn duration((val,): (Value,)) -> Result<Value> {
	Ok(val.cast_to::<Duration>()?.into())
}

pub async fn field(
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, Option<&Options>, Option<&CursorDoc>),
	(val,): (String,),
) -> Result<Value> {
	match opt {
		Some(opt) => {
			// Parse the string as an Idiom
			let idi: Idiom = syn::idiom(&val)?.into();
			// Return the Idiom or fetch the field
			idi.compute(stk, ctx, opt, doc).await.catch_return()
		}
		_ => Ok(Value::None),
	}
}

pub async fn fields(
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, Option<&Options>, Option<&CursorDoc>),
	(val,): (Vec<String>,),
) -> Result<Value> {
	match opt {
		Some(opt) => {
			let mut args: Vec<Value> = Vec::with_capacity(val.len());
			for v in val {
				// Parse the string as an Idiom
				let idi: Idiom = syn::idiom(&v)?.into();
				// Return the Idiom or fetch the field
				args.push(idi.compute(stk, ctx, opt, doc).await.catch_return()?);
			}
			Ok(args.into())
		}
		_ => Ok(Value::None),
	}
}

pub fn float((val,): (Value,)) -> Result<Value> {
	Ok(val.cast_to::<f64>()?.into())
}

pub fn geometry((val,): (Value,)) -> Result<Value> {
	Ok(val.cast_to::<Geometry>()?.into())
}

pub fn int((val,): (Value,)) -> Result<Value> {
	Ok(val.cast_to::<i64>()?.into())
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
	match val {
		//TODO: Replace with from_utf8_lossy_owned once stablized.
		Value::Bytes(x) => Ok(String::from_utf8_lossy(&x).into_owned().into()),
		x => {
			x.cast_to::<String>().map(Value::from).map_err(Error::from).map_err(anyhow::Error::new)
		}
	}
}

pub fn table((val,): (Value,)) -> Result<Value> {
	let table_name = match val {
		Value::Table(t) => t,
		Value::RecordId(t) => t.table,
		Value::String(t) => TableName::new(t.into()),
		v => bail!(Error::TbInvalid {
			value: v.into_raw_string(),
		}),
	};
	Ok(Value::Table(table_name))
}

pub fn record((arg1, Optional(arg2)): (Value, Optional<Value>)) -> Result<Value> {
	match (arg1, arg2) {
		// Empty table name
		(Value::String(arg1), _) if arg1.is_empty() => bail!(Error::TbInvalid {
			value: arg1.into(),
		}),

		// Handle second argument
		(arg1, Some(arg2)) => Ok(Value::RecordId(RecordId {
			key: match arg2 {
				Value::RecordId(v) => v.key,
				Value::Array(v) => v.into(),
				Value::Object(v) => v.into(),
				Value::Number(v) => match v {
					Number::Int(x) => x.into(),
					// Safety: float -> string conversion cannot contain a null byte.
					Number::Float(x) => x.to_string().into(),
					// Safety: decimal -> string conversion cannot contain a null byte.
					Number::Decimal(x) => x.to_string().into(),
				},
				Value::Range(v) => {
					let res =
						RecordIdKeyRange::from_value_range((*v).clone()).ok_or_else(|| {
							Error::IdInvalid {
								value: Value::Range(v).into_raw_string(),
							}
						})?;
					RecordIdKey::Range(Box::new(res))
				}
				Value::Uuid(u) => u.into(),
				ref v => {
					let s = v.to_raw_string();
					ensure!(
						!s.is_empty(),
						Error::IdInvalid {
							value: arg2.into_raw_string(),
						}
					);
					RecordIdKey::String(s)
				}
			},
			table: TableName::new(arg1.into_raw_string()),
		})),

		(arg1, None) => arg1
			.cast_to::<RecordId>()
			.map(Value::from)
			.map_err(Error::from)
			.map_err(anyhow::Error::new),
	}
}

pub fn uuid((val,): (Value,)) -> Result<Value> {
	val.cast_to::<Uuid>().map(Value::from).map_err(Error::from).map_err(anyhow::Error::new)
}

pub mod is {
	use anyhow::Result;

	use crate::fnc::args::Optional;
	use crate::val::{Geometry, TableName, Value};

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

	pub fn geometry((arg,): (Value,)) -> Result<Value> {
		Ok(arg.is_geometry().into())
	}

	pub fn int((arg,): (Value,)) -> Result<Value> {
		Ok(arg.is_int().into())
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
	use crate::err::Error;
	use crate::fnc::args::Optional;
	use crate::val::Value;

	#[test]
	fn is_array() {
		let value =
			super::is::array((
				vec![Value::String("hello".into()), Value::String("world".into())].into(),
			))
			.unwrap();
		assert_eq!(value, Value::Bool(true));

		let value = super::is::array(("test".into(),)).unwrap();
		assert_eq!(value, Value::Bool(false));
	}

	#[test]
	fn no_empty_record() {
		let value = super::record(("".into(), Optional(None)));
		let _expected = Error::TbInvalid {
			value: "".into(),
		};
		if !matches!(value, Err(_expected)) {
			panic!("An empty record tb part should result in an error");
		}

		let value = super::record(("table".into(), Optional(Some("".into()))));
		let _expected = Error::IdInvalid {
			value: "".into(),
		};
		if !matches!(value, Err(_expected)) {
			panic!("An empty record id part should result in an error");
		}
	}
}
