use std::ops::Deref;

use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::table::Table;
use crate::expr::thing::Thing;
use crate::expr::value::Value;
use crate::expr::{
	Array, Bytes, Datetime, Duration, File, FlowResultExt as _, Geometry, Kind, Number, Range,
	Strand, Uuid,
};
use crate::syn;
use anyhow::{Result, bail, ensure};
use geo::Point;
use reblessive::tree::Stk;
use rust_decimal::Decimal;

use super::args::Optional;

pub fn array((val,): (Value,)) -> Result<Value> {
	Ok(val.cast_to::<Array>()?.into())
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
	(stk, ctx, opt, doc): (&mut Stk, &Context, Option<&Options>, Option<&CursorDoc>),
	(val,): (String,),
) -> Result<Value> {
	match opt {
		Some(opt) => {
			// Parse the string as an Idiom
			let idi = syn::idiom(&val)?;
			// Return the Idiom or fetch the field
			idi.compute(stk, ctx, opt, doc).await.catch_return()
		}
		_ => Ok(Value::None),
	}
}

pub async fn fields(
	(stk, ctx, opt, doc): (&mut Stk, &Context, Option<&Options>, Option<&CursorDoc>),
	(val,): (Vec<String>,),
) -> Result<Value> {
	match opt {
		Some(opt) => {
			let mut args: Vec<Value> = Vec::with_capacity(val.len());
			for v in val {
				// Parse the string as an Idiom
				let idi = syn::idiom(&v)?;
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

pub fn record((rid, Optional(tb)): (Value, Optional<Value>)) -> Result<Value> {
	match tb {
		Some(Value::Strand(Strand(tb)) | Value::Table(Table(tb))) if tb.is_empty() => {
			Err(anyhow::Error::new(Error::TbInvalid {
				value: tb,
			}))
		}
		Some(Value::Strand(Strand(tb)) | Value::Table(Table(tb))) => {
			rid.cast_to_kind(&Kind::Record(vec![tb.into()])).map_err(From::from)
		}
		Some(_) => Err(anyhow::Error::new(Error::InvalidArguments {
			name: "type::record".into(),
			message: "The second argument must be a table name or a string.".into(),
		})),
		None => rid.cast_to_kind(&Kind::Record(vec![])).map_err(From::from),
	}
}

pub fn string((val,): (Value,)) -> Result<Value> {
	Ok(val.cast_to::<Strand>()?.into())
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
	Ok(Value::Table(Table(match val {
		Value::Thing(t) => t.tb,
		v => v.as_string(),
	})))
}

pub fn thing((arg1, Optional(arg2)): (Value, Optional<Value>)) -> Result<Value> {
	match (arg1, arg2) {
		// Empty table name
		(Value::Strand(arg1), _) if arg1.is_empty() => bail!(Error::TbInvalid {
			value: arg1.as_string(),
		}),

		// Handle second argument
		(arg1, Some(arg2)) => Ok(Value::Thing(Thing {
			id: match arg2 {
				Value::Thing(v) => v.id,
				Value::Array(v) => v.into(),
				Value::Object(v) => v.into(),
				Value::Number(v) => v.into(),
				Value::Range(v) => v.deref().to_owned().try_into()?,
				Value::Uuid(u) => u.into(),
				ref v => {
					let s = v.clone().as_string();
					ensure!(
						!s.is_empty(),
						Error::IdInvalid {
							value: arg2.as_string(),
						}
					);
					s.into()
				}
			},
			tb: arg1.as_string(),
		})),

		(arg1, None) => arg1
			.cast_to::<Thing>()
			.map(Value::from)
			.map_err(Error::from)
			.map_err(anyhow::Error::new),
	}
}

pub fn uuid((val,): (Value,)) -> Result<Value> {
	val.cast_to::<Uuid>().map(Value::from).map_err(Error::from).map_err(anyhow::Error::new)
}

pub mod is {
	use crate::expr::Geometry;
	use crate::expr::table::Table;
	use crate::expr::value::Value;
	use crate::fnc::args::Optional;
	use anyhow::Result;

	pub fn array((arg,): (Value,)) -> Result<Value> {
		Ok(arg.is_array().into())
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
		Ok(match table {
			Some(tb) => arg.is_record_type(&[Table(tb)]).into(),
			None => arg.is_thing().into(),
		})
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
	use crate::expr::value::Value;
	use crate::fnc::args::Optional;

	#[test]
	fn is_array() {
		let value = super::is::array((vec!["hello", "world"].into(),)).unwrap();
		assert_eq!(value, Value::Bool(true));

		let value = super::is::array(("test".into(),)).unwrap();
		assert_eq!(value, Value::Bool(false));
	}

	#[test]
	fn no_empty_thing() {
		let value = super::thing(("".into(), Optional(None)));
		let _expected = Error::TbInvalid {
			value: "".into(),
		};
		if !matches!(value, Err(_expected)) {
			panic!("An empty thing tb part should result in an error");
		}

		let value = super::thing(("table".into(), Optional(Some("".into()))));
		let _expected = Error::IdInvalid {
			value: "".into(),
		};
		if !matches!(value, Err(_expected)) {
			panic!("An empty thing id part should result in an error");
		}
	}
}
