use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::parser::idiom;
use crate::sql::table::Table;
use crate::sql::thing::Thing;
use crate::sql::value::Value;

pub fn bool((val,): (Value,)) -> Result<Value, Error> {
	val.convert_to_bool().map(Value::from)
}

pub fn datetime((val,): (Value,)) -> Result<Value, Error> {
	val.convert_to_datetime().map(Value::from)
}

pub fn decimal((val,): (Value,)) -> Result<Value, Error> {
	val.convert_to_decimal().map(Value::from)
}

pub fn duration((val,): (Value,)) -> Result<Value, Error> {
	val.convert_to_duration().map(Value::from)
}

pub async fn field(
	(ctx, opt, txn, doc): (
		&Context<'_>,
		Option<&Options>,
		Option<&Transaction>,
		Option<&CursorDoc<'_>>,
	),
	(val,): (String,),
) -> Result<Value, Error> {
	match (opt, txn) {
		(Some(opt), Some(txn)) => {
			// Parse the string as an Idiom
			let idi = idiom(&val)?;
			// Return the Idiom or fetch the field
			match opt.projections {
				true => Ok(idi.compute(ctx, opt, txn, doc).await?),
				false => Ok(idi.into()),
			}
		}
		_ => Ok(Value::None),
	}
}

pub async fn fields(
	(ctx, opt, txn, doc): (
		&Context<'_>,
		Option<&Options>,
		Option<&Transaction>,
		Option<&CursorDoc<'_>>,
	),
	(val,): (Vec<String>,),
) -> Result<Value, Error> {
	match (opt, txn) {
		(Some(opt), Some(txn)) => {
			let mut args: Vec<Value> = Vec::with_capacity(val.len());
			for v in val {
				// Parse the string as an Idiom
				let idi = idiom(&v)?;
				// Return the Idiom or fetch the field
				match opt.projections {
					true => args.push(idi.compute(ctx, opt, txn, doc).await?),
					false => args.push(idi.into()),
				}
			}
			Ok(args.into())
		}
		_ => Ok(Value::None),
	}
}

pub fn float((val,): (Value,)) -> Result<Value, Error> {
	val.convert_to_float().map(Value::from)
}

pub fn int((val,): (Value,)) -> Result<Value, Error> {
	val.convert_to_int().map(Value::from)
}

pub fn number((val,): (Value,)) -> Result<Value, Error> {
	val.convert_to_number().map(Value::from)
}

pub fn point((val,): (Value,)) -> Result<Value, Error> {
	val.convert_to_point().map(Value::from)
}

pub fn string((val,): (Value,)) -> Result<Value, Error> {
	val.convert_to_strand().map(Value::from)
}

pub fn table((val,): (Value,)) -> Result<Value, Error> {
	Ok(Value::Table(Table(match val {
		Value::Thing(t) => t.tb,
		v => v.as_string(),
	})))
}

pub fn thing((arg1, arg2): (Value, Option<Value>)) -> Result<Value, Error> {
	Ok(if let Some(arg2) = arg2 {
		Value::Thing(Thing {
			tb: arg1.as_string(),
			id: match arg2 {
				Value::Thing(v) => v.id,
				Value::Array(v) => v.into(),
				Value::Object(v) => v.into(),
				Value::Number(v) => v.into(),
				v => v.as_string().into(),
			},
		})
	} else {
		match arg1 {
			Value::Thing(v) => v.into(),
			_ => Value::None,
		}
	})
}
