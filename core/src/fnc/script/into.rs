use super::classes;
use crate::sql::number::Number;
use crate::sql::value::Value;
use js::Array;
use js::BigInt;
use js::Class;
use js::Ctx;
use js::Error;
use js::Exception;
use js::IntoJs;
use js::Null;
use js::Object;
use js::Undefined;
use rust_decimal::prelude::ToPrimitive;

const F64_INT_MAX: i64 = ((1u64 << f64::MANTISSA_DIGITS) - 1) as i64;
const F64_INT_MIN: i64 = -F64_INT_MAX - 1;

impl<'js> IntoJs<'js> for Value {
	fn into_js(self, ctx: &Ctx<'js>) -> Result<js::Value<'js>, Error> {
		(&self).into_js(ctx)
	}
}

impl<'js> IntoJs<'js> for &Value {
	fn into_js(self, ctx: &Ctx<'js>) -> Result<js::Value<'js>, Error> {
		match *self {
			Value::Null => Null.into_js(ctx),
			Value::None => Undefined.into_js(ctx),
			Value::Bool(boolean) => Ok(js::Value::new_bool(ctx.clone(), boolean)),
			Value::Strand(ref v) => js::String::from_str(ctx.clone(), v)?.into_js(ctx),
			Value::Number(Number::Int(v)) => {
				if ((i32::MIN as i64)..=(i32::MAX as i64)).contains(&v) {
					Ok(js::Value::new_int(ctx.clone(), v as i32))
				} else if (F64_INT_MIN..=F64_INT_MAX).contains(&v) {
					Ok(js::Value::new_float(ctx.clone(), v as f64))
				} else {
					Ok(js::Value::from(BigInt::from_i64(ctx.clone(), v)?))
				}
			}
			Value::Number(Number::Float(v)) => Ok(js::Value::new_float(ctx.clone(), v)),
			Value::Number(Number::Decimal(v)) => {
				if v.is_integer() {
					if let Some(v) = v.to_i64() {
						if ((i32::MIN as i64)..=(i32::MAX as i64)).contains(&v) {
							Ok(js::Value::new_int(ctx.clone(), v as i32))
						} else if (F64_INT_MIN..=F64_INT_MAX).contains(&v) {
							Ok(js::Value::new_float(ctx.clone(), v as f64))
						} else {
							Ok(js::Value::from(BigInt::from_i64(ctx.clone(), v)?))
						}
					} else {
						Err(Exception::from_message(
							ctx.clone(),
							"Couldn't convert SurrealQL Decimal to JavaScript number",
						)?
						.throw())
					}
				} else if let Ok(v) = v.try_into() {
					Ok(js::Value::new_float(ctx.clone(), v))
				} else {
					// FIXME: Add support for larger numbers if rquickjs ever adds support.
					Err(Exception::from_message(
						ctx.clone(),
						"Couldn't convert SurrealQL Decimal to a JavaScript number",
					)?
					.throw())
				}
			}
			Value::Datetime(ref v) => {
				let date: js::function::Constructor = ctx.globals().get("Date")?;
				date.construct((v.0.timestamp_millis(),))
			}
			Value::Thing(ref v) => Ok(Class::<classes::record::Record>::instance(
				ctx.clone(),
				classes::record::Record {
					value: v.to_owned(),
				},
			)?
			.into_value()),
			Value::Duration(ref v) => Ok(Class::<classes::duration::Duration>::instance(
				ctx.clone(),
				classes::duration::Duration {
					value: Some(v.to_owned()),
				},
			)?
			.into_value()),
			Value::Uuid(ref v) => Ok(Class::<classes::uuid::Uuid>::instance(
				ctx.clone(),
				classes::uuid::Uuid {
					value: Some(v.to_owned()),
				},
			)?
			.into_value()),
			Value::Array(ref v) => {
				let x = Array::new(ctx.clone())?;
				for (i, v) in v.iter().enumerate() {
					x.set(i, v)?;
				}
				x.into_js(ctx)
			}
			Value::Object(ref v) => {
				let x = Object::new(ctx.clone())?;
				for (k, v) in v.iter() {
					x.set(k, v)?;
				}
				x.into_js(ctx)
			}
			_ => Undefined.into_js(ctx),
		}
	}
}
