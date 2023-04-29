use super::classes;
use crate::sql::number::Number;
use crate::sql::value::Value;
use bigdecimal::ToPrimitive;
use js::Array;
use js::Class;
use js::Ctx;
use js::Error;
use js::IntoJs;
use js::Null;
use js::Object;
use js::Undefined;

impl<'js> IntoJs<'js> for Value {
	fn into_js(self, ctx: Ctx<'js>) -> Result<js::Value<'js>, Error> {
		(&self).into_js(ctx)
	}
}

impl<'js> IntoJs<'js> for &Value {
	fn into_js(self, ctx: Ctx<'js>) -> Result<js::Value<'js>, Error> {
		match self {
			Value::Null => Null.into_js(ctx),
			Value::None => Undefined.into_js(ctx),
			Value::Bool(boolean) => Ok(js::Value::new_bool(ctx, *boolean)),
			Value::Strand(v) => js::String::from_str(ctx, v)?.into_js(ctx),
			Value::Number(Number::Int(v)) => Ok(js::Value::new_int(ctx, *v as i32)),
			Value::Number(Number::Float(v)) => Ok(js::Value::new_float(ctx, *v)),
			Value::Number(Number::Decimal(v)) => match v.is_integer() {
				true => Ok(js::Value::new_int(ctx, v.to_i32().unwrap_or_default())),
				false => Ok(js::Value::new_float(ctx, v.to_f64().unwrap_or_default())),
			},
			Value::Datetime(v) => {
				let date: js::Function = ctx.globals().get("Date")?;
				date.construct((v.0.timestamp_millis(),))
			}
			Value::Thing(v) => Ok(Class::<classes::record::record::Record>::instance(
				ctx,
				classes::record::record::Record {
					value: v.to_owned(),
				},
			)?
			.into_value()),
			Value::Duration(v) => Ok(Class::<classes::duration::duration::Duration>::instance(
				ctx,
				classes::duration::duration::Duration {
					value: Some(v.to_owned()),
				},
			)?
			.into_value()),
			Value::Uuid(v) => Ok(Class::<classes::uuid::uuid::Uuid>::instance(
				ctx,
				classes::uuid::uuid::Uuid {
					value: Some(v.to_owned()),
				},
			)?
			.into_value()),
			Value::Array(v) => {
				let x = Array::new(ctx)?;
				for (i, v) in v.iter().enumerate() {
					x.set(i, v)?;
				}
				x.into_js(ctx)
			}
			Value::Object(v) => {
				let x = Object::new(ctx)?;
				for (k, v) in v.iter() {
					x.set(k, v)?;
				}
				x.into_js(ctx)
			}
			_ => Undefined.into_js(ctx),
		}
	}
}
