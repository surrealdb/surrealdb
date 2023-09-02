use super::classes;
use crate::sql::number::Number;
use crate::sql::value::Value;
use js::Array;
use js::Class;
use js::Ctx;
use js::Error;
use js::IntoJs;
use js::Null;
use js::Object;
use js::Undefined;

impl<'js> IntoJs<'js> for Value {
	fn into_js(self, ctx: &Ctx<'js>) -> Result<js::Value<'js>, Error> {
		(&self).into_js(ctx)
	}
}

impl<'js> IntoJs<'js> for &Value {
	fn into_js(self, ctx: &Ctx<'js>) -> Result<js::Value<'js>, Error> {
		match self {
			Value::Null => Null.into_js(ctx),
			Value::None => Undefined.into_js(ctx),
			Value::Bool(boolean) => Ok(js::Value::new_bool(ctx.clone(), *boolean)),
			Value::Strand(v) => js::String::from_str(ctx.clone(), v)?.into_js(ctx),
			Value::Number(Number::Int(v)) => Ok(js::Value::new_int(ctx.clone(), *v as i32)),
			Value::Number(Number::Float(v)) => Ok(js::Value::new_float(ctx.clone(), *v)),
			&Value::Number(Number::Decimal(v)) => match v.is_integer() {
				true => Ok(js::Value::new_int(ctx.clone(), v.try_into().unwrap_or_default())),
				false => Ok(js::Value::new_float(ctx.clone(), v.try_into().unwrap_or_default())),
			},
			Value::Datetime(v) => {
				let date: js::function::Constructor = ctx.globals().get("Date")?;
				date.construct((v.0.timestamp_millis(),))
			}
			Value::Thing(v) => Ok(Class::<classes::record::Record>::instance(
				ctx.clone(),
				classes::record::Record {
					value: v.to_owned(),
				},
			)?
			.into_value()),
			Value::Duration(v) => Ok(Class::<classes::duration::Duration>::instance(
				ctx.clone(),
				classes::duration::Duration {
					value: Some(v.to_owned()),
				},
			)?
			.into_value()),
			Value::Uuid(v) => Ok(Class::<classes::uuid::Uuid>::instance(
				ctx.clone(),
				classes::uuid::Uuid {
					value: Some(v.to_owned()),
				},
			)?
			.into_value()),
			Value::Array(v) => {
				let x = Array::new(ctx.clone())?;
				for (i, v) in v.iter().enumerate() {
					x.set(i, v)?;
				}
				x.into_js(ctx)
			}
			Value::Object(v) => {
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
