use super::classes;
use crate::sql::array::Array;
use crate::sql::datetime::Datetime;
use crate::sql::duration::Duration;
use crate::sql::object::Object;
use crate::sql::thing::Thing;
use crate::sql::uuid::Uuid;
use crate::sql::value::Value;
use chrono::{TimeZone, Utc};
use js::Ctx;
use js::Error;
use js::FromAtom;
use js::FromJs;

impl<'js> FromJs<'js> for Value {
	fn from_js(ctx: Ctx<'js>, val: js::Value<'js>) -> Result<Self, Error> {
		match val {
			val if val.type_name() == "null" => Ok(Value::Null),
			val if val.type_name() == "undefined" => Ok(Value::None),
			val if val.is_bool() => Ok(val.as_bool().unwrap().into()),
			val if val.is_string() => match val.into_string().unwrap().to_string() {
				Ok(v) => Ok(Value::from(v)),
				Err(e) => Err(e),
			},
			val if val.is_int() => Ok(val.as_int().unwrap().into()),
			val if val.is_float() => Ok(val.as_float().unwrap().into()),
			val if val.is_array() => {
				let v = val.as_array().unwrap();
				let mut x = Array::with_capacity(v.len());
				for i in v.iter() {
					let v = i?;
					let v = Value::from_js(ctx, v)?;
					x.push(v);
				}
				Ok(x.into())
			}
			val if val.is_object() => {
				// Extract the value as an object
				let v = val.into_object().unwrap();
				// Check to see if this object is an error
				if v.is_error() {
					let e: String = v.get("message")?;
					return Err(Error::Exception {
						line: -1,
						message: e,
						file: String::new(),
						stack: String::new(),
					});
				}
				// Check to see if this object is a duration
				if (v).instance_of::<classes::duration::duration::Duration>() {
					let v = v.into_instance::<classes::duration::duration::Duration>().unwrap();
					let v: &classes::duration::duration::Duration = v.as_ref();
					let v = v.value.clone();
					return Ok(Duration::from(v).into());
				}
				// Check to see if this object is a record
				if (v).instance_of::<classes::record::record::Record>() {
					let v = v.into_instance::<classes::record::record::Record>().unwrap();
					let v: &classes::record::record::Record = v.as_ref();
					let v = (v.tb.clone(), v.id.clone());
					return Ok(Thing::from(v).into());
				}
				// Check to see if this object is a uuid
				if (v).instance_of::<classes::uuid::uuid::Uuid>() {
					let v = v.into_instance::<classes::uuid::uuid::Uuid>().unwrap();
					let v: &classes::uuid::uuid::Uuid = v.as_ref();
					let v = v.value.clone();
					return Ok(Uuid::from(v).into());
				}
				// Check to see if this object is a date
				let date: js::Object = ctx.globals().get("Date")?;
				if (v).is_instance_of(&date) {
					let f: js::Function = v.get("getTime")?;
					let m: i64 = f.call((js::This(v),))?;
					let d = Utc.timestamp_millis_opt(m).unwrap();
					return Ok(Datetime::from(d).into());
				}
				// Check to see if this object is an array
				if let Some(v) = v.as_array() {
					let mut x = Array::with_capacity(v.len());
					for i in v.iter() {
						let v = i?;
						let v = Value::from_js(ctx, v)?;
						x.push(v);
					}
					return Ok(x.into());
				}
				// Check to see if this object is a function
				if v.as_function().is_some() {
					return Ok(Value::None);
				}
				// This object is a normal object
				let mut x = Object::default();
				for i in v.props() {
					let (k, v) = i?;
					let k = String::from_atom(k)?;
					let v = Value::from_js(ctx, v)?;
					x.insert(k, v);
				}
				Ok(x.into())
			}
			_ => Ok(Value::None),
		}
	}
}
