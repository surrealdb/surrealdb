use super::classes;
use crate::sql::array::Array;
use crate::sql::datetime::Datetime;
use crate::sql::object::Object;
use crate::sql::value::Value;
use crate::sql::Id;
use chrono::{TimeZone, Utc};
use js::prelude::This;
use js::Ctx;
use js::Error;
use js::Exception;
use js::FromAtom;
use js::FromJs;

fn check_nul(s: &str) -> Result<(), Error> {
	if s.contains('\0') {
		Err(Error::InvalidString(std::ffi::CString::new(s).unwrap_err()))
	} else {
		Ok(())
	}
}

impl<'js> FromJs<'js> for Value {
	fn from_js(ctx: &Ctx<'js>, val: js::Value<'js>) -> Result<Self, Error> {
		match val {
			val if val.type_name() == "null" => Ok(Value::Null),
			val if val.type_name() == "undefined" => Ok(Value::None),
			val if val.is_bool() => Ok(val.as_bool().unwrap().into()),
			val if val.is_string() => match val.into_string().unwrap().to_string() {
				Ok(v) => {
					check_nul(&v)?;
					Ok(Value::from(v))
				}
				Err(e) => Err(e),
			},
			val if val.is_number() => Ok(val.as_number().unwrap().into()),
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
					let (Ok(e) | Err(e)) =
						Exception::from_message(ctx.clone(), &e).map(|x| x.throw());
					return Err(e);
				}
				// Check to see if this object is a record
				if (v).instance_of::<classes::record::Record>() {
					let v = v.into_class::<classes::record::Record>().unwrap();
					let borrow = v.borrow();
					let v: &classes::record::Record = &borrow;
					check_nul(&v.value.tb)?;
					if let Id::String(s) = &v.value.id {
						check_nul(s)?;
					}
					return Ok(v.value.clone().into());
				}
				// Check to see if this object is a duration
				if (v).instance_of::<classes::duration::Duration>() {
					let v = v.into_class::<classes::duration::Duration>().unwrap();
					let borrow = v.borrow();
					let v: &classes::duration::Duration = &borrow;
					return match &v.value {
						Some(v) => Ok(v.clone().into()),
						None => Ok(Value::None),
					};
				}
				// Check to see if this object is a uuid
				if (v).instance_of::<classes::uuid::Uuid>() {
					let v = v.into_class::<classes::uuid::Uuid>().unwrap();
					let borrow = v.borrow();
					let v: &classes::uuid::Uuid = &borrow;
					return match &v.value {
						Some(v) => Ok(v.clone().into()),
						None => Ok(Value::None),
					};
				}
				// Check to see if this object is a date
				let date: js::Object = ctx.globals().get("Date")?;
				if (v).is_instance_of(&date) {
					let f: js::Function = v.get("getTime")?;
					let m: i64 = f.call((This(v),))?;
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
					check_nul(&k)?;
					let v = Value::from_js(ctx, v)?;
					x.insert(k, v);
				}
				Ok(x.into())
			}
			_ => Ok(Value::None),
		}
	}
}
