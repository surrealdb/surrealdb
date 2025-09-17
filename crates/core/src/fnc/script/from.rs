use chrono::{TimeZone, Utc};
use js::prelude::This;
use js::{Coerced, Ctx, Error, Exception, FromAtom, FromJs};
use rust_decimal::Decimal;

use super::classes;
use crate::val::{Array, Bytes, Datetime, Geometry, Object, RecordIdKey, Value};

fn check_nul(s: &str) -> Result<(), Error> {
	if s.contains('\0') {
		Err(Error::InvalidString(std::ffi::CString::new(s).unwrap_err()))
	} else {
		Ok(())
	}
}

impl<'js> FromJs<'js> for Value {
	fn from_js(ctx: &Ctx<'js>, val: js::Value<'js>) -> Result<Self, Error> {
		match val.type_of() {
			js::Type::Undefined => Ok(Value::None),
			js::Type::Null => Ok(Value::Null),
			js::Type::Bool => Ok(Value::from(val.as_bool().unwrap())),
			js::Type::Int => Ok(Value::from(val.as_int().unwrap() as f64)),
			js::Type::Float => Ok(Value::from(val.as_float().unwrap())),
			js::Type::String => Ok(Value::from(val.as_string().unwrap().to_string()?)),
			js::Type::Array => {
				let v = val.as_array().unwrap();
				let mut x = Array::with_capacity(v.len());
				for i in v.iter() {
					let v = i?;
					x.push(Value::from_js(ctx, v)?);
				}
				Ok(x.into())
			}
			js::Type::BigInt => {
				// TODO: Optimize this if rquickjs ever supports better conversion methods.
				let str = Coerced::<String>::from_js(ctx, val)?;
				if let Ok(i) = str.parse::<i64>() {
					return Ok(Value::from(i));
				}
				match str.parse::<Decimal>() {
					Ok(x) => Ok(Value::from(x)),
					Err(e) => Err(Exception::from_message(ctx.clone(), &e.to_string())?.throw()),
				}
			}
			js::Type::Object | js::Type::Exception => {
				// Extract the value as an object
				let v = val.into_object().unwrap();
				// Check to see if this object is an error
				if v.is_error() {
					let e: String = v.get(js::atom::PredefinedAtom::Message)?;
					let (Ok(e) | Err(e)) =
						Exception::from_message(ctx.clone(), &e).map(|x| x.throw());
					return Err(e);
				}
				// Check to see if this object is a record
				if let Some(v) = v.as_class::<classes::record::Record>() {
					let borrow = v.borrow();
					let v: &classes::record::Record = &borrow;
					check_nul(&v.value.table)?;
					if let RecordIdKey::String(s) = &v.value.key {
						check_nul(s)?;
					}
					return Ok(v.value.clone().into());
				}
				// Check to see if this object is a duration
				if let Some(v) = v.as_class::<classes::duration::Duration>() {
					let borrow = v.borrow();
					let v: &classes::duration::Duration = &borrow;
					return match &v.value {
						Some(v) => Ok((*v).into()),
						None => Ok(Value::None),
					};
				}
				// Check to see if this object is a uuid
				if let Some(v) = v.as_class::<classes::uuid::Uuid>() {
					let borrow = v.borrow();
					let v: &classes::uuid::Uuid = &borrow;
					return match &v.value {
						Some(v) => Ok((*v).into()),
						None => Ok(Value::None),
					};
				}
				// Check to see if this object is a file
				if let Some(v) = v.as_class::<classes::file::File>() {
					let borrow = v.borrow();
					let v: &classes::file::File = &borrow;
					return Ok(v.value.clone().into());
				}

				if let Some(v) = v.as_typed_array::<u8>() {
					let Some(data) = v.as_bytes() else {
						return Err(Error::new_from_js_message(
							"Uint8Array",
							"Bytes",
							"Uint8Array data was consumed.",
						));
					};

					return Ok(Value::Bytes(Bytes(data.to_vec())));
				}

				// Check to see if this object is a date
				let date: js::Object = ctx.globals().get(js::atom::PredefinedAtom::Date)?;
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

				if let Some(x) = Geometry::try_from_object(&x) {
					return Ok(x.into());
				}

				Ok(x.into())
			}
			_ => Ok(Value::Null),
		}
	}
}
