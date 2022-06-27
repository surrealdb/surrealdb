#![cfg(feature = "scripting")]

use crate::ctx::Context;
use crate::err::Error;
use crate::sql::array::Array;
use crate::sql::datetime::Datetime;
use crate::sql::duration::Duration;
use crate::sql::number::Number;
use crate::sql::object::Object;
use crate::sql::thing::Thing;
use crate::sql::value::Value;
use bigdecimal::ToPrimitive;
use boa::builtins::date::Date;
use boa::class::Class;
use boa::class::ClassBuilder;
use boa::object::JsArray;
use boa::object::JsObject;
use boa::object::ObjectData;
use boa::object::ObjectKind;
use boa::property::Attribute;
use boa::Context as Boa;
use boa::JsResult;
use boa::JsString;
use boa::JsValue;
use chrono::Datelike;
use chrono::Timelike;
use gc::Finalize;
use gc::Trace;

#[derive(Debug, Trace, Finalize)]
pub struct JsDuration {
	value: String,
}

impl Class for JsDuration {
	const NAME: &'static str = "Duration";
	const LENGTH: usize = 1;
	fn constructor(_this: &JsValue, args: &[JsValue], ctx: &mut Boa) -> JsResult<Self> {
		Ok(JsDuration {
			value: args.get(0).cloned().unwrap_or_default().to_string(ctx)?.to_string(),
		})
	}
	fn init(class: &mut ClassBuilder) -> JsResult<()> {
		class.method("value", 0, |this, _, _| {
			if let Some(v) = this.as_object() {
				if let Some(v) = v.downcast_ref::<JsDuration>() {
					return Ok(v.value.clone().into());
				}
			}
			Ok(JsValue::Undefined)
		});
		class.method("toString", 0, |this, _, _| {
			if let Some(v) = this.as_object() {
				if let Some(v) = v.downcast_ref::<JsDuration>() {
					return Ok(v.value.clone().into());
				}
			}
			Ok(JsValue::Undefined)
		});
		Ok(())
	}
}

#[derive(Debug, Trace, Finalize)]
pub struct JsRecord {
	tb: String,
	id: String,
}

impl Class for JsRecord {
	const NAME: &'static str = "Record";
	const LENGTH: usize = 2;
	fn constructor(_this: &JsValue, args: &[JsValue], ctx: &mut Boa) -> JsResult<Self> {
		Ok(JsRecord {
			tb: args.get(0).cloned().unwrap_or_default().to_string(ctx)?.to_string(),
			id: args.get(1).cloned().unwrap_or_default().to_string(ctx)?.to_string(),
		})
	}
	fn init(class: &mut ClassBuilder) -> JsResult<()> {
		class.method("tb", 0, |this, _, _| {
			if let Some(v) = this.as_object() {
				if let Some(v) = v.downcast_ref::<JsRecord>() {
					return Ok(v.tb.clone().into());
				}
			}
			Ok(JsValue::Undefined)
		});
		class.method("id", 0, |this, _, _| {
			if let Some(v) = this.as_object() {
				if let Some(v) = v.downcast_ref::<JsRecord>() {
					return Ok(v.id.clone().into());
				}
			}
			Ok(JsValue::Undefined)
		});
		class.method("toString", 0, |this, _, _| {
			if let Some(v) = this.as_object() {
				if let Some(v) = v.downcast_ref::<JsRecord>() {
					return Ok(format!("{}:{}", v.tb, v.id).into());
				}
			}
			Ok(JsValue::Undefined)
		});
		Ok(())
	}
}

pub fn run(ctx: &Context, src: &str, arg: Vec<Value>, doc: Option<&Value>) -> Result<Value, Error> {
	let _ = ctx.check()?;
	// Create an execution context
	let mut ctx = Boa::default();
	// Convert the arguments to JavaScript
	let args = JsValue::from(Value::from(arg));
	// Convert the current document to JavaScript
	let this = doc.map_or(JsValue::Undefined, JsValue::from);
	// Create the main function structure
	let src = format!("(function() {{ {} }}).apply(self, args)", src);
	// Register the current document as a global object
	ctx.register_global_property("self", this, Attribute::default());
	// Register the current document as a global object
	ctx.register_global_property("args", args, Attribute::default());
	// Register the JsDuration type as a global class
	ctx.register_global_class::<JsDuration>().unwrap();
	// Register the JsRecord type as a global class
	ctx.register_global_class::<JsRecord>().unwrap();
	// Attempt to execute the script
	match ctx.eval(src.as_bytes()) {
		// The script executed successfully
		Ok(ref v) => Ok(v.into()),
		// There was an error running the script
		Err(e) => Err(Error::InvalidScript {
			message: e.display().to_string(),
		}),
	}
}

impl From<Value> for JsValue {
	fn from(v: Value) -> Self {
		JsValue::from(&v)
	}
}

impl From<&Datetime> for Date {
	fn from(v: &Datetime) -> Self {
		let mut obj = Self::default();
		obj.set_components(
			true,
			Some(v.year() as f64),
			Some(v.month0() as f64),
			Some(v.day() as f64),
			Some(v.hour() as f64),
			Some(v.minute() as f64),
			Some(v.second() as f64),
			Some((v.nanosecond() / 1_000_000) as f64),
		);
		obj
	}
}

impl From<&Value> for JsValue {
	fn from(v: &Value) -> Self {
		match v {
			Value::Null => JsValue::Null,
			Value::Void => JsValue::Undefined,
			Value::None => JsValue::Undefined,
			Value::True => JsValue::Boolean(true),
			Value::False => JsValue::Boolean(false),
			Value::Strand(v) => JsValue::String(v.as_str().into()),
			Value::Number(Number::Int(v)) => JsValue::Integer(*v as i32),
			Value::Number(Number::Float(v)) => JsValue::Rational(*v as f64),
			Value::Number(Number::Decimal(v)) => match v.is_integer() {
				true => JsValue::BigInt(v.to_i64().unwrap_or_default().into()),
				false => JsValue::Rational(v.to_f64().unwrap_or_default()),
			},
			Value::Datetime(v) => JsValue::from(JsObject::from_proto_and_data(
				Boa::default().intrinsics().constructors().date().prototype(),
				ObjectData::date(v.into()),
			)),
			Value::Duration(v) => JsValue::from(JsObject::from_proto_and_data(
				Boa::default().intrinsics().constructors().object().prototype(),
				ObjectData::native_object(Box::new(JsDuration {
					value: v.to_string(),
				})),
			)),
			Value::Thing(v) => JsValue::from(JsObject::from_proto_and_data(
				Boa::default().intrinsics().constructors().object().prototype(),
				ObjectData::native_object(Box::new(JsRecord {
					tb: v.tb.to_string(),
					id: v.id.to_string(),
				})),
			)),
			Value::Array(v) => {
				let ctx = &mut Boa::default();
				let arr = JsArray::new(ctx);
				for v in v.iter() {
					arr.push(JsValue::from(v), ctx).unwrap();
				}
				JsValue::from(arr)
			}
			Value::Object(v) => {
				let ctx = &mut Boa::default();
				let obj = JsObject::default();
				for (k, v) in v.iter() {
					let k = JsString::from(k.as_str());
					let v = JsValue::from(v);
					obj.set(k, v, true, ctx).unwrap();
				}
				JsValue::from(obj)
			}
			_ => JsValue::Null,
		}
	}
}

impl From<&JsValue> for Value {
	fn from(v: &JsValue) -> Self {
		match v {
			JsValue::Null => Value::Null,
			JsValue::Undefined => Value::None,
			JsValue::Boolean(v) => Value::from(*v),
			JsValue::String(v) => Value::from(v.as_str()),
			JsValue::Integer(v) => Value::from(Number::Int(*v as i64)),
			JsValue::Rational(v) => Value::from(Number::Float(*v as f64)),
			JsValue::BigInt(v) => Value::from(Number::from(v.clone().to_string())),
			JsValue::Object(v) => {
				// Check to see if this object is a duration
				if v.is::<JsDuration>() {
					if let Some(v) = v.downcast_ref::<JsDuration>() {
						let v = v.value.clone();
						return Duration::from(v).into();
					}
				}
				// Check to see if this object is a record
				if v.is::<JsRecord>() {
					if let Some(v) = v.downcast_ref::<JsRecord>() {
						let v = (v.tb.clone(), v.id.clone());
						return Thing::from(v).into();
					}
				}
				// Check to see if this object is a date
				if let Some(v) = v.borrow().as_date() {
					if let Some(v) = v.to_utc() {
						return Datetime::from(v).into();
					}
				}
				// Get a borrowed reference to the object
				let o = v.borrow();
				// Check to see if this is a normal type
				match o.kind() {
					// This object is a Javascript Array
					ObjectKind::Array => {
						let mut x = Array::default();
						let ctx = &mut Boa::default();
						let len = v.get("length", ctx).unwrap().to_u32(ctx).unwrap();
						for i in 0..len {
							let v = o.properties().get(&i.into()).unwrap();
							if let Some(v) = v.value() {
								let v = Value::from(v);
								x.push(v);
							}
						}
						x.into()
					}
					// This object is a Javascript Object
					ObjectKind::Ordinary => {
						let mut x = Object::default();
						for (k, v) in o.properties().iter() {
							if let Some(v) = v.value() {
								let k = k.to_string();
								let v = Value::from(v);
								x.insert(k, v);
							}
						}
						x.into()
					}
					// This object is a Javascript Map
					ObjectKind::Map(v) => {
						let mut x = Object::default();
						for (k, v) in v.iter() {
							let k = Value::from(k).as_string();
							let v = Value::from(v);
							x.insert(k, v);
						}
						x.into()
					}
					// This object is a Javascript Set
					ObjectKind::Set(v) => {
						let mut x = Array::default();
						for v in v.iter() {
							let v = Value::from(v);
							x.push(v);
						}
						x.into()
					}
					_ => Value::Null,
				}
			}
			_ => Value::Null,
		}
	}
}
