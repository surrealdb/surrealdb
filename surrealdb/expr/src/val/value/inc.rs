#![deny(clippy::wildcard_enum_match_arm)]

use anyhow::Result;

use crate::expr::part::Part;
use crate::val::{Number, TryAdd, Value};

impl Value {
	/// Synchronous method for incrementing a field in a `Value`
	pub fn inc(&mut self, path: &[Part], val: Value) -> Result<()> {
		match self.pick(path) {
			Value::Array(v) => {
				if let Value::Array(x) = val {
					self.put(path, Value::from(v.concat(x)));
				} else {
					self.put(path, Value::from(v.with_push(val)));
				}
			}
			Value::None => match val {
				Value::Number(x) => self.put(path, Value::Number(Number::Int(0) + x)),
				Value::Array(x) => self.put(path, Value::from(x)),
				x @ (Value::None
				| Value::Null
				| Value::Bool(_)
				| Value::String(_)
				| Value::Duration(_)
				| Value::Datetime(_)
				| Value::Date(_)
				| Value::Time(_)
				| Value::Timestamp(_)
				| Value::Interval(_)
				| Value::Uuid(_)
				| Value::Set(_)
				| Value::Object(_)
				| Value::Geometry(_)
				| Value::Bytes(_)
				| Value::Table(_)
				| Value::RecordId(_)
				| Value::File(_)
				| Value::Regex(_)
				| Value::Range(_)
				| Value::Closure(_)) => self.put(path, Value::from(vec![x])),
			},
			v @ (Value::Null
			| Value::Bool(_)
			| Value::Number(_)
			| Value::String(_)
			| Value::Duration(_)
			| Value::Datetime(_)
			| Value::Date(_)
			| Value::Time(_)
			| Value::Timestamp(_)
			| Value::Interval(_)
			| Value::Uuid(_)
			| Value::Set(_)
			| Value::Object(_)
			| Value::Geometry(_)
			| Value::Bytes(_)
			| Value::Table(_)
			| Value::RecordId(_)
			| Value::File(_)
			| Value::Regex(_)
			| Value::Range(_)
			| Value::Closure(_)) => {
				let result = v.try_add(val)?;
				self.put(path, result);
			}
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::expr::idiom::Idiom;
	use crate::syn;

	macro_rules! parse_val {
		($input:expr) => {
			crate::val::convert_public_value_to_internal(syn::value($input).unwrap())
		};
	}

	#[tokio::test]
	async fn increment_none() {
		let idi: Idiom = syn::idiom("other").unwrap();
		let mut val = parse_val!("{ test: 100 }");
		let res = parse_val!("{ test: 100, other: +10 }");
		val.inc(&idi, Value::from(10)).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn increment_number() {
		let idi: Idiom = syn::idiom("test").unwrap();
		let mut val = parse_val!("{ test: 100 }");
		let res = parse_val!("{ test: 110 }");
		val.inc(&idi, Value::from(10)).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn increment_object_number_errors() {
		let idi: Idiom = syn::idiom("test").unwrap();
		let mut val = parse_val!("{ test: { a: 1 } }");
		assert!(val.inc(&idi, Value::from(10)).is_err());
	}

	#[tokio::test]
	async fn increment_array_number() {
		let idi: Idiom = syn::idiom("test[1]").unwrap();
		let mut val = parse_val!("{ test: [100, 200, 300] }");
		let res = parse_val!("{ test: [100, 210, 300] }");
		val.inc(&idi, Value::from(10)).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn increment_array_value() {
		let idi: Idiom = syn::idiom("test").unwrap();
		let mut val = parse_val!("{ test: [100, 200, 300] }");
		let res = parse_val!("{ test: [100, 200, 300, 200] }");
		val.inc(&idi, Value::from(200)).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn increment_array_array() {
		let idi: Idiom = syn::idiom("test").unwrap();
		let mut val = parse_val!("{ test: [100, 200, 300] }");
		let res = parse_val!("{ test: [100, 200, 300, 100, 300, 400, 500] }");
		val.inc(&idi, parse_val!("[100, 300, 400, 500]")).unwrap();
		assert_eq!(res, val);
	}
}
