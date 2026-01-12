use crate::expr::part::Part;
use crate::val::{Number, Value};

impl Value {
	/// Synchronous method for incrementing a field in a `Value`
	pub(crate) fn inc(&mut self, path: &[Part], val: Value) {
		match self.pick(path) {
			Value::Number(v) => {
				if let Value::Number(x) = val {
					self.put(path, Value::from(v + x))
				}
			}
			Value::Array(v) => match val {
				Value::Array(x) => self.put(path, Value::from(v.concat(x))),
				x => self.put(path, Value::from(v.with_push(x))),
			},
			Value::None => match val {
				Value::Number(x) => self.put(path, Value::Number(Number::Int(0) + x)),
				Value::Array(x) => self.put(path, Value::from(x)),
				x => self.put(path, Value::from(vec![x])),
			},
			_ => (),
		}
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
		let idi: Idiom = syn::idiom("other").unwrap().into();
		let mut val = parse_val!("{ test: 100 }");
		let res = parse_val!("{ test: 100, other: +10 }");
		val.inc(&idi, Value::from(10));
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn increment_number() {
		let idi: Idiom = syn::idiom("test").unwrap().into();
		let mut val = parse_val!("{ test: 100 }");
		let res = parse_val!("{ test: 110 }");
		val.inc(&idi, Value::from(10));
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn increment_array_number() {
		let idi: Idiom = syn::idiom("test[1]").unwrap().into();
		let mut val = parse_val!("{ test: [100, 200, 300] }");
		let res = parse_val!("{ test: [100, 210, 300] }");
		val.inc(&idi, Value::from(10));
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn increment_array_value() {
		let idi: Idiom = syn::idiom("test").unwrap().into();
		let mut val = parse_val!("{ test: [100, 200, 300] }");
		let res = parse_val!("{ test: [100, 200, 300, 200] }");
		val.inc(&idi, Value::from(200));
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn increment_array_array() {
		let idi: Idiom = syn::idiom("test").unwrap().into();
		let mut val = parse_val!("{ test: [100, 200, 300] }");
		let res = parse_val!("{ test: [100, 200, 300, 100, 300, 400, 500] }");
		val.inc(&idi, parse_val!("[100, 300, 400, 500]"));
		assert_eq!(res, val);
	}
}
