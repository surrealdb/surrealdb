use crate::sql::number::Number;
use crate::sql::part::Part;
use crate::sql::value::Value;

impl Value {
	/// Synchronous method for decrementing a field in a `Value`
	pub(crate) fn dec(&mut self, path: &[Part], val: Value) {
		match self.pick(path) {
			Value::Number(v) => {
				if let Value::Number(x) = val {
					self.put(path, Value::from(v - x))
				}
			}
			Value::Array(v) => match val {
				Value::Array(x) => self.put(path, Value::from(v - x)),
				x => self.put(path, Value::from(v - x)),
			},
			Value::None => {
				if let Value::Number(x) = val {
					self.put(path, Value::from(Number::from(0) - x))
				}
			}
			_ => (),
		}
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::sql::idiom::Idiom;
	use crate::syn::Parse;

	#[tokio::test]
	async fn decrement_none() {
		let idi = Idiom::parse("other");
		let mut val = Value::parse("{ test: 100 }");
		let res = Value::parse("{ test: 100, other: -10 }");
		val.dec(&idi, Value::from(10));
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn decrement_number() {
		let idi = Idiom::parse("test");
		let mut val = Value::parse("{ test: 100 }");
		let res = Value::parse("{ test: 90 }");
		val.dec(&idi, Value::from(10));
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn decrement_array_number() {
		let idi = Idiom::parse("test[1]");
		let mut val = Value::parse("{ test: [100, 200, 300] }");
		let res = Value::parse("{ test: [100, 190, 300] }");
		val.dec(&idi, Value::from(10));
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn decrement_array_value() {
		let idi = Idiom::parse("test");
		let mut val = Value::parse("{ test: [100, 200, 300] }");
		let res = Value::parse("{ test: [100, 300] }");
		val.dec(&idi, Value::from(200));
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn decrement_array_array() {
		let idi = Idiom::parse("test");
		let mut val = Value::parse("{ test: [100, 200, 300] }");
		let res = Value::parse("{ test: [200] }");
		val.dec(&idi, Value::parse("[100, 300]"));
		assert_eq!(res, val);
	}
}
