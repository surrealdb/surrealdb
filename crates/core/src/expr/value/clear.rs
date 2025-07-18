use crate::expr::value::Value;

impl Value {
	pub fn clear(&mut self) {
		*self = Value::None;
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::syn::Parse;

	#[tokio::test]
	async fn clear_value() {
		let mut val: Value = Value::parse("{ test: { other: null, something: 123 } }");
		let res = Value::None;
		val.clear();
		assert_eq!(res, val);
	}
}
