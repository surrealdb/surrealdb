use crate::val::Value;

impl Value {
	pub fn clear(&mut self) {
		*self = Value::None;
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::sql::expression::convert_public_value_to_internal;
	use crate::syn;

	#[tokio::test]
	async fn clear_value() {
		let mut val = convert_public_value_to_internal(
			syn::value("{ test: { other: null, something: 123 } }").unwrap(),
		);

		val.clear();
		assert_eq!(val, Value::None);
	}
}
