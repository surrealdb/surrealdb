use crate::expr::value::Value;

impl Value {
	pub fn clear(&mut self) {
		*self = Value::None;
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::{sql::SqlValue, syn::Parse};

	#[tokio::test]
	async fn clear_value() {
		let mut val: Value = SqlValue::parse("{ test: { other: null, something: 123 } }").into();
		let res = Value::None;
		val.clear();
		assert_eq!(res, val);
	}
}
