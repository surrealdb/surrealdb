use crate::val::Value;

impl Value {
	pub fn clear(&mut self) {
		*self = Value::None;
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::syn;

	#[tokio::test]
	async fn clear_value() {
		let mut val = syn::value("{ test: { other: null, something: 123 } }").unwrap();
		let res = Value::None;
		val.clear();
		assert_eq!(res, val);
	}
}
