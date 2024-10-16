use crate::err::Error;
use crate::sql::value::Value;

impl Value {
	pub fn clear(&mut self) -> Result<(), Error> {
		*self = Value::None;
		Ok(())
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::syn::Parse;

	#[tokio::test]
	async fn clear_value() {
		let mut val = Value::parse("{ test: { other: null, something: 123 } }");
		let res = Value::None;
		val.clear().unwrap();
		assert_eq!(res, val);
	}
}
