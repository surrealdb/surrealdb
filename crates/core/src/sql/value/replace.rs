use crate::err::Error;
use crate::sql::value::Value;

impl Value {
	pub(crate) fn replace(&mut self, val: Value) -> Result<(), Error> {
		// If this value is not an object, then error
		if !val.is_object() {
			return Err(Error::InvalidContent {
				value: val,
			});
		}
		// Otherwise replace the current value
		*self = val;
		Ok(())
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::syn::Parse;

	#[tokio::test]
	async fn replace() {
		let mut val = Value::parse("{ test: { other: null, something: 123 } }");
		let res = Value::parse("{ other: true }");
		let obj = Value::parse("{ other: true }");
		val.replace(obj).unwrap();
		assert_eq!(res, val);
	}
}
