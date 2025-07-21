use crate::err::Error;
use crate::expr::value::Value;
use anyhow::{Result, ensure};

impl Value {
	pub(crate) fn replace(&mut self, val: Value) -> Result<()> {
		// If this value is not an object, then error
		ensure!(
			val.is_object(),
			Error::InvalidContent {
				value: val,
			}
		);
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
		let mut val: Value = Value::parse("{ test: { other: null, something: 123 } }");
		let res: Value = Value::parse("{ other: true }");
		let obj: Value = Value::parse("{ other: true }");
		val.replace(obj).unwrap();
		assert_eq!(res, val);
	}
}
