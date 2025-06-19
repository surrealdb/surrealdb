use crate::err::Error;
use crate::val::Value;
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
	use crate::sql::SqlValue;
	use crate::syn::Parse;

	#[tokio::test]
	async fn replace() {
		let mut val: Value = SqlValue::parse("{ test: { other: null, something: 123 } }").into();
		let res: Value = SqlValue::parse("{ other: true }").into();
		let obj: Value = SqlValue::parse("{ other: true }").into();
		val.replace(obj).unwrap();
		assert_eq!(res, val);
	}
}
