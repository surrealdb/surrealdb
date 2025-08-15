use anyhow::{Result, ensure};

use crate::err::Error;
use crate::val::Value;

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

	use crate::syn;

	#[tokio::test]
	async fn replace() {
		let mut val = syn::value("{ test: { other: null, something: 123 } }").unwrap();
		let res = syn::value("{ other: true }").unwrap();
		let obj = syn::value("{ other: true }").unwrap();
		val.replace(obj).unwrap();
		assert_eq!(res, val);
	}
}
