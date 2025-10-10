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

	macro_rules! parse_val {
		($input:expr) => {
			crate::val::convert_public_value_to_internal(syn::value($input).unwrap())
		};
	}

	#[tokio::test]
	async fn replace() {
		let mut val = parse_val!("{ test: { other: null, something: 123 } }");
		let res = parse_val!("{ other: true }");
		let obj = parse_val!("{ other: true }");
		val.replace(obj).unwrap();
		assert_eq!(res, val);
	}
}
