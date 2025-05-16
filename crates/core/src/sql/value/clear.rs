use crate::err::Error;
use crate::sql::value::SqlValue;

impl SqlValue {
	pub fn clear(&mut self) -> Result<(), Error> {
		*self = SqlValue::None;
		Ok(())
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::syn::Parse;

	#[tokio::test]
	async fn clear_value() {
		let mut val = SqlValue::parse("{ test: { other: null, something: 123 } }");
		let res = SqlValue::None;
		val.clear().unwrap();
		assert_eq!(res, val);
	}
}
