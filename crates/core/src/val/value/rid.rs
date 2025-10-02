use crate::expr::paths::ID;
use crate::val::Value;

impl Value {
	pub fn rid(&self) -> Value {
		self.pick(&*ID)
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::syn;
	use crate::val::{RecordId, RecordIdKey};

	macro_rules! parse_val {
		($input:expr) => {
			crate::val::convert_public_value_to_internal(syn::value($input).unwrap())
		};
	}

	#[tokio::test]
	async fn rid_none() {
		let val = parse_val!("{ test: { other: null, something: 123 } }");
		let res = Value::None;
		assert_eq!(res, val.rid());
	}

	#[tokio::test]
	async fn rid_some() {
		let val = parse_val!("{ id: test:id, test: { other: null, something: 123 } }");
		let res = Value::RecordId(RecordId {
			table: String::from("test"),
			key: RecordIdKey::String("id".to_owned()),
		});
		assert_eq!(res, val.rid());
	}
}
