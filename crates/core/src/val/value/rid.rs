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

	#[tokio::test]
	async fn rid_none() {
		let val = syn::value("{ test: { other: null, something: 123 } }").unwrap();
		let res = Value::None;
		assert_eq!(res, val.rid());
	}

	#[tokio::test]
	async fn rid_some() {
		let val = syn::value("{ id: test:id, test: { other: null, something: 123 } }").unwrap();
		let res = Value::RecordId(RecordId {
			table: String::from("test"),
			key: RecordIdKey::String("id".to_owned()),
		});
		assert_eq!(res, val.rid());
	}
}
