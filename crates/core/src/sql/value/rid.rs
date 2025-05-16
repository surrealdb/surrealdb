use crate::sql::paths::ID;
use crate::sql::value::SqlValue;

impl SqlValue {
	pub fn rid(&self) -> SqlValue {
		self.pick(&*ID)
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::sql::id::Id;
	use crate::sql::thing::Thing;
	use crate::syn::Parse;

	#[tokio::test]
	async fn rid_none() {
		let val = SqlValue::parse("{ test: { other: null, something: 123 } }");
		let res = SqlValue::None;
		assert_eq!(res, val.rid());
	}

	#[tokio::test]
	async fn rid_some() {
		let val = SqlValue::parse("{ id: test:id, test: { other: null, something: 123 } }");
		let res = SqlValue::Thing(Thing {
			tb: String::from("test"),
			id: Id::from("id"),
		});
		assert_eq!(res, val.rid());
	}
}
