use crate::expr::paths::ID;
use crate::expr::value::Value;

impl Value {
	pub fn rid(&self) -> Value {
		self.pick(&*ID)
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::expr::id::Id;
	use crate::expr::thing::Thing;
	use crate::syn::Parse;

	#[tokio::test]
	async fn rid_none() {
		let val = Value::parse("{ test: { other: null, something: 123 } }");
		let res = Value::None;
		assert_eq!(res, val.rid());
	}

	#[tokio::test]
	async fn rid_some() {
		let val = Value::parse("{ id: test:id, test: { other: null, something: 123 } }");
		let res = Value::Thing(Thing {
			tb: String::from("test"),
			id: Id::from("id"),
		});
		assert_eq!(res, val.rid());
	}
}
