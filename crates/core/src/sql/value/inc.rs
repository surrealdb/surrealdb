use crate::sql::number::Number;
use crate::sql::part::Part;
use crate::sql::value::SqlValue;

impl SqlValue {
	/// Synchronous method for incrementing a field in a `Value`
	pub(crate) fn inc(&mut self, path: &[Part], val: SqlValue) {
		match self.pick(path) {
			SqlValue::Number(v) => {
				if let SqlValue::Number(x) = val {
					self.put(path, SqlValue::from(v + x))
				}
			}
			SqlValue::Array(v) => match val {
				SqlValue::Array(x) => self.put(path, SqlValue::from(v + x)),
				x => self.put(path, SqlValue::from(v + x)),
			},
			SqlValue::None => match val {
				SqlValue::Number(x) => self.put(path, SqlValue::from(Number::from(0) + x)),
				SqlValue::Array(x) => self.put(path, SqlValue::from(x)),
				x => self.put(path, SqlValue::from(vec![x])),
			},
			_ => (),
		}
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::sql::idiom::Idiom;
	use crate::syn::Parse;

	#[tokio::test]
	async fn increment_none() {
		let idi = Idiom::parse("other");
		let mut val = SqlValue::parse("{ test: 100 }");
		let res = SqlValue::parse("{ test: 100, other: +10 }");
		val.inc(&idi, SqlValue::from(10));
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn increment_number() {
		let idi = Idiom::parse("test");
		let mut val = SqlValue::parse("{ test: 100 }");
		let res = SqlValue::parse("{ test: 110 }");
		val.inc(&idi, SqlValue::from(10));
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn increment_array_number() {
		let idi = Idiom::parse("test[1]");
		let mut val = SqlValue::parse("{ test: [100, 200, 300] }");
		let res = SqlValue::parse("{ test: [100, 210, 300] }");
		val.inc(&idi, SqlValue::from(10));
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn increment_array_value() {
		let idi = Idiom::parse("test");
		let mut val = SqlValue::parse("{ test: [100, 200, 300] }");
		let res = SqlValue::parse("{ test: [100, 200, 300, 200] }");
		val.inc(&idi, SqlValue::from(200));
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn increment_array_array() {
		let idi = Idiom::parse("test");
		let mut val = SqlValue::parse("{ test: [100, 200, 300] }");
		let res = SqlValue::parse("{ test: [100, 200, 300, 100, 300, 400, 500] }");
		val.inc(&idi, SqlValue::parse("[100, 300, 400, 500]"));
		assert_eq!(res, val);
	}
}
