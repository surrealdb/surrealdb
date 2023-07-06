use crate::sql::part::Next;
use crate::sql::part::Part;
use crate::sql::value::Value;

impl Value {
	/// Synchronous method for deleting a field from a `Value`
	pub(crate) fn cut(&mut self, path: &[Part]) {
		if let Some(p) = path.first() {
			// Get the current path part
			match self {
				// Current path part is an object
				Value::Object(v) => match p {
					Part::Field(f) => match path.len() {
						1 => {
							v.remove(f.as_str());
						}
						_ => {
							if let Some(v) = v.get_mut(f.as_str()) {
								v.cut(path.next())
							}
						}
					},
					Part::Index(i) => match path.len() {
						1 => {
							v.remove(&i.to_string());
						}
						_ => {
							if let Some(v) = v.get_mut(&i.to_string()) {
								v.cut(path.next())
							}
						}
					},
					_ => {}
				},
				// Current path part is an array
				Value::Array(v) => match p {
					Part::All => match path.len() {
						1 => {
							v.clear();
						}
						_ => {
							let path = path.next();
							v.iter_mut().for_each(|v| v.cut(path));
						}
					},
					Part::First => match path.len() {
						1 => {
							if !v.is_empty() {
								let i = 0;
								v.remove(i);
							}
						}
						_ => {
							if let Some(v) = v.first_mut() {
								v.cut(path.next())
							}
						}
					},
					Part::Last => match path.len() {
						1 => {
							if !v.is_empty() {
								let i = v.len() - 1;
								v.remove(i);
							}
						}
						_ => {
							if let Some(v) = v.last_mut() {
								v.cut(path.next())
							}
						}
					},
					Part::Index(i) => match path.len() {
						1 => {
							if v.len().gt(&i.to_usize()) {
								v.remove(i.to_usize());
							}
						}
						_ => {
							if let Some(v) = v.get_mut(i.to_usize()) {
								v.cut(path.next())
							}
						}
					},
					_ => {
						v.iter_mut().for_each(|v| v.cut(path));
					}
				},
				// Ignore everything else
				_ => (),
			}
		}
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::dbs::test::mock;
	use crate::sql::idiom::Idiom;
	use crate::sql::test::Parse;

	#[tokio::test]
	async fn del_none() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::default();
		let mut val = Value::parse("{ test: { other: null, something: 123 } }");
		let res = Value::parse("{ test: { other: null, something: 123 } }");
		val.del(&ctx, &opt, &txn, &idi).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn del_reset() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test");
		let mut val = Value::parse("{ test: { other: null, something: 123 } }");
		let res = Value::parse("{ }");
		val.del(&ctx, &opt, &txn, &idi).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn del_basic() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.something");
		let mut val = Value::parse("{ test: { other: null, something: 123 } }");
		let res = Value::parse("{ test: { other: null } }");
		val.del(&ctx, &opt, &txn, &idi).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn del_wrong() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.something.wrong");
		let mut val = Value::parse("{ test: { other: null, something: 123 } }");
		let res = Value::parse("{ test: { other: null, something: 123 } }");
		val.del(&ctx, &opt, &txn, &idi).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn del_other() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.other.something");
		let mut val = Value::parse("{ test: { other: null, something: 123 } }");
		let res = Value::parse("{ test: { other: null, something: 123 } }");
		val.del(&ctx, &opt, &txn, &idi).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn del_array() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.something[1]");
		let mut val = Value::parse("{ test: { something: [123, 456, 789] } }");
		let res = Value::parse("{ test: { something: [123, 789] } }");
		val.del(&ctx, &opt, &txn, &idi).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn del_array_field() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.something[1].age");
		let mut val = Value::parse(
			"{ test: { something: [{ name: 'A', age: 34 }, { name: 'B', age: 36 }] } }",
		);
		let res = Value::parse("{ test: { something: [{ name: 'A', age: 34 }, { name: 'B' }] } }");
		val.del(&ctx, &opt, &txn, &idi).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn del_array_fields() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.something[*].age");
		let mut val = Value::parse(
			"{ test: { something: [{ name: 'A', age: 34 }, { name: 'B', age: 36 }] } }",
		);
		let res = Value::parse("{ test: { something: [{ name: 'A' }, { name: 'B' }] } }");
		val.del(&ctx, &opt, &txn, &idi).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn del_array_fields_flat() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.something.age");
		let mut val = Value::parse(
			"{ test: { something: [{ name: 'A', age: 34 }, { name: 'B', age: 36 }] } }",
		);
		let res = Value::parse("{ test: { something: [{ name: 'A' }, { name: 'B' }] } }");
		val.del(&ctx, &opt, &txn, &idi).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn del_array_where_field() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.something[WHERE age > 35].age");
		let mut val = Value::parse(
			"{ test: { something: [{ name: 'A', age: 34 }, { name: 'B', age: 36 }] } }",
		);
		let res = Value::parse("{ test: { something: [{ name: 'A', age: 34 }, { name: 'B' }] } }");
		val.del(&ctx, &opt, &txn, &idi).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn del_array_where_fields() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.something[WHERE age > 35]");
		let mut val = Value::parse(
			"{ test: { something: [{ name: 'A', age: 34 }, { name: 'B', age: 36 }] } }",
		);
		let res = Value::parse("{ test: { something: [{ name: 'A', age: 34 }] } }");
		val.del(&ctx, &opt, &txn, &idi).await.unwrap();
		assert_eq!(res, val);
	}
}
