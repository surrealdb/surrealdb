use crate::expr::part::{Next, Part};
use crate::val::Value;

impl Value {
	/// Synchronous method for deleting a field from a `Value`
	pub(crate) fn cut(&mut self, path: &[Part]) {
		if let Some(p) = path.first() {
			// Get the current value at path
			match self {
				// Current value at path is an object
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
					Part::All => match path.len() {
						1 => {
							v.clear();
						}
						_ => {
							let path = path.next();
							v.iter_mut().for_each(|(_, v)| v.cut(path));
						}
					},
					x => {
						if let Some(i) = x.as_old_index() {
							match path.len() {
								1 => {
									v.remove(&i.to_string());
								}
								_ => {
									if let Some(v) = v.get_mut(&i.to_string()) {
										v.cut(path.next())
									}
								}
							}
						}
					}
					_ => {}
				},
				// Current value at path is an array
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
					x => {
						if let Some(i) = x.as_old_index() {
							match path.len() {
								1 => {
									if v.len() > i {
										v.remove(i);
									}
								}
								_ => {
									if let Some(v) = v.get_mut(i) {
										v.cut(path.next())
									}
								}
							}
						} else {
							v.iter_mut().for_each(|v| v.cut(path));
						}
					}
				},
				// Ignore everything else
				_ => (),
			}
		}
	}
}

/*
#[cfg(test)]
mod tests {

	use super::*;
	use crate::expr::idiom::Idiom;
	use crate::sql::SqlValue;
	use crate::sql::idiom::Idiom as SqlIdiom;
	use crate::syn::Parse;

	#[tokio::test]
	async fn cut_none() {
		let idi: Idiom = SqlIdiom::default().into();
		let mut val: Value = SqlValue::parse("{ test: { other: null, something: 123 } }").into();
		let res: Value = SqlValue::parse("{ test: { other: null, something: 123 } }").into();
		val.cut(&idi);
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn cut_reset() {
		let idi: Idiom = SqlIdiom::parse("test").into();
		let mut val: Value = SqlValue::parse("{ test: { other: null, something: 123 } }").into();
		let res: Value = SqlValue::parse("{ }").into();
		val.cut(&idi);
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn cut_basic() {
		let idi: Idiom = SqlIdiom::parse("test.something").into();
		let mut val: Value = SqlValue::parse("{ test: { other: null, something: 123 } }").into();
		let res: Value = SqlValue::parse("{ test: { other: null } }").into();
		val.cut(&idi);
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn cut_wrong() {
		let idi: Idiom = SqlIdiom::parse("test.something.wrong").into();
		let mut val: Value = SqlValue::parse("{ test: { other: null, something: 123 } }").into();
		let res: Value = SqlValue::parse("{ test: { other: null, something: 123 } }").into();
		val.cut(&idi);
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn cut_other() {
		let idi: Idiom = SqlIdiom::parse("test.other.something").into();
		let mut val: Value = SqlValue::parse("{ test: { other: null, something: 123 } }").into();
		let res: Value = SqlValue::parse("{ test: { other: null, something: 123 } }").into();
		val.cut(&idi);
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn cut_array() {
		let idi: Idiom = SqlIdiom::parse("test.something[1]").into();
		let mut val: Value = SqlValue::parse("{ test: { something: [123, 456, 789] } }").into();
		let res: Value = SqlValue::parse("{ test: { something: [123, 789] } }").into();
		val.cut(&idi);
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn cut_array_field() {
		let idi: Idiom = SqlIdiom::parse("test.something[1].age").into();
		let mut val: Value = SqlValue::parse(
			"{ test: { something: [{ name: 'A', age: 34 }, { name: 'B', age: 36 }] } }",
		)
		.into();
		let res: Value =
			SqlValue::parse("{ test: { something: [{ name: 'A', age: 34 }, { name: 'B' }] } }")
				.into();
		val.cut(&idi);
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn cut_array_fields() {
		let idi: Idiom = SqlIdiom::parse("test.something[*].age").into();
		let mut val: Value = SqlValue::parse(
			"{ test: { something: [{ name: 'A', age: 34 }, { name: 'B', age: 36 }] } }",
		)
		.into();
		let res: Value =
			SqlValue::parse("{ test: { something: [{ name: 'A' }, { name: 'B' }] } }").into();
		val.cut(&idi);
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn cut_array_fields_flat() {
		let idi: Idiom = SqlIdiom::parse("test.something.age").into();
		let mut val: Value = SqlValue::parse(
			"{ test: { something: [{ name: 'A', age: 34 }, { name: 'B', age: 36 }] } }",
		)
		.into();
		let res: Value =
			SqlValue::parse("{ test: { something: [{ name: 'A' }, { name: 'B' }] } }").into();
		val.cut(&idi);
		assert_eq!(res, val);
	}
}
*/
