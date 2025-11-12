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
							v.remove(&**f);
						}
						_ => {
							if let Some(v) = v.get_mut(&**f) {
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

#[cfg(test)]
mod tests {

	use crate::expr::idiom::Idiom;
	use crate::syn;

	macro_rules! parse_val {
		($input:expr) => {
			crate::val::convert_public_value_to_internal(syn::value($input).unwrap())
		};
	}

	#[tokio::test]
	async fn cut_none() {
		let idi: Idiom = Idiom::default();
		let mut val = parse_val!("{ test: { other: null, something: 123 } }");
		let res = parse_val!("{ test: { other: null, something: 123 } }");
		val.cut(&idi);
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn cut_reset() {
		let idi: Idiom = syn::idiom("test").unwrap().into();
		let mut val = parse_val!("{ test: { other: null, something: 123 } }");
		let res = parse_val!("{ }");
		val.cut(&idi);
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn cut_basic() {
		let idi: Idiom = syn::idiom("test.something").unwrap().into();
		let mut val = parse_val!("{ test: { other: null, something: 123 } }");
		let res = parse_val!("{ test: { other: null } }");
		val.cut(&idi);
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn cut_wrong() {
		let idi: Idiom = syn::idiom("test.something.wrong").unwrap().into();
		let mut val = parse_val!("{ test: { other: null, something: 123 } }");
		let res = parse_val!("{ test: { other: null, something: 123 } }");
		val.cut(&idi);
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn cut_other() {
		let idi: Idiom = syn::idiom("test.other.something").unwrap().into();
		let mut val = parse_val!("{ test: { other: null, something: 123 } }");
		let res = parse_val!("{ test: { other: null, something: 123 } }");
		val.cut(&idi);
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn cut_array() {
		let idi: Idiom = syn::idiom("test.something[1]").unwrap().into();
		let mut val = parse_val!("{ test: { something: [123, 456, 789] } }");
		let res = parse_val!("{ test: { something: [123, 789] } }");
		val.cut(&idi);
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn cut_array_field() {
		let idi: Idiom = syn::idiom("test.something[1].age").unwrap().into();
		let mut val =
			parse_val!("{ test: { something: [{ name: 'A', age: 34 }, { name: 'B', age: 36 }] } }");
		let res = parse_val!("{ test: { something: [{ name: 'A', age: 34 }, { name: 'B' }] } }");
		val.cut(&idi);
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn cut_array_fields() {
		let idi: Idiom = syn::idiom("test.something[*].age").unwrap().into();
		let mut val =
			parse_val!("{ test: { something: [{ name: 'A', age: 34 }, { name: 'B', age: 36 }] } }");
		let res = parse_val!("{ test: { something: [{ name: 'A' }, { name: 'B' }] } }");
		val.cut(&idi);
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn cut_array_fields_flat() {
		let idi: Idiom = syn::idiom("test.something.age").unwrap().into();
		let mut val =
			parse_val!("{ test: { something: [{ name: 'A', age: 34 }, { name: 'B', age: 36 }] } }");
		let res = parse_val!("{ test: { something: [{ name: 'A' }, { name: 'B' }] } }");
		val.cut(&idi);
		assert_eq!(res, val);
	}
}
