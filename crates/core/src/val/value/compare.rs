use std::cmp::Ordering;

use crate::expr::part::{Next, Part};
use crate::val::Value;

impl Value {
	pub(crate) fn compare(
		&self,
		other: &Self,
		path: &[Part],
		collate: bool,
		numeric: bool,
	) -> Option<Ordering> {
		match path.first() {
			// Get the current path part
			Some(p) => match (self, other) {
				// Current path part is an object
				(Value::Object(a), Value::Object(b)) => match p {
					Part::Field(f) => match (a.get(&**f), b.get(&**f)) {
						(Some(a), Some(b)) => a.compare(b, path.next(), collate, numeric),
						(Some(_), None) => Some(Ordering::Greater),
						(None, Some(_)) => Some(Ordering::Less),
						(_, _) => Some(Ordering::Equal),
					},
					_ => None,
				},
				// Current path part is an array
				(Value::Array(a), Value::Array(b)) => match p {
					Part::All => {
						for (a, b) in a.iter().zip(b.iter()) {
							match a.compare(b, path.next(), collate, numeric) {
								Some(Ordering::Equal) => continue,
								None => continue,
								o => return o,
							}
						}
						match (a.len(), b.len()) {
							(a, b) if a > b => Some(Ordering::Greater),
							(a, b) if a < b => Some(Ordering::Less),
							_ => Some(Ordering::Equal),
						}
					}
					Part::First => match (a.first(), b.first()) {
						(Some(a), Some(b)) => a.compare(b, path.next(), collate, numeric),
						(Some(_), None) => Some(Ordering::Greater),
						(None, Some(_)) => Some(Ordering::Less),
						(_, _) => Some(Ordering::Equal),
					},
					Part::Last => match (a.last(), b.last()) {
						(Some(a), Some(b)) => a.compare(b, path.next(), collate, numeric),
						(Some(_), None) => Some(Ordering::Greater),
						(None, Some(_)) => Some(Ordering::Less),
						(_, _) => Some(Ordering::Equal),
					},
					//TODO: It is kind of weird that a[1] works but `a[+(1)]` or `let $b = 1;
					// a[$b]` for example doesn't as
					x => {
						if let Some(idx) = x.as_old_index() {
							match (a.get(idx), b.get(idx)) {
								(Some(a), Some(b)) => a.compare(b, path.next(), collate, numeric),
								(Some(_), None) => Some(Ordering::Greater),
								(None, Some(_)) => Some(Ordering::Less),
								(_, _) => Some(Ordering::Equal),
							}
						} else {
							for (a, b) in a.iter().zip(b.iter()) {
								match a.compare(b, path, collate, numeric) {
									Some(Ordering::Equal) => continue,
									None => continue,
									o => return o,
								}
							}
							Some(a.len().cmp(&b.len()))
						}
					}
				},
				// Ignore everything else
				(a, b) => a.compare(b, path.next(), collate, numeric),
			},
			// No more parts so get the value
			None => match (collate, numeric) {
				(true, true) => self.natural_lexical_cmp(other),
				(true, false) => self.lexical_cmp(other),
				(false, true) => self.natural_cmp(other),
				_ => self.partial_cmp(other),
			},
		}
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::expr::idiom::Idiom;
	use crate::syn;

	#[test]
	fn compare_none() {
		let idi: Idiom = Default::default();
		let one = syn::value("{ test: { other: null, something: 456 } }").unwrap();
		let two = syn::value("{ test: { other: null, something: 123 } }").unwrap();
		let res = one.compare(&two, &idi, false, false);
		assert_eq!(res, Some(Ordering::Greater));
	}

	#[test]
	fn compare_basic() {
		let idi: Idiom = syn::idiom("test.something").unwrap().into();
		let one = syn::value("{ test: { other: null, something: 456 } }").unwrap();
		let two = syn::value("{ test: { other: null, something: 123 } }").unwrap();
		let res = one.compare(&two, &idi, false, false);
		assert_eq!(res, Some(Ordering::Greater));
	}

	#[test]
	fn compare_basic_missing_left() {
		let idi: Idiom = syn::idiom("test.something").unwrap().into();
		let one = syn::value("{ test: { other: null } }").unwrap();
		let two = syn::value("{ test: { other: null, something: 123 } }").unwrap();
		let res = one.compare(&two, &idi, false, false);
		assert_eq!(res, Some(Ordering::Less));
	}

	#[test]
	fn compare_basic_missing_right() {
		let idi: Idiom = syn::idiom("test.something").unwrap().into();
		let one = syn::value("{ test: { other: null, something: 456 } }").unwrap();
		let two = syn::value("{ test: { other: null } }").unwrap();
		let res = one.compare(&two, &idi, false, false);
		assert_eq!(res, Some(Ordering::Greater));
	}

	#[test]
	fn compare_array() {
		let idi: Idiom = syn::idiom("test.something.*").unwrap().into();
		let one = syn::value("{ test: { other: null, something: [4, 5, 6] } }").unwrap();
		let two = syn::value("{ test: { other: null, something: [1, 2, 3] } }").unwrap();
		let res = one.compare(&two, &idi, false, false);
		assert_eq!(res, Some(Ordering::Greater));
	}

	#[test]
	fn compare_array_longer_left() {
		let idi: Idiom = syn::idiom("test.something.*").unwrap().into();
		let one = syn::value("{ test: { other: null, something: [1, 2, 3, 4, 5, 6] } }").unwrap();
		let two = syn::value("{ test: { other: null, something: [1, 2, 3] } }").unwrap();
		let res = one.compare(&two, &idi, false, false);
		assert_eq!(res, Some(Ordering::Greater));
	}

	#[test]
	fn compare_array_longer_right() {
		let idi: Idiom = syn::idiom("test.something.*").unwrap().into();
		let one = syn::value("{ test: { other: null, something: [1, 2, 3] } }").unwrap();
		let two = syn::value("{ test: { other: null, something: [1, 2, 3, 4, 5, 6] } }").unwrap();
		let res = one.compare(&two, &idi, false, false);
		assert_eq!(res, Some(Ordering::Less));
	}

	#[test]
	fn compare_array_missing_left() {
		let idi: Idiom = syn::idiom("test.something.*").unwrap().into();
		let one = syn::value("{ test: { other: null, something: null } }").unwrap();
		let two = syn::value("{ test: { other: null, something: [1, 2, 3] } }").unwrap();
		let res = one.compare(&two, &idi, false, false);
		assert_eq!(res, Some(Ordering::Less));
	}

	#[test]
	fn compare_array_missing_right() {
		let idi: Idiom = syn::idiom("test.something.*").unwrap().into();
		let one = syn::value("{ test: { other: null, something: [4, 5, 6] } }").unwrap();
		let two = syn::value("{ test: { other: null, something: null } }").unwrap();
		let res = one.compare(&two, &idi, false, false);
		assert_eq!(res, Some(Ordering::Greater));
	}

	#[test]
	fn compare_array_missing_value_left() {
		let idi: Idiom = syn::idiom("test.something.*").unwrap().into();
		let one = syn::value("{ test: { other: null, something: [1, null, 3] } }").unwrap();
		let two = syn::value("{ test: { other: null, something: [1, 2, 3] } }").unwrap();
		let res = one.compare(&two, &idi, false, false);
		assert_eq!(res, Some(Ordering::Less));
	}

	#[test]
	fn compare_array_missing_value_right() {
		let idi: Idiom = syn::idiom("test.something.*").unwrap().into();
		let one = syn::value("{ test: { other: null, something: [1, 2, 3] } }").unwrap();
		let two = syn::value("{ test: { other: null, something: [1, null, 3] } }").unwrap();
		let res = one.compare(&two, &idi, false, false);
		assert_eq!(res, Some(Ordering::Greater));
	}

	#[test]
	fn compare_last() {
		let idi: Idiom = syn::idiom("test[$]").unwrap().into();
		let one = syn::value("{ test: [1,5] }").unwrap();
		let two = syn::value("{ test: [2,4] }").unwrap();
		let res = one.compare(&two, &idi, false, false);
		assert_eq!(res, Some(Ordering::Greater))
	}
}
