use crate::expr::part::{Next, Part};
use crate::expr::{Expr, Literal};
use crate::val::Value;
use std::cmp::Ordering;

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
					Part::Field(f) => match (a.get(f.as_str()), b.get(f.as_str())) {
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
					//TODO: It is kind of weird that a[1] works but `a[+(1)]` or `let $b = 1; a[$b]` for example doesn't as
					Part::Value(Expr::Literal(l)) => {
						let idx = match l {
							//TODO: Improve this, this is just replicating previous behavior but
							//decimal > usize::MAX resulting in a comparision between index 0 is
							//strange behaviour.
							Literal::Float(x) => *x as usize,
							Literal::Integer(x) => *x as usize,
							Literal::Decimal(x) => x.try_into().unwrap_or_default(),
							_ => {
								for (a, b) in a.iter().zip(b.iter()) {
									match a.compare(b, path, collate, numeric) {
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
						};
						match (a.get(idx), b.get(idx)) {
							(Some(a), Some(b)) => a.compare(b, path.next(), collate, numeric),
							(Some(_), None) => Some(Ordering::Greater),
							(None, Some(_)) => Some(Ordering::Less),
							(_, _) => Some(Ordering::Equal),
						}
					}
					_ => {
						for (a, b) in a.iter().zip(b.iter()) {
							match a.compare(b, path, collate, numeric) {
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

/*
#[cfg(test)]
mod tests {

	use super::*;
	use crate::expr::idiom::Idiom;
	use crate::sql::SqlValue;
	use crate::sql::idiom::Idiom as SqlIdiom;
	use crate::syn::Parse;

	#[test]
	fn compare_none() {
		let idi: Idiom = SqlIdiom::default().into();
		let one: Value = SqlValue::parse("{ test: { other: null, something: 456 } }").into();
		let two: Value = SqlValue::parse("{ test: { other: null, something: 123 } }").into();
		let res = one.compare(&two, &idi, false, false);
		assert_eq!(res, Some(Ordering::Greater));
	}

	#[test]
	fn compare_basic() {
		let idi: Idiom = SqlIdiom::parse("test.something").into();
		let one: Value = SqlValue::parse("{ test: { other: null, something: 456 } }").into();
		let two: Value = SqlValue::parse("{ test: { other: null, something: 123 } }").into();
		let res = one.compare(&two, &idi, false, false);
		assert_eq!(res, Some(Ordering::Greater));
	}

	#[test]
	fn compare_basic_missing_left() {
		let idi: Idiom = SqlIdiom::parse("test.something").into();
		let one: Value = SqlValue::parse("{ test: { other: null } }").into();
		let two: Value = SqlValue::parse("{ test: { other: null, something: 123 } }").into();
		let res = one.compare(&two, &idi, false, false);
		assert_eq!(res, Some(Ordering::Less));
	}

	#[test]
	fn compare_basic_missing_right() {
		let idi: Idiom = SqlIdiom::parse("test.something").into();
		let one: Value = SqlValue::parse("{ test: { other: null, something: 456 } }").into();
		let two: Value = SqlValue::parse("{ test: { other: null } }").into();
		let res = one.compare(&two, &idi, false, false);
		assert_eq!(res, Some(Ordering::Greater));
	}

	#[test]
	fn compare_array() {
		let idi: Idiom = SqlIdiom::parse("test.something.*").into();
		let one: Value = SqlValue::parse("{ test: { other: null, something: [4, 5, 6] } }").into();
		let two: Value = SqlValue::parse("{ test: { other: null, something: [1, 2, 3] } }").into();
		let res = one.compare(&two, &idi, false, false);
		assert_eq!(res, Some(Ordering::Greater));
	}

	#[test]
	fn compare_array_longer_left() {
		let idi: Idiom = SqlIdiom::parse("test.something.*").into();
		let one: Value =
			SqlValue::parse("{ test: { other: null, something: [1, 2, 3, 4, 5, 6] } }").into();
		let two: Value = SqlValue::parse("{ test: { other: null, something: [1, 2, 3] } }").into();
		let res = one.compare(&two, &idi, false, false);
		assert_eq!(res, Some(Ordering::Greater));
	}

	#[test]
	fn compare_array_longer_right() {
		let idi: Idiom = SqlIdiom::parse("test.something.*").into();
		let one: Value = SqlValue::parse("{ test: { other: null, something: [1, 2, 3] } }").into();
		let two: Value =
			SqlValue::parse("{ test: { other: null, something: [1, 2, 3, 4, 5, 6] } }").into();
		let res = one.compare(&two, &idi, false, false);
		assert_eq!(res, Some(Ordering::Less));
	}

	#[test]
	fn compare_array_missing_left() {
		let idi: Idiom = SqlIdiom::parse("test.something.*").into();
		let one: Value = SqlValue::parse("{ test: { other: null, something: null } }").into();
		let two: Value = SqlValue::parse("{ test: { other: null, something: [1, 2, 3] } }").into();
		let res = one.compare(&two, &idi, false, false);
		assert_eq!(res, Some(Ordering::Less));
	}

	#[test]
	fn compare_array_missing_right() {
		let idi: Idiom = SqlIdiom::parse("test.something.*").into();
		let one: Value = SqlValue::parse("{ test: { other: null, something: [4, 5, 6] } }").into();
		let two: Value = SqlValue::parse("{ test: { other: null, something: null } }").into();
		let res = one.compare(&two, &idi, false, false);
		assert_eq!(res, Some(Ordering::Greater));
	}

	#[test]
	fn compare_array_missing_value_left() {
		let idi: Idiom = SqlIdiom::parse("test.something.*").into();
		let one: Value =
			SqlValue::parse("{ test: { other: null, something: [1, null, 3] } }").into();
		let two: Value = SqlValue::parse("{ test: { other: null, something: [1, 2, 3] } }").into();
		let res = one.compare(&two, &idi, false, false);
		assert_eq!(res, Some(Ordering::Less));
	}

	#[test]
	fn compare_array_missing_value_right() {
		let idi: Idiom = SqlIdiom::parse("test.something.*").into();
		let one: Value = SqlValue::parse("{ test: { other: null, something: [1, 2, 3] } }").into();
		let two: Value =
			SqlValue::parse("{ test: { other: null, something: [1, null, 3] } }").into();
		let res = one.compare(&two, &idi, false, false);
		assert_eq!(res, Some(Ordering::Greater));
	}

	#[test]
	fn compare_last() {
		let idi: Idiom = SqlIdiom::parse("test[$]").into();
		let one: Value = SqlValue::parse("{ test: [1,5] }").into();
		let two: Value = SqlValue::parse("{ test: [2,4] }").into();
		let res = one.compare(&two, &idi, false, false);
		assert_eq!(res, Some(Ordering::Greater))
	}
}
*/
