use crate::err::Error;
use crate::sql::value::Value;

impl Value {
	pub(crate) fn merge(&mut self, val: Value) -> Result<(), Error> {
		// If this value is not an object, then error
		if !val.is_object() {
			return Err(Error::InvalidMerge {
				value: val,
			});
		}
		// Otherwise loop through every object field
		for k in val.every(None, false, false).iter() {
			match val.pick(k) {
				Value::None => self.cut(k),
				v => self.put(k, v),
			}
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::syn::Parse;

	#[tokio::test]
	async fn merge_none() {
		let mut res = Value::parse(
			"{
				test: true,
				name: {
					first: 'Tobie',
					last: 'Morgan Hitchcock',
					initials: 'TMH',
				},
			}",
		);
		let none = Value::None;
		match res.merge(none.clone()).unwrap_err() {
			Error::InvalidMerge {
				value,
			} => assert_eq!(value, none),
			error => panic!("unexpected error: {error:?}"),
		}
	}

	#[tokio::test]
	async fn merge_empty() {
		let mut res = Value::parse(
			"{
				test: true,
				name: {
					first: 'Tobie',
					last: 'Morgan Hitchcock',
					initials: 'TMH',
				},
			}",
		);
		let val = Value::parse(
			"{
				test: true,
				name: {
					first: 'Tobie',
					last: 'Morgan Hitchcock',
					initials: 'TMH',
				},
			}",
		);
		let mrg = Value::Object(Default::default());
		res.merge(mrg).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn merge_basic() {
		let mut res = Value::parse(
			"{
				test: true,
				name: {
					first: 'Tobie',
					last: 'Morgan Hitchcock',
					initials: 'TMH',
				},
			}",
		);
		let mrg = Value::parse(
			"{
				name: {
					title: 'Mr',
					initials: NONE,
				},
				tags: ['Rust', 'Golang', 'JavaScript'],
			}",
		);
		let val = Value::parse(
			"{
				test: true,
				name: {
					title: 'Mr',
					first: 'Tobie',
					last: 'Morgan Hitchcock',
				},
				tags: ['Rust', 'Golang', 'JavaScript'],
			}",
		);
		res.merge(mrg).unwrap();
		assert_eq!(res, val);
	}
}
