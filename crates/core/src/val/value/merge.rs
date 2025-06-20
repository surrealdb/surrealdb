use crate::err::Error;
use crate::val::Value;
use anyhow::{Result, ensure};

impl Value {
	pub(crate) fn merge(&mut self, val: Value) -> Result<()> {
		// If this value is not an object, then error
		ensure!(
			val.is_object(),
			Error::InvalidMerge {
				value: val,
			}
		);
		// Otherwise loop through every object field
		for k in val.every(None, true, false).iter() {
			// Because we iterate every step, we need to this check
			// If old & new are both objects, we do not want to completely
			// replace the old object with the new object as that will drop
			// all the fields in the old object that are not in the new object
			let old = self.pick(k);
			let new = val.pick(k);
			if old.is_object() && new.is_object() {
				continue;
			}

			match new {
				Value::None => self.cut(k),
				v => self.put(k, v),
			}
		}
		Ok(())
	}
}

/*
#[cfg(test)]
mod tests {

	use super::*;
	use crate::sql::SqlValue;
	use crate::syn::Parse;

	#[tokio::test]
	async fn merge_none() {
		let mut res: Value = SqlValue::parse(
			"{
				test: true,
				name: {
					first: 'Tobie',
					last: 'Morgan Hitchcock',
					initials: 'TMH',
				},
			}",
		)
		.into();
		let none = Value::None;
		match res.merge(none.clone()).unwrap_err().downcast() {
			Ok(Error::InvalidMerge {
				value,
			}) => assert_eq!(value, none),
			Ok(error) => panic!("unexpected error: {error:?}"),
			Err(error) => panic!("unexpected error: {error:?}"),
		}
	}

	#[tokio::test]
	async fn merge_empty() {
		let mut res: Value = SqlValue::parse(
			"{
				test: true,
				name: {
					first: 'Tobie',
					last: 'Morgan Hitchcock',
					initials: 'TMH',
				},
			}",
		)
		.into();
		let val: Value = SqlValue::parse(
			"{
				test: true,
				name: {
					first: 'Tobie',
					last: 'Morgan Hitchcock',
					initials: 'TMH',
				},
			}",
		)
		.into();
		let mrg = Value::Object(Default::default());
		res.merge(mrg).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn merge_basic() {
		let mut res: Value = SqlValue::parse(
			"{
				test: true,
				name: {
					first: 'Tobie',
					last: 'Morgan Hitchcock',
					initials: 'TMH',
				},
			}",
		)
		.into();
		let mrg: Value = SqlValue::parse(
			"{
				name: {
					title: 'Mr',
					initials: NONE,
				},
				tags: ['Rust', 'Golang', 'JavaScript'],
			}",
		)
		.into();
		let val: Value = SqlValue::parse(
			"{
				test: true,
				name: {
					title: 'Mr',
					first: 'Tobie',
					last: 'Morgan Hitchcock',
				},
				tags: ['Rust', 'Golang', 'JavaScript'],
			}",
		)
		.into();
		res.merge(mrg).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn merge_new_object() {
		let mut res: Value = SqlValue::parse(
			"{
				test: true,
				name: 'Tobie',
				obj: {
					a: 1,
					b: 2,
				}
			}",
		)
		.into();
		let mrg: Value = SqlValue::parse(
			"{
				name: {
					title: 'Mr',
					initials: NONE,
				},
				obj: {
					a: 2,
					b: NONE,
				}
			}",
		)
		.into();
		let val: Value = SqlValue::parse(
			"{
				test: true,
				name: {
					title: 'Mr',
				},
				obj: {
					a: 2,
				},
			}",
		)
		.into();
		res.merge(mrg).unwrap();
		assert_eq!(res, val);
	}
}*/
