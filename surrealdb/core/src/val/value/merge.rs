use anyhow::{Result, ensure};

use crate::err::Error;
use crate::val::Value;
use crate::val::value::every::ArrayBehaviour;

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
		for k in val.every(None, true, ArrayBehaviour::Ignore).iter() {
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

#[cfg(test)]
mod tests {

	use super::*;
	use crate::syn;

	macro_rules! parse_val {
		($input:expr) => {
			crate::val::convert_public_value_to_internal(syn::value($input).unwrap())
		};
	}

	#[tokio::test]
	async fn merge_none() {
		let mut res = parse_val!(
			"{
				test: true,
				name: {
					first: 'Tobie',
					last: 'Morgan Hitchcock',
					initials: 'TMH',
				},
			}"
		);
		match res.merge(Value::NONE.clone()).unwrap_err().downcast() {
			Ok(Error::InvalidMerge {
				value,
			}) => assert_eq!(value, Value::NONE),
			Ok(error) => panic!("unexpected error: {error:?}"),
			Err(error) => panic!("unexpected error: {error:?}"),
		}
	}

	#[tokio::test]
	async fn merge_empty() {
		let mut res = parse_val!(
			"{
				test: true,
				name: {
					first: 'Tobie',
					last: 'Morgan Hitchcock',
					initials: 'TMH',
				},
			}"
		);
		let val = parse_val!(
			"{
				test: true,
				name: {
					first: 'Tobie',
					last: 'Morgan Hitchcock',
					initials: 'TMH',
				},
			}"
		);
		let mrg = Value::Object(Default::default());
		res.merge(mrg).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn merge_basic() {
		let mut res = parse_val!(
			"{
				test: true,
				name: {
					first: 'Tobie',
					last: 'Morgan Hitchcock',
					initials: 'TMH',
				},
			}"
		);
		let mrg = parse_val!(
			"{
				name: {
					title: 'Mr',
					initials: NONE,
				},
				tags: ['Rust', 'Golang', 'JavaScript'],
			}"
		);
		let val = parse_val!(
			"{
				test: true,
				name: {
					title: 'Mr',
					first: 'Tobie',
					last: 'Morgan Hitchcock',
				},
				tags: ['Rust', 'Golang', 'JavaScript'],
			}"
		);
		res.merge(mrg).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn merge_new_object() {
		let mut res = parse_val!(
			"{
				test: true,
				name: 'Tobie',
				obj: {
					a: 1,
					b: 2,
				}
			}"
		);
		let mrg = parse_val!(
			"{
				name: {
					title: 'Mr',
					initials: NONE,
				},
				obj: {
					a: 2,
					b: NONE,
				}
			}"
		);
		let val = parse_val!(
			"{
				test: true,
				name: {
					title: 'Mr',
				},
				obj: {
					a: 2,
				},
			}"
		);
		res.merge(mrg).unwrap();
		assert_eq!(res, val);
	}
}
