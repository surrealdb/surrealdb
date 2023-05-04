use crate::err::Error;
use crate::sql::value::Value;

impl Value {
	pub(crate) fn merge(&mut self, val: Value) -> Result<(), Error> {
		if val.is_object() {
			for k in val.every(None, false, false).iter() {
				match val.pick(&k.0) {
					Value::None => self.cut(&k.0),
					v => self.put(&k.0, v),
				}
			}
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::sql::test::Parse;

	#[tokio::test]
	async fn merge_none() {
		let mut res = Value::parse(
			"{
				name: {
					first: 'Tobie',
					last: 'Morgan Hitchcock',
					initials: 'TMH',
				},
			}",
		);
		let mrg = Value::None;
		let val = Value::parse(
			"{
				name: {
					first: 'Tobie',
					last: 'Morgan Hitchcock',
					initials: 'TMH',
				},
			}",
		);
		res.merge(mrg).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn merge_basic() {
		let mut res = Value::parse(
			"{
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
