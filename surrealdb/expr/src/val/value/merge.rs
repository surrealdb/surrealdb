use anyhow::{Result, bail};

use crate::expr::Error;
use crate::val::value::every::ArrayBehaviour;
use crate::val::{Object, Value};

impl Value {
	/// Merges the object `val` into `self`: a field of `val` replaces the field of `self`, unless
	/// both are objects, which merge in turn, and a field of `val` that is `NONE` removes the
	/// field. An array replaces whole, and a nested object that replaces a non-object arrives
	/// without its `NONE` fields.
	pub fn merge(&mut self, val: Value) -> Result<()> {
		let Value::Object(source) = val else {
			bail!(Error::InvalidMerge {
				value: val,
			});
		};
		match self {
			Value::Object(target) => merge_objects(target, source),
			other => other.merge_by_paths(&Value::Object(source)),
		}
		Ok(())
	}

	/// The merge of an object `val` into a value that is not one, field path by field path.
	fn merge_by_paths(&mut self, val: &Value) {
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
	}
}

/// Merges `source` into `target` field by field, moving the values of `source`.
fn merge_objects(target: &mut Object, source: Object) {
	for (key, value) in source {
		match value {
			Value::None => {
				target.remove(key.as_str());
			}
			Value::Object(mut nested) => match target.get_mut(key.as_str()) {
				Some(Value::Object(existing)) => merge_objects(existing, nested),
				_ => {
					remove_none_fields(&mut nested);
					target.insert(key, Value::Object(nested));
				}
			},
			value => {
				target.insert(key, value);
			}
		}
	}
}

/// Removes the `NONE` fields of `object`, at any depth of nested objects.
fn remove_none_fields(object: &mut Object) {
	object.retain(|_, value| !matches!(value, Value::None));
	for value in object.values_mut() {
		if let Value::Object(nested) = value {
			remove_none_fields(nested);
		}
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

	/// The merge of objects agrees with the merge of their field paths on every shape of overlap.
	#[tokio::test]
	async fn merge_agrees_with_the_path_merge() {
		let targets = [
			"{}",
			"{ id: 1 }",
			"{ a: 1, o: { x: 1, y: { z: 2 } }, arr: [1, 2], s: 'text' }",
			"{ o: 5, p: { q: [ { r: 1 } ] } }",
		];
		let sources = [
			"{}",
			"{ a: NONE }",
			"{ a: 2, b: 3 }",
			"{ o: { x: NONE, w: 4 } }",
			"{ o: { y: { z: NONE, k: { m: NONE, n: 1 } } } }",
			"{ o: {}, p: {} }",
			"{ o: 'scalar', arr: { k: NONE, v: 1 } }",
			"{ arr: [ { k: NONE } ], s: { t: { u: NONE } } }",
			"{ id: 2, nested: { a: { b: { c: NONE, d: [ NONE ] } } } }",
			"{ p: { q: { r: 2 } }, missing: NONE }",
		];
		for target in targets {
			for source in sources {
				let mut moved = parse_val!(target);
				let mut by_paths = parse_val!(target);
				moved.merge(parse_val!(source)).unwrap();
				by_paths.merge_by_paths(&parse_val!(source));
				assert_eq!(moved, by_paths, "merging {source} into {target}");
			}
		}
	}
}
