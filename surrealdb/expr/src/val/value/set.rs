use crate::val::field_path::{FieldPath, FieldPathPart};
use crate::val::{Object, Value};

impl Value {
	/// Synchronous method for setting a value at a FieldPath, consuming `val`.
	///
	/// Unlike `set()`, this doesn't require async/computation - just navigation.
	/// When encountering an array, applies the remaining path to all elements
	/// (cloning `val` for all-but-last and moving into the last). Otherwise
	/// `val` is moved straight to the leaf assignment. This is used for output
	/// path navigation in the projection operator, where each computed field
	/// value is produced fresh and would otherwise be cloned into the output.
	pub fn set_at_field_path_owned(&mut self, path: &FieldPath, val: Value) {
		self.set_at_field_path_owned_depth(path, val, 0);
	}

	fn set_at_field_path_owned_depth(&mut self, path: &FieldPath, val: Value, depth: usize) {
		if depth >= path.len() {
			*self = val;
			return;
		}

		let part = &path.0[depth];
		let is_last = depth == path.len() - 1;

		match (self, part) {
			// Object with Field/Lookup key
			(Value::Object(obj), FieldPathPart::Field(key) | FieldPathPart::Lookup(key)) => {
				if is_last {
					obj.insert(key.clone(), val);
				} else {
					let nested =
						obj.entry(key.clone()).or_insert_with(|| Value::Object(Object::default()));
					nested.set_at_field_path_owned_depth(path, val, depth + 1);
				}
			}
			// Index on array - must come before the catch-all array pattern
			(Value::Array(arr), FieldPathPart::Index(i)) if *i < arr.len() => {
				if is_last {
					arr[*i] = val;
				} else {
					arr[*i].set_at_field_path_owned_depth(path, val, depth + 1);
				}
			}
			// First element
			(Value::Array(arr), FieldPathPart::First) if !arr.is_empty() => {
				if is_last {
					arr[0] = val;
				} else {
					arr[0].set_at_field_path_owned_depth(path, val, depth + 1);
				}
			}
			// Last element
			(Value::Array(arr), FieldPathPart::Last) if !arr.is_empty() => {
				let last_idx = arr.len() - 1;
				if is_last {
					arr[last_idx] = val;
				} else {
					arr[last_idx].set_at_field_path_owned_depth(path, val, depth + 1);
				}
			}
			// Index on set - must come before the catch-all set pattern
			(Value::Set(set), FieldPathPart::Index(i)) if set.nth(*i).is_some() => {
				if let Some(elem) = set.nth_mut(*i) {
					if is_last {
						*elem = val;
					} else {
						elem.set_at_field_path_owned_depth(path, val, depth + 1);
					}
				}
			}
			// First element of set
			(Value::Set(set), FieldPathPart::First) if !set.is_empty() => {
				if let Some(elem) = set.first_mut() {
					if is_last {
						*elem = val;
					} else {
						elem.set_at_field_path_owned_depth(path, val, depth + 1);
					}
				}
			}
			// Last element of set
			(Value::Set(set), FieldPathPart::Last) if !set.is_empty() => {
				if let Some(elem) = set.last_mut() {
					if is_last {
						*elem = val;
					} else {
						elem.set_at_field_path_owned_depth(path, val, depth + 1);
					}
				}
			}
			// Array with Field/Lookup key: apply remaining path to all elements.
			// Clone `val` for all-but-last, move into the last element.
			(Value::Array(arr), FieldPathPart::Field(_) | FieldPathPart::Lookup(_)) => {
				if let Some((last, rest)) = arr.split_last_mut() {
					for elem in rest {
						elem.set_at_field_path_owned_depth(path, val.clone(), depth);
					}
					last.set_at_field_path_owned_depth(path, val, depth);
				}
			}
			// Non-object with Field/Lookup: replace with object containing the path
			(target, FieldPathPart::Field(key) | FieldPathPart::Lookup(key)) => {
				let mut obj = Object::default();
				if is_last {
					obj.insert(key.clone(), val);
				} else {
					let mut nested = Value::Object(Object::default());
					nested.set_at_field_path_owned_depth(path, val, depth + 1);
					obj.insert(key.clone(), nested);
				}
				*target = Value::Object(obj);
			}
			_ => {} // Other cases: no-op (val is dropped)
		}
	}
}
