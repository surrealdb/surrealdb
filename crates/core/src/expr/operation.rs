use std::fmt;

use revision::revisioned;

use crate::val::{Array, Object, Strand, Value};

#[derive(Debug)]
pub struct PatchError {
	pub message: String,
}

impl fmt::Display for PatchError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "Failed to parse JSON patch structure: {}", self.message)
	}
}

/// A type representing an delta change to a value.

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Operation {
	Add {
		path: Vec<String>,
		value: Value,
	},
	Remove {
		path: Vec<String>,
	},
	Replace {
		path: Vec<String>,
		value: Value,
	},
	Change {
		path: Vec<String>,
		value: Value,
	},
	Copy {
		path: Vec<String>,
		from: Vec<String>,
	},
	Move {
		path: Vec<String>,
		from: Vec<String>,
	},
	Test {
		path: Vec<String>,
		value: Value,
	},
}

impl Operation {
	fn value_to_jsonpath(val: &Value) -> Vec<String> {
		val.to_raw_string()
			.trim_start_matches('/')
			.split(&['.', '/'])
			.map(|x| x.to_owned())
			.collect()
	}

	pub fn into_object(self) -> Object {
		fn path_to_strand(p: &[String]) -> Value {
			let mut res = String::with_capacity(p.len() + p.iter().map(|x| x.len()).sum::<usize>());
			for p in p {
				res.push('/');
				res.push_str(p);
			}
			Strand::new(res).unwrap().into()
		}

		let res = match self {
			Operation::Add {
				path,
				value,
			} => {
				map! {
					"op".to_owned() => Value::Strand(strand!("add").to_owned()),
					// TODO: Ensure null byte correctness
					"path".to_owned() => path_to_strand(&path),
					"value".to_owned() => value,
				}
			}
			Operation::Remove {
				path,
			} => {
				map! {
					// safety: does not contain null bytes.
					"op".to_owned() => Value::Strand(strand!("remove").to_owned()),
					// TODO: Ensure null byte correctness
					"path".to_owned() => path_to_strand(&path),
				}
			}
			Operation::Replace {
				path,
				value,
			} => {
				map! {
					// safety: does not contain null bytes.
					"op".to_owned() => Value::Strand(strand!("replace").to_owned()),
					// TODO: Ensure null byte correctness
					"path".to_owned() => path_to_strand(&path),
					"value".to_owned() => value,
				}
			}
			Operation::Change {
				path,
				value,
			} => {
				map! {
					// safety: does not contain null bytes.
					"op".to_owned() => Value::Strand(strand!("change").to_owned()),
					// TODO: Ensure null byte correctness
					"path".to_owned() => path_to_strand(&path),
					"value".to_owned() => value,
				}
			}
			Operation::Copy {
				path,
				from,
			} => {
				map! {
					// safety: does not contain null bytes.
					"op".to_owned() => Value::Strand(strand!("copy").to_owned()),
					// TODO: Ensure null byte correctness
					"path".to_owned() => path_to_strand(&path),
					"from".to_owned() => path_to_strand(&from),
				}
			}
			Operation::Move {
				path,
				from,
			} => {
				map! {
					// safety: does not contain null bytes.
					"op".to_owned() => Value::Strand(strand!("map").to_owned()),
					// TODO: Ensure null byte correctness
					"path".to_owned() => path_to_strand(&path),
					"from".to_owned() => path_to_strand(&from),
				}
			}
			Operation::Test {
				path,
				value,
			} => {
				map! {
					// safety: does not contain null bytes.
					"op".to_owned() => Value::Strand(strand!("test").to_owned()),
					// TODO: Ensure null byte correctness
					"path".to_owned() => path_to_strand(&path),
					"value".to_owned() => value,
				}
			}
		};
		Object(res)
	}

	/// Returns the operaton encoded in the object, or an error if the object
	/// does not contain a valid operation.
	pub fn operation_from_object(object: Object) -> Result<Operation, PatchError> {
		let Some(op) = object.get("op") else {
			return Err(PatchError {
				message: "Key 'op' missing".to_owned(),
			});
		};

		let Value::Strand(op) = op else {
			return Err(PatchError {
				message: "Key 'op' not a string".to_owned(),
			});
		};

		let Some(path) = object.get("path") else {
			return Err(PatchError {
				message: "Key 'path' missing".to_owned(),
			});
		};

		let from = || {
			object.get("from").map(Operation::value_to_jsonpath).ok_or_else(|| PatchError {
				message: "Key 'from' missing".to_owned(),
			})
		};

		let value = || {
			object.get("value").cloned().ok_or_else(|| PatchError {
				message: "Key 'from' missing".to_owned(),
			})
		};

		let path = Operation::value_to_jsonpath(path);

		match op.as_str() {
			"add" => Ok(Operation::Add {
				path,
				value: value()?,
			}),
			"remove" => Ok(Operation::Remove {
				path,
			}),
			"replace" => Ok(Operation::Replace {
				path,
				value: value()?,
			}),
			"change" => Ok(Operation::Change {
				path,
				value: value()?,
			}),
			"copy" => Ok(Operation::Copy {
				path,
				from: from()?,
			}),
			"move" => Ok(Operation::Move {
				path,
				from: from()?,
			}),
			"test" => Ok(Operation::Test {
				path,
				value: value()?,
			}),

			x => Err(PatchError {
				message: format!("Invalid operation '{x}'"),
			}),
		}
	}

	/// Turns a value into a list of operations if the value has the right
	/// structure.
	pub fn value_to_operations(value: Value) -> Result<Vec<Operation>, PatchError> {
		let Value::Array(array) = value else {
			return Err(PatchError {
				message: "Patch operations should be an array of objects".to_owned(),
			});
		};

		let mut res = Vec::new();
		for o in array.into_iter() {
			let Value::Object(o) = o else {
				return Err(PatchError {
					message: "Patch operations should be an array of objects".to_owned(),
				});
			};
			res.push(Operation::operation_from_object(o)?)
		}
		Ok(res)
	}

	pub fn operations_to_value(operations: Vec<Operation>) -> Value {
		let array = operations.into_iter().map(|x| Value::Object(x.into_object())).collect();
		Value::Array(Array(array))
	}
}
