use std::fmt;

use crate::val::{Object, Strand, Value};
use revision::revisioned;
use serde::{Deserialize, Serialize};

#[derive(Debug)]
pub struct PatchError {
	message: String,
}

impl fmt::Display for PatchError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "Failed to parse JSON patch structure: {}", self.message)
	}
}

/// A type representing an delta change to a value.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[serde(tag = "op")]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
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
				.collect(),
	}

	pub fn into_object(self) -> Object{
		match self{
    Operation::Add { path, value } => {
		map!{
			// safety: does not contain null bytes.
			"op".to_owned() => Value::Strand(unsafe{ Strand::new_unchecked("add".to_owned()) }),
			// TODO: Ensure null byte correctness
			"path".to_owned() => Value::Strand(unsafe{ Strand::new_unchecked(path.join(".")) }),
			"value".to_owned() => value,

		}
	},
    Operation::Remove { path } => todo!(),
    Operation::Replace { path, value } => {
		map!{
			// safety: does not contain null bytes.
			"op".to_owned() => Value::Strand(unsafe{ Strand::new_unchecked("replace".to_owned()) }),
			// TODO: Ensure null byte correctness
			"path".to_owned() => Value::Strand(unsafe{ Strand::new_unchecked(path.join(".")) }),

			"value".to_owned() => value,
		}
	},
    Operation::Change { path, value } => {
		map!{
			// safety: does not contain null bytes.
			"op".to_owned() => Value::Strand(unsafe{ Strand::new_unchecked("replace".to_owned()) }),
			// TODO: Ensure null byte correctness
			"path".to_owned() => Value::Strand(unsafe{ Strand::new_unchecked(path.join(".")) }),
			"value".to_owned() => value,

		}
	}
    Operation::Copy { path, from } => todo!(),
    Operation::Move { path, from } => todo!(),
    Operation::Test { path, value } => todo!(),
}
	}


	/// Returns the operaton encoded in the object, or an error if the object does not contain a
	/// valid operation.
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

		match &**op {
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
}
