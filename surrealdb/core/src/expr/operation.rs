use std::fmt;

use revision::revisioned;
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

use crate::val::{Array, Object, Value};

#[derive(Debug)]
pub(crate) struct PatchError {
	pub message: String,
}

impl fmt::Display for PatchError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "Failed to parse JSON patch structure: {}", self.message.to_sql())
	}
}

/// A type representing an delta change to a value.

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) enum Operation {
	Add {
		path: Vec<Strand>,
		value: Value,
	},
	Remove {
		path: Vec<Strand>,
	},
	Replace {
		path: Vec<Strand>,
		value: Value,
	},
	Change {
		path: Vec<Strand>,
		value: Value,
	},
	Copy {
		path: Vec<Strand>,
		from: Vec<Strand>,
	},
	Move {
		path: Vec<Strand>,
		from: Vec<Strand>,
	},
	Test {
		path: Vec<Strand>,
		value: Value,
	},
}

/// Reject a root `from` pointer for `copy` / `move`. Without this guard, an
/// empty `from` makes `Value::pick` return the entire current document, which
/// can copy field values past their SELECT permissions into an attacker-chosen
/// destination path.
fn reject_root_from(op: &str, from: Vec<Strand>) -> Result<Vec<Strand>, PatchError> {
	if from.is_empty() {
		Err(PatchError {
			message: format!("'{op}' operation requires a non-root 'from' pointer"),
		})
	} else {
		Ok(from)
	}
}

impl Operation {
	/// Converts a value to a JSON path.
	fn value_to_jsonpath(val: &Value) -> Vec<Strand> {
		// Per RFC 6901, an empty JSON Pointer ("") refers to the root document.
		// Splitting an empty string would produce `[Strand("")]` rather than the
		// empty `Vec` round-trip target of `path_to_jsonpath(&[])`.
		let raw = val.to_raw_string();
		let trimmed = raw.trim_start_matches('/');
		if trimmed.is_empty() {
			Vec::new()
		} else {
			trimmed.split(&['.', '/']).map(Strand::from).collect()
		}
	}

	/// Converts the operation to a JSON patch object.
	pub fn into_object(self) -> Object {
		// Converts a path to a JSON path
		fn path_to_jsonpath(p: &[Strand]) -> Value {
			let mut res = String::with_capacity(p.len() + p.iter().map(|x| x.len()).sum::<usize>());
			for p in p {
				res.push('/');
				res.push_str(p.as_str());
			}
			res.into()
		}
		// Return the JSON patch operation
		Object(match self {
			Operation::Add {
				path,
				value,
			} => {
				map! {
					"op".into() => Value::from("add"),
					"path".into() => path_to_jsonpath(&path),
					"value".into() => value,
				}
			}
			Operation::Remove {
				path,
			} => {
				map! {
					"op".into() => Value::from("remove"),
					"path".into() => path_to_jsonpath(&path),
				}
			}
			Operation::Replace {
				path,
				value,
			} => {
				map! {
					"op".into() => Value::from("replace"),
					"path".into() => path_to_jsonpath(&path),
					"value".into() => value,
				}
			}
			Operation::Change {
				path,
				value,
			} => {
				map! {
					"op".into() => Value::from("change"),
					"path".into() => path_to_jsonpath(&path),
					"value".into() => value,
				}
			}
			Operation::Copy {
				path,
				from,
			} => {
				map! {
					"op".into() => Value::from("copy"),
					"path".into() => path_to_jsonpath(&path),
					"from".into() => path_to_jsonpath(&from),
				}
			}
			Operation::Move {
				path,
				from,
			} => {
				map! {
					"op".into() => Value::from("move"),
					"path".into() => path_to_jsonpath(&path),
					"from".into() => path_to_jsonpath(&from),
				}
			}
			Operation::Test {
				path,
				value,
			} => {
				map! {
					"op".into() => Value::from("test"),
					"path".into() => path_to_jsonpath(&path),
					"value".into() => value,
				}
			}
		})
	}

	/// Returns the operaton encoded in the object, or an error if the object
	/// does not contain a valid operation.
	pub fn operation_from_object(object: &Object) -> Result<Operation, PatchError> {
		let Some(op) = object.get("op") else {
			return Err(PatchError {
				message: "Key 'op' missing".to_owned(),
			});
		};

		let Value::String(op) = op else {
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
			"copy" => {
				let from = reject_root_from("copy", from()?)?;
				Ok(Operation::Copy {
					path,
					from,
				})
			}
			"move" => {
				let from = reject_root_from("move", from()?)?;
				Ok(Operation::Move {
					path,
					from,
				})
			}
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
		for o in array {
			let Value::Object(o) = o else {
				return Err(PatchError {
					message: "Patch operations should be an array of objects".to_owned(),
				});
			};
			res.push(Operation::operation_from_object(&o)?)
		}
		Ok(res)
	}

	pub fn operations_to_value(operations: Vec<Operation>) -> Value {
		let array = operations.into_iter().map(|x| Value::Object(x.into_object())).collect();
		Value::Array(Array(array))
	}
}

impl ToSql for Operation {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.clone().into_object().fmt_sql(f, fmt);
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn roundtrip(op: &Operation) {
		let obj = op.clone().into_object();
		let decoded = Operation::operation_from_object(&obj)
			.expect("operation_from_object should accept into_object output");
		assert_eq!(*op, decoded);
	}

	#[test]
	fn round_trip_all_variants() {
		let path: Vec<Strand> = vec!["a".into(), "b".into()];
		let from: Vec<Strand> = vec!["c".into(), "d".into()];
		let value = Value::Bool(true);

		roundtrip(&Operation::Add {
			path: path.clone(),
			value: value.clone(),
		});
		roundtrip(&Operation::Remove {
			path: path.clone(),
		});
		roundtrip(&Operation::Replace {
			path: path.clone(),
			value: value.clone(),
		});
		roundtrip(&Operation::Change {
			path: path.clone(),
			value: value.clone(),
		});
		roundtrip(&Operation::Copy {
			path: path.clone(),
			from: from.clone(),
		});
		roundtrip(&Operation::Move {
			path: path.clone(),
			from,
		});
		roundtrip(&Operation::Test {
			path,
			value,
		});
	}

	#[test]
	fn round_trip_root_path() {
		// Empty path Vec must round-trip through the JSON Patch object form,
		// otherwise diff/patch silently no-ops on top-level scalars (issue 7239).
		let empty: Vec<Strand> = Vec::new();
		let value = Value::from("Hello there!");

		roundtrip(&Operation::Add {
			path: empty.clone(),
			value: value.clone(),
		});
		roundtrip(&Operation::Remove {
			path: empty.clone(),
		});
		roundtrip(&Operation::Replace {
			path: empty.clone(),
			value: value.clone(),
		});
		roundtrip(&Operation::Change {
			path: empty.clone(),
			value: value.clone(),
		});
		roundtrip(&Operation::Test {
			path: empty,
			value,
		});
	}
}
