use anyhow::{Result, bail, ensure};
use surrealdb_types::ToSql;

use crate::expr::operation::PatchError;
use crate::expr::{Error, Operation};
use crate::val::{Set, Strand, Value};

/// What a write does with the position its pointer's last reference token
/// names inside an array.
#[derive(Clone, Copy)]
enum Write {
	/// The value takes that position and the elements from it onwards move up
	/// one. RFC 6902 §4.1 gives this to `add`, and §4.4 and §4.5 define the
	/// destination of `move` and `copy` as an `add`, so all three share it.
	Insert,
	/// The value replaces the element occupying that position, which is what
	/// `replace` and `change` mean.
	Overwrite,
}

/// The one location a JSON Pointer's reference tokens lead to.
enum Located<'a> {
	/// The value stored there.
	Found(&'a mut Value),
	/// Nothing along the pointer holds a value yet.
	Absent,
	/// Every element of a sequence answers to the pointer, because a token
	/// naming no position was followed through one. Such a pointer describes a
	/// value in each element rather than one location, so a position counted
	/// against it names something different in every element.
	Many,
}

/// Follows `path` one container at a time to the single location it names.
///
/// Only steps whose meaning is unambiguous are taken: the member of an object,
/// and the element at a position in an array. Reaching a sequence by a token
/// that names no position stops the walk, because from there the pointer
/// spreads across every element — the difference between selecting an element
/// and broadcasting over all of them, which is what the untyped write path
/// does with the same tokens.
fn locate<'a>(this: &'a mut Value, path: &[Strand]) -> Located<'a> {
	let mut at = this;
	for token in path {
		at = match at {
			Value::Object(o) => match o.get_mut(token.as_str()) {
				Some(v) => v,
				None => return Located::Absent,
			},
			Value::Array(a) => match Operation::array_index(token.as_str()) {
				Some(i) => match a.get_mut(i) {
					Some(v) => v,
					None => return Located::Absent,
				},
				None => return Located::Many,
			},
			Value::Set(_) => return Located::Many,
			_ => return Located::Absent,
		};
	}
	Located::Found(at)
}

/// Restores the sort-and-dedup invariant of every set in `value`.
///
/// A write reaches a set element by the position it happens to sort into and
/// leaves it where it was, so the set can come out unordered or holding two
/// equal elements. Storage encodes a set as an indexed set and will not decode
/// that shape, which costs the whole record rather than the one field, so the
/// invariant is re-established over the finished value before it is committed.
/// Sets nested inside an element are rebuilt first, since an element's own
/// ordering decides where it sorts in the set holding it.
fn normalize_sets(value: &mut Value) {
	match value {
		Value::Set(s) => {
			let mut items = Vec::from(std::mem::take(s));
			items.iter_mut().for_each(normalize_sets);
			*s = Set::from(items);
		}
		Value::Array(a) => a.iter_mut().for_each(normalize_sets),
		Value::Object(o) => o.iter_mut().for_each(|(_, v)| normalize_sets(v)),
		_ => (),
	}
}

/// Writes `value` at the location `path` points at.
///
/// The last reference token is resolved against whatever holds the parent
/// location, because only that says what the token names. RFC 6901 §4 spells
/// a position in an array as `0` or `[1-9][0-9]*`, and RFC 6902 adds `-` for
/// the position one past the last element; against anything else — an object,
/// or a location that holds nothing yet — every token is a member name.
///
/// A position must name somewhere the value can land, so it is bounded by the
/// array: one past the last element for an insert, which is the append `-`
/// also spells, and the last element itself for an overwrite. Writing outside
/// that is refused rather than dropped, because a pointer that reaches no
/// element would otherwise report success having changed nothing.
///
/// A position is refused outright when the parent pointer reaches every
/// element of a sequence instead of one location: counting a position against
/// each element in turn writes the same value into all of them, which is not
/// what the pointer asks for.
fn write(this: &mut Value, path: &[Strand], value: Value, mode: Write) -> Result<()> {
	let Some((last, left)) = path.split_last() else {
		*this = value;
		return Ok(());
	};
	// A token only needs the container to disambiguate it when it could name a
	// position at all; `-` names the position past the last element.
	let position = match last.as_str() {
		"-" => Some(None),
		token => Operation::array_index(token).map(Some),
	};
	if let Some(index) = position {
		match locate(this, left) {
			Located::Found(Value::Array(v)) => {
				let len = v.len();
				let out_of_bounds = || {
					Error::InvalidPatch(PatchError {
						message: format!(
							"index {at} is out of bounds for array of length {len}",
							at = index.unwrap_or(len),
						),
					})
				};
				match mode {
					Write::Insert => {
						let at = index.unwrap_or(len);
						if at > len {
							bail!(out_of_bounds());
						}
						v.insert(at, value);
					}
					Write::Overwrite => {
						let Some(slot) = index.and_then(|at| v.get_mut(at)) else {
							bail!(out_of_bounds());
						};
						*slot = value;
					}
				}
				return Ok(());
			}
			Located::Many => {
				bail!(Error::InvalidPatch(PatchError {
					message: format!(
						"'{last}' names a position, but the path before it reaches every element of a sequence rather than one of them",
					),
				}))
			}
			// The parent holds no array, so the token is a member name.
			Located::Found(_) | Located::Absent => (),
		}
	}
	this.put(&Operation::path_to_parts(path), value);
	Ok(())
}

impl Value {
	pub fn patch(&mut self, ops: Value) -> Result<()> {
		let mut this = self.clone();
		let operations = Operation::value_to_operations(ops)
			.map_err(Error::InvalidPatch)
			.map_err(anyhow::Error::new)?;

		// SECURITY: applying an operation walks `Value::put` / `cut` / `pick` once
		// per path segment, and those are natively recursive with no `Stk`. A
		// JSON-Pointer path is split from an arbitrary string with no length
		// bound, so a path of a few thousand segments overflows the host stack and
		// aborts the process (`panic = "abort"`) while the value is being built -
		// before `check_value_depth`, which inspects a finished value, could ever
		// see it. Bound the path to the value depth cap here, matching the depth a
		// patch could otherwise build, so an over-long path is rejected up front.
		for op in &operations {
			ensure!(
				op.max_path_len() <= crate::val::MAX_VALUE_DEPTH,
				Error::ValueTooDeep {
					max: crate::val::MAX_VALUE_DEPTH,
				}
			);
		}

		// Loop over the patch operations and apply them
		for operation in operations {
			match operation {
				// Add a value
				Operation::Add {
					path,
					value,
				} => {
					// A last token naming a position points inside a sequence.
					// Anything else points at the location itself, and one
					// holding an array is appended to rather than replaced:
					// this dialect reads `add` at an array-valued field as a
					// push.
					let names_position = path.last().is_some_and(|last| {
						last.as_str() == "-" || Operation::array_index(last.as_str()).is_some()
					});
					if !names_position
						&& matches!(locate(&mut this, &path), Located::Found(Value::Array(_)))
					{
						this.inc(&Operation::path_to_parts(&path), value)?;
					} else {
						write(&mut this, &path, value, Write::Insert)?;
					}
				}
				// Remove a value at the specified path
				Operation::Remove {
					path,
				} => {
					let path = Operation::path_to_parts(&path);
					this.cut(&path);
				}
				// Replace a value at the specified path
				Operation::Replace {
					path,
					value,
				} => write(&mut this, &path, value, Write::Overwrite)?,
				// Modify a string at the specified path
				Operation::Change {
					path,
					value,
				} => {
					let parts = Operation::path_to_parts(&path);
					if let Value::String(p) = value
						&& let Value::String(v) = this.pick(&parts)
					{
						let dmp = dmp::new();
						let pch = dmp.patch_from_text(p.into_string()).map_err(|e| {
							Error::InvalidPatch(PatchError {
								message: format!("{e:?}"),
							})
						})?;
						let (txt, _) = dmp.patch_apply(&pch, v.as_str()).map_err(|e| {
							Error::InvalidPatch(PatchError {
								message: format!("{e:?}"),
							})
						})?;
						let txt = txt.into_iter().collect::<String>();
						write(&mut this, &path, Value::from(txt), Write::Overwrite)?;
					}
				}
				// Copy a value from one field to another
				Operation::Copy {
					path,
					from,
				} => {
					let val = this.pick(&Operation::path_to_parts(&from));
					write(&mut this, &path, val, Write::Insert)?;
				}
				// Move a value from one field to another
				Operation::Move {
					path,
					from,
				} => {
					// RFC 6902 §4.4 defines a `move` as a `remove` at `from`
					// followed by an `add` at `path`. The source goes first so
					// that a destination inside the same array counts positions
					// in the array the removal left behind.
					let from = Operation::path_to_parts(&from);
					let val = this.pick(&from);
					this.cut(&from);
					write(&mut this, &path, val, Write::Insert)?;
				}
				// Test whether a value matches another value
				Operation::Test {
					path,
					value,
				} => {
					let path = Operation::path_to_parts(&path);
					let val = this.pick(&path);
					ensure!(
						value == val,
						Error::PatchTest {
							expected: value.to_sql(),
							got: val.to_sql(),
						}
					);
				}
			}
		}
		normalize_sets(&mut this);
		*self = this;
		// Everything ok
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use crate::expr::Operation;
	use crate::syn;

	macro_rules! parse_val {
		($input:expr) => {
			crate::val::convert_public_value_to_internal(syn::value($input).unwrap())
		};
	}

	#[tokio::test]
	async fn patch_add_simple() {
		let mut val = parse_val!("{ test: { other: null, something: 123 } }");
		let ops = parse_val!("[{ op: 'add', path: '/temp', value: true }]");
		let res = parse_val!("{ test: { other: null, something: 123 }, temp: true }");
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	// Applying a patch op walks `put` / `cut` / `pick` once per path segment, so a
	// JSON-Pointer path longer than the value depth cap is rejected before it can
	// overflow the stack. The path is split from an arbitrary string, so its
	// length is caller-controlled.
	#[tokio::test]
	async fn patch_rejects_path_past_depth_cap() {
		use crate::val::MAX_VALUE_DEPTH;
		let ops = parse_val!(&format!(
			"[{{ op: 'add', path: '{}', value: 1 }}]",
			"/a".repeat(MAX_VALUE_DEPTH + 1)
		));
		let mut val = parse_val!("{ existing: 1 }");
		let before = val.clone();
		let err = val.patch(ops).unwrap_err();
		assert!(err.to_string().contains("nested too deeply"), "unexpected error: {err}");
		// Failed closed: a rejected patch leaves the value untouched.
		assert_eq!(val, before);
	}

	// The `from` pointer of copy/move drives the same recursion through `pick`, so
	// it is bounded on the same footing as `path`.
	#[tokio::test]
	async fn patch_rejects_from_path_past_depth_cap() {
		use crate::val::MAX_VALUE_DEPTH;
		let ops = parse_val!(&format!(
			"[{{ op: 'copy', path: '/x', from: '{}' }}]",
			"/a".repeat(MAX_VALUE_DEPTH + 1)
		));
		let mut val = parse_val!("{ }");
		let err = val.patch(ops).unwrap_err();
		assert!(err.to_string().contains("nested too deeply"), "unexpected error: {err}");
	}

	// A path exactly at the cap still applies: its recursion is bounded and safe,
	// and the resulting value sits at the same depth the storage guard allows.
	#[tokio::test]
	async fn patch_accepts_path_at_depth_cap() {
		use crate::val::MAX_VALUE_DEPTH;
		let ops = parse_val!(&format!(
			"[{{ op: 'add', path: '{}', value: 1 }}]",
			"/a".repeat(MAX_VALUE_DEPTH)
		));
		let mut val = parse_val!("{ }");
		val.patch(ops).expect("a path at the cap must apply");
	}

	#[tokio::test]
	async fn patch_remove_simple() {
		let mut val = parse_val!("{ test: { other: null, something: 123 }, temp: true }");
		let ops = parse_val!("[{ op: 'remove', path: '/temp' }]");
		let res = parse_val!("{ test: { other: null, something: 123 } }");
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_replace_simple() {
		let mut val = parse_val!("{ test: { other: null, something: 123 }, temp: true }");
		let ops = parse_val!("[{ op: 'replace', path: '/temp', value: 'text' }]");
		let res = parse_val!("{ test: { other: null, something: 123 }, temp: 'text' }");
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_change_simple() {
		let mut val = parse_val!("{ test: { other: null, something: 123 }, temp: 'test' }");
		let ops = parse_val!(
			"[{ op: 'change', path: '/temp', value: '@@ -1,4 +1,4 @@\n te\n-s\n+x\n t\n' }]"
		);
		let res = parse_val!("{ test: { other: null, something: 123 }, temp: 'text' }");
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_copy_simple() {
		let mut val = parse_val!("{ test: 123, temp: true }");
		let ops = parse_val!("[{ op: 'copy', path: '/temp', from: '/test' }]");
		let res = parse_val!("{ test: 123, temp: 123 }");
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_move_simple() {
		let mut val = parse_val!("{ temp: true, some: 123 }");
		let ops = parse_val!("[{ op: 'move', path: '/other', from: '/temp' }]");
		let res = parse_val!("{ other: true, some: 123 }");
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_test_simple() {
		let mut val = parse_val!("{ test: { other: 'test', something: 123 }, temp: true }");
		let ops = parse_val!(
			"[{ op: 'remove', path: '/test/something' }, { op: 'test', path: '/temp', value: true }]"
		);
		let res = parse_val!("{ test: { other: 'test' }, temp: true }");
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_add_embedded() {
		let mut val = parse_val!("{ test: { other: null, something: 123 } }");
		let ops = parse_val!("[{ op: 'add', path: '/temp/test', value: true }]");
		let res = parse_val!("{ test: { other: null, something: 123 }, temp: { test: true } }");
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_remove_embedded() {
		let mut val = parse_val!("{ test: { other: null, something: 123 }, temp: true }");
		let ops = parse_val!("[{ op: 'remove', path: '/test/other' }]");
		let res = parse_val!("{ test: { something: 123 }, temp: true }");
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_remove_array_index() {
		let mut val = parse_val!("{ id: todo:1 }");
		let add = parse_val!("[{ op: 'add', path: '/list', value: ['Item here'] }]");
		let remove = parse_val!("[{ op: 'remove', path: '/list/0' }]");
		let res = parse_val!("{ id: todo:1, list: [] }");
		val.patch(add).unwrap();
		val.patch(remove).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_add_array_index_append_at_length() {
		// RFC 6902 §4.1: an index equal to the array length appends.
		let mut val = parse_val!("{ list: ['a', 'b'] }");
		let ops = parse_val!("[{ op: 'add', path: '/list/2', value: 'c' }]");
		let res = parse_val!("{ list: ['a', 'b', 'c'] }");
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_add_array_index_out_of_bounds_errors() {
		// RFC 6902 §4.1: an index greater than the array length is invalid.
		let mut val = parse_val!("{ list: ['a', 'b'] }");
		let ops = parse_val!("[{ op: 'add', path: '/list/5', value: 'c' }]");
		let err = val.patch(ops).unwrap_err();
		let msg = err.to_string();
		assert!(
			msg.contains("index 5 is out of bounds for array of length 2"),
			"unexpected error message: {msg}"
		);
		// The value must be unchanged when a patch op fails.
		assert_eq!(val, parse_val!("{ list: ['a', 'b'] }"));
	}

	#[tokio::test]
	async fn patch_replace_embedded() {
		let mut val = parse_val!("{ test: { other: null, something: 123 }, temp: true }");
		let ops = parse_val!("[{ op: 'replace', path: '/test/other', value: 'text' }]");
		let res = parse_val!("{ test: { other: 'text', something: 123 }, temp: true }");
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_change_embedded() {
		let mut val = parse_val!("{ test: { other: 'test', something: 123 }, temp: true }");
		let ops = parse_val!(
			"[{ op: 'change', path: '/test/other', value: '@@ -1,4 +1,4 @@\n te\n-s\n+x\n t\n' }]"
		);
		let res = parse_val!("{ test: { other: 'text', something: 123 }, temp: true }");
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_copy_embedded() {
		let mut val = parse_val!("{ test: { other: null }, temp: 123 }");
		let ops = parse_val!("[{ op: 'copy', path: '/test/other', from: '/temp' }]");
		let res = parse_val!("{ test: { other: 123 }, temp: 123 }");
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_move_embedded() {
		let mut val = parse_val!("{ test: { other: ':3', some: 123 }}");
		let ops = parse_val!("[{ op: 'move', path: '/temp', from: '/test/other' }]");
		let res = parse_val!("{ test: { some: 123 }, temp: ':3' }");
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_test_embedded() {
		let mut val = parse_val!("{ test: { other: 'test', something: 123 }, temp: true }");
		let ops = parse_val!(
			"[{ op: 'remove', path: '/test/other' }, { op: 'test', path: '/test/something', value: 123 }]"
		);
		let res = parse_val!("{ test: { something: 123 }, temp: true }");
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_change_invalid() {
		// See https://github.com/surrealdb/surrealdb/issues/2001
		let mut val = parse_val!("{ test: { other: 'test', something: 123 }, temp: true }");
		let ops = parse_val!("[{ op: 'change', path: '/test/other', value: 'text' }]");
		assert!(val.patch(ops).is_err());
	}

	#[tokio::test]
	async fn patch_test_invalid() {
		let mut val = parse_val!("{ test: { other: 'test', something: 123 }, temp: true }");
		let should = val.clone();
		let ops = parse_val!(
			"[{ op: 'remove', path: '/test/other' }, { op: 'test', path: '/test/something', value: 'not same' }]"
		);
		assert!(val.patch(ops).is_err());
		// It is important to test if patches applied even if test operation fails
		assert_eq!(val, should);
	}

	#[tokio::test]
	async fn patch_change_root() {
		// Issue 7239: empty-path patch ops must target the root value.
		let mut val = parse_val!("'Hello'");
		let ops = parse_val!(
			"[{ op: 'change', path: '', value: '@@ -1,5 +1,12 @@\n Hello\n+ there!\n' }]"
		);
		val.patch(ops).unwrap();
		assert_eq!(val, parse_val!("'Hello there!'"));
	}

	#[tokio::test]
	async fn patch_replace_root() {
		let mut val = parse_val!("1");
		let ops = parse_val!("[{ op: 'replace', path: '', value: 2 }]");
		val.patch(ops).unwrap();
		assert_eq!(val, parse_val!("2"));
	}

	#[tokio::test]
	async fn patch_diff_roundtrip_string_root() {
		let mut start = parse_val!("'Hello'");
		let after = parse_val!("'Hello there!'");
		let ops = Operation::operations_to_value(start.diff(&after));
		start.patch(ops).unwrap();
		assert_eq!(start, after);
	}

	#[tokio::test]
	async fn patch_replace_array_element_leaf() {
		let mut val = parse_val!("{ details: [{ isbn: 'a' }, { isbn: 'b' }] }");
		let ops = parse_val!("[{ op: 'replace', path: '/details/0/isbn', value: 'z' }]");
		let res = parse_val!("{ details: [{ isbn: 'z' }, { isbn: 'b' }] }");
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_replace_whole_array_element() {
		let mut val = parse_val!("{ details: [{ isbn: 'a' }, { isbn: 'b' }] }");
		let ops = parse_val!("[{ op: 'replace', path: '/details/1', value: 'gone' }]");
		let res = parse_val!("{ details: [{ isbn: 'a' }, 'gone'] }");
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_replace_nested_array_index() {
		let mut val = parse_val!("{ a: [[1, 2], [3, 4]] }");
		let ops = parse_val!("[{ op: 'replace', path: '/a/0/1', value: 99 }]");
		let res = parse_val!("{ a: [[1, 99], [3, 4]] }");
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_change_array_element() {
		let mut val = parse_val!("{ tags: ['test', 'keep'] }");
		let ops = parse_val!(
			"[{ op: 'change', path: '/tags/0', value: '@@ -1,4 +1,4 @@\n te\n-s\n+x\n t\n' }]"
		);
		let res = parse_val!("{ tags: ['text', 'keep'] }");
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_copy_from_array_element() {
		let mut val = parse_val!("{ list: ['p', 'q'] }");
		let ops = parse_val!("[{ op: 'copy', path: '/first', from: '/list/0' }]");
		let res = parse_val!("{ list: ['p', 'q'], first: 'p' }");
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_move_from_array_element() {
		let mut val = parse_val!("{ list: ['p', 'q'] }");
		let ops = parse_val!("[{ op: 'move', path: '/moved', from: '/list/0' }]");
		let res = parse_val!("{ list: ['q'], moved: 'p' }");
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_test_array_element() {
		let mut val = parse_val!("{ rows: [{ n: 1 }, { n: 2 }] }");
		let ops = parse_val!("[{ op: 'test', path: '/rows/1/n', value: 2 }]");
		val.patch(ops).unwrap();
		assert_eq!(val, parse_val!("{ rows: [{ n: 1 }, { n: 2 }] }"));

		let mut val = parse_val!("{ rows: [{ n: 1 }, { n: 2 }] }");
		let ops = parse_val!("[{ op: 'test', path: '/rows/1/n', value: 1 }]");
		assert!(val.patch(ops).is_err());
	}

	#[tokio::test]
	async fn patch_add_intermediate_array_index() {
		let mut val = parse_val!("{ details: [{ isbn: 'a' }, { isbn: 'b' }] }");
		let ops = parse_val!("[{ op: 'add', path: '/details/0/title', value: 'T' }]");
		let res = parse_val!("{ details: [{ isbn: 'a', title: 'T' }, { isbn: 'b' }] }");
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_add_array_index_under_array_index() {
		// The bounds check applies to the last token once the intermediate
		// index has selected the array it counts against.
		let mut val = parse_val!("{ a: [['x']] }");
		let ops = parse_val!("[{ op: 'add', path: '/a/0/1', value: 'y' }]");
		val.patch(ops).unwrap();
		assert_eq!(val, parse_val!("{ a: [['x', 'y']] }"));

		let mut val = parse_val!("{ a: [['x']] }");
		let ops = parse_val!("[{ op: 'add', path: '/a/0/5', value: 'y' }]");
		let err = val.patch(ops).unwrap_err();
		assert!(
			err.to_string().contains("index 5 is out of bounds for array of length 1"),
			"unexpected error message: {err}"
		);
		assert_eq!(val, parse_val!("{ a: [['x']] }"));
	}

	#[tokio::test]
	async fn patch_object_member_named_as_an_index() {
		// An index-spelled token names the member of that spelling on an
		// object, for every operation.
		let mut val = parse_val!("{ m: { '0': 'zero', '1': 'one' } }");
		let ops = parse_val!(
			"[{ op: 'replace', path: '/m/0', value: 'Z' }, { op: 'remove', path: '/m/1' }]"
		);
		let res = parse_val!("{ m: { '0': 'Z' } }");
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_object_member_not_spelled_as_an_index() {
		// `007`, `+3` and `-1` are member names, not indexes: RFC 6901 §4
		// spells an index as `0` or `[1-9][0-9]*` and nothing else.
		let mut val =
			parse_val!("{ m: { '007': 'bond', '7': 'seven', '+3': 'plus', '-1': 'neg' } }");
		let ops = parse_val!(
			"[{ op: 'remove', path: '/m/007' }, { op: 'remove', path: '/m/+3' }, { op: 'replace', path: '/m/-1', value: 'N' }]"
		);
		let res = parse_val!("{ m: { '7': 'seven', '-1': 'N' } }");
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_copy_into_array_position_inserts() {
		// RFC 6902 §4.5: a `copy` destination is an `add`, so a position in an
		// array is one the value takes rather than one it overwrites.
		let mut val = parse_val!("{ list: ['a', 'c'], v: 'b' }");
		let ops = parse_val!("[{ op: 'copy', path: '/list/1', from: '/v' }]");
		let res = parse_val!("{ list: ['a', 'b', 'c'], v: 'b' }");
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_move_within_one_array() {
		// The source is removed before the destination is written, so the
		// position counts against the array the removal left behind.
		let mut val = parse_val!("{ list: ['a', 'b', 'c'] }");
		let ops = parse_val!("[{ op: 'move', path: '/list/2', from: '/list/0' }]");
		let res = parse_val!("{ list: ['b', 'c', 'a'] }");
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_move_into_array_position_inserts() {
		let mut val = parse_val!("{ list: ['a', 'c'], v: 'b' }");
		let ops = parse_val!("[{ op: 'move', path: '/list/1', from: '/v' }]");
		let res = parse_val!("{ list: ['a', 'b', 'c'] }");
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_append_token_appends_or_names_a_member() {
		// `-` names the position past the last element of an array, for every
		// operation whose destination is an `add`.
		let mut val = parse_val!("{ list: ['a'], v: 'b' }");
		let ops = parse_val!(
			"[{ op: 'add', path: '/list/-', value: 'x' }, { op: 'copy', path: '/list/-', from: '/v' }]"
		);
		val.patch(ops).unwrap();
		assert_eq!(val, parse_val!("{ list: ['a', 'x', 'b'], v: 'b' }"));
		// Against an object it is an ordinary member name.
		let mut val = parse_val!("{ m: { a: 1 } }");
		let ops = parse_val!("[{ op: 'add', path: '/m/-', value: 2 }]");
		val.patch(ops).unwrap();
		assert_eq!(val, parse_val!("{ m: { a: 1, '-': 2 } }"));
	}

	#[tokio::test]
	async fn patch_overwrite_needs_a_position_that_exists() {
		// A `replace` reaching past the last element of an array changes
		// nothing, so it is refused rather than reported as applied.
		for pointer in ["/list/9", "/list/2", "/list/-"] {
			let mut val = parse_val!("{ list: ['a', 'b'] }");
			let ops = parse_val!(&format!("[{{ op: 'replace', path: '{pointer}', value: 'z' }}]"));
			let err = val.patch(ops).unwrap_err();
			assert!(
				err.to_string().contains("is out of bounds for array of length 2"),
				"unexpected error for {pointer}: {err}"
			);
			assert_eq!(val, parse_val!("{ list: ['a', 'b'] }"));
		}
	}

	#[tokio::test]
	async fn patch_add_index_on_a_non_array_names_a_member() {
		// The parent holds no array, so the token is a member name and the
		// parent keeps everything else it holds.
		let mut val = parse_val!("{ m: { x: 1 } }");
		let ops = parse_val!("[{ op: 'add', path: '/m/0', value: 5 }]");
		val.patch(ops).unwrap();
		assert_eq!(val, parse_val!("{ m: { x: 1, '0': 5 } }"));
	}

	#[tokio::test]
	async fn patch_set_keeps_its_invariant() {
		// A set is sorted and deduplicated. A write reaching one by position
		// lands on the element at that position and the set is rebuilt around
		// it, so what is stored is always a set again.
		let mut val = parse_val!("{ s: { 'b', 'c', } }");
		val.patch(parse_val!("[{ op: 'replace', path: '/s/0', value: 'z' }]")).unwrap();
		assert_eq!(val, parse_val!("{ s: { 'c', 'z', } }"));

		// A write that makes two elements equal leaves one of them.
		let mut val = parse_val!("{ s: { 'b', 'c', } }");
		val.patch(parse_val!("[{ op: 'replace', path: '/s/0', value: 'c' }]")).unwrap();
		assert_eq!(val, parse_val!("{ s: { 'c', } }"));

		// A pointer reaching through an element to a member of it re-sorts the
		// set around the element it changed.
		let mut val = parse_val!("{ s: { { n: 'zz' }, { n: 'aa' }, } }");
		val.patch(parse_val!("[{ op: 'replace', path: '/s/0/n', value: 'zz' }]")).unwrap();
		assert_eq!(val, parse_val!("{ s: { { n: 'zz' }, } }"));

		// A set nested inside an array inside a set is rebuilt too.
		let mut val = parse_val!("{ s: { [{ inner: { 'q', 'p', } }], } }");
		val.patch(parse_val!("[{ op: 'replace', path: '/s/0/0/inner', value: { 'y', 'x', } }]"))
			.unwrap();
		assert_eq!(val, parse_val!("{ s: { [{ inner: { 'x', 'y', } }], } }"));
	}

	#[tokio::test]
	async fn patch_position_under_a_broadcast_path_is_refused() {
		// `/list/name` reaches the `name` of every element rather than one
		// location, so a position counted against it names nothing to write.
		// Applying it to each element in turn would write the same value into
		// all of them, which is the corruption a position is meant to avoid.
		for op in [
			"{ op: 'replace', path: '/list/name/0', value: 'X' }",
			"{ op: 'add', path: '/list/name/0', value: 'X' }",
			"{ op: 'copy', path: '/list/name/0', from: '/v' }",
		] {
			let mut val = parse_val!("{ list: [{ name: 'AA' }, { name: 'BB' }], v: 'X' }");
			let err = val.patch(parse_val!(&format!("[{op}]"))).unwrap_err();
			assert!(
				err.to_string().contains("reaches every element of a sequence"),
				"unexpected error for {op}: {err}"
			);
			assert_eq!(val, parse_val!("{ list: [{ name: 'AA' }, { name: 'BB' }], v: 'X' }"));
		}
	}

	#[tokio::test]
	async fn patch_move_keeps_its_source_when_the_destination_is_refused() {
		// The source is removed before the destination is written, so a
		// destination that cannot be written must fail the whole patch rather
		// than report success having dropped the value.
		let mut val = parse_val!("{ list: [{ name: 'AA' }], v: 'keep' }");
		let ops = parse_val!("[{ op: 'move', path: '/list/name/0', from: '/v' }]");
		assert!(val.patch(ops).is_err());
		assert_eq!(val, parse_val!("{ list: [{ name: 'AA' }], v: 'keep' }"));
	}

	#[tokio::test]
	async fn patch_destination_inside_the_source_is_refused() {
		// The destination lies inside the value being read, so each application
		// would nest another snapshot of the source inside itself.
		for op in [
			"{ op: 'copy', path: '/a/-', from: '/a' }",
			"{ op: 'copy', path: '/a/0/b', from: '/a' }",
			"{ op: 'move', path: '/a/b', from: '/a' }",
		] {
			let mut val = parse_val!("{ a: [1, 2] }");
			let err = val.patch(parse_val!(&format!("[{op}]"))).unwrap_err();
			assert!(
				err.to_string().contains("that is not inside 'from'"),
				"unexpected error for {op}: {err}"
			);
			assert_eq!(val, parse_val!("{ a: [1, 2] }"));
		}
		// A pointer that merely shares a prefix is not inside the source.
		let mut val = parse_val!("{ a: [1, 2], ab: 'x' }");
		val.patch(parse_val!("[{ op: 'copy', path: '/ab', from: '/a' }]")).unwrap();
		assert_eq!(val, parse_val!("{ a: [1, 2], ab: [1, 2] }"));
	}

	#[tokio::test]
	async fn patch_diff_roundtrip_array_element() {
		for (a, b) in [
			("{ list: [1, 2, 3] }", "{ list: [1, 9, 3] }"),
			("{ list: ['aa', 'bb'] }", "{ list: ['aa', 'bx'] }"),
			("{ list: [{ n: 1 }, { n: 2 }] }", "{ list: [{ n: 1 }, { n: 7 }] }"),
			("{ list: [1] }", "{ list: [1, 2, 3] }"),
			("{ list: [1, 2, 3] }", "{ list: [1] }"),
			("{ list: [1, 2, 3, 4] }", "{ list: [9] }"),
			("{ list: [{ n: 1 }, { n: 2 }, { n: 3 }] }", "{ list: [{ n: 5 }] }"),
		] {
			let mut start = parse_val!(a);
			let after = parse_val!(b);
			let ops = Operation::operations_to_value(start.diff(&after));
			start.patch(ops).unwrap();
			assert_eq!(start, after, "diff of {a} -> {b} did not round-trip");
		}
	}

	#[tokio::test]
	async fn patch_diff_roundtrip_number_root() {
		let mut start = parse_val!("1");
		let after = parse_val!("2");
		let ops = Operation::operations_to_value(start.diff(&after));
		start.patch(ops).unwrap();
		assert_eq!(start, after);
	}
}
