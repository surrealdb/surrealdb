//! Field path type for pure field extraction without execution.
//!
//! This module provides a validated subset of `Idiom` that guarantees
//! no execution is required for field extraction. This is used in contexts
//! like sorting where we need to extract values synchronously without
//! database access or expression evaluation.

use std::borrow::Cow;
use std::fmt;

use crate::val::{Set, Value};

/// A part of a field path that can be navigated without execution.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FieldPathPart {
	/// Field access: `.name`
	Field(String),
	/// Literal integer index: `[0]`, `[1]`
	Index(usize),
	/// First element: `[0]`
	First,
	/// Last element: `[$]`
	Last,
	/// Graph traversal key: `->table`, `<-table`, `<->table`
	Lookup(String),
}

/// A path for pure field extraction, with no execution required.
///
/// This is a validated subset of `Idiom` that only contains parts that can be
/// extracted synchronously from a Value without database access or expression
/// evaluation.
///
/// Supported patterns:
/// - `a` - simple field
/// - `a.b.c` - nested fields
/// - `a[0]` - array index
/// - `a[$]` - last element
/// - `a[0].b.c` - mixed
///
/// # Examples
///
/// ```ignore
/// use surrealdb::exec::FieldPath;
///
/// // Simple field
/// let path = FieldPath::field("name");
///
/// // Convert from idiom (may fail for complex idioms)
/// let idiom = syn::idiom("user.address.city").unwrap();
/// let path = FieldPath::try_from(&idiom)?;
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FieldPath(pub Vec<FieldPathPart>);

impl FieldPath {
	/// Create a simple single-field path.
	pub fn field(name: impl Into<String>) -> Self {
		FieldPath(vec![FieldPathPart::Field(name.into())])
	}

	/// Check if this is an empty path.
	pub fn is_empty(&self) -> bool {
		self.0.is_empty()
	}

	/// Get the number of parts in this path.
	pub fn len(&self) -> usize {
		self.0.len()
	}

	/// Extract the value at this path from a record.
	/// Returns Value::None if the path doesn't exist.
	///
	/// Borrows into the input wherever the path is pure navigation
	/// (object fields, array/set element access), so the common case —
	/// a sort key like `a.b.c` — never clones the record. An owned value
	/// is produced only for synthesised results (field projection over an
	/// array/set, missing paths) or when descending through one.
	pub fn extract<'a>(&self, value: &'a Value) -> Cow<'a, Value> {
		let mut current = Cow::Borrowed(value);
		for part in &self.0 {
			current = match current {
				Cow::Borrowed(v) => match step(v, part) {
					Step::Borrowed(b) => Cow::Borrowed(b),
					Step::Owned(o) => Cow::Owned(o),
				},
				// The intermediate is already owned (a synthesised
				// array/set projection), so a borrowed step result must be
				// cloned out of it before it drops. These intermediates are
				// path-local, never the whole record.
				Cow::Owned(v) => Cow::Owned(match step(&v, part) {
					Step::Borrowed(b) => b.clone(),
					Step::Owned(o) => o,
				}),
			};
		}
		current
	}
}

/// Result of navigating one [`FieldPathPart`] into a value: a borrow into
/// the input where possible, an owned value where the step synthesises one.
enum Step<'v> {
	Borrowed(&'v Value),
	Owned(Value),
}

/// Navigate a single path part. Pure-navigation arms borrow; projection
/// arms (field access over an array/set) and misses produce owned values.
fn step<'v>(value: &'v Value, part: &FieldPathPart) -> Step<'v> {
	match (value, part) {
		// Field/Lookup access on object
		(Value::Object(obj), FieldPathPart::Field(name) | FieldPathPart::Lookup(name)) => {
			obj.get(name).map_or(Step::Owned(Value::None), Step::Borrowed)
		}
		// Index access on array
		(Value::Array(arr), FieldPathPart::Index(i)) => {
			arr.get(*i).map_or(Step::Owned(Value::None), Step::Borrowed)
		}
		// Index access on set
		(Value::Set(set), FieldPathPart::Index(i)) => {
			set.nth(*i).map_or(Step::Owned(Value::None), Step::Borrowed)
		}
		// First element of array
		(Value::Array(arr), FieldPathPart::First) => {
			arr.first().map_or(Step::Owned(Value::None), Step::Borrowed)
		}
		// First element of set
		(Value::Set(set), FieldPathPart::First) => {
			set.first().map_or(Step::Owned(Value::None), Step::Borrowed)
		}
		// Last element of array
		(Value::Array(arr), FieldPathPart::Last) => {
			arr.last().map_or(Step::Owned(Value::None), Step::Borrowed)
		}
		// Last element of set
		(Value::Set(set), FieldPathPart::Last) => {
			set.last().map_or(Step::Owned(Value::None), Step::Borrowed)
		}
		// Field/Lookup access on array applies to each element
		(Value::Array(arr), FieldPathPart::Field(name) | FieldPathPart::Lookup(name)) => {
			Step::Owned(Value::Array(
				arr.iter()
					.map(|v| match v {
						Value::Object(obj) => obj.get(name).cloned().unwrap_or(Value::None),
						_ => Value::None,
					})
					.collect::<Vec<_>>()
					.into(),
			))
		}
		// Field/Lookup access on set applies to each element
		(Value::Set(set), FieldPathPart::Field(name) | FieldPathPart::Lookup(name)) => {
			Step::Owned(Value::Set(Set::from(
				set.iter()
					.map(|v| match v {
						Value::Object(obj) => obj.get(name).cloned().unwrap_or(Value::None),
						_ => Value::None,
					})
					.collect::<Vec<_>>(),
			)))
		}
		// Any other combination returns None
		_ => Step::Owned(Value::None),
	}
}

impl fmt::Display for FieldPath {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		for (i, part) in self.0.iter().enumerate() {
			match part {
				FieldPathPart::Field(name) if i == 0 => write!(f, "{}", name)?,
				FieldPathPart::Field(name) => write!(f, ".{}", name)?,
				FieldPathPart::Index(idx) => write!(f, "[{}]", idx)?,
				FieldPathPart::First => write!(f, "[0]")?,
				FieldPathPart::Last => write!(f, "[$]")?,
				FieldPathPart::Lookup(key) if i == 0 => write!(f, "{}", key)?,
				FieldPathPart::Lookup(key) => write!(f, ".{}", key)?,
			}
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;

	use super::*;
	use crate::val::Object;

	/// Helper to create an Object from key-value pairs
	fn make_obj(pairs: Vec<(&str, Value)>) -> Object {
		let map: BTreeMap<String, Value> =
			pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect();
		Object::from(map)
	}

	#[test]
	fn test_field_path_simple() {
		let path = FieldPath::field("name");
		assert_eq!(path.to_string(), "name");
		assert_eq!(path.len(), 1);
	}

	#[test]
	fn test_field_path_extract_simple() {
		let path = FieldPath::field("name");
		let obj = make_obj(vec![("name", Value::from("Alice"))]);
		let value = Value::Object(obj);

		let result = path.extract(&value);
		assert_eq!(*result, Value::from("Alice"));
		// Pure navigation must borrow, not clone.
		assert!(matches!(result, Cow::Borrowed(_)));
	}

	#[test]
	fn test_field_path_extract_nested() {
		// Create path: user.address.city
		let path = FieldPath(vec![
			FieldPathPart::Field("user".into()),
			FieldPathPart::Field("address".into()),
			FieldPathPart::Field("city".into()),
		]);

		// Create nested object: { user: { address: { city: "Austin" } } }
		let city_obj = make_obj(vec![("city", Value::from("Austin"))]);
		let address_obj = make_obj(vec![("address", Value::Object(city_obj))]);
		let user_obj = make_obj(vec![("user", Value::Object(address_obj))]);
		let value = Value::Object(user_obj);

		let result = path.extract(&value);
		assert_eq!(*result, Value::from("Austin"));
		// Nested object navigation stays borrowed end-to-end.
		assert!(matches!(result, Cow::Borrowed(_)));
	}

	#[test]
	fn test_field_path_extract_array_index() {
		// Create path: items[0]
		let path = FieldPath(vec![FieldPathPart::Field("items".into()), FieldPathPart::Index(0)]);

		let items = Value::Array(vec![Value::from("first"), Value::from("second")].into());
		let obj = make_obj(vec![("items", items)]);
		let value = Value::Object(obj);

		let result = path.extract(&value);
		assert_eq!(*result, Value::from("first"));
		assert!(matches!(result, Cow::Borrowed(_)));
	}

	#[test]
	fn test_field_path_extract_array_last() {
		// Create path: items[$]
		let path = FieldPath(vec![FieldPathPart::Field("items".into()), FieldPathPart::Last]);

		let items = Value::Array(vec![Value::from("first"), Value::from("second")].into());
		let obj = make_obj(vec![("items", items)]);
		let value = Value::Object(obj);

		let result = path.extract(&value);
		assert_eq!(*result, Value::from("second"));
		assert!(matches!(result, Cow::Borrowed(_)));
	}

	#[test]
	fn test_field_path_extract_missing() {
		let path = FieldPath::field("missing");
		let obj = make_obj(vec![("name", Value::from("Alice"))]);
		let value = Value::Object(obj);

		let result = path.extract(&value);
		assert_eq!(*result, Value::None);
		// A miss synthesises Value::None, so it is owned.
		assert!(matches!(result, Cow::Owned(_)));
	}

	#[test]
	fn test_field_path_extract_past_missing_stays_none() {
		// Navigating further parts after a miss keeps returning None,
		// exactly as the pre-Cow implementation did.
		let path = FieldPath(vec![
			FieldPathPart::Field("missing".into()),
			FieldPathPart::Field("deeper".into()),
			FieldPathPart::Index(3),
		]);
		let obj = make_obj(vec![("name", Value::from("Alice"))]);
		let value = Value::Object(obj);

		let result = path.extract(&value);
		assert_eq!(*result, Value::None);
		assert!(matches!(result, Cow::Owned(_)));
	}

	#[test]
	fn test_field_path_extract_field_on_array() {
		// Create path: users.name (should extract name from each user)
		let path = FieldPath(vec![
			FieldPathPart::Field("users".into()),
			FieldPathPart::Field("name".into()),
		]);

		let user1 = Value::Object(make_obj(vec![("name", Value::from("Alice"))]));
		let user2 = Value::Object(make_obj(vec![("name", Value::from("Bob"))]));
		let users = Value::Array(vec![user1, user2].into());
		let obj = make_obj(vec![("users", users)]);
		let value = Value::Object(obj);

		let result = path.extract(&value);
		// A projection over an array synthesises a new array, so it is owned.
		assert!(matches!(result, Cow::Owned(_)));
		if let Value::Array(arr) = result.into_owned() {
			assert_eq!(arr.len(), 2);
			assert_eq!(arr[0], Value::from("Alice"));
			assert_eq!(arr[1], Value::from("Bob"));
		} else {
			panic!("Expected array result");
		}
	}

	#[test]
	fn test_field_path_extract_through_owned_intermediate() {
		// users.name[0] — the projection synthesises an owned array, and the
		// subsequent index step must clone out of it correctly.
		let path = FieldPath(vec![
			FieldPathPart::Field("users".into()),
			FieldPathPart::Field("name".into()),
			FieldPathPart::Index(0),
		]);

		let user1 = Value::Object(make_obj(vec![("name", Value::from("Alice"))]));
		let user2 = Value::Object(make_obj(vec![("name", Value::from("Bob"))]));
		let users = Value::Array(vec![user1, user2].into());
		let obj = make_obj(vec![("users", users)]);
		let value = Value::Object(obj);

		let result = path.extract(&value);
		assert_eq!(*result, Value::from("Alice"));
		assert!(matches!(result, Cow::Owned(_)));
	}

	#[test]
	fn test_field_path_display() {
		let path = FieldPath(vec![
			FieldPathPart::Field("user".into()),
			FieldPathPart::Field("address".into()),
			FieldPathPart::Index(0),
			FieldPathPart::Field("city".into()),
		]);
		assert_eq!(path.to_string(), "user.address[0].city");
	}
}
