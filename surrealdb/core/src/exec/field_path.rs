//! Field path type for pure field extraction without execution.
//!
//! This module provides a validated subset of `Idiom` that guarantees
//! no execution is required for field extraction. This is used in contexts
//! like sorting where we need to extract values synchronously without
//! database access or expression evaluation.

use std::fmt;

use crate::err::Error;
use crate::expr::part::Part;
use crate::expr::{Expr, Idiom, Literal};
use crate::val::{Set, Value};

/// A part of a field path that can be navigated without execution.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FieldPathPart {
	/// Field access: `.name`
	Field(String),
	/// Literal integer index: `[0]`, `[1]`
	Index(usize),
	/// First element: `[0]` or `$first`
	First,
	/// Last element: `[$]` or `$last`
	Last,
	/// Graph traversal key: `->table` or `<-table`
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

impl TryFrom<&Idiom> for FieldPath {
	type Error = Error;

	fn try_from(idiom: &Idiom) -> Result<Self, Self::Error> {
		use surrealdb_types::ToSql;

		let mut parts = Vec::with_capacity(idiom.len());
		for part in idiom.iter() {
			match part {
				Part::Field(name) => parts.push(FieldPathPart::Field(name.clone())),
				Part::First => parts.push(FieldPathPart::First),
				Part::Last => parts.push(FieldPathPart::Last),
				Part::Value(Expr::Literal(Literal::Integer(i))) if *i >= 0 => {
					parts.push(FieldPathPart::Index(*i as usize))
				}
				Part::Lookup(lookup) => {
					// Graph traversal key like "->table" - convert to string representation
					parts.push(FieldPathPart::Lookup(lookup.to_sql()))
				}
				// Skip parts that don't affect output path structure
				Part::Destructure(_) | Part::Start(_) => {}
				_ => {
					return Err(Error::PlannerUnimplemented(format!(
						"FieldPath cannot contain complex parts like where clauses or method calls. \
				 Only simple field access (a.b.c), literal indices ([0], [$]), and graph traversals are supported. \
				 Got: {:?}",
						idiom
					)));
				}
			}
		}
		Ok(FieldPath(parts))
	}
}

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
	pub fn extract(&self, value: &Value) -> Value {
		let mut current = value.clone();
		for part in &self.0 {
			current = match (&current, part) {
				// Field/Lookup access on object
				(Value::Object(obj), FieldPathPart::Field(name) | FieldPathPart::Lookup(name)) => {
					obj.get(name).cloned().unwrap_or(Value::None)
				}
				// Index access on array
				(Value::Array(arr), FieldPathPart::Index(i)) => {
					arr.get(*i).cloned().unwrap_or(Value::None)
				}
				// Index access on set
				(Value::Set(set), FieldPathPart::Index(i)) => {
					set.nth(*i).cloned().unwrap_or(Value::None)
				}
				// First element of array
				(Value::Array(arr), FieldPathPart::First) => {
					arr.first().cloned().unwrap_or(Value::None)
				}
				// First element of set
				(Value::Set(set), FieldPathPart::First) => {
					set.first().cloned().unwrap_or(Value::None)
				}
				// Last element of array
				(Value::Array(arr), FieldPathPart::Last) => {
					arr.last().cloned().unwrap_or(Value::None)
				}
				// Last element of set
				(Value::Set(set), FieldPathPart::Last) => {
					set.last().cloned().unwrap_or(Value::None)
				}
				// Field/Lookup access on array applies to each element
				(Value::Array(arr), FieldPathPart::Field(name) | FieldPathPart::Lookup(name)) => {
					Value::Array(
						arr.iter()
							.map(|v| match v {
								Value::Object(obj) => obj.get(name).cloned().unwrap_or(Value::None),
								_ => Value::None,
							})
							.collect::<Vec<_>>()
							.into(),
					)
				}
				// Field/Lookup access on set applies to each element
				(Value::Set(set), FieldPathPart::Field(name) | FieldPathPart::Lookup(name)) => {
					Value::Set(Set::from(
						set.iter()
							.map(|v| match v {
								Value::Object(obj) => obj.get(name).cloned().unwrap_or(Value::None),
								_ => Value::None,
							})
							.collect::<Vec<_>>(),
					))
				}
				// Any other combination returns None
				_ => Value::None,
			};
		}
		current
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
		assert_eq!(result, Value::from("Alice"));
	}

	#[test]
	fn test_field_path_extract_nested() {
		// Create path: user.address.city
		let path = FieldPath(vec![
			FieldPathPart::Field("user".to_string()),
			FieldPathPart::Field("address".to_string()),
			FieldPathPart::Field("city".to_string()),
		]);

		// Create nested object: { user: { address: { city: "Austin" } } }
		let city_obj = make_obj(vec![("city", Value::from("Austin"))]);
		let address_obj = make_obj(vec![("address", Value::Object(city_obj))]);
		let user_obj = make_obj(vec![("user", Value::Object(address_obj))]);
		let value = Value::Object(user_obj);

		let result = path.extract(&value);
		assert_eq!(result, Value::from("Austin"));
	}

	#[test]
	fn test_field_path_extract_array_index() {
		// Create path: items[0]
		let path =
			FieldPath(vec![FieldPathPart::Field("items".to_string()), FieldPathPart::Index(0)]);

		let items = Value::Array(vec![Value::from("first"), Value::from("second")].into());
		let obj = make_obj(vec![("items", items)]);
		let value = Value::Object(obj);

		let result = path.extract(&value);
		assert_eq!(result, Value::from("first"));
	}

	#[test]
	fn test_field_path_extract_array_last() {
		// Create path: items[$]
		let path = FieldPath(vec![FieldPathPart::Field("items".to_string()), FieldPathPart::Last]);

		let items = Value::Array(vec![Value::from("first"), Value::from("second")].into());
		let obj = make_obj(vec![("items", items)]);
		let value = Value::Object(obj);

		let result = path.extract(&value);
		assert_eq!(result, Value::from("second"));
	}

	#[test]
	fn test_field_path_extract_missing() {
		let path = FieldPath::field("missing");
		let obj = make_obj(vec![("name", Value::from("Alice"))]);
		let value = Value::Object(obj);

		let result = path.extract(&value);
		assert_eq!(result, Value::None);
	}

	#[test]
	fn test_field_path_extract_field_on_array() {
		// Create path: users.name (should extract name from each user)
		let path = FieldPath(vec![
			FieldPathPart::Field("users".to_string()),
			FieldPathPart::Field("name".to_string()),
		]);

		let user1 = Value::Object(make_obj(vec![("name", Value::from("Alice"))]));
		let user2 = Value::Object(make_obj(vec![("name", Value::from("Bob"))]));
		let users = Value::Array(vec![user1, user2].into());
		let obj = make_obj(vec![("users", users)]);
		let value = Value::Object(obj);

		let result = path.extract(&value);
		if let Value::Array(arr) = result {
			assert_eq!(arr.len(), 2);
			assert_eq!(arr[0], Value::from("Alice"));
			assert_eq!(arr[1], Value::from("Bob"));
		} else {
			panic!("Expected array result");
		}
	}

	#[test]
	fn test_field_path_display() {
		let path = FieldPath(vec![
			FieldPathPart::Field("user".to_string()),
			FieldPathPart::Field("address".to_string()),
			FieldPathPart::Index(0),
			FieldPathPart::Field("city".to_string()),
		]);
		assert_eq!(path.to_string(), "user.address[0].city");
	}
}
