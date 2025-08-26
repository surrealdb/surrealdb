#![doc = include_str!("../README.md")]
#![warn(missing_docs)]

mod flatbuffers;
mod kind;
mod traits;
pub(crate) mod utils;
mod value;

pub use flatbuffers::*;
pub use kind::*;
// Re-export the derive macro
pub use surrealdb_types_derive::*;
pub use traits::*;
pub use value::*;

/// Macro for creating a SurrealDB object.
///
/// This macro creates a SurrealDB object, which is a collection of key-value pairs.
/// All values must implement the `SurrealValue` trait.
///
/// # Example
///
/// ```rust
/// use surrealdb_types::object;
///
/// let obj = object! {
///     name: "John".to_string(),
///     "user-id": 12345,
/// };
/// ```
#[macro_export]
macro_rules! object {
    // Base case: empty object
    () => {
        $crate::Object::new()
    };

    // Handle a list of field-value pairs (supports both identifiers and literals)
    ($($key:tt: $value:expr),* $(,)?) => {
        {
            let mut obj = $crate::Object::new();
            $(
                $crate::object!(@insert obj, $key: $crate::Value::from($value));
            )*
            obj
        }
    };

    // Internal helper to insert a single field - handles identifiers
    (@insert $obj:expr, $key:ident: $value:expr) => {
        $obj.insert(stringify!($key).to_string(), $value);
    };

    // Internal helper to insert a single field - handles string literals
    (@insert $obj:expr, $key:literal: $value:expr) => {
        $obj.insert($key.to_string(), $value);
    };
}

/// Macro for creating a SurrealDB array.
///
/// This macro creates a SurrealDB array, which is a collection of values.
/// All values must implement the `SurrealValue` trait.
///
/// # Example
///
/// ```rust
/// use surrealdb_types::array;
///
/// let arr = array![1, 2, 3];
/// ```
#[macro_export]
macro_rules! array {
    // Base case: empty array
    [] => {
        $crate::Array::new()
    };

    // Handle a list of values
    [$($value:expr),* $(,)?] => {
        {
            let mut arr = $crate::Array::new();
            $(
                arr.push($crate::Value::from($value));
            )*
            arr
        }
    };
}

/// Example usage of the `object!` and `array!` macros:
///
/// ```rust
/// use surrealdb_types::{object, array};
///
/// // Create an empty object
/// let empty = object! {};
///
/// // Create an object with regular field names
/// let person = object! {
///     name: "John",
///     age: 30,
///     active: true,
/// };
///
/// // Create an object with quoted field names (for fields with hyphens, spaces, etc.)
/// let config = object! {
///     "first-name": "John",
///     "last-name": "Doe",
///     "user-id": 12345,
/// };
///
/// // Mix regular and quoted field names in the same object!
/// let mixed = object! {
///     name: "John",           // regular identifier
///     "last-name": "Doe",     // quoted string
///     age: 30,                // regular identifier
///     "user-id": 12345,       // quoted string
///     active: true,           // regular identifier
/// };
///
/// // Create arrays
/// let numbers = array! [1, 2, 3, 4, 5];
/// let mixed = array! ["hello", 42, true];
/// let nested = array! [1, person, "end"];  // can include objects and other values
/// ```
#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_object_macro() {
		// Test empty object
		let empty = object! {};
		assert_eq!(empty.0.len(), 0);

		let _ = Value::None.is_bool();

		// Test with regular field names
		let obj1 = object! {
			name: "John".to_string(),
			age: 30,
		};
		assert_eq!(obj1.0.get("name"), Some(&Value::String("John".to_string())));
		assert_eq!(obj1.0.get("age"), Some(&Value::Number(30.into())));

		// Test with quoted field names
		let obj2 = object! {
			"first-name": "John".to_string(),
			"last-name": "Doe".to_string(),
		};
		assert_eq!(obj2.0.get("first-name"), Some(&Value::String("John".to_string())));
		assert_eq!(obj2.0.get("last-name"), Some(&Value::String("Doe".to_string())));

		// Test mixed field names - this now works!
		let obj3 = object! {
			name: "John".to_string(),
			"last-name": "Doe".to_string(),
			age: 30,
			"user-id": 12345,
			active: true,
		};
		assert_eq!(obj3.0.get("name"), Some(&Value::String("John".to_string())));
		assert_eq!(obj3.0.get("last-name"), Some(&Value::String("Doe".to_string())));
		assert_eq!(obj3.0.get("age"), Some(&Value::Number(30.into())));
		assert_eq!(obj3.0.get("user-id"), Some(&Value::Number(12345.into())));
		assert_eq!(obj3.0.get("active"), Some(&Value::Bool(true)));

		// Test with trailing comma
		let obj4 = object! {
			name: "Alice".to_string(),
			"last-name": "Smith".to_string(),
		};
		assert_eq!(obj4.0.get("name"), Some(&Value::String("Alice".to_string())));
		assert_eq!(obj4.0.get("last-name"), Some(&Value::String("Smith".to_string())));
	}

	#[test]
	fn test_array_macro() {
		// Test empty array
		let empty = array![];
		assert_eq!(empty.len(), 0);

		// Test with simple values
		let arr1 = array![1, 2, 3];
		assert_eq!(arr1.len(), 3);
		assert_eq!(arr1[0], Value::Number(1.into()));
		assert_eq!(arr1[1], Value::Number(2.into()));
		assert_eq!(arr1[2], Value::Number(3.into()));

		// Test with mixed types
		let arr2 = array!["hello".to_string(), 42, true];
		assert_eq!(arr2.len(), 3);
		assert_eq!(arr2[0], Value::String("hello".to_string()));
		assert_eq!(arr2[1], Value::Number(42.into()));
		assert_eq!(arr2[2], Value::Bool(true));

		// Test with trailing comma
		let arr3 = array!["a".to_string(), "b".to_string()];
		assert_eq!(arr3.len(), 2);
		assert_eq!(arr3[0], Value::String("a".to_string()));
		assert_eq!(arr3[1], Value::String("b".to_string()));

		// Test with nested objects and arrays
		let nested_obj = object! { name: "John".to_string(), age: 30 };
		let arr4 = array![1, nested_obj.clone(), "end".to_string()];
		assert_eq!(arr4.len(), 3);
		assert_eq!(arr4[0], Value::Number(1.into()));
		assert_eq!(arr4[1], Value::Object(nested_obj));
		assert_eq!(arr4[2], Value::String("end".to_string()));
	}
}
