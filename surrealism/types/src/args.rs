//! Function argument marshalling for SurrealDB values.
//!
//! This module provides the [`Args`](crate::args::Args) trait for converting between typed tuples and
//! vectors of [`surrealdb_types::Value`]. This enables type-safe function signatures
//! while maintaining a uniform representation for cross-language communication.
//!
//! # Type Safety
//!
//! The trait provides:
//! - **Conversion to Values**: Transform typed arguments into a vector of `Value`
//! - **Conversion from Values**: Reconstruct typed arguments with validation
//! - **Type Metadata**: Query the expected types via `kinds()`
//!
//! # Supported Patterns
//!
//! - Tuples from 1 to 10 elements (each implementing [`SurrealValue`])
//! - `Vec<T>` for variadic arguments
//! - `()` for zero arguments
//!
//! [`SurrealValue`]: surrealdb_types::SurrealValue

use anyhow::Result;
use surrealdb_types::SurrealValue;

/// Trait for marshalling function arguments to and from [`surrealdb_types::Value`] vectors.
///
/// This trait enables type-safe function signatures while maintaining a language-agnostic
/// representation. It's implemented for tuples of types that implement [`SurrealValue`],
/// allowing functions to accept strongly-typed arguments that are converted to/from
/// SurrealDB's value type.
///
/// # Example
///
/// ```rust,ignore
/// use surrealism_types::args::Args;
/// use surrealdb_types::SurrealValue;
///
/// fn my_function(args: impl Args) -> Result<()> {
///     // Convert args to Values for processing
///     let values = args.to_values();
///     
///     // ... process values ...
///     
///     Ok(())
/// }
///
/// // Call with typed arguments
/// my_function(("hello".to_string(), 42i64, true))?;
///
/// // Or reconstruct from Values
/// let values = vec![/* ... */];
/// let (s, n, b): (String, i64, bool) = Args::from_values(values)?;
/// ```
pub trait Args: Sized {
	/// Convert this argument tuple into a vector of [`surrealdb_types::Value`].
	///
	/// This method consumes the arguments and produces a vector where each element
	/// corresponds to one argument converted via [`SurrealValue::into_value`].
	///
	/// # Example
	///
	/// ```rust,ignore
	/// let args = ("hello".to_string(), 42i64);
	/// let values = args.to_values();
	/// // values = [Value::Strand("hello"), Value::Number(42)]
	/// ```
	fn to_values(self) -> Vec<surrealdb_types::Value>;

	/// Reconstruct typed arguments from a vector of [`surrealdb_types::Value`].
	///
	/// This method attempts to convert each value in the vector to the corresponding
	/// type in the tuple. It validates both the number of arguments and their types.
	///
	/// # Errors
	///
	/// Returns an error if:
	/// - The number of values doesn't match the expected argument count
	/// - Any value cannot be converted to its expected type
	///
	/// # Example
	///
	/// ```rust,ignore
	/// let values = vec![/* ... */];
	/// let (s, n): (String, i64) = Args::from_values(values)?;
	/// ```
	fn from_values(values: Vec<surrealdb_types::Value>) -> Result<Self>;

	/// Get the expected types for each argument position.
	///
	/// This returns a vector of [`surrealdb_types::Kind`] describing the type of
	/// each argument. This is useful for validation, documentation, and error messages.
	///
	/// # Example
	///
	/// ```rust,ignore
	/// type MyArgs = (String, i64, bool);
	/// let kinds = MyArgs::kinds();
	/// // kinds = [Kind::String, Kind::Int, Kind::Bool]
	/// ```
	fn kinds() -> Vec<surrealdb_types::Kind>;
}

macro_rules! impl_args {
    ($($len:literal => ($($name:ident),+)),+ $(,)?) => {
        $(
            impl<$($name),+> Args for ($($name,)+)
            where
                $($name: SurrealValue),+
            {
                fn to_values(self) -> Vec<surrealdb_types::Value> {
                    #[allow(non_snake_case)]
                    let ($($name,)+) = self;
                    vec![
                        $($name.into_value(),)+
                    ]
                }

                fn from_values(values: Vec<surrealdb_types::Value>) -> Result<Self> {
                    if values.len() != $len {
                        return Err(anyhow::anyhow!("Expected ({}), found other arguments", Self::kinds().iter().map(|k| k.to_string()).collect::<Vec<String>>().join(", ")));
                    }

                    let mut values = values;

                    $(#[allow(non_snake_case)] let $name = values.remove(0);)+

                    Ok(($($name::from_value($name)?,)+))
                }

                fn kinds() -> Vec<surrealdb_types::Kind> {
                    vec![
                        $($name::kind_of(),)+
                    ]
                }
            }
        )+
    };
}

impl_args! {
	1 => (A),
	2 => (A, B),
	3 => (A, B, C),
	4 => (A, B, C, D),
	5 => (A, B, C, D, E),
	6 => (A, B, C, D, E, F),
	7 => (A, B, C, D, E, F, G),
	8 => (A, Bq, C, D, E, F, G, H),
	9 => (A, B, C, D, E, F, G, H, I),
	10 => (A, B, C, D, E, F, G, H, I, J),
}

/// Implementation for zero arguments (unit type).
///
/// Functions that take no arguments use `()` as their `Args` type.
impl Args for () {
	fn to_values(self) -> Vec<surrealdb_types::Value> {
		Vec::new()
	}

	fn from_values(values: Vec<surrealdb_types::Value>) -> Result<Self> {
		if !values.is_empty() {
			return Err(anyhow::anyhow!(
				"Expected ({}), found other arguments",
				Self::kinds().iter().map(|k| k.to_string()).collect::<Vec<String>>().join(", ")
			));
		}

		Ok(())
	}

	fn kinds() -> Vec<surrealdb_types::Kind> {
		Vec::new()
	}
}

/// Implementation for variadic arguments via [`Vec<T>`].
///
/// This allows functions to accept a variable number of arguments of the same type.
/// Useful for operations like batch inserts or aggregate functions.
///
/// # Example
///
/// ```rust,ignore
/// fn process_many(args: Vec<String>) -> Result<()> {
///     let values = args.to_values();
///     // ... process variable number of strings ...
///     Ok(())
/// }
/// ```
impl<T> Args for Vec<T>
where
	T: SurrealValue,
{
	fn to_values(self) -> Vec<surrealdb_types::Value> {
		self.into_iter().map(|x| x.into_value()).collect()
	}

	fn from_values(values: Vec<surrealdb_types::Value>) -> Result<Self> {
		values.into_iter().map(|x| T::from_value(x)).collect::<Result<Vec<T>>>()
	}

	/// Returns a single-element vector with the element type.
	///
	/// Note: This is used for dynamic argument transfer, not for static type annotations
	/// (since the length is variable).
	fn kinds() -> Vec<surrealdb_types::Kind> {
		vec![T::kind_of()]
	}
}
