//! Wrapper type for serializable function arguments.
//!
//! This module provides [`SerializableArg`](crate::arg::SerializableArg), a newtype wrapper that bridges between
//! types implementing [`surrealdb_types::SurrealValue`] and the serialization system.

use surrealdb_types::SurrealValue;

/// A wrapper for function arguments that implement [`SurrealValue`].
///
/// This type provides a bridge between the [`SurrealValue`] trait (which defines
/// conversion to/from [`surrealdb_types::Value`]) and the [`Serializable`] trait
/// (which defines binary serialization).
///
/// # Purpose
///
/// The wrapper allows any type implementing [`SurrealValue`] to be automatically
/// serialized by:
/// 1. Converting to [`surrealdb_types::Value`] via [`SurrealValue::into_value`]
/// 2. Serializing the `Value` using its FlatBuffers implementation
///
/// This avoids needing separate `Serializable` implementations for every SurrealDB type.
///
/// # Example
///
/// ```rust,ignore
/// use surrealism_types::arg::SerializableArg;
/// use surrealdb_types::SurrealValue;
///
/// fn process_arg<T: SurrealValue>(arg: T) -> Result<()> {
///     let wrapped = SerializableArg::from(arg);
///     // Now `wrapped` can be serialized...
///     Ok(())
/// }
/// ```
///
/// [`Serializable`]: crate::serialize::Serializable
/// [`SurrealValue`]: surrealdb_types::SurrealValue
pub struct SerializableArg<T: SurrealValue>(pub T);

impl<T: SurrealValue> From<T> for SerializableArg<T> {
	fn from(value: T) -> Self {
		SerializableArg(value)
	}
}
