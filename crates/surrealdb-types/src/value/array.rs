use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::Value;
use crate::utils::display::JoinDisplayable;

/// Represents an array of values in SurrealDB
///
/// An array is an ordered collection of values that can contain elements of any type.
/// The underlying storage is a `Vec<Value>`.
#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Array(pub Vec<Value>);

impl Array {
	/// Create a new empty array
	pub fn new() -> Self {
		Array(Vec::new())
	}

	/// Add a value to the end of the array
	pub fn push<V>(&mut self, value: V)
	where
		V: Into<Value>,
	{
		self.0.push(value.into());
	}
}

impl Display for Array {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "[{}]", self.0.join_displayable(", "))
	}
}
