//! Function signature definitions for type checking and documentation.

use crate::expr::Kind;

/// Describes the signature of a function including its return type.
#[derive(Debug, Clone)]
pub struct Signature {
	/// The return type (may depend on input types, so this is the "typical" return)
	pub returns: Kind,
}

impl Signature {
	/// Create a new signature builder
	pub fn new() -> Self {
		Self {
			returns: Kind::Any,
		}
	}

	/// Accept a required argument (builder pattern, retained for compatibility)
	pub fn arg(self, _name: &'static str, _kind: Kind) -> Self {
		self
	}

	/// Accept an optional argument (builder pattern, retained for compatibility)
	pub fn optional(self, _name: &'static str, _kind: Kind) -> Self {
		self
	}

	/// Set variadic argument type (builder pattern, retained for compatibility)
	pub fn variadic(self, _kind: Kind) -> Self {
		self
	}

	/// Set the return type
	pub fn returns(mut self, kind: Kind) -> Self {
		self.returns = kind;
		self
	}
}

impl Default for Signature {
	fn default() -> Self {
		Self::new()
	}
}
