//! Function signature definitions for type checking and documentation.

use crate::expr::Kind;

/// A single declared argument in a function signature.
#[allow(unused)]
#[derive(Debug, Clone)]
pub struct ArgSpec {
	/// The argument name (documentation only)
	pub name: &'static str,
	/// The expected argument kind
	pub kind: Kind,
	/// Whether the argument may be omitted
	pub optional: bool,
}

/// Describes the signature of a function including its arguments and return type.
#[derive(Debug, Clone)]
pub struct Signature {
	/// The declared arguments, in call order
	pub args: Vec<ArgSpec>,
	/// The kind accepted for any number of trailing arguments, if variadic
	pub variadic: Option<Kind>,
	/// The return type (may depend on input types, so this is the "typical" return)
	pub returns: Kind,
}

impl Signature {
	/// Create a new signature builder
	pub fn new() -> Self {
		Self {
			args: Vec::new(),
			variadic: None,
			returns: Kind::Any,
		}
	}

	/// Accept a required argument
	pub fn arg(mut self, name: &'static str, kind: Kind) -> Self {
		self.args.push(ArgSpec {
			name,
			kind,
			optional: false,
		});
		self
	}

	/// Accept an optional argument
	pub fn optional(mut self, name: &'static str, kind: Kind) -> Self {
		self.args.push(ArgSpec {
			name,
			kind,
			optional: true,
		});
		self
	}

	/// Set variadic argument type
	pub fn variadic(mut self, kind: Kind) -> Self {
		self.variadic = Some(kind);
		self
	}

	/// Set the return type
	pub fn returns(mut self, kind: Kind) -> Self {
		self.returns = kind;
		self
	}

	/// The declared arity as a `(lower, upper)` bound, where `upper` is
	/// `None` for variadic signatures.
	#[allow(unused)]
	pub fn arity(&self) -> (usize, Option<usize>) {
		let lower = self.args.iter().filter(|a| !a.optional).count();
		let upper = if self.variadic.is_some() {
			None
		} else {
			Some(self.args.len())
		};
		(lower, upper)
	}
}

impl Default for Signature {
	fn default() -> Self {
		Self::new()
	}
}
