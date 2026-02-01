//! Function signature definitions for type checking and documentation.

use crate::expr::Kind;

/// Describes the signature of a function including its arguments and return type.
#[derive(Debug, Clone)]
pub struct Signature {
	/// Required and optional arguments in order
	pub args: Vec<ArgSpec>,
	/// If present, the function accepts additional arguments of this type
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

	/// Add a required argument
	pub fn arg(mut self, name: &'static str, kind: Kind) -> Self {
		self.args.push(ArgSpec {
			name,
			kind,
			optional: false,
		});
		self
	}

	/// Add an optional argument
	pub fn optional(mut self, name: &'static str, kind: Kind) -> Self {
		self.args.push(ArgSpec {
			name,
			kind,
			optional: true,
		});
		self
	}

	/// Set variadic argument type (accepts any number of additional args)
	pub fn variadic(mut self, kind: Kind) -> Self {
		self.variadic = Some(kind);
		self
	}

	/// Set the return type
	pub fn returns(mut self, kind: Kind) -> Self {
		self.returns = kind;
		self
	}

	/// Get the minimum number of required arguments
	pub fn min_args(&self) -> usize {
		self.args.iter().filter(|a| !a.optional).count()
	}

	/// Get the maximum number of arguments (None if variadic)
	pub fn max_args(&self) -> Option<usize> {
		if self.variadic.is_some() {
			None
		} else {
			Some(self.args.len())
		}
	}
}

impl Default for Signature {
	fn default() -> Self {
		Self::new()
	}
}

/// Describes a single function argument
#[derive(Debug, Clone)]
pub struct ArgSpec {
	/// The name of the argument (for documentation/error messages)
	pub name: &'static str,
	/// The expected type
	pub kind: Kind,
	/// Whether this argument is optional
	pub optional: bool,
}
