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

/// How a function's result kind follows from its arguments' kinds.
///
/// The same rules the SurrealQL analyzer infers with, so that the planner
/// and the analyzer agree on what a call returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReturnRule {
	/// [`Signature::returns`], whatever the arguments.
	Fixed,
	/// The kind of argument `index`: a function that passes a value through,
	/// as `math::max` picks one of its arguments.
	SameAsArg(usize),
	/// The element kind of argument `index` when it is an array or a set, or
	/// `NONE`, as `array::first` returns an element of a non-empty array.
	ArrayElement(usize),
	/// A numeric aggregate over argument `index`: `int` when every element is
	/// an `int`, `number` otherwise. `math::sum([1, 2])` is `3`, not `3f`.
	NumericAggregate(usize),
}

/// Describes the signature of a function including its arguments and return type.
#[derive(Debug, Clone)]
pub struct Signature {
	/// The declared arguments, in call order
	pub args: Vec<ArgSpec>,
	/// The kind accepted for any number of trailing arguments, if variadic
	pub variadic: Option<Kind>,
	/// The return kind the function is declared with: what it returns when
	/// nothing is known of its arguments.
	pub returns: Kind,
	/// How the result kind follows from the arguments' kinds, where they are
	/// known.
	pub rule: ReturnRule,
}

impl Signature {
	/// Create a new signature builder
	pub fn new() -> Self {
		Self {
			args: Vec::new(),
			variadic: None,
			returns: Kind::Any,
			rule: ReturnRule::Fixed,
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

	/// Derive the result kind from the arguments' kinds by `rule`.
	pub fn rule(mut self, rule: ReturnRule) -> Self {
		self.rule = rule;
		self
	}

	/// The result kind of a call whose arguments have kinds `args`. A rule
	/// that reads an argument the call does not have, or one of another shape
	/// than the rule reads, answers the declared return kind.
	pub fn infer(&self, args: &[Kind]) -> Kind {
		match self.rule {
			ReturnRule::Fixed => self.returns.clone(),
			ReturnRule::SameAsArg(index) => {
				args.get(index).cloned().unwrap_or_else(|| self.returns.clone())
			}
			// An empty collection or an index past its end answers `NONE`.
			ReturnRule::ArrayElement(index) => match args.get(index) {
				Some(Kind::Array(element, _) | Kind::Set(element, _)) => {
					if let Kind::Any = &**element {
						Kind::Any
					} else {
						Kind::Either(vec![element.as_ref().clone(), Kind::None])
					}
				}
				_ => self.returns.clone(),
			},
			ReturnRule::NumericAggregate(index) => match args.get(index) {
				Some(Kind::Array(element, _) | Kind::Set(element, _))
					if matches!(**element, Kind::Int) =>
				{
					Kind::Int
				}
				_ => Kind::Number,
			},
		}
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
