//! Local, derivable facts about an expression's ability to modify data.
//!
//! [`Expr::contains_mutation`] answers "does this tree itself write?" as a
//! single yes/no. This module extracts the three facts a *call-graph* answer
//! needs instead: whether the tree writes directly, which user-defined
//! functions it calls (their stored bodies carry the rest of the answer), and
//! whether it invokes something whose body cannot be inspected at all. It also
//! names the builtins and methods the tree calls, whose declared effects live
//! on the function registry rather than here. The caller combines these facts
//! across function bodies with two polarities:
//!
//! - **Precise** ("provably writes"): only `direct_writes`, unioned over the reachable call graph.
//!   Opaque callables count as clean, because refusing them would refuse working read-only bodies
//!   (a pure script, a pure `eval`).
//! - **Conservative** ("possibly writes"): `direct_writes` or `opaque_effects` anywhere reachable.
//!   This is the polarity a planner must use before treating an expression as read-only.
//!
//! Known envelope: a closure *value* that reaches an invocation site as data
//! is invisible to this walk when the site itself looks harmless — as an
//! argument to a closure-taking builtin (`array::map($arr, $fn)`), or stored
//! on the receiver under a name that shadows a registered builtin method.
//! This matches the executors' own access-mode analysis. The runtime write
//! refusal and the transaction's own write gate remain the backstop for those
//! shapes. Method calls whose name is *not* a registered builtin can only
//! resolve to such a closure, so they count as opaque here.

use std::collections::BTreeSet;

use crate::expr::operator::PostfixOperator;
use crate::expr::visit::{Visit as _, Visitor};
use crate::expr::{Block, Expr, Function, Part};

/// What one expression tree contributes to a mutability answer, before any
/// callee bodies are consulted.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FunctionFacts {
	/// A data-modifying statement appears somewhere in the tree itself: the
	/// same construct set [`Expr::contains_mutation`] rejects, at the same
	/// full depth (subqueries, blocks, idiom parts, closure bodies and call
	/// arguments included).
	pub direct_writes: bool,
	/// Names of the user-defined functions the tree calls, without the
	/// `fn::` prefix, at any depth.
	pub calls: BTreeSet<String>,
	/// The tree invokes something whose body cannot be inspected: a script,
	/// module or silo function, a statement-evaluating builtin (the set
	/// [`Function::read_only`] rejects), or a call operator whose target is
	/// not a closure literal.
	pub opaque_effects: bool,
	/// Names of the builtin functions the tree calls, at any depth, whose
	/// declared effects a caller holding the function registry reads.
	pub builtins: BTreeSet<String>,
	/// Names of the methods the tree calls, at any depth, whose resolutions'
	/// declared effects a caller holding the function registry reads.
	pub methods: BTreeSet<String>,
	/// The tree hands a builtin or method a closure it runs that is not a
	/// literal of the call, whose body the tree cannot see; known only where
	/// the scan is told where callees run closures ([`ClosurePositions`]).
	pub unseen_closures: bool,
}

/// Where the builtins and methods a tree calls run a closure they are
/// handed: the registry's declarations, which this crate does not hold.
pub trait ClosurePositions {
	/// The argument positions at which the builtin `name` runs the closure
	/// it is handed.
	fn function(&self, name: &str) -> Vec<usize>;
	/// The argument positions, the receiver not counted, at which a builtin
	/// the method `name` resolves to runs the closure it is handed.
	fn method(&self, name: &str) -> Vec<usize>;
}

/// Whether an argument in one of `positions` is something other than a
/// closure literal written there, whose body the tree cannot see.
fn unseen_closure(args: &[Expr], positions: Vec<usize>) -> bool {
	positions
		.into_iter()
		.filter_map(|at| args.get(at))
		.any(|arg| !matches!(arg, Expr::Closure(_) | Expr::Literal(_)))
}

/// Collects [`FunctionFacts`] over a whole tree. Unlike the mutation scanner
/// this never short-circuits: `calls` must be complete even when a write has
/// already been found, because the caller builds a call graph from it.
struct FactsScanner<'a> {
	facts: FunctionFacts,
	/// With it, a builtin or method handed a closure that is not a literal of
	/// the call is noted ([`FunctionFacts::unseen_closures`]).
	closures: Option<&'a dyn ClosurePositions>,
}

impl Visitor for FactsScanner<'_> {
	type Error = std::convert::Infallible;

	/// The match is **exhaustive (no `_` arm) on purpose**, matching
	/// [`crate::expr::mutation`]: adding an `Expr` variant should be a build
	/// error here so a human classifies it against all three facts.
	fn visit_expr(&mut self, expr: &Expr) -> Result<(), Self::Error> {
		match expr {
			// Data-modifying statements and DDL.
			Expr::Create(_)
			| Expr::Update(_)
			| Expr::Upsert(_)
			| Expr::Delete(_)
			| Expr::Relate(_)
			| Expr::Insert(_)
			| Expr::Define(_)
			| Expr::Remove(_)
			| Expr::Rebuild(_)
			| Expr::Alter(_) => {
				self.facts.direct_writes = true;
			}

			// A GQL plan carries its mutations in its stages.
			// A call operator on anything but a closure literal executes a
			// body this tree cannot see. A literal's body is walked by the
			// default traversal, so it needs no special case.
			Expr::Postfix {
				expr: target,
				op: PostfixOperator::Call(_),
			} => {
				if !matches!(&**target, Expr::Closure(_)) {
					self.facts.opaque_effects = true;
				}
			}

			// A method call whose name is not a registered builtin resolves
			// to a closure stored on the receiver, whose body this tree
			// cannot see. `Part::Method` is the idiom-path shape of the same
			// call and is classified in `visit_part`.
			Expr::Postfix {
				op: PostfixOperator::MethodCall(name, args),
				..
			} => {
				if !crate::expr::method::is_builtin_method(name) {
					self.facts.opaque_effects = true;
				}
				if self.closures.is_some_and(|c| unseen_closure(args, c.method(name))) {
					self.facts.unseen_closures = true;
				}
				self.facts.methods.insert(name.clone());
			}

			// A builtin handed a closure it runs, other than a literal written
			// in the call, runs a body this tree cannot see.
			Expr::FunctionCall(call) => {
				if let (Function::Normal(name), Some(closures)) = (&call.receiver, self.closures)
					&& unseen_closure(&call.arguments, closures.function(name))
				{
					self.facts.unseen_closures = true;
				}
			}

			// Everything else contributes only what the default traversal
			// finds in its children: blocks, subqueries, idiom parts, closure
			// bodies and call arguments are all descended into.
			Expr::Literal(_)
			| Expr::Param(_)
			| Expr::Idiom(_)
			| Expr::Table(_)
			| Expr::Mock(_)
			| Expr::Block(_)
			| Expr::Constant(_)
			| Expr::Prefix {
				..
			}
			| Expr::Postfix {
				..
			}
			| Expr::Binary {
				..
			}
			| Expr::Closure(_)
			| Expr::Break
			| Expr::Continue
			| Expr::Return(_)
			| Expr::Throw(_)
			| Expr::IfElse(_)
			| Expr::Select(_)
			| Expr::Info(_)
			| Expr::Foreach(_)
			| Expr::Let(_)
			| Expr::Sleep(_)
			| Expr::Explain {
				..
			} => {}
		}
		expr.visit(self)
	}

	/// The idiom-path shape of a method call: an unregistered name resolves
	/// to a closure stored on the receiver, whose body this tree cannot see.
	fn visit_part(&mut self, part: &Part) -> Result<(), Self::Error> {
		if let Part::Method(name, args) = part {
			if !crate::expr::method::is_builtin_method(name) {
				self.facts.opaque_effects = true;
			}
			if self.closures.is_some_and(|c| unseen_closure(args, c.method(name))) {
				self.facts.unseen_closures = true;
			}
			self.facts.methods.insert(name.as_str().to_owned());
		}
		part.visit(self)
	}

	fn visit_function(&mut self, f: &Function) -> Result<(), Self::Error> {
		match f {
			// The callee's stored body carries the answer; record the edge.
			Function::Custom(name) => {
				self.facts.calls.insert(name.clone());
			}
			// Builtins, scripts, models, modules and silos: reuse the
			// transaction-type classification as the single source of truth
			// for which of them can evaluate arbitrary statements.
			other => {
				if !other.read_only() {
					self.facts.opaque_effects = true;
				}
				if let Function::Normal(name) = other {
					self.facts.builtins.insert(name.clone());
				}
			}
		}
		Ok(())
	}
}

impl Expr {
	/// Extract this tree's [`FunctionFacts`].
	pub fn function_facts(&self) -> FunctionFacts {
		self.function_facts_with(None)
	}

	/// [`Self::function_facts`], with a builtin or method handed a closure
	/// that is not a literal of the call noted where `closures` says where
	/// the callees run one.
	pub fn function_facts_with(&self, closures: Option<&dyn ClosurePositions>) -> FunctionFacts {
		let mut scanner = FactsScanner {
			facts: FunctionFacts::default(),
			closures,
		};
		// Enter through the visitor method, not the default traversal, so the
		// root node is classified too. Infallible: the scanner accumulates.
		let Ok(()) = scanner.visit_expr(self);
		scanner.facts
	}
}

impl Block {
	/// Extract this block's [`FunctionFacts`], the form stored function
	/// bodies take.
	pub fn function_facts(&self) -> FunctionFacts {
		self.function_facts_with(None)
	}

	/// [`Expr::function_facts_with`] for a block.
	pub fn function_facts_with(&self, closures: Option<&dyn ClosurePositions>) -> FunctionFacts {
		let mut scanner = FactsScanner {
			facts: FunctionFacts::default(),
			closures,
		};
		let Ok(()) = scanner.visit_block(self);
		scanner.facts
	}
}
