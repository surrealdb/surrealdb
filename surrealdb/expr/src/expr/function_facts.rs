//! Local, derivable facts about an expression's ability to modify data.
//!
//! [`Expr::contains_mutation`] answers "does this tree itself write?" as a
//! single yes/no. This module extracts the three facts a *call-graph* answer
//! needs instead: whether the tree writes directly, which user-defined
//! functions it calls (their stored bodies carry the rest of the answer), and
//! whether it invokes something whose body cannot be inspected at all. The
//! caller combines these facts across function bodies with two polarities:
//!
//! - **Precise** ("provably writes"): only `direct_writes`, unioned over the reachable call graph.
//!   Opaque callables count as clean, because refusing them would refuse working read-only bodies
//!   (a pure script, a pure `eval`).
//! - **Conservative** ("possibly writes"): `direct_writes` or `opaque_effects` anywhere reachable.
//!   This is the polarity a planner must use before treating an expression as read-only.
//!
//! Known envelope: a closure *value* that reaches an invocation site as data —
//! through a parameter, an object field invoked as a method, or as an argument
//! to a closure-taking builtin — is invisible to this walk, exactly as it is
//! to the executors' own access-mode analysis. The runtime write refusal and
//! the transaction's own write gate remain the backstop for those shapes.

use std::collections::BTreeSet;

use crate::expr::operator::PostfixOperator;
use crate::expr::visit::{Visit as _, Visitor};
use crate::expr::{Block, Expr, Function};

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
}

/// Collects [`FunctionFacts`] over a whole tree. Unlike the mutation scanner
/// this never short-circuits: `calls` must be complete even when a write has
/// already been found, because the caller builds a call graph from it.
struct FactsScanner {
	facts: FunctionFacts,
}

impl Visitor for FactsScanner {
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
			Expr::Match(plan) => {
				if plan.has_mutations() {
					self.facts.direct_writes = true;
				}
			}

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
			| Expr::FunctionCall(_)
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
			}
		}
		Ok(())
	}
}

impl Expr {
	/// Extract this tree's [`FunctionFacts`].
	pub fn function_facts(&self) -> FunctionFacts {
		let mut scanner = FactsScanner {
			facts: FunctionFacts::default(),
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
		let mut scanner = FactsScanner {
			facts: FunctionFacts::default(),
		};
		let Ok(()) = scanner.visit_block(self);
		scanner.facts
	}
}
