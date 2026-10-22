//! What evaluating stored code spends of the computation-depth budget
//! (`max_computation_depth`) where it is evaluated one expression at a time.
//!
//! A table's event clauses, the body of a user-defined function whose text
//! holds a write, and everything either reaches are evaluated expression by
//! expression: each expression entered spends one of the budget, and a
//! statement that processes records spends [`RECORDS_DEPTH`] more for the
//! records it processes. A user-defined function called there runs its body's
//! statements one level below the call, with no level of its own. The VM runs
//! such code with its own instructions and spends the amounts this walk
//! measures, at the same points: at the clause, at the entry of a body, and at
//! each call ([`ExprDepth`]).
//!
//! Elsewhere — a statement the session runs, a function body without a write
//! reached from one — a call spends one per re-entry and nothing per
//! expression.

use std::collections::BTreeMap;

use crate::expr::visit::{Visit as _, Visitor};
use crate::expr::{Block, Expr, Function};

/// What a statement processing records spends for them, beyond the one its
/// expression spends.
pub(super) const RECORDS_DEPTH: u32 = 4;

/// What a write statement spends of the budget before its records' events
/// run: one for its expression, and [`RECORDS_DEPTH`] for its records.
pub(super) const STATEMENT_DEPTH: u8 = 1 + RECORDS_DEPTH as u8;

/// What evaluating one piece of stored code spends, at the points the
/// evaluation re-enters.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ExprDepth {
	/// The most the code has spent at once anywhere in it outside the
	/// arguments of the user-defined functions it calls: the budget left must
	/// cover this for the code to be evaluated.
	pub(crate) deepest: u32,
	/// What the code has spent on the way to its shallowest write, the
	/// write's own [`STATEMENT_DEPTH`] not counted; `None` for code that
	/// writes nothing itself.
	pub(crate) write: Option<u32>,
	/// For each user-defined function the code calls, by name without its
	/// `fn::` prefix, what the code has spent at its shallowest call, the
	/// call's own expression counted.
	pub(crate) calls: BTreeMap<String, u32>,
	/// What the code has spent at its shallowest call of `api::invoke`, the
	/// call's own expression counted.
	pub(crate) api_invoke: Option<u32>,
}

impl ExprDepth {
	/// What evaluating `expr` spends.
	pub(crate) fn of_expr(expr: &Expr) -> Self {
		let mut walk = DepthWalk::default();
		let Ok(()) = walk.visit_expr(expr);
		walk.found
	}

	/// What running `block` as a function body spends: its statements one
	/// level below where the body is entered.
	pub(crate) fn of_body(block: &Block) -> Self {
		let mut walk = DepthWalk::default();
		let Ok(()) = walk.visit_block(block);
		walk.found
	}

	/// What an API handler spends where it is entered, given this measure of
	/// its body: the way to its shallowest write or, for a handler that writes
	/// nothing, the way to its shallowest `api::invoke`, which runs another
	/// handler.
	pub(crate) fn handler_charged(&self) -> u32 {
		self.write.or(self.api_invoke).unwrap_or(0)
	}

	/// What the code spends where it is entered: the way to its shallowest
	/// write, which everything evaluated inside it is charged with.
	pub(crate) fn charged(&self) -> u32 {
		self.write.unwrap_or(0)
	}

	/// What a call of `fn::name` the code makes spends beyond the one the
	/// call's own re-entry spends and what entering the code charged.
	pub(crate) fn call_extra(&self, name: &str) -> u32 {
		self.calls.get(name).map_or(0, |at| at.saturating_sub(1 + self.charged()))
	}
}

/// The fold [`ExprDepth`] makes over stored code.
#[derive(Default)]
struct DepthWalk {
	/// What is spent at the node being visited.
	depth: u32,
	/// How many calls of user-defined functions the node being visited is an
	/// argument of.
	in_call_args: u32,
	found: ExprDepth,
}

impl Visitor for DepthWalk {
	type Error = std::convert::Infallible;

	fn visit_expr(&mut self, expr: &Expr) -> Result<(), Self::Error> {
		let entered = self.depth;
		self.depth += 1;
		if let Expr::FunctionCall(call) = expr {
			match &call.receiver {
				Function::Custom(name) => {
					let at = self.found.calls.entry(name.clone()).or_insert(self.depth);
					*at = (*at).min(self.depth);
				}
				Function::Normal(name) if name == "api::invoke" => {
					let depth = self.depth;
					self.found.api_invoke =
						Some(self.found.api_invoke.map_or(depth, |at| at.min(depth)));
				}
				Function::Normal(_)
				| Function::Script(_)
				| Function::Model(_)
				| Function::Module(..)
				| Function::Silo {
					..
				} => {}
			}
		}
		let processes_records = matches!(
			expr,
			Expr::Select(_)
				| Expr::Create(_)
				| Expr::Update(_)
				| Expr::Upsert(_)
				| Expr::Delete(_)
				| Expr::Relate(_)
				| Expr::Insert(_)
		);
		if processes_records {
			self.depth += RECORDS_DEPTH;
			if !matches!(expr, Expr::Select(_)) {
				self.found.write = Some(self.found.write.map_or(entered, |at| at.min(entered)));
			}
		}
		// The arguments of a call are measured where the call spends its own
		// re-entry ([`super::program::CallDepth`]), with the body it enters.
		if self.in_call_args == 0 {
			self.found.deepest = self.found.deepest.max(self.depth);
		}
		let custom = matches!(expr, Expr::FunctionCall(call) if matches!(call.receiver, Function::Custom(_)));
		if custom {
			self.in_call_args += 1;
		}
		let visited = expr.visit(self);
		if custom {
			self.in_call_args -= 1;
		}
		self.depth = entered;
		visited
	}

	/// A block spends one for each statement it evaluates, except a `LET`,
	/// which binds what its value spends and spends nothing of its own.
	fn visit_block(&mut self, block: &Block) -> Result<(), Self::Error> {
		for statement in block.iter() {
			if let Expr::Let(_) = statement {
				statement.visit(self)?;
			} else {
				self.visit_expr(statement)?;
			}
		}
		Ok(())
	}
}
