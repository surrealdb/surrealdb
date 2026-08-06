//! Whether a stored expression can modify data anywhere in its own tree.
//!
//! Contrast with [`Expr::has_direct_write`], which inspects only the expression
//! itself and its immediate combinators: it treats subqueries, idioms and
//! closures as opaque because the `PERMISSIONS` clauses it guards have a runtime
//! backstop (`Options::new_for_permission_predicate`). A `COMPUTED` field body
//! has no such backstop — it is evaluated on every read of the field — so its
//! check walks the whole tree.
//!
//! A function *call* stays opaque in both: the callee's body is stored
//! separately and can be redefined after this expression is, so definition time
//! cannot be sound about it. Call *arguments* are evaluated in place and are
//! therefore walked.

use crate::expr::Expr;
use crate::expr::visit::{Visit, Visitor};

/// Stops the walk as soon as a mutating construct is reached.
struct FoundMutation;

struct MutationScanner;

impl Visitor for MutationScanner {
	type Error = FoundMutation;

	/// Walk an `Expr`, failing on anything that modifies data.
	///
	/// The match is **exhaustive (no `_` arm) on purpose**, matching
	/// [`crate::expr::computed_deps`]: adding an `Expr` variant should be a
	/// build error here so a human classifies it, rather than a new statement
	/// kind silently passing a check whose whole job is to reject writes.
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
			| Expr::Alter(_) => Err(FoundMutation),

			// A GQL plan carries its mutations in its stages.
			Expr::Match(plan) => {
				if plan.has_mutations() {
					Err(FoundMutation)
				} else {
					expr.visit(self)
				}
			}

			// Everything else delegates to the default traversal, which
			// descends into blocks, subqueries, idiom parts, closure bodies and
			// call arguments — so a write buried in any of them is still found.
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
			} => expr.visit(self),
		}
	}
}

impl Expr {
	/// Whether this expression's own tree contains a data-modifying statement,
	/// looking through subqueries, idiom parts, blocks and closure bodies.
	///
	/// A call to a user-defined function, script, module or silo is opaque: its
	/// body is stored separately and can change after this expression is, so a
	/// definition-time answer about it would not stay true. Arguments to such a
	/// call *are* walked, since they are evaluated at the call site.
	pub fn contains_mutation(&self) -> bool {
		MutationScanner.visit_expr(self).is_err()
	}
}
