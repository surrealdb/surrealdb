//! Whether a stored expression can modify data anywhere in its own tree.
//!
//! Used at definition time to reject a `COMPUTED` field body that writes: the
//! walk descends into subqueries, idioms, blocks and closure bodies, so a
//! mutation anywhere inside the body itself is found.
//!
//! A function *call* stays opaque: the callee's body lives in the catalog,
//! which this crate cannot see, and whether it writes may depend on which
//! branch the arguments select. Those are left to the runtime write refusal
//! (`Options::no_write`), which fires at the point a write is actually
//! reached. Call *arguments* are evaluated in place and are therefore walked.

use crate::expr::Expr;
use crate::expr::visit::{Visit, Visitor};

/// Stops the walk as soon as a mutating construct is reached.
struct FoundMutation;

struct MutationScanner;

impl Visitor for MutationScanner {
	type Error = FoundMutation;

	/// Walk an `Expr`, failing on anything that modifies data.
	///
	/// The classification itself lives in [`Expr::is_mutation_statement`], so
	/// the deep walk here and the runtime write refusal cannot disagree about
	/// what counts as a mutation.
	fn visit_expr(&mut self, expr: &Expr) -> Result<(), Self::Error> {
		if expr.is_mutation_statement() {
			return Err(FoundMutation);
		}
		// Everything else delegates to the default traversal, which descends
		// into blocks, subqueries, idiom parts, closure bodies and call
		// arguments — so a write buried in any of them is still found.
		expr.visit(self)
	}
}

impl Expr {
	/// Whether this node is itself a data-modifying statement or DDL.
	///
	/// Shallow: it classifies this node alone and descends into nothing. Use
	/// [`contains_mutation`](Self::contains_mutation) to ask about a whole tree.
	///
	/// The match is **exhaustive (no `_` arm) on purpose**, matching
	/// [`crate::expr::computed_deps`]: adding an `Expr` variant should be a
	/// build error here so a human classifies it, rather than a new statement
	/// kind silently passing a check whose whole job is to reject writes.
	///
	/// Both readers of this question call it — the definition-time walk in this
	/// module, and the runtime write refusal that `Options::no_write` drives —
	/// so the two cannot drift apart.
	pub fn is_mutation_statement(&self) -> bool {
		match self {
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
			| Expr::Alter(_) => true,

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
			} => false,
		}
	}

	/// Whether this expression's own tree contains a data-modifying statement,
	/// looking through subqueries, idiom parts, blocks and closure bodies.
	///
	/// A call to a user-defined function, script, module or silo is opaque:
	/// its body lives outside this expression, and is left to the runtime
	/// write refusal. Arguments to such a call *are* walked, since they are
	/// evaluated at the call site.
	pub fn contains_mutation(&self) -> bool {
		MutationScanner.visit_expr(self).is_err()
	}
}
