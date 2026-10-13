use std::sync::Arc;

use crate::exec::{EvalContext, ExecutionContext};

/// The context a subquery evaluated in `ctx` runs in.
///
/// The enclosing row is bound as `$parent` and `$this`, which lets a
/// correlated subquery such as `(SELECT ... FROM $this.field)` reach the outer
/// document, and is the current row when `inherits_row`. `document_root` is
/// preferred over `current_value` when both are set: nested evaluation (view
/// projections, graph traversals) may advance `current_value` along a field
/// chain while `document_root` still denotes the outer row `$parent` names.
/// The permission-skip flag carries in, so that inner plans executed during a
/// permission predicate do not re-enter table permission checks, which would
/// recurse on cyclic record links. A statement evaluated inside a
/// computed-field body inherits the record being computed, so a dereference of
/// that record in the statement is answered from the row rather than by
/// re-fetching it, which would evaluate the body again. With no row where a
/// KNN query vector's evaluation forbids the value in scope, the statement
/// binds `$parent` and `$this` as forbidden
/// ([`ExecutionContext::entering_statement`]).
pub(crate) fn subquery_context(ctx: &EvalContext<'_>, inherits_row: bool) -> ExecutionContext {
	let subquery_ctx = ctx.exec_ctx.entering_statement(ctx.document(), inherits_row);
	let subquery_ctx = if ctx.skip_fetch_perms {
		subquery_ctx.with_skip_fetch_perms(true)
	} else {
		subquery_ctx
	};
	match &ctx.computing_record {
		Some(record) => subquery_ctx.with_computing_record(Some(Arc::clone(record))),
		None => subquery_ctx,
	}
}
