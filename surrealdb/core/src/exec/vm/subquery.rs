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
/// re-fetching it, which would evaluate the body again.
pub(crate) fn subquery_context(ctx: &EvalContext<'_>, inherits_row: bool) -> ExecutionContext {
	let outer_row = ctx.document_root.or(ctx.current_value);
	let subquery_ctx = if let Some(parent_value) = outer_row {
		let derived = ctx
			.exec_ctx
			.with_param("parent", parent_value.clone())
			.with_param("this", parent_value.clone());
		if inherits_row {
			derived.with_current_row(Arc::new(parent_value.clone()))
		} else {
			derived
		}
	} else {
		ctx.exec_ctx.clone()
	};
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
