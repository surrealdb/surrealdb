//! Relational join operators.
//!
//! Two operators pair the rows of a left (probe) input with the rows of a
//! right (build or lookup) input:
//!
//! - [`HashJoin`] materialises the right side into a table keyed by the join key and streams the
//!   left side against it. It serves two row conventions: the binding rows of the GQL `MATCH`
//!   planner (`exec/planner/match_plan.rs`), keyed on shared binding names, and the document rows
//!   of a SurrealQL pipeline, keyed by expressions and merged under an alias — the physical form of
//!   a `LogicalOp::Join` whose right side is not addressed by record id.
//! - [`LookupJoin`] reads the right side by key: for each left batch it evaluates the join key,
//!   reads the right table's records with those ids in one batch, runs the right plan over them and
//!   pairs. The physical form of a `LogicalOp::Join` whose right side is a table keyed on its `id`
//!   — an `IN (SELECT VALUE id FROM t …)`.
//!
//! Both are language-neutral: they carry no GQL IR types and evaluate their
//! keys through `PhysicalExpr`. The relational strategies a SQL planner
//! additionally needs — nested-loop (theta joins) and sort-merge — belong
//! here alongside them.

pub(crate) mod hash_join;
pub(crate) mod lookup_join;

pub use hash_join::{HashJoin, JoinType};
pub use lookup_join::{LookupJoin, RecordBatchFetch};
use surrealdb_iam::AuthLimit;

use crate::exec::ExecutionContext;

/// The permission frame a join lifted out of a `PERMISSIONS FOR select`
/// predicate evaluates in: its keys, residual and right side run with
/// permission checks bypassed, under the definer's recorded authority, as the
/// predicate they came from would by `PermissionFilter`.
#[derive(Debug, Clone)]
pub struct JoinFrame {
	/// The authority the join's expressions run under; `None` runs as the
	/// caller.
	pub(crate) auth: Option<AuthLimit>,
	/// Whether the plan is compiled for a caller the permission is enforced
	/// on, who must not learn the definer's expressions or plan from
	/// `EXPLAIN`.
	pub(crate) checked_caller: bool,
}

impl JoinFrame {
	pub(crate) fn new(auth: Option<AuthLimit>, checked_caller: bool) -> Self {
		Self {
			auth,
			checked_caller,
		}
	}

	/// The context the join's expressions and right side evaluate in.
	pub(crate) fn context(&self, ctx: &ExecutionContext) -> ExecutionContext {
		let frame = ctx.clone().with_skip_fetch_perms(true);
		match &self.auth {
			Some(limit) => frame.with_limited_auth(limit),
			None => frame,
		}
	}
}
