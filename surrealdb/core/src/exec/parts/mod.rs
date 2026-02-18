//! Physical part expressions for streaming execution.
//!
//! Each part is a `PhysicalExpr` that reads its input from `ctx.current_value`
//! and produces a new value. `IdiomExpr` chains parts together via `ctx.with_value()`.
//!
//! This module replaces the old `PhysicalPart` enum with individual structs
//! that each implement `PhysicalExpr`, following the same pattern as the
//! `physical_expr` and `operators` modules.

use std::sync::Arc;

use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::expr::FlowResult;
// Re-export recursion utilities from the canonical definitions in `expr::idiom::recursion`.
// These are shared between the legacy compute path and the streaming execution engine.
pub(crate) use crate::expr::idiom::recursion::{clean_iteration, get_final, is_final};
use crate::val::{RecordId, Value};

pub(crate) mod array_ops;
pub(crate) mod destructure;
pub(crate) mod field;
pub(crate) mod filter;
pub(crate) mod index;
pub(crate) mod lookup;
pub(crate) mod method;
pub(crate) mod optional;
pub(crate) mod recurse;

// Re-export part types
pub(crate) use array_ops::{AllPart, FirstPart, FlattenPart, LastPart};
pub(crate) use destructure::{DestructureField, DestructurePart};
pub(crate) use field::FieldPart;
pub(crate) use filter::WherePart;
pub(crate) use index::IndexPart;
pub(crate) use lookup::{LookupDirection, LookupPart};
pub(crate) use method::{ClosureFieldCallPart, MethodPart};
pub(crate) use optional::OptionalChainPart;
pub(crate) use recurse::{PhysicalRecurseInstruction, RecursePart, RepeatRecursePart};

// ============================================================================
// Shared utilities
// ============================================================================

/// Fetch a record, evaluate computed fields, and apply permissions.
///
/// Delegates to the unified [`fetch_record`](crate::exec::operators::fetch::fetch_record)
/// which handles raw fetching, computed field evaluation, and table/field-level
/// permission checks in one place.
pub(crate) async fn fetch_record_with_computed_fields(
	rid: &RecordId,
	ctx: EvalContext<'_>,
) -> anyhow::Result<Value> {
	crate::exec::operators::fetch::fetch_record(ctx.exec_ctx, rid).await.map_err(|cf| match cf {
		crate::expr::ControlFlow::Err(e) => e,
		other => anyhow::anyhow!("{}", other),
	})
}

/// Evaluate a path of PhysicalExpr parts against a value.
///
/// This helper function traverses a sequence of parts, applying each one
/// in order to the current value. Used by recursion and destructure aliased paths.
///
/// Note: This function uses `Box::pin` to support recursive evaluation through
/// `RepeatRecursePart` -> `RecursePart` -> `evaluate_physical_path` chains.
/// The recursion depth is bounded by the system recursion limit.
pub(crate) fn evaluate_physical_path<'a>(
	value: &'a Value,
	path: &'a [Arc<dyn PhysicalExpr>],
	ctx: EvalContext<'a>,
) -> crate::exec::BoxFut<'a, FlowResult<Value>> {
	Box::pin(async move {
		let mut current = value.clone();
		for part in path {
			current = part.evaluate(ctx.with_value(&current)).await?;
		}
		Ok(current)
	})
}
