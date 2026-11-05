//! The recursion strategies the VM's recursion steps run: bounded and
//! unbounded recursive graph traversal with the default, collect, path,
//! shortest and `@` (repeat) strategies. Each strategy evaluates a body — the
//! path applied at each depth, supplied through [`DepthBody`] or
//! [`RepeatBody`] — until depth bounds, dead ends, or cycles are reached.
//!
//! ## RecordId enforcement
//!
//! Recursion is intended purely for RecordId graph traversal. The
//! `is_recursion_target` helper enforces this: only `RecordId` values (and
//! arrays containing them) are valid recursion targets. All other types
//! (String, Number, Object, Uuid, etc.) are treated as terminal and stop
//! recursion at that branch.
//!
//! ## Stack safety
//!
//! All strategies are fully iterative and use no stack recursion:
//!
//! - **Default, Collect, Path, Shortest**: Loop-based, safe at any depth.
//! - **RepeatRecurse (`@`) tree-building**: Uses a two-phase iterative approach (forward BFS
//!   discovery + backward bottom-up assembly). In the discovery phase, `@` writes its inputs to a
//!   shared sink and returns immediately. In the assembly phase, `@` does a cache lookup for
//!   pre-computed results. Neither phase uses stack recursion.

mod collect;
mod common;
mod default;
mod path;
mod repeat;
mod shortest;

pub(crate) use collect::evaluate_recurse_collect_with;
pub(crate) use common::{DepthBody, RecursionBounds};
pub(crate) use default::evaluate_recurse_default_with;
pub(crate) use path::evaluate_recurse_path_with;
pub(crate) use repeat::{RepeatBody, evaluate_recurse_iterative_with, evaluate_repeat_recurse};
pub(crate) use shortest::evaluate_recurse_shortest_with;

#[cfg(test)]
mod tests {

	// =========================================================================
	// Shared fixtures and helpers
	//
	// The strategy submodules are descendants of this module, so they reach
	// these through `super::super::tests::…`.
	// =========================================================================

	/// The system `idiom_recursion_limit` a context built by `TestDb` carries.
	pub(super) const SYSTEM_LIMIT: u32 = 256;

	// =========================================================================
	// RecursionOp — dispatch
	// =========================================================================
}
