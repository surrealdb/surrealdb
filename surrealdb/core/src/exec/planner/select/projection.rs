//! SELECT projection planning.
//!
//! Two paths converge here:
//!
//! - **`plan_projections_fast`**: the default. Pre-classifies every SELECT field via
//!   [`classify_select_field`], detects shadowing against [`ExpressionRegistry`] entries without
//!   mutating it, and only then commits to building a `SelectProject` (with an optional upstream
//!   `Compute` operator for non-trivial expressions).
//! - **`plan_projections`** (the slow path): falls back to the full `Project` operator when the
//!   fast path declines (projection function, multi-part alias, multi-part output path,
//!   source-field shadowing).
//!
//! [`ProjectedField`] is the type the classification phase returns.
//! Splitting classification from registry mutation is what makes shadowing
//! detection precise: the simulated commit uses an [`ExpressionRegistry`]
//! clone to compute the actual internal names that would be assigned and
//! checks them against simple-source-field reads. A discarded fast-path
//! attempt therefore never leaks Project-point entries into the real
//! registry.

use crate::exec::field_path::{FieldPath, FieldPathPart};
use crate::expr::logical::{OutputPath, OutputPathPart};

/// Where a projected value lands, as the path the `Project` operator writes
/// to: one key per [`OutputPath`] part, a lookup key included.
pub(crate) fn output_field_path(path: &OutputPath) -> FieldPath {
	FieldPath(
		path.parts()
			.iter()
			.map(|part| match part {
				OutputPathPart::Field(name) | OutputPathPart::Lookup(name) => {
					FieldPathPart::Field(name.clone())
				}
			})
			.collect(),
	)
}
