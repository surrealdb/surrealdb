//! SQL statement and expression binders.

pub mod mysql;
pub mod pgsql;

use crate::error::PlanError;
use crate::ir::BoundWindowSpec;

/// Refuse a window `spec` that changes what the window it refers to (`base`, named `name`)
/// declares: it may add an ORDER BY only to a window that has none, and may not add a PARTITION BY
/// or copy a window that has a frame. A reference that adds nothing reads the window as it is.
pub(crate) fn check_inherited_window(
	name: &str,
	base: &BoundWindowSpec,
	adds_partition: bool,
	adds_order: bool,
	adds_frame: bool,
) -> Result<(), PlanError> {
	if adds_partition {
		return Err(PlanError::WindowPartitionOverride(name.to_string()));
	}
	if adds_order && !base.order_by.is_empty() {
		return Err(PlanError::WindowOrderOverride(name.to_string()));
	}
	if (adds_order || adds_frame) && base.frame.is_some() {
		return Err(PlanError::WindowFrameCopy(name.to_string()));
	}
	Ok(())
}
