//! The lowering's registration with the IR's registry.
//!
//! Lowering contributes no pattern, rule, analysis or pass: it reads the physical plan and
//! decides nothing, so there is nothing for the registry to hold.

use crate::exec::ir::registry::Registry;

/// Registers the lowering's physical-dialect rules and analyses with `_r`. It registers none.
pub(crate) fn register(_r: &mut Registry) {}
