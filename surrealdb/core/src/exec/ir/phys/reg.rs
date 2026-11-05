//! Registration for the physical dialect: its conversion patterns and its physical patterns.
//!
//! This module registers one conversion pattern per logical tag it converts and the physical
//! rewrites of [`super::rewrite`]. The rules for
//! common subexpressions and dead code in this dialect are defined outside this module.

use super::{conversion, rewrite};
use crate::exec::ir::registry::Registry;

/// Registers the physical dialect's conversion patterns and rewrites with `r`.
pub(crate) fn register(r: &mut Registry) {
	for pattern in conversion::patterns() {
		r.conversion.add(pattern);
	}
	for pattern in rewrite::patterns() {
		r.phys_patterns.add(pattern);
	}
}
