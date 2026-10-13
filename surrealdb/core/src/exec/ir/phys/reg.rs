//! Registration for the physical dialect: its conversion patterns, its physical patterns and its
//! attribute syntax.
//!
//! This module registers one conversion pattern per logical tag it converts, the physical
//! rewrites of [`super::rewrite`] and the physical dialect's attribute syntax. The rules for
//! common subexpressions and dead code in this dialect are defined outside this module.

use super::print::PhysSyntax;
use super::{conversion, rewrite};
use crate::exec::ir::registry::Registry;

/// The index of the physical dialect's entry in [`Registry::attr_syntax`].
const PHYS_SYNTAX: usize = 2;

/// Registers the physical dialect's conversion patterns, rewrites and attribute syntax with `r`.
pub(crate) fn register(r: &mut Registry) {
	for pattern in conversion::patterns() {
		r.conversion.add(pattern);
	}
	for pattern in rewrite::patterns() {
		r.phys_patterns.add(pattern);
	}
	r.attr_syntax[PHYS_SYNTAX] = Some(&PhysSyntax);
}
