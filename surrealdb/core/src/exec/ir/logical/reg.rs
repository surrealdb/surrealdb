//! Registration for the logical dialect: its attribute syntax.

use super::print::LogicalSyntax;
use crate::exec::ir::registry::Registry;

/// The syntax that prints and parses the attributes of every logical op.
static LOGICAL_SYNTAX: LogicalSyntax = LogicalSyntax;

/// Sets the logical dialect's attribute syntax in `r.attr_syntax[1]`, the slot reserved for
/// this dialect.
pub(crate) fn register(r: &mut Registry) {
	r.attr_syntax[1] = Some(&LOGICAL_SYNTAX);
}
