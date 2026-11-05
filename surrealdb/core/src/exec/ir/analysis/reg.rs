//! Registers the names of the analyses this module provides.

use crate::exec::ir::registry::Registry;

/// The analyses, in registration order.
const ANALYSES: [&str; 16] = [
	"effects",
	"fallibility",
	"footprint",
	"write_epoch",
	"identity",
	"types",
	"multiplicity",
	"row_dep",
	"liveness",
	"frames",
	"slice",
	"certainty",
	"loop_legality",
	"phys_effects",
	"lowers",
	"selectivity",
];

/// Registers the names of the analyses with `r`, in a fixed order.
pub(crate) fn register(r: &mut Registry) {
	r.analyses.0.extend(ANALYSES);
}
