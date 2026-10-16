//! The analyses over a container: effects, footprints, fallibility, frames, write epochs, value
//! identity, type knowledge, row dependence, liveness, loop legality, multiplicity, read
//! certainty and the compute slice of a loop.
//!
//! Every analysis is a logical-layer fact: it is computed from the logical ops, memoised in the
//! fact cache by op and generation, and never recomputed from the physical plan. An analysis that
//! cannot answer answers the top of its lattice, so a consumer built against it is conservative
//! and never wrong.
//!
//! Three modules answer for the physical layer without reading it: `phys_effects` folds the
//! provenance of a physical op, `lowers` folds the lowering an op's kind asserts, and
//! `selectivity` is the slot an estimate would fill. None reads statistics or the physical
//! catalog.

pub(crate) mod certainty;
pub(crate) mod cx;
pub(crate) mod effects;
pub(crate) mod existence;
pub(crate) mod fallibility;
pub(crate) mod footprint;
pub(crate) mod frames;
pub(crate) mod identity;
pub(crate) mod liveness;
pub(crate) mod loop_legality;
pub(crate) mod lowers;
pub(crate) mod multiplicity;
pub(crate) mod phys_effects;
pub(crate) mod reg;
pub(crate) mod row_dep;
pub(crate) mod selectivity;
pub(crate) mod slice;
pub(crate) mod tokens;
pub(crate) mod types;
pub(crate) mod write_epoch;

#[cfg(test)]
mod tests;
