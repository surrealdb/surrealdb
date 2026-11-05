//! The state planning keeps: the per-planning catalog memo, the value-dependence record, the
//! slot table and the shape digest.
//!
//! Nothing here outlives one planning session. A catalog definition is memoised only for the
//! session that read it and is dropped when planning ends: no node-wide or version-keyed catalog
//! structure exists, and no program is cached in memory. The cache keys and premises of
//! [`iface::premise`](crate::exec::ir::iface::premise) are interfaces. Their producer and
//! consumer is a key-value program cache outside this module.
//!
//! - [`planning`] holds the per-planning memo of catalog definitions. It reads only through the
//!   requesting transaction and answers each lookup from what it holds.
//! - [`deps`] records which decision consumed which value. A policy uses this record to choose
//!   between folding a value into a program and reading it from a slot, without changing the IR. It
//!   also holds the recorder a planning session writes the record through.
//! - [`slots`] numbers the free `$` names of a statement. A slot is a VM run-time parameter read.
//! - [`shape`] digests a statement's structure into a process-local key.
//! - [`names`] collects the tables, functions and parameters a statement and its definitions reach,
//!   which is the set the memo loads before planning.
//! - [`answer`] defines the answers the memo gives and the plan-time value classes they use.

pub(crate) mod answer;
pub(crate) mod deps;
pub(crate) mod names;
pub(crate) mod planning;
pub(crate) mod reg;
pub(crate) mod shape;
pub(crate) mod slots;
#[cfg(test)]
mod tests;

// The bound is frozen in `deps`; this is the path the rest of the crate names it by.
#[allow(unused_imports)]
pub(crate) use deps::MAX_VALUE_DEPS;
