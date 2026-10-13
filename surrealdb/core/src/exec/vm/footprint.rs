//! The effect summary of a unit: what running it may do, and the tables it
//! may read and write.
//!
//! A unit's summary is derived from what it was compiled from, never
//! declared: the union of the declared effects of every builtin its scalars
//! call ([`crate::exec::logical::effects_of_plan`]), the tables its sources
//! and writes name, the tables its program reads records of by id
//! ([`super::dispatch::reach_of`]), and the user-defined functions it calls,
//! which it binds late: a function's footprint is its own and the footprints
//! of the functions it names, composed by whoever reads them.
//!
//! What the derivation cannot name is the whole of its kind: a source or a
//! write target evaluated when the statement runs, a graph or reference
//! lookup, a closure invoked through a value, and any body the call site
//! cannot see ([`EffectSet::OPAQUE_BODY`]) read and write any table. A
//! summary is therefore never smaller than what the unit does; a consumer
//! deciding placement or descriptor reloads from it errs only toward the
//! slower choice.
