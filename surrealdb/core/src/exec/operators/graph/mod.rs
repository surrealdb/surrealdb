//! Graph-traversal steps of binding-table execution for GQL v2.
//!
//! These modules implement the graph-specific part of executing a lowered
//! [`crate::expr::match_plan::MatchPlan`], for the VM's binding steps
//! (`vm/binding.rs`): each step produces rows that are `Value::Object`s keyed by
//! binding name (the binding-row convention, see `doc/gql/V2_DESIGN.md` §3).
//! [`expand`] performs a single-hop graph traversal, binding the edge and reached
//! node into each row; [`path_expand`] performs variable-length / quantified
//! traversal, emitting one row per path; [`shortest_path_expand`] keeps only the
//! shortest paths; [`endpoint`] binds a node from a bound edge's `in`/`out`
//! endpoint (for edge-anchored patterns); [`distinct_edges`] enforces the
//! per-MATCH DIFFERENT EDGES default (R2) across a clause's edge bindings.
//!
//! Every binding fetch (expand / endpoint / path targets and edges) goes through
//! [`resolve_with_field_state`](crate::exec::operators::scan::fetch::resolve_with_field_state)
//! so that the contents of a binding are exactly what a `SELECT` on that table
//! would return — table-level SELECT permission, computed fields, and
//! field-level SELECT permissions all applied. See that module for the security
//! rationale.

pub(crate) mod distinct_edges;
pub(crate) mod endpoint;
pub(crate) mod expand;
pub(crate) mod path_expand;
pub(crate) mod shortest_path_expand;

pub use endpoint::EndpointField;
pub use expand::{EdgeBinding, ExpandDir};
pub use path_expand::PathMode;
pub use shortest_path_expand::ShortestSelector;
