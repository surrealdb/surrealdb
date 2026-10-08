//! Graph-traversal operators — binding-table execution for GQL v2.
//!
//! These operators implement the graph-specific part of executing a lowered
//! [`crate::expr::match_plan::MatchPlan`]: each produces rows that are
//! `Value::Object`s keyed by binding name (the binding-row convention, see
//! `doc/gql/V2_DESIGN.md` §3). [`expand::Expand`] performs a single-hop
//! graph traversal, binding the edge and reached node into each row;
//! [`path_expand::PathExpand`] performs variable-length / quantified traversal,
//! emitting one row per path; [`endpoint::EndpointBind`] binds a node from a
//! bound edge's `in`/`out` endpoint (for edge-anchored patterns);
//! [`distinct_edges::DistinctEdges`] enforces the per-MATCH DIFFERENT EDGES
//! default (R2) across a clause's edge bindings.
//!
//! The language-neutral operators these compose with live elsewhere: the anchor
//! [`Bind`](crate::exec::operators::bind::Bind), whole-row
//! [`Distinct`](crate::exec::operators::distinct::Distinct), and
//! [`HashJoin`](crate::exec::operators::join::hash_join::HashJoin).
//!
//! Every binding fetch (Expand / EndpointBind / PathExpand targets and edges)
//! goes through
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

#[cfg(test)]
pub use distinct_edges::DistinctEdges;
#[cfg(test)]
pub use endpoint::EndpointBind;
pub use endpoint::EndpointField;
#[cfg(test)]
pub use expand::Expand;
pub use expand::{EdgeBinding, ExpandDir};
#[cfg(test)]
pub use path_expand::PathExpand;
pub use path_expand::PathMode;
#[cfg(test)]
pub use shortest_path_expand::ShortestPathExpand;
pub use shortest_path_expand::ShortestSelector;
