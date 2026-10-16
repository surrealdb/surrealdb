//! Building a container from a bound request: the expression, logical and physical builders.
//!
//! The builders are the only code that creates ops from the planner's earlier types. Each
//! interns the frame an op runs under and assigns the region kinds by position.

pub(crate) mod expr;
pub(crate) mod field_paths;
pub(crate) mod field_view;
pub(crate) mod inline;
pub(crate) mod logical;
pub(crate) mod lookup;
pub(crate) mod match_fold;
pub(crate) mod membership;
pub(crate) mod phys;
pub(crate) mod stage_bundle;
pub(crate) mod stmt;
pub(crate) mod subscription;
