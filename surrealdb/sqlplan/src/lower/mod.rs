//! Lowering from Bound IR to SurrealDB `LogicalOp`.

pub mod expr;
pub mod plan;

pub use plan::lower_bound_select;
