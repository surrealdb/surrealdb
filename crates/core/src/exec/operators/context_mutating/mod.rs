//! Context-mutating operators.
//!
//! These operators implement `mutates_context() = true` and provide
//! `output_context()` to compute the modified execution context.
//!
//! - `UsePlan`: Switches namespace and/or database context
//! - `LetPlan`: Binds a value to a parameter name
//! - `BeginPlan`: Starts a write transaction
//! - `CommitPlan`: Commits the current transaction
//! - `CancelPlan`: Cancels/rolls back the current transaction

mod begin_plan;
mod cancel_plan;
mod commit_plan;
mod let_plan;
mod use_plan;

pub use begin_plan::BeginPlan;
pub use cancel_plan::CancelPlan;
pub use commit_plan::CommitPlan;
pub use let_plan::LetPlan;
pub use use_plan::UsePlan;
