//! The logical dialect: statements, transactions, loops and the regions they own.
//!
//! A `logical` op says what a statement means, in terms of regions, operands and typed
//! attributes. It names no access path, no worker and no instruction.

pub(crate) mod access;
pub(crate) mod constraints;
pub(crate) mod ddl;
pub(crate) mod ddl_alter;
pub(crate) mod ddl_define;
pub(crate) mod ddl_remove;
pub(crate) mod digest;
pub(crate) mod effects;
pub(crate) mod live;
pub(crate) mod r#match;
pub(crate) mod ops;
pub(crate) mod print;
pub(crate) mod scope;
pub(crate) mod seam;
pub(crate) mod stages;
#[cfg(test)]
mod tests;
pub(crate) mod txn;
pub(crate) mod verify;
