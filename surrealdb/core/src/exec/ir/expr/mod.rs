//! The expression dialect: the scalar operations of a statement.
//!
//! An `expr` op computes one value from its operands. Its attributes say which operation it is
//! and nothing about where or how it runs.
//!
//! The dialect is split by the question each file answers:
//! - [`ops`]: the operations and their attribute payloads;
//! - [`effects`]: what an op does, how it can fail, and the structural facts of its shape;
//! - [`digest`]: the identity of an op's attributes;
//! - [`membership`]: whether a statement in value position is read as a membership set;
//! - [`print`]: the text form of those attributes;
//! - [`types`]: the kind the language guarantees for an op's result;
//! - [`verify`]: the rules an op must satisfy beyond the container's generic ones;
//! - [`reg`]: the registration of the attribute syntax.
//!
//! Binding a logical expression into these ops is `build::expr`, which reads the answers of these
//! files and decides nothing of its own.

pub(crate) mod digest;
pub(crate) mod effects;
pub(crate) mod membership;
pub(crate) mod ops;
pub(crate) mod print;
pub(crate) mod types;
pub(crate) mod verify;

#[cfg(test)]
mod tests;
