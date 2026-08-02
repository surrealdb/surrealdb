//! SurrealQL's pure function library.
//!
//! Every function here answers from its arguments alone. That is what lets the
//! crate sit below the executor: the executor's generated builtins call
//! downward into these, and the planner can fold a call to one of them at plan
//! time because there is nothing to consult.
//!
//! Functions that need the engine to answer — a record read, a socket, a
//! script, a user closure — live above this crate, beside the service each one
//! needs. Both halves register into the same function registry, so a query
//! cannot tell where a function came from.
//!
//! **This crate is an internal implementation detail of SurrealDB.** Its API is
//! unstable and changes without notice.

pub mod args;
pub mod array;
pub mod bytes;
pub mod count;
pub mod crypto;
pub mod duration;
pub mod geo;
pub mod math;
pub mod not;
pub mod object;
pub mod parse;
pub mod rand;
pub mod set;
pub mod string;
pub mod time;
pub mod r#type;
pub mod util;
pub mod vector;
