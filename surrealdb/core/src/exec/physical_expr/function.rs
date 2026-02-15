//! Function physical expressions for the streaming executor.
//!
//! This module contains separate PhysicalExpr types for each function variant:
//! - `BuiltinFunctionExec` - built-in functions like `math::abs`, `string::len`
//! - `UserDefinedFunctionExec` - user-defined `fn::` functions stored in the database
//! - `JsFunctionExec` - embedded JavaScript functions
//! - `ModelFunctionExec` - ML model inference functions
//! - `SurrealismModuleExec` - Surrealism WASM module functions
//! - `SiloModuleExec` - versioned Silo package functions
//! - `ClosureExec` / `ClosureCallExec` - closure creation and invocation
//! - `ProjectionFunctionExec` - projection functions (type::field, type::fields)
//! - `IndexFunctionExec` - index-bound functions (search::*, vector::distance::knn)

mod builtin;
mod closure;
pub(crate) mod helpers;
mod index;
mod model;
mod module;
mod projection;
mod script;
mod user_defined;

// Re-export all expression types
pub(crate) use builtin::BuiltinFunctionExec;
pub(crate) use closure::{ClosureCallExec, ClosureExec};
pub(crate) use helpers::validate_return;
pub(crate) use index::IndexFunctionExec;
pub(crate) use model::ModelFunctionExec;
pub(crate) use module::{SiloModuleExec, SurrealismModuleExec};
pub(crate) use projection::ProjectionFunctionExec;
pub(crate) use script::JsFunctionExec;
pub(crate) use user_defined::UserDefinedFunctionExec;
