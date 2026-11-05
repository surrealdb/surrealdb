//! Function call forms for the engine.
//!
//! This module contains a separate type for each function variant:
//! - `BuiltinFunctionExec` - built-in functions like `math::abs`, `string::len`
//! - `UserDefinedFunctionExec` - user-defined `fn::` functions stored in the database
//! - `JsFunctionExec` - embedded JavaScript functions
//! - `ModelFunctionExec` - ML model inference functions
//! - `SurrealismModuleExec` - Surrealism WASM module functions
//! - `SiloModuleExec` - versioned Silo package functions
//! - `ClosureExec` / `ClosureCallExec` - closure creation and invocation
//! - `ProjectionFunctionExec` - projection functions (type::field, type::fields)
//! - `IndexFunctionExec` - index-bound functions (search::*, vector::distance::knn)

mod closure;
pub(crate) mod helpers;
mod model;
mod module;
mod script;

// Re-export all expression types
pub(crate) use closure::{CompiledClosure, StreamingClosureEvaluator, bind_closure_args};
pub(crate) use model::{compute_model, enter_model};
#[cfg(not(feature = "surrealism"))]
pub(crate) use module::module_runtime_missing;
#[cfg(feature = "surrealism")]
pub(crate) use module::{ModuleExport, module_export, read_module, run_module_export};
pub(crate) use script::{check_script_allowed, run_script};

pub(crate) use super::function::EnteredCall;
