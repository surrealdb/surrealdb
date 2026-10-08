//! Function physical expressions for the engine.
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
#[cfg(test)]
pub(crate) use builtin::builtin_can_write;
pub(crate) use closure::{
	ClosureCallExec, ClosureExec, CompiledClosure, StreamingClosureEvaluator, bind_closure_args,
};
pub(crate) use index::IndexFunctionExec;
pub(crate) use model::{ModelFunctionExec, compute_model, enter_model};
#[cfg(not(feature = "surrealism"))]
pub(crate) use module::module_runtime_missing;
#[cfg(feature = "surrealism")]
pub(crate) use module::{ModuleExport, module_export, read_module, run_module_export};
pub(crate) use module::{SiloModuleExec, SurrealismModuleExec};
pub(crate) use projection::ProjectionFunctionExec;
pub(crate) use script::{JsFunctionExec, check_script_allowed, run_script};
pub(crate) use user_defined::{
	BodyPlan, EnteredCall, FunctionCall, PlannedBody, RecursiveBody, UserDefinedFunctionExec,
};
