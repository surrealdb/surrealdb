//! Function physical expressions for the streaming executor.
//!
//! This module contains separate PhysicalExpr types for each function variant:
//! - `BuiltinFunctionExpr` - built-in functions like `math::abs`, `string::len`
//! - `UserDefinedFunctionExpr` - user-defined `fn::` functions stored in the database
//! - `JsFunctionExpr` - embedded JavaScript functions
//! - `ModelFunctionExpr` - ML model inference functions
//! - `SurrealismModuleExpr` - Surrealism WASM module functions
//! - `SiloModuleExpr` - versioned Silo package functions

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use surrealdb_types::{SqlFormat, ToSql};

use crate::catalog::Permission;
use crate::catalog::providers::DatabaseProvider;
use crate::err::Error;
use crate::exec::physical_expr::{BlockPhysicalExpr, EvalContext, PhysicalExpr};
use crate::exec::planner::expr_to_physical_expr;
use crate::exec::{AccessMode, CombineAccessModes};
use crate::expr::{ControlFlow, FlowResult, Kind, Model, Script};
use crate::val::Value;

// =============================================================================
// Shared Helper Functions
// =============================================================================

/// Evaluate all argument expressions to values.
async fn evaluate_args(
	args: &[Arc<dyn PhysicalExpr>],
	ctx: EvalContext<'_>,
) -> FlowResult<Vec<Value>> {
	let mut values = Vec::with_capacity(args.len());
	for arg_expr in args {
		values.push(arg_expr.evaluate(ctx.clone()).await?);
	}
	Ok(values)
}

/// Check function permission.
async fn check_permission(
	permission: &Permission,
	func_name: &str,
	ctx: &EvalContext<'_>,
) -> FlowResult<()> {
	match permission {
		Permission::Full => Ok(()),
		Permission::None => Err(Error::FunctionPermissions {
			name: func_name.to_string(),
		}
		.into()),
		Permission::Specific(expr) => {
			// Plan and evaluate the permission expression
			match expr_to_physical_expr(expr.clone(), ctx.exec_ctx.ctx()).await {
				Ok(phys_expr) => {
					let result = phys_expr.evaluate(ctx.clone()).await?;
					if !result.is_truthy() {
						Err(Error::FunctionPermissions {
							name: func_name.to_string(),
						}
						.into())
					} else {
						Ok(())
					}
				}
				Err(_) => {
					// If we can't plan the expression, deny by default
					Err(Error::FunctionPermissions {
						name: func_name.to_string(),
					}
					.into())
				}
			}
		}
	}
}

/// Validate argument count against expected signature.
fn validate_arg_count(
	func_name: &str,
	actual: usize,
	expected: &[(String, Kind)],
) -> anyhow::Result<()> {
	let max_args = expected.len();
	// Count minimum required args (non-optional trailing args)
	let min_args = expected.iter().rev().fold(0, |acc, (_, kind)| {
		if kind.can_be_none() && acc == 0 {
			0
		} else {
			acc + 1
		}
	});

	if !(min_args..=max_args).contains(&actual) {
		return Err(Error::InvalidFunctionArguments {
			name: func_name.to_string(),
			message: match (min_args, max_args) {
				(1, 1) => "The function expects 1 argument.".to_string(),
				(r, t) if r == t => format!("The function expects {r} arguments."),
				(r, t) => format!("The function expects {r} to {t} arguments."),
			},
		}
		.into());
	}
	Ok(())
}

/// Validate and coerce return value to declared type.
pub(crate) fn validate_return(
	func_name: &str,
	return_kind: Option<&Kind>,
	result: Value,
) -> anyhow::Result<Value> {
	match return_kind {
		Some(kind) => result.coerce_to_kind(kind).map_err(|e| {
			Error::ReturnCoerce {
				name: func_name.to_string(),
				error: Box::new(e),
			}
			.into()
		}),
		None => Ok(result),
	}
}

/// Helper to compute access mode from arguments.
fn args_access_mode(args: &[Arc<dyn PhysicalExpr>]) -> AccessMode {
	args.iter().map(|e| e.access_mode()).combine_all()
}

/// Helper to compute the maximum required context from arguments.
fn args_required_context(args: &[Arc<dyn PhysicalExpr>]) -> crate::exec::ContextLevel {
	args.iter().map(|e| e.required_context()).max().unwrap_or(crate::exec::ContextLevel::Root)
}

// =============================================================================
// BuiltinFunctionExpr - for Function::Normal
// =============================================================================

/// Built-in function expression - math::abs(), string::len(), etc.
///
/// These functions are registered in the FunctionRegistry at startup.
#[derive(Debug, Clone)]
pub struct BuiltinFunctionExec {
	pub(crate) name: String,
	pub(crate) arguments: Vec<Arc<dyn PhysicalExpr>>,
	/// The required context level for this function (looked up at planning time).
	pub(crate) func_required_context: crate::exec::ContextLevel,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl PhysicalExpr for BuiltinFunctionExec {
	fn name(&self) -> &'static str {
		"BuiltinFunction"
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		// Built-in functions need either their declared context level or
		// whatever context their arguments need, whichever is higher
		let args_ctx = args_required_context(&self.arguments);
		args_ctx.max(self.func_required_context)
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> FlowResult<Value> {
		// Check if function is allowed by capabilities
		ctx.check_allowed_function(&self.name)?;

		// Look up the function in the registry
		let registry = ctx.exec_ctx.function_registry();
		let func = registry.get(&self.name).ok_or_else(|| {
			anyhow::anyhow!("Unknown function '{}' - not found in function registry", self.name)
		})?;

		// Evaluate all arguments
		let args = evaluate_args(&self.arguments, ctx.clone()).await?;

		// Invoke the function based on whether it's pure or needs context
		if func.is_pure() && !func.is_async() {
			Ok(func.invoke(args)?)
		} else {
			Ok(func.invoke_async(&ctx, args).await?)
		}
	}

	fn access_mode(&self) -> AccessMode {
		// api::invoke is read-write, everything else is read-only
		let func_mode = if self.name == "api::invoke" {
			AccessMode::ReadWrite
		} else {
			AccessMode::ReadOnly
		};
		func_mode.combine(args_access_mode(&self.arguments))
	}
}

impl ToSql for BuiltinFunctionExec {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str(&self.name);
		f.push_str("(...)");
	}
}

// =============================================================================
// UserDefinedFunctionExpr - for Function::Custom
// =============================================================================

/// User-defined function expression - fn::my_function(), etc.
///
/// These functions are stored in the database and retrieved at runtime.
#[derive(Debug, Clone)]
pub struct UserDefinedFunctionExec {
	/// Function name without the "fn::" prefix
	pub(crate) name: String,
	pub(crate) arguments: Vec<Arc<dyn PhysicalExpr>>,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl PhysicalExpr for UserDefinedFunctionExec {
	fn name(&self) -> &'static str {
		"UserDefinedFunction"
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		// User-defined functions are stored in the database, and arguments
		// may have their own context requirements
		args_required_context(&self.arguments).max(crate::exec::ContextLevel::Database)
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> FlowResult<Value> {
		let func_name = format!("fn::{}", self.name);

		// 1. Require database context
		let db_ctx = ctx.exec_ctx.database().map_err(|_| {
			anyhow::anyhow!("Custom function '{}' requires database context", func_name)
		})?;

		// 2. Check if function is allowed by capabilities
		if !ctx.capabilities().allows_function_name(&func_name) {
			return Err(Error::FunctionNotAllowed(func_name).into());
		}

		// 3. Retrieve function definition
		let ns_id = db_ctx.ns_ctx.ns.namespace_id;
		let db_id = db_ctx.db.database_id;
		let func_def = ctx
			.txn()
			.get_db_function(ns_id, db_id, &self.name)
			.await
			.map_err(|e| anyhow::anyhow!("Function '{}' not found: {}", func_name, e))?;

		// 4. Apply auth limiting — cap the caller's privileges to the definer's auth level,
		//    matching the old compute path behaviour.
		let auth_limit = crate::iam::AuthLimit::try_from(&func_def.auth_limit).map_err(|e| {
			anyhow::anyhow!("Invalid auth limit on function '{}': {}", func_name, e)
		})?;
		let limited_ctx = ctx.exec_ctx.with_limited_auth(&auth_limit);
		let ctx = EvalContext {
			exec_ctx: &limited_ctx,
			current_value: ctx.current_value,
			local_params: ctx.local_params,
			recursion_ctx: ctx.recursion_ctx,
			document_root: ctx.document_root,
		};

		// 5. Check permissions (with limited auth)
		check_permission(&func_def.permissions, &func_name, &ctx).await?;

		// 6. Evaluate all arguments
		let evaluated_args = evaluate_args(&self.arguments, ctx.clone()).await?;

		// 7. Validate argument count
		validate_arg_count(&func_name, evaluated_args.len(), &func_def.args)?;

		// 8. Create isolated context with function parameters bound
		let mut local_params: HashMap<String, Value> = HashMap::new();
		for ((param_name, kind), arg_value) in func_def.args.iter().zip(evaluated_args.into_iter())
		{
			let coerced =
				arg_value.coerce_to_kind(kind).map_err(|e| Error::InvalidFunctionArguments {
					name: func_name.clone(),
					message: format!("Failed to coerce argument `${param_name}`: {e}"),
				})?;
			local_params.insert(param_name.clone(), coerced);
		}

		// 9. Create a new execution context with the parameters
		let mut isolated_ctx = limited_ctx.clone();
		for (name, value) in &local_params {
			isolated_ctx = isolated_ctx.with_param(name.clone(), value.clone());
		}

		// 9. Execute the function block
		let block_expr = BlockPhysicalExpr {
			block: func_def.block.clone(),
		};
		let eval_ctx = EvalContext {
			exec_ctx: &isolated_ctx,
			current_value: ctx.current_value,
			local_params: Some(&local_params),
			recursion_ctx: None,
			document_root: None,
		};
		let result = match block_expr.evaluate(eval_ctx).await {
			Ok(v) => v,
			Err(ControlFlow::Return(v)) => v,
			Err(ControlFlow::Break) | Err(ControlFlow::Continue) => {
				// BREAK/CONTINUE inside a function (outside of loop) is an error
				return Err(Error::InvalidControlFlow.into());
			}
			Err(e) => return Err(e),
		};

		// 10. Validate and coerce return type
		Ok(validate_return(&func_name, func_def.returns.as_ref(), result)?)
	}

	fn access_mode(&self) -> AccessMode {
		// Custom functions are always potentially read-write
		AccessMode::ReadWrite.combine(args_access_mode(&self.arguments))
	}
}

impl ToSql for UserDefinedFunctionExec {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str("fn::");
		f.push_str(&self.name);
		f.push_str("(...)");
	}
}

// =============================================================================
// JsFunctionExpr - for Function::Script
// =============================================================================

/// JavaScript function expression - embedded script functions.
#[derive(Debug, Clone)]
pub struct JsFunctionExec {
	#[cfg_attr(not(feature = "scripting"), allow(unused_variables))]
	pub(crate) script: Script,
	pub(crate) arguments: Vec<Arc<dyn PhysicalExpr>>,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl PhysicalExpr for JsFunctionExec {
	fn name(&self) -> &'static str {
		"JsFunction"
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		// Script functions access database context through the frozen context
		// when needed, so they can operate at root level. Requiring Database
		// here would cause failures when no namespace/database is selected.
		crate::exec::ContextLevel::Root
	}

	#[cfg(feature = "scripting")]
	async fn evaluate(&self, ctx: EvalContext<'_>) -> FlowResult<Value> {
		use reblessive::TreeStack;

		use crate::doc::CursorDoc;
		use crate::fnc::script;

		// Get the frozen context and options
		let frozen_ctx = ctx.exec_ctx.ctx().clone();
		let opt = ctx
			.exec_ctx
			.options()
			.ok_or_else(|| anyhow::anyhow!("Script functions require Options context"))?
			.clone();

		// Check if scripting is allowed
		frozen_ctx.check_allowed_scripting()?;

		// Evaluate all arguments
		let args = evaluate_args(&self.arguments, ctx.clone()).await?;

		// Build CursorDoc from current value
		let doc = ctx.current_value.map(|v| CursorDoc::new(None, None, v.clone()));

		// Execute the script within a TreeStack context
		// This is required because JavaScript can call back into SurrealDB functions
		// via surrealdb.functions.* which need TreeStack for recursive computation
		let mut stack = TreeStack::new();
		Ok(stack
			.enter(|_stk| async {
				script::run(&frozen_ctx, &opt, doc.as_ref(), &self.script.0, args).await
			})
			.finish()
			.await?)
	}

	#[cfg(not(feature = "scripting"))]
	async fn evaluate(&self, _ctx: EvalContext<'_>) -> FlowResult<Value> {
		Err(Error::InvalidScript {
			message: String::from("Embedded functions are not enabled."),
		}
		.into())
	}

	fn access_mode(&self) -> AccessMode {
		// Script functions are always potentially read-write
		AccessMode::ReadWrite.combine(args_access_mode(&self.arguments))
	}
}

impl ToSql for JsFunctionExec {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str("function(...)");
	}
}

// =============================================================================
// ModelFunctionExpr - for Function::Model
// =============================================================================

/// ML model function expression - model inference.
#[derive(Debug, Clone)]
pub struct ModelFunctionExec {
	pub(crate) model: Model,
	pub(crate) arguments: Vec<Arc<dyn PhysicalExpr>>,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl PhysicalExpr for ModelFunctionExec {
	fn name(&self) -> &'static str {
		"ModelFunction"
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		// ML models are stored in the database, and arguments
		// may have their own context requirements
		args_required_context(&self.arguments).max(crate::exec::ContextLevel::Database)
	}

	#[cfg(feature = "ml")]
	async fn evaluate(&self, ctx: EvalContext<'_>) -> FlowResult<Value> {
		use surrealml_core::errors::error::SurrealError;
		use surrealml_core::execution::compute::ModelComputation;
		use surrealml_core::ndarray as mlNdarray;
		use surrealml_core::storage::surml_file::SurMlFile;

		use crate::expr::model::get_model_path;
		use crate::iam::Action;
		use crate::val::Number;

		const ARGUMENTS: &str = "The model expects 1 argument. The argument can be either a number, an object, or an array of numbers.";

		// Get the full name of this model
		let name = format!("ml::{}", self.model.name);

		// Check if this function is allowed
		ctx.check_allowed_function(&name)?;

		// Get the database context for model lookup
		let db_ctx = ctx
			.exec_ctx
			.database()
			.map_err(|_| anyhow::anyhow!("Model function '{}' requires database context", name))?;

		// Get namespace and database IDs
		let ns_id = db_ctx.ns_ctx.ns.namespace_id;
		let db_id = db_ctx.db.database_id;

		// Get the model definition
		let val = ctx
			.txn()
			.get_db_model(ns_id, db_id, &self.model.name, &self.model.version)
			.await?
			.ok_or_else(|| Error::MlNotFound {
				name: format!("{}<{}>", self.model.name, self.model.version),
			})?;

		// Calculate the model path using namespace and database names
		let ns_name = db_ctx.ns_name();
		let db_name = db_ctx.db_name();
		let path =
			get_model_path(ns_name, db_name, &self.model.name, &self.model.version, &val.hash);

		// Check permissions
		if ctx.exec_ctx.should_check_perms(Action::View)? {
			check_permission(&val.permissions, &self.model.name, &ctx).await?;
		}

		// Evaluate arguments
		let mut args = evaluate_args(&self.arguments, ctx.clone()).await?;

		// Validate argument count
		if args.len() != 1 {
			return Err(Error::InvalidFunctionArguments {
				name: format!("ml::{}<{}>", self.model.name, self.model.version),
				message: ARGUMENTS.into(),
			}
			.into());
		}

		// Take the first and only argument
		let argument = args.pop().expect("single argument validated above");

		match argument {
			// Perform buffered compute (with normalizers)
			Value::Object(v) => {
				let mut args = v
					.into_iter()
					.map(|(k, v)| {
						v.coerce_to::<f64>().map(|f| (k, f as f32)).map_err(|_| {
							Error::InvalidFunctionArguments {
								name: format!("ml::{}<{}>", self.model.name, self.model.version),
								message: ARGUMENTS.into(),
							}
						})
					})
					.collect::<Result<std::collections::HashMap<String, f32>, _>>()?;

				// Get the model file as bytes
				let bytes = crate::obs::get(&path).await?;

				// Run the compute in a blocking task
				let outcome: Vec<f32> = tokio::task::spawn_blocking(move || {
					let mut file = SurMlFile::from_bytes(bytes).map_err(|err: SurrealError| {
						anyhow::anyhow!("Failed to load model: {}", err.message)
					})?;
					let compute_unit = ModelComputation {
						surml_file: &mut file,
					};
					compute_unit.buffered_compute(&mut args).map_err(|err: SurrealError| {
						anyhow::anyhow!("Model computation failed: {}", err.message)
					})
				})
				.await
				.map_err(|e| anyhow::anyhow!("ML task failed: {e}"))??;

				// Convert the output to a value
				Ok(outcome.into_iter().map(|x| Value::Number(Number::Float(x as f64))).collect())
			}
			// Perform raw compute (number input)
			Value::Number(v) => {
				let args: f32 = Value::Number(v).coerce_to::<f64>().map_err(|_| {
					Error::InvalidFunctionArguments {
						name: format!("ml::{}<{}>", self.model.name, self.model.version),
						message: ARGUMENTS.into(),
					}
				})? as f32;

				// Get the model file as bytes
				let bytes = crate::obs::get(&path).await?;

				// Convert the argument to a tensor
				let tensor = mlNdarray::arr1::<f32>(&[args]).into_dyn();

				// Run the compute in a blocking task
				let outcome: Vec<f32> = tokio::task::spawn_blocking(move || {
					let mut file = SurMlFile::from_bytes(bytes).map_err(|err: SurrealError| {
						anyhow::anyhow!("Failed to load model: {}", err.message)
					})?;
					let compute_unit = ModelComputation {
						surml_file: &mut file,
					};
					compute_unit.raw_compute(tensor, None).map_err(|err: SurrealError| {
						anyhow::anyhow!("Model computation failed: {}", err.message)
					})
				})
				.await
				.map_err(|e| anyhow::anyhow!("ML task failed: {e}"))??;

				// Convert the output to a value
				Ok(outcome.into_iter().map(|x| Value::Number(Number::Float(x as f64))).collect())
			}
			// Perform raw compute (array input)
			Value::Array(v) => {
				let args = v
					.into_iter()
					.map(|x| x.coerce_to::<f64>().map(|x| x as f32))
					.collect::<Result<Vec<f32>, _>>()
					.map_err(|_| Error::InvalidFunctionArguments {
						name: format!("ml::{}<{}>", self.model.name, self.model.version),
						message: ARGUMENTS.into(),
					})?;

				// Get the model file as bytes
				let bytes = crate::obs::get(&path).await?;

				// Convert the argument to a tensor
				let tensor = mlNdarray::arr1::<f32>(&args).into_dyn();

				// Run the compute in a blocking task
				let outcome: Vec<f32> = tokio::task::spawn_blocking(move || {
					let mut file = SurMlFile::from_bytes(bytes).map_err(|err: SurrealError| {
						anyhow::anyhow!("Failed to load model: {}", err.message)
					})?;
					let compute_unit = ModelComputation {
						surml_file: &mut file,
					};
					compute_unit.raw_compute(tensor, None).map_err(|err: SurrealError| {
						anyhow::anyhow!("Model computation failed: {}", err.message)
					})
				})
				.await
				.map_err(|e| anyhow::anyhow!("ML task failed: {e}"))??;

				// Convert the output to a value
				Ok(outcome.into_iter().map(|x| Value::Number(Number::Float(x as f64))).collect())
			}
			// Invalid argument type
			_ => Err(Error::InvalidFunctionArguments {
				name: format!("ml::{}<{}>", self.model.name, self.model.version),
				message: ARGUMENTS.into(),
			}
			.into()),
		}
	}

	#[cfg(not(feature = "ml"))]
	async fn evaluate(&self, _ctx: EvalContext<'_>) -> FlowResult<Value> {
		Err(Error::InvalidModel {
			message: String::from("Machine learning computation is not enabled."),
		}
		.into())
	}

	fn access_mode(&self) -> AccessMode {
		// Model functions are read-only (inference doesn't mutate)
		AccessMode::ReadOnly.combine(args_access_mode(&self.arguments))
	}
}

impl ToSql for ModelFunctionExec {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.model.fmt_sql(f, fmt);
		f.push_str("(...)");
	}
}

// =============================================================================
// SurrealismModuleExpr - for Function::Module
// =============================================================================

/// Surrealism WASM module function expression.
#[derive(Debug, Clone)]
pub struct SurrealismModuleExec {
	pub(crate) module: String,
	pub(crate) sub: Option<String>,
	pub(crate) arguments: Vec<Arc<dyn PhysicalExpr>>,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl PhysicalExpr for SurrealismModuleExec {
	fn name(&self) -> &'static str {
		"SurrealismModule"
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		// Module functions require database context, and arguments
		// may have their own context requirements
		args_required_context(&self.arguments).max(crate::exec::ContextLevel::Database)
	}

	#[cfg(feature = "surrealism")]
	async fn evaluate(&self, ctx: EvalContext<'_>) -> FlowResult<Value> {
		use reblessive::TreeStack;

		use crate::doc::CursorDoc;
		use crate::expr::module::ModuleExecutable;

		// Build module and function names
		let mod_name = format!("mod::{}", self.module);
		let fnc_name = match &self.sub {
			Some(sub) => format!("{}::{}", mod_name, sub),
			None => mod_name.clone(),
		};

		// Check if this function is allowed
		ctx.check_allowed_function(&fnc_name)?;

		// Get the database context for module lookup
		let db_ctx = ctx.exec_ctx.database().map_err(|_| {
			anyhow::anyhow!("Module function '{}' requires database context", fnc_name)
		})?;

		// Get namespace and database IDs
		let ns_id = db_ctx.ns_ctx.ns.namespace_id;
		let db_id = db_ctx.db.database_id;

		// Get the module definition
		let val = ctx.txn().get_db_module(ns_id, db_id, &mod_name).await?;

		// Check permissions
		check_permission(&val.permissions, &mod_name, &ctx).await?;

		// Get the executable and signature
		let executable: ModuleExecutable = val.executable.clone().into();
		let frozen_ctx = ctx.exec_ctx.ctx();
		let signature =
			executable.signature(frozen_ctx, &ns_id, &db_id, self.sub.as_deref()).await?;

		// Evaluate all arguments
		let args = evaluate_args(&self.arguments, ctx.clone()).await?;

		// Validate argument count against signature
		if args.len() != signature.args.len() {
			return Err(Error::InvalidFunctionArguments {
				name: fnc_name,
				message: format!(
					"The function expects {} arguments, but {} were provided.",
					signature.args.len(),
					args.len()
				),
			}
			.into());
		}

		// Validate and coerce arguments to their expected types
		let mut coerced_args = Vec::with_capacity(args.len());
		for (arg, kind) in args.into_iter().zip(signature.args.iter()) {
			let coerced =
				arg.coerce_to_kind(kind).map_err(|e| Error::InvalidFunctionArguments {
					name: fnc_name.clone(),
					message: format!("Failed to coerce argument: {e}"),
				})?;
			coerced_args.push(coerced);
		}

		// Get the Options for the module execution
		let opt = ctx
			.exec_ctx
			.options()
			.ok_or_else(|| anyhow::anyhow!("Module functions require Options context"))?;

		// Build CursorDoc from current value
		let doc = ctx.current_value.map(|v| CursorDoc::new(None, None, v.clone()));

		// Run the module using the legacy stack-based execution
		let mut stack = TreeStack::new();
		let result = stack
			.enter(|stk| {
				executable.run(
					stk,
					frozen_ctx,
					opt,
					doc.as_ref(),
					coerced_args,
					self.sub.as_deref(),
				)
			})
			.finish()
			.await?;

		// Validate return value if signature specifies a return type
		validate_return(&fnc_name, signature.returns.as_ref(), result).map_err(Into::into)
	}

	#[cfg(not(feature = "surrealism"))]
	async fn evaluate(&self, _ctx: EvalContext<'_>) -> FlowResult<Value> {
		let name = match &self.sub {
			Some(s) => format!("mod::{}::{}", self.module, s),
			None => format!("mod::{}", self.module),
		};
		Err(anyhow::anyhow!(
			"Module function '{}' requires the 'surrealism' feature to be enabled",
			name
		)
		.into())
	}

	fn access_mode(&self) -> AccessMode {
		// Module functions are always potentially read-write
		AccessMode::ReadWrite.combine(args_access_mode(&self.arguments))
	}
}

impl ToSql for SurrealismModuleExec {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str("mod::");
		f.push_str(&self.module);
		if let Some(sub) = &self.sub {
			f.push_str("::");
			f.push_str(sub);
		}
		f.push_str("(...)");
	}
}

// =============================================================================
// SiloModuleExpr - for Function::Silo
// =============================================================================

/// Silo versioned package function expression.
#[derive(Debug, Clone)]
pub struct SiloModuleExec {
	pub(crate) org: String,
	pub(crate) pkg: String,
	pub(crate) major: u32,
	pub(crate) minor: u32,
	pub(crate) patch: u32,
	pub(crate) sub: Option<String>,
	pub(crate) arguments: Vec<Arc<dyn PhysicalExpr>>,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl PhysicalExpr for SiloModuleExec {
	fn name(&self) -> &'static str {
		"SiloModule"
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		// Silo package functions require database context, and arguments
		// may have their own context requirements
		args_required_context(&self.arguments).max(crate::exec::ContextLevel::Database)
	}

	async fn evaluate(&self, _ctx: EvalContext<'_>) -> FlowResult<Value> {
		let name = format!(
			"silo::{}::{}<{}.{}.{}>",
			self.org, self.pkg, self.major, self.minor, self.patch
		);
		Err(anyhow::anyhow!(
			"Silo function '{}' is not yet supported in the streaming executor",
			name
		)
		.into())
	}

	fn access_mode(&self) -> AccessMode {
		// Silo functions are always potentially read-write
		AccessMode::ReadWrite.combine(args_access_mode(&self.arguments))
	}
}

impl ToSql for SiloModuleExec {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str("silo::");
		f.push_str(&self.org);
		f.push_str("::");
		f.push_str(&self.pkg);
		f.push('<');
		f.push_str(&self.major.to_string());
		f.push('.');
		f.push_str(&self.minor.to_string());
		f.push('.');
		f.push_str(&self.patch.to_string());
		f.push('>');
		if let Some(sub) = &self.sub {
			f.push_str("::");
			f.push_str(sub);
		}
		f.push_str("(...)");
	}
}

// =============================================================================
// ClosurePhysicalExpr - kept as-is
// =============================================================================

/// Closure expression - |$x| $x * 2
#[derive(Debug, Clone)]
pub struct ClosureExec {
	pub(crate) closure: crate::expr::ClosureExpr,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl PhysicalExpr for ClosureExec {
	fn name(&self) -> &'static str {
		"Closure"
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		// Closures are just values, they don't need any special context
		crate::exec::ContextLevel::Root
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> FlowResult<Value> {
		use crate::dbs::ParameterCapturePass;
		use crate::val::Closure;

		// Capture parameters from the context that are referenced in the closure body
		let frozen_ctx = ctx.exec_ctx.ctx();
		let captures = ParameterCapturePass::capture(frozen_ctx, &self.closure.body);

		// Create a Value::Closure with the captured variables
		Ok(Value::Closure(Box::new(Closure::Expr {
			args: self.closure.args.clone(),
			returns: self.closure.returns.clone(),
			body: self.closure.body.clone(),
			captures,
		})))
	}

	fn access_mode(&self) -> AccessMode {
		// Closures themselves are read-only (they're values)
		// What they do when called is a different matter
		AccessMode::ReadOnly
	}
}

impl ToSql for ClosureExec {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.closure.fmt_sql(f, fmt);
	}
}

// =============================================================================
// ClosureCallExec - for invoking closures stored in parameters
// =============================================================================

/// Closure call expression - $closure(args...)
///
/// Invokes a closure value with the provided arguments. The target expression
/// must evaluate to a `Value::Closure`.
#[derive(Debug, Clone)]
pub struct ClosureCallExec {
	/// The expression that evaluates to a closure value
	pub(crate) target: Arc<dyn PhysicalExpr>,
	/// The argument expressions to pass to the closure
	pub(crate) arguments: Vec<Arc<dyn PhysicalExpr>>,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl PhysicalExpr for ClosureCallExec {
	fn name(&self) -> &'static str {
		"ClosureCall"
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		// The closure body may require any context level, so be conservative
		// and take the max of target and arguments
		let target_ctx = self.target.required_context();
		let args_ctx = args_required_context(&self.arguments);
		target_ctx.max(args_ctx)
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> FlowResult<Value> {
		use crate::val::Closure;

		// 1. Evaluate the target expression to get the closure
		let target_value = self.target.evaluate(ctx.clone()).await?;

		let closure = match target_value {
			Value::Closure(c) => c,
			other => {
				return Err(Error::InvalidFunction {
					name: "ANONYMOUS".to_string(),
					message: format!("'{}' is not a function", other.kind_of()),
				}
				.into());
			}
		};

		// 2. Evaluate all argument expressions
		let evaluated_args = evaluate_args(&self.arguments, ctx.clone()).await?;

		// 3. Invoke the closure based on its type
		match closure.as_ref() {
			Closure::Expr {
				args: arg_spec,
				returns,
				body,
				captures,
			} => {
				// Create isolated execution context with captured variables
				let mut isolated_ctx = ctx.exec_ctx.clone();
				for (name, value) in captures.clone() {
					isolated_ctx = isolated_ctx.with_param(name, value);
				}

				// Check for missing required arguments
				if arg_spec.len() > evaluated_args.len()
					&& let Some((param, kind)) =
						arg_spec[evaluated_args.len()..].iter().find(|(_, k)| !k.can_be_none())
				{
					return Err(Error::InvalidFunctionArguments {
						name: "ANONYMOUS".to_string(),
						message: format!(
							"Expected a value of type '{}' for argument {}",
							kind.to_sql(),
							param.to_sql()
						),
					}
					.into());
				}

				// Bind arguments to parameter names with type coercion
				let mut local_params: HashMap<String, Value> = HashMap::new();
				for ((param, kind), arg_value) in arg_spec.iter().zip(evaluated_args.into_iter()) {
					let coerced = arg_value.coerce_to_kind(kind).map_err(|_| {
						Error::InvalidFunctionArguments {
							name: "ANONYMOUS".to_string(),
							message: format!(
								"Expected a value of type '{}' for argument {}",
								kind.to_sql(),
								param.to_sql()
							),
						}
					})?;
					local_params.insert(param.clone().into_string(), coerced);
				}

				// Add parameters to the execution context
				for (name, value) in &local_params {
					isolated_ctx = isolated_ctx.with_param(name.clone(), value.clone());
				}

				// Execute the closure body
				let block_expr = BlockPhysicalExpr {
					block: crate::expr::Block(vec![body.clone()]),
				};
				let eval_ctx = EvalContext {
					exec_ctx: &isolated_ctx,
					current_value: ctx.current_value,
					local_params: Some(&local_params),
					recursion_ctx: None,
					document_root: None,
				};

				let result = match block_expr.evaluate(eval_ctx).await {
					Ok(v) => v,
					Err(ControlFlow::Return(v)) => v,
					Err(ControlFlow::Break) | Err(ControlFlow::Continue) => {
						// BREAK/CONTINUE inside a closure (outside of loop) is an error
						return Err(Error::InvalidControlFlow.into());
					}
					Err(e) => return Err(e),
				};

				// Coerce return value to declared type if specified
				Ok(validate_return("ANONYMOUS", returns.as_ref(), result)?)
			}
			Closure::Builtin(_) => {
				// Builtin closures are not yet supported in the streaming executor
				// They require the legacy compute path with Stk
				Err(anyhow::anyhow!(
					"Builtin closures are not yet supported in the streaming executor"
				)
				.into())
			}
		}
	}

	fn access_mode(&self) -> AccessMode {
		// Closures can potentially do anything, so be conservative
		AccessMode::ReadWrite
			.combine(self.target.access_mode())
			.combine(args_access_mode(&self.arguments))
	}
}

impl ToSql for ClosureCallExec {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.target.fmt_sql(f, fmt);
		f.push_str("(...)");
	}
}

// =============================================================================
// ProjectionFunctionExec - for type::field, type::fields
// =============================================================================

/// Projection function expression - type::field(), type::fields(), etc.
///
/// These functions produce field bindings rather than single values.
/// When used in SELECT projections, they expand into multiple output fields
/// with names derived from their arguments at runtime.
#[derive(Debug, Clone)]
pub struct ProjectionFunctionExec {
	pub(crate) name: String,
	pub(crate) arguments: Vec<Arc<dyn PhysicalExpr>>,
	/// The required context level for this function.
	pub(crate) func_required_context: crate::exec::ContextLevel,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl PhysicalExpr for ProjectionFunctionExec {
	fn name(&self) -> &'static str {
		"ProjectionFunction"
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		let args_ctx = args_required_context(&self.arguments);
		args_ctx.max(self.func_required_context)
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> FlowResult<Value> {
		// When evaluated as a regular expression (not in projection context),
		// return the first value from the projection bindings, or None if empty.
		// This handles cases like: RETURN type::field("name")
		if let Some(bindings) = self.evaluate_projection(ctx).await? {
			if bindings.len() == 1 {
				Ok(bindings.into_iter().next().expect("bindings verified non-empty").1)
			} else {
				// Multiple bindings - return as array of values
				Ok(Value::Array(bindings.into_iter().map(|(_, v)| v).collect()))
			}
		} else {
			Ok(Value::None)
		}
	}

	fn access_mode(&self) -> AccessMode {
		args_access_mode(&self.arguments)
	}

	fn is_projection_function(&self) -> bool {
		true
	}

	async fn evaluate_projection(
		&self,
		ctx: EvalContext<'_>,
	) -> FlowResult<Option<Vec<(crate::expr::idiom::Idiom, Value)>>> {
		// Look up the projection function in the registry
		let registry = ctx.exec_ctx.function_registry();
		let func = registry.get_projection(&self.name).ok_or_else(|| {
			anyhow::anyhow!(
				"Unknown projection function '{}' - not found in function registry",
				self.name
			)
		})?;

		// Evaluate all arguments
		let args = evaluate_args(&self.arguments, ctx.clone()).await?;

		// Invoke the projection function to get field bindings
		let bindings = func.invoke_async(&ctx, args).await?;

		Ok(Some(bindings))
	}
}

impl ToSql for ProjectionFunctionExec {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str(&self.name);
		f.push_str("(...)");
	}
}

// =============================================================================
// Index Function Expression (search::highlight, search::score, search::offsets,
//                            vector::distance::knn)
// =============================================================================

/// Index function expression - functions bound to WHERE clause predicates.
///
/// These functions are associated with an index operator in the WHERE condition
/// (e.g., MATCHES for full-text, `<|k,ef|>` for KNN). The planner resolves
/// the appropriate [`IndexContext`] at plan time based on the function's
/// declared [`IndexContextKind`].
///
/// Any plan-time reference arguments (e.g., match_ref) are extracted by the
/// planner and are NOT included in the stored arguments.
#[derive(Debug)]
pub struct IndexFunctionExec {
	pub(crate) name: String,
	pub(crate) arguments: Vec<Arc<dyn PhysicalExpr>>,
	/// Resolved index context (FullText or Knn).
	pub(crate) index_ctx: crate::exec::function::IndexContext,
	/// The required context level for this function.
	pub(crate) func_required_context: crate::exec::ContextLevel,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl PhysicalExpr for IndexFunctionExec {
	fn name(&self) -> &'static str {
		"IndexFunction"
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		let args_ctx = args_required_context(&self.arguments);
		args_ctx.max(self.func_required_context)
	}

	fn access_mode(&self) -> AccessMode {
		args_access_mode(&self.arguments)
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> FlowResult<Value> {
		// Look up the index function in the registry
		let registry = ctx.exec_ctx.function_registry();
		let func = registry.get_index_function(&self.name).ok_or_else(|| {
			anyhow::anyhow!(
				"Unknown index function '{}' - not found in function registry",
				self.name
			)
		})?;

		// Evaluate all arguments (any plan-time ref args were already extracted)
		let args = evaluate_args(&self.arguments, ctx.clone()).await?;

		// Invoke the index function with the resolved index context
		Ok(func.invoke_async(&ctx, &self.index_ctx, args).await?)
	}
}

impl ToSql for IndexFunctionExec {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str(&self.name);
		f.push_str("(...)");
	}
}

impl Clone for IndexFunctionExec {
	fn clone(&self) -> Self {
		Self {
			name: self.name.clone(),
			arguments: self.arguments.clone(),
			index_ctx: self.index_ctx.clone(),
			func_required_context: self.func_required_context,
		}
	}
}
