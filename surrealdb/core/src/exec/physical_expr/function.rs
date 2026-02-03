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
use crate::exec::physical_expr::block::{
	BlockPhysicalExpr, BreakControlFlow, ContinueControlFlow, ReturnValue,
};
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::planner::expr_to_physical_expr;
use crate::exec::{AccessMode, CombineAccessModes, ExecutionContext};
use crate::expr::{Kind, Model, Script};
use crate::val::Value;

// =============================================================================
// Shared Helper Functions
// =============================================================================

/// Evaluate all argument expressions to values.
async fn evaluate_args(
	args: &[Arc<dyn PhysicalExpr>],
	ctx: EvalContext<'_>,
) -> anyhow::Result<Vec<Value>> {
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
) -> anyhow::Result<()> {
	match permission {
		Permission::Full => Ok(()),
		Permission::None => Err(Error::FunctionPermissions {
			name: func_name.to_string(),
		}
		.into()),
		Permission::Specific(expr) => {
			// Plan and evaluate the permission expression
			match expr_to_physical_expr(expr.clone(), ctx.exec_ctx.ctx()) {
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
		return Err(Error::InvalidArguments {
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
fn validate_return(
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

/// Helper to check if any argument references current value.
fn args_reference_current_value(args: &[Arc<dyn PhysicalExpr>]) -> bool {
	args.iter().any(|e| e.references_current_value())
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

#[async_trait]
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

	async fn evaluate(&self, ctx: EvalContext<'_>) -> anyhow::Result<Value> {
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
			func.invoke(args)
		} else {
			func.invoke_async(&ctx, args).await
		}
	}

	fn references_current_value(&self) -> bool {
		args_reference_current_value(&self.arguments)
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

#[async_trait]
impl PhysicalExpr for UserDefinedFunctionExec {
	fn name(&self) -> &'static str {
		"UserDefinedFunction"
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		// User-defined functions are stored in the database
		crate::exec::ContextLevel::Database
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> anyhow::Result<Value> {
		let func_name = format!("fn::{}", self.name);

		// 1. Require database context
		let db_ctx = ctx.exec_ctx.database().map_err(|_| {
			anyhow::anyhow!("Custom function '{}' requires database context", func_name)
		})?;

		// 2. Check if function is allowed by capabilities
		if let Some(caps) = ctx.capabilities() {
			if !caps.allows_function_name(&func_name) {
				return Err(Error::FunctionNotAllowed(func_name).into());
			}
		}

		// 3. Retrieve function definition
		let ns_id = db_ctx.ns_ctx.ns.namespace_id;
		let db_id = db_ctx.db.database_id;
		let func_def = ctx
			.txn()
			.get_db_function(ns_id, db_id, &self.name)
			.await
			.map_err(|e| anyhow::anyhow!("Function '{}' not found: {}", func_name, e))?;

		// 4. Check permissions
		check_permission(&func_def.permissions, &func_name, &ctx).await?;

		// 5. Evaluate all arguments
		let evaluated_args = evaluate_args(&self.arguments, ctx.clone()).await?;

		// 6. Validate argument count
		validate_arg_count(&func_name, evaluated_args.len(), &func_def.args)?;

		// 7. Create isolated context with function parameters bound
		let mut local_params: HashMap<String, Value> = HashMap::new();
		for ((param_name, kind), arg_value) in func_def.args.iter().zip(evaluated_args.into_iter())
		{
			let coerced = arg_value.coerce_to_kind(kind).map_err(|e| Error::InvalidArguments {
				name: func_name.clone(),
				message: format!("Failed to coerce argument `${param_name}`: {e}"),
			})?;
			local_params.insert(param_name.clone(), coerced);
		}

		// 8. Create a new execution context with the parameters
		let mut isolated_ctx = ctx.exec_ctx.clone();
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
		};
		let result = match block_expr.evaluate(eval_ctx).await {
			Ok(v) => v,
			Err(e) => {
				// Check if this is a RETURN control flow - extract the value
				if let Some(return_value) = e.downcast_ref::<ReturnValue>() {
					return_value.0.clone()
				} else if e.is::<BreakControlFlow>() || e.is::<ContinueControlFlow>() {
					// BREAK/CONTINUE inside a function (outside of loop) is an error
					return Err(anyhow::Error::new(Error::InvalidControlFlow));
				} else {
					return Err(e);
				}
			}
		};

		// 10. Validate and coerce return type
		validate_return(&func_name, func_def.returns.as_ref(), result)
	}

	fn references_current_value(&self) -> bool {
		args_reference_current_value(&self.arguments)
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
	pub(crate) script: Script,
	pub(crate) arguments: Vec<Arc<dyn PhysicalExpr>>,
}

#[async_trait]
impl PhysicalExpr for JsFunctionExec {
	fn name(&self) -> &'static str {
		"JsFunction"
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		// Script functions require database context for full execution
		crate::exec::ContextLevel::Database
	}

	async fn evaluate(&self, _ctx: EvalContext<'_>) -> anyhow::Result<Value> {
		// Script functions require the scripting feature and full context
		Err(anyhow::anyhow!("Script functions are not yet supported in the streaming executor"))
	}

	fn references_current_value(&self) -> bool {
		args_reference_current_value(&self.arguments)
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

#[async_trait]
impl PhysicalExpr for ModelFunctionExec {
	fn name(&self) -> &'static str {
		"ModelFunction"
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		// ML models are stored in the database
		crate::exec::ContextLevel::Database
	}

	async fn evaluate(&self, _ctx: EvalContext<'_>) -> anyhow::Result<Value> {
		// Model functions require the ML runtime
		Err(anyhow::anyhow!(
			"Model function 'ml::{}' is not yet supported in the streaming executor",
			self.model.name
		))
	}

	fn references_current_value(&self) -> bool {
		args_reference_current_value(&self.arguments)
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

#[async_trait]
impl PhysicalExpr for SurrealismModuleExec {
	fn name(&self) -> &'static str {
		"SurrealismModule"
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		// Module functions may require database context
		crate::exec::ContextLevel::Database
	}

	async fn evaluate(&self, _ctx: EvalContext<'_>) -> anyhow::Result<Value> {
		let name = match &self.sub {
			Some(s) => format!("mod::{}::{}", self.module, s),
			None => format!("mod::{}", self.module),
		};
		Err(anyhow::anyhow!(
			"Module function '{}' is not yet supported in the streaming executor",
			name
		))
	}

	fn references_current_value(&self) -> bool {
		args_reference_current_value(&self.arguments)
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

#[async_trait]
impl PhysicalExpr for SiloModuleExec {
	fn name(&self) -> &'static str {
		"SiloModule"
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		// Silo package functions may require database context
		crate::exec::ContextLevel::Database
	}

	async fn evaluate(&self, _ctx: EvalContext<'_>) -> anyhow::Result<Value> {
		let name = format!(
			"silo::{}::{}<{}.{}.{}>",
			self.org, self.pkg, self.major, self.minor, self.patch
		);
		Err(anyhow::anyhow!(
			"Silo function '{}' is not yet supported in the streaming executor",
			name
		))
	}

	fn references_current_value(&self) -> bool {
		args_reference_current_value(&self.arguments)
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

#[async_trait]
impl PhysicalExpr for ClosureExec {
	fn name(&self) -> &'static str {
		"Closure"
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		// Closures are just values, they don't need any special context
		crate::exec::ContextLevel::Root
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> anyhow::Result<Value> {
		use crate::dbs::ParameterCapturePass;
		use crate::val::Closure;

		// Capture parameters from the context that are referenced in the closure body
		let frozen_ctx = &ctx.exec_ctx.root().ctx;
		let captures = ParameterCapturePass::capture(frozen_ctx, &self.closure.body);

		// Create a Value::Closure with the captured variables
		Ok(Value::Closure(Box::new(Closure::Expr {
			args: self.closure.args.clone(),
			returns: self.closure.returns.clone(),
			body: self.closure.body.clone(),
			captures,
		})))
	}

	fn references_current_value(&self) -> bool {
		// Closures capture their environment, but don't directly reference current value
		// The body might, but that's evaluated later when the closure is called
		false
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

#[async_trait]
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

	async fn evaluate(&self, ctx: EvalContext<'_>) -> anyhow::Result<Value> {
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
				for (name, value) in captures.clone().into_iter() {
					isolated_ctx = isolated_ctx.with_param(name, value);
				}

				// Check for missing required arguments
				if arg_spec.len() > evaluated_args.len() {
					if let Some((param, kind)) =
						arg_spec[evaluated_args.len()..].iter().find(|(_, k)| !k.can_be_none())
					{
						return Err(Error::InvalidArguments {
							name: "ANONYMOUS".to_string(),
							message: format!(
								"Expected a value of type '{}' for argument {}",
								kind.to_sql(),
								param.to_sql()
							),
						}
						.into());
					}
				}

				// Bind arguments to parameter names with type coercion
				let mut local_params: HashMap<String, Value> = HashMap::new();
				for ((param, kind), arg_value) in arg_spec.iter().zip(evaluated_args.into_iter()) {
					let coerced =
						arg_value.coerce_to_kind(kind).map_err(|_| Error::InvalidArguments {
							name: "ANONYMOUS".to_string(),
							message: format!(
								"Expected a value of type '{}' for argument {}",
								kind.to_sql(),
								param.to_sql()
							),
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
				};

				let result = match block_expr.evaluate(eval_ctx).await {
					Ok(v) => v,
					Err(e) => {
						// Handle RETURN control flow - extract the returned value
						if let Some(return_value) = e.downcast_ref::<ReturnValue>() {
							return_value.0.clone()
						} else if e.is::<BreakControlFlow>() || e.is::<ContinueControlFlow>() {
							// BREAK/CONTINUE inside a closure (outside of loop) is an error
							return Err(anyhow::Error::new(Error::InvalidControlFlow));
						} else {
							return Err(e);
						}
					}
				};

				// Coerce return value to declared type if specified
				validate_return("ANONYMOUS", returns.as_ref(), result)
			}
			Closure::Builtin(_) => {
				// Builtin closures are not yet supported in the streaming executor
				// They require the legacy compute path with Stk
				Err(anyhow::anyhow!(
					"Builtin closures are not yet supported in the streaming executor"
				))
			}
		}
	}

	fn references_current_value(&self) -> bool {
		self.target.references_current_value() || args_reference_current_value(&self.arguments)
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

#[async_trait]
impl PhysicalExpr for ProjectionFunctionExec {
	fn name(&self) -> &'static str {
		"ProjectionFunction"
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		let args_ctx = args_required_context(&self.arguments);
		args_ctx.max(self.func_required_context)
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> anyhow::Result<Value> {
		// When evaluated as a regular expression (not in projection context),
		// return the first value from the projection bindings, or None if empty.
		// This handles cases like: RETURN type::field("name")
		if let Some(bindings) = self.evaluate_projection(ctx).await? {
			if bindings.len() == 1 {
				Ok(bindings.into_iter().next().unwrap().1)
			} else {
				// Multiple bindings - return as array of values
				Ok(Value::Array(bindings.into_iter().map(|(_, v)| v).collect()))
			}
		} else {
			Ok(Value::None)
		}
	}

	fn references_current_value(&self) -> bool {
		// Projection functions inherently reference the current document
		true
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
	) -> anyhow::Result<Option<Vec<(crate::expr::idiom::Idiom, Value)>>> {
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
