//! Module function expressions - Surrealism WASM and Silo packages.

use std::sync::Arc;

use async_trait::async_trait;
use surrealdb_types::{SqlFormat, ToSql};

use super::helpers::{args_access_mode, args_required_context};
#[cfg(feature = "surrealism")]
use super::helpers::{check_permission, evaluate_args, validate_return};
#[cfg(feature = "surrealism")]
use crate::catalog::providers::DatabaseProvider;
#[cfg(feature = "surrealism")]
use crate::err::Error;
use crate::exec::AccessMode;
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::expr::FlowResult;
use crate::val::Value;

// =============================================================================
// SurrealismModuleExec - for Function::Module
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
// SiloModuleExec - for Function::Silo
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
