//! Method call and closure field call parts.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use surrealdb_types::{SqlFormat, ToSql};

use crate::exec::function::MethodDescriptor;
use crate::exec::physical_expr::function::validate_return;
use crate::exec::physical_expr::{BlockPhysicalExpr, EvalContext, PhysicalExpr};
use crate::exec::{AccessMode, CombineAccessModes, ContextLevel};
use crate::expr::FlowResult;
use crate::val::{Closure, Value};

// ============================================================================
// MethodPart
// ============================================================================

/// Method call - `.method(args)`.
///
/// Methods are syntactic sugar for function calls. For example:
/// - `"hello".len()` -> `string::len("hello")`
/// - `[1, 2, 3].len()` -> `array::len([1, 2, 3])`
///
/// The method descriptor (resolved at plan time) contains the per-type function
/// dispatch table. At eval time we just look up the function for the value's type.
#[derive(Debug, Clone)]
pub struct MethodPart {
	pub descriptor: Arc<MethodDescriptor>,
	pub args: Vec<Arc<dyn PhysicalExpr>>,
}

#[async_trait]
impl PhysicalExpr for MethodPart {
	fn name(&self) -> &'static str {
		"Method"
	}

	fn required_context(&self) -> ContextLevel {
		self.args.iter().map(|a| a.required_context()).max().unwrap_or(ContextLevel::Root)
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> FlowResult<Value> {
		let value = ctx.current_value.cloned().unwrap_or(Value::None);

		// Resolve the function for this value's type
		let func = self.descriptor.resolve(&value)?;

		// Build the arguments: receiver value first, then method arguments
		let mut func_args = Vec::with_capacity(1 + self.args.len());
		func_args.push(value);
		for arg_expr in &self.args {
			let arg_value = arg_expr.evaluate(ctx.clone()).await?;
			func_args.push(arg_value);
		}

		// Invoke the resolved function
		let result = if func.is_pure() {
			func.invoke(func_args)
		} else {
			func.invoke_async(&ctx, func_args).await
		};

		// Rewrite error names for method calls: when invoked as `.extend()`,
		// the error should say "function extend()" not "function object::extend()".
		match result {
			Ok(v) => Ok(v),
			Err(e) => {
				if let Some(crate::err::Error::InvalidArguments {
					message,
					..
				}) = e.downcast_ref::<crate::err::Error>()
				{
					Err(crate::err::Error::InvalidArguments {
						name: self.descriptor.name.to_string(),
						message: message.clone(),
					}
					.into())
				} else {
					Err(e.into())
				}
			}
		}
	}

	fn references_current_value(&self) -> bool {
		true
	}

	fn access_mode(&self) -> AccessMode {
		self.args.iter().map(|a| a.access_mode()).combine_all()
	}
}

impl ToSql for MethodPart {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push('.');
		f.push_str(self.descriptor.name);
		f.push('(');
		for (i, arg) in self.args.iter().enumerate() {
			if i > 0 {
				f.push_str(", ");
			}
			arg.fmt_sql(f, fmt);
		}
		f.push(')');
	}
}

// ============================================================================
// ClosureFieldCallPart
// ============================================================================

/// Closure field call - `.field(args)` where field is not a known method.
///
/// When a method name is not found in the method registry at plan time,
/// the planner creates this part. At runtime, it accesses the named field
/// on the value and, if it contains a closure, invokes it with the provided
/// arguments.
#[derive(Debug, Clone)]
pub struct ClosureFieldCallPart {
	pub field: String,
	pub args: Vec<Arc<dyn PhysicalExpr>>,
}

#[async_trait]
impl PhysicalExpr for ClosureFieldCallPart {
	fn name(&self) -> &'static str {
		"ClosureFieldCall"
	}

	fn required_context(&self) -> ContextLevel {
		self.args.iter().map(|a| a.required_context()).max().unwrap_or(ContextLevel::Root)
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> FlowResult<Value> {
		use crate::err::Error;

		let value = ctx.current_value.cloned().unwrap_or(Value::None);

		// Get the field value from the object
		let field_value = match &value {
			Value::Object(obj) => obj.get(self.field.as_str()).cloned(),
			_ => None,
		};

		// Check if the field contains a closure
		let closure = match field_value {
			Some(Value::Closure(c)) => c,
			_ => {
				return Err(Error::InvalidFunction {
					name: self.field.clone(),
					message: "no such method found for the object type".to_string(),
				}
				.into());
			}
		};

		// Evaluate all argument expressions
		let mut evaluated_args = Vec::with_capacity(self.args.len());
		for arg_expr in &self.args {
			evaluated_args.push(arg_expr.evaluate(ctx.clone()).await?);
		}

		// Invoke the closure
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
					recursion_ctx: None,
				};

				let result = match block_expr.evaluate(eval_ctx).await {
					Ok(v) => v,
					Err(crate::expr::ControlFlow::Return(v)) => v,
					Err(crate::expr::ControlFlow::Break)
					| Err(crate::expr::ControlFlow::Continue) => {
						return Err(Error::InvalidControlFlow.into());
					}
					Err(e) => return Err(e),
				};

				// Coerce return value to declared type if specified
				Ok(validate_return("ANONYMOUS", returns.as_ref(), result)?)
			}
			Closure::Builtin(_) => Err(anyhow::anyhow!(
				"Builtin closures are not yet supported in the streaming executor"
			)
			.into()),
		}
	}

	fn references_current_value(&self) -> bool {
		true
	}

	fn access_mode(&self) -> AccessMode {
		self.args.iter().map(|a| a.access_mode()).combine_all()
	}
}

impl ToSql for ClosureFieldCallPart {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push('.');
		f.push_str(&self.field);
		f.push('(');
		for (i, arg) in self.args.iter().enumerate() {
			if i > 0 {
				f.push_str(", ");
			}
			arg.fmt_sql(f, fmt);
		}
		f.push(')');
	}
}
