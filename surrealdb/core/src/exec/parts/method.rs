//! Method call and closure field call parts.

use std::collections::HashMap;
use std::sync::Arc;

use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

use crate::exec::function::MethodDescriptor;
use crate::exec::physical_expr::function::validate_return;
use crate::exec::physical_expr::{BlockPhysicalExpr, EvalContext, PhysicalExpr};
use crate::exec::{AccessMode, BoxFut, CombineAccessModes, ContextLevel, Error as ExecError};
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
impl PhysicalExpr for MethodPart {
	fn name(&self) -> &'static str {
		"Method"
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn required_context(&self) -> ContextLevel {
		self.args.iter().map(|a| a.required_context()).max().unwrap_or(ContextLevel::Root)
	}

	fn evaluate<'a>(&'a self, ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		Box::pin(async move {
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
			let result = if func.is_pure() && !func.is_async() {
				func.invoke(func_args)
			} else {
				func.invoke_async(&ctx, func_args).await
			};

			// Rewrite error names for method calls: when invoked as `.extend()`,
			// the error should say "function extend()" not "function object::extend()".
			match result {
				Ok(v) => Ok(v),
				Err(e) => {
					if let Some(crate::expr::Error::InvalidFunctionArguments {
						message,
						..
					}) = e.downcast_ref::<crate::expr::Error>()
					{
						Err(crate::expr::Error::InvalidMethodArguments {
							name: self.descriptor.name.to_string(),
							message: message.clone(),
						}
						.into())
					} else {
						Err(e.into())
					}
				}
			}
		})
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
impl PhysicalExpr for ClosureFieldCallPart {
	fn name(&self) -> &'static str {
		"ClosureFieldCall"
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn required_context(&self) -> ContextLevel {
		self.args.iter().map(|a| a.required_context()).max().unwrap_or(ContextLevel::Root)
	}

	fn evaluate<'a>(&'a self, ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		Box::pin(async move {
			use crate::expr::Error as ExprError;

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
					let type_name = value.kind_of().to_string();
					return Err(ExecError::InvalidFunction {
						name: self.field.clone(),
						message: format!("no such method found for the {} type", type_name),
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
						return Err(ExprError::InvalidFunctionArguments {
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
					let mut local_params: HashMap<Strand, Value> = HashMap::new();
					for ((param, kind), arg_value) in arg_spec.iter().zip(evaluated_args) {
						let coerced = arg_value.coerce_to_kind(kind).map_err(|_| {
							ExprError::InvalidFunctionArguments {
								name: "ANONYMOUS".to_string(),
								message: format!(
									"Expected a value of type '{}' for argument {}",
									kind.to_sql(),
									param.to_sql()
								),
							}
						})?;
						local_params.insert(param.clone().into_strand(), coerced);
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
						document_root: ctx.document_root,
						skip_fetch_perms: ctx.skip_fetch_perms,
						computing_record: ctx.computing_record.clone(),
						// The closure body is one re-entry deeper — continue the
						// depth count so nested recursion stays bounded.
						plan_depth: ctx.plan_depth + 1,
					};

					let result = match block_expr.evaluate(eval_ctx).await {
						Ok(v) => v,
						Err(crate::expr::ControlFlow::Return(v)) => v,
						Err(crate::expr::ControlFlow::Break)
						| Err(crate::expr::ControlFlow::Continue) => {
							return Err(ExecError::InvalidControlFlow.into());
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
		})
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

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::ExecutionContext;
	use crate::exec::operators::test_util::{eval_on, physical_expr, root_ctx, val};
	use crate::expr::FlowResult;

	/// Evaluate `tail` (a method-call suffix) with `receiver` bound as the
	/// value the part sees.
	async fn call(receiver: &str, tail: &str, ctx: &ExecutionContext) -> FlowResult<Value> {
		let row = val(&format!("{{ v: {receiver} }}")).await;
		eval_on(&format!("v{tail}"), &row, ctx).await
	}

	fn part(name: &str, args: Vec<Arc<dyn PhysicalExpr>>, ctx: &ExecutionContext) -> MethodPart {
		let descriptor = Arc::clone(
			ctx.function_registry().get_method(name).expect("method should be registered"),
		);
		MethodPart {
			descriptor,
			args,
		}
	}

	// =========================================================================
	// MethodPart -- dispatch
	// =========================================================================

	#[tokio::test]
	async fn one_method_name_dispatches_on_the_receiver_type() {
		// `len` is registered per type with no generic fallback, so the same
		// name has to reach three different functions.
		let ctx = root_ctx();
		assert_eq!(call("'hello'", ".len()", &ctx).await.unwrap(), Value::from(5));
		assert_eq!(call("[1, 2, 3]", ".len()", &ctx).await.unwrap(), Value::from(3));
		assert_eq!(call("{ a: 1, b: 2 }", ".len()", &ctx).await.unwrap(), Value::from(2));
	}

	#[tokio::test]
	async fn a_generic_method_reaches_types_that_have_no_specific_implementation() {
		// Booleans have no per-type method table at all, so this can only work
		// through the descriptor's fallback.
		let ctx = root_ctx();
		assert_eq!(call("true", ".to_string()", &ctx).await.unwrap(), Value::from("true"));
		assert_eq!(call("1", ".to_string()", &ctx).await.unwrap(), Value::from("1"));
	}

	#[tokio::test]
	async fn a_typed_only_method_on_an_unlisted_receiver_type_is_rejected() {
		// No per-type entry and no fallback: the descriptor must refuse rather
		// than invoke an arbitrary implementation.
		let ctx = root_ctx();
		let err = call("true", ".len()", &ctx).await.unwrap_err();
		assert!(
			err.to_string().contains("cannot be called on value of type 'bool'"),
			"expected the unresolvable-method error, got {err}"
		);
	}

	// =========================================================================
	// MethodPart -- receiver and argument threading
	// =========================================================================

	#[tokio::test]
	async fn the_receiver_is_passed_as_the_first_argument() {
		// `starts_with` is asymmetric, so swapping receiver and argument gives a
		// different answer -- this pins the order rather than just the wiring.
		let ctx = root_ctx();
		assert_eq!(call("'hello'", ".starts_with('he')", &ctx).await.unwrap(), Value::Bool(true));
		assert_eq!(call("'he'", ".starts_with('hello')", &ctx).await.unwrap(), Value::Bool(false));
	}

	#[tokio::test]
	async fn arguments_keep_their_written_order_after_the_receiver() {
		let ctx = root_ctx();
		// `replace(haystack, from, to)` -- reversing the two arguments changes
		// the result, so positional order is observable.
		assert_eq!(call("'a-b'", ".replace('-', '+')", &ctx).await.unwrap(), Value::from("a+b"));
		assert_eq!(call("'a-b'", ".replace('+', '-')", &ctx).await.unwrap(), Value::from("a-b"));
	}

	#[tokio::test]
	async fn an_argument_idiom_resolves_against_the_receiver_not_the_document_row() {
		// Arguments are evaluated with the part's own `EvalContext`, whose
		// current value is the receiver. A bare field name in an argument
		// therefore looks the field up on the *receiver*: with a string receiver
		// there is no such field and the argument arrives as NONE, which the
		// function then rejects on type.
		let ctx = root_ctx();
		let row = val("{ text: 'a-b', sep: '-' }").await;

		let err = eval_on("text.split(sep)", &row, &ctx).await.unwrap_err();
		assert!(
			err.to_string().contains("Expected `string` but found `NONE`"),
			"`sep` resolved against the row rather than the receiver: {err}"
		);

		// `$parent` is the reader that does reach the enclosing row.
		let with_parent = eval_on("text.split($parent.sep)", &row, &ctx).await.unwrap();
		assert_eq!(with_parent, val("['a', 'b']").await);
	}

	#[tokio::test]
	async fn an_argument_idiom_reads_fields_of_an_object_receiver() {
		// The mirror image of the rule above: when the receiver is an object, a
		// bare field name in an argument resolves against it.
		let ctx = root_ctx();
		let out = call("{ a: 1, extra: { b: 2 } }", ".extend(extra)", &ctx).await.unwrap();
		assert_eq!(out, val("{ a: 1, b: 2, extra: { b: 2 } }").await);
	}

	// =========================================================================
	// MethodPart -- error reporting
	// =========================================================================

	#[tokio::test]
	async fn an_arity_error_names_the_method_not_the_underlying_function() {
		// `.len()` dispatches to `string::len`, but the user wrote `len()`, so
		// the message must not leak the internal function name.
		let ctx = root_ctx();
		let err = call("'hello'", ".len(1)", &ctx).await.unwrap_err();
		let message = err.to_string();
		assert!(message.contains("method len()"), "expected a method-arity error, got {message}");
		assert!(!message.contains("string::len"), "the function name leaked: {message}");
	}

	#[tokio::test]
	async fn a_non_arity_failure_inside_the_function_is_left_untouched() {
		// Only `InvalidFunctionArguments` is rewritten; every other failure has
		// to reach the caller as it was raised.
		let ctx = root_ctx();
		let err = call("'not a number'", ".to_int()", &ctx).await.unwrap_err();
		assert!(
			!err.to_string().contains("Incorrect arguments for method"),
			"an unrelated failure was rewritten as an arity error: {err}"
		);
	}

	// =========================================================================
	// MethodPart -- plan metadata the engine acts on
	// =========================================================================

	#[tokio::test]
	async fn required_context_and_access_mode_come_from_the_arguments() {
		// The executor validates `required_context` before evaluating and the
		// planner picks the transaction mode from `access_mode`; a method with no
		// arguments constrains neither.
		let ctx = root_ctx();
		let bare = part("len", vec![], &ctx);
		assert_eq!(bare.required_context(), ContextLevel::Root);
		assert_eq!(bare.access_mode(), AccessMode::ReadOnly);

		// An argument that dereferences a record id needs a transaction, and the
		// method call has to report that requirement on its behalf.
		let deref = physical_expr("person:1.name", &ctx).await;
		assert_eq!(deref.required_context(), ContextLevel::Database, "fixture must need a txn");
		let with_arg = part("len", vec![deref], &ctx);
		assert_eq!(with_arg.required_context(), ContextLevel::Database);
	}

	#[tokio::test]
	async fn sql_rendering_puts_the_arguments_inside_the_method_call() {
		let ctx = root_ctx();
		let arg = physical_expr("','", &ctx).await;
		assert_eq!(part("split", vec![arg], &ctx).to_sql(), ".split(',')");
		assert_eq!(part("len", vec![], &ctx).to_sql(), ".len()");
	}

	// =========================================================================
	// ClosureFieldCallPart
	// =========================================================================

	#[tokio::test]
	async fn an_unregistered_method_name_reports_the_receiver_type() {
		// The planner turns an unknown name into a closure field call; at
		// runtime a non-object receiver has no such field, so the error names
		// the method and the receiver's type.
		let ctx = root_ctx();
		let err = call("'hello'", ".no_such_method()", &ctx).await.unwrap_err();
		let message = err.to_string();
		assert!(message.contains("no_such_method"), "expected the method name, got {message}");
		assert!(
			message.contains("no such method found for the string type"),
			"expected the receiver type, got {message}"
		);
	}

	#[tokio::test]
	async fn an_object_field_that_is_not_a_closure_reports_the_receiver_type() {
		let ctx = root_ctx();
		let err = call("{ thing: 1 }", ".thing()", &ctx).await.unwrap_err();
		assert!(
			err.to_string().contains("no such method found for the object type"),
			"expected the receiver type, got {err}"
		);
	}

	#[tokio::test]
	async fn an_object_field_holding_a_closure_is_invoked_with_the_given_arguments() {
		let ctx = root_ctx();
		let out = call("{ double: |$x: number| $x * 2 }", ".double(4)", &ctx).await.unwrap();
		assert_eq!(out, Value::from(8));
	}

	#[tokio::test]
	async fn a_closure_argument_is_coerced_to_its_declared_type() {
		let ctx = root_ctx();
		// An int argument reaches a float parameter as a float.
		let out = call("{ f: |$x: float| $x }", ".f(1)", &ctx).await.unwrap();
		assert_eq!(out, val("1.0f").await);

		// A value that cannot be coerced is an argument error, not a silent pass.
		let err = call("{ f: |$x: number| $x }", ".f('nope')", &ctx).await.unwrap_err();
		assert!(
			err.to_string().contains("Expected a value of type 'number'"),
			"expected a coercion failure, got {err}"
		);
	}

	#[tokio::test]
	async fn a_missing_required_closure_argument_is_rejected() {
		let ctx = root_ctx();
		let err = call("{ f: |$x: number| $x }", ".f()", &ctx).await.unwrap_err();
		let message = err.to_string();
		assert!(
			message.contains("Expected a value of type 'number'"),
			"expected the missing-argument error, got {message}"
		);
		// Closures have no name, so the report is anonymous.
		assert!(message.contains("ANONYMOUS"), "expected an anonymous report, got {message}");
	}

	#[tokio::test]
	async fn an_omitted_optional_closure_argument_is_left_unbound() {
		// A parameter whose kind admits NONE passes the missing-argument check,
		// but only supplied arguments are zipped into the parameter bindings, so
		// the body finds no such parameter at all rather than reading NONE.
		let ctx = root_ctx();
		let err = call("{ f: |$x: option<number>| $x }", ".f()", &ctx).await.unwrap_err();
		assert!(
			err.to_string().contains("Parameter not found: $x"),
			"expected the unbound-parameter error, got {err}"
		);

		// Supplying the argument binds it as normal.
		let out = call("{ f: |$x: option<number>| $x }", ".f(3)", &ctx).await.unwrap();
		assert_eq!(out, Value::from(3));
	}

	#[tokio::test]
	async fn a_declared_closure_return_type_is_enforced() {
		let ctx = root_ctx();
		let ok = call("{ f: |$x: number| -> float { $x } }", ".f(1)", &ctx).await.unwrap();
		assert_eq!(ok, val("1.0f").await);

		let err = call("{ f: |$x: number| -> string { $x } }", ".f(1)", &ctx).await.unwrap_err();
		assert!(
			err.to_string().contains("string"),
			"expected a return-type failure naming the declared type, got {err}"
		);
	}

	#[tokio::test]
	async fn a_return_inside_the_closure_body_becomes_its_value() {
		// The body is a block: a RETURN unwinds to the closure boundary and is
		// the call's result rather than propagating out of the enclosing query.
		let ctx = root_ctx();
		let out = call("{ f: |$x: number| { RETURN $x * 3 } }", ".f(2)", &ctx).await.unwrap();
		assert_eq!(out, Value::from(6));
	}

	#[tokio::test]
	async fn break_or_continue_out_of_a_closure_body_is_an_error() {
		// Neither signal has a loop to bind to at the closure boundary, so both
		// must surface as errors instead of escaping the call.
		let ctx = root_ctx();
		for body in ["{ BREAK }", "{ CONTINUE }"] {
			let err = call(&format!("{{ f: || {body} }}"), ".f()", &ctx).await.unwrap_err();
			assert!(
				err.to_string().contains("Invalid control flow"),
				"expected InvalidControlFlow for {body}, got {err}"
			);
		}
	}

	#[tokio::test]
	async fn closure_field_call_sql_rendering_keeps_the_field_name() {
		let ctx = root_ctx();
		let arg = physical_expr("1", &ctx).await;
		let call_part = ClosureFieldCallPart {
			field: "double".to_string(),
			args: vec![arg],
		};
		assert_eq!(call_part.to_sql(), ".double(1)");
	}
}
