//! Method call and closure field call parts.

use std::sync::Arc;

use surrealdb_types::{SqlFormat, ToSql};

use crate::exec::function::MethodDescriptor;
use crate::exec::physical_expr::function::helpers::invoke_closure;
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{AccessMode, BoxFut, CombineAccessModes, ContextLevel, Error as ExecError};
use crate::expr::FlowResult;
use crate::val::Value;

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
/// dispatch table. At eval time we look up the function for the value's type;
/// when the descriptor has no implementation for that type, the call falls back
/// to closure-field-call semantics (see [`ClosureFieldCallPart`]), so a closure
/// stored under a name that collides with a registered method is still callable.
///
/// # Evaluation frame
///
/// The receiver is the chain's current value and nothing else: the arguments,
/// and the call itself, evaluate in the frame the idiom was entered from — the
/// document root, when the context has one — so `tags.slice(offset, len)`
/// reads `offset` and `len` from the record and not from `tags`, and the body
/// of a closure argument reads the record too. Without a document root the
/// arguments evaluate in the context as given, as a dynamic index does.
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
			let ctx = call_frame(&ctx);

			// Resolve the function for this value's type. Builtin dispatch on
			// the receiver type takes precedence; when the descriptor has no
			// implementation for it, the call degrades to closure-field-call
			// semantics: an object field of this name holding a closure is
			// invoked, anything else is the unknown-method error. A name
			// collision between a builtin method and a closure field resolves
			// to the builtin.
			let Some(func) = self.descriptor.resolve(&value) else {
				return closure_field_call(self.descriptor.name, &value, &self.args, ctx).await;
			};

			// A method is the registry entry it resolves to, so it carries
			// that entry's capability check rather than the method name's:
			// `.len()` on a string is `string::len`. The receiver type decides
			// which entry runs, so the check follows the resolve; it precedes
			// the arguments, where the direct-call path puts it.
			ctx.check_allowed_function(func.name())?;

			// Build the arguments: receiver value first, then method arguments
			let mut func_args = Vec::with_capacity(1 + self.args.len());
			func_args.push(value);
			for arg_expr in &self.args {
				let arg_value = arg_expr.evaluate(ctx.clone()).await?;
				func_args.push(arg_value);
			}

			// Method dispatch reaches the same registry entry a direct call
			// does, so it is refused by the same capability, under the name
			// of the builtin the receiver resolved to, and a pure built-in
			// behind an experimental target is gated here too. Both follow
			// the arguments.
			ctx.check_allowed_function(func.name())?;
			if let Some(target) = crate::exec::function::experimental_target(func.name())
				&& !ctx.capabilities().allows_experimental(&target)
			{
				return Err(crate::exec::Error::InvalidFunction {
					name: self.descriptor.name.to_string(),
					message: format!("Experimental feature {target} is not enabled"),
				}
				.into());
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
		// Every function a descriptor can resolve to is a builtin, and no
		// registered method maps to one of the statement-evaluating builtins
		// that need a write transaction (`BuiltinFunctionExec::access_mode`'s
		// write set) — `no_registered_method_resolves_to_a_writing_builtin`
		// in `exec::function::method` pins that. Only the arguments carry
		// user expressions.
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
			let value = ctx.current_value.cloned().unwrap_or(Value::None);
			closure_field_call(&self.field, &value, &self.args, call_frame(&ctx)).await
		})
	}

	fn access_mode(&self) -> AccessMode {
		// The field holds a runtime value the plan cannot inspect, and its
		// body may write, so the call must be assumed to — mirroring
		// `ClosureCallExec::access_mode` for direct closure calls.
		AccessMode::ReadWrite
	}
}

/// The frame a method's arguments and its call evaluate in; see the
/// evaluation-frame contract on [`MethodPart`]. The receiver the part was
/// handed as the current value is only the chain's position, never the frame.
fn call_frame<'a>(ctx: &EvalContext<'a>) -> EvalContext<'a> {
	match ctx.document_root {
		Some(doc) => ctx.with_value(doc),
		None => ctx.clone(),
	}
}

/// Shared semantics for `.name(args)` resolved against a closure field.
///
/// The arguments are evaluated in `ctx`, which the callers have already moved
/// to the call frame; the receiver arrives as `value`.
///
/// Looks up `name` as a field on an object receiver and invokes the closure it
/// holds with the evaluated arguments. A non-object receiver, a missing field,
/// or a field holding a non-closure value is the unknown-method error naming
/// the receiver's type. Used by [`ClosureFieldCallPart`] for names absent from
/// the method registry, and by [`MethodPart`] when a registered method has no
/// implementation for the receiver's type.
async fn closure_field_call(
	field: &str,
	value: &Value,
	args: &[Arc<dyn PhysicalExpr>],
	ctx: EvalContext<'_>,
) -> FlowResult<Value> {
	// Get the field value from the object
	let field_value = match value {
		Value::Object(obj) => obj.get(field).cloned(),
		_ => None,
	};

	// Check if the field contains a closure
	let closure = match field_value {
		Some(Value::Closure(c)) => c,
		_ => {
			let type_name = value.kind_of().to_string();
			return Err(ExecError::InvalidFunction {
				name: field.to_string(),
				message: format!("no such method found for the {} type", type_name),
			}
			.into());
		}
	};

	// Evaluate all argument expressions
	let mut evaluated_args = Vec::with_capacity(args.len());
	for arg_expr in args {
		evaluated_args.push(arg_expr.evaluate(ctx.clone()).await?);
	}

	// Invoke the closure. A body the engine compiled runs natively;
	// any other closure value is invoked in the caller's frame.
	invoke_closure(&closure, evaluated_args, &ctx).await
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
	use crate::exec::operators::test_util::{
		TestDb, eval_on, eval_on_in_txn, physical_expr, root_ctx, val,
	};
	use crate::expr::FlowResult;

	/// Evaluate `tail` (a method-call suffix) with `receiver` bound as the
	/// value the part sees.
	async fn call(receiver: &str, tail: &str, ctx: &ExecutionContext) -> FlowResult<Value> {
		let row = val(&format!("{{ v: {receiver} }}")).await;
		eval_on(&format!("v{tail}"), &row, ctx).await
	}

	/// As [`call`], compiled in the transaction `ctx` holds, which a closure
	/// body that is a block needs to be planned.
	async fn call_in_txn(receiver: &str, tail: &str, ctx: &ExecutionContext) -> FlowResult<Value> {
		let row = val(&format!("{{ v: {receiver} }}")).await;
		eval_on_in_txn(&format!("v{tail}"), &row, ctx).await
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
	async fn a_file_method_is_gated_on_the_files_experimental_target() {
		// A file value can outlive the capability that created it — stored on a
		// record, or bound as a parameter — so method dispatch has to apply the
		// same experimental gate a direct `file::bucket()` call does. The value
		// is built directly because SurrealQL cannot parse a file literal while
		// the target is off, which is also why this is not a language test.
		let ctx = root_ctx();
		let receiver = Value::File(crate::val::File::new("bucket".to_string(), "/key".to_string()));
		let err = part("bucket", Vec::new(), &ctx)
			.evaluate(EvalContext::from_exec_ctx(&ctx).with_value_and_doc(&receiver))
			.await
			.unwrap_err();
		assert!(
			err.to_string().contains("Experimental feature"),
			"expected the experimental gate, got {err}"
		);
	}

	#[tokio::test]
	async fn a_typed_only_method_on_an_unlisted_receiver_type_is_rejected() {
		// No per-type entry and no fallback: the call degrades to a closure
		// field call, and a non-object receiver has no fields, so the result
		// is the unknown-method error rather than an arbitrary implementation.
		let ctx = root_ctx();
		let err = call("true", ".len()", &ctx).await.unwrap_err();
		assert!(
			err.to_string().contains("no such method found for the bool type"),
			"expected the unknown-method error, got {err}"
		);
	}

	#[tokio::test]
	async fn a_closure_field_named_after_an_unimplemented_method_is_invoked() {
		// `get` is registered (for file receivers), so the planner emits a
		// MethodPart rather than a ClosureFieldCallPart — but with no object
		// implementation the closure field must still be reached, exactly as
		// if the name had not been registered at all.
		let db = TestDb::new("").await;
		let ctx = db.exec_ctx().await;
		let out = call_in_txn("{ get: |$x: number| $x * 2 }", ".get(4)", &ctx).await.unwrap();
		assert_eq!(out, Value::from(8));
	}

	#[tokio::test]
	async fn a_method_implemented_for_the_receiver_type_wins_over_a_closure_field() {
		// `len` has an object implementation, so the builtin counts the keys;
		// the same-named closure field must not shadow it.
		let ctx = root_ctx();
		let out = call("{ len: || 42, other: true }", ".len()", &ctx).await.unwrap();
		assert_eq!(out, Value::from(2));
	}

	#[tokio::test]
	async fn an_unimplemented_method_without_a_closure_field_is_the_unknown_method_error() {
		// The degraded path ends exactly like ClosureFieldCallPart: colliding
		// with a registered name earns no different error.
		let ctx = root_ctx();
		let err = call("{ x: 1 }", ".get('x')", &ctx).await.unwrap_err();
		assert!(
			err.to_string().contains("no such method found for the object type"),
			"expected the unknown-method error, got {err}"
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
	async fn an_argument_idiom_resolves_against_the_document_row_not_the_receiver() {
		// `sep` is a field of the row, not of the string the chain stands on:
		// the arguments evaluate in the row's frame, with the cursor document.
		let ctx = root_ctx();
		let row = val("{ text: 'a-b', sep: '-' }").await;
		assert_eq!(eval_on("text.split(sep)", &row, &ctx).await.unwrap(), val("['a', 'b']").await);

		// `$parent` names the same row.
		let with_parent = eval_on("text.split($parent.sep)", &row, &ctx).await.unwrap();
		assert_eq!(with_parent, val("['a', 'b']").await);
	}

	#[tokio::test]
	async fn an_argument_idiom_does_not_read_fields_of_an_object_receiver() {
		// The receiver and the row both have an `extra` field; the argument is
		// the row's. Were it the receiver's, the result would be
		// `{ a: 1, b: 2, extra: { b: 2 } }`.
		let ctx = root_ctx();
		let row = val("{ cfg: { a: 1, extra: { b: 2 } }, extra: { c: 3 } }").await;
		let out = eval_on("cfg.extend(extra)", &row, &ctx).await.unwrap();
		assert_eq!(out, val("{ a: 1, c: 3, extra: { b: 2 } }").await);

		// A field only the receiver has is out of reach: the argument is NONE,
		// which the function rejects on type.
		let row = val("{ cfg: { a: 1, extra: { b: 2 } } }").await;
		let err = eval_on("cfg.extend(extra)", &row, &ctx).await.unwrap_err();
		assert!(
			err.to_string().contains("found `NONE`"),
			"`extra` resolved against the receiver rather than the row: {err}"
		);
	}

	#[tokio::test]
	async fn a_closure_argument_reads_the_document_row_in_its_body() {
		// The closure is invoked by the builtin in the call's frame, so a bare
		// field in its body is the row's.
		let db = TestDb::new("").await;
		let ctx = db.exec_ctx().await;
		let row = val("{ tags: ['a', 'b'], suffix: '!' }").await;
		let out = eval_on("tags.map(|$t| $t + suffix)", &row, &ctx).await.unwrap();
		assert_eq!(out, val("['a!', 'b!']").await);
	}

	#[tokio::test]
	async fn a_closure_field_call_evaluates_its_arguments_against_the_document_row() {
		// Both routes to a closure field -- an unregistered name, and a
		// registered method with no implementation for an object receiver --
		// read `n` from the row, not from the object holding the closure.
		let db = TestDb::new("").await;
		let ctx = db.exec_ctx().await;
		let row = val("{ obj: { double: |$x: number| $x * 2, n: 100 }, n: 4 }").await;
		assert_eq!(eval_on("obj.double(n)", &row, &ctx).await.unwrap(), Value::from(8));
		let row = val("{ obj: { get: |$x: number| $x * 2, n: 100 }, n: 4 }").await;
		assert_eq!(eval_on("obj.get(n)", &row, &ctx).await.unwrap(), Value::from(8));
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
		let db = TestDb::new("").await;
		let ctx = db.exec_ctx().await;
		let out = call_in_txn("{ double: |$x: number| $x * 2 }", ".double(4)", &ctx).await.unwrap();
		assert_eq!(out, Value::from(8));
	}

	#[tokio::test]
	async fn a_closure_argument_is_coerced_to_its_declared_type() {
		let db = TestDb::new("").await;
		let ctx = db.exec_ctx().await;
		// An int argument reaches a float parameter as a float.
		let out = call_in_txn("{ f: |$x: float| $x }", ".f(1)", &ctx).await.unwrap();
		assert_eq!(out, val("1.0f").await);

		// A value that cannot be coerced is an argument error, not a silent pass.
		let err = call_in_txn("{ f: |$x: number| $x }", ".f('nope')", &ctx).await.unwrap_err();
		assert!(
			err.to_string().contains("Expected a value of type 'number'"),
			"expected a coercion failure, got {err}"
		);
	}

	#[tokio::test]
	async fn a_missing_required_closure_argument_is_rejected() {
		let db = TestDb::new("").await;
		let ctx = db.exec_ctx().await;
		let err = call_in_txn("{ f: |$x: number| $x }", ".f()", &ctx).await.unwrap_err();
		let message = err.to_string();
		assert!(
			message.contains("Expected a value of type 'number'"),
			"expected the missing-argument error, got {message}"
		);
		// Closures have no name, so the report is anonymous.
		assert!(message.contains("ANONYMOUS"), "expected an anonymous report, got {message}");
	}

	#[tokio::test]
	async fn an_omitted_optional_closure_argument_is_none() {
		// A parameter whose kind admits NONE passes the missing-argument check,
		// and the body reads it as NONE.
		let db = TestDb::new("").await;
		let ctx = db.exec_ctx().await;
		let out = call_in_txn("{ f: |$x: option<number>| $x }", ".f()", &ctx).await.unwrap();
		assert_eq!(out, Value::None);

		// Supplying the argument binds it as normal.
		let out = call_in_txn("{ f: |$x: option<number>| $x }", ".f(3)", &ctx).await.unwrap();
		assert_eq!(out, Value::from(3));
	}

	#[tokio::test]
	async fn a_declared_closure_return_type_is_enforced() {
		let db = TestDb::new("").await;
		let ctx = db.exec_ctx().await;
		let ok = call_in_txn("{ f: |$x: number| -> float { $x } }", ".f(1)", &ctx).await.unwrap();
		assert_eq!(ok, val("1.0f").await);

		let err =
			call_in_txn("{ f: |$x: number| -> string { $x } }", ".f(1)", &ctx).await.unwrap_err();
		assert!(
			err.to_string().contains("string"),
			"expected a return-type failure naming the declared type, got {err}"
		);
	}

	#[tokio::test]
	async fn a_return_inside_the_closure_body_becomes_its_value() {
		// The body is a block: a RETURN unwinds to the closure boundary and is
		// the call's result rather than propagating out of the enclosing query.
		let db = TestDb::new("").await;
		let ctx = db.exec_ctx().await;
		let out =
			call_in_txn("{ f: |$x: number| { RETURN $x * 3 } }", ".f(2)", &ctx).await.unwrap();
		assert_eq!(out, Value::from(6));
	}

	#[tokio::test]
	async fn break_or_continue_out_of_a_closure_body_is_an_error() {
		// Neither signal has a loop to bind to at the closure boundary, so both
		// must surface as errors instead of escaping the call.
		let db = TestDb::new("").await;
		let ctx = db.exec_ctx().await;
		for body in ["{ BREAK }", "{ CONTINUE }"] {
			let err = call_in_txn(&format!("{{ f: || {body} }}"), ".f()", &ctx).await.unwrap_err();
			assert!(
				err.to_string().contains("Invalid control flow"),
				"expected InvalidControlFlow for {body}, got {err}"
			);
		}
	}

	#[test]
	fn a_closure_field_call_reports_a_write_access_mode() {
		// The field holds a runtime value: whether its body writes is
		// invisible at plan time, so the part must claim a write up front
		// even with no arguments at all. The planner picks the transaction
		// mode and the ordering barriers from this.
		let call_part = ClosureFieldCallPart {
			field: "w".to_string(),
			args: vec![],
		};
		assert_eq!(call_part.access_mode(), AccessMode::ReadWrite);
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
