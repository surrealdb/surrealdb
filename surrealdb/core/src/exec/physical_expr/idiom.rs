use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use surrealdb_types::{SqlFormat, ToSql};

use crate::catalog::providers::TableProvider;
use crate::cnf::IDIOM_RECURSION_LIMIT;
use crate::exec::function::MethodDescriptor;
use crate::exec::physical_expr::recurse::value_hash;
use crate::exec::physical_expr::{EvalContext, PhysicalExpr, RecursionCtx};
use crate::exec::physical_part::{
	PhysicalDestructurePart, PhysicalLookup, PhysicalPart, PhysicalRecurse,
	PhysicalRecurseInstruction,
};
use crate::exec::{AccessMode, CombineAccessModes};
use crate::expr::Idiom;
use crate::idx::planner::ScanDirection;
use crate::val::{RecordId, Value};

// ============================================================================
// IdiomExpr - Full idiom evaluation with complex parts
// ============================================================================

/// Full idiom expression that can evaluate complex paths including:
/// - Simple field access
/// - Array operations (All, First, Last, Flatten)
/// - Where filtering
/// - Method calls
/// - Destructuring
/// - Graph/reference lookups
/// - Recursion
///
/// For simple idioms (only Field and basic index parts), use the simpler `Field` type.
#[derive(Debug, Clone)]
pub struct IdiomExpr {
	/// The original idiom for display/debugging
	pub(crate) idiom: Idiom,
	/// Optional start expression that provides the base value for the idiom.
	/// When present, this expression is evaluated first and its result is used
	/// as the base value instead of `ctx.current_value`.
	/// This corresponds to `Part::Start(expr)` in the AST, e.g. `(INFO FOR KV).namespaces`.
	pub(crate) start_expr: Option<Arc<dyn PhysicalExpr>>,
	/// Pre-converted physical parts for evaluation
	pub(crate) parts: Vec<PhysicalPart>,
}

impl IdiomExpr {
	/// Create a new IdiomExpr with the given idiom and physical parts.
	pub fn new(
		idiom: Idiom,
		start_expr: Option<Arc<dyn PhysicalExpr>>,
		parts: Vec<PhysicalPart>,
	) -> Self {
		Self {
			idiom,
			start_expr,
			parts,
		}
	}

	/// Check if all parts are simple (can be evaluated synchronously).
	pub fn is_simple(&self) -> bool {
		self.parts.iter().all(|p| p.is_simple())
	}

	/// Check if this is a simple identifier (single Field part with no complex parts).
	/// When used without a current value context, simple identifiers can be
	/// treated as string literals (e.g., `INFO FOR USER test` where `test` is a name).
	pub fn is_simple_identifier(&self) -> bool {
		self.parts.len() == 1 && matches!(&self.parts[0], PhysicalPart::Field(_))
	}

	/// Get the simple identifier name if this is a simple identifier.
	pub fn simple_identifier_name(&self) -> Option<&str> {
		if self.parts.len() == 1
			&& let PhysicalPart::Field(name) = &self.parts[0]
		{
			return Some(name.as_str());
		}
		None
	}
}

#[async_trait]
impl PhysicalExpr for IdiomExpr {
	fn name(&self) -> &'static str {
		"IdiomExpr"
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		use crate::exec::ContextLevel;
		use crate::exec::physical_part::PhysicalPart;

		// If we have a start expression, check its context requirements
		if let Some(ref start) = self.start_expr {
			if start.required_context() == ContextLevel::Database {
				return ContextLevel::Database;
			}
		}

		// Check if any part requires database context
		// - Lookup parts (graph traversal) require database
		// - Field access on RecordId requires database (but we can't know types at plan time)
		// - Where clauses depend on their predicates
		// - Method calls may require database
		for part in &self.parts {
			match part {
				PhysicalPart::Lookup(_) => return ContextLevel::Database,
				PhysicalPart::Field(_) => {
					// Field access might trigger record fetch if applied to RecordId,
					// so we conservatively require database context
					return ContextLevel::Database;
				}
				PhysicalPart::Where(predicate) => {
					if predicate.required_context() == ContextLevel::Database {
						return ContextLevel::Database;
					}
				}
				PhysicalPart::Method {
					..
				} => {
					// Methods may require database context
					return ContextLevel::Database;
				}
				PhysicalPart::ClosureFieldCall {
					..
				} => {
					// Closure bodies may require database context
					return ContextLevel::Database;
				}
				PhysicalPart::Recurse(_) => {
					// Recursion requires database access
					return ContextLevel::Database;
				}
				PhysicalPart::Index(expr) => {
					if expr.required_context() == ContextLevel::Database {
						return ContextLevel::Database;
					}
				}
				PhysicalPart::Destructure(parts) => {
					// Check nested parts
					if parts.iter().any(|p| p.required_context() == ContextLevel::Database) {
						return ContextLevel::Database;
					}
				}
				PhysicalPart::RepeatRecurse => {
					// RepeatRecurse requires database context (recursion does graph traversal)
					return ContextLevel::Database;
				}
				PhysicalPart::All => {
					// All (.*) may trigger record fetch + computed field evaluation
					// if applied to a RecordId, so conservatively require database context
					return ContextLevel::Database;
				}
				// Simple parts that don't need database access
				PhysicalPart::Flatten
				| PhysicalPart::First
				| PhysicalPart::Last
				| PhysicalPart::Optional => {}
			}
		}

		ContextLevel::Root
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> crate::expr::FlowResult<Value> {
		// Determine the base value for the idiom evaluation.
		// If we have a start expression (e.g. `(INFO FOR KV).namespaces`), evaluate
		// it first to produce the base value. Otherwise use the current row value.
		let mut value = if let Some(ref start) = self.start_expr {
			start.evaluate(ctx.clone()).await?
		} else {
			// When there's no current_value:
			// - Simple identifiers return NONE (undefined variable)
			// - Complex idioms are an error (they need a document context)
			//
			// Note: Patterns like `INFO FOR USER test` where `test` should be a name
			// are handled at the planner level by converting simple identifiers to
			// string literals before creating the physical expression.
			match ctx.current_value {
				Some(v) => v.clone(),
				None => {
					if self.is_simple_identifier() {
						// Simple identifier without context evaluates to NONE
						// This matches legacy SurrealQL behavior for undefined variables
						return Ok(Value::None);
					}
					return Err(anyhow::anyhow!("Idiom evaluation requires current value").into());
				}
			}
		};

		for (i, part) in self.parts.iter().enumerate() {
			value = evaluate_part(&value, part, ctx.clone()).await?;

			// After a Lookup, flatten if the NEXT part is also a Lookup or Where
			// This matches legacy SurrealDB semantics
			if matches!(part, PhysicalPart::Lookup(_))
				&& let Some(next_part) = self.parts.get(i + 1)
				&& matches!(next_part, PhysicalPart::Lookup(_) | PhysicalPart::Where(_))
			{
				value = value.flatten();
			}

			// Short-circuit on None/Null for optional chaining (.?)
			// When an Optional part produces None/Null, skip all remaining parts
			// and return None immediately. This matches the semantics of
			// `a.?.b.to_string()` where if `a` is None, the chain short-circuits.
			if matches!(part, PhysicalPart::Optional) && matches!(value, Value::None | Value::Null)
			{
				return Ok(Value::None);
			}
		}

		Ok(value)
	}

	fn references_current_value(&self) -> bool {
		// When we have a start expression, the idiom provides its own base value
		// and doesn't need the current row value -- but the start expression itself
		// might reference it.
		if let Some(ref start) = self.start_expr {
			return start.references_current_value();
		}
		// Simple identifiers (single Field part) can be evaluated without current_value
		// - they return NONE (undefined variable)
		// Complex idioms require current_value to provide the base object for field access
		!self.is_simple_identifier()
	}

	fn access_mode(&self) -> AccessMode {
		let parts_mode = self.parts.iter().map(|p| p.access_mode()).combine_all();
		if let Some(ref start) = self.start_expr {
			parts_mode.combine(start.access_mode())
		} else {
			parts_mode
		}
	}
}

impl ToSql for IdiomExpr {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.idiom.fmt_sql(f, fmt);
	}
}

/// Evaluate a single physical part against a value.
async fn evaluate_part(
	value: &Value,
	part: &PhysicalPart,
	ctx: EvalContext<'_>,
) -> crate::expr::FlowResult<Value> {
	match part {
		PhysicalPart::Field(name) => Ok(evaluate_field(value, name, ctx.clone()).await?),

		PhysicalPart::Index(expr) => {
			let index = expr.evaluate(ctx).await?;
			Ok(evaluate_index(value, &index)?)
		}

		PhysicalPart::All => Ok(evaluate_all(value, ctx).await?),

		PhysicalPart::Flatten => Ok(evaluate_flatten(value)?),

		PhysicalPart::First => Ok(evaluate_first(value)?),

		PhysicalPart::Last => Ok(evaluate_last(value)?),

		PhysicalPart::Where(predicate) => evaluate_where(value, predicate.as_ref(), ctx).await,

		PhysicalPart::Method {
			descriptor,
			args,
		} => evaluate_method(value, descriptor, args, ctx).await,

		PhysicalPart::ClosureFieldCall {
			field,
			args,
		} => evaluate_closure_field_call(value, field, args, ctx).await,

		PhysicalPart::Destructure(parts) => evaluate_destructure(value, parts, ctx).await,

		PhysicalPart::Optional => {
			// Optional just returns the value as-is; None handling is done at the IdiomExpr level
			Ok(value.clone())
		}

		PhysicalPart::Lookup(lookup) => Ok(evaluate_lookup(value, lookup, ctx).await?),

		PhysicalPart::Recurse(recurse) => evaluate_recurse(value, recurse, ctx).await,

		PhysicalPart::RepeatRecurse => evaluate_repeat_recurse(value, ctx).await,
	}
}

/// Field access on objects, with support for RecordId auto-fetch.
///
/// When accessing a field on a RecordId, the record is automatically fetched
/// from the database and the field is accessed on the fetched object.
pub(crate) async fn evaluate_field(
	value: &Value,
	name: &str,
	ctx: EvalContext<'_>,
) -> anyhow::Result<Value> {
	match value {
		Value::Object(obj) => Ok(obj.get(name).cloned().unwrap_or(Value::None)),

		Value::RecordId(rid) => {
			// Fetch the record with computed fields evaluated.
			// This is necessary because computed fields (e.g. COMPUTED <~comment)
			// are not physically stored and must be dynamically evaluated.
			let fetched = fetch_record_with_computed_fields(rid, ctx).await?;
			match fetched {
				Value::Object(obj) => Ok(obj.get(name).cloned().unwrap_or(Value::None)),
				_ => Ok(Value::None),
			}
		}

		Value::Array(arr) => {
			// Apply field access to each element (may involve fetches)
			let mut results = Vec::with_capacity(arr.len());
			for v in arr.iter() {
				results.push(Box::pin(evaluate_field(v, name, ctx.clone())).await?);
			}
			Ok(Value::Array(results.into()))
		}

		_ => Ok(Value::None),
	}
}

/// Index access on arrays, sets, objects, and record IDs.
pub(crate) fn evaluate_index(value: &Value, index: &Value) -> anyhow::Result<Value> {
	match (value, index) {
		// Array with numeric index
		(Value::Array(arr), Value::Number(n)) => {
			let idx = n.to_usize();
			Ok(arr.get(idx).cloned().unwrap_or(Value::None))
		}
		// Set with numeric index
		(Value::Set(set), Value::Number(n)) => {
			let idx = n.to_usize();
			Ok(set.nth(idx).cloned().unwrap_or(Value::None))
		}
		// Array with range
		(Value::Array(arr), Value::Range(range)) => {
			let slice = range
				.as_ref()
				.clone()
				.coerce_to_typed::<i64>()
				.map_err(|e| anyhow::anyhow!("Invalid range: {}", e))?
				.slice(arr.as_slice())
				.map(|s| Value::Array(s.to_vec().into()))
				.unwrap_or(Value::None);
			Ok(slice)
		}
		// Object with string key
		(Value::Object(obj), Value::String(key)) => {
			Ok(obj.get(key.as_str()).cloned().unwrap_or(Value::None))
		}
		// Object with numeric key (converted to string)
		(Value::Object(obj), Value::Number(n)) => {
			let key = n.to_string();
			Ok(obj.get(&key).cloned().unwrap_or(Value::None))
		}
		// RecordId with numeric index - access the key as an array
		// This handles patterns like `id[1]` where `id` is a RecordId with array key
		(Value::RecordId(rid), Value::Number(n)) => {
			use crate::val::record_id::RecordIdKey;
			// The key might be an array (e.g., [3, o:1]) or a single value
			match &rid.key {
				RecordIdKey::Array(arr) => {
					let idx = n.to_usize();
					Ok(arr.get(idx).cloned().unwrap_or(Value::None))
				}
				// For non-array keys, index 0 returns the key value, others return None
				RecordIdKey::Number(num) => {
					if n.to_usize() == 0 {
						Ok(Value::from(*num))
					} else {
						Ok(Value::None)
					}
				}
				RecordIdKey::String(s) => {
					if n.to_usize() == 0 {
						Ok(Value::from(s.clone()))
					} else {
						Ok(Value::None)
					}
				}
				RecordIdKey::Uuid(u) => {
					if n.to_usize() == 0 {
						Ok(Value::from(*u))
					} else {
						Ok(Value::None)
					}
				}
				RecordIdKey::Object(o) => {
					if n.to_usize() == 0 {
						Ok(Value::from(o.clone()))
					} else {
						Ok(Value::None)
					}
				}
				RecordIdKey::Range(_) => {
					// Ranges don't support indexing
					Ok(Value::None)
				}
			}
		}
		_ => Ok(Value::None),
	}
}

/// All elements - `[*]` or `.*`.
///
/// When applied to a RecordId (e.g., `record.*`), fetches the record and returns it as an object.
/// When applied to an array of RecordIds (e.g., from `->edge->target.*`), fetches each record.
///
/// This function also evaluates computed fields on fetched records, ensuring that
/// recursive computed fields are properly resolved.
pub(crate) async fn evaluate_all(value: &Value, ctx: EvalContext<'_>) -> anyhow::Result<Value> {
	match value {
		Value::Array(arr) => {
			// Check if the array contains RecordIds that need fetching
			// This handles `->edge->target.*` where the lookup returns an array of RecordIds
			let has_record_ids = arr.iter().any(|v| matches!(v, Value::RecordId(_)));
			if has_record_ids {
				let mut results = Vec::with_capacity(arr.len());
				for item in arr.iter() {
					let fetched = Box::pin(evaluate_all(item, ctx.clone())).await?;
					results.push(fetched);
				}
				Ok(Value::Array(results.into()))
			} else {
				Ok(Value::Array(arr.clone()))
			}
		}
		Value::Object(obj) => {
			// Return all values from the object as an array
			Ok(Value::Array(obj.values().cloned().collect::<Vec<_>>().into()))
		}
		Value::RecordId(rid) => {
			// Fetch the record and return the full object with computed fields evaluated
			// This handles `record.*` syntax
			fetch_record_with_computed_fields(rid, ctx).await
		}
		// For other types, return as single-element array
		other => Ok(Value::Array(vec![other.clone()].into())),
	}
}

/// Fetch a record and evaluate any computed fields on it.
///
/// This is necessary for computed fields that reference other computed fields
/// to work correctly (e.g., `DEFINE FIELD subproducts ON product COMPUTED ->contains->product.*`).
async fn fetch_record_with_computed_fields(
	rid: &RecordId,
	ctx: EvalContext<'_>,
) -> anyhow::Result<Value> {
	use reblessive::TreeStack;

	use crate::catalog::providers::TableProvider;

	let db_ctx = ctx.exec_ctx.database().map_err(|e| anyhow::anyhow!("{}", e))?;
	let txn = ctx.exec_ctx.txn();

	// Fetch the raw record from storage
	let record = txn
		.get_record(
			db_ctx.ns_ctx.ns.namespace_id,
			db_ctx.db.database_id,
			&rid.table,
			&rid.key,
			None,
		)
		.await
		.map_err(|e| anyhow::anyhow!("Failed to fetch record: {}", e))?;

	let mut result = record.data.as_ref().clone();

	// If the record doesn't exist (e.g. was deleted), return None early.
	// Don't proceed to evaluate computed fields on a non-existent record.
	if result.is_none() {
		return Ok(Value::None);
	}

	// Get the table's field definitions to check for computed fields
	let fields = txn
		.all_tb_fields(db_ctx.ns_ctx.ns.namespace_id, db_ctx.db.database_id, &rid.table, None)
		.await
		.map_err(|e| anyhow::anyhow!("Failed to get field definitions: {}", e))?;

	// Check if any fields have computed values
	let has_computed = fields.iter().any(|fd| fd.computed.is_some());

	if has_computed {
		// We need to evaluate computed fields using the legacy compute path
		// Get the Options from the context (if available)
		let root = ctx.exec_ctx.root();
		if let Some(ref opt) = root.options {
			let frozen = &root.ctx;
			let rid_arc = std::sync::Arc::new(rid.clone());
			let fields_clone = fields.clone();

			// Use TreeStack for stack management during recursive computation
			let mut stack = TreeStack::new();
			result = stack
				.enter(|stk| async move {
					let mut doc_value = result;
					for fd in fields_clone.iter() {
						if let Some(computed) = &fd.computed {
							// Evaluate the computed expression using the legacy compute method
							// The document context is the current result value
							let doc = crate::doc::CursorDoc::new(
								Some(rid_arc.clone()),
								None,
								doc_value.clone(),
							);
							match computed.compute(stk, frozen, opt, Some(&doc)).await {
								Ok(val) => {
									// Coerce to the field's type if specified
									let coerced_val = if let Some(kind) = fd.field_kind.as_ref() {
										val.clone().coerce_to_kind(kind).unwrap_or(val)
									} else {
										val
									};
									doc_value.put(&fd.name, coerced_val);
								}
								Err(crate::expr::ControlFlow::Return(val)) => {
									doc_value.put(&fd.name, val);
								}
								Err(_) => {
									// If computation fails, leave the field as-is or set to None
									doc_value.put(&fd.name, Value::None);
								}
							}
						}
					}
					doc_value
				})
				.finish()
				.await;
		}
	}

	// Ensure the record has its ID
	result.def(rid);

	Ok(result)
}

/// Flatten nested arrays.
pub(crate) fn evaluate_flatten(value: &Value) -> anyhow::Result<Value> {
	match value {
		Value::Array(arr) => {
			let mut result = Vec::new();
			for item in arr.iter() {
				match item {
					Value::Array(inner) => result.extend(inner.iter().cloned()),
					other => result.push(other.clone()),
				}
			}
			Ok(Value::Array(result.into()))
		}
		other => Ok(other.clone()),
	}
}

/// First element.
pub(crate) fn evaluate_first(value: &Value) -> anyhow::Result<Value> {
	match value {
		Value::Array(arr) => Ok(arr.first().cloned().unwrap_or(Value::None)),
		other => Ok(other.clone()),
	}
}

/// Last element.
pub(crate) fn evaluate_last(value: &Value) -> anyhow::Result<Value> {
	match value {
		Value::Array(arr) => Ok(arr.last().cloned().unwrap_or(Value::None)),
		other => Ok(other.clone()),
	}
}

/// Where filtering on arrays.
async fn evaluate_where(
	value: &Value,
	predicate: &dyn PhysicalExpr,
	ctx: EvalContext<'_>,
) -> crate::expr::FlowResult<Value> {
	match value {
		Value::Array(arr) => {
			let mut result = Vec::new();
			for item in arr.iter() {
				let item_ctx = ctx.with_value(item);
				let matches = predicate.evaluate(item_ctx).await?.is_truthy();
				if matches {
					result.push(item.clone());
				}
			}
			Ok(Value::Array(result.into()))
		}
		// For non-arrays, check if the single value matches
		other => {
			let item_ctx = ctx.with_value(other);
			let matches = predicate.evaluate(item_ctx).await?.is_truthy();
			if matches {
				Ok(Value::Array(vec![other.clone()].into()))
			} else {
				Ok(Value::Array(crate::val::Array::default()))
			}
		}
	}
}

/// Method call evaluation.
///
/// Methods are syntactic sugar for function calls. For example:
/// - `"hello".len()` → `string::len("hello")`
/// - `[1, 2, 3].len()` → `array::len([1, 2, 3])`
///
/// The method descriptor (resolved at plan time) contains the per-type function
/// dispatch table. At eval time we just look up the function for the value's type.
async fn evaluate_method(
	value: &Value,
	descriptor: &MethodDescriptor,
	args: &[Arc<dyn PhysicalExpr>],
	ctx: EvalContext<'_>,
) -> crate::expr::FlowResult<Value> {
	// Resolve the function for this value's type
	let func = descriptor.resolve(value)?;

	// Build the arguments: receiver value first, then method arguments
	let mut func_args = Vec::with_capacity(1 + args.len());
	func_args.push(value.clone());
	for arg_expr in args {
		let arg_value = arg_expr.evaluate(ctx.clone()).await?;
		func_args.push(arg_value);
	}

	// Invoke the resolved function
	if func.is_pure() {
		Ok(func.invoke(func_args)?)
	} else {
		Ok(func.invoke_async(&ctx, func_args).await?)
	}
}

/// Closure field call evaluation.
///
/// When a method name is not found in the method registry at plan time,
/// the planner creates a `ClosureFieldCall` part. At runtime, this
/// accesses the named field on the value and, if it contains a closure,
/// invokes it with the provided arguments.
///
/// This implements the "fallback function" pattern where `$obj.fnc()`
/// first checks built-in methods (handled by `Method` part) and falls back
/// to field access + closure invocation (this part).
async fn evaluate_closure_field_call(
	value: &Value,
	field: &str,
	args: &[Arc<dyn PhysicalExpr>],
	ctx: EvalContext<'_>,
) -> crate::expr::FlowResult<Value> {
	use std::collections::HashMap;

	use super::block::BlockPhysicalExpr;
	use super::function::validate_return;
	use crate::err::Error;
	use crate::val::Closure;

	// Get the field value from the object
	let field_value = match value {
		Value::Object(obj) => obj.get(field).cloned(),
		_ => None,
	};

	// Check if the field contains a closure
	let closure = match field_value {
		Some(Value::Closure(c)) => c,
		_ => {
			return Err(Error::InvalidFunction {
				name: field.to_string(),
				message: "no such method found for the object type".to_string(),
			}
			.into());
		}
	};

	// Evaluate all argument expressions
	let mut evaluated_args = Vec::with_capacity(args.len());
	for arg_expr in args {
		evaluated_args.push(arg_expr.evaluate(ctx.clone()).await?);
	}

	// Invoke the closure (same logic as ClosureCallExec)
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
				Err(crate::expr::ControlFlow::Break) | Err(crate::expr::ControlFlow::Continue) => {
					return Err(Error::InvalidControlFlow.into());
				}
				Err(e) => return Err(e),
			};

			// Coerce return value to declared type if specified
			Ok(validate_return("ANONYMOUS", returns.as_ref(), result)?)
		}
		Closure::Builtin(_) => {
			Err(anyhow::anyhow!("Builtin closures are not yet supported in the streaming executor")
				.into())
		}
	}
}

/// Destructure evaluation - extract fields into a new object.
async fn evaluate_destructure(
	value: &Value,
	parts: &[PhysicalDestructurePart],
	ctx: EvalContext<'_>,
) -> crate::expr::FlowResult<Value> {
	match value {
		Value::Object(obj) => {
			let mut result = std::collections::BTreeMap::new();

			for part in parts {
				match part {
					PhysicalDestructurePart::All(field) => {
						// Include all fields from the nested object
						if let Some(Value::Object(nested)) = obj.get(field.as_str()) {
							for (k, v) in nested.iter() {
								result.insert(k.clone(), v.clone());
							}
						}
					}
					PhysicalDestructurePart::Field(field) => {
						let v = obj.get(field.as_str()).cloned().unwrap_or(Value::None);
						result.insert(field.clone(), v);
					}
					PhysicalDestructurePart::Aliased {
						field,
						path,
					} => {
						// Evaluate the aliased path starting from the current value
						// (not from obj.get(field)). The field name is just the output label.
						// For example, `team_name: team.name` evaluates `team.name` on the
						// current record and stores the result under key "team_name".
						let v = evaluate_physical_path(value, path, ctx.clone()).await?;
						result.insert(field.clone(), v);
					}
					PhysicalDestructurePart::Nested {
						field,
						parts: nested_parts,
					} => {
						let nested_value = obj.get(field.as_str()).cloned().unwrap_or(Value::None);
						let v = Box::pin(evaluate_destructure(
							&nested_value,
							nested_parts,
							ctx.clone(),
						))
						.await?;
						result.insert(field.clone(), v);
					}
				}
			}

			Ok(Value::Object(crate::val::Object(result)))
		}
		Value::RecordId(rid) => {
			// Fetch the record from the database, then apply destructure to it
			let db_ctx = ctx.exec_ctx.database().map_err(|e| anyhow::anyhow!("{}", e))?;
			let txn = ctx.exec_ctx.txn();
			let record = txn
				.get_record(
					db_ctx.ns_ctx.ns.namespace_id,
					db_ctx.db.database_id,
					&rid.table,
					&rid.key,
					None,
				)
				.await
				.map_err(|e| anyhow::anyhow!("Failed to fetch record: {}", e))?;

			// Get the record data as an object
			let fetched = record.data.as_ref();
			if fetched.is_none() {
				return Ok(Value::None);
			}

			// Continue destructure on the fetched object
			Box::pin(evaluate_destructure(fetched, parts, ctx)).await
		}
		Value::Array(arr) => {
			// Apply destructure to each element
			let mut results = Vec::with_capacity(arr.len());
			for item in arr.iter() {
				let v = Box::pin(evaluate_destructure(item, parts, ctx.clone())).await?;
				results.push(v);
			}
			Ok(Value::Array(results.into()))
		}
		_ => Ok(Value::None),
	}
}

/// Lookup evaluation - graph/reference traversal.
async fn evaluate_lookup(
	value: &Value,
	lookup: &PhysicalLookup,
	ctx: EvalContext<'_>,
) -> anyhow::Result<Value> {
	match value {
		Value::RecordId(rid) => {
			// Perform graph edge scan for this RecordId
			evaluate_lookup_for_rid(rid, lookup, ctx).await
		}
		Value::Object(obj) => {
			// When lookup is on an Object, extract the `id` field and evaluate on that
			// This matches SurrealDB semantics: `->edge` on an object uses its `id`
			match obj.get("id") {
				Some(Value::RecordId(rid)) => {
					Box::pin(evaluate_lookup(&Value::RecordId(rid.clone()), lookup, ctx)).await
				}
				Some(other) => {
					// If `id` is not a RecordId, try to evaluate on it anyway
					Box::pin(evaluate_lookup(other, lookup, ctx)).await
				}
				None => Ok(Value::None),
			}
		}
		Value::Array(arr) => {
			// Apply lookup to each element and flatten results
			// This matches SurrealDB semantics: `->edge` on an array of records
			// returns a flat array of all targets, not nested arrays
			let mut results = Vec::new();
			for item in arr.iter() {
				let result = Box::pin(evaluate_lookup(item, lookup, ctx.clone())).await?;
				// Flatten: extend results with array elements, or push single values
				match result {
					Value::Array(inner) => results.extend(inner.into_iter()),
					other => results.push(other),
				}
			}
			Ok(Value::Array(results.into()))
		}
		_ => Ok(Value::None),
	}
}

/// Perform graph/reference lookup for a specific RecordId by executing the pre-planned operator
/// tree.
///
/// This function:
/// 1. Creates an ExecutionContext with the source RecordId bound to a special parameter
/// 2. Executes the lookup plan (which may include GraphEdgeScan/ReferenceScan + Filter + Sort +
///    Limit)
/// 3. Collects and returns the results
async fn evaluate_lookup_for_rid(
	rid: &RecordId,
	lookup: &PhysicalLookup,
	ctx: EvalContext<'_>,
) -> anyhow::Result<Value> {
	use crate::exec::planner::LOOKUP_SOURCE_PARAM;

	// Create a new execution context with the source RecordId bound to the special parameter.
	// This allows the plan's source expression (Param("__lookup_source__")) to access the RecordId.
	let bound_ctx = ctx.exec_ctx.with_param(LOOKUP_SOURCE_PARAM, Value::RecordId(rid.clone()));

	// Execute the lookup plan
	let stream = lookup.plan.execute(&bound_ctx).map_err(|e| match e {
		crate::expr::ControlFlow::Err(e) => e,
		crate::expr::ControlFlow::Return(v) => {
			anyhow::anyhow!("Unexpected return in lookup: {:?}", v)
		}
		crate::expr::ControlFlow::Break => anyhow::anyhow!("Unexpected break in lookup"),
		crate::expr::ControlFlow::Continue => anyhow::anyhow!("Unexpected continue in lookup"),
	})?;

	// Collect all results into an array
	let mut results = Vec::new();
	futures::pin_mut!(stream);

	while let Some(batch_result) = stream.next().await {
		let batch = batch_result.map_err(|e| match e {
			crate::expr::ControlFlow::Err(e) => e,
			crate::expr::ControlFlow::Return(v) => {
				anyhow::anyhow!("Unexpected return in lookup: {:?}", v)
			}
			crate::expr::ControlFlow::Break => anyhow::anyhow!("Unexpected break in lookup"),
			crate::expr::ControlFlow::Continue => anyhow::anyhow!("Unexpected continue in lookup"),
		})?;
		results.extend(batch.values);
	}

	// When extract_id is set, the scan used FullEdge mode for WHERE/SPLIT filtering
	// but no explicit SELECT clause was present. Project results back to RecordIds.
	if lookup.extract_id {
		let results = results
			.into_iter()
			.filter_map(|v| match v {
				Value::Object(ref obj) => {
					obj.get("id").filter(|id| matches!(id, Value::RecordId(_))).cloned()
				}
				Value::RecordId(_) => Some(v),
				_ => None,
			})
			.collect();
		return Ok(Value::Array(results));
	}

	Ok(Value::Array(results.into()))
}

/// Perform reference lookup (<~) for a specific RecordId.
///
/// Reference lookups find all records that reference the given record ID
/// through a specific field. This is the inverse of a record link.
///
/// Example: `person:alice<~post.author` finds all posts where the author field
/// references person:alice.
async fn evaluate_reference_lookup(
	rid: &RecordId,
	lookup: &PhysicalLookup,
	ctx: EvalContext<'_>,
) -> anyhow::Result<Value> {
	// Get database context
	let db_ctx = ctx.exec_ctx.database().map_err(|e| anyhow::anyhow!("{}", e))?;
	let txn = ctx.exec_ctx.txn();
	let ns = &db_ctx.ns_ctx.ns;
	let db = &db_ctx.db;

	let mut results = Vec::new();

	// For reference lookups, edge_tables contains the referencing tables
	// If empty, we'd need to scan all tables (not supported for now)
	if lookup.edge_tables.is_empty() {
		// Scan all references to this record
		let beg = crate::key::r#ref::prefix(ns.namespace_id, db.database_id, &rid.table, &rid.key)
			.map_err(|e| anyhow::anyhow!("Failed to create prefix: {}", e))?;

		let end = crate::key::r#ref::suffix(ns.namespace_id, db.database_id, &rid.table, &rid.key)
			.map_err(|e| anyhow::anyhow!("Failed to create suffix: {}", e))?;

		let kv_stream = txn.stream_keys(beg..end, None, None, ScanDirection::Forward);
		futures::pin_mut!(kv_stream);

		while let Some(result) = kv_stream.next().await {
			let key = result.map_err(|e| anyhow::anyhow!("Failed to scan reference: {}", e))?;

			// Decode the reference key to get the referencing record ID
			let decoded = crate::key::r#ref::Ref::decode_key(&key)
				.map_err(|e| anyhow::anyhow!("Failed to decode ref key: {}", e))?;

			// The referencing record ID (ft = foreign table, fk = foreign key)
			let referencing_rid = RecordId {
				table: decoded.ft.into_owned(),
				key: decoded.fk.into_owned(),
			};
			results.push(Value::RecordId(referencing_rid));
		}
	} else {
		// Scan references from specific tables
		for ref_table in &lookup.edge_tables {
			let beg = crate::key::r#ref::ftprefix(
				ns.namespace_id,
				db.database_id,
				&rid.table,
				&rid.key,
				ref_table.as_str(),
			)
			.map_err(|e| anyhow::anyhow!("Failed to create prefix: {}", e))?;

			let end = crate::key::r#ref::ftsuffix(
				ns.namespace_id,
				db.database_id,
				&rid.table,
				&rid.key,
				ref_table.as_str(),
			)
			.map_err(|e| anyhow::anyhow!("Failed to create suffix: {}", e))?;

			let kv_stream = txn.stream_keys(beg..end, None, None, ScanDirection::Forward);
			futures::pin_mut!(kv_stream);

			while let Some(result) = kv_stream.next().await {
				let key = result.map_err(|e| anyhow::anyhow!("Failed to scan reference: {}", e))?;

				// Decode the reference key to get the referencing record ID
				let decoded = crate::key::r#ref::Ref::decode_key(&key)
					.map_err(|e| anyhow::anyhow!("Failed to decode ref key: {}", e))?;

				// The referencing record ID
				let referencing_rid = RecordId {
					table: decoded.ft.into_owned(),
					key: decoded.fk.into_owned(),
				};
				results.push(Value::RecordId(referencing_rid));
			}
		}
	}

	Ok(Value::Array(results.into()))
}

/// Recurse evaluation - bounded/unbounded recursion.
///
/// Implements recursive graph traversal with various collection strategies:
/// - Default: Follow path until bounds or dead end, return final value
/// - Collect: Gather all unique nodes encountered during traversal
/// - Path: Return all paths as arrays of arrays
/// - Shortest: Find shortest path to a target node (BFS)
///
/// Note: This function uses `Box::pin` to handle the recursive nature of
/// path evaluation (evaluate_part -> evaluate_recurse -> evaluate_physical_path).
fn evaluate_recurse<'a>(
	value: &'a Value,
	recurse: &'a PhysicalRecurse,
	ctx: EvalContext<'a>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = crate::expr::FlowResult<Value>> + Send + 'a>>
{
	Box::pin(async move {
		// Get the system recursion limit
		let system_limit = *IDIOM_RECURSION_LIMIT as u32;
		let max_depth = recurse.max_depth.unwrap_or(system_limit).min(system_limit);

		// When the path contains RepeatRecurse markers, use single-step evaluation.
		// The tree is built recursively through RepeatRecurse callbacks, not by looping.
		if recurse.has_repeat_recurse {
			let rec_ctx = RecursionCtx {
				path: &recurse.path,
				min_depth: recurse.min_depth,
				max_depth: Some(max_depth),
				instruction: &recurse.instruction,
				inclusive: recurse.inclusive,
				depth: 0,
			};
			return evaluate_recurse_with_plan(
				value,
				&recurse.path,
				ctx.with_recursion_ctx(rec_ctx),
			)
			.await;
		}

		match &recurse.instruction {
			PhysicalRecurseInstruction::Default => {
				evaluate_recurse_default(value, &recurse.path, recurse.min_depth, max_depth, ctx)
					.await
			}
			PhysicalRecurseInstruction::Collect => {
				evaluate_recurse_collect(
					value,
					&recurse.path,
					recurse.min_depth,
					max_depth,
					recurse.inclusive,
					ctx,
				)
				.await
			}
			PhysicalRecurseInstruction::Path => {
				evaluate_recurse_path(
					value,
					&recurse.path,
					recurse.min_depth,
					max_depth,
					recurse.inclusive,
					ctx,
				)
				.await
			}
			PhysicalRecurseInstruction::Shortest {
				target,
			} => {
				let target_value = target.evaluate(ctx.clone()).await?;
				evaluate_recurse_shortest(
					value,
					&target_value,
					&recurse.path,
					recurse.min_depth,
					max_depth,
					recurse.inclusive,
					ctx,
				)
				.await
			}
		}
	})
}

/// Evaluate a recursion that contains RepeatRecurse markers.
///
/// This performs a single evaluation of the path on the current value.
/// The actual recursion happens through RepeatRecurse callbacks within
/// the path evaluation (e.g., inside Destructure aliased fields).
///
/// The recursion context is set in EvalContext so that RepeatRecurse
/// handlers can re-invoke this function with incremented depth.
fn evaluate_recurse_with_plan<'a>(
	value: &'a Value,
	path: &'a [PhysicalPart],
	ctx: EvalContext<'a>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = crate::expr::FlowResult<Value>> + Send + 'a>>
{
	Box::pin(async move {
		let rec_ctx = ctx.recursion_ctx.as_ref().expect("recursion context must be set");
		let max_depth = rec_ctx.max_depth.unwrap_or(256);

		// Check depth limit before evaluating
		if rec_ctx.depth >= max_depth {
			return Ok(value.clone());
		}

		// Check if the value is final (dead end)
		if is_final(value) {
			return Ok(clean_iteration(get_final(value)));
		}

		// Evaluate the path once on the current value.
		// RepeatRecurse markers within the path will recursively call back
		// into evaluate_recurse_with_plan via evaluate_repeat_recurse.
		let value_ctx = ctx.with_value(value);
		evaluate_physical_path(value, path, value_ctx).await
	})
}

/// Handle the RepeatRecurse (@) marker during path evaluation.
///
/// This reads the recursion context from EvalContext and re-invokes
/// the recursion evaluator on the current value. For Array values,
/// each element is processed individually to build the recursive tree.
fn evaluate_repeat_recurse<'a>(
	value: &'a Value,
	ctx: EvalContext<'a>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = crate::expr::FlowResult<Value>> + Send + 'a>>
{
	Box::pin(async move {
		let rec_ctx = match &ctx.recursion_ctx {
			Some(rc) => rc.clone(),
			None => {
				// RepeatRecurse outside recursion context is an error
				return Err(crate::expr::ControlFlow::Err(anyhow::anyhow!(
					crate::err::Error::Query {
						message: "RepeatRecurse (@) used outside recursion context".to_string(),
					}
				)));
			}
		};

		// Increment depth for the recursive call
		let next_ctx = RecursionCtx {
			depth: rec_ctx.depth + 1,
			..rec_ctx
		};

		match value {
			// For arrays, process each element individually and collect results
			Value::Array(arr) => {
				let mut results = Vec::with_capacity(arr.len());
				for elem in arr.iter() {
					let elem_ctx = ctx.with_recursion_ctx(next_ctx.clone());
					let result = evaluate_recurse_with_plan(elem, next_ctx.path, elem_ctx).await?;
					// Filter out dead-end values
					if !is_final(&result) {
						results.push(result);
					}
				}
				Ok(Value::Array(results.into()))
			}
			// For single values, recurse directly
			_ => {
				let elem_ctx = ctx.with_recursion_ctx(next_ctx.clone());
				evaluate_recurse_with_plan(value, next_ctx.path, elem_ctx).await
			}
		}
	})
}

/// Get the final value for a dead-end in recursion.
fn get_final(value: &Value) -> Value {
	match value {
		Value::Array(_) => Value::Array(crate::val::Array(vec![])),
		Value::Null => Value::Null,
		_ => Value::None,
	}
}

/// Evaluate a path of PhysicalParts against a value.
///
/// This helper function traverses a sequence of parts, applying each one
/// in order to the current value. Used by recursion and can be reused
/// for other path evaluation needs.
///
/// Note: This function uses `Box::pin` internally to handle the recursive
/// nature of path evaluation (evaluate_part -> evaluate_recurse -> evaluate_physical_path).
pub(crate) fn evaluate_physical_path<'a>(
	value: &'a Value,
	path: &'a [PhysicalPart],
	ctx: EvalContext<'a>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = crate::expr::FlowResult<Value>> + Send + 'a>>
{
	Box::pin(async move {
		let mut current = value.clone();
		for (i, part) in path.iter().enumerate() {
			current = Box::pin(evaluate_part(&current, part, ctx.clone())).await?;

			// After a Lookup, flatten if the NEXT part is also a Lookup or Where
			// This matches SurrealDB semantics for consecutive lookups
			if matches!(part, PhysicalPart::Lookup(_))
				&& let Some(next_part) = path.get(i + 1)
				&& matches!(next_part, PhysicalPart::Lookup(_) | PhysicalPart::Where(_))
			{
				current = current.flatten();
			}
		}
		Ok(current)
	})
}

/// Check if a value is "final" (terminates recursion).
///
/// A value is final if it's None, Null, or an empty/all-none array.
fn is_final(value: &Value) -> bool {
	match value {
		Value::None | Value::Null => true,
		Value::Array(arr) => {
			arr.is_empty() || arr.iter().all(|v| matches!(v, Value::None | Value::Null))
		}
		_ => false,
	}
}

/// Clean iteration result by filtering out final values from arrays.
fn clean_iteration(value: Value) -> Value {
	if let Value::Array(arr) = value {
		let filtered: Vec<Value> = arr.into_iter().filter(|v| !is_final(v)).collect();
		Value::Array(filtered.into()).flatten()
	} else {
		value
	}
}

/// Default recursion: keep following the path until bounds or dead end.
///
/// Returns the final value after traversing the path up to max_depth times,
/// or None if min_depth is not reached before termination.
async fn evaluate_recurse_default(
	start: &Value,
	path: &[PhysicalPart],
	min_depth: u32,
	max_depth: u32,
	ctx: EvalContext<'_>,
) -> crate::expr::FlowResult<Value> {
	let mut current = start.clone();
	let mut depth = 0u32;

	while depth < max_depth {
		// Evaluate the path on the current value
		let value_ctx = ctx.with_value(&current);
		let next = evaluate_physical_path(&current, path, value_ctx).await?;

		depth += 1;

		// Clean up dead ends from array results
		let next = clean_iteration(next);

		// Check termination conditions
		if is_final(&next) || next == current {
			// Reached a dead end or cycle.
			// Use `depth > min_depth` (not `>=`) because the current iteration
			// produced a dead end, so we've only completed (depth - 1) successful
			// traversals. At the exact min_depth boundary, hitting a dead end means
			// we haven't fulfilled the minimum requirement.
			return if depth > min_depth {
				Ok(current)
			} else {
				Ok(Value::None)
			};
		}

		current = next;
	}

	// Reached max depth
	if depth >= min_depth {
		Ok(current)
	} else {
		Ok(Value::None)
	}
}

/// Collect recursion: gather all unique nodes encountered during BFS traversal.
///
/// Uses breadth-first search to collect all reachable nodes, respecting
/// depth bounds and avoiding cycles via hash-based deduplication.
async fn evaluate_recurse_collect(
	start: &Value,
	path: &[PhysicalPart],
	min_depth: u32,
	max_depth: u32,
	inclusive: bool,
	ctx: EvalContext<'_>,
) -> crate::expr::FlowResult<Value> {
	let mut collected = Vec::new();
	let mut seen: HashSet<u64> = HashSet::new();
	let mut frontier = vec![start.clone()];

	// Include starting node if inclusive
	if inclusive {
		collected.push(start.clone());
		seen.insert(value_hash(start));
	}

	let mut depth = 0u32;

	while depth < max_depth && !frontier.is_empty() {
		let mut next_frontier = Vec::new();

		for value in frontier {
			let value_ctx = ctx.with_value(&value);
			let result = evaluate_physical_path(&value, path, value_ctx).await?;

			// Process result (may be single value or array)
			let values = match result {
				Value::Array(arr) => arr.into_iter().collect::<Vec<_>>(),
				Value::None | Value::Null => continue,
				other => vec![other],
			};

			for v in values {
				if is_final(&v) {
					continue;
				}

				let hash = value_hash(&v);
				if !seen.contains(&hash) {
					seen.insert(hash);
					// Only collect if we've reached minimum depth
					if depth + 1 >= min_depth {
						collected.push(v.clone());
					}
					next_frontier.push(v);
				}
			}
		}

		frontier = next_frontier;
		depth += 1;
	}

	Ok(Value::Array(collected.into()))
}

/// Path recursion: return all paths as arrays of arrays.
///
/// Tracks all possible paths through the graph, returning each complete
/// path as an array. Paths terminate at dead ends or max depth.
async fn evaluate_recurse_path(
	start: &Value,
	path: &[PhysicalPart],
	min_depth: u32,
	max_depth: u32,
	inclusive: bool,
	ctx: EvalContext<'_>,
) -> crate::expr::FlowResult<Value> {
	let mut completed_paths: Vec<Value> = Vec::new();
	let mut active_paths: Vec<Vec<Value>> = if inclusive {
		vec![vec![start.clone()]]
	} else {
		vec![vec![]]
	};

	let mut depth = 0u32;

	while depth < max_depth && !active_paths.is_empty() {
		let mut next_paths = Vec::new();

		for current_path in active_paths {
			let current_value = current_path.last().unwrap_or(start);
			let value_ctx = ctx.with_value(current_value);
			let result = evaluate_physical_path(current_value, path, value_ctx).await?;

			let values = match result {
				Value::Array(arr) => arr.into_iter().collect::<Vec<_>>(),
				Value::None | Value::Null => {
					// Dead end - path is complete if min depth reached
					if depth >= min_depth && !current_path.is_empty() {
						completed_paths.push(Value::Array(current_path.into()));
					}
					continue;
				}
				other => vec![other],
			};

			if values.is_empty() || values.iter().all(is_final) {
				// Dead end
				if depth >= min_depth && !current_path.is_empty() {
					completed_paths.push(Value::Array(current_path.into()));
				}
			} else {
				// Extend path with each new value
				for v in values {
					if is_final(&v) {
						continue;
					}
					let mut new_path = current_path.clone();
					new_path.push(v);
					next_paths.push(new_path);
				}
			}
		}

		active_paths = next_paths;
		depth += 1;
	}

	// Add remaining active paths that reached max depth
	for path in active_paths {
		if !path.is_empty() && depth >= min_depth {
			completed_paths.push(Value::Array(path.into()));
		}
	}

	Ok(Value::Array(completed_paths.into()))
}

/// Shortest path recursion: find the shortest path to a target node using BFS.
///
/// Returns the first (shortest) path found to the target, or None if the
/// target is not reachable within max_depth.
async fn evaluate_recurse_shortest(
	start: &Value,
	target: &Value,
	path: &[PhysicalPart],
	min_depth: u32,
	max_depth: u32,
	inclusive: bool,
	ctx: EvalContext<'_>,
) -> crate::expr::FlowResult<Value> {
	let mut seen: HashSet<u64> = HashSet::new();

	// BFS with path tracking
	let initial_path = if inclusive {
		vec![start.clone()]
	} else {
		vec![]
	};
	let mut queue: VecDeque<(Value, Vec<Value>)> = VecDeque::new();
	queue.push_back((start.clone(), initial_path));
	seen.insert(value_hash(start));

	let mut depth = 0u32;

	while depth < max_depth && !queue.is_empty() {
		let level_size = queue.len();

		for _ in 0..level_size {
			let (current, current_path) = queue.pop_front().unwrap();

			let value_ctx = ctx.with_value(&current);
			let result = evaluate_physical_path(&current, path, value_ctx).await?;

			let values = match result {
				Value::Array(arr) => arr.into_iter().collect::<Vec<_>>(),
				Value::None | Value::Null => continue,
				other => vec![other],
			};

			for v in values {
				if is_final(&v) {
					continue;
				}

				// Check if we found the target (only if min_depth reached)
				if depth + 1 >= min_depth && &v == target {
					let mut final_path = current_path.clone();
					final_path.push(v);
					return Ok(Value::Array(final_path.into()));
				}

				let hash = value_hash(&v);
				if !seen.contains(&hash) {
					seen.insert(hash);
					let mut new_path = current_path.clone();
					new_path.push(v.clone());
					queue.push_back((v, new_path));
				}
			}
		}

		depth += 1;
	}

	// Target not found within max_depth.
	// Return the deepest explored paths in path format (array of arrays),
	// or NONE if no paths were explored beyond the start.
	let remaining_paths: Vec<Value> = queue
		.into_iter()
		.filter(|(_, p)| !p.is_empty())
		.map(|(_, p)| Value::Array(p.into()))
		.collect();

	if remaining_paths.is_empty() {
		Ok(Value::None)
	} else {
		Ok(Value::Array(remaining_paths.into()))
	}
}
