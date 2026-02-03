use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use surrealdb_types::{SqlFormat, ToSql};

use crate::catalog::providers::TableProvider;
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::physical_part::{
	LookupDirection, PhysicalDestructurePart, PhysicalLookup, PhysicalPart,
};
use crate::exec::{AccessMode, CombineAccessModes};
use crate::expr::{Dir, Idiom};
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
	/// Pre-converted physical parts for evaluation
	pub(crate) parts: Vec<PhysicalPart>,
}

impl IdiomExpr {
	/// Create a new IdiomExpr with the given idiom and physical parts.
	pub fn new(idiom: Idiom, parts: Vec<PhysicalPart>) -> Self {
		Self {
			idiom,
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
		if self.parts.len() == 1 {
			if let PhysicalPart::Field(name) = &self.parts[0] {
				return Some(name.as_str());
			}
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
				// Simple parts that don't need database access
				PhysicalPart::All
				| PhysicalPart::Flatten
				| PhysicalPart::First
				| PhysicalPart::Last
				| PhysicalPart::Optional => {}
			}
		}

		ContextLevel::Root
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> anyhow::Result<Value> {
		// Handle simple identifiers without current_value - treat as string literal
		// This supports patterns like `INFO FOR USER test` where `test` is a name
		let current = match ctx.current_value {
			Some(v) => v,
			None => {
				if let Some(name) = self.simple_identifier_name() {
					return Ok(Value::String(name.to_string()));
				}
				return Err(anyhow::anyhow!("Idiom evaluation requires current value"));
			}
		};

		// Start with the current value and apply each part in sequence
		let mut value = current.clone();

		for (i, part) in self.parts.iter().enumerate() {
			value = evaluate_part(&value, part, ctx.clone()).await?;

			// After a Lookup, flatten if the NEXT part is also a Lookup or Where
			// This matches legacy SurrealDB semantics
			if matches!(part, PhysicalPart::Lookup(_)) {
				if let Some(next_part) = self.parts.get(i + 1) {
					if matches!(next_part, PhysicalPart::Lookup(_) | PhysicalPart::Where(_)) {
						value = value.flatten();
					}
				}
			}

			// Short-circuit on None for optional chaining
			if matches!(value, Value::None) {
				// Check if the next part would handle None specially
				// For now, we continue evaluation
			}
		}

		Ok(value)
	}

	fn references_current_value(&self) -> bool {
		// Simple identifiers (single Field part) can be used without current_value
		// as they will be treated as string literals in that case
		!self.is_simple_identifier()
	}

	fn access_mode(&self) -> AccessMode {
		self.parts.iter().map(|p| p.access_mode()).combine_all()
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
) -> anyhow::Result<Value> {
	match part {
		PhysicalPart::Field(name) => evaluate_field(value, name, ctx.clone()).await,

		PhysicalPart::Index(expr) => {
			let index = expr.evaluate(ctx).await?;
			evaluate_index(value, &index)
		}

		PhysicalPart::All => evaluate_all(value, ctx).await,

		PhysicalPart::Flatten => evaluate_flatten(value),

		PhysicalPart::First => evaluate_first(value),

		PhysicalPart::Last => evaluate_last(value),

		PhysicalPart::Where(predicate) => evaluate_where(value, predicate.as_ref(), ctx).await,

		PhysicalPart::Method {
			name,
			args,
		} => evaluate_method(value, name, args, ctx).await,

		PhysicalPart::Destructure(parts) => evaluate_destructure(value, parts, ctx).await,

		PhysicalPart::Optional => {
			// Optional just returns the value as-is; None handling is done at the IdiomExpr level
			Ok(value.clone())
		}

		PhysicalPart::Lookup(lookup) => evaluate_lookup(value, lookup, ctx).await,

		PhysicalPart::Recurse(recurse) => evaluate_recurse(value, recurse, ctx).await,
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
			// Fetch the record from the database
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

			// Access field on fetched record
			let fetched = record.data.as_ref();
			if fetched.is_none() {
				return Ok(Value::None);
			}

			// The record data is a Value, get the field from it
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

/// Index access on arrays, sets, and objects.
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
		_ => Ok(Value::None),
	}
}

/// All elements - `[*]` or `.*`.
///
/// When applied to a RecordId (e.g., `record.*`), fetches the record and returns it as an object.
/// When applied to an array of RecordIds (e.g., from `->edge->target.*`), fetches each record.
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
			// Fetch the record and return the full object
			// This handles `record.*` syntax
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

			Ok(record.data.as_ref().clone())
		}
		// For other types, return as single-element array
		other => Ok(Value::Array(vec![other.clone()].into())),
	}
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
) -> anyhow::Result<Value> {
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
/// The method name is mapped to a function name based on the value type.
async fn evaluate_method(
	value: &Value,
	name: &str,
	args: &[Arc<dyn PhysicalExpr>],
	ctx: EvalContext<'_>,
) -> anyhow::Result<Value> {
	// Determine the type-specific function namespace based on the value type
	let namespace = match value {
		Value::String(_) => "string",
		Value::Array(_) | Value::Set(_) => "array",
		Value::Object(_) => "object",
		Value::Bytes(_) => "bytes",
		Value::Duration(_) => "duration",
		Value::Datetime(_) => "time",
		Value::Number(_) => "math",
		Value::Geometry(_) => "geo",
		Value::RecordId(_) => "record",
		Value::File(_) => "file",
		_ => {
			// Try common namespaces for generic methods
			return Err(anyhow::anyhow!(
				"Method '{}' cannot be called on value of type '{}'",
				name,
				value.kind_of()
			));
		}
	};

	// Build the full function name
	let func_name = format!("{}::{}", namespace, name);

	// Build the arguments: receiver value first, then method arguments
	let mut func_args = Vec::with_capacity(1 + args.len());
	func_args.push(value.clone());
	for arg_expr in args {
		let arg_value = arg_expr.evaluate(ctx.clone()).await?;
		func_args.push(arg_value);
	}

	// Get the function registry and invoke the function
	let registry = crate::exec::function::FunctionRegistry::with_builtins();

	if let Some(func) = registry.get(&func_name) {
		// Try sync invocation first for pure functions
		if func.is_pure() {
			func.invoke(func_args)
		} else {
			// Use async invocation for context-aware functions
			func.invoke_async(&ctx, func_args).await
		}
	} else {
		// Try without namespace (some methods might be value-generic)
		Err(anyhow::anyhow!(
			"Unknown method '{}' on type '{}' (tried function '{}')",
			name,
			value.kind_of(),
			func_name
		))
	}
}

/// Destructure evaluation - extract fields into a new object.
async fn evaluate_destructure(
	value: &Value,
	parts: &[PhysicalDestructurePart],
	ctx: EvalContext<'_>,
) -> anyhow::Result<Value> {
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
						// Start with the field value and apply the path
						let mut v = obj.get(field.as_str()).cloned().unwrap_or(Value::None);
						for p in path {
							v = Box::pin(evaluate_part(&v, p, ctx.clone())).await?;
						}
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

/// Perform graph edge scan for a specific RecordId.
async fn evaluate_lookup_for_rid(
	rid: &RecordId,
	lookup: &PhysicalLookup,
	ctx: EvalContext<'_>,
) -> anyhow::Result<Value> {
	// Get database context
	let db_ctx = ctx.exec_ctx.database().map_err(|e| anyhow::anyhow!("{}", e))?;
	let txn = ctx.exec_ctx.txn();
	let ns = &db_ctx.ns_ctx.ns;
	let db = &db_ctx.db;

	// Determine directions to scan
	let directions: Vec<Dir> = match lookup.direction {
		LookupDirection::Out => vec![Dir::Out],
		LookupDirection::In => vec![Dir::In],
		LookupDirection::Both => vec![Dir::Out, Dir::In],
		LookupDirection::Reference => {
			// Reference lookups use a different scan pattern
			return Err(anyhow::anyhow!(
				"Reference lookups (<~) not yet implemented in streaming engine"
			));
		}
	};

	let mut results = Vec::new();

	// Scan edges for each direction
	for dir in &directions {
		if lookup.edge_tables.is_empty() {
			// Scan all edges in this direction
			let beg = crate::key::graph::egprefix(
				ns.namespace_id,
				db.database_id,
				&rid.table,
				&rid.key,
				dir,
			)
			.map_err(|e| anyhow::anyhow!("Failed to create prefix: {}", e))?;

			let end = crate::key::graph::egsuffix(
				ns.namespace_id,
				db.database_id,
				&rid.table,
				&rid.key,
				dir,
			)
			.map_err(|e| anyhow::anyhow!("Failed to create suffix: {}", e))?;

			let kv_stream = txn.stream_keys(beg..end, None, None, ScanDirection::Forward);
			futures::pin_mut!(kv_stream);

			while let Some(result) = kv_stream.next().await {
				let key =
					result.map_err(|e| anyhow::anyhow!("Failed to scan graph edge: {}", e))?;

				// Decode the graph key to get the target RecordId
				let decoded = crate::key::graph::Graph::decode_key(&key)
					.map_err(|e| anyhow::anyhow!("Failed to decode graph key: {}", e))?;

				let target_rid = RecordId {
					table: decoded.ft.into_owned(),
					key: decoded.fk.into_owned(),
				};
				results.push(Value::RecordId(target_rid));
			}
		} else {
			// Scan specific edge tables
			for edge_table in &lookup.edge_tables {
				let beg = crate::key::graph::ftprefix(
					ns.namespace_id,
					db.database_id,
					&rid.table,
					&rid.key,
					dir,
					edge_table,
				)
				.map_err(|e| anyhow::anyhow!("Failed to create prefix: {}", e))?;

				let end = crate::key::graph::ftsuffix(
					ns.namespace_id,
					db.database_id,
					&rid.table,
					&rid.key,
					dir,
					edge_table,
				)
				.map_err(|e| anyhow::anyhow!("Failed to create suffix: {}", e))?;

				let kv_stream = txn.stream_keys(beg..end, None, None, ScanDirection::Forward);
				futures::pin_mut!(kv_stream);

				while let Some(result) = kv_stream.next().await {
					let key =
						result.map_err(|e| anyhow::anyhow!("Failed to scan graph edge: {}", e))?;

					// Decode the graph key to get the target RecordId
					let decoded = crate::key::graph::Graph::decode_key(&key)
						.map_err(|e| anyhow::anyhow!("Failed to decode graph key: {}", e))?;

					let target_rid = RecordId {
						table: decoded.ft.into_owned(),
						key: decoded.fk.into_owned(),
					};
					results.push(Value::RecordId(target_rid));
				}
			}
		}
	}

	Ok(Value::Array(results.into()))
}

/// Recurse evaluation - bounded/unbounded recursion.
async fn evaluate_recurse(
	value: &Value,
	recurse: &crate::exec::physical_part::PhysicalRecurse,
	ctx: EvalContext<'_>,
) -> anyhow::Result<Value> {
	// TODO: Implement recursion
	// This requires iterating until bounds are reached or termination condition
	let _ = (value, recurse, ctx);
	Err(anyhow::anyhow!(
		"Recursion evaluation not yet implemented - requires iterative execution with bounds"
	))
}
