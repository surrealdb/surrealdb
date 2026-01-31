use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::catalog::providers::TableProvider;
use crate::exec::physical_part::{
	LookupDirection, PhysicalDestructurePart, PhysicalLookup, PhysicalPart,
	PhysicalRecurseInstruction,
};
use crate::exec::{AccessMode, CombineAccessModes, ExecutionContext, OperatorPlan};
use crate::expr::{Dir, Idiom};
use crate::idx::planner::ScanDirection;
use crate::val::{RecordId, Value};

/// Evaluation context - what's available during expression evaluation.
///
/// This is a borrowed view into the execution context for expression evaluation.
/// It provides access to parameters, namespace/database names, and the current row
/// (for per-row expressions like filters and projections).
#[derive(Clone)]
pub struct EvalContext<'a> {
	pub exec_ctx: &'a ExecutionContext,

	/// Current row for per-row expressions (projections, filters).
	/// None when evaluating in "scalar context" (USE, LIMIT, TIMEOUT, etc.)
	pub current_value: Option<&'a Value>,
}

impl<'a> EvalContext<'a> {
	/// Convert from ExecutionContext enum for expression evaluation.
	///
	/// This extracts the appropriate fields based on the context level:
	/// - Root: params only, no ns/db/txn
	/// - Namespace: params, ns, txn
	/// - Database: params, ns, db, txn
	// pub(crate) fn from_exec_ctx(exec_ctx: &'a ExecutionContext) -> Self {
	// 	match exec_ctx {
	// 		ExecutionContext::Root(r) => Self::scalar(&r.params, None, None, None),
	// 		ExecutionContext::Namespace(n) => {
	// 			Self::scalar(&n.root.params, Some(&n.ns), None, Some(&n.txn))
	// 		}
	// 		ExecutionContext::Database(d) => Self::scalar(
	// 			&d.ns_ctx.root.params,
	// 			Some(&d.ns_ctx.ns),
	// 			Some(&d.db),
	// 			Some(&d.ns_ctx.txn),
	// 		),
	// 	}
	// }

	/// For session-level scalar evaluation (USE, LIMIT, etc.)
	pub(crate) fn from_exec_ctx(exec_ctx: &'a ExecutionContext) -> Self {
		Self {
			exec_ctx,
			current_value: None,
		}
	}

	/// For per-row evaluation (projections, filters)
	pub fn with_value(&self, value: &'a Value) -> Self {
		Self {
			current_value: Some(value),
			..*self
		}
	}
}

#[async_trait]
pub trait PhysicalExpr: ToSql + Send + Sync + Debug {
	fn name(&self) -> &'static str;

	/// Evaluate this expression to a value.
	///
	/// May execute subqueries internally, hence async.
	async fn evaluate(&self, ctx: EvalContext<'_>) -> anyhow::Result<Value>;

	/// Does this expression reference the current row?
	/// If false, can be evaluated in scalar context.
	fn references_current_value(&self) -> bool;

	/// Returns the access mode for this expression.
	///
	/// This is critical for plan-based mutability analysis:
	/// - If an expression contains a mutation subquery, it must return `ReadWrite`
	/// - Example: `(UPSERT person)` in a SELECT must propagate `ReadWrite` upward
	fn access_mode(&self) -> AccessMode;
}

/// Literal value - "foo", 42, true
#[derive(Debug, Clone)]
pub struct Literal(pub(crate) Value);

#[async_trait]
impl PhysicalExpr for Literal {
	fn name(&self) -> &'static str {
		"Literal"
	}

	async fn evaluate(&self, _ctx: EvalContext<'_>) -> anyhow::Result<Value> {
		Ok(self.0.clone())
	}

	fn references_current_value(&self) -> bool {
		false
	}

	fn access_mode(&self) -> AccessMode {
		// Literals are always read-only
		AccessMode::ReadOnly
	}
}

impl ToSql for Literal {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.0.fmt_sql(f, fmt);
	}
}

/// Parameter reference - $foo
#[derive(Debug, Clone)]
pub struct Param(pub(crate) String);

#[async_trait]
impl PhysicalExpr for Param {
	fn name(&self) -> &'static str {
		"Param"
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> anyhow::Result<Value> {
		ctx.exec_ctx
			.params()
			.get(self.0.as_str())
			.map(|v| (**v).clone())
			.ok_or_else(|| anyhow::anyhow!("Parameter not found: ${}", self.0))
	}

	fn references_current_value(&self) -> bool {
		false
	}

	fn access_mode(&self) -> AccessMode {
		// Parameter references are read-only
		AccessMode::ReadOnly
	}
}

impl ToSql for Param {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "${}", self.0)
	}
}

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
}

#[async_trait]
impl PhysicalExpr for IdiomExpr {
	fn name(&self) -> &'static str {
		"IdiomExpr"
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> anyhow::Result<Value> {
		let current = ctx
			.current_value
			.ok_or_else(|| anyhow::anyhow!("Idiom evaluation requires current value"))?;

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
		true
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

		PhysicalPart::All => evaluate_all(value),

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
async fn evaluate_field(value: &Value, name: &str, ctx: EvalContext<'_>) -> anyhow::Result<Value> {
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

/// Index access on arrays and objects.
fn evaluate_index(value: &Value, index: &Value) -> anyhow::Result<Value> {
	match (value, index) {
		// Array with numeric index
		(Value::Array(arr), Value::Number(n)) => {
			let idx = n.to_usize();
			Ok(arr.get(idx).cloned().unwrap_or(Value::None))
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

/// All elements - `[*]`.
fn evaluate_all(value: &Value) -> anyhow::Result<Value> {
	match value {
		Value::Array(arr) => Ok(Value::Array(arr.clone())),
		Value::Object(obj) => {
			// Return all values from the object as an array
			Ok(Value::Array(obj.values().cloned().collect::<Vec<_>>().into()))
		}
		// For other types, return as single-element array
		other => Ok(Value::Array(vec![other.clone()].into())),
	}
}

/// Flatten nested arrays.
fn evaluate_flatten(value: &Value) -> anyhow::Result<Value> {
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
fn evaluate_first(value: &Value) -> anyhow::Result<Value> {
	match value {
		Value::Array(arr) => Ok(arr.first().cloned().unwrap_or(Value::None)),
		other => Ok(other.clone()),
	}
}

/// Last element.
fn evaluate_last(value: &Value) -> anyhow::Result<Value> {
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
async fn evaluate_method(
	_value: &Value,
	name: &str,
	_args: &[Arc<dyn PhysicalExpr>],
	_ctx: EvalContext<'_>,
) -> anyhow::Result<Value> {
	// TODO: Implement method calls
	// This requires access to the function registry and proper execution context
	// For now, return an error
	Err(anyhow::anyhow!(
		"Method call '{}' not yet supported in physical expressions - requires function registry",
		name
	))
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
			// Apply lookup to each element in the array
			// Do NOT flatten - each element's lookup result is kept as-is
			// This matches SurrealDB semantics where chained lookups produce nested arrays
			let mut results = Vec::with_capacity(arr.len());
			for item in arr.iter() {
				let result = Box::pin(evaluate_lookup(item, lookup, ctx.clone())).await?;
				results.push(result);
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

/// Binary operation - left op right (e.g., age > 10)
#[derive(Debug, Clone)]
pub struct BinaryOp {
	pub(crate) left: Arc<dyn PhysicalExpr>,
	pub(crate) op: crate::expr::operator::BinaryOperator,
	pub(crate) right: Arc<dyn PhysicalExpr>,
}

#[async_trait]
impl PhysicalExpr for BinaryOp {
	fn name(&self) -> &'static str {
		"BinaryOp"
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> anyhow::Result<Value> {
		use crate::expr::operator::BinaryOperator;
		use crate::fnc::operate;

		// Evaluate both sides (could parallelize if both are independent)
		let left = self.left.evaluate(ctx.clone()).await?;

		macro_rules! eval {
			($expr:expr) => {
				$expr.evaluate(ctx).await?
			};
		}

		// Apply the operator
		match &self.op {
			BinaryOperator::Add => operate::add(left, eval!(self.right)),
			BinaryOperator::Subtract => operate::sub(left, eval!(self.right)),
			BinaryOperator::Multiply => operate::mul(left, eval!(self.right)),
			BinaryOperator::Divide => operate::div(left, eval!(self.right)),
			BinaryOperator::Remainder => operate::rem(left, eval!(self.right)),
			BinaryOperator::Power => operate::pow(left, eval!(self.right)),

			BinaryOperator::Equal => operate::equal(&left, &eval!(self.right)),
			BinaryOperator::ExactEqual => operate::exact(&left, &eval!(self.right)),
			BinaryOperator::NotEqual => operate::not_equal(&left, &eval!(self.right)),
			BinaryOperator::AllEqual => operate::all_equal(&left, &eval!(self.right)),
			BinaryOperator::AnyEqual => operate::any_equal(&left, &eval!(self.right)),

			BinaryOperator::LessThan => operate::less_than(&left, &eval!(self.right)),
			BinaryOperator::LessThanEqual => operate::less_than_or_equal(&left, &eval!(self.right)),
			BinaryOperator::MoreThan => operate::more_than(&left, &eval!(self.right)),
			BinaryOperator::MoreThanEqual => operate::more_than_or_equal(&left, &eval!(self.right)),

			BinaryOperator::And => {
				// Short-circuit AND
				if !left.is_truthy() {
					Ok(left)
				} else {
					Ok(eval!(self.right))
				}
			}
			BinaryOperator::Or => {
				// Short-circuit OR
				if left.is_truthy() {
					Ok(left)
				} else {
					Ok(eval!(self.right))
				}
			}

			BinaryOperator::Contain => operate::contain(&left, &eval!(self.right)),
			BinaryOperator::NotContain => operate::not_contain(&left, &eval!(self.right)),
			BinaryOperator::ContainAll => operate::contain_all(&left, &eval!(self.right)),
			BinaryOperator::ContainAny => operate::contain_any(&left, &eval!(self.right)),
			BinaryOperator::ContainNone => operate::contain_none(&left, &eval!(self.right)),
			BinaryOperator::Inside => operate::inside(&left, &eval!(self.right)),
			BinaryOperator::NotInside => operate::not_inside(&left, &eval!(self.right)),
			BinaryOperator::AllInside => operate::inside_all(&left, &eval!(self.right)),
			BinaryOperator::AnyInside => operate::inside_any(&left, &eval!(self.right)),
			BinaryOperator::NoneInside => operate::inside_none(&left, &eval!(self.right)),

			BinaryOperator::Outside => operate::outside(&left, &eval!(self.right)),
			BinaryOperator::Intersects => operate::intersects(&left, &eval!(self.right)),

			BinaryOperator::NullCoalescing => {
				if !left.is_nullish() {
					Ok(left)
				} else {
					Ok(eval!(self.right))
				}
			}
			BinaryOperator::TenaryCondition => {
				// Same as OR for this context
				if left.is_truthy() {
					Ok(left)
				} else {
					Ok(eval!(self.right))
				}
			}

			// Range operators - create Range values
			BinaryOperator::Range => {
				// a..b means start: Included(a), end: Excluded(b)
				Ok(Value::Range(Box::new(crate::val::Range {
					start: std::ops::Bound::Included(left),
					end: std::ops::Bound::Excluded(eval!(self.right)),
				})))
			}
			BinaryOperator::RangeInclusive => {
				// a..=b means start: Included(a), end: Included(b)
				Ok(Value::Range(Box::new(crate::val::Range {
					start: std::ops::Bound::Included(left),
					end: std::ops::Bound::Included(eval!(self.right)),
				})))
			}
			BinaryOperator::RangeSkip => {
				// a>..b means start: Excluded(a), end: Excluded(b)
				Ok(Value::Range(Box::new(crate::val::Range {
					start: std::ops::Bound::Excluded(left),
					end: std::ops::Bound::Excluded(eval!(self.right)),
				})))
			}
			BinaryOperator::RangeSkipInclusive => {
				// a>..=b means start: Excluded(a), end: Included(b)
				Ok(Value::Range(Box::new(crate::val::Range {
					start: std::ops::Bound::Excluded(left),
					end: std::ops::Bound::Included(eval!(self.right)),
				})))
			}

			// Match operators require full-text search context
			BinaryOperator::Matches(_) => {
				Err(anyhow::anyhow!("MATCHES operator not yet supported in physical expressions"))
			}

			// Nearest neighbor requires vector index context
			BinaryOperator::NearestNeighbor(_) => {
				Err(anyhow::anyhow!("KNN operator not yet supported in physical expressions"))
			}
		}
	}

	fn references_current_value(&self) -> bool {
		self.left.references_current_value() || self.right.references_current_value()
	}

	fn access_mode(&self) -> AccessMode {
		// Combine both sides' access modes
		self.left.access_mode().combine(self.right.access_mode())
	}
}

impl ToSql for BinaryOp {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "{} {} {}", self.left, self.op, self.right)
	}
}

/// Unary/Prefix operation - op expr (e.g., -5, !true, +x)
#[derive(Debug, Clone)]
pub struct UnaryOp {
	pub(crate) op: crate::expr::operator::PrefixOperator,
	pub(crate) expr: Arc<dyn PhysicalExpr>,
}

#[async_trait]
impl PhysicalExpr for UnaryOp {
	fn name(&self) -> &'static str {
		"UnaryOp"
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> anyhow::Result<Value> {
		use crate::expr::operator::PrefixOperator;
		use crate::fnc::operate;

		let value = self.expr.evaluate(ctx).await?;

		match &self.op {
			PrefixOperator::Not => operate::not(value),
			PrefixOperator::Negate => operate::neg(value),
			PrefixOperator::Positive => {
				// Positive is essentially a no-op for numbers
				Ok(value)
			}
			PrefixOperator::Range => {
				// ..value creates range with unbounded start, excluded end
				Ok(Value::Range(Box::new(crate::val::Range {
					start: std::ops::Bound::Unbounded,
					end: std::ops::Bound::Excluded(value),
				})))
			}
			PrefixOperator::RangeInclusive => {
				// ..=value creates range with unbounded start, included end
				Ok(Value::Range(Box::new(crate::val::Range {
					start: std::ops::Bound::Unbounded,
					end: std::ops::Bound::Included(value),
				})))
			}
			PrefixOperator::Cast(kind) => {
				// Type casting
				value.cast_to_kind(kind).map_err(|e| anyhow::anyhow!("{}", e))
			}
		}
	}

	fn references_current_value(&self) -> bool {
		self.expr.references_current_value()
	}

	fn access_mode(&self) -> AccessMode {
		// Propagate inner expression's access mode
		self.expr.access_mode()
	}
}

impl ToSql for UnaryOp {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "{} {}", self.op, self.expr)
	}
}

/// Postfix operation - expr op (e.g., value.., value>..)
#[derive(Debug, Clone)]
pub struct PostfixOp {
	pub(crate) op: crate::expr::operator::PostfixOperator,
	pub(crate) expr: Arc<dyn PhysicalExpr>,
}

#[async_trait]
impl PhysicalExpr for PostfixOp {
	fn name(&self) -> &'static str {
		"PostfixOp"
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> anyhow::Result<Value> {
		use crate::expr::operator::PostfixOperator;

		let value = self.expr.evaluate(ctx).await?;

		match &self.op {
			PostfixOperator::Range => {
				// value.. creates range with included start, unbounded end
				Ok(Value::Range(Box::new(crate::val::Range {
					start: std::ops::Bound::Included(value),
					end: std::ops::Bound::Unbounded,
				})))
			}
			PostfixOperator::RangeSkip => {
				// value>.. creates range with excluded start, unbounded end
				Ok(Value::Range(Box::new(crate::val::Range {
					start: std::ops::Bound::Excluded(value),
					end: std::ops::Bound::Unbounded,
				})))
			}
			PostfixOperator::MethodCall(..) => {
				Err(anyhow::anyhow!("Method calls not yet supported in physical expressions"))
			}
			PostfixOperator::Call(..) => {
				Err(anyhow::anyhow!("Function calls not yet supported in physical expressions"))
			}
		}
	}

	fn references_current_value(&self) -> bool {
		self.expr.references_current_value()
	}

	fn access_mode(&self) -> AccessMode {
		// Propagate inner expression's access mode
		self.expr.access_mode()
	}
}

impl ToSql for PostfixOp {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "{} {}", self.expr, self.op)
	}
}

/// Array literal - [1, 2, 3] or [expr1, expr2, ...]
#[derive(Debug, Clone)]
pub struct ArrayLiteral {
	pub(crate) elements: Vec<Arc<dyn PhysicalExpr>>,
}

#[async_trait]
impl PhysicalExpr for ArrayLiteral {
	fn name(&self) -> &'static str {
		"ArrayLiteral"
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> anyhow::Result<Value> {
		let mut values = Vec::with_capacity(self.elements.len());
		for elem in &self.elements {
			let value = elem.evaluate(ctx.clone()).await?;
			values.push(value);
		}
		Ok(Value::Array(crate::val::Array::from(values)))
	}

	fn references_current_value(&self) -> bool {
		self.elements.iter().any(|e| e.references_current_value())
	}

	fn access_mode(&self) -> AccessMode {
		self.elements.iter().map(|e| e.access_mode()).combine_all()
	}
}

impl ToSql for ArrayLiteral {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push('[');
		for (i, elem) in self.elements.iter().enumerate() {
			if i > 0 {
				f.push_str(", ");
			}
			elem.fmt_sql(f, fmt);
		}
		f.push(']');
	}
}

/// Scalar subquery - (SELECT ... LIMIT 1)
#[derive(Debug, Clone)]
pub struct ScalarSubquery {
	pub(crate) plan: Arc<dyn OperatorPlan>,
}

#[async_trait]
impl PhysicalExpr for ScalarSubquery {
	fn name(&self) -> &'static str {
		"ScalarSubquery"
	}

	async fn evaluate(&self, _ctx: EvalContext<'_>) -> anyhow::Result<Value> {
		// TODO: Implement scalar subquery evaluation
		// This requires bridging EvalContext (which has borrowed &Transaction)
		// with ExecutionContext (which needs Arc<Transaction>).
		// Options:
		// 1. Store Arc<Transaction> in EvalContext
		// 2. Add a method to create ExecutionContext from borrowed context
		// 3. Make ExecutionContext work with borrowed Transaction (but this conflicts with 'static
		//    stream requirement)

		Err(anyhow::anyhow!(
			"ScalarSubquery evaluation not yet fully implemented - need Arc<Transaction> in EvalContext"
		))
	}

	fn references_current_value(&self) -> bool {
		// For now, assume subqueries don't reference current value
		// TODO: Track if plan references outer scope for correlated subqueries
		false
	}

	fn access_mode(&self) -> AccessMode {
		// CRITICAL: Propagate the subquery's access mode!
		// This is why `SELECT *, (UPSERT person) FROM person` is ReadWrite
		self.plan.access_mode()
	}
}

impl ToSql for ScalarSubquery {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "TODO: Not implemented")
	}
}

/// Object literal - { key1: expr1, key2: expr2, ... }
#[derive(Debug, Clone)]
pub struct ObjectLiteral {
	pub(crate) entries: Vec<(String, Arc<dyn PhysicalExpr>)>,
}

#[async_trait]
impl PhysicalExpr for ObjectLiteral {
	fn name(&self) -> &'static str {
		"ObjectLiteral"
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> anyhow::Result<Value> {
		let mut map = std::collections::BTreeMap::new();
		for (key, expr) in &self.entries {
			let value = expr.evaluate(ctx.clone()).await?;
			map.insert(key.clone(), value);
		}
		Ok(Value::Object(crate::val::Object(map)))
	}

	fn references_current_value(&self) -> bool {
		self.entries.iter().any(|(_, e)| e.references_current_value())
	}

	fn access_mode(&self) -> AccessMode {
		self.entries.iter().map(|(_, e)| e.access_mode()).combine_all()
	}
}

impl ToSql for ObjectLiteral {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push('{');
		for (i, (key, expr)) in self.entries.iter().enumerate() {
			if i > 0 {
				f.push_str(", ");
			}
			write_sql!(f, fmt, "{}: {}", key, expr);
		}
		f.push('}');
	}
}

/// Set literal - <{expr1, expr2, ...}>
#[derive(Debug, Clone)]
pub struct SetLiteral {
	pub(crate) elements: Vec<Arc<dyn PhysicalExpr>>,
}

#[async_trait]
impl PhysicalExpr for SetLiteral {
	fn name(&self) -> &'static str {
		"SetLiteral"
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> anyhow::Result<Value> {
		let mut set = crate::val::Set::new();
		for elem in &self.elements {
			let value = elem.evaluate(ctx.clone()).await?;
			set.insert(value);
		}
		Ok(Value::Set(set))
	}

	fn references_current_value(&self) -> bool {
		self.elements.iter().any(|e| e.references_current_value())
	}

	fn access_mode(&self) -> AccessMode {
		self.elements.iter().map(|e| e.access_mode()).combine_all()
	}
}

impl ToSql for SetLiteral {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push_str("<{");
		for (i, elem) in self.elements.iter().enumerate() {
			if i > 0 {
				f.push_str(", ");
			}
			elem.fmt_sql(f, fmt);
		}
		f.push_str("}>");
	}
}

/// Function call - count(), string::concat(a, b), etc.
#[derive(Debug, Clone)]
pub struct FunctionCallExpr {
	pub(crate) function: crate::expr::Function,
	pub(crate) arguments: Vec<Arc<dyn PhysicalExpr>>,
}

#[async_trait]
impl PhysicalExpr for FunctionCallExpr {
	fn name(&self) -> &'static str {
		"FunctionCall"
	}

	async fn evaluate(&self, _ctx: EvalContext<'_>) -> anyhow::Result<Value> {
		// TODO: Function calls need full execution context with Stk, Options, and CursorDoc
		// These are not yet available in EvalContext for physical expressions
		// This will need to be implemented when the execution context is extended
		Err(anyhow::anyhow!(
			"Function call evaluation not yet supported in physical expressions - need Stk and Options in EvalContext"
		))
	}

	fn references_current_value(&self) -> bool {
		// Check if any argument references the current value
		self.arguments.iter().any(|e| e.references_current_value())
	}

	fn access_mode(&self) -> AccessMode {
		// Function calls may be read-write depending on the function
		// For now, check if the function itself is read-only
		let func_mode = if self.function.read_only() {
			AccessMode::ReadOnly
		} else {
			AccessMode::ReadWrite
		};

		// Combine with argument access modes
		let args_mode = self.arguments.iter().map(|e| e.access_mode()).combine_all();
		func_mode.combine(args_mode)
	}
}

impl ToSql for FunctionCallExpr {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		// Convert to FunctionCall for formatting
		// Note: We can't easily convert physical exprs back to logical exprs,
		// so we just show the function name without arguments
		f.push_str(&self.function.to_idiom().to_sql());
		f.push_str("(...)");
	}
}

/// Closure expression - |$x| $x * 2
#[derive(Debug, Clone)]
pub struct ClosurePhysicalExpr {
	pub(crate) closure: crate::expr::ClosureExpr,
}

#[async_trait]
impl PhysicalExpr for ClosurePhysicalExpr {
	fn name(&self) -> &'static str {
		"Closure"
	}

	async fn evaluate(&self, _ctx: EvalContext<'_>) -> anyhow::Result<Value> {
		// Closures evaluate to a Value::Closure
		// This is similar to how the old executor handles it
		// TODO: Need to capture parameters from context
		Err(anyhow::anyhow!(
			"Closure evaluation not yet fully implemented - need parameter capture from context"
		))
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

impl ToSql for ClosurePhysicalExpr {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.closure.fmt_sql(f, fmt);
	}
}

/// IF/THEN/ELSE expression - IF condition THEN value ELSE other END
#[derive(Debug, Clone)]
pub struct IfElseExpr {
	/// List of (condition, value) pairs for IF and ELSE IF branches
	pub(crate) branches: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)>,
	/// Optional ELSE branch (final fallback)
	pub(crate) otherwise: Option<Arc<dyn PhysicalExpr>>,
}

#[async_trait]
impl PhysicalExpr for IfElseExpr {
	fn name(&self) -> &'static str {
		"IfElse"
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> anyhow::Result<Value> {
		// Evaluate each condition in order
		for (condition, value) in &self.branches {
			let cond_result = condition.evaluate(ctx.clone()).await?;
			// Check if condition is truthy
			if cond_result.is_truthy() {
				return value.evaluate(ctx).await;
			}
		}

		// No condition was true, evaluate the else branch if present
		if let Some(otherwise) = &self.otherwise {
			otherwise.evaluate(ctx).await
		} else {
			// No else branch, return NONE
			Ok(Value::None)
		}
	}

	fn references_current_value(&self) -> bool {
		// Check if any branch references current value
		self.branches
			.iter()
			.any(|(cond, val)| cond.references_current_value() || val.references_current_value())
			|| self.otherwise.as_ref().map_or(false, |e| e.references_current_value())
	}

	fn access_mode(&self) -> AccessMode {
		// Combine all branches' access modes
		let branches_mode = self
			.branches
			.iter()
			.flat_map(|(cond, val)| [cond.access_mode(), val.access_mode()])
			.combine_all();

		let otherwise_mode =
			self.otherwise.as_ref().map_or(AccessMode::ReadOnly, |e| e.access_mode());

		branches_mode.combine(otherwise_mode)
	}
}

impl ToSql for IfElseExpr {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		for (i, (condition, value)) in self.branches.iter().enumerate() {
			if i == 0 {
				write_sql!(f, fmt, "IF {} THEN {}", condition, value);
			} else {
				write_sql!(f, fmt, " ELSE IF {} THEN {}", condition, value);
			}
		}
		if let Some(otherwise) = &self.otherwise {
			write_sql!(f, fmt, " ELSE {}", otherwise);
		}
		f.push_str(" END");
	}
}

// ============================================================================
// LookupExpr - Graph/Reference lookup as correlated subquery
// ============================================================================

/// Lookup expression that evaluates a graph or reference traversal.
///
/// This expression wraps a pre-planned lookup operation (GraphEdgeScan or ReferenceScan
/// with optional Filter, Sort, Limit, Project) and executes it as a correlated subquery,
/// binding the source from the current evaluation context.
///
/// Example: For `person:alice->knows->person`, this would:
/// 1. Extract `person:alice` from the current value (or use it directly if literal)
/// 2. Execute the GraphEdgeScan plan to find connected records
/// 3. Return the results as an array
#[derive(Debug, Clone)]
pub struct LookupExpr {
	/// The pre-planned lookup operator tree
	pub(crate) plan: Arc<dyn OperatorPlan>,

	/// Direction of the lookup (for display purposes)
	pub(crate) direction: LookupDirection,

	/// Optional alias for multi-yield expressions
	pub(crate) alias: Option<crate::expr::Idiom>,
}

impl LookupExpr {
	/// Create a new LookupExpr with the given plan and direction.
	pub fn new(plan: Arc<dyn OperatorPlan>, direction: LookupDirection) -> Self {
		Self {
			plan,
			direction,
			alias: None,
		}
	}

	/// Set an alias for multi-yield expressions.
	pub fn with_alias(mut self, alias: crate::expr::Idiom) -> Self {
		self.alias = Some(alias);
		self
	}
}

#[async_trait]
impl PhysicalExpr for LookupExpr {
	fn name(&self) -> &'static str {
		"LookupExpr"
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> anyhow::Result<Value> {
		use futures::StreamExt;

		// Execute the lookup plan
		// The plan should be a GraphEdgeScan or ReferenceScan that has been
		// pre-configured with the source expression
		let stream = self
			.plan
			.execute(ctx.exec_ctx)
			.map_err(|e| anyhow::anyhow!("Failed to execute lookup plan: {}", e))?;

		// Collect all results into an array
		let mut results = Vec::new();
		futures::pin_mut!(stream);

		while let Some(batch_result) = stream.next().await {
			let batch = batch_result.map_err(|e| match e {
				crate::expr::ControlFlow::Err(e) => e,
				crate::expr::ControlFlow::Return(v) => {
					anyhow::anyhow!("Unexpected return in lookup: {:?}", v)
				}
				crate::expr::ControlFlow::Break => {
					anyhow::anyhow!("Unexpected break in lookup")
				}
				crate::expr::ControlFlow::Continue => {
					anyhow::anyhow!("Unexpected continue in lookup")
				}
			})?;
			results.extend(batch.values);
		}

		Ok(Value::Array(results.into()))
	}

	fn references_current_value(&self) -> bool {
		// Lookups typically reference the current value as the source
		// (unless they have a literal source which is pre-bound in the plan)
		true
	}

	fn access_mode(&self) -> AccessMode {
		self.plan.access_mode()
	}
}

impl ToSql for LookupExpr {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		match self.direction {
			LookupDirection::Out => f.push_str("->..."),
			LookupDirection::In => f.push_str("<-..."),
			LookupDirection::Both => f.push_str("<->..."),
			LookupDirection::Reference => f.push_str("<~..."),
		}
	}
}

// ============================================================================
// RecurseExpr - Recursive traversal expression
// ============================================================================

/// Recursion expression that evaluates bounded/unbounded graph traversal.
///
/// This expression handles recursion patterns like `{1..5}->knows->person`
/// with various instructions (collect, path, shortest).
#[derive(Debug, Clone)]
pub struct RecurseExpr {
	/// Minimum recursion depth (default 1)
	pub(crate) min_depth: u32,

	/// Maximum recursion depth (None = unbounded up to system limit)
	pub(crate) max_depth: Option<u32>,

	/// The path expression to evaluate at each recursion step
	pub(crate) path_expr: Arc<dyn PhysicalExpr>,

	/// The recursion instruction
	pub(crate) instruction: PhysicalRecurseInstruction,

	/// Whether to include the starting node in results
	pub(crate) inclusive: bool,
}

#[async_trait]
impl PhysicalExpr for RecurseExpr {
	fn name(&self) -> &'static str {
		"RecurseExpr"
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> anyhow::Result<Value> {
		let current =
			ctx.current_value.ok_or_else(|| anyhow::anyhow!("Recursion requires current value"))?;

		// Implement recursion based on instruction type
		match &self.instruction {
			PhysicalRecurseInstruction::Default => self.evaluate_default(current, ctx).await,
			PhysicalRecurseInstruction::Collect => self.evaluate_collect(current, ctx).await,
			PhysicalRecurseInstruction::Path => self.evaluate_path(current, ctx).await,
			PhysicalRecurseInstruction::Shortest {
				target,
			} => {
				let target_value = target.evaluate(ctx.clone()).await?;
				self.evaluate_shortest(current, &target_value, ctx).await
			}
		}
	}

	fn references_current_value(&self) -> bool {
		true
	}

	fn access_mode(&self) -> AccessMode {
		let path_mode = self.path_expr.access_mode();

		let instruction_mode = match &self.instruction {
			PhysicalRecurseInstruction::Default
			| PhysicalRecurseInstruction::Collect
			| PhysicalRecurseInstruction::Path => AccessMode::ReadOnly,
			PhysicalRecurseInstruction::Shortest {
				target,
			} => target.access_mode(),
		};

		path_mode.combine(instruction_mode)
	}
}

impl RecurseExpr {
	/// Default recursion: keep following the path until bounds or dead end.
	async fn evaluate_default(&self, start: &Value, ctx: EvalContext<'_>) -> anyhow::Result<Value> {
		let max_depth = self.max_depth.unwrap_or(100); // System limit
		let mut current = start.clone();
		let mut depth = 0u32;

		while depth < max_depth {
			// Evaluate the path expression on the current value
			let next_ctx = ctx.with_value(&current);
			let next = self.path_expr.evaluate(next_ctx).await?;

			depth += 1;

			// Check termination
			if matches!(next, Value::None) || next == current {
				break;
			}

			// Check if we've reached minimum depth
			if depth >= self.min_depth {
				current = next;
				break;
			}

			current = next;
		}

		// Return final value if depth is within bounds
		if depth >= self.min_depth {
			Ok(current)
		} else {
			Ok(Value::None)
		}
	}

	/// Collect: gather all unique nodes encountered during traversal.
	async fn evaluate_collect(&self, start: &Value, ctx: EvalContext<'_>) -> anyhow::Result<Value> {
		let max_depth = self.max_depth.unwrap_or(100);
		let mut collected = Vec::new();
		let mut seen = std::collections::HashSet::new();
		let mut frontier = vec![start.clone()];

		if self.inclusive {
			collected.push(start.clone());
			seen.insert(value_hash(start));
		}

		let mut depth = 0u32;

		while depth < max_depth && !frontier.is_empty() {
			let mut next_frontier = Vec::new();

			for value in frontier {
				let value_ctx = ctx.with_value(&value);
				let result = self.path_expr.evaluate(value_ctx).await?;

				// Process result (may be single value or array)
				let values = match result {
					Value::Array(arr) => arr.iter().cloned().collect::<Vec<_>>(),
					Value::None => continue,
					other => vec![other],
				};

				for v in values {
					let hash = value_hash(&v);
					if !seen.contains(&hash) {
						seen.insert(hash);
						if depth + 1 >= self.min_depth {
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

	/// Path: return all paths as arrays of arrays.
	async fn evaluate_path(&self, start: &Value, ctx: EvalContext<'_>) -> anyhow::Result<Value> {
		let max_depth = self.max_depth.unwrap_or(100);
		let mut completed_paths = Vec::new();
		let mut active_paths: Vec<Vec<Value>> = if self.inclusive {
			vec![vec![start.clone()]]
		} else {
			vec![vec![]]
		};

		let mut depth = 0u32;

		while depth < max_depth && !active_paths.is_empty() {
			let mut next_paths = Vec::new();

			for path in active_paths {
				let current = path.last().unwrap_or(start);
				let value_ctx = ctx.with_value(current);
				let result = self.path_expr.evaluate(value_ctx).await?;

				let values = match result {
					Value::Array(arr) => arr.iter().cloned().collect::<Vec<_>>(),
					Value::None => {
						// Dead end - this path is complete
						if depth >= self.min_depth && !path.is_empty() {
							completed_paths.push(Value::Array(path.into()));
						}
						continue;
					}
					other => vec![other],
				};

				if values.is_empty() {
					// Dead end
					if depth >= self.min_depth && !path.is_empty() {
						completed_paths.push(Value::Array(path.into()));
					}
				} else {
					for v in values {
						let mut new_path = path.clone();
						new_path.push(v);
						next_paths.push(new_path);
					}
				}
			}

			active_paths = next_paths;
			depth += 1;
		}

		// Add any remaining active paths that reached max depth
		for path in active_paths {
			if !path.is_empty() {
				completed_paths.push(Value::Array(path.into()));
			}
		}

		Ok(Value::Array(completed_paths.into()))
	}

	/// Shortest: find the shortest path to a target node.
	async fn evaluate_shortest(
		&self,
		start: &Value,
		target: &Value,
		ctx: EvalContext<'_>,
	) -> anyhow::Result<Value> {
		let max_depth = self.max_depth.unwrap_or(100);
		let mut seen = std::collections::HashSet::new();

		// BFS with path tracking
		let initial_path = if self.inclusive {
			vec![start.clone()]
		} else {
			vec![]
		};
		let mut queue = std::collections::VecDeque::new();
		queue.push_back((start.clone(), initial_path));
		seen.insert(value_hash(start));

		let mut depth = 0u32;

		while depth < max_depth && !queue.is_empty() {
			let level_size = queue.len();

			for _ in 0..level_size {
				let (current, path) = queue.pop_front().unwrap();

				let value_ctx = ctx.with_value(&current);
				let result = self.path_expr.evaluate(value_ctx).await?;

				let values = match result {
					Value::Array(arr) => arr.iter().cloned().collect::<Vec<_>>(),
					Value::None => continue,
					other => vec![other],
				};

				for v in values {
					// Check if we found the target
					if &v == target {
						let mut final_path = path.clone();
						final_path.push(v);
						return Ok(Value::Array(final_path.into()));
					}

					let hash = value_hash(&v);
					if !seen.contains(&hash) {
						seen.insert(hash);
						let mut new_path = path.clone();
						new_path.push(v.clone());
						queue.push_back((v, new_path));
					}
				}
			}

			depth += 1;
		}

		// Target not found
		Ok(Value::None)
	}
}

impl ToSql for RecurseExpr {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str(".{");
		if self.min_depth > 1 {
			f.push_str(&self.min_depth.to_string());
		}
		f.push_str("..");
		if let Some(max) = self.max_depth {
			f.push_str(&max.to_string());
		}

		match &self.instruction {
			PhysicalRecurseInstruction::Default => {}
			PhysicalRecurseInstruction::Collect => f.push_str("+collect"),
			PhysicalRecurseInstruction::Path => f.push_str("+path"),
			PhysicalRecurseInstruction::Shortest {
				..
			} => f.push_str("+shortest=..."),
		}

		if self.inclusive {
			f.push_str("+inclusive");
		}

		f.push('}');
	}
}

/// Helper function to create a hash for value deduplication.
fn value_hash(value: &Value) -> u64 {
	use std::hash::{Hash, Hasher};
	let mut hasher = std::collections::hash_map::DefaultHasher::new();
	// Use the display representation as a proxy for equality
	format!("{:?}", value).hash(&mut hasher);
	hasher.finish()
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::val::{Array, Number, Object};

	// =========================================================================
	// Simple Field Access Tests
	// =========================================================================

	#[test]
	fn test_evaluate_field_on_object() {
		let obj = Value::Object(Object::from_iter([
			("name".to_string(), Value::String("Alice".to_string())),
			("age".to_string(), Value::Number(Number::Int(30))),
		]));

		let result = evaluate_field(&obj, "name").unwrap();
		assert_eq!(result, Value::String("Alice".to_string()));

		let result = evaluate_field(&obj, "age").unwrap();
		assert_eq!(result, Value::Number(Number::Int(30)));

		let result = evaluate_field(&obj, "missing").unwrap();
		assert_eq!(result, Value::None);
	}

	#[test]
	fn test_evaluate_field_on_array() {
		let arr = Value::Array(Array::from(vec![
			Value::Object(Object::from_iter([(
				"name".to_string(),
				Value::String("Alice".to_string()),
			)])),
			Value::Object(Object::from_iter([(
				"name".to_string(),
				Value::String("Bob".to_string()),
			)])),
		]));

		let result = evaluate_field(&arr, "name").unwrap();
		assert_eq!(
			result,
			Value::Array(Array::from(vec![
				Value::String("Alice".to_string()),
				Value::String("Bob".to_string()),
			]))
		);
	}

	// =========================================================================
	// Index Access Tests
	// =========================================================================

	#[test]
	fn test_evaluate_index_on_array() {
		let arr = Value::Array(Array::from(vec![
			Value::Number(Number::Int(1)),
			Value::Number(Number::Int(2)),
			Value::Number(Number::Int(3)),
		]));

		let result = evaluate_index(&arr, &Value::Number(Number::Int(0))).unwrap();
		assert_eq!(result, Value::Number(Number::Int(1)));

		let result = evaluate_index(&arr, &Value::Number(Number::Int(2))).unwrap();
		assert_eq!(result, Value::Number(Number::Int(3)));

		let result = evaluate_index(&arr, &Value::Number(Number::Int(5))).unwrap();
		assert_eq!(result, Value::None);
	}

	#[test]
	fn test_evaluate_index_on_object() {
		let obj = Value::Object(Object::from_iter([(
			"key1".to_string(),
			Value::String("value1".to_string()),
		)]));

		let result = evaluate_index(&obj, &Value::String("key1".to_string())).unwrap();
		assert_eq!(result, Value::String("value1".to_string()));
	}

	// =========================================================================
	// Array Operation Tests
	// =========================================================================

	#[test]
	fn test_evaluate_all() {
		let arr = Value::Array(Array::from(vec![
			Value::Number(Number::Int(1)),
			Value::Number(Number::Int(2)),
		]));

		let result = evaluate_all(&arr).unwrap();
		assert_eq!(result, arr);
	}

	#[test]
	fn test_evaluate_flatten() {
		let nested = Value::Array(Array::from(vec![
			Value::Array(Array::from(vec![
				Value::Number(Number::Int(1)),
				Value::Number(Number::Int(2)),
			])),
			Value::Array(Array::from(vec![Value::Number(Number::Int(3))])),
		]));

		let result = evaluate_flatten(&nested).unwrap();
		assert_eq!(
			result,
			Value::Array(Array::from(vec![
				Value::Number(Number::Int(1)),
				Value::Number(Number::Int(2)),
				Value::Number(Number::Int(3)),
			]))
		);
	}

	#[test]
	fn test_evaluate_first_and_last() {
		let arr = Value::Array(Array::from(vec![
			Value::Number(Number::Int(1)),
			Value::Number(Number::Int(2)),
			Value::Number(Number::Int(3)),
		]));

		let first = evaluate_first(&arr).unwrap();
		assert_eq!(first, Value::Number(Number::Int(1)));

		let last = evaluate_last(&arr).unwrap();
		assert_eq!(last, Value::Number(Number::Int(3)));

		// Empty array
		let empty = Value::Array(Array::from(Vec::<Value>::new()));
		assert_eq!(evaluate_first(&empty).unwrap(), Value::None);
		assert_eq!(evaluate_last(&empty).unwrap(), Value::None);
	}

	// =========================================================================
	// PhysicalPart Tests
	// =========================================================================

	#[test]
	fn test_physical_part_is_simple() {
		use crate::exec::physical_part::PhysicalPart;

		assert!(PhysicalPart::Field("test".to_string()).is_simple());
		assert!(PhysicalPart::All.is_simple());
		assert!(PhysicalPart::First.is_simple());
		assert!(PhysicalPart::Last.is_simple());
		assert!(PhysicalPart::Flatten.is_simple());
		assert!(PhysicalPart::Optional.is_simple());
	}

	// =========================================================================
	// IdiomExpr Tests
	// =========================================================================

	#[test]
	fn test_idiom_expr_is_simple() {
		use crate::exec::physical_part::PhysicalPart;
		use crate::expr::Idiom;
		use crate::expr::part::Part;

		let idiom = Idiom(vec![Part::Field("test".to_string())]);
		let parts = vec![PhysicalPart::Field("test".to_string())];
		let expr = IdiomExpr::new(idiom, parts);

		assert!(expr.is_simple());
	}

	// =========================================================================
	// Value Hash Tests
	// =========================================================================

	#[test]
	fn test_value_hash_consistency() {
		let v1 = Value::Number(Number::Int(42));
		let v2 = Value::Number(Number::Int(42));
		let v3 = Value::Number(Number::Int(43));

		assert_eq!(value_hash(&v1), value_hash(&v2));
		assert_ne!(value_hash(&v1), value_hash(&v3));
	}
}
