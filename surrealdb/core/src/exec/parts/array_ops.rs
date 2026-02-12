//! Array operation parts -- `[*]`, `...`, `[$]`, `[~]`.

use async_trait::async_trait;
use surrealdb_types::{SqlFormat, ToSql};

use super::fetch_record_with_computed_fields;
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{AccessMode, ContextLevel};
use crate::expr::FlowResult;
use crate::val::Value;

/// Threshold below which we evaluate sequentially (no parallelism overhead).
const PARALLEL_BATCH_THRESHOLD: usize = 2;

// ============================================================================
// AllPart -- [*] or .*
// ============================================================================

/// All elements - `[*]` or `.*`.
///
/// When applied to a RecordId (e.g., `record.*`), fetches the record and returns it as an object.
/// When applied to an array of RecordIds (e.g., from `->edge->target.*`), fetches each record.
#[derive(Debug, Clone)]
pub struct AllPart;

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl PhysicalExpr for AllPart {
	fn name(&self) -> &'static str {
		"All"
	}

	fn required_context(&self) -> ContextLevel {
		// All (.*) may trigger record fetch + computed field evaluation
		ContextLevel::Database
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> FlowResult<Value> {
		let value = ctx.current_value.cloned().unwrap_or(Value::None);
		Ok(evaluate_all(&value, ctx).await?)
	}

	/// Parallel batch evaluation for `[*]` / `.*`.
	///
	/// When applied to arrays of RecordIds, this triggers record fetches.
	/// Parallelizing across rows lets multiple fetches proceed concurrently.
	async fn evaluate_batch(
		&self,
		ctx: EvalContext<'_>,
		values: &[Value],
	) -> FlowResult<Vec<Value>> {
		if values.len() < PARALLEL_BATCH_THRESHOLD {
			let mut results = Vec::with_capacity(values.len());
			for value in values {
				results.push(self.evaluate(ctx.with_value(value)).await?);
			}
			return Ok(results);
		}
		let futures: Vec<_> =
			values.iter().map(|value| self.evaluate(ctx.with_value(value))).collect();
		futures::future::try_join_all(futures).await
	}

	fn references_current_value(&self) -> bool {
		true
	}

	fn access_mode(&self) -> AccessMode {
		AccessMode::ReadOnly
	}
}

impl ToSql for AllPart {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str("[*]");
	}
}

/// All elements evaluation.
pub(crate) async fn evaluate_all(value: &Value, ctx: EvalContext<'_>) -> anyhow::Result<Value> {
	match value {
		Value::Array(arr) => {
			// Check if the array contains RecordIds that need fetching
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
		Value::Object(_) => {
			// All (*) on an Object is a no-op - returns the object itself.
			// This matches the old executor's behavior where Part::All on Object
			// simply continues with the remaining path.
			Ok(value.clone())
		}
		Value::RecordId(rid) => {
			// Fetch the record and return the full object with computed fields evaluated
			fetch_record_with_computed_fields(rid, ctx).await
		}
		// For other types, return as single-element array
		other => Ok(Value::Array(vec![other.clone()].into())),
	}
}

// ============================================================================
// FlattenPart -- ...
// ============================================================================

/// Flatten nested arrays - `...` or flatten operation.
///
/// Also inserted by the planner between consecutive lookups.
#[derive(Debug, Clone)]
pub struct FlattenPart;

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl PhysicalExpr for FlattenPart {
	fn name(&self) -> &'static str {
		"Flatten"
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Root
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> FlowResult<Value> {
		let value = ctx.current_value.cloned().unwrap_or(Value::None);
		Ok(evaluate_flatten(&value)?)
	}

	fn references_current_value(&self) -> bool {
		true
	}

	fn access_mode(&self) -> AccessMode {
		AccessMode::ReadOnly
	}
}

impl ToSql for FlattenPart {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str("...");
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

// ============================================================================
// FirstPart -- [$]
// ============================================================================

/// First element - `[$]` or `.first()`.
#[derive(Debug, Clone)]
pub struct FirstPart;

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl PhysicalExpr for FirstPart {
	fn name(&self) -> &'static str {
		"First"
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Root
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> FlowResult<Value> {
		let value = ctx.current_value.cloned().unwrap_or(Value::None);
		match value {
			Value::Array(arr) => Ok(arr.first().cloned().unwrap_or(Value::None)),
			other => Ok(other),
		}
	}

	fn references_current_value(&self) -> bool {
		true
	}

	fn access_mode(&self) -> AccessMode {
		AccessMode::ReadOnly
	}
}

impl ToSql for FirstPart {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str("[$]");
	}
}

// ============================================================================
// LastPart -- [~]
// ============================================================================

/// Last element - `[~]` or `.last()`.
#[derive(Debug, Clone)]
pub struct LastPart;

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl PhysicalExpr for LastPart {
	fn name(&self) -> &'static str {
		"Last"
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Root
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> FlowResult<Value> {
		let value = ctx.current_value.cloned().unwrap_or(Value::None);
		match value {
			Value::Array(arr) => Ok(arr.last().cloned().unwrap_or(Value::None)),
			other => Ok(other),
		}
	}

	fn references_current_value(&self) -> bool {
		true
	}

	fn access_mode(&self) -> AccessMode {
		AccessMode::ReadOnly
	}
}

impl ToSql for LastPart {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str("[~]");
	}
}
