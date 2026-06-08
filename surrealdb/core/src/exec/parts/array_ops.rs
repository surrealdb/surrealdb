//! Array operation parts -- `[*]`, `...`, `[$]`, `[~]`.

use surrealdb_types::{SqlFormat, ToSql};

use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{AccessMode, BoxFut, ContextLevel};
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
impl PhysicalExpr for AllPart {
	fn name(&self) -> &'static str {
		"All"
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn required_context(&self) -> ContextLevel {
		// All (.*) may trigger record fetch + computed field evaluation
		ContextLevel::Database
	}

	fn evaluate<'a>(&'a self, ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		Box::pin(async move {
			let value = ctx.current_value.unwrap_or(&Value::NONE);
			evaluate_all(value, ctx).await
		})
	}

	/// Parallel batch evaluation for `[*]` / `.*`.
	///
	/// When applied to arrays of RecordIds, this triggers record fetches.
	/// Parallelizing across rows lets multiple fetches proceed concurrently.
	fn evaluate_batch<'a>(
		&'a self,
		ctx: EvalContext<'a>,
		values: &'a [Value],
	) -> BoxFut<'a, FlowResult<Vec<Value>>> {
		Box::pin(async move {
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
		})
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
pub(crate) async fn evaluate_all(value: &Value, ctx: EvalContext<'_>) -> FlowResult<Value> {
	match value {
		Value::Array(arr) => {
			let has_record_ids = arr.iter().any(|v| matches!(v, Value::RecordId(_)));
			if has_record_ids {
				let mut results = Vec::with_capacity(arr.len());
				for item in arr.iter() {
					// Match legacy `val/value/get.rs`: under `.*`, only RecordId
					// elements trigger a fetch; everything else passes through
					// unchanged.
					let processed = match item {
						Value::RecordId(_) => Box::pin(evaluate_all(item, ctx.clone())).await?,
						_ => item.clone(),
					};
					results.push(processed);
				}
				Ok(Value::Array(results.into()))
			} else {
				Ok(Value::Array(arr.clone()))
			}
		}
		Value::Object(_) => Ok(value.clone()),
		Value::RecordId(rid) => {
			if ctx.skip_fetch_perms {
				crate::exec::operators::fetch::fetch_record_no_perms(ctx.exec_ctx, rid).await
			} else {
				crate::exec::operators::fetch::fetch_record(ctx.exec_ctx, rid).await
			}
		}
		// Anything else (NONE, NULL, scalars, geometries) returns NONE, matching the
		// legacy compute path. Issue #7143: previously this wrapped the value in a
		// single-element array, so `none.*` produced `[NONE]` instead of `NONE`.
		_ => Ok(Value::None),
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
impl PhysicalExpr for FlattenPart {
	fn name(&self) -> &'static str {
		"Flatten"
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Root
	}

	fn evaluate<'a>(&'a self, ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		Box::pin(async move {
			let value = ctx.current_value.unwrap_or(&Value::NONE);
			Ok(evaluate_flatten(value)?)
		})
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
impl PhysicalExpr for FirstPart {
	fn name(&self) -> &'static str {
		"First"
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Root
	}

	fn evaluate<'a>(&'a self, ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		Box::pin(async move {
			let value = ctx.current_value.cloned().unwrap_or(Value::None);
			match value {
				Value::Array(arr) => Ok(arr.first().cloned().unwrap_or(Value::None)),
				other => Ok(other),
			}
		})
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
impl PhysicalExpr for LastPart {
	fn name(&self) -> &'static str {
		"Last"
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Root
	}

	fn evaluate<'a>(&'a self, ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		Box::pin(async move {
			let value = ctx.current_value.cloned().unwrap_or(Value::None);
			match value {
				Value::Array(arr) => Ok(arr.last().cloned().unwrap_or(Value::None)),
				other => Ok(other),
			}
		})
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
