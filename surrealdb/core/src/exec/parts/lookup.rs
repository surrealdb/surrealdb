//! Lookup part -- graph/reference traversal.

use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use surrealdb_types::{SqlFormat, ToSql};

use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{AccessMode, ContextLevel, ExecOperator};
use crate::expr::FlowResult;
use crate::val::Value;

/// Direction for lookup operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LookupDirection {
	/// Outgoing edges: `->`
	Out,
	/// Incoming edges: `<-`
	In,
	/// Both directions: `<->`
	Both,
	/// Record references: `<~`
	Reference,
}

impl From<crate::expr::Dir> for LookupDirection {
	fn from(dir: crate::expr::Dir) -> Self {
		match dir {
			crate::expr::Dir::Out => LookupDirection::Out,
			crate::expr::Dir::In => LookupDirection::In,
			crate::expr::Dir::Both => LookupDirection::Both,
		}
	}
}

impl From<&crate::expr::Dir> for LookupDirection {
	fn from(dir: &crate::expr::Dir) -> Self {
		match dir {
			crate::expr::Dir::Out => LookupDirection::Out,
			crate::expr::Dir::In => LookupDirection::In,
			crate::expr::Dir::Both => LookupDirection::Both,
		}
	}
}

/// Graph/reference lookup - `->edge->target`, `<-edge<-source`, `<~table`.
#[derive(Debug, Clone)]
pub struct LookupPart {
	/// The direction of the lookup (In, Out, Both for graph; Reference for <~)
	pub direction: LookupDirection,

	/// The pre-planned operator tree for executing the lookup.
	/// This includes GraphEdgeScan/ReferenceScan + optional Filter, Sort, Limit, Project.
	pub plan: Arc<dyn ExecOperator>,

	/// When true, extract just the RecordId from result objects.
	/// This is set when the scan uses FullEdge mode for WHERE/SPLIT filtering
	/// but no explicit SELECT clause is present, so the final result should be
	/// RecordIds rather than full objects.
	pub extract_id: bool,

	/// Whether this LookupPart contains a fused chain of multiple consecutive lookups.
	/// When true, the continuation logic in `evaluate_parts_with_continuation` maps
	/// per-element over non-lookup arrays even when this is the last part in the idiom.
	pub fused: bool,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl PhysicalExpr for LookupPart {
	fn name(&self) -> &'static str {
		"Lookup"
	}

	fn required_context(&self) -> ContextLevel {
		// Lookups need database context, combined with the child plan's context
		self.plan.required_context().max(ContextLevel::Database)
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> FlowResult<Value> {
		let value = ctx.current_value.cloned().unwrap_or(Value::None);
		Ok(evaluate_lookup(&value, self, ctx).await?)
	}

	/// Parallel batch evaluation for graph/reference lookups.
	///
	/// Each lookup executes a plan per RecordId, which involves I/O.
	/// Parallelizing across rows lets multiple lookups proceed concurrently.
	/// Falls back to sequential for ReadWrite plans to preserve mutation ordering.
	async fn evaluate_batch(
		&self,
		ctx: EvalContext<'_>,
		values: &[Value],
	) -> FlowResult<Vec<Value>> {
		if values.len() < 2 || self.access_mode() == AccessMode::ReadWrite {
			// Sequential for small batches or mutation plans
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

	fn access_mode(&self) -> AccessMode {
		self.plan.access_mode()
	}

	fn embedded_operators(&self) -> Vec<(&str, &Arc<dyn ExecOperator>)> {
		vec![("lookup", &self.plan)]
	}

	fn is_fused_lookup(&self) -> bool {
		self.fused
	}
}

impl ToSql for LookupPart {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		match self.direction {
			LookupDirection::Out => f.push_str("->..."),
			LookupDirection::In => f.push_str("<-..."),
			LookupDirection::Both => f.push_str("<->..."),
			LookupDirection::Reference => f.push_str("<~..."),
		}
	}
}

/// Lookup evaluation - graph/reference traversal.
async fn evaluate_lookup(
	value: &Value,
	lookup: &LookupPart,
	ctx: EvalContext<'_>,
) -> anyhow::Result<Value> {
	match value {
		Value::RecordId(_) | Value::Object(_) => {
			// Execute the lookup plan with this value as the current_value.
			// The CurrentValueSource operator at the leaf of the plan will
			// yield this value, and GraphEdgeScan/ReferenceScan will extract
			// RecordIds from it (including extracting `id` from Objects).
			evaluate_lookup_for_value(value, lookup, ctx).await
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

/// Perform graph/reference lookup for a specific value by executing the pre-planned operator tree.
///
/// Sets `current_value` on the `ExecutionContext` so that the `CurrentValueSource`
/// operator at the leaf of the plan yields this value into the stream.
async fn evaluate_lookup_for_value(
	value: &Value,
	lookup: &LookupPart,
	ctx: EvalContext<'_>,
) -> anyhow::Result<Value> {
	// Create a new execution context with the current value set.
	// The CurrentValueSource operator reads this to seed the operator chain.
	let bound_ctx = ctx.exec_ctx.with_current_value(value.clone());

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
			crate::expr::ControlFlow::Continue => {
				anyhow::anyhow!("Unexpected continue in lookup")
			}
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
