//! Lookup expressions for graph/reference traversal.
//!
//! Note: This module is work-in-progress for graph traversal expressions.
#![allow(dead_code)]

use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use surrealdb_types::{SqlFormat, ToSql};

use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::physical_part::LookupDirection;
use crate::exec::{AccessMode, ExecOperator};
use crate::val::Value;

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
	pub(crate) plan: Arc<dyn ExecOperator>,

	/// Direction of the lookup (for display purposes)
	pub(crate) direction: LookupDirection,

	/// Optional alias for multi-yield expressions
	pub(crate) alias: Option<crate::expr::Idiom>,
}

impl LookupExpr {
	/// Create a new LookupExpr with the given plan and direction.
	pub fn new(plan: Arc<dyn ExecOperator>, direction: LookupDirection) -> Self {
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

	fn required_context(&self) -> crate::exec::ContextLevel {
		// Graph/reference traversal requires database context
		crate::exec::ContextLevel::Database
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> anyhow::Result<Value> {
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
