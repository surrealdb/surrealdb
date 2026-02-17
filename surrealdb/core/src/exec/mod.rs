//! # Streaming Execution Module
//!
//! This module implements a streaming query execution engine for SurrealDB. It provides
//! a complete replacement for the recursive `compute()` method path used by the `expr`
//! module, enabling push-based, batched execution of query plans.
//!
//! ## Design Principles
//!
//! - **No compute methods**: This module must not call any `compute()` methods from the `expr`
//!   module. All evaluation logic is implemented through [`PhysicalExpr`] and [`OperatorPlan`]
//!   traits to maintain a clean separation between the legacy compute path and the streaming
//!   execution path.
//!
//! - **Push-based streaming**: Rather than pulling results through recursive calls, operators push
//!   batches of values downstream through async streams. This enables better memory efficiency and
//!   supports incremental result delivery.
//!
//! - **Batched execution**: Values are processed in [`ValueBatch`] containers, allowing operators
//!   to amortize per-record overhead and enabling future optimizations like columnar execution.
//!
//! ## Module Structure
//!
//! - [`planner`]: Transforms parsed statements into executable operator plans
//! - [`operators`]: Physical operators (scan, filter, project, aggregate, etc.)
//! - [`physical_expr`]: Expression evaluation within the streaming context
//! - [`context`]: Execution context hierarchy (root → namespace → database)
//! - [`statement`]: Statement-level execution coordination
//!
//! ## Execution Flow
//!
//! 1. The [`planner`] converts a parsed statement into an [`OperatorPlan`] tree
//! 2. Context requirements are validated against the current session
//! 3. Each operator's `execute()` method returns a [`ValueBatchStream`]
//! 4. Streams are composed and consumed to produce query results

use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use futures::Stream;

use crate::err::Error;
// Re-export FlowResult, FlowResultExt, and ControlFlowExt for operator implementations
pub(crate) use crate::expr::{ControlFlowExt, FlowResult, FlowResultExt};
use crate::val::Value;

// =========================================================================
// WASM-compat helpers: conditional Send/Sync bounds
// =========================================================================

/// Conditional `Send + Sync` requirement.
///
/// On non-WASM targets this requires `Send + Sync`; on WASM (single-threaded)
/// it is a blanket trait satisfied by every type.
#[cfg(target_family = "wasm")]
pub(crate) trait SendSyncRequirement {}
#[cfg(target_family = "wasm")]
impl<T> SendSyncRequirement for T {}

#[cfg(not(target_family = "wasm"))]
pub(crate) trait SendSyncRequirement: Send + Sync {}
#[cfg(not(target_family = "wasm"))]
impl<T: Send + Sync> SendSyncRequirement for T {}

/// A boxed future that is `Send` only on non-WASM targets.
#[cfg(target_family = "wasm")]
pub(crate) type BoxFut<'a, T> = Pin<Box<dyn std::future::Future<Output = T> + 'a>>;
/// A boxed future that is `Send` only on non-WASM targets.
#[cfg(not(target_family = "wasm"))]
pub(crate) type BoxFut<'a, T> = Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

pub(crate) mod access_mode;
pub(crate) mod buffer;
pub(crate) mod cardinality;
pub(crate) mod context;
pub(crate) mod expression_registry;
pub(crate) mod field_path;
pub(crate) mod function;
pub(crate) mod index;
pub(crate) mod metrics;
pub(crate) mod operators;
pub(crate) mod ordering;
pub(crate) mod parts;
pub(crate) mod permission;
pub(crate) mod physical_expr;
pub(crate) mod plan_or_compute;
pub(crate) mod planner;

// Re-export access mode types
pub(crate) use access_mode::{AccessMode, CombineAccessModes};
// Re-export buffer helper
pub(crate) use buffer::buffer_stream;
// Re-export cardinality hint
pub(crate) use cardinality::CardinalityHint;
// Re-export context types
pub(crate) use context::{ContextLevel, DatabaseContext, ExecutionContext};
// Re-export metrics types
pub(crate) use metrics::{OperatorMetrics, monitor_stream};
// Re-export ordering types
pub(crate) use ordering::OutputOrdering;
// Re-export physical expression types
pub(crate) use physical_expr::{EvalContext, PhysicalExpr};

/// A batch of values returned by an execution plan.
///
/// Idea: In the future, this could become an `enum` to support columnar execution as well:
/// ```rust
/// enum ValueBatch {
///     Values(Vec<Value>),
///     Columnar(arrow::RecordBatch),
/// }
/// ```
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ValueBatch {
	pub(crate) values: Vec<Value>,
}

#[cfg(target_family = "wasm")]
pub(crate) type ValueBatchStream = Pin<Box<dyn Stream<Item = FlowResult<ValueBatch>>>>;
#[cfg(not(target_family = "wasm"))]
pub(crate) type ValueBatchStream = Pin<Box<dyn Stream<Item = FlowResult<ValueBatch>> + Send>>;

/// A trait for execution plans that can be executed and produce a stream of value batches.
///
/// Execution plans form a tree structure where each node declares its minimum required
/// context level via `required_context()`. The executor validates that the current session
/// meets these requirements before execution begins.
#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
pub(crate) trait ExecOperator: Debug + SendSyncRequirement {
	fn name(&self) -> &'static str;

	fn attrs(&self) -> Vec<(String, String)> {
		vec![]
	}

	/// The minimum context level required to execute this plan.
	///
	/// Used for pre-flight validation: the executor checks that the current session
	/// has at least this context level before calling `execute()`.
	fn required_context(&self) -> ContextLevel;

	/// Executes the execution plan and returns a stream of value batches.
	///
	/// The context is guaranteed to meet the requirements declared by `required_context()`
	/// if the executor performs proper validation.
	///
	/// Returns `FlowResult` to support control flow signals:
	/// - `Ok(stream)` - normal execution producing a stream of batches
	/// - `Err(ControlFlow::Return(value))` - early return from block/function
	/// - `Err(ControlFlow::Break)` - break from loop
	/// - `Err(ControlFlow::Continue)` - continue to next loop iteration
	/// - `Err(ControlFlow::Err(e))` - error condition
	///
	/// NOTE: This is intentionally not async to ensure that the execution graph is constructed
	/// fully before any execution begins.
	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream>;

	/// Returns references to child execution plans for tree traversal.
	///
	/// Used for:
	/// - Pre-flight validation (recursive context requirement checking)
	/// - Query optimization
	/// - EXPLAIN output
	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		vec![]
	}

	/// Does this operator modify the execution context?
	///
	/// True for USE, LET, BEGIN, COMMIT, CANCEL operators.
	/// When true, the executor will call `output_context()` after execution
	/// to get the modified context for downstream statements.
	fn mutates_context(&self) -> bool {
		false
	}

	/// Compute the output context after execution.
	///
	/// Only called if `mutates_context()` returns true.
	/// This method may perform async operations (like looking up namespace/database
	/// definitions or creating transactions).
	async fn output_context(&self, input: &ExecutionContext) -> Result<ExecutionContext, Error> {
		Ok(input.clone())
	}

	/// Returns the access mode for this plan (and all its children).
	///
	/// This determines whether the plan performs mutations:
	/// - `AccessMode::ReadOnly`: Only reads data, can run in parallel with other reads
	/// - `AccessMode::ReadWrite`: May write data, acts as a barrier
	///
	/// **Critical**: This must recursively check all children and expressions.
	/// A `SELECT` with a mutation subquery (e.g., `SELECT *, (UPSERT person) FROM person`)
	/// must return `ReadWrite` even though it's syntactically a SELECT.
	fn access_mode(&self) -> AccessMode;

	/// Returns a hint about the expected number of output rows.
	///
	/// Used by [`buffer_stream`] to choose an appropriate buffering strategy:
	/// - `AtMostOne`: skip buffering entirely (no point spawning a task for one row)
	/// - `Bounded(n)`: use cooperative prefetch when `n` is small
	/// - `Unbounded`: full buffering based on [`AccessMode`]
	///
	/// The default is `Unbounded` (conservative, status-quo behaviour).
	/// Override in leaf operators with known small cardinality.
	fn cardinality_hint(&self) -> CardinalityHint {
		CardinalityHint::Unbounded
	}

	/// Returns true if this plan represents a scalar expression.
	///
	/// Scalar expressions return a single value directly, while queries
	/// return results wrapped in an array. This is used by the executor
	/// to format results correctly.
	fn is_scalar(&self) -> bool {
		false
	}

	/// Returns the operator-level metrics for this node, if available.
	///
	/// Used by `EXPLAIN ANALYZE` to collect runtime statistics after
	/// the plan has been fully consumed.
	fn metrics(&self) -> Option<&OperatorMetrics> {
		None
	}

	/// Recursively enable metrics collection on this operator and all
	/// its children.
	///
	/// Called by `AnalyzePlan` before execution so that `monitor_stream`
	/// wraps each operator's output with timing/counting instrumentation.
	/// For normal (non-ANALYZE) queries, metrics remain disabled and
	/// `monitor_stream` returns the inner stream directly with zero overhead.
	fn enable_metrics(&self) {
		if let Some(m) = self.metrics() {
			m.enable();
		}
		for child in self.children() {
			child.enable_metrics();
		}
	}

	/// Returns named references to physical expressions owned by this operator.
	///
	/// Used by `EXPLAIN` / `EXPLAIN ANALYZE` to display the expression tree
	/// beneath each operator. The name describes the role of the expression
	/// (e.g. "predicate", "projection", "sort_key").
	fn expressions(&self) -> Vec<(&str, &Arc<dyn PhysicalExpr>)> {
		vec![]
	}

	/// Returns the output ordering guarantee for this operator's stream.
	///
	/// Used by the planner to determine whether a downstream Sort operator
	/// can be eliminated. The default is [`OutputOrdering::Unordered`].
	///
	/// Operators that preserve input ordering (Filter, Limit, Project, etc.)
	/// should delegate to `self.input.output_ordering()`. Operators that
	/// produce ordered output (Sort, IndexScan, TableScan) should return
	/// `OutputOrdering::Sorted(...)`.
	fn output_ordering(&self) -> OutputOrdering {
		OutputOrdering::Unordered
	}
}
