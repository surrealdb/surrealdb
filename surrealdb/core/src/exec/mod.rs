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

use futures::Stream;

use crate::exe::FlowResultExt;
// Re-export FlowResult, FlowResultExt, and ControlFlowExt for operator implementations
pub(crate) use crate::expr::{ControlFlowExt, FlowResult};

/// A boxed `Send` future, used at trait boundaries to keep async state
/// machines from inflating the parent future.
pub(crate) type BoxFut<'a, T> = Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

pub(crate) mod access_mode;
pub(crate) mod batch;
pub(crate) mod buffer;
pub(crate) mod cardinality;
pub(crate) mod config;
pub(crate) mod context;
pub(crate) mod error;
pub(crate) mod expression_registry;
pub(crate) mod fan_out;
pub(crate) use crate::val::field_path;
pub(crate) mod field_path_convert;
pub(crate) mod function;
pub(crate) mod index;
pub(crate) mod metrics;
pub(crate) mod object_extract;
pub(crate) mod operators;
pub(crate) mod ordering;
pub(crate) mod partitioning;
pub(crate) mod parts;
pub(crate) mod permission;
pub(crate) mod physical_expr;
pub(crate) mod plan_or_compute;
pub(crate) mod planner;
pub(crate) mod pre_decode_filter;
pub(crate) mod topk_pushdown;

// Re-export access mode types
pub(crate) use access_mode::{AccessMode, CombineAccessModes};
// Re-export the operator data unit
pub(crate) use batch::ValueBatch;
// Re-export buffer helper
pub(crate) use buffer::buffer_stream;
// Re-export cardinality hint
pub(crate) use cardinality::CardinalityHint;
// Re-export context types
pub(crate) use context::{ContextLevel, DatabaseContext, ExecutionContext};
pub(crate) use error::Error;
// Re-export metrics types
pub(crate) use metrics::{OperatorMetrics, monitor_stream};
// Re-export ordering types
pub(crate) use ordering::OutputOrdering;
// Re-export the output-partitioning property
pub(crate) use partitioning::Partitioning;
// Re-export physical expression types
pub(crate) use physical_expr::{EvalContext, PhysicalExpr};

/// The shape an operator's output takes when it is a statement's result.
///
/// The plan declares this rather than the executor inferring it from how many
/// values arrived, because a consumer that forwards values as they are produced
/// has to know the shape before it knows the count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OutputShape {
	/// The statement's value is an array of every value emitted.
	Rows,
	/// The statement's value is the one value emitted, unwrapped. `SELECT ONLY`
	/// and every non-query expression (`RETURN 1 + 2`, `IF …`, a block) have
	/// this shape.
	///
	/// An operator declaring it emits exactly one value; emitting none or
	/// several is a bug in that operator, not a shape the caller chooses
	/// between.
	Scalar,
}

impl OutputShape {
	/// Whether the statement's value is the single emitted value rather than an
	/// array of them.
	pub(crate) fn is_scalar(self) -> bool {
		matches!(self, OutputShape::Scalar)
	}
}

pub(crate) type ValueBatchStream = Pin<Box<dyn Stream<Item = FlowResult<ValueBatch>> + Send>>;

/// A trait for execution plans that can be executed and produce a stream of value batches.
///
/// Execution plans form a tree structure where each node declares its minimum required
/// context level via `required_context()`. The executor validates that the current session
/// meets these requirements before execution begins.
pub(crate) trait ExecOperator: Debug + Send + Sync {
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
	///
	/// Returns `FlowResult` for the same reason `execute` does: computing the
	/// context runs the bound expression, and a `BREAK` or `CONTINUE` raised in
	/// there belongs to an enclosing loop, which sits above this call. Flattening
	/// the signal into an error here would strand it at the binding site, so the
	/// loop never sees it.
	///
	/// `ControlFlow::Err` carries an `anyhow::Error` rather than a concrete enum
	/// so that a failure raised while evaluating the bound expression reaches the
	/// boundary with its type intact. Flattening it cost the transactor's retry
	/// loop its ability to see a write conflict, and cancellation and timeout
	/// their classification.
	fn output_context<'a>(
		&'a self,
		input: &'a ExecutionContext,
	) -> BoxFut<'a, crate::expr::FlowResult<ExecutionContext>> {
		Box::pin(async move { Ok(input.clone()) })
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

	/// How this operator's output is divided across parallel streams.
	///
	/// The default is [`Partitioning::Single`]: one stream carrying every row,
	/// which is what every operator produces. An operator that passes rows
	/// through unchanged should delegate to its input, the way it delegates
	/// [`output_ordering`](Self::output_ordering); an operator that must see
	/// every row together must declare `Single` and merge its input rather than
	/// let a partial view reach anything downstream.
	///
	/// Only a `ReadOnly` subtree may ever be partitioned; see
	/// [`Partitioning`] for why a write cannot be.
	fn output_partitioning(&self) -> Partitioning {
		Partitioning::Single
	}

	/// The shape this operator's output takes when it is a statement's result.
	///
	/// See [`OutputShape`]. The default is [`OutputShape::Rows`]: an operator
	/// that emits rows.
	fn output_shape(&self) -> OutputShape {
		OutputShape::Rows
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

	/// Returns field paths that are guaranteed to have a single constant
	/// value across all output rows.
	///
	/// Used by sort-elimination: if a leading ORDER BY field references a
	/// constant column, it can be stripped from the requirement because any
	/// direction trivially holds for a single-valued column.
	///
	/// The default is an empty list (no constant fields).  IndexScan
	/// overrides this for equality-pinned columns.  Pass-through operators
	/// (Filter, Limit, etc.) should delegate to their input.
	fn constant_output_fields(&self) -> Vec<crate::exec::field_path::FieldPath> {
		vec![]
	}
}
