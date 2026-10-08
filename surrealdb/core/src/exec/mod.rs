//! # Execution Module
//!
//! This module is SurrealDB's query execution engine. The bytecode VM ([`vm`]) runs every
//! statement; a statement the VM cannot run is an error, and nothing falls back to another
//! engine (`doc/adr/0007-one-engine-the-bytecode-vm.md`).
//!
//! ## Design Principles
//!
//! - **One engine**: Evaluation is implemented through [`PhysicalExpr`] and the VM's programs. The
//!   operators of [`operators`] are host services the VM calls — storage scans, index iterators,
//!   record fetches — and not a second executor.
//!
//! - **Push-based streaming**: Operators push batches of values downstream through async streams
//!   rather than pulling results through recursive calls. This enables better memory efficiency and
//!   supports incremental result delivery.
//!
//! - **Batched execution**: Values are processed in [`ValueBatch`] containers, allowing operators
//!   to amortize per-record overhead and enabling future optimizations like columnar execution.
//!
//! ## Module Structure
//!
//! - [`planner`]: Binds a logical plan and plans it into a VM program, compiling the host services
//!   the program needs
//! - [`vm`]: The physical plan, the bytecode lowering and the interpreter
//! - [`operators`]: Physical operators the VM uses as host services (scan, graph, join, etc.)
//! - [`physical_expr`]: Expression evaluation within the execution context
//! - [`context`]: Execution context hierarchy (root → namespace → database)
//!
//! ## Execution Flow
//!
//! 1. The [`planner`] binds a statement's logical plan for one transaction and principal
//! 2. Context requirements are validated against the current session
//! 3. The VM maps, optimises and lowers the plan to a program and runs it
//! 4. The host serves the program's storage reads; results are produced as a [`ValueBatchStream`]

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
pub(crate) mod analyzer_function;
pub(crate) mod api;
pub(crate) mod batch;
pub(crate) mod buffer;
pub(crate) mod cardinality;
pub(crate) mod catalog_deps;
pub(crate) mod closure;
pub(crate) mod compiles;
pub(crate) mod config;
pub(crate) mod context;
pub(crate) mod entry;
pub(crate) mod error;
pub(crate) mod expression_registry;
pub(crate) mod fan_out;
pub(crate) mod frame;
pub(crate) mod kill;
pub(crate) mod live;
pub(crate) mod scalar_unit;
pub(crate) mod zero_limit;
pub(crate) use crate::val::field_path;
pub(crate) mod field_path_convert;
pub(crate) mod field_rules;
pub(crate) mod function;
pub(crate) mod guard_verdicts;
pub(crate) mod index;
pub(crate) mod logical;
pub(crate) mod metrics;
pub(crate) mod object_extract;
pub(crate) mod operators;
pub(crate) mod ordering;
pub(crate) mod partitioning;
pub(crate) mod parts;
pub(crate) mod permission;
pub(crate) mod physical_expr;
pub(crate) mod planner;
pub(crate) mod pre_decode_filter;
pub(crate) mod reference_purge;
pub mod scheduler;
pub(crate) mod table_view;
pub(crate) mod topk_pushdown;
pub(crate) mod trampoline;
pub(crate) mod vm;
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
pub(crate) trait ExecOperator: Debug + Send + Sync + std::any::Any {
	fn name(&self) -> &'static str;

	/// The name an expression holding this operator's statement shows it
	/// by: the operator at the top of the plan `EXPLAIN` renders for it.
	fn display_name(&self) -> &'static str {
		self.name()
	}

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
	/// - EXPLAIN output, through [`Self::explain_children`]
	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		vec![]
	}

	/// For a scan of storage, the limit it is bound to, which spares it its
	/// records when that limit evaluates to zero (see [`zero_limit`]).
	fn zero_limit_binding(&self) -> Option<&zero_limit::ZeroLimitBinding> {
		None
	}

	/// The children `EXPLAIN` renders beneath this operator: every child,
	/// unless the operator hides one from the caller. A join lifted out of a
	/// permission predicate hides its right side from a caller the permission
	/// is enforced on, as `PermissionFilter` hides its predicate. Every other
	/// traversal — validation, metrics, instrumentation — uses
	/// [`Self::children`].
	fn explain_children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		self.children()
	}

	/// Whether `EXPLAIN ANALYZE` withholds the metrics of every operator
	/// beneath this one. `true` for a node enforcing a definer's declaration
	/// on a caller it is enforced on — a table permission, a field-guard
	/// layer, a join lifted from a permission predicate — since the row
	/// counts beneath it are counts over records that caller may not see.
	/// The node's own metrics are still rendered: its output is what the
	/// caller receives.
	fn hides_metrics_beneath(&self) -> bool {
		false
	}

	/// Whether `EXPLAIN` renders the access path of every operator beneath
	/// this one as `<permission>`. `true` for a permission node whose
	/// predicate was offered to the access path of the scan beneath it, in a
	/// plan compiled for a caller the predicate is enforced on: the bound an
	/// index scan or a record-id lookup would print is the definer's.
	fn redacts_access_paths_beneath(&self) -> bool {
		false
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
		// Operator trees embedded in this operator's expressions — a lookup
		// part's pre-planned scan, for example — are rendered by EXPLAIN
		// ANALYZE exactly like children, so they must be instrumented like
		// children too.
		for (_, expr) in self.expressions() {
			for (_, embedded) in expr.embedded_operators() {
				embedded.enable_metrics();
			}
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
