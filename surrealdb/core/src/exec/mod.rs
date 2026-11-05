//! # Execution Module
//!
//! This module is SurrealDB's query execution engine. The bytecode VM ([`vm`]) runs every
//! statement; a statement the VM cannot run is an error, and nothing falls back to another
//! engine (`doc/adr/0007-one-engine-the-bytecode-vm.md`).
//!
//! ## Design Principles
//!
//! - **One engine**: Evaluation is implemented through [`Clause`] and the VM's programs. The
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
//! - [`clause`]: The stored expression the engine evaluates, as one concrete type
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

use futures::Stream;

// Re-export FlowResult and ControlFlowExt for operator implementations
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
pub(crate) mod catch_scope;
pub(crate) mod clause;
pub(crate) mod clause_effects;
pub(crate) mod closure;
pub(crate) mod compiles;
pub(crate) mod config;
pub(crate) mod context;
pub(crate) mod entry;
pub(crate) mod error;
pub(crate) mod eval_context;
pub(crate) mod expression_registry;
pub(crate) mod fan_out;
pub(crate) mod frame;
pub(crate) mod kill;
pub(crate) mod live;
pub(crate) mod scalar_unit;
pub(crate) mod session;
pub(crate) mod surface;
pub(crate) mod trace;
pub(crate) use crate::val::field_path;
pub(crate) mod field_path_convert;
pub(crate) mod field_rules;
pub(crate) mod function;
pub(crate) mod guard_verdicts;
pub(crate) mod index;
#[allow(dead_code)]
pub(crate) mod ir;
pub(crate) mod logical;
pub(crate) mod materialize;
pub mod memory;
pub(crate) mod metrics;
pub(crate) mod migrate;
pub(crate) mod object_extract;
pub(crate) mod operators;
pub(crate) mod ordering;
pub(crate) mod packed_view;
pub(crate) mod parts;
pub(crate) mod permission;
pub mod places;
pub(crate) mod planner;
pub(crate) mod pre_decode_filter;
pub(crate) mod reference_purge;
#[doc(hidden)]
pub mod row_frame_probe;
pub mod scheduler;
#[cfg(feature = "shaped")]
pub(crate) mod shaped;
pub(crate) mod table_view;
pub(crate) mod topk_pushdown;
pub(crate) mod trampoline;
pub(crate) mod vm;
// Re-export access mode types
pub(crate) use access_mode::AccessMode;
// Re-export the operator data unit
pub(crate) use batch::ValueBatch;
// Re-export buffer helper
pub(crate) use buffer::buffer_stream;
// Re-export cardinality hint
pub(crate) use cardinality::CardinalityHint;
// Re-export the stored-expression type and its evaluation context
pub(crate) use clause::Clause;
// Re-export context types
pub(crate) use context::{ContextLevel, DatabaseContext, ExecutionContext};
pub(crate) use error::Error;
pub(crate) use eval_context::EvalContext;
// Re-export metrics types
pub(crate) use metrics::{OperatorMetrics, monitor_stream};

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
/// Each plan declares its minimum required context level via `required_context()`. The executor
/// validates that the current session meets that requirement before execution begins.
pub(crate) trait ExecOperator: Debug + Send + Sync {
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

	/// [`Self::execute`] for a consumer that writes the statement's rows in
	/// `format`: an operator that can encode its own rows hands them on as
	/// encoded batches ([`ValueBatch::encoded`]), which no public value is
	/// built for. Rows of nested statements are never encoded. By default the
	/// rows are values, as `execute` hands them on.
	fn execute_encoded(
		&self,
		ctx: &ExecutionContext,
		format: surrealdb_rpc::RowFormat,
	) -> FlowResult<ValueBatchStream> {
		let _ = format;
		self.execute(ctx)
	}

	/// Returns the access mode for this plan.
	///
	/// This determines whether the plan performs mutations:
	/// - `AccessMode::ReadOnly`: Only reads data, can run in parallel with other reads
	/// - `AccessMode::ReadWrite`: May write data, acts as a barrier
	///
	/// **Critical**: This must recursively check all children and expressions.
	/// A `SELECT` with a mutation subquery (e.g., `SELECT *, (UPSERT person) FROM person`)
	/// must return `ReadWrite` even though it's syntactically a SELECT.
	fn access_mode(&self) -> AccessMode;

	/// The shape this operator's output takes when it is a statement's result.
	///
	/// See [`OutputShape`]. The default is [`OutputShape::Rows`]: an operator
	/// that emits rows.
	fn output_shape(&self) -> OutputShape {
		OutputShape::Rows
	}
}
