//! Statement-level execution types for the DAG-based script executor.
//!
//! The execution model separates concerns into:
//! - **Operators**: Data processing units that receive context and produce streams
//! - **Statements**: Top-level units that manage context flow, ordering, and barriers
//!
//! Statements form a DAG where:
//! - `context_source` tracks where execution context comes from (USE, LET)
//! - `wait_for` encodes ordering constraints
//! - Parallelism emerges naturally from the DAG structure

use std::sync::Arc;
use std::time::Duration;

use crate::exec::{ExecutionContext, OperatorPlan, PhysicalExpr};
use crate::val::Value;

/// Unique identifier for statements within a script.
///
/// Used to reference statements in the DAG for dependency tracking.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct StatementId(pub usize);

impl std::fmt::Display for StatementId {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "S{}", self.0)
	}
}

/// Classification of statement types for dependency tracking.
///
/// Different statement kinds have different effects on execution ordering:
/// - `ContextMutation`: Modifies execution context (USE, LET), affects downstream context
/// - `DataMutation`: Modifies stored data, creates full barriers
/// - `PureRead`: Reads only, can parallelize freely
/// - `Transaction`: Transaction control, creates barriers
/// - `Schema`: Schema changes, creates barriers
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum StatementKind {
	/// USE, LET - modifies execution context
	///
	/// These statements don't block parallel execution but their output
	/// context is used by downstream statements that reference their variables.
	ContextMutation,

	/// CREATE, UPDATE, DELETE, UPSERT, INSERT - modifies stored data
	///
	/// These are **full barriers**: they must wait for ALL prior statements
	/// to complete, and ALL subsequent statements must wait for them.
	/// This is because mutations can have side effects (events, computed fields)
	/// that affect any table.
	DataMutation,

	/// SELECT, INFO, SHOW - reads only, can parallelize
	///
	/// These statements can run in parallel with other reads, but must
	/// wait for any preceding mutations to complete.
	PureRead,

	/// BEGIN, COMMIT, CANCEL - transaction control
	///
	/// These are full barriers that affect the transaction state.
	Transaction,

	/// DEFINE, REMOVE - schema changes
	///
	/// These are full barriers because schema changes affect table definitions.
	Schema,
}

impl StatementKind {
	/// Returns true if this statement kind creates a full barrier.
	///
	/// Full barriers must wait for ALL prior statements to complete,
	/// and ALL subsequent statements must wait for them.
	pub fn is_full_barrier(&self) -> bool {
		matches!(self, Self::DataMutation | Self::Transaction | Self::Schema)
	}

	/// Returns true if this statement mutates the execution context.
	pub fn mutates_context(&self) -> bool {
		matches!(self, Self::ContextMutation)
	}
}

/// The value bound by a LET statement in the statement layer
#[derive(Debug, Clone)]
pub enum StatementLetValue {
	/// Scalar expression - evaluates to exactly one Value
	Scalar(Arc<dyn PhysicalExpr>),
	/// Query - stream is collected into Value::Array
	Query(Arc<dyn OperatorPlan>),
}

/// Content of a statement - what actually gets executed.
#[derive(Debug, Clone)]
pub enum StatementContent {
	/// A query plan (SELECT, etc.)
	Query(Arc<dyn OperatorPlan>),

	/// A session command (USE NS/DB)
	Use {
		ns: Option<Arc<dyn PhysicalExpr>>,
		db: Option<Arc<dyn PhysicalExpr>>,
	},

	/// A LET statement binding
	Let {
		name: String,
		value: StatementLetValue,
	},

	/// A scalar expression evaluated as a top-level statement
	Scalar(Arc<dyn PhysicalExpr>),

	/// Transaction control
	Begin,
	Commit,
	Cancel,
}

/// A planned statement with DAG dependencies.
///
/// Each statement in a script becomes a `StatementPlan` node in the execution DAG.
/// The DAG structure enables parallel execution while maintaining correctness:
///
/// - Statements wait for their `context_source` to get their execution context
/// - Statements wait for their `wait_for` dependencies for ordering
/// - Mutations create full barriers that serialize execution
pub struct StatementPlan {
	/// Unique ID within the script
	pub id: StatementId,

	/// The statement that provides our execution context.
	///
	/// This is the most recent context-mutating statement (USE/LET) before us.
	/// None means we use the initial context.
	pub context_source: Option<StatementId>,

	/// Statements we must wait for before executing.
	///
	/// - For pure reads: empty (context dependency is handled separately)
	/// - For mutations: contains ALL prior statement IDs (full barrier)
	/// - For statements after a mutation: contains the mutation ID
	pub wait_for: Vec<StatementId>,

	/// What this statement executes
	pub content: StatementContent,

	/// Statement classification for dependency tracking
	pub kind: StatementKind,
}

impl std::fmt::Debug for StatementPlan {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("StatementPlan")
			.field("id", &self.id)
			.field("context_source", &self.context_source)
			.field("wait_for", &self.wait_for)
			.field("kind", &self.kind)
			.finish()
	}
}

impl StatementPlan {
	/// Does this statement create a full barrier?
	pub fn is_full_barrier(&self) -> bool {
		self.kind.is_full_barrier()
	}

	/// Does this statement modify the execution context?
	pub fn mutates_context(&self) -> bool {
		self.kind.mutates_context()
	}
}

/// Result of executing a statement.
///
/// Contains the output context (possibly modified), the results,
/// and timing information.
#[derive(Clone, Debug)]
pub struct StatementOutput {
	/// The execution context after this statement.
	///
	/// For context-mutating statements (USE/LET), this contains the
	/// modified context. For other statements, this is the input context.
	pub context: ExecutionContext,

	/// The data produced by this statement.
	///
	/// Empty for statements like USE that produce no data.
	pub results: Vec<Value>,

	/// How long the statement took to execute.
	pub duration: Duration,
}

/// A collection of statements forming the execution DAG.
///
/// The script plan contains all statements with their dependencies,
/// ready for parallel execution.
#[derive(Debug)]
pub struct ScriptPlan {
	/// All statements in the script, in order
	pub statements: Vec<StatementPlan>,
}

impl ScriptPlan {
	/// Create a new empty script plan.
	pub fn new() -> Self {
		Self {
			statements: Vec::new(),
		}
	}

	/// Get a statement by ID.
	pub fn get(&self, id: StatementId) -> Option<&StatementPlan> {
		self.statements.get(id.0)
	}

	/// Get the number of statements.
	pub fn len(&self) -> usize {
		self.statements.len()
	}

	/// Check if the script is empty.
	pub fn is_empty(&self) -> bool {
		self.statements.is_empty()
	}
}

impl Default for ScriptPlan {
	fn default() -> Self {
		Self::new()
	}
}

