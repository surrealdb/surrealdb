//! Block execution types for the unified block execution model.
//!
//! This module defines the core types for executing statement blocks in SurrealDB.
//! The model applies to:
//! - Top-level scripts
//! - FOR loop bodies
//! - IF/ELSE branches
//! - FUNCTION bodies
//! - Transaction blocks

use std::sync::Arc;
use std::time::Duration;

use crate::exec::statement::StatementId;
use crate::exec::{AccessMode, CombineAccessModes, OperatorPlan, PhysicalExpr};
use crate::val::Value;

/// How the block's output should be handled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockOutputMode {
	/// Collect all statement results into a Vec<Value>.
	/// Used for top-level scripts where each statement's result is returned.
	Collect,
	/// Discard statement results, only return the final/control flow value.
	/// Used for FOR bodies, IF bodies, FUNCTION bodies.
	Discard,
}

impl Default for BlockOutputMode {
	fn default() -> Self {
		Self::Collect
	}
}

/// Result of executing a block.
#[derive(Debug, Clone)]
pub enum BlockResult {
	/// Block completed normally.
	/// - In Collect mode: contains Vec of all statement results
	/// - In Discard mode: contains last statement's value (or NONE)
	Completed(Vec<Value>),

	/// BREAK signal encountered (propagates from FOR loops).
	Break,

	/// CONTINUE signal encountered (propagates from FOR loops).
	Continue,

	/// RETURN signal with value.
	Return(Value),

	/// THROW signal with error value.
	Throw(Value),
}

impl BlockResult {
	/// Returns true if this is a control flow signal (not normal completion).
	pub fn is_control_signal(&self) -> bool {
		!matches!(self, Self::Completed(_))
	}

	/// Extract the value if this is a Completed result with exactly one value.
	pub fn into_single_value(self) -> Option<Value> {
		match self {
			Self::Completed(mut values) if values.len() == 1 => values.pop(),
			_ => None,
		}
	}
}

/// Classification of statements for dependency analysis.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatementClass {
	/// Pure read - can run in parallel with other reads.
	/// No dependencies except context_source.
	PureRead,

	/// Mutation (barrier) - must wait for all prior statements.
	/// Derived from plan.access_mode() == ReadWrite.
	Mutation,

	/// Context mutation - LET and USE statements.
	/// Subsequent statements depend on this for context.
	ContextMutation,

	/// Control signal - BREAK, CONTINUE, RETURN, THROW.
	/// Creates barrier, forces sequential execution after this point.
	ControlSignal,
}

/// A single operation that a statement can perform.
#[derive(Debug, Clone)]
pub enum StatementOperation {
	/// Execute an operator plan (SELECT, CREATE, UPDATE, DELETE, etc.)
	Operator(Arc<dyn OperatorPlan>),

	/// Bind a value to a parameter name.
	Let {
		name: String,
		value: LetValueSource,
	},

	/// Switch namespace/database context.
	Use {
		ns: Option<Arc<dyn PhysicalExpr>>,
		db: Option<Arc<dyn PhysicalExpr>>,
	},

	/// FOR loop iteration.
	For(Box<ForPlan>),

	/// IF/ELSE conditional.
	If(Box<IfPlan>),

	/// BREAK signal (exits FOR loop).
	Break,

	/// CONTINUE signal (skips to next FOR iteration).
	Continue,

	/// RETURN statement with value.
	Return(Arc<dyn PhysicalExpr>),

	/// THROW statement with error value.
	Throw(Arc<dyn PhysicalExpr>),

	/// SLEEP statement.
	Sleep(Duration),
}

/// Source of value for LET statements.
#[derive(Debug, Clone)]
pub enum LetValueSource {
	/// Scalar expression: `LET $x = 1 + 2`
	Scalar(Arc<dyn PhysicalExpr>),
	/// Query result: `LET $x = SELECT * FROM person`
	Query(Arc<dyn OperatorPlan>),
}

impl LetValueSource {
	pub fn access_mode(&self) -> AccessMode {
		match self {
			Self::Scalar(expr) => expr.access_mode(),
			Self::Query(plan) => plan.access_mode(),
		}
	}
}

/// A planned statement within a block.
#[derive(Debug, Clone)]
pub struct PlannedStatement {
	/// Unique identifier within the block.
	pub id: StatementId,

	/// Source of context for this statement.
	/// Points to the most recent context-mutating statement (LET, USE)
	/// or None if using the block's initial context.
	pub context_source: Option<StatementId>,

	/// Dependencies this statement must wait for before executing.
	/// Empty for first statement or statements with no dependencies.
	pub wait_for: Vec<StatementId>,

	/// The operation this statement performs.
	pub operation: StatementOperation,
}

impl PlannedStatement {
	/// Classify this statement for dependency analysis.
	pub fn classify(&self) -> StatementClass {
		match &self.operation {
			// Context mutations
			StatementOperation::Let {
				..
			}
			| StatementOperation::Use {
				..
			} => StatementClass::ContextMutation,

			// Control signals - always barriers
			StatementOperation::Break
			| StatementOperation::Continue
			| StatementOperation::Return(_)
			| StatementOperation::Throw(_) => StatementClass::ControlSignal,

			// Operators - check access mode
			StatementOperation::Operator(plan) => {
				if plan.is_read_only() {
					StatementClass::PureRead
				} else {
					StatementClass::Mutation
				}
			}

			// FOR/IF - check body access mode
			StatementOperation::For(for_plan) => {
				if for_plan.is_read_only() {
					StatementClass::PureRead
				} else {
					StatementClass::Mutation
				}
			}
			StatementOperation::If(if_plan) => {
				if if_plan.is_read_only() {
					StatementClass::PureRead
				} else {
					StatementClass::Mutation
				}
			}

			// SLEEP is read-only (side effect is just time delay)
			StatementOperation::Sleep(_) => StatementClass::PureRead,
		}
	}

	/// Get the access mode for this statement's operation.
	pub fn access_mode(&self) -> AccessMode {
		match &self.operation {
			StatementOperation::Operator(plan) => plan.access_mode(),
			StatementOperation::Let {
				value,
				..
			} => value.access_mode(),
			StatementOperation::Use {
				ns,
				db,
			} => {
				let mut mode = AccessMode::ReadOnly;
				if let Some(ns_expr) = ns {
					mode = mode.combine(ns_expr.access_mode());
				}
				if let Some(db_expr) = db {
					mode = mode.combine(db_expr.access_mode());
				}
				mode
			}
			StatementOperation::For(for_plan) => for_plan.access_mode(),
			StatementOperation::If(if_plan) => if_plan.access_mode(),
			StatementOperation::Break | StatementOperation::Continue => AccessMode::ReadOnly,
			StatementOperation::Return(expr) | StatementOperation::Throw(expr) => {
				expr.access_mode()
			}
			StatementOperation::Sleep(_) => AccessMode::ReadOnly,
		}
	}
}

/// A block of planned statements with resolved dependencies.
#[derive(Debug, Clone)]
pub struct BlockPlan {
	/// The statements in this block.
	pub statements: Vec<PlannedStatement>,

	/// How to handle output (Collect vs Discard).
	pub output_mode: BlockOutputMode,
}

impl BlockPlan {
	/// Create a new empty block plan.
	pub fn new(output_mode: BlockOutputMode) -> Self {
		Self {
			statements: Vec::new(),
			output_mode,
		}
	}

	/// Add a statement to the block.
	pub fn push(&mut self, statement: PlannedStatement) {
		self.statements.push(statement);
	}

	/// Get the combined access mode for this entire block.
	pub fn access_mode(&self) -> AccessMode {
		self.statements.iter().map(|s| s.access_mode()).combine_all()
	}

	/// Check if this block is read-only.
	pub fn is_read_only(&self) -> bool {
		self.access_mode() == AccessMode::ReadOnly
	}

	/// Calculate dependencies for all statements in the block.
	///
	/// This implements the dependency calculation algorithm from the spec:
	/// 1. Track last_context_source for LET/USE statements
	/// 2. Track last_barrier for mutations
	/// 3. Track statements_since_barrier for reads
	/// 4. Assign context_source and wait_for based on classification
	pub fn calculate_dependencies(&mut self) {
		let mut last_context_source: Option<StatementId> = None;
		let mut last_barrier: Option<StatementId> = None;
		let mut statements_since_barrier: Vec<StatementId> = Vec::new();

		for i in 0..self.statements.len() {
			let stmt = &self.statements[i];
			let class = stmt.classify();

			// Determine context_source
			let context_source = last_context_source;

			// Determine wait_for based on classification
			let wait_for = match class {
				StatementClass::PureRead => {
					// Wait only for last barrier (if any)
					last_barrier.into_iter().collect()
				}
				StatementClass::Mutation | StatementClass::ControlSignal => {
					// Wait for all statements since last barrier
					statements_since_barrier.clone()
				}
				StatementClass::ContextMutation => {
					// Wait for all statements since last barrier (like mutation)
					statements_since_barrier.clone()
				}
			};

			// Update tracking based on classification
			match class {
				StatementClass::PureRead => {
					statements_since_barrier.push(stmt.id);
				}
				StatementClass::Mutation | StatementClass::ControlSignal => {
					last_barrier = Some(stmt.id);
					statements_since_barrier.clear();
					statements_since_barrier.push(stmt.id);
				}
				StatementClass::ContextMutation => {
					last_context_source = Some(stmt.id);
					last_barrier = Some(stmt.id);
					statements_since_barrier.clear();
					statements_since_barrier.push(stmt.id);
				}
			}

			// Update the statement with calculated dependencies
			self.statements[i].context_source = context_source;
			self.statements[i].wait_for = wait_for;
		}
	}
}

/// FOR loop plan.
#[derive(Debug, Clone)]
pub struct ForPlan {
	/// Variable name to bind (without $).
	pub variable: String,

	/// Expression that produces the iterable.
	pub iterable: Arc<dyn PhysicalExpr>,

	/// Body to execute for each iteration.
	pub body: BlockPlan,
}

impl ForPlan {
	/// Get the access mode for this FOR loop.
	pub fn access_mode(&self) -> AccessMode {
		self.iterable.access_mode().combine(self.body.access_mode())
	}

	/// Check if this FOR loop is read-only.
	pub fn is_read_only(&self) -> bool {
		self.access_mode() == AccessMode::ReadOnly
	}
}

/// A single branch in an IF statement.
#[derive(Debug, Clone)]
pub struct IfBranch {
	/// Condition to evaluate (must be truthy to take this branch).
	pub condition: Arc<dyn PhysicalExpr>,

	/// Body to execute if condition is true.
	pub body: BlockPlan,
}

impl IfBranch {
	/// Get the access mode for this branch.
	pub fn access_mode(&self) -> AccessMode {
		self.condition.access_mode().combine(self.body.access_mode())
	}
}

/// IF/ELSE plan.
#[derive(Debug, Clone)]
pub struct IfPlan {
	/// Branches to evaluate in order (condition, body pairs).
	pub branches: Vec<IfBranch>,

	/// Optional ELSE branch (executed if no conditions match).
	pub else_branch: Option<BlockPlan>,
}

impl IfPlan {
	/// Get the access mode for this IF statement.
	pub fn access_mode(&self) -> AccessMode {
		let mut mode = self.branches.iter().map(|b| b.access_mode()).combine_all();
		if let Some(else_branch) = &self.else_branch {
			mode = mode.combine(else_branch.access_mode());
		}
		mode
	}

	/// Check if this IF statement is read-only.
	pub fn is_read_only(&self) -> bool {
		self.access_mode() == AccessMode::ReadOnly
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_block_output_mode_default() {
		assert_eq!(BlockOutputMode::default(), BlockOutputMode::Collect);
	}

	#[test]
	fn test_block_result_is_control_signal() {
		assert!(!BlockResult::Completed(vec![]).is_control_signal());
		assert!(BlockResult::Break.is_control_signal());
		assert!(BlockResult::Continue.is_control_signal());
		assert!(BlockResult::Return(Value::None).is_control_signal());
		assert!(BlockResult::Throw(Value::None).is_control_signal());
	}

	#[test]
	fn test_statement_class_from_sleep() {
		let stmt = PlannedStatement {
			id: StatementId(0),
			context_source: None,
			wait_for: vec![],
			operation: StatementOperation::Sleep(Duration::from_secs(1)),
		};
		assert_eq!(stmt.classify(), StatementClass::PureRead);
	}

	#[test]
	fn test_statement_class_from_break() {
		let stmt = PlannedStatement {
			id: StatementId(0),
			context_source: None,
			wait_for: vec![],
			operation: StatementOperation::Break,
		};
		assert_eq!(stmt.classify(), StatementClass::ControlSignal);
	}
}
