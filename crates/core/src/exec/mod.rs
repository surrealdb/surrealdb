use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use futures::Stream;

use crate::err::Error;
use crate::expr::{FlowResult, Idiom};
use crate::kvs::Transaction;
use crate::val::Value;

pub(crate) mod context;
pub(crate) mod filter;
pub(crate) mod lookup;
pub(crate) mod planner;
pub(crate) mod scan;

// Re-export context types
pub(crate) use context::{
	ContextLevel, DatabaseContext, ExecutionContext, NamespaceContext, Parameters, RootContext,
};

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
pub(crate) struct ValueBatch {
	pub(crate) values: Vec<Value>,
}

pub type ValueBatchStream = Pin<Box<dyn Stream<Item = FlowResult<ValueBatch>> + Send>>;

/// A trait for execution plans that can be executed and produce a stream of value batches.
///
/// Execution plans form a tree structure where each node declares its minimum required
/// context level via `required_context()`. The executor validates that the current session
/// meets these requirements before execution begins.
pub(crate) trait ExecutionPlan: Debug + Send + Sync {
	/// The minimum context level required to execute this plan.
	///
	/// Used for pre-flight validation: the executor checks that the current session
	/// has at least this context level before calling `execute()`.
	fn required_context(&self) -> ContextLevel;

	/// Executes the execution plan and returns a stream of value batches.
	///
	/// The context is guaranteed to meet the requirements declared by `required_context()`
	/// if the executor performs proper validation.
	fn execute(&self, ctx: &ExecutionContext) -> Result<ValueBatchStream, Error>;

	/// Returns references to child execution plans for tree traversal.
	///
	/// Used for:
	/// - Pre-flight validation (recursive context requirement checking)
	/// - Query optimization
	/// - EXPLAIN output
	fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
		vec![]
	}
}

#[derive(Debug, Clone)]
pub(crate) enum PlannedStatement {
	Query(Arc<dyn ExecutionPlan>),
	SessionCommand(SessionCommand),
}

#[derive(Debug, Clone)]
pub(crate) enum SessionCommand {
	Use {
		ns: Option<Arc<dyn PhysicalExpr>>,
		db: Option<Arc<dyn PhysicalExpr>>,
	},
	Begin,
	Commit,
	Cancel,
}

// pub(crate) struct Ident(String);

// pub(crate) trait IdentProvider: ExecutionPlan {
// 	fn evaluate(&self) -> Result<Pin<Box<Ident>>, Error> {
// 		todo!("STU")
// 	}
// }

// pub(crate) trait ContextResolver: Debug + Send + Sync {
// 	fn resolve(&self, ctx: &Context) -> Result<Pin<Box<Context>>, Error>;
// }

// #[derive(Debug, Clone)]
// struct UseResolver {
// 	ns: Option<Arc<dyn IdentProvider>>,
// 	db: Option<Arc<dyn IdentProvider>>,
// }

// impl ContextResolver for UseResolver {
// 	fn resolve(&self, ctx: &Context) -> Result<Pin<Box<Context>>, Error> {
// 		todo!("STU")
// 	}
// }

/// Evaluation context - what's available during expression evaluation.
///
/// This is a borrowed view into the execution context for expression evaluation.
/// It provides access to parameters, namespace/database names, and the current row
/// (for per-row expressions like filters and projections).
pub struct EvalContext<'a> {
	pub params: &'a Parameters,
	pub ns: Option<&'a str>,
	pub db: Option<&'a str>,
	pub txn: Option<&'a Transaction>,

	/// Current row for per-row expressions (projections, filters).
	/// None when evaluating in "scalar context" (USE, LIMIT, TIMEOUT, etc.)
	pub current_value: Option<&'a Value>,
}

impl<'a> EvalContext<'a> {
	/// Convert from ExecutionContext enum for expression evaluation.
	///
	/// This extracts the appropriate fields based on the context level:
	/// - Root: params only, no ns/db/txn
	/// - Namespace: params, ns, txn
	/// - Database: params, ns, db, txn
	pub(crate) fn from_exec_ctx(exec_ctx: &'a ExecutionContext) -> Self {
		match exec_ctx {
			ExecutionContext::Root(r) => Self::scalar(&r.params, None, None, None),
			ExecutionContext::Namespace(n) => {
				Self::scalar(&n.root.params, Some(&n.ns.name), None, Some(&n.txn))
			}
			ExecutionContext::Database(d) => Self::scalar(
				&d.ns_ctx.root.params,
				Some(&d.ns_ctx.ns.name),
				Some(&d.db.name),
				Some(&d.ns_ctx.txn),
			),
		}
	}

	/// For session-level scalar evaluation (USE, LIMIT, etc.)
	pub fn scalar(
		params: &'a Parameters,
		ns: Option<&'a str>,
		db: Option<&'a str>,
		txn: Option<&'a Transaction>,
	) -> Self {
		Self {
			params,
			ns,
			db,
			txn,
			current_value: None,
		}
	}

	/// For per-row evaluation (projections, filters)
	pub fn with_value(&self, value: &'a Value) -> Self {
		Self {
			current_value: Some(value),
			..*self
		}
	}
}

#[async_trait]
pub trait PhysicalExpr: Send + Sync + Debug {
	/// Evaluate this expression to a value.
	///
	/// May execute subqueries internally, hence async.
	async fn evaluate(&self, ctx: EvalContext<'_>) -> anyhow::Result<Value>;

	/// Does this expression reference the current row?
	/// If false, can be evaluated in scalar context.
	fn references_current_value(&self) -> bool;
}

/// Literal value - "foo", 42, true
#[derive(Debug, Clone)]
pub struct Literal(pub(crate) Value);

#[async_trait]
impl PhysicalExpr for Literal {
	async fn evaluate(&self, _ctx: EvalContext<'_>) -> anyhow::Result<Value> {
		Ok(self.0.clone())
	}

	fn references_current_value(&self) -> bool {
		false
	}
}

/// Parameter reference - $foo
#[derive(Debug, Clone)]
pub struct Param(pub(crate) String);

#[async_trait]
impl PhysicalExpr for Param {
	async fn evaluate(&self, ctx: EvalContext<'_>) -> anyhow::Result<Value> {
		ctx.params
			.get(self.0.as_str())
			.map(|v| (**v).clone())
			.ok_or_else(|| anyhow::anyhow!("Parameter not found: ${}", self.0))
	}

	fn references_current_value(&self) -> bool {
		false
	}
}

/// Field access - foo.bar.baz or just `age`
#[derive(Debug, Clone)]
pub struct Field(pub(crate) Idiom);

#[async_trait]
impl PhysicalExpr for Field {
	async fn evaluate(&self, ctx: EvalContext<'_>) -> anyhow::Result<Value> {
		let current = ctx
			.current_value
			.ok_or_else(|| anyhow::anyhow!("Field access requires current value"))?;

		// Simple synchronous field access - handles basic idioms
		// This does NOT use the old compute() system
		Ok(get_field_simple(current, &self.0.0))
	}

	fn references_current_value(&self) -> bool {
		true
	}
}

/// Simple field access without async/compute machinery
/// Handles basic field paths like foo.bar.baz
fn get_field_simple(value: &Value, path: &[crate::expr::part::Part]) -> Value {
	use crate::expr::part::Part;

	if path.is_empty() {
		return value.clone();
	}

	match (value, &path[0]) {
		// Field access on object
		(Value::Object(obj), Part::Field(field_name)) => match obj.get(field_name.as_str()) {
			Some(v) => get_field_simple(v, &path[1..]),
			None => Value::None,
		},
		// Index access on array
		(Value::Array(arr), part) => {
			if let Some(idx) = part.as_old_index() {
				match arr.0.get(idx) {
					Some(v) => get_field_simple(v, &path[1..]),
					None => Value::None,
				}
			} else {
				// For other array operations, return None for now
				Value::None
			}
		}
		// Start part - evaluate the expression (simplified)
		(_, Part::Start(_expr)) => {
			// For simple cases, we can't evaluate expressions here
			// This would require the full compute machinery
			// For now, return None - this will need to be extended later
			Value::None
		}
		// For any other combination, return None
		_ => Value::None,
	}
}

// #[derive(Debug, Clone)]
// pub struct BinaryOp {
//     left: Arc<dyn PhysicalExpr>,
//     op: Operator,
//     right: Arc<dyn PhysicalExpr>,
// }

// #[async_trait]
// impl PhysicalExpr for BinaryOp {
//     async fn evaluate(&self, ctx: &EvalContext<'_>) -> Result<Value> {
//         // Could parallelize these if both are independent
//         let (left, right) = tokio::try_join!(
//             self.left.evaluate(ctx),
//             self.right.evaluate(ctx),
//         )?;
//         self.op.apply(left, right)
//     }

//     fn references_current_value(&self) -> bool {
//         self.left.references_current_value() || self.right.references_current_value()
//     }
// }

/// Scalar subquery - (SELECT ... LIMIT 1)
#[derive(Debug, Clone)]
pub struct ScalarSubquery {
	pub(crate) plan: Arc<dyn ExecutionPlan>,
}

#[async_trait]
impl PhysicalExpr for ScalarSubquery {
	async fn evaluate(&self, _ctx: EvalContext<'_>) -> anyhow::Result<Value> {
		// TODO: Implement scalar subquery evaluation
		// This requires bridging EvalContext (which has borrowed &Transaction)
		// with ExecutionContext (which needs Arc<Transaction>).
		// Options:
		// 1. Store Arc<Transaction> in EvalContext
		// 2. Add a method to create ExecutionContext from borrowed context
		// 3. Make ExecutionContext work with borrowed Transaction (but this conflicts with 'static
		//    stream requirement)

		Err(anyhow::anyhow!(
			"ScalarSubquery evaluation not yet fully implemented - need Arc<Transaction> in EvalContext"
		))
	}

	fn references_current_value(&self) -> bool {
		// For now, assume subqueries don't reference current value
		// TODO: Track if plan references outer scope for correlated subqueries
		false
	}
}
