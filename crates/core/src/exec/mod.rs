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
pub(crate) mod union;

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
// Clone is implemented manually because #[derive(Clone)] doesn't work well
// with lifetime parameters when we just have references.
#[derive(Clone)]
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

/// Binary operation - left op right (e.g., age > 10)
#[derive(Debug, Clone)]
pub struct BinaryOp {
	pub(crate) left: Arc<dyn PhysicalExpr>,
	pub(crate) op: crate::expr::operator::BinaryOperator,
	pub(crate) right: Arc<dyn PhysicalExpr>,
}

#[async_trait]
impl PhysicalExpr for BinaryOp {
	async fn evaluate(&self, ctx: EvalContext<'_>) -> anyhow::Result<Value> {
		use crate::expr::operator::BinaryOperator;
		use crate::fnc::operate;

		// Evaluate both sides (could parallelize if both are independent)
		let left = self.left.evaluate(ctx.clone()).await?;
		let right = self.right.evaluate(ctx).await?;

		// Apply the operator
		match &self.op {
			BinaryOperator::Add => operate::add(left, right),
			BinaryOperator::Subtract => operate::sub(left, right),
			BinaryOperator::Multiply => operate::mul(left, right),
			BinaryOperator::Divide => operate::div(left, right),
			BinaryOperator::Remainder => operate::rem(left, right),
			BinaryOperator::Power => operate::pow(left, right),

			BinaryOperator::Equal => operate::equal(&left, &right),
			BinaryOperator::ExactEqual => operate::exact(&left, &right),
			BinaryOperator::NotEqual => operate::not_equal(&left, &right),
			BinaryOperator::AllEqual => operate::all_equal(&left, &right),
			BinaryOperator::AnyEqual => operate::any_equal(&left, &right),

			BinaryOperator::LessThan => operate::less_than(&left, &right),
			BinaryOperator::LessThanEqual => operate::less_than_or_equal(&left, &right),
			BinaryOperator::MoreThan => operate::more_than(&left, &right),
			BinaryOperator::MoreThanEqual => operate::more_than_or_equal(&left, &right),

			BinaryOperator::And => {
				// Short-circuit AND
				if !left.is_truthy() {
					Ok(left)
				} else {
					Ok(right)
				}
			}
			BinaryOperator::Or => {
				// Short-circuit OR
				if left.is_truthy() {
					Ok(left)
				} else {
					Ok(right)
				}
			}

			BinaryOperator::Contain => operate::contain(&left, &right),
			BinaryOperator::NotContain => operate::not_contain(&left, &right),
			BinaryOperator::ContainAll => operate::contain_all(&left, &right),
			BinaryOperator::ContainAny => operate::contain_any(&left, &right),
			BinaryOperator::ContainNone => operate::contain_none(&left, &right),
			BinaryOperator::Inside => operate::inside(&left, &right),
			BinaryOperator::NotInside => operate::not_inside(&left, &right),
			BinaryOperator::AllInside => operate::inside_all(&left, &right),
			BinaryOperator::AnyInside => operate::inside_any(&left, &right),
			BinaryOperator::NoneInside => operate::inside_none(&left, &right),

			BinaryOperator::Outside => operate::outside(&left, &right),
			BinaryOperator::Intersects => operate::intersects(&left, &right),

			BinaryOperator::NullCoalescing => {
				if !left.is_nullish() {
					Ok(left)
				} else {
					Ok(right)
				}
			}
			BinaryOperator::TenaryCondition => {
				// Same as OR for this context
				if left.is_truthy() {
					Ok(left)
				} else {
					Ok(right)
				}
			}

			// Range operators not typically used in WHERE clauses
			BinaryOperator::Range
			| BinaryOperator::RangeInclusive
			| BinaryOperator::RangeSkip
			| BinaryOperator::RangeSkipInclusive => {
				Err(anyhow::anyhow!("Range operators not yet supported in physical expressions"))
			}

			// Match operators require full-text search context
			BinaryOperator::Matches(_) => {
				Err(anyhow::anyhow!("MATCHES operator not yet supported in physical expressions"))
			}

			// Nearest neighbor requires vector index context
			BinaryOperator::NearestNeighbor(_) => {
				Err(anyhow::anyhow!("KNN operator not yet supported in physical expressions"))
			}
		}
	}

	fn references_current_value(&self) -> bool {
		self.left.references_current_value() || self.right.references_current_value()
	}
}

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

#[cfg(test)]
mod tests {
	use super::*;
	use crate::dbs::Session;
	use crate::expr::statements::SelectStatement;
	use crate::expr::{Fields, LogicalPlan, TopLevelExpr};
	use crate::kvs::Datastore;
	use crate::val::TableName;

	/// Helper to set up test data in an in-memory datastore
	async fn setup_test_data() -> Datastore {
		let ds = Datastore::new("memory").await.unwrap();
		let ses = Session::owner().with_ns("test").with_db("test");

		// Create test namespace and database, then insert test data
		let sql = r#"
			DEFINE NAMESPACE test;
			USE NS test;
			DEFINE DATABASE test;
			USE DB test;
			DEFINE TABLE users;
			INSERT INTO users [
				{ id: users:1, name: "Alice", age: 30 },
				{ id: users:2, name: "Bob", age: 25 },
				{ id: users:3, name: "Charlie", age: 35 }
			];
			DEFINE TABLE posts;
			INSERT INTO posts [
				{ id: posts:1, title: "First Post", author: users:1 },
				{ id: posts:2, title: "Second Post", author: users:2 }
			];
		"#;

		ds.execute(sql, &ses, None).await.expect("Failed to set up test data");
		ds
	}

	/// Test SELECT * FROM table (full table scan)
	#[tokio::test]
	async fn test_select_all_from_table() {
		let ds = setup_test_data().await;
		let ses = Session::owner().with_ns("test").with_db("test");

		// Create SELECT * FROM users
		let select_stmt = SelectStatement {
			what: vec![crate::expr::Expr::Table(TableName::from("users".to_string()))],
			expr: Fields::all(),
			..Default::default()
		};

		let plan = LogicalPlan {
			expressions: vec![TopLevelExpr::Expr(crate::expr::Expr::Select(Box::new(select_stmt)))],
		};

		// Execute through the normal path
		let results = ds.process_plan(plan, &ses, None).await.expect("Query failed");

		assert_eq!(results.len(), 1);
		let result = &results[0];
		assert!(result.result.is_ok());

		// Check we got 3 users
		if let Ok(value) = &result.result {
			let value: Value = value.clone().into();
			if let Value::Array(arr) = value {
				assert_eq!(arr.len(), 3, "Expected 3 users, got {}", arr.len());
			} else {
				panic!("Expected Array result, got {:?}", value);
			}
		}
	}

	/// Test SELECT * FROM table:id (record ID lookup)
	#[tokio::test]
	async fn test_select_from_record_id() {
		let ds = setup_test_data().await;
		let ses = Session::owner().with_ns("test").with_db("test");

		// Create SELECT * FROM users:1
		let record_id_lit = crate::expr::RecordIdLit {
			table: TableName::from("users".to_string()),
			key: crate::expr::record_id::RecordIdKeyLit::Number(1),
		};

		let select_stmt = SelectStatement {
			what: vec![crate::expr::Expr::Literal(crate::expr::literal::Literal::RecordId(
				record_id_lit,
			))],
			expr: Fields::all(),
			..Default::default()
		};

		let plan = LogicalPlan {
			expressions: vec![TopLevelExpr::Expr(crate::expr::Expr::Select(Box::new(select_stmt)))],
		};

		// Execute through the normal path
		let results = ds.process_plan(plan, &ses, None).await.expect("Query failed");

		assert_eq!(results.len(), 1);
		let result = &results[0];
		assert!(result.result.is_ok());

		// Check we got exactly 1 user (Alice)
		if let Ok(value) = &result.result {
			let value: Value = value.clone().into();
			if let Value::Array(arr) = value {
				assert_eq!(arr.len(), 1, "Expected 1 user, got {}", arr.len());
				// Verify it's Alice
				if let Value::Object(obj) = &arr[0] {
					assert_eq!(
						obj.get("name"),
						Some(&Value::String("Alice".to_string())),
						"Expected Alice"
					);
				}
			} else {
				panic!("Expected Array result, got {:?}", value);
			}
		}
	}

	/// Test SELECT * FROM table WHERE field > value (scan with filter)
	#[tokio::test]
	async fn test_select_with_where_clause() {
		let ds = setup_test_data().await;
		let ses = Session::owner().with_ns("test").with_db("test");

		// Create SELECT * FROM users WHERE age > 28
		// The condition: age > 28
		let cond = crate::expr::Cond(crate::expr::Expr::Binary {
			left: Box::new(crate::expr::Expr::Idiom(crate::expr::Idiom(vec![
				crate::expr::part::Part::Field("age".to_string()),
			]))),
			op: crate::expr::operator::BinaryOperator::MoreThan,
			right: Box::new(crate::expr::Expr::Literal(crate::expr::literal::Literal::Integer(28))),
		});

		let select_stmt = SelectStatement {
			what: vec![crate::expr::Expr::Table(TableName::from("users".to_string()))],
			expr: Fields::all(),
			cond: Some(cond),
			..Default::default()
		};

		let plan = LogicalPlan {
			expressions: vec![TopLevelExpr::Expr(crate::expr::Expr::Select(Box::new(select_stmt)))],
		};

		// Execute through the normal path
		let results = ds.process_plan(plan, &ses, None).await.expect("Query failed");

		assert_eq!(results.len(), 1);
		let result = &results[0];
		assert!(result.result.is_ok(), "Query failed: {:?}", result.result);

		// Check we got 2 users (Alice age 30, Charlie age 35)
		if let Ok(value) = &result.result {
			let value: Value = value.clone().into();
			if let Value::Array(arr) = value {
				assert_eq!(arr.len(), 2, "Expected 2 users with age > 28, got {}", arr.len());
			} else {
				panic!("Expected Array result, got {:?}", value);
			}
		}
	}

	/// Test SELECT * FROM a, b (union of multiple tables)
	#[tokio::test]
	async fn test_select_from_multiple_tables() {
		let ds = setup_test_data().await;
		let ses = Session::owner().with_ns("test").with_db("test");

		// Create SELECT * FROM users, posts
		let select_stmt = SelectStatement {
			what: vec![
				crate::expr::Expr::Table(TableName::from("users".to_string())),
				crate::expr::Expr::Table(TableName::from("posts".to_string())),
			],
			expr: Fields::all(),
			..Default::default()
		};

		let plan = LogicalPlan {
			expressions: vec![TopLevelExpr::Expr(crate::expr::Expr::Select(Box::new(select_stmt)))],
		};

		// Execute through the normal path
		let results = ds.process_plan(plan, &ses, None).await.expect("Query failed");

		assert_eq!(results.len(), 1);
		let result = &results[0];
		assert!(result.result.is_ok(), "Query failed: {:?}", result.result);

		// Check we got 5 records total (3 users + 2 posts)
		if let Ok(value) = &result.result {
			let value: Value = value.clone().into();
			if let Value::Array(arr) = value {
				assert_eq!(
					arr.len(),
					5,
					"Expected 5 records (3 users + 2 posts), got {}",
					arr.len()
				);
			} else {
				panic!("Expected Array result, got {:?}", value);
			}
		}
	}

	/// Test the planner directly to verify Union is created for multiple sources
	#[test]
	fn test_planner_creates_union_for_multiple_sources() {
		use crate::exec::planner::logical_plan_to_execution_plan;

		let select_stmt = SelectStatement {
			what: vec![
				crate::expr::Expr::Table(TableName::from("a".to_string())),
				crate::expr::Expr::Table(TableName::from("b".to_string())),
				crate::expr::Expr::Table(TableName::from("c".to_string())),
			],
			expr: Fields::all(),
			..Default::default()
		};

		let plan = LogicalPlan {
			expressions: vec![TopLevelExpr::Expr(crate::expr::Expr::Select(Box::new(select_stmt)))],
		};

		let result = logical_plan_to_execution_plan(plan);
		assert!(result.is_ok(), "Planning failed: {:?}", result.err());

		let planned = result.unwrap();
		assert_eq!(planned.len(), 1);

		// Verify the first statement is a Query with a Union at the root
		if let PlannedStatement::Query(exec_plan) = &planned[0] {
			// The plan should be a Union with 3 children
			assert_eq!(exec_plan.children().len(), 3, "Union should have 3 children");
		} else {
			panic!("Expected PlannedStatement::Query");
		}
	}

	/// Test the planner with a single source (no Union created)
	#[test]
	fn test_planner_no_union_for_single_source() {
		use crate::exec::planner::logical_plan_to_execution_plan;

		let select_stmt = SelectStatement {
			what: vec![crate::expr::Expr::Table(TableName::from("users".to_string()))],
			expr: Fields::all(),
			..Default::default()
		};

		let plan = LogicalPlan {
			expressions: vec![TopLevelExpr::Expr(crate::expr::Expr::Select(Box::new(select_stmt)))],
		};

		let result = logical_plan_to_execution_plan(plan);
		assert!(result.is_ok(), "Planning failed: {:?}", result.err());

		let planned = result.unwrap();
		assert_eq!(planned.len(), 1);

		// Verify the first statement is a Query with a Scan (not Union)
		if let PlannedStatement::Query(exec_plan) = &planned[0] {
			// A single source should not have children (it's a Scan, not Union)
			assert_eq!(
				exec_plan.children().len(),
				0,
				"Single source should produce Scan, not Union"
			);
		} else {
			panic!("Expected PlannedStatement::Query");
		}
	}
}
