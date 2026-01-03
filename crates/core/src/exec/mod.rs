use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;

use futures::Stream;

use crate::err::Error;
use crate::expr::FlowResult;
use crate::val::Value;

pub(crate) mod completion_map;
pub(crate) mod context;
pub(crate) mod explain;
pub(crate) mod operators;
pub(crate) mod permission;
pub(crate) mod physical_expr;
pub(crate) mod planner;
pub(crate) mod script_executor;
pub(crate) mod statement;

// Re-export context types
pub(crate) use context::{
	ContextLevel, DatabaseContext, ExecutionContext, NamespaceContext, Parameters, RootContext,
};
pub(crate) use physical_expr::{EvalContext, PhysicalExpr};

// Re-export statement types
pub(crate) use completion_map::{CompletionError, CompletionMap};
pub(crate) use script_executor::{ScriptExecutionError, ScriptExecutor};
pub(crate) use statement::{
	ScriptPlan, StatementContent, StatementId, StatementKind, StatementLetValue, StatementOutput,
	StatementPlan,
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
pub(crate) trait OperatorPlan: Debug + Send + Sync {
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
	fn execute(&self, ctx: &ExecutionContext) -> Result<ValueBatchStream, Error>;

	/// Returns references to child execution plans for tree traversal.
	///
	/// Used for:
	/// - Pre-flight validation (recursive context requirement checking)
	/// - Query optimization
	/// - EXPLAIN output
	fn children(&self) -> Vec<&Arc<dyn OperatorPlan>> {
		vec![]
	}
}

/// The value bound by a LET statement
#[derive(Debug, Clone)]
pub(crate) enum LetValue {
	/// Scalar expression - evaluates to exactly one Value
	Scalar(Arc<dyn PhysicalExpr>),
	/// Query - stream is collected into Value::Array
	Query(Arc<dyn OperatorPlan>),
}

#[derive(Debug, Clone)]
pub(crate) enum PlannedStatement {
	Query(Arc<dyn OperatorPlan>),
	SessionCommand(SessionCommand),
	/// Bind expression result to a parameter
	Let {
		name: String,
		value: LetValue,
	},
	/// A scalar expression evaluated as a top-level statement (e.g., `1 + 1;`, `$param;`)
	Scalar(Arc<dyn PhysicalExpr>),
	/// EXPLAIN statement - shows execution plan instead of executing
	Explain {
		format: crate::expr::ExplainFormat,
		statement: Box<PlannedStatement>,
	},
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

#[cfg(test)]
mod tests {
	use super::*;
	use crate::dbs::Session;
	use crate::expr::statements::SelectStatement;
	use crate::expr::{Fields, LogicalPlan, TopLevelExpr};
	use crate::kvs::Datastore;
	use crate::types::{PublicNumber, PublicObject, PublicValue};
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
		let ses = Session::owner().with_ns("test").with_db("test").require_new_planner();

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
			if let PublicValue::Array(arr) = value {
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
		let ses = Session::owner().with_ns("test").with_db("test").require_new_planner();

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
			if let PublicValue::Array(arr) = value {
				assert_eq!(arr.len(), 1, "Expected 1 user, got {}", arr.len());
				// Verify it's Alice
				if let PublicValue::Object(obj) = &arr[0] {
					assert_eq!(
						obj.get("name"),
						Some(&PublicValue::String("Alice".to_string())),
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
		let ses = Session::owner().with_ns("test").with_db("test").require_new_planner();

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
			if let PublicValue::Array(arr) = value {
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
		let ses = Session::owner().with_ns("test").with_db("test").require_new_planner();

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
			if let PublicValue::Array(arr) = value {
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

		let result = logical_plan_to_execution_plan(&plan);
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

		let result = logical_plan_to_execution_plan(&plan);
		assert!(result.is_ok(), "Planning failed: {:?}", result.err());

		let planned = result.unwrap();
		assert_eq!(planned.len(), 1);

		// Verify the first statement is a Query with ComputeFields->Scan (not Union)
		if let PlannedStatement::Query(exec_plan) = &planned[0] {
			// A single source should have 1 child (ComputeFields wraps Scan),
			// not 3+ children which would indicate a Union
			assert!(
				exec_plan.children().len() <= 1,
				"Single source should not produce Union (expected <= 1 child, got {})",
				exec_plan.children().len()
			);
		} else {
			panic!("Expected PlannedStatement::Query");
		}
	}

	// =========================================================================
	// Permission Tests
	// =========================================================================

	/// Helper to set up test data with table permissions
	async fn setup_test_data_with_permissions() -> Datastore {
		let ds = Datastore::new("memory").await.unwrap();
		let ses = Session::owner().with_ns("test").with_db("test");

		// Create test namespace, database, and tables with permissions
		let sql = r#"
			DEFINE NAMESPACE test;
			USE NS test;
			DEFINE DATABASE test;
			USE DB test;
			
			-- Table with FULL permissions (explicit)
			DEFINE TABLE public_data PERMISSIONS FULL;
			INSERT INTO public_data [
				{ id: public_data:1, value: "public1" },
				{ id: public_data:2, value: "public2" }
			];
			
			-- Table with NONE permissions for select
			DEFINE TABLE private_data PERMISSIONS FOR select NONE;
			INSERT INTO private_data [
				{ id: private_data:1, secret: "classified" }
			];
			
			-- Table with conditional SELECT permission (WHERE id = $auth.id)
			DEFINE TABLE user_data PERMISSIONS FOR select WHERE id = $auth;
			INSERT INTO user_data [
				{ id: user_data:alice, owner: "alice", data: "alice's data" },
				{ id: user_data:bob, owner: "bob", data: "bob's data" }
			];
		"#;

		ds.execute(sql, &ses, None).await.expect("Failed to set up test data with permissions");
		ds
	}

	/// Test that owner role bypasses all table permissions
	#[tokio::test]
	async fn test_select_owner_bypasses_permissions() {
		let ds = setup_test_data_with_permissions().await;
		let ses = Session::owner().with_ns("test").with_db("test").require_new_planner();

		// Create SELECT * FROM private_data (which has PERMISSIONS NONE)
		let select_stmt = SelectStatement {
			what: vec![crate::expr::Expr::Table(TableName::from("private_data".to_string()))],
			expr: Fields::all(),
			..Default::default()
		};

		let plan = LogicalPlan {
			expressions: vec![TopLevelExpr::Expr(crate::expr::Expr::Select(Box::new(select_stmt)))],
		};

		// Execute as owner - should bypass permissions and see all data
		let results = ds.process_plan(plan, &ses, None).await.expect("Query failed");

		assert_eq!(results.len(), 1);
		let result = &results[0];
		assert!(result.result.is_ok(), "Query failed: {:?}", result.result);

		// Owner should see the private data (1 record)
		if let Ok(value) = &result.result {
			if let PublicValue::Array(arr) = value {
				assert_eq!(arr.len(), 1, "Owner should see 1 private record, got {}", arr.len());
			} else {
				panic!("Expected Array result, got {:?}", value);
			}
		}
	}

	/// Test that PERMISSIONS NONE returns empty results for non-owner users
	#[tokio::test]
	async fn test_select_permission_none_returns_empty() {
		let ds = setup_test_data_with_permissions().await;

		// Create a record user session
		let rid = PublicValue::Object(PublicObject::from_iter([(
			"id".to_string(),
			PublicValue::String("user:test".to_string()),
		)]));
		let ses = Session::for_record("test", "test", "user", rid).require_new_planner();

		// Create SELECT * FROM private_data (which has PERMISSIONS NONE)
		let select_stmt = SelectStatement {
			what: vec![crate::expr::Expr::Table(TableName::from("private_data".to_string()))],
			expr: Fields::all(),
			..Default::default()
		};

		let plan = LogicalPlan {
			expressions: vec![TopLevelExpr::Expr(crate::expr::Expr::Select(Box::new(select_stmt)))],
		};

		// Execute as record user - should get empty results due to PERMISSIONS NONE
		let results = ds.process_plan(plan, &ses, None).await.expect("Query failed");

		assert_eq!(results.len(), 1);
		let result = &results[0];
		assert!(result.result.is_ok(), "Query failed: {:?}", result.result);

		// Record user should see no private data
		if let Ok(value) = &result.result {
			if let PublicValue::Array(arr) = value {
				assert_eq!(
					arr.len(),
					0,
					"Record user should see 0 records with PERMISSIONS NONE, got {}",
					arr.len()
				);
			} else {
				panic!("Expected Array result, got {:?}", value);
			}
		}
	}

	/// Test that public table (FULL permissions) is accessible to record users
	#[tokio::test]
	async fn test_select_permission_full_allows_access() {
		let ds = setup_test_data_with_permissions().await;

		// Create a record user session
		let rid = PublicValue::Object(PublicObject::from_iter([(
			"id".to_string(),
			PublicValue::String("user:test".to_string()),
		)]));
		let ses = Session::for_record("test", "test", "user", rid).require_new_planner();

		// Create SELECT * FROM public_data (which has default FULL permissions)
		let select_stmt = SelectStatement {
			what: vec![crate::expr::Expr::Table(TableName::from("public_data".to_string()))],
			expr: Fields::all(),
			..Default::default()
		};

		let plan = LogicalPlan {
			expressions: vec![TopLevelExpr::Expr(crate::expr::Expr::Select(Box::new(select_stmt)))],
		};

		// Execute as record user - should see all public data
		let results = ds.process_plan(plan, &ses, None).await.expect("Query failed");

		assert_eq!(results.len(), 1);
		let result = &results[0];
		assert!(result.result.is_ok(), "Query failed: {:?}", result.result);

		// Record user should see all public data (2 records)
		if let Ok(value) = &result.result {
			if let PublicValue::Array(arr) = value {
				assert_eq!(
					arr.len(),
					2,
					"Record user should see 2 public records, got {}",
					arr.len()
				);
			} else {
				panic!("Expected Array result, got {:?}", value);
			}
		}
	}

	/// Test that schemaless (undefined) tables are denied for record users
	#[tokio::test]
	async fn test_select_schemaless_table_denied_for_record_user() {
		let ds = setup_test_data_with_permissions().await;

		// Create a record user session
		let rid = PublicValue::Object(PublicObject::from_iter([(
			"id".to_string(),
			PublicValue::String("user:test".to_string()),
		)]));
		let ses = Session::for_record("test", "test", "user", rid).require_new_planner();

		// Create SELECT * FROM undefined_table (table doesn't exist - schemaless)
		let select_stmt = SelectStatement {
			what: vec![crate::expr::Expr::Table(TableName::from("undefined_table".to_string()))],
			expr: Fields::all(),
			..Default::default()
		};

		let plan = LogicalPlan {
			expressions: vec![TopLevelExpr::Expr(crate::expr::Expr::Select(Box::new(select_stmt)))],
		};

		// Execute as record user - should get empty results (undefined table = NONE permission)
		let results = ds.process_plan(plan, &ses, None).await.expect("Query failed");

		assert_eq!(results.len(), 1);
		let result = &results[0];
		assert!(result.result.is_ok(), "Query failed: {:?}", result.result);

		// Record user should see no data from undefined table
		if let Ok(value) = &result.result {
			if let PublicValue::Array(arr) = value {
				assert_eq!(
					arr.len(),
					0,
					"Record user should see 0 records from undefined table, got {}",
					arr.len()
				);
			} else {
				panic!("Expected Array result, got {:?}", value);
			}
		}
	}

	/// Test that owner can access schemaless (undefined) tables
	#[tokio::test]
	async fn test_select_schemaless_table_allowed_for_owner() {
		let ds = setup_test_data_with_permissions().await;
		let ses = Session::owner().with_ns("test").with_db("test").require_new_planner();

		// Create SELECT * FROM undefined_table (table doesn't exist - schemaless)
		let select_stmt = SelectStatement {
			what: vec![crate::expr::Expr::Table(TableName::from("undefined_table".to_string()))],
			expr: Fields::all(),
			..Default::default()
		};

		let plan = LogicalPlan {
			expressions: vec![TopLevelExpr::Expr(crate::expr::Expr::Select(Box::new(select_stmt)))],
		};

		// Execute as owner - should succeed (even though table is empty/undefined)
		let results = ds.process_plan(plan, &ses, None).await.expect("Query failed");

		assert_eq!(results.len(), 1);
		let result = &results[0];
		// Owner bypasses permissions, so query should succeed (empty result is fine)
		assert!(result.result.is_ok(), "Query failed: {:?}", result.result);
	}

	// =========================================================================
	// Scalar Expression Tests
	// =========================================================================

	/// Test executing a literal integer as a top-level statement
	#[tokio::test]
	async fn test_scalar_literal_integer() {
		let ds = Datastore::new("memory").await.unwrap();
		let ses = Session::owner().require_new_planner();

		// Expression: 42
		let plan = LogicalPlan {
			expressions: vec![TopLevelExpr::Expr(crate::expr::Expr::Literal(
				crate::expr::literal::Literal::Integer(42),
			))],
		};

		let results = ds.process_plan(plan, &ses, None).await.expect("Query failed");

		assert_eq!(results.len(), 1);
		let result = &results[0];
		assert!(result.result.is_ok(), "Query failed: {:?}", result.result);

		if let Ok(value) = &result.result {
			assert_eq!(value, &PublicValue::Number(PublicNumber::Int(42)));
		}
	}

	/// Test executing a literal string as a top-level statement
	#[tokio::test]
	async fn test_scalar_literal_string() {
		let ds = Datastore::new("memory").await.unwrap();
		let ses = Session::owner().require_new_planner();

		// Expression: "hello"
		let plan = LogicalPlan {
			expressions: vec![TopLevelExpr::Expr(crate::expr::Expr::Literal(
				crate::expr::literal::Literal::String("hello".to_string()),
			))],
		};

		let results = ds.process_plan(plan, &ses, None).await.expect("Query failed");

		assert_eq!(results.len(), 1);
		let result = &results[0];
		assert!(result.result.is_ok(), "Query failed: {:?}", result.result);

		if let Ok(value) = &result.result {
			assert_eq!(value, PublicValue::String("hello".to_string()));
		}
	}

	/// Test executing a binary expression (1 + 2) as a top-level statement
	#[tokio::test]
	async fn test_scalar_binary_expression() {
		let ds = Datastore::new("memory").await.unwrap();
		let ses = Session::owner().require_new_planner();

		// Expression: 1 + 2
		let plan = LogicalPlan {
			expressions: vec![TopLevelExpr::Expr(crate::expr::Expr::Binary {
				left: Box::new(crate::expr::Expr::Literal(crate::expr::literal::Literal::Integer(
					1,
				))),
				op: crate::expr::operator::BinaryOperator::Add,
				right: Box::new(crate::expr::Expr::Literal(
					crate::expr::literal::Literal::Integer(2),
				)),
			})],
		};

		let results = ds.process_plan(plan, &ses, None).await.expect("Query failed");

		assert_eq!(results.len(), 1);
		let result = &results[0];
		assert!(result.result.is_ok(), "Query failed: {:?}", result.result);

		if let Ok(value) = &result.result {
			assert_eq!(value, &PublicValue::Number(PublicNumber::Int(3)));
		}
	}

	/// Test executing a prefix/unary expression (-5) as a top-level statement
	#[tokio::test]
	async fn test_scalar_prefix_negate() {
		let ds = Datastore::new("memory").await.unwrap();
		let ses = Session::owner().require_new_planner();

		// Expression: -5
		let plan = LogicalPlan {
			expressions: vec![TopLevelExpr::Expr(crate::expr::Expr::Prefix {
				op: crate::expr::operator::PrefixOperator::Negate,
				expr: Box::new(crate::expr::Expr::Literal(crate::expr::literal::Literal::Integer(
					5,
				))),
			})],
		};

		let results = ds.process_plan(plan, &ses, None).await.expect("Query failed");

		assert_eq!(results.len(), 1);
		let result = &results[0];
		assert!(result.result.is_ok(), "Query failed: {:?}", result.result);

		if let Ok(value) = &result.result {
			assert_eq!(value, &PublicValue::Number(PublicNumber::Int(-5)));
		}
	}

	/// Test executing a prefix/unary NOT expression (!true) as a top-level statement
	#[tokio::test]
	async fn test_scalar_prefix_not() {
		let ds = Datastore::new("memory").await.unwrap();
		let ses = Session::owner().require_new_planner();

		// Expression: !true
		let plan = LogicalPlan {
			expressions: vec![TopLevelExpr::Expr(crate::expr::Expr::Prefix {
				op: crate::expr::operator::PrefixOperator::Not,
				expr: Box::new(crate::expr::Expr::Literal(crate::expr::literal::Literal::Bool(
					true,
				))),
			})],
		};

		let results = ds.process_plan(plan, &ses, None).await.expect("Query failed");

		assert_eq!(results.len(), 1);
		let result = &results[0];
		assert!(result.result.is_ok(), "Query failed: {:?}", result.result);

		if let Ok(value) = &result.result {
			assert_eq!(value, &PublicValue::Bool(false));
		}
	}

	/// Test executing a constant expression (MATH::PI) as a top-level statement
	#[tokio::test]
	async fn test_scalar_constant_math_pi() {
		let ds = Datastore::new("memory").await.unwrap();
		let ses = Session::owner().require_new_planner();

		// Expression: MATH::PI
		let plan = LogicalPlan {
			expressions: vec![TopLevelExpr::Expr(crate::expr::Expr::Constant(
				crate::expr::Constant::MathPi,
			))],
		};

		let results = ds.process_plan(plan, &ses, None).await.expect("Query failed");

		assert_eq!(results.len(), 1);
		let result = &results[0];
		assert!(result.result.is_ok(), "Query failed: {:?}", result.result);

		if let Ok(value) = &result.result {
			if let PublicValue::Number(PublicNumber::Float(f)) = value {
				assert!((f - std::f64::consts::PI).abs() < 0.0001, "Expected PI, got {}", f);
			} else {
				panic!("Expected Float result, got {:?}", value);
			}
		}
	}

	/// Test that idiom expressions (field access) fail without a FROM clause
	#[test]
	fn test_scalar_idiom_requires_table() {
		use crate::exec::planner::logical_plan_to_execution_plan;

		// Expression: field_name (idiom without table)
		let plan = LogicalPlan {
			expressions: vec![TopLevelExpr::Expr(crate::expr::Expr::Idiom(crate::expr::Idiom(
				vec![crate::expr::part::Part::Field("field_name".to_string())],
			)))],
		};

		// This should fail because idioms require row context
		let result = logical_plan_to_execution_plan(&plan);
		assert!(result.is_err(), "Expected error for idiom without table context");
	}
}
