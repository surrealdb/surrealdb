//! Query Planner for the Streaming Executor
//!
//! This module converts SurrealQL AST expressions (`Expr`) into physical execution
//! plans (`Arc<dyn ExecOperator>`). The planner is a critical component of the
//! streaming query executor, determining how queries are executed.
//!
//! # Architecture
//!
//! ```text
//! ┌──────────────┐     ┌──────────────┐     ┌────────────────┐
//! │   SurrealQL  │     │   Planner    │     │   Execution    │
//! │    (Expr)    │ ──► │   (this)     │ ──► │     Plan       │
//! │              │     │              │     │ (ExecOperator) │
//! └──────────────┘     └──────────────┘     └────────────────┘
//! ```
//!
//! # Usage
//!
//! The main entry point is the [`Planner`] struct:
//!
//! ```ignore
//! use surrealdb_core::exec::planner::Planner;
//!
//! let planner = Planner::new(&ctx);
//! let plan = planner.plan(expr)?;
//! ```
//!
//! For backwards compatibility, [`try_plan_expr`] delegates to `Planner::plan()`.
//!
//! # SELECT Pipeline
//!
//! SELECT statements are planned into a standard operator pipeline:
//!
//! ```text
//! Scan/Union (FROM)
//!     │
//!     ▼
//! Filter (WHERE)
//!     │
//!     ▼
//! Split (SPLIT BY)
//!     │
//!     ▼
//! Aggregate (GROUP BY)
//!     │
//!     ▼
//! Sort (ORDER BY)
//!     │
//!     ▼
//! Limit (LIMIT/START)
//!     │
//!     ▼
//! Fetch (FETCH)
//!     │
//!     ▼
//! Project (SELECT fields)
//!     │
//!     ▼
//! Timeout (TIMEOUT)
//! ```

mod aggregate;
mod idiom;
mod select;
mod source;
pub(crate) mod util;

use std::sync::Arc;

// Re-exports for external callers
use self::util::literal_to_value;
use crate::cnf::MAX_COMPUTATION_DEPTH;
use crate::ctx::FrozenContext;
use crate::dbs::NewPlannerStrategy;
use crate::err::Error;
use crate::exec::ExecOperator;
use crate::exec::function::FunctionRegistry;
use crate::exec::operators::{
	AnalyzePlan, DatabaseInfoPlan, ExplainPlan, ExprPlan, Fetch, ForeachPlan, IfElsePlan,
	IndexInfoPlan, NamespaceInfoPlan, ReturnPlan, RootInfoPlan, SequencePlan, SleepPlan,
	TableInfoPlan, UserInfoPlan,
};
use crate::exec::physical_expr::ControlFlowKind;
use crate::expr::statements::IfelseStatement;
use crate::expr::{Expr, Function, FunctionCall};

/// Query planner that converts logical expressions to physical execution plans.
///
/// The `Planner` holds shared resources (context, function registry) to avoid
/// passing them through every function call. Methods on `Planner` are spread
/// across submodules:
///
/// - [`select`] — SELECT pipeline planning
/// - [`aggregate`] — GROUP BY and aggregate extraction
/// - [`idiom`] — Idiom-to-physical-part conversion
/// - [`source`] — Lookup, index function, and source planning
/// - [`util`] — Pure utility functions
pub struct Planner<'ctx> {
	/// The frozen context containing query parameters, capabilities, and session info.
	ctx: &'ctx FrozenContext,
	/// Cached reference to the function registry for aggregate/projection detection.
	function_registry: &'ctx FunctionRegistry,
	/// Optional transaction for plan-time index resolution.
	///
	/// When present, the planner can resolve table definitions and indexes
	/// at plan time, enabling concrete scan operators (IndexScan, TableScan)
	/// instead of the generic Scan operator. This in turn enables
	/// optimizations like sort elimination via [`OutputOrdering`].
	///
	/// When `None`, the planner creates Scan operators that resolve their
	/// access path at execution time (the legacy behavior).
	pub(crate) txn: Option<Arc<crate::kvs::Transaction>>,
	/// Optional namespace name for plan-time catalog lookups.
	pub(crate) ns: Option<String>,
	/// Optional database name for plan-time catalog lookups.
	pub(crate) db: Option<String>,
}

impl<'ctx> Planner<'ctx> {
	/// Create a new planner with the given context (no transaction).
	///
	/// Table sources will use the generic `Scan` operator that resolves
	/// indexes at execution time. This is used by `physical_expr` for
	/// scalar subqueries and by callers that don't have transaction access.
	pub fn new(ctx: &'ctx FrozenContext) -> Self {
		Self {
			ctx,
			function_registry: ctx.function_registry(),
			txn: None,
			ns: None,
			db: None,
		}
	}

	/// Create a new planner with the given context and transaction.
	///
	/// When a transaction is provided, the planner can resolve table
	/// definitions and indexes at plan time, producing concrete scan
	/// operators (IndexScan, TableScan, etc.) and enabling optimizations
	/// like sort elimination.
	pub fn with_txn(
		ctx: &'ctx FrozenContext,
		txn: Arc<crate::kvs::Transaction>,
		ns: Option<String>,
		db: Option<String>,
	) -> Self {
		Self {
			ctx,
			function_registry: ctx.function_registry(),
			txn: Some(txn),
			ns,
			db,
		}
	}

	/// Get the function registry.
	#[inline]
	pub fn function_registry(&self) -> &'ctx FunctionRegistry {
		self.function_registry
	}

	// ========================================================================
	// Top-Level Planning
	// ========================================================================

	/// Plan an expression, converting it to an executable operator tree.
	///
	/// This is the main entry point for the planner. When a transaction is
	/// available, performs plan-time index resolution and sort elimination.
	pub async fn plan(&self, expr: &Expr) -> Result<Arc<dyn ExecOperator>, Error> {
		match expr {
			// DML/DDL — same as sync plan, always fall back to old executor
			Expr::Create(_) => Err(Error::PlannerUnsupported(
				"CREATE statements not yet supported in execution plans".to_string(),
			)),
			Expr::Update(_) => Err(Error::PlannerUnsupported(
				"UPDATE statements not yet supported in execution plans".to_string(),
			)),
			Expr::Upsert(_) => Err(Error::PlannerUnsupported(
				"UPSERT statements not yet supported in execution plans".to_string(),
			)),
			Expr::Delete(_) => Err(Error::PlannerUnsupported(
				"DELETE statements not yet supported in execution plans".to_string(),
			)),
			Expr::Insert(_) => Err(Error::PlannerUnsupported(
				"INSERT statements not yet supported in execution plans".to_string(),
			)),
			Expr::Relate(_) => Err(Error::PlannerUnsupported(
				"RELATE statements not yet supported in execution plans".to_string(),
			)),
			Expr::Define(_) => Err(Error::PlannerUnsupported(
				"DEFINE statements not yet supported in execution plans".to_string(),
			)),
			Expr::Remove(_) => Err(Error::PlannerUnsupported(
				"REMOVE statements not yet supported in execution plans".to_string(),
			)),
			Expr::Rebuild(_) => Err(Error::PlannerUnsupported(
				"REBUILD statements not yet supported in execution plans".to_string(),
			)),
			Expr::Alter(_) => Err(Error::PlannerUnsupported(
				"ALTER statements not yet supported in execution plans".to_string(),
			)),

			other => {
				let result = self.plan_expr(other.clone()).await;
				self.require_planned(result)
			}
		}
	}

	// ========================================================================
	// Expression-to-PhysicalExpr Conversion
	// ========================================================================

	/// Convert an expression to a physical expression.
	///
	/// Physical expressions are evaluated at runtime to produce values.
	/// This is used for expressions within operators (e.g., WHERE predicates,
	/// SELECT field expressions, ORDER BY expressions).
	pub async fn physical_expr(
		&self,
		expr: Expr,
	) -> Result<Arc<dyn crate::exec::PhysicalExpr>, Error> {
		use crate::exec::physical_expr::{
			ArrayLiteral, BinaryOp, BlockPhysicalExpr, BuiltinFunctionExec, ClosureCallExec,
			ClosureExec, ControlFlowExpr, IfElseExpr, JsFunctionExec, Literal as PhysicalLiteral,
			MockExpr, ModelFunctionExec, ObjectLiteral, Param, PostfixOp, ProjectionFunctionExec,
			RecordIdExpr, ScalarSubquery, SetLiteral, SiloModuleExec, SurrealismModuleExec,
			UnaryOp, UserDefinedFunctionExec,
		};

		match expr {
			Expr::Literal(crate::expr::literal::Literal::Array(elements)) => {
				let mut phys_elements = Vec::with_capacity(elements.len());
				for elem in elements {
					phys_elements.push(Box::pin(self.physical_expr(elem)).await?);
				}
				Ok(Arc::new(ArrayLiteral {
					elements: phys_elements,
				}))
			}
			Expr::Literal(crate::expr::literal::Literal::Object(entries)) => {
				let mut phys_entries = Vec::with_capacity(entries.len());
				for entry in entries {
					let value = Box::pin(self.physical_expr(entry.value)).await?;
					phys_entries.push((entry.key, value));
				}
				Ok(Arc::new(ObjectLiteral {
					entries: phys_entries,
				}))
			}
			Expr::Literal(crate::expr::literal::Literal::Set(elements)) => {
				let mut phys_elements = Vec::with_capacity(elements.len());
				for elem in elements {
					phys_elements.push(Box::pin(self.physical_expr(elem)).await?);
				}
				Ok(Arc::new(SetLiteral {
					elements: phys_elements,
				}))
			}
			Expr::Literal(crate::expr::literal::Literal::RecordId(rid_lit)) => {
				let key = self.convert_record_key_to_physical(&rid_lit.key).await?;
				Ok(Arc::new(RecordIdExpr {
					table: rid_lit.table,
					key,
				}))
			}
			Expr::Literal(lit) => {
				let value = literal_to_value(lit)?;
				Ok(Arc::new(PhysicalLiteral(value)))
			}
			Expr::Param(param) => Ok(Arc::new(Param(param.as_str().to_string()))),
			Expr::Idiom(idiom) => Box::pin(self.convert_idiom(idiom)).await,
			Expr::Binary {
				left,
				op,
				right,
			} => {
				// For MATCHES operators with idiom left and string-literal right,
				// create a MatchesOp that evaluates via the full-text index.
				if let crate::expr::operator::BinaryOperator::Matches(ref matches_op) = op
					&& let Expr::Idiom(idiom) = *left
				{
					if let Expr::Literal(crate::expr::literal::Literal::String(query)) = *right {
						// Multi-part idioms (e.g. `t.name`) may traverse record links
						// to fields on other tables. MatchesOp can only evaluate
						// MATCHES against a fulltext index on the source table — it
						// cannot resolve cross-table record links.
						if idiom.0.len() > 1 {
							return Err(Error::PlannerUnimplemented(
								"MATCHES with multi-part field path not yet supported \
								 in streaming executor"
									.to_string(),
							));
						}
						let idiom_clone = idiom.clone();
						let query_clone = query.clone();
						let left_phys = Box::pin(self.physical_expr(Expr::Idiom(idiom))).await?;
						let right_phys = Box::pin(self.physical_expr(Expr::Literal(
							crate::expr::literal::Literal::String(query),
						)))
						.await?;
						return Ok(Arc::new(crate::exec::physical_expr::MatchesOp::new(
							left_phys,
							right_phys,
							matches_op.clone(),
							idiom_clone,
							query_clone,
						)));
					}
					// Left was idiom but right wasn't a string literal — reassemble
					let left_phys = Box::pin(self.physical_expr(Expr::Idiom(idiom))).await?;
					let right_phys = Box::pin(self.physical_expr(*right)).await?;
					return Ok(Arc::new(BinaryOp {
						left: left_phys,
						op,
						right: right_phys,
					}));
				}
				// All other binary operators (and non-standard MATCHES patterns)
				let left_phys = Box::pin(self.physical_expr(*left)).await?;
				let right_phys = Box::pin(self.physical_expr(*right)).await?;
				Ok(Arc::new(BinaryOp {
					left: left_phys,
					op,
					right: right_phys,
				}))
			}
			Expr::Constant(constant) => {
				let value = constant.compute();
				Ok(Arc::new(PhysicalLiteral(value)))
			}
			Expr::Prefix {
				op,
				expr,
			} => {
				// Check for excessively deep prefix/cast chains. The old
				// compute path enforces a recursion depth limit via TreeStack;
				// the new physical-expr evaluator does not track depth. Detect
				// deep chains at planning time and reject with the same error.
				{
					let mut d = 0u32;
					let mut cur = &*expr;
					while let Expr::Prefix {
						expr: inner,
						..
					} = cur
					{
						d += 1;
						if d > *MAX_COMPUTATION_DEPTH {
							return Err(Error::ComputationDepthExceeded);
						}
						cur = inner;
					}
				}
				let inner = Box::pin(self.physical_expr(*expr)).await?;
				Ok(Arc::new(UnaryOp {
					op,
					expr: inner,
				}))
			}
			Expr::Postfix {
				op,
				expr,
			} => {
				use crate::expr::operator::PostfixOperator;

				match op {
					PostfixOperator::Call(args) => {
						let target = Box::pin(self.physical_expr(*expr)).await?;
						let mut phys_args = Vec::with_capacity(args.len());
						for arg in args {
							phys_args.push(Box::pin(self.physical_expr(arg)).await?);
						}
						Ok(Arc::new(ClosureCallExec {
							target,
							arguments: phys_args,
						}))
					}
					_ => {
						let inner = Box::pin(self.physical_expr(*expr)).await?;
						Ok(Arc::new(PostfixOp {
							op,
							expr: inner,
						}))
					}
				}
			}
			Expr::Table(table_name) => {
				Ok(Arc::new(PhysicalLiteral(crate::val::Value::Table(table_name))))
			}
			Expr::FunctionCall(func_call) => {
				let FunctionCall {
					receiver,
					arguments,
				} = *func_call;

				macro_rules! phys_args {
					($($arg:expr),*) => {{
						let mut phys_args = Vec::with_capacity(arguments.len());
						for arg in arguments {
							phys_args.push(Box::pin(self.physical_expr(arg)).await?);
						}
						phys_args
					}};
				}

				match receiver {
					Function::Normal(name) => {
						let registry = self.function_registry();

						if registry.is_index_function(&name) {
							return Box::pin(self.plan_index_function(&name, arguments)).await;
						}

						if registry.is_projection(&name) {
							let func_ctx = registry
								.get_projection(&name)
								.map(|f| f.required_context())
								.unwrap_or(crate::exec::ContextLevel::Database);
							Ok(Arc::new(ProjectionFunctionExec {
								name,
								arguments: phys_args!(arguments),
								func_required_context: func_ctx,
							}))
						} else {
							let func_ctx = registry
								.get(&name)
								.map(|f| f.required_context())
								.unwrap_or(crate::exec::ContextLevel::Root);
							Ok(Arc::new(BuiltinFunctionExec {
								name,
								arguments: phys_args!(arguments),
								func_required_context: func_ctx,
							}))
						}
					}
					Function::Custom(name) => Ok(Arc::new(UserDefinedFunctionExec {
						name,
						arguments: phys_args!(arguments),
					})),
					Function::Script(script) => Ok(Arc::new(JsFunctionExec {
						script,
						arguments: phys_args!(arguments),
					})),
					Function::Model(model) => Ok(Arc::new(ModelFunctionExec {
						model,
						arguments: phys_args!(arguments),
					})),
					Function::Module(module, sub) => Ok(Arc::new(SurrealismModuleExec {
						module,
						sub,
						arguments: phys_args!(arguments),
					})),
					Function::Silo {
						org,
						pkg,
						major,
						minor,
						patch,
						sub,
					} => Ok(Arc::new(SiloModuleExec {
						org,
						pkg,
						major,
						minor,
						patch,
						sub,
						arguments: phys_args!(arguments),
					})),
				}
			}
			Expr::Closure(closure) => Ok(Arc::new(ClosureExec {
				closure: *closure,
			})),
			Expr::IfElse(ifelse) => {
				let IfelseStatement {
					exprs,
					close,
				} = *ifelse;
				let mut branches = Vec::with_capacity(exprs.len());
				for (condition, body) in exprs {
					let cond_phys = Box::pin(self.physical_expr(condition)).await?;
					let body_phys = Box::pin(self.physical_expr(body)).await?;
					branches.push((cond_phys, body_phys));
				}
				let otherwise = if let Some(else_expr) = close {
					Some(Box::pin(self.physical_expr(else_expr)).await?)
				} else {
					None
				};
				Ok(Arc::new(IfElseExpr {
					branches,
					otherwise,
				}))
			}
			Expr::Select(select) => {
				let plan = Box::pin(self.plan_select_statement(*select)).await?;
				Ok(Arc::new(ScalarSubquery {
					plan,
				}))
			}

			// Control flow
			Expr::Break => Ok(Arc::new(ControlFlowExpr {
				kind: ControlFlowKind::Break,
				inner: None,
			})),
			Expr::Continue => Ok(Arc::new(ControlFlowExpr {
				kind: ControlFlowKind::Continue,
				inner: None,
			})),
			Expr::Return(output_stmt) => {
				let inner = Box::pin(self.physical_expr(output_stmt.what)).await?;
				Ok(Arc::new(ControlFlowExpr {
					kind: ControlFlowKind::Return,
					inner: Some(inner),
				}))
			}

			// DDL — cannot be used in expression context
			Expr::Define(_) => Err(Error::PlannerUnsupported(
				"DEFINE statements cannot be used in expression context".to_string(),
			)),
			Expr::Remove(_) => Err(Error::PlannerUnsupported(
				"REMOVE statements cannot be used in expression context".to_string(),
			)),
			Expr::Rebuild(_) => Err(Error::PlannerUnsupported(
				"REBUILD statements cannot be used in expression context".to_string(),
			)),
			Expr::Alter(_) => Err(Error::PlannerUnsupported(
				"ALTER statements cannot be used in expression context".to_string(),
			)),

			// INFO sub-expressions (e.g. `(INFO FOR DATABASE).analyzers`)
			Expr::Info(info) => {
				use crate::exec::operators::RootInfoPlan;
				use crate::expr::statements::info::InfoStatement;

				let plan: Arc<dyn ExecOperator> = match *info {
					InfoStatement::Root(structured) => Arc::new(RootInfoPlan::new(structured)),
					InfoStatement::Ns(structured) => Arc::new(NamespaceInfoPlan::new(structured)),
					InfoStatement::Db(structured, version) => {
						let version = match version {
							Some(v) => Some(Box::pin(self.physical_expr(v)).await?),
							None => None,
						};
						Arc::new(DatabaseInfoPlan::new(structured, version))
					}
					InfoStatement::Tb(table, structured, version) => {
						let table = self.physical_expr_as_name(table).await?;
						let version = match version {
							Some(v) => Some(Box::pin(self.physical_expr(v)).await?),
							None => None,
						};
						Arc::new(TableInfoPlan::new(table, structured, version))
					}
					InfoStatement::User(user, base, structured) => {
						let user = self.physical_expr_as_name(user).await?;
						Arc::new(UserInfoPlan::new(user, base, structured))
					}
					InfoStatement::Index(index, table, structured) => {
						let index = self.physical_expr_as_name(index).await?;
						let table = self.physical_expr_as_name(table).await?;
						Arc::new(IndexInfoPlan::new(index, table, structured))
					}
				};
				Ok(Arc::new(crate::exec::physical_expr::ScalarSubquery {
					plan,
				}))
			}
			Expr::Foreach(_) => Err(Error::Query {
				message: "FOR loops cannot be used in expression context".to_string(),
			}),
			Expr::Sleep(_) => Err(Error::Query {
				message: "SLEEP statements cannot be used in expression context".to_string(),
			}),
			Expr::Let(_) => Err(Error::Query {
				message: "LET statements cannot be used in expression context".to_string(),
			}),
			Expr::Explain {
				format,
				analyze,
				statement,
			} => {
				let inner_plan = self.plan_expr(*statement).await?;
				let plan: Arc<dyn ExecOperator> = if analyze {
					Arc::new(AnalyzePlan {
						plan: inner_plan,
						format,
						redact_duration: self.ctx.redact_duration(),
					})
				} else {
					Arc::new(ExplainPlan {
						plan: inner_plan,
						format,
					})
				};
				Ok(Arc::new(ScalarSubquery {
					plan,
				}))
			}

			Expr::Mock(mock) => Ok(Arc::new(MockExpr(mock))),
			Expr::Block(block) => Ok(Arc::new(BlockPhysicalExpr {
				block: *block,
			})),
			Expr::Throw(expr) => {
				let inner = Box::pin(self.physical_expr(*expr)).await?;
				Ok(Arc::new(ControlFlowExpr {
					kind: ControlFlowKind::Throw,
					inner: Some(inner),
				}))
			}

			// DML subqueries — not yet implemented
			Expr::Create(_) => Err(Error::PlannerUnsupported(
				"CREATE subqueries not yet supported in execution plans".to_string(),
			)),
			Expr::Update(_) => Err(Error::PlannerUnsupported(
				"UPDATE subqueries not yet supported in execution plans".to_string(),
			)),
			Expr::Upsert(_) => Err(Error::PlannerUnsupported(
				"UPSERT subqueries not yet supported in execution plans".to_string(),
			)),
			Expr::Delete(_) => Err(Error::PlannerUnsupported(
				"DELETE subqueries not yet supported in execution plans".to_string(),
			)),
			Expr::Relate(_) => Err(Error::PlannerUnsupported(
				"RELATE subqueries not yet supported in execution plans".to_string(),
			)),
			Expr::Insert(_) => Err(Error::PlannerUnsupported(
				"INSERT subqueries not yet supported in execution plans".to_string(),
			)),
		}
	}

	/// Convert an expression to a physical expression, treating simple identifiers as strings.
	///
	/// Used for `INFO FOR USER test` where `test` is a name, not a variable.
	pub async fn physical_expr_as_name(
		&self,
		expr: Expr,
	) -> Result<Arc<dyn crate::exec::PhysicalExpr>, Error> {
		use crate::exec::physical_expr::Literal as PhysicalLiteral;
		use crate::expr::part::Part;

		if let Expr::Idiom(ref idiom) = expr
			&& idiom.0.len() == 1
			&& let Part::Field(name) = &idiom.0[0]
		{
			return Ok(Arc::new(PhysicalLiteral(crate::val::Value::String(name.clone()))));
		}

		if let Expr::Table(name) = expr {
			return Ok(Arc::new(PhysicalLiteral(crate::val::Value::String(name.to_string()))));
		}

		Box::pin(self.physical_expr(expr)).await
	}

	// ========================================================================
	// Record ID Key Conversion
	// ========================================================================

	/// Convert a `RecordIdKeyLit` to a `PhysicalRecordIdKey` for runtime evaluation.
	///
	/// Scalar key types (Number, String, Uuid, Generate) are mapped directly.
	/// Array and Object elements are converted via `physical_expr()` so they
	/// can contain arbitrary expressions (function calls, params, etc.).
	/// Range bounds recurse through this method.
	fn convert_record_key_to_physical<'a>(
		&'a self,
		key: &'a crate::expr::RecordIdKeyLit,
	) -> crate::exec::BoxFut<
		'a,
		Result<crate::exec::physical_expr::record_id::PhysicalRecordIdKey, Error>,
	> {
		Box::pin(async move {
			use crate::exec::physical_expr::record_id::PhysicalRecordIdKey;
			use crate::expr::RecordIdKeyLit;

			match key {
				RecordIdKeyLit::Number(n) => Ok(PhysicalRecordIdKey::Number(*n)),
				RecordIdKeyLit::String(s) => Ok(PhysicalRecordIdKey::String(s.clone())),
				RecordIdKeyLit::Uuid(u) => Ok(PhysicalRecordIdKey::Uuid(*u)),
				RecordIdKeyLit::Generate(generator) => {
					Ok(PhysicalRecordIdKey::Generate(generator.clone()))
				}
				RecordIdKeyLit::Array(exprs) => {
					let mut phys = Vec::with_capacity(exprs.len());
					for expr in exprs {
						phys.push(Box::pin(self.physical_expr(expr.clone())).await?);
					}
					Ok(PhysicalRecordIdKey::Array(phys))
				}
				RecordIdKeyLit::Object(entries) => {
					let mut phys = Vec::with_capacity(entries.len());
					for entry in entries {
						let value = Box::pin(self.physical_expr(entry.value.clone())).await?;
						phys.push((entry.key.clone(), value));
					}
					Ok(PhysicalRecordIdKey::Object(phys))
				}
				RecordIdKeyLit::Range(range) => {
					let start = self.convert_bound_to_physical(&range.start).await?;
					let end = self.convert_bound_to_physical(&range.end).await?;
					Ok(PhysicalRecordIdKey::Range {
						start,
						end,
					})
				}
			}
		})
	}

	/// Convert a `Bound<RecordIdKeyLit>` to a `Bound<Box<PhysicalRecordIdKey>>`.
	async fn convert_bound_to_physical(
		&self,
		bound: &std::ops::Bound<crate::expr::RecordIdKeyLit>,
	) -> Result<
		std::ops::Bound<Box<crate::exec::physical_expr::record_id::PhysicalRecordIdKey>>,
		Error,
	> {
		match bound {
			std::ops::Bound::Unbounded => Ok(std::ops::Bound::Unbounded),
			std::ops::Bound::Included(key) => Ok(std::ops::Bound::Included(Box::new(
				self.convert_record_key_to_physical(key).await?,
			))),
			std::ops::Bound::Excluded(key) => Ok(std::ops::Bound::Excluded(Box::new(
				self.convert_record_key_to_physical(key).await?,
			))),
		}
	}

	// ========================================================================
	// Internal Planning
	// ========================================================================

	/// When `AllReadOnlyStatements` strategy is active, convert `Error::PlannerUnimplemented`
	/// into `Error::Query` so it becomes a hard error instead of a silent fallback.
	///
	/// `PlannerUnsupported` (DML/DDL) is left untouched — those always fall back to compute.
	fn require_planned<T>(&self, result: Result<T, Error>) -> Result<T, Error> {
		match result {
			Err(Error::PlannerUnimplemented(msg))
				if *self.ctx.new_planner_strategy()
					== NewPlannerStrategy::AllReadOnlyStatements =>
			{
				Err(Error::Query {
					message: format!("New executor does not support: {msg}"),
				})
			}
			other => other,
		}
	}

	/// Plan an expression into an operator tree. Recursive calls are boxed
	/// to satisfy the compiler's async recursion requirements.
	fn plan_expr(
		&self,
		expr: Expr,
	) -> crate::exec::BoxFut<'_, Result<Arc<dyn ExecOperator>, Error>> {
		Box::pin(async move {
			match expr {
				Expr::Select(select) => self.plan_select_statement(*select).await,
				Expr::Block(block) => self.plan_block(*block).await,
				Expr::Return(output_stmt) => self.plan_return_statement(*output_stmt).await,
				Expr::Let(let_stmt) => self.plan_let_statement(*let_stmt).await,
				Expr::Explain {
					format,
					analyze,
					statement,
				} => self.plan_explain_statement(format, analyze, *statement).await,
				Expr::Info(info) => self.plan_info_statement(*info).await,
				Expr::Foreach(stmt) => self.plan_foreach_statement(*stmt),
				Expr::IfElse(stmt) => self.plan_if_else_statement(*stmt),
				Expr::Sleep(sleep_stmt) => self.plan_sleep_statement(*sleep_stmt),

				expr @ (Expr::FunctionCall(_)
				| Expr::Closure(_)
				| Expr::Literal(_)
				| Expr::Param(_)
				| Expr::Constant(_)
				| Expr::Prefix {
					..
				}
				| Expr::Binary {
					..
				}
				| Expr::Postfix {
					..
				}
				| Expr::Table(_)
				| Expr::Idiom(_)
				| Expr::Mock(_)
				| Expr::Throw(_)
				| Expr::Break
				| Expr::Continue) => self.plan_expr_as_operator(expr).await,

				Expr::Create(_)
				| Expr::Update(_)
				| Expr::Upsert(_)
				| Expr::Delete(_)
				| Expr::Insert(_)
				| Expr::Relate(_) => Err(Error::PlannerUnsupported(
					"DML statements not yet supported in execution plans".to_string(),
				)),
				Expr::Define(_) | Expr::Remove(_) | Expr::Rebuild(_) | Expr::Alter(_) => {
					Err(Error::PlannerUnsupported(
						"DDL statements not yet supported in execution plans".to_string(),
					))
				}
			}
		})
	}

	async fn plan_block(&self, block: crate::expr::Block) -> Result<Arc<dyn ExecOperator>, Error> {
		if block.0.is_empty() {
			use crate::exec::physical_expr::Literal as PhysicalLiteral;
			Ok(Arc::new(ExprPlan::new(Arc::new(PhysicalLiteral(crate::val::Value::None))))
				as Arc<dyn ExecOperator>)
		} else if block.0.len() == 1 {
			self.plan_expr(block.0.into_iter().next().expect("block verified non-empty")).await
		} else {
			Ok(Arc::new(SequencePlan::new(block)) as Arc<dyn ExecOperator>)
		}
	}

	async fn plan_return_statement(
		&self,
		output_stmt: crate::expr::statements::OutputStatement,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		let inner = self.plan_expr(output_stmt.what).await?;

		let inner = if let Some(fetchs) = output_stmt.fetch {
			let mut fields = Vec::with_capacity(fetchs.len());
			for fetch_item in fetchs {
				let mut idioms = self.resolve_field_idioms(fetch_item.0).await?;
				fields.append(&mut idioms);
			}
			if fields.is_empty() {
				inner
			} else {
				Arc::new(Fetch::new(inner, fields)) as Arc<dyn ExecOperator>
			}
		} else {
			inner
		};

		Ok(Arc::new(ReturnPlan::new(inner)))
	}

	async fn plan_explain_statement(
		&self,
		format: crate::expr::ExplainFormat,
		analyze: bool,
		statement: Expr,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		let inner_plan = self.plan_expr(statement).await?;
		if analyze {
			Ok(Arc::new(AnalyzePlan {
				plan: inner_plan,
				format,
				redact_duration: self.ctx.redact_duration(),
			}))
		} else {
			Ok(Arc::new(ExplainPlan {
				plan: inner_plan,
				format,
			}))
		}
	}

	async fn plan_let_statement(
		&self,
		let_stmt: crate::expr::statements::SetStatement,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		let crate::expr::statements::SetStatement {
			name,
			what,
			kind: _,
		} = let_stmt;

		let value: Arc<dyn ExecOperator> = match what {
			Expr::Select(select) => self.plan_select_statement(*select).await?,
			Expr::Create(_) => {
				return Err(Error::PlannerUnsupported(
					"CREATE statements in LET not yet supported in execution plans".to_string(),
				));
			}
			Expr::Update(_) => {
				return Err(Error::PlannerUnsupported(
					"UPDATE statements in LET not yet supported in execution plans".to_string(),
				));
			}
			Expr::Upsert(_) => {
				return Err(Error::PlannerUnsupported(
					"UPSERT statements in LET not yet supported in execution plans".to_string(),
				));
			}
			Expr::Delete(_) => {
				return Err(Error::PlannerUnsupported(
					"DELETE statements in LET not yet supported in execution plans".to_string(),
				));
			}
			Expr::Insert(_) => {
				return Err(Error::PlannerUnsupported(
					"INSERT statements in LET not yet supported in execution plans".to_string(),
				));
			}
			Expr::Relate(_) => {
				return Err(Error::PlannerUnsupported(
					"RELATE statements in LET not yet supported in execution plans".to_string(),
				));
			}
			other => {
				let expr = Box::pin(self.physical_expr(other)).await?;
				Arc::new(ExprPlan::new(expr))
			}
		};

		Ok(Arc::new(crate::exec::operators::LetPlan::new(name, value)))
	}

	async fn plan_info_statement(
		&self,
		info: crate::expr::statements::info::InfoStatement,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		use crate::expr::statements::info::InfoStatement;
		match info {
			InfoStatement::Root(structured) => {
				Ok(Arc::new(RootInfoPlan::new(structured)) as Arc<dyn ExecOperator>)
			}
			InfoStatement::Ns(structured) => {
				Ok(Arc::new(NamespaceInfoPlan::new(structured)) as Arc<dyn ExecOperator>)
			}
			InfoStatement::Db(structured, version) => {
				let version = match version {
					Some(v) => Some(Box::pin(self.physical_expr(v)).await?),
					None => None,
				};
				Ok(Arc::new(DatabaseInfoPlan::new(structured, version)) as Arc<dyn ExecOperator>)
			}
			InfoStatement::Tb(table, structured, version) => {
				let table = self.physical_expr_as_name(table).await?;
				let version = match version {
					Some(v) => Some(Box::pin(self.physical_expr(v)).await?),
					None => None,
				};
				Ok(Arc::new(TableInfoPlan::new(table, structured, version))
					as Arc<dyn ExecOperator>)
			}
			InfoStatement::User(user, base, structured) => {
				let user = self.physical_expr_as_name(user).await?;
				Ok(Arc::new(UserInfoPlan::new(user, base, structured)) as Arc<dyn ExecOperator>)
			}
			InfoStatement::Index(index, table, structured) => {
				let index = self.physical_expr_as_name(index).await?;
				let table = self.physical_expr_as_name(table).await?;
				Ok(Arc::new(IndexInfoPlan::new(index, table, structured)) as Arc<dyn ExecOperator>)
			}
		}
	}

	fn plan_foreach_statement(
		&self,
		stmt: crate::expr::statements::ForeachStatement,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		let crate::expr::statements::ForeachStatement {
			param,
			range,
			block,
		} = stmt;
		Ok(Arc::new(ForeachPlan::new(param, range, block)) as Arc<dyn ExecOperator>)
	}

	fn plan_if_else_statement(
		&self,
		stmt: IfelseStatement,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		let IfelseStatement {
			exprs,
			close,
		} = stmt;
		Ok(Arc::new(IfElsePlan::new(exprs, close)) as Arc<dyn ExecOperator>)
	}

	fn plan_sleep_statement(
		&self,
		sleep_stmt: crate::expr::statements::SleepStatement,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		Ok(Arc::new(SleepPlan::new(sleep_stmt.duration)))
	}

	/// Plan an expression by converting it to a physical expression and wrapping
	/// it in an [`ExprPlan`] operator.
	///
	/// Used for expressions that don't need special operator-level planning
	/// (literals, params, function calls, closures, etc.).
	async fn plan_expr_as_operator(&self, expr: Expr) -> Result<Arc<dyn ExecOperator>, Error> {
		let phys_expr = Box::pin(self.physical_expr(expr)).await?;
		Ok(Arc::new(ExprPlan::new(phys_expr)) as Arc<dyn ExecOperator>)
	}
}

// ============================================================================
// Public API Wrappers
// ============================================================================

/// Plan an expression into an executable operator tree.
///
/// This is the main entry point for the planner, delegating to `Planner::plan()`.
/// Returns `Error::PlannerUnsupported` when `ComputeOnly` strategy is active.
/// Plan an expression into an executable operator tree.
///
/// When a transaction is provided, the planner resolves table definitions
/// and indexes at plan time, enabling sort elimination and concrete scan operators.
pub(crate) async fn try_plan_expr(
	expr: &Expr,
	ctx: &FrozenContext,
	txn: Arc<crate::kvs::Transaction>,
) -> Result<Arc<dyn ExecOperator>, Error> {
	// Extract ns/db from the context session parameters if available
	let ns =
		ctx.value("session").and_then(|v| v.as_object()).and_then(|o| o.get("ns")).and_then(|v| {
			match v {
				crate::val::Value::String(s) => Some(s.clone()),
				_ => None,
			}
		});
	let db =
		ctx.value("session").and_then(|v| v.as_object()).and_then(|o| o.get("db")).and_then(|v| {
			match v {
				crate::val::Value::String(s) => Some(s.clone()),
				_ => None,
			}
		});
	if *ctx.new_planner_strategy() == NewPlannerStrategy::ComputeOnly {
		return Err(Error::PlannerUnsupported(
			"ComputeOnly strategy: skipping new planner".to_string(),
		));
	}
	Planner::with_txn(ctx, txn, ns, db).plan(expr).await
}

/// Convert an expression to a physical expression.
///
/// Thin wrapper that constructs a `Planner` and calls `physical_expr`. External
/// callers that plan multiple expressions should construct a `Planner` directly.
pub(crate) async fn expr_to_physical_expr(
	expr: Expr,
	ctx: &FrozenContext,
) -> Result<Arc<dyn crate::exec::PhysicalExpr>, Error> {
	Planner::new(ctx).physical_expr(expr).await
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod planner_tests {
	use super::*;
	use crate::ctx::Context;

	#[tokio::test]
	async fn test_planner_creates_let_operator() {
		let expr = Expr::Let(Box::new(crate::expr::statements::SetStatement {
			name: "x".to_string(),
			what: Expr::Literal(crate::expr::literal::Literal::Integer(42)),
			kind: None,
		}));

		let ctx = Arc::new(Context::background());
		let plan = Planner::new(&ctx).plan(&expr).await.expect("Planning failed");

		assert_eq!(plan.name(), "Let");
		assert!(plan.mutates_context());
	}

	#[tokio::test]
	async fn test_planner_creates_scalar_plan() {
		let expr = Expr::Literal(crate::expr::literal::Literal::Integer(42));

		let ctx = Arc::new(Context::background());
		let plan = Planner::new(&ctx).plan(&expr).await.expect("Planning failed");

		assert_eq!(plan.name(), "Expr");
		assert!(plan.is_scalar());
	}
}
