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
//! For backwards compatibility, [`try_plan_expr!`] delegates to `Planner::plan()`.
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
use crate::exec::physical_expr::{
	ArrayLiteral, BinaryOp, BlockPhysicalExpr, BuiltinFunctionExec, ClosureCallExec, ClosureExec,
	ControlFlowExpr, ControlFlowKind, IfElseExpr, JsFunctionExec, Literal as PhysicalLiteral,
	MockExpr, ModelFunctionExec, ObjectLiteral, Param, PostfixOp, ProjectionFunctionExec,
	RecordIdExpr, ScalarSubquery, SetLiteral, SiloModuleExec, SurrealismModuleExec, UnaryOp,
	UserDefinedFunctionExec,
};
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
	///
	/// Each `Expr` variant is handled by a focused helper method; this function
	/// is a thin dispatcher.
	pub async fn physical_expr(
		&self,
		expr: Expr,
	) -> Result<Arc<dyn crate::exec::PhysicalExpr>, Error> {
		match expr {
			// Literals and constant values
			Expr::Literal(lit) => Box::pin(self.physical_literal(lit)).await,
			Expr::Constant(c) => Ok(Arc::new(PhysicalLiteral(c.compute()))),
			Expr::Table(t) => Ok(Arc::new(PhysicalLiteral(crate::val::Value::Table(t)))),
			Expr::Param(p) => Ok(Arc::new(Param(p.as_str().to_string()))),
			Expr::Idiom(idiom) => Box::pin(self.convert_idiom(idiom)).await,

			// Operators
			Expr::Binary {
				left,
				op,
				right,
			} => Box::pin(self.physical_binary_expr(*left, op, *right)).await,
			Expr::Prefix {
				op,
				expr,
			} => Box::pin(self.physical_prefix_expr(op, *expr)).await,
			Expr::Postfix {
				op,
				expr,
			} => Box::pin(self.physical_postfix_expr(op, *expr)).await,

			// Functions and closures
			Expr::FunctionCall(fc) => Box::pin(self.physical_function_call(*fc)).await,
			Expr::Closure(c) => Ok(Arc::new(ClosureExec {
				closure: *c,
			})),

			// Compound expressions
			Expr::IfElse(stmt) => Box::pin(self.physical_if_else(*stmt)).await,
			Expr::Mock(m) => Ok(Arc::new(MockExpr(m))),
			Expr::Block(b) => Ok(Arc::new(BlockPhysicalExpr {
				block: *b,
			})),

			// Control flow
			Expr::Break => Ok(Arc::new(ControlFlowExpr {
				kind: ControlFlowKind::Break,
				inner: None,
			})),
			Expr::Continue => Ok(Arc::new(ControlFlowExpr {
				kind: ControlFlowKind::Continue,
				inner: None,
			})),
			Expr::Return(s) => {
				let inner = Box::pin(self.physical_expr(s.what)).await?;
				Ok(Arc::new(ControlFlowExpr {
					kind: ControlFlowKind::Return,
					inner: Some(inner),
				}))
			}
			Expr::Throw(e) => {
				let inner = Box::pin(self.physical_expr(*e)).await?;
				Ok(Arc::new(ControlFlowExpr {
					kind: ControlFlowKind::Throw,
					inner: Some(inner),
				}))
			}

			// Statement subqueries (wrapped in ScalarSubquery)
			Expr::Select(_)
			| Expr::Info(_)
			| Expr::Foreach(_)
			| Expr::Sleep(_)
			| Expr::Explain {
				..
			} => Box::pin(self.physical_statement_subquery(expr)).await,

			// LET is handled by block/sequence operators, not as an expression
			Expr::Let(_) => Err(Error::Query {
				message: "LET statements are handled by block or sequence operators".to_string(),
			}),

			// DDL — cannot be used in expression context
			Expr::Define(_) | Expr::Remove(_) | Expr::Rebuild(_) | Expr::Alter(_) => {
				Err(Error::PlannerUnsupported(
					"DDL statements cannot be used in expression context".to_string(),
				))
			}

			// DML subqueries — not yet implemented
			Expr::Create(_)
			| Expr::Update(_)
			| Expr::Upsert(_)
			| Expr::Delete(_)
			| Expr::Relate(_)
			| Expr::Insert(_) => Err(Error::PlannerUnsupported(
				"DML subqueries not yet supported in execution plans".to_string(),
			)),
		}
	}

	/// Convert a literal expression to a physical expression.
	async fn physical_literal(
		&self,
		lit: crate::expr::literal::Literal,
	) -> Result<Arc<dyn crate::exec::PhysicalExpr>, Error> {
		use crate::expr::literal::Literal;

		match lit {
			Literal::Array(elements) => {
				let elements = self.physical_args(elements).await?;
				Ok(Arc::new(ArrayLiteral {
					elements,
				}))
			}
			Literal::Object(entries) => {
				let mut phys_entries = Vec::with_capacity(entries.len());
				for entry in entries {
					let value = Box::pin(self.physical_expr(entry.value)).await?;
					phys_entries.push((entry.key, value));
				}
				Ok(Arc::new(ObjectLiteral {
					entries: phys_entries,
				}))
			}
			Literal::Set(elements) => {
				let elements = self.physical_args(elements).await?;
				Ok(Arc::new(SetLiteral {
					elements,
				}))
			}
			Literal::RecordId(rid_lit) => {
				let key = self.convert_record_key_to_physical(&rid_lit.key).await?;
				Ok(Arc::new(RecordIdExpr {
					table: rid_lit.table,
					key,
				}))
			}
			other => {
				let value = literal_to_value(other)?;
				Ok(Arc::new(PhysicalLiteral(value)))
			}
		}
	}

	/// Convert a binary expression to a physical expression.
	///
	/// Handles the MATCHES operator special-case (full-text index evaluation)
	/// and the `SimpleBinaryOp` optimisation for `field op literal` patterns.
	async fn physical_binary_expr(
		&self,
		left: Expr,
		op: crate::expr::operator::BinaryOperator,
		right: Expr,
	) -> Result<Arc<dyn crate::exec::PhysicalExpr>, Error> {
		// For MATCHES operators with idiom left and string-literal right,
		// create a MatchesOp that evaluates via the full-text index.
		if let crate::expr::operator::BinaryOperator::Matches(ref matches_op) = op
			&& let Expr::Idiom(idiom) = left
		{
			if let Expr::Literal(crate::expr::literal::Literal::String(query)) = right {
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
				let right_phys = Box::pin(
					self.physical_expr(Expr::Literal(crate::expr::literal::Literal::String(query))),
				)
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
			let right_phys = Box::pin(self.physical_expr(right)).await?;
			return Ok(Arc::new(BinaryOp {
				left: left_phys,
				op,
				right: right_phys,
			}));
		}

		// All other binary operators (and non-standard MATCHES patterns)
		let left_phys = Box::pin(self.physical_expr(left)).await?;
		let right_phys = Box::pin(self.physical_expr(right)).await?;

		// Optimisation: detect `field op literal` or `literal op field`
		// patterns and emit a SimpleBinaryOp that inlines field access
		// and avoids per-record async dispatch + Value cloning.
		if is_simple_binary_eligible(&op) {
			if let Some(field) = left_phys.try_simple_field()
				&& let Some(lit) = right_phys.try_literal()
			{
				return Ok(Arc::new(crate::exec::physical_expr::SimpleBinaryOp {
					field_name: field.to_string(),
					op,
					literal: lit.clone(),
					reversed: false,
				}));
			} else if let Some(field) = right_phys.try_simple_field()
				&& let Some(lit) = left_phys.try_literal()
			{
				return Ok(Arc::new(crate::exec::physical_expr::SimpleBinaryOp {
					field_name: field.to_string(),
					op,
					literal: lit.clone(),
					reversed: true,
				}));
			}
		}

		Ok(Arc::new(BinaryOp {
			left: left_phys,
			op,
			right: right_phys,
		}))
	}

	/// Convert a prefix (unary) expression to a physical expression.
	async fn physical_prefix_expr(
		&self,
		op: crate::expr::operator::PrefixOperator,
		expr: Expr,
	) -> Result<Arc<dyn crate::exec::PhysicalExpr>, Error> {
		// Check for excessively deep prefix/cast chains. The old compute path
		// enforces a recursion depth limit via TreeStack; the new physical-expr
		// evaluator does not track depth. Detect deep chains at planning time
		// and reject with the same error.
		{
			let mut d = 0u32;
			let mut cur = &expr;
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
		let inner = Box::pin(self.physical_expr(expr)).await?;
		Ok(Arc::new(UnaryOp {
			op,
			expr: inner,
		}))
	}

	/// Convert a postfix expression to a physical expression.
	async fn physical_postfix_expr(
		&self,
		op: crate::expr::operator::PostfixOperator,
		expr: Expr,
	) -> Result<Arc<dyn crate::exec::PhysicalExpr>, Error> {
		use crate::expr::operator::PostfixOperator;

		match op {
			PostfixOperator::Call(args) => {
				let target = Box::pin(self.physical_expr(expr)).await?;
				let arguments = self.physical_args(args).await?;
				Ok(Arc::new(ClosureCallExec {
					target,
					arguments,
				}))
			}
			_ => {
				let inner = Box::pin(self.physical_expr(expr)).await?;
				Ok(Arc::new(PostfixOp {
					op,
					expr: inner,
				}))
			}
		}
	}

	/// Convert a function call to a physical expression.
	async fn physical_function_call(
		&self,
		func_call: FunctionCall,
	) -> Result<Arc<dyn crate::exec::PhysicalExpr>, Error> {
		let FunctionCall {
			receiver,
			arguments,
		} = func_call;

		match receiver {
			Function::Normal(name) => {
				let registry = self.function_registry();

				if registry.is_index_function(&name) {
					return Box::pin(self.plan_index_function(&name, arguments)).await;
				}

				let arguments = self.physical_args(arguments).await?;
				if registry.is_projection(&name) {
					let func_ctx = registry
						.get_projection(&name)
						.map(|f| f.required_context())
						.unwrap_or(crate::exec::ContextLevel::Database);
					Ok(Arc::new(ProjectionFunctionExec {
						name,
						arguments,
						func_required_context: func_ctx,
					}))
				} else {
					let func_ctx = registry
						.get(&name)
						.map(|f| f.required_context())
						.unwrap_or(crate::exec::ContextLevel::Root);
					Ok(Arc::new(BuiltinFunctionExec {
						name,
						arguments,
						func_required_context: func_ctx,
					}))
				}
			}
			Function::Custom(name) => {
				let arguments = self.physical_args(arguments).await?;
				Ok(Arc::new(UserDefinedFunctionExec {
					name,
					arguments,
				}))
			}
			Function::Script(script) => {
				let arguments = self.physical_args(arguments).await?;
				Ok(Arc::new(JsFunctionExec {
					script,
					arguments,
				}))
			}
			Function::Model(model) => {
				let arguments = self.physical_args(arguments).await?;
				Ok(Arc::new(ModelFunctionExec {
					model,
					arguments,
				}))
			}
			Function::Module(module, sub) => {
				let arguments = self.physical_args(arguments).await?;
				Ok(Arc::new(SurrealismModuleExec {
					module,
					sub,
					arguments,
				}))
			}
			Function::Silo {
				org,
				pkg,
				major,
				minor,
				patch,
				sub,
			} => {
				let arguments = self.physical_args(arguments).await?;
				Ok(Arc::new(SiloModuleExec {
					org,
					pkg,
					major,
					minor,
					patch,
					sub,
					arguments,
				}))
			}
		}
	}

	/// Convert a list of argument expressions to physical expressions.
	async fn physical_args(
		&self,
		args: Vec<Expr>,
	) -> Result<Vec<Arc<dyn crate::exec::PhysicalExpr>>, Error> {
		let mut phys = Vec::with_capacity(args.len());
		for arg in args {
			phys.push(Box::pin(self.physical_expr(arg)).await?);
		}
		Ok(phys)
	}

	/// Convert an if-else expression to a physical expression.
	async fn physical_if_else(
		&self,
		stmt: IfelseStatement,
	) -> Result<Arc<dyn crate::exec::PhysicalExpr>, Error> {
		let IfelseStatement {
			exprs,
			close,
		} = stmt;
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

	/// Convert a statement expression (SELECT, INFO, FOREACH, SLEEP, EXPLAIN)
	/// into a physical expression by wrapping its operator plan in a
	/// [`ScalarSubquery`].
	async fn physical_statement_subquery(
		&self,
		expr: Expr,
	) -> Result<Arc<dyn crate::exec::PhysicalExpr>, Error> {
		let plan: Arc<dyn ExecOperator> = match expr {
			Expr::Select(select) => Box::pin(self.plan_select_statement(*select)).await?,
			Expr::Info(info) => self.plan_info_statement(*info).await?,
			Expr::Foreach(stmt) => self.plan_foreach_statement(*stmt)?,
			Expr::Sleep(stmt) => self.plan_sleep_statement(*stmt)?,
			Expr::Explain {
				format,
				analyze,
				statement,
			} => {
				let inner_plan = self.plan_expr(*statement).await?;
				if analyze {
					Arc::new(AnalyzePlan {
						plan: inner_plan,
						format,
						redact_volatile_explain_attrs: self.ctx.redact_volatile_explain_attrs(),
					})
				} else {
					Arc::new(ExplainPlan {
						plan: inner_plan,
						format,
					})
				}
			}
			_ => unreachable!("physical_statement_subquery called with non-statement expr"),
		};
		Ok(Arc::new(ScalarSubquery {
			plan,
		}))
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
				redact_volatile_explain_attrs: self.ctx.redact_volatile_explain_attrs(),
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

macro_rules! try_plan_expr {
	($expr:expr, $ctx:expr, $txn:expr) => {{
		let __expr: &$crate::expr::Expr = $expr;
		if matches!(
			__expr,
			$crate::expr::Expr::Create(_)
				| $crate::expr::Expr::Update(_)
				| $crate::expr::Expr::Upsert(_)
				| $crate::expr::Expr::Delete(_)
				| $crate::expr::Expr::Insert(_)
				| $crate::expr::Expr::Relate(_)
				| $crate::expr::Expr::Define(_)
				| $crate::expr::Expr::Remove(_)
				| $crate::expr::Expr::Rebuild(_)
				| $crate::expr::Expr::Alter(_)
		) {
			Err($crate::err::Error::PlannerUnsupported(String::new()))
		} else if *$ctx.new_planner_strategy() == $crate::dbs::NewPlannerStrategy::ComputeOnly {
			Err($crate::err::Error::PlannerUnsupported(String::new()))
		} else {
			$crate::exec::planner::plan_expr_inner(__expr, $ctx, $txn).await
		}
	}};
}

pub(crate) use try_plan_expr;

/// Plan an expression into an executable operator tree.
///
/// This is the inner planning function called by the `try_plan_expr!` macro
/// after DML/DDL rejection and ComputeOnly checks have been performed inline.
///
/// When a transaction is provided, the planner resolves table definitions
/// and indexes at plan time, enabling sort elimination and concrete scan operators.
pub(crate) async fn plan_expr_inner(
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

/// Returns `true` if the binary operator is eligible for `SimpleBinaryOp` optimisation.
///
/// Only comparison and containment operators are eligible — these take `(&Value, &Value)`
/// and produce a boolean result. Operators that produce non-boolean results (arithmetic,
/// ranges), require short-circuit logic (And, Or, NullCoalescing), or need special index
/// context (Matches, NearestNeighbor) are excluded.
fn is_simple_binary_eligible(op: &crate::expr::operator::BinaryOperator) -> bool {
	use crate::expr::operator::BinaryOperator;
	matches!(
		op,
		BinaryOperator::Equal
			| BinaryOperator::ExactEqual
			| BinaryOperator::NotEqual
			| BinaryOperator::AllEqual
			| BinaryOperator::AnyEqual
			| BinaryOperator::LessThan
			| BinaryOperator::LessThanEqual
			| BinaryOperator::MoreThan
			| BinaryOperator::MoreThanEqual
			| BinaryOperator::Contain
			| BinaryOperator::NotContain
			| BinaryOperator::ContainAll
			| BinaryOperator::ContainAny
			| BinaryOperator::ContainNone
			| BinaryOperator::Inside
			| BinaryOperator::NotInside
			| BinaryOperator::AllInside
			| BinaryOperator::AnyInside
			| BinaryOperator::NoneInside
			| BinaryOperator::Outside
			| BinaryOperator::Intersects
	)
}

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
