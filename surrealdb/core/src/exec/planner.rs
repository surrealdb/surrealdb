//! Query Planner for the Streaming Executor
//!
//! This module converts SurrealQL AST expressions (`Expr`) into physical execution
//! plans (`Arc<dyn ExecOperator>`). The planner is a critical component of the
//! streaming query executor, determining how queries are executed.
//!
//! # Architecture
//!
//! ```text
//! ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
//! │   SurrealQL  │     │   Planner    │     │  Execution   │
//! │     AST      │ ──► │   (this)     │ ──► │    Plan      │
//! │    (Expr)    │     │              │     │ (ExecOperator) │
//! └──────────────┘     └──────────────┘     └──────────────┘
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
mod util;

use std::sync::Arc;

// Re-exports for external callers
pub(crate) use self::source::LOOKUP_SOURCE_PARAM;
use self::util::literal_to_value;
use crate::ctx::FrozenContext;
use crate::dbs::NewPlannerStrategy;
use crate::err::Error;
use crate::exec::ExecOperator;
use crate::exec::function::FunctionRegistry;
use crate::exec::operators::{
	DatabaseInfoPlan, ExplainPlan, ExprPlan, Fetch, ForeachPlan, IfElsePlan, IndexInfoPlan,
	NamespaceInfoPlan, ReturnPlan, RootInfoPlan, SequencePlan, SleepPlan, TableInfoPlan,
	UserInfoPlan,
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
}

impl<'ctx> Planner<'ctx> {
	/// Create a new planner with the given context.
	pub fn new(ctx: &'ctx FrozenContext) -> Self {
		Self {
			ctx,
			function_registry: ctx.function_registry(),
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
	/// This is the main entry point for the planner.
	pub fn plan(&self, expr: Expr) -> Result<Arc<dyn ExecOperator>, Error> {
		self.plan_expr(expr)
	}

	// ========================================================================
	// Expression-to-PhysicalExpr Conversion
	// ========================================================================

	/// Convert an expression to a physical expression.
	///
	/// Physical expressions are evaluated at runtime to produce values.
	/// This is used for expressions within operators (e.g., WHERE predicates,
	/// SELECT field expressions, ORDER BY expressions).
	pub fn physical_expr(&self, expr: Expr) -> Result<Arc<dyn crate::exec::PhysicalExpr>, Error> {
		use crate::exec::physical_expr::{
			ArrayLiteral, BinaryOp, BlockPhysicalExpr, BuiltinFunctionExec, ClosureCallExec,
			ClosureExec, ControlFlowExpr, IfElseExpr, JsFunctionExec, Literal as PhysicalLiteral,
			MockExpr, ModelFunctionExec, ObjectLiteral, Param, PostfixOp, ProjectionFunctionExec,
			ScalarSubquery, SetLiteral, SiloModuleExec, SurrealismModuleExec, UnaryOp,
			UserDefinedFunctionExec,
		};

		match expr {
			Expr::Literal(crate::expr::literal::Literal::Array(elements)) => {
				let mut phys_elements = Vec::with_capacity(elements.len());
				for elem in elements {
					phys_elements.push(self.physical_expr(elem)?);
				}
				Ok(Arc::new(ArrayLiteral {
					elements: phys_elements,
				}))
			}
			Expr::Literal(crate::expr::literal::Literal::Object(entries)) => {
				let mut phys_entries = Vec::with_capacity(entries.len());
				for entry in entries {
					let value = self.physical_expr(entry.value)?;
					phys_entries.push((entry.key, value));
				}
				Ok(Arc::new(ObjectLiteral {
					entries: phys_entries,
				}))
			}
			Expr::Literal(crate::expr::literal::Literal::Set(elements)) => {
				let mut phys_elements = Vec::with_capacity(elements.len());
				for elem in elements {
					phys_elements.push(self.physical_expr(elem)?);
				}
				Ok(Arc::new(SetLiteral {
					elements: phys_elements,
				}))
			}
			Expr::Literal(lit) => {
				let value = literal_to_value(lit)?;
				Ok(Arc::new(PhysicalLiteral(value)))
			}
			Expr::Param(param) => Ok(Arc::new(Param(param.as_str().to_string()))),
			Expr::Idiom(idiom) => self.convert_idiom(idiom),
			Expr::Binary {
				left,
				op,
				right,
			} => {
				let left_phys = self.physical_expr(*left)?;
				let right_phys = self.physical_expr(*right)?;
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
				let inner = self.physical_expr(*expr)?;
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
						let target = self.physical_expr(*expr)?;
						let mut phys_args = Vec::with_capacity(args.len());
						for arg in args {
							phys_args.push(self.physical_expr(arg)?);
						}
						Ok(Arc::new(ClosureCallExec {
							target,
							arguments: phys_args,
						}))
					}
					_ => {
						let inner = self.physical_expr(*expr)?;
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
					($($arg:expr),*) => {
						{
							let mut phys_args = Vec::with_capacity(arguments.len());
							for arg in arguments {
								phys_args.push(self.physical_expr(arg)?);
							}
							phys_args
						}
					};
				};

				match receiver {
					Function::Normal(name) => {
						let registry = self.function_registry();

						if registry.is_index_function(&name) {
							return self.plan_index_function(&name, arguments);
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
					let cond_phys = self.physical_expr(condition)?;
					let body_phys = self.physical_expr(body)?;
					branches.push((cond_phys, body_phys));
				}
				let otherwise = if let Some(else_expr) = close {
					Some(self.physical_expr(else_expr)?)
				} else {
					None
				};
				Ok(Arc::new(IfElseExpr {
					branches,
					otherwise,
				}))
			}
			Expr::Select(select) => {
				let plan = self.plan_select_statement(*select)?;
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
				let inner = self.physical_expr(output_stmt.what)?;
				Ok(Arc::new(ControlFlowExpr {
					kind: ControlFlowKind::Return,
					inner: Some(inner),
				}))
			}

			// DDL — cannot be used in expression context
			Expr::Define(_) => Err(Error::Unimplemented(
				"DEFINE statements cannot be used in expression context".to_string(),
			)),
			Expr::Remove(_) => Err(Error::Unimplemented(
				"REMOVE statements cannot be used in expression context".to_string(),
			)),
			Expr::Rebuild(_) => Err(Error::Unimplemented(
				"REBUILD statements cannot be used in expression context".to_string(),
			)),
			Expr::Alter(_) => Err(Error::Unimplemented(
				"ALTER statements cannot be used in expression context".to_string(),
			)),

			// INFO sub-expressions (e.g. `(INFO FOR DATABASE).analyzers`)
			Expr::Info(info) => {
				use crate::exec::operators::RootInfoPlan;
				use crate::expr::statements::info::InfoStatement;

				let plan: Arc<dyn ExecOperator> = match *info {
					InfoStatement::Root(structured) => Arc::new(RootInfoPlan {
						structured,
					}),
					InfoStatement::Ns(structured) => Arc::new(NamespaceInfoPlan {
						structured,
					}),
					InfoStatement::Db(structured, version) => {
						let version = version.map(|v| self.physical_expr(v)).transpose()?;
						Arc::new(DatabaseInfoPlan {
							structured,
							version,
						})
					}
					InfoStatement::Tb(table, structured, version) => {
						let table = self.physical_expr_as_name(table)?;
						let version = version.map(|v| self.physical_expr(v)).transpose()?;
						Arc::new(TableInfoPlan {
							table,
							structured,
							version,
						})
					}
					InfoStatement::User(user, base, structured) => {
						let user = self.physical_expr_as_name(user)?;
						Arc::new(UserInfoPlan {
							user,
							base,
							structured,
						})
					}
					InfoStatement::Index(index, table, structured) => {
						let index = self.physical_expr_as_name(index)?;
						let table = self.physical_expr_as_name(table)?;
						Arc::new(IndexInfoPlan {
							index,
							table,
							structured,
						})
					}
				};
				Ok(Arc::new(crate::exec::physical_expr::ScalarSubquery {
					plan,
				}))
			}
			Expr::Foreach(_) => Err(Error::Unimplemented(
				"FOR loops cannot be used in expression context".to_string(),
			)),
			Expr::Sleep(_) => Err(Error::Unimplemented(
				"SLEEP statements cannot be used in expression context".to_string(),
			)),
			Expr::Let(_) => Err(Error::Unimplemented(
				"LET statements cannot be used in expression context".to_string(),
			)),
			Expr::Explain {
				..
			} => Err(Error::Unimplemented(
				"EXPLAIN statements cannot be used in expression context".to_string(),
			)),

			Expr::Mock(mock) => Ok(Arc::new(MockExpr(mock))),
			Expr::Block(block) => Ok(Arc::new(BlockPhysicalExpr {
				block: *block,
			})),
			Expr::Throw(expr) => {
				let inner = self.physical_expr(*expr)?;
				Ok(Arc::new(ControlFlowExpr {
					kind: ControlFlowKind::Throw,
					inner: Some(inner),
				}))
			}

			// DML subqueries — not yet implemented
			Expr::Create(_) => Err(Error::Unimplemented(
				"CREATE subqueries not yet supported in execution plans".to_string(),
			)),
			Expr::Update(_) => Err(Error::Unimplemented(
				"UPDATE subqueries not yet supported in execution plans".to_string(),
			)),
			Expr::Upsert(_) => Err(Error::Unimplemented(
				"UPSERT subqueries not yet supported in execution plans".to_string(),
			)),
			Expr::Delete(_) => Err(Error::Unimplemented(
				"DELETE subqueries not yet supported in execution plans".to_string(),
			)),
			Expr::Relate(_) => Err(Error::Unimplemented(
				"RELATE subqueries not yet supported in execution plans".to_string(),
			)),
			Expr::Insert(_) => Err(Error::Unimplemented(
				"INSERT subqueries not yet supported in execution plans".to_string(),
			)),
		}
	}

	/// Convert an expression to a physical expression, treating simple identifiers as strings.
	///
	/// Used for `INFO FOR USER test` where `test` is a name, not a variable.
	pub fn physical_expr_as_name(
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

		self.physical_expr(expr)
	}

	// ========================================================================
	// Internal Planning
	// ========================================================================

	/// When `AllReadOnlyStatements` strategy is active, convert `Error::Unimplemented`
	/// into `Error::Query` so it becomes a hard error instead of a silent fallback.
	fn require_planned<T>(&self, result: Result<T, Error>) -> Result<T, Error> {
		match result {
			Err(Error::Unimplemented(msg))
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

	fn plan_expr(&self, expr: Expr) -> Result<Arc<dyn ExecOperator>, Error> {
		match expr {
			// DML — always fall back to old executor
			Expr::Create(_) => Err(Error::Unimplemented(
				"CREATE statements not yet supported in execution plans".to_string(),
			)),
			Expr::Update(_) => Err(Error::Unimplemented(
				"UPDATE statements not yet supported in execution plans".to_string(),
			)),
			Expr::Upsert(_) => Err(Error::Unimplemented(
				"UPSERT statements not yet supported in execution plans".to_string(),
			)),
			Expr::Delete(_) => Err(Error::Unimplemented(
				"DELETE statements not yet supported in execution plans".to_string(),
			)),
			Expr::Insert(_) => Err(Error::Unimplemented(
				"INSERT statements not yet supported in execution plans".to_string(),
			)),
			Expr::Relate(_) => Err(Error::Unimplemented(
				"RELATE statements not yet supported in execution plans".to_string(),
			)),

			// DDL — always fall back to old executor
			Expr::Define(_) => Err(Error::Unimplemented(
				"DEFINE statements not yet supported in execution plans".to_string(),
			)),
			Expr::Remove(_) => Err(Error::Unimplemented(
				"REMOVE statements not yet supported in execution plans".to_string(),
			)),
			Expr::Rebuild(_) => Err(Error::Unimplemented(
				"REBUILD statements not yet supported in execution plans".to_string(),
			)),
			Expr::Alter(_) => Err(Error::Unimplemented(
				"ALTER statements not yet supported in execution plans".to_string(),
			)),

			other => self.require_planned(self.plan_non_ddl_dml_expr(other)),
		}
	}

	fn plan_non_ddl_dml_expr(&self, expr: Expr) -> Result<Arc<dyn ExecOperator>, Error> {
		match expr {
			Expr::Select(select) => self.plan_select_statement(*select),
			Expr::Let(let_stmt) => self.plan_let_statement(*let_stmt),
			Expr::Info(info) => self.plan_info_statement(*info),
			Expr::Foreach(stmt) => self.plan_foreach_statement(*stmt),
			Expr::IfElse(stmt) => self.plan_if_else_statement(*stmt),
			Expr::Block(block) => self.plan_block(*block),
			Expr::Return(output_stmt) => self.plan_return_statement(*output_stmt),
			Expr::Sleep(sleep_stmt) => self.plan_sleep_statement(*sleep_stmt),
			Expr::Explain {
				format,
				statement,
			} => self.plan_explain_statement(format, *statement),

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
			| Expr::Continue) => self.plan_expr_as_operator(expr),

			Expr::Create(_)
			| Expr::Update(_)
			| Expr::Upsert(_)
			| Expr::Delete(_)
			| Expr::Insert(_)
			| Expr::Relate(_)
			| Expr::Define(_)
			| Expr::Remove(_)
			| Expr::Rebuild(_)
			| Expr::Alter(_) => {
				unreachable!("DDL/DML statements should be handled in plan_expr")
			}
		}
	}

	fn plan_info_statement(
		&self,
		info: crate::expr::statements::info::InfoStatement,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		use crate::expr::statements::info::InfoStatement;
		match info {
			InfoStatement::Root(structured) => Ok(Arc::new(RootInfoPlan {
				structured,
			}) as Arc<dyn ExecOperator>),
			InfoStatement::Ns(structured) => Ok(Arc::new(NamespaceInfoPlan {
				structured,
			}) as Arc<dyn ExecOperator>),
			InfoStatement::Db(structured, version) => {
				let version = version.map(|v| self.physical_expr(v)).transpose()?;
				Ok(Arc::new(DatabaseInfoPlan {
					structured,
					version,
				}) as Arc<dyn ExecOperator>)
			}
			InfoStatement::Tb(table, structured, version) => {
				let table = self.physical_expr_as_name(table)?;
				let version = version.map(|v| self.physical_expr(v)).transpose()?;
				Ok(Arc::new(TableInfoPlan {
					table,
					structured,
					version,
				}) as Arc<dyn ExecOperator>)
			}
			InfoStatement::User(user, base, structured) => {
				let user = self.physical_expr_as_name(user)?;
				Ok(Arc::new(UserInfoPlan {
					user,
					base,
					structured,
				}) as Arc<dyn ExecOperator>)
			}
			InfoStatement::Index(index, table, structured) => {
				let index = self.physical_expr_as_name(index)?;
				let table = self.physical_expr_as_name(table)?;
				Ok(Arc::new(IndexInfoPlan {
					index,
					table,
					structured,
				}) as Arc<dyn ExecOperator>)
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
		Ok(Arc::new(ForeachPlan {
			param,
			range,
			body: block,
		}) as Arc<dyn ExecOperator>)
	}

	fn plan_if_else_statement(
		&self,
		stmt: IfelseStatement,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		let IfelseStatement {
			exprs,
			close,
		} = stmt;
		Ok(Arc::new(IfElsePlan {
			branches: exprs,
			else_body: close,
		}) as Arc<dyn ExecOperator>)
	}

	fn plan_block(
		&self,
		block: crate::expr::Block,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		if block.0.is_empty() {
			use crate::exec::physical_expr::Literal as PhysicalLiteral;
			Ok(Arc::new(ExprPlan {
				expr: Arc::new(PhysicalLiteral(crate::val::Value::None)),
			}) as Arc<dyn ExecOperator>)
		} else if block.0.len() == 1 {
			self.plan_expr(block.0.into_iter().next().unwrap())
		} else {
			Ok(Arc::new(SequencePlan {
				block,
			}) as Arc<dyn ExecOperator>)
		}
	}

	fn plan_return_statement(
		&self,
		output_stmt: crate::expr::statements::OutputStatement,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		let inner = self.plan_expr(output_stmt.what)?;

		let inner = if let Some(fetchs) = output_stmt.fetch {
			let mut fields = Vec::with_capacity(fetchs.len());
			for fetch_item in fetchs {
				let mut idioms = self.resolve_field_idioms(fetch_item.0)?;
				fields.append(&mut idioms);
			}
			if fields.is_empty() {
				inner
			} else {
				Arc::new(Fetch {
					input: inner,
					fields,
				}) as Arc<dyn ExecOperator>
			}
		} else {
			inner
		};

		Ok(Arc::new(ReturnPlan {
			inner,
		}))
	}

	fn plan_sleep_statement(
		&self,
		sleep_stmt: crate::expr::statements::SleepStatement,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		Ok(Arc::new(SleepPlan {
			duration: sleep_stmt.duration,
		}))
	}

	fn plan_explain_statement(
		&self,
		format: crate::expr::ExplainFormat,
		statement: Expr,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		let inner_plan = self.plan_expr(statement)?;
		Ok(Arc::new(ExplainPlan {
			plan: inner_plan,
			format,
		}))
	}

	/// Plan an expression by converting it to a physical expression and wrapping
	/// it in an [`ExprPlan`] operator.
	///
	/// Used for expressions that don't need special operator-level planning
	/// (literals, params, function calls, closures, etc.).
	fn plan_expr_as_operator(&self, expr: Expr) -> Result<Arc<dyn ExecOperator>, Error> {
		let phys_expr = self.physical_expr(expr)?;
		Ok(Arc::new(ExprPlan {
			expr: phys_expr,
		}) as Arc<dyn ExecOperator>)
	}
}

// ============================================================================
// Public API Wrappers
// ============================================================================

/// Plan an expression into an executable operator tree.
///
/// This is the main entry point for the planner, delegating to `Planner::plan()`.
/// Returns `Error::Unimplemented` when `ComputeOnly` strategy is active.
pub(crate) fn try_plan_expr(
	expr: Expr,
	ctx: &FrozenContext,
) -> Result<Arc<dyn ExecOperator>, Error> {
	if *ctx.new_planner_strategy() == NewPlannerStrategy::ComputeOnly {
		return Err(Error::Unimplemented("ComputeOnly strategy: skipping new planner".to_string()));
	}
	Planner::new(ctx).plan(expr)
}

/// Convert an expression to a physical expression.
///
/// Thin wrapper that constructs a `Planner` and calls `physical_expr`. External
/// callers that plan multiple expressions should construct a `Planner` directly.
pub(crate) fn expr_to_physical_expr(
	expr: Expr,
	ctx: &FrozenContext,
) -> Result<Arc<dyn crate::exec::PhysicalExpr>, Error> {
	Planner::new(ctx).physical_expr(expr)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod planner_tests {
	use super::*;
	use crate::ctx::Context;

	#[test]
	fn test_planner_creates_let_operator() {
		let expr = Expr::Let(Box::new(crate::expr::statements::SetStatement {
			name: "x".to_string(),
			what: Expr::Literal(crate::expr::literal::Literal::Integer(42)),
			kind: None,
		}));

		let ctx = Arc::new(Context::background());
		let plan = try_plan_expr(expr, &ctx).expect("Planning failed");

		assert_eq!(plan.name(), "Let");
		assert!(plan.mutates_context());
	}

	#[test]
	fn test_planner_creates_scalar_plan() {
		let expr = Expr::Literal(crate::expr::literal::Literal::Integer(42));

		let ctx = Arc::new(Context::background());
		let plan = try_plan_expr(expr, &ctx).expect("Planning failed");

		assert_eq!(plan.name(), "Expr");
		assert!(plan.is_scalar());
	}
}
