use std::sync::Arc;

use crate::cnf::MAX_ORDER_LIMIT_PRIORITY_QUEUE_SIZE;
use crate::ctx::FrozenContext;
use crate::err::Error;
use crate::exec::ExecOperator;
#[cfg(storage)]
use crate::exec::operators::ExternalSort;
use crate::exec::operators::{
	Aggregate, AggregateField, AggregateInfo, ClosurePlan, ControlFlowKind, ControlFlowPlan,
	DatabaseInfoPlan, ExplainPlan, ExprPlan, Fetch, FieldSelection, Filter, ForeachPlan,
	IfElsePlan, IndexInfoPlan, LetPlan, Limit, NamespaceInfoPlan, OrderByField, Project,
	ProjectValue, RandomShuffle, RootInfoPlan, Scan, SequencePlan, SleepPlan, Sort, SortDirection,
	SortTopK, SourceExpr, Split, TableInfoPlan, Timeout, Union, UserInfoPlan,
};
use crate::expr::field::{Field, Fields, Selector};
use crate::expr::statements::IfelseStatement;
use crate::expr::{Expr, Function, FunctionCall, Literal};

pub(crate) fn expr_to_physical_expr(
	expr: Expr,
	ctx: &FrozenContext,
) -> Result<Arc<dyn crate::exec::PhysicalExpr>, Error> {
	use crate::exec::physical_expr::{
		ArrayLiteral, BinaryOp, BlockPhysicalExpr, BuiltinFunctionExec, ClosureExec, IfElseExpr,
		JsFunctionExec, Literal as PhysicalLiteral, ModelFunctionExec, ObjectLiteral, Param,
		PostfixOp, ScalarSubquery, SetLiteral, SiloModuleExec, SurrealismModuleExec, UnaryOp,
		UserDefinedFunctionExec,
	};

	match expr {
		Expr::Literal(crate::expr::literal::Literal::Array(elements)) => {
			// Array literal - convert each element to a physical expression
			let mut phys_elements = Vec::with_capacity(elements.len());
			for elem in elements {
				phys_elements.push(expr_to_physical_expr(elem, ctx)?);
			}
			Ok(Arc::new(ArrayLiteral {
				elements: phys_elements,
			}))
		}
		Expr::Literal(crate::expr::literal::Literal::Object(entries)) => {
			// Object literal - convert each entry to a physical expression
			let mut phys_entries = Vec::with_capacity(entries.len());
			for entry in entries {
				let value = expr_to_physical_expr(entry.value, ctx)?;
				phys_entries.push((entry.key, value));
			}
			Ok(Arc::new(ObjectLiteral {
				entries: phys_entries,
			}))
		}
		Expr::Literal(crate::expr::literal::Literal::Set(elements)) => {
			// Set literal - convert each element to a physical expression
			let mut phys_elements = Vec::with_capacity(elements.len());
			for elem in elements {
				phys_elements.push(expr_to_physical_expr(elem, ctx)?);
			}
			Ok(Arc::new(SetLiteral {
				elements: phys_elements,
			}))
		}
		Expr::Literal(lit) => {
			// Convert the logical Literal to a physical Value
			let value = literal_to_value(lit)?;
			Ok(Arc::new(PhysicalLiteral(value)))
		}
		Expr::Param(param) => Ok(Arc::new(Param(param.as_str().to_string()))),
		Expr::Idiom(idiom) => convert_idiom_to_physical_expr(&idiom, ctx),
		Expr::Binary {
			left,
			op,
			right,
		} => {
			let left_phys = expr_to_physical_expr(*left, ctx)?;
			let right_phys = expr_to_physical_expr(*right, ctx)?;
			Ok(Arc::new(BinaryOp {
				left: left_phys,
				op,
				right: right_phys,
			}))
		}
		Expr::Constant(constant) => {
			// Convert constant to its computed value
			let value = constant
				.compute()
				.map_err(|e| Error::Unimplemented(format!("Failed to compute constant: {}", e)))?;
			Ok(Arc::new(PhysicalLiteral(value)))
		}
		Expr::Prefix {
			op,
			expr,
		} => {
			let inner = expr_to_physical_expr(*expr, ctx)?;
			Ok(Arc::new(UnaryOp {
				op,
				expr: inner,
			}))
		}
		Expr::Postfix {
			op,
			expr,
		} => {
			let inner = expr_to_physical_expr(*expr, ctx)?;
			Ok(Arc::new(PostfixOp {
				op,
				expr: inner,
			}))
		}
		Expr::Table(table_name) => {
			// Table name as a string value
			Ok(Arc::new(PhysicalLiteral(crate::val::Value::String(
				table_name.as_str().to_string(),
			))))
		}
		Expr::FunctionCall(func_call) => {
			let FunctionCall {
				receiver,
				arguments,
			} = *func_call;

			// Function call - convert arguments to physical expressions
			let mut phys_args = Vec::with_capacity(arguments.len());
			for arg in arguments {
				phys_args.push(expr_to_physical_expr(arg, ctx)?);
			}

			// Dispatch to appropriate PhysicalExpr type based on function variant
			match receiver {
				Function::Normal(name) => Ok(Arc::new(BuiltinFunctionExec {
					name,
					arguments: phys_args,
				})),
				Function::Custom(name) => Ok(Arc::new(UserDefinedFunctionExec {
					name,
					arguments: phys_args,
				})),
				Function::Script(script) => {
					return Err(Error::Unimplemented(
						"Script functions not yet supported in execution plans".to_string(),
					));
				}
				Function::Model(model) => {
					return Err(Error::Unimplemented(
						"Model functions not yet supported in execution plans".to_string(),
					));
				}
				Function::Module(module, sub) => {
					return Err(Error::Unimplemented(
						"Module functions not yet supported in execution plans".to_string(),
					));
				}
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
					arguments: phys_args,
				})),
			}
		}
		Expr::Closure(closure) => {
			// Closure expression - wrap in physical expression
			Ok(Arc::new(ClosureExec {
				closure: *closure,
			}))
		}
		Expr::IfElse(ifelse) => {
			let IfelseStatement {
				exprs,
				close,
			} = *ifelse;
			// IF/THEN/ELSE expression - convert all branches
			let mut branches = Vec::with_capacity(exprs.len());
			for (condition, body) in exprs {
				let cond_phys = expr_to_physical_expr(condition, ctx)?;
				let body_phys = expr_to_physical_expr(body, ctx)?;
				branches.push((cond_phys, body_phys));
			}
			let otherwise = if let Some(else_expr) = close {
				Some(expr_to_physical_expr(else_expr, ctx)?)
			} else {
				None
			};
			Ok(Arc::new(IfElseExpr {
				branches,
				otherwise,
			}))
		}
		Expr::Select(select) => {
			// Scalar subquery - plan the SELECT and wrap in ScalarSubquery
			let plan = plan_select(*select, ctx)?;
			Ok(Arc::new(ScalarSubquery {
				plan,
			}))
		}

		// Control flow expressions - cannot be used in expression context
		Expr::Break => Err(Error::Unimplemented(
			"BREAK cannot be used in expression context - only valid in loops".to_string(),
		)),
		Expr::Continue => Err(Error::Unimplemented(
			"CONTINUE cannot be used in expression context - only valid in loops".to_string(),
		)),
		Expr::Return(_) => Err(Error::Unimplemented(
			"RETURN cannot be used in expression context - only valid in functions".to_string(),
		)),

		// DDL statements - cannot be used in expression context
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

		// Utility statements - cannot be used in expression context
		Expr::Info(_) => Err(Error::Unimplemented(
			"INFO statements cannot be used in expression context".to_string(),
		)),
		Expr::Foreach(_) => {
			Err(Error::Unimplemented("FOR loops cannot be used in expression context".to_string()))
		}
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

		// Value expressions - not yet implemented
		Expr::Mock(_) => Err(Error::Unimplemented(
			"Mock expressions not yet supported in execution plans".to_string(),
		)),
		Expr::Block(block) => {
			// Deferred planning: wrap the block without converting inner expressions.
			// The BlockPhysicalExpr will plan and execute each expression at evaluation
			// time, allowing LET bindings to inform subsequent expression planning.
			Ok(Arc::new(BlockPhysicalExpr {
				block: *block,
			}))
		}
		Expr::Throw(_) => Err(Error::Unimplemented(
			"THROW expressions not yet supported in execution plans".to_string(),
		)),

		// DML subqueries - not yet implemented
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

/// Convert a RecordIdKeyLit to a RecordIdKey for range bounds
fn convert_record_key_lit(
	key_lit: &crate::expr::record_id::RecordIdKeyLit,
) -> Result<crate::val::RecordIdKey, Error> {
	use crate::expr::record_id::RecordIdKeyLit;
	use crate::val::RecordIdKey;

	match key_lit {
		RecordIdKeyLit::Number(n) => Ok(RecordIdKey::Number(*n)),
		RecordIdKeyLit::String(s) => Ok(RecordIdKey::String(s.clone())),
		RecordIdKeyLit::Uuid(u) => Ok(RecordIdKey::Uuid(*u)),
		RecordIdKeyLit::Generate(generator) => Ok(generator.compute()),
		RecordIdKeyLit::Array(_) => Err(Error::Unimplemented(
			"Array record keys not yet supported in execution plans".to_string(),
		)),
		RecordIdKeyLit::Object(_) => Err(Error::Unimplemented(
			"Object record keys not yet supported in execution plans".to_string(),
		)),
		RecordIdKeyLit::Range(_) => Err(Error::Unimplemented(
			"Nested range record keys not supported in execution plans".to_string(),
		)),
	}
}

/// Convert a Literal to a Value for static (non-computed) cases
/// This is used for USE statement expressions which must be static
fn literal_to_value(lit: crate::expr::literal::Literal) -> Result<crate::val::Value, Error> {
	use crate::expr::literal::Literal;
	use crate::val::{Number, Range, Value};

	match lit {
		Literal::None => Ok(Value::None),
		Literal::Null => Ok(Value::Null),
		Literal::UnboundedRange => Ok(Value::Range(Box::new(Range::unbounded()))),
		Literal::Bool(x) => Ok(Value::Bool(x)),
		Literal::Float(x) => Ok(Value::Number(Number::Float(x))),
		Literal::Integer(i) => Ok(Value::Number(Number::Int(i))),
		Literal::Decimal(d) => Ok(Value::Number(Number::Decimal(d))),
		Literal::String(s) => Ok(Value::String(s)),
		Literal::Bytes(b) => Ok(Value::Bytes(b)),
		Literal::Regex(r) => Ok(Value::Regex(r)),
		Literal::Duration(d) => Ok(Value::Duration(d)),
		Literal::Datetime(dt) => Ok(Value::Datetime(dt)),
		Literal::Uuid(u) => Ok(Value::Uuid(u)),
		Literal::Geometry(g) => Ok(Value::Geometry(g)),
		Literal::File(f) => Ok(Value::File(f)),
		// RecordId literals - convert to RecordId value for Scan operator
		Literal::RecordId(rid_lit) => {
			use std::ops::Bound;

			use crate::expr::record_id::RecordIdKeyLit;
			use crate::val::{RecordId, RecordIdKey, RecordIdKeyRange};

			let key = match &rid_lit.key {
				RecordIdKeyLit::Number(n) => RecordIdKey::Number(*n),
				RecordIdKeyLit::String(s) => RecordIdKey::String(s.clone()),
				RecordIdKeyLit::Uuid(u) => RecordIdKey::Uuid(*u),
				RecordIdKeyLit::Generate(generator) => generator.compute(),
				RecordIdKeyLit::Range(range_lit) => {
					// Convert RecordIdKeyRangeLit to RecordIdKeyRange
					let start = match &range_lit.start {
						Bound::Unbounded => Bound::Unbounded,
						Bound::Included(key_lit) => {
							Bound::Included(convert_record_key_lit(key_lit)?)
						}
						Bound::Excluded(key_lit) => {
							Bound::Excluded(convert_record_key_lit(key_lit)?)
						}
					};
					let end = match &range_lit.end {
						Bound::Unbounded => Bound::Unbounded,
						Bound::Included(key_lit) => {
							Bound::Included(convert_record_key_lit(key_lit)?)
						}
						Bound::Excluded(key_lit) => {
							Bound::Excluded(convert_record_key_lit(key_lit)?)
						}
					};
					RecordIdKey::Range(Box::new(RecordIdKeyRange {
						start,
						end,
					}))
				}
				RecordIdKeyLit::Array(_) => {
					return Err(Error::Unimplemented(
						"Array record keys not yet supported in execution plans".to_string(),
					));
				}
				RecordIdKeyLit::Object(_) => {
					return Err(Error::Unimplemented(
						"Object record keys not yet supported in execution plans".to_string(),
					));
				}
			};

			Ok(Value::RecordId(RecordId {
				table: rid_lit.table.clone(),
				key,
			}))
		}
		Literal::Array(_) => Err(Error::Unimplemented(
			"Array literals in USE statements not yet supported".to_string(),
		)),
		Literal::Set(_) => Err(Error::Unimplemented(
			"Set literals in USE statements not yet supported".to_string(),
		)),
		Literal::Object(_) => Err(Error::Unimplemented(
			"Object literals in USE statements not yet supported".to_string(),
		)),
	}
}

pub(crate) fn try_plan_expr(
	expr: Expr,
	ctx: &FrozenContext,
) -> Result<Arc<dyn ExecOperator>, Error> {
	match expr {
		// Supported statements
		Expr::Select(select) => plan_select(*select, ctx),
		Expr::Let(let_stmt) => convert_let_statement(*let_stmt, ctx),

		// DML statements - not yet supported
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

		// DDL statements - not yet supported
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

		// INFO statements
		Expr::Info(info) => {
			use crate::expr::statements::info::InfoStatement;
			match *info {
				InfoStatement::Root(structured) => Ok(Arc::new(RootInfoPlan {
					structured,
				}) as Arc<dyn ExecOperator>),
				InfoStatement::Ns(structured) => Ok(Arc::new(NamespaceInfoPlan {
					structured,
				}) as Arc<dyn ExecOperator>),
				InfoStatement::Db(structured, version) => {
					let version = version.map(|v| expr_to_physical_expr(v, ctx)).transpose()?;
					Ok(Arc::new(DatabaseInfoPlan {
						structured,
						version,
					}) as Arc<dyn ExecOperator>)
				}
				InfoStatement::Tb(table, structured, version) => {
					let table = expr_to_physical_expr(table, ctx)?;
					let version = version.map(|v| expr_to_physical_expr(v, ctx)).transpose()?;
					Ok(Arc::new(TableInfoPlan {
						table,
						structured,
						version,
					}) as Arc<dyn ExecOperator>)
				}
				InfoStatement::User(user, base, structured) => {
					let user = expr_to_physical_expr(user, ctx)?;
					Ok(Arc::new(UserInfoPlan {
						user,
						base,
						structured,
					}) as Arc<dyn ExecOperator>)
				}
				InfoStatement::Index(index, table, structured) => {
					let index = expr_to_physical_expr(index, ctx)?;
					let table = expr_to_physical_expr(table, ctx)?;
					Ok(Arc::new(IndexInfoPlan {
						index,
						table,
						structured,
					}) as Arc<dyn ExecOperator>)
				}
			}
		}
		Expr::Foreach(stmt) => Ok(Arc::new(ForeachPlan {
			param: stmt.param.clone(),
			range: stmt.range.clone(),
			body: stmt.block.clone(),
		}) as Arc<dyn ExecOperator>),
		Expr::IfElse(stmt) => Ok(Arc::new(IfElsePlan {
			branches: stmt.exprs.clone(),
			else_body: stmt.close.clone(),
		}) as Arc<dyn ExecOperator>),
		Expr::Block(block) => {
			// Deferred planning: wrap the block without converting inner expressions.
			// The SequencePlan will plan and execute each expression at runtime,
			// allowing LET bindings to inform subsequent expression planning.
			if block.0.is_empty() {
				// Empty block returns NONE immediately
				use crate::exec::physical_expr::Literal as PhysicalLiteral;
				Ok(Arc::new(ExprPlan {
					expr: Arc::new(PhysicalLiteral(crate::val::Value::None)),
				}) as Arc<dyn ExecOperator>)
			} else if block.0.len() == 1 {
				// Single statement - plan directly without wrapper
				try_plan_expr(block.0.into_iter().next().unwrap(), ctx)
			} else {
				// Multiple statements - use SequencePlan with deferred planning
				Ok(Arc::new(SequencePlan {
					block: *block,
				}) as Arc<dyn ExecOperator>)
			}
		}
		Expr::FunctionCall(_) => {
			// Function calls are value expressions - convert to physical expression
			let phys_expr = expr_to_physical_expr(expr, ctx)?;
			// Validate that the expression doesn't require row context
			if phys_expr.references_current_value() {
				return Err(Error::Unimplemented(
					"Function call references row context but no table specified".to_string(),
				));
			}
			Ok(Arc::new(ExprPlan {
				expr: phys_expr,
			}) as Arc<dyn ExecOperator>)
		}
		Expr::Closure(closure) => Ok(Arc::new(ClosurePlan {
			args: closure.args.clone(),
			returns: closure.returns.clone(),
			body: closure.body.clone(),
		}) as Arc<dyn ExecOperator>),
		Expr::Return(output_stmt) => {
			// Plan the inner expression
			let inner = try_plan_expr(output_stmt.what, ctx)?;

			// Wrap with Fetch operator if FETCH clause is present
			let inner = if let Some(fetchs) = output_stmt.fetch {
				// Extract idioms from fetch expressions
				// FETCH expressions are typically Expr::Idiom(idiom)
				let fields: Vec<_> = fetchs
					.iter()
					.filter_map(|f| {
						if let Expr::Idiom(idiom) = &f.0 {
							Some(idiom.clone())
						} else {
							// Non-idiom fetch expressions are not supported in the new planner
							None
						}
					})
					.collect();
				if fields.is_empty() {
					// No idiom fields to fetch, pass through
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

			Ok(Arc::new(ControlFlowPlan {
				kind: ControlFlowKind::Return,
				inner: Some(inner),
			}))
		}
		Expr::Throw(expr) => {
			let inner = try_plan_expr(*expr, ctx)?;
			Ok(Arc::new(ControlFlowPlan {
				kind: ControlFlowKind::Throw,
				inner: Some(inner),
			}))
		}
		Expr::Break => Ok(Arc::new(ControlFlowPlan {
			kind: ControlFlowKind::Break,
			inner: None,
		})),
		Expr::Continue => Ok(Arc::new(ControlFlowPlan {
			kind: ControlFlowKind::Continue,
			inner: None,
		})),
		Expr::Sleep(sleep_stmt) => Ok(Arc::new(SleepPlan {
			duration: sleep_stmt.duration.clone(),
		})),
		Expr::Explain {
			format,
			statement,
		} => {
			// Plan the inner statement
			let inner_plan = try_plan_expr(*statement, ctx)?;
			// Wrap it in an ExplainPlan operator
			Ok(Arc::new(ExplainPlan {
				plan: inner_plan,
				format,
			}))
		}

		// Value expressions - evaluate in scalar context and return result
		Expr::Literal(_)
		| Expr::Param(_)
		| Expr::Constant(_)
		| Expr::Prefix {
			..
		}
		| Expr::Binary {
			..
		}
		| Expr::Table(_) => {
			let phys_expr = expr_to_physical_expr(expr, ctx)?;
			// Validate that the expression doesn't require row context
			if phys_expr.references_current_value() {
				return Err(Error::Unimplemented(
					"Expression references row context but no table specified".to_string(),
				));
			}
			Ok(Arc::new(ExprPlan {
				expr: phys_expr,
			}) as Arc<dyn ExecOperator>)
		}

		// Idiom expressions require row context, so they need special handling
		Expr::Idiom(_) => {
			let phys_expr = expr_to_physical_expr(expr, ctx)?;
			// Idioms always reference current_value, so this will be an error for top-level
			if phys_expr.references_current_value() {
				return Err(Error::Unimplemented(
					"Field expressions require a FROM clause to provide row context".to_string(),
				));
			}
			Ok(Arc::new(ExprPlan {
				expr: phys_expr,
			}) as Arc<dyn ExecOperator>)
		}

		// Mock expressions generate test data - defer for now
		Expr::Mock(_) => Err(Error::Unimplemented(
			"Mock expressions not yet supported in execution plans".to_string(),
		)),

		// Postfix expressions (ranges, method calls)
		Expr::Postfix {
			..
		} => {
			let phys_expr = expr_to_physical_expr(expr, ctx)?;
			// Validate that the expression doesn't require row context
			if phys_expr.references_current_value() {
				return Err(Error::Unimplemented(
					"Postfix expression references row context but no table specified".to_string(),
				));
			}
			Ok(Arc::new(ExprPlan {
				expr: phys_expr,
			}) as Arc<dyn ExecOperator>)
		}
	}
}

/// Plan a SELECT statement
///
/// The operator pipeline is built in this order:
/// 1. Scan/Union (source from FROM clause)
/// 2. Filter (WHERE)
/// 3. Split (SPLIT BY)
/// 4. Aggregate (GROUP BY)
/// 5. Sort (ORDER BY)
/// 6. Limit (LIMIT/START)
/// 7. Fetch (FETCH)
/// 8. Project (SELECT fields) or ProjectValue (SELECT VALUE)
/// 9. Timeout (TIMEOUT)
fn plan_select(
	select: crate::expr::statements::SelectStatement,
	ctx: &FrozenContext,
) -> Result<Arc<dyn ExecOperator>, Error> {
	let crate::expr::statements::SelectStatement {
		mut fields,
		omit,
		only,
		what,
		with,
		cond,
		split,
		group,
		order,
		limit,
		start,
		fetch,
		version,
		timeout,
		explain,
		tempfiles,
	} = select;

	// ONLY clause (unwraps single results)
	if only {
		return Err(Error::Unimplemented(
			"SELECT ... ONLY not yet supported in execution plans".to_string(),
		));
	}

	// EXPLAIN clause (query explain output)
	if explain.is_some() {
		return Err(Error::Unimplemented(
			"SELECT ... EXPLAIN not yet supported in execution plans".to_string(),
		));
	}

	// WITH clause (index hints)
	if with.is_some() {
		return Err(Error::Unimplemented(
			"SELECT ... WITH not yet supported in execution plans".to_string(),
		));
	}

	// Extract VERSION timestamp if present (for time-travel queries)
	let version = extract_version(version)?;

	// Build the source plan from `what` (FROM clause)
	let source = plan_select_sources(what, version, ctx)?;

	// Apply WHERE clause if present (before grouping)
	let filtered = if let Some(cond) = cond {
		let predicate = expr_to_physical_expr(cond.0, ctx)?;
		Arc::new(Filter {
			input: source,
			predicate,
		}) as Arc<dyn ExecOperator>
	} else {
		source
	};

	// Apply SPLIT BY if present (before filtering)
	let split = if let Some(splits) = split {
		let idioms: Vec<_> = splits.into_iter().map(|s| s.0).collect();
		Arc::new(Split {
			input: filtered,
			idioms,
		}) as Arc<dyn ExecOperator>
	} else {
		filtered
	};

	// Apply GROUP BY if present
	let (grouped, skip_projections) = if let Some(groups) = group {
		let group_by: Vec<_> = groups.0.into_iter().map(|g| g.0).collect();

		// Build aggregate fields from the SELECT expression
		let aggregates = plan_aggregation(&fields, &group_by, ctx)?;

		// For GROUP BY, the Aggregate operator handles projections internally
		// Skip the separate projection step
		(
			Arc::new(Aggregate {
				input: split,
				group_by,
				aggregates,
			}) as Arc<dyn ExecOperator>,
			true,
		)
	} else {
		(split, false)
	};

	// Apply ORDER BY if present
	// Select the appropriate sort operator based on query characteristics:
	// - RandomShuffle: for ORDER BY RAND()
	// - ExternalSort: when TEMPFILES is specified (disk-based sorting)
	// - SortTopK: when limit is small (heap-based top-k selection)
	// - Sort: default full in-memory sort
	let sorted = if let Some(order) = &order {
		plan_sort(grouped, order, &start, &limit, tempfiles, ctx)?
	} else {
		grouped
	};

	// Apply LIMIT/START if present
	let limited = if limit.is_some() || start.is_some() {
		let limit_expr = if let Some(limit) = &limit {
			Some(expr_to_physical_expr(limit.0.clone(), ctx)?)
		} else {
			None
		};
		let offset_expr = if let Some(start) = &start {
			Some(expr_to_physical_expr(start.0.clone(), ctx)?)
		} else {
			None
		};
		Arc::new(Limit {
			input: sorted,
			limit: limit_expr,
			offset: offset_expr,
		}) as Arc<dyn ExecOperator>
	} else {
		sorted
	};

	// Apply FETCH if present
	// Fetch adds to the projections list.
	let fetched = plan_fetch(fetch, limited, &mut fields)?;

	// Apply projections (SELECT fields or SELECT VALUE)
	// Skip if GROUP BY is present (handled by Aggregate operator)
	let projected = if skip_projections {
		fetched
	} else {
		plan_projections(&fields, &omit, fetched, ctx)?
	};

	// Apply TIMEOUT if present (timeout is always Expr but may be Literal::None)
	let timed = match timeout {
		Expr::Literal(Literal::None) => projected,
		timeout_expr => {
			let timeout_phys = expr_to_physical_expr(timeout_expr, ctx)?;
			Arc::new(Timeout {
				input: projected,
				timeout: Some(timeout_phys),
			}) as Arc<dyn ExecOperator>
		}
	};

	Ok(timed)
}

/// Plan projections (SELECT fields or SELECT VALUE)
///
/// This handles:
/// - `SELECT *` - pass through without projection
/// - `SELECT * OMIT field` - use Project with empty fields and omit populated
/// - `SELECT VALUE expr` - use ProjectValue operator
/// - `SELECT field1, field2` - use Project operator
/// - `SELECT field1, *, field2` - mixed wildcards (returns Unimplemented for now)
fn plan_projections(
	fields: &Fields,
	omit: &[Expr],
	input: Arc<dyn ExecOperator>,
	ctx: &FrozenContext,
) -> Result<Arc<dyn ExecOperator>, Error> {
	match fields {
		// SELECT VALUE expr - return raw values (OMIT doesn't make sense here)
		Fields::Value(selector) => {
			if !omit.is_empty() {
				return Err(Error::Unimplemented(
					"OMIT clause with SELECT VALUE not supported".to_string(),
				));
			}
			let expr = expr_to_physical_expr(selector.expr.clone(), ctx)?;
			Ok(Arc::new(ProjectValue {
				input,
				expr,
			}) as Arc<dyn ExecOperator>)
		}

		// SELECT field1, field2, ... or SELECT *
		Fields::Select(field_list) => {
			// Check if this is just SELECT * (all fields, no specific fields)
			let is_select_all =
				field_list.len() == 1 && matches!(field_list.first(), Some(Field::All));

			if is_select_all {
				// SELECT * - pass through without projection
				// But apply OMIT if present using Project operator
				if !omit.is_empty() {
					let omit_fields = plan_omit_fields(omit.to_vec(), ctx)?;
					return Ok(Arc::new(Project {
						input,
						fields: vec![], // No specific fields - pass through
						omit: omit_fields,
						include_all: true,
					}) as Arc<dyn ExecOperator>);
				}
				return Ok(input);
			}

			// Check for wildcards mixed with specific fields
			let has_wildcard = field_list.iter().any(|f| matches!(f, Field::All));

			// OMIT doesn't make sense with specific field projections (without wildcard)
			if !omit.is_empty() && !has_wildcard {
				return Err(Error::Unimplemented(
					"OMIT clause with specific field projections not supported".to_string(),
				));
			}

			// Build field selections for specific fields (skip wildcards)
			let mut field_selections = Vec::with_capacity(field_list.len());

			for field in field_list {
				if let Field::Single(selector) = field {
					// Determine the output name
					let output_name = if let Some(alias) = &selector.alias {
						// Use alias if provided (extract raw name from single-part idiom)
						idiom_to_field_name(alias)
					} else {
						// Derive name from expression
						derive_field_name(&selector.expr)
					};

					// Convert expression to physical
					let expr = expr_to_physical_expr(selector.expr.clone(), ctx)?;

					field_selections.push(FieldSelection {
						output_name,
						expr,
					});
				}
				// Skip Field::All - handled by include_all flag
			}

			// Handle OMIT if present (only valid with wildcards)
			let omit_fields = if has_wildcard && !omit.is_empty() {
				plan_omit_fields(omit.to_vec(), ctx)?
			} else {
				vec![]
			};

			Ok(Arc::new(Project {
				input,
				fields: field_selections,
				omit: omit_fields,
				include_all: has_wildcard,
			}) as Arc<dyn ExecOperator>)
		}
	}
}

/// Plan OMIT fields - convert expressions to idioms
fn plan_omit_fields(
	omit: Vec<Expr>,
	_ctx: &FrozenContext,
) -> Result<Vec<crate::expr::idiom::Idiom>, Error> {
	let mut fields = Vec::with_capacity(omit.len());

	for expr in omit {
		match expr {
			Expr::Idiom(idiom) => {
				fields.push(idiom);
			}
			_ => {
				// Only simple idiom references are supported for OMIT
				return Err(Error::Unimplemented(
					"OMIT with non-idiom expressions not supported in execution plans".to_string(),
				));
			}
		}
	}

	Ok(fields)
}

/// Derive a field name from an expression for projection output
fn derive_field_name(expr: &Expr) -> String {
	match expr {
		// Simple field reference - extract the raw field name
		Expr::Idiom(idiom) => idiom_to_field_name(idiom),
		// Function call - use the function's idiom representation (name without arguments)
		Expr::FunctionCall(call) => {
			let idiom: crate::expr::idiom::Idiom = call.receiver.to_idiom().into();
			idiom_to_field_name(&idiom)
		}
		// For other expressions, use the SQL representation
		_ => {
			use surrealdb_types::ToSql;
			expr.to_sql()
		}
	}
}

/// Extract a field name from an idiom, preferring raw names for simple idioms.
///
/// This mirrors the legacy behavior where the idiom is simplified (removing
/// Destructure, All, Where, etc.) before deriving the field name.
fn idiom_to_field_name(idiom: &crate::expr::idiom::Idiom) -> String {
	use surrealdb_types::ToSql;

	use crate::expr::part::Part;

	// Simplify the idiom first - this removes Destructure, All, Where, etc.
	// and keeps only Field, Start, and Lookup parts
	let simplified = idiom.simplify();

	// For simple single-part idioms, use the raw field name
	if simplified.len() == 1 {
		if let Some(Part::Field(name)) = simplified.first() {
			return name.to_string();
		}
	}
	// For complex idioms, use the SQL representation of the simplified idiom
	simplified.to_sql()
}

/// Plan FETCH clause
fn plan_fetch(
	fetch: Option<crate::expr::fetch::Fetchs>,
	input: Arc<dyn ExecOperator>,
	projection: &mut Fields,
) -> Result<Arc<dyn ExecOperator>, Error> {
	let Some(fetchs) = fetch else {
		return Ok(input);
	};

	// Convert fetch expressions to idioms
	// We only support simple idiom fetches for now
	let mut fields = Vec::with_capacity(fetchs.len());
	for fetch_item in fetchs {
		// The Fetch struct wraps an Expr in field .0
		match fetch_item.0 {
			Expr::Idiom(idiom) => {
				fields.push(idiom.clone());

				// Add to the final projection list to ensure the fetch is not dropped.
				projection.push(Field::Single(Selector {
					expr: Expr::Idiom(idiom),
					alias: None,
				}))?;
			}
			_ => {
				// Complex fetch expressions (params, function calls) not yet supported
				return Err(Error::Unimplemented(
					"FETCH with non-idiom expressions not yet supported in execution plans"
						.to_string(),
				));
			}
		}
	}

	Ok(Arc::new(Fetch {
		input,
		fields,
	}) as Arc<dyn ExecOperator>)
}

/// Plan aggregation fields from SELECT expression and GROUP BY.
///
/// This extracts:
/// - Group-by keys (passed through unchanged)
/// - Aggregate functions (detected via the function registry)
/// - Other expressions (evaluated with the first value in the group)
fn plan_aggregation(
	fields: &Fields,
	group_by: &[crate::expr::idiom::Idiom],
	ctx: &FrozenContext,
) -> Result<Vec<AggregateField>, Error> {
	// Use the global built-in function registry
	let registry = crate::exec::function::FunctionRegistry::with_builtins();

	match fields {
		// SELECT VALUE with GROUP BY - the VALUE expression may contain aggregates
		Fields::Value(selector) => {
			// Check if the VALUE expression is a group-by key
			let is_group_key = is_group_key_expression(&selector.expr, group_by);

			let (aggregate_info, fallback_expr) = if is_group_key {
				// Group-by key - no aggregate, no fallback expr needed
				(None, None)
			} else {
				// Try to extract aggregate function info
				extract_aggregate_info(&selector.expr, &registry, ctx)?
			};

			// For VALUE, we use an empty name since the result isn't wrapped in an object
			Ok(vec![AggregateField {
				name: String::new(),
				is_group_key,
				aggregate_info,
				fallback_expr,
			}])
		}

		// SELECT field1, field2, ... with GROUP BY
		Fields::Select(field_list) => {
			let mut aggregates = Vec::with_capacity(field_list.len());

			for field in field_list {
				match field {
					Field::All => {
						// SELECT * with GROUP BY doesn't make sense
						return Err(Error::Unimplemented(
							"SELECT * with GROUP BY not supported in execution plans".to_string(),
						));
					}
					Field::Single(selector) => {
						// Determine the output name
						let output_name = if let Some(alias) = &selector.alias {
							idiom_to_field_name(alias)
						} else {
							derive_field_name(&selector.expr)
						};

						// Check if this is a group-by key
						let is_group_key = is_group_key_expression(&selector.expr, group_by);

						let (aggregate_info, fallback_expr) = if is_group_key {
							// Group-by key - no aggregate, no fallback expr needed
							(None, None)
						} else {
							// Try to extract aggregate function info
							extract_aggregate_info(&selector.expr, &registry, ctx)?
						};

						aggregates.push(AggregateField {
							name: output_name,
							is_group_key,
							aggregate_info,
							fallback_expr,
						});
					}
				}
			}

			Ok(aggregates)
		}
	}
}

/// Check if an expression is a direct aggregate function call.
fn is_aggregate_function(
	expr: &Expr,
	registry: &crate::exec::function::FunctionRegistry,
) -> bool {
	if let Expr::FunctionCall(func_call) = expr {
		if let Function::Normal(name) = &func_call.receiver {
			return registry.get_aggregate(name.as_str()).is_some();
		}
	}
	false
}

/// Recursively check if an expression contains any aggregate function.
fn contains_aggregate(expr: &Expr, registry: &crate::exec::function::FunctionRegistry) -> bool {
	match expr {
		Expr::FunctionCall(func_call) => {
			// Check if this function itself is an aggregate
			if let Function::Normal(name) = &func_call.receiver {
				if registry.get_aggregate(name.as_str()).is_some() {
					return true;
				}
			}
			// Check arguments recursively
			func_call.arguments.iter().any(|arg| contains_aggregate(arg, registry))
		}
		Expr::Binary {
			left,
			right,
			..
		} => contains_aggregate(left, registry) || contains_aggregate(right, registry),
		Expr::Prefix {
			expr,
			..
		} => contains_aggregate(expr, registry),
		Expr::Postfix {
			expr,
			..
		} => contains_aggregate(expr, registry),
		// Other expression types don't contain aggregates
		_ => false,
	}
}

/// Result of extracting a nested aggregate from an expression.
struct NestedAggregateExtraction {
	/// The aggregate function call that was found.
	func_call: FunctionCall,
	/// The original expression with the aggregate replaced by `$this`.
	/// This becomes the post-aggregate expression.
	post_expr: Expr,
}

/// Extract a single aggregate function from an expression, replacing it with `$this`.
/// Returns None if no aggregate is found, or if there are multiple/nested aggregates.
fn extract_nested_aggregate(
	expr: &Expr,
	registry: &crate::exec::function::FunctionRegistry,
) -> Option<NestedAggregateExtraction> {
	match expr {
		Expr::FunctionCall(func_call) => {
			if let Function::Normal(name) = &func_call.receiver {
				if registry.get_aggregate(name.as_str()).is_some() {
					// Check for nested aggregates in arguments (not allowed)
					for arg in &func_call.arguments {
						if contains_aggregate(arg, registry) {
							return None; // Nested aggregates not supported
						}
					}
					// This is an aggregate at the top level - no outer expression needed
					return None;
				}
			}
			// Not an aggregate - check arguments for nested aggregate
			// (but this is a function call containing an aggregate, which we handle differently)
			None
		}
		Expr::Binary {
			left,
			op,
			right,
		} => {
			let left_has_agg = contains_aggregate(left, registry);
			let right_has_agg = contains_aggregate(right, registry);

			if left_has_agg && right_has_agg {
				// Multiple aggregates in one expression - not supported in this simple model
				// TODO: Support multiple aggregates by extracting each separately
				return None;
			}

			if left_has_agg {
				// Check if left is directly an aggregate
				if is_aggregate_function(left, registry) {
					if let Expr::FunctionCall(func_call) = left.as_ref() {
						return Some(NestedAggregateExtraction {
							func_call: func_call.as_ref().clone(),
							post_expr: Expr::Binary {
								left: Box::new(Expr::Param(crate::expr::Param::from("this".to_string()))),
								op: op.clone(),
								right: right.clone(),
							},
						});
					}
				}
				// Left contains aggregate deeper - recurse
				if let Some(nested) = extract_nested_aggregate(left, registry) {
					return Some(NestedAggregateExtraction {
						func_call: nested.func_call,
						post_expr: Expr::Binary {
							left: Box::new(nested.post_expr),
							op: op.clone(),
							right: right.clone(),
						},
					});
				}
			}

			if right_has_agg {
				// Check if right is directly an aggregate
				if is_aggregate_function(right, registry) {
					if let Expr::FunctionCall(func_call) = right.as_ref() {
						return Some(NestedAggregateExtraction {
							func_call: func_call.as_ref().clone(),
							post_expr: Expr::Binary {
								left: left.clone(),
								op: op.clone(),
								right: Box::new(Expr::Param(crate::expr::Param::from("this".to_string()))),
							},
						});
					}
				}
				// Right contains aggregate deeper - recurse
				if let Some(nested) = extract_nested_aggregate(right, registry) {
					return Some(NestedAggregateExtraction {
						func_call: nested.func_call,
						post_expr: Expr::Binary {
							left: left.clone(),
							op: op.clone(),
							right: Box::new(nested.post_expr),
						},
					});
				}
			}

			None
		}
		Expr::Prefix {
			op,
			expr: inner,
		} => {
			if is_aggregate_function(inner, registry) {
				if let Expr::FunctionCall(func_call) = inner.as_ref() {
					return Some(NestedAggregateExtraction {
						func_call: func_call.as_ref().clone(),
						post_expr: Expr::Prefix {
							op: op.clone(),
							expr: Box::new(Expr::Param(crate::expr::Param::from("this".to_string()))),
						},
					});
				}
			}
			// Check for deeper nested aggregate
			if let Some(nested) = extract_nested_aggregate(inner, registry) {
				return Some(NestedAggregateExtraction {
					func_call: nested.func_call,
					post_expr: Expr::Prefix {
						op: op.clone(),
						expr: Box::new(nested.post_expr),
					},
				});
			}
			None
		}
		Expr::Postfix {
			expr: inner,
			op,
		} => {
			if is_aggregate_function(inner, registry) {
				if let Expr::FunctionCall(func_call) = inner.as_ref() {
					return Some(NestedAggregateExtraction {
						func_call: func_call.as_ref().clone(),
						post_expr: Expr::Postfix {
							expr: Box::new(Expr::Param(crate::expr::Param::from("this".to_string()))),
							op: op.clone(),
						},
					});
				}
			}
			// Check for deeper nested aggregate
			if let Some(nested) = extract_nested_aggregate(inner, registry) {
				return Some(NestedAggregateExtraction {
					func_call: nested.func_call,
					post_expr: Expr::Postfix {
						expr: Box::new(nested.post_expr),
						op: op.clone(),
					},
				});
			}
			None
		}
		_ => None,
	}
}

/// Extract aggregate function information from an expression.
///
/// If the expression is a direct aggregate function call (e.g., `math::mean(a)`),
/// returns the aggregate info with the function and argument expression.
///
/// If the expression contains a nested aggregate (e.g., `math::mean(v) + 1`),
/// extracts the aggregate and creates a post-aggregate expression.
///
/// If the expression is not an aggregate, returns a fallback physical expression
/// that will be evaluated to get the first value in the group.
fn extract_aggregate_info(
	expr: &Expr,
	registry: &crate::exec::function::FunctionRegistry,
	ctx: &FrozenContext,
) -> Result<(Option<AggregateInfo>, Option<Arc<dyn crate::exec::PhysicalExpr>>), Error> {
	// Check if this is a direct aggregate function call at the top level
	if let Expr::FunctionCall(func_call) = expr {
		if let Function::Normal(name) = &func_call.receiver {
			// Look up the function in the aggregate registry
			if let Some(agg_func) = registry.get_aggregate(name.as_str()) {
				// This is a registered aggregate function
				// Extract the first argument expression (the accumulated value)
				let argument_expr = if func_call.arguments.is_empty() {
					// For count() with no args, use a dummy literal
					expr_to_physical_expr(Expr::Literal(Literal::None), ctx)?
				} else {
					// First argument is the one to accumulate per-row
					expr_to_physical_expr(func_call.arguments[0].clone(), ctx)?
				};

				// Convert any additional arguments to extra_args
				// These are evaluated once per group (e.g., separator in array::join)
				let extra_args = if func_call.arguments.len() > 1 {
					func_call.arguments[1..]
						.iter()
						.map(|arg| expr_to_physical_expr(arg.clone(), ctx))
						.collect::<Result<Vec<_>, _>>()?
				} else {
					vec![]
				};

				return Ok((
					Some(AggregateInfo {
						function: agg_func.clone(),
						argument_expr,
						extra_args,
						post_aggregate_expr: None,
					}),
					None,
				));
			}
		}
	}

	// Check if this expression contains a nested aggregate (e.g., `math::mean(v) + 1`)
	if let Some(extraction) = extract_nested_aggregate(expr, registry) {
		// Found a nested aggregate - extract it
		if let Function::Normal(name) = &extraction.func_call.receiver {
			if let Some(agg_func) = registry.get_aggregate(name.as_str()) {
				// Extract the argument expression for the aggregate
				let argument_expr = if extraction.func_call.arguments.is_empty() {
					expr_to_physical_expr(Expr::Literal(Literal::None), ctx)?
				} else {
					expr_to_physical_expr(extraction.func_call.arguments[0].clone(), ctx)?
				};

				// Convert any additional arguments to extra_args
				let extra_args = if extraction.func_call.arguments.len() > 1 {
					extraction.func_call.arguments[1..]
						.iter()
						.map(|arg| expr_to_physical_expr(arg.clone(), ctx))
						.collect::<Result<Vec<_>, _>>()?
				} else {
					vec![]
				};

				// Convert the post-aggregate expression to physical form
				let post_aggregate_expr = expr_to_physical_expr(extraction.post_expr, ctx)?;

				return Ok((
					Some(AggregateInfo {
						function: agg_func.clone(),
						argument_expr,
						extra_args,
						post_aggregate_expr: Some(post_aggregate_expr),
					}),
					None,
				));
			}
		}
	}

	// Not an aggregate function - use implicit array::group aggregation
	// This collects all values into an array (SurrealDB's default GROUP BY behavior)
	let argument_expr = expr_to_physical_expr(expr.clone(), ctx)?;
	let array_group = registry
		.get_aggregate("array::group")
		.expect("array::group should always be registered")
		.clone();
	Ok((
		Some(AggregateInfo {
			function: array_group,
			argument_expr,
			extra_args: vec![],
			post_aggregate_expr: None,
		}),
		None,
	))
}

/// Check if an expression is a group-by key reference.
fn is_group_key_expression(expr: &Expr, group_by: &[crate::expr::idiom::Idiom]) -> bool {
	use surrealdb_types::ToSql;

	// Only simple idiom expressions can be group keys
	if let Expr::Idiom(idiom) = expr {
		// Check if this idiom matches any group-by idiom
		return group_by.iter().any(|g| g.to_sql() == idiom.to_sql());
	}
	false
}

/// Extract version timestamp from VERSION clause expression.
/// Currently only supports literal Datetime values.
fn extract_version(version_expr: Expr) -> Result<Option<u64>, Error> {
	match version_expr {
		Expr::Literal(Literal::None) => Ok(None),
		Expr::Literal(Literal::Datetime(dt)) => {
			let stamp = dt
				.to_version_stamp()
				.map_err(|e| Error::Unimplemented(format!("Invalid VERSION timestamp: {}", e)))?;
			Ok(Some(stamp))
		}
		_ => Err(Error::Unimplemented(
			"VERSION clause only supports literal datetime values in execution plans".to_string(),
		)),
	}
}

/// Plan the FROM sources - handles multiple targets with Union
///
/// The `version` parameter is an optional timestamp for time-travel queries (VERSION clause).
fn plan_select_sources(
	what: Vec<Expr>,
	version: Option<u64>,
	ctx: &FrozenContext,
) -> Result<Arc<dyn ExecOperator>, Error> {
	if what.is_empty() {
		return Err(Error::Unimplemented("SELECT requires at least one source".to_string()));
	}

	// Convert each source to a plan
	let mut source_plans = Vec::with_capacity(what.len());
	for expr in what {
		let plan = plan_single_source(expr, version, ctx)?;
		source_plans.push(plan);
	}

	// If multiple sources, wrap in Union; otherwise just return the single source
	if source_plans.len() == 1 {
		Ok(source_plans.pop().unwrap())
	} else {
		Ok(Arc::new(Union {
			inputs: source_plans,
		}))
	}
}

/// Plan a single FROM source (table or record ID)
///
/// The `version` parameter is an optional timestamp for time-travel queries (VERSION clause).
/// Scan handles KV store sources: table names, record IDs (point or range).
/// SourceExpr handles value sources: arrays, scalars, computed expressions.
fn plan_single_source(
	expr: Expr,
	version: Option<u64>,
	ctx: &FrozenContext,
) -> Result<Arc<dyn ExecOperator>, Error> {
	use crate::val::Value;

	match expr {
		// Table name: SELECT * FROM users
		Expr::Table(table_name) => {
			// Convert table name to a literal string for the physical expression
			let table_expr = expr_to_physical_expr(
				Expr::Literal(crate::expr::literal::Literal::String(
					table_name.as_str().to_string(),
				)),
				ctx,
			)?;
			Ok(Arc::new(Scan {
				source: table_expr,
				version,
			}) as Arc<dyn ExecOperator>)
		}

		// Record ID literal: SELECT * FROM users:123
		// Scan handles record IDs internally via ScanTarget::RecordId
		Expr::Literal(crate::expr::literal::Literal::RecordId(record_id_lit)) => {
			// Convert the record ID literal to an expression that Scan can evaluate
			// Scan will handle point lookups and range scans internally
			let table_expr = expr_to_physical_expr(
				Expr::Literal(crate::expr::literal::Literal::RecordId(record_id_lit)),
				ctx,
			)?;
			Ok(Arc::new(Scan {
				source: table_expr,
				version,
			}) as Arc<dyn ExecOperator>)
		}

		// Subquery: SELECT * FROM (SELECT * FROM table)
		Expr::Select(inner_select) => {
			// Recursively plan the inner SELECT
			plan_select(*inner_select, ctx)
		}

		// Array literal: SELECT * FROM [1, 2, 3]
		Expr::Literal(crate::expr::literal::Literal::Array(_)) => {
			// Convert to SourceExpr which will unnest the array elements
			let phys_expr = expr_to_physical_expr(expr, ctx)?;
			Ok(Arc::new(SourceExpr {
				expr: phys_expr,
			}) as Arc<dyn ExecOperator>)
		}

		// Parameter: SELECT * FROM $param
		// Inspect the parameter value to determine if it's a KV source or value source
		Expr::Param(param) => {
			match ctx.value(param.as_str()) {
				Some(Value::Table(_)) | Some(Value::RecordId(_)) => {
					// KV store source → Scan
					let table_expr = expr_to_physical_expr(Expr::Param(param.clone()), ctx)?;
					Ok(Arc::new(Scan {
						source: table_expr,
						version,
					}) as Arc<dyn ExecOperator>)
				}
				Some(_) | None => {
					// Array, scalar, or unknown → SourceExpr
					let phys_expr = expr_to_physical_expr(Expr::Param(param), ctx)?;
					Ok(Arc::new(SourceExpr {
						expr: phys_expr,
					}) as Arc<dyn ExecOperator>)
				}
			}
		}

		// Idiom that might be a table or record reference
		// TODO: I think Idiom can just be treated as a SourceExpr
		// Expr::Idiom(idiom) => {
		// 	// Simple idiom (just a name) is a table reference
		// 	// Convert to a table scan using the idiom as a physical expression
		// 	let table_expr = expr_to_physical_expr(Expr::Idiom(idiom.clone()))?;
		// 	Ok(Arc::new(Scan {
		// 		source: table_expr,
		// 		version,
		// 	}) as Arc<dyn OperatorPlan>)
		// }

		// Other expressions (strings, objects, etc.) → SourceExpr
		other => {
			let phys_expr = expr_to_physical_expr(other, ctx)?;
			Ok(Arc::new(SourceExpr {
				expr: phys_expr,
			}) as Arc<dyn ExecOperator>)
		}
	}
}

/// Convert a LET statement to an execution plan
fn convert_let_statement(
	let_stmt: crate::expr::statements::SetStatement,
	ctx: &FrozenContext,
) -> Result<Arc<dyn ExecOperator>, Error> {
	let crate::expr::statements::SetStatement {
		name,
		what,
		kind: _,
	} = let_stmt;

	// Determine if the expression is a query or scalar
	let value: Arc<dyn ExecOperator> = match what {
		// SELECT produces a stream that gets collected into an array
		Expr::Select(select) => plan_select(*select, ctx)?,

		// DML statements in LET are not yet supported
		Expr::Create(_) => {
			return Err(Error::Unimplemented(
				"CREATE statements in LET not yet supported in execution plans".to_string(),
			));
		}
		Expr::Update(_) => {
			return Err(Error::Unimplemented(
				"UPDATE statements in LET not yet supported in execution plans".to_string(),
			));
		}
		Expr::Upsert(_) => {
			return Err(Error::Unimplemented(
				"UPSERT statements in LET not yet supported in execution plans".to_string(),
			));
		}
		Expr::Delete(_) => {
			return Err(Error::Unimplemented(
				"DELETE statements in LET not yet supported in execution plans".to_string(),
			));
		}
		Expr::Insert(_) => {
			return Err(Error::Unimplemented(
				"INSERT statements in LET not yet supported in execution plans".to_string(),
			));
		}
		Expr::Relate(_) => {
			return Err(Error::Unimplemented(
				"RELATE statements in LET not yet supported in execution plans".to_string(),
			));
		}

		// Everything else is a scalar expression - wrap in ExprPlan
		other => {
			let expr = expr_to_physical_expr(other.clone(), ctx)?;

			// Validate: LET expressions can't reference current row
			if expr.references_current_value() {
				return Err(Error::Unimplemented(
					"LET expression cannot reference current row context".to_string(),
				));
			}

			Arc::new(ExprPlan {
				expr,
			}) as Arc<dyn ExecOperator>
		}
	};

	Ok(Arc::new(LetPlan {
		name,
		value,
	}) as Arc<dyn ExecOperator>)
}

/// Plan ORDER BY clause by selecting the appropriate sort operator.
///
/// This function chooses the optimal sort operator based on query characteristics:
/// - `RandomShuffle`: for ORDER BY RAND()
/// - `ExternalSort`: when TEMPFILES is specified (disk-based sorting)
/// - `SortTopK`: when limit is small (heap-based top-k selection)
/// - `Sort`: default full in-memory sort with parallel sorting
fn plan_sort(
	input: Arc<dyn ExecOperator>,
	order: &crate::expr::order::Ordering,
	start: &Option<crate::expr::start::Start>,
	limit: &Option<crate::expr::limit::Limit>,
	#[allow(unused)] tempfiles: bool,
	ctx: &FrozenContext,
) -> Result<Arc<dyn ExecOperator>, Error> {
	use crate::expr::order::Ordering;

	match order {
		Ordering::Random => {
			// ORDER BY RAND() - use RandomShuffle operator
			// Try to get effective limit if both start and limit are literals
			let effective_limit = get_effective_limit_literal(start, limit);
			Ok(Arc::new(RandomShuffle {
				input,
				limit: effective_limit,
			}) as Arc<dyn ExecOperator>)
		}
		Ordering::Order(order_list) => {
			// Convert order list to OrderByField vec
			let order_by = convert_order_list(order_list, ctx)?;

			// Check if we should use ExternalSort (TEMPFILES specified)
			#[cfg(storage)]
			if tempfiles {
				if let Some(temp_dir) = ctx.temporary_directory() {
					return Ok(Arc::new(ExternalSort {
						input,
						order_by,
						temp_dir: temp_dir.to_path_buf(),
					}) as Arc<dyn ExecOperator>);
				}
			}

			// Check if we should use SortTopK (small limit)
			if let Some(effective_limit) = get_effective_limit_literal(start, limit) {
				if effective_limit <= *MAX_ORDER_LIMIT_PRIORITY_QUEUE_SIZE as usize {
					return Ok(Arc::new(SortTopK {
						input,
						order_by,
						limit: effective_limit,
					}) as Arc<dyn ExecOperator>);
				}
			}

			// Default: full in-memory sort with parallel sorting
			Ok(Arc::new(Sort {
				input,
				order_by,
			}) as Arc<dyn ExecOperator>)
		}
	}
}

/// Convert an OrderList to a Vec of OrderByField.
fn convert_order_list(
	order_list: &crate::expr::order::OrderList,
	ctx: &FrozenContext,
) -> Result<Vec<OrderByField>, Error> {
	let mut fields = Vec::with_capacity(order_list.len());
	for order_field in order_list.iter() {
		// Convert idiom to physical expression
		let expr: Arc<dyn crate::exec::PhysicalExpr> =
			convert_idiom_to_physical_expr(&order_field.value, ctx)?;

		let direction = if order_field.direction {
			SortDirection::Asc
		} else {
			SortDirection::Desc
		};

		fields.push(OrderByField {
			expr,
			direction,
			collate: order_field.collate,
			numeric: order_field.numeric,
		});
	}
	Ok(fields)
}

/// Try to get the effective limit (start + limit) if both are literals.
///
/// Returns None if either value is not a literal or cannot be evaluated at plan time.
fn get_effective_limit_literal(
	start: &Option<crate::expr::start::Start>,
	limit: &Option<crate::expr::limit::Limit>,
) -> Option<usize> {
	// Get limit value if it's a literal
	let limit_val = limit.as_ref().and_then(|l| match &l.0 {
		Expr::Literal(Literal::Integer(n)) if *n >= 0 => Some(*n as usize),
		Expr::Literal(Literal::Float(n)) if *n >= 0.0 => Some(*n as usize),
		_ => None,
	})?;

	// Get start value if it's a literal (default to 0)
	let start_val = start
		.as_ref()
		.map(|s| match &s.0 {
			Expr::Literal(Literal::Integer(n)) if *n >= 0 => Some(*n as usize),
			Expr::Literal(Literal::Float(n)) if *n >= 0.0 => Some(*n as usize),
			_ => None,
		})
		.unwrap_or(Some(0))?;

	Some(start_val + limit_val)
}

// ============================================================================
// Idiom Conversion Functions
// ============================================================================

use crate::exec::physical_expr::IdiomExpr;
use crate::exec::physical_part::{
	LookupDirection, PhysicalDestructurePart, PhysicalLookup, PhysicalPart, PhysicalRecurse,
	PhysicalRecurseInstruction,
};
use crate::expr::part::{DestructurePart, Part, RecurseInstruction};

/// Convert an idiom to a physical expression.
///
/// All idioms are converted to `IdiomExpr` which handles runtime type checking
/// (e.g., fetching records when accessing fields on RecordIds).
fn convert_idiom_to_physical_expr(
	idiom: &crate::expr::idiom::Idiom,
	ctx: &FrozenContext,
) -> Result<Arc<dyn crate::exec::PhysicalExpr>, Error> {
	// Always convert all parts - runtime handles type-specific behavior
	let physical_parts = convert_parts_to_physical(&idiom.0, ctx)?;
	Ok(Arc::new(IdiomExpr::new(idiom.clone(), physical_parts)))
}

/// Convert idiom parts to physical parts.
fn convert_parts_to_physical(
	parts: &[Part],
	ctx: &FrozenContext,
) -> Result<Vec<PhysicalPart>, Error> {
	let mut physical_parts = Vec::with_capacity(parts.len());

	for part in parts {
		let physical_part = convert_single_part(part, ctx)?;
		physical_parts.push(physical_part);
	}

	Ok(physical_parts)
}

/// Convert a single Part to a PhysicalPart.
fn convert_single_part(part: &Part, ctx: &FrozenContext) -> Result<PhysicalPart, Error> {
	match part {
		Part::Field(name) => Ok(PhysicalPart::Field(name.clone())),

		Part::Value(expr) => {
			let phys_expr = expr_to_physical_expr(expr.clone(), ctx)?;
			Ok(PhysicalPart::Index(phys_expr))
		}

		Part::All => Ok(PhysicalPart::All),
		Part::Flatten => Ok(PhysicalPart::Flatten),
		Part::First => Ok(PhysicalPart::First),
		Part::Last => Ok(PhysicalPart::Last),
		Part::Optional => Ok(PhysicalPart::Optional),

		Part::Where(expr) => {
			let phys_expr = expr_to_physical_expr(expr.clone(), ctx)?;
			Ok(PhysicalPart::Where(phys_expr))
		}

		Part::Method(name, args) => {
			let mut phys_args = Vec::with_capacity(args.len());
			for arg in args {
				phys_args.push(expr_to_physical_expr(arg.clone(), ctx)?);
			}
			Ok(PhysicalPart::Method {
				name: name.clone(),
				args: phys_args,
			})
		}

		Part::Destructure(parts) => {
			let phys_parts = convert_destructure_parts(parts, ctx)?;
			Ok(PhysicalPart::Destructure(phys_parts))
		}

		Part::Start(_) => {
			// Start parts are handled at the idiom level, not as individual parts
			Err(Error::Unimplemented(
				"Start parts should be handled at the idiom level".to_string(),
			))
		}

		Part::Lookup(lookup) => {
			// Lookups need special handling - create a plan
			let plan = plan_lookup(lookup, ctx)?;
			let direction = match &lookup.kind {
				crate::expr::lookup::LookupKind::Graph(dir) => LookupDirection::from(dir),
				crate::expr::lookup::LookupKind::Reference => LookupDirection::Reference,
			};
			// Extract edge tables from the lookup subjects
			let edge_tables: Vec<_> = lookup
				.what
				.iter()
				.map(|s| match s {
					crate::expr::lookup::LookupSubject::Table {
						table,
						..
					} => table.clone(),
					crate::expr::lookup::LookupSubject::Range {
						table,
						..
					} => table.clone(),
				})
				.collect();
			Ok(PhysicalPart::Lookup(PhysicalLookup {
				direction,
				edge_tables,
				plan,
				alias: lookup.alias.clone(),
			}))
		}

		Part::Recurse(recurse, inner_path, instruction) => {
			let (min_depth, max_depth) = match recurse {
				crate::expr::part::Recurse::Fixed(n) => (*n, Some(*n)),
				crate::expr::part::Recurse::Range(min, max) => (min.unwrap_or(1), *max),
			};

			let path = if let Some(p) = inner_path {
				convert_parts_to_physical(&p.0, ctx)?
			} else {
				vec![]
			};

			let instr = convert_recurse_instruction(instruction, ctx)?;

			Ok(PhysicalPart::Recurse(PhysicalRecurse {
				min_depth,
				max_depth,
				path,
				instruction: instr,
				inclusive: matches!(
					instruction,
					Some(RecurseInstruction::Path {
						inclusive: true,
						..
					}) | Some(RecurseInstruction::Collect {
						inclusive: true,
						..
					}) | Some(RecurseInstruction::Shortest {
						inclusive: true,
						..
					})
				),
			}))
		}

		Part::Doc => {
			// Doc ($) refers to the document, which is the current value
			// This should be handled at the idiom level
			Ok(PhysicalPart::Field("id".to_string()))
		}

		Part::RepeatRecurse => {
			// RepeatRecurse (@) is handled within recursion context
			Err(Error::Unimplemented(
				"RepeatRecurse should be handled within recursion context".to_string(),
			))
		}
	}
}

/// Convert destructure parts to physical destructure parts.
fn convert_destructure_parts(
	parts: &[DestructurePart],
	ctx: &FrozenContext,
) -> Result<Vec<PhysicalDestructurePart>, Error> {
	let mut physical_parts = Vec::with_capacity(parts.len());

	for part in parts {
		let phys_part = match part {
			DestructurePart::All(field) => PhysicalDestructurePart::All(field.clone()),
			DestructurePart::Field(field) => PhysicalDestructurePart::Field(field.clone()),
			DestructurePart::Aliased(field, idiom) => {
				let path = convert_parts_to_physical(&idiom.0, ctx)?;
				PhysicalDestructurePart::Aliased {
					field: field.clone(),
					path,
				}
			}
			DestructurePart::Destructure(field, nested) => {
				let nested_parts = convert_destructure_parts(nested, ctx)?;
				PhysicalDestructurePart::Nested {
					field: field.clone(),
					parts: nested_parts,
				}
			}
		};
		physical_parts.push(phys_part);
	}

	Ok(physical_parts)
}

/// Convert a RecurseInstruction to a PhysicalRecurseInstruction.
fn convert_recurse_instruction(
	instruction: &Option<RecurseInstruction>,
	ctx: &FrozenContext,
) -> Result<PhysicalRecurseInstruction, Error> {
	match instruction {
		None => Ok(PhysicalRecurseInstruction::Default),
		Some(RecurseInstruction::Collect {
			..
		}) => Ok(PhysicalRecurseInstruction::Collect),
		Some(RecurseInstruction::Path {
			..
		}) => Ok(PhysicalRecurseInstruction::Path),
		Some(RecurseInstruction::Shortest {
			expects,
			..
		}) => {
			let target = expr_to_physical_expr(expects.clone(), ctx)?;
			Ok(PhysicalRecurseInstruction::Shortest {
				target,
			})
		}
	}
}

/// Plan a Lookup operation, creating the operator tree.
fn plan_lookup(
	lookup: &crate::expr::lookup::Lookup,
	ctx: &FrozenContext,
) -> Result<Arc<dyn ExecOperator>, Error> {
	use crate::exec::operators::{Filter, GraphEdgeScan, GraphScanOutput, Limit, ReferenceScan};

	// Determine the source expression (current value's record ID)
	// For now, use a literal placeholder - the actual source binding happens at eval time
	let source_expr: Arc<dyn crate::exec::PhysicalExpr> =
		Arc::new(crate::exec::physical_expr::Literal(crate::val::Value::None));

	// Create the base scan operator
	let base_scan: Arc<dyn ExecOperator> = match &lookup.kind {
		crate::expr::lookup::LookupKind::Graph(dir) => {
			// Convert lookup subjects to table names
			let edge_tables: Vec<_> = lookup
				.what
				.iter()
				.map(|s| match s {
					crate::expr::lookup::LookupSubject::Table {
						table,
						..
					} => table.clone(),
					crate::expr::lookup::LookupSubject::Range {
						table,
						..
					} => table.clone(),
				})
				.collect();

			Arc::new(GraphEdgeScan {
				source: source_expr,
				direction: LookupDirection::from(dir),
				edge_tables,
				output_mode: GraphScanOutput::TargetId,
			})
		}
		crate::expr::lookup::LookupKind::Reference => {
			// For references, we need the referencing table
			let (referencing_table, referencing_field) = lookup
				.what
				.first()
				.map(|s| match s {
					crate::expr::lookup::LookupSubject::Table {
						table,
						referencing_field,
					} => (table.clone(), referencing_field.clone()),
					crate::expr::lookup::LookupSubject::Range {
						table,
						referencing_field,
						..
					} => (table.clone(), referencing_field.clone()),
				})
				.unwrap_or_else(|| ("unknown".into(), None));

			Arc::new(ReferenceScan {
				source: source_expr,
				referencing_table,
				referencing_field,
			})
		}
	};

	// Apply filter if present
	let filtered: Arc<dyn ExecOperator> = if let Some(cond) = &lookup.cond {
		let predicate = expr_to_physical_expr(cond.0.clone(), ctx)?;
		Arc::new(Filter {
			input: base_scan,
			predicate,
		})
	} else {
		base_scan
	};

	// Apply limit if present
	let limited: Arc<dyn ExecOperator> = if lookup.limit.is_some() || lookup.start.is_some() {
		let limit_expr =
			lookup.limit.as_ref().map(|l| expr_to_physical_expr(l.0.clone(), ctx)).transpose()?;
		let offset_expr =
			lookup.start.as_ref().map(|s| expr_to_physical_expr(s.0.clone(), ctx)).transpose()?;
		Arc::new(Limit {
			input: filtered,
			limit: limit_expr,
			offset: offset_expr,
		})
	} else {
		filtered
	};

	// TODO: Add Sort, Split, Aggregate, and Project as needed

	Ok(limited)
}

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
		// Test a simple literal
		let expr = Expr::Literal(crate::expr::literal::Literal::Integer(42));

		let ctx = Arc::new(Context::background());
		let plan = try_plan_expr(expr, &ctx).expect("Planning failed");

		assert_eq!(plan.name(), "Expr");
		assert!(plan.is_scalar());
	}
}
