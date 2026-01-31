use std::sync::Arc;

use crate::err::Error;
use crate::exec::operators::{
	Aggregate, AggregateField, AggregateType, ComputeFields, ExprPlan, Fetch, FieldSelection,
	Filter, LetPlan, Limit, Omit, Project, ProjectValue, RecordIdLookup, Scan, Sort, Split,
	Timeout, Union,
};
use crate::exec::OperatorPlan;
use crate::expr::field::{Field, Fields};
use crate::expr::{Expr, Function, Literal};
use crate::val::TableName;

pub(crate) fn expr_to_physical_expr(
	expr: Expr,
) -> Result<Arc<dyn crate::exec::PhysicalExpr>, Error> {
	use crate::exec::physical_expr::{
		BinaryOp, Field, Literal as PhysicalLiteral, Param, PostfixOp, UnaryOp,
	};

	match expr {
		Expr::Literal(lit) => {
			// Convert the logical Literal to a physical Value
			let value = literal_to_value(lit)?;
			Ok(Arc::new(PhysicalLiteral(value)))
		}
		Expr::Param(param) => Ok(Arc::new(Param(param.as_str().to_string()))),
		Expr::Idiom(idiom) => Ok(Arc::new(Field(idiom))),
		Expr::Binary {
			left,
			op,
			right,
		} => {
			let left_phys = expr_to_physical_expr(*left)?;
			let right_phys = expr_to_physical_expr(*right)?;
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
			let inner = expr_to_physical_expr(*expr)?;
			Ok(Arc::new(UnaryOp {
				op,
				expr: inner,
			}))
		}
		Expr::Postfix {
			op,
			expr,
		} => {
			let inner = expr_to_physical_expr(*expr)?;
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
		_ => Err(Error::Unimplemented(format!(
			"Expression type not yet supported in execution plans: {:?}",
			std::mem::discriminant(&expr)
		))),
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
		// These require async computation, not allowed for USE statements
		Literal::RecordId(_) => Err(Error::Unimplemented(
			"RecordId literals in USE statements not yet supported".to_string(),
		)),
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

pub(crate) fn try_plan_expr(expr: &Expr) -> Result<Arc<dyn OperatorPlan>, Error> {
	match expr {
		// Supported statements
		Expr::Select(select) => plan_select(select),
		Expr::Let(let_stmt) => convert_let_statement(let_stmt),

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

		// Other statements - not yet supported
		Expr::Info(_) => Err(Error::Unimplemented(
			"INFO statements not yet supported in execution plans".to_string(),
		)),
		Expr::Foreach(_) => Err(Error::Unimplemented(
			"FOR statements not yet supported in execution plans".to_string(),
		)),
		Expr::IfElse(_) => Err(Error::Unimplemented(
			"IF statements not yet supported in execution plans".to_string(),
		)),
		Expr::Block(_) => Err(Error::Unimplemented(
			"Block expressions not yet supported in execution plans".to_string(),
		)),
		Expr::FunctionCall(_) => Err(Error::Unimplemented(
			"Function call expressions not yet supported in execution plans".to_string(),
		)),
		Expr::Closure(_) => Err(Error::Unimplemented(
			"Closure expressions not yet supported in execution plans".to_string(),
		)),
		Expr::Return(_) => Err(Error::Unimplemented(
			"RETURN statements not yet supported in execution plans".to_string(),
		)),
		Expr::Throw(_) => Err(Error::Unimplemented(
			"THROW statements not yet supported in execution plans".to_string(),
		)),
		Expr::Break => Err(Error::Unimplemented(
			"BREAK statements not yet supported in execution plans".to_string(),
		)),
		Expr::Continue => Err(Error::Unimplemented(
			"CONTINUE statements not yet supported in execution plans".to_string(),
		)),
		Expr::Sleep(_) => Err(Error::Unimplemented(
			"SLEEP statements not yet supported in execution plans".to_string(),
		)),

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
			let phys_expr = expr_to_physical_expr(expr.clone())?;
			// Validate that the expression doesn't require row context
			if phys_expr.references_current_value() {
				return Err(Error::Unimplemented(
					"Expression references row context but no table specified".to_string(),
				));
			}
			Ok(Arc::new(ExprPlan {
				expr: phys_expr,
			}) as Arc<dyn OperatorPlan>)
		}

		// Idiom expressions require row context, so they need special handling
		Expr::Idiom(_) => {
			let phys_expr = expr_to_physical_expr(expr.clone())?;
			// Idioms always reference current_value, so this will be an error for top-level
			if phys_expr.references_current_value() {
				return Err(Error::Unimplemented(
					"Field expressions require a FROM clause to provide row context".to_string(),
				));
			}
			Ok(Arc::new(ExprPlan {
				expr: phys_expr,
			}) as Arc<dyn OperatorPlan>)
		}

		// Mock expressions generate test data - defer for now
		Expr::Mock(_) => Err(Error::Unimplemented(
			"Mock expressions not yet supported in execution plans".to_string(),
		)),

		// Postfix expressions (ranges, method calls)
		Expr::Postfix {
			..
		} => {
			let phys_expr = expr_to_physical_expr(expr.clone())?;
			// Validate that the expression doesn't require row context
			if phys_expr.references_current_value() {
				return Err(Error::Unimplemented(
					"Postfix expression references row context but no table specified".to_string(),
				));
			}
			Ok(Arc::new(ExprPlan {
				expr: phys_expr,
			}) as Arc<dyn OperatorPlan>)
		}
	}
}

/// Plan a SELECT statement
///
/// The operator pipeline is built in this order:
/// 1. Scan/Union (source from FROM clause)
/// 2. Split (SPLIT BY - before filtering/grouping)
/// 3. Filter (WHERE - before grouping)
/// 4. Aggregate (GROUP BY)
/// 5. Project (SELECT fields) or ProjectValue (SELECT VALUE)
/// 6. Sort (ORDER BY)
/// 7. Limit (LIMIT/START)
/// 8. Fetch (FETCH)
/// 9. Timeout (TIMEOUT)
fn plan_select(
	select: &crate::expr::statements::SelectStatement,
) -> Result<Arc<dyn OperatorPlan>, Error> {
	// Return Unimplemented for features not yet supported in the new executor
	// These will fall back to the old executor

	// ONLY clause (unwraps single results)
	if select.only {
		return Err(Error::Unimplemented(
			"SELECT ... ONLY not yet supported in execution plans".to_string(),
		));
	}

	// EXPLAIN clause (query explain output)
	if select.explain.is_some() {
		return Err(Error::Unimplemented(
			"SELECT ... EXPLAIN not yet supported in execution plans".to_string(),
		));
	}

	// WITH clause (index hints)
	if select.with.is_some() {
		return Err(Error::Unimplemented(
			"SELECT ... WITH not yet supported in execution plans".to_string(),
		));
	}

	// Extract VERSION timestamp if present (for time-travel queries)
	let version = extract_version(&select.version)?;

	// Extract table name from the first source for projection permissions
	// This is a simplification - complex queries with multiple tables need more work
	let table_name = extract_table_name(&select.what)?;

	// Build the source plan from `what` (FROM clause)
	let source = plan_select_sources(&select.what, version)?;

	// Apply SPLIT BY if present (before filtering)
	let split = if let Some(splits) = &select.split {
		let idioms: Vec<_> = splits.iter().map(|s| s.0.clone()).collect();
		Arc::new(Split {
			input: source,
			idioms,
		}) as Arc<dyn OperatorPlan>
	} else {
		source
	};

	// Apply WHERE clause if present (before grouping)
	let filtered = if let Some(cond) = &select.cond {
		let predicate = expr_to_physical_expr(cond.0.clone())?;
		Arc::new(Filter {
			input: split,
			predicate,
		}) as Arc<dyn OperatorPlan>
	} else {
		split
	};

	// Apply GROUP BY if present
	let (grouped, skip_projections) = if let Some(groups) = &select.group {
		let group_by: Vec<_> = groups.0.iter().map(|g| g.0.clone()).collect();

		// Build aggregate fields from the SELECT expression
		let aggregates = plan_aggregation(&select.fields, &group_by)?;

		// For GROUP BY, the Aggregate operator handles projections internally
		// Skip the separate projection step
		(
			Arc::new(Aggregate {
				input: filtered,
				group_by,
				aggregates,
			}) as Arc<dyn OperatorPlan>,
			true,
		)
	} else {
		(filtered, false)
	};

	// Apply projections (SELECT fields or SELECT VALUE)
	// Skip if GROUP BY is present (handled by Aggregate operator)
	let projected = if skip_projections {
		grouped
	} else {
		plan_projections(&select.fields, &select.omit, &table_name, grouped)?
	};

	// Apply ORDER BY if present
	let sorted = if let Some(order) = &select.order {
		let order_by = plan_order_by(order)?;
		Arc::new(Sort {
			input: projected,
			order_by,
		}) as Arc<dyn OperatorPlan>
	} else {
		projected
	};

	// Apply LIMIT/START if present
	let limited = if select.limit.is_some() || select.start.is_some() {
		let limit_expr = if let Some(limit) = &select.limit {
			Some(expr_to_physical_expr(limit.0.clone())?)
		} else {
			None
		};
		let offset_expr = if let Some(start) = &select.start {
			Some(expr_to_physical_expr(start.0.clone())?)
		} else {
			None
		};
		Arc::new(Limit {
			input: sorted,
			limit: limit_expr,
			offset: offset_expr,
		}) as Arc<dyn OperatorPlan>
	} else {
		sorted
	};

	// Apply FETCH if present
	let fetched = plan_fetch(&select.fetch, limited)?;

	// Apply TIMEOUT if present (timeout is always Expr but may be Literal::None)
	let timed = match &select.timeout {
		Expr::Literal(Literal::None) => fetched,
		timeout_expr => {
			let timeout_phys = expr_to_physical_expr(timeout_expr.clone())?;
			Arc::new(Timeout {
				input: fetched,
				timeout: Some(timeout_phys),
			}) as Arc<dyn OperatorPlan>
		}
	};

	Ok(timed)
}

/// Plan projections (SELECT fields or SELECT VALUE)
///
/// This handles:
/// - `SELECT *` - pass through without projection
/// - `SELECT * OMIT field` - pass through with Omit operator
/// - `SELECT VALUE expr` - use ProjectValue operator
/// - `SELECT field1, field2` - use Project operator
/// - `SELECT field1, *, field2` - mixed wildcards (returns Unimplemented for now)
fn plan_projections(
	fields: &Fields,
	omit: &[Expr],
	table_name: &Option<TableName>,
	input: Arc<dyn OperatorPlan>,
) -> Result<Arc<dyn OperatorPlan>, Error> {
	match fields {
		// SELECT VALUE expr - return raw values (OMIT doesn't make sense here)
		Fields::Value(selector) => {
			if !omit.is_empty() {
				return Err(Error::Unimplemented(
					"OMIT clause with SELECT VALUE not supported".to_string(),
				));
			}
			let expr = expr_to_physical_expr(selector.expr.clone())?;
			Ok(Arc::new(ProjectValue {
				input,
				expr,
			}) as Arc<dyn OperatorPlan>)
		}

		// SELECT field1, field2, ... or SELECT *
		Fields::Select(field_list) => {
			// Check if this is just SELECT * (all fields, no specific fields)
			let is_select_all = field_list.len() == 1 && matches!(field_list.first(), Some(Field::All));

			if is_select_all {
				// SELECT * - pass through without projection
				// But apply OMIT if present
				if !omit.is_empty() {
					let omit_fields = plan_omit_fields(omit)?;
					return Ok(Arc::new(Omit {
						input,
						fields: omit_fields,
					}) as Arc<dyn OperatorPlan>);
				}
				return Ok(input);
			}

			// Check for wildcards mixed with specific fields
			let has_wildcard = field_list.iter().any(|f| matches!(f, Field::All));
			if has_wildcard {
				// Mixed wildcards (e.g., SELECT field1, *, field2) are complex
				// The old executor handles this by starting with the full document
				// and then overwriting specific fields. We'll defer this for now.
				return Err(Error::Unimplemented(
					"Mixed wildcard projections (SELECT field, *, ...) not yet supported in execution plans".to_string(),
				));
			}

			// OMIT doesn't make sense with specific field projections
			if !omit.is_empty() {
				return Err(Error::Unimplemented(
					"OMIT clause with specific field projections not supported".to_string(),
				));
			}

			// Build field selections for specific fields only
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
					let expr = expr_to_physical_expr(selector.expr.clone())?;

					// Determine if this is a simple field reference (for permissions)
					let field_name = extract_simple_field_name(&selector.expr);

					field_selections.push(FieldSelection {
						output_name,
						expr,
						field_name,
					});
				}
			}

			// Need a table name for the Project operator
			let table = table_name.clone().ok_or_else(|| {
				Error::Unimplemented(
					"Projections require a table source for permission checking".to_string(),
				)
			})?;

			Ok(Arc::new(Project {
				input,
				table,
				fields: field_selections,
			}) as Arc<dyn OperatorPlan>)
		}
	}
}

/// Plan OMIT fields - convert expressions to idioms
fn plan_omit_fields(omit: &[Expr]) -> Result<Vec<crate::expr::idiom::Idiom>, Error> {
	let mut fields = Vec::with_capacity(omit.len());

	for expr in omit {
		match expr {
			Expr::Idiom(idiom) => {
				fields.push(idiom.clone());
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

/// Extract a table name from the FROM sources (for projection permissions)
fn extract_table_name(what: &[Expr]) -> Result<Option<TableName>, Error> {
	if what.is_empty() {
		return Ok(None);
	}

	// Get table name from first source
	match &what[0] {
		Expr::Table(name) => Ok(Some(name.clone())),
		Expr::Literal(crate::expr::literal::Literal::RecordId(rid)) => {
			Ok(Some(rid.table.clone()))
		}
		Expr::Idiom(idiom) => {
			// Simple idiom is a table name
			use crate::expr::part::Part;
			if idiom.len() == 1 {
				if let Some(Part::Field(name)) = idiom.first() {
					return Ok(Some(TableName::from(name.to_string())));
				}
			}
			Ok(None)
		}
		_ => Ok(None),
	}
}

/// Derive a field name from an expression for projection output
fn derive_field_name(expr: &Expr) -> String {
	match expr {
		// Simple field reference - extract the raw field name
		Expr::Idiom(idiom) => idiom_to_field_name(idiom),
		// For other expressions, use the SQL representation
		_ => {
			use surrealdb_types::ToSql;
			expr.to_sql()
		}
	}
}

/// Extract a field name from an idiom, preferring raw names for simple idioms.
fn idiom_to_field_name(idiom: &crate::expr::idiom::Idiom) -> String {
	use crate::expr::part::Part;
	use surrealdb_types::ToSql;

	// For simple single-part idioms, use the raw field name
	if idiom.len() == 1 {
		if let Some(Part::Field(name)) = idiom.first() {
			return name.to_string();
		}
	}
	// For complex idioms, use the SQL representation
	idiom.to_sql()
}

/// Extract a simple field name from an expression (for permission lookup)
/// Returns None if the expression is not a simple field reference
fn extract_simple_field_name(expr: &Expr) -> Option<String> {
	use crate::expr::part::Part;

	match expr {
		Expr::Idiom(idiom) => {
			// Only simple single-part idioms are field references
			if idiom.len() == 1 {
				if let Some(Part::Field(name)) = idiom.first() {
					return Some(name.to_string());
				}
			}
			None
		}
		_ => None,
	}
}

/// Plan FETCH clause
fn plan_fetch(
	fetch: &Option<crate::expr::fetch::Fetchs>,
	input: Arc<dyn OperatorPlan>,
) -> Result<Arc<dyn OperatorPlan>, Error> {
	let Some(fetchs) = fetch else {
		return Ok(input);
	};

	// Convert fetch expressions to idioms
	// We only support simple idiom fetches for now
	let mut fields = Vec::new();
	for fetch_item in fetchs.iter() {
		// The Fetch struct wraps an Expr in field .0
		match &fetch_item.0 {
			Expr::Idiom(idiom) => {
				fields.push(idiom.clone());
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
	}) as Arc<dyn OperatorPlan>)
}

/// Plan aggregation fields from SELECT expression and GROUP BY.
///
/// This extracts:
/// - Group-by keys (passed through unchanged)
/// - Aggregate functions (COUNT, SUM, MIN, MAX, AVG, etc.)
/// - Other expressions (evaluated with the first value in the group)
fn plan_aggregation(
	fields: &Fields,
	group_by: &[crate::expr::idiom::Idiom],
) -> Result<Vec<AggregateField>, Error> {
	use surrealdb_types::ToSql;

	match fields {
		// SELECT VALUE with GROUP BY - the VALUE expression may contain aggregates
		Fields::Value(selector) => {
			// Check if the VALUE expression is a group-by key
			let is_group_key = is_group_key_expression(&selector.expr, group_by);
			let agg_type = if is_group_key {
				None
			} else {
				detect_aggregate_type(&selector.expr)
			};
			let expr = expr_to_physical_expr(selector.expr.clone())?;

			// For VALUE, we use an empty name since the result isn't wrapped in an object
			Ok(vec![AggregateField {
				name: String::new(),
				expr,
				is_group_key,
				aggregate_type: agg_type,
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

						// Detect aggregate function type
						let agg_type = if is_group_key {
							None
						} else {
							detect_aggregate_type(&selector.expr)
						};

						// Convert expression to physical
						let expr = expr_to_physical_expr(selector.expr.clone())?;

						aggregates.push(AggregateField {
							name: output_name,
							expr,
							is_group_key,
							aggregate_type: agg_type,
						});
					}
				}
			}

			Ok(aggregates)
		}
	}
}

/// Check if an expression is a group-by key reference.
fn is_group_key_expression(
	expr: &Expr,
	group_by: &[crate::expr::idiom::Idiom],
) -> bool {
	use surrealdb_types::ToSql;

	// Only simple idiom expressions can be group keys
	if let Expr::Idiom(idiom) = expr {
		// Check if this idiom matches any group-by idiom
		return group_by.iter().any(|g| g.to_sql() == idiom.to_sql());
	}
	false
}

/// Detect the aggregate function type from an expression.
/// Returns None if the expression is not a simple aggregate function.
fn detect_aggregate_type(expr: &Expr) -> Option<AggregateType> {
	match expr {
		Expr::FunctionCall(func_call) => {
			// Check for aggregate functions
			match &func_call.receiver {
				Function::Normal(name) => {
					match name.as_str() {
						// Core aggregate functions
						"count" => {
							if func_call.arguments.is_empty() {
								Some(AggregateType::Count)
							} else {
								Some(AggregateType::CountField)
							}
						}
						"sum" => Some(AggregateType::Sum),
						"min" => Some(AggregateType::Min),
						"max" => Some(AggregateType::Max),
						// Math module aggregates
						"math::sum" => Some(AggregateType::Sum),
						"math::min" => Some(AggregateType::Min),
						"math::max" => Some(AggregateType::Max),
						"math::mean" => Some(AggregateType::Avg),
						// Array aggregates
						"array::group" => Some(AggregateType::ArrayGroup),
						_ => None,
					}
				}
				_ => None,
			}
		}
		// Nested expressions - check if they contain aggregates
		// For simplicity, we don't support nested aggregate expressions for now
		_ => None,
	}
}

/// Extract version timestamp from VERSION clause expression.
/// Currently only supports literal Datetime values.
fn extract_version(version_expr: &Expr) -> Result<Option<u64>, Error> {
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
	what: &[Expr],
	version: Option<u64>,
) -> Result<Arc<dyn OperatorPlan>, Error> {
	if what.is_empty() {
		return Err(Error::Unimplemented("SELECT requires at least one source".to_string()));
	}

	// Convert each source to a plan
	let mut source_plans = Vec::with_capacity(what.len());
	for expr in what {
		let plan = plan_single_source(expr, version)?;
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
/// Always wraps source operators (Scan, RecordIdLookup) with ComputeFields
///
/// The `version` parameter is an optional timestamp for time-travel queries (VERSION clause).
fn plan_single_source(expr: &Expr, version: Option<u64>) -> Result<Arc<dyn OperatorPlan>, Error> {
	match expr {
		// Table name: SELECT * FROM users
		Expr::Table(table_name) => {
			// Convert table name to a literal string for the physical expression
			let table_expr = expr_to_physical_expr(Expr::Literal(
				crate::expr::literal::Literal::String(table_name.as_str().to_string()),
			))?;
			let scan = Arc::new(Scan {
				table: table_expr.clone(),
				version,
			}) as Arc<dyn OperatorPlan>;
			// Wrap with ComputeFields to evaluate computed fields
			Ok(Arc::new(ComputeFields {
				input: scan,
				table: table_expr,
			}))
		}

		// Record ID literal: SELECT * FROM users:123
		Expr::Literal(crate::expr::literal::Literal::RecordId(record_id_lit)) => {
			// Convert the RecordIdLit to an actual RecordId
			// For now, we only support static record IDs (table:key)
			// More complex expressions would need async evaluation
			let record_id = record_id_lit_to_record_id(record_id_lit)?;
			// Get table name for ComputeFields
			let table_expr = expr_to_physical_expr(Expr::Literal(
				crate::expr::literal::Literal::String(record_id.table.as_str().to_string()),
			))?;
			let lookup = Arc::new(RecordIdLookup {
				record_id,
				version,
			}) as Arc<dyn OperatorPlan>;
			// Wrap with ComputeFields to evaluate computed fields
			Ok(Arc::new(ComputeFields {
				input: lookup,
				table: table_expr,
			}))
		}

		// Idiom that might be a table or record reference
		Expr::Idiom(idiom) => {
			// Simple idiom (just a name) is a table reference
			// Convert to a table scan using the idiom as a physical expression
			let table_expr = expr_to_physical_expr(Expr::Idiom(idiom.clone()))?;
			let scan = Arc::new(Scan {
				table: table_expr.clone(),
				version,
			}) as Arc<dyn OperatorPlan>;
			// Wrap with ComputeFields to evaluate computed fields
			Ok(Arc::new(ComputeFields {
				input: scan,
				table: table_expr,
			}))
		}

		// Parameter that will be resolved at runtime
		Expr::Param(param) => {
			// Parameters could be record IDs or table names
			// We'll treat them as table references - Scan evaluates at runtime
			let table_expr = expr_to_physical_expr(Expr::Param(param.clone()))?;
			let scan = Arc::new(Scan {
				table: table_expr.clone(),
				version,
			}) as Arc<dyn OperatorPlan>;
			// Wrap with ComputeFields to evaluate computed fields
			Ok(Arc::new(ComputeFields {
				input: scan,
				table: table_expr,
			}))
		}

		_ => Err(Error::Unimplemented(format!(
			"Unsupported FROM source type: {:?}",
			std::mem::discriminant(expr)
		))),
	}
}

/// Convert a RecordIdLit to an actual RecordId
/// For now, only supports static key patterns (Number, String, Uuid)
fn record_id_lit_to_record_id(
	record_id_lit: &crate::expr::RecordIdLit,
) -> Result<crate::val::RecordId, Error> {
	use crate::expr::record_id::RecordIdKeyLit;
	use crate::val::RecordIdKey;

	let key = match &record_id_lit.key {
		RecordIdKeyLit::Number(n) => RecordIdKey::Number(*n),
		RecordIdKeyLit::String(s) => RecordIdKey::String(s.clone()),
		RecordIdKeyLit::Uuid(u) => RecordIdKey::Uuid(*u),
		RecordIdKeyLit::Generate(generator) => generator.compute(),
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
		RecordIdKeyLit::Range(_) => {
			return Err(Error::Unimplemented(
				"Range record keys not yet supported in execution plans".to_string(),
			));
		}
	};

	Ok(crate::val::RecordId {
		table: record_id_lit.table.clone(),
		key,
	})
}

/// Convert a LET statement to an execution plan
fn convert_let_statement(
	let_stmt: &crate::expr::statements::SetStatement,
) -> Result<Arc<dyn OperatorPlan>, Error> {
	let name = let_stmt.name.clone();

	// Determine if the expression is a query or scalar
	let value: Arc<dyn OperatorPlan> = match &let_stmt.what {
		// SELECT produces a stream that gets collected into an array
		Expr::Select(select) => plan_select(select)?,

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
			let expr = expr_to_physical_expr(other.clone())?;

			// Validate: LET expressions can't reference current row
			if expr.references_current_value() {
				return Err(Error::Unimplemented(
					"LET expression cannot reference current row context".to_string(),
				));
			}

			Arc::new(ExprPlan {
				expr,
			}) as Arc<dyn OperatorPlan>
		}
	};

	Ok(Arc::new(LetPlan {
		name,
		value,
	}) as Arc<dyn OperatorPlan>)
}

/// Plan ORDER BY clause into OrderByField list
fn plan_order_by(
	order: &crate::expr::order::Ordering,
) -> Result<Vec<crate::exec::operators::OrderByField>, Error> {
	use crate::exec::operators::{NullsOrder, OrderByField, SortDirection};
	use crate::exec::physical_expr::Field;
	use crate::expr::order::Ordering;

	match order {
		Ordering::Random => {
			// ORDER BY RAND() - not yet supported in the new executor
			Err(Error::Unimplemented(
				"ORDER BY RAND() not yet supported in execution plans".to_string(),
			))
		}
		Ordering::Order(order_list) => {
			let mut fields = Vec::with_capacity(order_list.len());
			for order_field in order_list.iter() {
				// Convert idiom to field expression
				let expr: Arc<dyn crate::exec::PhysicalExpr> =
					Arc::new(Field(order_field.value.clone()));

				let direction = if order_field.direction {
					SortDirection::Asc
				} else {
					SortDirection::Desc
				};

				// Default nulls order based on direction
				// ASC -> nulls last, DESC -> nulls first
				let nulls = if order_field.direction {
					NullsOrder::Last
				} else {
					NullsOrder::First
				};

				fields.push(OrderByField {
					expr,
					direction,
					nulls,
				});
			}

			Ok(fields)
		}
	}
}

#[cfg(test)]
mod planner_tests {
	use super::*;

	#[test]
	fn test_planner_creates_let_operator() {
		let expr = Expr::Let(Box::new(crate::expr::statements::SetStatement {
			name: "x".to_string(),
			what: Expr::Literal(crate::expr::literal::Literal::Integer(42)),
			kind: None,
		}));

		let plan = try_plan_expr(&expr).expect("Planning failed");

		assert_eq!(plan.name(), "Let");
		assert!(plan.mutates_context());
	}

	#[test]
	fn test_planner_creates_scalar_plan() {
		// Test a simple literal
		let expr = Expr::Literal(crate::expr::literal::Literal::Integer(42));

		let plan = try_plan_expr(&expr).expect("Planning failed");

		assert_eq!(plan.name(), "Expr");
		assert!(plan.is_scalar());
	}
}
