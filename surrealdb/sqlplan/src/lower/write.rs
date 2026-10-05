//! Typed write lowering from PostgreSQL and MySQL AST to SurrealDB `surrealdb_expr::Expr`.

use surrealdb_expr::expr::data::Assignment;
use surrealdb_expr::expr::field::Selector;
use surrealdb_expr::expr::statements::{
	DeleteStatement, IfelseStatement, InsertStatement, SelectStatement, UpdateStatement,
};
use surrealdb_expr::expr::{
	AssignOperator, BinaryOperator, Cond, Data, Expr, Field, Fields, Function, FunctionCall, Idiom,
	Literal, Output, Param, Part, PrefixOperator,
};
use surrealdb_strand::{Strand, TableName};

use crate::error::PlanError;

// ============================================================================
// PostgreSQL Expression Lowering
// ============================================================================

/// Lower a PostgreSQL AST expression into a SurrealDB `Expr`.
pub fn lower_pgsql_expr(expr: &surrealdb_pgsql::ast::expr::Expr) -> Result<Expr, PlanError> {
	match expr {
		surrealdb_pgsql::ast::expr::Expr::Literal {
			lit,
			..
		} => match lit {
			surrealdb_pgsql::ast::literal::Literal::Integer(i) => {
				Ok(Expr::Literal(Literal::Integer(*i)))
			}
			surrealdb_pgsql::ast::literal::Literal::Float(f) => {
				Ok(Expr::Literal(Literal::Float(*f)))
			}
			surrealdb_pgsql::ast::literal::Literal::String(s) => {
				Ok(Expr::Literal(Literal::String(Strand::from(s.as_str()))))
			}
			surrealdb_pgsql::ast::literal::Literal::Boolean(b) => {
				Ok(Expr::Literal(Literal::Bool(*b)))
			}
			surrealdb_pgsql::ast::literal::Literal::Null => Ok(Expr::Literal(Literal::Null)),
			surrealdb_pgsql::ast::literal::Literal::BitString {
				value,
				..
			} => Ok(Expr::Literal(Literal::String(Strand::from(value.as_str())))),
		},

		surrealdb_pgsql::ast::expr::Expr::ColumnRef(col) => {
			// Check for DEFAULT
			if col.fields.len() == 1 && col.fields[0].eq_ignore_ascii_case("DEFAULT") {
				return Ok(Expr::Literal(Literal::None));
			}

			// Check for EXCLUDED.col in ON CONFLICT DO UPDATE
			if col.fields.len() == 2 && col.fields[0].eq_ignore_ascii_case("EXCLUDED") {
				return Ok(Expr::Idiom(Idiom(vec![
					Part::Start(Expr::Param(Param::from("input".to_string()))),
					Part::Field(Strand::from(col.fields[1].as_str())),
				])));
			}

			// Normal column reference: col or table.col
			let parts: Vec<Part> =
				col.fields.iter().map(|f| Part::Field(Strand::from(f.as_str()))).collect();
			Ok(Expr::Idiom(Idiom(parts)))
		}

		surrealdb_pgsql::ast::expr::Expr::Param {
			id,
			..
		} => Ok(Expr::Param(Param::from(format!("_{id}")))),

		surrealdb_pgsql::ast::expr::Expr::UnaryOp {
			op,
			expr,
			..
		} => {
			let inner = lower_pgsql_expr(expr)?;
			let prefix_op = match op {
				surrealdb_pgsql::ast::op::UnaryOperator::Plus => PrefixOperator::Positive,
				surrealdb_pgsql::ast::op::UnaryOperator::Minus => PrefixOperator::Negate,
				surrealdb_pgsql::ast::op::UnaryOperator::Not => PrefixOperator::Not,
				_ => {
					return Err(PlanError::UnsupportedExpression(format!("Unary operator {op:?}")));
				}
			};
			Ok(Expr::Prefix {
				op: prefix_op,
				expr: Box::new(inner),
			})
		}

		surrealdb_pgsql::ast::expr::Expr::BinaryOp {
			left,
			op,
			right,
			..
		} => {
			let l = lower_pgsql_expr(left)?;
			let r = lower_pgsql_expr(right)?;
			let bin_op = match op {
				surrealdb_pgsql::ast::op::BinaryOperator::Add => BinaryOperator::Add,
				surrealdb_pgsql::ast::op::BinaryOperator::Sub => BinaryOperator::Subtract,
				surrealdb_pgsql::ast::op::BinaryOperator::Mul => BinaryOperator::Multiply,
				surrealdb_pgsql::ast::op::BinaryOperator::Div => BinaryOperator::Divide,
				surrealdb_pgsql::ast::op::BinaryOperator::Mod => BinaryOperator::Remainder,
				surrealdb_pgsql::ast::op::BinaryOperator::Eq => BinaryOperator::Equal,
				surrealdb_pgsql::ast::op::BinaryOperator::NotEq => BinaryOperator::NotEqual,
				surrealdb_pgsql::ast::op::BinaryOperator::Lt => BinaryOperator::LessThan,
				surrealdb_pgsql::ast::op::BinaryOperator::LtEq => BinaryOperator::LessThanEqual,
				surrealdb_pgsql::ast::op::BinaryOperator::Gt => BinaryOperator::MoreThan,
				surrealdb_pgsql::ast::op::BinaryOperator::GtEq => BinaryOperator::MoreThanEqual,
				surrealdb_pgsql::ast::op::BinaryOperator::And => BinaryOperator::And,
				surrealdb_pgsql::ast::op::BinaryOperator::Or => BinaryOperator::Or,
				_ => {
					return Err(PlanError::UnsupportedExpression(format!(
						"Binary operator {op:?}"
					)));
				}
			};
			Ok(Expr::Binary {
				left: Box::new(l),
				op: bin_op,
				right: Box::new(r),
			})
		}

		surrealdb_pgsql::ast::expr::Expr::TypeCast {
			expr,
			..
		} => lower_pgsql_expr(expr),

		surrealdb_pgsql::ast::expr::Expr::FunctionCall {
			name,
			args,
			..
		} => {
			let fn_name = name.parts.last().cloned().unwrap_or_default();
			let mut lowered_args = Vec::with_capacity(args.len());
			for a in args {
				lowered_args.push(lower_pgsql_expr(a)?);
			}
			Ok(Expr::FunctionCall(Box::new(FunctionCall {
				receiver: Function::Normal(fn_name),
				arguments: lowered_args,
			})))
		}

		surrealdb_pgsql::ast::expr::Expr::Grouping {
			expr,
			..
		} => lower_pgsql_expr(expr),

		surrealdb_pgsql::ast::expr::Expr::IsNull {
			expr,
			negated,
			..
		} => {
			let inner = lower_pgsql_expr(expr)?;
			let op = if *negated {
				BinaryOperator::NotEqual
			} else {
				BinaryOperator::Equal
			};
			Ok(Expr::Binary {
				left: Box::new(inner),
				op,
				right: Box::new(Expr::Literal(Literal::Null)),
			})
		}

		surrealdb_pgsql::ast::expr::Expr::Between {
			expr,
			low,
			high,
			negated,
			..
		} => {
			let val = lower_pgsql_expr(expr)?;
			let l = lower_pgsql_expr(low)?;
			let h = lower_pgsql_expr(high)?;

			let lower_cmp = Expr::Binary {
				left: Box::new(val.clone()),
				op: if *negated {
					BinaryOperator::LessThan
				} else {
					BinaryOperator::MoreThanEqual
				},
				right: Box::new(l),
			};
			let upper_cmp = Expr::Binary {
				left: Box::new(val),
				op: if *negated {
					BinaryOperator::MoreThan
				} else {
					BinaryOperator::LessThanEqual
				},
				right: Box::new(h),
			};

			Ok(Expr::Binary {
				left: Box::new(lower_cmp),
				op: if *negated {
					BinaryOperator::Or
				} else {
					BinaryOperator::And
				},
				right: Box::new(upper_cmp),
			})
		}

		surrealdb_pgsql::ast::expr::Expr::InList {
			expr,
			list,
			negated,
			..
		} => {
			let target = lower_pgsql_expr(expr)?;
			let mut items = Vec::with_capacity(list.len());
			for item in list {
				items.push(lower_pgsql_expr(item)?);
			}
			let array_expr = Expr::Literal(Literal::Array(items));
			let op = if *negated {
				BinaryOperator::NotInside
			} else {
				BinaryOperator::Inside
			};
			Ok(Expr::Binary {
				left: Box::new(target),
				op,
				right: Box::new(array_expr),
			})
		}

		surrealdb_pgsql::ast::expr::Expr::Case {
			branches,
			else_expr,
			..
		} => {
			let mut exprs = Vec::with_capacity(branches.len());
			for b in branches {
				let cond = lower_pgsql_expr(&b.when)?;
				let then = lower_pgsql_expr(&b.then)?;
				exprs.push((cond, then));
			}
			let close = match else_expr {
				Some(e) => Some(lower_pgsql_expr(e)?),
				None => None,
			};
			Ok(Expr::IfElse(Box::new(IfelseStatement {
				exprs,
				close,
			})))
		}

		other => Err(PlanError::UnsupportedExpression(format!("{other:?}"))),
	}
}

fn is_wildcard_target(target: &surrealdb_pgsql::ast::stmt::ResTarget) -> bool {
	if let surrealdb_pgsql::ast::expr::Expr::ColumnRef(col) = &target.expr {
		col.fields.len() == 1 && col.fields[0] == "*"
	} else {
		false
	}
}

fn lower_pgsql_returning(
	targets: &[surrealdb_pgsql::ast::stmt::ResTarget],
) -> Result<Output, PlanError> {
	if targets.len() == 1 && is_wildcard_target(&targets[0]) {
		Ok(Output::Fields(Fields::all()))
	} else {
		let mut fields = Vec::with_capacity(targets.len());
		for item in targets {
			if is_wildcard_target(item) {
				fields.push(Field::All);
			} else {
				let lowered_expr = lower_pgsql_expr(&item.expr)?;
				let alias_idiom = item.alias.as_ref().map(|a| Idiom::field(a.clone()));
				fields.push(Field::Single(Selector {
					expr: lowered_expr,
					alias: alias_idiom,
				}));
			}
		}
		Ok(Output::Fields(Fields::Select(fields)))
	}
}

fn lower_pgsql_select_stmt(
	select: &surrealdb_pgsql::ast::stmt::SelectStmt,
) -> Result<SelectStatement, PlanError> {
	let fields = if select.targets.len() == 1 && is_wildcard_target(&select.targets[0]) {
		Fields::all()
	} else {
		let mut f_vec = Vec::with_capacity(select.targets.len());
		for item in &select.targets {
			if is_wildcard_target(item) {
				f_vec.push(Field::All);
			} else {
				let lowered = lower_pgsql_expr(&item.expr)?;
				let a = item.alias.as_ref().map(|s| Idiom::field(s.clone()));
				f_vec.push(Field::Single(Selector {
					expr: lowered,
					alias: a,
				}));
			}
		}
		Fields::Select(f_vec)
	};

	let mut what = Vec::new();
	for t in &select.from {
		if let surrealdb_pgsql::ast::stmt::TableRef::Table {
			name,
			..
		} = t
		{
			let (_, t_name) = crate::namespace::resolve_pgsql_table_name(&name.parts, None)?;
			what.push(Expr::Table(TableName::from(t_name)));
		}
	}

	let cond = match &select.where_clause {
		Some(w) => Some(Cond(lower_pgsql_expr(w)?)),
		None => None,
	};

	Ok(SelectStatement {
		fields,
		omit: Vec::new(),
		only: false,
		what,
		with: None,
		cond,
		split: None,
		group: None,
		order: None,
		limit: None,
		start: None,
		fetch: None,
		version: Expr::Literal(Literal::None),
		timeout: Expr::Literal(Literal::None),
		explain: None,
		tempfiles: false,
		for_update: false,
	})
}

// ============================================================================
// PostgreSQL DML Lowering
// ============================================================================

/// Lower a PostgreSQL `InsertStmt` into a SurrealDB `Expr::Insert`.
pub fn lower_pgsql_insert(
	insert: &surrealdb_pgsql::ast::stmt::InsertStmt,
) -> Result<Expr, PlanError> {
	let (_, table_name) = crate::namespace::resolve_pgsql_table_name(&insert.relation.parts, None)?;
	let into = Some(Expr::Table(TableName::from(table_name)));

	let mut ignore = false;
	let mut update = None;

	if let Some(on_conflict) = &insert.on_conflict {
		match &on_conflict.action {
			surrealdb_pgsql::ast::stmt::OnConflictAction::DoNothing => {
				ignore = true;
			}
			surrealdb_pgsql::ast::stmt::OnConflictAction::DoUpdate(assignments) => {
				let mut update_assignments = Vec::with_capacity(assignments.len());
				for assign in assignments {
					let val = lower_pgsql_expr(&assign.value)?;
					update_assignments.push(Assignment {
						place: Idiom::field(assign.column.clone()),
						operator: AssignOperator::Assign,
						value: val,
					});
				}
				update = Some(Data::UpdateExpression(update_assignments));
			}
		}
	}

	let data = match (&insert.values, &insert.query) {
		(None, None) => Data::SingleExpression(Expr::Literal(Literal::Object(Vec::new()))),
		(Some(rows), None) if rows.is_empty() => {
			Data::SingleExpression(Expr::Literal(Literal::Object(Vec::new())))
		}
		(None, Some(select_stmt)) => {
			let sel = lower_pgsql_select_stmt(select_stmt)?;
			Data::SingleExpression(Expr::Select(Box::new(sel)))
		}
		(Some(rows), _) => {
			let mut values_rows = Vec::with_capacity(rows.len());
			for row in rows {
				let mut row_tuples = Vec::with_capacity(row.len());
				for (idx, val) in row.iter().enumerate() {
					let col_name = if idx < insert.columns.len() {
						insert.columns[idx].clone()
					} else {
						format!("_{idx}")
					};
					let lowered_val = lower_pgsql_expr(val)?;
					row_tuples.push((Idiom::field(col_name), lowered_val));
				}
				values_rows.push(row_tuples);
			}
			Data::ValuesExpression(values_rows)
		}
	};

	let output = insert.returning.as_deref().map(lower_pgsql_returning).transpose()?;

	Ok(Expr::Insert(Box::new(InsertStatement {
		into,
		data,
		ignore,
		update,
		output,
		timeout: Expr::Literal(Literal::None),
		relation: false,
	})))
}

/// Lower a PostgreSQL `UpdateStmt` into a SurrealDB `Expr::Update`.
pub fn lower_pgsql_update(
	update: &surrealdb_pgsql::ast::stmt::UpdateStmt,
) -> Result<Expr, PlanError> {
	let (_, table_name) = crate::namespace::resolve_pgsql_table_name(&update.relation.parts, None)?;
	let mut what = vec![Expr::Table(TableName::from(table_name))];

	if let Some(from_tables) = &update.from {
		for t in from_tables {
			if let surrealdb_pgsql::ast::stmt::TableRef::Table {
				name,
				..
			} = t
			{
				let (_, t_name) = crate::namespace::resolve_pgsql_table_name(&name.parts, None)?;
				what.push(Expr::Table(TableName::from(t_name)));
			}
		}
	}

	let mut assignments = Vec::with_capacity(update.assignments.len());
	for assign in &update.assignments {
		let val = lower_pgsql_expr(&assign.value)?;
		assignments.push(Assignment {
			place: Idiom::field(assign.column.clone()),
			operator: AssignOperator::Assign,
			value: val,
		});
	}

	let cond = match &update.where_clause {
		Some(w) => Some(Cond(lower_pgsql_expr(w)?)),
		None => None,
	};

	let output = update.returning.as_deref().map(lower_pgsql_returning).transpose()?;

	Ok(Expr::Update(Box::new(UpdateStatement {
		what,
		data: Some(Data::SetExpression(assignments)),
		cond,
		output,
		..Default::default()
	})))
}

/// Lower a PostgreSQL `DeleteStmt` into a SurrealDB `Expr::Delete`.
pub fn lower_pgsql_delete(
	delete: &surrealdb_pgsql::ast::stmt::DeleteStmt,
) -> Result<Expr, PlanError> {
	let (_, table_name) = crate::namespace::resolve_pgsql_table_name(&delete.relation.parts, None)?;
	let mut what = vec![Expr::Table(TableName::from(table_name))];

	if let Some(using_tables) = &delete.using {
		for t in using_tables {
			if let surrealdb_pgsql::ast::stmt::TableRef::Table {
				name,
				..
			} = t
			{
				let (_, t_name) = crate::namespace::resolve_pgsql_table_name(&name.parts, None)?;
				what.push(Expr::Table(TableName::from(t_name)));
			}
		}
	}

	let cond = match &delete.where_clause {
		Some(w) => Some(Cond(lower_pgsql_expr(w)?)),
		None => None,
	};

	let output = delete.returning.as_deref().map(lower_pgsql_returning).transpose()?;

	Ok(Expr::Delete(Box::new(DeleteStatement {
		what,
		cond,
		output,
		..Default::default()
	})))
}

/// Lower a PostgreSQL `TruncateStmt` into a SurrealDB `Expr::Delete`.
pub fn lower_pgsql_truncate(
	truncate: &surrealdb_pgsql::ast::stmt::TruncateStmt,
) -> Result<Expr, PlanError> {
	let (_, table_name) = crate::namespace::resolve_pgsql_table_name(&truncate.table.parts, None)?;
	Ok(Expr::Delete(Box::new(DeleteStatement {
		what: vec![Expr::Table(TableName::from(table_name))],
		cond: None,
		output: None,
		..Default::default()
	})))
}

// ============================================================================
// MySQL Expression Lowering
// ============================================================================

/// Lower a MySQL AST expression into a SurrealDB `Expr`.
pub fn lower_mysql_expr(expr: &surrealdb_mysql::ast::expr::Expr) -> Result<Expr, PlanError> {
	match expr {
		surrealdb_mysql::ast::expr::Expr::Literal(lit, _) => match lit {
			surrealdb_mysql::ast::literal::Literal::Integer(i) => {
				Ok(Expr::Literal(Literal::Integer(*i)))
			}
			surrealdb_mysql::ast::literal::Literal::Float(f) => {
				Ok(Expr::Literal(Literal::Float(*f)))
			}
			surrealdb_mysql::ast::literal::Literal::String(s) => {
				Ok(Expr::Literal(Literal::String(Strand::from(s.as_str()))))
			}
			surrealdb_mysql::ast::literal::Literal::Boolean(b) => {
				Ok(Expr::Literal(Literal::Bool(*b)))
			}
			surrealdb_mysql::ast::literal::Literal::Null => Ok(Expr::Literal(Literal::Null)),
			surrealdb_mysql::ast::literal::Literal::HexString(bytes)
			| surrealdb_mysql::ast::literal::Literal::BitString(bytes) => Ok(Expr::Literal(
				Literal::String(Strand::from(String::from_utf8_lossy(bytes).as_ref())),
			)),
		},

		surrealdb_mysql::ast::expr::Expr::ColumnRef(col) => {
			if col.fields.len() == 1 && col.fields[0].eq_ignore_ascii_case("DEFAULT") {
				return Ok(Expr::Literal(Literal::None));
			}
			let parts: Vec<Part> =
				col.fields.iter().map(|f| Part::Field(Strand::from(f.as_str()))).collect();
			Ok(Expr::Idiom(Idiom(parts)))
		}

		surrealdb_mysql::ast::expr::Expr::Param(_idx, _) => Err(PlanError::unsupported(
			"parameter placeholder '?' is not supported in simple queries without prepared statements".to_string(),
		)),

		surrealdb_mysql::ast::expr::Expr::UserVar(name, _) => {
			Ok(Expr::Param(Param::from(name.clone())))
		}

		surrealdb_mysql::ast::expr::Expr::UnaryOp {
			op,
			expr,
			..
		} => {
			let inner = lower_mysql_expr(expr)?;
			let prefix_op = match op {
				surrealdb_mysql::ast::operator::UnaryOperator::Plus => PrefixOperator::Positive,
				surrealdb_mysql::ast::operator::UnaryOperator::Minus => PrefixOperator::Negate,
				surrealdb_mysql::ast::operator::UnaryOperator::Not => PrefixOperator::Not,
				_ => {
					return Err(PlanError::UnsupportedExpression(format!("Unary operator {op:?}")));
				}
			};
			Ok(Expr::Prefix {
				op: prefix_op,
				expr: Box::new(inner),
			})
		}

		surrealdb_mysql::ast::expr::Expr::BinaryOp {
			left,
			op,
			right,
			..
		} => {
			let l = lower_mysql_expr(left)?;
			let r = lower_mysql_expr(right)?;
			let bin_op = match op {
				surrealdb_mysql::ast::operator::BinaryOperator::Add => BinaryOperator::Add,
				surrealdb_mysql::ast::operator::BinaryOperator::Sub => BinaryOperator::Subtract,
				surrealdb_mysql::ast::operator::BinaryOperator::Mul => BinaryOperator::Multiply,
				surrealdb_mysql::ast::operator::BinaryOperator::Div => BinaryOperator::Divide,
				surrealdb_mysql::ast::operator::BinaryOperator::Mod => BinaryOperator::Remainder,
				surrealdb_mysql::ast::operator::BinaryOperator::Equal => BinaryOperator::Equal,
				surrealdb_mysql::ast::operator::BinaryOperator::NotEqual => {
					BinaryOperator::NotEqual
				}
				surrealdb_mysql::ast::operator::BinaryOperator::Less => BinaryOperator::LessThan,
				surrealdb_mysql::ast::operator::BinaryOperator::LessEqual => {
					BinaryOperator::LessThanEqual
				}
				surrealdb_mysql::ast::operator::BinaryOperator::Greater => BinaryOperator::MoreThan,
				surrealdb_mysql::ast::operator::BinaryOperator::GreaterEqual => {
					BinaryOperator::MoreThanEqual
				}
				surrealdb_mysql::ast::operator::BinaryOperator::And => BinaryOperator::And,
				surrealdb_mysql::ast::operator::BinaryOperator::Or => BinaryOperator::Or,
				_ => {
					return Err(PlanError::UnsupportedExpression(format!(
						"Binary operator {op:?}"
					)));
				}
			};
			Ok(Expr::Binary {
				left: Box::new(l),
				op: bin_op,
				right: Box::new(r),
			})
		}

		surrealdb_mysql::ast::expr::Expr::FunctionCall {
			name,
			args,
			..
		} => {
			// Check for VALUES(col) in ON DUPLICATE KEY UPDATE
			if name.eq_ignore_ascii_case("VALUES")
				&& args.len() == 1
				&& let [surrealdb_mysql::ast::expr::Expr::ColumnRef(col)] = &args[..]
			{
				let col_name = col.fields.last().cloned().unwrap_or_default();
				return Ok(Expr::Idiom(Idiom(vec![
					Part::Start(Expr::Param(Param::from("input".to_string()))),
					Part::Field(Strand::from(col_name)),
				])));
			}

			let mut lowered_args = Vec::with_capacity(args.len());
			for a in args {
				lowered_args.push(lower_mysql_expr(a)?);
			}
			Ok(Expr::FunctionCall(Box::new(FunctionCall {
				receiver: Function::Normal(name.clone()),
				arguments: lowered_args,
			})))
		}

		surrealdb_mysql::ast::expr::Expr::Cast {
			expr,
			..
		} => lower_mysql_expr(expr),

		surrealdb_mysql::ast::expr::Expr::IsNull {
			expr,
			negated,
			..
		} => {
			let inner = lower_mysql_expr(expr)?;
			let op = if *negated {
				BinaryOperator::NotEqual
			} else {
				BinaryOperator::Equal
			};
			Ok(Expr::Binary {
				left: Box::new(inner),
				op,
				right: Box::new(Expr::Literal(Literal::Null)),
			})
		}

		surrealdb_mysql::ast::expr::Expr::Between {
			expr,
			low,
			high,
			negated,
			..
		} => {
			let val = lower_mysql_expr(expr)?;
			let l = lower_mysql_expr(low)?;
			let h = lower_mysql_expr(high)?;

			let lower_cmp = Expr::Binary {
				left: Box::new(val.clone()),
				op: if *negated {
					BinaryOperator::LessThan
				} else {
					BinaryOperator::MoreThanEqual
				},
				right: Box::new(l),
			};
			let upper_cmp = Expr::Binary {
				left: Box::new(val),
				op: if *negated {
					BinaryOperator::MoreThan
				} else {
					BinaryOperator::LessThanEqual
				},
				right: Box::new(h),
			};

			Ok(Expr::Binary {
				left: Box::new(lower_cmp),
				op: if *negated {
					BinaryOperator::Or
				} else {
					BinaryOperator::And
				},
				right: Box::new(upper_cmp),
			})
		}

		surrealdb_mysql::ast::expr::Expr::InList {
			expr,
			list,
			negated,
			..
		} => {
			let target = lower_mysql_expr(expr)?;
			let mut items = Vec::with_capacity(list.len());
			for item in list {
				items.push(lower_mysql_expr(item)?);
			}
			let array_expr = Expr::Literal(Literal::Array(items));
			let op = if *negated {
				BinaryOperator::NotInside
			} else {
				BinaryOperator::Inside
			};
			Ok(Expr::Binary {
				left: Box::new(target),
				op,
				right: Box::new(array_expr),
			})
		}

		surrealdb_mysql::ast::expr::Expr::Case {
			branches,
			else_expr,
			..
		} => {
			let mut exprs = Vec::with_capacity(branches.len());
			for b in branches {
				let cond = lower_mysql_expr(&b.when)?;
				let then = lower_mysql_expr(&b.then)?;
				exprs.push((cond, then));
			}
			let close = match else_expr {
				Some(e) => Some(lower_mysql_expr(e)?),
				None => None,
			};
			Ok(Expr::IfElse(Box::new(IfelseStatement {
				exprs,
				close,
			})))
		}

		other => Err(PlanError::UnsupportedExpression(format!("{other:?}"))),
	}
}

fn collect_mysql_tables(tref: &surrealdb_mysql::ast::stmt::TableRef, out: &mut Vec<Expr>) {
	match tref {
		surrealdb_mysql::ast::stmt::TableRef::Table {
			name,
			..
		} => {
			if let Ok((_, t_name)) = crate::namespace::resolve_mysql_table_name(&name.parts, None) {
				out.push(Expr::Table(TableName::from(t_name)));
			}
		}
		surrealdb_mysql::ast::stmt::TableRef::Join {
			left,
			right,
			..
		} => {
			collect_mysql_tables(left, out);
			collect_mysql_tables(right, out);
		}
		surrealdb_mysql::ast::stmt::TableRef::Subquery {
			..
		} => {}
	}
}

fn lower_mysql_select_stmt(
	select: &surrealdb_mysql::ast::stmt::SelectStmt,
) -> Result<SelectStatement, PlanError> {
	let fields = if select.projection.len() == 1
		&& matches!(select.projection[0], surrealdb_mysql::ast::stmt::SelectItem::Wildcard(_))
	{
		Fields::all()
	} else {
		let mut f_vec = Vec::with_capacity(select.projection.len());
		for item in &select.projection {
			match item {
				surrealdb_mysql::ast::stmt::SelectItem::Wildcard(_)
				| surrealdb_mysql::ast::stmt::SelectItem::QualifiedWildcard(..) => {
					f_vec.push(Field::All);
				}
				surrealdb_mysql::ast::stmt::SelectItem::Expr {
					expr,
					alias,
					..
				} => {
					let lowered = lower_mysql_expr(expr)?;
					let a = alias.as_ref().map(|s| Idiom::field(s.clone()));
					f_vec.push(Field::Single(Selector {
						expr: lowered,
						alias: a,
					}));
				}
			}
		}
		Fields::Select(f_vec)
	};

	let mut what = Vec::new();
	if let Some(from_table) = &select.from {
		collect_mysql_tables(from_table, &mut what);
	}

	let cond = match &select.where_clause {
		Some(w) => Some(Cond(lower_mysql_expr(w)?)),
		None => None,
	};

	Ok(SelectStatement {
		fields,
		omit: Vec::new(),
		only: false,
		what,
		with: None,
		cond,
		split: None,
		group: None,
		order: None,
		limit: None,
		start: None,
		fetch: None,
		version: Expr::Literal(Literal::None),
		timeout: Expr::Literal(Literal::None),
		explain: None,
		tempfiles: false,
		for_update: false,
	})
}

// ============================================================================
// MySQL DML Lowering
// ============================================================================

/// Lower a MySQL `InsertStmt` into a SurrealDB `Expr::Insert`.
pub fn lower_mysql_insert(
	insert: &surrealdb_mysql::ast::stmt::InsertStmt,
) -> Result<Expr, PlanError> {
	let (_, table_name) = crate::namespace::resolve_mysql_table_name(&insert.table.parts, None)?;
	let into = Some(Expr::Table(TableName::from(table_name)));

	let mut update = None;
	if !insert.on_duplicate_key_update.is_empty() {
		let mut update_assignments = Vec::with_capacity(insert.on_duplicate_key_update.len());
		for (col, expr) in &insert.on_duplicate_key_update {
			let val = lower_mysql_expr(expr)?;
			update_assignments.push(Assignment {
				place: Idiom::field(col.clone()),
				operator: AssignOperator::Assign,
				value: val,
			});
		}
		update = Some(Data::UpdateExpression(update_assignments));
	}

	let data = if insert.values.is_empty() && insert.query.is_none() {
		Data::SingleExpression(Expr::Literal(Literal::Object(Vec::new())))
	} else if let Some(query) = &insert.query {
		let sel = lower_mysql_select_stmt(query)?;
		Data::SingleExpression(Expr::Select(Box::new(sel)))
	} else {
		let mut values_rows = Vec::with_capacity(insert.values.len());
		for row in &insert.values {
			let mut row_tuples = Vec::with_capacity(row.len());
			for (idx, val) in row.iter().enumerate() {
				let col_name = if idx < insert.columns.len() {
					insert.columns[idx].clone()
				} else {
					format!("_{idx}")
				};
				let lowered_val = lower_mysql_expr(val)?;
				row_tuples.push((Idiom::field(col_name), lowered_val));
			}
			values_rows.push(row_tuples);
		}
		Data::ValuesExpression(values_rows)
	};

	Ok(Expr::Insert(Box::new(InsertStatement {
		into,
		data,
		ignore: insert.ignore,
		update,
		output: None,
		timeout: Expr::Literal(Literal::None),
		relation: false,
	})))
}

/// Lower a MySQL `ReplaceStmt` into a SurrealDB `Expr::Insert` with update replacements.
pub fn lower_mysql_replace(
	replace: &surrealdb_mysql::ast::stmt::ReplaceStmt,
) -> Result<Expr, PlanError> {
	let (_, table_name) = crate::namespace::resolve_mysql_table_name(&replace.table.parts, None)?;
	let into = Some(Expr::Table(TableName::from(table_name)));

	let mut update_assignments = Vec::with_capacity(replace.columns.len());
	for col in &replace.columns {
		update_assignments.push(Assignment {
			place: Idiom::field(col.clone()),
			operator: AssignOperator::Assign,
			value: Expr::Idiom(Idiom(vec![
				Part::Start(Expr::Param(Param::from("input".to_string()))),
				Part::Field(Strand::from(col.clone())),
			])),
		});
	}
	let update = if update_assignments.is_empty() {
		None
	} else {
		Some(Data::UpdateExpression(update_assignments))
	};

	let data = if replace.values.is_empty() && replace.query.is_none() {
		Data::SingleExpression(Expr::Literal(Literal::Object(Vec::new())))
	} else if let Some(query) = &replace.query {
		let sel = lower_mysql_select_stmt(query)?;
		Data::SingleExpression(Expr::Select(Box::new(sel)))
	} else {
		let mut values_rows = Vec::with_capacity(replace.values.len());
		for row in &replace.values {
			let mut row_tuples = Vec::with_capacity(row.len());
			for (idx, val) in row.iter().enumerate() {
				let col_name = if idx < replace.columns.len() {
					replace.columns[idx].clone()
				} else {
					format!("_{idx}")
				};
				let lowered_val = lower_mysql_expr(val)?;
				row_tuples.push((Idiom::field(col_name), lowered_val));
			}
			values_rows.push(row_tuples);
		}
		Data::ValuesExpression(values_rows)
	};

	Ok(Expr::Insert(Box::new(InsertStatement {
		into,
		data,
		ignore: false,
		update,
		output: None,
		timeout: Expr::Literal(Literal::None),
		relation: false,
	})))
}

/// Lower a MySQL `UpdateStmt` into a SurrealDB `Expr::Update`.
pub fn lower_mysql_update(
	update: &surrealdb_mysql::ast::stmt::UpdateStmt,
) -> Result<Expr, PlanError> {
	let mut what = Vec::new();
	collect_mysql_tables(&update.table, &mut what);

	let mut assignments = Vec::with_capacity(update.assignments.len());
	for (col, expr) in &update.assignments {
		let val = lower_mysql_expr(expr)?;
		assignments.push(Assignment {
			place: Idiom::field(col.clone()),
			operator: AssignOperator::Assign,
			value: val,
		});
	}

	let cond = match &update.where_clause {
		Some(w) => Some(Cond(lower_mysql_expr(w)?)),
		None => None,
	};

	Ok(Expr::Update(Box::new(UpdateStatement {
		what,
		data: Some(Data::SetExpression(assignments)),
		cond,
		output: None,
		..Default::default()
	})))
}

/// Lower a MySQL `DeleteStmt` into a SurrealDB `Expr::Delete`.
pub fn lower_mysql_delete(
	delete: &surrealdb_mysql::ast::stmt::DeleteStmt,
) -> Result<Expr, PlanError> {
	let (_, table_name) = crate::namespace::resolve_mysql_table_name(&delete.table.parts, None)?;
	let what = vec![Expr::Table(TableName::from(table_name))];

	let cond = match &delete.where_clause {
		Some(w) => Some(Cond(lower_mysql_expr(w)?)),
		None => None,
	};

	Ok(Expr::Delete(Box::new(DeleteStatement {
		what,
		cond,
		output: None,
		..Default::default()
	})))
}

/// Lower a MySQL `TruncateStmt` into a SurrealDB `Expr::Delete`.
pub fn lower_mysql_truncate(
	truncate: &surrealdb_mysql::ast::stmt::TruncateStmt,
) -> Result<Expr, PlanError> {
	let (_, table_name) = crate::namespace::resolve_mysql_table_name(&truncate.table.parts, None)?;
	Ok(Expr::Delete(Box::new(DeleteStatement {
		what: vec![Expr::Table(TableName::from(table_name))],
		cond: None,
		output: None,
		..Default::default()
	})))
}
