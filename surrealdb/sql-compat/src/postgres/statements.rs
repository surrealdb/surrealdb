use sqlparser::ast::{self as pg, SetExpr, TableFactor};
use surrealdb_core::expr::cond::Cond;
use surrealdb_core::expr::data::{Assignment, Data};
use surrealdb_core::expr::expression::Expr;
use surrealdb_core::expr::group::{Group, Groups};
use surrealdb_core::expr::idiom::Idiom;
use surrealdb_core::expr::join::{JoinExpr, JoinKind};
use surrealdb_core::expr::limit::Limit;
use surrealdb_core::expr::literal::Literal;
use surrealdb_core::expr::operator::AssignOperator;
use surrealdb_core::expr::part::Part;
use surrealdb_core::expr::plan::TopLevelExpr;
use surrealdb_core::expr::start::Start;
use surrealdb_core::expr::statements::delete::DeleteStatement;
use surrealdb_core::expr::statements::insert::InsertStatement;
use surrealdb_core::expr::statements::select::SelectStatement;
use surrealdb_core::expr::statements::update::UpdateStatement;
use surrealdb_core::val::TableName;

use super::expressions;
use crate::error::TranslateError;

pub fn translate_statement(stmt: pg::Statement) -> Result<TopLevelExpr, TranslateError> {
	match stmt {
		pg::Statement::Query(query) => translate_query(*query),
		pg::Statement::Insert(insert) => translate_insert(insert),
		pg::Statement::Update(update) => translate_update(update),
		pg::Statement::Delete(delete) => translate_delete(delete),
		pg::Statement::CreateTable(ct) => translate_create_table(ct),
		pg::Statement::CreateIndex(ci) => translate_create_index(ci),
		pg::Statement::Drop {
			object_type,
			names,
			if_exists,
			..
		} => translate_drop(object_type, names, if_exists),
		pg::Statement::StartTransaction {
			..
		} => Ok(TopLevelExpr::Begin),
		pg::Statement::Commit {
			..
		} => Ok(TopLevelExpr::Commit),
		pg::Statement::Rollback {
			..
		} => Ok(TopLevelExpr::Cancel),
		other => Err(TranslateError::unsupported(format!("statement: {other}"))),
	}
}

fn translate_query(query: pg::Query) -> Result<TopLevelExpr, TranslateError> {
	let order_by = query
		.order_by
		.map(|ob| match ob.kind {
			pg::OrderByKind::All(_opts) => Vec::new(),
			pg::OrderByKind::Expressions(exprs) => exprs,
		})
		.unwrap_or_default();

	let (limit_expr, offset) = match query.limit_clause {
		Some(pg::LimitClause::LimitOffset {
			limit,
			offset,
			..
		}) => (limit, offset),
		_ => (None, None),
	};

	let body = *query.body;
	match body {
		SetExpr::Select(select) => {
			let select_stmt = translate_select(*select, order_by, limit_expr, offset)?;
			Ok(TopLevelExpr::Expr(Expr::Select(Box::new(select_stmt))))
		}
		other => Err(TranslateError::unsupported(format!("query body: {other}"))),
	}
}

fn translate_select(
	select: pg::Select,
	order_by: Vec<pg::OrderByExpr>,
	limit: Option<pg::Expr>,
	offset: Option<pg::Offset>,
) -> Result<SelectStatement, TranslateError> {
	let fields = expressions::translate_select_items(select.projection)?;
	let what = translate_from(select.from)?;
	let cond = expressions::translate_where(select.selection)?;

	let order = if order_by.is_empty() {
		None
	} else {
		Some(expressions::translate_order_by(order_by)?)
	};

	let limit_expr = expressions::translate_limit(limit)?;
	let offset_expr = expressions::translate_offset(offset)?;

	let group = translate_group_by(&select.group_by)?;

	Ok(SelectStatement {
		fields,
		omit: Vec::new(),
		only: false,
		what,
		with: None,
		cond,
		split: None,
		group,
		order,
		limit: wrap_limit(limit_expr),
		start: wrap_start(offset_expr),
		fetch: None,
		version: Expr::Literal(Literal::None),
		timeout: Expr::Literal(Literal::None),
		explain: None,
		tempfiles: false,
	})
}

fn translate_group_by(group_by: &pg::GroupByExpr) -> Result<Option<Groups>, TranslateError> {
	match group_by {
		pg::GroupByExpr::All(_) => Err(TranslateError::unsupported("GROUP BY ALL")),
		pg::GroupByExpr::Expressions(exprs, _) => {
			if exprs.is_empty() {
				return Ok(None);
			}
			let groups: Result<Vec<_>, _> = exprs
				.iter()
				.map(|e| {
					let expr = expressions::translate_expr(e.clone())?;
					match expr {
						Expr::Idiom(idiom) => Ok(Group(idiom)),
						Expr::Literal(Literal::String(s)) => Ok(Group(Idiom(vec![Part::Field(s)]))),
						_ => Err(TranslateError::mapping("GROUP BY requires field references")),
					}
				})
				.collect();
			Ok(Some(Groups(groups?)))
		}
	}
}

fn translate_from(from: Vec<pg::TableWithJoins>) -> Result<Vec<Expr>, TranslateError> {
	let mut results = Vec::new();
	for table_with_joins in from {
		let (left_expr, left_alias) = translate_table_factor(table_with_joins.relation)?;

		if table_with_joins.joins.is_empty() {
			results.push(left_expr);
		} else {
			let mut current = left_expr;
			let mut current_alias = left_alias;

			for join in table_with_joins.joins {
				let (right_expr, right_alias) = translate_table_factor(join.relation)?;
				let (kind, cond) = translate_join_operator(join.join_operator)?;

				current = Expr::Join(Box::new(JoinExpr {
					kind,
					left: current,
					right: right_expr,
					cond,
					left_alias: current_alias,
					right_alias: right_alias.clone(),
				}));
				current_alias = None;
			}

			results.push(current);
		}
	}
	Ok(results)
}

fn translate_table_factor(factor: TableFactor) -> Result<(Expr, Option<String>), TranslateError> {
	match factor {
		TableFactor::Table {
			name,
			alias,
			..
		} => {
			let table_name = name.to_string();
			let alias_str = alias.map(|a| a.name.value);
			Ok((Expr::Table(TableName(table_name)), alias_str))
		}
		other => Err(TranslateError::unsupported(format!("table factor: {other}"))),
	}
}

fn translate_join_operator(
	op: pg::JoinOperator,
) -> Result<(JoinKind, Option<Cond>), TranslateError> {
	match op {
		pg::JoinOperator::Inner(constraint) | pg::JoinOperator::Join(constraint) => {
			let cond = translate_join_constraint(constraint)?;
			Ok((JoinKind::Inner, cond))
		}
		pg::JoinOperator::Left(constraint) | pg::JoinOperator::LeftOuter(constraint) => {
			let cond = translate_join_constraint(constraint)?;
			Ok((JoinKind::Left, cond))
		}
		pg::JoinOperator::Right(constraint) | pg::JoinOperator::RightOuter(constraint) => {
			let cond = translate_join_constraint(constraint)?;
			Ok((JoinKind::Right, cond))
		}
		pg::JoinOperator::CrossJoin(_) => Ok((JoinKind::Cross, None)),
		other => Err(TranslateError::unsupported(format!("join type: {other:?}"))),
	}
}

fn translate_join_constraint(
	constraint: pg::JoinConstraint,
) -> Result<Option<Cond>, TranslateError> {
	match constraint {
		pg::JoinConstraint::On(expr) => {
			let translated = expressions::translate_expr(expr)?;
			Ok(Some(Cond(translated)))
		}
		pg::JoinConstraint::None => Ok(None),
		other => Err(TranslateError::unsupported(format!("join constraint: {other:?}"))),
	}
}

fn translate_insert(insert: pg::Insert) -> Result<TopLevelExpr, TranslateError> {
	let table_name = match &insert.table {
		pg::TableObject::TableName(name) => name.to_string(),
		other => return Err(TranslateError::unsupported(format!("INSERT target: {other}"))),
	};

	let columns: Vec<Idiom> =
		insert.columns.iter().map(|col| Idiom(vec![Part::Field(col.value.clone())])).collect();

	let source = insert.source.ok_or_else(|| TranslateError::mapping("INSERT without VALUES"))?;
	match *source.body {
		SetExpr::Values(values) => {
			let mut rows = Vec::new();
			for row in values.rows {
				let mut pairs = Vec::new();
				for (i, val_expr) in row.into_iter().enumerate() {
					let col = columns.get(i).cloned().ok_or_else(|| {
						TranslateError::mapping("more values than columns in INSERT")
					})?;
					let expr = expressions::translate_expr(val_expr)?;
					pairs.push((col, expr));
				}
				rows.push(pairs);
			}

			let data = Data::ValuesExpression(rows);
			let into = Some(Expr::Table(TableName(table_name)));

			Ok(TopLevelExpr::Expr(Expr::Insert(Box::new(InsertStatement {
				into,
				data,
				ignore: false,
				update: None,
				output: None,
				timeout: Expr::Literal(Literal::None),
				relation: false,
			}))))
		}
		other => Err(TranslateError::unsupported(format!("INSERT source: {other}"))),
	}
}

fn translate_update(update: pg::Update) -> Result<TopLevelExpr, TranslateError> {
	let table_name = update.table.relation.to_string();
	let what = vec![Expr::Table(TableName(table_name))];

	let assignments: Result<Vec<Assignment>, TranslateError> = update
		.assignments
		.into_iter()
		.map(|a| {
			let col_parts: Vec<Part> = match a.target {
				pg::AssignmentTarget::ColumnName(name) => name
					.0
					.iter()
					.map(|i| {
						Part::Field(
							i.as_ident()
								.map(|id| id.value.clone())
								.unwrap_or_else(|| i.to_string()),
						)
					})
					.collect(),
				pg::AssignmentTarget::Tuple(names) => {
					names.into_iter().map(|n| Part::Field(n.to_string())).collect()
				}
			};
			let idiom = Idiom(col_parts);
			let value = expressions::translate_expr(a.value)?;
			Ok(Assignment {
				place: idiom,
				operator: AssignOperator::Assign,
				value,
			})
		})
		.collect();

	let data = Data::SetExpression(assignments?);
	let cond = expressions::translate_where(update.selection)?;

	Ok(TopLevelExpr::Expr(Expr::Update(Box::new(UpdateStatement {
		only: false,
		what,
		with: None,
		data: Some(data),
		cond,
		output: None,
		timeout: Expr::Literal(Literal::None),
		explain: None,
	}))))
}

fn translate_delete(delete: pg::Delete) -> Result<TopLevelExpr, TranslateError> {
	let table_name = match delete.from {
		pg::FromTable::WithFromKeyword(ref tables) | pg::FromTable::WithoutKeyword(ref tables) => {
			tables.first().map(|twj| twj.relation.to_string()).unwrap_or_default()
		}
	};

	let what = vec![Expr::Table(TableName(table_name))];
	let cond = expressions::translate_where(delete.selection)?;

	Ok(TopLevelExpr::Expr(Expr::Delete(Box::new(DeleteStatement {
		only: false,
		what,
		with: None,
		cond,
		output: None,
		timeout: Expr::Literal(Literal::None),
		explain: None,
	}))))
}

fn wrap_limit(expr: Expr) -> Option<Limit> {
	match expr {
		Expr::Literal(Literal::None) => None,
		e => Some(Limit(e)),
	}
}

fn wrap_start(expr: Expr) -> Option<Start> {
	match expr {
		Expr::Literal(Literal::None) => None,
		e => Some(Start(e)),
	}
}

fn translate_create_table(_ct: pg::CreateTable) -> Result<TopLevelExpr, TranslateError> {
	// DDL translation requires access to catalog types (Permissions, TableType, Index)
	// which are currently pub(crate) in surrealdb-core. For now, return unsupported
	// until those types are opened up.
	Err(TranslateError::unsupported_with_hint(
		"CREATE TABLE",
		"DDL translation requires catalog types to be made public in surrealdb-core",
	))
}

fn translate_create_index(_ci: pg::CreateIndex) -> Result<TopLevelExpr, TranslateError> {
	Err(TranslateError::unsupported_with_hint(
		"CREATE INDEX",
		"DDL translation requires catalog types to be made public in surrealdb-core",
	))
}

fn translate_drop(
	object_type: pg::ObjectType,
	names: Vec<pg::ObjectName>,
	if_exists: bool,
) -> Result<TopLevelExpr, TranslateError> {
	use surrealdb_core::expr::statements::remove::{RemoveStatement, RemoveTableStatement};

	match object_type {
		pg::ObjectType::Table => {
			let name = names.first().map(|n| n.to_string()).unwrap_or_default();
			Ok(TopLevelExpr::Expr(Expr::Remove(Box::new(RemoveStatement::Table(
				RemoveTableStatement {
					name: Expr::Literal(Literal::String(name)),
					if_exists,
					expunge: false,
				},
			)))))
		}
		other => Err(TranslateError::unsupported(format!("DROP {other}"))),
	}
}
