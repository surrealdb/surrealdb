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

/// If the FROM clause is a single unjoined table with an alias, return that alias.
fn detect_single_table_alias(from: &[pg::TableWithJoins]) -> Option<String> {
	if from.len() != 1 || !from[0].joins.is_empty() {
		return None;
	}
	match &from[0].relation {
		TableFactor::Table {
			alias: Some(a),
			..
		} => Some(a.name.value.clone()),
		_ => None,
	}
}

/// Rewrite a `pg::Select` and its companion `ORDER BY` list in-place,
/// stripping every leading `alias.` qualifier from compound identifiers.
fn strip_alias_from_select(
	select: &mut pg::Select,
	order_by: &mut Vec<pg::OrderByExpr>,
	alias: &str,
) {
	select.projection =
		select.projection.drain(..).map(|item| strip_qualifier_select_item(item, alias)).collect();

	select.selection = select.selection.take().map(|e| strip_qualifier_expr(e, alias));

	if let pg::GroupByExpr::Expressions(exprs, modifiers) = &mut select.group_by {
		*exprs = exprs.drain(..).map(|e| strip_qualifier_expr(e, alias)).collect();
		let _ = modifiers;
	}

	for o in order_by.iter_mut() {
		o.expr = strip_qualifier_expr(o.expr.clone(), alias);
	}
}

fn strip_qualifier_select_item(item: pg::SelectItem, alias: &str) -> pg::SelectItem {
	match item {
		pg::SelectItem::UnnamedExpr(e) => {
			pg::SelectItem::UnnamedExpr(strip_qualifier_expr(e, alias))
		}
		pg::SelectItem::ExprWithAlias {
			expr,
			alias: a,
		} => pg::SelectItem::ExprWithAlias {
			expr: strip_qualifier_expr(expr, alias),
			alias: a,
		},
		pg::SelectItem::QualifiedWildcard(ref kind, _) => {
			let matches = match kind {
				pg::SelectItemQualifiedWildcardKind::ObjectName(name) => {
					name.0.len() == 1
						&& name.0[0]
							.as_ident()
							.map(|id| id.value.eq_ignore_ascii_case(alias))
							.unwrap_or(false)
				}
				_ => false,
			};
			if matches {
				pg::SelectItem::Wildcard(pg::WildcardAdditionalOptions::default())
			} else {
				item
			}
		}
		other => other,
	}
}

/// Recursively strip a leading table qualifier from compound identifiers.
/// Does NOT descend into subquery bodies (they have independent scoping).
fn strip_qualifier_expr(expr: pg::Expr, alias: &str) -> pg::Expr {
	match expr {
		pg::Expr::CompoundIdentifier(ref parts)
			if parts.len() >= 2 && parts[0].value.eq_ignore_ascii_case(alias) =>
		{
			if parts.len() == 2 {
				pg::Expr::Identifier(parts[1].clone())
			} else {
				pg::Expr::CompoundIdentifier(parts[1..].to_vec())
			}
		}

		pg::Expr::BinaryOp {
			left,
			op,
			right,
		} => pg::Expr::BinaryOp {
			left: Box::new(strip_qualifier_expr(*left, alias)),
			op,
			right: Box::new(strip_qualifier_expr(*right, alias)),
		},

		pg::Expr::UnaryOp {
			op,
			expr: inner,
		} => pg::Expr::UnaryOp {
			op,
			expr: Box::new(strip_qualifier_expr(*inner, alias)),
		},

		pg::Expr::Nested(inner) => pg::Expr::Nested(Box::new(strip_qualifier_expr(*inner, alias))),

		pg::Expr::IsNull(inner) => pg::Expr::IsNull(Box::new(strip_qualifier_expr(*inner, alias))),
		pg::Expr::IsNotNull(inner) => {
			pg::Expr::IsNotNull(Box::new(strip_qualifier_expr(*inner, alias)))
		}

		pg::Expr::Between {
			expr: e,
			negated,
			low,
			high,
		} => pg::Expr::Between {
			expr: Box::new(strip_qualifier_expr(*e, alias)),
			negated,
			low: Box::new(strip_qualifier_expr(*low, alias)),
			high: Box::new(strip_qualifier_expr(*high, alias)),
		},

		pg::Expr::InList {
			expr: e,
			list,
			negated,
		} => pg::Expr::InList {
			expr: Box::new(strip_qualifier_expr(*e, alias)),
			list: list.into_iter().map(|l| strip_qualifier_expr(l, alias)).collect(),
			negated,
		},

		// Do NOT descend into subquery bodies -- they have their own scope.
		pg::Expr::InSubquery {
			expr: e,
			subquery,
			negated,
		} => pg::Expr::InSubquery {
			expr: Box::new(strip_qualifier_expr(*e, alias)),
			subquery,
			negated,
		},

		pg::Expr::Function(mut func) => {
			func.args = match func.args {
				pg::FunctionArguments::List(mut list) => {
					list.args = list
						.args
						.into_iter()
						.map(|arg| match arg {
							pg::FunctionArg::Unnamed(pg::FunctionArgExpr::Expr(e)) => {
								pg::FunctionArg::Unnamed(pg::FunctionArgExpr::Expr(
									strip_qualifier_expr(e, alias),
								))
							}
							pg::FunctionArg::Named {
								name,
								arg: pg::FunctionArgExpr::Expr(e),
								operator,
							} => pg::FunctionArg::Named {
								name,
								arg: pg::FunctionArgExpr::Expr(strip_qualifier_expr(e, alias)),
								operator,
							},
							other => other,
						})
						.collect();
					pg::FunctionArguments::List(list)
				}
				other => other,
			};
			pg::Expr::Function(func)
		}

		pg::Expr::Cast {
			expr: e,
			data_type,
			format,
			kind,
			array,
		} => pg::Expr::Cast {
			expr: Box::new(strip_qualifier_expr(*e, alias)),
			data_type,
			format,
			kind,
			array,
		},

		pg::Expr::Like {
			negated,
			expr: e,
			pattern,
			escape_char,
			any,
		} => pg::Expr::Like {
			negated,
			expr: Box::new(strip_qualifier_expr(*e, alias)),
			pattern: Box::new(strip_qualifier_expr(*pattern, alias)),
			escape_char,
			any,
		},

		pg::Expr::ILike {
			negated,
			expr: e,
			pattern,
			escape_char,
			any,
		} => pg::Expr::ILike {
			negated,
			expr: Box::new(strip_qualifier_expr(*e, alias)),
			pattern: Box::new(strip_qualifier_expr(*pattern, alias)),
			escape_char,
			any,
		},

		other => other,
	}
}

fn translate_select(
	mut select: pg::Select,
	mut order_by: Vec<pg::OrderByExpr>,
	limit: Option<pg::Expr>,
	offset: Option<pg::Offset>,
) -> Result<SelectStatement, TranslateError> {
	// When a single table has an alias (e.g. `FROM users AS u`), strip the
	// qualifier from compound identifiers so `u.name` becomes just `name`.
	// SurrealDB scans produce flat records without alias wrappers.
	if let Some(alias) = detect_single_table_alias(&select.from) {
		strip_alias_from_select(&mut select, &mut order_by, &alias);
	}

	let fields = expressions::translate_select_items(select.projection)?;
	let mut what = translate_from(select.from)?;

	// Extract IN/NOT IN subquery patterns from WHERE and rewrite as Semi/Anti joins.
	let (remaining_where, semi_anti_joins) = extract_semi_anti_joins(select.selection)?;
	for join in semi_anti_joins {
		what = vec![wrap_what_in_join(what, join)];
	}

	let cond = expressions::translate_where(remaining_where)?;

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

/// A Semi or Anti join extracted from a WHERE clause IN/NOT IN subquery.
struct SemiAntiJoin {
	kind: JoinKind,
	right_table: Expr,
	right_alias: Option<String>,
	cond: Cond,
}

/// Extract `col IN (SELECT col FROM table)` / `col NOT IN (...)` patterns
/// from a WHERE expression. Returns the remaining WHERE clause (with the
/// extracted predicates removed) and a list of Semi/Anti joins.
fn extract_semi_anti_joins(
	selection: Option<pg::Expr>,
) -> Result<(Option<pg::Expr>, Vec<SemiAntiJoin>), TranslateError> {
	let Some(expr) = selection else {
		return Ok((None, Vec::new()));
	};

	let mut joins = Vec::new();
	let remaining = extract_semi_anti_recursive(expr, &mut joins)?;
	Ok((remaining, joins))
}

fn extract_semi_anti_recursive(
	expr: pg::Expr,
	joins: &mut Vec<SemiAntiJoin>,
) -> Result<Option<pg::Expr>, TranslateError> {
	match expr {
		pg::Expr::InSubquery {
			expr: left_expr,
			subquery,
			negated,
		} => {
			if let Some(join) = try_build_semi_anti(*left_expr, *subquery, negated)? {
				joins.push(join);
				return Ok(None);
			}
			Err(TranslateError::unsupported("complex IN (SELECT ...) subquery"))
		}
		pg::Expr::BinaryOp {
			left,
			op: pg::BinaryOperator::And,
			right,
		} => {
			let left_remaining = extract_semi_anti_recursive(*left, joins)?;
			let right_remaining = extract_semi_anti_recursive(*right, joins)?;
			match (left_remaining, right_remaining) {
				(Some(l), Some(r)) => Ok(Some(pg::Expr::BinaryOp {
					left: Box::new(l),
					op: pg::BinaryOperator::And,
					right: Box::new(r),
				})),
				(Some(e), None) | (None, Some(e)) => Ok(Some(e)),
				(None, None) => Ok(None),
			}
		}
		other => Ok(Some(other)),
	}
}

fn try_build_semi_anti(
	left_expr: pg::Expr,
	subquery: pg::Query,
	negated: bool,
) -> Result<Option<SemiAntiJoin>, TranslateError> {
	let body = *subquery.body;
	let pg::SetExpr::Select(select) = body else {
		return Ok(None);
	};

	// Must have exactly one FROM table and one projected column
	if select.from.len() != 1 {
		return Ok(None);
	}
	let twj = &select.from[0];
	if !twj.joins.is_empty() {
		return Ok(None);
	}
	let (right_expr, right_alias) = translate_table_factor(twj.relation.clone())?;

	// Extract the single selected column from the subquery
	if select.projection.len() != 1 {
		return Ok(None);
	}
	let sub_col = match &select.projection[0] {
		pg::SelectItem::UnnamedExpr(e) => expressions::translate_expr(e.clone())?,
		_ => return Ok(None),
	};

	let left_translated = expressions::translate_expr(left_expr)?;

	// Build equi-join condition: left_col = right_subquery_col
	let mut cond_expr = Expr::Binary {
		left: Box::new(left_translated),
		op: surrealdb_core::expr::operator::BinaryOperator::Equal,
		right: Box::new(sub_col),
	};

	// Incorporate the subquery's WHERE filter so that predicates like
	// `x IN (SELECT y FROM t WHERE active)` filter the right side.
	if let Some(where_expr) = select.selection {
		let filter = expressions::translate_expr(where_expr)?;
		cond_expr = Expr::Binary {
			left: Box::new(cond_expr),
			op: surrealdb_core::expr::operator::BinaryOperator::And,
			right: Box::new(filter),
		};
	}

	let kind = if negated {
		JoinKind::Anti
	} else {
		JoinKind::Semi
	};

	Ok(Some(SemiAntiJoin {
		kind,
		right_table: right_expr,
		right_alias,
		cond: Cond(cond_expr),
	}))
}

/// Wrap the current FROM list in a Semi/Anti join node.
fn wrap_what_in_join(what: Vec<Expr>, join: SemiAntiJoin) -> Expr {
	let left = what.into_iter().next().unwrap_or(Expr::Literal(Literal::None));

	// Determine left alias: if the left side is already a Join, it has no
	// single alias; otherwise extract from the join or table.
	let left_alias = match &left {
		Expr::Join(j) => j.left_alias.clone(),
		_ => None,
	};

	let alias_suffix = "_semi";
	let right_alias = join.right_alias.or_else(|| Some(alias_suffix.to_string()));

	Expr::Join(Box::new(JoinExpr {
		kind: join.kind,
		left,
		right: join.right_table,
		cond: Some(join.cond),
		left_alias,
		right_alias,
	}))
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
