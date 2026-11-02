//! Metadata-only binding of PostgreSQL writes. The analysis query is never executed.

use std::collections::{HashMap, HashSet};

use surrealdb_pgsql::ast::expr::{Expr, QualifiedName};
use surrealdb_pgsql::ast::stmt::{OnConflictAction, ResTarget, SelectStmt, Statement, TableRef};

use super::{ParameterDescriptor, ResultDescriptor, SqlType};
use crate::binder::pgsql::{bind_expr, bind_select};
use crate::ir::BoundSelect;
use crate::{BinderCatalog, BoundBinaryOp, BoundExpr, DialectPolicy, PlanError, QueryScope};

/// A SELECT-shaped analysis of a write's target, predicates and returned expressions.
/// It supplies catalog dependencies without executing or lowering the write.
pub struct PgWriteAnalysis {
	pub query: SelectStmt,
	relation: QualifiedName,
	assignments: Vec<(String, Expr)>,
	/// The `WHERE` of `ON CONFLICT DO UPDATE`, which may read `EXCLUDED`.
	conflict_condition: Option<Expr>,
	/// The alias of the target table of an `INSERT`.
	alias: Option<String>,
	returning: bool,
}

pub struct PgWriteMetadata {
	pub parameters: ParameterDescriptor,
	pub result: Option<ResultDescriptor>,
	/// The declared types of the columns a `RETURNING *` stands for.
	pub wildcard_types: HashMap<String, SqlType>,
	/// The SQL types the columns a `RETURNING *` stands for are declared with.
	pub wildcard_declared: HashMap<String, crate::declared_type::DeclaredSqlType>,
}

impl PgWriteAnalysis {
	pub fn new(statement: &Statement) -> Option<Self> {
		let mut conflict_condition = None;
		let mut alias = None;
		let (relation, with, from, predicate, returning, assignments, span) = match statement {
			Statement::Insert(insert) => {
				alias.clone_from(&insert.alias);
				let mut assignments = Vec::new();
				if let Some(rows) = &insert.values {
					for row in rows {
						assignments.extend(insert.columns.iter().cloned().zip(row.iter().cloned()));
					}
				}
				if let Some(conflict) = &insert.on_conflict
					&& let OnConflictAction::DoUpdate(items) = &conflict.action
				{
					assignments
						.extend(items.iter().map(|item| (item.column.clone(), item.value.clone())));
					conflict_condition.clone_from(&conflict.where_clause);
				}
				(
					&insert.relation,
					&insert.with,
					None,
					None,
					&insert.returning,
					assignments,
					insert.span,
				)
			}
			Statement::Update(update) => (
				&update.relation,
				&update.with,
				update.from.as_ref(),
				update.where_clause.clone(),
				&update.returning,
				update
					.assignments
					.iter()
					.map(|item| (item.column.clone(), item.value.clone()))
					.collect(),
				update.span,
			),
			Statement::Delete(delete) => (
				&delete.relation,
				&delete.with,
				delete.using.as_ref(),
				delete.where_clause.clone(),
				&delete.returning,
				Vec::new(),
				delete.span,
			),
			Statement::Copy(_)
			| Statement::Select(_)
			| Statement::Truncate(_)
			| Statement::CreateTable(_)
			| Statement::CreateIndex(_)
			| Statement::CreateView(_)
			| Statement::AlterTable(_)
			| Statement::CreateSchema(_)
			| Statement::CreateExtension(_)
			| Statement::CreateType(_)
			| Statement::AlterType(_)
			| Statement::CreateDomain(_)
			| Statement::AlterDomain(_)
			| Statement::CreateCompositeType(_)
			| Statement::Drop(_)
			| Statement::Transaction(_)
			| Statement::Set(_)
			| Statement::Show(_)
			| Statement::Reset(_)
			| Statement::Explain(_)
			| Statement::Prepare(_)
			| Statement::Execute(_)
			| Statement::Deallocate(_)
			| Statement::CreatePropertyGraph(_)
			| Statement::DropPropertyGraph(_)
			| Statement::Empty => return None,
		};
		let mut sources = vec![TableRef::Table {
			name: relation.clone(),
			alias: alias.clone().map(|name| surrealdb_pgsql::ast::stmt::TableAlias {
				name,
				columns: Vec::new(),
				span: relation.span,
			}),
			span: relation.span,
		}];
		sources.extend(from.into_iter().flatten().cloned());
		Some(Self {
			query: SelectStmt {
				with: with.clone(),
				distinct: None,
				targets: returning.clone().unwrap_or_default(),
				from: sources,
				where_clause: predicate,
				group_by: Vec::new(),
				having: None,
				window: Vec::new(),
				order_by: Vec::new(),
				limit: None,
				offset: None,
				locking: Vec::new(),
				set_op: None,
				span,
			},
			relation: relation.clone(),
			assignments,
			conflict_condition,
			alias,
			returning: returning.is_some(),
		})
	}

	pub fn describe(
		&self,
		catalog: &dyn BinderCatalog,
		declared: &[i32],
	) -> Result<PgWriteMetadata, PlanError> {
		if self.query.from.len() > 1 {
			return Err(PlanError::UnsupportedStatement(
				"UPDATE FROM and DELETE USING require join-based write lowering".into(),
			));
		}
		let policy = DialectPolicy::postgres();
		let bound = bind_select(&self.query, catalog, &policy, None)?;
		let mut enum_params: HashMap<usize, u32> = HashMap::new();
		let mut params: HashMap<usize, SqlType> = bound
			.describe_parameters(catalog)
			.param_types
			.into_iter()
			.enumerate()
			.map(|(i, ty)| (i + 1, ty))
			.collect();
		let (schema, table) = crate::resolve_pgsql_table_name(&self.relation.parts, Some(catalog))?;
		let mut scope = QueryScope::new();
		scope.register_relation(schema, &table, self.alias.clone(), catalog, &policy);
		// EXCLUDED has the target's column types, without adding an ambiguous second relation.
		scope.name_to_relation.insert("excluded".to_string(), 0);
		for (column, value) in &self.assignments {
			let target = BoundExpr::Column(scope.resolve_column(
				std::slice::from_ref(column),
				&policy,
				catalog,
			)?);
			if is_default_keyword(value) {
				continue;
			}
			let value = bind_expr(value, &scope, catalog, &policy)?;
			let constraint = BoundExpr::Binary {
				left: Box::new(target),
				op: BoundBinaryOp::Equal,
				right: Box::new(value),
			};
			bound.collect_param_types_from_expr(&constraint, catalog, &mut params);
			BoundSelect::collect_enum_params(&constraint, catalog, &mut enum_params);
		}
		if let Some(condition) = &self.conflict_condition {
			let condition = bind_expr(condition, &scope, catalog, &policy)?;
			bound.collect_param_types_from_expr(&condition, catalog, &mut params);
		}
		let max = params.keys().copied().max().unwrap_or(0);
		let parameters = ParameterDescriptor {
			param_types: (1..=max)
				.map(|i| params.get(&i).copied().unwrap_or(SqlType::Unknown))
				.collect(),
			enum_params: BoundSelect::enum_param_slots(&enum_params, max),
		};
		let result = if self.returning {
			bound.describe_static(catalog, &policy)
		} else {
			None
		};
		let result = result.map(|mut result| {
			// The rows of a write are named by `returning_name`, whatever the analysis query
			// would have named its columns, so they are read by name.
			for column in &mut result.columns {
				column.key = None;
			}
			if result.columns.len() == self.query.targets.len() {
				for (index, (column, target)) in
					result.columns.iter_mut().zip(&self.query.targets).enumerate()
				{
					column.name = returning_name(target);
					if let BoundExpr::Param {
						index,
						..
					} = &bound.targets[index].expr
					{
						column.ty = declared
							.get(index - 1)
							.and_then(|oid| SqlType::from_pg_oid(*oid))
							.or_else(|| parameters.param_types.get(index - 1).copied())
							.unwrap_or(SqlType::Unknown);
					}
				}
			}
			result
		});
		if let Some(result) = &result {
			let mut names = HashSet::new();
			if result.columns.iter().any(|column| !names.insert(&column.name)) {
				return Err(PlanError::UnsupportedExpression(
					"duplicate RETURNING column names; use distinct aliases".to_string(),
				));
			}
		}
		let (wildcard_types, wildcard_declared) = if self.returning && result.is_none() {
			(bound.wildcard_column_types(catalog), bound.wildcard_declared_sql_types(catalog))
		} else {
			(HashMap::new(), HashMap::new())
		};
		Ok(PgWriteMetadata {
			parameters,
			result,
			wildcard_types,
			wildcard_declared,
		})
	}
}

/// Whether `expr` is the `DEFAULT` keyword of a `VALUES` row or an assignment, which the parser
/// reads as a column reference of that name. It stands for the column's default and names no
/// column of the table.
fn is_default_keyword(expr: &Expr) -> bool {
	matches!(
		expr,
		Expr::ColumnRef(column)
			if column.fields.len() == 1 && column.fields[0].eq_ignore_ascii_case("DEFAULT")
	)
}

/// The label shared by RETURNING metadata and write lowering.
pub fn returning_name(target: &ResTarget) -> String {
	fn label(expr: &Expr) -> String {
		match expr {
			Expr::ColumnRef(column) => {
				column.fields.last().cloned().unwrap_or_else(|| "?column?".into())
			}
			Expr::FunctionCall {
				name,
				..
			} => name.parts.last().cloned().unwrap_or_else(|| "?column?".into()),
			Expr::FieldSelect {
				field,
				..
			} => field.clone(),
			Expr::Grouping {
				expr,
				..
			}
			| Expr::TypeCast {
				expr,
				..
			} => label(expr),
			Expr::Literal {
				..
			}
			| Expr::Param {
				..
			}
			| Expr::UnaryOp {
				..
			}
			| Expr::BinaryOp {
				..
			}
			| Expr::Subscript {
				..
			}
			| Expr::Slice {
				..
			}
			| Expr::Between {
				..
			}
			| Expr::InList {
				..
			}
			| Expr::IsNull {
				..
			}
			| Expr::IsTrue {
				..
			}
			| Expr::IsFalse {
				..
			}
			| Expr::IsUnknown {
				..
			}
			| Expr::IsDistinctFrom {
				..
			}
			| Expr::Case {
				..
			}
			| Expr::Collate {
				..
			}
			| Expr::ArrayConstructor {
				..
			}
			| Expr::Row {
				..
			}
			| Expr::IsBound {
				..
			}
			| Expr::JsonObject {
				..
			}
			| Expr::JsonArray {
				..
			}
			| Expr::JsonExists {
				..
			}
			| Expr::JsonValue {
				..
			}
			| Expr::JsonQuery {
				..
			}
			| Expr::Subquery {
				..
			}
			| Expr::Exists {
				..
			}
			| Expr::InSubquery {
				..
			}
			| Expr::ArraySubquery {
				..
			}
			| Expr::Quantified {
				..
			}
			| Expr::QuantifiedArray {
				..
			} => "?column?".into(),
		}
	}
	target.alias.clone().unwrap_or_else(|| label(&target.expr))
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::MemoryCatalog;

	fn describe(sql: &str) -> Result<PgWriteMetadata, PlanError> {
		let catalog = MemoryCatalog::new()
			.with_column_type("people", "id", SqlType::Int8)
			.with_column_type("people", "age", SqlType::Int8)
			.with_column_type("people", "active", SqlType::Bool)
			.with_column_type("people", "name", SqlType::Text);
		let statements = surrealdb_pgsql::parse_statements(sql).unwrap();
		PgWriteAnalysis::new(&statements[0]).unwrap().describe(&catalog, &[])
	}

	#[test]
	fn the_where_of_do_update_constrains_parameters_through_a_qualified_column() {
		let metadata = describe("INSERT INTO \"public\".\"people\" (id, name, age, active) VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO UPDATE SET age = $5 WHERE (\"public\".\"people\".\"id\" = $6 AND 1 = 1) RETURNING id, age").unwrap();
		assert_eq!(
			metadata.parameters.param_types,
			[
				SqlType::Int8,
				SqlType::Text,
				SqlType::Int8,
				SqlType::Bool,
				SqlType::Int8,
				SqlType::Int8
			]
		);
		let metadata = describe("INSERT INTO people AS p (id, age) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET age = EXCLUDED.age WHERE p.age < $3 AND EXCLUDED.age > $4").unwrap();
		assert_eq!(
			metadata.parameters.param_types,
			[SqlType::Int8, SqlType::Int8, SqlType::Int8, SqlType::Int8]
		);
	}

	#[test]
	fn multiple_values_and_conflict_assignments_constrain_parameters() {
		let metadata = describe("INSERT INTO people (id, active) VALUES ($1, $2), ($3, $4) ON CONFLICT (id) DO UPDATE SET active = $5 RETURNING active, id").unwrap();
		assert_eq!(
			metadata.parameters.param_types,
			[SqlType::Int8, SqlType::Bool, SqlType::Int8, SqlType::Bool, SqlType::Bool]
		);
		let columns = metadata.result.unwrap().columns;
		assert_eq!(
			columns.iter().map(|c| (c.name.as_str(), c.ty)).collect::<Vec<_>>(),
			[("active", SqlType::Bool), ("id", SqlType::Int8)]
		);
	}

	#[test]
	fn explicit_casts_and_sparse_parameters_keep_their_types() {
		let metadata = describe("UPDATE people SET age = $3::int4 WHERE id = $2 AND $1 IS NULL RETURNING age AS years, $3 AS input").unwrap();
		assert_eq!(
			metadata.parameters.param_types,
			[SqlType::Unknown, SqlType::Int8, SqlType::Int4]
		);
		let columns = metadata.result.unwrap().columns;
		assert_eq!(
			columns.iter().map(|c| (c.name.as_str(), c.ty)).collect::<Vec<_>>(),
			[("years", SqlType::Int8), ("input", SqlType::Int4)]
		);
	}

	#[test]
	fn the_columns_of_a_returning_list_are_read_by_name() {
		let columns = describe("DELETE FROM people RETURNING id, age::int4, name AS who")
			.unwrap()
			.result
			.unwrap()
			.columns;
		assert_eq!(
			columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
			["id", "age", "who"]
		);
		assert!(columns.iter().all(|column| column.key.is_none()));
	}

	#[test]
	fn duplicate_returning_labels_are_rejected_before_execution() {
		assert!(describe("DELETE FROM people RETURNING id, age AS id").is_err());
		assert!(describe("DELETE FROM people RETURNING id AS key, age AS years").is_ok());
	}

	#[test]
	fn wildcard_metadata_remains_dynamic_and_no_returning_has_no_result() {
		assert!(describe("DELETE FROM people RETURNING *").unwrap().result.is_none());
		let metadata = describe("UPDATE people SET active = $1 WHERE id = $2").unwrap();
		assert!(metadata.result.is_none());
		assert_eq!(metadata.parameters.param_types, [SqlType::Bool, SqlType::Int8]);
	}
}
