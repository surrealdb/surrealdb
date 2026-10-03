//! Statement translation from PostgreSQL to SurrealQL.

use std::fmt::Write;

use crate::ast::*;
use crate::translate::context::TranslationContext;
use crate::translate::error::TranslationError;
use crate::translate::expr::translate_expr;

/// A trait for types that can be translated to SurrealQL query strings.
pub trait ToSurrealQl {
	/// Translate this AST node into a SurrealQL string.
	fn to_surrealql(&self, ctx: &TranslationContext) -> Result<String, TranslationError>;
}

impl ToSurrealQl for Statement {
	fn to_surrealql(&self, ctx: &TranslationContext) -> Result<String, TranslationError> {
		let mut out = String::new();
		translate_statement(self, ctx, &mut out)?;
		Ok(out)
	}
}

/// Translates a PostgreSQL statement into its SurrealQL string representation.
pub fn translate_statement(
	stmt: &Statement,
	ctx: &TranslationContext,
	out: &mut String,
) -> Result<(), TranslationError> {
	match stmt {
		Statement::Select(s) => translate_select(s, ctx, out),
		Statement::Insert(s) => translate_insert(s, ctx, out),
		Statement::Update(s) => translate_update(s, ctx, out),
		Statement::Delete(s) => translate_delete(s, ctx, out),
		Statement::CreateTable(s) => translate_create_table(s, ctx, out),
		Statement::Drop(s) => translate_drop(s, out),
		Statement::Transaction(s) => translate_transaction(s, out),
		Statement::Set(s) => translate_set(s, out),
		Statement::Show(s) => translate_show(s, out),
		Statement::Reset(s) => translate_reset(s, out),
		Statement::Explain(s) => translate_explain(s, ctx, out),
		Statement::Empty => Ok(()),
		Statement::CreateIndex(s) => translate_create_index(s, ctx, out),
		Statement::CreateView(s) => translate_create_view(s, ctx, out),
		Statement::CreateSchema(_) => {
			// In SurrealDB, schemas are namespaces or databases.
			out.push_str("-- CREATE SCHEMA (handled at connection scope)");
			Ok(())
		}
		Statement::AlterTable(s) => translate_alter_table(s, ctx, out),
		Statement::CreatePropertyGraph(s) => translate_create_property_graph(s, out),
		Statement::DropPropertyGraph(s) => translate_drop_property_graph(s, out),
		Statement::Prepare(_) | Statement::Execute(_) | Statement::Deallocate(_) => {
			// Prepared statements are managed directly by the wire protocol layer.
			out.push_str("-- PREPARED STATEMENT PROTOCOL");
			Ok(())
		}
	}
}

fn translate_select(
	select: &SelectStmt,
	ctx: &TranslationContext,
	out: &mut String,
) -> Result<(), TranslationError> {
	if select.from.is_empty() {
		out.push_str("RETURN [{ ");
		for (i, target) in select.targets.iter().enumerate() {
			if i > 0 {
				out.push_str(", ");
			}
			let key = target.alias.clone().unwrap_or_else(|| {
				if select.targets.len() == 1 {
					"?column?".to_string()
				} else {
					format!("?column?_{}", i + 1)
				}
			});
			write!(out, "{key:?}: ")?;
			translate_expr(&target.expr, ctx, out)?;
		}
		out.push_str(" }];");
		return Ok(());
	}

	out.push_str("SELECT ");

	if select.distinct.is_some() {
		out.push_str("DISTINCT ");
	}

	if select.targets.is_empty() {
		out.push('*');
	} else {
		let mut first = true;
		for target in &select.targets {
			if !first {
				out.push_str(", ");
			}
			first = false;
			translate_expr(&target.expr, ctx, out)?;
			if let Some(alias) = &target.alias {
				write!(out, " AS {alias}")?;
			}
		}
	}

	if !select.from.is_empty() {
		out.push_str(" FROM ");
		let mut first = true;
		for table_ref in &select.from {
			if !first {
				out.push_str(", ");
			}
			first = false;
			translate_table_ref(table_ref, ctx, out)?;
		}
	}

	if let Some(where_clause) = &select.where_clause {
		out.push_str(" WHERE ");
		translate_expr(where_clause, ctx, out)?;
	}

	if !select.group_by.is_empty() {
		out.push_str(" GROUP BY ");
		let mut first = true;
		for gb in &select.group_by {
			if !first {
				out.push_str(", ");
			}
			first = false;
			translate_expr(gb, ctx, out)?;
		}
	}

	if let Some(having) = &select.having {
		out.push_str(" HAVING ");
		translate_expr(having, ctx, out)?;
	}

	if !select.order_by.is_empty() {
		out.push_str(" ORDER BY ");
		let mut first = true;
		for ob in &select.order_by {
			if !first {
				out.push_str(", ");
			}
			first = false;
			translate_expr(&ob.expr, ctx, out)?;
			match ob.direction {
				Some(OrderDirection::Asc) => out.push_str(" ASC"),
				Some(OrderDirection::Desc) => out.push_str(" DESC"),
				None => {}
			}
		}
	}

	if let Some(limit) = &select.limit {
		out.push_str(" LIMIT ");
		translate_expr(limit, ctx, out)?;
	}

	if let Some(offset) = &select.offset {
		out.push_str(" START ");
		translate_expr(offset, ctx, out)?;
	}

	out.push(';');
	Ok(())
}

fn translate_table_ref(
	table_ref: &TableRef,
	ctx: &TranslationContext,
	out: &mut String,
) -> Result<(), TranslationError> {
	match table_ref {
		TableRef::Table {
			name,
			alias,
			..
		} => {
			let tbl_name = match name.parts.last() {
				Some(p) => p.as_str(),
				None => {
					return Err(TranslationError::Invalid("Empty table name".to_string()));
				}
			};
			out.push_str(tbl_name);
			if let Some(al) = alias {
				write!(out, " AS {}", al.name)?;
			}
			Ok(())
		}
		TableRef::Join {
			left,
			join_type,
			right,
			condition,
			..
		} => {
			translate_table_ref(left, ctx, out)?;
			match join_type {
				JoinType::Inner => out.push_str(" JOIN "),
				JoinType::Left => out.push_str(" LEFT JOIN "),
				JoinType::Right => out.push_str(" RIGHT JOIN "),
				JoinType::Full => out.push_str(" FULL JOIN "),
				JoinType::Cross => out.push_str(" CROSS JOIN "),
			}
			translate_table_ref(right, ctx, out)?;
			if let Some(cond) = condition {
				match cond {
					JoinCondition::On(expr) => {
						out.push_str(" ON ");
						translate_expr(expr, ctx, out)?;
					}
					JoinCondition::Using(cols) => {
						out.push_str(" USING (");
						let mut first = true;
						for c in cols {
							if !first {
								out.push_str(", ");
							}
							first = false;
							out.push_str(c);
						}
						out.push(')');
					}
				}
			}
			Ok(())
		}
		TableRef::Subquery {
			query,
			alias,
			..
		} => {
			out.push('(');
			let mut inner = String::new();
			translate_select(query, ctx, &mut inner)?;
			// Trim trailing semicolon from subquery
			out.push_str(inner.trim_end_matches(';'));
			out.push(')');
			write!(out, " AS {}", alias.name)?;
			Ok(())
		}
		TableRef::GraphTable {
			pattern,
			columns,
			alias,
			..
		} => {
			out.push('(');
			out.push_str("SELECT ");
			if columns.is_empty() {
				out.push('*');
			} else {
				for (i, col) in columns.iter().enumerate() {
					if i > 0 {
						out.push_str(", ");
					}
					translate_expr(&col.expr, ctx, out)?;
					if let Some(ref a) = col.alias {
						write!(out, " AS {a}")?;
					}
				}
			}
			out.push_str(" FROM ");
			translate_graph_pattern(pattern, ctx, out)?;
			out.push(')');
			if let Some(a) = alias {
				write!(out, " AS {}", a.name)?;
			}
			Ok(())
		}
		TableRef::JsonTable(jt) => {
			out.push('(');
			out.push_str("SELECT ");
			for (i, col) in jt.columns.iter().enumerate() {
				if i > 0 {
					out.push_str(", ");
				}
				if let Some(ref p) = col.path {
					let field = p.trim_start_matches("$.");
					write!(out, "{field} AS {}", col.name)?;
				} else {
					write!(out, "{} AS {}", col.name, col.name)?;
				}
			}
			out.push_str(" FROM ");
			translate_expr(&jt.context_item, ctx, out)?;
			out.push(')');
			if let Some(a) = &jt.alias {
				write!(out, " AS {}", a.name)?;
			}
			Ok(())
		}
	}
}

fn translate_insert(
	insert: &InsertStmt,
	ctx: &TranslationContext,
	out: &mut String,
) -> Result<(), TranslationError> {
	let tbl_name = match insert.relation.parts.last() {
		Some(p) => p.as_str(),
		None => {
			return Err(TranslationError::Invalid("Empty relation name".to_string()));
		}
	};

	write!(out, "INSERT INTO {tbl_name}")?;

	if !insert.columns.is_empty() {
		out.push_str(" (");
		let mut first = true;
		for col in &insert.columns {
			if !first {
				out.push_str(", ");
			}
			first = false;
			out.push_str(col);
		}
		out.push(')');
	}

	if let Some(values) = &insert.values {
		out.push_str(" VALUES ");
		let mut first_row = true;
		for row in values {
			if !first_row {
				out.push_str(", ");
			}
			first_row = false;
			out.push('(');
			let mut first_val = true;
			for val in row {
				if !first_val {
					out.push_str(", ");
				}
				first_val = false;
				translate_expr(val, ctx, out)?;
			}
			out.push(')');
		}
	} else if let Some(query) = &insert.query {
		out.push(' ');
		translate_select(query, ctx, out)?;
		return Ok(());
	}

	out.push(';');
	Ok(())
}

fn translate_update(
	update: &UpdateStmt,
	ctx: &TranslationContext,
	out: &mut String,
) -> Result<(), TranslationError> {
	let tbl_name = match update.relation.parts.last() {
		Some(p) => p.as_str(),
		None => {
			return Err(TranslationError::Invalid("Empty relation name".to_string()));
		}
	};

	write!(out, "UPDATE {tbl_name} SET ")?;

	let mut first = true;
	for assign in &update.assignments {
		if !first {
			out.push_str(", ");
		}
		first = false;
		write!(out, "{} = ", assign.column)?;
		translate_expr(&assign.value, ctx, out)?;
	}

	if let Some(where_clause) = &update.where_clause {
		out.push_str(" WHERE ");
		translate_expr(where_clause, ctx, out)?;
	}

	out.push(';');
	Ok(())
}

fn translate_delete(
	delete: &DeleteStmt,
	ctx: &TranslationContext,
	out: &mut String,
) -> Result<(), TranslationError> {
	let tbl_name = match delete.relation.parts.last() {
		Some(p) => p.as_str(),
		None => {
			return Err(TranslationError::Invalid("Empty relation name".to_string()));
		}
	};

	write!(out, "DELETE FROM {tbl_name}")?;

	if let Some(where_clause) = &delete.where_clause {
		out.push_str(" WHERE ");
		translate_expr(where_clause, ctx, out)?;
	}

	out.push(';');
	Ok(())
}

fn translate_create_table(
	create: &CreateTableStmt,
	_ctx: &TranslationContext,
	out: &mut String,
) -> Result<(), TranslationError> {
	let tbl_name = match create.name.parts.last() {
		Some(p) => p.as_str(),
		None => {
			return Err(TranslationError::Invalid("Empty table name".to_string()));
		}
	};

	writeln!(out, "DEFINE TABLE {tbl_name} SCHEMAFULL;")?;

	for col in &create.columns {
		let col_type = match col.data_type.name.parts.last() {
			Some(t) => match t.to_ascii_lowercase().as_str() {
				"int" | "integer" | "int2" | "int4" | "int8" | "bigint" | "smallint" => "int",
				"float" | "float4" | "float8" | "real" | "numeric" | "decimal" => "float",
				"text" | "varchar" | "char" | "bpchar" | "string" => "string",
				"bool" | "boolean" => "bool",
				"date" | "time" | "timestamp" | "timestamptz" => "datetime",
				"json" | "jsonb" => "object",
				"bytea" => "bytes",
				_ => "string",
			},
			None => "string",
		};

		write!(out, "DEFINE FIELD {} ON TABLE {tbl_name} TYPE {col_type}", col.name)?;

		for con in &col.constraints {
			if matches!(con, ColumnConstraint::NotNull) {
				out.push_str(" ASSERT $value != NONE");
			}
		}
		writeln!(out, ";")?;

		for con in &col.constraints {
			if matches!(con, ColumnConstraint::PrimaryKey) {
				writeln!(
					out,
					"DEFINE INDEX {tbl_name}_{}_pk ON TABLE {tbl_name} COLUMNS {} UNIQUE;",
					col.name, col.name
				)?;
			}
		}
	}

	for con in &create.constraints {
		if let TableConstraint::PrimaryKey(cols) = con {
			let cols_joined = cols.join(", ");
			let cols_name = cols.join("_");
			writeln!(
				out,
				"DEFINE INDEX {tbl_name}_{cols_name}_pk ON TABLE {tbl_name} COLUMNS {cols_joined} UNIQUE;"
			)?;
		}
	}

	Ok(())
}

fn translate_drop(drop: &DropStmt, out: &mut String) -> Result<(), TranslationError> {
	let mut first = true;
	for name in &drop.names {
		if !first {
			out.push('\n');
		}
		first = false;
		let tbl_name = match name.parts.last() {
			Some(p) => p.as_str(),
			None => {
				return Err(TranslationError::Invalid("Empty object name".to_string()));
			}
		};
		write!(out, "REMOVE TABLE ")?;
		if drop.if_exists {
			out.push_str("IF EXISTS ");
		}
		write!(out, "{tbl_name};")?;
	}
	Ok(())
}

fn translate_transaction(tx: &TransactionStmt, out: &mut String) -> Result<(), TranslationError> {
	match tx {
		TransactionStmt::Begin {
			..
		} => out.push_str("BEGIN TRANSACTION;"),
		TransactionStmt::Commit {
			..
		} => out.push_str("COMMIT TRANSACTION;"),
		TransactionStmt::Rollback {
			..
		} => out.push_str("CANCEL TRANSACTION;"),
		TransactionStmt::Savepoint {
			name,
			..
		} => write!(out, "-- SAVEPOINT {name};")?,
		TransactionStmt::ReleaseSavepoint {
			name,
			..
		} => write!(out, "-- RELEASE SAVEPOINT {name};")?,
		TransactionStmt::RollbackToSavepoint {
			name,
			..
		} => write!(out, "-- ROLLBACK TO SAVEPOINT {name};")?,
	}
	Ok(())
}

fn translate_set(set: &SetStmt, out: &mut String) -> Result<(), TranslationError> {
	write!(out, "-- SET {} = '{}';", set.name, set.value)?;
	Ok(())
}

fn translate_show(show: &ShowStmt, out: &mut String) -> Result<(), TranslationError> {
	write!(out, "-- SHOW {};", show.name)?;
	Ok(())
}

fn translate_reset(reset: &ResetStmt, out: &mut String) -> Result<(), TranslationError> {
	write!(out, "-- RESET {};", reset.name)?;
	Ok(())
}

fn translate_explain(
	explain: &ExplainStmt,
	ctx: &TranslationContext,
	out: &mut String,
) -> Result<(), TranslationError> {
	out.push_str("EXPLAIN ");
	translate_statement(&explain.statement, ctx, out)?;
	Ok(())
}

fn translate_create_index(
	idx: &CreateIndexStmt,
	_ctx: &TranslationContext,
	out: &mut String,
) -> Result<(), TranslationError> {
	let tbl_name = match idx.table.parts.last() {
		Some(p) => p.as_str(),
		None => {
			return Err(TranslationError::Invalid("Empty table name".to_string()));
		}
	};
	let idx_name = match &idx.name {
		Some(n) => n.clone(),
		None => format!("{tbl_name}_idx"),
	};

	write!(out, "DEFINE INDEX {idx_name} ON TABLE {tbl_name} COLUMNS ")?;
	let mut first = true;
	for col in &idx.columns {
		if !first {
			out.push_str(", ");
		}
		first = false;
		if let Expr::ColumnRef(cr) = &col.expr
			&& let Some(name) = cr.fields.last()
		{
			out.push_str(name);
		}
	}
	if idx.unique {
		out.push_str(" UNIQUE");
	}
	out.push(';');
	Ok(())
}

fn translate_create_view(
	view: &CreateViewStmt,
	ctx: &TranslationContext,
	out: &mut String,
) -> Result<(), TranslationError> {
	let view_name = match view.name.parts.last() {
		Some(p) => p.as_str(),
		None => {
			return Err(TranslationError::Invalid("Empty view name".to_string()));
		}
	};

	write!(out, "DEFINE TABLE {view_name} AS ")?;
	let mut query_sql = String::new();
	translate_select(&view.query, ctx, &mut query_sql)?;
	out.push_str(query_sql.trim_end_matches(';'));
	out.push(';');
	Ok(())
}

fn translate_alter_table(
	alter: &AlterTableStmt,
	_ctx: &TranslationContext,
	out: &mut String,
) -> Result<(), TranslationError> {
	let tbl_name = match alter.name.parts.last() {
		Some(p) => p.as_str(),
		None => {
			return Err(TranslationError::Invalid("Empty table name".to_string()));
		}
	};

	for action in &alter.actions {
		match action {
			AlterTableAction::AddColumn {
				column,
				..
			} => {
				writeln!(out, "DEFINE FIELD {} ON TABLE {tbl_name};", column.name)?;
			}
			AlterTableAction::DropColumn {
				name,
				..
			} => {
				writeln!(out, "REMOVE FIELD {name} ON TABLE {tbl_name};")?;
			}
			AlterTableAction::AlterColumnType {
				name,
				data_type,
			} => {
				let type_str = match data_type.name.parts.last() {
					Some(t) => t.as_str(),
					None => "any",
				};
				writeln!(out, "DEFINE FIELD {name} ON TABLE {tbl_name} TYPE {type_str};")?;
			}
			_ => {
				writeln!(out, "-- ALTER TABLE {tbl_name} action")?;
			}
		}
	}
	Ok(())
}

fn translate_create_property_graph(
	stmt: &crate::ast::graph::CreatePropertyGraphStmt,
	out: &mut String,
) -> Result<(), TranslationError> {
	for vt in &stmt.vertex_tables {
		let name = vt.table_name.parts.last().map(|s| s.as_str()).unwrap_or("vertex");
		writeln!(out, "DEFINE TABLE {name} TYPE NORMAL;")?;
	}
	for et in &stmt.edge_tables {
		let name = et.table_name.parts.last().map(|s| s.as_str()).unwrap_or("edge");
		let src = et.source_table.parts.last().map(|s| s.as_str()).unwrap_or("any");
		let tgt = et.target_table.parts.last().map(|s| s.as_str()).unwrap_or("any");
		writeln!(out, "DEFINE TABLE {name} TYPE RELATION IN {src} OUT {tgt};")?;
	}
	Ok(())
}

fn translate_drop_property_graph(
	stmt: &crate::ast::graph::DropPropertyGraphStmt,
	out: &mut String,
) -> Result<(), TranslationError> {
	let name = stmt.name.parts.last().map(|s| s.as_str()).unwrap_or("graph");
	writeln!(out, "-- DROP PROPERTY GRAPH {name};")?;
	Ok(())
}

fn translate_graph_pattern(
	pattern: &crate::ast::graph::GraphPattern,
	ctx: &TranslationContext,
	out: &mut String,
) -> Result<(), TranslationError> {
	for (i, path) in pattern.paths.iter().enumerate() {
		if i > 0 {
			out.push_str(", ");
		}
		for elem in &path.elements {
			match elem {
				crate::ast::graph::GraphElementPattern::Vertex(v) => {
					if let Some(ref var) = v.variable {
						out.push_str(var);
					} else if let Some(first_label) = v.labels.first() {
						out.push_str(first_label);
					} else {
						out.push('*');
					}
				}
				crate::ast::graph::GraphElementPattern::Edge(e) => {
					let label = e.labels.first().map(|s| s.as_str()).unwrap_or("");
					match e.direction {
						crate::ast::graph::GraphEdgeDirection::Right => {
							if label.is_empty() {
								out.push_str("->");
							} else {
								write!(out, "->{label}->")?;
							}
						}
						crate::ast::graph::GraphEdgeDirection::Left => {
							if label.is_empty() {
								out.push_str("<-");
							} else {
								write!(out, "<-{label}<-")?;
							}
						}
						crate::ast::graph::GraphEdgeDirection::Any => {
							if label.is_empty() {
								out.push_str("<->");
							} else {
								write!(out, "<->{label}<->")?;
							}
						}
						crate::ast::graph::GraphEdgeDirection::Bidirectional => {
							if label.is_empty() {
								out.push_str("<->");
							} else {
								write!(out, "<->{label}<->")?;
							}
						}
					}
				}
			}
		}
	}
	if let Some(ref w) = pattern.where_clause {
		out.push_str(" WHERE ");
		translate_expr(w, ctx, out)?;
	}
	Ok(())
}
