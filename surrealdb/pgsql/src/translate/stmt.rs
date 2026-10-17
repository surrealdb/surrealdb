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
		Statement::Insert(_)
		| Statement::Update(_)
		| Statement::Delete(_)
		| Statement::Truncate(_) => {
			Err(TranslationError::Unsupported(
				"DML statements must be lowered via surrealdb_sqlplan typed write lowering".into(),
			))
		}
		Statement::CreateTable(_)
		| Statement::CreateIndex(_)
		| Statement::CreateView(_)
		| Statement::AlterTable(_)
		| Statement::Drop(_) => {
			Err(TranslationError::Unsupported(
				"DDL statements must be lowered via surrealdb_sqlplan typed DDL lowering".into(),
			))
		}
		Statement::Transaction(s) => translate_transaction(s, out),
		Statement::Set(s) => translate_set(s, out),
		Statement::Show(s) => translate_show(s, out),
		Statement::Reset(s) => translate_reset(s, out),
		Statement::Explain(s) => translate_explain(s, ctx, out),
		Statement::Empty => Ok(()),
		Statement::CreateSchema(_) => {
			// In SurrealDB, schemas are namespaces or databases.
			out.push_str("-- CREATE SCHEMA (handled at connection scope)");
			Ok(())
		}
		Statement::CreatePropertyGraph(s) => translate_create_property_graph(s, out),
		Statement::DropPropertyGraph(s) => translate_drop_property_graph(s, out),
		Statement::Prepare(_) | Statement::Execute(_) | Statement::Deallocate(_) => {
			Err(TranslationError::Unsupported(
				"SQL PREPARE/EXECUTE/DEALLOCATE statements are not supported; use extended query protocol".into(),
			))
		}
		Statement::Copy(_) => {
			Err(TranslationError::Unsupported(
				"COPY is a streaming wire protocol command and cannot be translated to SurrealQL directly".into(),
			))
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

fn translate_transaction(tx: &TransactionStmt, out: &mut String) -> Result<(), TranslationError> {
	match tx {
		TransactionStmt::Begin {
			..
		} => {
			out.push_str("BEGIN TRANSACTION;");
			Ok(())
		}
		TransactionStmt::Commit {
			..
		} => {
			out.push_str("COMMIT TRANSACTION;");
			Ok(())
		}
		TransactionStmt::Rollback {
			..
		} => {
			out.push_str("CANCEL TRANSACTION;");
			Ok(())
		}
		TransactionStmt::Savepoint {
			name,
			..
		} => {
			write!(out, "-- SAVEPOINT {name};")?;
			Ok(())
		}
		TransactionStmt::ReleaseSavepoint {
			name,
			..
		} => {
			write!(out, "-- RELEASE SAVEPOINT {name};")?;
			Ok(())
		}
		TransactionStmt::RollbackToSavepoint {
			name,
			..
		} => {
			write!(out, "-- ROLLBACK TO SAVEPOINT {name};")?;
			Ok(())
		}
	}
}

fn translate_set(set: &SetStmt, _out: &mut String) -> Result<(), TranslationError> {
	Err(TranslationError::Unsupported(format!("SET {} is not supported", set.name)))
}

fn translate_show(show: &ShowStmt, _out: &mut String) -> Result<(), TranslationError> {
	Err(TranslationError::Unsupported(format!("SHOW {} is not supported", show.name)))
}

fn translate_reset(reset: &ResetStmt, _out: &mut String) -> Result<(), TranslationError> {
	Err(TranslationError::Unsupported(format!("RESET {} is not supported", reset.name)))
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
