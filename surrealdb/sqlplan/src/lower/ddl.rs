//! Typed DDL lowering from PostgreSQL and MySQL AST to SurrealDB Expr.
//!
//! Converts SQL DDL (`CREATE TABLE`, `CREATE INDEX`, `CREATE VIEW`, `ALTER TABLE`, `DROP`)
//! directly into SurrealDB `Expr::Define`, `Expr::Remove`, etc., without text serialization.

use surrealdb_expr::expr::field::Selector;
use surrealdb_expr::expr::index_kind::Index;
use surrealdb_expr::expr::operator::BinaryOperator;
use surrealdb_expr::expr::reference::{Reference, ReferenceDeleteStrategy};
use surrealdb_expr::expr::statements::define::{
	DefineDefault, DefineFieldStatement, DefineIndexStatement, DefineKind, DefineStatement,
	DefineTableStatement,
};
use surrealdb_expr::expr::statements::remove::{
	RemoveFieldStatement, RemoveIndexStatement, RemoveStatement, RemoveTableStatement,
};
use surrealdb_expr::expr::table_type::TableType;
use surrealdb_expr::expr::{Cond, Expr, Field, Fields, Idiom, Kind, Literal, Param, Part, View};
use surrealdb_mysql::ast::stmt::{
	CreateTableStmt as MyCreateTableStmt, DropTableStmt as MyDropTableStmt,
	Statement as MyStatement, TableConstraint as MyTableConstraint,
};
use surrealdb_pgsql::ast::stmt::{
	AlterTableAction, AlterTableStmt, ColumnConstraint, CreateIndexStmt,
	CreateTableStmt as PgCreateTableStmt, CreateViewStmt, DropObjectType, DropStmt as PgDropStmt,
	Statement as PgStatement, TableConstraint as PgTableConstraint, TableRef as PgTableRef,
};
use surrealdb_strand::{Strand, TableName};

use super::write::{lower_mysql_expr, lower_pgsql_expr};
use crate::error::PlanError;

/// Map PostgreSQL TypeName to SurrealDB Kind.
pub fn map_pgsql_type(type_name: &surrealdb_pgsql::ast::expr::TypeName) -> Kind {
	let raw_name = match type_name.name.parts.last() {
		Some(s) => s.to_ascii_lowercase(),
		None => return Kind::Any,
	};

	let is_array = !type_name.array_bounds.is_empty() || raw_name.ends_with("[]");
	let base_name = raw_name.trim_end_matches("[]");

	let inner_kind = match base_name {
		"int" | "integer" | "int4" | "int2" | "smallint" | "int8" | "bigint" | "serial"
		| "bigserial" | "smallserial" => Kind::Int,
		"float" | "float4" | "real" | "float8" | "double precision" | "double" => Kind::Float,
		"numeric" | "decimal" | "dec" => Kind::Decimal,
		"text" | "varchar" | "char" | "character" | "character varying" | "bpchar" | "string" => {
			Kind::String
		}
		"bool" | "boolean" => Kind::Bool,
		"date"
		| "time"
		| "timestamp"
		| "timestamptz"
		| "timestamp with time zone"
		| "timestamp without time zone" => Kind::Datetime,
		"uuid" => Kind::Uuid,
		"json" | "jsonb" => Kind::Object,
		"bytea" => Kind::Bytes,
		_ => Kind::Any,
	};

	if is_array {
		Kind::Array(Box::new(inner_kind), None)
	} else {
		inner_kind
	}
}

/// Map MySQL data_type string to SurrealDB Kind.
pub fn map_mysql_type(type_str: &str) -> Kind {
	let lower = type_str.to_ascii_lowercase();
	let without_paren = lower.split('(').next().unwrap_or(&lower).trim();
	let first_word = without_paren.split_whitespace().next().unwrap_or("").trim();

	if without_paren == "double precision" {
		return Kind::Float;
	}

	match first_word {
		"int" | "integer" | "tinyint" | "smallint" | "mediumint" | "bigint" => Kind::Int,
		"float" | "double" | "real" => Kind::Float,
		"decimal" | "dec" | "numeric" | "fixed" => Kind::Decimal,
		"char" | "varchar" | "tinytext" | "text" | "mediumtext" | "longtext" | "enum" | "set" => {
			Kind::String
		}
		"bool" | "boolean" => Kind::Bool,
		"date" | "time" | "datetime" | "timestamp" | "year" => Kind::Datetime,
		"json" => Kind::Object,
		"binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob" | "bit" => {
			Kind::Bytes
		}
		_ => Kind::Any,
	}
}

fn not_null_assertion() -> Expr {
	Expr::Binary {
		left: Box::new(Expr::Param(Param::from("value".to_string()))),
		op: BinaryOperator::NotEqual,
		right: Box::new(Expr::Literal(Literal::None)),
	}
}

/// Lower a PostgreSQL CREATE TABLE statement into SurrealDB expressions.
pub fn lower_pgsql_create_table(create: &PgCreateTableStmt) -> Result<Vec<Expr>, PlanError> {
	let (_, tbl_name_str) = crate::namespace::resolve_pgsql_table_name(&create.name.parts, None)?;
	let tbl_name = tbl_name_str.as_str();
	let safe_idx_prefix = tbl_name.replace('.', "_");

	let mut exprs = Vec::new();
	let mut index_exprs = Vec::new();

	let table_def = DefineTableStatement {
		name: Expr::Table(TableName::from(tbl_name)),
		kind: if create.if_not_exists {
			DefineKind::IfNotExists
		} else {
			DefineKind::Default
		},
		full: true,
		table_type: TableType::Normal,
		..Default::default()
	};
	exprs.push(Expr::Define(Box::new(DefineStatement::Table(table_def))));

	let mut index_counter = 0;

	for col in &create.columns {
		let mut field_def = DefineFieldStatement {
			name: Expr::Idiom(Idiom::field(col.name.as_str())),
			what: Expr::Table(TableName::from(tbl_name)),
			field_kind: Some(map_pgsql_type(&col.data_type)),
			kind: if create.if_not_exists {
				DefineKind::IfNotExists
			} else {
				DefineKind::Default
			},
			..Default::default()
		};

		for con in &col.constraints {
			match con {
				ColumnConstraint::NotNull => {
					field_def.assert = Some(not_null_assertion());
				}
				ColumnConstraint::Default(def) => {
					let lowered = lower_pgsql_expr(def)?;
					field_def.default = DefineDefault::Set(lowered);
				}
				ColumnConstraint::References {
					..
				} => {
					field_def.reference = Some(Reference {
						on_delete: ReferenceDeleteStrategy::Reject,
					});
				}
				ColumnConstraint::PrimaryKey => {
					index_counter += 1;
					let idx = DefineIndexStatement {
						name: Expr::Idiom(Idiom::field(format!(
							"{safe_idx_prefix}_{}_pk",
							col.name
						))),
						what: Expr::Table(TableName::from(tbl_name)),
						cols: vec![Expr::Idiom(Idiom(vec![Part::Field(Strand::from(
							col.name.as_str(),
						))]))],
						index: Index::Uniq,
						kind: if create.if_not_exists {
							DefineKind::IfNotExists
						} else {
							DefineKind::Default
						},
						..Default::default()
					};
					index_exprs.push(Expr::Define(Box::new(DefineStatement::Index(idx))));
				}
				ColumnConstraint::Unique => {
					index_counter += 1;
					let idx = DefineIndexStatement {
						name: Expr::Idiom(Idiom::field(format!(
							"{safe_idx_prefix}_{}_uniq",
							col.name
						))),
						what: Expr::Table(TableName::from(tbl_name)),
						cols: vec![Expr::Idiom(Idiom(vec![Part::Field(Strand::from(
							col.name.as_str(),
						))]))],
						index: Index::Uniq,
						kind: if create.if_not_exists {
							DefineKind::IfNotExists
						} else {
							DefineKind::Default
						},
						..Default::default()
					};
					index_exprs.push(Expr::Define(Box::new(DefineStatement::Index(idx))));
				}
				ColumnConstraint::Null => {}
			}
		}

		exprs.push(Expr::Define(Box::new(DefineStatement::Field(Box::new(field_def)))));
	}

	for con in &create.constraints {
		match con {
			PgTableConstraint::PrimaryKey(cols) => {
				index_counter += 1;
				let idx_name = format!("{safe_idx_prefix}_{}_pk", cols.join("_"));
				let idx = DefineIndexStatement {
					name: Expr::Idiom(Idiom::field(idx_name)),
					what: Expr::Table(TableName::from(tbl_name)),
					cols: cols
						.iter()
						.map(|c| Expr::Idiom(Idiom(vec![Part::Field(Strand::from(c.as_str()))])))
						.collect(),
					index: Index::Uniq,
					kind: if create.if_not_exists {
						DefineKind::IfNotExists
					} else {
						DefineKind::Default
					},
					..Default::default()
				};
				index_exprs.push(Expr::Define(Box::new(DefineStatement::Index(idx))));
			}
			PgTableConstraint::Unique(cols) => {
				index_counter += 1;
				let idx_name = format!("{safe_idx_prefix}_{}_uniq", cols.join("_"));
				let idx = DefineIndexStatement {
					name: Expr::Idiom(Idiom::field(idx_name)),
					what: Expr::Table(TableName::from(tbl_name)),
					cols: cols
						.iter()
						.map(|c| Expr::Idiom(Idiom(vec![Part::Field(Strand::from(c.as_str()))])))
						.collect(),
					index: Index::Uniq,
					kind: if create.if_not_exists {
						DefineKind::IfNotExists
					} else {
						DefineKind::Default
					},
					..Default::default()
				};
				index_exprs.push(Expr::Define(Box::new(DefineStatement::Index(idx))));
			}
			PgTableConstraint::ForeignKey {
				columns,
				..
			} => {
				for col in columns {
					let field_def = DefineFieldStatement {
						name: Expr::Idiom(Idiom::field(col.as_str())),
						what: Expr::Table(TableName::from(tbl_name)),
						reference: Some(Reference {
							on_delete: ReferenceDeleteStrategy::Reject,
						}),
						kind: DefineKind::Overwrite,
						..Default::default()
					};
					exprs.push(Expr::Define(Box::new(DefineStatement::Field(Box::new(field_def)))));
				}
			}
		}
	}

	exprs.extend(index_exprs);
	let _ = index_counter;
	Ok(exprs)
}

/// Lower a PostgreSQL CREATE INDEX statement into SurrealDB expressions.
pub fn lower_pgsql_create_index(idx: &CreateIndexStmt) -> Result<Vec<Expr>, PlanError> {
	let (_, tbl_str) = crate::namespace::resolve_pgsql_table_name(&idx.table.parts, None)?;
	let tbl = tbl_str.as_str();

	let mut cols = Vec::new();
	for col in &idx.columns {
		if let surrealdb_pgsql::ast::expr::Expr::ColumnRef(cr) = &col.expr
			&& let Some(name) = cr.fields.last()
		{
			cols.push(Expr::Idiom(Idiom(vec![Part::Field(Strand::from(name.as_str()))])));
		}
	}

	let idx_name = match &idx.name {
		Some(n) => n.clone(),
		None => {
			let col_names: Vec<String> = idx
				.columns
				.iter()
				.filter_map(|c| match &c.expr {
					surrealdb_pgsql::ast::expr::Expr::ColumnRef(cr) => cr.fields.last().cloned(),
					_ => None,
				})
				.collect();
			let safe_tbl = tbl.replace('.', "_");
			format!("{safe_tbl}_{}_idx", col_names.join("_"))
		}
	};

	let index_stmt = DefineIndexStatement {
		name: Expr::Idiom(Idiom::field(idx_name)),
		what: Expr::Table(TableName::from(tbl)),
		cols,
		index: if idx.unique {
			Index::Uniq
		} else {
			Index::Idx
		},
		kind: if idx.if_not_exists {
			DefineKind::IfNotExists
		} else {
			DefineKind::Default
		},
		concurrently: idx.concurrently,
		..Default::default()
	};

	Ok(vec![Expr::Define(Box::new(DefineStatement::Index(index_stmt)))])
}

/// Lower a PostgreSQL CREATE VIEW statement into SurrealDB expressions.
pub fn lower_pgsql_create_view(view: &CreateViewStmt) -> Result<Vec<Expr>, PlanError> {
	let (_, view_name_str) = crate::namespace::resolve_pgsql_table_name(&view.name.parts, None)?;
	let view_name = view_name_str.as_str();

	let mut from_tables = Vec::new();
	for tr in &view.query.from {
		if let PgTableRef::Table {
			name,
			..
		} = tr && let Ok((_, t)) = crate::namespace::resolve_pgsql_table_name(&name.parts, None)
		{
			from_tables.push(TableName::from(t));
		}
	}

	let fields = if view.query.targets.is_empty()
		|| (view.query.targets.len() == 1
			&& matches!(&view.query.targets[0].expr, surrealdb_pgsql::ast::expr::Expr::ColumnRef(cr) if cr.fields.len() == 1 && cr.fields[0] == "*"))
	{
		Fields::all()
	} else {
		let mut selectors = Vec::new();
		for target in &view.query.targets {
			let expr = lower_pgsql_expr(&target.expr)?;
			let alias = target.alias.as_ref().map(|a| Idiom::field(a.as_str()));
			selectors.push(Field::Single(Selector {
				expr,
				alias,
			}));
		}
		Fields::Select(selectors)
	};

	let cond = if let Some(wh) = &view.query.where_clause {
		Some(Cond(lower_pgsql_expr(wh)?))
	} else {
		None
	};

	let table_def = DefineTableStatement {
		name: Expr::Table(TableName::from(view_name)),
		kind: if view.or_replace {
			DefineKind::Overwrite
		} else {
			DefineKind::Default
		},
		view: Some(View {
			expr: fields,
			what: from_tables,
			cond,
			group: None,
		}),
		..Default::default()
	};

	Ok(vec![Expr::Define(Box::new(DefineStatement::Table(table_def)))])
}

/// Lower a PostgreSQL ALTER TABLE statement into SurrealDB expressions.
pub fn lower_pgsql_alter_table(alter: &AlterTableStmt) -> Result<Vec<Expr>, PlanError> {
	let (_, tbl_name_str) = crate::namespace::resolve_pgsql_table_name(&alter.name.parts, None)?;
	let tbl_name = tbl_name_str.as_str();

	let mut exprs = Vec::new();

	for action in &alter.actions {
		match action {
			AlterTableAction::AddColumn {
				if_not_exists,
				column,
			} => {
				let mut field_def = DefineFieldStatement {
					name: Expr::Idiom(Idiom::field(column.name.as_str())),
					what: Expr::Table(TableName::from(tbl_name)),
					field_kind: Some(map_pgsql_type(&column.data_type)),
					kind: if *if_not_exists {
						DefineKind::IfNotExists
					} else {
						DefineKind::Default
					},
					..Default::default()
				};
				for con in &column.constraints {
					match con {
						ColumnConstraint::NotNull => {
							field_def.assert = Some(not_null_assertion());
						}
						ColumnConstraint::Default(def) => {
							field_def.default = DefineDefault::Set(lower_pgsql_expr(def)?);
						}
						_ => {}
					}
				}
				exprs.push(Expr::Define(Box::new(DefineStatement::Field(Box::new(field_def)))));
			}
			AlterTableAction::DropColumn {
				if_exists,
				name,
				..
			} => {
				let rem = RemoveFieldStatement {
					name: Expr::Idiom(Idiom::field(name.as_str())),
					table_name: Expr::Table(TableName::from(tbl_name)),
					if_exists: *if_exists,
				};
				exprs.push(Expr::Remove(Box::new(RemoveStatement::Field(rem))));
			}
			AlterTableAction::AlterColumnType {
				name,
				data_type,
			} => {
				let field_def = DefineFieldStatement {
					name: Expr::Idiom(Idiom::field(name.as_str())),
					what: Expr::Table(TableName::from(tbl_name)),
					field_kind: Some(map_pgsql_type(data_type)),
					kind: DefineKind::Overwrite,
					..Default::default()
				};
				exprs.push(Expr::Define(Box::new(DefineStatement::Field(Box::new(field_def)))));
			}
			AlterTableAction::SetDefault {
				column,
				default,
			} => {
				let field_def = DefineFieldStatement {
					name: Expr::Idiom(Idiom::field(column.as_str())),
					what: Expr::Table(TableName::from(tbl_name)),
					default: DefineDefault::Set(lower_pgsql_expr(default)?),
					kind: DefineKind::Overwrite,
					..Default::default()
				};
				exprs.push(Expr::Define(Box::new(DefineStatement::Field(Box::new(field_def)))));
			}
			AlterTableAction::DropDefault {
				column,
			} => {
				let field_def = DefineFieldStatement {
					name: Expr::Idiom(Idiom::field(column.as_str())),
					what: Expr::Table(TableName::from(tbl_name)),
					default: DefineDefault::None,
					kind: DefineKind::Overwrite,
					..Default::default()
				};
				exprs.push(Expr::Define(Box::new(DefineStatement::Field(Box::new(field_def)))));
			}
			AlterTableAction::SetNotNull {
				column,
			} => {
				let field_def = DefineFieldStatement {
					name: Expr::Idiom(Idiom::field(column.as_str())),
					what: Expr::Table(TableName::from(tbl_name)),
					assert: Some(not_null_assertion()),
					kind: DefineKind::Overwrite,
					..Default::default()
				};
				exprs.push(Expr::Define(Box::new(DefineStatement::Field(Box::new(field_def)))));
			}
			AlterTableAction::DropNotNull {
				column,
			} => {
				let field_def = DefineFieldStatement {
					name: Expr::Idiom(Idiom::field(column.as_str())),
					what: Expr::Table(TableName::from(tbl_name)),
					assert: None,
					kind: DefineKind::Overwrite,
					..Default::default()
				};
				exprs.push(Expr::Define(Box::new(DefineStatement::Field(Box::new(field_def)))));
			}
			AlterTableAction::AddConstraint(PgTableConstraint::PrimaryKey(cols)) => {
				let idx = DefineIndexStatement {
					name: Expr::Idiom(Idiom::field(format!("{tbl_name}_{}_pk", cols.join("_")))),
					what: Expr::Table(TableName::from(tbl_name)),
					cols: cols
						.iter()
						.map(|c| Expr::Idiom(Idiom(vec![Part::Field(Strand::from(c.as_str()))])))
						.collect(),
					index: Index::Uniq,
					..Default::default()
				};
				exprs.push(Expr::Define(Box::new(DefineStatement::Index(idx))));
			}
			AlterTableAction::AddConstraint(PgTableConstraint::Unique(cols)) => {
				let idx = DefineIndexStatement {
					name: Expr::Idiom(Idiom::field(format!("{tbl_name}_{}_uniq", cols.join("_")))),
					what: Expr::Table(TableName::from(tbl_name)),
					cols: cols
						.iter()
						.map(|c| Expr::Idiom(Idiom(vec![Part::Field(Strand::from(c.as_str()))])))
						.collect(),
					index: Index::Uniq,
					..Default::default()
				};
				exprs.push(Expr::Define(Box::new(DefineStatement::Index(idx))));
			}
			AlterTableAction::DropConstraint {
				if_exists,
				name,
				..
			} => {
				let rem = RemoveIndexStatement {
					name: Expr::Idiom(Idiom::field(name.as_str())),
					what: Expr::Table(TableName::from(tbl_name)),
					if_exists: *if_exists,
				};
				exprs.push(Expr::Remove(Box::new(RemoveStatement::Index(rem))));
			}
			_ => {}
		}
	}

	Ok(exprs)
}

/// Lower a PostgreSQL DROP statement into SurrealDB expressions.
pub fn lower_pgsql_drop(drop: &PgDropStmt) -> Result<Vec<Expr>, PlanError> {
	let mut exprs = Vec::new();

	for name in &drop.names {
		let (_, obj_name_str) = crate::namespace::resolve_pgsql_table_name(&name.parts, None)?;
		let obj_name = obj_name_str.as_str();

		match drop.object_type {
			DropObjectType::Table | DropObjectType::View => {
				let rem = RemoveTableStatement {
					name: Expr::Table(TableName::from(obj_name)),
					if_exists: drop.if_exists,
					expunge: false,
				};
				exprs.push(Expr::Remove(Box::new(RemoveStatement::Table(rem))));
			}
			DropObjectType::Index => {
				let rem = RemoveIndexStatement {
					name: Expr::Idiom(Idiom::field(obj_name)),
					what: Expr::Literal(Literal::None),
					if_exists: drop.if_exists,
				};
				exprs.push(Expr::Remove(Box::new(RemoveStatement::Index(rem))));
			}
			_ => {
				return Err(PlanError::unsupported(format!(
					"DROP {:?} is not supported for typed lowering",
					drop.object_type
				)));
			}
		}
	}

	Ok(exprs)
}

/// Lower a PostgreSQL DDL statement.
pub fn lower_pgsql_ddl(stmt: &PgStatement) -> Result<Vec<Expr>, PlanError> {
	match stmt {
		PgStatement::CreateTable(s) => lower_pgsql_create_table(s),
		PgStatement::CreateIndex(s) => lower_pgsql_create_index(s),
		PgStatement::CreateView(s) => lower_pgsql_create_view(s),
		PgStatement::AlterTable(s) => lower_pgsql_alter_table(s),
		PgStatement::Drop(s) => lower_pgsql_drop(s),
		_ => Err(PlanError::unsupported(format!(
			"PostgreSQL statement not recognized as DDL: {stmt:?}"
		))),
	}
}

/// Lower a MySQL CREATE TABLE statement into SurrealDB expressions.
pub fn lower_mysql_create_table(create: &MyCreateTableStmt) -> Result<Vec<Expr>, PlanError> {
	let (_, tbl_name_str) = crate::namespace::resolve_mysql_table_name(&create.name.parts, None)?;
	let tbl_name = tbl_name_str.as_str();

	let mut exprs = Vec::new();
	let mut index_exprs = Vec::new();

	let table_def = DefineTableStatement {
		name: Expr::Table(TableName::from(tbl_name)),
		kind: if create.if_not_exists {
			DefineKind::IfNotExists
		} else {
			DefineKind::Default
		},
		full: true,
		table_type: TableType::Normal,
		..Default::default()
	};
	exprs.push(Expr::Define(Box::new(DefineStatement::Table(table_def))));

	for col in &create.columns {
		let mut field_def = DefineFieldStatement {
			name: Expr::Idiom(Idiom::field(col.name.as_str())),
			what: Expr::Table(TableName::from(tbl_name)),
			field_kind: Some(map_mysql_type(&col.data_type)),
			kind: if create.if_not_exists {
				DefineKind::IfNotExists
			} else {
				DefineKind::Default
			},
			..Default::default()
		};

		if col.not_null {
			field_def.assert = Some(not_null_assertion());
		}

		if let Some(def) = &col.default {
			field_def.default = DefineDefault::Set(lower_mysql_expr(def)?);
		}

		if col.primary_key {
			let idx = DefineIndexStatement {
				name: Expr::Idiom(Idiom::field(format!("{tbl_name}_{}_pk", col.name))),
				what: Expr::Table(TableName::from(tbl_name)),
				cols: vec![Expr::Idiom(Idiom(vec![Part::Field(Strand::from(col.name.as_str()))]))],
				index: Index::Uniq,
				kind: if create.if_not_exists {
					DefineKind::IfNotExists
				} else {
					DefineKind::Default
				},
				..Default::default()
			};
			index_exprs.push(Expr::Define(Box::new(DefineStatement::Index(idx))));
		}

		if col.unique {
			let idx = DefineIndexStatement {
				name: Expr::Idiom(Idiom::field(format!("{tbl_name}_{}_uniq", col.name))),
				what: Expr::Table(TableName::from(tbl_name)),
				cols: vec![Expr::Idiom(Idiom(vec![Part::Field(Strand::from(col.name.as_str()))]))],
				index: Index::Uniq,
				kind: if create.if_not_exists {
					DefineKind::IfNotExists
				} else {
					DefineKind::Default
				},
				..Default::default()
			};
			index_exprs.push(Expr::Define(Box::new(DefineStatement::Index(idx))));
		}

		exprs.push(Expr::Define(Box::new(DefineStatement::Field(Box::new(field_def)))));
	}

	for con in &create.constraints {
		match con {
			MyTableConstraint::PrimaryKey {
				columns,
				..
			} => {
				let idx_name = format!("{tbl_name}_{}_pk", columns.join("_"));
				let idx = DefineIndexStatement {
					name: Expr::Idiom(Idiom::field(idx_name)),
					what: Expr::Table(TableName::from(tbl_name)),
					cols: columns
						.iter()
						.map(|c| Expr::Idiom(Idiom(vec![Part::Field(Strand::from(c.as_str()))])))
						.collect(),
					index: Index::Uniq,
					kind: if create.if_not_exists {
						DefineKind::IfNotExists
					} else {
						DefineKind::Default
					},
					..Default::default()
				};
				index_exprs.push(Expr::Define(Box::new(DefineStatement::Index(idx))));
			}
			MyTableConstraint::Unique {
				name,
				columns,
				..
			} => {
				let idx_name = match name {
					Some(n) => n.clone(),
					None => format!("{tbl_name}_{}_uniq", columns.join("_")),
				};
				let idx = DefineIndexStatement {
					name: Expr::Idiom(Idiom::field(idx_name)),
					what: Expr::Table(TableName::from(tbl_name)),
					cols: columns
						.iter()
						.map(|c| Expr::Idiom(Idiom(vec![Part::Field(Strand::from(c.as_str()))])))
						.collect(),
					index: Index::Uniq,
					kind: if create.if_not_exists {
						DefineKind::IfNotExists
					} else {
						DefineKind::Default
					},
					..Default::default()
				};
				index_exprs.push(Expr::Define(Box::new(DefineStatement::Index(idx))));
			}
			MyTableConstraint::ForeignKey {
				columns,
				..
			} => {
				for col in columns {
					let field_def = DefineFieldStatement {
						name: Expr::Idiom(Idiom::field(col.as_str())),
						what: Expr::Table(TableName::from(tbl_name)),
						reference: Some(Reference {
							on_delete: ReferenceDeleteStrategy::Reject,
						}),
						kind: DefineKind::Overwrite,
						..Default::default()
					};
					exprs.push(Expr::Define(Box::new(DefineStatement::Field(Box::new(field_def)))));
				}
			}
		}
	}

	exprs.extend(index_exprs);

	Ok(exprs)
}

/// Lower a MySQL DROP TABLE statement into SurrealDB expressions.
pub fn lower_mysql_drop_table(drop: &MyDropTableStmt) -> Result<Vec<Expr>, PlanError> {
	let mut exprs = Vec::new();

	for name in &drop.names {
		let (_, tbl_name_str) = crate::namespace::resolve_mysql_table_name(&name.parts, None)?;
		let tbl_name = tbl_name_str.as_str();

		let rem = RemoveTableStatement {
			name: Expr::Table(TableName::from(tbl_name)),
			if_exists: drop.if_exists,
			expunge: false,
		};
		exprs.push(Expr::Remove(Box::new(RemoveStatement::Table(rem))));
	}

	Ok(exprs)
}

/// Lower a MySQL `USE` statement into a SurrealDB `UseStatement`.
pub fn lower_mysql_use(
	use_stmt: &surrealdb_mysql::ast::stmt::UseStmt,
) -> Result<surrealdb_expr::expr::statements::UseStatement, PlanError> {
	if use_stmt.database.trim().is_empty() {
		return Err(PlanError::Custom("empty database name in USE".into()));
	}
	Ok(surrealdb_expr::expr::statements::UseStatement::Db(Expr::Idiom(Idiom::field(
		use_stmt.database.clone(),
	))))
}

/// Lower a MySQL DDL statement.
pub fn lower_mysql_ddl(stmt: &MyStatement) -> Result<Vec<Expr>, PlanError> {
	match stmt {
		MyStatement::CreateTable(s) => lower_mysql_create_table(s),
		MyStatement::DropTable(s) => lower_mysql_drop_table(s),
		_ => {
			Err(PlanError::unsupported(format!("MySQL statement not recognized as DDL: {stmt:?}")))
		}
	}
}
