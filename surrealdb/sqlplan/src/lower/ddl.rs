//! Typed DDL lowering from PostgreSQL and MySQL AST to SurrealDB Expr.
//!
//! Converts SQL DDL (`CREATE TABLE`, `CREATE INDEX`, `CREATE VIEW`, `ALTER TABLE`, `DROP`)
//! directly into SurrealDB `Expr::Define`, `Expr::Remove`, etc., without text serialization.

use surrealdb_expr::expr::field::Selector;
use surrealdb_expr::expr::index_kind::Index;
use surrealdb_expr::expr::operator::BinaryOperator;
use surrealdb_expr::expr::reference::{Reference, ReferenceDeleteStrategy};
use surrealdb_expr::expr::statements::alter::{AlterDefault, AlterKind};
use surrealdb_expr::expr::statements::define::{
	DefineDatabaseStatement, DefineDefault, DefineFieldStatement, DefineIndexStatement, DefineKind,
	DefineSequenceStatement, DefineStatement, DefineTableStatement,
};
use surrealdb_expr::expr::statements::remove::{
	RemoveDatabaseStatement, RemoveFieldStatement, RemoveIndexStatement, RemoveStatement,
	RemoveTableStatement,
};
use surrealdb_expr::expr::table_type::TableType;
use surrealdb_expr::expr::{
	Cond, Expr, Field, Fields, Function, FunctionCall, Idiom, Kind, Literal, Param, Part, View,
};
use surrealdb_mysql::ast::stmt::{
	AlterTableAction as MyAlterTableAction, AlterTableStmt as MyAlterTableStmt,
	CreateDatabaseStmt as MyCreateDatabaseStmt, CreateIndexStmt as MyCreateIndexStmt,
	CreateTableStmt as MyCreateTableStmt, CreateViewStmt as MyCreateViewStmt,
	DropDatabaseStmt as MyDropDatabaseStmt, DropIndexStmt as MyDropIndexStmt,
	DropTableStmt as MyDropTableStmt, DropViewStmt as MyDropViewStmt, Statement as MyStatement,
	TableConstraint as MyTableConstraint, TableRef as MyTableRef,
};
use surrealdb_pgsql::ast::stmt::{
	AlterTableAction, AlterTableStmt, ColumnConstraint, CreateIndexStmt,
	CreateTableStmt as PgCreateTableStmt, CreateViewStmt, DropObjectType, DropStmt as PgDropStmt,
	Statement as PgStatement, TableConstraint as PgTableConstraint, TableRef as PgTableRef,
};
use surrealdb_strand::{Strand, TableName};

use super::foreign_key::{
	alter as alter_field, alter_column, check_create_targets, check_target_exists,
	column_foreign_key, drop_foreign_key, existence_assertion, link_kind, lower_add_foreign_key,
	reference, table_foreign_keys,
};
use super::write::{link_assignment, lower_mysql_expr, lower_pgsql_expr};
use crate::catalog::relations::{ConstraintTarget, constraint_target};
use crate::catalog::staged::StagedCatalog;
use crate::catalog::{BinderCatalog, EmptyCatalog, TableSchema, kind_admits_none};
use crate::error::PlanError;

/// Map PostgreSQL TypeName to SurrealDB Kind.
pub fn map_pgsql_type(type_name: &surrealdb_pgsql::ast::expr::TypeName) -> Kind {
	if super::vector_ddl::is_vector(type_name) {
		return super::vector_ddl::kind(type_name);
	}
	let raw_name = match type_name.name.parts.last() {
		Some(s) => s.to_ascii_lowercase(),
		None => return Kind::Any,
	};

	let is_array = !type_name.array_bounds.is_empty() || raw_name.ends_with("[]");
	let base_name = raw_name.trim_end_matches("[]");

	let inner_kind = match base_name {
		"int" | "integer" | "int4" | "int2" | "smallint" | "int8" | "bigint" | "serial"
		| "serial4" | "bigserial" | "serial8" | "smallserial" | "serial2" => Kind::Int,
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
		| "timestamp without time zone"
		| "time with time zone"
		| "time without time zone" => Kind::Datetime,
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

/// The type of a column that may hold NULL unless a constraint forbids it, so
/// a column added to a table with rows accepts the rows that have no value.
fn column_kind(data_type: &surrealdb_pgsql::ast::expr::TypeName, not_null: bool) -> Kind {
	let base_kind = map_pgsql_type(data_type);
	if not_null || matches!(base_kind, Kind::Any) {
		base_kind
	} else {
		Kind::either(vec![Kind::None, Kind::Null, base_kind])
	}
}

/// Whether a column of `kind` holds a JSON document (`json`, `jsonb`, an array of them, or one
/// that may be NULL). Its field is `FLEXIBLE`, so a document keeps the keys it is written with in
/// the SCHEMAFULL table `CREATE TABLE` defines.
fn holds_json(kind: &Kind) -> bool {
	match kind {
		Kind::Object => true,
		Kind::Array(elem, _) => holds_json(elem),
		Kind::Either(kinds) => kinds.iter().any(holds_json),
		Kind::Any
		| Kind::None
		| Kind::Null
		| Kind::Bool
		| Kind::Bytes
		| Kind::Datetime
		| Kind::Decimal
		| Kind::Duration
		| Kind::Float
		| Kind::Int
		| Kind::I8
		| Kind::U8
		| Kind::I16
		| Kind::U16
		| Kind::I32
		| Kind::U32
		| Kind::U64
		| Kind::F16
		| Kind::F32
		| Kind::Number
		| Kind::String
		| Kind::Uuid
		| Kind::Regex
		| Kind::Table(_)
		| Kind::Record(_)
		| Kind::Geometry(_)
		| Kind::Set(..)
		| Kind::Function(..)
		| Kind::Range
		| Kind::Literal(_)
		| Kind::File(_) => false,
	}
}

/// Whether a PostgreSQL column of `type_name` is a serial column, numbered by a sequence of its
/// own.
fn is_serial(type_name: &surrealdb_pgsql::ast::expr::TypeName) -> bool {
	type_name.array_bounds.is_empty()
		&& type_name.name.parts.last().is_some_and(|name| {
			matches!(
				name.to_ascii_lowercase().as_str(),
				"serial" | "serial4" | "bigserial" | "serial8" | "smallserial" | "serial2"
			)
		})
}

/// The sequence that numbers the serial or auto-increment `column` of `table`, named as
/// PostgreSQL names it (`<table>_<column>_seq`) and starting at 1, and the `DEFAULT` of the column
/// that draws its next value. The `id` column's `DEFAULT` is its record key when a write names no
/// id.
///
/// The sequence is defined only if it does not exist, so a `CREATE TABLE IF NOT EXISTS` of a table
/// that exists leaves its numbering alone. A sequence outlives its table, so a table created again
/// under a dropped one's name carries on from the last number the old one drew.
fn column_sequence(table: &str, column: &str) -> (Expr, DefineDefault) {
	let name = format!("{}_{column}_seq", table.replace('.', "_"));
	let define = DefineSequenceStatement {
		kind: DefineKind::IfNotExists,
		name: Expr::Idiom(Idiom::field(name.as_str())),
		batch: Expr::Literal(Literal::Integer(1000)),
		start: Expr::Literal(Literal::Integer(1)),
		..Default::default()
	};
	let next = Expr::FunctionCall(Box::new(FunctionCall {
		receiver: Function::Normal("sequence::nextval".to_string()),
		arguments: vec![Expr::Literal(Literal::String(Strand::from(name.as_str())))],
	}));
	(Expr::Define(Box::new(DefineStatement::Sequence(define))), DefineDefault::Set(next))
}

pub(crate) fn not_null_assertion() -> Expr {
	Expr::Binary {
		left: Box::new(Expr::Param(Param::from("value".to_string()))),
		op: BinaryOperator::NotEqual,
		right: Box::new(Expr::Literal(Literal::None)),
	}
}

/// Whether `expr` is the assertion `NOT NULL` lowers to.
pub(crate) fn is_not_null_assertion(expr: &Expr) -> bool {
	*expr == not_null_assertion()
}

/// Lower a PostgreSQL CREATE TABLE statement into SurrealDB expressions.
///
/// A column with a foreign key is a link to the referenced table; see [`super::foreign_key`].
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
	let foreign_keys = table_foreign_keys(create)?;

	for col in &create.columns {
		let is_id = col.name == "id";
		let is_table_pk = create.constraints.iter().any(|c| {
			if let PgTableConstraint::PrimaryKey(cols) = c {
				cols.iter().any(|c| c == &col.name)
			} else {
				false
			}
		});
		super::vector_ddl::check_column(&col.data_type)?;
		let serial = is_serial(&col.data_type);
		let is_not_null = is_id
			|| is_table_pk
			|| serial || col
			.constraints
			.iter()
			.any(|c| matches!(c, ColumnConstraint::NotNull | ColumnConstraint::PrimaryKey));
		let foreign_key = foreign_keys.get(col.name.as_str());
		if foreign_key.is_some() && (is_id || serial) {
			return Err(PlanError::UnsupportedStatement(format!(
				"a foreign key on the {} column \"{}\" of \"{tbl_name}\"",
				if is_id {
					"id"
				} else {
					"serial"
				},
				col.name
			)));
		}
		let field_kind = match foreign_key {
			Some((target, _)) => link_kind(target, !is_not_null),
			None => column_kind(&col.data_type, is_not_null),
		};

		let mut field_def = DefineFieldStatement {
			name: Expr::Idiom(Idiom::field(col.name.as_str())),
			what: Expr::Table(TableName::from(tbl_name)),
			flexible: holds_json(&field_kind),
			field_kind: Some(field_kind),
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
					if foreign_key.is_none() {
						field_def.assert = Some(not_null_assertion());
					}
				}
				ColumnConstraint::Default(def) => {
					let lowered = lower_pgsql_expr(def)?;
					field_def.default = DefineDefault::Set(match foreign_key {
						Some((target, _)) => link_assignment(target, lowered, !is_not_null),
						None => lowered,
					});
				}
				ColumnConstraint::References {
					..
				} => {}
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
						index: Index::Uniq {
							nulls_not_distinct: false,
						},
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
						index: Index::Uniq {
							nulls_not_distinct: false,
						},
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

		if let Some((target, on_delete)) = foreign_key {
			field_def.reference = Some(reference(*on_delete));
			field_def.assert = Some(existence_assertion(tbl_name, &col.name, target, !is_not_null));
		}

		if serial && matches!(field_def.default, DefineDefault::None) {
			let (sequence, default) = column_sequence(tbl_name, &col.name);
			exprs.push(sequence);
			field_def.default = default;
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
					index: Index::Uniq {
						nulls_not_distinct: false,
					},
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
					index: Index::Uniq {
						nulls_not_distinct: false,
					},
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
				..
			} => {}
		}
	}

	exprs.extend(index_exprs);
	let _ = index_counter;
	Ok(exprs)
}

/// Lower a PostgreSQL CREATE INDEX statement into SurrealDB expressions.
///
/// An `hnsw` index needs the dimensions of the column it covers, which this lowering does not
/// know: see [`lower_pgsql_create_index_over`].
pub fn lower_pgsql_create_index(idx: &CreateIndexStmt) -> Result<Vec<Expr>, PlanError> {
	lower_pgsql_create_index_over(idx, &|_, _| None)
}

/// A source of the dimensions of the vector column `column` of the table `table`, if it is one.
pub type VectorDimensions<'a> = &'a dyn Fn(&str, &str) -> Option<usize>;

/// Lower a PostgreSQL CREATE INDEX statement into SurrealDB expressions, where `dimensions` says
/// how many dimensions the vector column the index covers has.
pub fn lower_pgsql_create_index_over(
	idx: &CreateIndexStmt,
	dimensions: VectorDimensions<'_>,
) -> Result<Vec<Expr>, PlanError> {
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
				.filter_map(|c| {
					if let surrealdb_pgsql::ast::expr::Expr::ColumnRef(cr) = &c.expr {
						cr.fields.last().cloned()
					} else {
						None
					}
				})
				.collect();
			let safe_tbl = tbl.replace('.', "_");
			format!("{safe_tbl}_{}_idx", col_names.join("_"))
		}
	};

	let method = idx.method.as_deref().map(str::to_ascii_lowercase);
	let index = match method.as_deref() {
		Some("hnsw") => {
			let column = match idx.columns.first().map(|column| &column.expr) {
				Some(surrealdb_pgsql::ast::expr::Expr::ColumnRef(column)) => {
					column.fields.last().map(String::as_str)
				}
				_ => None,
			};
			super::vector_ddl::hnsw_index(idx, column.and_then(|column| dimensions(tbl, column)))?
		}
		Some("ivfflat") => {
			return Err(PlanError::UnsupportedStatement("access method \"ivfflat\"".into()));
		}
		Some(_) | None => {
			super::vector_ddl::refuse_vector_options(idx)?;
			if idx.unique {
				Index::Uniq {
					nulls_not_distinct: false,
				}
			} else {
				Index::Idx
			}
		}
	};

	let index_stmt = DefineIndexStatement {
		name: Expr::Idiom(Idiom::field(idx_name)),
		what: Expr::Table(TableName::from(tbl)),
		cols,
		index,
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

/// Lower a PostgreSQL ALTER TABLE statement into SurrealDB expressions, with no description of the
/// schema: see [`lower_pgsql_alter_table_with`].
pub fn lower_pgsql_alter_table(alter: &AlterTableStmt) -> Result<Vec<Expr>, PlanError> {
	lower_pgsql_alter_table_with(alter, &EmptyCatalog)
}

/// The table of `catalog` that an `ALTER TABLE` of the table `name` in `schema` names, when the
/// catalog lists it.
fn listed_table(
	catalog: &dyn BinderCatalog,
	schema: Option<&str>,
	name: &str,
) -> Option<TableSchema> {
	if schema.is_some_and(|schema| schema != "public") {
		return None;
	}
	catalog.constraint_tables().into_iter().find(|table| table.name == name)
}

/// Lower `DROP CONSTRAINT name` of table `table`.
///
/// The name is resolved against the constraints the catalog lists for the table
/// ([`constraint_target`]):
///
/// - a `UNIQUE` constraint removes its index;
/// - a foreign key removes the `REFERENCE` and the existence `ASSERT` of its field and keeps its
///   type, so the column still reads and writes as a key;
/// - the primary key removes the `<table>_<columns>_pk` index when there is one, and is refused
///   (`0A000`) when it is the record id, which cannot be dropped;
/// - a name the table does not have is `42704`, or nothing to do under `IF EXISTS`.
///
/// A table the catalog does not list (the session may not view definitions, or the table does not
/// exist) cannot be resolved against, and the name is taken as the name of an index.
fn lower_drop_constraint(
	catalog: &dyn BinderCatalog,
	schema: Option<&str>,
	table: &str,
	name: &str,
	if_exists: bool,
) -> Result<Vec<Expr>, PlanError> {
	let remove_index = |index: &str| {
		Expr::Remove(Box::new(RemoveStatement::Index(RemoveIndexStatement {
			name: Expr::Idiom(Idiom::field(index)),
			what: Expr::Table(TableName::from(table)),
			if_exists,
		})))
	};
	let Some(listed) = listed_table(catalog, schema, table) else {
		return Ok(vec![remove_index(name)]);
	};
	match constraint_target(&listed, name) {
		Some(ConstraintTarget::Index {
			name: index,
			..
		}) => Ok(vec![remove_index(&index)]),
		Some(ConstraintTarget::ForeignKey {
			column,
		}) => Ok(vec![drop_foreign_key(table, &column)]),
		Some(ConstraintTarget::RecordId) => Err(PlanError::UnsupportedStatement(format!(
			"DROP CONSTRAINT \"{name}\": the primary key of \"{table}\" is its record id, which cannot be dropped"
		))),
		None if if_exists => Ok(Vec::new()),
		None => Err(PlanError::UndefinedConstraint {
			constraint: name.to_owned(),
			table: table.to_owned(),
		}),
	}
}

/// Lower a PostgreSQL ALTER TABLE statement into SurrealDB expressions.
///
/// `catalog` describes the schema the statement runs against. `DROP CONSTRAINT` resolves its name
/// against the constraints the catalog lists, and `ADD FOREIGN KEY` needs to know that its column
/// is declared; see [`lower_drop_constraint`] and [`lower_add_foreign_key`].
pub fn lower_pgsql_alter_table_with(
	alter: &AlterTableStmt,
	catalog: &dyn BinderCatalog,
) -> Result<Vec<Expr>, PlanError> {
	let (schema, tbl_name_str) =
		crate::namespace::resolve_pgsql_table_name(&alter.name.parts, None)?;
	let tbl_name = tbl_name_str.as_str();

	let mut exprs = Vec::new();
	// An action reads the declarations the actions before it make, as the statement runs them in
	// order.
	let mut staged = StagedCatalog::new(catalog);

	for action in &alter.actions {
		let staged_from = exprs.len();
		match action {
			AlterTableAction::AddColumn {
				if_not_exists,
				column,
			} => {
				super::vector_ddl::check_column(&column.data_type)?;
				let not_null = column
					.constraints
					.iter()
					.any(|c| matches!(c, ColumnConstraint::NotNull | ColumnConstraint::PrimaryKey));
				let foreign_key = column_foreign_key(column)?;
				if let Some((target, _)) = &foreign_key {
					check_target_exists(&staged, target, tbl_name)?;
				}
				let mut field_def = DefineFieldStatement {
					name: Expr::Idiom(Idiom::field(column.name.as_str())),
					what: Expr::Table(TableName::from(tbl_name)),
					field_kind: Some(match &foreign_key {
						Some((target, _)) => link_kind(target, !not_null),
						None => column_kind(&column.data_type, not_null),
					}),
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
							if foreign_key.is_none() {
								field_def.assert = Some(not_null_assertion());
							}
						}
						ColumnConstraint::Default(def) => {
							let lowered = lower_pgsql_expr(def)?;
							field_def.default = DefineDefault::Set(match &foreign_key {
								Some((target, _)) => link_assignment(target, lowered, !not_null),
								None => lowered,
							});
						}
						ColumnConstraint::PrimaryKey
						| ColumnConstraint::Null
						| ColumnConstraint::Unique
						| ColumnConstraint::References {
							..
						} => {}
					}
				}
				if let Some((target, on_delete)) = &foreign_key {
					field_def.reference = Some(reference(*on_delete));
					field_def.assert =
						Some(existence_assertion(tbl_name, &column.name, target, !not_null));
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
				super::vector_ddl::check_column(data_type)?;
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
				let mut field = alter_column(tbl_name, column);
				let lowered = lower_pgsql_expr(default)?;
				field.default = AlterDefault::Set(
					match staged.link_table(schema.as_deref(), tbl_name, column) {
						// A default for a link column names the row it links to.
						Some(target) => link_assignment(
							&target,
							lowered,
							staged.link_admits_none(schema.as_deref(), tbl_name, column),
						),
						None => lowered,
					},
				);
				exprs.push(alter_field(field));
			}
			AlterTableAction::DropDefault {
				column,
			} => {
				let mut field = alter_column(tbl_name, column);
				field.default = AlterDefault::Drop;
				exprs.push(alter_field(field));
			}
			AlterTableAction::SetNotNull {
				column,
			} => {
				let mut field = alter_column(tbl_name, column);
				match staged.link_table(schema.as_deref(), tbl_name, column) {
					Some(target) => {
						field.kind = AlterKind::Set(link_kind(&target, false));
						field.assert =
							AlterKind::Set(existence_assertion(tbl_name, column, &target, false));
					}
					None => field.assert = AlterKind::Set(not_null_assertion()),
				}
				exprs.push(alter_field(field));
			}
			AlterTableAction::DropNotNull {
				column,
			} => {
				let mut field = alter_column(tbl_name, column);
				match staged.link_table(schema.as_deref(), tbl_name, column) {
					Some(target) => {
						field.kind = AlterKind::Set(link_kind(&target, true));
						field.assert =
							AlterKind::Set(existence_assertion(tbl_name, column, &target, true));
					}
					None => {
						field.assert = AlterKind::Drop;
						// A scalar column made `NOT NULL` by its type takes `NULL` and `NONE`
						// again.
						if let Some(shape) =
							staged.column_shape(schema.as_deref(), tbl_name, column)
							&& let Some(kind) = shape.kind
							&& !kind_admits_none(&kind)
						{
							field.kind =
								AlterKind::Set(Kind::either(vec![Kind::None, Kind::Null, kind]));
						}
					}
				}
				exprs.push(alter_field(field));
			}
			AlterTableAction::AddConstraint(PgTableConstraint::PrimaryKey(cols)) => {
				let idx = DefineIndexStatement {
					name: Expr::Idiom(Idiom::field(format!("{tbl_name}_{}_pk", cols.join("_")))),
					what: Expr::Table(TableName::from(tbl_name)),
					cols: cols
						.iter()
						.map(|c| Expr::Idiom(Idiom(vec![Part::Field(Strand::from(c.as_str()))])))
						.collect(),
					index: Index::Uniq {
						nulls_not_distinct: false,
					},
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
					index: Index::Uniq {
						nulls_not_distinct: false,
					},
					..Default::default()
				};
				exprs.push(Expr::Define(Box::new(DefineStatement::Index(idx))));
			}
			AlterTableAction::DropConstraint {
				if_exists,
				name,
				..
			} => {
				exprs.extend(lower_drop_constraint(
					&staged,
					schema.as_deref(),
					tbl_name,
					name,
					*if_exists,
				)?);
			}
			AlterTableAction::AddConstraint(
				key @ PgTableConstraint::ForeignKey {
					..
				},
			) => {
				exprs.extend(lower_add_foreign_key(&staged, schema.as_deref(), tbl_name, key)?);
			}
			AlterTableAction::RenameColumn {
				..
			}
			| AlterTableAction::RenameTable {
				..
			} => {}
		}
		staged.stage(&exprs[staged_from..]);
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
			DropObjectType::Schema | DropObjectType::Type => {
				return Err(PlanError::unsupported(format!(
					"DROP {:?} is not supported for typed lowering",
					drop.object_type
				)));
			}
		}
	}

	Ok(exprs)
}

/// Lower a PostgreSQL DDL statement against a catalog that describes no table.
///
/// Returns the definitions it runs, one expression each, in the order they run.
#[cfg(test)]
pub(crate) fn lower_pgsql_ddl_definitions(stmt: &PgStatement) -> Result<Vec<Expr>, PlanError> {
	lower_pgsql_ddl_definitions_with(stmt, &EmptyCatalog)
}

/// Lower a PostgreSQL DDL statement against `catalog`, which `ALTER TABLE` reads for the
/// constraints of the table (see [`lower_pgsql_alter_table_with`]) and which says how many
/// dimensions a vector column has.
///
/// Returns the definitions it runs, one expression each, in the order they run.
pub(crate) fn lower_pgsql_ddl_definitions_with(
	stmt: &PgStatement,
	catalog: &dyn BinderCatalog,
) -> Result<Vec<Expr>, PlanError> {
	lower_pgsql_ddl_definitions_over(stmt, catalog, &|table, column| {
		catalog.vector_dimension(None, table, column)
	})
}

/// Lower a PostgreSQL DDL statement as [`lower_pgsql_ddl_definitions_with`] does, where
/// `dimensions` says how many dimensions a vector column has.
fn lower_pgsql_ddl_definitions_over(
	stmt: &PgStatement,
	catalog: &dyn BinderCatalog,
	dimensions: VectorDimensions<'_>,
) -> Result<Vec<Expr>, PlanError> {
	match stmt {
		PgStatement::CreateTable(s) => {
			check_create_targets(s, catalog)?;
			lower_pgsql_create_table(s)
		}
		PgStatement::CreateIndex(s) => lower_pgsql_create_index_over(s, dimensions),
		PgStatement::CreateExtension(s) => super::vector_ddl::lower_create_extension(s),
		PgStatement::CreateView(s) => lower_pgsql_create_view(s),
		PgStatement::AlterTable(s) => lower_pgsql_alter_table_with(s, catalog),
		PgStatement::Drop(s) => lower_pgsql_drop(s),
		PgStatement::Copy(_)
		| PgStatement::Select(_)
		| PgStatement::Insert(_)
		| PgStatement::Update(_)
		| PgStatement::Delete(_)
		| PgStatement::Truncate(_)
		| PgStatement::CreateSchema(_)
		| PgStatement::Transaction(_)
		| PgStatement::Set(_)
		| PgStatement::Show(_)
		| PgStatement::Reset(_)
		| PgStatement::Explain(_)
		| PgStatement::Prepare(_)
		| PgStatement::Execute(_)
		| PgStatement::Deallocate(_)
		| PgStatement::CreatePropertyGraph(_)
		| PgStatement::DropPropertyGraph(_)
		| PgStatement::Empty => Err(PlanError::unsupported(format!(
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
		let is_id = col.name == "id";
		let is_not_null = is_id || col.not_null || col.primary_key;
		let base_kind = map_mysql_type(&col.data_type);
		let field_kind = if is_not_null || matches!(base_kind, Kind::Any) {
			base_kind
		} else {
			Kind::either(vec![Kind::None, Kind::Null, base_kind])
		};

		let mut field_def = DefineFieldStatement {
			name: Expr::Idiom(Idiom::field(col.name.as_str())),
			what: Expr::Table(TableName::from(tbl_name)),
			flexible: holds_json(&field_kind),
			field_kind: Some(field_kind),
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
		} else if col.auto_increment {
			let (sequence, default) = column_sequence(tbl_name, &col.name);
			exprs.push(sequence);
			field_def.default = default;
		}

		if col.primary_key {
			let idx = DefineIndexStatement {
				name: Expr::Idiom(Idiom::field(format!("{tbl_name}_{}_pk", col.name))),
				what: Expr::Table(TableName::from(tbl_name)),
				cols: vec![Expr::Idiom(Idiom(vec![Part::Field(Strand::from(col.name.as_str()))]))],
				index: Index::Uniq {
					nulls_not_distinct: false,
				},
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
				index: Index::Uniq {
					nulls_not_distinct: false,
				},
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
					index: Index::Uniq {
						nulls_not_distinct: false,
					},
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
					index: Index::Uniq {
						nulls_not_distinct: false,
					},
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
							materialize: None,
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

/// Lower a MySQL `CREATE DATABASE` statement into SurrealDB expressions.
pub fn lower_mysql_create_database(create: &MyCreateDatabaseStmt) -> Result<Vec<Expr>, PlanError> {
	let def = DefineDatabaseStatement {
		name: Expr::Idiom(Idiom::field(create.name.as_str())),
		kind: if create.if_not_exists {
			DefineKind::IfNotExists
		} else {
			DefineKind::Default
		},
		..Default::default()
	};
	Ok(vec![Expr::Define(Box::new(DefineStatement::Database(def)))])
}

/// Lower a MySQL `DROP DATABASE` statement into SurrealDB expressions.
pub fn lower_mysql_drop_database(drop: &MyDropDatabaseStmt) -> Result<Vec<Expr>, PlanError> {
	let rem = RemoveDatabaseStatement {
		name: Expr::Idiom(Idiom::field(drop.name.as_str())),
		if_exists: drop.if_exists,
		..Default::default()
	};
	Ok(vec![Expr::Remove(Box::new(RemoveStatement::Database(rem)))])
}

/// Lower a MySQL `CREATE INDEX` statement into SurrealDB expressions.
pub fn lower_mysql_create_index(idx: &MyCreateIndexStmt) -> Result<Vec<Expr>, PlanError> {
	let (_, tbl_str) = crate::namespace::resolve_mysql_table_name(&idx.table.parts, None)?;
	let tbl = tbl_str.as_str();

	let mut cols = Vec::new();
	for col in &idx.columns {
		cols.push(Expr::Idiom(Idiom(vec![Part::Field(Strand::from(col.as_str()))])));
	}

	let def = DefineIndexStatement {
		name: Expr::Idiom(Idiom::field(idx.name.as_str())),
		what: Expr::Table(TableName::from(tbl)),
		cols,
		index: if idx.unique {
			Index::Uniq {
				nulls_not_distinct: false,
			}
		} else {
			Index::Idx
		},
		kind: DefineKind::Default,
		..Default::default()
	};
	Ok(vec![Expr::Define(Box::new(DefineStatement::Index(def)))])
}

/// Lower a MySQL `DROP INDEX` statement into SurrealDB expressions.
pub fn lower_mysql_drop_index(drop: &MyDropIndexStmt) -> Result<Vec<Expr>, PlanError> {
	let (_, tbl_str) = crate::namespace::resolve_mysql_table_name(&drop.table.parts, None)?;
	let tbl = tbl_str.as_str();

	let rem = RemoveIndexStatement {
		name: Expr::Idiom(Idiom::field(drop.name.as_str())),
		what: Expr::Table(TableName::from(tbl)),
		if_exists: false,
	};
	Ok(vec![Expr::Remove(Box::new(RemoveStatement::Index(rem)))])
}

/// Lower a MySQL `CREATE VIEW` statement into SurrealDB expressions.
pub fn lower_mysql_create_view(view: &MyCreateViewStmt) -> Result<Vec<Expr>, PlanError> {
	let (_, view_name_str) = crate::namespace::resolve_mysql_table_name(&view.name.parts, None)?;
	let view_name = view_name_str.as_str();

	let mut from_tables = Vec::new();
	if let Some(MyTableRef::Table {
		name,
		..
	}) = &view.query.from
		&& let Ok((_, t)) = crate::namespace::resolve_mysql_table_name(&name.parts, None)
	{
		from_tables.push(TableName::from(t));
	}

	let fields = if view.query.projection.is_empty()
		|| (view.query.projection.len() == 1
			&& matches!(
				&view.query.projection[0],
				surrealdb_mysql::ast::stmt::SelectItem::Wildcard(_)
			)) {
		Fields::all()
	} else {
		let mut selectors = Vec::new();
		for item in &view.query.projection {
			match item {
				surrealdb_mysql::ast::stmt::SelectItem::Wildcard(_) => {
					selectors.push(Field::All);
				}
				surrealdb_mysql::ast::stmt::SelectItem::Expr {
					expr,
					alias,
					..
				} => {
					let lowered = lower_mysql_expr(expr)?;
					let a = alias.as_ref().map(|s| Idiom::field(s.as_str()));
					selectors.push(Field::Single(Selector {
						expr: lowered,
						alias: a,
					}));
				}
				surrealdb_mysql::ast::stmt::SelectItem::QualifiedWildcard(parts, _) => {
					selectors.push(Field::Single(Selector {
						expr: Expr::Idiom(Idiom(
							parts
								.iter()
								.map(|part| Part::Field(Strand::from(part.as_str())))
								.chain(std::iter::once(Part::All))
								.collect(),
						)),
						alias: None,
					}));
				}
			}
		}
		Fields::Select(selectors)
	};

	let cond = if let Some(wh) = &view.query.where_clause {
		Some(Cond(lower_mysql_expr(wh)?))
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

/// Lower a MySQL `DROP VIEW` statement into SurrealDB expressions.
pub fn lower_mysql_drop_view(drop: &MyDropViewStmt) -> Result<Vec<Expr>, PlanError> {
	let mut exprs = Vec::new();
	for name in &drop.names {
		let (_, vname_str) = crate::namespace::resolve_mysql_table_name(&name.parts, None)?;
		let rem = RemoveTableStatement {
			name: Expr::Table(TableName::from(vname_str.as_str())),
			if_exists: drop.if_exists,
			expunge: false,
		};
		exprs.push(Expr::Remove(Box::new(RemoveStatement::Table(rem))));
	}
	Ok(exprs)
}

/// Lower a MySQL `ALTER TABLE` statement into SurrealDB expressions.
pub fn lower_mysql_alter_table(alter: &MyAlterTableStmt) -> Result<Vec<Expr>, PlanError> {
	let (_, tbl_name_str) = crate::namespace::resolve_mysql_table_name(&alter.table.parts, None)?;
	let tbl_name = tbl_name_str.as_str();

	let mut exprs = Vec::new();

	for action in &alter.actions {
		match action {
			MyAlterTableAction::AddColumn(col) => {
				let base_kind = map_mysql_type(&col.data_type);
				let is_not_null = col.name == "id" || col.not_null || col.primary_key;
				let field_kind = if is_not_null || matches!(base_kind, Kind::Any) {
					base_kind
				} else {
					Kind::either(vec![Kind::None, Kind::Null, base_kind])
				};

				let mut field_def = DefineFieldStatement {
					name: Expr::Idiom(Idiom::field(col.name.as_str())),
					what: Expr::Table(TableName::from(tbl_name)),
					field_kind: Some(field_kind),
					kind: DefineKind::Default,
					..Default::default()
				};
				if let Some(def) = &col.default {
					field_def.default = DefineDefault::Set(lower_mysql_expr(def)?);
				}
				exprs.push(Expr::Define(Box::new(DefineStatement::Field(Box::new(field_def)))));
			}
			MyAlterTableAction::DropColumn {
				name,
			} => {
				let rem = RemoveFieldStatement {
					name: Expr::Idiom(Idiom::field(name.as_str())),
					table_name: Expr::Table(TableName::from(tbl_name)),
					if_exists: false,
				};
				exprs.push(Expr::Remove(Box::new(RemoveStatement::Field(rem))));
			}
			MyAlterTableAction::ModifyColumn(col) => {
				let base_kind = map_mysql_type(&col.data_type);
				let is_not_null = col.name == "id" || col.not_null || col.primary_key;
				let field_kind = if is_not_null || matches!(base_kind, Kind::Any) {
					base_kind
				} else {
					Kind::either(vec![Kind::None, Kind::Null, base_kind])
				};

				let mut field_def = DefineFieldStatement {
					name: Expr::Idiom(Idiom::field(col.name.as_str())),
					what: Expr::Table(TableName::from(tbl_name)),
					field_kind: Some(field_kind),
					kind: DefineKind::Overwrite,
					..Default::default()
				};
				if let Some(def) = &col.default {
					field_def.default = DefineDefault::Set(lower_mysql_expr(def)?);
				}
				exprs.push(Expr::Define(Box::new(DefineStatement::Field(Box::new(field_def)))));
			}
			MyAlterTableAction::ChangeColumn {
				old_name,
				new_def,
			} => {
				let rem = RemoveFieldStatement {
					name: Expr::Idiom(Idiom::field(old_name.as_str())),
					table_name: Expr::Table(TableName::from(tbl_name)),
					if_exists: true,
				};
				exprs.push(Expr::Remove(Box::new(RemoveStatement::Field(rem))));

				let base_kind = map_mysql_type(&new_def.data_type);
				let is_not_null = new_def.name == "id" || new_def.not_null || new_def.primary_key;
				let field_kind = if is_not_null || matches!(base_kind, Kind::Any) {
					base_kind
				} else {
					Kind::either(vec![Kind::None, Kind::Null, base_kind])
				};

				let mut field_def = DefineFieldStatement {
					name: Expr::Idiom(Idiom::field(new_def.name.as_str())),
					what: Expr::Table(TableName::from(tbl_name)),
					field_kind: Some(field_kind),
					kind: DefineKind::Default,
					..Default::default()
				};
				if let Some(def) = &new_def.default {
					field_def.default = DefineDefault::Set(lower_mysql_expr(def)?);
				}
				exprs.push(Expr::Define(Box::new(DefineStatement::Field(Box::new(field_def)))));
			}
			MyAlterTableAction::AddConstraint(c) => match c {
				MyTableConstraint::PrimaryKey {
					columns,
					..
				} => {
					let idx = DefineIndexStatement {
						name: Expr::Idiom(Idiom::field(format!("{tbl_name}_pk"))),
						what: Expr::Table(TableName::from(tbl_name)),
						cols: columns
							.iter()
							.map(|col| {
								Expr::Idiom(Idiom(vec![Part::Field(Strand::from(col.as_str()))]))
							})
							.collect(),
						index: Index::Uniq {
							nulls_not_distinct: false,
						},
						kind: DefineKind::Default,
						..Default::default()
					};
					exprs.push(Expr::Define(Box::new(DefineStatement::Index(idx))));
				}
				MyTableConstraint::Unique {
					name,
					columns,
					..
				} => {
					let idx_name = name.clone().unwrap_or_else(|| format!("{tbl_name}_uniq"));
					let idx = DefineIndexStatement {
						name: Expr::Idiom(Idiom::field(idx_name)),
						what: Expr::Table(TableName::from(tbl_name)),
						cols: columns
							.iter()
							.map(|col| {
								Expr::Idiom(Idiom(vec![Part::Field(Strand::from(col.as_str()))]))
							})
							.collect(),
						index: Index::Uniq {
							nulls_not_distinct: false,
						},
						kind: DefineKind::Default,
						..Default::default()
					};
					exprs.push(Expr::Define(Box::new(DefineStatement::Index(idx))));
				}
				MyTableConstraint::ForeignKey {
					..
				} => {}
			},
			MyAlterTableAction::DropPrimaryKey => {
				let rem = RemoveIndexStatement {
					name: Expr::Idiom(Idiom::field(format!("{tbl_name}_pk"))),
					what: Expr::Table(TableName::from(tbl_name)),
					if_exists: true,
				};
				exprs.push(Expr::Remove(Box::new(RemoveStatement::Index(rem))));
			}
			MyAlterTableAction::DropIndex {
				name,
			} => {
				let rem = RemoveIndexStatement {
					name: Expr::Idiom(Idiom::field(name.as_str())),
					what: Expr::Table(TableName::from(tbl_name)),
					if_exists: true,
				};
				exprs.push(Expr::Remove(Box::new(RemoveStatement::Index(rem))));
			}
			MyAlterTableAction::RenameTable {
				new_name,
			} => {
				let (_, new_tbl_str) =
					crate::namespace::resolve_mysql_table_name(&new_name.parts, None)?;
				let table_def = DefineTableStatement {
					name: Expr::Table(TableName::from(new_tbl_str.as_str())),
					kind: DefineKind::Default,
					full: true,
					table_type: TableType::Normal,
					..Default::default()
				};
				exprs.push(Expr::Define(Box::new(DefineStatement::Table(table_def))));
			}
		}
	}

	Ok(exprs)
}

/// Lower a MySQL DDL statement.
///
/// Returns the definitions it runs, one expression each, in the order they run.
pub(crate) fn lower_mysql_ddl_definitions(stmt: &MyStatement) -> Result<Vec<Expr>, PlanError> {
	match stmt {
		MyStatement::CreateTable(s) => lower_mysql_create_table(s),
		MyStatement::DropTable(s) => lower_mysql_drop_table(s),
		MyStatement::CreateIndex(s) => lower_mysql_create_index(s),
		MyStatement::DropIndex(s) => lower_mysql_drop_index(s),
		MyStatement::CreateView(s) => lower_mysql_create_view(s),
		MyStatement::DropView(s) => lower_mysql_drop_view(s),
		MyStatement::AlterTable(s) => lower_mysql_alter_table(s),
		MyStatement::CreateDatabase(s) => lower_mysql_create_database(s),
		MyStatement::DropDatabase(s) => lower_mysql_drop_database(s),
		MyStatement::Select(_)
		| MyStatement::Insert(_)
		| MyStatement::Update(_)
		| MyStatement::Delete(_)
		| MyStatement::Replace(_)
		| MyStatement::Truncate(_)
		| MyStatement::CreateUser(_)
		| MyStatement::DropUser(_)
		| MyStatement::Grant(_)
		| MyStatement::Revoke(_)
		| MyStatement::LockTables(_)
		| MyStatement::UnlockTables(_)
		| MyStatement::Prepare(_)
		| MyStatement::Execute(_)
		| MyStatement::Deallocate(_)
		| MyStatement::Use(_)
		| MyStatement::Set(_)
		| MyStatement::Show(_)
		| MyStatement::Describe(_)
		| MyStatement::Explain(_)
		| MyStatement::Transaction(_) => {
			Err(PlanError::unsupported(format!("MySQL statement not recognized as DDL: {stmt:?}")))
		}
	}
}

/// Lower a PostgreSQL DDL statement into the expression that answers it.
///
/// The result holds exactly one expression, so a caller that runs every returned expression as a
/// statement of its own answers the one SQL statement once. A statement that lowers to several
/// definitions (a table with its fields and indexes, an `ALTER TABLE` with several actions, a
/// `DROP` of several tables) lowers to a block of them: a definition that fails fails the
/// statement as a whole, and none of its definitions is committed.
pub fn lower_pgsql_ddl(stmt: &PgStatement) -> Result<Vec<Expr>, PlanError> {
	lower_pgsql_ddl_with(stmt, &EmptyCatalog)
}

/// Lower a PostgreSQL DDL statement against `catalog` into the expression that answers it, as
/// [`lower_pgsql_ddl`] does; `ALTER TABLE` reads `catalog` for the constraints of the table.
pub fn lower_pgsql_ddl_with(
	stmt: &PgStatement,
	catalog: &dyn BinderCatalog,
) -> Result<Vec<Expr>, PlanError> {
	lower_pgsql_ddl_definitions_with(stmt, catalog)
		.map(|definitions| vec![one_statement(definitions)])
}

/// Lower a PostgreSQL DDL statement as [`lower_pgsql_ddl`] does, where `dimensions` says how many
/// dimensions a vector column has, which an `hnsw` index needs.
pub fn lower_pgsql_ddl_over(
	stmt: &PgStatement,
	dimensions: VectorDimensions<'_>,
) -> Result<Vec<Expr>, PlanError> {
	lower_pgsql_ddl_definitions_over(stmt, &EmptyCatalog, dimensions)
		.map(|definitions| vec![one_statement(definitions)])
}

/// Lower a MySQL DDL statement into the expression that answers it.
///
/// The result holds exactly one expression, as [`lower_pgsql_ddl`] does, and a statement that
/// lowers to several definitions (a table with its fields and indexes, several tables dropped at
/// once) lowers to a block of them with the same all-or-nothing outcome.
pub fn lower_mysql_ddl(stmt: &MyStatement) -> Result<Vec<Expr>, PlanError> {
	lower_mysql_ddl_definitions(stmt).map(|definitions| vec![one_statement(definitions)])
}

/// The single expression that runs `definitions`: the definition itself when there is exactly
/// one, otherwise a block of them in order. A statement that lowers to no definition is an empty
/// block, which answers `NONE`.
fn one_statement(mut definitions: Vec<Expr>) -> Expr {
	if definitions.len() == 1
		&& let Some(definition) = definitions.pop()
	{
		return definition;
	}
	Expr::Block(Box::new(surrealdb_expr::expr::Block(definitions)))
}

#[cfg(test)]
mod statement_tests;
