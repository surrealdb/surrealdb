//! SQL foreign keys, which make their column a record link.
//!
//! A column with a foreign key to table `t` is a field of type `record<t>` (`option<record<t>>`
//! when it is nullable) with a `REFERENCE` whose `ON DELETE` strategy carries out the SQL action.
//! A SQL client still reads and writes the column as the key of the target row
//! ([`crate::lower::link`]), and the catalog reports the SQL type of the target's key.
//!
//! The `REFERENCE` does not check that a link names a row, so the field's `ASSERT` does
//! ([`existence_assertion`]): a write that names no row of `t` fails as a foreign key violation
//! (`23503`). The assertion is the only assertion a foreign key column carries; its type already
//! refuses `NULL` where the column is `NOT NULL`.
//!
//! `ALTER TABLE ... ADD FOREIGN KEY` over a scalar column converts the column and its rows
//! ([`lower_add_foreign_key`]).

use std::collections::HashMap;

use surrealdb_expr::expr::field::Selector;
use surrealdb_expr::expr::operator::BinaryOperator;
use surrealdb_expr::expr::reference::{Reference, ReferenceDeleteStrategy};
use surrealdb_expr::expr::statements::alter::{AlterFieldStatement, AlterKind};
use surrealdb_expr::expr::statements::define::MigrateClause;
use surrealdb_expr::expr::statements::{
	AlterStatement, IfelseStatement, SelectStatement, UpdateStatement,
};
use surrealdb_expr::expr::{
	AssignOperator, Block, Cond, Data, Expr, Fields, Function, FunctionCall, Idiom, Kind, Limit,
	Literal, Output, Param, PrefixOperator, RecordIdKeyLit, RecordIdLit,
};
use surrealdb_pgsql::ast::stmt::{
	ColumnConstraint, ColumnDef, CreateTableStmt, ReferentialAction,
	TableConstraint as PgTableConstraint,
};
use surrealdb_strand::{Strand, TableName};

use super::ddl::not_null_assertion;
use super::write::link_value;
use crate::catalog::{BinderCatalog, ColumnAssert, ColumnShape};
use crate::error::PlanError;

/// The `ON DELETE` action of a foreign key, which must be one a `REFERENCE` carries out:
/// `SET DEFAULT` has no counterpart and is refused (`0A000`).
fn supported_action(action: ReferentialAction) -> Result<ReferentialAction, PlanError> {
	match action {
		ReferentialAction::SetDefault => {
			Err(unsupported("ON DELETE SET DEFAULT: a reference cannot assign a default"))
		}
		ReferentialAction::NoAction
		| ReferentialAction::Restrict
		| ReferentialAction::Cascade
		| ReferentialAction::SetNull => Ok(action),
	}
}

/// The reference strategy that carries out a foreign key's `ON DELETE` action. `NO ACTION` and
/// `RESTRICT` refuse the delete.
pub(crate) fn delete_strategy(action: ReferentialAction) -> ReferenceDeleteStrategy {
	match action {
		ReferentialAction::Cascade => ReferenceDeleteStrategy::Cascade,
		ReferentialAction::SetNull => ReferenceDeleteStrategy::Unset,
		ReferentialAction::NoAction
		| ReferentialAction::Restrict
		| ReferentialAction::SetDefault => ReferenceDeleteStrategy::Reject,
	}
}

/// The `REFERENCE` of a foreign key with the `ON DELETE` action `action`.
pub(crate) fn reference(action: ReferentialAction) -> Reference {
	Reference {
		on_delete: delete_strategy(action),
		materialize: None,
	}
}

/// The type of a column that links to `target`: `record<target>`, or `option<record<target>>`
/// where the column may be `NULL`. A nullable link cannot also name `NULL` as a type member: a
/// `REFERENCE` field admits only records and `NONE`.
pub(crate) fn link_kind(target: &str, nullable: bool) -> Kind {
	let link = Kind::Record(vec![TableName::from(target)]);
	if nullable {
		Kind::option(link)
	} else {
		link
	}
}

/// The name PostgreSQL gives the foreign key of `column` in `table`, which the catalog lists.
fn constraint_name(table: &str, column: &str) -> String {
	format!("{table}_{column}_fkey")
}

/// The refusal of a write that names no row of the target, worded as PostgreSQL words it. The
/// wire layer maps it to `23503`.
fn violation(table: &str, column: &str) -> Expr {
	Expr::Throw(Box::new(Expr::Literal(Literal::String(Strand::from(
		format!(
			"insert or update on table \"{table}\" violates foreign key constraint \"{}\"",
			constraint_name(table, column)
		)
		.as_str(),
	)))))
}

fn call(name: &str, arguments: Vec<Expr>) -> Expr {
	Expr::FunctionCall(Box::new(FunctionCall {
		receiver: Function::Normal(name.to_string()),
		arguments,
	}))
}

fn binary(left: Expr, op: BinaryOperator, right: Expr) -> Expr {
	Expr::Binary {
		left: Box::new(left),
		op,
		right: Box::new(right),
	}
}

fn param(name: &str) -> Expr {
	Expr::Param(Param::from(name.to_string()))
}

fn none() -> Expr {
	Expr::Literal(Literal::None)
}

/// The `ASSERT` of a foreign key column: the value is absent (when the column admits that), is
/// the value the row already holds, or names a row of the target. Anything else is a foreign key
/// violation.
///
/// A row that keeps its value is not judged again, so a write of other columns costs no read of
/// the target. A foreign key to the table's own rows (`target` is `table`) also accepts the row
/// itself, which does not exist yet when its own write is judged.
pub(crate) fn existence_assertion(
	table: &str,
	column: &str,
	target: &str,
	admits_absence: bool,
) -> Expr {
	let unchanged = binary(param("value"), BinaryOperator::Equal, param("before"));
	let exists = call("record::exists", vec![param("value")]);
	let mut expr = binary(unchanged, BinaryOperator::Or, exists);
	if target == table {
		let itself = binary(param("value"), BinaryOperator::Equal, Expr::Idiom(Idiom::field("id")));
		expr = binary(itself, BinaryOperator::Or, expr);
	}
	if admits_absence {
		expr =
			binary(binary(param("value"), BinaryOperator::Equal, none()), BinaryOperator::Or, expr);
	}
	binary(expr, BinaryOperator::Or, violation(table, column))
}

/// A statement that fails as a foreign key violation when a row of `table` holds a value in
/// `column` that names no row of `target`. `converted` is the column's value as a link: for a
/// column that does not hold links yet, the record its key names.
fn orphan_guard(table: &str, column: &str, target: &str, converted: bool) -> Expr {
	let value = || Expr::Idiom(Idiom::field(column));
	let linked = if converted {
		link_value(target, value())
	} else {
		value()
	};
	let present = binary(
		binary(value(), BinaryOperator::NotEqual, none()),
		BinaryOperator::And,
		binary(value(), BinaryOperator::NotEqual, Expr::Literal(Literal::Null)),
	);
	let dangling = Expr::Prefix {
		op: PrefixOperator::Not,
		expr: Box::new(call("record::exists", vec![linked])),
	};
	let orphans = Expr::Select(Box::new(SelectStatement {
		fields: Fields::Value(Box::new(Selector {
			expr: Expr::Idiom(Idiom::field("id")),
			alias: None,
		})),
		omit: Vec::new(),
		only: false,
		what: vec![Expr::Table(TableName::from(table))],
		with: None,
		cond: Some(Cond(binary(present, BinaryOperator::And, dangling))),
		split: None,
		group: None,
		order: None,
		limit: Some(Limit(Expr::Literal(Literal::Integer(1)))),
		start: None,
		fetch: None,
		version: none(),
		timeout: none(),
		explain: None,
		tempfiles: false,
		for_update: false,
	}));
	Expr::IfElse(Box::new(IfelseStatement {
		exprs: vec![(
			binary(
				call("array::len", vec![orphans]),
				BinaryOperator::MoreThan,
				Expr::Literal(Literal::Integer(0)),
			),
			violation(table, column),
		)],
		close: None,
	}))
}

pub(crate) fn alter_column(table: &str, column: &str) -> AlterFieldStatement {
	AlterFieldStatement {
		name: Expr::Idiom(Idiom::field(column)),
		what: Expr::Table(TableName::from(table)),
		..Default::default()
	}
}

pub(crate) fn alter(field: AlterFieldStatement) -> Expr {
	Expr::Alter(Box::new(AlterStatement::Field(Box::new(field))))
}

/// `ALTER FIELD column ON table` that drops the `REFERENCE` and the existence `ASSERT` of a foreign
/// key and keeps the column's type.
pub(crate) fn drop_foreign_key(table: &str, column: &str) -> Expr {
	let mut field = alter_column(table, column);
	field.reference = AlterKind::Drop;
	field.assert = AlterKind::Drop;
	alter(field)
}

/// The members of `kind` other than `NONE` and `NULL`.
fn base_members(kind: &Kind) -> Vec<&Kind> {
	if let Kind::Either(kinds) = kind {
		kinds.iter().flat_map(base_members).collect()
	} else if matches!(kind, Kind::None | Kind::Null) {
		Vec::new()
	} else {
		vec![kind]
	}
}

/// Whether a column of `kind` can hold `NONE` or `NULL`.
fn admits_absence(kind: &Kind) -> bool {
	if let Kind::Either(kinds) = kind {
		kinds.iter().any(admits_absence)
	} else {
		matches!(kind, Kind::Any | Kind::None | Kind::Null)
	}
}

fn unsupported(reason: impl Into<String>) -> PlanError {
	PlanError::UnsupportedStatement(reason.into())
}

/// The table `target` that the foreign key `ref_table (ref_columns)` references, which must be
/// the `id` of the table.
pub(crate) fn referenced_table(
	ref_table: &surrealdb_pgsql::ast::expr::QualifiedName,
	ref_columns: &[String],
) -> Result<String, PlanError> {
	if !matches!(ref_columns, [] | [_]) || ref_columns.iter().any(|c| c != "id") {
		return Err(unsupported("a foreign key must reference the id column of the target table"));
	}
	let (_, target) = crate::namespace::resolve_pgsql_table_name(&ref_table.parts, None)?;
	Ok(target)
}

/// The foreign key of a column from its own `REFERENCES` clause: the table it links to and the
/// `ON DELETE` action.
pub(crate) fn column_foreign_key(
	column: &ColumnDef,
) -> Result<Option<(String, ReferentialAction)>, PlanError> {
	let mut key = None;
	for constraint in &column.constraints {
		if let ColumnConstraint::References {
			table,
			column: referenced,
			on_delete,
		} = constraint
		{
			if key.is_some() {
				return Err(unsupported(format!(
					"column \"{}\" has several foreign keys: a column links to one table",
					column.name
				)));
			}
			key = Some((
				referenced_table(table, referenced.as_slice())?,
				supported_action(*on_delete)?,
			));
		}
	}
	Ok(key)
}

/// The foreign keys of the columns of a table, by column: those of the columns' own `REFERENCES`
/// clauses and of the table's `FOREIGN KEY` constraints. A foreign key is the reference of one
/// column, so a constraint over several columns, and a second foreign key on one column, are
/// refused (`0A000`).
pub(crate) fn table_foreign_keys(
	create: &CreateTableStmt,
) -> Result<HashMap<String, (String, ReferentialAction)>, PlanError> {
	let mut keys = HashMap::new();
	for column in &create.columns {
		if let Some(key) = column_foreign_key(column)? {
			keys.insert(column.name.clone(), key);
		}
	}
	let refuse_second = |column: &str| {
		unsupported(format!(
			"column \"{column}\" has several foreign keys: a column links to one table"
		))
	};
	for constraint in &create.constraints {
		let PgTableConstraint::ForeignKey {
			columns,
			ref_table,
			ref_columns,
			on_delete,
		} = constraint
		else {
			continue;
		};
		let [column] = columns.as_slice() else {
			return Err(unsupported(
				"a foreign key over several columns: a foreign key is the reference of one column",
			));
		};
		if !create.columns.iter().any(|c| &c.name == column) {
			return Err(PlanError::UndefinedColumn(column.clone()));
		}
		let key = (referenced_table(ref_table, ref_columns)?, supported_action(*on_delete)?);
		if keys.insert(column.clone(), key).is_some() {
			return Err(refuse_second(column));
		}
	}
	Ok(keys)
}

/// Refuse a foreign key whose `target` is none of the tables the catalog lists, other than `own`,
/// the table being created or altered. A catalog that lists no table (one that is not allowed to,
/// or that only records what is asked of it) accepts every target.
pub(crate) fn check_target_exists(
	catalog: &dyn BinderCatalog,
	target: &str,
	own: &str,
) -> Result<(), PlanError> {
	let listed = catalog.constraint_tables();
	if listed.is_empty() || target == own {
		return Ok(());
	}
	let Some(table) = listed.iter().find(|table| table.name == target) else {
		return Err(PlanError::UndefinedTable(target.to_owned()));
	};
	// A link names the `id` of its target: a target whose primary key is another column, which
	// `REFERENCES target` without a column list would name, is refused.
	let pk_index = format!("{target}_");
	let other_key = table.indexes.iter().find(|index| {
		index.unique
			&& index.name.starts_with(&pk_index)
			&& index.name.ends_with("_pk")
			&& index.columns != ["id"]
	});
	if other_key.is_some() {
		return Err(unsupported(format!(
			"a foreign key must reference the id column of \"{target}\", whose primary key is another column"
		)));
	}
	Ok(())
}

/// Refuse the foreign keys of `create` that name a table the catalog does not list.
pub(crate) fn check_create_targets(
	create: &CreateTableStmt,
	catalog: &dyn BinderCatalog,
) -> Result<(), PlanError> {
	let (_, own) = crate::namespace::resolve_pgsql_table_name(&create.name.parts, None)?;
	// A reference to the table itself names its `id`: a primary key on another column, which
	// `REFERENCES table` without a column list would name, is refused.
	let other_column_key = create.columns.iter().any(|column| {
		column.name != "id"
			&& column.constraints.iter().any(|c| matches!(c, ColumnConstraint::PrimaryKey))
	}) || create.constraints.iter().any(
		|constraint| matches!(constraint, PgTableConstraint::PrimaryKey(columns) if columns != &["id"]),
	);
	for (target, _) in table_foreign_keys(create)?.values() {
		if target == own.as_str() && other_column_key {
			return Err(unsupported(format!(
				"a foreign key must reference the id column of \"{target}\", whose primary key is another column"
			)));
		}
		check_target_exists(catalog, target, own.as_str())?;
	}
	Ok(())
}

/// `steps` as one statement: they run in one transaction, in order, and the first to fail
/// abandons the rest and what the earlier ones did. Separate statements would each commit, and
/// the ones after a failed check would still run.
fn atomically(steps: Vec<Expr>) -> Expr {
	Expr::Block(Box::new(Block(steps)))
}

/// Lower `ADD FOREIGN KEY (column) REFERENCES target (id)` of table `table`.
///
/// The result is one statement ([`atomically`]). A column that already links to `target` gets the
/// `REFERENCE` and the existence `ASSERT`. A scalar column (an integer, string or uuid, which are
/// the types of a record key) becomes the link to the record its value names, and so do its
/// rows, in these steps:
///
/// 1. a check that no row holds a value that names no row of `target` (a violation fails the
///    statement before anything changes);
/// 2. the column's type widened to take links as well as its own values;
/// 3. the rows rewritten to hold links (`NULL` becomes absent, as a link column holds no `NULL`);
/// 4. the type narrowed to the link, with a migration that keeps the links the rows hold;
/// 5. the `REFERENCE` and the existence `ASSERT`.
///
/// The rows are rewritten in step 3, ahead of the narrowing, because the engine refuses to migrate
/// the field of a `REFERENCE` and keeps no backlink for a row it migrates lazily. A column whose
/// assertion is not the `NOT NULL` one, or that has a `DEFAULT`, or of another type, is refused
/// (`0A000`) rather than converted: the new type would not satisfy them.
pub(crate) fn lower_add_foreign_key(
	catalog: &dyn BinderCatalog,
	schema: Option<&str>,
	table: &str,
	key: &PgTableConstraint,
) -> Result<Vec<Expr>, PlanError> {
	let PgTableConstraint::ForeignKey {
		columns,
		ref_table,
		ref_columns,
		on_delete,
	} = key
	else {
		return Err(PlanError::unsupported("a foreign key was expected"));
	};
	let [column] = columns.as_slice() else {
		return Err(unsupported(
			"ADD FOREIGN KEY over several columns: a foreign key is the reference of one column",
		));
	};
	let target = referenced_table(ref_table, ref_columns)?;
	let target = target.as_str();
	let on_delete = &supported_action(*on_delete)?;
	// The tables the catalog lists, which is none for a lowering that reads no schema: the target
	// must be one of them, and the column must not already have a foreign key, which a second
	// would replace.
	let listed = catalog.constraint_tables();
	if !listed.is_empty() {
		check_target_exists(catalog, target, table)?;
		let has_key = listed
			.iter()
			.any(|t| t.name == table && t.foreign_keys.iter().any(|key| key.column == *column));
		if has_key {
			return Err(unsupported(format!(
				"ADD FOREIGN KEY: column \"{column}\" of \"{table}\" already has a foreign key"
			)));
		}
	}
	let shape = catalog.column_shape(schema, table, column);
	let linked = catalog.link_table(schema, table, column);
	if let Some(linked) = linked.as_deref() {
		if linked != target {
			return Err(unsupported(format!(
				"ADD FOREIGN KEY: column \"{column}\" of \"{table}\" links to \"{linked}\", not \"{target}\""
			)));
		}
		let admits = shape.as_ref().is_none_or(|s| s.assert != ColumnAssert::NotNull);
		return Ok(vec![atomically(vec![
			orphan_guard(table, column, target, false),
			set_reference(table, column, target, *on_delete, admits),
		])]);
	}
	let Some(shape) = shape else {
		if catalog.defers_schema_checks() {
			return Ok(Vec::new());
		}
		return Err(PlanError::UndefinedColumn(format!("{table}.{column}")));
	};
	scalar_to_link(table, column, target, *on_delete, &shape)
}

/// `ALTER FIELD column ON table` that sets the `REFERENCE` and the existence `ASSERT` of a
/// foreign key and nothing else.
fn set_reference(
	table: &str,
	column: &str,
	target: &str,
	action: ReferentialAction,
	admits_absence: bool,
) -> Expr {
	let mut field = alter_column(table, column);
	field.reference = AlterKind::Set(reference(action));
	field.assert = AlterKind::Set(existence_assertion(table, column, target, admits_absence));
	alter(field)
}

/// The statements that convert the scalar column `column` of `table` to a link to `target`.
fn scalar_to_link(
	table: &str,
	column: &str,
	target: &str,
	action: ReferentialAction,
	shape: &ColumnShape,
) -> Result<Vec<Expr>, PlanError> {
	let refuse = |reason: &str| {
		unsupported(format!(
			"ADD FOREIGN KEY: column \"{column}\" of \"{table}\" cannot become a link: {reason}"
		))
	};
	let Some(kind) = shape.kind.as_ref() else {
		return Err(refuse("it has no declared type"));
	};
	if !matches!(
		base_members(kind).as_slice(),
		[Kind::Int
			| Kind::I8
			| Kind::I16
			| Kind::I32
			| Kind::U8
			| Kind::U16
			| Kind::U32
			| Kind::String
			| Kind::Uuid]
	) {
		return Err(refuse("only an integer, text or uuid column can hold the key of a row"));
	}
	if shape.assert == ColumnAssert::Other {
		return Err(refuse("it has an assertion of its own"));
	}
	if shape.has_default {
		return Err(refuse("it has a default"));
	}
	let not_null = !admits_absence(kind) || shape.assert == ColumnAssert::NotNull;

	// Widening keeps what the column accepted and adds the link, and restates a `NOT NULL`
	// assertion the engine would refuse to keep over a changed type.
	let mut widen = alter_column(table, column);
	widen.kind = AlterKind::Set(Kind::either(vec![kind.clone(), link_kind(target, false)]));
	widen.assert = match (shape.assert, admits_absence(kind)) {
		(ColumnAssert::NotNull, true) => AlterKind::Set(not_null_assertion()),
		(ColumnAssert::NotNull, false) => AlterKind::Drop,
		(ColumnAssert::None | ColumnAssert::Other, _) => AlterKind::None,
	};

	let value = || Expr::Idiom(Idiom::field(column));
	let mut converted = link_value(target, value());
	if !not_null {
		converted = Expr::IfElse(Box::new(IfelseStatement {
			exprs: vec![(
				binary(value(), BinaryOperator::Equal, Expr::Literal(Literal::Null)),
				none(),
			)],
			close: Some(converted),
		}));
	}
	let rewrite = Expr::Update(Box::new(UpdateStatement {
		what: vec![Expr::Table(TableName::from(table))],
		data: Some(Data::SetExpression(vec![surrealdb_expr::expr::data::Assignment {
			place: Idiom::field(column),
			operator: AssignOperator::Assign,
			value: converted,
		}])),
		cond: Some(Cond(binary(value(), BinaryOperator::NotEqual, none()))),
		output: Some(Output::None),
		..Default::default()
	}));

	// The rows hold links by now, so the migration keeps them, and it converts a key a row
	// still holds all the same. It is the engine's way to narrow a type over stored rows; its
	// fallback is the constant the engine requires of it, which only a value that is no key
	// reaches.
	let mut narrow = alter_column(table, column);
	narrow.kind = AlterKind::Set(link_kind(target, !not_null));
	narrow.assert = match (shape.assert, admits_absence(kind)) {
		(ColumnAssert::NotNull, true) => AlterKind::Drop,
		(ColumnAssert::NotNull | ColumnAssert::None | ColumnAssert::Other, _) => AlterKind::None,
	};
	let (using, fallback) = if not_null {
		(
			link_value(target, param("value")),
			Expr::Literal(Literal::RecordId(RecordIdLit {
				table: TableName::from(target),
				key: RecordIdKeyLit::Number(0),
			})),
		)
	} else {
		(
			Expr::IfElse(Box::new(IfelseStatement {
				exprs: vec![(
					binary(param("value"), BinaryOperator::Equal, Expr::Literal(Literal::Null)),
					none(),
				)],
				close: Some(link_value(target, param("value"))),
			})),
			none(),
		)
	};
	narrow.migrate = Some(MigrateClause {
		name: constraint_name(table, column),
		using,
		fallback: Some(fallback),
	});

	Ok(vec![atomically(vec![
		orphan_guard(table, column, target, true),
		alter(widen),
		rewrite,
		alter(narrow),
		set_reference(table, column, target, action, !not_null),
	])])
}
