//! Typed write lowering from PostgreSQL and MySQL AST to SurrealDB `surrealdb_expr::Expr`.

use surrealdb_expr::expr::data::Assignment;
use surrealdb_expr::expr::field::Selector;
use surrealdb_expr::expr::statements::{
	DeleteStatement, IfelseStatement, InsertStatement, SelectStatement, SetStatement,
	UpdateStatement,
};
use surrealdb_expr::expr::{
	AssignOperator, BinaryOperator, Block, Cond, Data, Expr, Field, Fields, Function, FunctionCall,
	Idiom, Literal, ObjectEntry, Output, Param, Part, PrefixOperator, RecordIdKeyLit, RecordIdLit,
};
use surrealdb_strand::{Strand, TableName};

use crate::catalog::BinderCatalog;
use crate::dialect::SqlDialect;
use crate::error::PlanError;
use crate::functions::FunctionRegistry;
use crate::lower::id_key::{KEY_KINDS, NAMING_KINDS, RECORD_KIND};
use crate::lower::write_null as sql_null;

// ============================================================================
// The `id` column of a statement's target table
// ============================================================================

/// The base table a write statement targets, whose `id` column its expressions read as the key of
/// the stored record id (see [`crate::lower::id_key`]), and whose link columns (see
/// [`crate::lower::link`]) its assignments and comparisons write and search by key and its other
/// expressions read as the key of the link.
///
/// The catalog is asked about each assigned column and each column of the table that an expression
/// names (a column may be asked about more than once), never about the table as a whole.
#[derive(Clone)]
struct IdScope<'a> {
	/// The resolved name of the table.
	table: String,
	/// The schema the table was named in, if any.
	schema: Option<String>,
	/// The names a column reference may qualify `id` by: the table's names and its alias.
	qualifiers: Vec<String>,
	/// Whether the expressions read the row the statement deleted rather than the row it left:
	/// the projection of a `DELETE`, whose result row is empty, reads its columns from `$before`.
	deleted_row: bool,
	/// Whether a column name matches `id` whatever its case, as a MySQL column name does. A
	/// PostgreSQL name is matched exactly: its parser has folded an unquoted `ID` to `id`, so a
	/// column spelled `ID` there is a quoted one, another column than the key.
	columns_ignore_case: bool,
	/// The variable `EXCLUDED` reads: the row an `INSERT` proposes.
	excluded: &'static str,
	/// The catalog that says which columns of the table hold record links.
	catalog: &'a dyn BinderCatalog,
}

impl<'a> IdScope<'a> {
	fn new(
		table: &str,
		schema: Option<String>,
		qualifiers: &[&str],
		columns_ignore_case: bool,
		catalog: &'a dyn BinderCatalog,
	) -> Self {
		Self {
			table: table.to_string(),
			schema,
			qualifiers: qualifiers.iter().map(|q| q.to_string()).collect(),
			deleted_row: false,
			columns_ignore_case,
			excluded: "input",
			catalog,
		}
	}

	/// Whether `column` is the name of the key column.
	fn is_key_name(&self, column: &str) -> bool {
		if self.columns_ignore_case {
			column.eq_ignore_ascii_case("id")
		} else {
			column == "id"
		}
	}

	/// The dimensions of the column `column` of the target table when it holds vectors.
	fn vector_column(&self, column: &str) -> Option<usize> {
		self.catalog.vector_dimension(self.schema.as_deref(), &self.table, column)
	}

	/// The table the column `column` of the target table holds record links to, if it holds them.
	/// `id` never holds links.
	fn link_column(&self, column: &str) -> Option<String> {
		self.catalog
			.link_table(self.schema.as_deref(), &self.table, column)
			.filter(|_| !self.is_key_name(column))
	}

	/// Whether the link column `column` of the target table admits `NONE`.
	fn link_admits_none(&self, column: &str) -> bool {
		self.catalog.link_admits_none(self.schema.as_deref(), &self.table, column)
	}

	/// The column of the target table that the column reference `fields` names (`col`, or `col`
	/// qualified by one of the table's names) and the table it holds record links to, if it holds
	/// them. `None` when the reference names a column of another relation.
	fn link_of(&self, fields: &[String]) -> Option<(String, String)> {
		let (column, prefix) = fields.split_last()?;
		if !prefix.is_empty() && !self.is_qualifier(&prefix.join(".")) {
			return None;
		}
		self.link_column(column).map(|table| (column.clone(), table))
	}

	/// This scope, read over the row the statement deleted.
	fn over_deleted_row(&self) -> Self {
		Self {
			deleted_row: true,
			..self.clone()
		}
	}

	/// Whether `qualifier` is one of the names the target table is read by.
	fn is_qualifier(&self, qualifier: &str) -> bool {
		self.qualifiers.iter().any(|name| {
			if self.columns_ignore_case {
				name.eq_ignore_ascii_case(qualifier)
			} else {
				name == qualifier
			}
		})
	}

	/// Whether the column reference `fields` names the `id` of the target table: `id`, or `id`
	/// qualified by one of the table's names.
	fn reads_key(&self, fields: &[String]) -> bool {
		let Some((last, prefix)) = fields.split_last() else {
			return false;
		};
		self.is_key_name(last) && (prefix.is_empty() || self.is_qualifier(&prefix.join(".")))
	}

	/// The lowered column reference `fields` when the scope reads it other than as written: the
	/// key for `id`, a link column of the table, and a field of the deleted row over a deleted row.
	///
	/// A link column is the key of its link (see [`guarded_key`]) unless `stored`, which is how a
	/// value that stands alone is read: then it is the link itself. Either way it is the bare field
	/// of the row, whatever qualifier names it.
	fn column(&self, fields: &[String], stored: bool) -> Option<Expr> {
		if self.reads_key(fields) {
			return Some(self.key());
		}
		let link = self.link_of(fields);
		if link.is_none()
			&& !self.deleted_row
			&& !(fields.len() > 1 && self.is_qualifier(&fields[..fields.len() - 1].join(".")))
		{
			return None;
		}
		let name = match &link {
			Some((column, _)) => std::slice::from_ref(column),
			None => match fields {
				[.., name]
					if fields.len() > 1
						&& self.is_qualifier(&fields[..fields.len() - 1].join(".")) =>
				{
					std::slice::from_ref(name)
				}
				[name] => std::slice::from_ref(name),
				[qualifier, rest @ ..] if self.is_qualifier(qualifier) => rest,
				all => all,
			},
		};
		let mut parts = Vec::with_capacity(name.len() + 1);
		if self.deleted_row {
			parts.push(Part::Start(Expr::Param(Param::from("before".to_string()))));
		}
		parts.extend(name.iter().map(|f| Part::Field(Strand::from(f.as_str()))));
		let column = Expr::Idiom(Idiom(parts));
		Some(match link {
			Some(_) if !stored => guarded_key(column),
			_ => column,
		})
	}

	/// The key of the record id of the row the expressions read.
	fn key(&self) -> Expr {
		if self.deleted_row {
			call("record::id", vec![deleted_id()])
		} else {
			record_key()
		}
	}

	/// The projection that echoes the row a statement wrote, or the row it deleted, in full with
	/// the key as its `id`.
	fn echo(&self) -> Output {
		if self.deleted_row {
			Output::Fields(deleted_row_with_key())
		} else {
			Output::Fields(row_with_key())
		}
	}
}

/// An operand of an `id` comparison that names a record of the table: the key as an integer or
/// string literal, or a bound parameter, or the value of a column or an expression, which holds the
/// key or a record link to the row.
enum WriteKey {
	Int(i64),
	Str(String),
	Param(String),
	Value(Expr),
}

fn call(name: &str, arguments: Vec<Expr>) -> Expr {
	Expr::FunctionCall(Box::new(FunctionCall {
		receiver: Function::Normal(name.to_string()),
		arguments,
	}))
}

/// The stored `id` field.
fn stored_id() -> Expr {
	Expr::Idiom(Idiom::field("id".to_string()))
}

/// `record::id(id)`: the key of the stored record id.
fn record_key() -> Expr {
	call("record::id", vec![stored_id()])
}

/// The key of the link `record`: `IF type::is_record(record) THEN record::id(record) ELSE record
/// END`. A value that is no link (`NULL`, absent) is its own key. It is spelled as the `SELECT`
/// lowering spells the key of a link column, so that one predicate lowers alike in a read and in a
/// write.
fn guarded_key(record: Expr) -> Expr {
	Expr::IfElse(Box::new(IfelseStatement {
		exprs: vec![(
			call(RECORD_KIND, vec![record.clone()]),
			call("record::id", vec![record.clone()]),
		)],
		close: Some(record),
	}))
}

/// The record of `table` that `key` names. An absent or `NULL` parameter names no record, so it
/// gives `NONE` rather than the record `type::record` would name by the string `"NONE"` or
/// `"NULL"`.
fn record_named(table: &str, key: &WriteKey) -> Expr {
	record_named_with(table, key, Literal::None)
}

/// The record of `table` that `key` names, or `no_record` when the key names none.
fn record_named_with(table: &str, key: &WriteKey, no_record: Literal) -> Expr {
	let key_lit = |key| {
		Expr::Literal(Literal::RecordId(RecordIdLit {
			table: TableName::from(table),
			key,
		}))
	};
	match key {
		WriteKey::Int(i) => key_lit(RecordIdKeyLit::Number(*i)),
		WriteKey::Str(s) => key_lit(RecordIdKeyLit::String(Strand::from(s.as_str()))),
		WriteKey::Param(name) => {
			record_named_by_value_with(table, Expr::Param(Param::from(name.clone())), no_record)
		}
		WriteKey::Value(value) => record_named_by_value_with(table, value.clone(), no_record),
	}
}

/// `IF <value is no key of one of kinds> THEN <otherwise> ELSE type::record(<table>, <value>) END`.
///
/// A value is no key when it is the empty string, which `type::record` refuses, or none of `kinds`,
/// which are [`KEY_KINDS`] or [`NAMING_KINDS`].
fn record_if_key(table: &str, value: Expr, kinds: &[&str], otherwise: Expr) -> Expr {
	let test = |name: &&str| call(name, vec![value.clone()]);
	let either = |left, right| Expr::Binary {
		left: Box::new(left),
		op: BinaryOperator::Or,
		right: Box::new(right),
	};
	let is_key = kinds.iter().map(test).reduce(either).expect("a list of key kinds");
	let empty = Expr::Binary {
		left: Box::new(value.clone()),
		op: BinaryOperator::Equal,
		right: Box::new(Expr::Literal(Literal::String(Strand::from("")))),
	};
	Expr::IfElse(Box::new(IfelseStatement {
		exprs: vec![(
			either(
				empty,
				Expr::Prefix {
					op: PrefixOperator::Not,
					expr: Box::new(is_key),
				},
			),
			otherwise,
		)],
		close: Some(call(
			"type::record",
			vec![Expr::Literal(Literal::String(Strand::from(table))), value],
		)),
	}))
}

/// The record of `table` that `value` names: by its key when it is a record link, and by the value
/// itself otherwise (`type::record`). A value that is no record key (an empty string, a float, a
/// boolean, a datetime, `NULL`, absent) names no record, so it gives `no_record` rather than the
/// record `type::record` would name by its text, or the error it raises for an empty key.
fn record_named_by_value_with(table: &str, value: Expr, no_record: Literal) -> Expr {
	record_if_key(table, value, &NAMING_KINDS, Expr::Literal(no_record))
}

/// `id = <record of table named by key>` (or `!=`), the form the engine reads a record by directly.
fn id_compare(table: &str, key: &WriteKey, op: BinaryOperator) -> Expr {
	Expr::Binary {
		left: Box::new(stored_id()),
		op,
		right: Box::new(record_named(table, key)),
	}
}

/// `id IN [<records of table named by keys>]` (or `NOT IN`); of one key, the equality the engine
/// reads a record by.
fn id_in(table: &str, keys: &[WriteKey], negated: bool) -> Expr {
	if let [key] = keys {
		let op = if negated {
			BinaryOperator::NotEqual
		} else {
			BinaryOperator::Equal
		};
		return id_compare(table, key, op);
	}
	Expr::Binary {
		left: Box::new(stored_id()),
		op: if negated {
			BinaryOperator::NotInside
		} else {
			BinaryOperator::Inside
		},
		right: Box::new(Expr::Literal(Literal::Array(
			keys.iter().map(|key| record_named(table, key)).collect(),
		))),
	}
}

/// The `id` of the row a `DELETE` removed.
fn deleted_id() -> Expr {
	Expr::Idiom(Idiom(vec![
		Part::Start(Expr::Param(Param::from("before".to_string()))),
		Part::Field(Strand::from("id")),
	]))
}

/// The row a `DELETE` removed, in full, with `id` as the record key.
fn deleted_row_with_key() -> Fields {
	let before = Expr::Param(Param::from("before".to_string()));
	let key = Expr::Literal(Literal::Object(vec![ObjectEntry {
		key: Strand::from("id"),
		value: call("record::id", vec![deleted_id()]),
	}]));
	Fields::Value(Box::new(Selector {
		expr: call("object::extend", vec![before, key]),
		alias: None,
	}))
}

/// The fields of a row echoed or returned in full: every field, with `id` as the record key.
fn row_with_key() -> Fields {
	Fields::Select(vec![
		Field::All,
		Field::Single(Selector {
			expr: record_key(),
			alias: Some(Idiom::field("id".to_string())),
		}),
	])
}

/// Make `fields`, the projection list of a statement over the target table, read a wildcard's
/// `id` as the key: the key follows the last wildcard and replaces its record id.
fn wildcard_id_as_key(fields: &mut Vec<Field>) {
	if let Some(last_all) = fields.iter().rposition(|f| matches!(f, Field::All)) {
		fields.insert(
			last_all + 1,
			Field::Single(Selector {
				expr: record_key(),
				alias: Some(Idiom::field("id".to_string())),
			}),
		);
	}
}

// ============================================================================
// Record link columns
// ============================================================================

/// `build(value)` with `value` evaluated once: a literal, a parameter or a column is used as it
/// is, and any other expression (which may be costly or volatile, a function call for one) is bound
/// by a `LET` in a block that holds the result.
fn evaluated_once(value: Expr, build: impl FnOnce(Expr) -> Expr) -> Expr {
	if matches!(value, Expr::Literal(_) | Expr::Param(_) | Expr::Idiom(_)) {
		return build(value);
	}
	let name = "_link";
	Expr::Block(Box::new(Block(vec![
		Expr::Let(Box::new(SetStatement {
			name: Strand::from(name),
			what: value,
			kind: None,
		})),
		build(Expr::Param(Param::from(name.to_string()))),
	])))
}

/// The record literal `table:<key>` for an integer or non-empty string literal `value`.
fn record_literal(table: &str, value: &Expr) -> Option<Expr> {
	let key = match value {
		Expr::Literal(Literal::Integer(i)) => RecordIdKeyLit::Number(*i),
		Expr::Literal(Literal::String(s)) if !s.as_str().is_empty() => {
			RecordIdKeyLit::String(s.clone())
		}
		Expr::Literal(_)
		| Expr::Param(_)
		| Expr::Idiom(_)
		| Expr::Table(_)
		| Expr::Mock(_)
		| Expr::Block(_)
		| Expr::Constant(_)
		| Expr::Prefix {
			..
		}
		| Expr::Postfix {
			..
		}
		| Expr::Binary {
			..
		}
		| Expr::FunctionCall(_)
		| Expr::Closure(_)
		| Expr::Break
		| Expr::Continue
		| Expr::Return(_)
		| Expr::Throw(_)
		| Expr::IfElse(_)
		| Expr::Select(_)
		| Expr::Create(_)
		| Expr::Update(_)
		| Expr::Upsert(_)
		| Expr::Delete(_)
		| Expr::Relate(_)
		| Expr::Insert(_)
		| Expr::Define(_)
		| Expr::Remove(_)
		| Expr::Rebuild(_)
		| Expr::Alter(_)
		| Expr::Info(_)
		| Expr::Foreach(_)
		| Expr::Let(_)
		| Expr::Sleep(_)
		| Expr::Explain {
			..
		} => return None,
	};
	Some(Expr::Literal(Literal::RecordId(RecordIdLit {
		table: TableName::from(table),
		key,
	})))
}

/// `value`, written to a column that holds record links to `table`.
///
/// An integer or non-empty string literal is the record literal `table:<key>`. A value that is a
/// record when evaluated is left as it is, even a record of another table, which the column's own
/// type check judges. An integer, non-empty string, uuid, array or object is the record
/// `type::record(<table>, <value>)` names. Anything else is left as it is, for the type check of
/// the column to accept (`NONE`) or refuse: a float, a boolean, a datetime, `NULL` or the empty
/// string is no key.
pub(crate) fn link_value(table: &str, value: Expr) -> Expr {
	if let Some(record) = record_literal(table, &value) {
		return record;
	}
	if matches!(value, Expr::Literal(_)) {
		return value;
	}
	evaluated_once(value, |value| record_if_key(table, value.clone(), &KEY_KINDS, value))
}

/// The operand `lowered` of a comparison with a column that holds record links to `table`, as the
/// record it names. `column` is the column reference the operand was written as, if it was one.
///
/// A key is the record literal `table:<key>`, and a parameter, a column or an expression is the
/// record `type::record(<table>, <value>)` names, or `NULL` when it names none, which a link never
/// equals. The `id` of `table` and a link column to `table` are compared as stored, and the `id` of
/// another table is compared by its key. A literal that is no key is left as it is.
fn link_comparand(
	scope: &IdScope<'_>,
	table: &str,
	lowered: Expr,
	column: Option<&[String]>,
) -> Expr {
	link_comparand_with(scope, table, lowered, column, false)
}

fn link_comparand_with(
	scope: &IdScope<'_>,
	table: &str,
	lowered: Expr,
	column: Option<&[String]>,
	preserve_value: bool,
) -> Expr {
	if let Some(fields) = column {
		if scope.reads_key(fields) && table == scope.table {
			return stored_id();
		}
		if let Some((name, link)) = scope.link_of(fields)
			&& link == table
		{
			return Expr::Idiom(Idiom::field(name));
		}
	}
	if let Some(record) = record_literal(table, &lowered) {
		return record;
	}
	if preserve_value
		&& let Expr::Literal(Literal::Float(value)) = &lowered
		&& let Some(value) = surrealdb_expr::val::Number::Float(*value).as_int_lossless()
	{
		return record_named(table, &WriteKey::Int(value));
	}

	if matches!(lowered, Expr::Literal(_)) {
		return lowered;
	}
	evaluated_once(lowered, |value| {
		if preserve_value {
			let fallback = sql_null::raw(
				value.clone(),
				BinaryOperator::NullCoalescing,
				Expr::Literal(Literal::Null),
			);
			record_if_key(table, value, &NAMING_KINDS, fallback)
		} else {
			record_named_by_value_with(table, value, Literal::Null)
		}
	})
}

/// `<column> = <record>` (or `!=`) of the link column `column`.
fn link_compare(column: String, op: BinaryOperator, record: Expr) -> Expr {
	Expr::Binary {
		left: Box::new(Expr::Idiom(Idiom::field(column))),
		op,
		right: Box::new(record),
	}
}

/// `<column> IN [<records>]` (or `NOT IN`) of the link column `column`; of one record, the equality
/// the engine reads by an index.
fn link_in(column: String, mut records: Vec<Expr>, negated: bool) -> Expr {
	if records.len() == 1
		&& let Some(record) = records.pop()
	{
		let op = if negated {
			BinaryOperator::NotEqual
		} else {
			BinaryOperator::Equal
		};
		return link_compare(column, op, record);
	}
	Expr::Binary {
		left: Box::new(Expr::Idiom(Idiom::field(column))),
		op: if negated {
			BinaryOperator::NotInside
		} else {
			BinaryOperator::Inside
		},
		right: Box::new(Expr::Literal(Literal::Array(records))),
	}
}

/// [`link_value`] for a column that admits `NONE` (`admits_none`): SQL `NULL` is stored as `NONE`,
/// which reads back as `NULL`. A column that does not admit `NONE` keeps refusing `NULL`.
pub(crate) fn link_assignment(table: &str, value: Expr, admits_none: bool) -> Expr {
	if !admits_none {
		return link_value(table, value);
	}
	if matches!(value, Expr::Literal(Literal::Null)) {
		return Expr::Literal(Literal::None);
	}
	let converted = link_value(table, value);
	if matches!(converted, Expr::Literal(_)) {
		return converted;
	}
	evaluated_once(converted, |value| {
		Expr::IfElse(Box::new(IfelseStatement {
			exprs: vec![(
				Expr::Binary {
					left: Box::new(value.clone()),
					op: BinaryOperator::Equal,
					right: Box::new(Expr::Literal(Literal::Null)),
				},
				Expr::Literal(Literal::None),
			)],
			close: Some(value),
		}))
	})
}

/// The `value` written to the column `column` of the target table, converted by [`link_assignment`]
/// when the column is a link column.
fn assigned_value(scope: &IdScope<'_>, column: &str, value: Expr) -> Expr {
	match scope.link_column(column) {
		Some(table) => {
			let admits_none = scope.link_admits_none(column);
			link_assignment(&table, value, admits_none)
		}
		None => value,
	}
}

/// The `value` written to the column `column` of the target table by a PostgreSQL statement:
/// converted as [`assigned_value`] converts it, and read as a vector when the column holds
/// vectors.
fn assigned_pgsql_value(scope: &IdScope<'_>, column: &str, value: Expr) -> Result<Expr, PlanError> {
	match scope.vector_column(column) {
		Some(dimensions) if scope.link_column(column).is_none() => {
			vector_value(value, Some(dimensions))
		}
		Some(_) | None => Ok(assigned_value(scope, column, value)),
	}
}

/// The `value` written to a vector column of `dimensions` dimensions, where text is the text form
/// of a vector.
///
/// A text literal is read when the statement is lowered, and is refused if it is not a vector of
/// those dimensions. A parameter or a column is read when the statement runs: text is the text
/// form of a vector, and any other value is written as it is. Any other expression is written as
/// it is.
fn vector_value(value: Expr, dimensions: Option<usize>) -> Result<Expr, PlanError> {
	if let Expr::Literal(Literal::String(text)) = &value {
		let elements = crate::vector::parse_text(text.as_str())?;
		if let Some(expected) = dimensions
			&& expected != elements.len()
		{
			return Err(PlanError::DataException(format!(
				"expected {expected} dimensions, not {}",
				elements.len()
			)));
		}
		return Ok(Expr::Literal(Literal::Array(
			elements.into_iter().map(|element| Expr::Literal(Literal::Float(element))).collect(),
		)));
	}
	let is_cheap_to_read_twice = match &value {
		Expr::Param(_) => true,
		Expr::Idiom(Idiom(parts)) => parts.iter().all(|part| matches!(part, Part::Field(_))),
		Expr::Literal(_)
		| Expr::Table(_)
		| Expr::Mock(_)
		| Expr::Block(_)
		| Expr::Constant(_)
		| Expr::Prefix {
			..
		}
		| Expr::Postfix {
			..
		}
		| Expr::Binary {
			..
		}
		| Expr::FunctionCall(_)
		| Expr::Closure(_)
		| Expr::Break
		| Expr::Continue
		| Expr::Return(_)
		| Expr::Throw(_)
		| Expr::IfElse(_)
		| Expr::Select(_)
		| Expr::Create(_)
		| Expr::Update(_)
		| Expr::Upsert(_)
		| Expr::Delete(_)
		| Expr::Relate(_)
		| Expr::Insert(_)
		| Expr::Define(_)
		| Expr::Remove(_)
		| Expr::Rebuild(_)
		| Expr::Alter(_)
		| Expr::Info(_)
		| Expr::Foreach(_)
		| Expr::Let(_)
		| Expr::Sleep(_)
		| Expr::Explain {
			..
		} => false,
	};
	if is_cheap_to_read_twice {
		return text_or_value(value);
	}
	if matches!(value, Expr::Literal(_)) {
		return Ok(value);
	}
	// Any other expression is evaluated once, and its value is read as text or as it is.
	Ok(Expr::Block(Box::new(Block(vec![
		Expr::Let(Box::new(SetStatement {
			name: Strand::from("_sql_vector"),
			what: value,
			kind: None,
		})),
		text_or_value(Expr::Param(Param::from("_sql_vector".to_owned())))?,
	]))))
}

/// Convert the items of the projection `fields` of a PostgreSQL `INSERT ... SELECT` that are
/// written to vector columns of the target table, as [`wrap_positional_links`] does the links.
fn wrap_positional_vectors(
	scope: &IdScope<'_>,
	columns: &[String],
	has_wildcard: bool,
	fields: &mut Fields,
) -> Result<(), PlanError> {
	let Fields::Select(items) = fields else {
		return Ok(());
	};
	if has_wildcard {
		return Ok(());
	}
	for (idx, column) in columns.iter().enumerate() {
		let Some(dimensions) = scope.vector_column(column) else {
			continue;
		};
		if scope.link_column(column).is_some() {
			continue;
		}
		if let Some(Field::Single(selector)) = items.get_mut(idx) {
			let value = std::mem::replace(&mut selector.expr, Expr::Literal(Literal::None));
			selector.expr = vector_value(value, Some(dimensions))?;
			if selector.alias.is_none() {
				selector.alias = Some(Idiom::field(column.clone()));
			}
		}
	}
	Ok(())
}

/// `IF type::is_string(value) THEN <the vector the text spells> ELSE value END`, over a value that
/// is cheap to read twice.
///
/// The text is read as pgvector reads it, not as JSON: spaces and brackets are dropped, the rest is
/// split at the commas, and each piece is read as a float.
fn text_or_value(value: Expr) -> Result<Expr, PlanError> {
	let form = crate::vector::text_form()?;
	let text = |s: &str| Expr::Literal(Literal::String(Strand::from(s)));
	let mut inner = value.clone();
	for dropped in [" ", "\t", "\r", "\n", "[", "]"] {
		inner = call("string::replace", vec![inner, text(dropped), text("")]);
	}
	let read = Expr::Prefix {
		op: PrefixOperator::Cast(surrealdb_expr::expr::Kind::Array(
			Box::new(surrealdb_expr::expr::Kind::Float),
			None,
		)),
		expr: Box::new(call("string::split", vec![inner, text(",")])),
	};
	let checked = Expr::IfElse(Box::new(IfelseStatement {
		exprs: vec![(
			call("string::matches", vec![value.clone(), Expr::Literal(Literal::Regex(form))]),
			read,
		)],
		close: Some(Expr::Throw(Box::new(text(crate::vector::MALFORMED)))),
	}));
	Ok(Expr::IfElse(Box::new(IfelseStatement {
		exprs: vec![(call("type::is_string", vec![value.clone()]), checked)],
		close: Some(value),
	})))
}

/// Convert the items of the projection `fields` of an `INSERT ... SELECT` that are written to link
/// columns of the target table: the item at a position is written to the column of `columns` at
/// that position, and keeps its alias, or is named by the column when it has none. With a wildcard
/// in the projection the positions of the items are not those of the columns, and nothing is
/// converted. The items written to other columns are written by name, as the projection names them.
fn wrap_positional_links(
	scope: &IdScope<'_>,
	columns: &[String],
	has_wildcard: bool,
	fields: &mut Fields,
) {
	let Fields::Select(items) = fields else {
		return;
	};
	for (idx, column) in columns.iter().enumerate() {
		let Some(table) = scope.link_column(column) else {
			continue;
		};
		if has_wildcard {
			continue;
		}
		if let Some(Field::Single(selector)) = items.get_mut(idx) {
			let value = std::mem::replace(&mut selector.expr, Expr::Literal(Literal::None));
			selector.expr = link_assignment(&table, value, scope.link_admits_none(column));
			if selector.alias.is_none() {
				selector.alias = Some(Idiom::field(column.clone()));
			}
		}
	}
}

// ============================================================================
// PostgreSQL Expression Lowering
// ============================================================================

/// Lower a PostgreSQL AST expression into a SurrealDB `Expr`.
pub fn lower_pgsql_expr(expr: &surrealdb_pgsql::ast::expr::Expr) -> Result<Expr, PlanError> {
	lower_pgsql_expr_in(expr, None)
}

/// A datetime cast to `date`, which has no time of day: the start of its day.
fn truncated_to_date_expr(target: &str, value: Expr) -> Expr {
	let lower = target.to_ascii_lowercase();
	if lower.split('(').next().unwrap_or(&lower).trim() != "date" {
		return value;
	}
	Expr::FunctionCall(Box::new(FunctionCall {
		receiver: Function::Normal("time::group".to_owned()),
		arguments: vec![value, Expr::Literal(Literal::String(Strand::from("day")))],
	}))
}

/// A text literal that is cast to a datetime, in the form the engine reads (see
/// `functions::normalize_datetime_text`). Any other operand is left as it is.
fn datetime_text_literal(kind: &surrealdb_expr::expr::Kind, value: Expr) -> Expr {
	match (kind, &value) {
		(surrealdb_expr::expr::Kind::Datetime, Expr::Literal(Literal::String(text))) => {
			Expr::Literal(Literal::String(Strand::from(
				crate::lower::functions::normalize_datetime_text(text.as_str()).as_str(),
			)))
		}
		_ => value,
	}
}

/// A call of the registered function `func_def` in an expression a write evaluates.
///
/// A call with fewer or more arguments than the registry allows is refused, as the binder refuses
/// it in a read. A function whose lowering is its own (`crate::lower::functions`) is refused: the
/// write lowering calls the engine function the registry names, which does not answer what the SQL
/// function does. The math and string functions answer NULL for a NULL argument.
fn lower_write_function(
	dialect: SqlDialect,
	func_def: &crate::functions::SqlFunctionDef,
	fn_name: &str,
	mapped: &str,
	lowered_args: Vec<Expr>,
) -> Result<Expr, PlanError> {
	let got = lowered_args.len();
	if got < func_def.min_args {
		return Err(PlanError::FunctionArgumentCountMismatch {
			name: fn_name.to_owned(),
			expected: format!("at least {}", func_def.min_args),
			got,
		});
	}
	if let Some(max_args) = func_def.max_args
		&& got > max_args
	{
		return Err(PlanError::FunctionArgumentCountMismatch {
			name: fn_name.to_owned(),
			expected: format!("at most {max_args}"),
			got,
		});
	}
	if func_def.name == "uuid" {
		// The registry declares text, and a string field takes no uuid value.
		return Ok(Expr::FunctionCall(Box::new(FunctionCall {
			receiver: Function::Normal("type::string".to_owned()),
			arguments: vec![Expr::FunctionCall(Box::new(FunctionCall {
				receiver: Function::Normal("rand::uuid::v4".to_owned()),
				arguments: Vec::new(),
			}))],
		})));
	}
	if crate::lower::functions::has_own_lowering(func_def.name, dialect, lowered_args.len()) {
		return Err(PlanError::UnsupportedExpression(format!("{fn_name} in a write statement")));
	}
	if matches!(func_def.name, "current_date" | "curdate") {
		return Ok(crate::lower::functions::today_expr());
	}
	let guarded = matches!(mapped, "crypto::md5" | "crypto::sha1")
		|| (dialect == SqlDialect::Postgres
			&& (mapped.starts_with("math::")
				|| matches!(
					mapped,
					"string::lowercase"
						| "string::uppercase"
						| "string::len" | "string::reverse"
						| "string::replace"
						| "string::repeat"
				)));
	let numeric =
		|arg: &Expr| matches!(arg, Expr::Literal(Literal::Integer(_) | Literal::Float(_)));
	let present = |arg: &Expr| matches!(arg, Expr::Literal(literal) if !matches!(literal, Literal::Null | Literal::None));
	if func_def.is_scalar()
		&& guarded
		&& !lowered_args.is_empty()
		&& !(mapped.starts_with("math::") && lowered_args.iter().all(numeric))
		&& (mapped.starts_with("math::") || !lowered_args.iter().all(present))
	{
		return Ok(sql_null::strict_call(mapped, lowered_args));
	}
	Ok(Expr::FunctionCall(Box::new(FunctionCall {
		receiver: Function::Normal(mapped.to_string()),
		arguments: lowered_args,
	})))
}

/// Lower a PostgreSQL AST expression into a SurrealDB `Expr`, reading the `id` column of the
/// statement's target table (`scope`) as the key of the record id, and a link column of it as the
/// key of its link.
fn lower_pgsql_expr_in(
	expr: &surrealdb_pgsql::ast::expr::Expr,
	scope: Option<&IdScope<'_>>,
) -> Result<Expr, PlanError> {
	lower_pgsql_expr_as(expr, scope, false)
}

/// Lower a PostgreSQL AST expression that stands alone (an assigned value, an item of a projection,
/// the operand of `IS NULL`, an operand handed to `type::record`): as [`lower_pgsql_expr_in`],
/// except that a link column of the target table is the link itself.
fn lower_pgsql_stored_in(
	expr: &surrealdb_pgsql::ast::expr::Expr,
	scope: Option<&IdScope<'_>>,
) -> Result<Expr, PlanError> {
	lower_pgsql_expr_as(expr, scope, true)
}

/// Lower a PostgreSQL AST expression. `stored` says that it stands alone, which only a column
/// reference, bare or parenthesised, reads; every operand of a larger expression is lowered as
/// [`lower_pgsql_expr_in`] does.
fn lower_pgsql_expr_as(
	expr: &surrealdb_pgsql::ast::expr::Expr,
	scope: Option<&IdScope<'_>>,
	stored: bool,
) -> Result<Expr, PlanError> {
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

			// The `*` of a relation selects columns; no value is read from it.
			if let [qualifier @ .., star] = col.fields.as_slice()
				&& !qualifier.is_empty()
				&& star == "*"
			{
				return Err(PlanError::UnsupportedExpression(format!(
					"{}.* is supported only in the select list of a SELECT statement",
					qualifier.join(".")
				)));
			}

			// Check for EXCLUDED.col in ON CONFLICT DO UPDATE
			if col.fields.len() == 2 && col.fields[0].eq_ignore_ascii_case("EXCLUDED") {
				return Ok(Expr::Idiom(Idiom(vec![
					Part::Start(Expr::Param(Param::from(
						scope.map_or("input", |scope| scope.excluded).to_string(),
					))),
					Part::Field(Strand::from(col.fields[1].as_str())),
				])));
			}

			if let Some(lowered) = scope.and_then(|scope| scope.column(&col.fields, stored)) {
				return Ok(lowered);
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
			let inner = lower_pgsql_expr_in(expr, scope)?;
			let prefix_op = match op {
				surrealdb_pgsql::ast::op::UnaryOperator::Plus => PrefixOperator::Positive,
				surrealdb_pgsql::ast::op::UnaryOperator::Minus => PrefixOperator::Negate,
				surrealdb_pgsql::ast::op::UnaryOperator::Not => PrefixOperator::Not,
				surrealdb_pgsql::ast::op::UnaryOperator::BitNot
				| surrealdb_pgsql::ast::op::UnaryOperator::Custom(_) => {
					return Err(PlanError::UnsupportedExpression(format!("Unary operator {op:?}")));
				}
			};
			Ok(sql_null::unary(prefix_op, inner))
		}

		surrealdb_pgsql::ast::expr::Expr::BinaryOp {
			left,
			op,
			right,
			..
		} => {
			if let Some(scope) = scope.filter(|scope| !scope.deleted_row)
				&& let Some(comparison) = pgsql_link_comparison(left, op, right, scope)?
			{
				return Ok(sql_null::guard(comparison));
			}
			if let Some(scope) = scope.filter(|scope| !scope.deleted_row)
				&& let Some(comparison) = pgsql_id_comparison(left, op, right, scope)?
			{
				return Ok(sql_null::guard(comparison));
			}
			let l = lower_pgsql_expr_in(left, scope)?;
			let r = lower_pgsql_expr_in(right, scope)?;
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
				surrealdb_pgsql::ast::op::BinaryOperator::Concat
				| surrealdb_pgsql::ast::op::BinaryOperator::Like
				| surrealdb_pgsql::ast::op::BinaryOperator::NotLike
				| surrealdb_pgsql::ast::op::BinaryOperator::ILike
				| surrealdb_pgsql::ast::op::BinaryOperator::NotILike
				| surrealdb_pgsql::ast::op::BinaryOperator::RegexMatch
				| surrealdb_pgsql::ast::op::BinaryOperator::RegexMatchCi
				| surrealdb_pgsql::ast::op::BinaryOperator::Arrow
				| surrealdb_pgsql::ast::op::BinaryOperator::LongArrow
				| surrealdb_pgsql::ast::op::BinaryOperator::HashLongArrow
				| surrealdb_pgsql::ast::op::BinaryOperator::Contains
				| surrealdb_pgsql::ast::op::BinaryOperator::Exp
				| surrealdb_pgsql::ast::op::BinaryOperator::BitAnd
				| surrealdb_pgsql::ast::op::BinaryOperator::BitOr
				| surrealdb_pgsql::ast::op::BinaryOperator::BitXor
				| surrealdb_pgsql::ast::op::BinaryOperator::ShiftLeft
				| surrealdb_pgsql::ast::op::BinaryOperator::ShiftRight
				| surrealdb_pgsql::ast::op::BinaryOperator::SimilarTo
				| surrealdb_pgsql::ast::op::BinaryOperator::NotSimilarTo
				| surrealdb_pgsql::ast::op::BinaryOperator::RegexNotMatch
				| surrealdb_pgsql::ast::op::BinaryOperator::RegexNotMatchCi
				| surrealdb_pgsql::ast::op::BinaryOperator::HashArrow
				| surrealdb_pgsql::ast::op::BinaryOperator::ContainedBy => {
					return Err(PlanError::UnsupportedExpression(format!(
						"Binary operator {op:?}"
					)));
				}
				surrealdb_pgsql::ast::op::BinaryOperator::Custom(custom)
					if crate::vector::VectorMetric::from_operator(custom).is_some() =>
				{
					return Err(PlanError::UnsupportedExpression(format!(
						"the {custom} operator in a write"
					)));
				}
				surrealdb_pgsql::ast::op::BinaryOperator::Custom(_) => {
					return Err(PlanError::UnsupportedExpression(format!(
						"Binary operator {op:?}"
					)));
				}
			};
			Ok(sql_null::binary(l, bin_op, r))
		}

		surrealdb_pgsql::ast::expr::Expr::TypeCast {
			expr,
			data_type,
			..
		} => {
			let kind = crate::lower::ddl::map_pgsql_type(data_type);
			let value = datetime_text_literal(&kind, lower_pgsql_expr_in(expr, scope)?);
			if let Some(dimensions) = crate::lower::vector_cast_target(data_type)? {
				return Ok(sql_null::unary(
					PrefixOperator::Cast(kind),
					vector_value(value, dimensions)?,
				));
			}
			if matches!(kind, surrealdb_expr::expr::Kind::Any) {
				Ok(value)
			} else {
				let target = data_type.name.parts.join(".");
				Ok(truncated_to_date_expr(
					&target,
					sql_null::unary(PrefixOperator::Cast(kind), value),
				))
			}
		}

		surrealdb_pgsql::ast::expr::Expr::FunctionCall {
			name,
			args,
			..
		} => {
			let fn_name =
				if name.parts.len() == 2 && name.parts[0].eq_ignore_ascii_case("pg_catalog") {
					name.parts[1].to_ascii_lowercase()
				} else {
					name.parts.last().map(|s| s.to_ascii_lowercase()).unwrap_or_default()
				};
			let func_def = FunctionRegistry::lookup(&fn_name, SqlDialect::Postgres)
				.ok_or_else(|| PlanError::UndefinedFunction(fn_name.clone()))?;
			if crate::vector::is_distance_function(&fn_name) || fn_name == "inner_product" {
				return Err(PlanError::UnsupportedExpression(format!(
					"the {fn_name} function in a write"
				)));
			}
			let mapped = func_def.internal_name.unwrap_or(func_def.name);
			let mut lowered_args = Vec::with_capacity(args.len());
			for a in args {
				lowered_args.push(lower_pgsql_expr_in(a, scope)?);
			}
			lower_write_function(SqlDialect::Postgres, func_def, &fn_name, mapped, lowered_args)
		}

		surrealdb_pgsql::ast::expr::Expr::Grouping {
			expr,
			..
		} => lower_pgsql_expr_as(expr, scope, stored),

		surrealdb_pgsql::ast::expr::Expr::IsNull {
			expr,
			negated,
			..
		} => {
			let inner = lower_pgsql_stored_in(expr, scope)?;
			Ok(sql_null::is_null(inner, *negated))
		}

		surrealdb_pgsql::ast::expr::Expr::Between {
			expr,
			low,
			high,
			negated,
			..
		} => Ok(sql_null::between(
			lower_pgsql_expr_in(expr, scope)?,
			lower_pgsql_expr_in(low, scope)?,
			lower_pgsql_expr_in(high, scope)?,
			*negated,
		)),

		surrealdb_pgsql::ast::expr::Expr::InList {
			expr,
			list,
			negated,
			..
		} => {
			if let Some(scope) = scope.filter(|scope| !scope.deleted_row)
				&& let Some(membership) = pgsql_id_membership(expr, list, *negated, scope)?
			{
				return Ok(sql_null::guard(membership));
			}
			if let Some(scope) = scope.filter(|scope| !scope.deleted_row)
				&& let Some(membership) = pgsql_link_membership(expr, list, *negated, scope)?
			{
				return Ok(sql_null::guard(membership));
			}
			let target = lower_pgsql_expr_in(expr, scope)?;
			let mut items = Vec::with_capacity(list.len());
			for item in list {
				items.push(lower_pgsql_expr_in(item, scope)?);
			}
			Ok(sql_null::membership(target, items, *negated))
		}

		surrealdb_pgsql::ast::expr::Expr::Case {
			branches,
			else_expr,
			..
		} => {
			let mut exprs = Vec::with_capacity(branches.len());
			for b in branches {
				let cond = lower_pgsql_expr_in(&b.when, scope)?;
				let then = lower_pgsql_expr_in(&b.then, scope)?;
				exprs.push((cond, then));
			}
			let close = match else_expr {
				Some(e) => Some(lower_pgsql_expr_in(e, scope)?),
				None => None,
			};
			Ok(Expr::IfElse(Box::new(IfelseStatement {
				exprs,
				close,
			})))
		}

		other @ (surrealdb_pgsql::ast::expr::Expr::IsDistinctFrom {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::Subquery {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::Exists {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::InSubquery {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::ArraySubquery {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::Quantified {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::QuantifiedArray {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::Subscript {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::Slice {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::IsTrue {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::IsFalse {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::IsUnknown {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::Collate {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::ArrayConstructor {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::Row {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::IsBound {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::JsonObject {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::JsonArray {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::JsonExists {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::JsonValue {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::JsonQuery {
			..
		}) => Err(PlanError::UnsupportedExpression(format!("{other:?}"))),
	}
}

/// Lower a WHERE predicate, retaining indexable equality conjuncts whose raw truth agrees with SQL.
fn lower_pgsql_predicate(
	expr: &surrealdb_pgsql::ast::expr::Expr,
	scope: Option<&IdScope<'_>>,
) -> Result<Expr, PlanError> {
	use surrealdb_pgsql::ast::expr::Expr as PgExpr;
	use surrealdb_pgsql::ast::literal::Literal as PgLiteral;
	use surrealdb_pgsql::ast::op::BinaryOperator as PgOp;
	match expr {
		PgExpr::Grouping {
			expr,
			..
		} => lower_pgsql_predicate(expr, scope),
		PgExpr::BinaryOp {
			left,
			op: PgOp::And,
			right,
			..
		} => Ok(sql_null::raw(
			lower_pgsql_predicate(left, scope)?,
			BinaryOperator::And,
			lower_pgsql_predicate(right, scope)?,
		)),
		PgExpr::BinaryOp {
			left,
			op: PgOp::Eq,
			right,
			..
		} => {
			if let Some(scope) = scope
				&& let Some(comparison) = pgsql_id_comparison(left, &PgOp::Eq, right, scope)?
			{
				return Ok(comparison);
			}
			let nonnull_literal = |value: &PgExpr| matches!(value, PgExpr::Literal { lit, .. } if !matches!(lit, PgLiteral::Null));
			if nonnull_literal(left) || nonnull_literal(right) {
				if let Some(scope) = scope
					&& let Some(comparison) = pgsql_link_comparison(left, &PgOp::Eq, right, scope)?
				{
					return Ok(comparison);
				}
				return Ok(sql_null::raw(
					lower_pgsql_expr_in(left, scope)?,
					BinaryOperator::Equal,
					lower_pgsql_expr_in(right, scope)?,
				));
			}
			lower_pgsql_expr_in(expr, scope)
		}
		PgExpr::InList {
			expr: value,
			list,
			negated: false,
			..
		} => {
			if let Some(scope) = scope
				&& let Some(membership) = pgsql_id_membership(value, list, false, scope)?
			{
				return Ok(membership);
			}
			if list.iter().all(
				|item| matches!(item, PgExpr::Literal { lit, .. } if !matches!(lit, PgLiteral::Null)),
			) && let Some(scope) = scope
				&& let Some(membership) = pgsql_link_membership(value, list, false, scope)?
			{
				return Ok(membership);
			}
			lower_pgsql_expr_in(expr, scope)
		}
		PgExpr::Literal {
			..
		}
		| PgExpr::ColumnRef(_)
		| PgExpr::Param {
			..
		}
		| PgExpr::UnaryOp {
			..
		}
		| PgExpr::BinaryOp {
			..
		}
		| PgExpr::TypeCast {
			..
		}
		| PgExpr::FunctionCall {
			..
		}
		| PgExpr::Subscript {
			..
		}
		| PgExpr::Slice {
			..
		}
		| PgExpr::Between {
			..
		}
		| PgExpr::InList {
			..
		}
		| PgExpr::IsNull {
			..
		}
		| PgExpr::IsTrue {
			..
		}
		| PgExpr::IsFalse {
			..
		}
		| PgExpr::IsUnknown {
			..
		}
		| PgExpr::IsDistinctFrom {
			..
		}
		| PgExpr::Case {
			..
		}
		| PgExpr::Collate {
			..
		}
		| PgExpr::ArrayConstructor {
			..
		}
		| PgExpr::Row {
			..
		}
		| PgExpr::IsBound {
			..
		}
		| PgExpr::JsonObject {
			..
		}
		| PgExpr::JsonArray {
			..
		}
		| PgExpr::JsonExists {
			..
		}
		| PgExpr::JsonValue {
			..
		}
		| PgExpr::JsonQuery {
			..
		}
		| PgExpr::Subquery {
			..
		}
		| PgExpr::Exists {
			..
		}
		| PgExpr::InSubquery {
			..
		}
		| PgExpr::ArraySubquery {
			..
		}
		| PgExpr::Quantified {
			..
		}
		| PgExpr::QuantifiedArray {
			..
		} => lower_pgsql_expr_in(expr, scope),
	}
}

/// The key a PostgreSQL expression names, when it is an integer or string literal or a parameter.
fn pgsql_key_operand(expr: &surrealdb_pgsql::ast::expr::Expr) -> Option<WriteKey> {
	match expr {
		surrealdb_pgsql::ast::expr::Expr::Literal {
			lit: surrealdb_pgsql::ast::literal::Literal::Integer(i),
			..
		} => Some(WriteKey::Int(*i)),
		surrealdb_pgsql::ast::expr::Expr::Literal {
			lit: surrealdb_pgsql::ast::literal::Literal::String(s),
			..
		} if !s.is_empty() => Some(WriteKey::Str(s.clone())),
		surrealdb_pgsql::ast::expr::Expr::Param {
			id,
			..
		} => Some(WriteKey::Param(format!("_{id}"))),
		surrealdb_pgsql::ast::expr::Expr::Literal {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::ColumnRef(_)
		| surrealdb_pgsql::ast::expr::Expr::UnaryOp {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::BinaryOp {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::TypeCast {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::FunctionCall {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::Subscript {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::Slice {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::Between {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::InList {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::IsNull {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::IsTrue {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::IsFalse {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::IsUnknown {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::IsDistinctFrom {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::Case {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::Collate {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::ArrayConstructor {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::Row {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::Grouping {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::IsBound {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::JsonObject {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::JsonArray {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::JsonExists {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::JsonValue {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::JsonQuery {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::Subquery {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::Exists {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::InSubquery {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::ArraySubquery {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::Quantified {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::QuantifiedArray {
			..
		} => None,
	}
}

fn pgsql_is_id(expr: &surrealdb_pgsql::ast::expr::Expr, scope: &IdScope<'_>) -> bool {
	matches!(expr, surrealdb_pgsql::ast::expr::Expr::ColumnRef(col) if scope.reads_key(&col.fields))
}

/// The record of the table that a PostgreSQL expression compared with an `id` names: by a key (an
/// integer or string literal or a parameter) or by the value of a column or an expression. `None`
/// when it is neither: a literal that is no key, or an `id` itself.
fn pgsql_id_operand(
	expr: &surrealdb_pgsql::ast::expr::Expr,
	scope: &IdScope<'_>,
) -> Result<Option<WriteKey>, PlanError> {
	if let Some(key) = pgsql_key_operand(expr) {
		return Ok(Some(key));
	}
	if matches!(expr, surrealdb_pgsql::ast::expr::Expr::Literal { .. }) || pgsql_is_id(expr, scope)
	{
		return Ok(None);
	}
	Ok(Some(WriteKey::Value(lower_pgsql_stored_in(expr, Some(scope))?)))
}

/// Equality or inequality of the target id with a key or link, as a comparison with the record
/// the operand names. The caller adds SQL NULL handling except in a positive lookup conjunct.
fn pgsql_id_comparison(
	left: &surrealdb_pgsql::ast::expr::Expr,
	op: &surrealdb_pgsql::ast::op::BinaryOperator,
	right: &surrealdb_pgsql::ast::expr::Expr,
	scope: &IdScope<'_>,
) -> Result<Option<Expr>, PlanError> {
	let bin_op = match op {
		surrealdb_pgsql::ast::op::BinaryOperator::Eq => BinaryOperator::Equal,
		surrealdb_pgsql::ast::op::BinaryOperator::NotEq => BinaryOperator::NotEqual,
		surrealdb_pgsql::ast::op::BinaryOperator::Add
		| surrealdb_pgsql::ast::op::BinaryOperator::Sub
		| surrealdb_pgsql::ast::op::BinaryOperator::Mul
		| surrealdb_pgsql::ast::op::BinaryOperator::Div
		| surrealdb_pgsql::ast::op::BinaryOperator::Mod
		| surrealdb_pgsql::ast::op::BinaryOperator::Exp
		| surrealdb_pgsql::ast::op::BinaryOperator::BitAnd
		| surrealdb_pgsql::ast::op::BinaryOperator::BitOr
		| surrealdb_pgsql::ast::op::BinaryOperator::BitXor
		| surrealdb_pgsql::ast::op::BinaryOperator::ShiftLeft
		| surrealdb_pgsql::ast::op::BinaryOperator::ShiftRight
		| surrealdb_pgsql::ast::op::BinaryOperator::And
		| surrealdb_pgsql::ast::op::BinaryOperator::Or
		| surrealdb_pgsql::ast::op::BinaryOperator::Lt
		| surrealdb_pgsql::ast::op::BinaryOperator::LtEq
		| surrealdb_pgsql::ast::op::BinaryOperator::Gt
		| surrealdb_pgsql::ast::op::BinaryOperator::GtEq
		| surrealdb_pgsql::ast::op::BinaryOperator::Concat
		| surrealdb_pgsql::ast::op::BinaryOperator::Like
		| surrealdb_pgsql::ast::op::BinaryOperator::NotLike
		| surrealdb_pgsql::ast::op::BinaryOperator::ILike
		| surrealdb_pgsql::ast::op::BinaryOperator::NotILike
		| surrealdb_pgsql::ast::op::BinaryOperator::SimilarTo
		| surrealdb_pgsql::ast::op::BinaryOperator::NotSimilarTo
		| surrealdb_pgsql::ast::op::BinaryOperator::RegexMatch
		| surrealdb_pgsql::ast::op::BinaryOperator::RegexMatchCi
		| surrealdb_pgsql::ast::op::BinaryOperator::RegexNotMatch
		| surrealdb_pgsql::ast::op::BinaryOperator::RegexNotMatchCi
		| surrealdb_pgsql::ast::op::BinaryOperator::Arrow
		| surrealdb_pgsql::ast::op::BinaryOperator::LongArrow
		| surrealdb_pgsql::ast::op::BinaryOperator::HashArrow
		| surrealdb_pgsql::ast::op::BinaryOperator::HashLongArrow
		| surrealdb_pgsql::ast::op::BinaryOperator::Contains
		| surrealdb_pgsql::ast::op::BinaryOperator::ContainedBy
		| surrealdb_pgsql::ast::op::BinaryOperator::Custom(_) => return Ok(None),
	};
	let other = match (pgsql_is_id(left, scope), pgsql_is_id(right, scope)) {
		(true, false) => right,
		(false, true) => left,
		_ => return Ok(None),
	};

	if pgsql_id_operand(other, scope)?.is_none() {
		return Ok(None);
	}
	let value = lower_pgsql_stored_in(other, Some(scope))?;
	let record = link_comparand_with(scope, &scope.table, value, pgsql_column_fields(other), true);
	Ok(Some(sql_null::raw(stored_id(), bin_op, record)))
}

/// IN or NOT IN over the target id as membership of the records the operands name. The caller
/// retains the raw form only for a positive lookup conjunct and otherwise adds SQL NULL handling.
fn pgsql_id_membership(
	expr: &surrealdb_pgsql::ast::expr::Expr,
	list: &[surrealdb_pgsql::ast::expr::Expr],
	negated: bool,
	scope: &IdScope<'_>,
) -> Result<Option<Expr>, PlanError> {
	if !pgsql_is_id(expr, scope) || list.is_empty() {
		return Ok(None);
	}

	let mut records = Vec::with_capacity(list.len());
	for member in list {
		if pgsql_id_operand(member, scope)?.is_none() {
			return Ok(None);
		}
		let value = lower_pgsql_stored_in(member, Some(scope))?;
		records.push(link_comparand_with(
			scope,
			&scope.table,
			value,
			pgsql_column_fields(member),
			true,
		));
	}
	Ok(Some(link_in("id".to_string(), records, negated)))
}

/// The fields of the column reference `expr`, when it is one.
fn pgsql_column_fields(expr: &surrealdb_pgsql::ast::expr::Expr) -> Option<&[String]> {
	match expr {
		surrealdb_pgsql::ast::expr::Expr::ColumnRef(col) => Some(&col.fields),
		surrealdb_pgsql::ast::expr::Expr::Literal {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::Param {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::UnaryOp {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::BinaryOp {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::TypeCast {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::FunctionCall {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::Subscript {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::Slice {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::Between {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::InList {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::IsNull {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::IsTrue {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::IsFalse {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::IsUnknown {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::IsDistinctFrom {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::Case {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::Collate {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::ArrayConstructor {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::Row {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::Grouping {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::IsBound {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::JsonObject {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::JsonArray {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::JsonExists {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::JsonValue {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::JsonQuery {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::Subquery {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::Exists {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::InSubquery {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::ArraySubquery {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::Quantified {
			..
		}
		| surrealdb_pgsql::ast::expr::Expr::QuantifiedArray {
			..
		} => None,
	}
}

/// `<link column> = <value>` or `<value> = <link column>` (and `<>`) as a comparison of the stored
/// link with the record the value names. `None` when neither side is a link column of the target
/// table, and for any other operator.
fn pgsql_link_comparison(
	left: &surrealdb_pgsql::ast::expr::Expr,
	op: &surrealdb_pgsql::ast::op::BinaryOperator,
	right: &surrealdb_pgsql::ast::expr::Expr,
	scope: &IdScope<'_>,
) -> Result<Option<Expr>, PlanError> {
	let bin_op = match op {
		surrealdb_pgsql::ast::op::BinaryOperator::Eq => BinaryOperator::Equal,
		surrealdb_pgsql::ast::op::BinaryOperator::NotEq => BinaryOperator::NotEqual,
		surrealdb_pgsql::ast::op::BinaryOperator::Add
		| surrealdb_pgsql::ast::op::BinaryOperator::Sub
		| surrealdb_pgsql::ast::op::BinaryOperator::Mul
		| surrealdb_pgsql::ast::op::BinaryOperator::Div
		| surrealdb_pgsql::ast::op::BinaryOperator::Mod
		| surrealdb_pgsql::ast::op::BinaryOperator::Exp
		| surrealdb_pgsql::ast::op::BinaryOperator::BitAnd
		| surrealdb_pgsql::ast::op::BinaryOperator::BitOr
		| surrealdb_pgsql::ast::op::BinaryOperator::BitXor
		| surrealdb_pgsql::ast::op::BinaryOperator::ShiftLeft
		| surrealdb_pgsql::ast::op::BinaryOperator::ShiftRight
		| surrealdb_pgsql::ast::op::BinaryOperator::And
		| surrealdb_pgsql::ast::op::BinaryOperator::Or
		| surrealdb_pgsql::ast::op::BinaryOperator::Lt
		| surrealdb_pgsql::ast::op::BinaryOperator::LtEq
		| surrealdb_pgsql::ast::op::BinaryOperator::Gt
		| surrealdb_pgsql::ast::op::BinaryOperator::GtEq
		| surrealdb_pgsql::ast::op::BinaryOperator::Concat
		| surrealdb_pgsql::ast::op::BinaryOperator::Like
		| surrealdb_pgsql::ast::op::BinaryOperator::NotLike
		| surrealdb_pgsql::ast::op::BinaryOperator::ILike
		| surrealdb_pgsql::ast::op::BinaryOperator::NotILike
		| surrealdb_pgsql::ast::op::BinaryOperator::SimilarTo
		| surrealdb_pgsql::ast::op::BinaryOperator::NotSimilarTo
		| surrealdb_pgsql::ast::op::BinaryOperator::RegexMatch
		| surrealdb_pgsql::ast::op::BinaryOperator::RegexMatchCi
		| surrealdb_pgsql::ast::op::BinaryOperator::RegexNotMatch
		| surrealdb_pgsql::ast::op::BinaryOperator::RegexNotMatchCi
		| surrealdb_pgsql::ast::op::BinaryOperator::Arrow
		| surrealdb_pgsql::ast::op::BinaryOperator::LongArrow
		| surrealdb_pgsql::ast::op::BinaryOperator::HashArrow
		| surrealdb_pgsql::ast::op::BinaryOperator::HashLongArrow
		| surrealdb_pgsql::ast::op::BinaryOperator::Contains
		| surrealdb_pgsql::ast::op::BinaryOperator::ContainedBy
		| surrealdb_pgsql::ast::op::BinaryOperator::Custom(_) => return Ok(None),
	};
	let (column, table, other) = if let Some((column, table)) =
		pgsql_column_fields(left).and_then(|fields| scope.link_of(fields))
	{
		(column, table, right)
	} else if let Some((column, table)) =
		pgsql_column_fields(right).and_then(|fields| scope.link_of(fields))
	{
		(column, table, left)
	} else {
		return Ok(None);
	};
	let lowered = lower_pgsql_stored_in(other, Some(scope))?;
	let record = link_comparand_with(scope, &table, lowered, pgsql_column_fields(other), true);
	Ok(Some(link_compare(column, bin_op, record)))
}

/// `<link column> IN (<values>)` as a membership of the records the values name, and `NOT IN`.
fn pgsql_link_membership(
	expr: &surrealdb_pgsql::ast::expr::Expr,
	list: &[surrealdb_pgsql::ast::expr::Expr],
	negated: bool,
	scope: &IdScope<'_>,
) -> Result<Option<Expr>, PlanError> {
	let Some((column, table)) = pgsql_column_fields(expr).and_then(|fields| scope.link_of(fields))
	else {
		return Ok(None);
	};
	if list.is_empty() {
		return Ok(None);
	}
	let mut records = Vec::with_capacity(list.len());
	for member in list {
		let lowered = lower_pgsql_stored_in(member, Some(scope))?;
		records.push(link_comparand_with(
			scope,
			&table,
			lowered,
			pgsql_column_fields(member),
			true,
		));
	}
	Ok(Some(link_in(column, records, negated)))
}

fn is_wildcard_target(target: &surrealdb_pgsql::ast::stmt::ResTarget) -> bool {
	if let surrealdb_pgsql::ast::expr::Expr::ColumnRef(col) = &target.expr {
		col.fields.len() == 1 && col.fields[0] == "*"
	} else {
		false
	}
}

/// The qualifier of a target that is the `*` of a relation (`s.*`, `schema.s.*`), as the dotted
/// name the relation is read by.
fn relation_star_qualifier(target: &surrealdb_pgsql::ast::stmt::ResTarget) -> Option<String> {
	if let surrealdb_pgsql::ast::expr::Expr::ColumnRef(col) = &target.expr
		&& let [qualifier @ .., star] = col.fields.as_slice()
		&& !qualifier.is_empty()
		&& star == "*"
	{
		Some(qualifier.join("."))
	} else {
		None
	}
}

fn lower_pgsql_returning(
	targets: &[surrealdb_pgsql::ast::stmt::ResTarget],
	scope: &IdScope<'_>,
) -> Result<Output, PlanError> {
	if scope.deleted_row && targets.iter().any(is_wildcard_target) {
		if let [_] = targets {
			return Ok(scope.echo());
		}
		return Err(PlanError::UnsupportedExpression(
			"RETURNING * together with other expressions in DELETE".into(),
		));
	}
	let mut names = std::collections::HashSet::new();
	let mut targets = targets.to_vec();
	for target in &mut targets {
		if is_wildcard_target(target) {
			continue;
		}
		let name = crate::descriptor::pgsql::returning_name(target);
		if !names.insert(name.clone()) {
			return Err(PlanError::UnsupportedExpression(
				"duplicate RETURNING column names; use distinct aliases".to_string(),
			));
		}
		if target.alias.is_none()
			&& !matches!(target.expr, surrealdb_pgsql::ast::expr::Expr::ColumnRef(_))
		{
			target.alias = Some(name);
		}
	}

	Ok(Output::Fields(lower_pgsql_targets(&targets, Some(scope))?))
}

/// Lower a projection list. An unaliased `id` column keeps the name `id` when it is read as a
/// key, an unaliased column of a deleted row keeps its own name, and a wildcard yields the key as
/// its `id`.
fn lower_pgsql_targets(
	targets: &[surrealdb_pgsql::ast::stmt::ResTarget],
	scope: Option<&IdScope<'_>>,
) -> Result<Fields, PlanError> {
	let mut fields = Vec::with_capacity(targets.len());
	for item in targets {
		if is_wildcard_target(item) {
			fields.push(Field::All);
		} else if let Some(qualifier) = relation_star_qualifier(item) {
			// The one table the projection reads is read by every name it can be qualified by, and
			// its `*` selects what a bare `*` does.
			match scope {
				Some(scope) if scope.is_qualifier(&qualifier) => fields.push(Field::All),
				Some(_) => return Err(PlanError::MissingFromClauseEntry(qualifier)),
				None => {
					return Err(PlanError::UnsupportedExpression(format!(
						"{qualifier}.* in a projection that does not read one table"
					)));
				}
			}
		} else {
			let lowered_expr = lower_pgsql_stored_in(&item.expr, scope)?;
			let alias_idiom = match (&item.alias, &item.expr) {
				(Some(a), _) => Some(Idiom::field(a.clone())),
				(None, _) if scope.is_some_and(|scope| pgsql_is_id(&item.expr, scope)) => {
					Some(Idiom::field("id".to_string()))
				}
				(None, surrealdb_pgsql::ast::expr::Expr::ColumnRef(col))
					if scope.is_some_and(|scope| scope.deleted_row) =>
				{
					col.fields.last().map(|name| Idiom::field(name.clone()))
				}
				(None, _) => None,
			};
			fields.push(Field::Single(Selector {
				expr: lowered_expr,
				alias: alias_idiom,
			}));
		}
	}
	if scope.is_some() {
		wildcard_id_as_key(&mut fields);
	}
	Ok(Fields::Select(fields))
}

/// The scope of the `id` column and the link columns of the table `relation` names, which resolves
/// to `table_name` in `schema`.
fn pgsql_scope<'a>(
	relation: &surrealdb_pgsql::ast::expr::QualifiedName,
	schema: Option<String>,
	table_name: &str,
	catalog: &'a dyn BinderCatalog,
) -> IdScope<'a> {
	let mut qualifiers = vec![table_name.to_string(), relation.parts.join(".")];
	if schema.is_none() {
		// A table named without a schema is the one of the default schema.
		qualifiers.push(format!("public.{table_name}"));
	}
	if let Some(last) = relation.parts.last() {
		qualifiers.push(last.clone());
	}
	let qualifiers: Vec<&str> = qualifiers.iter().map(String::as_str).collect();
	IdScope::new(table_name, schema, &qualifiers, false, catalog)
}

/// Lower the `SELECT` that is the source of `INSERT`. `into` is the target of the `INSERT` and its
/// columns when `select` is that source: the item at a position of the projection is written to the
/// column at that position (see [`wrap_positional_links`]).
fn lower_pgsql_select_stmt(
	select: &surrealdb_pgsql::ast::stmt::SelectStmt,
	catalog: &dyn BinderCatalog,
	into: Option<(&IdScope<'_>, &[String])>,
) -> Result<SelectStatement, PlanError> {
	if select.with.is_some()
		|| select.distinct.is_some()
		|| !select.group_by.is_empty()
		|| select.having.is_some()
		|| !select.window.is_empty()
		|| !select.order_by.is_empty()
		|| select.limit.is_some()
		|| select.offset.is_some()
		|| select.set_op.is_some()
		|| select.from.len() > 1
	{
		return Err(PlanError::UnsupportedStatement(
			"INSERT ... SELECT with complex SELECT clauses is not supported".into(),
		));
	}

	let mut what = Vec::new();
	let mut scope = None;
	for t in &select.from {
		match t {
			surrealdb_pgsql::ast::stmt::TableRef::Table {
				name,
				alias,
				..
			} => {
				let (schema, t_name) =
					crate::namespace::resolve_pgsql_table_name(&name.parts, Some(catalog))?;
				if select.from.len() == 1 {
					let mut id_scope = pgsql_scope(name, schema, &t_name, catalog);
					if let Some(alias) = alias {
						id_scope.qualifiers.push(alias.name.clone());
					}
					scope = Some(id_scope);
				}
				what.push(Expr::Table(TableName::from(t_name)));
			}
			surrealdb_pgsql::ast::stmt::TableRef::Subquery {
				..
			}
			| surrealdb_pgsql::ast::stmt::TableRef::Join {
				..
			}
			| surrealdb_pgsql::ast::stmt::TableRef::Function {
				..
			}
			| surrealdb_pgsql::ast::stmt::TableRef::GraphTable {
				..
			}
			| surrealdb_pgsql::ast::stmt::TableRef::JsonTable(_) => {
				return Err(PlanError::UnsupportedStatement(
					"INSERT ... SELECT over a join or subquery is not supported".into(),
				));
			}
		}
	}

	// The positions of the items next to a `*` are not those of the columns of the target, and the
	// columns the `*` stands for are not known.
	if select.targets.len() > 1
		&& select.targets.iter().any(|t| relation_star_qualifier(t).is_some())
	{
		return Err(PlanError::UnsupportedStatement(
			"an INSERT ... SELECT that selects the * of a table together with another item".into(),
		));
	}

	let mut fields = lower_pgsql_targets(&select.targets, scope.as_ref())?;
	if let Some((target, columns)) = into {
		let has_wildcard = select
			.targets
			.iter()
			.any(|target| is_wildcard_target(target) || relation_star_qualifier(target).is_some());
		wrap_positional_links(target, columns, has_wildcard, &mut fields);
		wrap_positional_vectors(target, columns, has_wildcard, &mut fields)?;
	}

	let cond = match &select.where_clause {
		Some(w) => Some(Cond(lower_pgsql_predicate(w, scope.as_ref())?)),
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
///
/// `catalog` says which columns of the target table hold record links: a value written to one is
/// converted by [`link_value`].
pub fn lower_pgsql_insert(
	insert: &surrealdb_pgsql::ast::stmt::InsertStmt,
	catalog: &dyn BinderCatalog,
) -> Result<Expr, PlanError> {
	let (schema, table_name) =
		crate::namespace::resolve_pgsql_table_name(&insert.relation.parts, Some(catalog))?;
	let mut scope = pgsql_scope(&insert.relation, schema, &table_name, catalog);
	if let Some(alias) = &insert.alias {
		scope.qualifiers.push(alias.clone());
	}
	let into = Some(Expr::Table(TableName::from(table_name)));

	let mut ignore = false;
	let mut update = None;

	if let Some(on_conflict) = &insert.on_conflict {
		if on_conflict.target_where.is_some() {
			return Err(PlanError::UnsupportedStatement(
				"ON CONFLICT with an index predicate".into(),
			));
		}
		if on_conflict.where_clause.is_some() {
			return lower_pgsql_conditional_upsert(insert, on_conflict, scope);
		}
		match &on_conflict.action {
			surrealdb_pgsql::ast::stmt::OnConflictAction::DoNothing => {
				ignore = true;
			}
			surrealdb_pgsql::ast::stmt::OnConflictAction::DoUpdate(assignments) => {
				let mut update_assignments = Vec::with_capacity(assignments.len());
				for assign in assignments {
					let val = lower_pgsql_stored_in(&assign.value, Some(&scope))?;
					update_assignments.push(Assignment {
						place: Idiom::field(assign.column.clone()),
						operator: AssignOperator::Assign,
						value: assigned_pgsql_value(&scope, &assign.column, val)?,
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
			let sel =
				lower_pgsql_select_stmt(select_stmt, catalog, Some((&scope, &insert.columns)))?;
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
					let lowered_val =
						assigned_pgsql_value(&scope, &col_name, lower_pgsql_expr(val)?)?;
					row_tuples.push((Idiom::field(col_name), lowered_val));
				}
				values_rows.push(row_tuples);
			}
			Data::ValuesExpression(values_rows)
		}
	};

	let output = match insert.returning.as_deref() {
		Some(targets) => lower_pgsql_returning(targets, &scope)?,
		None => scope.echo(),
	};

	Ok(Expr::Insert(Box::new(InsertStatement {
		into,
		data,
		ignore,
		update,
		output: Some(output),
		timeout: Expr::Literal(Literal::None),
		relation: false,
	})))
}

/// Lower `INSERT ... ON CONFLICT (target) DO UPDATE SET ... WHERE cond` of one `VALUES` row.
///
/// `ON DUPLICATE KEY UPDATE` cannot leave a conflicting row alone, so the statement is a block
/// that looks the conflicting record up by the conflict target and then runs one of two writes:
/// the plain `INSERT` when no record conflicts, or an `UPDATE` of that record whose `WHERE`
/// carries `cond`. A row for which `cond` is not true is neither written nor returned. The
/// proposed row is evaluated once, into `$excluded`; the lookup, the inserted row and
/// `EXCLUDED.col` all read it from there.
fn lower_pgsql_conditional_upsert(
	insert: &surrealdb_pgsql::ast::stmt::InsertStmt,
	on_conflict: &surrealdb_pgsql::ast::stmt::OnConflictClause,
	mut scope: IdScope<'_>,
) -> Result<Expr, PlanError> {
	use surrealdb_pgsql::ast::expr::{ColumnRef, Expr as PgExpr};
	use surrealdb_pgsql::ast::op::BinaryOperator as PgOp;
	use surrealdb_pgsql::ast::stmt::OnConflictAction;

	let unsupported = |what: &str| {
		Err(PlanError::UnsupportedStatement(format!("ON CONFLICT DO UPDATE ... WHERE {what}")))
	};
	let OnConflictAction::DoUpdate(assignments) = &on_conflict.action else {
		return unsupported("without DO UPDATE");
	};
	let (Some(condition), Some(target), None, Some([row])) =
		(&on_conflict.where_clause, &on_conflict.target, &insert.query, insert.values.as_deref())
	else {
		return unsupported("needs a conflict target and one VALUES row");
	};
	if insert.columns.len() != row.len() {
		return unsupported("needs one value for each listed column");
	}

	// The conflicting record is the one whose target columns equal the proposed ones.
	let mut matched: Option<PgExpr> = None;
	for column in target {
		if !insert.columns.contains(column) {
			return unsupported("needs every target column among the inserted columns");
		}
		let eq = PgExpr::BinaryOp {
			left: Box::new(PgExpr::ColumnRef(ColumnRef {
				fields: vec![column.clone()],
				span: on_conflict.span,
			})),
			op: PgOp::Eq,
			right: Box::new(PgExpr::ColumnRef(ColumnRef {
				fields: vec!["excluded".to_string(), column.clone()],
				span: on_conflict.span,
			})),
			span: on_conflict.span,
		};
		matched = Some(match matched {
			Some(left) => PgExpr::BinaryOp {
				left: Box::new(left),
				op: PgOp::And,
				right: Box::new(eq),
				span: on_conflict.span,
			},
			None => eq,
		});
	}
	let Some(matched) = matched else {
		return unsupported("needs a conflict target");
	};

	let table = TableName::from(scope.table.clone());
	scope.excluded = "excluded";
	let matched = lower_pgsql_predicate(&matched, Some(&scope))?;

	let mut proposed = Vec::with_capacity(row.len());
	let mut values = Vec::with_capacity(row.len());
	for (column, value) in insert.columns.iter().zip(row) {
		let lowered = assigned_pgsql_value(&scope, column, lower_pgsql_expr(value)?)?;
		let read = Expr::Idiom(Idiom(vec![
			Part::Start(Expr::Param(Param::from("excluded".to_string()))),
			Part::Field(Strand::from(column.as_str())),
		]));
		values.push((Idiom::field(column.clone()), read));
		proposed.push(ObjectEntry {
			key: Strand::from(column.as_str()),
			value: lowered,
		});
	}

	let mut update_assignments = Vec::with_capacity(assignments.len());
	for assign in assignments {
		let val = lower_pgsql_stored_in(&assign.value, Some(&scope))?;
		update_assignments.push(Assignment {
			place: Idiom::field(assign.column.clone()),
			operator: AssignOperator::Assign,
			value: assigned_pgsql_value(&scope, &assign.column, val)?,
		});
	}
	let condition = lower_pgsql_predicate(condition, Some(&scope))?;
	let output = match insert.returning.as_deref() {
		Some(targets) => lower_pgsql_returning(targets, &scope)?,
		None => scope.echo(),
	};

	let existing = Expr::Select(Box::new(SelectStatement {
		fields: Fields::Value(Box::new(Selector {
			expr: stored_id(),
			alias: None,
		})),
		omit: Vec::new(),
		only: false,
		what: vec![Expr::Table(table.clone())],
		with: None,
		cond: Some(Cond(matched.clone())),
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
	}));
	let insert_row = Expr::Insert(Box::new(InsertStatement {
		into: Some(Expr::Table(table.clone())),
		data: Data::ValuesExpression(vec![values]),
		ignore: false,
		update: None,
		output: Some(output.clone()),
		timeout: Expr::Literal(Literal::None),
		relation: false,
	}));
	let update_row = Expr::Update(Box::new(UpdateStatement {
		what: vec![Expr::Table(table)],
		data: Some(Data::SetExpression(update_assignments)),
		cond: Some(Cond(sql_null::raw(matched, BinaryOperator::And, condition))),
		output: Some(output),
		..Default::default()
	}));

	let let_value = |name: &str, what: Expr| {
		Expr::Let(Box::new(SetStatement {
			name: Strand::from(name),
			what,
			kind: None,
		}))
	};
	let param = |name: &str| Expr::Param(Param::from(name.to_string()));
	Ok(Expr::Block(Box::new(Block(vec![
		let_value("excluded", Expr::Literal(Literal::Object(proposed))),
		let_value("conflicting", existing),
		Expr::IfElse(Box::new(IfelseStatement {
			exprs: vec![(
				sql_null::raw(
					call("array::len", vec![param("conflicting")]),
					BinaryOperator::Equal,
					Expr::Literal(Literal::Integer(0)),
				),
				insert_row,
			)],
			close: Some(update_row),
		})),
	]))))
}

/// Lower a PostgreSQL `UpdateStmt` into a SurrealDB `Expr::Update`.
///
/// `catalog` says which columns of the target table hold record links: a value assigned to one is
/// converted by [`link_value`], and a comparison of one in the `WHERE` clause names the record of
/// its table. Any other expression reads a link column as the key of its link.
pub fn lower_pgsql_update(
	update: &surrealdb_pgsql::ast::stmt::UpdateStmt,
	catalog: &dyn BinderCatalog,
) -> Result<Expr, PlanError> {
	let (schema, table_name) =
		crate::namespace::resolve_pgsql_table_name(&update.relation.parts, Some(catalog))?;
	let scope = pgsql_scope(&update.relation, schema, &table_name, catalog);
	let what = vec![Expr::Table(TableName::from(table_name))];

	if update.from.as_ref().is_some_and(|tables| !tables.is_empty()) {
		return Err(PlanError::UnsupportedStatement(
			"UPDATE FROM requires join-based write lowering".into(),
		));
	}

	let mut assignments = Vec::with_capacity(update.assignments.len());
	for assign in &update.assignments {
		let val = lower_pgsql_stored_in(&assign.value, Some(&scope))?;
		assignments.push(Assignment {
			place: Idiom::field(assign.column.clone()),
			operator: AssignOperator::Assign,
			value: assigned_pgsql_value(&scope, &assign.column, val)?,
		});
	}

	let cond = match &update.where_clause {
		Some(w) => Some(Cond(lower_pgsql_predicate(w, Some(&scope))?)),
		None => None,
	};

	let output = match update.returning.as_deref() {
		Some(targets) => lower_pgsql_returning(targets, &scope)?,
		None => scope.echo(),
	};

	Ok(Expr::Update(Box::new(UpdateStatement {
		what,
		data: Some(Data::SetExpression(assignments)),
		cond,
		output: Some(output),
		..Default::default()
	})))
}

/// Lower a PostgreSQL `DeleteStmt` into a SurrealDB `Expr::Delete`.
///
/// `catalog` says which columns of the target table hold record links: a comparison of one in the
/// `WHERE` clause names the record of its table, and any other expression reads one as the key of
/// its link.
pub fn lower_pgsql_delete(
	delete: &surrealdb_pgsql::ast::stmt::DeleteStmt,
	catalog: &dyn BinderCatalog,
) -> Result<Expr, PlanError> {
	let (schema, table_name) =
		crate::namespace::resolve_pgsql_table_name(&delete.relation.parts, Some(catalog))?;
	let scope = pgsql_scope(&delete.relation, schema, &table_name, catalog);
	let what = vec![Expr::Table(TableName::from(table_name))];

	if delete.using.as_ref().is_some_and(|tables| !tables.is_empty()) {
		return Err(PlanError::UnsupportedStatement(
			"DELETE USING requires join-based write lowering".into(),
		));
	}

	let cond = match &delete.where_clause {
		Some(w) => Some(Cond(lower_pgsql_predicate(w, Some(&scope))?)),
		None => None,
	};

	let deleted = scope.over_deleted_row();
	let output = match delete.returning.as_deref() {
		Some(targets) => lower_pgsql_returning(targets, &deleted)?,
		None => deleted.echo(),
	};

	Ok(Expr::Delete(Box::new(DeleteStatement {
		what,
		cond,
		output: Some(output),
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
	lower_mysql_expr_in(expr, None)
}

/// Lower a MySQL AST expression into a SurrealDB `Expr`, reading the `id` column of the
/// statement's target table (`scope`) as the key of the record id, and a link column of it as the
/// key of its link.
fn lower_mysql_expr_in(
	expr: &surrealdb_mysql::ast::expr::Expr,
	scope: Option<&IdScope<'_>>,
) -> Result<Expr, PlanError> {
	lower_mysql_expr_as(expr, scope, false)
}

/// Lower a MySQL AST expression that stands alone (an assigned value, an item of a projection, the
/// operand of `IS NULL`, an operand handed to `type::record`): as [`lower_mysql_expr_in`], except
/// that a link column of the target table is the link itself.
fn lower_mysql_stored_in(
	expr: &surrealdb_mysql::ast::expr::Expr,
	scope: Option<&IdScope<'_>>,
) -> Result<Expr, PlanError> {
	lower_mysql_expr_as(expr, scope, true)
}

/// Lower a MySQL AST expression. `stored` says that it stands alone, which only a column reference
/// reads; every operand of a larger expression is lowered as [`lower_mysql_expr_in`] does.
fn lower_mysql_expr_as(
	expr: &surrealdb_mysql::ast::expr::Expr,
	scope: Option<&IdScope<'_>>,
	stored: bool,
) -> Result<Expr, PlanError> {
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
			surrealdb_mysql::ast::literal::Literal::IntroducedString {
				value,
				..
			} => Ok(Expr::Literal(Literal::String(Strand::from(value.as_str())))),
			surrealdb_mysql::ast::literal::Literal::Boolean(b) => {
				Ok(Expr::Literal(Literal::Bool(*b)))
			}
			surrealdb_mysql::ast::literal::Literal::Null => Ok(Expr::Literal(Literal::Null)),
			surrealdb_mysql::ast::literal::Literal::HexString(bytes)
			| surrealdb_mysql::ast::literal::Literal::IntroducedHexString {
				value: bytes,
				..
			}
			| surrealdb_mysql::ast::literal::Literal::BitString(bytes) => Ok(Expr::Literal(
				Literal::String(Strand::from(String::from_utf8_lossy(bytes).as_ref())),
			)),
		},

		surrealdb_mysql::ast::expr::Expr::ColumnRef(col) => {
			if col.fields.len() == 1 && col.fields[0].eq_ignore_ascii_case("DEFAULT") {
				return Ok(Expr::Literal(Literal::None));
			}
			if let Some(lowered) = scope.and_then(|scope| scope.column(&col.fields, stored)) {
				return Ok(lowered);
			}
			let parts: Vec<Part> =
				col.fields.iter().map(|f| Part::Field(Strand::from(f.as_str()))).collect();
			Ok(Expr::Idiom(Idiom(parts)))
		}

		surrealdb_mysql::ast::expr::Expr::Param(idx, _) => {
			Ok(Expr::Param(Param::from(surrealdb_types::positional_param_name(*idx))))
		}

		surrealdb_mysql::ast::expr::Expr::UserVar(name, _) => {
			Ok(Expr::Param(Param::from(name.clone())))
		}

		surrealdb_mysql::ast::expr::Expr::UnaryOp {
			op,
			expr,
			..
		} => {
			let inner = lower_mysql_expr_in(expr, scope)?;
			let prefix_op = match op {
				surrealdb_mysql::ast::operator::UnaryOperator::Plus => PrefixOperator::Positive,
				surrealdb_mysql::ast::operator::UnaryOperator::Minus => PrefixOperator::Negate,
				surrealdb_mysql::ast::operator::UnaryOperator::Not => PrefixOperator::Not,
				surrealdb_mysql::ast::operator::UnaryOperator::BitNot
				| surrealdb_mysql::ast::operator::UnaryOperator::Binary => {
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
			if let Some(scope) = scope.filter(|scope| !scope.deleted_row)
				&& let Some(comparison) = mysql_link_comparison(left, *op, right, scope)?
			{
				return Ok(comparison);
			}
			if let Some(scope) = scope.filter(|scope| !scope.deleted_row)
				&& let Some(comparison) = mysql_id_comparison(left, *op, right, scope)?
			{
				return Ok(comparison);
			}
			let l = lower_mysql_expr_in(left, scope)?;
			let r = lower_mysql_expr_in(right, scope)?;
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
				surrealdb_mysql::ast::operator::BinaryOperator::Concat
				| surrealdb_mysql::ast::operator::BinaryOperator::NullSafeEqual
				| surrealdb_mysql::ast::operator::BinaryOperator::IntDiv
				| surrealdb_mysql::ast::operator::BinaryOperator::Like
				| surrealdb_mysql::ast::operator::BinaryOperator::Regexp
				| surrealdb_mysql::ast::operator::BinaryOperator::Rlike
				| surrealdb_mysql::ast::operator::BinaryOperator::BitAnd
				| surrealdb_mysql::ast::operator::BinaryOperator::BitOr
				| surrealdb_mysql::ast::operator::BinaryOperator::BitXor
				| surrealdb_mysql::ast::operator::BinaryOperator::ShiftLeft
				| surrealdb_mysql::ast::operator::BinaryOperator::ShiftRight
				| surrealdb_mysql::ast::operator::BinaryOperator::Xor
				| surrealdb_mysql::ast::operator::BinaryOperator::Assign => {
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

			let fn_name = name.to_ascii_lowercase();
			let func_def = FunctionRegistry::lookup(&fn_name, SqlDialect::MySql)
				.ok_or_else(|| PlanError::UndefinedFunction(fn_name.clone()))?;
			let mapped = func_def.internal_name.unwrap_or(func_def.name);
			let mut lowered_args = Vec::with_capacity(args.len());
			for a in args {
				lowered_args.push(lower_mysql_expr_in(a, scope)?);
			}
			lower_write_function(SqlDialect::MySql, func_def, &fn_name, mapped, lowered_args)
		}

		surrealdb_mysql::ast::expr::Expr::Cast {
			expr,
			target_type,
			..
		} => {
			let kind = crate::lower::expr::cast_kind(target_type);
			let value = datetime_text_literal(&kind, lower_mysql_expr_in(expr, scope)?);
			if matches!(kind, surrealdb_expr::expr::Kind::Any) {
				Ok(value)
			} else {
				Ok(truncated_to_date_expr(
					target_type,
					sql_null::unary(PrefixOperator::Cast(kind), value),
				))
			}
		}

		surrealdb_mysql::ast::expr::Expr::IsNull {
			expr,
			negated,
			..
		} => {
			let inner = lower_mysql_stored_in(expr, scope)?;
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
			let val = lower_mysql_expr_in(expr, scope)?;
			let l = lower_mysql_expr_in(low, scope)?;
			let h = lower_mysql_expr_in(high, scope)?;

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
			if let Some(scope) = scope.filter(|scope| !scope.deleted_row)
				&& let Some(membership) = mysql_id_membership(expr, list, *negated, scope)?
			{
				return Ok(membership);
			}
			if let Some(scope) = scope.filter(|scope| !scope.deleted_row)
				&& let Some(membership) = mysql_link_membership(expr, list, *negated, scope)?
			{
				return Ok(membership);
			}
			let target = lower_mysql_expr_in(expr, scope)?;
			let mut items = Vec::with_capacity(list.len());
			for item in list {
				items.push(lower_mysql_expr_in(item, scope)?);
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
				let cond = lower_mysql_expr_in(&b.when, scope)?;
				let then = lower_mysql_expr_in(&b.then, scope)?;
				exprs.push((cond, then));
			}
			let close = match else_expr {
				Some(e) => Some(lower_mysql_expr_in(e, scope)?),
				None => None,
			};
			Ok(Expr::IfElse(Box::new(IfelseStatement {
				exprs,
				close,
			})))
		}

		other @ (surrealdb_mysql::ast::expr::Expr::SystemVar {
			..
		}
		| surrealdb_mysql::ast::expr::Expr::Subquery {
			..
		}
		| surrealdb_mysql::ast::expr::Expr::Exists {
			..
		}
		| surrealdb_mysql::ast::expr::Expr::InSubquery {
			..
		}
		| surrealdb_mysql::ast::expr::Expr::Quantified {
			..
		}
		| surrealdb_mysql::ast::expr::Expr::IsTruthValue {
			..
		}
		| surrealdb_mysql::ast::expr::Expr::Collate {
			..
		}
		| surrealdb_mysql::ast::expr::Expr::Interval {
			..
		}) => Err(PlanError::UnsupportedExpression(format!("{other:?}"))),
	}
}

/// The key a MySQL expression names, when it is an integer or string literal or a parameter.
fn mysql_key_operand(expr: &surrealdb_mysql::ast::expr::Expr) -> Option<WriteKey> {
	match expr {
		surrealdb_mysql::ast::expr::Expr::Literal(
			surrealdb_mysql::ast::literal::Literal::Integer(i),
			_,
		) => Some(WriteKey::Int(*i)),
		surrealdb_mysql::ast::expr::Expr::Literal(
			surrealdb_mysql::ast::literal::Literal::String(s),
			_,
		) if !s.is_empty() => Some(WriteKey::Str(s.clone())),
		surrealdb_mysql::ast::expr::Expr::Param(idx, _) => {
			Some(WriteKey::Param(surrealdb_types::positional_param_name(*idx)))
		}
		surrealdb_mysql::ast::expr::Expr::Literal(..)
		| surrealdb_mysql::ast::expr::Expr::ColumnRef(_)
		| surrealdb_mysql::ast::expr::Expr::UserVar(..)
		| surrealdb_mysql::ast::expr::Expr::SystemVar {
			..
		}
		| surrealdb_mysql::ast::expr::Expr::UnaryOp {
			..
		}
		| surrealdb_mysql::ast::expr::Expr::BinaryOp {
			..
		}
		| surrealdb_mysql::ast::expr::Expr::FunctionCall {
			..
		}
		| surrealdb_mysql::ast::expr::Expr::Cast {
			..
		}
		| surrealdb_mysql::ast::expr::Expr::Case {
			..
		}
		| surrealdb_mysql::ast::expr::Expr::InList {
			..
		}
		| surrealdb_mysql::ast::expr::Expr::InSubquery {
			..
		}
		| surrealdb_mysql::ast::expr::Expr::Quantified {
			..
		}
		| surrealdb_mysql::ast::expr::Expr::Between {
			..
		}
		| surrealdb_mysql::ast::expr::Expr::IsNull {
			..
		}
		| surrealdb_mysql::ast::expr::Expr::IsTruthValue {
			..
		}
		| surrealdb_mysql::ast::expr::Expr::Exists {
			..
		}
		| surrealdb_mysql::ast::expr::Expr::Subquery {
			..
		}
		| surrealdb_mysql::ast::expr::Expr::Collate {
			..
		}
		| surrealdb_mysql::ast::expr::Expr::Interval {
			..
		} => None,
	}
}

fn mysql_is_id(expr: &surrealdb_mysql::ast::expr::Expr, scope: &IdScope<'_>) -> bool {
	matches!(expr, surrealdb_mysql::ast::expr::Expr::ColumnRef(col) if scope.reads_key(&col.fields))
}

/// The record of the table that a MySQL expression compared with an `id` names: by a key (an
/// integer or string literal or a parameter) or by the value of a column or an expression. `None`
/// when it is neither: a literal that is no key, or an `id` itself.
fn mysql_id_operand(
	expr: &surrealdb_mysql::ast::expr::Expr,
	scope: &IdScope<'_>,
) -> Result<Option<WriteKey>, PlanError> {
	if let Some(key) = mysql_key_operand(expr) {
		return Ok(Some(key));
	}
	if matches!(expr, surrealdb_mysql::ast::expr::Expr::Literal(..)) || mysql_is_id(expr, scope) {
		return Ok(None);
	}
	Ok(Some(WriteKey::Value(lower_mysql_stored_in(expr, Some(scope))?)))
}

/// Equality or inequality of the target id with a key or link, as a comparison with the record
/// the operand names. The caller adds SQL NULL handling except in a positive lookup conjunct.
fn mysql_id_comparison(
	left: &surrealdb_mysql::ast::expr::Expr,
	op: surrealdb_mysql::ast::operator::BinaryOperator,
	right: &surrealdb_mysql::ast::expr::Expr,
	scope: &IdScope<'_>,
) -> Result<Option<Expr>, PlanError> {
	let bin_op = match op {
		surrealdb_mysql::ast::operator::BinaryOperator::Equal => BinaryOperator::Equal,
		surrealdb_mysql::ast::operator::BinaryOperator::NotEqual => BinaryOperator::NotEqual,
		surrealdb_mysql::ast::operator::BinaryOperator::Add
		| surrealdb_mysql::ast::operator::BinaryOperator::Sub
		| surrealdb_mysql::ast::operator::BinaryOperator::Mul
		| surrealdb_mysql::ast::operator::BinaryOperator::Div
		| surrealdb_mysql::ast::operator::BinaryOperator::IntDiv
		| surrealdb_mysql::ast::operator::BinaryOperator::Mod
		| surrealdb_mysql::ast::operator::BinaryOperator::BitAnd
		| surrealdb_mysql::ast::operator::BinaryOperator::BitOr
		| surrealdb_mysql::ast::operator::BinaryOperator::BitXor
		| surrealdb_mysql::ast::operator::BinaryOperator::ShiftLeft
		| surrealdb_mysql::ast::operator::BinaryOperator::ShiftRight
		| surrealdb_mysql::ast::operator::BinaryOperator::NullSafeEqual
		| surrealdb_mysql::ast::operator::BinaryOperator::Less
		| surrealdb_mysql::ast::operator::BinaryOperator::LessEqual
		| surrealdb_mysql::ast::operator::BinaryOperator::Greater
		| surrealdb_mysql::ast::operator::BinaryOperator::GreaterEqual
		| surrealdb_mysql::ast::operator::BinaryOperator::Like
		| surrealdb_mysql::ast::operator::BinaryOperator::Regexp
		| surrealdb_mysql::ast::operator::BinaryOperator::Rlike
		| surrealdb_mysql::ast::operator::BinaryOperator::And
		| surrealdb_mysql::ast::operator::BinaryOperator::Or
		| surrealdb_mysql::ast::operator::BinaryOperator::Xor
		| surrealdb_mysql::ast::operator::BinaryOperator::Concat
		| surrealdb_mysql::ast::operator::BinaryOperator::Assign => return Ok(None),
	};
	let other = match (mysql_is_id(left, scope), mysql_is_id(right, scope)) {
		(true, false) => right,
		(false, true) => left,
		_ => return Ok(None),
	};
	Ok(match mysql_id_operand(other, scope)? {
		Some(key @ WriteKey::Value(_)) => Some(id_compare(&scope.table, &key, bin_op)),
		Some(key) if bin_op == BinaryOperator::Equal => {
			Some(id_compare(&scope.table, &key, bin_op))
		}
		_ => None,
	})
}

/// IN or NOT IN over the target id as membership of the records the operands name. The caller
/// retains the raw form only for a positive lookup conjunct and otherwise adds SQL NULL handling.
fn mysql_id_membership(
	expr: &surrealdb_mysql::ast::expr::Expr,
	list: &[surrealdb_mysql::ast::expr::Expr],
	negated: bool,
	scope: &IdScope<'_>,
) -> Result<Option<Expr>, PlanError> {
	if !mysql_is_id(expr, scope) || list.is_empty() {
		return Ok(None);
	}
	let mut keys = Vec::with_capacity(list.len());
	for member in list {
		let Some(key) = mysql_id_operand(member, scope)? else {
			return Ok(None);
		};
		keys.push(key);
	}
	if negated && !keys.iter().any(|key| matches!(key, WriteKey::Value(_))) {
		return Ok(None);
	}
	Ok(Some(id_in(&scope.table, &keys, negated)))
}

/// The fields of the column reference `expr`, when it is one.
fn mysql_column_fields(expr: &surrealdb_mysql::ast::expr::Expr) -> Option<&[String]> {
	match expr {
		surrealdb_mysql::ast::expr::Expr::ColumnRef(col) => Some(&col.fields),
		surrealdb_mysql::ast::expr::Expr::Literal(..)
		| surrealdb_mysql::ast::expr::Expr::UserVar(..)
		| surrealdb_mysql::ast::expr::Expr::SystemVar {
			..
		}
		| surrealdb_mysql::ast::expr::Expr::Param(..)
		| surrealdb_mysql::ast::expr::Expr::UnaryOp {
			..
		}
		| surrealdb_mysql::ast::expr::Expr::BinaryOp {
			..
		}
		| surrealdb_mysql::ast::expr::Expr::FunctionCall {
			..
		}
		| surrealdb_mysql::ast::expr::Expr::Cast {
			..
		}
		| surrealdb_mysql::ast::expr::Expr::Case {
			..
		}
		| surrealdb_mysql::ast::expr::Expr::InList {
			..
		}
		| surrealdb_mysql::ast::expr::Expr::InSubquery {
			..
		}
		| surrealdb_mysql::ast::expr::Expr::Quantified {
			..
		}
		| surrealdb_mysql::ast::expr::Expr::Between {
			..
		}
		| surrealdb_mysql::ast::expr::Expr::IsNull {
			..
		}
		| surrealdb_mysql::ast::expr::Expr::IsTruthValue {
			..
		}
		| surrealdb_mysql::ast::expr::Expr::Exists {
			..
		}
		| surrealdb_mysql::ast::expr::Expr::Subquery {
			..
		}
		| surrealdb_mysql::ast::expr::Expr::Collate {
			..
		}
		| surrealdb_mysql::ast::expr::Expr::Interval {
			..
		} => None,
	}
}

/// `<link column> = <value>` or `<value> = <link column>` (and `<>`) as a comparison of the stored
/// link with the record the value names. `None` when neither side is a link column of the target
/// table, and for any other operator.
fn mysql_link_comparison(
	left: &surrealdb_mysql::ast::expr::Expr,
	op: surrealdb_mysql::ast::operator::BinaryOperator,
	right: &surrealdb_mysql::ast::expr::Expr,
	scope: &IdScope<'_>,
) -> Result<Option<Expr>, PlanError> {
	let bin_op = match op {
		surrealdb_mysql::ast::operator::BinaryOperator::Equal => BinaryOperator::Equal,
		surrealdb_mysql::ast::operator::BinaryOperator::NotEqual => BinaryOperator::NotEqual,
		surrealdb_mysql::ast::operator::BinaryOperator::Add
		| surrealdb_mysql::ast::operator::BinaryOperator::Sub
		| surrealdb_mysql::ast::operator::BinaryOperator::Mul
		| surrealdb_mysql::ast::operator::BinaryOperator::Div
		| surrealdb_mysql::ast::operator::BinaryOperator::IntDiv
		| surrealdb_mysql::ast::operator::BinaryOperator::Mod
		| surrealdb_mysql::ast::operator::BinaryOperator::BitAnd
		| surrealdb_mysql::ast::operator::BinaryOperator::BitOr
		| surrealdb_mysql::ast::operator::BinaryOperator::BitXor
		| surrealdb_mysql::ast::operator::BinaryOperator::ShiftLeft
		| surrealdb_mysql::ast::operator::BinaryOperator::ShiftRight
		| surrealdb_mysql::ast::operator::BinaryOperator::NullSafeEqual
		| surrealdb_mysql::ast::operator::BinaryOperator::Less
		| surrealdb_mysql::ast::operator::BinaryOperator::LessEqual
		| surrealdb_mysql::ast::operator::BinaryOperator::Greater
		| surrealdb_mysql::ast::operator::BinaryOperator::GreaterEqual
		| surrealdb_mysql::ast::operator::BinaryOperator::Like
		| surrealdb_mysql::ast::operator::BinaryOperator::Regexp
		| surrealdb_mysql::ast::operator::BinaryOperator::Rlike
		| surrealdb_mysql::ast::operator::BinaryOperator::And
		| surrealdb_mysql::ast::operator::BinaryOperator::Or
		| surrealdb_mysql::ast::operator::BinaryOperator::Xor
		| surrealdb_mysql::ast::operator::BinaryOperator::Concat
		| surrealdb_mysql::ast::operator::BinaryOperator::Assign => return Ok(None),
	};
	let (column, table, other) = if let Some((column, table)) =
		mysql_column_fields(left).and_then(|fields| scope.link_of(fields))
	{
		(column, table, right)
	} else if let Some((column, table)) =
		mysql_column_fields(right).and_then(|fields| scope.link_of(fields))
	{
		(column, table, left)
	} else {
		return Ok(None);
	};
	let lowered = lower_mysql_stored_in(other, Some(scope))?;
	let record = link_comparand(scope, &table, lowered, mysql_column_fields(other));
	Ok(Some(link_compare(column, bin_op, record)))
}

/// `<link column> IN (<values>)` as a membership of the records the values name, and `NOT IN`.
fn mysql_link_membership(
	expr: &surrealdb_mysql::ast::expr::Expr,
	list: &[surrealdb_mysql::ast::expr::Expr],
	negated: bool,
	scope: &IdScope<'_>,
) -> Result<Option<Expr>, PlanError> {
	let Some((column, table)) = mysql_column_fields(expr).and_then(|fields| scope.link_of(fields))
	else {
		return Ok(None);
	};
	if list.is_empty() {
		return Ok(None);
	}
	let mut records = Vec::with_capacity(list.len());
	for member in list {
		let lowered = lower_mysql_stored_in(member, Some(scope))?;
		records.push(link_comparand(scope, &table, lowered, mysql_column_fields(member)));
	}
	Ok(Some(link_in(column, records, negated)))
}

/// The scope of the `id` column and the link columns of the table `name` resolves to in `schema`,
/// read as `alias` when it has one.
fn mysql_name_scope<'a>(
	name: &surrealdb_mysql::ast::stmt::QualifiedName,
	schema: Option<String>,
	table_name: &str,
	alias: Option<&str>,
	catalog: &'a dyn BinderCatalog,
) -> IdScope<'a> {
	let mut qualifiers = vec![table_name.to_string(), name.parts.join(".")];
	if let Some(last) = name.parts.last() {
		qualifiers.push(last.clone());
	}
	if let Some(alias) = alias {
		qualifiers.push(alias.to_string());
	}
	let qualifiers: Vec<&str> = qualifiers.iter().map(String::as_str).collect();
	IdScope::new(table_name, schema, &qualifiers, true, catalog)
}

/// Lower the `SELECT` that is the source of `INSERT` or `REPLACE`. `into` is the target of the
/// statement and its columns: the item at a position of the projection is written to the column at
/// that position (see [`wrap_positional_links`]).
fn lower_mysql_select_stmt(
	select: &surrealdb_mysql::ast::stmt::SelectStmt,
	catalog: &dyn BinderCatalog,
	into: Option<(&IdScope<'_>, &[String])>,
) -> Result<SelectStatement, PlanError> {
	if select.with.is_some()
		|| select.distinct
		|| !select.group_by.is_empty()
		|| select.having.is_some()
		|| !select.window.is_empty()
		|| !select.order_by.is_empty()
		|| select.limit.is_some()
		|| select.set_op.is_some()
	{
		return Err(PlanError::UnsupportedStatement(
			"INSERT ... SELECT with complex SELECT clauses is not supported".into(),
		));
	}

	let (what, scope) = match &select.from {
		None => (Vec::new(), None),
		Some(surrealdb_mysql::ast::stmt::TableRef::Table {
			name,
			alias,
			..
		}) => {
			let (schema, t_name) =
				crate::namespace::resolve_mysql_table_name(&name.parts, Some(catalog))?;
			let scope = mysql_name_scope(name, schema, &t_name, alias.as_deref(), catalog);
			(vec![Expr::Table(TableName::from(t_name))], Some(scope))
		}
		Some(
			surrealdb_mysql::ast::stmt::TableRef::Join {
				..
			}
			| surrealdb_mysql::ast::stmt::TableRef::Subquery {
				..
			},
		) => {
			return Err(PlanError::UnsupportedStatement(
				"INSERT ... SELECT over a join or subquery is not supported".into(),
			));
		}
	};

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
				let lowered = lower_mysql_stored_in(expr, scope.as_ref())?;
				let a = match alias {
					Some(s) => Some(Idiom::field(s.clone())),
					None if scope.as_ref().is_some_and(|scope| mysql_is_id(expr, scope)) => {
						Some(Idiom::field("id".to_string()))
					}
					None => None,
				};
				f_vec.push(Field::Single(Selector {
					expr: lowered,
					alias: a,
				}));
			}
		}
	}
	if scope.is_some() {
		wildcard_id_as_key(&mut f_vec);
	}
	let mut fields = Fields::Select(f_vec);
	if let Some((target, columns)) = into {
		let has_wildcard = select.projection.iter().any(|item| {
			matches!(
				item,
				surrealdb_mysql::ast::stmt::SelectItem::Wildcard(_)
					| surrealdb_mysql::ast::stmt::SelectItem::QualifiedWildcard(..)
			)
		});
		wrap_positional_links(target, columns, has_wildcard, &mut fields);
	}

	let cond = match &select.where_clause {
		Some(w) => Some(Cond(lower_mysql_expr_in(w, scope.as_ref())?)),
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
///
/// `catalog` says which columns of the target table hold record links: a value written to one is
/// converted by [`link_value`].
pub fn lower_mysql_insert(
	insert: &surrealdb_mysql::ast::stmt::InsertStmt,
	catalog: &dyn BinderCatalog,
) -> Result<Expr, PlanError> {
	let (schema, table_name) =
		crate::namespace::resolve_mysql_table_name(&insert.table.parts, Some(catalog))?;
	let scope = mysql_name_scope(&insert.table, schema, &table_name, None, catalog);
	let into = Some(Expr::Table(TableName::from(table_name)));

	let mut update = None;
	if !insert.on_duplicate_key_update.is_empty() {
		let mut update_assignments = Vec::with_capacity(insert.on_duplicate_key_update.len());
		for (col, expr) in &insert.on_duplicate_key_update {
			let val = lower_mysql_stored_in(expr, Some(&scope))?;
			update_assignments.push(Assignment {
				place: Idiom::field(col.clone()),
				operator: AssignOperator::Assign,
				value: assigned_value(&scope, col, val),
			});
		}
		update = Some(Data::UpdateExpression(update_assignments));
	}

	let data = if insert.values.is_empty() && insert.query.is_none() {
		Data::SingleExpression(Expr::Literal(Literal::Object(Vec::new())))
	} else if let Some(query) = &insert.query {
		let sel = lower_mysql_select_stmt(query, catalog, Some((&scope, &insert.columns)))?;
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
				let lowered_val = assigned_value(&scope, &col_name, lower_mysql_expr(val)?);
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
		output: Some(scope.echo()),
		timeout: Expr::Literal(Literal::None),
		relation: false,
	})))
}

/// Lower a MySQL `ReplaceStmt` into a SurrealDB `Expr::Insert` with update replacements.
///
/// `catalog` says which columns of the target table hold record links: a value written to one is
/// converted by [`link_value`].
pub fn lower_mysql_replace(
	replace: &surrealdb_mysql::ast::stmt::ReplaceStmt,
	catalog: &dyn BinderCatalog,
) -> Result<Expr, PlanError> {
	let (schema, table_name) =
		crate::namespace::resolve_mysql_table_name(&replace.table.parts, Some(catalog))?;
	let scope = mysql_name_scope(&replace.table, schema, &table_name, None, catalog);
	let into = Some(Expr::Table(TableName::from(table_name)));

	let mut update_assignments = Vec::with_capacity(replace.columns.len());
	for col in &replace.columns {
		update_assignments.push(Assignment {
			place: Idiom::field(col.clone()),
			operator: AssignOperator::Assign,
			value: assigned_value(
				&scope,
				col,
				Expr::Idiom(Idiom(vec![
					Part::Start(Expr::Param(Param::from("input".to_string()))),
					Part::Field(Strand::from(col.clone())),
				])),
			),
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
		let sel = lower_mysql_select_stmt(query, catalog, Some((&scope, &replace.columns)))?;
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
				let lowered_val = assigned_value(&scope, &col_name, lower_mysql_expr(val)?);
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
		output: Some(scope.echo()),
		timeout: Expr::Literal(Literal::None),
		relation: false,
	})))
}

/// Lower a MySQL `UpdateStmt` into a SurrealDB `Expr::Update`.
///
/// `catalog` says which columns of the target table hold record links: a value assigned to one is
/// converted by [`link_value`], and a comparison of one in the `WHERE` clause names the record of
/// its table. Any other expression reads a link column as the key of its link. Updates over a join
/// or subquery and updates with `ORDER BY` or `LIMIT` are rejected with
/// [`PlanError::UnsupportedStatement`].
pub fn lower_mysql_update(
	update: &surrealdb_mysql::ast::stmt::UpdateStmt,
	catalog: &dyn BinderCatalog,
) -> Result<Expr, PlanError> {
	let surrealdb_mysql::ast::stmt::TableRef::Table {
		name,
		alias,
		..
	} = &update.table
	else {
		return Err(PlanError::UnsupportedStatement(
			"UPDATE over a join or subquery requires join-based write lowering".into(),
		));
	};
	if !update.order_by.is_empty() || update.limit.is_some() {
		return Err(PlanError::UnsupportedStatement(
			"UPDATE with ORDER BY or LIMIT is not supported".into(),
		));
	}

	let (schema, table_name) =
		crate::namespace::resolve_mysql_table_name(&name.parts, Some(catalog))?;
	let scope = mysql_name_scope(name, schema, &table_name, alias.as_deref(), catalog);
	let what = vec![Expr::Table(TableName::from(table_name))];

	let mut assignments = Vec::with_capacity(update.assignments.len());
	for (col, expr) in &update.assignments {
		let val = lower_mysql_stored_in(expr, Some(&scope))?;
		let value = assigned_value(&scope, col, val);
		assignments.push(Assignment {
			place: Idiom::field(col.clone()),
			operator: AssignOperator::Assign,
			value,
		});
	}

	let cond = match &update.where_clause {
		Some(w) => Some(Cond(lower_mysql_expr_in(w, Some(&scope))?)),
		None => None,
	};

	Ok(Expr::Update(Box::new(UpdateStatement {
		what,
		data: Some(Data::SetExpression(assignments)),
		cond,
		output: Some(scope.echo()),
		..Default::default()
	})))
}

/// Lower a MySQL `DeleteStmt` into a SurrealDB `Expr::Delete`.
///
/// `catalog` says which columns of the target table hold record links: a comparison of one in the
/// `WHERE` clause names the record of its table, and any other expression reads one as the key of
/// its link. Deletes with `ORDER BY` or `LIMIT` are rejected with
/// [`PlanError::UnsupportedStatement`].
pub fn lower_mysql_delete(
	delete: &surrealdb_mysql::ast::stmt::DeleteStmt,
	catalog: &dyn BinderCatalog,
) -> Result<Expr, PlanError> {
	if !delete.order_by.is_empty() || delete.limit.is_some() {
		return Err(PlanError::UnsupportedStatement(
			"DELETE with ORDER BY or LIMIT is not supported".into(),
		));
	}
	let (schema, table_name) =
		crate::namespace::resolve_mysql_table_name(&delete.table.parts, Some(catalog))?;
	let scope = mysql_name_scope(&delete.table, schema, &table_name, None, catalog);
	let what = vec![Expr::Table(TableName::from(table_name))];

	let cond = match &delete.where_clause {
		Some(w) => Some(Cond(lower_mysql_expr_in(w, Some(&scope))?)),
		None => None,
	};

	Ok(Expr::Delete(Box::new(DeleteStatement {
		what,
		cond,
		output: Some(scope.over_deleted_row().echo()),
		..Default::default()
	})))
}

/// Lower a MySQL `TruncateStmt` into a SurrealDB `Expr::Delete`.
///
/// The statement returns nothing: MySQL answers `TRUNCATE` with an OK packet that reports no
/// affected rows, so the rows it removes are neither collected nor sent.
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
