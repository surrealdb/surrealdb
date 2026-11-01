//! `SHOW` and `DESCRIBE`, answered as the result sets MySQL gives.
//!
//! Each statement translates to one SurrealQL expression that evaluates to an array of objects,
//! one object per row, keyed by MySQL's column names. An object's keys are sorted, so
//! [`show_columns`] gives the order and the types of the columns a result set is laid out in.
//!
//! What a statement lists is what `INFO FOR DB`, `INFO FOR NS` and `INFO FOR TABLE` report to
//! the session that runs it: they check the session's view of the database or namespace before
//! they read anything, so a session that may not view it gets their refusal and no listing.
//! Nothing here reads the catalog any other way, and every name a statement mentions reaches the
//! expression as a quoted identifier or string literal.
//!
//! The answers of `SHOW` statements about the server itself (`VARIABLES`, `ENGINES`, `COLLATION`,
//! `CHARACTER SET`) are fixed rows; `SHOW VARIABLES` lists the table of
//! [`system_vars`](crate::system_vars), which is also what `SELECT @@name` answers.
//! `STATUS`, `PROCESSLIST`, `WARNINGS` and `ERRORS` have no rows to report and answer with their
//! columns alone.

use std::fmt::Write;

use crate::ast::stmt::{DescribeStmt, QualifiedName, ShowStmt, Statement};
use crate::system_vars::{MYSQL_DEFAULT_COLLATION, mysql_system_variables};
use crate::translate::error::TranslationError;

/// The longest `LIKE` pattern, in characters, that `SHOW` accepts. The pattern becomes a regular
/// expression the engine compiles and runs against every name, so its length bounds that work.
const MAX_LIKE_PATTERN_CHARS: usize = 512;

/// The kind of value a column of a `SHOW` result holds, which decides its MySQL type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShowColumnKind {
	/// Text (`VAR_STRING`).
	Text,
	/// A 32-bit integer (`LONG`).
	Int,
	/// A 64-bit integer (`LONGLONG`).
	BigInt,
}

/// A column of a `SHOW` result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShowColumn {
	/// The column's name, which is also the key of its cell in each row object.
	pub name: String,
	pub kind: ShowColumnKind,
}

/// The columns of the result of `SHOW COLUMNS` and `DESCRIBE`.
const COLUMNS: &[&str] = &["Field", "Type", "Null", "Key", "Default", "Extra"];

/// The columns of the result of `SHOW FULL COLUMNS`.
const FULL_COLUMNS: &[&str] =
	&["Field", "Type", "Collation", "Null", "Key", "Default", "Extra", "Privileges", "Comment"];

/// The columns of the result of `SHOW INDEX`, with the kind of each.
const INDEX_COLUMNS: &[(&str, ShowColumnKind)] = &[
	("Table", ShowColumnKind::Text),
	("Non_unique", ShowColumnKind::Int),
	("Key_name", ShowColumnKind::Text),
	("Seq_in_index", ShowColumnKind::Int),
	("Column_name", ShowColumnKind::Text),
	("Collation", ShowColumnKind::Text),
	("Cardinality", ShowColumnKind::BigInt),
	("Sub_part", ShowColumnKind::BigInt),
	("Packed", ShowColumnKind::Text),
	("Null", ShowColumnKind::Text),
	("Index_type", ShowColumnKind::Text),
	("Comment", ShowColumnKind::Text),
	("Index_comment", ShowColumnKind::Text),
	("Visible", ShowColumnKind::Text),
	("Expression", ShowColumnKind::Text),
];

/// The columns of the result of `SHOW PROCESSLIST`, with the kind of each.
const PROCESSLIST_COLUMNS: &[(&str, ShowColumnKind)] = &[
	("Id", ShowColumnKind::BigInt),
	("User", ShowColumnKind::Text),
	("Host", ShowColumnKind::Text),
	("db", ShowColumnKind::Text),
	("Command", ShowColumnKind::Text),
	("Time", ShowColumnKind::Int),
	("State", ShowColumnKind::Text),
	("Info", ShowColumnKind::Text),
];

/// The columns of the result of `SHOW WARNINGS` and `SHOW ERRORS`, with the kind of each.
const DIAGNOSTICS_COLUMNS: &[(&str, ShowColumnKind)] = &[
	("Level", ShowColumnKind::Text),
	("Code", ShowColumnKind::Int),
	("Message", ShowColumnKind::Text),
];

/// The columns of the result of `SHOW COLLATION`, with the kind of each.
const COLLATION_COLUMNS: &[(&str, ShowColumnKind)] = &[
	("Collation", ShowColumnKind::Text),
	("Charset", ShowColumnKind::Text),
	("Id", ShowColumnKind::BigInt),
	("Default", ShowColumnKind::Text),
	("Compiled", ShowColumnKind::Text),
	("Sortlen", ShowColumnKind::Int),
	("Pad_attribute", ShowColumnKind::Text),
];

/// The columns of the result of `SHOW TABLE STATUS`, with the kind of each.
const TABLE_STATUS_COLUMNS: &[(&str, ShowColumnKind)] = &[
	("Name", ShowColumnKind::Text),
	("Engine", ShowColumnKind::Text),
	("Version", ShowColumnKind::Int),
	("Row_format", ShowColumnKind::Text),
	("Rows", ShowColumnKind::BigInt),
	("Avg_row_length", ShowColumnKind::BigInt),
	("Data_length", ShowColumnKind::BigInt),
	("Max_data_length", ShowColumnKind::BigInt),
	("Index_length", ShowColumnKind::BigInt),
	("Data_free", ShowColumnKind::BigInt),
	("Auto_increment", ShowColumnKind::BigInt),
	("Create_time", ShowColumnKind::Text),
	("Update_time", ShowColumnKind::Text),
	("Check_time", ShowColumnKind::Text),
	("Collation", ShowColumnKind::Text),
	("Checksum", ShowColumnKind::BigInt),
	("Create_options", ShowColumnKind::Text),
	("Comment", ShowColumnKind::Text),
];

/// The columns of the result of `SHOW CHARACTER SET`, with the kind of each.
const CHARSET_COLUMNS: &[(&str, ShowColumnKind)] = &[
	("Charset", ShowColumnKind::Text),
	("Description", ShowColumnKind::Text),
	("Default collation", ShowColumnKind::Text),
	("Maxlen", ShowColumnKind::Int),
];

/// The columns of the result of a `SHOW` or `DESCRIBE` statement, in the order MySQL lists them,
/// and `None` for any other statement, whose columns are those of the rows it returns.
/// `database` completes the `Tables_in_<database>` column of `SHOW TABLES`.
pub fn show_columns(stmt: &Statement, database: &str) -> Option<Vec<ShowColumn>> {
	if let Statement::Describe(_) = stmt {
		return Some(text_columns(COLUMNS));
	}
	let Statement::Show(show) = stmt else {
		return None;
	};
	Some(match show {
		ShowStmt::Tables {
			full,
			..
		} => {
			let mut columns = vec![text(format!("Tables_in_{database}"))];
			if *full {
				columns.push(text("Table_type".to_string()));
			}
			columns
		}
		ShowStmt::TableStatus {
			..
		} => typed_columns(TABLE_STATUS_COLUMNS),
		ShowStmt::Triggers {
			..
		} => text_columns(&[
			"Trigger",
			"Event",
			"Table",
			"Statement",
			"Timing",
			"Created",
			"sql_mode",
			"Definer",
			"character_set_client",
			"collation_connection",
			"Database Collation",
		]),
		ShowStmt::Databases {
			..
		} => text_columns(&["Database"]),
		ShowStmt::Columns {
			full,
			..
		} => text_columns(if *full {
			FULL_COLUMNS
		} else {
			COLUMNS
		}),
		ShowStmt::CreateTable {
			..
		} => text_columns(&["Table", "Create Table"]),
		ShowStmt::Variables {
			..
		}
		| ShowStmt::Status {
			..
		} => text_columns(&["Variable_name", "Value"]),
		ShowStmt::Processlist {
			..
		} => typed_columns(PROCESSLIST_COLUMNS),
		ShowStmt::Warnings {
			..
		}
		| ShowStmt::Errors {
			..
		} => typed_columns(DIAGNOSTICS_COLUMNS),
		ShowStmt::Index {
			..
		} => typed_columns(INDEX_COLUMNS),
		ShowStmt::Engines {
			..
		} => text_columns(&["Engine", "Support", "Comment", "Transactions", "XA", "Savepoints"]),
		ShowStmt::Collation {
			..
		} => typed_columns(COLLATION_COLUMNS),
		ShowStmt::Charset {
			..
		} => typed_columns(CHARSET_COLUMNS),
	})
}

fn text(name: String) -> ShowColumn {
	ShowColumn {
		name,
		kind: ShowColumnKind::Text,
	}
}

fn text_columns(names: &[&str]) -> Vec<ShowColumn> {
	names.iter().map(|name| text((*name).to_string())).collect()
}

fn typed_columns(columns: &[(&str, ShowColumnKind)]) -> Vec<ShowColumn> {
	columns
		.iter()
		.map(|(name, kind)| ShowColumn {
			name: (*name).to_string(),
			kind: *kind,
		})
		.collect()
}

/// Translate a `SHOW` statement to the SurrealQL expression that answers it.
pub fn translate_show(show: &ShowStmt, out: &mut String) -> Result<(), TranslationError> {
	match show {
		ShowStmt::Tables {
			full,
			database,
			like,
			..
		} => translate_tables(*full, database.as_deref(), like.as_deref(), out),
		ShowStmt::TableStatus {
			database,
			like,
			..
		} => translate_table_status(database.as_deref(), like.as_deref(), out),
		ShowStmt::Databases {
			like,
			..
		} => {
			out.push_str("RETURN object::keys((INFO FOR NS).databases)");
			push_like_filter(out, "$d", "$d", like.as_deref(), false)?;
			out.push_str(".map(|$d| { Database: $d });");
			Ok(())
		}
		ShowStmt::Columns {
			full,
			table,
			like,
			..
		} => translate_columns(table, *full, like.as_deref(), out),
		ShowStmt::CreateTable {
			table,
			..
		} => translate_create_table(table, out),
		ShowStmt::Index {
			table,
			..
		} => translate_index(table, out),
		ShowStmt::Variables {
			like,
			..
		} => {
			push_variables(out)?;
			push_like_filter(out, "$r", "$r.Variable_name", like.as_deref(), true)?;
			out.push(';');
			Ok(())
		}
		ShowStmt::Status {
			..
		}
		| ShowStmt::Triggers {
			..
		}
		| ShowStmt::Processlist {
			..
		}
		| ShowStmt::Warnings {
			..
		}
		| ShowStmt::Errors {
			..
		} => {
			out.push_str("RETURN [];");
			Ok(())
		}
		ShowStmt::Engines {
			..
		} => {
			out.push_str(
				"RETURN [{ Engine: 'InnoDB', Support: 'DEFAULT', Comment: 'Supports \
				 transactions, row-level locking, and foreign keys', Transactions: 'YES', XA: \
				 'NO', Savepoints: 'YES' }];",
			);
			Ok(())
		}
		ShowStmt::Collation {
			like,
			..
		} => {
			write!(
				out,
				"RETURN [{{ Collation: {}, Charset: 'utf8mb4', Id: 255, Default: 'Yes', Compiled: \
				 'Yes', Sortlen: 0, Pad_attribute: 'NO PAD' }}]",
				surql_string(MYSQL_DEFAULT_COLLATION)
			)?;
			push_like_filter(out, "$r", "$r.Collation", like.as_deref(), true)?;
			out.push(';');
			Ok(())
		}
		ShowStmt::Charset {
			like,
			..
		} => {
			write!(
				out,
				"RETURN [{{ Charset: 'utf8mb4', Description: 'UTF-8 Unicode', 'Default collation': \
				 {}, Maxlen: 4 }}]",
				surql_string(MYSQL_DEFAULT_COLLATION)
			)?;
			push_like_filter(out, "$r", "$r.Charset", like.as_deref(), true)?;
			out.push(';');
			Ok(())
		}
	}
}

/// Translate `DESCRIBE <table> [<column pattern>]`, which is `SHOW COLUMNS`.
pub fn translate_describe(desc: &DescribeStmt, out: &mut String) -> Result<(), TranslationError> {
	translate_columns(&desc.table, false, desc.column.as_deref(), out)
}

/// `SHOW [FULL] TABLES`: a row per table of the database, under `Tables_in_<database>`. A table
/// is a view when its definition reads `AS SELECT`; the definition is read as text because the
/// structured form of `INFO FOR DB` also loads every module of the database.
fn translate_tables(
	full: bool,
	database: Option<&str>,
	like: Option<&str>,
	out: &mut String,
) -> Result<(), TranslationError> {
	out.push_str("{ ");
	if let Some(database) = database {
		push_database_guard(out, database, database)?;
	}
	if full {
		out.push_str("RETURN object::entries((INFO FOR DB).tables)");
		push_like_filter(out, "$e", "$e[0]", like, false)?;
		out.push_str(
			".map(|$e| object::from_entries([['Tables_in_' + session::db(), $e[0]], \
			 ['Table_type', IF string::contains($e[1], ' AS SELECT ') { 'VIEW' } ELSE { 'BASE \
			 TABLE' }]]));",
		);
	} else {
		out.push_str("RETURN object::keys((INFO FOR DB).tables)");
		push_like_filter(out, "$t", "$t", like, false)?;
		out.push_str(".map(|$t| object::from_entries([['Tables_in_' + session::db(), $t]]));");
	}
	out.push_str(" }");
	Ok(())
}

/// `SHOW TABLE STATUS`: a row per table of the database. The storage figures and the times are
/// not tracked, so the sizes are zero and the rest `NULL`; `Comment` is `VIEW` for a view, as
/// MySQL reports it, which is how a dump tool tells a view from a table.
fn translate_table_status(
	database: Option<&str>,
	like: Option<&str>,
	out: &mut String,
) -> Result<(), TranslationError> {
	out.push_str("{ ");
	if let Some(database) = database {
		push_database_guard(out, database, database)?;
	}
	out.push_str("RETURN object::entries((INFO FOR DB).tables)");
	push_like_filter(out, "$e", "$e[0]", like, false)?;
	write!(
		out,
		".map(|$e| {{ Name: $e[0], Engine: 'InnoDB', Version: 10, Row_format: 'Dynamic', Rows: \
		 0, Avg_row_length: 0, Data_length: 0, Max_data_length: 0, Index_length: 0, Data_free: 0, \
		 Auto_increment: NULL, Create_time: NULL, Update_time: NULL, Check_time: NULL, \
		 Collation: {}, Checksum: NULL, Create_options: '', Comment: IF string::contains($e[1], ' \
		 AS SELECT ') {{ 'VIEW' }} ELSE {{ '' }} }});",
		surql_string(MYSQL_DEFAULT_COLLATION)
	)?;
	out.push_str(" }");
	Ok(())
}

/// `SHOW [FULL] COLUMNS` and `DESCRIBE`: a row per field the table defines at its top level, with
/// the type MySQL would show for the field's kind, whether it may be `NULL`, whether an index
/// makes it a key (`PRI` when it is a column of the index MySQL calls `PRIMARY`, `UNI` when a
/// unique index covers it alone, `MUL` when it starts any other index), its default and what is
/// generated. A field of a nested object or an array element is not a column.
fn translate_columns(
	table: &QualifiedName,
	full: bool,
	like: Option<&str>,
	out: &mut String,
) -> Result<(), TranslationError> {
	let name = begin_table_block(table, out)?;
	write!(out, "LET $info = INFO FOR TABLE {} STRUCTURE; ", surql_ident(name))?;
	out.push_str(
		"LET $pk = $info.indexes.filter(|$i| string::ends_with($i.name, '_pk')); \
		 RETURN $info.fields.filter(|$f| string::starts_with($f.name, '`') OR \
		 !string::contains($f.name, '.')).map(|$f| { \
		 LET $name = IF string::starts_with($f.name, '`') { string::slice($f.name, 1, \
		 string::len($f.name) - 1) } ELSE { $f.name }; \
		 LET $kinds = string::split($f.kind ?? 'any', ' | '); \
		 LET $core = $kinds.filter(|$k| $k != 'none' AND $k != 'null'); \
		 LET $base = string::split($core[0] ?? 'any', '<')[0]; \
		 LET $d = $f.default; \
		 LET $seq = $d != NONE AND string::contains($d, '::nextval('); \
		 LET $str = $d != NONE AND string::len($d) >= 2 AND (string::starts_with($d, \"'\") OR \
		 string::starts_with($d, '\"')) AND string::ends_with($d, string::slice($d, 0, 1)); \
		 LET $num = $d != NONE AND string::matches($d, <regex> \
		 '^-?[0-9]+(\\\\.[0-9]+)?(f|dec)?$'); \
		 LET $bool = $d IN ['true', 'false']; \
		 RETURN { Field: $name, \
		 Type: IF array::len($core) = 1 { { int: 'bigint', float: 'double', number: 'double', \
		 decimal: 'decimal(65,30)', string: 'text', bool: 'tinyint(1)', datetime: 'datetime', \
		 duration: 'time', uuid: 'char(36)', bytes: 'blob', object: 'json', array: 'json', set: \
		 'json', geometry: 'json', point: 'json', line: 'json', polygon: 'json', multipoint: \
		 'json', multiline: 'json', multipolygon: 'json', collection: 'json' }[$base] ?? 'text' \
		 } ELSE { 'text' }, \
		 Null: IF array::len($core) < array::len($kinds) OR $base = 'any' { 'YES' } ELSE { 'NO' \
		 }, \
		 Key: IF array::any($pk, |$i| $f.name IN $i.cols) { 'PRI' } ELSE IF \
		 array::any($info.indexes, |$i| $i.index = 'UNIQUE' AND $i.cols = [$f.name]) { 'UNI' } \
		 ELSE IF array::any($info.indexes, |$i| $i.cols[0] = $f.name) { 'MUL' } ELSE { '' }, \
		 Default: IF $d = NONE OR $seq { NULL } ELSE IF $str { string::slice($d, 1, \
		 string::len($d) - 1) } ELSE IF $num { string::replace($d, <regex> '(f|dec)$', '') } \
		 ELSE IF $bool { IF $d = 'true' { '1' } ELSE { '0' } } ELSE IF $d = 'time::now()' { \
		 'CURRENT_TIMESTAMP' } ELSE { $d }, \
		 Extra: IF $seq { 'auto_increment' } ELSE IF $f.computed != NONE { 'VIRTUAL GENERATED' } \
		 ELSE IF $d != NONE AND !($str OR $num OR $bool) { 'DEFAULT_GENERATED' } ELSE { '' }",
	);
	if full {
		write!(
			out,
			", Collation: IF array::len($core) = 1 AND $base = 'string' {{ {} }} ELSE {{ NULL }}, \
			 Privileges: 'select,insert,update,references', Comment: $f.comment ?? ''",
			surql_string(MYSQL_DEFAULT_COLLATION)
		)?;
	}
	out.push_str(" }; })");
	push_like_filter(out, "$r", "$r.Field", like, true)?;
	out.push_str("; }");
	Ok(())
}

/// `SHOW CREATE TABLE`: one row holding the table's name and the SurrealQL that defines it, the
/// table, its fields, its indexes and its events, each definition a statement.
fn translate_create_table(table: &QualifiedName, out: &mut String) -> Result<(), TranslationError> {
	let name = begin_table_block(table, out)?;
	write!(
		out,
		"LET $info = INFO FOR TABLE {ident}; RETURN [{{ Table: {text}, 'Create Table': \
		 array::join(array::concat([(INFO FOR DB).tables[{text}]], object::values($info.fields), \
		 object::values($info.indexes), object::values($info.events)), ';\\n') }}]; }}",
		ident = surql_ident(name),
		text = surql_string(name),
	)?;
	Ok(())
}

/// `SHOW INDEX`: a row per column of each index, `PRIMARY` first. An index is the primary key
/// when its name ends in `_pk`, the name `CREATE TABLE ... PRIMARY KEY` gives it.
fn translate_index(table: &QualifiedName, out: &mut String) -> Result<(), TranslationError> {
	let name = begin_table_block(table, out)?;
	write!(out, "LET $info = INFO FOR TABLE {} STRUCTURE; ", surql_ident(name))?;
	write!(
		out,
		"LET $nullable = $info.fields.filter(|$f| array::any(string::split($f.kind ?? 'any', ' | \
		 '), |$k| $k IN ['none', 'null', 'any'])).map(|$f| $f.name); \
		 LET $rows = array::flatten($info.indexes.map(|$i| $i.cols.map(|$c| {{ Table: {text}, \
		 Non_unique: IF $i.index = 'UNIQUE' {{ 0 }} ELSE {{ 1 }}, \
		 Key_name: IF string::ends_with($i.name, '_pk') {{ 'PRIMARY' }} ELSE {{ $i.name }}, \
		 Seq_in_index: array::find_index($i.cols, $c) + 1, Column_name: $c, Collation: 'A', \
		 Cardinality: NULL, Sub_part: NULL, Packed: NULL, \
		 Null: IF $c IN $nullable {{ 'YES' }} ELSE {{ '' }}, \
		 Index_type: IF $i.index = '' OR $i.index = 'UNIQUE' {{ 'BTREE' }} ELSE {{ \
		 string::uppercase(string::split($i.index, ' ')[0]) }}, \
		 Comment: '', Index_comment: $i.comment ?? '', Visible: 'YES', Expression: NULL }}))); \
		 RETURN array::concat($rows.filter(|$r| $r.Key_name = 'PRIMARY'), \
		 $rows.filter(|$r| $r.Key_name != 'PRIMARY')); }}",
		text = surql_string(name),
	)?;
	Ok(())
}

/// Open a block that reads the catalog for `table`, and return the table's name. The block
/// refuses, as MySQL does, a table that does not exist or that a database other than the
/// session's qualifies; the caller finishes the block.
fn begin_table_block<'a>(
	table: &'a QualifiedName,
	out: &mut String,
) -> Result<&'a str, TranslationError> {
	let (database, name) = match table.parts.as_slice() {
		[name] => (None, name.as_str()),
		[database, name] => (Some(database.as_str()), name.as_str()),
		[] => return Err(TranslationError::Invalid("empty table name".to_string())),
		parts => {
			return Err(TranslationError::Unsupported(format!(
				"database qualification depth not supported: {}",
				parts.join(".")
			)));
		}
	};
	out.push_str("{ ");
	if let Some(database) = database {
		push_database_guard(out, database, &table.parts.join("."))?;
	}
	write!(
		out,
		"IF (INFO FOR DB).tables[{key}] = NONE {{ THROW {message} }}; ",
		key = surql_string(name),
		message = surql_string(&format!("Table '{name}' doesn't exist")),
	)?;
	Ok(name)
}

/// Make the block refuse unless the session's database is `database`, whose name matches in
/// any case. `qualified` is what the statement wrote, for the refusal.
fn push_database_guard(
	out: &mut String,
	database: &str,
	qualified: &str,
) -> Result<(), TranslationError> {
	write!(
		out,
		"IF string::lowercase(session::db() ?? '') != string::lowercase({database}) {{ THROW \
		 {message} }}; ",
		database = surql_string(database),
		message = surql_string(&format!("cross-database queries not supported: {qualified}")),
	)?;
	Ok(())
}

/// `RETURN [{ Variable_name: <name>, Value: <value> }, ...]` over the variables of the server, in
/// ascending order of name. A value is listed as MySQL lists it: a switch as `ON` or `OFF` and a
/// number as its text.
fn push_variables(out: &mut String) -> Result<(), TranslationError> {
	out.push_str("RETURN [");
	for (i, (name, variable)) in mysql_system_variables().iter().enumerate() {
		if i > 0 {
			out.push_str(", ");
		}
		write!(
			out,
			"{{ Variable_name: {}, Value: {} }}",
			surql_string(name),
			surql_string(&variable.listed())
		)?;
	}
	out.push(']');
	Ok(())
}

/// `.filter(|<var>| string::matches(<subject>, <pattern>))` for a `LIKE` pattern, ignoring case
/// where `insensitive` is set, and nothing without a pattern. `subject` is the text of `var` that
/// the pattern is matched against.
fn push_like_filter(
	out: &mut String,
	var: &str,
	subject: &str,
	like: Option<&str>,
	insensitive: bool,
) -> Result<(), TranslationError> {
	if let Some(pattern) = like {
		let regex = like_to_regex(pattern, insensitive)?;
		write!(
			out,
			".filter(|{var}| string::matches({subject}, <regex> {}))",
			surql_string(&regex)
		)?;
	}
	Ok(())
}

/// The regular expression that matches exactly what a MySQL `LIKE` pattern matches: `%` is any
/// run of characters, `_` any one, a backslash makes the next character literal, a backslash that
/// ends the pattern has nothing to escape and stands for itself, and every other character
/// stands for itself. A pattern is refused beyond [`MAX_LIKE_PATTERN_CHARS`].
fn like_to_regex(pattern: &str, insensitive: bool) -> Result<String, TranslationError> {
	if pattern.chars().count() > MAX_LIKE_PATTERN_CHARS {
		return Err(TranslationError::Unsupported(format!(
			"LIKE pattern longer than {MAX_LIKE_PATTERN_CHARS} characters"
		)));
	}
	let mut regex = String::from(if insensitive {
		"(?is)^"
	} else {
		"(?s)^"
	});
	let mut chars = pattern.chars().peekable();
	while let Some(c) = chars.next() {
		match c {
			'%' => {
				while chars.peek() == Some(&'%') {
					chars.next();
				}
				regex.push_str(".*");
			}
			'_' => regex.push('.'),
			'\\' => push_literal(&mut regex, chars.next().unwrap_or('\\')),
			c => push_literal(&mut regex, c),
		}
	}
	regex.push('$');
	Ok(regex)
}

fn push_literal(regex: &mut String, c: char) {
	if "\\.+*?()|[]{}^$#&-~".contains(c) {
		regex.push('\\');
	}
	regex.push(c);
}

/// A SurrealQL string literal holding `text`.
fn surql_string(text: &str) -> String {
	let mut literal = String::with_capacity(text.len() + 2);
	literal.push('\'');
	for c in text.chars() {
		if matches!(c, '\\' | '\'') {
			literal.push('\\');
		}
		literal.push(c);
	}
	literal.push('\'');
	literal
}

/// A SurrealQL identifier that reads as `name` whatever characters it holds.
fn surql_ident(name: &str) -> String {
	let mut ident = String::with_capacity(name.len() + 2);
	ident.push('`');
	for c in name.chars() {
		if matches!(c, '\\' | '`') {
			ident.push('\\');
		}
		ident.push(c);
	}
	ident.push('`');
	ident
}
