//! MySQL query translation and execution helper.

use anyhow::{Result, bail};
use surrealdb_mysql::ast::expr::Expr;
use surrealdb_mysql::ast::literal::Literal;
use surrealdb_mysql::ast::stmt::{LimitClause, SelectItem, SelectStmt, Statement, VariableTarget};
use surrealdb_mysql::lexer::Lexer;
use surrealdb_mysql::{ParserSettings, TokenKind};
use surrealdb_sqlplan::{
	SystemVariable, SystemVariableError, check_mysql_system_variable, mysql_system_variable,
};
use surrealdb_types::{Number, Value};

use crate::constants::{MYSQL_TYPE_LONGLONG, MYSQL_TYPE_VAR_STRING};
use crate::msg::{ColumnDefinition41, ErrPacket};
use crate::typing::ResultShape;

/// Translate a MySQL query string into SurrealQL.
pub fn translate_query(sql: &str) -> Result<String> {
	match surrealdb_mysql::translate_sql(sql) {
		Ok(surql) => Ok(surql),
		Err(e) => bail!("Failed to translate MySQL query: {e}"),
	}
}

/// The error MySQL answers when `sql` names a system variable it refuses, or `None` when every
/// variable the statement names is one MySQL accepts.
///
/// MySQL resolves each `@@name` as it reads the statement, so a statement that names a variable
/// MySQL does not have fails with error 1193 whether or not the reference is ever evaluated, and
/// it fails before the statement is prepared. A statement that names a variable `@@global.name`
/// that only a session has fails with error 1238. A `SET` is refused for an unknown target
/// written with or without `@@`, with no scope check: the scopes of a `SET` have their own
/// errors.
///
/// Only a statement that holds `@@` or starts with `SET` is read, and it is read without being
/// held in memory as a whole token list, so the cost is one pass over a statement the parser
/// bounds anyway. A statement the parser cannot read is left to it.
pub(crate) fn system_variable_refusal(sql: &str) -> Option<ErrPacket> {
	let set = starts_with_set(sql);
	if (!set && !sql.contains("@@")) || sql.len() > ParserSettings::default().max_input_bytes {
		return None;
	}
	let refusal = |error: SystemVariableError| {
		ErrPacket::general(error.mysql_error_code(), b"HY000", error.to_string())
	};

	let mut lexer = Lexer::new(sql);
	while let Ok(token) = lexer.next_token() {
		if matches!(token.kind, TokenKind::Eof) {
			break;
		}
		if let TokenKind::SystemVar {
			name,
			is_global,
		} = token.kind
			&& let Err(error) = check_mysql_system_variable(&name, is_global && !set)
		{
			return Some(refusal(error));
		}
	}

	if set && let Ok(Statement::Set(statement)) = surrealdb_mysql::parse_statement(sql) {
		for (target, _) in &statement.assignments {
			// `SET NAMES` and `SET PASSWORD` are parsed as assignments to a target of that name,
			// and are statements of their own in MySQL.
			if let VariableTarget::System {
				name,
				..
			} = target && !name.eq_ignore_ascii_case("names")
				&& !name.eq_ignore_ascii_case("password")
				&& let Err(error) = check_mysql_system_variable(name, false)
			{
				return Some(refusal(error));
			}
		}
	}
	None
}

/// Whether the statement starts with the word `SET`.
fn starts_with_set(sql: &str) -> bool {
	let bytes = sql.trim_start().as_bytes();
	bytes.get(..3).is_some_and(|word| word.eq_ignore_ascii_case(b"set"))
		&& bytes.get(3).is_none_or(|next| !(next.is_ascii_alphanumeric() || *next == b'_'))
}

/// The longest column name, in bytes, that MySQL sends.
const MAX_COLUMN_NAME: usize = 255;

/// The most columns [`system_variable_answer`] gives. A longer select list is left to the engine,
/// so the size of the answer is bounded by this and not by the length of the statement.
const MAX_COLUMNS: usize = 4096;

/// The answer to `sql` when it is one `SELECT` of nothing but system variables, for a connection
/// that has selected no database. The items are system variables, with an alias or without, and
/// the statement has no clause but a `LIMIT` of integer literals. `None` for any other statement,
/// which the engine answers, and for one that names a variable MySQL does not have, which is
/// refused before ([`system_variable_refusal`]). `schema` is the database the columns are
/// reported under.
///
/// A driver reads the settings of its session with such a statement as soon as it connects: the
/// `mysql` client sends `select @@version_comment limit 1` and Connector/J one `SELECT` of all
/// its settings. The engine runs a statement only in a database, so a connection that has not
/// selected one cannot have it answer. The answer is the one the engine gives when a database is
/// selected: one column per variable, named by its alias or as written, typed and valued by the
/// planner's table of system variables, and NULL for a variable MySQL has and the table does not
/// model.
pub(crate) fn system_variable_answer(sql: &str, schema: &str) -> Option<ResultShape> {
	let statements = surrealdb_mysql::parse_statements(sql).ok()?;
	let [Statement::Select(select)] = statements.as_slice() else {
		return None;
	};
	// Every field is named, so a clause added to `SELECT` is not ignored here without notice.
	let SelectStmt {
		with: None,
		distinct: false,
		projection,
		from: None,
		where_clause: None,
		group_by,
		with_rollup: false,
		having: None,
		window,
		order_by,
		limit,
		lock: None,
		set_op: None,
		span: _,
	} = &**select
	else {
		return None;
	};
	if !group_by.is_empty()
		|| !window.is_empty()
		|| !order_by.is_empty()
		|| projection.len() > MAX_COLUMNS
	{
		return None;
	}
	let has_row = match limit {
		None => true,
		Some(limit) => limit_keeps_the_row(limit)?,
	};

	let mut columns = Vec::with_capacity(projection.len());
	let mut row = Vec::with_capacity(projection.len());
	let mut raw_row = Vec::with_capacity(projection.len());
	for item in projection {
		let SelectItem::Expr {
			expr: Expr::SystemVar {
				name,
				span,
				..
			},
			alias,
			..
		} = item
		else {
			return None;
		};
		// An item without an alias is named by its text, as MySQL names it.
		let written = sql.get(span.start..span.end)?;
		let column_name = alias.as_deref().unwrap_or(written);
		// `floor_char_boundary` is a character boundary at or below its argument, so the slice
		// is a prefix of at most `MAX_COLUMN_NAME` bytes that splits no character.
		let column_name = &column_name[..column_name.floor_char_boundary(MAX_COLUMN_NAME)];
		let (column_type, text, value) = match mysql_system_variable(name) {
			Some(SystemVariable::Int(value)) => {
				(MYSQL_TYPE_LONGLONG, Some(value.to_string()), Value::Number(Number::Int(value)))
			}
			Some(SystemVariable::Flag(on)) => {
				let value = i64::from(on);
				(MYSQL_TYPE_LONGLONG, Some(value.to_string()), Value::Number(Number::Int(value)))
			}
			Some(SystemVariable::Text(value)) => {
				(MYSQL_TYPE_VAR_STRING, Some(value.to_string()), Value::String(value.to_string()))
			}
			None if check_mysql_system_variable(name, false).is_ok() => {
				(MYSQL_TYPE_VAR_STRING, None, Value::Null)
			}
			None => return None,
		};
		columns.push(ColumnDefinition41::new(schema, "", column_name, column_type));
		row.push(text);
		raw_row.push(value);
	}
	let (rows, raw_rows) = if has_row {
		(vec![row], vec![raw_row])
	} else {
		(Vec::new(), Vec::new())
	};
	Some(ResultShape {
		columns,
		rows,
		raw_rows,
	})
}

/// Whether `limit` keeps the one row of the statement, and `None` when it is not made of integer
/// literals.
fn limit_keeps_the_row(limit: &LimitClause) -> Option<bool> {
	let literal = |expr: &Expr| {
		if let Expr::Literal(Literal::Integer(value), _) = expr
			&& *value >= 0
		{
			Some(*value)
		} else {
			None
		}
	};
	let count = literal(&limit.count)?;
	let offset = match &limit.offset {
		Some(offset) => literal(offset)?,
		None => 0,
	};
	Some(count > 0 && offset == 0)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_translate_query() {
		let res = translate_query("SELECT 1").unwrap();
		assert_eq!(res, "RETURN [{ \"col_1\": 1 }];");
	}

	fn names(shape: &ResultShape) -> Vec<&str> {
		shape.columns.iter().map(|column| column.name.as_str()).collect()
	}

	#[test]
	fn a_select_of_variables_is_answered_as_written() {
		let shape = system_variable_answer(
			"SELECT @@version, @@session.autocommit, @@GLOBAL.TIME_ZONE AS zone, @@`sql_mode`, @@max_connections",
			"test",
		)
		.expect("answered");
		assert_eq!(
			names(&shape),
			["@@version", "@@session.autocommit", "zone", "@@`sql_mode`", "@@max_connections"]
		);
		assert_eq!(
			shape.columns.iter().map(|column| column.column_type).collect::<Vec<_>>(),
			[
				MYSQL_TYPE_VAR_STRING,
				MYSQL_TYPE_LONGLONG,
				MYSQL_TYPE_VAR_STRING,
				MYSQL_TYPE_VAR_STRING,
				MYSQL_TYPE_VAR_STRING
			]
		);
		assert_eq!(shape.rows.len(), 1);
		assert_eq!(shape.rows[0][1].as_deref(), Some("1"));
		assert_eq!(shape.rows[0][2].as_deref(), Some("SYSTEM"));
		assert_eq!(shape.rows[0][4], None);
		assert_eq!(shape.raw_rows[0][1], Value::Number(Number::Int(1)));
		assert_eq!(shape.raw_rows[0][4], Value::Null);
	}

	#[test]
	fn limit_keeps_or_drops_the_one_row_and_dual_is_no_table() {
		for (sql, rows) in [
			("select @@version_comment limit 1", 1),
			("SELECT @@version FROM DUAL", 1),
			("SELECT @@max_allowed_packet;", 1),
			("  SELECT @@max_allowed_packet ;  ", 1),
			("SELECT @@version LIMIT 5", 1),
			("SELECT @@version LIMIT 5 OFFSET 0", 1),
			("SELECT @@version LIMIT 0", 0),
			("SELECT @@version LIMIT 1 OFFSET 1", 0),
			("SELECT @@version LIMIT 1, 1", 0),
		] {
			let shape =
				system_variable_answer(sql, "test").unwrap_or_else(|| panic!("{sql} is answered"));
			assert_eq!(shape.rows.len(), rows, "{sql}");
			assert_eq!(shape.columns.len(), 1, "{sql}");
		}
	}

	#[test]
	fn any_other_statement_is_left_to_the_engine() {
		for sql in [
			"SELECT 1",
			"SELECT @x",
			"SELECT @@version, 1",
			"SELECT @@version + 1",
			"SELECT @@version AS v, DATABASE()",
			"SELECT * FROM t",
			"SELECT @@version FROM t",
			"SELECT @@version WHERE 1 = 1",
			"SELECT @@version ORDER BY 1",
			"SELECT @@version GROUP BY 1",
			"SELECT @@version HAVING 1",
			"SELECT DISTINCT @@version",
			"SELECT @@version LIMIT (SELECT 1)",
			"SELECT @@version LIMIT ?",
			"SELECT @@version UNION SELECT @@version",
			"SELECT @@version; SELECT @@version",
			"WITH c AS (SELECT 1) SELECT @@version",
			"SELECT @@version FOR UPDATE",
			"SET @@autocommit = 1",
			"SELECT @@",
			"",
		] {
			assert!(system_variable_answer(sql, "test").is_none(), "{sql}");
		}
	}

	#[test]
	fn a_select_list_is_answered_up_to_a_bound() {
		let list = |count: usize| vec!["@@version"; count].join(", ");
		let shape = system_variable_answer(&format!("SELECT {};", list(MAX_COLUMNS)), "test")
			.expect("answered");
		assert_eq!(shape.columns.len(), MAX_COLUMNS);
		assert_eq!(shape.rows[0].len(), MAX_COLUMNS);
		assert!(
			system_variable_answer(&format!("SELECT {}", list(MAX_COLUMNS + 1)), "test").is_none()
		);
	}

	#[test]
	fn a_column_name_is_at_most_255_bytes() {
		let name = "é".repeat(200);
		let shape = system_variable_answer(&format!("SELECT @@version AS {name}"), "test")
			.expect("answered");
		assert_eq!(shape.columns[0].name.len(), 254);
		let name = "a".repeat(400);
		let shape = system_variable_answer(&format!("SELECT @@version AS {name}"), "test")
			.expect("answered");
		assert_eq!(shape.columns[0].name.len(), MAX_COLUMN_NAME);
	}

	#[test]
	fn a_column_name_is_cut_on_a_character_boundary_wherever_the_limit_falls() {
		// A name of one to four bytes a character, led by zero to three ASCII bytes so that the
		// limit falls on each byte of a character in turn, and long enough to pass the limit.
		for ch in ["a", "\u{e9}", "\u{20ac}", "\u{1f600}"] {
			for lead in 0..4 {
				for count in 0..=(MAX_COLUMN_NAME + 8) / ch.len() + 1 {
					let name = format!("{}{}", "a".repeat(lead), ch.repeat(count));
					let shape =
						system_variable_answer(&format!("SELECT @@version AS `{name}`"), "test")
							.expect("answered");
					let cut = &shape.columns[0].name;
					assert!(name.starts_with(cut.as_str()), "{name:?}");
					assert!(cut.len() <= MAX_COLUMN_NAME, "{name:?}");
					if name.len() <= MAX_COLUMN_NAME {
						assert_eq!(cut, &name);
					} else {
						// The cut drops no more than the character that straddles the limit.
						assert!(cut.len() + ch.len() > MAX_COLUMN_NAME, "{name:?}");
					}
				}
			}
		}
	}

	#[test]
	fn a_variable_mysql_does_not_have_is_not_answered() {
		for sql in [
			"SELECT @@no_such_variable",
			"SELECT @@version, @@no_such_variable AS v",
			"SELECT @@tx_isolation",
			"SELECT @@query_cache_size LIMIT 1",
		] {
			assert!(system_variable_answer(sql, "test").is_none(), "{sql}");
		}
	}

	/// The number and message of the error `sql` is refused with, if it is refused.
	fn refused(sql: &str) -> Option<(u16, [u8; 5], String)> {
		system_variable_refusal(sql).map(|err| (err.error_code, err.sql_state, err.error_message))
	}

	fn unknown(name: &str) -> Option<(u16, [u8; 5], String)> {
		Some((1193, *b"HY000", format!("Unknown system variable '{name}'")))
	}

	#[test]
	fn a_statement_that_names_a_variable_mysql_does_not_have_is_refused_as_mysql_does() {
		for (sql, name) in [
			("SELECT @@no_such_variable", "no_such_variable"),
			("select @@NoSuch limit 1", "NoSuch"),
			("SELECT @@global.nosuch, @@version", "nosuch"),
			("SELECT @@version, @@session.nosuch", "nosuch"),
			("SELECT @@`a b`", "a b"),
			("SELECT 1 WHERE 0 AND @@nosuch", "nosuch"),
			("SELECT * FROM t WHERE c = @@nosuch", "nosuch"),
			("SELECT 1 /*! , @@nosuch */", "nosuch"),
			("INSERT INTO t VALUES (@@nosuch)", "nosuch"),
			("DO @@nosuch", "nosuch"),
			// The first of two names is the one refused.
			("SELECT @@first, @@second", "first"),
			// Names MySQL 8.0.3 removed.
			("SELECT @@tx_isolation", "tx_isolation"),
			("SELECT @@query_cache_size", "query_cache_size"),
		] {
			assert_eq!(refused(sql), unknown(name), "{sql}");
		}
		let long = format!("SELECT @@{}", "a".repeat(1000));
		assert_eq!(refused(&long), unknown(&"a".repeat(64)));
	}

	#[test]
	fn the_name_an_error_repeats_is_cut_on_a_character_boundary_wherever_the_limit_falls() {
		// A name of one to four bytes a character, led by zero to three ASCII bytes so that the
		// limit of 64 bytes falls on each byte of a character in turn, and long enough to pass it.
		for ch in ["a", "\u{e9}", "\u{20ac}", "\u{1f600}"] {
			for lead in 0..4 {
				for count in 0..=70 {
					let name = format!("zz{}{}", "z".repeat(lead), ch.repeat(count));
					let (code, _, message) =
						refused(&format!("SELECT @@`{name}`")).expect("refused as unknown");
					assert_eq!(code, 1193, "{name:?}");
					let shown = message
						.strip_prefix("Unknown system variable '")
						.and_then(|rest| rest.strip_suffix('\''))
						.expect("the message names the variable");
					assert!(name.starts_with(shown), "{name:?}");
					assert!(shown.len() <= 64, "{name:?}");
					if name.len() <= 64 {
						assert_eq!(shown, name);
					} else {
						// The cut drops no more than the character that straddles the limit.
						assert!(shown.len() + ch.len() > 64, "{name:?}");
					}
				}
			}
		}
	}

	#[test]
	fn a_global_read_of_a_variable_only_a_session_has_is_refused_as_mysql_does() {
		for (sql, name) in [
			("SELECT @@global.timestamp", "timestamp"),
			("SELECT @@version, @@GLOBAL.SQL_LOG_BIN", "sql_log_bin"),
		] {
			assert_eq!(
				refused(sql),
				Some((1238, *b"HY000", format!("Variable '{name}' is a SESSION variable"))),
				"{sql}"
			);
		}
	}

	#[test]
	fn a_statement_with_no_such_reference_is_not_refused() {
		for sql in [
			"SELECT 1",
			"SELECT @@version, @@session.autocommit, @@global.max_connections",
			"SELECT @@timestamp, @@session.timestamp, @@local.sql_log_bin",
			"SELECT @@have_ssl, @@expire_logs_days",
			"SELECT @@`version`",
			"SELECT '@@nosuch'",
			"SELECT \"@@nosuch\"",
			"SELECT 1 /* @@nosuch */",
			"SELECT 1 -- @@nosuch",
			"SELECT 1 # @@nosuch",
			"SELECT @nosuch",
			"SELECT `@@nosuch`",
			"SELECT 1 AS `@@nosuch`",
			"",
			// A statement that does not lex is left to the parser, which refuses it.
			"SELECT 'unterminated @@nosuch",
			"SELECT @@",
		] {
			assert_eq!(refused(sql), None, "{sql}");
		}
	}

	#[test]
	fn a_set_is_refused_for_an_unknown_target_with_or_without_the_at_signs() {
		for sql in [
			"SET @@nosuch = 1",
			"SET nosuch = 1",
			"SET SESSION nosuch = 1",
			"SET GLOBAL nosuch = 1",
			"SET LOCAL nosuch = 1",
			"SET @@global.nosuch = 1",
			"SET @@SESSION.nosuch = 1",
			"SET autocommit = 1, nosuch = 2",
			"SET nosuch = 2, autocommit = 1",
			"SET autocommit = @@nosuch",
			"SET @a = @@nosuch",
			"Set nosuch = 1;",
		] {
			assert_eq!(refused(sql), unknown("nosuch"), "{sql}");
		}
	}

	#[test]
	fn a_set_of_variables_mysql_has_is_not_refused_whatever_the_scope() {
		for sql in [
			"SET NAMES utf8mb4",
			"set names 'utf8mb4'",
			"SET autocommit = 1",
			"SET SESSION autocommit = 0",
			"SET @@session.autocommit = 1",
			"SET @@global.max_connections = 5",
			// The scopes of a `SET` have their own errors, which are not read here.
			"SET @@global.timestamp = 1",
			"SET GLOBAL timestamp = 1",
			"SET @@max_connections = 1, sql_mode = ''",
			"SET @user = 1",
			"SET @user = @@version",
			"SET PASSWORD = 'x'",
			// A word that starts with `set` is no SET statement.
			"SETTINGS",
		] {
			assert_eq!(refused(sql), None, "{sql}");
		}
	}

	#[test]
	fn a_statement_longer_than_the_parser_reads_is_not_read_here() {
		let sql = format!("SELECT @@nosuch{}", " ".repeat(16 * 1024 * 1024));
		assert_eq!(refused(&sql), None);
	}
}
