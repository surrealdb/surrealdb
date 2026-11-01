//! `SHOW` and `DESCRIBE`: what they parse to and what they translate to.

use surrealdb_mysql::ast::stmt::{DescribeStmt, ShowStmt, Statement};
use surrealdb_mysql::translate::{ShowColumn, ShowColumnKind, show_columns};
use surrealdb_mysql::{TranslationError, parse_statement, translate_sql};

fn parse(sql: &str) -> Statement {
	parse_statement(sql).unwrap_or_else(|e| panic!("`{sql}` did not parse: {e}"))
}

fn column_names(sql: &str, database: &str) -> Vec<String> {
	let columns: Vec<ShowColumn> = show_columns(&parse(sql), database)
		.unwrap_or_else(|| panic!("`{sql}` has no columns of its own"));
	columns.into_iter().map(|column| column.name).collect()
}

/// `surql` without the content of its quoted identifiers and string literals.
fn outside_quotes(surql: &str) -> String {
	let mut out = String::new();
	let mut quote = None;
	let mut chars = surql.chars();
	while let Some(c) = chars.next() {
		match quote {
			Some(open) => {
				if c == '\\' {
					chars.next();
				} else if c == open {
					quote = None;
				}
			}
			None => {
				if matches!(c, '\'' | '"' | '`') {
					quote = Some(c);
				} else {
					out.push(c);
				}
			}
		}
	}
	assert!(quote.is_none(), "a quote is left open in {surql}");
	out
}

#[test]
fn show_and_describe_name_their_columns_as_mysql_does() {
	let table = ["Field", "Type", "Null", "Key", "Default", "Extra"];
	assert_eq!(column_names("SHOW TABLES", "shop"), ["Tables_in_shop"]);
	assert_eq!(column_names("SHOW FULL TABLES", "shop"), ["Tables_in_shop", "Table_type"]);
	assert_eq!(column_names("SHOW DATABASES", "shop"), ["Database"]);
	assert_eq!(column_names("SHOW SCHEMAS", "shop"), ["Database"]);
	assert_eq!(column_names("SHOW COLUMNS FROM t", "shop"), table);
	assert_eq!(column_names("DESCRIBE t", "shop"), table);
	assert_eq!(
		column_names("SHOW FULL COLUMNS FROM t", "shop"),
		["Field", "Type", "Collation", "Null", "Key", "Default", "Extra", "Privileges", "Comment"]
	);
	assert_eq!(column_names("SHOW CREATE TABLE t", "shop"), ["Table", "Create Table"]);
	assert_eq!(
		column_names("SHOW INDEX FROM t", "shop"),
		[
			"Table",
			"Non_unique",
			"Key_name",
			"Seq_in_index",
			"Column_name",
			"Collation",
			"Cardinality",
			"Sub_part",
			"Packed",
			"Null",
			"Index_type",
			"Comment",
			"Index_comment",
			"Visible",
			"Expression"
		]
	);
	assert_eq!(column_names("SHOW VARIABLES", "shop"), ["Variable_name", "Value"]);
	assert_eq!(column_names("SHOW STATUS", "shop"), ["Variable_name", "Value"]);
	assert_eq!(
		column_names("SHOW PROCESSLIST", "shop"),
		["Id", "User", "Host", "db", "Command", "Time", "State", "Info"]
	);
	assert_eq!(column_names("SHOW WARNINGS", "shop"), ["Level", "Code", "Message"]);
	assert_eq!(column_names("SHOW ERRORS", "shop"), ["Level", "Code", "Message"]);
	assert_eq!(
		column_names("SHOW ENGINES", "shop"),
		["Engine", "Support", "Comment", "Transactions", "XA", "Savepoints"]
	);
	assert_eq!(
		column_names("SHOW COLLATION", "shop"),
		["Collation", "Charset", "Id", "Default", "Compiled", "Sortlen", "Pad_attribute"]
	);
	assert_eq!(
		column_names("SHOW CHARACTER SET", "shop"),
		["Charset", "Description", "Default collation", "Maxlen"]
	);
}

#[test]
fn the_integer_columns_of_a_show_result_are_typed_as_mysql_types_them() {
	let kinds = |sql: &str| -> Vec<(String, ShowColumnKind)> {
		show_columns(&parse(sql), "shop")
			.expect("columns")
			.into_iter()
			.map(|column| (column.name, column.kind))
			.collect()
	};
	let index = kinds("SHOW INDEX FROM t");
	let kind_of = |name: &str| index.iter().find(|(n, _)| n == name).expect(name).1;
	assert_eq!(kind_of("Table"), ShowColumnKind::Text);
	assert_eq!(kind_of("Non_unique"), ShowColumnKind::Int);
	assert_eq!(kind_of("Seq_in_index"), ShowColumnKind::Int);
	assert_eq!(kind_of("Cardinality"), ShowColumnKind::BigInt);
	let processlist = kinds("SHOW PROCESSLIST");
	assert_eq!(processlist[0], ("Id".to_string(), ShowColumnKind::BigInt));
	assert_eq!(processlist[5], ("Time".to_string(), ShowColumnKind::Int));
	let collation = kinds("SHOW COLLATION");
	assert_eq!(collation[2], ("Id".to_string(), ShowColumnKind::BigInt));
	assert_eq!(collation[5], ("Sortlen".to_string(), ShowColumnKind::Int));
}

#[test]
fn a_statement_that_is_not_a_show_has_no_show_columns() {
	for sql in ["SELECT 1", "INSERT INTO t VALUES (1)", "USE shop", "EXPLAIN SELECT 1"] {
		assert!(show_columns(&parse(sql), "shop").is_none(), "{sql}");
	}
}

#[test]
fn a_name_reaches_the_expression_only_inside_quotes() {
	// A table name that tries to close its quotes and start another statement.
	for sql in [
		"SHOW COLUMNS FROM `a``'; REMOVE TABLE b; `",
		"SHOW FULL COLUMNS FROM `a``'; REMOVE TABLE b; `",
		"DESCRIBE `a``'; REMOVE TABLE b; `",
		"SHOW INDEX FROM `a``'; REMOVE TABLE b; `",
		"SHOW CREATE TABLE `a``'; REMOVE TABLE b; `",
		"SHOW COLUMNS FROM t FROM `a``'; REMOVE TABLE b; `",
		"SHOW TABLES FROM `a``'; REMOVE TABLE b; `",
		"SHOW TABLES LIKE 'a''; REMOVE TABLE b; '",
		"SHOW COLUMNS FROM t LIKE '\\\\''; REMOVE TABLE b; '",
		"SHOW VARIABLES LIKE '\\\\''; REMOVE TABLE b; '",
	] {
		let surql = translate_sql(sql).unwrap_or_else(|e| panic!("`{sql}`: {e}"));
		let code = outside_quotes(&surql);
		assert!(!code.contains("REMOVE"), "`{sql}` translates to {surql}");
	}

	// The table name is one quoted identifier, with its backtick and backslash escaped.
	let surql = translate_sql("SHOW COLUMNS FROM `a``b\\c`").unwrap();
	assert!(surql.contains("INFO FOR TABLE `a\\`b\\\\c` STRUCTURE"), "{surql}");
}

#[test]
fn like_patterns_match_as_mysql_does() {
	// `%` is any run, `_` any one character, a backslash makes the next character literal and a
	// character that is special to a regular expression stands for itself.
	let surql = translate_sql("SHOW TABLES LIKE 'x_y%z.w'").unwrap();
	assert!(surql.contains(r"string::matches($t, <regex> '(?s)^x.y.*z\\.w$')"), "{surql}");
	let surql = translate_sql(r"SHOW TABLES LIKE 'a\\_b\\%c\\\\d'").unwrap();
	assert!(surql.contains(r"'(?s)^a_b%c\\\\d$'"), "{surql}");

	// A backslash that ends the pattern has nothing to escape and stands for itself: `ab\` is
	// the name `ab` followed by a backslash, never the name `ab`.
	let surql = translate_sql(r"SHOW TABLES LIKE 'ab\\'").unwrap();
	assert!(surql.contains(r"'(?s)^ab\\\\$'"), "{surql}");
	let surql = translate_sql(r"SHOW TABLES LIKE 'ab%\\'").unwrap();
	assert!(surql.contains(r"'(?s)^ab.*\\\\$'"), "{surql}");
	let surql = translate_sql(r"SHOW VARIABLES LIKE '\\'").unwrap();
	assert!(surql.contains(r"'(?is)^\\\\$'"), "{surql}");
	// A pair of backslashes is one literal backslash and an escaped wildcard is a literal one.
	let surql = translate_sql(r"SHOW TABLES LIKE 'ab\\\\'").unwrap();
	assert!(surql.contains(r"'(?s)^ab\\\\$'"), "{surql}");
	let surql = translate_sql(r"SHOW TABLES LIKE 'ab\\%'").unwrap();
	assert!(surql.contains(r"'(?s)^ab%$'"), "{surql}");

	// A run of `%` is one `%`.
	let surql = translate_sql("SHOW TABLES LIKE '%%%a%%'").unwrap();
	assert!(surql.contains("'(?s)^.*a.*$'"), "{surql}");

	// A brace is a character of the name, not the start or end of a repetition: `a{1}` is the
	// name `a{1}`, never the name `a`, and a lone brace is never a syntax error.
	let surql = translate_sql("SHOW TABLES LIKE 'a{1}'").unwrap();
	assert!(surql.contains(r"'(?s)^a\\{1\\}$'"), "{surql}");
	let surql = translate_sql("SHOW TABLES LIKE 'a{2,3}'").unwrap();
	assert!(surql.contains(r"'(?s)^a\\{2,3\\}$'"), "{surql}");
	let surql = translate_sql("SHOW TABLES LIKE '{'").unwrap();
	assert!(surql.contains(r"'(?s)^\\{$'"), "{surql}");
	let surql = translate_sql("SHOW TABLES LIKE '}'").unwrap();
	assert!(surql.contains(r"'(?s)^\\}$'"), "{surql}");

	// Tables and databases are matched as written; columns, variables, collations and
	// character sets ignore case.
	assert!(translate_sql("SHOW DATABASES LIKE 'a'").unwrap().contains("'(?s)^a$'"));
	assert!(translate_sql("SHOW COLUMNS FROM t LIKE 'a'").unwrap().contains("'(?is)^a$'"));
	assert!(translate_sql("DESCRIBE t a").unwrap().contains("'(?is)^a$'"));
	assert!(translate_sql("SHOW VARIABLES LIKE 'a'").unwrap().contains("'(?is)^a$'"));
	assert!(translate_sql("SHOW COLLATION LIKE 'a'").unwrap().contains("'(?is)^a$'"));
	assert!(translate_sql("SHOW CHARACTER SET LIKE 'a'").unwrap().contains("'(?is)^a$'"));
}

#[test]
fn show_variables_lists_a_switch_as_on_or_off_and_a_number_as_its_text() {
	let surql = translate_sql("SHOW VARIABLES").unwrap();
	assert!(surql.contains("{ Variable_name: 'autocommit', Value: 'ON' }"), "{surql}");
	assert!(surql.contains("{ Variable_name: 'sql_auto_is_null', Value: 'OFF' }"), "{surql}");
	assert!(
		surql.contains("{ Variable_name: 'max_allowed_packet', Value: '67108864' }"),
		"{surql}"
	);
	// What MySQL 8.0.3 removed is not listed.
	for name in ["tx_isolation", "tx_read_only", "query_cache_size", "query_cache_type"] {
		assert!(!surql.contains(name), "{name} is listed: {surql}");
	}
}

#[test]
fn a_like_pattern_is_bounded() {
	let longest = format!("SHOW TABLES LIKE '{}'", "é".repeat(512));
	translate_sql(&longest).expect("a pattern of 512 characters is accepted");

	let too_long = format!("SHOW TABLES LIKE '{}'", "é".repeat(513));
	assert!(matches!(translate_sql(&too_long), Err(TranslationError::Unsupported(_))));
}

#[test]
fn a_database_other_than_the_sessions_is_refused_when_the_statement_runs() {
	// The expression compares the database to the session's, whatever the case, and refuses
	// before it reads anything.
	for (sql, qualified) in [
		("SHOW TABLES FROM other", "other"),
		("SHOW TABLES IN other", "other"),
		("SHOW COLUMNS FROM other.t", "other.t"),
		("SHOW COLUMNS FROM t FROM other", "other.t"),
		("SHOW INDEX FROM t IN other", "other.t"),
		("SHOW CREATE TABLE other.t", "other.t"),
		("DESCRIBE other.t", "other.t"),
	] {
		let surql = translate_sql(sql).unwrap();
		assert!(
			surql.contains(
				"IF string::lowercase(session::db() ?? '') != string::lowercase('other')"
			),
			"{sql}: {surql}"
		);
		assert!(
			surql.contains(&format!("THROW 'cross-database queries not supported: {qualified}'")),
			"{sql}: {surql}"
		);
	}
	for sql in ["SHOW TABLES", "SHOW COLUMNS FROM t", "SHOW INDEX FROM t", "DESCRIBE t"] {
		assert!(!translate_sql(sql).unwrap().contains("session::db() ??"), "{sql}");
	}
}

#[test]
fn a_table_name_with_more_than_a_database_and_a_table_is_refused() {
	for sql in ["SHOW COLUMNS FROM a.b.c", "DESCRIBE a.b.c", "SHOW CREATE TABLE a.b.c"] {
		assert_eq!(
			translate_sql(sql),
			Err(TranslationError::Unsupported(
				"database qualification depth not supported: a.b.c".to_string()
			)),
			"{sql}"
		);
	}
}

#[test]
fn a_table_that_does_not_exist_is_refused_when_the_statement_runs() {
	for sql in ["SHOW COLUMNS FROM t", "DESCRIBE t", "SHOW INDEX FROM t", "SHOW CREATE TABLE t"] {
		assert!(
			translate_sql(sql).unwrap().contains(
				"IF (INFO FOR DB).tables['t'] = NONE { THROW 'Table \\'t\\' doesn\\'t exist' }"
			),
			"{sql}"
		);
	}
}

#[test]
fn statements_that_report_nothing_answer_with_an_empty_array() {
	for sql in [
		"SHOW STATUS",
		"SHOW STATUS LIKE 'Uptime'",
		"SHOW PROCESSLIST",
		"SHOW WARNINGS",
		"SHOW ERRORS",
	] {
		assert_eq!(translate_sql(sql).unwrap(), "RETURN [];", "{sql}");
	}
}

#[test]
fn show_columns_names_every_key_of_the_rows_of_a_statement_with_fixed_rows() {
	// The keys of a fixed row are its column names, so a result set can be laid out in order.
	for (sql, keys) in [
		("SHOW ENGINES", &["Engine", "Support", "Comment", "Transactions", "XA", "Savepoints"][..]),
		(
			"SHOW COLLATION",
			&["Collation", "Charset", "Id", "Default", "Compiled", "Sortlen", "Pad_attribute"][..],
		),
		("SHOW CHARACTER SET", &["Charset", "Description", "'Default collation'", "Maxlen"][..]),
		("SHOW VARIABLES", &["Variable_name", "Value"][..]),
	] {
		let surql = translate_sql(sql).unwrap();
		for key in keys {
			assert!(surql.contains(&format!("{key}:")), "{sql}: {surql}");
		}
	}
}

#[test]
fn show_columns_and_show_fields_are_one_statement() {
	for sql in
		["SHOW COLUMNS FROM t", "SHOW COLUMNS IN t", "SHOW FIELDS FROM t", "show fields in t"]
	{
		let Statement::Show(ShowStmt::Columns {
			full,
			table,
			like,
			..
		}) = parse(sql)
		else {
			panic!("`{sql}` is not SHOW COLUMNS");
		};
		assert!(!full);
		assert_eq!(table.parts, ["t"]);
		assert_eq!(like, None);
	}
}

#[test]
fn a_database_after_the_table_qualifies_it() {
	for (sql, parts) in [
		("SHOW COLUMNS FROM t FROM db", ["db", "t"]),
		("SHOW FULL COLUMNS IN t IN db", ["db", "t"]),
		("SHOW COLUMNS FROM `t` FROM `db`", ["db", "t"]),
	] {
		let Statement::Show(ShowStmt::Columns {
			table,
			..
		}) = parse(sql)
		else {
			panic!("`{sql}` is not SHOW COLUMNS");
		};
		assert_eq!(table.parts, parts, "{sql}");
	}
	let Statement::Show(ShowStmt::Index {
		table,
		..
	}) = parse("SHOW INDEX FROM t FROM db")
	else {
		panic!("not SHOW INDEX");
	};
	assert_eq!(table.parts, ["db", "t"]);

	// A database named twice is no statement.
	for sql in ["SHOW COLUMNS FROM a.t FROM b", "SHOW INDEX FROM a.t IN b"] {
		assert!(parse_statement(sql).is_err(), "{sql}");
	}
}

#[test]
fn show_tables_takes_from_or_in() {
	for sql in ["SHOW TABLES FROM db", "SHOW FULL TABLES IN db LIKE 'a%'"] {
		let Statement::Show(ShowStmt::Tables {
			database,
			..
		}) = parse(sql)
		else {
			panic!("`{sql}` is not SHOW TABLES");
		};
		assert_eq!(database.as_deref(), Some("db"), "{sql}");
	}
}

#[test]
fn show_index_keys_and_indexes_are_one_statement() {
	for sql in ["SHOW INDEX FROM t", "SHOW INDEXES FROM t", "SHOW KEYS FROM t", "SHOW KEYS IN t"] {
		assert!(matches!(parse(sql), Statement::Show(ShowStmt::Index { .. })), "{sql}");
	}
}

#[test]
fn explain_a_table_is_describe_and_explain_a_statement_is_not() {
	let Statement::Describe(DescribeStmt {
		table,
		column,
		..
	}) = parse("EXPLAIN t")
	else {
		panic!("`EXPLAIN t` is not DESCRIBE");
	};
	assert_eq!(table.parts, ["t"]);
	assert_eq!(column, None);
	assert!(matches!(
		parse("EXPLAIN t c"),
		Statement::Describe(DescribeStmt {
			column: Some(_),
			..
		})
	));
	assert!(matches!(parse("EXPLAIN SELECT 1"), Statement::Explain(_)));
	assert!(matches!(parse("EXPLAIN DELETE FROM t"), Statement::Explain(_)));
}

#[test]
fn show_columns_needs_from_or_in() {
	assert!(parse_statement("SHOW COLUMNS t").is_err());
	assert!(parse_statement("SHOW COLUMNS").is_err());
}

#[test]
fn show_table_status_takes_from_or_in_and_like() {
	for sql in
		["SHOW TABLE STATUS", "SHOW TABLE STATUS FROM db LIKE 'a%'", "SHOW TABLE STATUS IN db"]
	{
		assert!(matches!(parse(sql), Statement::Show(ShowStmt::TableStatus { .. })), "{sql}");
	}
	let columns = column_names("SHOW TABLE STATUS", "shop");
	assert_eq!(columns.first().map(String::as_str), Some("Name"));
	assert_eq!(columns.last().map(String::as_str), Some("Comment"));
	assert_eq!(columns.len(), 18);
}

#[test]
fn show_triggers_answers_with_its_columns() {
	for sql in ["SHOW TRIGGERS", "SHOW TRIGGERS LIKE 'person'", "SHOW TRIGGERS FROM db"] {
		assert!(matches!(parse(sql), Statement::Show(ShowStmt::Triggers { .. })), "{sql}");
	}
	assert_eq!(column_names("SHOW TRIGGERS", "shop").len(), 11);
}

#[test]
fn a_select_skips_its_cache_hints() {
	for sql in [
		"SELECT SQL_NO_CACHE * FROM t",
		"SELECT /*!40001 SQL_NO_CACHE */ * FROM t",
		"SELECT DISTINCT HIGH_PRIORITY SQL_BIG_RESULT a FROM t",
	] {
		assert!(matches!(parse(sql), Statement::Select(_)), "{sql}");
	}
	assert_eq!(
		translate_sql("SELECT SQL_NO_CACHE a FROM t").expect("translates"),
		translate_sql("SELECT a FROM t").expect("translates")
	);
}
