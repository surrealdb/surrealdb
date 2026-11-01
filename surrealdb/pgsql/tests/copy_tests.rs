use surrealdb_pgsql::{CopyDirection, CopyFormat, CopyTarget, Statement, parse_statement};

#[test]
fn test_copy_from_stdin_text() {
	let sql = "COPY my_table (id, name, age) FROM STDIN";
	let stmt = parse_statement(sql).expect("parse copy");
	let Statement::Copy(c) = stmt else {
		panic!("expected copy statement");
	};
	assert_eq!(c.direction, CopyDirection::FromStdin);
	assert_eq!(c.options.format, CopyFormat::Text);
	let CopyTarget::Table {
		table,
		columns,
	} = c.target
	else {
		panic!("expected table target");
	};
	assert_eq!(table.parts, vec!["my_table"]);
	assert_eq!(columns, vec!["id", "name", "age"]);
}

#[test]
fn test_copy_to_stdout_csv_options() {
	let sql = "COPY users TO STDOUT WITH (FORMAT CSV, HEADER true, DELIMITER ',', NULL 'N/A')";
	let stmt = parse_statement(sql).expect("parse copy");
	let Statement::Copy(c) = stmt else {
		panic!("expected copy statement");
	};
	assert_eq!(c.direction, CopyDirection::ToStdout);
	assert_eq!(c.options.format, CopyFormat::Csv);
	assert_eq!(c.options.header, Some(true));
	assert_eq!(c.options.delimiter, Some(b','));
	assert_eq!(c.options.null.as_deref(), Some("N/A"));
}

#[test]
fn test_copy_query_to_stdout() {
	let sql = "COPY (SELECT id, name FROM users WHERE id > 10) TO STDOUT WITH CSV HEADER";
	let stmt = parse_statement(sql).expect("parse copy");
	let Statement::Copy(c) = stmt else {
		panic!("expected copy statement");
	};
	assert_eq!(c.direction, CopyDirection::ToStdout);
	assert_eq!(c.options.format, CopyFormat::Csv);
	assert_eq!(c.options.header, Some(true));
	let CopyTarget::Query(_) = c.target else {
		panic!("expected query target");
	};
}

#[test]
fn test_copy_ignored_option_takes_its_value() {
	let stmt = parse_statement("COPY t FROM STDIN WITH (FREEZE ON, FORMAT CSV, HEADER)")
		.expect("an option with a value the server ignores");
	let Statement::Copy(c) = stmt else {
		panic!("expected copy statement");
	};
	assert_eq!(c.direction, CopyDirection::FromStdin);
	assert_eq!(c.options.format, CopyFormat::Csv);
	assert!(parse_statement("COPY t FROM STDIN WITH (FREEZE true)").is_ok());
	assert!(parse_statement("COPY t FROM STDIN WITH (FREEZE = ON, FORMAT CSV)").is_ok());
}
