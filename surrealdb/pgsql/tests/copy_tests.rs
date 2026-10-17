use surrealdb_pgsql::{CopyDirection, CopyFormat, CopyTarget, Statement, parse_statement};

#[test]
fn test_copy_from_stdin_text() {
	let sql = "COPY my_table (id, name, age) FROM STDIN";
	let stmt = parse_statement(sql).expect("parse copy");
	match stmt {
		Statement::Copy(c) => {
			assert_eq!(c.direction, CopyDirection::FromStdin);
			assert_eq!(c.options.format, CopyFormat::Text);
			match c.target {
				CopyTarget::Table {
					table,
					columns,
				} => {
					assert_eq!(table.parts, vec!["my_table"]);
					assert_eq!(columns, vec!["id", "name", "age"]);
				}
				_ => panic!("expected table target"),
			}
		}
		_ => panic!("expected copy statement"),
	}
}

#[test]
fn test_copy_to_stdout_csv_options() {
	let sql = "COPY users TO STDOUT WITH (FORMAT CSV, HEADER true, DELIMITER ',', NULL 'N/A')";
	let stmt = parse_statement(sql).expect("parse copy");
	match stmt {
		Statement::Copy(c) => {
			assert_eq!(c.direction, CopyDirection::ToStdout);
			assert_eq!(c.options.format, CopyFormat::Csv);
			assert_eq!(c.options.header, Some(true));
			assert_eq!(c.options.delimiter, Some(b','));
			assert_eq!(c.options.null.as_deref(), Some("N/A"));
		}
		_ => panic!("expected copy statement"),
	}
}

#[test]
fn test_copy_query_to_stdout() {
	let sql = "COPY (SELECT id, name FROM users WHERE id > 10) TO STDOUT WITH CSV HEADER";
	let stmt = parse_statement(sql).expect("parse copy");
	match stmt {
		Statement::Copy(c) => {
			assert_eq!(c.direction, CopyDirection::ToStdout);
			assert_eq!(c.options.format, CopyFormat::Csv);
			assert_eq!(c.options.header, Some(true));
			match c.target {
				CopyTarget::Query(_) => {}
				_ => panic!("expected query target"),
			}
		}
		_ => panic!("expected copy statement"),
	}
}
