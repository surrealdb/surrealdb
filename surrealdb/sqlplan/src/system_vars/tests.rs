//! The tables of system variables as the planner reads them: each variable's value and the type of
//! its result column agree, every spelling of a name reads the same variable, the table holds
//! nothing about the host, and a name MySQL does not have is refused.

use surrealdb_expr::expr::logical::{LogicalExpr, LogicalLiteral};
use surrealdb_mysql::system_vars::mysql_system_variables;
use surrealdb_strand::Strand;

use super::{MYSQL_SERVER_VERSION, SystemVariable, mysql_system_variable};
use crate::binder::mysql::bind_select;
use crate::catalog::EmptyCatalog;
use crate::descriptor::SqlType;
use crate::dialect::DialectPolicy;
use crate::error::PlanError;
use crate::lower::expr::lower_bound_expr;

/// What binding `sql` gives, which is an error for a statement the planner refuses.
fn bind(sql: &str) -> Result<crate::ir::BoundSelect, PlanError> {
	let statements = surrealdb_mysql::parse_statements(sql).expect("parse");
	let surrealdb_mysql::ast::stmt::Statement::Select(select) = &statements[0] else {
		panic!("expected SELECT");
	};
	bind_select(select, &EmptyCatalog, &DialectPolicy::mysql(), None)
}

/// What `SELECT <item>` reads: the type of its one result column and the literal it lowers to.
fn read(item: &str) -> (SqlType, LogicalExpr) {
	let policy = DialectPolicy::mysql();
	let bound = bind(&format!("SELECT {item}")).expect("bind");
	let descriptor = bound.describe(&EmptyCatalog, &policy);
	assert_eq!(descriptor.columns.len(), 1);
	let lowered = lower_bound_expr(&bound.targets[0].expr, &policy).expect("lower");
	(descriptor.columns[0].ty, lowered)
}

fn literal(value: LogicalLiteral) -> LogicalExpr {
	LogicalExpr::Literal(value)
}

#[test]
fn a_variable_is_typed_as_the_value_it_answers() {
	for (name, variable) in mysql_system_variables() {
		let (ty, lowered) = read(&format!("@@{name}"));
		match variable {
			SystemVariable::Int(value) => {
				assert_eq!(ty, SqlType::Int8, "@@{name}");
				assert_eq!(lowered, literal(LogicalLiteral::Integer(*value)), "@@{name}");
			}
			SystemVariable::Flag(on) => {
				assert_eq!(ty, SqlType::Int8, "@@{name}");
				assert_eq!(lowered, literal(LogicalLiteral::Integer(i64::from(*on))), "@@{name}");
			}
			SystemVariable::Text(value) => {
				assert_eq!(ty, SqlType::Text, "@@{name}");
				assert_eq!(
					lowered,
					literal(LogicalLiteral::String(Strand::from(*value))),
					"@@{name}"
				);
			}
		}
	}
}

#[test]
fn the_names_of_the_table_are_lower_case_and_ascend() {
	let variables = mysql_system_variables();
	for (name, _) in variables {
		assert_eq!(*name, name.to_ascii_lowercase());
	}
	// Strictly ascending also means that no name is listed twice.
	for pair in variables.windows(2) {
		assert!(pair[0].0 < pair[1].0, "{} is not listed before {}", pair[0].0, pair[1].0);
	}
}

#[test]
fn every_spelling_of_a_name_reads_the_same_variable() {
	let (expected_type, expected) = read("@@max_allowed_packet");
	for spelling in [
		"@@MAX_ALLOWED_PACKET",
		"@@Max_Allowed_Packet",
		"@@session.max_allowed_packet",
		"@@SESSION.max_allowed_packet",
		"@@local.max_allowed_packet",
		"@@LOCAL.MAX_ALLOWED_PACKET",
		"@@global.max_allowed_packet",
		"@@GLOBAL.Max_Allowed_Packet",
		"@@`max_allowed_packet`",
		"@@global.`max_allowed_packet`",
	] {
		let (ty, lowered) = read(spelling);
		assert_eq!(ty, expected_type, "{spelling}");
		assert_eq!(lowered, expected, "{spelling}");
	}
}

#[test]
fn a_variable_mysql_has_and_the_table_does_not_hold_reads_as_null_text() {
	for item in [
		"@@max_connections",
		"@@global.max_connections",
		"@@session.MAX_CONNECTIONS",
		"@@have_ssl",
		"@@sql_log_bin",
		"@@global.sql_log_bin",
	] {
		let (ty, lowered) = read(item);
		assert_eq!(ty, SqlType::Text, "{item}");
		assert_eq!(lowered, literal(LogicalLiteral::Null), "{item}");
	}
}

#[test]
fn a_variable_mysql_does_not_have_is_refused_wherever_it_is_written() {
	for sql in [
		"SELECT @@no_such_variable",
		"SELECT @@global.no_such_variable",
		"SELECT @@session.NO_SUCH_VARIABLE",
		"SELECT @@`no_such_variable`",
		"SELECT 1 WHERE @@no_such_variable = 1",
		"SELECT 1 WHERE 0 AND @@no_such_variable",
		"SELECT COALESCE(@@no_such_variable, 1)",
		"SELECT CASE WHEN 1 = 0 THEN @@no_such_variable ELSE 2 END",
		"SELECT (SELECT @@no_such_variable)",
		"SELECT 1 FROM (SELECT @@no_such_variable AS v) AS t",
		"SELECT @@version, @@autocommit, @@no_such_variable",
		// Names MySQL 8.0.3 and later do not have.
		"SELECT @@tx_isolation",
		"SELECT @@tx_read_only",
		"SELECT @@query_cache_size",
		"SELECT @@query_cache_type",
	] {
		let Err(PlanError::UndefinedObject(shown)) = bind(sql) else {
			panic!("{sql} is refused as an unknown variable");
		};
		assert!(sql.to_ascii_lowercase().contains(&shown.to_ascii_lowercase()), "{sql}: {shown}");
	}
}

#[test]
fn the_name_a_refusal_repeats_is_at_most_64_bytes() {
	let Err(PlanError::UndefinedObject(shown)) = bind(&format!("SELECT @@{}", "a".repeat(5000)))
	else {
		panic!("refused");
	};
	assert_eq!(shown, "a".repeat(64));
	let Err(PlanError::UndefinedObject(shown)) = bind(&format!("SELECT @@`{}`", "é".repeat(100)))
	else {
		panic!("refused");
	};
	assert_eq!(shown, "é".repeat(32));
	// The refusal carries the number MySQL gives it.
	let error = PlanError::UndefinedObject("no_such_variable".to_string());
	assert_eq!(error.mysql_error_code(), 1193);
}

#[test]
fn a_variable_of_mysql_is_accepted_in_every_scope_and_spelling() {
	for sql in [
		"SELECT @@version",
		"SELECT @@Version, @@global.VERSION_COMMENT, @@session.sql_mode, @@local.autocommit",
		"SELECT @@`sql_mode`",
		// A name only a session has can be read through the session or with no scope.
		"SELECT @@timestamp, @@session.timestamp, @@local.sql_log_bin",
		// A user variable is not a system variable, whatever its name.
		"SELECT @no_such_variable",
	] {
		bind(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
	}
}

#[test]
fn the_table_holds_nothing_about_the_host() {
	// What MySQL answers for these names describes the machine it runs on.
	for name in [
		"datadir",
		"basedir",
		"tmpdir",
		"plugin_dir",
		"lc_messages_dir",
		"socket",
		"pid_file",
		"log_error",
		"general_log_file",
		"slow_query_log_file",
		"secure_file_priv",
		"hostname",
		"report_host",
		"port",
		"bind_address",
		"mysqlx_port",
		"version_compile_os",
		"version_compile_machine",
		"ssl_ca",
		"ssl_cert",
		"ssl_key",
	] {
		assert_eq!(mysql_system_variable(name), None, "@@{name}");
		assert_eq!(read(&format!("@@{name}")).1, literal(LogicalLiteral::Null), "@@{name}");
	}
	// No answer is a path or the name of the platform the server was built for.
	for (name, variable) in mysql_system_variables() {
		if let SystemVariable::Text(value) = variable {
			assert!(!value.contains(['/', '\\']), "@@{name} answers a path");
			assert!(
				!value.eq_ignore_ascii_case(std::env::consts::OS)
					&& !value.eq_ignore_ascii_case(std::env::consts::ARCH),
				"@@{name} answers the platform"
			);
		}
	}
}

#[test]
fn the_version_is_one_value_wherever_it_is_read() {
	let version = literal(LogicalLiteral::String(Strand::from(MYSQL_SERVER_VERSION)));
	assert_eq!(read("@@version").1, version);
	assert_eq!(read("VERSION()").1, version);
}
