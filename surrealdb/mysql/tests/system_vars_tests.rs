//! The tables of MySQL system variables: the names MySQL has with their scopes, the values the
//! server models, and the refusal of a name MySQL does not have. Every name, scope and value was
//! read from a MySQL 8.4.13 server (and the 8.0.35 server for the names of the 8.0 series).

use surrealdb_mysql::system_vars::{
	MYSQL_DEFAULT_COLLATION, MYSQL_SERVER_VERSION, SystemVariable, SystemVariableError,
	VariableScope, check_mysql_system_variable, mysql_system_variable, mysql_system_variable_names,
	mysql_system_variable_scope, mysql_system_variables,
};

#[test]
fn the_names_are_lower_case_ascend_and_each_is_found_in_any_case() {
	let names = mysql_system_variable_names();
	for (name, scope) in names {
		assert!(
			name.bytes()
				.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'),
			"{name} is not a lower case name"
		);
		assert_eq!(mysql_system_variable_scope(name), Some(*scope), "{name}");
		assert_eq!(mysql_system_variable_scope(&name.to_ascii_uppercase()), Some(*scope), "{name}");
	}
	// Strictly ascending also means that no name is listed twice, and it is what a lookup relies
	// on.
	for pair in names.windows(2) {
		assert!(pair[0].0 < pair[1].0, "{} is not listed before {}", pair[0].0, pair[1].0);
	}
}

#[test]
fn the_list_is_the_one_of_a_server_with_no_plugin_installed() {
	let names = mysql_system_variable_names();
	let count = |scope| names.iter().filter(|(_, known)| *known == scope).count();
	assert_eq!(names.len(), 663);
	assert_eq!(count(VariableScope::Session), 24);
	assert_eq!(count(VariableScope::Both), 149);
	assert_eq!(count(VariableScope::Global), 490);

	let session_only: Vec<&str> = names
		.iter()
		.filter(|(_, scope)| *scope == VariableScope::Session)
		.map(|(name, _)| *name)
		.collect();
	assert_eq!(
		session_only,
		[
			"error_count",
			"external_user",
			"gtid_next",
			"identity",
			"immediate_server_version",
			"insert_id",
			"last_insert_id",
			"original_commit_timestamp",
			"original_server_version",
			"proxy_user",
			"pseudo_replica_mode",
			"pseudo_slave_mode",
			"pseudo_thread_id",
			"rand_seed1",
			"rand_seed2",
			"require_row_format",
			"resultset_metadata",
			"show_create_table_skip_secondary_engine",
			"sql_log_bin",
			"statement_id",
			"timestamp",
			"transaction_allow_batching",
			"use_secondary_engine",
			"warning_count",
		]
	);

	for (name, scope) in [
		("autocommit", VariableScope::Both),
		("sql_mode", VariableScope::Both),
		("time_zone", VariableScope::Both),
		("max_allowed_packet", VariableScope::Both),
		("transaction_isolation", VariableScope::Both),
		("version", VariableScope::Global),
		("version_comment", VariableScope::Global),
		("license", VariableScope::Global),
		("init_connect", VariableScope::Global),
		("lower_case_table_names", VariableScope::Global),
		("max_connections", VariableScope::Global),
		("timestamp", VariableScope::Session),
		("sql_log_bin", VariableScope::Session),
	] {
		assert_eq!(mysql_system_variable_scope(name), Some(scope), "{name}");
	}
}

#[test]
fn the_names_of_the_8_0_series_that_8_4_dropped_are_names_the_server_has() {
	for name in [
		"have_ssl",
		"have_openssl",
		"default_authentication_plugin",
		"expire_logs_days",
		"master_info_repository",
		"relay_log_info_repository",
		"transaction_write_set_extraction",
	] {
		assert!(mysql_system_variable_scope(name).is_some(), "{name}");
		assert_eq!(mysql_system_variable(name), None, "{name} is not modelled");
	}
}

#[test]
fn the_names_that_no_version_of_the_8_series_has_are_not_names_the_server_has() {
	// The query cache and the old spellings of the isolation level and the read-only flag went in
	// MySQL 8.0.3, and a driver of a server that announces 8.0.35 reads the new spellings.
	for name in [
		"query_cache_size",
		"query_cache_type",
		"query_cache_limit",
		"tx_isolation",
		"tx_read_only",
		"no_such_variable",
		"",
	] {
		assert_eq!(mysql_system_variable_scope(name), None, "{name}");
		assert_eq!(mysql_system_variable(name), None, "{name}");
	}
}

#[test]
fn the_modelled_variables_are_names_mysql_has_in_ascending_order() {
	let variables = mysql_system_variables();
	for (name, _) in variables {
		assert!(mysql_system_variable_scope(name).is_some(), "{name} is not a name MySQL has");
		assert_eq!(mysql_system_variable(name), mysql_system_variable(&name.to_ascii_uppercase()));
	}
	for pair in variables.windows(2) {
		assert!(pair[0].0 < pair[1].0, "{} is not listed before {}", pair[0].0, pair[1].0);
	}
}

#[test]
fn the_modelled_values_are_those_of_an_unconfigured_server() {
	let text = |name: &str| match mysql_system_variable(name) {
		Some(SystemVariable::Text(value)) => value.to_string(),
		other => panic!("{name} is {other:?}"),
	};
	let int = |name: &str| match mysql_system_variable(name) {
		Some(SystemVariable::Int(value)) => value,
		other => panic!("{name} is {other:?}"),
	};
	let flag = |name: &str| match mysql_system_variable(name) {
		Some(SystemVariable::Flag(on)) => on,
		other => panic!("{name} is {other:?}"),
	};
	assert_eq!(text("version"), MYSQL_SERVER_VERSION);
	assert_eq!(text("collation_server"), MYSQL_DEFAULT_COLLATION);
	assert_eq!(text("character_set_filesystem"), "binary");
	assert_eq!(text("character_set_system"), "utf8mb3");
	assert_eq!(text("default_storage_engine"), "InnoDB");
	assert_eq!(text("time_zone"), "SYSTEM");
	assert_eq!(text("transaction_isolation"), "REPEATABLE-READ");
	assert_eq!(int("max_allowed_packet"), 67_108_864);
	assert_eq!(int("net_buffer_length"), 16_384);
	assert_eq!(int("wait_timeout"), 28_800);
	assert!(flag("autocommit"));
	assert!(flag("session_track_schema"));
	assert!(!flag("sql_auto_is_null"));
	assert!(!flag("transaction_read_only"));
	assert!(!flag("session_track_state_change"));
}

#[test]
fn a_switch_is_listed_as_on_or_off_and_any_other_value_as_its_text() {
	let listed = |name: &str| mysql_system_variable(name).map(SystemVariable::listed);
	assert_eq!(listed("autocommit").as_deref(), Some("ON"));
	assert_eq!(listed("sql_auto_is_null").as_deref(), Some("OFF"));
	assert_eq!(listed("max_allowed_packet").as_deref(), Some("67108864"));
	assert_eq!(listed("lower_case_table_names").as_deref(), Some("0"));
	assert_eq!(listed("time_zone").as_deref(), Some("SYSTEM"));
	assert_eq!(listed("init_connect").as_deref(), Some(""));
	assert_eq!(listed("no_such_variable"), None);
}

#[test]
fn a_name_mysql_does_not_have_is_refused_in_every_scope() {
	for (name, global) in [
		("no_such_variable", false),
		("no_such_variable", true),
		("NO_SUCH_VARIABLE", false),
		("tx_isolation", false),
		("query_cache_size", true),
	] {
		let error = check_mysql_system_variable(name, global).expect_err(name);
		assert_eq!(error, SystemVariableError::Unknown(name.to_string()), "{name}");
		assert_eq!(error.mysql_error_code(), 1193);
		assert_eq!(error.to_string(), format!("Unknown system variable '{name}'"));
	}
}

#[test]
fn the_name_an_error_repeats_is_cut_at_64_bytes_on_a_character_boundary() {
	let shown = |name: &str| match check_mysql_system_variable(name, false) {
		Err(SystemVariableError::Unknown(shown)) => shown,
		other => panic!("{other:?}"),
	};
	assert_eq!(shown(&"a".repeat(63)).len(), 63);
	assert_eq!(shown(&"a".repeat(64)).len(), 64);
	assert_eq!(shown(&"a".repeat(65)).len(), 64);
	assert_eq!(shown(&"a".repeat(1 << 20)).len(), 64);
	// Two bytes a character: 32 characters are 64 bytes.
	assert_eq!(shown(&"é".repeat(40)), "é".repeat(32));
	// Three bytes a character: 21 characters are 63 bytes and a 22nd would end at 66.
	assert_eq!(shown(&"€".repeat(30)), "€".repeat(21));
	let message = check_mysql_system_variable(&"a".repeat(500), true).expect_err("refused");
	assert!(message.to_string().len() < 100);
}

#[test]
fn a_global_read_of_a_variable_only_a_session_has_is_refused() {
	for name in ["timestamp", "sql_log_bin", "warning_count", "TIMESTAMP"] {
		let error = check_mysql_system_variable(name, true).expect_err(name);
		assert_eq!(error, SystemVariableError::SessionOnly(name.to_ascii_lowercase()), "{name}");
		assert_eq!(error.mysql_error_code(), 1238);
		assert_eq!(
			error.to_string(),
			format!("Variable '{}' is a SESSION variable", name.to_ascii_lowercase())
		);
		// Without the scope, or through the session, the variable is read.
		assert_eq!(check_mysql_system_variable(name, false), Ok(()), "{name}");
	}
}

#[test]
fn a_name_mysql_has_is_accepted_whatever_its_scope_and_whether_or_not_the_server_models_it() {
	for (name, _) in mysql_system_variable_names() {
		assert_eq!(check_mysql_system_variable(name, false), Ok(()), "@@{name}");
	}
	for (name, scope) in mysql_system_variable_names() {
		let accepted = check_mysql_system_variable(name, true).is_ok();
		assert_eq!(accepted, *scope != VariableScope::Session, "@@global.{name}");
	}
}
