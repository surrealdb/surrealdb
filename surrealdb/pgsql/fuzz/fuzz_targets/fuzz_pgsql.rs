#![no_main]

use libfuzzer_sys::fuzz_target;
use surrealdb_pgsql::{ToPgSql, parse_expr, parse_statements, split_statements};

fuzz_target!(|data: &str| {
	// 1. Statement splitting must never panic
	let _ = split_statements(data);

	// 2. Statement parsing must never panic
	if let Ok(stmts) = parse_statements(data) {
		for stmt in &stmts {
			let _ = stmt.to_pgsql_string();
		}
	}

	// 3. Expression parsing must never panic
	if let Ok(expr) = parse_expr(data) {
		let _ = expr.to_pgsql_string();
	}
});
