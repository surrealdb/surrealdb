#![forbid(unsafe_code)]
#![cfg_attr(
	not(test),
	deny(
		clippy::indexing_slicing,
		clippy::string_slice,
		clippy::panic,
		clippy::panic_in_result_fn,
		clippy::unreachable,
		clippy::todo,
		clippy::unimplemented,
		clippy::expect_used,
		clippy::unwrap_used,
		clippy::print_stdout,
		clippy::print_stderr,
		clippy::dbg_macro
	)
)]

//! PostgreSQL language front end for SurrealDB.
//!
//! Shared base for PostgreSQL 17, 18, and 19 grammar parsing, AST representation,
//! and faithful printing. Unrelated to `surrealdb-sql` (SurrealQL).
