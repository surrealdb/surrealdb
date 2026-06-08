//! Invoke SurrealQL functions (built-in and user-defined) safely.
//!
//! The function name is validated to a strict `identifier(::identifier)*`
//! grammar -- no whitespace, parentheses, or statement terminators -- and
//! every argument is passed via typed `Variables` bindings. This mirrors the
//! safety pattern used in [`crate::tools::crud`]. Authorization (user-defined
//! `fn::*` PERMISSIONS clauses, capability-gated built-ins like `http::*`)
//! is enforced natively by SurrealDB during `execute`.

use rmcp::ErrorData;
use rmcp::model::CallToolResult;
use schemars::JsonSchema;
use serde::Deserialize;
use surrealdb_types::Variables;

use super::{json_to_surreal_value, single_statement_result};
use crate::error::invalid_params;
use crate::session::McpSession;

#[derive(Debug, Deserialize, JsonSchema)]
pub struct RunParams {
	/// Function name. Examples: `math::sum`, `string::concat`, `fn::my_function`.
	pub function: String,
	/// Optional argument list. Values are bound with their native types --
	/// numbers stay numbers, objects stay objects. Embed typed SurrealDB
	/// values via `{"$ql": "<expr>"}` -- e.g. pass a record id as
	/// `{"$ql": "person:alice"}` or a decimal as `{"$ql": "9.99dec"}`.
	pub args: Option<Vec<serde_json::Value>>,
}

/// Invoke a SurrealQL function with typed argument bindings. Works for built-in
/// functions (e.g. `math::sum`, `string::concat`, `crypto::argon2::generate`)
/// and user-defined functions (`fn::*`) that the session is allowed to call.
pub async fn run(session: &McpSession, p: RunParams) -> Result<CallToolResult, ErrorData> {
	validate_function_name(&p.function)?;
	// `args` is optional in the wire schema; an absent value means "invoke
	// the function with no arguments". REVIEW.md forbids
	// `.unwrap_or_default()` in non-test code, so the empty fallback is
	// spelled out explicitly.
	#[expect(
		clippy::manual_unwrap_or_default,
		reason = "REVIEW.md forbids unwrap_or_default in non-test code"
	)]
	let args = match p.args {
		Some(a) => a,
		None => Vec::new(),
	};

	let mcp = session.config();
	let max_args = mcp.run_max_args;
	if args.len() > max_args {
		return Err(invalid_params(format!(
			"`run` accepts at most {max_args} arguments, got {}",
			args.len()
		)));
	}

	let core = session.datastore().config();
	let mut vars = Variables::new();
	let mut placeholders = Vec::with_capacity(args.len());
	for (i, v) in args.iter().enumerate() {
		let name = format!("_a{i}");
		vars.insert(name.clone(), json_to_surreal_value(v, mcp, core.as_ref())?);
		placeholders.push(format!("${name}"));
	}
	let query = format!("RETURN {}({})", p.function, placeholders.join(", "));
	let mut results = session.execute(&query, Some(vars)).await?;
	// `RETURN <fn>(...)` is always a single statement.
	let result = results.pop().unwrap_or_else(|| {
		tracing::warn!(target: "surrealdb::mcp", "run() returned no statements");
		surrealdb_core::dbs::QueryResultBuilder::instant_none()
	});
	Ok(single_statement_result(result, mcp.max_result_bytes))
}

/// Accept names matching `identifier(::identifier)*` where `identifier` is
/// `[A-Za-z_][A-Za-z0-9_]*`. This covers all built-in function paths
/// (`math::sum`, `crypto::argon2::generate`, `ml::model`) and user-defined
/// functions (`fn::auth::login`) without allowing any SurrealQL punctuation
/// or control characters that could escape the `RETURN` statement.
fn validate_function_name(name: &str) -> Result<(), ErrorData> {
	if name.is_empty() {
		return Err(invalid_params("Function name cannot be empty"));
	}
	for segment in name.split("::") {
		if segment.is_empty() {
			return Err(invalid_params("Function name contains an empty segment"));
		}
		let mut chars = segment.chars();
		let first = chars.next().expect("segment is non-empty");
		if !(first.is_ascii_alphabetic() || first == '_') {
			return Err(invalid_params(
				"Function name segments must start with a letter or underscore",
			));
		}
		for c in chars {
			if !(c.is_ascii_alphanumeric() || c == '_') {
				return Err(invalid_params(
					"Function name segments may only contain letters, digits, and underscores",
				));
			}
		}
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use serde_json::Value as JsonValue;

	use super::*;

	#[test]
	fn accepts_builtin_names() {
		assert!(validate_function_name("math::sum").is_ok());
		assert!(validate_function_name("string::concat").is_ok());
		assert!(validate_function_name("crypto::argon2::generate").is_ok());
		assert!(validate_function_name("time").is_ok());
		assert!(validate_function_name("fn::auth::login").is_ok());
		assert!(validate_function_name("_private").is_ok());
	}

	#[test]
	fn rejects_injection_attempts() {
		assert!(validate_function_name("").is_err());
		assert!(validate_function_name("math::sum; DROP TABLE x").is_err());
		assert!(validate_function_name("math::sum()").is_err());
		assert!(validate_function_name("math::").is_err());
		assert!(validate_function_name("::sum").is_err());
		assert!(validate_function_name("math sum").is_err());
		assert!(validate_function_name("1abc").is_err());
		assert!(validate_function_name("math::sum\nDELETE").is_err());
		assert!(validate_function_name("math::$arg").is_err());
	}

	/// `run` is the most direct path for forcing per-arg allocation work,
	/// so the cap is the key DoS mitigation. Validation must fire *before*
	/// we touch the datastore.
	#[tokio::test]
	async fn run_rejects_arg_array_over_cap() {
		use std::sync::Arc;

		use surrealdb_core::dbs::Session;
		use surrealdb_core::kvs::Datastore;

		use crate::cnf::McpConfig;
		let cap = McpConfig::default().run_max_args;
		let args: Vec<JsonValue> = (0..=cap).map(|i| JsonValue::from(i as i64)).collect();
		let ds = Arc::new(Datastore::new("memory").await.expect("in-memory datastore"));
		let session = crate::session::McpSession::new(ds, Session::default());
		let err = run(
			&session,
			RunParams {
				function: "math::sum".to_string(),
				args: Some(args),
			},
		)
		.await
		.expect_err("over-cap arg list must be rejected");
		let message = format!("{err:?}");
		assert!(message.contains("at most"), "expected cap message, got: {message}");
	}
}
