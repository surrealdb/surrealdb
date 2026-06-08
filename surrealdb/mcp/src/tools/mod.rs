//! MCP tool implementations for SurrealDB operations.
//!
//! All tools execute SurrealQL through `Datastore::execute()` with proper
//! Session context. Data values are bound via Variables; identifiers are
//! validated to prevent statement injection.

pub mod connection;
pub mod crud;
pub mod output;
pub(crate) mod output_schemas;
pub mod query;
pub mod run;
pub mod schema;

use std::str::FromStr;

use rmcp::ErrorData;
use surrealdb_core::cnf::CommonConfig;
use surrealdb_types::{Decimal, Number, SurrealValue, Value, Variables};

pub(crate) use self::output::{
	multi_statement_result, single_statement_result, structured_success, tool_error_from_surreal,
};
use crate::cnf::McpConfig;

/// Validate that a string is safe to use as a SurrealQL identifier or simple
/// record ID when interpolated directly into a query string.
///
/// Accepted grammar (strict allow-list):
/// - Bare identifier: `[A-Za-z_][A-Za-z0-9_]*`
/// - Backtick-quoted identifier: `` `...` `` where the body has no `` ` ``, newline, carriage
///   return, or NUL.
/// - Record id: `<table>:<key>` where `<table>` is one of the two identifier forms above and
///   `<key>` is either `[A-Za-z0-9_-]+` (covering digits, UUIDs, and ULIDs) or a backtick-quoted
///   body.
///
/// Anything containing whitespace, commas, `->`/`<-`, `--`, parentheses,
/// operators, SurrealQL keywords or any other structural tokens is rejected.
/// Complex object / array / range record-id keys are not accepted here --
/// callers that need them should go through the raw `query` tool with typed
/// parameter bindings.
pub fn validate_identifier(s: &str) -> Result<&str, ErrorData> {
	if s.is_empty() {
		return Err(crate::error::invalid_params("Identifier cannot be empty"));
	}
	// A backtick-quoted identifier may itself contain `:` in its body, so we
	// have to detect the quoted form before splitting on `:`.
	if s.starts_with('`') {
		let rest = consume_backtick_ident(s)?;
		if rest.is_empty() {
			return Ok(s);
		}
		// Must be followed by `:<key>` if there's anything after the closing
		// backtick.
		let key = rest.strip_prefix(':').ok_or_else(|| {
			crate::error::invalid_params("Identifier contains invalid characters")
		})?;
		validate_record_id_key(key)?;
		return Ok(s);
	}

	match s.split_once(':') {
		Some((table, key)) => {
			validate_bare_ident(table)?;
			validate_record_id_key(key)?;
		}
		None => {
			validate_bare_ident(s)?;
		}
	}
	Ok(s)
}

/// Validate a SurrealQL table name for interpolation into statements like
/// `INFO FOR TABLE <name>` or `DEFINE TABLE <name>`.
///
/// Unlike [`validate_identifier`], this rejects record IDs (e.g. `person:john`):
/// statements that take a table name do not accept a record ID in that slot,
/// so catching it here produces a clear error instead of a downstream parse
/// failure. The returned slice is the validated input verbatim and is safe
/// to interpolate directly without adding extra backticks — doing so would
/// double-quote an already backtick-quoted name and cause a parse error.
pub fn validate_table_name(s: &str) -> Result<&str, ErrorData> {
	if s.is_empty() {
		return Err(crate::error::invalid_params("Table name cannot be empty"));
	}
	if s.starts_with('`') {
		let rest = consume_backtick_ident(s)?;
		if !rest.is_empty() {
			return Err(crate::error::invalid_params("Table name contains invalid characters"));
		}
		return Ok(s);
	}
	validate_bare_ident(s)?;
	Ok(s)
}

/// Validate a bare SurrealQL identifier: `[A-Za-z_][A-Za-z0-9_]*`.
fn validate_bare_ident(s: &str) -> Result<(), ErrorData> {
	let mut chars = s.chars();
	let first = chars
		.next()
		.ok_or_else(|| crate::error::invalid_params("Identifier segment cannot be empty"))?;
	if !(first.is_ascii_alphabetic() || first == '_') {
		return Err(crate::error::invalid_params(
			"Identifier must start with a letter or underscore",
		));
	}
	for c in chars {
		if !(c.is_ascii_alphanumeric() || c == '_') {
			return Err(crate::error::invalid_params("Identifier contains invalid characters"));
		}
	}
	Ok(())
}

/// Validate the `<key>` part of a `table:key` record id. Accepts either a
/// backtick-quoted body or a bare key that must start with an alphanumeric
/// / underscore and may continue with `[A-Za-z0-9_-]`, disallowing consecutive
/// `-` (which would form a SurrealQL `--` line-comment if ever combined with
/// surrounding text). Any remaining input after a valid quoted key is a
/// parse error.
fn validate_record_id_key(key: &str) -> Result<(), ErrorData> {
	if key.is_empty() {
		return Err(crate::error::invalid_params("Record id key cannot be empty"));
	}
	if key.starts_with('`') {
		let rest = consume_backtick_ident(key)?;
		if !rest.is_empty() {
			return Err(crate::error::invalid_params("Record id contains invalid characters"));
		}
		return Ok(());
	}
	let mut prev_dash = false;
	let mut first = true;
	for c in key.chars() {
		let is_alnum = c.is_ascii_alphanumeric() || c == '_';
		let is_dash = c == '-';
		if first {
			if !is_alnum {
				return Err(crate::error::invalid_params(
					"Record id key must start with a letter, digit, or underscore",
				));
			}
			first = false;
			prev_dash = false;
			continue;
		}
		if is_alnum {
			prev_dash = false;
			continue;
		}
		if is_dash && !prev_dash {
			prev_dash = true;
			continue;
		}
		return Err(crate::error::invalid_params("Record id key contains invalid characters"));
	}
	if prev_dash {
		return Err(crate::error::invalid_params("Record id key cannot end with a dash"));
	}
	Ok(())
}

/// Consume a backtick-quoted identifier starting at `s[0] == '\`'`. Returns
/// the slice of `s` *after* the closing backtick (possibly empty). The body
/// must be non-empty and free of backticks, newlines, carriage returns, and
/// NUL bytes.
fn consume_backtick_ident(s: &str) -> Result<&str, ErrorData> {
	debug_assert!(s.starts_with('`'));
	let body_and_tail = &s[1..];
	let close = body_and_tail
		.find('`')
		.ok_or_else(|| crate::error::invalid_params("Unterminated backtick-quoted identifier"))?;
	let body = &body_and_tail[..close];
	if body.is_empty() {
		return Err(crate::error::invalid_params("Backtick-quoted identifier cannot be empty"));
	}
	if body.contains('\n') || body.contains('\r') || body.contains('\0') {
		return Err(crate::error::invalid_params("Identifier contains invalid characters"));
	}
	Ok(&body_and_tail[close + 1..])
}

/// Sentinel key that escapes a JSON object into a SurrealQL pass-through.
///
/// A single-key object `{ "$ql": "<surrealql expression>" }` is replaced by
/// the parsed [`Value`] of that expression. This is the canonical way for
/// MCP clients to express types JSON cannot represent natively (decimal,
/// datetime, duration, record id, uuid, bytes, geometry literals, ...)
/// without resorting to the raw `query` tool. The body is parsed by
/// [`surrealdb_core::syn::value_legacy_strand`], which honours the
/// parser's per-call object/query depth limits, so the same defences
/// that protect the `query` path also apply here.
pub(crate) const QL_SENTINEL: &str = "$ql";

/// Convert a [`serde_json::Value`] into a [`surrealdb_types::Value`],
/// preserving types and recognising the [`QL_SENTINEL`] escape.
///
/// `mcp` carries the MCP-side caps (notably
/// [`McpConfig::params_max_ql_bytes`]); `core` carries the datastore's
/// own [`CommonConfig`] so the `$ql` pass-through parser honours the
/// running parser depth limits rather than silently falling back to
/// `CommonConfig::default()`.
///
/// Returns a structured `invalid_params` error if a `$ql` body is empty,
/// exceeds the configured byte cap, is non-string, appears alongside
/// other keys, or fails to parse as a SurrealQL value. JSON numbers
/// that don't fit `i64`/`u64`/`f64` (in that order of preference) are
/// promoted to `Number::Decimal` so a value like `u64::MAX` survives
/// round-tripping instead of silently becoming `Value::None`.
pub(crate) fn json_to_surreal_value(
	json: &serde_json::Value,
	mcp: &McpConfig,
	core: &CommonConfig,
) -> Result<Value, ErrorData> {
	match json {
		serde_json::Value::Null => Ok(Value::Null),
		serde_json::Value::Bool(b) => Ok((*b).into_value()),
		serde_json::Value::Number(n) => json_number_to_value(n),
		serde_json::Value::String(s) => Ok(s.as_str().into_value()),
		serde_json::Value::Array(arr) => {
			let mut vals: Vec<Value> = Vec::with_capacity(arr.len());
			for v in arr {
				vals.push(json_to_surreal_value(v, mcp, core)?);
			}
			Ok(vals.into_value())
		}
		serde_json::Value::Object(map) => {
			// `$ql` escape: a single-key object whose key is the sentinel
			// and whose value is a string is replaced by the parsed
			// SurrealQL value. Mixed objects that *also* contain the
			// sentinel key are rejected to keep the wire shape
			// unambiguous.
			if let Some(ql) = map.get(QL_SENTINEL) {
				if map.len() != 1 {
					return Err(crate::error::invalid_params(format!(
						"`{QL_SENTINEL}` must be the only key when present; found {} keys",
						map.len()
					)));
				}
				return parse_ql_passthrough(ql, mcp, core);
			}
			let mut obj = surrealdb_types::Object::default();
			for (k, v) in map {
				obj.insert(k, json_to_surreal_value(v, mcp, core)?);
			}
			Ok(obj.into_value())
		}
	}
}

/// Convert a [`serde_json::Number`] to a [`Value`], promoting to
/// [`Number::Decimal`] for values that don't fit any of the primitive
/// JSON number types (e.g. integers larger than `i64::MAX` but within
/// `u64`, or arbitrary-precision numbers when the upstream serializer
/// emits them as strings via the `arbitrary_precision` feature).
fn json_number_to_value(n: &serde_json::Number) -> Result<Value, ErrorData> {
	if let Some(i) = n.as_i64() {
		return Ok(i.into_value());
	}
	if let Some(u) = n.as_u64() {
		// `u64::MAX` does not fit in `i64`. Round-trip through
		// `Decimal` so the value is preserved losslessly rather than
		// silently dropped as the previous implementation did.
		let dec = Decimal::from(u);
		return Ok(Value::Number(Number::Decimal(dec)));
	}
	if let Some(f) = n.as_f64() {
		return Ok(f.into_value());
	}
	// `arbitrary_precision` is not enabled in this workspace, but if a
	// future upgrade flips it, fall back to parsing the string form as
	// a `Decimal` rather than dropping the value.
	let s = n.to_string();
	if let Ok(dec) = Decimal::from_str(&s) {
		return Ok(Value::Number(Number::Decimal(dec)));
	}
	Err(crate::error::invalid_params(format!(
		"JSON number `{s}` cannot be represented as a SurrealDB value"
	)))
}

/// Parse the body of a `$ql` sentinel object into a [`Value`].
///
/// The body must be a non-empty string within
/// [`McpConfig::params_max_ql_bytes`]; it is then parsed as a single
/// SurrealQL value via the production parser using the running
/// datastore's [`CommonConfig`], so operator-set parser recursion
/// limits actually apply. Any parse failure surfaces as
/// `invalid_params` with the parser's message so the LLM can
/// self-correct.
fn parse_ql_passthrough(
	body: &serde_json::Value,
	mcp: &McpConfig,
	core: &CommonConfig,
) -> Result<Value, ErrorData> {
	let s = body.as_str().ok_or_else(|| {
		crate::error::invalid_params(format!("`{QL_SENTINEL}` value must be a string"))
	})?;
	if s.is_empty() {
		return Err(crate::error::invalid_params(format!(
			"`{QL_SENTINEL}` value must be a non-empty SurrealQL expression"
		)));
	}
	let max_bytes = mcp.params_max_ql_bytes;
	if s.len() > max_bytes {
		return Err(crate::error::invalid_params(format!(
			"`{QL_SENTINEL}` body is {} bytes, exceeding the {max_bytes}-byte cap",
			s.len()
		)));
	}
	surrealdb_core::syn::value_legacy_strand(s, core).map_err(|e| {
		crate::error::invalid_params(format!(
			"`{QL_SENTINEL}` body failed to parse as a SurrealQL value: {e}"
		))
	})
}

/// Convert a JSON object into typed Variables for query binding.
///
/// Enforces the per-call [`McpConfig::params_max_keys`] cap so one call
/// cannot force unbounded allocation work. Nesting depth does not need
/// a separate cap here: `serde_json` already enforces a hard 128-level
/// recursion limit during deserialization (the `unbounded_depth`
/// feature is not enabled in this workspace), so any value that
/// reaches this function is guaranteed to be of bounded depth.
pub fn json_to_variables(
	json: &serde_json::Value,
	mcp: &McpConfig,
	core: &CommonConfig,
) -> Result<Variables, ErrorData> {
	match json {
		serde_json::Value::Object(map) => {
			let max_keys = mcp.params_max_keys;
			if map.len() > max_keys {
				return Err(crate::error::invalid_params(format!(
					"Parameters object has {} keys, exceeding the maximum of {max_keys}",
					map.len()
				)));
			}
			let mut vars = Variables::new();
			for (k, v) in map {
				vars.insert(k, json_to_surreal_value(v, mcp, core)?);
			}
			Ok(vars)
		}
		_ => Err(crate::error::invalid_params("Parameters must be a JSON object")),
	}
}

#[cfg(test)]
mod cap_tests {
	use serde_json::{Value as JsonValue, json};

	use super::*;

	/// Build a default `(McpConfig, CommonConfig)` pair for tests that
	/// don't care about overrides. Returning owned values keeps the
	/// borrow checker happy for the duration of each test body.
	fn default_configs() -> (McpConfig, CommonConfig) {
		(McpConfig::default(), CommonConfig::default())
	}

	#[test]
	fn json_to_variables_rejects_too_many_keys() {
		let (mcp, core) = default_configs();
		let cap = mcp.params_max_keys;
		let mut map = serde_json::Map::with_capacity(cap + 1);
		for i in 0..=cap {
			map.insert(format!("k{i}"), JsonValue::from(i as i64));
		}
		let payload = JsonValue::Object(map);
		let err =
			json_to_variables(&payload, &mcp, &core).expect_err("over-cap params must be rejected");
		let message = format!("{err:?}");
		assert!(message.contains("exceeding the maximum"), "expected cap message, got: {message}");
	}

	#[test]
	fn json_to_variables_accepts_payload_within_caps() {
		let (mcp, core) = default_configs();
		let payload = json!({"name": "alice", "nested": {"k": [1, 2, 3]}});
		let vars = json_to_variables(&payload, &mcp, &core).expect("within-cap payload accepted");
		// Sanity check that conversion produced both keys.
		assert_eq!(vars.len(), 2);
	}

	/// `$ql` must round-trip every typed scalar that JSON cannot
	/// natively represent. We assert the discriminant rather than the
	/// formatted SQL so the test stays robust against display tweaks.
	#[test]
	fn ql_passthrough_round_trips_typed_scalars() {
		use surrealdb_types::Value;

		let (mcp, core) = default_configs();
		type ValuePredicate = fn(&Value) -> bool;
		let cases: &[(&str, ValuePredicate)] = &[
			("9.99dec", |v| matches!(v, Value::Number(Number::Decimal(_)))),
			("d'2026-04-27T11:40:00Z'", |v| matches!(v, Value::Datetime(_))),
			("1h30m", |v| matches!(v, Value::Duration(_))),
			("customer:alice", |v| matches!(v, Value::RecordId(_))),
			("u'01933a3c-8d40-7e1c-9b59-0123456789ab'", |v| matches!(v, Value::Uuid(_))),
		];
		for (body, predicate) in cases {
			let payload = json!({ "$ql": *body });
			let value =
				json_to_surreal_value(&payload, &mcp, &core).expect("$ql parse should succeed");
			assert!(predicate(&value), "`{body}` produced unexpected variant: {value:?}");
		}
	}

	/// The sentinel must be the **only** key in its object: a mixed
	/// object with `$ql` plus other keys is rejected to keep the wire
	/// shape unambiguous.
	#[test]
	fn ql_rejects_mixed_object() {
		let (mcp, core) = default_configs();
		let payload = json!({ "$ql": "9dec", "other": 1 });
		let err = json_to_surreal_value(&payload, &mcp, &core)
			.expect_err("mixed object must be rejected");
		assert!(format!("{err:?}").contains("only key"));
	}

	#[test]
	fn ql_rejects_non_string_body() {
		let (mcp, core) = default_configs();
		let payload = json!({ "$ql": 42 });
		let err = json_to_surreal_value(&payload, &mcp, &core)
			.expect_err("non-string body must be rejected");
		assert!(format!("{err:?}").contains("must be a string"));
	}

	#[test]
	fn ql_rejects_empty_body() {
		let (mcp, core) = default_configs();
		let payload = json!({ "$ql": "" });
		let err =
			json_to_surreal_value(&payload, &mcp, &core).expect_err("empty body must be rejected");
		assert!(format!("{err:?}").contains("non-empty"));
	}

	#[test]
	fn ql_rejects_oversized_body() {
		let (mcp, core) = default_configs();
		let cap = mcp.params_max_ql_bytes;
		let body = "x".repeat(cap + 1);
		let payload = json!({ "$ql": body });
		let err = json_to_surreal_value(&payload, &mcp, &core)
			.expect_err("oversized body must be rejected");
		assert!(format!("{err:?}").contains("exceeding"));
	}

	/// Statement-shaped input (e.g. `SELECT 1`) is not a value and must
	/// be rejected at the parameter boundary -- the `$ql` escape is
	/// strictly for value literals.
	#[test]
	fn ql_rejects_non_value_input() {
		let (mcp, core) = default_configs();
		let payload = json!({ "$ql": "SELECT 1" });
		let err =
			json_to_surreal_value(&payload, &mcp, &core).expect_err("statements must be rejected");
		let msg = format!("{err:?}");
		assert!(msg.contains("failed to parse"), "expected parser error, got: {msg}");
	}

	/// The sentinel must work nested inside arrays and objects, since
	/// CRUD tools take entire record bodies and the LLM should be free
	/// to mark any scalar as typed regardless of nesting.
	#[test]
	fn ql_works_nested_in_arrays_and_objects() {
		use surrealdb_types::Value;

		let (mcp, core) = default_configs();
		let payload = json!({
			"items": [
				{ "product": { "$ql": "product:widget" }, "qty": 3 },
				{ "product": { "$ql": "product:gadget" }, "qty": 1 },
			],
		});
		let value =
			json_to_surreal_value(&payload, &mcp, &core).expect("nested $ql must round-trip");
		let outer = match &value {
			Value::Object(o) => o,
			other => panic!("expected object, got {other:?}"),
		};
		let items = match outer.get("items") {
			Some(Value::Array(a)) => a,
			other => panic!("expected array, got {other:?}"),
		};
		assert_eq!(items.len(), 2);
		for item in items.iter() {
			let obj = match item {
				Value::Object(o) => o,
				other => panic!("expected item object, got {other:?}"),
			};
			assert!(matches!(obj.get("product"), Some(Value::RecordId(_))));
		}
	}

	/// `u64::MAX` is larger than `i64::MAX` and must round-trip via
	/// `Number::Decimal` rather than collapsing to `Value::None` as the
	/// pre-fix implementation did.
	#[test]
	fn json_number_overflow_promotes_to_decimal() {
		use surrealdb_types::Value;
		let (mcp, core) = default_configs();
		let payload = json!(u64::MAX);
		let value = json_to_surreal_value(&payload, &mcp, &core).expect("u64::MAX must convert");
		match value {
			Value::Number(Number::Decimal(d)) => {
				assert_eq!(d.to_string(), u64::MAX.to_string());
			}
			other => panic!("expected Decimal for u64::MAX, got: {other:?}"),
		}
	}
}
