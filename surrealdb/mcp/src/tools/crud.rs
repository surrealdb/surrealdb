//! CRUD operation tools: select, create, insert, upsert, update, delete, relate.
//!
//! Data values (CONTENT, MERGE, INSERT data) are bound via Variables to prevent
//! injection. WHERE clauses are SurrealQL expression fragments (same model as
//! /sql and /rpc endpoints). Table names and record IDs are validated.

use rmcp::ErrorData;
use rmcp::model::CallToolResult;
use schemars::JsonSchema;
use serde::Deserialize;
use surrealdb_core::dbs::QueryResult;
use surrealdb_types::Variables;

use super::{
	json_to_surreal_value, multi_statement_result, single_statement_result, validate_identifier,
	validate_table_name,
};
use crate::session::McpSession;

/// Build Variables with a single "data" key bound to the JSON value.
///
/// Returns an `invalid_params` error if any embedded `$ql` sentinel
/// inside `json` fails to parse, the body exceeds the configured cap,
/// or the JSON contains a number that cannot be represented as a
/// SurrealDB value. See [`json_to_surreal_value`] for the recognised
/// shapes.
fn bind_data(session: &McpSession, json: &serde_json::Value) -> Result<Variables, ErrorData> {
	let core = session.datastore().config();
	let mut vars = Variables::new();
	vars.insert("data", json_to_surreal_value(json, session.config(), core.as_ref())?);
	Ok(vars)
}

/// Reduce a statement vector to a single [`CallToolResult`] for tools that
/// always emit exactly one SurrealQL statement. If the datastore ever
/// returns zero or >1 results (e.g. a future refactor inserts a helper
/// statement), fall back to the multi-statement formatter so no data is
/// hidden.
fn collapse_single(session: &McpSession, mut results: Vec<QueryResult>) -> CallToolResult {
	let max_bytes = session.config().max_result_bytes;
	match results.len() {
		1 => single_statement_result(results.pop().expect("len == 1"), max_bytes),
		_ => multi_statement_result(results, max_bytes),
	}
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct SelectParams {
	/// Table or record target (e.g. "person", "person:john"). A single
	/// identifier or record id; comma-separated multi-target selects are
	/// not supported here -- use the raw `query` tool for those.
	pub target: String,
	/// Optional projection list (SurrealQL expression fragment; defaults
	/// to `*`). For dynamic values use `$param` bindings via the raw
	/// `query` tool.
	pub fields: Option<String>,
	/// Optional `WHERE` clause as a SurrealQL expression fragment (e.g.
	/// `age > 18`). For dynamic values use `$param` bindings via the raw
	/// `query` tool.
	pub where_clause: Option<String>,
	/// Optional `ORDER BY` clause (SurrealQL expression fragment, e.g.
	/// `name ASC`).
	pub order_clause: Option<String>,
	/// Optional `LIMIT` value.
	pub limit_clause: Option<u64>,
	/// Optional `START` value for pagination.
	pub start_clause: Option<u64>,
	/// Optional `GROUP BY` clause (SurrealQL expression fragment).
	pub group_clause: Option<String>,
	/// Optional `SPLIT` clause (SurrealQL expression fragment).
	pub split_clause: Option<String>,
	/// Optional `FETCH` clause: comma-separated SurrealQL expression
	/// fragment naming record-link fields to hydrate (e.g.
	/// `customer, items.*.product`). Use this to follow `record<...>`
	/// references in one round-trip rather than issuing follow-up
	/// queries.
	pub fetch_clause: Option<String>,
}

pub async fn select(session: &McpSession, p: SelectParams) -> Result<CallToolResult, ErrorData> {
	validate_identifier(&p.target)?;
	let fields = p.fields.as_deref().unwrap_or("*");
	let mut q = format!("SELECT {fields} FROM {}", p.target);
	if let Some(w) = &p.where_clause {
		q.push_str(&format!(" WHERE {w}"));
	}
	if let Some(s) = &p.split_clause {
		q.push_str(&format!(" SPLIT {s}"));
	}
	if let Some(g) = &p.group_clause {
		q.push_str(&format!(" GROUP BY {g}"));
	}
	if let Some(o) = &p.order_clause {
		q.push_str(&format!(" ORDER BY {o}"));
	}
	if let Some(l) = p.limit_clause {
		q.push_str(&format!(" LIMIT {l}"));
	}
	if let Some(s) = p.start_clause {
		q.push_str(&format!(" START {s}"));
	}
	if let Some(f) = &p.fetch_clause {
		q.push_str(&format!(" FETCH {f}"));
	}
	let results = session.execute(&q, None).await?;
	Ok(collapse_single(session, results))
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct CreateParams {
	/// Target table or record ID (e.g. "person" or "person:john").
	pub target: String,
	/// JSON data for the record content. Bound as $data variable.
	/// Use `{"$ql": "<surrealql expr>"}` anywhere in the tree to embed a
	/// typed SurrealDB value (decimal, datetime, duration, record id,
	/// uuid, ...) -- e.g. `{"price": {"$ql": "9.99dec"}, "customer":
	/// {"$ql": "customer:alice"}}`.
	pub data: Option<serde_json::Value>,
}

pub async fn create(session: &McpSession, p: CreateParams) -> Result<CallToolResult, ErrorData> {
	validate_identifier(&p.target)?;
	let (q, vars) = match &p.data {
		Some(d) => (format!("CREATE {} CONTENT $data", p.target), Some(bind_data(session, d)?)),
		None => (format!("CREATE {}", p.target), None),
	};
	let results = session.execute(&q, vars).await?;
	Ok(collapse_single(session, results))
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct InsertParams {
	/// Target table to insert into.
	pub target: String,
	/// JSON array of objects or single object to insert. Bound as $data
	/// variable. Use `{"$ql": "<surrealql expr>"}` anywhere in the tree
	/// to embed a typed SurrealDB value (decimal, datetime, duration,
	/// record id, uuid, ...).
	pub data: serde_json::Value,
	/// Whether to ignore duplicate key errors.
	#[serde(default)]
	pub ignore: bool,
	/// Whether this is a relation insert.
	#[serde(default)]
	pub relation: bool,
}

pub async fn insert(session: &McpSession, p: InsertParams) -> Result<CallToolResult, ErrorData> {
	// `INSERT INTO` only accepts a table name; passing a record id
	// produces a downstream parse error. Use the stricter validator so
	// the LLM gets a clean structured error instead.
	validate_table_name(&p.target)?;
	// The SurrealQL parser consumes the optional INSERT modifiers in a
	// fixed order: RELATION → IGNORE → INTO. Emitting them in any other
	// order (e.g. `INSERT IGNORE RELATION INTO ...`) would produce a
	// parse error when both flags are set.
	let relation = if p.relation {
		" RELATION"
	} else {
		""
	};
	let ignore = if p.ignore {
		" IGNORE"
	} else {
		""
	};
	let q = format!("INSERT{relation}{ignore} INTO {} $data", p.target);
	let results = session.execute(&q, Some(bind_data(session, &p.data)?)).await?;
	Ok(collapse_single(session, results))
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct UpsertParams {
	/// Target table or record (e.g. "person" or "person:john").
	pub target: String,
	/// JSON data for CONTENT mode (replaces entire record). Bound as
	/// $data. Embed typed SurrealDB values via `{"$ql": "<expr>"}` (e.g.
	/// `{"price": {"$ql": "9.99dec"}}`).
	pub content_data: Option<serde_json::Value>,
	/// JSON data for MERGE mode (merges with existing). Bound as $data.
	/// Same `$ql` escape applies for typed values.
	pub merge_data: Option<serde_json::Value>,
	/// JSON patch operations for PATCH mode. Bound as $data.
	pub patch_data: Option<serde_json::Value>,
	/// Optional `WHERE` clause (SurrealQL expression fragment). For dynamic
	/// values use `$param` bindings via the raw `query` tool.
	pub where_clause: Option<String>,
}

pub async fn upsert(session: &McpSession, p: UpsertParams) -> Result<CallToolResult, ErrorData> {
	validate_identifier(&p.target)?;
	let (mode, data) =
		resolve_update_mode(p.content_data.as_ref(), p.merge_data.as_ref(), p.patch_data.as_ref())?;
	let mut q = format!("UPSERT {} {mode} $data", p.target);
	if let Some(w) = &p.where_clause {
		q.push_str(&format!(" WHERE {w}"));
	}
	let results = session.execute(&q, Some(bind_data(session, data)?)).await?;
	Ok(collapse_single(session, results))
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct UpdateParams {
	/// Target table or record (e.g. "person" or "person:john").
	pub target: String,
	/// JSON data for CONTENT mode. Bound as $data. Embed typed
	/// SurrealDB values via `{"$ql": "<expr>"}`.
	pub content_data: Option<serde_json::Value>,
	/// JSON data for MERGE mode. Bound as $data. Same `$ql` escape
	/// applies.
	pub merge_data: Option<serde_json::Value>,
	/// JSON patch operations for PATCH mode. Bound as $data.
	pub patch_data: Option<serde_json::Value>,
	/// Optional `WHERE` clause (SurrealQL expression fragment). For dynamic
	/// values use `$param` bindings via the raw `query` tool.
	pub where_clause: Option<String>,
}

pub async fn update(session: &McpSession, p: UpdateParams) -> Result<CallToolResult, ErrorData> {
	validate_identifier(&p.target)?;
	let (mode, data) =
		resolve_update_mode(p.content_data.as_ref(), p.merge_data.as_ref(), p.patch_data.as_ref())?;
	let mut q = format!("UPDATE {} {mode} $data", p.target);
	if let Some(w) = &p.where_clause {
		q.push_str(&format!(" WHERE {w}"));
	}
	let results = session.execute(&q, Some(bind_data(session, data)?)).await?;
	Ok(collapse_single(session, results))
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct DeleteParams {
	/// Target table or record to delete.
	pub target: String,
	/// Optional `WHERE` clause (SurrealQL expression fragment). For dynamic
	/// values use `$param` bindings via the raw `query` tool.
	pub where_clause: Option<String>,
}

pub async fn delete(session: &McpSession, p: DeleteParams) -> Result<CallToolResult, ErrorData> {
	validate_identifier(&p.target)?;
	let mut q = format!("DELETE FROM {}", p.target);
	if let Some(w) = &p.where_clause {
		q.push_str(&format!(" WHERE {w}"));
	}
	let results = session.execute(&q, None).await?;
	Ok(collapse_single(session, results))
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct RelateParams {
	/// Source record(s) (e.g. "person:john").
	pub from: String,
	/// Edge table name (e.g. "knows", "wrote").
	pub table: String,
	/// Target record(s) (e.g. "person:bob").
	pub with: String,
	/// Optional JSON content data for the edge record. Bound as $data.
	/// Embed typed SurrealDB values via `{"$ql": "<expr>"}` (e.g.
	/// `{"order": {"$ql": "order:o1"}, "quantity": 2}`).
	pub content_data: Option<serde_json::Value>,
}

pub async fn relate(session: &McpSession, p: RelateParams) -> Result<CallToolResult, ErrorData> {
	validate_identifier(&p.from)?;
	validate_identifier(&p.table)?;
	validate_identifier(&p.with)?;
	let mut q = format!("RELATE {}->{}->{}", p.from, p.table, p.with);
	let vars = if let Some(d) = &p.content_data {
		q.push_str(" CONTENT $data");
		Some(bind_data(session, d)?)
	} else {
		None
	};
	let results = session.execute(&q, vars).await?;
	Ok(collapse_single(session, results))
}

fn resolve_update_mode<'a>(
	content: Option<&'a serde_json::Value>,
	merge: Option<&'a serde_json::Value>,
	patch: Option<&'a serde_json::Value>,
) -> Result<(&'static str, &'a serde_json::Value), ErrorData> {
	match (content, merge, patch) {
		(Some(d), None, None) => Ok(("CONTENT", d)),
		(None, Some(d), None) => Ok(("MERGE", d)),
		(None, None, Some(d)) => Ok(("PATCH", d)),
		(None, None, None) => Err(crate::error::invalid_params(
			"One of content_data, merge_data, or patch_data must be provided",
		)),
		_ => Err(crate::error::invalid_params(
			"Only one of content_data, merge_data, or patch_data may be provided",
		)),
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use serde_json::json;
	use surrealdb_core::dbs::Session;
	use surrealdb_core::kvs::Datastore;

	use super::*;

	async fn session() -> McpSession {
		let ds = Arc::new(Datastore::new("memory").await.expect("datastore"));
		ds.execute("DEFINE NAMESPACE test;", &Session::owner(), None).await.expect("ns");
		ds.execute("DEFINE DATABASE test;", &Session::owner().with_ns("test"), None)
			.await
			.expect("db");
		McpSession::new(ds, Session::owner().with_ns("test").with_db("test"))
	}

	/// `select` with a `FETCH` clause must hydrate `record<...>` links
	/// in-place. We build a tiny owner/post graph, then assert that
	/// fetching `owner` replaces the record-id reference with the full
	/// owner record.
	#[tokio::test]
	async fn select_fetch_clause_hydrates_record_links() {
		let session = session().await;
		// Seed two related records via the raw query path so we don't
		// depend on the structured tools we're testing.
		session
			.execute(
				"CREATE owner:alice SET name = 'Alice'; \
				 CREATE post:p1 SET title = 'hello', owner = owner:alice;",
				None,
			)
			.await
			.expect("seed");

		// Without FETCH: `owner` stays as a record id reference.
		let no_fetch = select(
			&session,
			SelectParams {
				target: "post:p1".into(),
				fields: None,
				where_clause: None,
				order_clause: None,
				limit_clause: None,
				start_clause: None,
				group_clause: None,
				split_clause: None,
				fetch_clause: None,
			},
		)
		.await
		.expect("select without fetch");
		// `SELECT * FROM post:p1` returns an array of records, even
		// when the target is a single record id.
		let row = first_row(&no_fetch);
		assert_eq!(row.get("owner").and_then(|v| v.as_str()), Some("owner:alice"));

		// With FETCH: `owner` is hydrated to the full owner record.
		let fetched = select(
			&session,
			SelectParams {
				target: "post:p1".into(),
				fields: None,
				where_clause: None,
				order_clause: None,
				limit_clause: None,
				start_clause: None,
				group_clause: None,
				split_clause: None,
				fetch_clause: Some("owner".into()),
			},
		)
		.await
		.expect("select with fetch");
		let row = first_row(&fetched);
		let owner = row.get("owner").expect("owner key");
		assert!(owner.is_object(), "FETCH should hydrate owner to an object, got: {owner:?}");
		assert_eq!(owner.get("name").and_then(|v| v.as_str()), Some("Alice"));
	}

	/// Pull the first row out of a structured `SELECT *` response,
	/// regardless of whether the underlying query returned a bare
	/// record or an array.
	fn first_row(call: &CallToolResult) -> serde_json::Value {
		let value = call
			.structured_content
			.as_ref()
			.expect("structured content")
			.get("value")
			.cloned()
			.expect("value");
		match value {
			serde_json::Value::Array(mut arr) if !arr.is_empty() => arr.remove(0),
			other => other,
		}
	}

	/// The structured CRUD tools must accept typed values via `$ql`.
	/// We exercise `create` end-to-end so the binding path, parser, and
	/// schema coercion all run together: a `decimal` field must succeed
	/// when expressed via `$ql` but fail when the same value is sent as
	/// a JSON string against a SCHEMAFULL table.
	#[tokio::test]
	async fn create_accepts_ql_passthrough_for_typed_values() {
		let session = session().await;
		session
			.execute(
				"DEFINE TABLE product SCHEMAFULL; \
				 DEFINE FIELD price ON product TYPE decimal;",
				None,
			)
			.await
			.expect("schema");

		// A bare JSON string `\"9.99\"` must be rejected by the schema
		// because the field is typed `decimal`.
		let r = create(
			&session,
			CreateParams {
				target: "product:bad".into(),
				data: Some(json!({ "price": "9.99" })),
			},
		)
		.await
		.expect("call returns");
		let structured = r.structured_content.as_ref().expect("structured");
		assert_eq!(
			structured.get("status").and_then(|v| v.as_str()),
			Some("error"),
			"bare string into a decimal field should fail; got: {structured}"
		);

		// The same payload via `$ql` must succeed.
		let r = create(
			&session,
			CreateParams {
				target: "product:good".into(),
				data: Some(json!({ "price": { "$ql": "9.99dec" } })),
			},
		)
		.await
		.expect("call returns");
		let structured = r.structured_content.as_ref().expect("structured");
		assert_eq!(structured.get("status").and_then(|v| v.as_str()), Some("ok"));
	}

	/// Regression test for the INSERT keyword-order bug. The SurrealQL
	/// parser consumes the optional INSERT modifiers in a fixed order
	/// (RELATION → IGNORE → INTO), so emitting `INSERT IGNORE RELATION
	/// INTO ...` produced a parse error when both flags were set. We
	/// drive the both-flags-on path end-to-end with a real edge schema
	/// and assert that the statement parses and runs cleanly.
	#[tokio::test]
	async fn insert_relation_with_ignore_does_not_parse_error() {
		let session = session().await;
		// Seed two endpoints so the RELATION insert has valid `in`/`out`
		// references.
		session.execute("CREATE person:a; CREATE person:b;", None).await.expect("seed endpoints");

		let result = insert(
			&session,
			InsertParams {
				target: "wrote".into(),
				data: json!([
					{ "in": { "$ql": "person:a" }, "out": { "$ql": "person:b" } },
				]),
				ignore: true,
				relation: true,
			},
		)
		.await
		.expect("insert call must not return a protocol error");

		let structured = result.structured_content.as_ref().expect("structured content");
		// The statement must parse and execute. We check `status == ok`
		// (no parse-time error) — the wire shape `INSERT IGNORE
		// RELATION INTO ...` would have produced `status == error` with
		// kind `Parse` because `RELATION` is not a valid value
		// expression start.
		assert_eq!(
			structured.get("status").and_then(|v| v.as_str()),
			Some("ok"),
			"INSERT with both relation and ignore must parse cleanly; got: {structured}"
		);
	}
}
