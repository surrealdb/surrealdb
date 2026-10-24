//! CRUD operation tools: select, create, insert, upsert, update, delete, relate.
//!
//! Data values (CONTENT, MERGE, INSERT data) are bound via Variables to prevent
//! injection. WHERE clauses are SurrealQL expression fragments (same model as
//! /sql and /rpc endpoints). Table names and record IDs are validated.

use rmcp::ErrorData;
use rmcp::model::CallToolResult;
use schemars::JsonSchema;
use serde::Deserialize;
use surrealdb_rpc::QueryResult;
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
	let core = session.datastore().parser_config();
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
	/// Namespace and database this call runs against. Flattened, so the
	/// emitted schema carries `namespace` and `database` directly, each
	/// documenting its own fallback order.
	#[serde(flatten)]
	pub scope: super::ToolScope,
}

/// Build [`surrealdb_core::syn::ParserSettings`] from the running datastore's
/// [`surrealdb_core::syn::ParserConfig`].
fn parser_settings(session: &McpSession) -> surrealdb_core::syn::ParserSettings {
	let core = session.datastore().parser_config();
	surrealdb_core::syn::ParserSettings::from_config(core.as_ref())
}

/// Validate a `SELECT` projection list (`fields`) before interpolating it into a
/// `SELECT` statement.
///
/// Rejects empty fragments, trailing clauses (`OMIT`, `FROM`, `WHERE`, `;`),
/// non-read-only expressions, and unclosed comments.
fn validate_fields_clause(session: &McpSession, fields: &str) -> Result<(), ErrorData> {
	if fields.trim().is_empty() {
		return Err(crate::error::invalid_params("SELECT fields clause cannot be empty"));
	}
	let parsed =
		surrealdb_core::syn::runtime::fields_with_settings(fields, parser_settings(session), true)
			.map_err(|e| {
				crate::error::invalid_params(format!("Invalid SELECT fields clause: {e}"))
			})?;
	if !parsed.read_only() {
		return Err(crate::error::invalid_params("SELECT fields clause must be read-only"));
	}
	validate_select_probe(session, "SELECT fields", &format!("SELECT {fields}\nFROM __mcp_probe"))
}

/// Validate a `WHERE` expression fragment before interpolating it into a
/// `SELECT`, `UPSERT`, `UPDATE`, or `DELETE` statement.
///
/// The fragment must parse as a single complete read-only expression with no
/// trailing clauses (`ORDER BY`, `LIMIT`, `RETURN`, `TIMEOUT`, `;`) or statement
/// keywords.
fn validate_where_clause(session: &McpSession, where_clause: &str) -> Result<(), ErrorData> {
	if where_clause.trim().is_empty() {
		return Err(crate::error::invalid_params("WHERE clause cannot be empty"));
	}
	let parsed = surrealdb_core::syn::runtime::expr_with_settings(
		where_clause,
		parser_settings(session),
		true,
	)
	.map_err(|e| crate::error::invalid_params(format!("Invalid WHERE clause: {e}")))?;
	if !parsed.read_only() {
		return Err(crate::error::invalid_params("WHERE clause must be a read-only expression"));
	}
	validate_select_probe(
		session,
		"WHERE",
		&format!("SELECT * FROM __mcp_probe\nWHERE (\n{where_clause}\n)\nORDER BY __mcp_probe"),
	)
}

/// Parse `probe_sql` as a SurrealQL query and verify that it resolves to a
/// single read-only statement.
fn validate_select_probe(
	session: &McpSession,
	clause_name: &str,
	probe_sql: &str,
) -> Result<(), ErrorData> {
	let ast =
		surrealdb_core::syn::runtime::query_with_settings(probe_sql, parser_settings(session))
			.map_err(|e| {
				crate::error::invalid_params(format!("Invalid {clause_name} clause: {e}"))
			})?;
	if ast.expressions.len() != 1 || !ast.expressions[0].read_only() {
		return Err(crate::error::invalid_params(format!(
			"{clause_name} clause must be a single read-only clause"
		)));
	}
	Ok(())
}

/// Parse `query_sql` as a SurrealQL query and verify that it resolves to a
/// single statement.
fn validate_single_statement(session: &McpSession, query_sql: &str) -> Result<(), ErrorData> {
	let ast =
		surrealdb_core::syn::runtime::query_with_settings(query_sql, parser_settings(session))
			.map_err(|e| crate::error::invalid_params(format!("Invalid statement: {e}")))?;
	if ast.expressions.len() != 1 {
		return Err(crate::error::invalid_params(
			"Structured CRUD tool must resolve to a single statement",
		));
	}
	Ok(())
}

pub async fn select(session: &McpSession, p: SelectParams) -> Result<CallToolResult, ErrorData> {
	validate_identifier(&p.target)?;
	if let Some(f) = &p.fields {
		validate_fields_clause(session, f)?;
	}
	let fields = p.fields.as_deref().unwrap_or("*");
	let mut q = format!("SELECT {fields}\nFROM {}", p.target);
	if let Some(w) = &p.where_clause {
		validate_where_clause(session, w)?;
		q.push_str(&format!("\nWHERE {w}"));
	}
	if let Some(s) = &p.split_clause {
		if s.trim().is_empty() {
			return Err(crate::error::invalid_params("SPLIT clause cannot be empty"));
		}
		validate_select_probe(
			session,
			"SPLIT",
			&format!("SELECT {fields}\nFROM {}\nSPLIT {s}\nORDER BY __mcp_probe", p.target),
		)?;
		q.push_str(&format!("\nSPLIT {s}"));
	}
	if let Some(g) = &p.group_clause {
		if g.trim().is_empty() {
			return Err(crate::error::invalid_params("GROUP BY clause cannot be empty"));
		}
		validate_select_probe(
			session,
			"GROUP BY",
			&format!("SELECT {fields}\nFROM {}\nGROUP BY {g}\nORDER BY __mcp_probe", p.target),
		)?;
		q.push_str(&format!("\nGROUP BY {g}"));
	}
	if let Some(o) = &p.order_clause {
		if o.trim().is_empty() {
			return Err(crate::error::invalid_params("ORDER BY clause cannot be empty"));
		}
		validate_select_probe(
			session,
			"ORDER BY",
			&format!("SELECT * FROM {}\nORDER BY {o}\nLIMIT 1\nSTART 0", p.target),
		)?;
		q.push_str(&format!("\nORDER BY {o}"));
	}
	if let Some(l) = p.limit_clause {
		q.push_str(&format!("\nLIMIT {l}"));
	}
	if let Some(s) = p.start_clause {
		q.push_str(&format!("\nSTART {s}"));
	}
	if let Some(f) = &p.fetch_clause {
		if f.trim().is_empty() {
			return Err(crate::error::invalid_params("FETCH clause cannot be empty"));
		}
		validate_select_probe(
			session,
			"FETCH",
			&format!("SELECT * FROM {}\nFETCH {f}\nVERSION d'1970-01-01T00:00:00Z'", p.target),
		)?;
		q.push_str(&format!("\nFETCH {f}"));
	}
	validate_select_probe(session, "SELECT", &q)?;
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
	/// Namespace and database this call runs against. Flattened, so the
	/// emitted schema carries `namespace` and `database` directly, each
	/// documenting its own fallback order.
	#[serde(flatten)]
	pub scope: super::ToolScope,
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
	/// Namespace and database this call runs against. Flattened, so the
	/// emitted schema carries `namespace` and `database` directly, each
	/// documenting its own fallback order.
	#[serde(flatten)]
	pub scope: super::ToolScope,
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
	/// Namespace and database this call runs against. Flattened, so the
	/// emitted schema carries `namespace` and `database` directly, each
	/// documenting its own fallback order.
	#[serde(flatten)]
	pub scope: super::ToolScope,
}

pub async fn upsert(session: &McpSession, p: UpsertParams) -> Result<CallToolResult, ErrorData> {
	validate_identifier(&p.target)?;
	let (mode, data) =
		resolve_update_mode(p.content_data.as_ref(), p.merge_data.as_ref(), p.patch_data.as_ref())?;
	let mut q = format!("UPSERT {} {mode} $data", p.target);
	if let Some(w) = &p.where_clause {
		validate_where_clause(session, w)?;
		q.push_str(&format!("\nWHERE {w}"));
	}
	validate_single_statement(session, &q)?;
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
	/// Namespace and database this call runs against. Flattened, so the
	/// emitted schema carries `namespace` and `database` directly, each
	/// documenting its own fallback order.
	#[serde(flatten)]
	pub scope: super::ToolScope,
}

pub async fn update(session: &McpSession, p: UpdateParams) -> Result<CallToolResult, ErrorData> {
	validate_identifier(&p.target)?;
	let (mode, data) =
		resolve_update_mode(p.content_data.as_ref(), p.merge_data.as_ref(), p.patch_data.as_ref())?;
	let mut q = format!("UPDATE {} {mode} $data", p.target);
	if let Some(w) = &p.where_clause {
		validate_where_clause(session, w)?;
		q.push_str(&format!("\nWHERE {w}"));
	}
	validate_single_statement(session, &q)?;
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
	/// Namespace and database this call runs against. Flattened, so the
	/// emitted schema carries `namespace` and `database` directly, each
	/// documenting its own fallback order.
	#[serde(flatten)]
	pub scope: super::ToolScope,
}

pub async fn delete(session: &McpSession, p: DeleteParams) -> Result<CallToolResult, ErrorData> {
	validate_identifier(&p.target)?;
	let mut q = format!("DELETE FROM {}", p.target);
	if let Some(w) = &p.where_clause {
		validate_where_clause(session, w)?;
		q.push_str(&format!("\nWHERE {w}"));
	}
	validate_single_statement(session, &q)?;
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
	/// Namespace and database this call runs against. Flattened, so the
	/// emitted schema carries `namespace` and `database` directly, each
	/// documenting its own fallback order.
	#[serde(flatten)]
	pub scope: super::ToolScope,
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

	use serde_json::json;
	use surrealdb_core::dbs::Session;
	use surrealdb_core::kvs::Datastore;

	use super::*;

	async fn session() -> McpSession {
		let ds = Datastore::new("memory").await.expect("datastore");
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
				scope: Default::default(),
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
				scope: Default::default(),
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
			other @ serde_json::Value::Null
			| other @ serde_json::Value::Bool(_)
			| other @ serde_json::Value::Number(_)
			| other @ serde_json::Value::String(_)
			| other @ serde_json::Value::Array(_)
			| other @ serde_json::Value::Object(_) => other,
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
				scope: Default::default(),
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
				scope: Default::default(),
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
				scope: Default::default(),
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

	/// The schema of a table whose `id` names its components.
	const READING: &str = "DEFINE TABLE reading SCHEMALESS; \
		DEFINE FIELD id ON reading TYPE [city: string, time: datetime];";

	/// The create tool derives the record id from the components the data
	/// carries, answers with the components as fields, and refuses data that
	/// leaves one out rather than drawing a key at random.
	#[tokio::test]
	async fn create_derives_the_record_id_from_the_components_of_a_named_id() {
		let session = session().await;
		session.execute(READING, None).await.expect("schema");

		let created = create(
			&session,
			CreateParams {
				target: "reading".into(),
				data: Some(json!({
					"city": "London",
					"time": { "$ql": "d'2024-01-01T00:00:00Z'" },
					"temp": 1,
				})),
				scope: Default::default(),
			},
		)
		.await
		.expect("call returns");
		let structured = created.structured_content.as_ref().expect("structured");
		assert_eq!(structured.get("status").and_then(|v| v.as_str()), Some("ok"), "{structured}");
		assert_eq!(
			first_row(&created),
			json!({
				"city": "London",
				"id": "reading:['London', d'2024-01-01T00:00:00Z']",
				"temp": 1,
				"time": "2024-01-01T00:00:00Z",
			})
		);

		let refused = create(
			&session,
			CreateParams {
				target: "reading".into(),
				data: Some(json!({ "city": "London", "temp": 2 })),
				scope: Default::default(),
			},
		)
		.await
		.expect("call returns");
		let structured = refused.structured_content.as_ref().expect("structured");
		assert_eq!(structured.get("status").and_then(|v| v.as_str()), Some("error"));
		assert_eq!(
			structured.get("error").and_then(|v| v.as_str()),
			Some(
				"Cannot create a record in `reading` without the record id component `time`: set `time`, give the record id, or define a DEFAULT on the `id` field"
			)
		);
	}

	/// The update tool may restate a component of the record id and may not
	/// change it: the refusal names the value written and the record keeps its
	/// key and its fields.
	#[tokio::test]
	async fn update_cannot_change_a_component_of_a_named_id() {
		let session = session().await;
		session.execute(READING, None).await.expect("schema");
		session
			.execute(
				"CREATE reading SET city = 'London', time = d'2024-01-01T00:00:00Z', temp = 1;",
				None,
			)
			.await
			.expect("seed");

		let refused = update(
			&session,
			UpdateParams {
				target: "reading".into(),
				content_data: None,
				merge_data: Some(json!({ "city": "Paris" })),
				patch_data: None,
				where_clause: Some("city = 'London'".into()),
				scope: Default::default(),
			},
		)
		.await
		.expect("call returns");
		let structured = refused.structured_content.as_ref().expect("structured");
		assert_eq!(structured.get("status").and_then(|v| v.as_str()), Some("error"));
		assert_eq!(
			structured.get("error").and_then(|v| v.as_str()),
			Some(
				"Found 'Paris' for the `city` field, but it is part of the record id of `reading` and does not match it"
			)
		);

		let merged = update(
			&session,
			UpdateParams {
				target: "reading".into(),
				content_data: None,
				merge_data: Some(json!({ "city": "London", "temp": 2 })),
				patch_data: None,
				where_clause: Some("city = 'London'".into()),
				scope: Default::default(),
			},
		)
		.await
		.expect("call returns");
		let structured = merged.structured_content.as_ref().expect("structured");
		assert_eq!(structured.get("status").and_then(|v| v.as_str()), Some("ok"), "{structured}");
		assert_eq!(
			first_row(&merged),
			json!({
				"city": "London",
				"id": "reading:['London', d'2024-01-01T00:00:00Z']",
				"temp": 2,
				"time": "2024-01-01T00:00:00Z",
			})
		);
	}

	/// A record id with an array key is not a target of the structured tools,
	/// so no record of such a table is named through them: they are given the
	/// table, and the key is derived.
	#[tokio::test]
	async fn an_array_record_id_is_not_a_target() {
		let session = session().await;
		session.execute(READING, None).await.expect("schema");
		let rejected = create(
			&session,
			CreateParams {
				target: "reading:['London', d'2024-01-01T00:00:00Z']".into(),
				data: Some(json!({ "temp": 3 })),
				scope: Default::default(),
			},
		)
		.await;
		assert!(rejected.is_err(), "an array key reached the statement");
	}

	#[tokio::test]
	async fn crud_clauses_reject_statement_injection_and_mutations() {
		let session = session().await;
		session
			.execute("CREATE person:alice SET name = 'Alice', age = 30;", None)
			.await
			.expect("seed");

		// Injecting a second statement or FROM clause via `fields` must be rejected.
		let bad_fields = select(
			&session,
			SelectParams {
				target: "person".into(),
				fields: Some("* FROM person; DELETE person; --".into()),
				where_clause: None,
				order_clause: None,
				limit_clause: None,
				start_clause: None,
				group_clause: None,
				split_clause: None,
				fetch_clause: None,
				scope: Default::default(),
			},
		)
		.await;
		assert!(bad_fields.is_err(), "fields clause smuggling FROM/statement must fail");

		// Injecting a mutation expression via `fields` must be rejected.
		let mut_fields = select(
			&session,
			SelectParams {
				target: "person".into(),
				fields: Some("(DELETE person)".into()),
				where_clause: None,
				order_clause: None,
				limit_clause: None,
				start_clause: None,
				group_clause: None,
				split_clause: None,
				fetch_clause: None,
				scope: Default::default(),
			},
		)
		.await;
		assert!(mut_fields.is_err(), "mutating fields clause must fail");

		// Injecting a second statement or trailing clause via `where_clause` must be rejected.
		let bad_where = select(
			&session,
			SelectParams {
				target: "person".into(),
				fields: None,
				where_clause: Some("age > 10; DELETE person".into()),
				order_clause: None,
				limit_clause: None,
				start_clause: None,
				group_clause: None,
				split_clause: None,
				fetch_clause: None,
				scope: Default::default(),
			},
		)
		.await;
		assert!(bad_where.is_err(), "where_clause smuggling statement must fail");

		let bad_where_mut = delete(
			&session,
			DeleteParams {
				target: "person".into(),
				where_clause: Some("(CREATE secret:1) IS NOT NONE".into()),
				scope: Default::default(),
			},
		)
		.await;
		assert!(bad_where_mut.is_err(), "mutating where_clause in delete must fail");

		// Injecting trailing clauses via `order_clause` or `fetch_clause` must be rejected.
		let bad_order = select(
			&session,
			SelectParams {
				target: "person".into(),
				fields: None,
				where_clause: None,
				order_clause: Some("name ASC LIMIT 1".into()),
				limit_clause: None,
				start_clause: None,
				group_clause: None,
				split_clause: None,
				fetch_clause: None,
				scope: Default::default(),
			},
		)
		.await;
		assert!(bad_order.is_err(), "order_clause smuggling LIMIT must fail");

		let bad_fetch = select(
			&session,
			SelectParams {
				target: "person".into(),
				fields: None,
				where_clause: None,
				order_clause: None,
				limit_clause: None,
				start_clause: None,
				group_clause: None,
				split_clause: None,
				fetch_clause: Some("name; DELETE person".into()),
				scope: Default::default(),
			},
		)
		.await;
		assert!(bad_fetch.is_err(), "fetch_clause smuggling statement must fail");
	}
}
