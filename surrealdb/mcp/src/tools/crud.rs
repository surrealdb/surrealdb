//! CRUD operation tools: select, create, insert, upsert, update, delete, relate.
//!
//! Data values (CONTENT, MERGE, INSERT data) are bound via Variables to prevent
//! injection. WHERE clauses are SurrealQL expression fragments (same model as
//! /sql and /rpc endpoints). Table names and record IDs are validated.

use rmcp::ErrorData;
use rmcp::model::CallToolResult;
use schemars::JsonSchema;
use serde::Deserialize;
use surrealdb_types::Variables;

use super::{json_to_surreal_value, result_to_text, validate_identifier};
use crate::session::McpSession;

/// Build Variables with a single "data" key bound to the JSON value.
fn bind_data(json: &serde_json::Value) -> Variables {
	let mut vars = Variables::new();
	vars.insert("data", json_to_surreal_value(json));
	vars
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct SelectParams {
	/// Table or record targets (e.g. "person", "person:john").
	pub targets: String,
	/// Optional fields to select (defaults to *).
	pub fields: Option<String>,
	/// Optional WHERE clause -- a SurrealQL expression (e.g. "age > 18").
	pub where_clause: Option<String>,
	/// Optional ORDER BY clause (e.g. "name ASC").
	pub order_clause: Option<String>,
	/// Optional LIMIT value.
	pub limit_clause: Option<u64>,
	/// Optional START value for pagination.
	pub start_clause: Option<u64>,
	/// Optional GROUP BY clause.
	pub group_clause: Option<String>,
	/// Optional SPLIT clause.
	pub split_clause: Option<String>,
}

pub async fn select(session: &McpSession, p: SelectParams) -> Result<CallToolResult, ErrorData> {
	validate_identifier(&p.targets)?;
	let fields = p.fields.as_deref().unwrap_or("*");
	let mut q = format!("SELECT {fields} FROM {}", p.targets);
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
	let results = session.execute(&q, None).await?;
	Ok(result_to_text(results))
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct CreateParams {
	/// Target table or record ID (e.g. "person" or "person:john").
	pub target: String,
	/// JSON data for the record content. Bound as $data variable.
	pub data: Option<serde_json::Value>,
}

pub async fn create(session: &McpSession, p: CreateParams) -> Result<CallToolResult, ErrorData> {
	validate_identifier(&p.target)?;
	let (q, vars) = match &p.data {
		Some(d) => (format!("CREATE {} CONTENT $data", p.target), Some(bind_data(d))),
		None => (format!("CREATE {}", p.target), None),
	};
	let results = session.execute(&q, vars).await?;
	Ok(result_to_text(results))
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct InsertParams {
	/// Target table to insert into.
	pub target: String,
	/// JSON array of objects or single object to insert. Bound as $data variable.
	pub data: serde_json::Value,
	/// Whether to ignore duplicate key errors.
	#[serde(default)]
	pub ignore: bool,
	/// Whether this is a relation insert.
	#[serde(default)]
	pub relation: bool,
}

pub async fn insert(session: &McpSession, p: InsertParams) -> Result<CallToolResult, ErrorData> {
	validate_identifier(&p.target)?;
	let ignore = if p.ignore {
		" IGNORE"
	} else {
		""
	};
	let relation = if p.relation {
		" RELATION"
	} else {
		""
	};
	let q = format!("INSERT{ignore}{relation} INTO {} $data", p.target);
	let results = session.execute(&q, Some(bind_data(&p.data))).await?;
	Ok(result_to_text(results))
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct UpsertParams {
	/// Target table or record (e.g. "person" or "person:john").
	pub targets: String,
	/// JSON data for CONTENT mode (replaces entire record). Bound as $data.
	pub content_data: Option<serde_json::Value>,
	/// JSON data for MERGE mode (merges with existing). Bound as $data.
	pub merge_data: Option<serde_json::Value>,
	/// JSON patch operations for PATCH mode. Bound as $data.
	pub patch_data: Option<serde_json::Value>,
	/// Optional WHERE clause -- a SurrealQL expression.
	pub where_clause: Option<String>,
}

pub async fn upsert(session: &McpSession, p: UpsertParams) -> Result<CallToolResult, ErrorData> {
	validate_identifier(&p.targets)?;
	let (mode, data) =
		resolve_update_mode(p.content_data.as_ref(), p.merge_data.as_ref(), p.patch_data.as_ref())?;
	let mut q = format!("UPSERT {} {mode} $data", p.targets);
	if let Some(w) = &p.where_clause {
		q.push_str(&format!(" WHERE {w}"));
	}
	let results = session.execute(&q, Some(bind_data(data))).await?;
	Ok(result_to_text(results))
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct UpdateParams {
	/// Target table or record (e.g. "person" or "person:john").
	pub targets: String,
	/// JSON data for CONTENT mode. Bound as $data.
	pub content_data: Option<serde_json::Value>,
	/// JSON data for MERGE mode. Bound as $data.
	pub merge_data: Option<serde_json::Value>,
	/// JSON patch operations for PATCH mode. Bound as $data.
	pub patch_data: Option<serde_json::Value>,
	/// Optional WHERE clause -- a SurrealQL expression.
	pub where_clause: Option<String>,
}

pub async fn update(session: &McpSession, p: UpdateParams) -> Result<CallToolResult, ErrorData> {
	validate_identifier(&p.targets)?;
	let (mode, data) =
		resolve_update_mode(p.content_data.as_ref(), p.merge_data.as_ref(), p.patch_data.as_ref())?;
	let mut q = format!("UPDATE {} {mode} $data", p.targets);
	if let Some(w) = &p.where_clause {
		q.push_str(&format!(" WHERE {w}"));
	}
	let results = session.execute(&q, Some(bind_data(data))).await?;
	Ok(result_to_text(results))
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct DeleteParams {
	/// Target table or record to delete.
	pub targets: String,
	/// Optional WHERE clause -- a SurrealQL expression.
	pub where_clause: Option<String>,
}

pub async fn delete(session: &McpSession, p: DeleteParams) -> Result<CallToolResult, ErrorData> {
	validate_identifier(&p.targets)?;
	let mut q = format!("DELETE FROM {}", p.targets);
	if let Some(w) = &p.where_clause {
		q.push_str(&format!(" WHERE {w}"));
	}
	let results = session.execute(&q, None).await?;
	Ok(result_to_text(results))
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
	pub content_data: Option<serde_json::Value>,
}

pub async fn relate(session: &McpSession, p: RelateParams) -> Result<CallToolResult, ErrorData> {
	validate_identifier(&p.from)?;
	validate_identifier(&p.table)?;
	validate_identifier(&p.with)?;
	let mut q = format!("RELATE {}->{}->{}", p.from, p.table, p.with);
	let vars = if let Some(d) = &p.content_data {
		q.push_str(" CONTENT $data");
		Some(bind_data(d))
	} else {
		None
	};
	let results = session.execute(&q, vars).await?;
	Ok(result_to_text(results))
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
