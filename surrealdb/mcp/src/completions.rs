//! Auto-completion support for MCP tool arguments.

use rmcp::model::{CompleteRequestParams, CompleteResult, CompletionInfo};
use surrealdb_core::dbs::QueryResult;

use crate::session::McpSession;

/// Handle a completion request by providing suggestions based on context.
pub async fn handle_completion(
	session: &McpSession,
	params: &CompleteRequestParams,
) -> CompleteResult {
	let values = match params.argument.name.as_str() {
		"table" | "target" => list_tables(session).await,
		"namespace" => list_namespaces(session).await,
		"database" => list_databases(session).await,
		_ => Vec::new(),
	};

	let info = CompletionInfo::with_all_values(values)
		.unwrap_or_else(|_| CompletionInfo::new(Vec::new()).expect("empty vec always valid"));
	CompleteResult::new(info)
}

async fn list_tables(session: &McpSession) -> Vec<String> {
	let Ok(results) = session.execute("INFO FOR DB", None).await else {
		return Vec::new();
	};
	extract_keys(results, "tables")
}

async fn list_namespaces(session: &McpSession) -> Vec<String> {
	let Ok(results) = session.execute("INFO FOR ROOT", None).await else {
		return Vec::new();
	};
	extract_keys(results, "namespaces")
}

async fn list_databases(session: &McpSession) -> Vec<String> {
	let Ok(results) = session.execute("INFO FOR NS", None).await else {
		return Vec::new();
	};
	extract_keys(results, "databases")
}

/// Extract the keys of a single named subtree from an `INFO FOR <scope>`
/// result.
///
/// `INFO FOR <scope>` returns a top-level object keyed by entity kind
/// (`tables`, `functions`, `analyzers`, `params`, `users`, ...). Completion
/// suggestions are kind-specific, so we only iterate the named subtree --
/// otherwise e.g. a `table` argument would suggest function or analyzer
/// names because they live under sibling keys in the same INFO response.
fn extract_keys(results: Vec<QueryResult>, subtree: &str) -> Vec<String> {
	let Some(result) = results.into_iter().next() else {
		return Vec::new();
	};
	let Ok(value) = result.result else {
		return Vec::new();
	};
	let json = value.into_json_value();
	let serde_json::Value::Object(top) = json else {
		return Vec::new();
	};
	top.get(subtree)
		.and_then(|v| v.as_object())
		.into_iter()
		.flat_map(|obj| obj.keys().cloned())
		.take(100)
		.collect()
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use rmcp::model::{ArgumentInfo, CompleteRequestParams, Reference};
	use surrealdb_core::dbs::Session;
	use surrealdb_core::kvs::Datastore;

	use super::*;

	async fn seeded_session() -> McpSession {
		let ds = Arc::new(Datastore::new("memory").await.expect("datastore"));
		ds.execute("DEFINE NAMESPACE test;", &Session::owner(), None).await.expect("ns");
		ds.execute("DEFINE DATABASE test;", &Session::owner().with_ns("test"), None)
			.await
			.expect("db");
		// Seed one entry of every DB-scope kind that sits alongside `tables`
		// under `INFO FOR DB` so that the contamination regressions have
		// something concrete to filter on.
		ds.execute(
			r#"
				DEFINE TABLE alpha;
				DEFINE TABLE beta;
				DEFINE FUNCTION fn::contaminate() { RETURN 1; };
				DEFINE ANALYZER simple TOKENIZERS blank;
				DEFINE PARAM $maximum VALUE 100;
				DEFINE USER leaked ON DATABASE PASSWORD 'correct horse battery staple' ROLES VIEWER;
			"#,
			&Session::owner().with_ns("test").with_db("test"),
			None,
		)
		.await
		.expect("seed db entities");
		// Seed a NS-scope user so `INFO FOR NS` has a sibling of `databases`
		// to prove we don't leak into database suggestions.
		ds.execute(
			"DEFINE USER ns_leaked ON NAMESPACE PASSWORD 'correct horse battery staple' ROLES VIEWER;",
			&Session::owner().with_ns("test"),
			None,
		)
		.await
		.expect("seed ns user");
		// Seed a ROOT-scope user so `INFO FOR ROOT` has a sibling of
		// `namespaces` to prove we don't leak into namespace suggestions.
		ds.execute(
			"DEFINE USER root_leaked ON ROOT PASSWORD 'correct horse battery staple' ROLES VIEWER;",
			&Session::owner(),
			None,
		)
		.await
		.expect("seed root user");
		McpSession::new(ds, Session::owner().with_ns("test").with_db("test"))
	}

	fn make_request(name: &str) -> CompleteRequestParams {
		CompleteRequestParams::new(
			Reference::for_prompt("any"),
			ArgumentInfo {
				name: name.to_string(),
				value: String::new(),
			},
		)
	}

	#[tokio::test]
	async fn dispatches_table_arg_to_table_listing() {
		let session = seeded_session().await;
		for arg in ["table", "target"] {
			let result = handle_completion(&session, &make_request(arg)).await;
			let values = result.completion.values;
			assert!(values.iter().any(|v| v == "alpha"), "{arg}: expected alpha in {values:?}");
			assert!(values.iter().any(|v| v == "beta"), "{arg}: expected beta in {values:?}");
			// Sibling DB-scope subtrees must not leak into table completions.
			for contaminant in ["fn::contaminate", "simple", "maximum", "leaked"] {
				assert!(
					!values.iter().any(|v| v == contaminant),
					"{arg}: `{contaminant}` must not appear in table completions: {values:?}"
				);
			}
		}
	}

	#[tokio::test]
	async fn dispatches_namespace_arg_to_ns_listing() {
		let session = seeded_session().await;
		let result = handle_completion(&session, &make_request("namespace")).await;
		let values = result.completion.values;
		assert!(values.iter().any(|v| v == "test"), "expected `test` namespace in {values:?}");
		// `INFO FOR ROOT` also contains `users` / `nodes`; neither should
		// leak into namespace completions.
		assert!(
			!values.iter().any(|v| v == "root_leaked"),
			"root user must not appear in namespace completions: {values:?}"
		);
	}

	#[tokio::test]
	async fn dispatches_database_arg_to_db_listing() {
		let session = seeded_session().await;
		let result = handle_completion(&session, &make_request("database")).await;
		let values = result.completion.values;
		assert!(values.iter().any(|v| v == "test"), "expected `test` database in {values:?}");
		// `INFO FOR NS` also contains `users` / `accesses`; neither should
		// leak into database completions.
		assert!(
			!values.iter().any(|v| v == "ns_leaked"),
			"ns user must not appear in database completions: {values:?}"
		);
	}

	#[tokio::test]
	async fn unknown_arg_returns_empty() {
		let session = seeded_session().await;
		let result = handle_completion(&session, &make_request("unknown_arg")).await;
		assert!(result.completion.values.is_empty());
	}
}
