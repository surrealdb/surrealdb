//! Tests for SurrealDB running in the browser, in `tests/`, and the helpers
//! they share.

#![cfg(all(target_family = "wasm", target_vendor = "unknown", target_os = "unknown"))]
#![recursion_limit = "256"]
#![allow(clippy::unwrap_used)]

use surrealdb_core::dbs::capabilities::{ExperimentalTarget, Targets};
use surrealdb_core::dbs::{Capabilities, Session};
use surrealdb_core::kvs::Datastore;
use surrealdb_types::ToSql;

/// The OPFS directory that file buckets in these tests live below.
pub const DIR: &str = "surrealdb-web-tests";

/// Opens an in-memory datastore whose file buckets may use [`DIR`].
pub async fn datastore() -> Datastore {
	Datastore::builder()
		.with_bucket_folder_allowlist(Some(vec![format!("/{DIR}").into()]))
		.with_capabilities(
			Capabilities::all()
				.with_experimental(Targets::Some([ExperimentalTarget::Files].into())),
		)
		.build_with_path("memory")
		.await
		.unwrap()
}

/// Runs a script and returns each statement's result as SurrealQL, or its
/// error message.
pub async fn run(ds: &Datastore, sql: &str) -> Vec<Result<String, String>> {
	let sess = Session::owner().with_ns("test").with_db("test");
	let res = ds.execute(sql, &sess, None).await.unwrap();
	res.into_iter().map(|r| r.result.map(|v| v.to_sql()).map_err(|e| e.to_string())).collect()
}
