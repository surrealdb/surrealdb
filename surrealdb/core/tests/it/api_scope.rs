//! Regression coverage for GHSA-848m-r628-vrxw.
//!
//! Custom API handlers ultimately run with permissions disabled, so reaching
//! one for a namespace/database the caller is not authenticated for is a
//! cross-tenant authorization bypass. The target ns/db can be steered by
//! caller-controlled input — the URL path on the HTTP route
//! (`/api/:ns/:db/:endpoint`), or the session's selected ns/db (headers / `USE`)
//! for the `api::invoke` SQL function. The authenticated level — not the
//! caller-supplied ns/db — is the source of truth, so a principal scoped to one
//! tenant must be rejected before another tenant's handler runs.

use anyhow::Result;
use surrealdb_catalog::ApiMethod;
use surrealdb_core::api::request::ApiRequest;
use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_iam::{Level, Role};
use surrealdb_types::ToSql;

use crate::helpers::new_ds;

async fn run(dbs: &Datastore, sql: &str, sess: &Session) -> Result<()> {
	for res in dbs.execute(sql, sess, None).await? {
		res.result?;
	}
	Ok(())
}

fn get_request() -> ApiRequest {
	ApiRequest {
		method: ApiMethod::Get,
		request_id: "ghsa-848m".to_string(),
		..Default::default()
	}
}

/// Define the victim tenant: a `PERMISSIONS NONE` secret table and a
/// `PERMISSIONS FULL` custom API that reads it. Returns a root session already
/// scoped to the victim namespace/database.
async fn setup_victim_tenant(dbs: &Datastore) -> Result<Session> {
	let root = Session::owner();
	run(dbs, "DEFINE NAMESPACE victim_ns", &root).await?;
	run(dbs, "DEFINE DATABASE victim_db", &root.clone().with_ns("victim_ns")).await?;
	let victim_admin = root.with_ns("victim_ns").with_db("victim_db");
	run(
		dbs,
		r#"
			DEFINE TABLE secrets PERMISSIONS NONE;
			CREATE secrets:one SET flag = 'FLAG_SHOULD_NOT_LEAK';
			DEFINE API "/leak" FOR get PERMISSIONS FULL THEN {
				{ status: 200, body: (SELECT VALUE flag FROM secrets) };
			};
		"#,
		&victim_admin,
	)
	.await?;
	Ok(victim_admin)
}

/// The HTTP custom API route (`invoke_api_handler`) must reject a caller whose
/// authenticated scope does not cover the URL ns/db, while same-scope,
/// namespace-scope and root callers keep working.
#[tokio::test]
async fn http_route_rejects_cross_tenant_scope() -> Result<()> {
	// Datastore with auth enabled; the helper pre-creates attacker_ns/attacker_db.
	let (_, dbs) = new_ds("attacker_ns", "attacker_db", true).await?;
	let victim_admin = setup_victim_tenant(&dbs).await?;

	// Attacker: only a database Viewer for attacker_ns/attacker_db. The HTTP
	// route overwrites the session's selected ns/db with the victim scope from
	// the URL, so replicate that here — the authenticated level stays attacker.
	let attacker = Session::for_level(
		Level::Database("attacker_ns".to_string(), "attacker_db".to_string()),
		Role::Viewer,
	)
	.with_ns("victim_ns")
	.with_db("victim_db");

	let resp =
		dbs.invoke_api_handler("victim_ns", "victim_db", "leak", &attacker, get_request()).await?;
	assert_eq!(
		resp.status.as_u16(),
		403,
		"cross-tenant custom API call must be rejected with 403, got {} body {:?}",
		resp.status,
		resp.body
	);
	assert!(
		!format!("{:?}", resp.body).contains("FLAG_SHOULD_NOT_LEAK"),
		"victim secret leaked across the tenant boundary: {:?}",
		resp.body
	);

	// A legitimate victim-scope database user still reaches its own API.
	let victim_user = Session::for_level(
		Level::Database("victim_ns".to_string(), "victim_db".to_string()),
		Role::Viewer,
	)
	.with_ns("victim_ns")
	.with_db("victim_db");
	let resp = dbs
		.invoke_api_handler("victim_ns", "victim_db", "leak", &victim_user, get_request())
		.await?;
	assert_eq!(
		resp.status.as_u16(),
		200,
		"victim-scope call should succeed, got {} body {:?}",
		resp.status,
		resp.body
	);

	// A namespace-scoped principal may reach any database in its namespace.
	let victim_ns_user =
		Session::for_level(Level::Namespace("victim_ns".to_string()), Role::Viewer)
			.with_ns("victim_ns")
			.with_db("victim_db");
	let resp = dbs
		.invoke_api_handler("victim_ns", "victim_db", "leak", &victim_ns_user, get_request())
		.await?;
	assert_eq!(
		resp.status.as_u16(),
		200,
		"namespace-scope call should succeed, got {} body {:?}",
		resp.status,
		resp.body
	);

	// Root may invoke any tenant's API.
	let resp = dbs
		.invoke_api_handler("victim_ns", "victim_db", "leak", &victim_admin, get_request())
		.await?;
	assert_eq!(
		resp.status.as_u16(),
		200,
		"root call should succeed, got {} body {:?}",
		resp.status,
		resp.body
	);

	Ok(())
}

/// The `api::invoke` SQL function shares the dispatch chokepoint with the HTTP
/// route. A caller authenticated for one tenant whose session points at another
/// tenant (as mismatched HTTP headers or `USE` would do) must not be able to run
/// the victim handler.
#[tokio::test]
async fn api_invoke_rejects_cross_tenant_scope() -> Result<()> {
	let (_, dbs) = new_ds("attacker_ns", "attacker_db", true).await?;
	let _ = setup_victim_tenant(&dbs).await?;

	// Attacker authenticated for attacker_ns/attacker_db but pointing the
	// session at the victim scope.
	let attacker = Session::for_level(
		Level::Database("attacker_ns".to_string(), "attacker_db".to_string()),
		Role::Viewer,
	)
	.with_ns("victim_ns")
	.with_db("victim_db");

	let mut res = dbs.execute(r#"RETURN api::invoke("/leak")"#, &attacker, None).await?;
	let rendered = res.remove(0).result?.to_sql();
	assert!(
		!rendered.contains("FLAG_SHOULD_NOT_LEAK"),
		"api::invoke leaked victim data across the tenant boundary: {rendered}"
	);
	assert!(
		rendered.contains("403"),
		"expected a 403 response from cross-tenant api::invoke, got: {rendered}"
	);

	// The legitimate victim-scope user still reaches the handler via api::invoke.
	let victim_user = Session::for_level(
		Level::Database("victim_ns".to_string(), "victim_db".to_string()),
		Role::Viewer,
	)
	.with_ns("victim_ns")
	.with_db("victim_db");
	let mut res = dbs.execute(r#"RETURN api::invoke("/leak")"#, &victim_user, None).await?;
	let rendered = res.remove(0).result?.to_sql();
	assert!(
		rendered.contains("FLAG_SHOULD_NOT_LEAK"),
		"victim-scope api::invoke should return its own data, got: {rendered}"
	);

	Ok(())
}
