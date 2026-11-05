//! The security mode of a custom API, on the route the HTTP server uses.
//!
//! `invoke_api_handler` is what `/api/:ns/:db/:endpoint` calls. A handler
//! defined `SECURITY DEFINER` (the default) runs with table and field
//! permissions lifted; one defined `SECURITY INVOKER` runs under the caller's,
//! whoever that is — including a caller with no credentials at all, which is
//! what makes a public endpoint public. In both modes the handler is bounded by
//! the auth of the identity that defined it.

use anyhow::Result;
use surrealdb_catalog::ApiMethod;
use surrealdb_core::api::request::ApiRequest;
use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_iam::{Level, Role};
use surrealdb_types::{RecordId, ToSql, Value};

use crate::helpers::new_ds;

fn get_request() -> ApiRequest {
	ApiRequest {
		method: ApiMethod::Get,
		request_id: "api-security".to_string(),
		..Default::default()
	}
}

async fn run(dbs: &Datastore, sql: &str, sess: &Session) -> Result<()> {
	for res in dbs.execute(sql, sess, None).await? {
		res.result?;
	}
	Ok(())
}

/// The rendering of the body the handler at `path` answers with.
async fn body(dbs: &Datastore, sess: &Session, path: &str) -> Result<String> {
	let resp = dbs.invoke_api_handler("test", "test", path, sess, get_request()).await?;
	assert_eq!(
		resp.status.as_u16(),
		200,
		"`{path}` should answer 200, got {} body {:?}",
		resp.status,
		resp.body
	);
	Ok(resp.body.to_sql())
}

/// A table nobody but a system user may read, and one public API per security
/// mode that reads it.
async fn setup(dbs: &Datastore) -> Result<()> {
	run(
		dbs,
		r#"
			DEFINE ACCESS account ON DATABASE TYPE RECORD
				SIGNIN ( SELECT * FROM user WHERE id = $id )
				DURATION FOR SESSION 1h;
			CREATE user:one;
			DEFINE TABLE secrets PERMISSIONS NONE;
			CREATE secrets:one SET flag = 'THE_SECRET';
			DEFINE API "/definer" FOR get PERMISSIONS FULL THEN {
				{ status: 200, body: (SELECT VALUE flag FROM secrets) };
			};
			DEFINE API "/invoker" FOR get PERMISSIONS FULL THEN {
				{ status: 200, body: (SELECT VALUE flag FROM secrets) };
			} SECURITY INVOKER;
		"#,
		&Session::owner().with_ns("test").with_db("test"),
	)
	.await
}

/// An API a caller with no credentials may reach still reads what the caller may
/// read when it is `SECURITY INVOKER`, and what the table refuses it when it is
/// not.
#[tokio::test]
async fn an_anonymous_caller_is_held_to_its_own_permissions_by_an_invoker_api() -> Result<()> {
	let (_, dbs) = new_ds("test", "test", true).await?;
	setup(&dbs).await?;

	let anonymous = Session::default().with_ns("test").with_db("test");

	assert!(
		body(&dbs, &anonymous, "definer").await?.contains("THE_SECRET"),
		"a definer API runs with the table's permissions lifted"
	);
	let invoked = body(&dbs, &anonymous, "invoker").await?;
	assert!(
		!invoked.contains("THE_SECRET"),
		"an invoker API handed an anonymous caller a table that refuses it: {invoked}"
	);
	Ok(())
}

/// The same holds for a record user, and a system user keeps what its role
/// grants in either mode.
#[tokio::test]
async fn a_record_user_is_held_to_its_own_permissions_by_an_invoker_api() -> Result<()> {
	let (_, dbs) = new_ds("test", "test", true).await?;
	setup(&dbs).await?;

	let record = Session::for_record(
		"test",
		"test",
		"account",
		Value::RecordId(RecordId::new("user", "one".to_string())),
	);
	assert!(body(&dbs, &record, "definer").await?.contains("THE_SECRET"));
	let invoked = body(&dbs, &record, "invoker").await?;
	assert!(
		!invoked.contains("THE_SECRET"),
		"an invoker API handed a record user a table that refuses it: {invoked}"
	);

	let owner = Session::owner().with_ns("test").with_db("test");
	assert!(body(&dbs, &owner, "definer").await?.contains("THE_SECRET"));
	assert!(
		body(&dbs, &owner, "invoker").await?.contains("THE_SECRET"),
		"an owner is not subject to table permissions, so an invoker API keeps its view"
	);
	Ok(())
}

/// A handler is bounded by the authority of whoever defined the API, in either
/// mode: `SECURITY INVOKER` takes the caller's permissions, never the caller's
/// privilege. Only an owner may define an API, so the bound that matters is the
/// level: a database owner's API, run by a root owner, still cannot reach what
/// only a root identity can.
///
/// `INFO FOR ROOT` is the probe — it needs a root-level identity — and the
/// namespace planted for it to find is what a leak would show.
#[tokio::test]
async fn a_handler_is_bounded_by_its_definers_authority_in_either_mode() -> Result<()> {
	let (_, dbs) = new_ds("test", "test", true).await?;

	let root = Session::owner().with_ns("test").with_db("test");
	let db_owner =
		Session::for_level(Level::Database("test".to_string(), "test".to_string()), Role::Owner)
			.with_ns("test")
			.with_db("test");

	run(&dbs, "DEFINE NAMESPACE leaked_ns", &Session::owner()).await?;
	run(
		&dbs,
		r#"
			DEFINE API "/definer" FOR get PERMISSIONS FULL THEN {
				{ status: 200, body: (INFO FOR ROOT) };
			};
			DEFINE API "/invoker" FOR get PERMISSIONS FULL THEN {
				{ status: 200, body: (INFO FOR ROOT) };
			} SECURITY INVOKER;
		"#,
		&db_owner,
	)
	.await?;

	// The probe finds the planted namespace when the caller really is root.
	let mut res = dbs.execute("INFO FOR ROOT", &root, None).await?;
	let direct = res.remove(0).result?.to_sql();
	assert!(
		direct.contains("leaked_ns"),
		"the probe does not show the planted namespace to a root caller: {direct}"
	);

	for path in ["definer", "invoker"] {
		let resp = dbs.invoke_api_handler("test", "test", path, &root, get_request()).await?;
		let rendered = format!("{:?}", resp.body);
		assert!(
			!rendered.contains("leaked_ns"),
			"the `{path}` API ran with its root caller's authority instead of its definer's \
			 (answered {}): {rendered}",
			resp.status
		);
	}
	Ok(())
}
