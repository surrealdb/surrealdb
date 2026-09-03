//! Regression tests for the security gate on implicit `DEFINE NAMESPACE`
//! / `DEFINE DATABASE` creation through `USE` (RPC `Method::Use`,
//! `Datastore::process_use`, and the SurrealQL `USE` statement).
//!
//! See `SECURITY_GUIDE.md` section 3: "Namespace/database auto-creation
//! via USE in non-strict mode must require the same authorization as
//! explicit DEFINE NAMESPACE/DATABASE."

use anyhow::Result;
use surrealdb_core::dbs::Session;
use surrealdb_iam::{Level, Role};
use surrealdb_types::{RecordId, ToSql, Value};

use crate::helpers::new_ds;

/// Issue `INFO FOR ROOT` as an owner and return the rendered structure.
/// Used to assert which namespaces exist after a `USE` attempt.
async fn root_info(ds: &surrealdb_core::kvs::Datastore) -> Result<String> {
	let owner = Session::owner();
	let mut resp = ds.execute("INFO FOR ROOT", &owner, None).await?;
	let out = resp.remove(0).output()?;
	Ok(out.to_sql())
}

/// Regression: a namespace-level Editor whose namespace has been
/// dropped (or never existed) must NOT be able to re-materialize that
/// namespace via the upwards path of `ensure_ns_db`. The asymmetric
/// case — the database authorization check passes because the session
/// level matches the (missing) parent namespace — is what makes this a
/// real bypass distinct from the simpler "viewer can't create anything"
/// case covered in the language-test reproduction.
#[tokio::test]
async fn use_ns_db_does_not_recreate_dropped_parent_for_namespace_editor() -> Result<()> {
	let (_, ds) = new_ds("host", "host", true).await?;

	// Issue an Editor at Level::Namespace("stale_ns") — equivalent to a
	// token signed at a time when "stale_ns" existed. The namespace was
	// never created in this datastore, so this represents the "namespace
	// was dropped after token issue" state.
	let sess = Session::for_level(Level::Namespace("stale_ns".to_owned()), Role::Editor);

	// `USE NS stale_ns DB anything` via SurrealQL. Pre-fix this would
	// have called `tx.ensure_ns_db("stale_ns", "anything")`, which is
	// `get_or_add_db_upwards(..., upwards = true)` and recreates the
	// parent NS as a side effect. The fix gates database materialization
	// on the parent NS either existing or the caller having `Edit` on
	// `Namespace`@`Root`; the editor here has neither.
	ds.execute("USE NS stale_ns DB anything", &sess, None).await?;

	let info = root_info(&ds).await?;
	assert!(
		!info.contains("stale_ns"),
		"USE must not silently re-create a namespace the caller cannot DEFINE; INFO FOR ROOT mentioned `stale_ns`: {info}",
	);
	Ok(())
}

/// Same property for the `Datastore::process_use` entry point — exercised
/// by the embedded SDK's `Command::Use`.
#[tokio::test]
async fn process_use_does_not_recreate_dropped_parent_for_namespace_editor() -> Result<()> {
	let (_, ds) = new_ds("host", "host", true).await?;

	let mut sess = Session::for_level(Level::Namespace("stale_ns".to_owned()), Role::Editor);

	ds.process_use(None, &mut sess, Some("stale_ns".to_owned()), Some("anything".to_owned()))
		.await
		.expect("process_use returns context, not error");

	assert_eq!(sess.ns.as_deref(), Some("stale_ns"));
	assert_eq!(sess.db.as_deref(), Some("anything"));

	let info = root_info(&ds).await?;
	assert!(
		!info.contains("stale_ns"),
		"process_use must not silently re-create the parent namespace; INFO FOR ROOT mentioned `stale_ns`: {info}",
	);
	Ok(())
}

/// And the same property for an anonymous session with auth enabled — the
/// classic PoC case. The pre-signin RPC pattern still works (context is
/// set), but no resources are created.
#[tokio::test]
async fn process_use_does_not_create_for_anonymous_session_with_auth_enabled() -> Result<()> {
	let (_, ds) = new_ds("host", "host", true).await?;

	let mut sess = Session::default();

	ds.process_use(None, &mut sess, Some("anon_ns".to_owned()), Some("anon_db".to_owned())).await?;

	assert_eq!(sess.ns.as_deref(), Some("anon_ns"));
	assert_eq!(sess.db.as_deref(), Some("anon_db"));

	let info = root_info(&ds).await?;
	assert!(
		!info.contains("anon_ns"),
		"anonymous USE must not auto-create namespace when auth is enabled; INFO FOR ROOT mentioned `anon_ns`: {info}",
	);
	Ok(())
}

/// Sanity check: a Root-level Editor (a user who legitimately has
/// `Edit` on `Namespace`@`Root`) CAN still materialize via `USE` — the
/// fix is targeted at unauthorized creation, not "all creation".
#[tokio::test]
async fn process_use_materializes_for_root_editor() -> Result<()> {
	let (_, ds) = new_ds("host", "host", true).await?;

	let mut sess = Session::editor();

	ds.process_use(None, &mut sess, Some("fresh_ns".to_owned()), Some("fresh_db".to_owned()))
		.await?;

	let info = root_info(&ds).await?;
	assert!(
		info.contains("fresh_ns"),
		"Root Editor must still be able to auto-create namespaces via USE; INFO FOR ROOT did not mention `fresh_ns`: {info}",
	);
	Ok(())
}

/// Provision a second tenant `ns_b`/`db_b` with a `PERMISSIONS FULL` table
/// (fully writable) holding one seed row, as owner.
async fn provision_writable_tenant_b(ds: &surrealdb_core::kvs::Datastore) -> Result<()> {
	ds.execute("DEFINE NS ns_b", &Session::owner(), None).await?;
	ds.execute(
		"DEFINE DB db_b; DEFINE TABLE fullsecret SCHEMALESS PERMISSIONS FULL; \
		 CREATE fullsecret:1 SET data = 'TENANT_B_SECRET';",
		&Session::owner().with_ns("ns_b").with_db("db_b"),
		None,
	)
	.await?;
	Ok(())
}

/// Build a record user of `ns_a`/`db_a` whose session has been pivoted to
/// `ns_b`/`db_b` (the state reached after `USE NS ns_b DB db_b`). The Auth
/// level stays bound to the record's home tenant.
fn pivoted_record_user() -> Session {
	Session::for_record(
		"ns_a",
		"db_a",
		"acc",
		Value::RecordId(RecordId::new("appuser", "alice".to_string())),
	)
	.with_ns("ns_b")
	.with_db("db_b")
}

/// Regression (record-user write isolation): the write pipeline must confine a
/// record user to its own tenant. Even against a `PERMISSIONS FULL` table,
/// every mutation (CREATE / UPDATE / DELETE / INSERT / UPSERT) issued by a
/// record user whose session has been pivoted into a foreign tenant must be
/// denied — mirroring the read path's `validate_record_user_access` gate.
#[tokio::test]
async fn write_pipeline_denies_cross_tenant_record_user() -> Result<()> {
	let (_, ds) = new_ds("ns_a", "db_a", true).await?;
	provision_writable_tenant_b(&ds).await?;

	let sess = pivoted_record_user();

	for stmt in [
		"CREATE fullsecret:evil SET data = 'INJECT'",
		"UPDATE fullsecret:1 SET data = 'TAMPER'",
		"INSERT INTO fullsecret { id: fullsecret:ins, data: 'INS' }",
		"UPSERT fullsecret:1 SET data = 'UPSERTED'",
		"DELETE fullsecret:1",
	] {
		let mut resp = ds.execute(stmt, &sess, None).await?;
		let res = resp.remove(0).output();
		assert!(res.is_err(), "cross-tenant record write must be denied: `{stmt}` => {res:?}");
	}

	// Ground truth: tenant B is untouched and the seed row survives.
	let mut resp = ds
		.execute(
			"SELECT * FROM fullsecret",
			&Session::owner().with_ns("ns_b").with_db("db_b"),
			None,
		)
		.await?;
	let view = resp.remove(0).output()?.to_sql();
	for marker in ["INJECT", "TAMPER", "INS", "UPSERTED"] {
		assert!(!view.contains(marker), "tenant B was mutated cross-tenant ({marker}): {view}");
	}
	assert!(view.contains("TENANT_B_SECRET"), "seed row must survive the blocked DELETE: {view}");
	Ok(())
}

/// Sanity: a record user writing within its OWN tenant, against a table whose
/// permission it satisfies, still works — the gate targets cross-tenant writes
/// only.
#[tokio::test]
async fn write_pipeline_allows_same_tenant_record_user() -> Result<()> {
	let (_, ds) = new_ds("ns_a", "db_a", true).await?;
	ds.execute(
		"DEFINE TABLE t SCHEMALESS PERMISSIONS FULL",
		&Session::owner().with_ns("ns_a").with_db("db_a"),
		None,
	)
	.await?;

	let sess = Session::for_record(
		"ns_a",
		"db_a",
		"acc",
		Value::RecordId(RecordId::new("appuser", "alice".to_string())),
	);
	let mut resp = ds.execute("CREATE t:1 SET ok = true", &sess, None).await?;
	assert!(resp.remove(0).output().is_ok(), "same-tenant record write must be allowed");
	Ok(())
}

/// The namespace and database arguments of `USE` are expressions, so
/// evaluating one can read or write the datastore. Computing them requires
/// the statement's transaction on the context: `Context::tx()` resolves a
/// context without one through `unreachable!()`, and release profiles are
/// `panic = 'abort'`, so an argument that reached the datastore aborted the
/// whole server process rather than failing the query.
///
/// The language-test reproduction covers the same statements across the
/// storage modes; these assert what it cannot, namely that the failed
/// statement's writes are rolled back.
#[tokio::test]
async fn use_ns_db_expression_arguments_fail_as_queries() -> Result<()> {
	let (_, ds) = new_ds("test", "test", false).await?;
	let sess = Session::owner().with_ns("test").with_db("test");

	ds.execute("DEFINE TABLE anything", &sess, None).await?.remove(0).output()?;

	// Each argument reaches the datastore: a subquery reads, `INFO` reads
	// through the catalog, and a nested `DEFINE` writes.
	for (sql, expected) in [
		("USE DB (SELECT VALUE id FROM anything)", "Expected `database` but found `[]`"),
		("USE DB (INFO FOR DB).analyzers", "Expected `database` but found `{  }`"),
		("USE NS (INFO FOR DB).analyzers", "Expected `namespace` but found `{  }`"),
		("USE NS DEFINE NAMESPACE zz", "Expected `namespace` but found `NONE`"),
	] {
		let err = ds
			.execute(sql, &sess, None)
			.await?
			.remove(0)
			.output()
			.expect_err(&format!("`{sql}` must fail as a query"))
			.to_string();
		assert_eq!(err, expected, "unexpected error for `{sql}`");
	}

	// The rejected statements left the session where it was, and the
	// namespace the nested `DEFINE` would have created was rolled back.
	let info = root_info(&ds).await?;
	assert!(!info.contains("zz"), "a failed USE must not commit its argument's writes: {info}");

	let out = ds.execute("SELECT * FROM anything", &sess, None).await?.remove(0).output()?;
	assert_eq!(out.to_sql(), "[]");

	Ok(())
}

/// A computed argument that does coerce to a name selects it, which is what
/// keeps the expression form worth having.
#[tokio::test]
async fn use_db_accepts_a_computed_name() -> Result<()> {
	let (_, ds) = new_ds("test", "test", false).await?;
	let sess = Session::owner().with_ns("test").with_db("test");

	for sql in ["USE DB type::string('x')", "LET $db = 'from_param'; USE DB $db"] {
		let mut resp = ds.execute(sql, &sess, None).await?;
		let out = resp.pop().expect("a response per statement").output()?;
		assert!(
			out.to_sql().contains("database"),
			"`{sql}` should report the selected context, got {}",
			out.to_sql()
		);
	}

	Ok(())
}
