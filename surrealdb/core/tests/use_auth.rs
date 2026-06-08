#![recursion_limit = "256"]
#![allow(clippy::unwrap_used)]

//! Regression tests for the security gate on implicit `DEFINE NAMESPACE`
//! / `DEFINE DATABASE` creation through `USE` (RPC `Method::Use`,
//! `Datastore::process_use`, and the SurrealQL `USE` statement).
//!
//! See `SECURITY_GUIDE.md` section 3: "Namespace/database auto-creation
//! via USE in non-strict mode must require the same authorization as
//! explicit DEFINE NAMESPACE/DATABASE."

mod helpers;

use anyhow::Result;
use helpers::new_ds;
use surrealdb_core::dbs::Session;
use surrealdb_core::iam::{Level, Role};
use surrealdb_types::ToSql;

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
