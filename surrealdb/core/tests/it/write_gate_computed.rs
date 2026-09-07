//! Regression tests for table write `PERMISSIONS` predicates that read a
//! `COMPUTED` field (GHSA-f6h4-5xf2-86q9).
//!
//! A computed field is stripped before storage, so the record a write gate is
//! evaluated against carries `NONE` for it. A deny-intended clause such as
//! `WHERE blocked != true` was therefore satisfied by `NONE != true` and let
//! every caller through. The gate now materialises the computed fields its
//! predicate reads — on a scratch copy, so the derived values never reach
//! storage — and does so for every write pipeline, not only create/update/delete.

use std::sync::Arc;

use anyhow::Result;
use surrealdb_core::dbs::Session;
use surrealdb_iam::Auth;
use surrealdb_types::ToSql;

use crate::helpers::new_ds;

/// A database record-access session for `account:<name>`, the identity a
/// `DEFINE ACCESS ... TYPE RECORD` user carries. Table permissions are enforced
/// for it, unlike a system user.
fn record_session(name: &str) -> Session {
	Session {
		au: Arc::new(Auth::for_record(format!("account:{name}"), "test", "test", "user")),
		ns: Some("test".to_owned()),
		db: Some("test".to_owned()),
		ac: Some("user".to_owned()),
		..Session::default()
	}
}

/// The derived value a write gate reads must not be written to storage. The
/// gate is evaluated against a scratch copy; `store_record_data` persists
/// `self.current`, which must keep its stored shape.
#[tokio::test]
async fn write_gate_computed_field_not_persisted() -> Result<()> {
	let (_, ds) = new_ds("test", "test", true).await?;
	let owner = Session::owner().with_ns("test").with_db("test");

	// The create/update clauses PASS (`gate` computes to true): the point of this
	// test is what reaches storage on an allowed write, not the deny decision.
	ds.execute(
		"DEFINE ACCESS user ON DATABASE TYPE RECORD
			SIGNUP ( CREATE type::record('account', $id) )
			SIGNIN ( SELECT * FROM type::record('account', $id) )
			DURATION FOR TOKEN 1h, FOR SESSION 1h;
		 DEFINE TABLE keep SCHEMALESS
			PERMISSIONS FOR select FULL, FOR create, update WHERE gate = true;
		 DEFINE FIELD gate ON keep COMPUTED true;
		 CREATE account:alice;",
		&owner,
		None,
	)
	.await?;

	// The record user's create gate reads the COMPUTED `gate`, so it is
	// materialised for the gate to observe.
	let alice = record_session("alice");
	let out = ds.execute("CREATE keep:1 SET name = 'x'", &alice, None).await?.remove(0).output()?;
	assert!(
		out.to_sql().contains("gate: true"),
		"the create gate must observe the computed value and allow the write: {}",
		out.to_sql()
	);

	// Drop the COMPUTED definition so the read-time overlay is gone: a schemaless
	// SELECT then returns only what is stored. A `gate` here would mean the
	// derived value had been persisted by the gate.
	ds.execute("REMOVE FIELD gate ON keep", &owner, None).await?;
	let stored =
		ds.execute("SELECT * FROM keep:1", &owner, None).await?.remove(0).output()?.to_sql();
	assert_eq!(
		stored, "[{ id: keep:1, name: 'x' }]",
		"a computed field a write gate reads must not be persisted: {stored}"
	);

	Ok(())
}

/// The gate is enforced on every write pipeline, not only the create/update/
/// delete entry points the fix's first form touched. `upsert`, `insert` (both
/// its create and its `ON DUPLICATE KEY UPDATE` branch) and `relate` route
/// through the same `check_*_permissions` functions, so a computed-field deny
/// predicate rejects them too.
#[tokio::test]
async fn write_gate_computed_field_enforced_across_pipelines() -> Result<()> {
	let (_, ds) = new_ds("test", "test", true).await?;
	let owner = Session::owner().with_ns("test").with_db("test");

	// `blocked` computes to true, so `blocked != true` is false and every
	// create/update is denied. `a`/`b` are open vertices for the RELATE edge.
	ds.execute(
		"DEFINE ACCESS user ON DATABASE TYPE RECORD
			SIGNUP ( CREATE type::record('account', $id) )
			SIGNIN ( SELECT * FROM type::record('account', $id) )
			DURATION FOR TOKEN 1h, FOR SESSION 1h;
		 DEFINE TABLE t SCHEMALESS
			PERMISSIONS FOR select FULL, FOR create, update WHERE blocked != true;
		 DEFINE FIELD blocked ON t COMPUTED true;
		 DEFINE TABLE edge SCHEMALESS
			PERMISSIONS FOR select FULL, FOR create, update WHERE blocked != true;
		 DEFINE FIELD blocked ON edge COMPUTED true;
		 DEFINE TABLE a SCHEMALESS PERMISSIONS FULL;
		 DEFINE TABLE b SCHEMALESS PERMISSIONS FULL;
		 CREATE account:alice;
		 CREATE a:1, b:1;
		 CREATE t:seed SET name = 'x';",
		&owner,
		None,
	)
	.await?;

	let alice = record_session("alice");

	// Each rejected write is filtered from the result rather than erroring, the
	// same shape the update/delete reproduction asserts.
	for sql in [
		// create branch
		"UPSERT t:new SET name = 'y'",
		"INSERT INTO t { id: t:ins, name: 'y' }",
		"RELATE a:1->edge->b:1",
		// update branch
		"UPSERT t:seed SET name = 'z'",
		"UPDATE t:seed SET name = 'z'",
		"INSERT INTO t { id: t:seed, name: 'y' } ON DUPLICATE KEY UPDATE name = 'z'",
	] {
		let out = ds.execute(sql, &alice, None).await?.remove(0).output()?.to_sql();
		assert_eq!(out, "[]", "the computed-field write gate must reject `{sql}`: {out}");
	}

	// The seed record is untouched and none of the rejected creates persisted.
	let seed =
		ds.execute("SELECT name FROM t:seed", &owner, None).await?.remove(0).output()?.to_sql();
	assert_eq!(seed, "[{ name: 'x' }]", "a rejected update must not mutate the record: {seed}");
	let ids = ds
		.execute("SELECT VALUE id FROM t ORDER BY id", &owner, None)
		.await?
		.remove(0)
		.output()?
		.to_sql();
	assert_eq!(ids, "[t:seed]", "rejected creates must not persist a record: {ids}");
	let edges =
		ds.execute("SELECT VALUE id FROM edge", &owner, None).await?.remove(0).output()?.to_sql();
	assert_eq!(edges, "[]", "a rejected RELATE must not persist an edge: {edges}");

	Ok(())
}
