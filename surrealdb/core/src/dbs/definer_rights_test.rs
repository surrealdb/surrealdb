//! Definer's-rights (`AUTH LIMIT`) parity between the two execution engines.
//!
//! `DEFINE FIELD` stamps the definer's `(level, max_role)` onto the field
//! (`AuthLimit::new_from_auth`), and `Options::limited_by` narrows the auth used
//! to evaluate that field's bodies so a body cannot exercise more privilege than
//! the identity that wrote it.
//!
//! The narrowing applies on the write path (`doc/field.rs`, `doc/event.rs`), to a
//! field's `PERMISSIONS FOR select` predicate (`doc/reduce.rs`), and to a
//! `COMPUTED` body evaluated at read time (`doc/compute.rs` on the legacy engine,
//! `compute_fields_for_value` on the streaming one). Both engines must agree.
//!
//! Note the narrowing is a no-op for record and anonymous readers
//! (`Actor::new_limited` returns early for those levels), so a test that wants to
//! observe it at all needs a system-user definer strictly below a system-user
//! reader — which is what these use.

use anyhow::Result;
use surrealdb_types::Value;
use test_log::test;

use crate::catalog::providers::CatalogProvider;
use crate::dbs::{NewPlannerStrategy, Session};
use crate::iam::{Level, Role};
use crate::kvs::{Datastore, TransactionType};

/// Read `thing:1`'s root-only `COMPUTED` field as root Owner, where the field was
/// defined by a database-level Editor.
///
/// `INFO FOR ROOT` is the probe: it requires a root-level identity, so it is
/// refused under the definer's `(Database, Editor)` narrowing and returns the
/// root catalog without it.
async fn read_root_only_computed_field(
	strategy: NewPlannerStrategy,
) -> Result<Result<Value, surrealdb_types::Error>> {
	let ds = Datastore::new("memory").await?;
	{
		let tx = ds.transaction(TransactionType::Write).await?;
		tx.ensure_ns_db(None, "test", "test").await?;
		tx.commit().await?;
	}

	// Root Owner prepares the table and one record.
	let root = Session::owner().with_ns("test").with_db("test");
	for response in
		ds.execute("DEFINE TABLE thing SCHEMALESS; CREATE thing:1;", &root, None).await?
	{
		response.result?;
	}

	// A database-level Editor defines the computed field, stamping its own
	// (Database, Editor) auth onto it as the field's AUTH LIMIT.
	let definer =
		Session::for_level(Level::Database("test".to_owned(), "test".to_owned()), Role::Editor);
	for response in
		ds.execute("DEFINE FIELD leak ON thing COMPUTED (INFO FOR ROOT);", &definer, None).await?
	{
		response.result?;
	}

	// Root Owner reads the field back.
	let reader = Session::owner().with_ns("test").with_db("test").new_planner_strategy(strategy);
	let mut responses = ds.execute("SELECT leak FROM thing:1;", &reader, None).await?;
	Ok(responses.remove(0).result)
}

/// Describe a probe outcome as either the namespace names it disclosed or the
/// refusal it produced, so the two engines can be compared on both axes at once.
fn outcome(result: Result<Value, surrealdb_types::Error>) -> Result<Vec<String>, String> {
	let value = match result {
		Ok(value) => value,
		Err(err) => return Err(err.to_string()),
	};
	let Value::Array(rows) = value else {
		return Err(format!("expected an array of rows, got {value:?}"));
	};
	let Some(Value::Object(row)) = rows.first().cloned() else {
		return Err(format!("expected one object row, got {rows:?}"));
	};
	let Some(Value::Object(info)) = row.get("leak").cloned() else {
		return Err(format!("expected an object in `leak`, got {row:?}"));
	};
	let Some(Value::Object(namespaces)) = info.get("namespaces").cloned() else {
		return Err(format!("expected `namespaces` in the root info, got {info:?}"));
	};
	let mut names: Vec<String> = namespaces.keys().cloned().collect();
	names.sort();
	Ok(names)
}

/// A `(Database, Editor)` definer's `INFO FOR ROOT` body must be refused even
/// when the reader is root Owner, and both engines must refuse it the same way.
#[test(tokio::test(flavor = "multi_thread"))]
async fn both_engines_narrow_a_read_time_computed_body_to_the_definer() -> Result<()> {
	let legacy = outcome(read_root_only_computed_field(NewPlannerStrategy::ComputeOnly).await?);
	let streaming =
		outcome(read_root_only_computed_field(NewPlannerStrategy::AllReadOnlyStatements).await?);

	assert_eq!(
		legacy, streaming,
		"the two engines disagree on the auth used for a read-time COMPUTED body"
	);
	let err = legacy.expect_err("a (Database, Editor) definer must not reach INFO FOR ROOT");
	assert!(err.contains("Not enough permissions"), "expected a permission refusal, got: {err}");
	Ok(())
}
