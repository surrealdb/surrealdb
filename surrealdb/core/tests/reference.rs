#![recursion_limit = "256"]
#![allow(clippy::unwrap_used)]

mod helpers;

use std::sync::{Arc, Mutex};

use anyhow::Result;
use helpers::{Test, new_ds, new_ns_db};
use surrealdb_core::dbs::Session;
use surrealdb_core::dbs::capabilities::Capabilities;
use surrealdb_core::kvs::Datastore;
use surrealdb_core::observe::{
	ExecutionObserver, Outcome, TransactionEvent, TransactionMetricsSnapshot,
};
use surrealdb_core::syn;
use surrealdb_types::Value;

#[derive(Default)]
struct CapturingTransactionObserver {
	metrics: Mutex<Vec<TransactionMetricsSnapshot>>,
}

impl CapturingTransactionObserver {
	fn clear(&self) {
		self.metrics.lock().unwrap().clear();
	}

	fn snapshot(&self) -> Vec<TransactionMetricsSnapshot> {
		self.metrics.lock().unwrap().clone()
	}
}

impl ExecutionObserver for CapturingTransactionObserver {
	fn on_transaction_complete(&self, event: &TransactionEvent) {
		if event.safe.write && event.safe.outcome == Outcome::Success {
			self.metrics.lock().unwrap().push(event.safe.metrics);
		}
	}
}

async fn new_observed_ds() -> Result<(Datastore, Arc<CapturingTransactionObserver>)> {
	let observer = Arc::new(CapturingTransactionObserver::default());
	let ds = Datastore::builder()
		.with_capabilities(Capabilities::all())
		.with_observer(observer.clone())
		.build_with_path("memory")
		.await?;
	new_ns_db(&ds, "test", "test").await?;
	observer.clear();
	Ok((ds, observer))
}

async fn run_ok(ds: &Datastore, ses: &Session, sql: &str) -> Result<()> {
	for response in ds.execute(sql, ses, None).await? {
		response.output()?;
	}
	Ok(())
}

async fn query_value(ds: &Datastore, ses: &Session, sql: &str) -> Result<Value> {
	let mut res = ds.execute(sql, ses, None).await?;
	Ok(res.remove(0).output()?)
}

#[tokio::test]
async fn define_reference_backfills_existing_records() -> Result<()> {
	let (_, ds) = new_ds("test", "test", false).await?;
	let ses = Session::owner().with_ns("test").with_db("test");

	run_ok(
		&ds,
		&ses,
		"
		DEFINE TABLE child SCHEMAFULL;
		DEFINE FIELD name ON child TYPE string;
		DEFINE TABLE parent SCHEMAFULL;
		DEFINE FIELD name ON parent TYPE string;
		DEFINE FIELD child_ref ON parent TYPE record<child>;
		CREATE child:c1 SET name = 'child1';
		CREATE parent:p1 SET name = 'parent1', child_ref = child:c1;
		DEFINE FIELD OVERWRITE child_ref ON parent TYPE record<child> REFERENCE ON DELETE REJECT;
		DEFINE FIELD refs ON child COMPUTED <~(parent FIELD child_ref);
		",
	)
	.await?;

	assert_eq!(
		query_value(&ds, &ses, "RETURN child:c1<~(parent FIELD child_ref)").await?,
		syn::value("[parent:p1]")?
	);
	assert_eq!(query_value(&ds, &ses, "RETURN child:c1.refs").await?, syn::value("[parent:p1]")?);

	Ok(())
}

#[tokio::test]
async fn alter_reference_rejects_invalid_reference_fields() -> Result<()> {
	Test::new(
		"
		DEFINE FIELD ref ON parent TYPE number;
		ALTER FIELD ref ON parent REFERENCE;
		DEFINE FIELD nested.ref ON parent TYPE record;
		ALTER FIELD nested.ref ON parent REFERENCE;
		",
	)
	.await?
	.expect_val("NONE")?
	.expect_error("Cannot use the `REFERENCE` keyword with `TYPE number`. Specify only a `record` type, or a type containing only records, instead.")?
	.expect_val("NONE")?
	.expect_error("Cannot use the `REFERENCE` keyword on nested field `nested.ref`. Specify a referencing field at the root level instead.")?;

	Ok(())
}

#[tokio::test]
async fn define_reference_does_not_backfill_records_outside_declared_type() -> Result<()> {
	let (_, ds) = new_ds("test", "test", false).await?;
	let ses = Session::owner().with_ns("test").with_db("test");

	run_ok(
		&ds,
		&ses,
		"
		CREATE other:o;
		CREATE parent:p1 SET child_ref = other:o;
		DEFINE FIELD OVERWRITE child_ref ON parent TYPE record<child> REFERENCE ON DELETE IGNORE;
		",
	)
	.await?;
	assert_eq!(
		query_value(&ds, &ses, "RETURN other:o<~(parent FIELD child_ref)").await?,
		syn::value("[]")?
	);

	run_ok(&ds, &ses, "ALTER FIELD child_ref ON parent DROP REFERENCE").await?;
	assert_eq!(
		query_value(&ds, &ses, "RETURN other:o<~(parent FIELD child_ref)").await?,
		syn::value("[]")?
	);

	Ok(())
}

#[tokio::test]
async fn define_reference_type_narrowing_removes_old_outside_type_reference_keys() -> Result<()> {
	let (_, ds) = new_ds("test", "test", false).await?;
	let ses = Session::owner().with_ns("test").with_db("test");

	run_ok(
		&ds,
		&ses,
		"
		CREATE other:o;
		DEFINE FIELD child_ref ON parent TYPE record REFERENCE ON DELETE IGNORE;
		CREATE parent:p1 SET child_ref = other:o;
		",
	)
	.await?;
	assert_eq!(
		query_value(&ds, &ses, "RETURN other:o<~(parent FIELD child_ref)").await?,
		syn::value("[parent:p1]")?
	);

	run_ok(
		&ds,
		&ses,
		"DEFINE FIELD OVERWRITE child_ref ON parent TYPE record<child> REFERENCE ON DELETE IGNORE",
	)
	.await?;
	assert_eq!(
		query_value(&ds, &ses, "RETURN other:o<~(parent FIELD child_ref)").await?,
		syn::value("[]")?
	);

	Ok(())
}

#[tokio::test]
async fn alter_reference_type_narrowing_removes_old_outside_type_reference_keys() -> Result<()> {
	let (_, ds) = new_ds("test", "test", false).await?;
	let ses = Session::owner().with_ns("test").with_db("test");

	run_ok(
		&ds,
		&ses,
		"
		CREATE other:o;
		DEFINE FIELD child_ref ON parent TYPE record REFERENCE ON DELETE IGNORE;
		CREATE parent:p1 SET child_ref = other:o;
		",
	)
	.await?;
	assert_eq!(
		query_value(&ds, &ses, "RETURN other:o<~(parent FIELD child_ref)").await?,
		syn::value("[parent:p1]")?
	);

	run_ok(&ds, &ses, "ALTER FIELD child_ref ON parent TYPE record<child>").await?;
	assert_eq!(
		query_value(&ds, &ses, "RETURN other:o<~(parent FIELD child_ref)").await?,
		syn::value("[]")?
	);

	Ok(())
}

#[tokio::test]
async fn alter_reference_backfills_existing_records() -> Result<()> {
	let (_, ds) = new_ds("test", "test", false).await?;
	let ses = Session::owner().with_ns("test").with_db("test");

	run_ok(
		&ds,
		&ses,
		"
		DEFINE TABLE child SCHEMAFULL;
		DEFINE FIELD name ON child TYPE string;
		DEFINE TABLE parent SCHEMAFULL;
		DEFINE FIELD name ON parent TYPE string;
		DEFINE FIELD child_ref ON parent TYPE record<child>;
		CREATE child:c1 SET name = 'child1';
		CREATE parent:p1 SET name = 'parent1', child_ref = child:c1;
		ALTER FIELD child_ref ON parent REFERENCE ON DELETE REJECT;
		",
	)
	.await?;

	assert_eq!(
		query_value(&ds, &ses, "RETURN child:c1<~(parent FIELD child_ref)").await?,
		syn::value("[parent:p1]")?
	);

	Ok(())
}

#[tokio::test]
async fn changing_reference_delete_strategy_does_not_rebuild_reference_keys() -> Result<()> {
	let (ds, observer) = new_observed_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");

	run_ok(
		&ds,
		&ses,
		"
		DEFINE FIELD child_ref ON parent TYPE record<child> REFERENCE ON DELETE IGNORE;
		CREATE child:c1;
		CREATE parent:p1 SET child_ref = child:c1;
		",
	)
	.await?;
	observer.clear();

	run_ok(&ds, &ses, "ALTER FIELD child_ref ON parent REFERENCE ON DELETE REJECT").await?;

	let deleted_keys: u32 = observer.snapshot().iter().map(|metrics| metrics.ops_del).sum();
	assert_eq!(
		deleted_keys, 0,
		"changing only ON DELETE strategy must not rebuild durable reference keys"
	);
	assert_eq!(
		query_value(&ds, &ses, "RETURN child:c1<~(parent FIELD child_ref)").await?,
		syn::value("[parent:p1]")?
	);

	Ok(())
}

#[tokio::test]
async fn update_reference_field_recreates_missing_reference_key() -> Result<()> {
	let (_, ds) = new_ds("test", "test", false).await?;
	let ses = Session::owner().with_ns("test").with_db("test");

	run_ok(
		&ds,
		&ses,
		"
		DEFINE TABLE child SCHEMAFULL;
		DEFINE FIELD name ON child TYPE string;
		DEFINE TABLE parent SCHEMAFULL;
		DEFINE FIELD name ON parent TYPE string;
		DEFINE FIELD child_ref ON parent TYPE record<child> REFERENCE ON DELETE IGNORE;
		CREATE child:c1 SET name = 'child1';
		CREATE parent:p1 SET name = 'parent1', child_ref = child:c1;
		DELETE child:c1;
		CREATE child:c1 SET name = 'child1';
		",
	)
	.await?;
	assert_eq!(
		query_value(&ds, &ses, "RETURN child:c1<~(parent FIELD child_ref)").await?,
		syn::value("[]")?
	);

	run_ok(&ds, &ses, "UPDATE parent:p1 SET child_ref = child:c1").await?;
	assert_eq!(
		query_value(&ds, &ses, "RETURN child:c1<~(parent FIELD child_ref)").await?,
		syn::value("[parent:p1]")?
	);

	Ok(())
}

#[tokio::test]
async fn patch_reference_field_recreates_missing_reference_key() -> Result<()> {
	let (_, ds) = new_ds("test", "test", false).await?;
	let ses = Session::owner().with_ns("test").with_db("test");

	run_ok(
		&ds,
		&ses,
		"
		DEFINE TABLE child SCHEMAFULL;
		DEFINE FIELD name ON child TYPE string;
		DEFINE TABLE parent SCHEMAFULL;
		DEFINE FIELD name ON parent TYPE string;
		DEFINE FIELD child_ref ON parent TYPE record<child> REFERENCE ON DELETE IGNORE;
		CREATE child:c1 SET name = 'child1';
		CREATE parent:p1 SET name = 'parent1', child_ref = child:c1;
		DELETE child:c1;
		CREATE child:c1 SET name = 'child1';
		",
	)
	.await?;
	assert_eq!(
		query_value(&ds, &ses, "RETURN child:c1<~(parent FIELD child_ref)").await?,
		syn::value("[]")?
	);

	run_ok(
		&ds,
		&ses,
		"UPDATE parent:p1 PATCH [{ op: 'replace', path: '/child_ref', value: child:c1 }]",
	)
	.await?;
	assert_eq!(
		query_value(&ds, &ses, "RETURN child:c1<~(parent FIELD child_ref)").await?,
		syn::value("[parent:p1]")?
	);

	Ok(())
}

#[tokio::test]
async fn unrelated_update_does_not_recreate_ignored_reference_key() -> Result<()> {
	let (_, ds) = new_ds("test", "test", false).await?;
	let ses = Session::owner().with_ns("test").with_db("test");

	run_ok(
		&ds,
		&ses,
		"
		DEFINE TABLE child SCHEMAFULL;
		DEFINE FIELD name ON child TYPE string;
		DEFINE TABLE parent SCHEMAFULL;
		DEFINE FIELD name ON parent TYPE string;
		DEFINE FIELD child_ref ON parent TYPE record<child> REFERENCE ON DELETE IGNORE;
		CREATE child:c1 SET name = 'child1';
		CREATE parent:p1 SET name = 'parent1', child_ref = child:c1;
		DELETE child:c1;
		",
	)
	.await?;
	assert_eq!(
		query_value(&ds, &ses, "RETURN child:c1<~(parent FIELD child_ref)").await?,
		syn::value("[]")?
	);

	run_ok(&ds, &ses, "UPDATE parent:p1 SET name = 'renamed'").await?;
	assert_eq!(
		query_value(&ds, &ses, "RETURN child:c1<~(parent FIELD child_ref)").await?,
		syn::value("[]")?
	);

	Ok(())
}

#[tokio::test]
async fn alter_drop_reference_removes_reference_keys() -> Result<()> {
	let (_, ds) = new_ds("test", "test", false).await?;
	let ses = Session::owner().with_ns("test").with_db("test");

	run_ok(
		&ds,
		&ses,
		"
		DEFINE TABLE child SCHEMAFULL;
		DEFINE FIELD name ON child TYPE string;
		DEFINE TABLE parent SCHEMAFULL;
		DEFINE FIELD name ON parent TYPE string;
		DEFINE FIELD child_ref ON parent TYPE record<child> REFERENCE ON DELETE IGNORE;
		CREATE child:c1 SET name = 'child1';
		CREATE parent:p1 SET name = 'parent1', child_ref = child:c1;
		",
	)
	.await?;
	assert_eq!(
		query_value(&ds, &ses, "RETURN child:c1<~(parent FIELD child_ref)").await?,
		syn::value("[parent:p1]")?
	);

	run_ok(&ds, &ses, "ALTER FIELD child_ref ON parent DROP REFERENCE").await?;
	assert_eq!(
		query_value(&ds, &ses, "RETURN child:c1<~(parent FIELD child_ref)").await?,
		syn::value("[]")?
	);

	Ok(())
}

#[tokio::test]
async fn define_overwrite_without_reference_removes_reference_keys() -> Result<()> {
	let (_, ds) = new_ds("test", "test", false).await?;
	let ses = Session::owner().with_ns("test").with_db("test");

	run_ok(
		&ds,
		&ses,
		"
		DEFINE TABLE child SCHEMAFULL;
		DEFINE FIELD name ON child TYPE string;
		DEFINE TABLE parent SCHEMAFULL;
		DEFINE FIELD name ON parent TYPE string;
		DEFINE FIELD child_ref ON parent TYPE record<child> REFERENCE ON DELETE IGNORE;
		CREATE child:c1 SET name = 'child1';
		CREATE parent:p1 SET name = 'parent1', child_ref = child:c1;
		",
	)
	.await?;
	assert_eq!(
		query_value(&ds, &ses, "RETURN child:c1<~(parent FIELD child_ref)").await?,
		syn::value("[parent:p1]")?
	);

	run_ok(&ds, &ses, "DEFINE FIELD OVERWRITE child_ref ON parent TYPE record<child>").await?;
	assert_eq!(
		query_value(&ds, &ses, "RETURN child:c1<~(parent FIELD child_ref)").await?,
		syn::value("[]")?
	);

	Ok(())
}
