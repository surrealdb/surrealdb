use std::borrow::Cow;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use anyhow::Result;
use chrono::Utc;
use surrealdb_kvs::TransactionType;
#[cfg(any(feature = "kv-surrealkv", feature = "kv-rocksdb"))]
use temp_dir::TempDir;
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use web_time::Instant;

use super::builder::{Building, IndexKey};
use super::state::{build_owner_expired, report_status_from_phase};
use super::*;
use crate::catalog::providers::{
	CatalogProvider, DatabaseProvider, NamespaceProvider, TableProvider,
};
use crate::catalog::{DatabaseId, Index, IndexDefinition, IndexId, NamespaceId, Record};
use crate::dbs::Session;
use crate::err::Error;
use crate::idx::IndexKeyBase;
use crate::idx::index::IndexOperation;
use crate::key::reclaim::{Expunge, ReclaimKind};
use crate::key::schema::{
	DocKeyPrefix, DocLookupKey, DocLookupPrefix, DocPendingKey, DocPendingPrefix, EntryPrefix,
	IdxRoot, IndexCountKey, IndexCountPrefix, ReclaimKey, ReclaimPrefix, RecordPrefix,
};
use crate::key::{KVKey, KVKeyDecode, KVSubspace, KVValue, Key};
use crate::kvs::index::CleanUncommittedBuild;
use crate::kvs::index::admission::CachedAdmission;
use crate::kvs::testing::{
	NonRetryableErrorSite, RetryableConflictGuard, RetryableConflictSite,
	inject_non_retryable_error, inject_retryable_conflict, inject_retryable_conflicts,
	retryable_conflict_count,
};
use crate::kvs::tx::{
	CachedIndexBuildReservationKey, CachedIndexBuildReservationLookup, IndexBuildReservationRelease,
};
use crate::kvs::{Datastore, DatastoreError, is_retryable_transaction_conflict, storage_error};
use crate::val::{RecordId, RecordIdKey, RecordIdentity, TableName, Value};

const REPEATED_RETRY_CONFLICTS: usize = 1000;

async fn new_index_test_ds() -> Result<(Arc<Datastore>, Session)> {
	// These tests adopt, resume and observe index builds directly, and assert on
	// when the datastore's own state is released. A background scheduler running
	// `resume_stalled_index_builds` would claim the same builds.
	let ds = Datastore::builder().without_maintenance_tasks().build_with_path("memory").await?;
	let session = Session::owner().with_ns("test").with_db("test");
	let tx = ds.transaction(TransactionType::Write).await?;
	tx.ensure_ns_db(None, "test", "test").await?;
	tx.commit().await?;
	Ok((ds, session))
}

#[cfg(feature = "kv-mem")]
async fn new_distributed_index_test_ds() -> Result<(Arc<Datastore>, Datastore, Session)> {
	let (ds_a, session) = new_index_test_ds().await?;
	let ds_b = ds_a.fork_for_test_with_node_id(uuid::Uuid::new_v4());
	// Both simulated compute nodes must be visible in durable node
	// membership so reservation liveness checks treat their tickets as
	// owned by active writers.
	ds_a.insert_node().await?;
	ds_b.insert_node().await?;
	Ok((ds_a, ds_b, session))
}

async fn execute_all(ds: &Datastore, session: &Session, sql: &str) -> Result<()> {
	for result in ds.execute(sql, session, None).await? {
		result.result?;
	}
	Ok(())
}

async fn execute_cancelled_transaction(ds: &Datastore, session: &Session, sql: &str) -> Result<()> {
	let results = ds.execute(sql, session, None).await?;
	let error = results.into_iter().find_map(|result| result.result.err());
	assert!(
		error
			.expect("transaction should be reported as cancelled")
			.to_string()
			.contains("cancelled transaction")
	);
	Ok(())
}

#[cfg(feature = "kv-mem")]
fn is_retryable_statement_conflict(err: &anyhow::Error) -> bool {
	is_retryable_transaction_conflict(err)
		|| err.to_string().starts_with("Transaction conflict:")
		|| err.downcast_ref::<surrealdb_types::Error>().is_some_and(|err| {
			matches!(
				err.details(),
				surrealdb_types::ErrorDetails::Query(Some(
					surrealdb_types::QueryError::TransactionConflict
				))
			)
		})
}

#[cfg(feature = "kv-mem")]
async fn execute_all_retrying_conflicts(
	ds: &Datastore,
	session: &Session,
	sql: &str,
) -> Result<()> {
	// The paused-build tests intentionally hold the builder in a retry loop.
	// User writes can observe statement-level transaction conflicts while
	// the durable admission state is being advanced, so retry here to keep
	// the assertions focused on whether queued writes are eventually replayed.
	timeout(Duration::from_secs(10), async {
		loop {
			match execute_all(ds, session, sql).await {
				Ok(()) => return Ok(()),
				Err(err) if is_retryable_statement_conflict(&err) => {
					sleep(Duration::from_millis(10)).await;
				}
				Err(err) => return Err(err),
			}
		}
	})
	.await
	.map_err(|_| anyhow::anyhow!("timed out retrying statement during index build"))?
}

#[cfg(feature = "kv-mem")]
async fn execute_cancelled_transaction_retrying_conflicts(
	ds: &Datastore,
	session: &Session,
	sql: &str,
) -> Result<()> {
	timeout(Duration::from_secs(10), async {
		loop {
			match execute_cancelled_transaction(ds, session, sql).await {
				Ok(()) => return Ok(()),
				Err(err) if is_retryable_statement_conflict(&err) => {
					sleep(Duration::from_millis(10)).await;
				}
				Err(err) => return Err(err),
			}
		}
	})
	.await
	.map_err(|_| anyhow::anyhow!("timed out retrying cancelled statement during index build"))?
}

#[cfg(feature = "kv-mem")]
async fn execute_error_text_retrying_conflicts(
	ds: &Datastore,
	session: &Session,
	sql: &str,
) -> Result<String> {
	timeout(Duration::from_secs(10), async {
		loop {
			let results = ds.execute(sql, session, None).await?;
			let error = results
				.into_iter()
				.find_map(|result| result.result.err())
				.expect("transaction should report an error")
				.to_string();
			if error.starts_with("Transaction conflict:") {
				sleep(Duration::from_millis(10)).await;
				continue;
			}
			return Ok(error);
		}
	})
	.await
	.map_err(|_| anyhow::anyhow!("timed out retrying errored statement during index build"))?
}

async fn wait_for_index_ready(
	ds: &Datastore,
	session: &Session,
	table: &str,
	index: &str,
) -> Result<()> {
	let sql = format!("INFO FOR INDEX {index} ON {table}");
	timeout(Duration::from_secs(10), async {
		loop {
			let mut results = ds.execute(&sql, session, None).await?;
			let value = results.remove(0).result?;
			let json = value.into_json_value();
			let status = json
				.pointer("/building/status")
				.and_then(|status| status.as_str())
				.unwrap_or_default();
			match status {
				"ready" => return Ok(()),
				"error" => anyhow::bail!("index build entered error state: {json}"),
				_ => sleep(Duration::from_millis(20)).await,
			}
		}
	})
	.await
	.map_err(|_| anyhow::anyhow!("timed out waiting for concurrent index build"))?
}

async fn index_building_json(
	ds: &Datastore,
	session: &Session,
	table: &str,
	index: &str,
) -> Result<serde_json::Value> {
	let sql = format!("INFO FOR INDEX {index} ON {table}");
	let mut results = ds.execute(&sql, session, None).await?;
	let value = results.remove(0).result?;
	let json = value.into_json_value();
	json.get("building")
		.cloned()
		.ok_or_else(|| anyhow::anyhow!("index info did not include building status: {json}"))
}

async fn index_building_status(
	ds: &Datastore,
	session: &Session,
	table: &str,
	index: &str,
) -> Result<String> {
	let building = index_building_json(ds, session, table, index).await?;
	building
		.get("status")
		.and_then(|status| status.as_str())
		.map(str::to_owned)
		.ok_or_else(|| anyhow::anyhow!("index info did not include building.status: {building}"))
}

async fn durable_build_state(ds: &Datastore, ikb: &IndexKeyBase) -> Result<IndexBuildState> {
	let tx = ds.transaction(TransactionType::Read).await?;
	let state = catch!(tx, tx.get_key(&ikb.new_bs_key(), None).await)
		.ok_or_else(|| anyhow::anyhow!("durable build state should exist"))?;
	tx.cancel().await?;
	Ok(state)
}

/// Read the build state and its generation's writer-ticket counter from one
/// snapshot.
///
/// The `Building` -> `Closing` fence advances the counter in the same
/// transaction as the phase write, so a caller that has to tell a writer
/// admission from that fence must observe both keys at the same version.
async fn durable_build_state_with_ticket_counter(
	ds: &Datastore,
	ikb: &IndexKeyBase,
) -> Result<(IndexBuildState, Option<BuildTicket>)> {
	let tx = ds.transaction(TransactionType::Read).await?;
	let state = catch!(tx, tx.get_key(&ikb.new_bs_key(), None).await)
		.ok_or_else(|| anyhow::anyhow!("durable build state should exist"))?;
	let counter = catch!(tx, tx.get_key(&ikb.new_bt_key(state.generation), None).await);
	tx.cancel().await?;
	Ok((state, counter))
}

async fn set_durable_build_state(
	ds: &Datastore,
	ikb: &IndexKeyBase,
	state: IndexBuildState,
) -> Result<()> {
	let tx = ds.transaction(TransactionType::Write).await?;
	tx.set_key(&ikb.new_bs_key(), &state).await?;
	tx.commit().await
}

fn durable_build_state_for_phase(
	phase: IndexBuildPhase,
	generation: BuildGeneration,
	owner: Option<Uuid>,
) -> IndexBuildState {
	let now = Utc::now();
	IndexBuildState {
		generation,
		phase,
		owner,
		next_ticket: 0,
		initial_complete: true,
		updated_at: now,
		owner_heartbeat_at: owner.map(|_| now),
		error: None,
		report_status: Some(report_status_from_phase(phase)),
		initial: Some(1),
		updated: Some(0),
		pending: Some(0),
		initial_cursor: None,
	}
}

async fn durable_build_state_exists(ds: &Datastore, ikb: &IndexKeyBase) -> Result<bool> {
	let tx = ds.transaction(TransactionType::Read).await?;
	let state: Option<IndexBuildState> = catch!(tx, tx.get_key(&ikb.new_bs_key(), None).await);
	tx.cancel().await?;
	Ok(state.is_some())
}

async fn new_building_for_index(
	ds: &Datastore,
	session: &Session,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
	ix: Arc<IndexDefinition>,
) -> Result<Building> {
	let tx = ds.transaction(TransactionType::Read).await?;
	let table_def = catch!(tx, tx.get_tb(ns, db, table, None).await).expect("table should exist");
	let mut ctx = ds.setup_ctx()?;
	let tx = Arc::new(tx);
	ctx.set_transaction(Arc::clone(&tx));
	let ctx = ctx.freeze();
	let build = Building::new(
		&ctx,
		ds.transaction_factory().clone(),
		ds.setup_options(session),
		table_def.table_id,
		Arc::clone(&ix),
		Arc::new(IndexKey::new(ns, db, table, ix.index_id)),
	)?;
	tx.cancel().await?;
	Ok(build)
}

#[cfg(feature = "kv-mem")]
async fn start_index_build_paused(
	ds: &Datastore,
	session: &Session,
	sql: &str,
) -> Result<RetryableConflictGuard> {
	let site = RetryableConflictSite::ConcurrentIndexInitialCleanup;
	let node_id = ds.id();
	let guard = inject_retryable_conflicts(site, node_id, REPEATED_RETRY_CONFLICTS);
	execute_all(ds, session, sql).await?;
	// Wait until the builder has reached the injected conflict site before
	// returning. At that point durable state is Building and second-node
	// writes must go through the admission queue.
	wait_for_retry_conflict(site, node_id, REPEATED_RETRY_CONFLICTS).await?;
	Ok(guard)
}

#[cfg(feature = "kv-mem")]
struct PausedRemoveBuild {
	ds: Arc<Datastore>,
	session: Session,
	guard: RetryableConflictGuard,
	ns: NamespaceId,
	db: DatabaseId,
	table: TableName,
	ix: Arc<IndexDefinition>,
	builder: IndexBuilding,
}

#[cfg(feature = "kv-mem")]
async fn start_paused_remove_build() -> Result<PausedRemoveBuild> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:one SET email = 'one@example.com' RETURN NONE;
			CREATE user:two SET email = 'two@example.com' RETURN NONE;
			",
	)
	.await?;

	let guard = start_index_build_paused(
		&ds,
		&session,
		"DEFINE INDEX test ON user FIELDS email CONCURRENTLY",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "user", "test").await?;
	let builder = local_builder_for_key(&ds, ns, db, &table, ix.index_id)
		.await?
		.expect("local builder should be running");

	Ok(PausedRemoveBuild {
		ds,
		session,
		guard,
		ns,
		db,
		table,
		ix,
		builder,
	})
}

#[cfg(feature = "kv-mem")]
async fn assert_cancelled_remove_keeps_local_builder(sql: &str) -> Result<()> {
	let PausedRemoveBuild {
		ds,
		session,
		guard,
		ns,
		db,
		table,
		ix,
		builder,
	} = start_paused_remove_build().await?;

	execute_cancelled_transaction_retrying_conflicts(&ds, &session, sql).await?;
	sleep(Duration::from_millis(200)).await;

	assert!(
		!builder.is_finished(),
		"cancelled cascading remove must not abort the still-valid local builder"
	);
	assert!(
		local_builder_for_key(&ds, ns, db, &table, ix.index_id).await?.is_some(),
		"cancelled cascading remove must keep the builder map entry"
	);

	drop(guard);
	wait_for_index_ready(&ds, &session, "user", "test").await?;
	Ok(())
}

/// Resume a parked build, optionally deleting its durable state first, and
/// report what it committed: the number of index-data keys, and whether `!bs`
/// came back.
///
/// No retirement runs here and no abort flag is raised, so the only thing that
/// can stop the build is the state deletion itself. That is the point: the
/// process-local abort would otherwise stop the task first and the fence would
/// never be exercised.
#[cfg(feature = "kv-mem")]
async fn resume_paused_build_and_count(delete_build_state: bool) -> Result<(usize, bool)> {
	let PausedRemoveBuild {
		ds,
		session: _session,
		guard,
		ns,
		db,
		table,
		ix,
		builder,
	} = start_paused_remove_build().await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	assert!(
		durable_build_state_exists(&ds, &ikb).await?,
		"the parked build must have durable state for either arm to mean anything"
	);
	if delete_build_state {
		let txn = ds.transaction(TransactionType::Write).await?;
		catch!(txn, txn.del_key(&ikb.new_bs_key()).await);
		txn.commit().await?;
		assert!(
			!builder.is_finished(),
			"the builder must still be live when the state is deleted, or the window \
			 under test was not reproduced"
		);
	}
	// Wait on the task rather than on the builder map entry: no retirement ran
	// here, so nothing retires that entry — what settles is the task itself.
	drop(guard);
	timeout(Duration::from_secs(30), builder.wait_finished())
		.await
		.map_err(|_| anyhow::anyhow!("the resumed build task did not finish"))?;
	Ok((
		index_prefix_key_count(&ds, ns, db, &table, ix.index_id).await?,
		durable_build_state_exists(&ds, &ikb).await?,
	))
}

/// Deleting `!bs` is what stops a build, and this is the test that says so.
///
/// Every retirement path relies on it: the schema transaction deletes `!bs`
/// beside the catalog entry, and from that point a builder cannot commit index
/// data because each of its write transactions reads and compare-and-swaps that
/// key before the batch's other writes. Aborting the process-local task is a
/// courtesy on top, and it is deliberately not used here — it would stop the
/// task before the fence was ever reached, which is why the removal tests
/// cannot stand in for this one.
///
/// Three call sites enforce it for a build in progress —
/// `maintain_build_ownership`, `update_build_state_in_tx` and
/// `update_owned_build_state_inner` — and a build's path through a batch
/// crosses all three, so relaxing any one of them alone does not open the
/// window. Anything that makes a missing `!bs` survivable at *every* one of
/// them fails the fenced arm below. Compaction runs after `Online` and is
/// fenced separately, by the locked read in
/// `compaction_write_still_owns_index`; see
/// `compaction_write_fence_conflicts_with_a_retirement_that_commits_after_it`.
///
/// Two arms, because that is what makes it mean anything: the control resumes
/// the same harness untouched and asserts it *does* commit, so a sabotaged
/// fence fails the test instead of quietly satisfying it.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn deleting_build_state_stops_a_running_builder_committing() -> Result<()> {
	let (control_keys, _) = resume_paused_build_and_count(false).await?;
	assert!(
		control_keys > 0,
		"the control arm must commit index data, or the fenced arm proves nothing"
	);

	let (fenced_keys, state_back) = resume_paused_build_and_count(true).await?;
	assert_eq!(
		fenced_keys, 0,
		"a build whose durable state was deleted must not commit index data \
		 (control committed {control_keys})"
	);
	assert!(!state_back, "a stopped build must not recreate the durable state it was stopped by");
	Ok(())
}

#[cfg(feature = "kv-mem")]
async fn assert_committed_remove_aborts_local_builder(sql: &str) -> Result<()> {
	let PausedRemoveBuild {
		ds,
		session,
		guard,
		ns,
		db,
		table,
		ix,
		builder,
	} = start_paused_remove_build().await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	seed_durable_queue_generation(&ds, &ikb, 99).await?;
	assert_eq!(durable_queue_all_generations_count(&ds, &ikb).await?, 3);

	execute_all_retrying_conflicts(&ds, &session, sql).await?;
	wait_for_no_local_builder(&ds, ns, db, &table, ix.index_id).await?;
	// The removal's own transaction deleted the build state and every queue,
	// so nothing is left before the paused builder is even released.
	assert_no_index_build_artifacts(&ds, ns, db, &table, ix.index_id).await?;
	// Releasing it is the fence test. The builder resumes into a batch that
	// reads `!bs` first, finds it gone, and stops without committing. Waiting
	// for the task is what makes the second assertion mean "it ran and wrote
	// nothing" rather than "it had not run yet".
	drop(guard);
	wait_for_finished_builder(&builder).await?;
	assert_no_index_build_artifacts(&ds, ns, db, &table, ix.index_id).await?;
	Ok(())
}

async fn query_array_len(ds: &Datastore, session: &Session, sql: &str) -> Result<usize> {
	let mut results = ds.execute(sql, session, None).await?;
	let value = results.remove(0).result?;
	let surrealdb_types::Value::Array(rows) = value else {
		anyhow::bail!("query returned non-array value: {value:?}");
	};
	Ok(rows.len())
}

async fn expect_indexed_query_len(
	ds: &Datastore,
	session: &Session,
	sql: &str,
	expected: usize,
) -> Result<()> {
	let len = query_array_len(ds, session, sql).await?;
	assert_eq!(len, expected, "unexpected row count for query: {sql}");
	Ok(())
}

async fn get_table_index(
	ds: &Datastore,
	table: &str,
	index: &str,
) -> Result<(NamespaceId, DatabaseId, TableName, Arc<IndexDefinition>)> {
	let table = TableName::from(table);
	let tx = ds.transaction(TransactionType::Read).await?;
	let ns = catch!(tx, tx.get_ns_by_name("test", None).await).expect("namespace should exist");
	let db =
		catch!(tx, tx.get_db_by_name("test", "test", None).await).expect("database should exist");
	let ix = catch!(tx, tx.expect_tb_index(ns.namespace_id, db.database_id, &table, index).await);
	tx.cancel().await?;
	Ok((ns.namespace_id, db.database_id, table, ix))
}

async fn get_table_ids(
	ds: &Datastore,
	table: &str,
) -> Result<(NamespaceId, DatabaseId, TableName)> {
	let table = TableName::from(table);
	let tx = ds.transaction(TransactionType::Read).await?;
	let ns = catch!(tx, tx.get_ns_by_name("test", None).await).expect("namespace should exist");
	let db =
		catch!(tx, tx.get_db_by_name("test", "test", None).await).expect("database should exist");
	catch!(tx, tx.get_tb(ns.namespace_id, db.database_id, &table, None).await)
		.expect("table should exist");
	tx.cancel().await?;
	Ok((ns.namespace_id, db.database_id, table))
}

async fn local_builder_for_key(
	ds: &Datastore,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
	ix: IndexId,
) -> Result<Option<IndexBuilding>> {
	let ctx = ds.setup_ctx()?;
	let Some(index_builder) = ctx.get_index_builder() else {
		return Ok(None);
	};
	let key = Arc::new(IndexKey::new(ns, db, table, ix));
	Ok(index_builder.indexes.read().await.get(&key).cloned())
}

/// Wait for a builder task to exit.
///
/// A committed retirement returns without waiting for the task: it is fenced by
/// the `!bs` delete rather than by a close-time drain, so its map entry is gone
/// while the task itself is still winding down. A test that asserts on what the
/// task did has to wait for it here.
async fn wait_for_finished_builder(building: &IndexBuilding) -> Result<()> {
	timeout(Duration::from_secs(10), async {
		loop {
			if building.is_finished() {
				return Ok(());
			}
			sleep(Duration::from_millis(10)).await;
		}
	})
	.await
	.map_err(|_| anyhow::anyhow!("timed out waiting for the index builder task to exit"))?
}

async fn wait_for_no_local_builder(
	ds: &Datastore,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
	ix: IndexId,
) -> Result<()> {
	timeout(Duration::from_secs(10), async {
		loop {
			if local_builder_for_key(ds, ns, db, table, ix).await?.is_none() {
				return Ok(());
			}
			sleep(Duration::from_millis(10)).await;
		}
	})
	.await
	.map_err(|_| anyhow::anyhow!("timed out waiting for local index builder abort"))?
}

fn previous_index_id(ix: IndexId) -> IndexId {
	assert!(ix.0 > 0, "test expected a previous allocated index id before {ix:?}");
	IndexId(ix.0 - 1)
}

async fn assert_no_index_build_artifacts(
	ds: &Datastore,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
	ix: IndexId,
) -> Result<()> {
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix);
	assert!(!durable_build_state_exists(ds, &ikb).await?);
	assert_eq!(durable_queue_all_generations_count(ds, &ikb).await?, 0);
	assert_eq!(
		durable_ticket_counter_count(ds, &ikb).await?,
		0,
		"a retired or rolled-back build must not strand its generation ticket counter"
	);
	assert_eq!(index_prefix_key_count(ds, ns, db, table, ix).await?, 0);
	assert!(local_builder_for_key(ds, ns, db, table, ix).await?.is_none());
	Ok(())
}

async fn index_reclaim_modes(
	ds: &Datastore,
	ns: NamespaceId,
	db: DatabaseId,
	tb: &TableName,
	ix: IndexId,
) -> Result<Vec<Expunge>> {
	let tx = ds.transaction(TransactionType::Read).await?;
	let entries = catch!(tx, tx.getr_raw(ReclaimPrefix {}.range()?, None).await);
	tx.cancel().await?;
	Ok(entries
		.into_iter()
		.filter_map(|(key, _)| {
			let reclaim = ReclaimKey::decode_key(&key).ok()?;
			(reclaim.kind == ReclaimKind::Index
				&& reclaim.ns == ns
				&& reclaim.db == db
				&& reclaim.tb.as_ref() == tb
				&& reclaim.ix == ix)
				.then_some(reclaim.expunge)
		})
		.collect())
}

async fn index_prefix_key_count(
	ds: &Datastore,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
	ix: IndexId,
) -> Result<usize> {
	let tx = ds.transaction(TransactionType::Read).await?;
	let key = IdxRoot {
		ns,
		db,
		tb: Cow::Borrowed(table),
		ix,
	};
	let keys: Vec<(Vec<u8>, Vec<u8>)> = catch!(tx, tx.get_prefix_key(&key, None).await);
	tx.cancel().await?;
	Ok(keys.len())
}

async fn seed_durable_queue_generation(
	ds: &Datastore,
	ikb: &IndexKeyBase,
	generation: BuildGeneration,
) -> Result<()> {
	let tx = ds.transaction(TransactionType::Write).await?;
	let id = RecordIdKey::from(format!("stale-{generation}"));
	let ticket = generation;
	let mutation_seq = 0;
	tx.set_key(
		&ikb.new_bg_key(generation, ticket, mutation_seq),
		&Appending {
			old_values: None,
			new_values: None,
			id: id.clone(),
			count_cond_match: None,
		},
	)
	.await?;
	tx.set_key(
		&ikb.new_bp_key(generation, &id),
		&PrimaryAppendingTicket {
			ticket,
			mutation_seq,
		},
	)
	.await?;
	tx.set_key(
		&ikb.new_br_key(generation, ticket),
		&IndexBuildReservation {
			node: ds.id(),
			expires_at: Utc::now() + chrono::Duration::seconds(BUILD_RESERVATION_TTL_SECS),
		},
	)
	.await?;
	tx.commit().await?;
	Ok(())
}

async fn seed_uncommitted_index_build_artifacts(
	ds: &Datastore,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
	ix: IndexId,
) -> Result<()> {
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix);
	let tx = ds.transaction(TransactionType::Write).await?;
	tx.set_key(
		&ikb.new_bs_key(),
		&durable_build_state_for_phase(IndexBuildPhase::Building, 1, Some(ds.id())),
	)
	.await?;
	let id = RecordIdKey::from("orphan".to_owned());
	let ticket = 1;
	let mutation_seq = 0;
	tx.set_key(
		&ikb.new_bg_key(1, ticket, mutation_seq),
		&Appending {
			old_values: None,
			new_values: None,
			id: id.clone(),
			count_cond_match: None,
		},
	)
	.await?;
	tx.set_key(
		&ikb.new_bp_key(1, &id),
		&PrimaryAppendingTicket {
			ticket,
			mutation_seq,
		},
	)
	.await?;
	tx.set_key(
		&ikb.new_br_key(1, ticket),
		&IndexBuildReservation {
			node: ds.id(),
			expires_at: Utc::now() + chrono::Duration::seconds(BUILD_RESERVATION_TTL_SECS),
		},
	)
	.await?;
	let index_data_key = IdxRoot {
		ns,
		db,
		tb: Cow::Borrowed(table),
		ix,
	}
	.encode_bound()?;
	let mut idx_key = index_data_key.into_vec();
	idx_key.extend_from_slice(b"orphan");
	tx.set(Key::from(idx_key), b"orphan".to_vec()).await?;
	tx.commit().await?;
	Ok(())
}

async fn durable_queue_generation_count(
	ds: &Datastore,
	ikb: &IndexKeyBase,
	generation: BuildGeneration,
) -> Result<usize> {
	let tx = ds.transaction(TransactionType::Read).await?;
	let bg = catch!(tx, tx.keys(ikb.new_bg_range(generation)?, u32::MAX, 0, None).await);
	let bp = catch!(tx, tx.keys(ikb.new_bp_range(generation)?, u32::MAX, 0, None).await);
	let br = catch!(tx, tx.keys(ikb.new_br_range(generation)?, u32::MAX, 0, None).await);
	tx.cancel().await?;
	Ok(bg.len() + bp.len() + br.len())
}

/// Count the ticket counters left behind across every generation of an index.
async fn durable_ticket_counter_count(ds: &Datastore, ikb: &IndexKeyBase) -> Result<usize> {
	let tx = ds.transaction(TransactionType::Read).await?;
	let bt = catch!(tx, tx.keys(ikb.new_bt_all_generations_range()?, u32::MAX, 0, None).await);
	tx.cancel().await?;
	Ok(bt.len())
}

async fn durable_queue_all_generations_count(ds: &Datastore, ikb: &IndexKeyBase) -> Result<usize> {
	let tx = ds.transaction(TransactionType::Read).await?;
	let bg = catch!(tx, tx.keys(ikb.new_bg_all_generations_range()?, u32::MAX, 0, None).await);
	let bp = catch!(tx, tx.keys(ikb.new_bp_all_generations_range()?, u32::MAX, 0, None).await);
	let br = catch!(tx, tx.keys(ikb.new_br_all_generations_range()?, u32::MAX, 0, None).await);
	tx.cancel().await?;
	Ok(bg.len() + bp.len() + br.len())
}

#[cfg(feature = "kv-mem")]
async fn expect_statement_error(ds: &Datastore, session: &Session, sql: &str) -> Result<()> {
	let mut results = ds.execute(sql, session, None).await?;
	let result = results.remove(0).result;
	if result.is_ok() {
		anyhow::bail!("statement unexpectedly succeeded: {sql}");
	}
	Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn define_index_import_replay_preserves_existing_index_without_rebuild() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:one SET email = 'one@example.com' RETURN NONE;
			DEFINE INDEX test ON user FIELDS email;
			",
	)
	.await?;
	let (ns, db, table, old_ix) = get_table_index(&ds, "user", "test").await?;
	let ikb = IndexKeyBase::new(ns, db, table, old_ix.index_id);
	let before = durable_build_state(&ds, &ikb).await?;

	execute_all(&ds, &session, "OPTION IMPORT; DEFINE INDEX test ON user FIELDS email;").await?;

	let (_, _, _, current_ix) = get_table_index(&ds, "user", "test").await?;
	let after = durable_build_state(&ds, &ikb).await?;
	assert_eq!(current_ix.index_id, old_ix.index_id);
	assert_eq!(after.generation, before.generation);
	assert_eq!(after.phase, IndexBuildPhase::Online);
	assert_eq!(index_building_status(&ds, &session, "user", "test").await?, "ready");
	expect_indexed_query_len(
		&ds,
		&session,
		"SELECT * FROM user WITH INDEX test WHERE email = 'one@example.com'",
		1,
	)
	.await?;
	Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn define_index_import_replay_updates_comment_without_rebuild() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:one SET email = 'one@example.com' RETURN NONE;
			DEFINE INDEX test ON user FIELDS email COMMENT 'old comment';
			",
	)
	.await?;
	let (ns, db, table, old_ix) = get_table_index(&ds, "user", "test").await?;
	let ikb = IndexKeyBase::new(ns, db, table, old_ix.index_id);
	let before = durable_build_state(&ds, &ikb).await?;

	execute_all(
		&ds,
		&session,
		"OPTION IMPORT; DEFINE INDEX test ON user FIELDS email COMMENT 'new comment';",
	)
	.await?;

	let (_, _, _, current_ix) = get_table_index(&ds, "user", "test").await?;
	let after = durable_build_state(&ds, &ikb).await?;
	assert_eq!(current_ix.index_id, old_ix.index_id);
	assert_eq!(current_ix.comment.as_deref(), Some("new comment"));
	assert_eq!(after.generation, before.generation);
	assert_eq!(after.phase, IndexBuildPhase::Online);
	expect_indexed_query_len(
		&ds,
		&session,
		"SELECT * FROM user WITH INDEX test WHERE email = 'one@example.com'",
		1,
	)
	.await?;
	Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn define_index_import_changed_definition_rebuilds_with_fresh_index_id() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:one SET email = 'one@example.com', account = 'apple' RETURN NONE;
			DEFINE INDEX test ON user FIELDS email;
			",
	)
	.await?;
	let (ns, db, table, old_ix) = get_table_index(&ds, "user", "test").await?;
	let old_ikb = IndexKeyBase::new(ns, db, table.clone(), old_ix.index_id);
	let before = durable_build_state(&ds, &old_ikb).await?;

	execute_all(&ds, &session, "OPTION IMPORT; DEFINE INDEX test ON user FIELDS account;").await?;

	let (_, _, _, current_ix) = get_table_index(&ds, "user", "test").await?;
	let current_ikb = IndexKeyBase::new(ns, db, table.clone(), current_ix.index_id);
	let after = durable_build_state(&ds, &current_ikb).await?;
	assert_ne!(current_ix.index_id, old_ix.index_id);
	assert_eq!(after.generation, 1);
	assert_eq!(after.phase, IndexBuildPhase::Online);
	expect_indexed_query_len(
		&ds,
		&session,
		"SELECT * FROM user WITH INDEX test WHERE account = 'apple'",
		1,
	)
	.await?;

	let tx = ds.transaction(TransactionType::Read).await?;
	let old_lookup = catch!(tx, tx.get_tb_index_by_id(ns, db, &table, old_ix.index_id, None).await);
	tx.cancel().await?;
	assert!(old_lookup.is_none(), "old index id lookup should be retired");
	assert_eq!(before.phase, IndexBuildPhase::Online);
	Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn remove_index_cancel_preserves_durable_ready_state() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:one SET email = 'one@example.com' RETURN NONE;
			DEFINE INDEX test ON user FIELDS email;
			",
	)
	.await?;
	assert_eq!(index_building_status(&ds, &session, "user", "test").await?, "ready");

	execute_cancelled_transaction(&ds, &session, "BEGIN; REMOVE INDEX test ON user; CANCEL;")
		.await?;

	assert_eq!(index_building_status(&ds, &session, "user", "test").await?, "ready");
	expect_indexed_query_len(
		&ds,
		&session,
		"SELECT * FROM user WITH INDEX test WHERE email = 'one@example.com'",
		1,
	)
	.await?;
	Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn remove_index_committed_deletes_durable_queue_keys() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
				DEFINE TABLE user SCHEMALESS;
				CREATE user:one SET email = 'one@example.com' RETURN NONE;
				DEFINE INDEX test ON user FIELDS email;
				",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "user", "test").await?;
	let ikb = IndexKeyBase::new(ns, db, table, ix.index_id);
	seed_durable_queue_generation(&ds, &ikb, 1).await?;
	assert!(durable_build_state_exists(&ds, &ikb).await?);
	assert_eq!(durable_queue_all_generations_count(&ds, &ikb).await?, 3);

	execute_all(&ds, &session, "REMOVE INDEX test ON user").await?;

	assert!(!durable_build_state_exists(&ds, &ikb).await?);
	assert_eq!(durable_queue_all_generations_count(&ds, &ikb).await?, 0);
	Ok(())
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn remove_index_cancel_keeps_local_builder_running() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
				DEFINE TABLE user SCHEMALESS;
				CREATE user:one SET email = 'one@example.com' RETURN NONE;
				CREATE user:two SET email = 'two@example.com' RETURN NONE;
				",
	)
	.await?;

	let guard = start_index_build_paused(
		&ds,
		&session,
		"DEFINE INDEX test ON user FIELDS email CONCURRENTLY",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "user", "test").await?;
	let builder = local_builder_for_key(&ds, ns, db, &table, ix.index_id)
		.await?
		.expect("local builder should be running");

	execute_cancelled_transaction_retrying_conflicts(
		&ds,
		&session,
		"BEGIN; REMOVE INDEX test ON user; CANCEL;",
	)
	.await?;
	sleep(Duration::from_millis(200)).await;

	assert!(
		!builder.is_finished(),
		"cancelled REMOVE INDEX must not abort the still-valid local builder"
	);
	assert!(
		local_builder_for_key(&ds, ns, db, &table, ix.index_id).await?.is_some(),
		"cancelled REMOVE INDEX must keep the builder map entry"
	);

	drop(guard);
	wait_for_index_ready(&ds, &session, "user", "test").await?;
	Ok(())
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn remove_index_commit_aborts_local_builder_after_commit() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
				DEFINE TABLE user SCHEMALESS;
				CREATE user:one SET email = 'one@example.com' RETURN NONE;
				CREATE user:two SET email = 'two@example.com' RETURN NONE;
				",
	)
	.await?;

	let guard = start_index_build_paused(
		&ds,
		&session,
		"DEFINE INDEX test ON user FIELDS email CONCURRENTLY",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "user", "test").await?;
	let builder = local_builder_for_key(&ds, ns, db, &table, ix.index_id)
		.await?
		.expect("local builder should be running");

	execute_all_retrying_conflicts(&ds, &session, "REMOVE INDEX test ON user").await?;
	wait_for_no_local_builder(&ds, ns, db, &table, ix.index_id).await?;
	assert_no_index_build_artifacts(&ds, ns, db, &table, ix.index_id).await?;
	// The released builder must not be able to bring any of it back: its next
	// batch reads the `!bs` the removal deleted and stops there.
	drop(guard);
	wait_for_finished_builder(&builder).await?;
	assert_no_index_build_artifacts(&ds, ns, db, &table, ix.index_id).await?;
	// Scoped to this index's own entries: dropping the table's last doc-ID
	// consumer also queues the table's shared doc-ID prefixes, which this
	// assertion is not about. One entry, committed with the catalog removal,
	// owns the data prefix; the retirement queues nothing after it.
	assert_eq!(
		index_reclaim_modes(&ds, ns, db, &table, ix.index_id).await?.len(),
		1,
		"retirement must queue exactly the schema tombstone for the index data"
	);
	Ok(())
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn remove_table_cancel_keeps_local_builder_running() -> Result<()> {
	assert_cancelled_remove_keeps_local_builder("BEGIN; REMOVE TABLE user; CANCEL;").await
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn remove_database_cancel_keeps_local_builder_running() -> Result<()> {
	assert_cancelled_remove_keeps_local_builder("BEGIN; REMOVE DATABASE test; CANCEL;").await
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn remove_namespace_cancel_keeps_local_builder_running() -> Result<()> {
	assert_cancelled_remove_keeps_local_builder("BEGIN; REMOVE NAMESPACE test; CANCEL;").await
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn remove_table_commit_aborts_local_builder_after_commit() -> Result<()> {
	assert_committed_remove_aborts_local_builder("REMOVE TABLE user").await
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn remove_database_commit_aborts_local_builder_after_commit() -> Result<()> {
	assert_committed_remove_aborts_local_builder("REMOVE DATABASE test").await
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn remove_namespace_commit_aborts_local_builder_after_commit() -> Result<()> {
	assert_committed_remove_aborts_local_builder("REMOVE NAMESPACE test").await
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn remove_table_expunge_aborts_local_builder_after_commit() -> Result<()> {
	assert_committed_remove_aborts_local_builder("REMOVE TABLE AND EXPUNGE user").await
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn remove_database_expunge_aborts_local_builder_after_commit() -> Result<()> {
	assert_committed_remove_aborts_local_builder("REMOVE DATABASE AND EXPUNGE test").await
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn remove_namespace_expunge_aborts_local_builder_after_commit() -> Result<()> {
	assert_committed_remove_aborts_local_builder("REMOVE NAMESPACE AND EXPUNGE test").await
}

/// `EXPUNGE` must reach the build-state keys a cascading removal retires, not
/// only the data prefixes it queues. Each statement retires them through a
/// different level of the cascade, so each is exercised: an `EXPUNGE` that a
/// level drops leaves a historical `!bs` a versioned read can still resolve.
#[cfg(feature = "kv-surrealkv")]
async fn assert_removal_expunges_historical_build_state(sql: &str) -> Result<()> {
	let dir = TempDir::new()?;
	let path = format!("surrealkv://{}?versioned=true&retention=1h", dir.path().to_string_lossy());
	let ds = Datastore::builder().without_maintenance_tasks().build_with_path(&path).await?;
	let session = Session::owner().with_ns("test").with_db("test");
	let tx = ds.transaction(TransactionType::Write).await?;
	tx.ensure_ns_db(None, "test", "test").await?;
	tx.commit().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:one SET email = 'one@example.com' RETURN NONE;
			DEFINE INDEX test ON user FIELDS email;
			",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "user", "test").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);

	// A build that has reached `Online` still holds a committed `!bs`, which
	// is the version the removal has to expunge.
	let tx = ds.transaction(TransactionType::Read).await?;
	assert!(tx.get_key(&ikb.new_bs_key(), None).await?.is_some());
	let before_removal: u64 = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)?
		.as_nanos()
		.try_into()?;
	assert!(tx.get_key(&ikb.new_bs_key(), Some(before_removal)).await?.is_some());
	tx.cancel().await?;

	execute_all(&ds, &session, sql).await?;

	let tx = ds.transaction(TransactionType::Read).await?;
	let historical = tx.get_key(&ikb.new_bs_key(), Some(before_removal)).await?;
	tx.cancel().await?;
	assert!(
		historical.is_none(),
		"`{sql}` must expunge the historical durable build state it retires"
	);
	Ok(())
}

#[cfg(feature = "kv-surrealkv")]
#[tokio::test]
async fn remove_table_expunge_removes_historical_durable_build_state() -> Result<()> {
	assert_removal_expunges_historical_build_state("REMOVE TABLE AND EXPUNGE user").await
}

#[cfg(feature = "kv-surrealkv")]
#[tokio::test]
async fn remove_database_expunge_removes_historical_durable_build_state() -> Result<()> {
	assert_removal_expunges_historical_build_state("REMOVE DATABASE AND EXPUNGE test").await
}

#[cfg(feature = "kv-surrealkv")]
#[tokio::test]
async fn remove_namespace_expunge_removes_historical_durable_build_state() -> Result<()> {
	assert_removal_expunges_historical_build_state("REMOVE NAMESPACE AND EXPUNGE test").await
}

#[cfg(feature = "kv-surrealkv")]
#[tokio::test]
async fn expunge_removes_historical_durable_build_state() -> Result<()> {
	let dir = TempDir::new()?;
	let path = format!("surrealkv://{}?versioned=true&retention=1h", dir.path().to_string_lossy());
	let ds = Datastore::builder().without_maintenance_tasks().build_with_path(&path).await?;
	let ns = NamespaceId(1);
	let db = DatabaseId(2);
	let table = TableName::from("user");
	let ix = IndexId(3);
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix);

	let tx = ds.transaction(TransactionType::Write).await?;
	tx.set_key(
		&ikb.new_bs_key(),
		&durable_build_state_for_phase(IndexBuildPhase::Building, 1, Some(ds.id())),
	)
	.await?;
	tx.commit().await?;

	let tx = ds.transaction(TransactionType::Read).await?;
	assert!(tx.get_key(&ikb.new_bs_key(), None).await?.is_some());
	let before_expunge = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)?
		.as_nanos()
		.try_into()?;
	assert!(tx.get_key(&ikb.new_bs_key(), Some(before_expunge)).await?.is_some());
	tx.cancel().await?;

	let tx = ds.transaction(TransactionType::Write).await?;
	retire_durable_index(&tx, ns, db, &table, ix, true).await?;
	tx.commit().await?;

	let tx = ds.transaction(TransactionType::Read).await?;
	assert!(
		tx.get_key(&ikb.new_bs_key(), Some(before_expunge)).await?.is_none(),
		"EXPUNGE must remove historical durable build-state versions"
	);
	tx.cancel().await?;
	Ok(())
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn same_transaction_define_concurrent_index_write_uses_fresh_fence_snapshot() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
				DEFINE TABLE user SCHEMALESS;
				CREATE user:one SET email = 'old@example.com' RETURN NONE;
				",
	)
	.await?;

	let site = RetryableConflictSite::ConcurrentIndexInitialCleanup;
	let node_id = ds.id();
	let guard = inject_retryable_conflicts(site, node_id, REPEATED_RETRY_CONFLICTS);
	execute_all(
		&ds,
		&session,
		"
				BEGIN;
				DEFINE INDEX test ON user FIELDS email CONCURRENTLY;
				UPDATE user:one SET email = 'new@example.com' RETURN NONE;
				COMMIT;
				",
	)
	.await?;
	wait_for_retry_conflict(site, node_id, REPEATED_RETRY_CONFLICTS).await?;
	drop(guard);
	wait_for_index_ready(&ds, &session, "user", "test").await?;

	expect_indexed_query_len(
		&ds,
		&session,
		"SELECT id FROM user WITH INDEX test WHERE email = 'new@example.com'",
		1,
	)
	.await?;
	expect_indexed_query_len(
		&ds,
		&session,
		"SELECT id FROM user WITH INDEX test WHERE email = 'old@example.com'",
		0,
	)
	.await?;
	Ok(())
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn same_transaction_overwrite_concurrent_index_write_uses_fresh_fence_snapshot() -> Result<()>
{
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
				DEFINE TABLE user SCHEMALESS;
				CREATE user:one SET email = 'old@example.com', account = 'old-account' RETURN NONE;
				DEFINE INDEX test ON user FIELDS email;
				",
	)
	.await?;

	let site = RetryableConflictSite::ConcurrentIndexInitialCleanup;
	let node_id = ds.id();
	let guard = inject_retryable_conflicts(site, node_id, REPEATED_RETRY_CONFLICTS);
	execute_all(
		&ds,
		&session,
		"
				BEGIN;
				DEFINE INDEX OVERWRITE test ON user FIELDS account CONCURRENTLY;
				UPDATE user:one SET account = 'new-account' RETURN NONE;
				COMMIT;
				",
	)
	.await?;
	wait_for_retry_conflict(site, node_id, REPEATED_RETRY_CONFLICTS).await?;
	drop(guard);
	wait_for_index_ready(&ds, &session, "user", "test").await?;

	expect_indexed_query_len(
		&ds,
		&session,
		"SELECT id FROM user WITH INDEX test WHERE account = 'new-account'",
		1,
	)
	.await?;
	expect_indexed_query_len(
		&ds,
		&session,
		"SELECT id FROM user WITH INDEX test WHERE account = 'old-account'",
		0,
	)
	.await?;
	Ok(())
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn define_index_overwrite_aborts_retired_local_builder_after_commit() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
				DEFINE TABLE user SCHEMALESS;
				CREATE user:one SET email = 'one@example.com', account = 'apple' RETURN NONE;
				CREATE user:two SET email = 'two@example.com', account = 'banana' RETURN NONE;
				",
	)
	.await?;

	let guard = start_index_build_paused(
		&ds,
		&session,
		"DEFINE INDEX test ON user FIELDS email CONCURRENTLY",
	)
	.await?;
	let (ns, db, table, old_ix) = get_table_index(&ds, "user", "test").await?;
	let old_builder = local_builder_for_key(&ds, ns, db, &table, old_ix.index_id)
		.await?
		.expect("retired local builder should be running");

	execute_all_retrying_conflicts(
		&ds,
		&session,
		"DEFINE INDEX OVERWRITE test ON user FIELDS account CONCURRENTLY",
	)
	.await?;
	wait_for_no_local_builder(&ds, ns, db, &table, old_ix.index_id).await?;
	assert_no_index_build_artifacts(&ds, ns, db, &table, old_ix.index_id).await?;
	drop(guard);
	// The replacement's own build shares the injected conflict site, so the
	// retired builder is released alongside it. It must still write nothing.
	wait_for_finished_builder(&old_builder).await?;
	assert_no_index_build_artifacts(&ds, ns, db, &table, old_ix.index_id).await?;
	wait_for_index_ready(&ds, &session, "user", "test").await?;

	let (_, _, _, new_ix) = get_table_index(&ds, "user", "test").await?;
	assert_ne!(new_ix.index_id, old_ix.index_id);
	Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn define_index_overwrite_cancel_preserves_previous_index() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:one SET email = 'one@example.com', account = 'apple' RETURN NONE;
			DEFINE INDEX test ON user FIELDS email;
			",
	)
	.await?;
	let (_, _, _, old_ix) = get_table_index(&ds, "user", "test").await?;

	execute_cancelled_transaction(
		&ds,
		&session,
		"BEGIN; DEFINE INDEX OVERWRITE test ON user FIELDS account; CANCEL;",
	)
	.await?;

	let (_, _, _, current_ix) = get_table_index(&ds, "user", "test").await?;
	assert_eq!(current_ix.index_id, old_ix.index_id);
	assert_eq!(index_building_status(&ds, &session, "user", "test").await?, "ready");
	expect_indexed_query_len(
		&ds,
		&session,
		"SELECT * FROM user WITH INDEX test WHERE email = 'one@example.com'",
		1,
	)
	.await?;
	Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn define_index_concurrent_cancel_cleans_uncommitted_build_artifacts() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:one SET email = 'one@example.com' RETURN NONE;
			CREATE user:two SET email = 'two@example.com' RETURN NONE;
			",
	)
	.await?;

	execute_cancelled_transaction(
		&ds,
		&session,
		"BEGIN; DEFINE INDEX test ON user FIELDS email CONCURRENTLY; CANCEL;",
	)
	.await?;

	execute_all(&ds, &session, "DEFINE INDEX test ON user FIELDS email CONCURRENTLY").await?;
	wait_for_index_ready(&ds, &session, "user", "test").await?;
	let (ns, db, table, current_ix) = get_table_index(&ds, "user", "test").await?;
	let cancelled_ix = previous_index_id(current_ix.index_id);
	assert_no_index_build_artifacts(&ds, ns, db, &table, cancelled_ix).await?;
	Ok(())
}

/// The rollback cleanup deletes an uncommitted build's durable state from its
/// own transaction, so it must not return until the builder task has stopped
/// writing: a builder write ordered after that delete re-creates state for a
/// build the catalog never referenced, and nothing collects it.
///
/// `define_index_concurrent_cancel_cleans_uncommitted_build_artifacts` covers
/// the same guarantee end to end but only fails when the race lands. Here the
/// builder is held at an injected conflict site, so a signal-only abort leaves
/// it demonstrably unfinished at the point the deletes would run.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn remove_index_and_wait_returns_only_after_the_builder_task_exits() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:one SET email = 'one@example.com' RETURN NONE;
			CREATE user:two SET email = 'two@example.com' RETURN NONE;
			",
	)
	.await?;

	let _guard = start_index_build_paused(
		&ds,
		&session,
		"DEFINE INDEX test ON user FIELDS email CONCURRENTLY",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "user", "test").await?;
	let building = local_builder_for_key(&ds, ns, db, &table, ix.index_id)
		.await?
		.expect("local builder should be running");
	assert!(!building.is_finished(), "the paused builder should still be running");

	ds.index_builder()
		.remove_index_and_wait(ns, db, &table, ix.index_id, build_abort_deadline(Instant::now()))
		.await;

	assert!(
		building.is_finished(),
		"remove_index_and_wait returned while the builder task was still writing"
	);
	assert!(
		local_builder_for_key(&ds, ns, db, &table, ix.index_id).await?.is_none(),
		"the aborted builder must be gone from the local task map"
	);
	Ok(())
}

/// The wait budget belongs to the whole transaction-close drain, so a cleanup
/// that runs after the budget is spent must not wait again: N indexes in one
/// rolled-back schema transaction cost one budget, not N.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn remove_index_and_wait_stops_waiting_once_the_drain_budget_is_spent() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:one SET email = 'one@example.com' RETURN NONE;
			",
	)
	.await?;

	let _guard = start_index_build_paused(
		&ds,
		&session,
		"DEFINE INDEX test ON user FIELDS email CONCURRENTLY",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "user", "test").await?;
	let building = local_builder_for_key(&ds, ns, db, &table, ix.index_id)
		.await?
		.expect("local builder should be running");

	// A deadline in the past stands for an earlier cleanup in the same drain
	// having consumed the budget.
	let started = Instant::now();
	ds.index_builder().remove_index_and_wait(ns, db, &table, ix.index_id, Instant::now()).await;
	assert!(
		started.elapsed() < Duration::from_secs(2),
		"a spent budget must not buy another wait (took {:?})",
		started.elapsed()
	);
	assert!(
		building.aborted.load(Ordering::Relaxed),
		"the builder must still be signalled to abort when the wait is skipped"
	);
	Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn define_index_blocking_cancel_cleans_uncommitted_build_artifacts() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:one SET email = 'one@example.com' RETURN NONE;
			CREATE user:two SET email = 'two@example.com' RETURN NONE;
			",
	)
	.await?;

	execute_cancelled_transaction(
		&ds,
		&session,
		"BEGIN; DEFINE INDEX test ON user FIELDS email; CANCEL;",
	)
	.await?;

	execute_all(&ds, &session, "DEFINE INDEX test ON user FIELDS email").await?;
	let (ns, db, table, current_ix) = get_table_index(&ds, "user", "test").await?;
	let cancelled_ix = previous_index_id(current_ix.index_id);
	assert_no_index_build_artifacts(&ds, ns, db, &table, cancelled_ix).await?;
	Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn define_index_overwrite_cancel_cleans_new_build_artifacts() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:one SET email = 'one@example.com', account = 'apple' RETURN NONE;
			CREATE user:two SET email = 'two@example.com', account = 'banana' RETURN NONE;
			DEFINE INDEX test ON user FIELDS email;
			",
	)
	.await?;
	let (ns, db, table, old_ix) = get_table_index(&ds, "user", "test").await?;

	execute_cancelled_transaction(
		&ds,
		&session,
		"BEGIN; DEFINE INDEX OVERWRITE test ON user FIELDS account CONCURRENTLY; CANCEL;",
	)
	.await?;
	let (_, _, _, preserved_ix) = get_table_index(&ds, "user", "test").await?;
	assert_eq!(preserved_ix.index_id, old_ix.index_id);

	execute_all(&ds, &session, "DEFINE INDEX OVERWRITE test ON user FIELDS account CONCURRENTLY")
		.await?;
	wait_for_index_ready(&ds, &session, "user", "test").await?;
	let (_, _, _, current_ix) = get_table_index(&ds, "user", "test").await?;
	let cancelled_ix = previous_index_id(current_ix.index_id);
	assert_ne!(cancelled_ix, old_ix.index_id);
	assert_no_index_build_artifacts(&ds, ns, db, &table, cancelled_ix).await?;
	Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn define_index_overwrite_commits_new_id_and_retires_old_lookup() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:one SET email = 'one@example.com', account = 'apple' RETURN NONE;
			DEFINE INDEX test ON user FIELDS email;
			",
	)
	.await?;
	let (ns, db, table, old_ix) = get_table_index(&ds, "user", "test").await?;
	let old_ikb = IndexKeyBase::new(ns, db, table.clone(), old_ix.index_id);
	seed_durable_queue_generation(&ds, &old_ikb, 1).await?;
	assert!(durable_build_state_exists(&ds, &old_ikb).await?);
	assert_eq!(durable_queue_all_generations_count(&ds, &old_ikb).await?, 3);

	execute_all(&ds, &session, "DEFINE INDEX OVERWRITE test ON user FIELDS account").await?;

	let (_, _, _, current_ix) = get_table_index(&ds, "user", "test").await?;
	assert_ne!(current_ix.index_id, old_ix.index_id);
	expect_indexed_query_len(
		&ds,
		&session,
		"SELECT * FROM user WITH INDEX test WHERE account = 'apple'",
		1,
	)
	.await?;

	let tx = ds.transaction(TransactionType::Read).await?;
	let old_lookup = catch!(tx, tx.get_tb_index_by_id(ns, db, &table, old_ix.index_id, None).await);
	let current_lookup =
		catch!(tx, tx.get_tb_index_by_id(ns, db, &table, current_ix.index_id, None).await);
	tx.cancel().await?;
	assert!(old_lookup.is_none(), "old index id lookup should be retired");
	assert!(current_lookup.is_some(), "current index id lookup should remain");
	assert!(
		!durable_build_state_exists(&ds, &old_ikb).await?,
		"old durable build state should be retired"
	);
	assert!(
		durable_build_state_exists(&ds, &IndexKeyBase::new(ns, db, table, current_ix.index_id))
			.await?,
		"current durable build state should remain"
	);
	assert_eq!(
		durable_queue_all_generations_count(&ds, &old_ikb).await?,
		0,
		"old durable queue keys should be retired"
	);
	Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn missing_durable_state_filters_retired_cached_index_definitions() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:one SET email = 'one@example.com', account = 'apple', name = 'one', age = 1 RETURN NONE;
			DEFINE INDEX online ON user FIELDS email;
			DEFINE INDEX building ON user FIELDS account;
			DEFINE INDEX legacy ON user FIELDS name;
			DEFINE INDEX stale ON user FIELDS age;
			",
	)
	.await?;
	let (ns, db, table, online_ix) = get_table_index(&ds, "user", "online").await?;
	let (_, _, _, building_ix) = get_table_index(&ds, "user", "building").await?;
	let (_, _, _, legacy_ix) = get_table_index(&ds, "user", "legacy").await?;
	let (_, _, _, stale_ix) = get_table_index(&ds, "user", "stale").await?;
	let online_ikb = IndexKeyBase::new(ns, db, table.clone(), online_ix.index_id);
	let building_ikb = IndexKeyBase::new(ns, db, table.clone(), building_ix.index_id);
	let legacy_ikb = IndexKeyBase::new(ns, db, table.clone(), legacy_ix.index_id);

	// Exercise every branch in one cached catalog slice: durable online stays
	// visible, durable building is hidden, missing current state is legacy-ready,
	// and missing retired state is filtered.
	let tx = ds.transaction(TransactionType::Write).await?;
	tx.set_key(
		&online_ikb.new_bs_key(),
		&durable_build_state_for_phase(IndexBuildPhase::Online, 1, None),
	)
	.await?;
	tx.set_key(
		&building_ikb.new_bs_key(),
		&durable_build_state_for_phase(IndexBuildPhase::Building, 1, Some(ds.id())),
	)
	.await?;
	tx.del_key(&legacy_ikb.new_bs_key()).await?;
	tx.commit().await?;

	execute_all(&ds, &session, "DEFINE INDEX OVERWRITE stale ON user FIELDS score").await?;

	let tx = ds.transaction(TransactionType::Read).await?;
	let indexes: Arc<[IndexDefinition]> = Arc::from(vec![
		online_ix.as_ref().clone(),
		building_ix.as_ref().clone(),
		legacy_ix.as_ref().clone(),
		stale_ix.as_ref().clone(),
	]);
	let filtered = filter_online_indexes(&tx, ns, db, indexes).await?;
	tx.cancel().await?;
	let names: Vec<_> = filtered.iter().map(|ix| ix.name.as_str()).collect();
	assert_eq!(
		names,
		vec!["online", "legacy"],
		"only durable-online and catalog-reachable legacy indexes should remain"
	);
	Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn filter_online_indexes_batches_durable_state_reads() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:one SET email = 'one@example.com', account = 'apple', name = 'one' RETURN NONE;
			DEFINE INDEX one ON user FIELDS email;
			DEFINE INDEX two ON user FIELDS account;
			DEFINE INDEX three ON user FIELDS name;
			",
	)
	.await?;
	let (ns, db, table, one_ix) = get_table_index(&ds, "user", "one").await?;
	let (_, _, _, two_ix) = get_table_index(&ds, "user", "two").await?;
	let (_, _, _, three_ix) = get_table_index(&ds, "user", "three").await?;

	let tx = ds.transaction(TransactionType::Write).await?;
	for ix in [&one_ix, &two_ix, &three_ix] {
		let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
		tx.set_key(
			&ikb.new_bs_key(),
			&durable_build_state_for_phase(IndexBuildPhase::Online, 1, None),
		)
		.await?;
	}
	tx.commit().await?;

	let tx = ds.transaction(TransactionType::Read).await?;
	let indexes: Arc<[IndexDefinition]> = Arc::from(vec![
		one_ix.as_ref().clone(),
		two_ix.as_ref().clone(),
		three_ix.as_ref().clone(),
	]);
	let filtered = filter_online_indexes(&tx, ns, db, indexes).await?;
	let metrics = tx.metrics_snapshot_for_test();
	tx.cancel().await?;
	assert_eq!(filtered.len(), 3);
	assert_eq!(metrics.ops_get, 1, "durable build states should be read with one batched get");
	Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn consume_skips_retired_cached_index_definition() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:one SET email = 'one@example.com', account = 'apple' RETURN NONE;
			DEFINE INDEX test ON user FIELDS email;
			",
	)
	.await?;
	let (_, _, table, old_ix) = get_table_index(&ds, "user", "test").await?;

	execute_all(&ds, &session, "DEFINE INDEX OVERWRITE test ON user FIELDS account").await?;

	let tx = Arc::new(ds.transaction(TransactionType::Write).await?);
	let db_def = tx.get_db_by_name("test", "test", None).await?.expect("database should exist");
	let mut ctx = ds.setup_ctx()?;
	ctx.set_transaction(Arc::clone(&tx));
	let ctx = ctx.freeze();
	let rid = RecordId {
		table,
		key: RecordIdKey::from("two".to_owned()),
	};
	let result = ctx
		.get_index_builder()
		.expect("index builder should be present")
		.consume(
			db_def.as_ref(),
			&ctx,
			&old_ix,
			IndexMutation {
				old_values: None,
				new_values: None,
				rid: &rid,
				count_cond_match: None,
			},
		)
		.await?;
	tx.cancel().await?;

	assert!(matches!(result, ConsumeResult::Retired));
	Ok(())
}

/// The same blind spot as the cached-admission recheck, on the path a bulk
/// write takes for its *first* record.
///
/// First-use admission also has to tell a legacy index with no `!bs` apart from
/// one the catalog has retired, and asking the write's own transaction cannot
/// do it: that transaction read the definition before the removal and answers
/// from its cache. Getting it wrong here is the worse direction — the write
/// would index normally into a retired index's prefix, leaving entries under an
/// id the catalog no longer names.
#[tokio::test(flavor = "multi_thread")]
async fn consume_skips_an_index_retired_after_the_write_began() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:one SET email = 'one@example.com' RETURN NONE;
			DEFINE INDEX test ON user FIELDS email;
			",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "user", "test").await?;

	// The write's transaction, opened and its catalog cache warmed before the
	// removal commits.
	let tx = Arc::new(ds.transaction(TransactionType::Write).await?);
	let db_def = tx.get_db_by_name("test", "test", None).await?.expect("database should exist");
	assert!(
		tx.get_tb_index(ns, db, &table, ix.name.as_str(), None).await?.is_some(),
		"the write's transaction must have the definition cached for this to mean anything"
	);

	execute_all(&ds, &session, "REMOVE INDEX test ON user").await?;

	let mut ctx = ds.setup_ctx()?;
	ctx.set_transaction(Arc::clone(&tx));
	let ctx = ctx.freeze();
	let rid = RecordId {
		table,
		key: RecordIdKey::from("two".to_owned()),
	};
	let result = ctx
		.get_index_builder()
		.expect("index builder should be present")
		.consume(
			db_def.as_ref(),
			&ctx,
			&ix,
			IndexMutation {
				old_values: None,
				new_values: Some(vec![Value::from("two@example.com")]),
				rid: &rid,
				count_cond_match: None,
			},
		)
		.await?;
	tx.cancel().await?;

	assert!(
		matches!(result, ConsumeResult::Retired),
		"a write must not index into an index retired after it began"
	);
	Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn compaction_write_fence_rejects_committed_index_removal() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:one SET email = 'one@example.com' RETURN NONE;
			DEFINE INDEX test ON user FIELDS email;
			",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "user", "test").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	let generation = durable_build_state(&ds, &ikb).await?.generation;

	let tx = ds.transaction(TransactionType::Read).await?;
	let table_def = catch!(tx, tx.get_tb(ns, db, &table, None).await).expect("table should exist");
	let mut ctx = ds.setup_ctx()?;
	let tx = Arc::new(tx);
	ctx.set_transaction(Arc::clone(&tx));
	let ctx = ctx.freeze();
	let build = Building::new(
		&ctx,
		ds.transaction_factory().clone(),
		ds.setup_options(&session),
		table_def.table_id,
		Arc::clone(&ix),
		Arc::new(IndexKey::new(ns, db, &table, ix.index_id)),
	)?;
	build.build_generation.store(generation, Ordering::Release);
	tx.cancel().await?;

	let tx = ds.transaction(TransactionType::Write).await?;
	assert!(build.compaction_write_still_owns_index(&tx, generation).await?);
	tx.cancel().await?;

	execute_all(&ds, &session, "REMOVE INDEX test ON user").await?;

	let tx = ds.transaction(TransactionType::Write).await?;
	assert!(
		!build.compaction_write_still_owns_index(&tx, generation).await?,
		"retired indexes must not accept post-online builder compaction writes"
	);
	tx.cancel().await?;
	Ok(())
}

/// Passing the compaction fence has to bind at commit, not just at the check.
///
/// Compaction runs after `Online`, so it has no ownership heartbeat to
/// compare-and-swap the way a build batch does — its guard is a read. A
/// retirement that commits between that read and the apply's commit has to
/// take the apply with it; otherwise the compaction writes index data under a
/// prefix the catalog no longer names, and only the queued reclaim would sweep
/// it, if it had not already finished.
#[cfg(feature = "kv-rocksdb")]
#[tokio::test(flavor = "multi_thread")]
async fn compaction_write_fence_conflicts_with_a_retirement_that_commits_after_it() -> Result<()> {
	let dir = TempDir::new()?;
	let ds = Datastore::builder()
		.without_maintenance_tasks()
		.build_with_path(&format!("rocksdb://{}", dir.path().to_string_lossy()))
		.await?;
	let session = Session::owner().with_ns("test").with_db("test");
	let tx = ds.transaction(TransactionType::Write).await?;
	tx.ensure_ns_db(None, "test", "test").await?;
	tx.commit().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:one SET email = 'one@example.com' RETURN NONE;
			DEFINE INDEX test ON user FIELDS email;
			",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "user", "test").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	let state = durable_build_state(&ds, &ikb).await?;

	let tx = ds.transaction(TransactionType::Write).await?;
	let mut ctx = ds.setup_ctx()?;
	ctx.set_transaction(Arc::new(ds.transaction(TransactionType::Read).await?));
	let ctx = ctx.freeze();
	let table_def = ds
		.transaction(TransactionType::Read)
		.await?
		.get_tb(ns, db, &table, None)
		.await?
		.expect("table should exist");
	let build = Building::new(
		&ctx,
		ds.transaction_factory().clone(),
		ds.setup_options(&session),
		table_def.table_id,
		Arc::clone(&ix),
		Arc::new(IndexKey::new(ns, db, &table, ix.index_id)),
	)?;

	// The compaction transaction passes its guard while the index is live.
	assert!(build.compaction_write_still_owns_index(&tx, state.generation).await?);

	// The retirement commits in between, as it may on any node.
	execute_all(&ds, &session, "REMOVE INDEX test ON user").await?;

	// The apply this guard was standing in for. It writes index data, not the
	// guarded key: a write to `!bs` itself would conflict on the write alone
	// and prove nothing about whether the guard's read is tracked.
	let mut entry = IdxRoot {
		ns,
		db,
		tb: Cow::Borrowed(&table),
		ix: ix.index_id,
	}
	.range()?
	.start()
	.to_vec();
	entry.push(0x01);
	tx.set(Key::from(entry), vec![0x01]).await?;
	let err = tx.commit().await.expect_err("the retirement must take the compaction with it");
	assert!(is_retryable_transaction_conflict(&err), "expected a retryable conflict, got: {err}");
	let _ = tx.cancel().await;
	Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn compaction_write_fence_rejects_previous_rebuild_generation() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:one SET email = 'one@example.com' RETURN NONE;
			DEFINE INDEX test ON user FIELDS email;
			",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "user", "test").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	let generation = durable_build_state(&ds, &ikb).await?.generation;

	let tx = ds.transaction(TransactionType::Read).await?;
	let table_def = catch!(tx, tx.get_tb(ns, db, &table, None).await).expect("table should exist");
	let mut ctx = ds.setup_ctx()?;
	let tx = Arc::new(tx);
	ctx.set_transaction(Arc::clone(&tx));
	let ctx = ctx.freeze();
	let build = Building::new(
		&ctx,
		ds.transaction_factory().clone(),
		ds.setup_options(&session),
		table_def.table_id,
		Arc::clone(&ix),
		Arc::new(IndexKey::new(ns, db, &table, ix.index_id)),
	)?;
	build.build_generation.store(generation, Ordering::Release);
	tx.cancel().await?;

	let tx = ds.transaction(TransactionType::Write).await?;
	assert!(build.compaction_write_still_owns_index(&tx, generation).await?);
	tx.cancel().await?;

	execute_all(&ds, &session, "REBUILD INDEX test ON user").await?;

	let (_, _, _, rebuilt_ix) = get_table_index(&ds, "user", "test").await?;
	assert_eq!(rebuilt_ix.index_id, ix.index_id);
	assert_eq!(rebuilt_ix.name, ix.name);

	let rebuilt_state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(rebuilt_state.generation, generation.saturating_add(1));
	assert_eq!(rebuilt_state.phase, IndexBuildPhase::Online);

	let tx = ds.transaction(TransactionType::Write).await?;
	assert!(
		!build.compaction_write_still_owns_index(&tx, generation).await?,
		"previous build generations must not accept post-online compaction writes after rebuild"
	);
	assert!(
		build.compaction_write_still_owns_index(&tx, rebuilt_state.generation).await?,
		"current online generation should still accept post-online compaction writes"
	);
	tx.cancel().await?;
	Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn fresh_build_cleans_stale_durable_queue_generations() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
				DEFINE TABLE user SCHEMALESS;
				CREATE user:one SET email = 'one@example.com' RETURN NONE;
				DEFINE INDEX test ON user FIELDS email;
				",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "user", "test").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	for generation in 1..=3 {
		seed_durable_queue_generation(&ds, &ikb, generation).await?;
	}
	let tx = ds.transaction(TransactionType::Write).await?;
	tx.set_key(
		&ikb.new_bs_key(),
		&IndexBuildState {
			generation: 3,
			phase: IndexBuildPhase::Error,
			owner: None,
			next_ticket: 4,
			initial_complete: false,
			updated_at: Utc::now(),
			owner_heartbeat_at: None,
			error: Some("previous build failed".to_string()),
			report_status: Some(IndexBuildReportStatus::Error),
			initial: None,
			updated: None,
			pending: None,
			initial_cursor: None,
		},
	)
	.await?;
	tx.commit().await?;

	let tx = ds.transaction(TransactionType::Read).await?;
	let table_def = tx.get_tb(ns, db, &table, None).await?.expect("table should exist");
	let mut ctx = ds.setup_ctx()?;
	let tx = Arc::new(tx);
	ctx.set_transaction(Arc::clone(&tx));
	let ctx = ctx.freeze();
	let build = Building::new(
		&ctx,
		ds.transaction_factory().clone(),
		ds.setup_options(&session),
		table_def.table_id,
		Arc::clone(&ix),
		Arc::new(IndexKey::new(ns, db, &table, ix.index_id)),
	)?;
	let acquired = build.acquire_build_state().await?.expect("fresh build should start");
	tx.cancel().await?;

	assert_eq!(acquired.generation, 4);
	for generation in 1..=3 {
		assert_eq!(durable_queue_generation_count(&ds, &ikb, generation).await?, 0);
	}
	Ok(())
}

async fn count_query_value(ds: &Datastore, session: &Session, sql: &str) -> Result<i64> {
	let mut results = ds.execute(sql, session, None).await?;
	let value = results.remove(0).result?;
	let surrealdb_types::Value::Array(rows) = value else {
		anyhow::bail!("count query returned non-array value: {value:?}");
	};
	let Some(surrealdb_types::Value::Object(row)) = rows.first() else {
		anyhow::bail!("count query returned no object row: {rows:?}");
	};
	let Some(surrealdb_types::Value::Number(count)) = row.get("count") else {
		anyhow::bail!("count query returned no numeric count field: {row:?}");
	};
	count.to_int().ok_or_else(|| anyhow::anyhow!("count value is not an integer"))
}

async fn count_index_value(ds: &Datastore, session: &Session) -> Result<i64> {
	count_query_value(ds, session, "SELECT count() FROM user GROUP ALL").await
}

/// Number of undrained count-index delta entries for one index.
#[cfg(feature = "kv-mem")]
async fn count_index_delta_keys(
	ds: &Datastore,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
	ix: IndexId,
) -> Result<usize> {
	let tx = ds.transaction(TransactionType::Read).await?;
	let rng = IndexCountPrefix {
		ns,
		db,
		tb: Cow::Borrowed(table),
		ix,
	}
	.range()?;
	let keys = catch!(tx, tx.keys(rng, u32::MAX, 0, None).await);
	tx.cancel().await?;
	Ok(keys.len())
}

/// Put this index's durable build state into `Building` under an owner that is
/// not this node, so the compaction task sees a build in flight elsewhere in
/// the cluster. `heartbeat_age` back-dates the owner's heartbeat.
#[cfg(feature = "kv-mem")]
async fn claim_build_for_a_remote_owner(
	ds: &Datastore,
	ikb: &IndexKeyBase,
	heartbeat_age: chrono::Duration,
) -> Result<()> {
	let mut state = durable_build_state(ds, ikb).await?;
	state.phase = IndexBuildPhase::Building;
	state.owner = Some(Uuid::now_v7());
	state.owner_heartbeat_at = Some(Utc::now() - heartbeat_age);
	set_durable_build_state(ds, ikb, state).await
}

/// Number of HNSW pending entries queued for one index.
#[cfg(feature = "kv-mem")]
async fn count_hnsw_pendings(ds: &Datastore, ikb: &IndexKeyBase) -> Result<usize> {
	let tx = ds.transaction(TransactionType::Read).await?;
	let count = catch!(tx, tx.count(ikb.new_hr_range()?, None).await);
	tx.cancel().await?;
	Ok(count)
}

async fn wait_for_retry_conflict(
	site: RetryableConflictSite,
	node_id: uuid::Uuid,
	initial_count: usize,
) -> Result<()> {
	timeout(Duration::from_secs(10), async {
		loop {
			if retryable_conflict_count(site, node_id) < initial_count {
				return Ok(());
			}
			sleep(Duration::from_millis(10)).await;
		}
	})
	.await
	.map_err(|_| anyhow::anyhow!("timed out waiting for injected retry conflict"))?
}

async fn wait_for_retry_conflict_count_to_stabilize(
	site: RetryableConflictSite,
	node_id: uuid::Uuid,
) -> Result<usize> {
	timeout(Duration::from_secs(10), async {
		let mut previous = retryable_conflict_count(site, node_id);
		loop {
			sleep(Duration::from_millis(250)).await;
			let current = retryable_conflict_count(site, node_id);
			if current == previous {
				return Ok(current);
			}
			previous = current;
		}
	})
	.await
	.map_err(|_| anyhow::anyhow!("timed out waiting for retry conflict count to stabilize"))?
}

#[tokio::test(flavor = "multi_thread")]
async fn count_index_duplicate_initial_build_does_not_overcount() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:1 RETURN NONE;
			CREATE user:2 RETURN NONE;
			",
	)
	.await?;

	let table_name = TableName::from("user");
	let (ns_id, db_id, table_id, index) = {
		let tx = ds.transaction(TransactionType::Write).await?;
		let ns = tx.get_ns_by_name("test", None).await?.expect("namespace should exist");
		let db = tx.get_db_by_name("test", "test", None).await?.expect("database should exist");
		let table = tx
			.get_tb(ns.namespace_id, db.database_id, &table_name, None)
			.await?
			.expect("table should exist");
		let index = IndexDefinition {
			index_id: IndexId(1),
			name: "test".into(),
			table_name: table_name.clone(),
			cols: Vec::new(),
			index: Index::Count(None),
			count_cond: None,
			comment: None,
			prepare_remove: false,
			format_version: 1,
		};
		tx.put_tb_index(ns.namespace_id, db.database_id, &table_name, &index).await?;
		tx.commit().await?;
		(ns.namespace_id, db.database_id, table.table_id, Arc::new(index))
	};

	let index_key = Arc::new(IndexKey::new(ns_id, db_id, &table_name, index.index_id));
	let opt = ds.setup_options(&session);
	let mut ctx = ds.setup_ctx()?;
	let read_tx = Arc::new(ds.transaction(TransactionType::Read).await?);
	ctx.set_transaction(Arc::clone(&read_tx));
	let ctx = ctx.freeze();
	let build_a = Building::new(
		&ctx,
		ds.transaction_factory().clone(),
		opt.clone(),
		table_id,
		Arc::clone(&index),
		Arc::clone(&index_key),
	)?;
	let build_b =
		Building::new(&ctx, ds.transaction_factory().clone(), opt, table_id, index, index_key)?;
	read_tx.cancel().await?;

	let (a, b) = tokio::join!(build_a.run(), build_b.run());
	a?;
	b?;

	assert_eq!(count_index_value(&ds, &session).await?, 2);
	Ok(())
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn count_index_initial_scan_preserves_where_count_baseline() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:one SET status = 'active' RETURN NONE;
			CREATE user:two SET status = 'active' RETURN NONE;
			CREATE user:three SET status = 'inactive' RETURN NONE;
			DEFINE INDEX test ON user COUNT WHERE status = 'active' CONCURRENTLY;
			",
	)
	.await?;
	wait_for_index_ready(&ds, &session, "user", "test").await?;

	assert_eq!(
		count_query_value(
			&ds,
			&session,
			"SELECT count() FROM user WHERE status = 'active' GROUP ALL"
		)
		.await?,
		2
	);
	execute_all_retrying_conflicts(
		&ds,
		&session,
		"CREATE user:four SET status = 'active' RETURN NONE",
	)
	.await?;
	assert_eq!(
		count_query_value(
			&ds,
			&session,
			"SELECT count() FROM user WHERE status = 'active' GROUP ALL"
		)
		.await?,
		3
	);
	Ok(())
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn count_index_delete_before_scan_preserves_plain_count_baseline() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:one RETURN NONE;
			",
	)
	.await?;

	let guard =
		start_index_build_paused(&ds, &session, "DEFINE INDEX test ON user COUNT CONCURRENTLY")
			.await?;
	execute_all_retrying_conflicts(&ds, &session, "DELETE user:one RETURN NONE").await?;
	drop(guard);
	wait_for_index_ready(&ds, &session, "user", "test").await?;

	execute_all_retrying_conflicts(&ds, &session, "CREATE user:two RETURN NONE").await?;
	assert_eq!(count_index_value(&ds, &session).await?, 1);
	Ok(())
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn count_index_delete_before_scan_preserves_where_count_baseline() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:one SET status = 'active' RETURN NONE;
			",
	)
	.await?;

	let guard = start_index_build_paused(
		&ds,
		&session,
		"DEFINE INDEX test ON user COUNT WHERE status = 'active' CONCURRENTLY",
	)
	.await?;
	execute_all_retrying_conflicts(&ds, &session, "DELETE user:one RETURN NONE").await?;
	drop(guard);
	wait_for_index_ready(&ds, &session, "user", "test").await?;

	execute_all_retrying_conflicts(
		&ds,
		&session,
		"CREATE user:two SET status = 'active' RETURN NONE",
	)
	.await?;
	assert_eq!(
		count_query_value(
			&ds,
			&session,
			"SELECT count() FROM user WHERE status = 'active' GROUP ALL"
		)
		.await?,
		1
	);
	Ok(())
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn count_index_updates_before_scan_preserve_where_count_baseline() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:one SET status = 'active' RETURN NONE;
			CREATE user:two SET status = 'inactive' RETURN NONE;
			",
	)
	.await?;

	let guard = start_index_build_paused(
		&ds,
		&session,
		"DEFINE INDEX test ON user COUNT WHERE status = 'active' CONCURRENTLY",
	)
	.await?;
	execute_all_retrying_conflicts(
		&ds,
		&session,
		"
			UPDATE user:one SET status = 'inactive' RETURN NONE;
			UPDATE user:two SET status = 'active' RETURN NONE;
			",
	)
	.await?;
	drop(guard);
	wait_for_index_ready(&ds, &session, "user", "test").await?;

	assert_eq!(
		count_query_value(
			&ds,
			&session,
			"SELECT count() FROM user WHERE status = 'active' GROUP ALL"
		)
		.await?,
		1
	);
	execute_all_retrying_conflicts(
		&ds,
		&session,
		"CREATE user:three SET status = 'active' RETURN NONE",
	)
	.await?;
	assert_eq!(
		count_query_value(
			&ds,
			&session,
			"SELECT count() FROM user WHERE status = 'active' GROUP ALL"
		)
		.await?,
		2
	);
	Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn takeover_preserves_durable_progress_counts() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:one SET email = 'one@example.com' RETURN NONE;
			CREATE user:two SET email = 'two@example.com' RETURN NONE;
			DEFINE INDEX test ON user FIELDS email;
			",
	)
	.await?;

	let (ns, db, table, ix) = get_table_index(&ds, "user", "test").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	let cases = [(IndexBuildPhase::Building, 2, 42, 7), (IndexBuildPhase::Closing, 3, 84, 11)];

	for (phase, generation, initial, updated) in cases {
		let expired = Utc::now() - chrono::Duration::seconds(BUILD_OWNER_LEASE_SECS + 5);
		let tx = ds.transaction(TransactionType::Write).await?;
		tx.set_key(
			&ikb.new_bs_key(),
			&IndexBuildState {
				generation,
				phase,
				owner: Some(uuid::Uuid::new_v4()),
				next_ticket: 0,
				initial_complete: true,
				updated_at: expired,
				owner_heartbeat_at: Some(expired),
				error: None,
				report_status: Some(IndexBuildReportStatus::Indexing),
				initial: Some(initial),
				updated: Some(updated),
				pending: Some(5),
				initial_cursor: None,
			},
		)
		.await?;
		tx.commit().await?;

		let build = new_building_for_index(&ds, &session, ns, db, &table, Arc::clone(&ix)).await?;
		let acquired = build
			.acquire_build_state()
			.await?
			.expect("expired build state should be available for takeover");
		build.run_acquired(acquired).await?;

		let state = durable_build_state(&ds, &ikb).await?;
		assert_eq!(state.phase, IndexBuildPhase::Online);
		assert_eq!(state.initial, Some(initial));
		assert_eq!(state.updated, Some(updated));
		assert_eq!(state.pending, Some(0));

		let building = index_building_json(&ds, &session, "user", "test").await?;
		assert_eq!(building.get("status").and_then(|status| status.as_str()), Some("ready"));
		assert_eq!(building.get("initial").and_then(|initial| initial.as_u64()), Some(initial));
		assert_eq!(building.get("updated").and_then(|updated| updated.as_u64()), Some(updated));
	}

	Ok(())
}

/// A takeover of a `Building` generation whose owner committed at least one
/// initial-scan batch resumes the scan right after the persisted checkpoint
/// instead of wiping the partial index data and rescanning from the start.
#[tokio::test(flavor = "multi_thread")]
async fn takeover_resumes_initial_scan_from_checkpoint() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:a SET email = 'a@example.com' RETURN NONE;
			CREATE user:b SET email = 'b@example.com' RETURN NONE;
			CREATE user:c SET email = 'c@example.com' RETURN NONE;
			CREATE user:d SET email = 'd@example.com' RETURN NONE;
			CREATE user:e SET email = 'e@example.com' RETURN NONE;
			CREATE user:f SET email = 'f@example.com' RETURN NONE;
			DEFINE INDEX test ON user FIELDS email;
			",
	)
	.await?;

	let (ns, db, table, ix) = get_table_index(&ds, "user", "test").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);

	// Simulate an owner that crashed mid-scan: wipe the index data the
	// blocking build produced (the crashed generation never reached these
	// records) and strand a `Building` generation whose durable checkpoint
	// says the scan committed through `user:c`.
	let expired = Utc::now() - chrono::Duration::seconds(BUILD_OWNER_LEASE_SECS + 5);
	let tx = ds.transaction(TransactionType::Write).await?;
	tx.del_prefix_key(&IdxRoot {
		ns,
		db,
		tb: Cow::Borrowed(&table),
		ix: ix.index_id,
	})
	.await?;
	tx.set_key(
		&ikb.new_bs_key(),
		&IndexBuildState {
			generation: 2,
			phase: IndexBuildPhase::Building,
			owner: Some(Uuid::new_v4()),
			next_ticket: 0,
			initial_complete: false,
			updated_at: expired,
			owner_heartbeat_at: Some(expired),
			error: None,
			report_status: Some(IndexBuildReportStatus::Indexing),
			initial: Some(42),
			updated: None,
			pending: None,
			initial_cursor: Some(RecordIdKey::from("c".to_string())),
		},
	)
	.await?;
	tx.commit().await?;

	let build = new_building_for_index(&ds, &session, ns, db, &table, Arc::clone(&ix)).await?;
	let acquired = build
		.acquire_build_state()
		.await?
		.expect("expired build state should be available for takeover");
	build.run_acquired(acquired).await?;

	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(state.phase, IndexBuildPhase::Online);
	// Resumed counters continue from the persisted value instead of being
	// reset by a wipe-and-rescan: 42 checkpointed + `user:d..f` scanned.
	assert_eq!(state.initial, Some(45));
	assert_eq!(state.initial_cursor, None);

	// Only the records after the checkpoint were indexed. A wipe-and-rescan
	// would have re-indexed all six records.
	assert_eq!(
		index_prefix_key_count(&ds, ns, db, &table, ix.index_id).await?,
		3,
		"only user:d..f should be indexed after a resumed scan"
	);

	Ok(())
}

/// A COUNT-index takeover resumes the baseline scan from the checkpoint: the
/// primary-appending catch-up cursor continues from the same position, so
/// records at or before the checkpoint are not baselined a second time.
#[tokio::test(flavor = "multi_thread")]
async fn takeover_resumes_count_initial_scan_from_checkpoint() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:a RETURN NONE;
			CREATE user:b RETURN NONE;
			CREATE user:c RETURN NONE;
			CREATE user:d RETURN NONE;
			CREATE user:e RETURN NONE;
			CREATE user:f RETURN NONE;
			DEFINE INDEX test ON user COUNT;
			",
	)
	.await?;

	let (ns, db, table, ix) = get_table_index(&ds, "user", "test").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);

	// Same crash simulation as above, for a COUNT index: no index data
	// survives for the stranded generation, and the checkpoint says the
	// baseline scan committed through `user:c`.
	let expired = Utc::now() - chrono::Duration::seconds(BUILD_OWNER_LEASE_SECS + 5);
	let tx = ds.transaction(TransactionType::Write).await?;
	tx.del_prefix_key(&IdxRoot {
		ns,
		db,
		tb: Cow::Borrowed(&table),
		ix: ix.index_id,
	})
	.await?;
	tx.set_key(
		&ikb.new_bs_key(),
		&IndexBuildState {
			generation: 2,
			phase: IndexBuildPhase::Building,
			owner: Some(Uuid::new_v4()),
			next_ticket: 0,
			initial_complete: false,
			updated_at: expired,
			owner_heartbeat_at: Some(expired),
			error: None,
			report_status: Some(IndexBuildReportStatus::Indexing),
			initial: Some(4),
			updated: None,
			pending: None,
			initial_cursor: Some(RecordIdKey::from("c".to_string())),
		},
	)
	.await?;
	tx.commit().await?;

	let build = new_building_for_index(&ds, &session, ns, db, &table, Arc::clone(&ix)).await?;
	let acquired = build
		.acquire_build_state()
		.await?
		.expect("expired build state should be available for takeover");
	build.run_acquired(acquired).await?;

	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(state.phase, IndexBuildPhase::Online);
	// 4 checkpointed + `user:d..f` scanned; a wipe-and-rescan would report 6.
	assert_eq!(state.initial, Some(7));
	assert_eq!(state.initial_cursor, None);
	// The index-backed count only includes the post-checkpoint baseline.
	assert_eq!(count_index_value(&ds, &session).await?, 3);

	Ok(())
}

/// A COUNT build that dies right after the tail-pass transaction commits must
/// already be durably `initial_complete`: the tail baselines `!bp` old states
/// without deleting the markers, so a takeover that re-entered the tail pass
/// would count the same records twice before publishing `Online`.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn count_tail_crash_after_commit_does_not_double_count_on_takeover() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:one RETURN NONE;
			CREATE user:two RETURN NONE;
			CREATE user:three RETURN NONE;
			CREATE user:four RETURN NONE;
			CREATE user:five RETURN NONE;
			CREATE user:six RETURN NONE;
			DEFINE INDEX test ON user COUNT;
			",
	)
	.await?;

	let (ns, db, table, ix) = get_table_index(&ds, "user", "test").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);

	// Strand a `Building` generation with an expired owner and no checkpoint,
	// wiping the data of the completed blocking build, so the takeover below
	// runs a full initial scan including the tail pass.
	let expired = Utc::now() - chrono::Duration::seconds(BUILD_OWNER_LEASE_SECS + 5);
	let tx = ds.transaction(TransactionType::Write).await?;
	tx.del_prefix_key(&IdxRoot {
		ns,
		db,
		tb: Cow::Borrowed(&table),
		ix: ix.index_id,
	})
	.await?;
	tx.set_key(
		&ikb.new_bs_key(),
		&IndexBuildState {
			generation: 2,
			phase: IndexBuildPhase::Building,
			owner: Some(Uuid::new_v4()),
			next_ticket: 0,
			initial_complete: false,
			updated_at: expired,
			owner_heartbeat_at: Some(expired),
			error: None,
			report_status: Some(IndexBuildReportStatus::Indexing),
			initial: None,
			updated: None,
			pending: None,
			initial_cursor: None,
		},
	)
	.await?;
	tx.commit().await?;

	// Queue a delete for the record that sorts last (`two`), so its `!bp` old
	// state is only reachable by the tail pass (`through = None`), not by any
	// batch-scoped catch-up committed with a checkpoint.
	execute_all_retrying_conflicts(&ds, &session, "DELETE user:two RETURN NONE").await?;

	// First takeover: run the full scan and kill the builder right after the
	// tail-pass transaction commits.
	let _release_guard = inject_non_retryable_error(
		NonRetryableErrorSite::ConcurrentIndexCountTailCommitted,
		ds.id(),
	);
	let build = new_building_for_index(&ds, &session, ns, db, &table, Arc::clone(&ix)).await?;
	let acquired = build
		.acquire_build_state()
		.await?
		.expect("expired build state should be available for takeover");
	let err = build
		.run_acquired(acquired)
		.await
		.expect_err("builder should die at the injected crash site");
	assert!(
		err.to_string().contains("injected non-retryable error"),
		"unexpected builder error: {err}"
	);

	// The tail-pass transaction committed, so the scan must already be
	// durably complete: 5 live records + the queued old state of `user:two`.
	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(state.phase, IndexBuildPhase::Building);
	assert!(state.initial_complete, "tail commit must complete the scan atomically");
	assert_eq!(state.initial, Some(6));
	assert_eq!(state.initial_cursor, None);

	// Expire the dead builder's heartbeat so a second takeover can proceed.
	let tx = ds.transaction(TransactionType::Write).await?;
	let mut state = tx
		.get_key(&ikb.new_bs_key(), None)
		.await?
		.ok_or_else(|| anyhow::anyhow!("durable build state should exist"))?;
	state.updated_at = expired;
	state.owner_heartbeat_at = Some(expired);
	tx.set_key(&ikb.new_bs_key(), &state).await?;
	tx.commit().await?;

	// Second takeover: the scan is complete, so it only replays the queued
	// delete and publishes `Online` without re-running the tail pass.
	let build = new_building_for_index(&ds, &session, ns, db, &table, Arc::clone(&ix)).await?;
	let acquired = build
		.acquire_build_state()
		.await?
		.expect("expired build state should be available for takeover");
	build.run_acquired(acquired).await?;

	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(state.phase, IndexBuildPhase::Online);
	// Baseline 6 (5 live + deleted `user:two` old state) minus the replayed
	// delete: a double-counted tail would report 6 here instead.
	assert_eq!(count_index_value(&ds, &session).await?, 5);

	Ok(())
}

/// An abort observed mid-batch must not commit that batch: the checkpoint
/// would otherwise cover records that were never indexed, and a takeover
/// would resume past them, leaving silent gaps in the index. The abort
/// timing is racy by nature, so the assertion is timing-invariant: however
/// far the durable state says the scan got, the index must contain exactly
/// the records up to that point.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn abort_mid_scan_never_checkpoints_unindexed_records() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE |user:1..=1000| SET email = 'user@example.com' RETURN NONE;
			DEFINE INDEX test ON user FIELDS email;
			",
	)
	.await?;

	let (ns, db, table, ix) = get_table_index(&ds, "user", "test").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);

	// Strand a resumable generation: no index data, checkpoint at `user:1`,
	// so the takeover scans `user:2..1000` without a cleanup phase.
	let expired = Utc::now() - chrono::Duration::seconds(BUILD_OWNER_LEASE_SECS + 5);
	let tx = ds.transaction(TransactionType::Write).await?;
	tx.del_prefix_key(&IdxRoot {
		ns,
		db,
		tb: Cow::Borrowed(&table),
		ix: ix.index_id,
	})
	.await?;
	tx.set_key(
		&ikb.new_bs_key(),
		&IndexBuildState {
			generation: 2,
			phase: IndexBuildPhase::Building,
			owner: Some(Uuid::new_v4()),
			next_ticket: 0,
			initial_complete: false,
			updated_at: expired,
			owner_heartbeat_at: Some(expired),
			error: None,
			report_status: Some(IndexBuildReportStatus::Indexing),
			initial: Some(1),
			updated: None,
			pending: None,
			initial_cursor: Some(RecordIdKey::from(1i64)),
		},
	)
	.await?;
	tx.commit().await?;

	let building =
		Arc::new(new_building_for_index(&ds, &session, ns, db, &table, Arc::clone(&ix)).await?);
	let acquired = building
		.acquire_build_state()
		.await?
		.expect("expired build state should be available for takeover");
	// Abort concurrently with the scan; any landing point must be safe.
	let aborter = Arc::clone(&building);
	tokio::spawn(async move {
		sleep(Duration::from_millis(3)).await;
		aborter.abort();
	});
	building.run_acquired(acquired).await?;

	// However far the durable state says the scan got, exactly that many
	// records must be indexed. `user:1` is covered by the seeded checkpoint
	// but carries no index key, hence the `- 1`.
	let state = durable_build_state(&ds, &ikb).await?;
	let expected = if state.initial_complete {
		999
	} else {
		match &state.initial_cursor {
			Some(RecordIdKey::Number(n)) => usize::try_from(n - 1).unwrap(),
			other => panic!("unexpected checkpoint cursor after abort: {other:?}"),
		}
	};
	assert_eq!(
		index_prefix_key_count(&ds, ns, db, &table, ix.index_id).await?,
		expected,
		"durable checkpoint must cover exactly the indexed records (state: {state:?})"
	);

	Ok(())
}

/// Direct coverage for the COUNT tail-pass abort re-check: an abort observed
/// during the tail pass must cancel the transaction rather than commit the
/// completion marker over baselines the truncated pass never wrote. The
/// abort timing is racy by nature, so the assertion is timing-invariant:
/// while the scan is durably incomplete the committed baseline (signed sum
/// of the count-delta entries) must be zero, and once the completion marker
/// exists the sum must equal the number of still-queued deletes — every
/// replayed delete decrements the sum and consumes its `!bg` entry in the
/// same transaction, so a partial tail under a completion marker breaks the
/// equality at any replay progress.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn abort_during_count_tail_pass_never_commits_partial_baselines() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE |user:1..=3000| RETURN NONE;
			DEFINE INDEX test ON user COUNT;
			",
	)
	.await?;

	let (ns, db, table, ix) = get_table_index(&ds, "user", "test").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);

	// Strand a resumable generation whose checkpoint claims `user:1..=500`
	// were scanned, with no surviving index data.
	let expired = Utc::now() - chrono::Duration::seconds(BUILD_OWNER_LEASE_SECS + 5);
	let tx = ds.transaction(TransactionType::Write).await?;
	tx.del_prefix_key(&IdxRoot {
		ns,
		db,
		tb: Cow::Borrowed(&table),
		ix: ix.index_id,
	})
	.await?;
	tx.set_key(
		&ikb.new_bs_key(),
		&IndexBuildState {
			generation: 2,
			phase: IndexBuildPhase::Building,
			owner: Some(Uuid::new_v4()),
			next_ticket: 0,
			initial_complete: false,
			updated_at: expired,
			owner_heartbeat_at: Some(expired),
			error: None,
			report_status: Some(IndexBuildReportStatus::Indexing),
			initial: Some(500),
			updated: None,
			pending: None,
			initial_cursor: Some(RecordIdKey::from(500i64)),
		},
	)
	.await?;
	tx.commit().await?;

	// Queue deletes for every record past the checkpoint while the build is
	// in `Building`. No live record remains after `user:500`, so their `!bp`
	// old states are reachable only by the tail pass (`through = None`).
	execute_all_retrying_conflicts(&ds, &session, "DELETE user:501..=3000 RETURN NONE").await?;

	let building =
		Arc::new(new_building_for_index(&ds, &session, ns, db, &table, Arc::clone(&ix)).await?);
	let acquired = building
		.acquire_build_state()
		.await?
		.expect("expired build state should be available for takeover");
	// Abort concurrently: the empty live scan reaches the tail pass almost
	// immediately, so this usually lands mid-tail; any landing point must
	// satisfy the invariant below.
	let aborter = Arc::clone(&building);
	tokio::spawn(async move {
		sleep(Duration::from_millis(4)).await;
		aborter.abort();
	});
	building.run_acquired(acquired).await?;

	// Ground truth: committed baseline = signed sum of count-delta entries;
	// outstanding replay work = queued `!bg` mutations still present.
	let tx = ds.transaction(TransactionType::Read).await?;
	let rng = IndexCountPrefix {
		ns,
		db,
		tb: Cow::Borrowed(&table),
		ix: ix.index_id,
	}
	.range()?;
	let keys = catch!(tx, tx.keys(rng, u32::MAX, 0, None).await);
	let mut sum: i64 = 0;
	for key in &keys {
		let iu = IndexCountKey::decode_key(key)?;
		let delta = i64::try_from(iu.count).expect("count delta out of range");
		sum += if iu.pos {
			delta
		} else {
			-delta
		};
	}
	let pending = catch!(tx, tx.keys(ikb.new_bg_range(2)?, u32::MAX, 0, None).await).len();
	tx.cancel().await?;

	let state = durable_build_state(&ds, &ikb).await?;
	if state.initial_complete {
		// The completion marker commits atomically with the full tail, so
		// the baseline sum tracks the unreplayed queue exactly.
		assert_eq!(
			sum,
			i64::try_from(pending).unwrap(),
			"completion marker committed over a partial tail (state: {state:?})"
		);
	} else {
		// The aborted tail pass must not have committed anything.
		assert_eq!(sum, 0, "partial tail baselines committed (state: {state:?})");
		assert_eq!(pending, 2500, "queued deletes must be untouched (state: {state:?})");
	}

	Ok(())
}

/// The periodic resume scan adopts a `CONCURRENTLY` build that a crashed owner
/// left stranded (expired lease, `Building` phase) and drives it to `Online`,
/// and is a no-op once the index is healthy again.
#[tokio::test(flavor = "multi_thread")]
async fn resume_scan_adopts_stalled_concurrent_build() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:one SET email = 'one@example.com' RETURN NONE;
			CREATE user:two SET email = 'two@example.com' RETURN NONE;
			DEFINE INDEX test ON user FIELDS email;
			",
	)
	.await?;

	let (ns, db, table, ix) = get_table_index(&ds, "user", "test").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);

	// Simulate an ungraceful crash mid-build: a `Building` generation whose owner
	// lease has expired and whose initial scan never completed.
	let expired = Utc::now() - chrono::Duration::seconds(BUILD_OWNER_LEASE_SECS + 5);
	let tx = ds.transaction(TransactionType::Write).await?;
	tx.set_key(
		&ikb.new_bs_key(),
		&IndexBuildState {
			generation: 2,
			phase: IndexBuildPhase::Building,
			owner: Some(Uuid::new_v4()),
			next_ticket: 0,
			initial_complete: false,
			updated_at: expired,
			owner_heartbeat_at: Some(expired),
			error: None,
			report_status: Some(IndexBuildReportStatus::Indexing),
			initial: Some(0),
			updated: None,
			pending: None,
			initial_cursor: None,
		},
	)
	.await?;
	tx.commit().await?;

	// The scan should adopt exactly this build.
	let resumed = ds
		.resume_stalled_index_builds(
			Duration::from_secs(30),
			tokio_util::sync::CancellationToken::new(),
		)
		.await?;
	assert_eq!(resumed, 1, "scan should adopt the stalled build");

	// The adopted build runs asynchronously; wait for it to reach `Online`.
	let deadline = Instant::now() + Duration::from_secs(30);
	loop {
		let state = durable_build_state(&ds, &ikb).await?;
		if state.phase == IndexBuildPhase::Online {
			break;
		}
		assert!(
			Instant::now() < deadline,
			"resumed build did not complete; phase={:?}",
			state.phase
		);
		sleep(Duration::from_millis(100)).await;
	}
	let building = index_building_json(&ds, &session, "user", "test").await?;
	assert_eq!(building.get("status").and_then(|status| status.as_str()), Some("ready"));

	// With the index healthy again, a second scan must be a no-op.
	let resumed_again = ds
		.resume_stalled_index_builds(
			Duration::from_secs(30),
			tokio_util::sync::CancellationToken::new(),
		)
		.await?;
	assert_eq!(resumed_again, 0, "healthy index must not be re-adopted");

	Ok(())
}

/// End-to-end check of the *periodic* path: an interval-driven loop calling
/// `resume_stalled_index_builds` (exactly what the engine's maintenance
/// scheduler runs) must, on its own, adopt a stalled `CONCURRENTLY` build and drive it to
/// `Online` — no manual `REBUILD`/`REMOVE`. This pins the behaviour in CI
/// independently of a live server harness.
#[tokio::test(flavor = "multi_thread")]
async fn periodic_task_resumes_stalled_build() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:one SET email = 'one@example.com' RETURN NONE;
			CREATE user:two SET email = 'two@example.com' RETURN NONE;
			DEFINE INDEX test ON user FIELDS email;
			",
	)
	.await?;

	let (ns, db, table, ix) = get_table_index(&ds, "user", "test").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);

	// Strand a `Building` generation with an expired owner lease, as an
	// ungraceful crash mid-build would leave it.
	let expired = Utc::now() - chrono::Duration::seconds(BUILD_OWNER_LEASE_SECS + 5);
	let tx = ds.transaction(TransactionType::Write).await?;
	tx.set_key(
		&ikb.new_bs_key(),
		&IndexBuildState {
			generation: 2,
			phase: IndexBuildPhase::Building,
			owner: Some(Uuid::new_v4()),
			next_ticket: 0,
			initial_complete: false,
			updated_at: expired,
			owner_heartbeat_at: Some(expired),
			error: None,
			report_status: Some(IndexBuildReportStatus::Indexing),
			initial: Some(0),
			updated: None,
			pending: None,
			initial_cursor: None,
		},
	)
	.await?;
	tx.commit().await?;

	// Spawn the periodic resume loop, mirroring the engine's maintenance
	// scheduler: a timer that calls `resume_stalled_index_builds` until cancelled.
	let canceller = tokio_util::sync::CancellationToken::new();
	let task = {
		let ds = Arc::clone(&ds);
		let canceller = canceller.clone();
		tokio::spawn(async move {
			let tick = Duration::from_millis(200);
			let mut interval = tokio::time::interval(tick);
			loop {
				tokio::select! {
					biased;
					_ = canceller.cancelled() => break,
					_ = interval.tick() => {
						let _ = ds.resume_stalled_index_builds(tick, canceller.clone()).await;
					}
				}
			}
		})
	};

	// The loop should adopt the stalled build and drive it to `Online` on its own.
	let deadline = Instant::now() + Duration::from_secs(30);
	loop {
		if durable_build_state(&ds, &ikb).await?.phase == IndexBuildPhase::Online {
			break;
		}
		assert!(
			Instant::now() < deadline,
			"the periodic resume task did not adopt and complete the stalled build"
		);
		sleep(Duration::from_millis(100)).await;
	}
	canceller.cancel();
	let _ = task.await;

	let building = index_building_json(&ds, &session, "user", "test").await?;
	assert_eq!(building.get("status").and_then(|status| status.as_str()), Some("ready"));

	Ok(())
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn distributed_writer_admission_does_not_extend_builder_lease() -> Result<()> {
	let (ds_a, ds_b, session) = new_distributed_index_test_ds().await?;
	execute_all(
		&ds_a,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:seed SET email = 'seed@example.com' RETURN NONE;
			DEFINE INDEX test ON user FIELDS email;
			",
	)
	.await?;

	let (ns, db, table, ix) = get_table_index(&ds_a, "user", "test").await?;
	let generation = 2;
	let expired = Utc::now() - chrono::Duration::seconds(BUILD_OWNER_LEASE_SECS + 5);
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	let stale_state = IndexBuildState {
		generation,
		phase: IndexBuildPhase::Building,
		owner: Some(uuid::Uuid::new_v4()),
		next_ticket: 0,
		initial_complete: false,
		updated_at: expired,
		owner_heartbeat_at: Some(expired),
		error: None,
		report_status: Some(IndexBuildReportStatus::Indexing),
		initial: None,
		updated: None,
		pending: None,
		initial_cursor: None,
	};
	let tx = ds_a.transaction(TransactionType::Write).await?;
	tx.set_key(&ikb.new_bs_key(), &stale_state).await?;
	tx.commit().await?;

	for record in ["one", "two", "three"] {
		execute_all(
			&ds_b,
			&session,
			&format!("CREATE user:{record} SET email = '{record}@example.com' RETURN NONE"),
		)
		.await?;
	}

	let tx = ds_a.transaction(TransactionType::Read).await?;
	let admitted_state: IndexBuildState =
		tx.get_key(&ikb.new_bs_key(), None).await?.expect("build state should exist");
	tx.cancel().await?;
	assert_eq!(admitted_state.next_ticket, 3);
	assert_eq!(admitted_state.owner_heartbeat_at, Some(expired));
	assert!(
		admitted_state.updated_at > expired,
		"writer admission should update durable state metadata"
	);
	assert!(
		build_owner_expired(&admitted_state, Utc::now()),
		"writer admission must not refresh builder lease"
	);

	let tx = ds_a.transaction(TransactionType::Read).await?;
	let table_def = tx.get_tb(ns, db, &table, None).await?.expect("table should exist");
	let mut ctx = ds_a.setup_ctx()?;
	let tx = Arc::new(tx);
	ctx.set_transaction(Arc::clone(&tx));
	let ctx = ctx.freeze();
	let build = Building::new(
		&ctx,
		ds_a.transaction_factory().clone(),
		ds_a.setup_options(&session),
		table_def.table_id,
		Arc::clone(&ix),
		Arc::new(IndexKey::new(ns, db, &table, ix.index_id)),
	)?;
	let acquired = build
		.acquire_build_state()
		.await?
		.expect("expired builder lease should be available for takeover");
	tx.cancel().await?;

	assert_eq!(acquired.generation, generation);
	assert_eq!(acquired.phase, IndexBuildPhase::Building);
	assert!(!acquired.initial_complete);

	let tx = ds_a.transaction(TransactionType::Read).await?;
	let taken_over: IndexBuildState =
		tx.get_key(&ikb.new_bs_key(), None).await?.expect("build state should exist");
	tx.cancel().await?;
	assert_eq!(taken_over.owner, Some(build.owner));
	assert!(taken_over.owner_heartbeat_at.is_some());
	assert!(!build_owner_expired(&taken_over, Utc::now()));
	Ok(())
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn writer_admission_batches_reservations_per_user_transaction() -> Result<()> {
	// A single user transaction that performs many indexed mutations against
	// the same index must allocate exactly one durable `!br` reservation —
	// one ticket per (user-txn, index) — and write a distinct `!bg` entry
	// per mutation. The earlier protocol allocated one `!br` per mutation,
	// which the per-user-txn reservation cache eliminates.
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:seed SET email = 'seed@example.com' RETURN NONE;
			DEFINE INDEX test ON user FIELDS email;
			",
	)
	.await?;

	let (ns, db, table, ix) = get_table_index(&ds, "user", "test").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	let generation = 2;
	let building = IndexBuildState {
		generation,
		phase: IndexBuildPhase::Building,
		owner: Some(ds.id()),
		next_ticket: 0,
		initial_complete: false,
		updated_at: Utc::now(),
		owner_heartbeat_at: Some(Utc::now()),
		error: None,
		report_status: Some(IndexBuildReportStatus::Indexing),
		initial: None,
		updated: None,
		pending: None,
		initial_cursor: None,
	};
	let tx = ds.transaction(TransactionType::Write).await?;
	tx.set_key(&ikb.new_bs_key(), &building).await?;
	// Give the generation its ticket counter, so admission takes the same path
	// production does. Without it the fabricated state looks like a generation
	// predating the counter and the whole test runs on the legacy fallback.
	tx.set_key(&ikb.new_bt_key(generation), &0u64).await?;
	tx.commit().await?;

	// Single user transaction that inserts five records — all go through
	// admission. The reservation cache should make the second through fifth
	// inserts skip the !br write and reuse the cached ticket.
	execute_all(
		&ds,
		&session,
		"
			BEGIN;
			CREATE user:one SET email = 'one@example.com' RETURN NONE;
			CREATE user:two SET email = 'two@example.com' RETURN NONE;
			CREATE user:three SET email = 'three@example.com' RETURN NONE;
			CREATE user:four SET email = 'four@example.com' RETURN NONE;
			CREATE user:five SET email = 'five@example.com' RETURN NONE;
			COMMIT;
			",
	)
	.await?;

	// Exactly one ticket should have been allocated by the whole user
	// transaction's batch.
	assert_eq!(
		durable_ticket_counter(&ds, &ikb, generation).await?,
		Some(1),
		"a single user transaction must consume exactly one durable ticket regardless of mutation count"
	);

	// Five `!bg` entries, one per mutation, all sharing the same `(generation, ticket)`.
	let tx = ds.transaction(TransactionType::Read).await?;
	let bg_keys = tx.keys(ikb.new_bg_range(generation)?, u32::MAX, 0, None).await?;
	let bp_keys = tx.keys(ikb.new_bp_range(generation)?, u32::MAX, 0, None).await?;
	let br_keys = tx.keys(ikb.new_br_range(generation)?, u32::MAX, 0, None).await?;
	tx.cancel().await?;
	assert_eq!(
		bg_keys.len(),
		5,
		"each indexed mutation should produce one `!bg` entry; got {} for 5 mutations",
		bg_keys.len()
	);
	assert_eq!(
		bp_keys.len(),
		5,
		"first-time-per-record admission during initial scan should produce one `!bp` per record; got {} for 5 records",
		bp_keys.len()
	);
	assert!(
		br_keys.is_empty(),
		"the user transaction's close-time release should have removed the `!br`; found {} stranded reservation keys",
		br_keys.len()
	);
	Ok(())
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn writer_admission_cancelled_batch_clears_durable_queue() -> Result<()> {
	// If a user transaction's batched mutations roll back, no `!bg` may
	// survive and the single `!br` allocated for the batch must be released
	// from the close path.
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			DEFINE INDEX test ON user FIELDS email;
			",
	)
	.await?;

	let (ns, db, table, ix) = get_table_index(&ds, "user", "test").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	let generation = 2;
	let building = IndexBuildState {
		generation,
		phase: IndexBuildPhase::Building,
		owner: Some(ds.id()),
		next_ticket: 0,
		initial_complete: false,
		updated_at: Utc::now(),
		owner_heartbeat_at: Some(Utc::now()),
		error: None,
		report_status: Some(IndexBuildReportStatus::Indexing),
		initial: None,
		updated: None,
		pending: None,
		initial_cursor: None,
	};
	let tx = ds.transaction(TransactionType::Write).await?;
	tx.set_key(&ikb.new_bs_key(), &building).await?;
	// Give the generation its ticket counter, so admission takes the same path
	// production does rather than the legacy `next_ticket` fallback.
	tx.set_key(&ikb.new_bt_key(generation), &0u64).await?;
	tx.commit().await?;

	// Single user transaction that issues three indexed mutations then
	// cancels. The reservation is still allocated (the !br commit is in its
	// own short transaction), but the cancel path must release it.
	execute_cancelled_transaction(
		&ds,
		&session,
		"
			BEGIN;
			CREATE user:one SET email = 'one@example.com' RETURN NONE;
			CREATE user:two SET email = 'two@example.com' RETURN NONE;
			CREATE user:three SET email = 'three@example.com' RETURN NONE;
			CANCEL;
			",
	)
	.await?;

	let tx = ds.transaction(TransactionType::Read).await?;
	let bg_keys = tx.keys(ikb.new_bg_range(generation)?, u32::MAX, 0, None).await?;
	let bp_keys = tx.keys(ikb.new_bp_range(generation)?, u32::MAX, 0, None).await?;
	let br_keys = tx.keys(ikb.new_br_range(generation)?, u32::MAX, 0, None).await?;
	tx.cancel().await?;
	assert_eq!(
		durable_ticket_counter(&ds, &ikb, generation).await?,
		Some(1),
		"cancelled batch still consumes one durable ticket"
	);
	assert!(
		bg_keys.is_empty(),
		"cancelled user transaction must not leave any `!bg` entries; found {}",
		bg_keys.len()
	);
	assert!(
		bp_keys.is_empty(),
		"cancelled user transaction must not leave any `!bp` entries; found {}",
		bp_keys.len()
	);
	assert!(
		br_keys.is_empty(),
		"cancel path must release the durable reservation; found {} stranded `!br` keys",
		br_keys.len()
	);
	Ok(())
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn cached_index_build_reservation_remove_clears_entry() -> Result<()> {
	// `consume()` drops the cached reservation after the first-use fence
	// returns `IndexNormally`, so subsequent mutations re-enter reservation
	// and rediscover the online state instead of writing orphan `!bg` against
	// a released ticket. Verify the underlying mechanic: insert, lookup hits
	// with an incrementing `mutation_seq`, remove, lookup misses.
	let (ds, _) = new_index_test_ds().await?;
	let tx = ds.transaction(TransactionType::Write).await?;
	let key = CachedIndexBuildReservationKey {
		ns: NamespaceId(1),
		db: DatabaseId(1),
		tb: TableName::from("user"),
		ix: IndexId(1),
	};

	assert!(
		tx.lookup_cached_index_build_reservation(&key).await?.is_none(),
		"empty cache should miss"
	);

	let first = tx.insert_cached_index_build_reservation(key.clone(), 1, 7, false).await;
	match first {
		CachedIndexBuildReservationLookup::FirstUse {
			generation,
			ticket,
			mutation_seq,
			initial_complete,
		} => {
			assert_eq!(generation, 1);
			assert_eq!(ticket, 7);
			assert_eq!(mutation_seq, 0);
			assert!(!initial_complete);
		}
		CachedIndexBuildReservationLookup::Reused {
			..
		} => panic!("first admission must return FirstUse, not Reused"),
	}

	let reused = tx
		.lookup_cached_index_build_reservation(&key)
		.await?
		.expect("cache should hit after insert");
	match reused {
		CachedIndexBuildReservationLookup::Reused {
			generation,
			ticket,
			mutation_seq,
			initial_complete,
		} => {
			assert_eq!(generation, 1);
			assert_eq!(ticket, 7);
			assert_eq!(mutation_seq, 1, "second mutation should consume seq 1");
			assert!(!initial_complete);
		}
		CachedIndexBuildReservationLookup::FirstUse {
			..
		} => panic!("subsequent admission must return Reused, not FirstUse"),
	}

	tx.remove_cached_index_build_reservation(&key).await;
	assert!(
		tx.lookup_cached_index_build_reservation(&key).await?.is_none(),
		"cache should miss after removal so subsequent mutations re-enter reservation"
	);

	tx.cancel().await?;
	Ok(())
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn cached_index_build_reservation_lookup_errors_on_seq_overflow() -> Result<()> {
	// Saturating-add would silently overwrite `!bg(gen, ticket, u32::MAX)`
	// after `mutation_seq` clamps; the cache must instead surface an error
	// so the user transaction is aborted before any data loss occurs.
	let (ds, _) = new_index_test_ds().await?;
	let tx = ds.transaction(TransactionType::Write).await?;
	let key = CachedIndexBuildReservationKey {
		ns: NamespaceId(1),
		db: DatabaseId(1),
		tb: TableName::from("user"),
		ix: IndexId(1),
	};

	tx.seed_cached_index_build_reservation_for_test(key.clone(), 1, 0, false, u32::MAX).await;
	let err = tx
		.lookup_cached_index_build_reservation(&key)
		.await
		.expect_err("lookup at u32::MAX must surface an overflow error");
	let downcast = err
		.downcast_ref::<DatastoreError>()
		.expect("error should be the typed IndexingBuildingCancelled");
	assert!(
		matches!(downcast, DatastoreError::IndexingBuildingCancelled { .. }),
		"expected IndexingBuildingCancelled, got {downcast:?}"
	);
	assert!(
		err.to_string().contains("mutation sequence overflowed"),
		"unexpected error message: {err}"
	);

	tx.cancel().await?;
	Ok(())
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn recheck_cached_admission_rejects_mid_transaction_state_changes() -> Result<()> {
	// Cache reuse must revalidate the live `!bs` state every time. A
	// generation rotation, an Online transition, or vanished build state
	// must abort the user transaction with `IndexingBuildingCancelled` —
	// otherwise later mutations write `!bg` against a generation no builder
	// will replay. An Error transition keeps queueing instead: the errored
	// generation's queue is wiped and the table rescanned on `REBUILD`.
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
				DEFINE TABLE user SCHEMALESS;
				DEFINE INDEX test ON user FIELDS email;
				",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "user", "test").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);

	// Matching state — recheck succeeds.
	seed_build_state(&ds, &ikb, IndexBuildPhase::Building, 1).await?;
	let outcome = run_recheck_cached_admission(&ds, ns, db, &ikb, ix.as_ref(), 1)
		.await?
		.expect("recheck on matching Building state should succeed");
	assert!(matches!(outcome, CachedAdmission::Valid));

	// Closing is still queueable.
	seed_build_state(&ds, &ikb, IndexBuildPhase::Closing, 1).await?;
	let outcome = run_recheck_cached_admission(&ds, ns, db, &ikb, ix.as_ref(), 1)
		.await?
		.expect("recheck on matching Closing state should succeed");
	assert!(matches!(outcome, CachedAdmission::Valid));

	// Generation rotation — recheck aborts.
	seed_build_state(&ds, &ikb, IndexBuildPhase::Building, 2).await?;
	let err = run_recheck_cached_admission(&ds, ns, db, &ikb, ix.as_ref(), 1)
		.await?
		.expect_err("generation mismatch must abort cached admission");
	assert!(err.to_string().contains("generation"), "unexpected error: {err}");

	// Online phase — cached writers cannot trust the cached ticket.
	seed_build_state(&ds, &ikb, IndexBuildPhase::Online, 1).await?;
	let err = run_recheck_cached_admission(&ds, ns, db, &ikb, ix.as_ref(), 1)
		.await?
		.expect_err("online phase must abort cached admission");
	assert!(err.to_string().contains("online"), "unexpected error: {err}");

	// Error phase — keeps queueing: a failed build must not abort user
	// writes. The queued mutations die with the errored generation's wipe
	// and the records are rescanned by the recovering `REBUILD INDEX`.
	seed_build_state(&ds, &ikb, IndexBuildPhase::Error, 1).await?;
	let outcome = run_recheck_cached_admission(&ds, ns, db, &ikb, ix.as_ref(), 1)
		.await?
		.expect("recheck on matching Error state should keep queueing");
	assert!(matches!(outcome, CachedAdmission::Valid));

	// Missing state under a definition the catalog still carries — recheck
	// aborts: a build vanished under a live index, which this cannot repair.
	let tx = ds.transaction(TransactionType::Write).await?;
	tx.del_key(&ikb.new_bs_key()).await?;
	tx.commit().await?;
	let err = run_recheck_cached_admission(&ds, ns, db, &ikb, ix.as_ref(), 1)
		.await?
		.expect_err("missing build state must abort cached admission");
	assert!(err.to_string().contains("no longer exists"), "unexpected error: {err}");

	Ok(())
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn recheck_cached_admission_skips_a_retired_index_instead_of_failing_the_write() -> Result<()>
{
	// A bulk write admits once and reuses the reservation for every later
	// record. If `REMOVE INDEX` commits in between, the recheck finds no `!bs`
	// — the same observation as a build that vanished under a live definition,
	// but the opposite conclusion: there is no index left to maintain, so the
	// write skips it rather than failing. First-use admission has always split
	// on the catalog this way; the cached path must agree.
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
				DEFINE TABLE user SCHEMALESS;
				DEFINE INDEX test ON user FIELDS email;
				",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "user", "test").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	seed_build_state(&ds, &ikb, IndexBuildPhase::Building, 1).await?;

	// The definition a mid-transaction write still holds is read before the
	// removal, which is exactly the stale handle the recheck has to judge.
	let cached_ix = Arc::clone(&ix);
	execute_all(&ds, &session, "REMOVE INDEX test ON user").await?;

	let outcome = run_recheck_cached_admission(&ds, ns, db, &ikb, cached_ix.as_ref(), 1)
		.await?
		.expect("a retired index must not fail the write that was mid-transaction");
	assert!(
		matches!(outcome, CachedAdmission::Retired),
		"the recheck must report the index retired so the mutation skips it"
	);
	Ok(())
}

#[cfg(feature = "kv-mem")]
async fn seed_build_state(
	ds: &Datastore,
	ikb: &IndexKeyBase,
	phase: IndexBuildPhase,
	generation: u64,
) -> Result<()> {
	let mut state = IndexBuildState {
		generation,
		phase,
		owner: Some(ds.id()),
		next_ticket: 0,
		initial_complete: false,
		updated_at: Utc::now(),
		owner_heartbeat_at: Some(Utc::now()),
		error: None,
		report_status: Some(report_status_from_phase(phase)),
		initial: None,
		updated: None,
		pending: None,
		initial_cursor: None,
	};
	if matches!(phase, IndexBuildPhase::Error) {
		state.error = Some("seeded test failure".to_string());
		state.report_status = Some(IndexBuildReportStatus::Error);
	}
	let tx = ds.transaction(TransactionType::Write).await?;
	tx.set_key(&ikb.new_bs_key(), &state).await?;
	tx.commit().await?;
	Ok(())
}

/// The retirement the recheck has to notice is one that commits *during* the
/// write, so the write's own transaction cannot be what reports it.
///
/// A bulk write reads the index definition once and reuses it, which leaves
/// that definition in its transaction cache. A catalog read issued through
/// that transaction afterwards is answered from the cache and still sees the
/// index, whatever the backend's snapshot rules — so the recheck has to ask the
/// same fresh transaction that observed `!bs` missing.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn recheck_cached_admission_sees_a_retirement_the_user_transaction_cannot() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
				DEFINE TABLE user SCHEMALESS;
				DEFINE INDEX test ON user FIELDS email;
				",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "user", "test").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	seed_build_state(&ds, &ikb, IndexBuildPhase::Building, 1).await?;

	// The write's transaction, opened and its catalog cache warmed before the
	// removal — exactly what record 1 of a bulk write leaves behind.
	let tx = Arc::new(ds.transaction(TransactionType::Read).await?);
	assert!(
		tx.get_tb_index(ns, db, &table, ix.name.as_str(), None).await?.is_some(),
		"the write's transaction must have the definition cached for this to mean anything"
	);

	execute_all(&ds, &session, "REMOVE INDEX test ON user").await?;

	let mut ctx = ds.setup_ctx()?;
	ctx.set_transaction(Arc::clone(&tx));
	let frozen = ctx.freeze();
	let builder = frozen
		.get_index_builder()
		.expect("index builder should be available on the configured Datastore")
		.clone();
	let outcome = builder.recheck_cached_admission(&frozen, &ikb, ix.as_ref(), ns, db, 1).await;
	tx.cancel().await?;

	assert!(
		matches!(outcome?, CachedAdmission::Retired),
		"a retirement that commits mid-write must still be reported as retired"
	);
	Ok(())
}

/// A replacement is only a retirement for a write that can already see it.
///
/// `DEFINE INDEX OVERWRITE` retires the old id and defines a new one under the
/// same name. A write whose own view still carries the replaced definition
/// cannot reach the new index — it is not in that write's snapshot — so
/// skipping the mutation would let the replacement publish without this
/// record. Failing is what sends the write back to retry against the
/// definition that now exists.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn recheck_cached_admission_rejects_a_replacement_the_write_cannot_see() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
				DEFINE TABLE user SCHEMALESS;
				CREATE user:one SET email = 'one@example.com', account = 'apple' RETURN NONE;
				DEFINE INDEX test ON user FIELDS email;
				",
	)
	.await?;
	let (ns, db, table, old_ix) = get_table_index(&ds, "user", "test").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), old_ix.index_id);
	seed_build_state(&ds, &ikb, IndexBuildPhase::Building, 1).await?;

	// The write's transaction, opened and its catalog cache warmed before the
	// replacement commits, so its view still carries the old definition.
	let tx = Arc::new(ds.transaction(TransactionType::Read).await?);
	assert!(
		tx.get_tb_index(ns, db, &table, old_ix.name.as_str(), None).await?.is_some(),
		"the write's transaction must have the old definition cached"
	);

	execute_all(&ds, &session, "DEFINE INDEX OVERWRITE test ON user FIELDS account").await?;
	let (_, _, _, new_ix) = get_table_index(&ds, "user", "test").await?;
	assert_ne!(new_ix.index_id, old_ix.index_id, "the overwrite must allocate a fresh id");

	let mut ctx = ds.setup_ctx()?;
	ctx.set_transaction(Arc::clone(&tx));
	let frozen = ctx.freeze();
	let builder = frozen
		.get_index_builder()
		.expect("index builder should be available on the configured Datastore")
		.clone();
	let outcome = builder.recheck_cached_admission(&frozen, &ikb, old_ix.as_ref(), ns, db, 1).await;
	tx.cancel().await?;

	let err = outcome.expect_err("a replacement the write cannot see must abort it");
	assert!(err.to_string().contains("replaced"), "unexpected error: {err}");
	Ok(())
}

#[cfg(feature = "kv-mem")]
async fn run_recheck_cached_admission(
	ds: &Datastore,
	ns: NamespaceId,
	db: DatabaseId,
	ikb: &IndexKeyBase,
	ix: &IndexDefinition,
	cached_generation: u64,
) -> Result<Result<CachedAdmission>> {
	let tx = ds.transaction(TransactionType::Read).await?;
	let mut ctx = ds.setup_ctx()?;
	let tx = Arc::new(tx);
	ctx.set_transaction(Arc::clone(&tx));
	let frozen = ctx.freeze();
	let builder = frozen
		.get_index_builder()
		.expect("index builder should be available on the configured Datastore")
		.clone();
	let result =
		builder.recheck_cached_admission(&frozen, ikb, ix, ns, db, cached_generation).await;
	tx.cancel().await?;
	Ok(result)
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn acquire_build_state_waits_for_prior_generation_reservations() -> Result<()> {
	// A new-generation takeover on one node must drain in-flight `!br` from
	// writers on *other* nodes before wiping the stale queues. Without the
	// drain, the wipe destroys the writer's anchor and the new build's
	// initial scan can start before the writer's commit, missing main-table
	// writes. (The takeover installs the new generation before draining, so
	// no further old-generation reservations can appear while it waits.)
	let (ds_a, ds_b, session) = new_distributed_index_test_ds().await?;
	execute_all(
		&ds_a,
		&session,
		"
				DEFINE TABLE user SCHEMALESS;
				DEFINE INDEX test ON user FIELDS email;
				",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds_a, "user", "test").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);

	// Seed `!bs` in `Error` so takeover takes the new-generation branch.
	let errored = IndexBuildState {
		generation: 1,
		phase: IndexBuildPhase::Error,
		owner: Some(ds_a.id()),
		next_ticket: 1,
		initial_complete: false,
		updated_at: Utc::now(),
		owner_heartbeat_at: Some(Utc::now()),
		error: Some("seeded test failure".to_string()),
		report_status: Some(IndexBuildReportStatus::Error),
		initial: None,
		updated: None,
		pending: None,
		initial_cursor: None,
	};
	let seed_tx = ds_a.transaction(TransactionType::Write).await?;
	seed_tx.set_key(&ikb.new_bs_key(), &errored).await?;
	let reservation = IndexBuildReservation {
		node: ds_a.id(),
		expires_at: Utc::now() + chrono::Duration::seconds(BUILD_RESERVATION_TTL_SECS),
	};
	let br_key = ikb.new_br_key(1, 0);
	seed_tx.set_key(&br_key, &reservation).await?;
	seed_tx.commit().await?;

	// `ds_b` is the would-be takeover node. Its drain must block on ds_a's
	// live `!br`.
	let building = new_building_for_index(&ds_b, &session, ns, db, &table, Arc::clone(&ix)).await?;
	let timeout_result =
		timeout(Duration::from_millis(200), building.wait_for_prior_generation_reservations(2))
			.await;
	assert!(
		timeout_result.is_err(),
		"drain must block while another node's !br is alive in durable membership"
	);

	// Simulate ds_a's deferred release firing (e.g. user transaction committed
	// or cancelled, removing its `!br`).
	let release_tx = ds_a.transaction(TransactionType::Write).await?;
	release_tx.del_key(&br_key).await?;
	release_tx.commit().await?;

	// Drain should now return promptly.
	timeout(Duration::from_secs(5), building.wait_for_prior_generation_reservations(2))
		.await
		.expect("drain must complete after !br is removed")?;

	// Takeover proceeds with generation 2 and leaves no stranded reservations.
	let acquired =
		building.acquire_build_state().await?.expect("takeover should succeed after drain");
	assert_eq!(acquired.generation, 2);
	assert!(matches!(acquired.phase, IndexBuildPhase::Building));
	let tx = ds_a.transaction(TransactionType::Read).await?;
	let br_keys = tx.keys(ikb.new_br_all_generations_range()?, u32::MAX, 0, None).await?;
	tx.cancel().await?;
	assert!(br_keys.is_empty(), "no `!br` should remain after a clean takeover");
	Ok(())
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn drain_prior_generation_reservations_cleans_dead_writers() -> Result<()> {
	// Stale `!br` from a writer whose node is no longer in durable membership
	// must be cleaned up by the drain. Otherwise a single crashed writer
	// would block every new-generation takeover until its TTL expired and
	// some other mechanism removed the entry.
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
				DEFINE TABLE user SCHEMALESS;
				DEFINE INDEX test ON user FIELDS email;
				",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "user", "test").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);

	// Reservation owned by a node id that was never registered in durable
	// membership (`reservation_node_is_live` returns false) and whose TTL is
	// already past.
	let stale_node = Uuid::new_v4();
	let reservation = IndexBuildReservation {
		node: stale_node,
		expires_at: Utc::now() - chrono::Duration::seconds(1),
	};
	let br_key = ikb.new_br_key(1, 0);
	let seed_tx = ds.transaction(TransactionType::Write).await?;
	seed_tx.set_key(&br_key, &reservation).await?;
	seed_tx.commit().await?;

	let building = new_building_for_index(&ds, &session, ns, db, &table, Arc::clone(&ix)).await?;
	timeout(Duration::from_secs(5), building.wait_for_prior_generation_reservations(2))
		.await
		.expect("drain must complete promptly for a dead writer's reservation")?;

	let tx = ds.transaction(TransactionType::Read).await?;
	let br_keys = tx.keys(ikb.new_br_all_generations_range()?, u32::MAX, 0, None).await?;
	tx.cancel().await?;
	assert!(br_keys.is_empty(), "stale `!br` from a dead writer should have been removed");
	Ok(())
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn distributed_info_uses_durable_state_from_second_node() -> Result<()> {
	let (ds_a, ds_b, session) = new_distributed_index_test_ds().await?;
	execute_all(
		&ds_a,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:one SET email = 'one@example.com' RETURN NONE;
			CREATE user:two SET email = 'two@example.com' RETURN NONE;
			",
	)
	.await?;

	let guard = start_index_build_paused(
		&ds_a,
		&session,
		"DEFINE INDEX test ON user FIELDS email CONCURRENTLY",
	)
	.await?;
	assert_eq!(index_building_status(&ds_b, &session, "user", "test").await?, "cleaning");

	drop(guard);
	wait_for_index_ready(&ds_a, &session, "user", "test").await?;
	assert_eq!(index_building_status(&ds_b, &session, "user", "test").await?, "ready");
	Ok(())
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn distributed_blocking_rebuild_waits_for_remote_owner() -> Result<()> {
	let (ds_a, ds_b, session) = new_distributed_index_test_ds().await?;
	execute_all(
		&ds_a,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:one SET email = 'one@example.com' RETURN NONE;
			CREATE user:two SET email = 'two@example.com' RETURN NONE;
			",
	)
	.await?;

	let guard = start_index_build_paused(
		&ds_a,
		&session,
		"DEFINE INDEX test ON user FIELDS email CONCURRENTLY",
	)
	.await?;
	let session_rebuild = session.clone();
	let rebuild = tokio::spawn(async move {
		execute_all(&ds_b, &session_rebuild, "REBUILD INDEX test ON user").await
	});

	sleep(Duration::from_millis(200)).await;
	assert!(
		!rebuild.is_finished(),
		"blocking REBUILD INDEX returned while the remote build was still paused"
	);

	drop(guard);
	timeout(Duration::from_secs(10), rebuild)
		.await
		.map_err(|_| anyhow::anyhow!("timed out waiting for blocking rebuild"))???;
	assert_eq!(index_building_status(&ds_a, &session, "user", "test").await?, "ready");
	Ok(())
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn distributed_blocking_rebuild_takes_over_expired_remote_owner() -> Result<()> {
	let (ds_a, ds_b, session) = new_distributed_index_test_ds().await?;
	execute_all(
		&ds_a,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:one SET email = 'one@example.com' RETURN NONE;
			DEFINE INDEX test ON user FIELDS email;
			",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds_a, "user", "test").await?;
	let ikb = IndexKeyBase::new(ns, db, table, ix.index_id);
	let generation = 2;
	let now = Utc::now();
	let tx = ds_a.transaction(TransactionType::Write).await?;
	tx.set_key(
		&ikb.new_bs_key(),
		&IndexBuildState {
			generation,
			phase: IndexBuildPhase::Building,
			owner: Some(uuid::Uuid::new_v4()),
			next_ticket: 0,
			initial_complete: true,
			updated_at: now,
			owner_heartbeat_at: Some(now),
			error: None,
			report_status: Some(IndexBuildReportStatus::Indexing),
			initial: Some(1),
			updated: Some(0),
			pending: Some(0),
			initial_cursor: None,
		},
	)
	.await?;
	tx.commit().await?;

	let session_rebuild = session.clone();
	let rebuild = tokio::spawn(async move {
		execute_all(&ds_b, &session_rebuild, "REBUILD INDEX test ON user").await
	});
	sleep(Duration::from_millis(200)).await;
	assert!(
		!rebuild.is_finished(),
		"blocking REBUILD INDEX returned before the remote lease expired"
	);

	let expired = Utc::now() - chrono::Duration::seconds(BUILD_OWNER_LEASE_SECS + 5);
	let tx = ds_a.transaction(TransactionType::Write).await?;
	let mut state: IndexBuildState =
		tx.get_key(&ikb.new_bs_key(), None).await?.expect("build state should exist");
	state.updated_at = expired;
	state.owner_heartbeat_at = Some(expired);
	tx.set_key(&ikb.new_bs_key(), &state).await?;
	tx.commit().await?;

	timeout(Duration::from_secs(10), rebuild)
		.await
		.map_err(|_| anyhow::anyhow!("timed out waiting for takeover rebuild"))???;
	let state = durable_build_state(&ds_a, &ikb).await?;
	assert_eq!(state.phase, IndexBuildPhase::Online);
	assert_eq!(state.generation, generation);
	assert_eq!(state.owner, None);
	Ok(())
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn distributed_info_reports_durable_error_from_second_node() -> Result<()> {
	let (ds_a, ds_b, session) = new_distributed_index_test_ds().await?;
	execute_all(
		&ds_a,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:one SET account = 'apple', email = 'test@surrealdb.com' RETURN NONE;
			CREATE user:two SET account = 'apple', email = 'test@surrealdb.com' RETURN NONE;
			",
	)
	.await?;
	execute_all(
		&ds_a,
		&session,
		"DEFINE INDEX test ON user FIELDS account, email UNIQUE CONCURRENTLY",
	)
	.await?;

	let building = timeout(Duration::from_secs(10), async {
		loop {
			let building = index_building_json(&ds_b, &session, "user", "test").await?;
			if building.get("status").and_then(|status| status.as_str()) == Some("error") {
				return Ok::<_, anyhow::Error>(building);
			}
			sleep(Duration::from_millis(20)).await;
		}
	})
	.await
	.map_err(|_| anyhow::anyhow!("timed out waiting for durable index build error"))??;

	assert_eq!(building.get("status").and_then(|status| status.as_str()), Some("error"));
	let error_reason = building
		.get("error")
		.and_then(|error| error.as_str())
		.ok_or_else(|| anyhow::anyhow!("index info did not include building.error: {building}"))?;
	assert!(
		error_reason.contains("already contains"),
		"unexpected durable index build error: {building}"
	);
	// A failed build must not block user writes on any node: the mutation is
	// admitted (queued under the errored generation, to be wiped and
	// rescanned by a recovering `REBUILD INDEX`) and the statement succeeds.
	execute_all_retrying_conflicts(
		&ds_b,
		&session,
		"CREATE user:three SET account = 'tesla', email = 'three@surrealdb.com' RETURN NONE",
	)
	.await?;
	Ok(())
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn distributed_standard_index_replays_second_node_update() -> Result<()> {
	// Node A builds the index while node B updates the indexed field. Once
	// the build closes, only the new value should be present in the index.
	let (ds_a, ds_b, session) = new_distributed_index_test_ds().await?;
	execute_all(
		&ds_a,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:one SET email = 'old@example.com' RETURN NONE;
			CREATE user:two SET email = 'steady@example.com' RETURN NONE;
			",
	)
	.await?;

	let guard = start_index_build_paused(
		&ds_a,
		&session,
		"DEFINE INDEX test ON user FIELDS email CONCURRENTLY",
	)
	.await?;
	execute_all_retrying_conflicts(
		&ds_b,
		&session,
		"UPDATE user:one SET email = 'new@example.com' RETURN NONE",
	)
	.await?;
	execute_all_retrying_conflicts(&ds_b, &session, "DELETE user:two RETURN NONE").await?;
	drop(guard);
	wait_for_index_ready(&ds_a, &session, "user", "test").await?;
	let building = index_building_json(&ds_a, &session, "user", "test").await?;
	assert_eq!(building.get("pending").and_then(|pending| pending.as_u64()), Some(0));
	let updated =
		building.get("updated").and_then(|updated| updated.as_u64()).ok_or_else(|| {
			anyhow::anyhow!("index info did not include building.updated: {building}")
		})?;
	assert!(
		updated >= 2,
		"expected queued update and delete to be replayed in index status: {building}"
	);

	assert_eq!(
		query_array_len(
			&ds_a,
			&session,
			"SELECT id FROM user WITH INDEX test WHERE email = 'new@example.com'"
		)
		.await?,
		1
	);
	assert_eq!(
		query_array_len(
			&ds_a,
			&session,
			"SELECT id FROM user WITH INDEX test WHERE email = 'old@example.com'"
		)
		.await?,
		0
	);
	assert_eq!(
		query_array_len(
			&ds_a,
			&session,
			"SELECT id FROM user WITH INDEX test WHERE email = 'steady@example.com'"
		)
		.await?,
		0
	);
	Ok(())
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn distributed_local_reservation_release_retries_conflict() -> Result<()> {
	let (ds_a, ds_b, session) = new_distributed_index_test_ds().await?;
	execute_all(
		&ds_a,
		&session,
		"
				DEFINE TABLE user SCHEMALESS;
				CREATE user:one SET email = 'old@example.com' RETURN NONE;
				",
	)
	.await?;

	let build_guard = start_index_build_paused(
		&ds_a,
		&session,
		"DEFINE INDEX test ON user FIELDS email CONCURRENTLY",
	)
	.await?;
	let release_site = RetryableConflictSite::ConcurrentIndexReservationRelease;
	let writer_node = ds_b.id();
	let _release_guard = inject_retryable_conflict(release_site, writer_node);
	execute_all_retrying_conflicts(
		&ds_b,
		&session,
		"UPDATE user:one SET email = 'new@example.com' RETURN NONE",
	)
	.await?;
	assert_eq!(retryable_conflict_count(release_site, writer_node), 0);

	drop(build_guard);
	wait_for_index_ready(&ds_a, &session, "user", "test").await?;
	expect_indexed_query_len(
		&ds_a,
		&session,
		"SELECT id FROM user WITH INDEX test WHERE email = 'new@example.com'",
		1,
	)
	.await?;
	Ok(())
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn commit_failure_preserves_primary_error_when_reservation_cleanup_fails() -> Result<()> {
	let (ds, _) = new_index_test_ds().await?;
	let ikb = IndexKeyBase::new(NamespaceId(1), DatabaseId(1), TableName::from("user"), IndexId(1));
	let generation = 1;
	let ticket = 1;
	let reservation = IndexBuildReservation {
		node: ds.id(),
		expires_at: Utc::now() + chrono::Duration::seconds(BUILD_RESERVATION_TTL_SECS),
	};
	let br = ikb.new_br_key(generation, ticket);
	let br_key = br.encode_key()?;
	let br_val = reservation.kv_encode_value()?;

	let seed_tx = ds.transaction(TransactionType::Write).await?;
	seed_tx.set_key(&br, &reservation).await?;
	(*seed_tx).set("commit-conflict".as_bytes().into(), b"initial".as_slice()).await?;
	seed_tx.commit().await?;

	let user_tx = ds.transaction(TransactionType::Write).await?;
	(*user_tx).set("commit-conflict".as_bytes().into(), b"user-write".as_slice()).await?;
	user_tx
		.register_index_build_reservation_release(IndexBuildReservationRelease::new(
			ds.transaction_factory().clone(),
			ds.sequences().clone(),
			ds.id(),
			br_key,
			br_val,
		))
		.await;

	let conflicting_tx = ds.transaction(TransactionType::Write).await?;
	(*conflicting_tx)
		.set("commit-conflict".as_bytes().into(), b"conflicting-write".as_slice())
		.await?;
	conflicting_tx.commit().await?;

	let _release_guard = inject_non_retryable_error(
		NonRetryableErrorSite::ConcurrentIndexReservationRelease,
		ds.id(),
	);
	let err = user_tx
		.commit()
		.await
		.expect_err("commit conflict should remain visible when cleanup also fails");
	assert!(
		is_retryable_transaction_conflict(&err),
		"primary commit error was not preserved: {err}"
	);
	assert!(
		!err.to_string().contains("injected non-retryable error"),
		"cleanup error replaced primary commit error: {err}"
	);
	Ok(())
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn commit_failure_cleans_uncommitted_index_build_artifacts() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:one SET email = 'one@example.com' RETURN NONE;
			",
	)
	.await?;
	let (ns, db, table) = get_table_ids(&ds, "user").await?;
	let ix = IndexId(42);
	seed_uncommitted_index_build_artifacts(&ds, ns, db, &table, ix).await?;

	let user_tx = ds.transaction(TransactionType::Write).await?;
	(*user_tx).set("commit-conflict".as_bytes().into(), b"user-write".as_slice()).await?;
	let ctx = ds.setup_ctx()?;
	let builder = ctx.get_index_builder().expect("index builder should exist").clone();
	user_tx
		.on_rollback(CleanUncommittedBuild::boxed(
			builder.clone(),
			builder.transaction_factory(),
			user_tx.sequences(),
			ns,
			db,
			table.clone(),
			ix,
		))
		.await;

	let conflicting_tx = ds.transaction(TransactionType::Write).await?;
	(*conflicting_tx)
		.set("commit-conflict".as_bytes().into(), b"conflicting-write".as_slice())
		.await?;
	conflicting_tx.commit().await?;

	let err = user_tx.commit().await.expect_err("commit conflict should remain visible");
	assert!(
		is_retryable_transaction_conflict(&err),
		"primary commit error was not preserved: {err}"
	);
	assert_no_index_build_artifacts(&ds, ns, db, &table, ix).await?;
	Ok(())
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn store_changes_failure_preserves_primary_error_when_cleanup_fails() -> Result<()> {
	let (ds, _) = new_index_test_ds().await?;
	let ikb = IndexKeyBase::new(NamespaceId(1), DatabaseId(1), TableName::from("user"), IndexId(1));
	let generation = 1;
	let ticket = 1;
	let reservation = IndexBuildReservation {
		node: ds.id(),
		expires_at: Utc::now() + chrono::Duration::seconds(BUILD_RESERVATION_TTL_SECS),
	};
	let br = ikb.new_br_key(generation, ticket);
	let br_key = br.encode_key()?;
	let br_val = reservation.kv_encode_value()?;

	let seed_tx = ds.transaction(TransactionType::Write).await?;
	seed_tx.set_key(&br, &reservation).await?;
	seed_tx.commit().await?;

	let user_tx = ds.transaction(TransactionType::Read).await?;
	let table = TableName::from("user");
	let record = RecordId {
		table: table.clone(),
		key: RecordIdKey::from("one".to_owned()),
	};
	let current: Value = "new@example.com".into();
	user_tx.changefeed_buffer_record_change(
		NamespaceId(1),
		DatabaseId(1),
		&table,
		&record,
		Record::new(Value::None).into_read_only(),
		Record::new(current).into_read_only(),
		false,
	);
	user_tx
		.register_index_build_reservation_release(IndexBuildReservationRelease::new(
			ds.transaction_factory().clone(),
			ds.sequences().clone(),
			ds.id(),
			br_key,
			br_val,
		))
		.await;

	let _release_guard = inject_non_retryable_error(
		NonRetryableErrorSite::ConcurrentIndexReservationRelease,
		ds.id(),
	);
	let err = user_tx
		.commit()
		.await
		.expect_err("store_changes failure should remain visible when cleanup also fails");
	assert!(
		matches!(storage_error(&err), Some(crate::kvs::Error::TransactionReadonly)),
		"primary store_changes error was not preserved: {err}"
	);
	assert!(
		!err.to_string().contains("injected non-retryable error"),
		"cleanup error replaced primary store_changes error: {err}"
	);
	Ok(())
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn distributed_committed_cleanup_failure_recovers_from_appending() -> Result<()> {
	let (ds_a, ds_b, session) = new_distributed_index_test_ds().await?;
	execute_all(
		&ds_a,
		&session,
		"
					DEFINE TABLE user SCHEMALESS;
					CREATE user:one SET email = 'old@example.com' RETURN NONE;
					",
	)
	.await?;

	let build_guard = start_index_build_paused(
		&ds_a,
		&session,
		"DEFINE INDEX test ON user FIELDS email CONCURRENTLY",
	)
	.await?;
	let _release_guard = inject_non_retryable_error(
		NonRetryableErrorSite::ConcurrentIndexReservationRelease,
		ds_b.id(),
	);
	execute_all_retrying_conflicts(
		&ds_b,
		&session,
		"UPDATE user:one SET email = 'new@example.com' RETURN NONE",
	)
	.await?;

	drop(build_guard);
	wait_for_index_ready(&ds_a, &session, "user", "test").await?;
	expect_indexed_query_len(
		&ds_a,
		&session,
		"SELECT id FROM user WITH INDEX test WHERE email = 'new@example.com'",
		1,
	)
	.await?;
	expect_indexed_query_len(
		&ds_a,
		&session,
		"SELECT id FROM user WITH INDEX test WHERE email = 'old@example.com'",
		0,
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds_a, "user", "test").await?;
	let ikb = IndexKeyBase::new(ns, db, table, ix.index_id);
	assert_eq!(durable_queue_all_generations_count(&ds_a, &ikb).await?, 0);
	Ok(())
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn distributed_writer_admission_honors_statement_timeout_while_closing() -> Result<()> {
	let (ds_a, ds_b, session) = new_distributed_index_test_ds().await?;
	execute_all(
		&ds_a,
		&session,
		"
						DEFINE TABLE user SCHEMALESS;
						CREATE user:one SET email = 'old@example.com' RETURN NONE;
						DEFINE INDEX test ON user FIELDS email;
						",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds_a, "user", "test").await?;
	let ikb = IndexKeyBase::new(ns, db, table, ix.index_id);
	set_durable_build_state(
		&ds_a,
		&ikb,
		durable_build_state_for_phase(IndexBuildPhase::Closing, 2, Some(ds_a.id())),
	)
	.await?;

	let started = Instant::now();
	let mut results = timeout(
		Duration::from_secs(5),
		ds_b.execute("UPDATE user:one SET email = 'new@example.com' TIMEOUT 50ms", &session, None),
	)
	.await
	.map_err(|_| anyhow::anyhow!("writer admission ignored the statement timeout"))??;
	let error = results
		.remove(0)
		.result
		.expect_err("write should time out while durable state is Closing")
		.to_string();

	assert!(
		started.elapsed() < Duration::from_secs(5),
		"write waited for an internal timeout instead of the statement timeout"
	);
	assert!(error.contains("exceeded the timeout: 50ms"), "unexpected timeout error: {error}");
	assert_eq!(durable_build_state(&ds_a, &ikb).await?.phase, IndexBuildPhase::Closing);
	Ok(())
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn distributed_writer_admission_waits_until_closing_online() -> Result<()> {
	let (ds_a, ds_b, session) = new_distributed_index_test_ds().await?;
	execute_all(
		&ds_a,
		&session,
		"
						DEFINE TABLE user SCHEMALESS;
						CREATE user:one SET email = 'old@example.com' RETURN NONE;
						DEFINE INDEX test ON user FIELDS email;
						",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds_a, "user", "test").await?;
	let ikb = IndexKeyBase::new(ns, db, table, ix.index_id);
	set_durable_build_state(
		&ds_a,
		&ikb,
		durable_build_state_for_phase(IndexBuildPhase::Closing, 2, Some(ds_a.id())),
	)
	.await?;

	let session_write = session.clone();
	let writer = tokio::spawn(async move {
		execute_all(
			&ds_b,
			&session_write,
			"UPDATE user:one SET email = 'new@example.com' RETURN NONE",
		)
		.await
	});
	sleep(Duration::from_millis(250)).await;
	assert!(!writer.is_finished(), "write returned before durable Closing became Online");

	set_durable_build_state(
		&ds_a,
		&ikb,
		durable_build_state_for_phase(IndexBuildPhase::Online, 2, None),
	)
	.await?;
	timeout(Duration::from_secs(10), writer)
		.await
		.map_err(|_| anyhow::anyhow!("timed out waiting for Closing admission write"))???;

	expect_indexed_query_len(
		&ds_a,
		&session,
		"SELECT id FROM user WITH INDEX test WHERE email = 'new@example.com'",
		1,
	)
	.await?;
	Ok(())
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn distributed_rolled_back_writer_releases_reservation_after_close() -> Result<()> {
	let (ds_a, ds_b, session) = new_distributed_index_test_ds().await?;
	execute_all(
		&ds_a,
		&session,
		"
					DEFINE TABLE user SCHEMALESS;
					CREATE user:one SET email = 'old@example.com' RETURN NONE;
					",
	)
	.await?;

	let build_guard = start_index_build_paused(
		&ds_a,
		&session,
		"DEFINE INDEX test ON user FIELDS email CONCURRENTLY",
	)
	.await?;
	execute_cancelled_transaction_retrying_conflicts(
		&ds_b,
		&session,
		"BEGIN; UPDATE user:one SET email = 'new@example.com'; CANCEL;",
	)
	.await?;

	drop(build_guard);
	wait_for_index_ready(&ds_a, &session, "user", "test").await?;
	expect_indexed_query_len(
		&ds_a,
		&session,
		"SELECT id FROM user WITH INDEX test WHERE email = 'old@example.com'",
		1,
	)
	.await?;
	expect_indexed_query_len(
		&ds_a,
		&session,
		"SELECT id FROM user WITH INDEX test WHERE email = 'new@example.com'",
		0,
	)
	.await?;
	Ok(())
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn distributed_admission_error_after_registration_releases_reservation() -> Result<()> {
	let (ds_a, ds_b, session) = new_distributed_index_test_ds().await?;
	execute_all(
		&ds_a,
		&session,
		"
					DEFINE TABLE user SCHEMALESS;
					CREATE user:one SET email = 'old@example.com' RETURN NONE;
					",
	)
	.await?;

	let build_guard = start_index_build_paused(
		&ds_a,
		&session,
		"DEFINE INDEX test ON user FIELDS email CONCURRENTLY",
	)
	.await?;
	let _guard = inject_non_retryable_error(
		NonRetryableErrorSite::ConcurrentIndexAfterReservationRegistration,
		ds_b.id(),
	);
	let error = execute_error_text_retrying_conflicts(
		&ds_b,
		&session,
		"UPDATE user:one SET email = 'new@example.com' RETURN NONE",
	)
	.await?;
	assert!(error.contains("injected non-retryable error"), "unexpected error: {error}");

	drop(build_guard);
	wait_for_index_ready(&ds_a, &session, "user", "test").await?;
	expect_indexed_query_len(
		&ds_a,
		&session,
		"SELECT id FROM user WITH INDEX test WHERE email = 'old@example.com'",
		1,
	)
	.await?;
	expect_indexed_query_len(
		&ds_a,
		&session,
		"SELECT id FROM user WITH INDEX test WHERE email = 'new@example.com'",
		0,
	)
	.await?;
	Ok(())
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn distributed_rollback_reservation_cleanup_failure_marks_build_error() -> Result<()> {
	let (ds_a, ds_b, session) = new_distributed_index_test_ds().await?;
	execute_all(
		&ds_a,
		&session,
		"
					DEFINE TABLE user SCHEMALESS;
					CREATE user:one SET email = 'old@example.com' RETURN NONE;
					",
	)
	.await?;

	let build_guard = start_index_build_paused(
		&ds_a,
		&session,
		"DEFINE INDEX test ON user FIELDS email CONCURRENTLY",
	)
	.await?;
	let _release_guard = inject_non_retryable_error(
		NonRetryableErrorSite::ConcurrentIndexReservationRelease,
		ds_b.id(),
	);
	let error = execute_error_text_retrying_conflicts(
		&ds_b,
		&session,
		"BEGIN; UPDATE user:one SET email = 'new@example.com'; CANCEL;",
	)
	.await?;
	assert!(
		error.contains("injected non-retryable error")
			|| error.contains("durable index-build reservation")
			|| error.contains("cancelled transaction"),
		"unexpected transaction error: {error}"
	);

	let building = index_building_json(&ds_a, &session, "user", "test").await?;
	assert_eq!(building.get("status").and_then(|status| status.as_str()), Some("error"));
	let reason = building.get("error").and_then(|error| error.as_str()).unwrap_or_default();
	assert!(
		reason.contains("Failed to release durable index-build reservation"),
		"unexpected durable error reason: {building}"
	);
	drop(build_guard);
	Ok(())
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn distributed_unique_index_replays_second_node_insert() -> Result<()> {
	// The queued insert must populate the unique index before the index is
	// marked ready, so a later duplicate is rejected by the online index.
	let (ds_a, ds_b, session) = new_distributed_index_test_ds().await?;
	execute_all(
		&ds_a,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:one SET email = 'one@example.com' RETURN NONE;
			CREATE user:two SET email = 'two@example.com' RETURN NONE;
			",
	)
	.await?;

	let guard = start_index_build_paused(
		&ds_a,
		&session,
		"DEFINE INDEX test ON user FIELDS email UNIQUE CONCURRENTLY",
	)
	.await?;
	execute_all_retrying_conflicts(
		&ds_b,
		&session,
		"CREATE user:queued SET email = 'queued@example.com' RETURN NONE",
	)
	.await?;
	drop(guard);
	wait_for_index_ready(&ds_a, &session, "user", "test").await?;

	assert_eq!(
		query_array_len(
			&ds_a,
			&session,
			"SELECT id FROM user WITH INDEX test WHERE email = 'queued@example.com'"
		)
		.await?,
		1
	);
	expect_statement_error(
		&ds_b,
		&session,
		"CREATE user:duplicate SET email = 'queued@example.com' RETURN NONE",
	)
	.await?;
	Ok(())
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn distributed_fulltext_index_replays_second_node_update() -> Result<()> {
	// Full-text replay has to remove terms for the old value and add terms
	// for the new value written by the second node during the build.
	let (ds_a, ds_b, session) = new_distributed_index_test_ds().await?;
	execute_all(
		&ds_a,
		&session,
		"
			DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase;
			DEFINE TABLE doc SCHEMALESS;
			CREATE doc:one SET text = 'old phrase' RETURN NONE;
			CREATE doc:two SET text = 'stable text' RETURN NONE;
			",
	)
	.await?;

	let guard = start_index_build_paused(
		&ds_a,
		&session,
		"DEFINE INDEX test ON doc FIELDS text FULLTEXT ANALYZER simple BM25 HIGHLIGHTS CONCURRENTLY",
	)
	.await?;
	execute_all_retrying_conflicts(
		&ds_b,
		&session,
		"UPDATE doc:one SET text = 'queued phrase' RETURN NONE",
	)
	.await?;
	drop(guard);
	wait_for_index_ready(&ds_a, &session, "doc", "test").await?;
	let building = index_building_json(&ds_a, &session, "doc", "test").await?;
	assert_eq!(building.get("pending").and_then(|pending| pending.as_u64()), Some(0));
	let updated =
		building.get("updated").and_then(|updated| updated.as_u64()).ok_or_else(|| {
			anyhow::anyhow!("index info did not include building.updated: {building}")
		})?;
	assert!(
		updated >= 1,
		"expected queued full-text update to be replayed in index status: {building}"
	);

	assert_eq!(
		query_array_len(
			&ds_a,
			&session,
			"SELECT id FROM doc WITH INDEX test WHERE text @@ 'queued'"
		)
		.await?,
		1
	);
	assert_eq!(
		query_array_len(&ds_a, &session, "SELECT id FROM doc WITH INDEX test WHERE text @@ 'old'")
			.await?,
		0
	);
	Ok(())
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn distributed_hnsw_index_replays_second_node_insert() -> Result<()> {
	// HNSW replay is append-only for this scenario: a vector inserted by the
	// second node while the build is paused must be searchable after ready.
	let (ds_a, ds_b, session) = new_distributed_index_test_ds().await?;
	execute_all(
		&ds_a,
		&session,
		"
			DEFINE TABLE vec SCHEMALESS;
			CREATE vec:one SET vector = [0f, 0f] RETURN NONE;
			CREATE vec:two SET vector = [20f, 20f] RETURN NONE;
			",
	)
	.await?;

	let guard = start_index_build_paused(
			&ds_a,
			&session,
			"DEFINE INDEX test ON vec FIELDS vector HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32 EFC 16 M 4 CONCURRENTLY",
		)
		.await?;
	execute_all_retrying_conflicts(
		&ds_b,
		&session,
		"CREATE vec:queued SET vector = [10f, 10f] RETURN NONE",
	)
	.await?;
	drop(guard);
	wait_for_index_ready(&ds_a, &session, "vec", "test").await?;

	assert_eq!(
		query_array_len(
			&ds_a,
			&session,
			"SELECT id FROM vec WITH INDEX test WHERE vector <|1,40|> [10f, 10f] AND id = vec:queued"
		)
		.await?,
		1
	);
	Ok(())
}

/// Where a `CONCURRENTLY` build is held while a second node's writes queue
/// behind it.
#[cfg(feature = "kv-mem")]
#[derive(Clone, Copy, Debug)]
enum BuildPause {
	/// Before the initial scan reads a row: the scan then finds each record at
	/// its final value and has to index the queued baseline instead.
	BeforeScan,
	/// After the initial scan read its batch, while that batch's commit
	/// retries: the rows the scan holds are older than the queued writes.
	InScanBatch,
}

#[cfg(feature = "kv-mem")]
impl BuildPause {
	const ALL: [Self; 2] = [Self::BeforeScan, Self::InScanBatch];

	fn site(self) -> RetryableConflictSite {
		match self {
			Self::BeforeScan => RetryableConflictSite::ConcurrentIndexInitialCleanup,
			Self::InScanBatch => RetryableConflictSite::ConcurrentIndexInitialBatch,
		}
	}
}

/// The records of table `t` and the value each starts with; `f` is the
/// indexed field. Values are names, spelled per index kind by a literal
/// function: a word for the scalar kinds, a point for the vector kinds.
#[cfg(feature = "kv-mem")]
const TWICE_MODIFIED_SEED: [(&str, &str); 5] =
	[("twice", "v0"), ("in_one", "w0"), ("gone", "x0"), ("back", "y0"), ("still", "z0")];

/// Every way to change one record twice before a builder has replayed the
/// first change: two updates in two transactions, two updates in one, an
/// update then a delete, and a delete then a re-create. Each step is one
/// transaction, so retrying a conflicted step repeats only that step.
/// `{name}` is replaced by the literal for that value.
#[cfg(feature = "kv-mem")]
const TWICE_MODIFIED_STEPS: [&str; 7] = [
	"UPDATE t:twice SET f = {v1} RETURN NONE",
	"UPDATE t:twice SET f = {v2} RETURN NONE",
	"BEGIN; UPDATE t:in_one SET f = {w1} RETURN NONE; UPDATE t:in_one SET f = {w2} RETURN NONE; COMMIT",
	"UPDATE t:gone SET f = {x1} RETURN NONE",
	"DELETE t:gone RETURN NONE",
	"DELETE t:back RETURN NONE",
	"CREATE t:back SET f = {y1} RETURN NONE",
];

/// The records left once every step has committed, with their values.
#[cfg(feature = "kv-mem")]
const TWICE_MODIFIED_FINAL: [(&str, &str); 4] =
	[("back", "y1"), ("in_one", "w2"), ("still", "z0"), ("twice", "v2")];

/// Every value some record held at some point.
#[cfg(feature = "kv-mem")]
const TWICE_MODIFIED_HISTORY: [&str; 11] =
	["v0", "v1", "v2", "w0", "w1", "w2", "x0", "x1", "y0", "y1", "z0"];

#[cfg(feature = "kv-mem")]
fn word(name: &str) -> String {
	format!("'{name}'")
}

/// A 2-d point per value, spaced so that no query point in the history is
/// equidistant from two of the final records.
#[cfg(feature = "kv-mem")]
fn point(name: &str) -> String {
	let (x, y) = match name {
		"v0" => (0, 0),
		"v1" => (10, 0),
		"v2" => (20, 0),
		"w0" => (0, 10),
		"w1" => (0, 20),
		"w2" => (0, 30),
		"x0" => (-10, 0),
		"x1" => (-20, 0),
		"y0" => (0, -10),
		"y1" => (0, -25),
		"z0" => (50, 50),
		other => unreachable!("no point for {other}"),
	};
	format!("[{x}f, {y}f]")
}

#[cfg(feature = "kv-mem")]
fn spell(template: &str, literal: fn(&str) -> String) -> String {
	TWICE_MODIFIED_HISTORY
		.iter()
		.fold(template.to_owned(), |sql, name| sql.replace(&format!("{{{name}}}"), &literal(name)))
}

/// The ids of the records whose final value is `value`, in id order.
#[cfg(feature = "kv-mem")]
fn twice_modified_holders(value: &str) -> Vec<String> {
	TWICE_MODIFIED_FINAL
		.iter()
		.filter(|(_, held)| *held == value)
		.map(|(id, _)| (*id).to_owned())
		.collect()
}

/// The number of changes queued behind a build, across every generation.
#[cfg(feature = "kv-mem")]
async fn durable_appending_count(ds: &Datastore, ikb: &IndexKeyBase) -> Result<usize> {
	let tx = ds.transaction(TransactionType::Read).await?;
	let bg = catch!(tx, tx.keys(ikb.new_bg_all_generations_range()?, u32::MAX, 0, None).await);
	tx.cancel().await?;
	Ok(bg.len())
}

/// A query's rows as JSON, for comparing two queries' answers.
#[cfg(feature = "kv-mem")]
async fn query_json(ds: &Datastore, session: &Session, sql: &str) -> Result<serde_json::Value> {
	let mut results = ds.execute(sql, session, None).await?;
	Ok(results.remove(0).result?.into_json_value())
}

/// Seeds `t`, starts `define` (an index named `ix`) held at `pause`, applies
/// [`TWICE_MODIFIED_STEPS`] through a second node so every write that changes
/// what the index holds queues behind the build — `queued` of them — then
/// releases the build and waits for it to publish.
#[cfg(feature = "kv-mem")]
async fn build_over_records_modified_twice(
	define: &str,
	pause: BuildPause,
	literal: fn(&str) -> String,
	queued: usize,
) -> Result<(Arc<Datastore>, Session)> {
	let (ds_a, ds_b, session) = new_distributed_index_test_ds().await?;
	let seed: String = TWICE_MODIFIED_SEED
		.iter()
		.map(|(id, value)| format!("CREATE t:{id} SET f = {} RETURN NONE;", literal(value)))
		.collect();
	execute_all(&ds_a, &session, &format!("DEFINE TABLE t SCHEMALESS; {seed}")).await?;
	let site = pause.site();
	let node_id = ds_a.id();
	let guard = inject_retryable_conflicts(site, node_id, REPEATED_RETRY_CONFLICTS);
	execute_all(&ds_a, &session, define).await?;
	wait_for_retry_conflict(site, node_id, REPEATED_RETRY_CONFLICTS).await?;
	for step in TWICE_MODIFIED_STEPS {
		execute_all_retrying_conflicts(&ds_b, &session, &spell(step, literal)).await?;
	}
	// The queue keeps one entry per change, a record's changes included:
	// nothing is merged before the builder replays them in order.
	let (ns, db, table, ix) = get_table_index(&ds_a, "t", "ix").await?;
	let ikb = IndexKeyBase::new(ns, db, table, ix.index_id);
	assert_eq!(
		durable_appending_count(&ds_a, &ikb).await?,
		queued,
		"{pause:?}: one entry per change"
	);
	assert!(
		retryable_conflict_count(site, node_id) > 0,
		"{pause:?}: the build must still be held when the last write commits"
	);
	drop(guard);
	wait_for_index_ready(&ds_a, &session, "t", "ix").await?;
	Ok((ds_a, session))
}

/// Every value in the history resolves, through the index, to exactly the
/// records holding it now, and to the same rows a scan finds.
#[cfg(feature = "kv-mem")]
async fn assert_equality_lookups_match_the_rows(
	ds: &Datastore,
	session: &Session,
	pause: BuildPause,
) -> Result<()> {
	for value in TWICE_MODIFIED_HISTORY {
		let indexed = format!(
			"SELECT VALUE record::id(id) FROM t WITH INDEX ix WHERE f = '{value}' ORDER BY id"
		);
		let scanned = indexed.replace("WITH INDEX ix", "WITH NOINDEX");
		let got = query_json(ds, session, &indexed).await?;
		assert_eq!(got, serde_json::json!(twice_modified_holders(value)), "{pause:?}: {indexed}");
		assert_eq!(got, query_json(ds, session, &scanned).await?, "{pause:?}: {indexed}");
	}
	Ok(())
}

/// A standard index built while records are changed twice holds exactly one
/// entry per record, for its final value: no entry for a value a record left,
/// and none for a deleted record.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_standard_index_replays_records_modified_twice() -> Result<()> {
	for pause in BuildPause::ALL {
		let (ds, session) = build_over_records_modified_twice(
			"DEFINE INDEX ix ON t FIELDS f CONCURRENTLY",
			pause,
			word,
			TWICE_MODIFIED_STEPS.len() + 1,
		)
		.await?;
		assert_equality_lookups_match_the_rows(&ds, &session, pause).await?;
		let all = query_json(
			&ds,
			&session,
			"SELECT VALUE record::id(id) FROM t WITH INDEX ix WHERE f >= 'a' ORDER BY id",
		)
		.await?;
		let finals: Vec<&str> = TWICE_MODIFIED_FINAL.iter().map(|(id, _)| *id).collect();
		assert_eq!(all, serde_json::json!(finals), "{pause:?}: one entry per live record");
	}
	Ok(())
}

/// A unique index built while records are changed twice claims exactly the
/// final values: each is still enforced, and every value a record left, or a
/// deleted record held, is free to claim again.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_unique_index_replays_records_modified_twice() -> Result<()> {
	for pause in BuildPause::ALL {
		let (ds, session) = build_over_records_modified_twice(
			"DEFINE INDEX ix ON t FIELDS f UNIQUE CONCURRENTLY",
			pause,
			word,
			TWICE_MODIFIED_STEPS.len() + 1,
		)
		.await?;
		assert_equality_lookups_match_the_rows(&ds, &session, pause).await?;
		for value in TWICE_MODIFIED_HISTORY {
			let sql = format!("CREATE t:{value}_again SET f = '{value}' RETURN NONE");
			let created = execute_all(&ds, &session, &sql).await;
			let held = !twice_modified_holders(value).is_empty();
			assert_eq!(
				created.is_err(),
				held,
				"{pause:?}: {sql} must fail exactly when a record holds '{value}': {created:?}"
			);
		}
	}
	Ok(())
}

/// The total a COUNT index holds: every `!iu` delta it has stored, summed.
#[cfg(feature = "kv-mem")]
async fn count_index_total(
	ds: &Datastore,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
	ix: IndexId,
) -> Result<i64> {
	let tx = ds.transaction(TransactionType::Read).await?;
	let rng = IndexCountPrefix {
		ns,
		db,
		tb: Cow::Borrowed(table),
		ix,
	}
	.range()?;
	let keys = catch!(tx, tx.keys(rng, u32::MAX, 0, None).await);
	tx.cancel().await?;
	let mut total = 0;
	for key in keys {
		let iu = IndexCountKey::decode_key(&key)?;
		total += if iu.pos {
			iu.count as i64
		} else {
			-(iu.count as i64)
		};
	}
	Ok(total)
}

/// A COUNT index built while records are changed twice counts each live
/// record once, and a conditional COUNT index counts a record whose match
/// flips twice by its final value alone. The index's stored total is read
/// directly: an unconditional count query is answered from the table's keys,
/// not the index.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_count_index_replays_records_modified_twice() -> Result<()> {
	// An unconditional COUNT index is touched only by the two deletes and the
	// re-create; a conditional one by every change that flips a match.
	let cases = [
		("DEFINE INDEX ix ON t COUNT CONCURRENTLY", "SELECT count() FROM t GROUP ALL", 4, 3),
		// `twice`, `in_one` and `gone` each match only at their intermediate
		// value; `back` matches only once it is re-created.
		(
			"DEFINE INDEX ix ON t COUNT WHERE f IN ['v1', 'w1', 'x1', 'y1'] CONCURRENTLY",
			"SELECT count() FROM t WHERE f IN ['v1', 'w1', 'x1', 'y1'] GROUP ALL",
			1,
			TWICE_MODIFIED_STEPS.len() + 1,
		),
	];
	for (define, count, expected, queued) in cases {
		for pause in BuildPause::ALL {
			let (ds, session) =
				build_over_records_modified_twice(define, pause, word, queued).await?;
			let (ns, db, table, ix) = get_table_index(&ds, "t", "ix").await?;
			assert_eq!(
				count_index_total(&ds, ns, db, &table, ix.index_id).await?,
				expected,
				"{pause:?}: {define}"
			);
			assert_eq!(
				count_query_value(&ds, &session, count).await?,
				expected,
				"{pause:?}: {count}"
			);
			let scanned = count.replace("FROM t", "FROM t WITH NOINDEX");
			assert_eq!(
				count_query_value(&ds, &session, &scanned).await?,
				expected,
				"{pause:?}: {scanned}"
			);
		}
	}
	Ok(())
}

/// A full-text index built while records are changed twice matches each term
/// exactly where the rows hold it, and scores every match as an index built
/// over the final rows does: its document count and lengths carry nothing of
/// a value a record left or a record that was deleted.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_fulltext_index_replays_records_modified_twice() -> Result<()> {
	const ANALYZER: &str = "DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase;";
	for pause in BuildPause::ALL {
		let (ds, session) = build_over_records_modified_twice(
			&format!(
				"{ANALYZER} DEFINE INDEX ix ON t FIELDS f FULLTEXT ANALYZER simple BM25 CONCURRENTLY"
			),
			pause,
			word,
			TWICE_MODIFIED_STEPS.len() + 1,
		)
		.await?;
		execute_all(
			&ds,
			&session,
			"DEFINE INDEX reference ON t FIELDS f FULLTEXT ANALYZER simple BM25",
		)
		.await?;
		for value in TWICE_MODIFIED_HISTORY {
			let indexed = format!(
				"SELECT record::id(id) AS rid, search::score(0) AS score FROM t WITH INDEX ix WHERE f @0@ '{value}' ORDER BY rid"
			);
			let got = query_json(&ds, &session, &indexed).await?;
			let ids: Vec<&str> = got
				.as_array()
				.into_iter()
				.flatten()
				.filter_map(|row| row["rid"].as_str())
				.collect();
			assert_eq!(ids, twice_modified_holders(value), "{pause:?}: {indexed}");
			let reference = indexed.replace("WITH INDEX ix", "WITH INDEX reference");
			assert_eq!(got, query_json(&ds, &session, &reference).await?, "{pause:?}: {indexed}");
		}
	}
	Ok(())
}

/// Waits until the index's pending vector updates are folded into its graph.
#[cfg(feature = "kv-mem")]
async fn wait_for_index_drained(ds: &Datastore, session: &Session, index: &str) -> Result<()> {
	timeout(Duration::from_secs(10), async {
		loop {
			let building = index_building_json(ds, session, "t", index).await?;
			if building.get("compacting").and_then(|compacting| compacting.as_bool()) == Some(false)
			{
				return Ok(());
			}
			sleep(Duration::from_millis(20)).await;
		}
	})
	.await
	.map_err(|_| anyhow::anyhow!("timed out waiting for {index} to drain its pending updates"))?
}

/// From every point a record ever held, the index's nearest neighbour and its
/// distance are the brute-force nearest neighbour and distance over the live
/// rows, and a wide search returns each live record exactly once. Distances are
/// compared to three decimals: the index scores in the vectors' `F32`, brute
/// force in the rows' floats. No point is equidistant from two live records.
#[cfg(feature = "kv-mem")]
async fn assert_nearest_neighbours_match_brute_force(
	ds: &Datastore,
	session: &Session,
	context: &str,
	points: &[String],
	live: &[&str],
) -> Result<()> {
	for at in points {
		let indexed = format!(
			"SELECT record::id(id) AS id, math::fixed(vector::distance::knn(), 3) AS distance FROM t WITH INDEX ix WHERE f <|1,40|> {at}"
		);
		let brute = format!(
			"SELECT record::id(id) AS id, math::fixed(vector::distance::knn(), 3) AS distance FROM t WITH NOINDEX WHERE f <|1,EUCLIDEAN|> {at}"
		);
		assert_eq!(
			query_json(ds, session, &indexed).await?,
			query_json(ds, session, &brute).await?,
			"{context}: nearest neighbour of {at}"
		);
	}
	let wide = query_json(
		ds,
		session,
		"SELECT VALUE record::id(id) FROM t WITH INDEX ix WHERE f <|10,40|> [0f, 0f] ORDER BY id",
	)
	.await?;
	assert_eq!(wide, serde_json::json!(live), "{context}: each live record exactly once");
	Ok(())
}

/// Builds a vector index while records are changed twice, then changes
/// records twice again once it is online, and checks the answers each time:
/// with the queued changes still pending, once the build has drained them into
/// the graph, and once compaction has folded the online changes — which
/// remove vectors the graph holds — in as well.
#[cfg(feature = "kv-mem")]
async fn assert_vector_index_replays_records_modified_twice(define: &str) -> Result<()> {
	for pause in BuildPause::ALL {
		let (ds, session) =
			build_over_records_modified_twice(define, pause, point, TWICE_MODIFIED_STEPS.len() + 1)
				.await?;
		let history: Vec<String> = TWICE_MODIFIED_HISTORY.iter().map(|name| point(name)).collect();
		let finals: Vec<&str> = TWICE_MODIFIED_FINAL.iter().map(|(id, _)| *id).collect();
		let context = format!("{pause:?} after the build");
		assert_nearest_neighbours_match_brute_force(&ds, &session, &context, &history, &finals)
			.await?;
		wait_for_index_drained(&ds, &session, "ix").await?;
		let context = format!("{pause:?} after the drain");
		assert_nearest_neighbours_match_brute_force(&ds, &session, &context, &history, &finals)
			.await?;

		for step in [
			"UPDATE t:twice SET f = [30f, 0f] RETURN NONE",
			"UPDATE t:twice SET f = [40f, 0f] RETURN NONE",
			"BEGIN; UPDATE t:in_one SET f = [0f, 40f] RETURN NONE; UPDATE t:in_one SET f = [0f, 44f] RETURN NONE; COMMIT",
			"UPDATE t:still SET f = [60f, 60f] RETURN NONE",
			"DELETE t:still RETURN NONE",
		] {
			execute_all(&ds, &session, step).await?;
		}
		let online: Vec<String> = [
			"[20f, 0f]",
			"[30f, 0f]",
			"[40f, 0f]",
			"[0f, 30f]",
			"[0f, 40f]",
			"[0f, 44f]",
			"[50f, 50f]",
			"[60f, 60f]",
		]
		.into_iter()
		.chain(history.iter().map(String::as_str))
		.map(str::to_owned)
		.collect();
		let live = ["back", "in_one", "twice"];
		let context = format!("{pause:?} with online changes pending");
		assert_nearest_neighbours_match_brute_force(&ds, &session, &context, &online, &live)
			.await?;
		Datastore::index_compaction(
			Arc::clone(&ds),
			Duration::from_secs(1),
			CancellationToken::new(),
		)
		.await?;
		wait_for_index_drained(&ds, &session, "ix").await?;
		let context = format!("{pause:?} after compacting the online changes");
		assert_nearest_neighbours_match_brute_force(&ds, &session, &context, &online, &live)
			.await?;
	}
	Ok(())
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_hnsw_index_replays_records_modified_twice() -> Result<()> {
	assert_vector_index_replays_records_modified_twice(
		"DEFINE INDEX ix ON t FIELDS f HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32 EFC 16 M 4 CONCURRENTLY",
	)
	.await
}

#[cfg(all(feature = "kv-mem", diskann))]
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_diskann_index_replays_records_modified_twice() -> Result<()> {
	assert_vector_index_replays_records_modified_twice(
		"DEFINE INDEX ix ON t FIELDS f DISKANN DIMENSION 2 DIST EUCLIDEAN TYPE F32 CONCURRENTLY",
	)
	.await
}

/// Records changed twice while the build's own drain is folding pendings into
/// the graph: every drain batch is built and then fails its commit, so it is
/// re-planned from the pendings as they stand, until the build is released.
/// Each record keeps a single pending entry however often it changes, and the
/// drain that finally commits leaves the graph answering for the final rows.
#[cfg(feature = "kv-mem")]
async fn assert_vector_drain_folds_records_modified_twice(
	define: &str,
	site: RetryableConflictSite,
) -> Result<()> {
	let (ds_a, ds_b, session) = new_distributed_index_test_ds().await?;
	let seed: String = TWICE_MODIFIED_SEED
		.iter()
		.map(|(id, value)| format!("CREATE t:{id} SET f = {} RETURN NONE;", point(value)))
		.collect();
	execute_all(&ds_a, &session, &format!("DEFINE TABLE t SCHEMALESS; {seed}")).await?;
	let node_id = ds_a.id();
	let guard = inject_retryable_conflicts(site, node_id, REPEATED_RETRY_CONFLICTS);
	execute_all(&ds_a, &session, define).await?;
	wait_for_retry_conflict(site, node_id, REPEATED_RETRY_CONFLICTS).await?;
	for step in TWICE_MODIFIED_STEPS {
		execute_all_retrying_conflicts(&ds_b, &session, &spell(step, point)).await?;
	}
	assert!(
		retryable_conflict_count(site, node_id) > 0,
		"the drain must still be retrying when the last write commits"
	);
	drop(guard);
	wait_for_index_drained(&ds_a, &session, "ix").await?;
	let history: Vec<String> = TWICE_MODIFIED_HISTORY.iter().map(|name| point(name)).collect();
	let finals: Vec<&str> = TWICE_MODIFIED_FINAL.iter().map(|(id, _)| *id).collect();
	assert_nearest_neighbours_match_brute_force(
		&ds_a,
		&session,
		"after the drain",
		&history,
		&finals,
	)
	.await
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn hnsw_build_drain_folds_records_modified_twice() -> Result<()> {
	assert_vector_drain_folds_records_modified_twice(
		"DEFINE INDEX ix ON t FIELDS f HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32 EFC 16 M 4 CONCURRENTLY",
		RetryableConflictSite::ConcurrentIndexHnswPendingCompaction,
	)
	.await
}

#[cfg(all(feature = "kv-mem", diskann))]
#[tokio::test(flavor = "multi_thread")]
async fn diskann_build_drain_folds_records_modified_twice() -> Result<()> {
	assert_vector_drain_folds_records_modified_twice(
		"DEFINE INDEX ix ON t FIELDS f DISKANN DIMENSION 2 DIST EUCLIDEAN TYPE F32 CONCURRENTLY",
		RetryableConflictSite::ConcurrentIndexDiskAnnPendingCompaction,
	)
	.await
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_indexing_retries_initial_cleanup_commit_conflict() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	let site = RetryableConflictSite::ConcurrentIndexInitialCleanup;
	let node_id = ds.id();
	let _guard = inject_retryable_conflict(site, node_id);

	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			DEFINE INDEX test ON user FIELDS email CONCURRENTLY;
			",
	)
	.await?;
	wait_for_index_ready(&ds, &session, "user", "test").await?;

	assert_eq!(retryable_conflict_count(site, node_id), 0);
	Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_indexing_retries_initial_batch_commit_conflict() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:1 SET email = 'one@example.com' RETURN NONE;
			CREATE user:2 SET email = 'two@example.com' RETURN NONE;
			",
	)
	.await?;
	let site = RetryableConflictSite::ConcurrentIndexInitialBatch;
	let node_id = ds.id();
	let _guard = inject_retryable_conflict(site, node_id);

	execute_all(&ds, &session, "DEFINE INDEX test ON user FIELDS email CONCURRENTLY").await?;
	wait_for_index_ready(&ds, &session, "user", "test").await?;

	assert_eq!(retryable_conflict_count(site, node_id), 0);
	Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_indexing_retries_initial_cleanup_stops_after_abort() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	let site = RetryableConflictSite::ConcurrentIndexInitialCleanup;
	let node_id = ds.id();
	let _guard = inject_retryable_conflicts(site, node_id, REPEATED_RETRY_CONFLICTS);

	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			DEFINE INDEX test ON user FIELDS email CONCURRENTLY;
			",
	)
	.await?;
	wait_for_retry_conflict(site, node_id, REPEATED_RETRY_CONFLICTS).await?;

	execute_all(&ds, &session, "REMOVE INDEX test ON user").await?;

	let remaining = wait_for_retry_conflict_count_to_stabilize(site, node_id).await?;
	assert!(remaining > 0, "abort should stop retries before all conflicts are consumed");
	Ok(())
}

/// The HNSW pending drain that closes a blocking build contends with the
/// datastore's periodic index compaction over the same batch, so the loser's
/// commit fails with a retryable conflict. That conflict must be retried
/// against a freshly read plan, not returned: it is the statement's own error
/// otherwise, and `REBUILD INDEX` reports a raw transaction conflict the caller
/// can do nothing with.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn rebuild_index_retries_a_conflicted_hnsw_pending_compaction() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE INDEX hx ON t FIELDS vec HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32;
		 CREATE t:1 SET vec = [1.0, 0.0];
		 CREATE t:2 SET vec = [0.0, 1.0];",
	)
	.await?;
	let site = RetryableConflictSite::ConcurrentIndexHnswPendingCompaction;
	let node_id = ds.id();
	let _guard = inject_retryable_conflict(site, node_id);

	execute_all(&ds, &session, "REBUILD INDEX hx ON t").await?;

	assert_eq!(
		retryable_conflict_count(site, node_id),
		0,
		"the rebuild must consume the injected conflict by retrying the drain"
	);
	// A retry that gave up early would leave pendings uncompacted, so the graph
	// would answer with fewer neighbours than the table holds.
	expect_indexed_query_len(&ds, &session, "SELECT id FROM t WHERE vec <|2,40|> [1.0f, 0.0f]", 2)
		.await
}

/// DiskANN counterpart of
/// [`rebuild_index_retries_a_conflicted_hnsw_pending_compaction`]. Its apply
/// owns the commit, so the conflict arrives as the apply's own error.
#[cfg(all(feature = "kv-mem", diskann))]
#[tokio::test(flavor = "multi_thread")]
async fn rebuild_index_retries_a_conflicted_diskann_pending_compaction() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE INDEX dx ON t FIELDS vec DISKANN DIMENSION 2 DIST EUCLIDEAN TYPE F32;
		 CREATE t:1 SET vec = [1.0, 0.0];
		 CREATE t:2 SET vec = [0.0, 1.0];",
	)
	.await?;
	let site = RetryableConflictSite::ConcurrentIndexDiskAnnPendingCompaction;
	let node_id = ds.id();
	let _guard = inject_retryable_conflict(site, node_id);

	execute_all(&ds, &session, "REBUILD INDEX dx ON t").await?;

	assert_eq!(
		retryable_conflict_count(site, node_id),
		0,
		"the rebuild must consume the injected conflict by retrying the drain"
	);
	expect_indexed_query_len(&ds, &session, "SELECT id FROM t WHERE vec <|2,40|> [1.0f, 0.0f]", 2)
		.await
}

/// The periodic index-compaction task must leave an index alone while a local
/// build owns it.
///
/// A `REBUILD INDEX` that migrates the on-disk format holds the new definition
/// in its own uncommitted transaction, so the compaction task reads the retired
/// one from the catalog. Compacting from there folds the index's pendings into
/// the layout the rebuild is replacing, and the published index is left without
/// them — a silently empty vector index rather than a failed statement.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn index_compaction_leaves_an_index_its_builder_still_owns() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE INDEX hx ON t FIELDS vec HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32;
		 CREATE t:1 SET vec = [1.0, 0.0];
		 CREATE t:2 SET vec = [0.0, 1.0];",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "t", "hx").await?;
	let ikb = IndexKeyBase::new(ns, db, table, ix.index_id);
	let pendings = count_hnsw_pendings(&ds, &ikb).await?;
	assert!(pendings > 0, "the fixture must leave pendings for a compaction to consume");

	// Park a build in its pre-scan cleanup retry loop. The injection aborts the
	// transaction that both wipes the index data and would commit the wipe, so
	// the pendings above survive while the index stays registered to a builder.
	let site = RetryableConflictSite::ConcurrentIndexInitialCleanup;
	let node_id = ds.id();
	let _guard = inject_retryable_conflicts(site, node_id, REPEATED_RETRY_CONFLICTS);
	execute_all(&ds, &session, "REBUILD INDEX hx ON t CONCURRENTLY").await?;
	wait_for_retry_conflict(site, node_id, REPEATED_RETRY_CONFLICTS).await?;

	Datastore::index_compaction(Arc::clone(&ds), Duration::from_secs(1), CancellationToken::new())
		.await?;

	assert_eq!(
		count_hnsw_pendings(&ds, &ikb).await?,
		pendings,
		"compaction must leave the pendings to the builder that owns the index"
	);
	// Stop the parked build so the task does not outlive the test.
	execute_all(&ds, &session, "REMOVE INDEX hx ON t").await?;
	Ok(())
}

/// `INFO FOR INDEX` must say whether the index is still folding queued writes
/// into itself, because `status` alone cannot.
///
/// An index whose compaction queue is not drained answers a kNN search by
/// scoring the queued entries by hand, which costs far more than the index it
/// is standing in for — measured at ~600ms against ~5ms on a 1M-row HNSW index.
/// Every other field here reads exactly the same in both states: `status` is
/// `ready`, and `pending` — which counts writes that arrived *during* a
/// `CONCURRENTLY` build — is 0. Without `compacting`, a caller that builds an
/// index and then queries has no way to tell the two apart.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn info_for_index_reports_an_undrained_compaction_queue() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE INDEX hx ON t FIELDS vec HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32;
		 CREATE t:1 SET vec = [1.0, 0.0];
		 CREATE t:2 SET vec = [0.0, 1.0];",
	)
	.await?;

	let building = index_building_json(&ds, &session, "t", "hx").await?;
	assert_eq!(
		building.get("compacting").and_then(|v| v.as_bool()),
		Some(true),
		"records were written and not yet folded into the graph: {building}"
	);
	// The fields that already existed cannot distinguish this state, which is
	// the whole reason for the two above.
	assert_eq!(building.get("status").and_then(|v| v.as_str()), Some("ready"), "{building}");
	assert_eq!(building.get("pending").and_then(|v| v.as_u64()), Some(0), "{building}");

	Datastore::index_compaction(Arc::clone(&ds), Duration::from_secs(1), CancellationToken::new())
		.await?;

	let building = index_building_json(&ds, &session, "t", "hx").await?;
	assert_eq!(
		building.get("compacting").and_then(|v| v.as_bool()),
		Some(false),
		"the queue drained, so the index now answers at index speed: {building}"
	);
	Ok(())
}

/// The DiskANN half of [`info_for_index_reports_an_undrained_compaction_queue`].
///
/// Worth its own test rather than a parameter: the two engines keep pending
/// work in different places. HNSW appends to one range; DiskANN writes to a
/// sharded range and gates each shard on its own guard, so a probe that reads
/// only the unsharded legacy range reports a settled index no matter how much
/// is outstanding. That is precisely the shape of the bug this covers.
#[cfg(all(feature = "kv-mem", diskann))]
#[tokio::test(flavor = "multi_thread")]
async fn info_for_index_reports_an_undrained_diskann_pending_set() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE INDEX dx ON t FIELDS vec DISKANN DIMENSION 2 DIST EUCLIDEAN TYPE F32;
		 CREATE t:1 SET vec = [1.0, 0.0];
		 CREATE t:2 SET vec = [0.0, 1.0];",
	)
	.await?;

	let building = index_building_json(&ds, &session, "t", "dx").await?;
	assert_eq!(
		building.get("compacting").and_then(|v| v.as_bool()),
		Some(true),
		"records were written and not yet folded into the graph: {building}"
	);
	assert_eq!(building.get("status").and_then(|v| v.as_str()), Some("ready"), "{building}");
	assert_eq!(building.get("pending").and_then(|v| v.as_u64()), Some(0), "{building}");

	Datastore::index_compaction(Arc::clone(&ds), Duration::from_secs(1), CancellationToken::new())
		.await?;

	let building = index_building_json(&ds, &session, "t", "dx").await?;
	assert_eq!(
		building.get("compacting").and_then(|v| v.as_bool()),
		Some(false),
		"the pending set drained, so the index now answers at index speed: {building}"
	);
	Ok(())
}

/// The exclusion must hold cluster-wide, not just for builds on this node. The
/// `IndexCompaction` task lease can sit on a different node from the one running
/// the rebuild, and that node's local builder registry knows nothing about it.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn index_compaction_leaves_an_index_a_remote_builder_owns() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE INDEX hx ON t FIELDS vec HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32;
		 CREATE t:1 SET vec = [1.0, 0.0];
		 CREATE t:2 SET vec = [0.0, 1.0];",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "t", "hx").await?;
	let ikb = IndexKeyBase::new(ns, db, table, ix.index_id);
	let pendings = count_hnsw_pendings(&ds, &ikb).await?;
	assert!(pendings > 0, "the fixture must leave pendings for a compaction to consume");
	claim_build_for_a_remote_owner(&ds, &ikb, chrono::Duration::zero()).await?;

	Datastore::index_compaction(Arc::clone(&ds), Duration::from_secs(1), CancellationToken::new())
		.await?;

	assert_eq!(
		count_hnsw_pendings(&ds, &ikb).await?,
		pendings,
		"compaction must leave the pendings to the builder that owns the index"
	);
	Ok(())
}

/// A heartbeat lapse is not a fence. `maintain_build_ownership` accepts the
/// same `(generation, owner)` whatever the heartbeat age, so a builder stalled
/// inside one long uninterruptible stretch can resume and keep writing; letting
/// compaction in on wall-clock expiry alone would put it alongside a live
/// writer. Only a committed takeover changes the owner.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn index_compaction_still_defers_to_a_build_whose_heartbeat_lapsed() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE INDEX hx ON t FIELDS vec HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32;
		 CREATE t:1 SET vec = [1.0, 0.0];
		 CREATE t:2 SET vec = [0.0, 1.0];",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "t", "hx").await?;
	let ikb = IndexKeyBase::new(ns, db, table, ix.index_id);
	let pendings = count_hnsw_pendings(&ds, &ikb).await?;
	assert!(pendings > 0, "the fixture must leave pendings for a compaction to consume");
	claim_build_for_a_remote_owner(
		&ds,
		&ikb,
		chrono::Duration::seconds(BUILD_OWNER_LEASE_SECS * 4),
	)
	.await?;

	Datastore::index_compaction(Arc::clone(&ds), Duration::from_secs(1), CancellationToken::new())
		.await?;

	assert_eq!(
		count_hnsw_pendings(&ds, &ikb).await?,
		pendings,
		"an expired heartbeat is a takeover trigger, not evidence that nobody is writing"
	);
	Ok(())
}

/// The window between publishing `Online` and the statement committing its
/// catalog change is still owned: the build's own pending drain runs there, and
/// the catalog still advertises the format the rebuild is replacing. Ownership
/// is not released until that drain is done.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn index_compaction_defers_to_an_online_build_that_has_not_released() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE INDEX hx ON t FIELDS vec HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32;
		 CREATE t:1 SET vec = [1.0, 0.0];
		 CREATE t:2 SET vec = [0.0, 1.0];",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "t", "hx").await?;
	let ikb = IndexKeyBase::new(ns, db, table, ix.index_id);
	let pendings = count_hnsw_pendings(&ds, &ikb).await?;
	assert!(pendings > 0, "the fixture must leave pendings for a compaction to consume");
	// A published generation whose owner has not yet released it — what a
	// blocking rebuild looks like from another node while it drains.
	let mut state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(state.phase, IndexBuildPhase::Online);
	assert_eq!(state.owner, None, "a finished build must have released ownership");
	state.owner = Some(Uuid::now_v7());
	state.owner_heartbeat_at = Some(Utc::now());
	set_durable_build_state(&ds, &ikb, state).await?;

	Datastore::index_compaction(Arc::clone(&ds), Duration::from_secs(1), CancellationToken::new())
		.await?;

	assert_eq!(
		count_hnsw_pendings(&ds, &ikb).await?,
		pendings,
		"an Online generation still owned by its builder must be left to that builder"
	);
	Ok(())
}

/// Run a build the way a statement does: inside a write transaction that stays
/// open across it and is the caller's to close.
///
/// Returns once the build task has exited, whether the statement waited on it
/// (`blocking`) or only spawned it, so a caller can assert on what the finished
/// build left behind while its statement is still uncommitted.
#[cfg(feature = "kv-mem")]
async fn build_index_inside_an_open_statement_transaction(
	ds: &Arc<Datastore>,
	session: &Session,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
	ix: Arc<IndexDefinition>,
	blocking: bool,
) -> Result<Arc<crate::kvs::Transaction>> {
	let txn = Arc::new(ds.transaction(TransactionType::Write).await?);
	let table_def = catch!(txn, txn.get_tb(ns, db, table, None).await).expect("table should exist");
	let mut ctx = ds.setup_ctx()?;
	ctx.set_transaction(Arc::clone(&txn));
	let ctx = ctx.freeze();
	let builder = ctx.get_index_builder().expect("index builder should exist").clone();
	let index_id = ix.index_id;
	let rcv =
		builder.build(&ctx, ds.setup_options(session), table_def.table_id, ix, blocking).await?;
	match rcv {
		Some(rcv) => rcv.await.expect("the build task must report its result")?,
		None => {
			let building = local_builder_for_key(ds, ns, db, table, index_id)
				.await?
				.expect("the build registers a local builder");
			wait_for_finished_builder(&building).await?;
		}
	}
	Ok(txn)
}

/// `CONCURRENTLY` returns as soon as the build task is spawned, so its
/// statement transaction — the one staging the definition the build is made
/// against — is still uncommitted while the build runs and can outlast it on a
/// small index. Ownership has to cover that too, not just the blocking case.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn build_ownership_outlives_a_concurrent_build_until_its_statement_commits() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE INDEX hx ON t FIELDS vec HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32;
		 CREATE t:1 SET vec = [1.0, 0.0];",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "t", "hx").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);

	let txn = build_index_inside_an_open_statement_transaction(
		&ds,
		&session,
		ns,
		db,
		&table,
		Arc::clone(&ix),
		false,
	)
	.await?;

	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(state.phase, IndexBuildPhase::Online);
	assert!(
		state.owner.is_some(),
		"a concurrent build must hold the fence until its statement transaction closes"
	);

	txn.commit().await?;

	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(state.owner, None, "the commit must release the fence");
	Ok(())
}

/// A blocking build publishes `Online` and drains its pendings while the
/// statement that started it still holds the definition it built against in an
/// uncommitted transaction. Every other node reads the previous definition for
/// that whole window, so ownership — the only durable signal that the build
/// happened — has to outlast the builder task itself.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn build_ownership_outlives_the_builder_until_its_statement_commits() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE INDEX hx ON t FIELDS vec HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32;
		 CREATE t:1 SET vec = [1.0, 0.0];
		 CREATE t:2 SET vec = [0.0, 1.0];",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "t", "hx").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);

	let txn = build_index_inside_an_open_statement_transaction(
		&ds,
		&session,
		ns,
		db,
		&table,
		Arc::clone(&ix),
		true,
	)
	.await?;

	let building = local_builder_for_key(&ds, ns, db, &table, ix.index_id)
		.await?
		.expect("the build registers a local builder");
	assert!(building.is_finished(), "a blocking build reports only after its task has exited");
	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(state.phase, IndexBuildPhase::Online);
	assert!(
		state.owner.is_some(),
		"ownership must still fence compaction while the statement transaction is open"
	);

	txn.commit().await?;

	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(
		state.owner, None,
		"the commit that publishes the definition must release the fence"
	);
	Ok(())
}

/// The fence answers a compaction request with "leave it to the builder", and
/// the queue entry that carried it is deleted with its batch either way. That
/// costs nothing while the builder still has a drain to run, but the fence
/// outlives its last one: a write committing between the drain and the
/// statement's commit queues a pending whose request is consumed by a pass that
/// does not act on it. Releasing the fence has to put a request back.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn releasing_build_ownership_requeues_compaction_skipped_by_the_fence() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE INDEX hx ON t FIELDS vec HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32;
		 CREATE t:1 SET vec = [1.0, 0.0];",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "t", "hx").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);

	let txn = build_index_inside_an_open_statement_transaction(
		&ds,
		&session,
		ns,
		db,
		&table,
		Arc::clone(&ix),
		true,
	)
	.await?;
	assert_eq!(
		count_hnsw_pendings(&ds, &ikb).await?,
		0,
		"the build drains the index's pendings before it reports"
	);

	// A write in the window the fence outlives: the build has drained for the
	// last time, and the statement that publishes its definition is still open.
	execute_all(&ds, &session, "CREATE t:2 SET vec = [0.0, 1.0]").await?;
	let pendings = count_hnsw_pendings(&ds, &ikb).await?;
	assert!(pendings > 0, "the write must queue a pending for compaction to consume");

	// Consumes the request that write queued without acting on it.
	Datastore::index_compaction(Arc::clone(&ds), Duration::from_secs(1), CancellationToken::new())
		.await?;
	assert_eq!(
		count_hnsw_pendings(&ds, &ikb).await?,
		pendings,
		"the fence must leave the pending to the builder"
	);

	txn.commit().await?;

	Datastore::index_compaction(Arc::clone(&ds), Duration::from_secs(1), CancellationToken::new())
		.await?;
	assert_eq!(
		count_hnsw_pendings(&ds, &ikb).await?,
		0,
		"the released fence must queue the compaction it turned away"
	);
	Ok(())
}

/// A statement that does not commit leaves the definition background
/// compaction was already reading, so the fence it was holding has to go too —
/// otherwise a rolled-back rebuild strands the index with no maintenance.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn build_ownership_is_released_when_its_statement_rolls_back() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE INDEX hx ON t FIELDS vec HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32;
		 CREATE t:1 SET vec = [1.0, 0.0];",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "t", "hx").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);

	let txn = build_index_inside_an_open_statement_transaction(
		&ds,
		&session,
		ns,
		db,
		&table,
		Arc::clone(&ix),
		true,
	)
	.await?;
	txn.cancel().await?;

	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(state.owner, None, "a rolled-back statement must not leave the index fenced");
	Ok(())
}

/// The exclusion is scoped to the index kinds whose builders drain their own
/// pendings. A full-text or count builder has no such drain, so skipping its
/// compaction request would drop it — the batch that carried it is deleted
/// either way — and leave the deltas uncompacted until the next write.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn index_compaction_still_drains_a_count_index_under_build() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE INDEX cx ON t COUNT;
		 CREATE t:1 SET n = 1;
		 CREATE t:2 SET n = 2;
		 CREATE t:3 SET n = 3;",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "t", "cx").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	let deltas = count_index_delta_keys(&ds, ns, db, &table, ix.index_id).await?;
	assert!(deltas > 1, "the fixture must leave deltas for a compaction to fold");
	claim_build_for_a_remote_owner(&ds, &ikb, chrono::Duration::zero()).await?;

	Datastore::index_compaction(Arc::clone(&ds), Duration::from_secs(1), CancellationToken::new())
		.await?;

	assert!(
		count_index_delta_keys(&ds, ns, db, &table, ix.index_id).await? < deltas,
		"a count index under build must still be compacted"
	);
	Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_indexing_retries_initial_batch_stops_after_abort() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:1 SET email = 'one@example.com' RETURN NONE;
			CREATE user:2 SET email = 'two@example.com' RETURN NONE;
			",
	)
	.await?;
	let site = RetryableConflictSite::ConcurrentIndexInitialBatch;
	let node_id = ds.id();
	let _guard = inject_retryable_conflicts(site, node_id, REPEATED_RETRY_CONFLICTS);

	execute_all(&ds, &session, "DEFINE INDEX test ON user FIELDS email CONCURRENTLY").await?;
	wait_for_retry_conflict(site, node_id, REPEATED_RETRY_CONFLICTS).await?;

	execute_all(&ds, &session, "REMOVE INDEX test ON user").await?;

	let remaining = wait_for_retry_conflict_count_to_stabilize(site, node_id).await?;
	assert!(remaining > 0, "abort should stop retries before all conflicts are consumed");
	Ok(())
}

/// Regression test for surrealdb/surrealdb#7304.
///
/// The background `Building` task's `FrozenContext` used to clone the
/// owning `IndexBuilder` back into itself, forming an
/// `Arc<RwLock<HashMap<.., Arc<Building>>>>` cycle that pinned the
/// `Datastore` (and its storage handles, leaking ~7 fds per RocksDB and
/// ~3 per SurrealKV instance) for the lifetime of the process.
///
/// After the fix, dropping the `Datastore` must let the inner
/// `IndexBuilder::indexes` `Arc` reach zero strong references once any
/// in-flight build task observes the drop.
#[tokio::test(flavor = "multi_thread")]
async fn datastore_drop_releases_index_builder_after_build() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
		DEFINE TABLE user SCHEMALESS;
		DEFINE INDEX test ON user FIELDS email CONCURRENTLY;
		",
	)
	.await?;
	wait_for_index_ready(&ds, &session, "user", "test").await?;

	let weak_indexes = Arc::downgrade(&ds.index_builder().indexes);
	drop(ds);

	// The build task captured an `Arc<Building>` for its lifetime; once
	// `Datastore` drops, the `IndexBuilder::indexes` Arc should be
	// released as soon as the spawn task finalises its `BuildingFinishGuard`.
	timeout(Duration::from_secs(5), async {
		loop {
			if weak_indexes.upgrade().is_none() {
				return;
			}
			sleep(Duration::from_millis(20)).await;
		}
	})
	.await
	.map_err(|_| {
		anyhow::anyhow!(
			"IndexBuilder::indexes Arc not released after Datastore drop — \
			 index Building still pins the back-reference (regression of #7304)"
		)
	})?;
	Ok(())
}

// ---------------------------------------------------------------------------
// Table-level doc-ID space lifecycle under concurrent index DDL.
//
// A table's shared doc-ID space (`!di` record→doc, `!dd` doc→record) exists iff
// the table carries at least one doc-ID-consuming index (full-text / HNSW /
// DiskAnn). These tests exercise that lifecycle through the real DEFINE/REMOVE
// INDEX statement paths — including concurrent removal, where each REMOVE
// independently decides whether it dropped the last consumer.
// ---------------------------------------------------------------------------

/// Drain the deferred reclaim queue, so a test can assert on the shared doc-ID
/// space a last-consumer drop enqueued for background reclaim.
async fn drain_reclaim_queue(ds: &Arc<Datastore>) {
	const MAX_PASSES: usize = 64;
	for _ in 0..MAX_PASSES {
		let (batches, _) = Datastore::reclaim_tombstones(
			Arc::clone(ds),
			web_time::Duration::from_secs(1),
			web_time::Duration::ZERO,
			tokio_util::sync::CancellationToken::new(),
		)
		.await
		.expect("the reclaim pass must not fail");
		if batches == 0 {
			return;
		}
	}
}

/// Returns `(record→doc count, doc→record count)` for `table`'s shared doc-ID
/// space. Each prefix is scanned over its own range, so the `!dh`/`!ds` sequence
/// keys (which deliberately survive a purge) are excluded from the counts.
async fn count_doc_id_mappings(ds: &Datastore, table: &str) -> Result<(usize, usize)> {
	let tx = ds.transaction(TransactionType::Read).await?;
	let ns = tx.get_ns_by_name("test", None).await?.expect("namespace should exist");
	let db = tx.get_db_by_name("test", "test", None).await?.expect("database should exist");
	let tb: TableName = table.into();
	let di = DocLookupPrefix::new(ns.namespace_id, db.database_id, Cow::Borrowed(&tb)).range()?;
	let dd = DocKeyPrefix::new(ns.namespace_id, db.database_id, Cow::Borrowed(&tb)).range()?;
	let di_keys = tx.keys(di, u32::MAX, 0, None).await?;
	let dd_keys = tx.keys(dd, u32::MAX, 0, None).await?;
	tx.cancel().await?;
	Ok((di_keys.len(), dd_keys.len()))
}

/// The shared doc-ID space must survive removal of a non-last consumer and be
/// reclaimed only when the last doc-ID-consuming index is dropped.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn table_doc_ids_purged_only_when_last_consumer_removed() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	// Two full-text indexes on the same table share one doc-ID space.
	execute_all(
		&ds,
		&session,
		"DEFINE ANALYZER simple TOKENIZERS blank;
		 DEFINE INDEX ft1 ON t FIELDS a FULLTEXT ANALYZER simple BM25;
		 DEFINE INDEX ft2 ON t FIELDS b FULLTEXT ANALYZER simple BM25;
		 CREATE t:1 SET a = 'alpha', b = 'one';
		 CREATE t:2 SET a = 'beta',  b = 'two';
		 CREATE t:3 SET a = 'gamma', b = 'three';",
	)
	.await?;
	// One shared doc-ID per record, in both directions.
	assert_eq!(count_doc_id_mappings(&ds, "t").await?, (3, 3), "one shared doc-ID per record");

	// Dropping one of two consumers leaves the mappings — ft2 still needs them.
	execute_all(&ds, &session, "REMOVE INDEX ft1 ON t;").await?;
	assert_eq!(
		count_doc_id_mappings(&ds, "t").await?,
		(3, 3),
		"mappings survive while another doc-ID index remains"
	);

	// Dropping the last consumer reclaims the whole shared space.
	execute_all(&ds, &session, "REMOVE INDEX ft2 ON t;").await?;
	drain_reclaim_queue(&ds).await;
	assert_eq!(
		count_doc_id_mappings(&ds, "t").await?,
		(0, 0),
		"mappings purged once the last doc-ID index is removed"
	);
	Ok(())
}

/// B-tree indexes at the current format version append the record's shared
/// doc-ID to every entry value and consume the table's doc-ID space: mappings
/// are created on insert, reclaimed when the record is deleted, and purged
/// when the last consumer index is removed.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn btree_index_entries_carry_doc_ids() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE INDEX idx_a ON t FIELDS a;
		 DEFINE INDEX uniq_b ON t FIELDS b UNIQUE;
		 CREATE t:1 SET a = 'alpha', b = 1;
		 CREATE t:2 SET a = 'beta',  b = 2;",
	)
	.await?;
	// Both b-tree indexes share one doc-ID per record.
	assert_eq!(count_doc_id_mappings(&ds, "t").await?, (2, 2), "one shared doc-ID per record");

	// Every index entry value carries the record's shared doc-ID.
	let tx = ds.transaction(TransactionType::Read).await?;
	let ns = tx.get_ns_by_name("test", None).await?.expect("namespace should exist");
	let db = tx.get_db_by_name("test", "test", None).await?.expect("database should exist");
	let tb: TableName = "t".into();
	let docids = crate::idx::docids::TableDocIds::new(ns.namespace_id, db.database_id, tb.clone());
	let indexes = tx.all_tb_indexes(ns.namespace_id, db.database_id, &tb, None).await?;
	assert_eq!(indexes.len(), 2);
	let mut entries = 0;
	for ix in indexes.iter() {
		assert!(ix.has_entry_doc_ids(), "fresh b-tree indexes carry entry doc-IDs");
		let rng = EntryPrefix {
			ns: ns.namespace_id,
			db: db.database_id,
			tb: Cow::Borrowed(&tb),
			ix: ix.index_id,
		}
		.range()?;
		for (_, entry) in tx.scan(rng, u32::MAX, 0, None).await? {
			let expected = docids.get_doc_id(&tx, &entry.rid.key).await?;
			assert!(expected.is_some(), "indexed record has a shared doc-ID mapping");
			assert_eq!(entry.doc_id, expected, "entry doc-ID matches the shared mapping");
			entries += 1;
		}
	}
	assert_eq!(entries, 4, "one entry per record per index");
	tx.cancel().await?;

	// Record deletion removes the index entries and reclaims the mapping.
	execute_all(&ds, &session, "DELETE t:1;").await?;
	assert_eq!(count_doc_id_mappings(&ds, "t").await?, (1, 1), "mapping reclaimed on delete");

	// Removing one consumer keeps the space; removing the last reclaims it.
	execute_all(&ds, &session, "REMOVE INDEX idx_a ON t;").await?;
	assert_eq!(
		count_doc_id_mappings(&ds, "t").await?,
		(1, 1),
		"mappings survive while another b-tree consumer remains"
	);
	execute_all(&ds, &session, "REMOVE INDEX uniq_b ON t;").await?;
	drain_reclaim_queue(&ds).await;
	assert_eq!(
		count_doc_id_mappings(&ds, "t").await?,
		(0, 0),
		"mappings purged once the last consumer is removed"
	);
	Ok(())
}

/// An index-only bitmap COUNT (`SELECT count() … WHERE <AND of indexed
/// predicates> GROUP ALL`, issue #547) reads index entries only — zero
/// record fetches. Proved behaviorally: after deleting the record *values*
/// directly at the KV layer (leaving index entries and doc-ID mappings
/// intact), the bitmap count still reports the indexed cardinality, while a
/// NOINDEX count — which must fetch records — reports zero.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn bitmap_count_performs_zero_record_fetches() -> Result<()> {
	use surrealdb_types::Value as PublicValue;

	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE FIELD a ON t TYPE string;
		 DEFINE FIELD b ON t TYPE bool;
		 DEFINE INDEX idx_a ON t FIELDS a;
		 DEFINE INDEX idx_b ON t FIELDS b;
		 CREATE t:1 SET a = 'x', b = true;
		 CREATE t:2 SET a = 'x', b = true;
		 CREATE t:3 SET a = 'x', b = false;
		 CREATE t:4 SET a = 'y', b = true;",
	)
	.await?;

	let count = |sql: &'static str| {
		let ds = &ds;
		let session = &session;
		async move {
			let mut results = ds.execute(sql, session, None).await?;
			let value = results.remove(0).result?;
			match value {
				PublicValue::Array(rows) if rows.len() == 1 => match rows.into_iter().next() {
					Some(PublicValue::Object(obj)) => match obj.get("count") {
						Some(PublicValue::Number(n)) => {
							n.to_int().ok_or_else(|| anyhow::anyhow!("non-integer count"))
						}
						other => anyhow::bail!("unexpected count value: {other:?}"),
					},
					other => anyhow::bail!("unexpected count row: {other:?}"),
				},
				other => anyhow::bail!("unexpected count result: {other:?}"),
			}
		}
	};

	const BITMAP_COUNT: &str = "SELECT count() FROM t WHERE a = 'x' AND b = true GROUP ALL";
	const NOINDEX_COUNT: &str =
		"SELECT count() FROM t WITH NOINDEX WHERE a = 'x' AND b = true GROUP ALL";
	assert_eq!(count(BITMAP_COUNT).await?, 2);
	assert_eq!(count(NOINDEX_COUNT).await?, 2);

	// Delete the record values directly at the KV layer, leaving the index
	// entries and the shared doc-ID mappings untouched.
	{
		let tx = ds.transaction(TransactionType::Write).await?;
		let ns = tx.get_ns_by_name("test", None).await?.expect("namespace should exist");
		let db = tx.get_db_by_name("test", "test", None).await?.expect("database should exist");
		let tb: TableName = "t".into();
		let rng = RecordPrefix {
			ns: ns.namespace_id,
			db: db.database_id,
			tb: std::borrow::Cow::Borrowed(&tb),
		}
		.range()?;
		for key in tx.keys_raw(rng, u32::MAX, 0, None).await? {
			tx.del(key.into()).await?;
		}
		tx.commit().await?;
	}

	// The bitmap count never fetched a record, so it still reports the
	// indexed cardinality; the NOINDEX count reads records and finds none.
	assert_eq!(count(BITMAP_COUNT).await?, 2, "bitmap count reads index entries only");
	assert_eq!(count(NOINDEX_COUNT).await?, 0, "record-fetching count sees the deletions");
	Ok(())
}

/// Rewrites the stored `format_version` of `table`'s `index`, simulating an
/// index persisted by an older binary. The table definition is rewritten in the
/// same transaction, so cached index lists are refreshed and the next statement
/// plans and maintains the index through the layout that version selects.
///
/// The index must be empty: only the definition changes, so any data already
/// written in another layout would be left unreachable.
#[cfg(feature = "kv-mem")]
async fn downgrade_index_format(
	ds: &Datastore,
	table: &str,
	index: &str,
	format_version: u16,
) -> Result<()> {
	let tx = ds.transaction(TransactionType::Write).await?;
	let ns = tx.get_ns_by_name("test", None).await?.expect("namespace should exist");
	let db = tx.get_db_by_name("test", "test", None).await?.expect("database should exist");
	let tb: TableName = table.into();
	let ix = tx
		.get_tb_index(ns.namespace_id, db.database_id, &tb, index, None)
		.await?
		.expect("index should exist");
	let mut old = (*ix).clone();
	old.format_version = format_version;
	tx.put_tb_index(ns.namespace_id, db.database_id, &tb, &old).await?;
	// Bump the table definition so cached index lists are refreshed.
	let tb_def = tx.expect_tb(ns.namespace_id, db.database_id, &tb).await?;
	tx.put_tb("test", "test", &tb_def).await?;
	tx.commit().await?;
	Ok(())
}

/// Runs `sql` under an explicit planner strategy — the two query engines are
/// selected per session — and returns every row's record ID in sorted
/// `table:key` form. Rows may be bare record IDs (`SELECT VALUE id`) or objects
/// carrying an `id` field.
#[cfg(feature = "kv-mem")]
async fn planner_result_ids(
	ds: &Datastore,
	strategy: crate::dbs::NewPlannerStrategy,
	sql: &str,
) -> Result<Vec<String>> {
	use surrealdb_types::ToSql;

	let session = Session::owner().with_ns("test").with_db("test").new_planner_strategy(strategy);
	let mut results = ds.execute(sql, &session, None).await?;
	let value = results.remove(0).result?;
	let surrealdb_types::Value::Array(rows) = value else {
		anyhow::bail!("query returned non-array value: {value:?}");
	};
	let mut ids = Vec::with_capacity(rows.len());
	for row in rows.iter() {
		let id = match row {
			surrealdb_types::Value::Object(obj) => obj.get("id"),
			other => Some(other),
		};
		match id {
			Some(surrealdb_types::Value::RecordId(rid)) => ids.push(rid.to_sql()),
			other => anyhow::bail!("row without a record id: {other:?}"),
		}
	}
	ids.sort();
	Ok(ids)
}

/// Asserts that the engine `strategy` selects answers `sql` with exactly
/// `expected` (sorted `table:key` form).
#[cfg(feature = "kv-mem")]
async fn assert_planner_returns(
	ds: &Datastore,
	strategy: crate::dbs::NewPlannerStrategy,
	sql: &str,
	expected: &[&str],
) -> Result<()> {
	let ids = planner_result_ids(ds, strategy, sql).await?;
	assert_eq!(ids, expected, "{strategy} engine: {sql}");
	Ok(())
}

/// Asserts that both query engines answer `sql` with exactly `expected`. The
/// default `BestEffortReadOnlyStatements` strategy cannot pin an engine, so
/// each side asks for its engine explicitly.
#[cfg(feature = "kv-mem")]
async fn assert_both_planners_return(ds: &Datastore, sql: &str, expected: &[&str]) -> Result<()> {
	use crate::dbs::NewPlannerStrategy;

	assert_planner_returns(ds, NewPlannerStrategy::ComputeOnly, sql, expected).await?;
	assert_planner_returns(ds, NewPlannerStrategy::AllReadOnlyStatements, sql, expected).await?;
	Ok(())
}

/// An index defined before the doc-ID entry format (simulated by downgrading
/// `format_version`) keeps working with bare record-ID entry values: writes,
/// guarded deletes and index-backed queries stay consistent, and the table
/// never allocates shared doc-ID mappings for it.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn btree_index_pre_doc_id_format_stays_maintainable() -> Result<()> {
	use crate::catalog::BTREE_ENTRY_DOC_IDS_FORMAT_VERSION;

	let (ds, session) = new_index_test_ds().await?;
	execute_all(&ds, &session, "DEFINE INDEX idx_a ON t FIELDS a;").await?;

	// Downgrade the (empty) index definition to the pre-doc-ID format,
	// simulating an index defined by an older binary.
	downgrade_index_format(&ds, "t", "idx_a", BTREE_ENTRY_DOC_IDS_FORMAT_VERSION - 1).await?;
	let (_, _, _, ix) = get_table_index(&ds, "t", "idx_a").await?;
	assert!(!ix.uses_doc_ids());

	// Writes through the old-format definition keep the index consistent
	// (bare record-ID values, guarded deletes still match)...
	execute_all(
		&ds,
		&session,
		"CREATE t:1 SET a = 'alpha';
		 CREATE t:2 SET a = 'gamma';
		 UPDATE t:1 SET a = 'beta';
		 DELETE t:2;",
	)
	.await?;
	assert_eq!(query_array_len(&ds, &session, "SELECT * FROM t WHERE a = 'beta'").await?, 1);
	assert_eq!(query_array_len(&ds, &session, "SELECT * FROM t WHERE a = 'alpha'").await?, 0);
	assert_eq!(query_array_len(&ds, &session, "SELECT * FROM t WHERE a = 'gamma'").await?, 0);
	// ...and no shared doc-ID mappings are allocated for it.
	assert_eq!(count_doc_id_mappings(&ds, "t").await?, (0, 0));
	Ok(())
}

/// A full-text index whose stored format predates the shared table-level
/// doc-ID space (simulated by downgrading `format_version` to 0) serves
/// MATCHES under both query engines. Its maintenance stays on the private
/// per-index doc-ID layout — the table allocates no shared doc-ID mappings for
/// it — until an explicit REBUILD moves it onto the shared space, which
/// changes neither engine's answer.
///
/// MATCHES is only answerable from a full-text index, so every assertion here
/// reads the index under test.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn legacy_fulltext_index_serves_matches_under_both_planners() -> Result<()> {
	// 'bravo' is carried by t:1 and t:2 as created, by t:3 through the update,
	// and no longer by the deleted t:4.
	const MATCHES: &str = "SELECT VALUE id FROM t WHERE content @@ 'bravo'";
	const CARRIERS: &[&str] = &["t:1", "t:2", "t:3"];

	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE ANALYZER simple TOKENIZERS blank;
		 DEFINE INDEX ft ON t FIELDS content FULLTEXT ANALYZER simple BM25;",
	)
	.await?;
	downgrade_index_format(&ds, "t", "ft", 0).await?;
	let (_, _, _, ix) = get_table_index(&ds, "t", "ft").await?;
	assert!(ix.uses_doc_ids() && !ix.uses_shared_doc_ids());

	// Legacy maintenance: inserts, an update that both retracts and adds a
	// term, and a delete that retracts one.
	execute_all(
		&ds,
		&session,
		"CREATE t:1 SET content = 'alpha bravo';
		 CREATE t:2 SET content = 'bravo charlie';
		 CREATE t:3 SET content = 'delta';
		 CREATE t:4 SET content = 'bravo echo';
		 UPDATE t:3 SET content = 'bravo foxtrot';
		 DELETE t:4;",
	)
	.await?;
	assert_eq!(
		count_doc_id_mappings(&ds, "t").await?,
		(0, 0),
		"a legacy full-text index keeps its private doc-ID space"
	);
	assert_both_planners_return(&ds, MATCHES, CARRIERS).await?;

	// Compact the postings the writes appended, so the same terms have to be
	// resolved from the compacted legacy layout. `new_index_test_ds` runs
	// without maintenance tasks, so this drains the compaction queue itself.
	let (batches, errors) = Datastore::index_compaction(
		Arc::clone(&ds),
		Duration::from_secs(60),
		tokio_util::sync::CancellationToken::new(),
	)
	.await?;
	assert_eq!(errors, 0, "the legacy layout must compact cleanly");
	assert!(batches > 0, "the writes must leave the index queued for compaction");
	assert_eq!(count_doc_id_mappings(&ds, "t").await?, (0, 0));
	assert_both_planners_return(&ds, MATCHES, CARRIERS).await?;

	execute_all(&ds, &session, "REBUILD INDEX ft ON t;").await?;
	let (_, _, _, ix) = get_table_index(&ds, "t", "ft").await?;
	assert!(ix.uses_shared_doc_ids(), "the rebuild must stamp the current format");
	assert_eq!(
		count_doc_id_mappings(&ds, "t").await?,
		(3, 3),
		"the rebuilt index allocates one shared doc-ID per surviving record"
	);
	assert_both_planners_return(&ds, MATCHES, CARRIERS).await?;
	Ok(())
}

/// Coverage shared by the legacy HNSW and DiskANN tests. `define_index` must
/// define a 2-D vector index named `vx` on `t.pt` with `DIST EUCLIDEAN`, which
/// is downgraded to the pre-shared-doc-ID format while still empty.
///
/// Euclidean distances from the query point `[0, 0]`: t:1 = 1, t:6 = 1.5,
/// t:2 = 2, t:3 = 3, t:5 = 5 (the update moved it out from 0.75). The deleted
/// t:4 was the nearest of all at 0.5, so a stale entry for either the updated
/// or the deleted record would displace an expected neighbour.
///
/// 'bravo' is carried by t:1, t:2 and t:3 but not by t:6, which sits between
/// t:1 and t:2: a KNN restricted to the matching records answers `[t:1, t:2]`,
/// while one that takes the two nearest overall and filters afterwards keeps
/// only `[t:1]`. Every KNN-with-MATCHES expectation below therefore pins which
/// of the two an engine performs.
#[cfg(feature = "kv-mem")]
async fn legacy_vector_index_knn_under_both_planners(define_index: &str) -> Result<()> {
	use crate::dbs::NewPlannerStrategy::{AllReadOnlyStatements, ComputeOnly};

	// An explicit `ef` plans through the ANN index on both engines.
	const KNN: &str = "SELECT VALUE id FROM t WHERE pt <|2,40|> [0.0, 0.0]";
	// A MATCHES conjunct that cannot drive an index iterator of its own,
	// paired with the `<|k,DIST|>` KNN form.
	const OR_MATCHES_KNN: &str = "SELECT VALUE id FROM t \
		WHERE (content @@ 'bravo' OR content = 'unmatched') AND pt <|2,EUCLIDEAN|> [0.0, 0.0]";
	// A top-level MATCHES, which the current-format full-text index serves.
	const MATCHES_KNN: &str =
		"SELECT VALUE id FROM t WHERE content @@ 'bravo' AND pt <|2,40|> [0.0, 0.0]";
	const NEAREST_TWO: &[&str] = &["t:1", "t:6"];
	const NEAREST_TWO_MATCHING: &[&str] = &["t:1", "t:2"];

	let (ds, session) = new_index_test_ds().await?;
	execute_all(&ds, &session, define_index).await?;
	downgrade_index_format(&ds, "t", "vx", 0).await?;
	let (_, _, _, ix) = get_table_index(&ds, "t", "vx").await?;
	assert!(ix.uses_doc_ids() && !ix.uses_shared_doc_ids());

	// Legacy maintenance: inserts, an update that moves a vector out of the
	// nearest pair, and a delete that removes the nearest point of all.
	execute_all(
		&ds,
		&session,
		"CREATE t:1 SET pt = [1.0, 0.0], content = 'alpha bravo';
		 CREATE t:2 SET pt = [0.0, 2.0], content = 'bravo charlie';
		 CREATE t:3 SET pt = [3.0, 0.0], content = 'bravo delta';
		 CREATE t:4 SET pt = [0.5, 0.0], content = 'echo';
		 CREATE t:5 SET pt = [0.0, 0.75], content = 'foxtrot';
		 CREATE t:6 SET pt = [1.5, 0.0], content = 'zulu golf';
		 UPDATE t:5 SET pt = [0.0, 5.0];
		 DELETE t:4;",
	)
	.await?;
	assert_eq!(
		count_doc_id_mappings(&ds, "t").await?,
		(0, 0),
		"a legacy vector index keeps its private doc-ID space"
	);
	// Answered from the record-keyed pendings, which no compaction has drained
	// into the graph yet.
	assert_both_planners_return(&ds, KNN, NEAREST_TWO).await?;

	// Compact, so the same rows have to come back out of the graph the legacy
	// layout persisted. `new_index_test_ds` runs without maintenance tasks, so
	// this drains the compaction queue itself.
	let (batches, errors) = Datastore::index_compaction(
		Arc::clone(&ds),
		Duration::from_secs(60),
		tokio_util::sync::CancellationToken::new(),
	)
	.await?;
	assert_eq!(errors, 0, "the legacy layout must compact cleanly");
	assert!(batches > 0, "the writes must leave the index queued for compaction");
	assert_eq!(count_doc_id_mappings(&ds, "t").await?, (0, 0));
	assert_both_planners_return(&ds, KNN, NEAREST_TWO).await?;

	// A current-format full-text index built over the existing records opens
	// the table's shared doc-ID space. The legacy vector index keeps ignoring
	// it, so both layouts are live on one table and serve one statement
	// together.
	execute_all(&ds, &session, "DEFINE ANALYZER simple TOKENIZERS blank;").await?;
	execute_all(
		&ds,
		&session,
		"DEFINE INDEX ft ON t FIELDS content FULLTEXT ANALYZER simple BM25;",
	)
	.await?;
	assert_eq!(
		count_doc_id_mappings(&ds, "t").await?,
		(5, 5),
		"the current-format full-text index allocates one shared doc-ID per record"
	);
	assert_both_planners_return(&ds, KNN, NEAREST_TWO).await?;

	// A MATCHES the full-text index cannot answer on its own leaves the
	// compute-only engine on a table scan, where excluding the legacy vector
	// index from KNN planning hands the statement to the brute-force KNN pass.
	// The exec engine takes the same rows through a top-K over its own filtered
	// scan. Both restrict the search to the matching records.
	assert_both_planners_return(&ds, OR_MATCHES_KNN, NEAREST_TWO_MATCHING).await?;

	// A top-level MATCHES is served by the full-text index. The exec engine
	// keeps the ANN search and evaluates MATCHES per candidate — a legacy
	// vector index cannot join the shared-doc-ID bitmap prefilter, so this is
	// the path that keeps the search restricted to the matching records.
	assert_planner_returns(&ds, AllReadOnlyStatements, MATCHES_KNN, NEAREST_TWO_MATCHING).await?;
	// KNN nested under OR: the exec planner rejects the shape outright before
	// union analysis, so a multi-index OR union can never contain a KNN
	// branch — legacy or current format.
	const UNION_MATCHES_KNN: &str = "SELECT VALUE id FROM t \
		WHERE (content @@ 'bravo' AND pt <|2,40|> [0.0, 0.0]) OR content @@ 'zulu'";
	let err = planner_result_ids(&ds, AllReadOnlyStatements, UNION_MATCHES_KNN)
		.await
		.expect_err("KNN under OR must be rejected by the exec planner");
	assert!(err.to_string().contains("top level of the WHERE clause"), "unexpected error: {err}");
	// The compute-only engine accepts the shape, but MATCHES anywhere in the
	// statement excludes the legacy vector index from indexed KNN planning,
	// and the `<|k,ef|>` form has no brute-force fallback: the KNN conjunct
	// evaluates false for every row, the whole branch contributes nothing,
	// and only the 'zulu' branch survives.
	assert_planner_returns(&ds, ComputeOnly, UNION_MATCHES_KNN, &["t:6"]).await?;

	// A SELECT planned inside a function body is compiled without plan-time
	// catalog context, so its source resolves at execute time through
	// DynamicScan, which re-plans the index access per run. The legacy vector
	// index must keep the MATCHES conjunct evaluable during ANN traversal on
	// that path too.
	execute_all(
		&ds,
		&session,
		"DEFINE FUNCTION fn::knn_plain() { \
			RETURN SELECT VALUE id FROM t WHERE pt <|2,40|> [0.0, 0.0]; };
		 DEFINE FUNCTION fn::knn_matches() { \
			RETURN SELECT VALUE id FROM t \
			WHERE content @@ 'bravo' AND pt <|2,40|> [0.0, 0.0]; };",
	)
	.await?;
	assert_planner_returns(&ds, AllReadOnlyStatements, "RETURN fn::knn_plain()", NEAREST_TWO)
		.await?;
	assert_planner_returns(
		&ds,
		AllReadOnlyStatements,
		"RETURN fn::knn_matches()",
		NEAREST_TWO_MATCHING,
	)
	.await?;
	// The compute-only engine cannot combine an index-served MATCHES with a KNN
	// conjunct: the full-text index becomes its iterator, which disables the
	// brute-force KNN pass, and the KNN conjunct is then false for every row.
	// That is a property of the engine rather than of the legacy layout — the
	// rebuilt, current-format index below answers the same way.
	assert_planner_returns(&ds, ComputeOnly, MATCHES_KNN, &[]).await?;

	// The rebuild moves the vector index onto the shared space it was ignoring.
	execute_all(&ds, &session, "REBUILD INDEX vx ON t;").await?;
	let (_, _, _, ix) = get_table_index(&ds, "t", "vx").await?;
	assert!(ix.uses_shared_doc_ids(), "the rebuild must stamp the current format");
	assert_eq!(count_doc_id_mappings(&ds, "t").await?, (5, 5));
	assert_both_planners_return(&ds, KNN, NEAREST_TWO).await?;
	// The exec engine now restricts the ANN search with the shared-doc-ID
	// bitmap prefilter instead, and answers both MATCHES shapes identically.
	assert_planner_returns(&ds, AllReadOnlyStatements, MATCHES_KNN, NEAREST_TWO_MATCHING).await?;
	assert_planner_returns(&ds, AllReadOnlyStatements, OR_MATCHES_KNN, NEAREST_TWO_MATCHING)
		.await?;
	// On the compute-only engine a current-format vector index is planned for
	// every KNN, including the shapes above: MATCHES is then evaluated inside
	// the ANN traversal, where it computes to `false`. Keeping the legacy index
	// out of those plans is what the rows above depend on.
	assert_planner_returns(&ds, ComputeOnly, MATCHES_KNN, &[]).await?;
	assert_planner_returns(&ds, ComputeOnly, OR_MATCHES_KNN, &[]).await?;
	// A runtime-resolved source never carries a bitmap prefilter, so on that
	// path the MATCHES conjunct stays evaluable during traversal for the
	// rebuilt, current-format index too: the top-k is still chosen among the
	// matching records.
	assert_planner_returns(&ds, AllReadOnlyStatements, "RETURN fn::knn_plain()", NEAREST_TWO)
		.await?;
	assert_planner_returns(
		&ds,
		AllReadOnlyStatements,
		"RETURN fn::knn_matches()",
		NEAREST_TWO_MATCHING,
	)
	.await?;
	Ok(())
}

/// A current-format vector index combined with a MATCHES served by a legacy
/// (format 0) full-text index: the MATCHES conjunct cannot join the
/// shared-doc-ID bitmap prefilter, so the ANN search must keep it evaluable
/// during traversal — with no prefilter at all, and alongside an allow-list
/// built from another coverable conjunct. Distances from `[0, 0]`:
/// t:1 = 1, t:6 = 1.5, t:2 = 2, t:3 = 3; 'bravo' is carried by t:1, t:2 and
/// t:3 but not t:6, so a top-2 chosen without the text predicate would keep
/// t:6 and lose t:2.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn uncovered_matches_restricts_current_format_knn() -> Result<()> {
	use crate::dbs::NewPlannerStrategy::AllReadOnlyStatements;

	const MATCHES_KNN: &str =
		"SELECT VALUE id FROM t WHERE content @@ 'bravo' AND pt <|2,40|> [0.0, 0.0]";
	const COVERED_AND_MATCHES_KNN: &str = "SELECT VALUE id FROM t \
		WHERE flag = true AND content @@ 'bravo' AND pt <|2,40|> [0.0, 0.0]";
	const NEAREST_TWO_MATCHING: &[&str] = &["t:1", "t:2"];

	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE ANALYZER simple TOKENIZERS blank;
		 DEFINE INDEX ft ON t FIELDS content FULLTEXT ANALYZER simple BM25;",
	)
	.await?;
	downgrade_index_format(&ds, "t", "ft", 0).await?;
	execute_all(
		&ds,
		&session,
		"DEFINE INDEX vx ON t FIELDS pt HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32 EFC 16 M 4;
		 DEFINE INDEX flag_idx ON t FIELDS flag;
		 CREATE t:1 SET pt = [1.0, 0.0], content = 'alpha bravo', flag = true;
		 CREATE t:2 SET pt = [0.0, 2.0], content = 'bravo charlie', flag = true;
		 CREATE t:3 SET pt = [3.0, 0.0], content = 'bravo delta', flag = true;
		 CREATE t:6 SET pt = [1.5, 0.0], content = 'zulu golf', flag = true;",
	)
	.await?;
	let (_, _, _, vx) = get_table_index(&ds, "t", "vx").await?;
	assert!(vx.uses_shared_doc_ids(), "the vector index under test is current-format");

	// The legacy MATCHES is the only non-KNN conjunct: no prefilter can be
	// built, so the compiled MATCHES-aware condition restricts the traversal.
	assert_planner_returns(&ds, AllReadOnlyStatements, MATCHES_KNN, NEAREST_TWO_MATCHING).await?;
	// A coverable conjunct produces an allow-list that cannot include the
	// MATCHES: the compiled condition must stay active alongside it.
	assert_planner_returns(
		&ds,
		AllReadOnlyStatements,
		COVERED_AND_MATCHES_KNN,
		NEAREST_TWO_MATCHING,
	)
	.await?;
	Ok(())
}

/// An HNSW index whose stored format predates the shared table-level doc-ID
/// space (simulated by downgrading `format_version` to 0) serves KNN under
/// both query engines — on its own and alongside a MATCHES conjunct answered
/// by a current-format full-text index on the same table — and keeps its
/// private doc-ID layout until an explicit REBUILD. See
/// [`legacy_vector_index_knn_under_both_planners`] for the fixture's distances.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn legacy_hnsw_index_serves_knn_under_both_planners() -> Result<()> {
	legacy_vector_index_knn_under_both_planners(
		"DEFINE INDEX vx ON t FIELDS pt HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32 EFC 16 M 4;",
	)
	.await
}

/// DiskANN counterpart of [`legacy_hnsw_index_serves_knn_under_both_planners`]:
/// a pre-shared-doc-ID DiskANN index serves KNN under both query engines, on
/// its own and with a MATCHES conjunct, and keeps its private doc-ID layout
/// until an explicit REBUILD.
#[cfg(all(feature = "kv-mem", diskann))]
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn legacy_diskann_index_serves_knn_under_both_planners() -> Result<()> {
	legacy_vector_index_knn_under_both_planners(
		"DEFINE INDEX vx ON t FIELDS pt DISKANN DIMENSION 2 DIST EUCLIDEAN TYPE F32;",
	)
	.await
}

/// Concurrent removal of the *only* two consumers must not leak the shared
/// space. Each REMOVE independently checks whether another consumer remains, so
/// without serialization both could observe the other still present and skip the
/// purge. The table-definition write serializes them, so whichever commits last
/// sees no remaining consumer and reclaims the mappings.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[test_log::test]
async fn concurrent_removal_of_last_doc_id_indexes_purges_mappings() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE ANALYZER simple TOKENIZERS blank;
		 DEFINE INDEX ft1 ON t FIELDS a FULLTEXT ANALYZER simple BM25;
		 DEFINE INDEX ft2 ON t FIELDS b FULLTEXT ANALYZER simple BM25;
		 CREATE t:1 SET a = 'alpha', b = 'one';
		 CREATE t:2 SET a = 'beta',  b = 'two';
		 CREATE t:3 SET a = 'gamma', b = 'three';",
	)
	.await?;
	assert_eq!(count_doc_id_mappings(&ds, "t").await?, (3, 3));

	// Drop both consumers concurrently, retrying the loser of the
	// table-definition write conflict.
	let task_a = {
		let (ds, session) = (Arc::clone(&ds), session.clone());
		tokio::spawn(async move {
			execute_all_retrying_conflicts(&ds, &session, "REMOVE INDEX ft1 ON t;").await
		})
	};
	let task_b = {
		let (ds, session) = (Arc::clone(&ds), session.clone());
		tokio::spawn(async move {
			execute_all_retrying_conflicts(&ds, &session, "REMOVE INDEX ft2 ON t;").await
		})
	};
	let (ra, rb) = tokio::join!(task_a, task_b);
	ra.expect("task A panicked")?;
	rb.expect("task B panicked")?;

	// Both consumers gone → space fully reclaimed, nothing leaked.
	drain_reclaim_queue(&ds).await;
	assert_eq!(
		count_doc_id_mappings(&ds, "t").await?,
		(0, 0),
		"no leaked mappings after concurrent last-consumer removal"
	);
	Ok(())
}

/// Concurrently building two doc-ID indexes over the same pre-existing records
/// must converge on a single shared doc-ID per record (no divergence or
/// duplication): `resolve_or_assign` is serialized by the per-record `!di` write
/// conflict.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[test_log::test]
async fn concurrent_definition_of_doc_id_indexes_shares_one_space() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE ANALYZER simple TOKENIZERS blank;
		 CREATE t:1 SET a = 'alpha', b = 'one';
		 CREATE t:2 SET a = 'beta',  b = 'two';
		 CREATE t:3 SET a = 'gamma', b = 'three';",
	)
	.await?;
	// No doc-ID index yet, so the space is empty.
	assert_eq!(
		count_doc_id_mappings(&ds, "t").await?,
		(0, 0),
		"no mappings before any doc-ID index"
	);

	let task_a = {
		let (ds, session) = (Arc::clone(&ds), session.clone());
		tokio::spawn(async move {
			execute_all_retrying_conflicts(
				&ds,
				&session,
				"DEFINE INDEX ft1 ON t FIELDS a FULLTEXT ANALYZER simple BM25;",
			)
			.await
		})
	};
	let task_b = {
		let (ds, session) = (Arc::clone(&ds), session.clone());
		tokio::spawn(async move {
			execute_all_retrying_conflicts(
				&ds,
				&session,
				"DEFINE INDEX ft2 ON t FIELDS b FULLTEXT ANALYZER simple BM25;",
			)
			.await
		})
	};
	let (ra, rb) = tokio::join!(task_a, task_b);
	ra.expect("task A panicked")?;
	rb.expect("task B panicked")?;

	// Exactly one shared doc-ID per record after both builds backfill.
	assert_eq!(
		count_doc_id_mappings(&ds, "t").await?,
		(3, 3),
		"one shared doc-ID per record after concurrent index builds"
	);
	Ok(())
}

/// Multi-instance variant of the concurrent-removal test: two SurrealDB
/// instances with distinct node ids share one storage engine (the deployment
/// shape the doc-ID space must tolerate) and each drops one of the two — and
/// only — consumers concurrently. Serialization happens at the shared-storage
/// table-definition write, not in any node-local state, so the space is
/// reclaimed exactly once with no leak regardless of which node commits last.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[test_log::test]
async fn distributed_concurrent_removal_of_last_doc_id_indexes_purges_mappings() -> Result<()> {
	let (ds_a, ds_b, session) = new_distributed_index_test_ds().await?;
	execute_all(
		&ds_a,
		&session,
		"DEFINE ANALYZER simple TOKENIZERS blank;
		 DEFINE INDEX ft1 ON t FIELDS a FULLTEXT ANALYZER simple BM25;
		 DEFINE INDEX ft2 ON t FIELDS b FULLTEXT ANALYZER simple BM25;
		 CREATE t:1 SET a = 'alpha', b = 'one';
		 CREATE t:2 SET a = 'beta',  b = 'two';
		 CREATE t:3 SET a = 'gamma', b = 'three';",
	)
	.await?;
	assert_eq!(count_doc_id_mappings(&ds_a, "t").await?, (3, 3));

	// Node A drops ft1, node B drops ft2, at the same time.
	let ds_a = Arc::new(ds_a);
	let ds_b = Arc::new(ds_b);
	let task_a = {
		let (ds, session) = (Arc::clone(&ds_a), session.clone());
		tokio::spawn(async move {
			execute_all_retrying_conflicts(&ds, &session, "REMOVE INDEX ft1 ON t;").await
		})
	};
	let task_b = {
		let (ds, session) = (Arc::clone(&ds_b), session.clone());
		tokio::spawn(async move {
			execute_all_retrying_conflicts(&ds, &session, "REMOVE INDEX ft2 ON t;").await
		})
	};
	let (ra, rb) = tokio::join!(task_a, task_b);
	ra.expect("node A task panicked")?;
	rb.expect("node B task panicked")?;

	// Read from a fresh transaction: both consumers gone across both nodes, the
	// shared space is reclaimed, and nothing leaked.
	drain_reclaim_queue(&ds_a).await;
	assert_eq!(
		count_doc_id_mappings(&ds_a, "t").await?,
		(0, 0),
		"no leaked mappings after concurrent cross-instance last-consumer removal"
	);
	Ok(())
}

/// `DEFINE INDEX ... OVERWRITE` that replaces the last doc-ID consumer with a
/// non-doc-ID index must reclaim the shared space, just like `REMOVE INDEX`.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn overwrite_last_doc_id_index_with_plain_index_purges_mappings() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE ANALYZER simple TOKENIZERS blank;
		 DEFINE INDEX ix ON t FIELDS a FULLTEXT ANALYZER simple BM25;
		 CREATE t:1 SET a = 'alpha';
		 CREATE t:2 SET a = 'beta';",
	)
	.await?;
	assert_eq!(count_doc_id_mappings(&ds, "t").await?, (2, 2));

	// Overwriting the only doc-ID index with a plain b-tree index keeps the
	// space: b-tree indexes at the current format are doc-ID consumers too.
	execute_all(&ds, &session, "DEFINE INDEX OVERWRITE ix ON t FIELDS a;").await?;
	assert_eq!(
		count_doc_id_mappings(&ds, "t").await?,
		(2, 2),
		"mappings kept when OVERWRITE replaces the consumer with a b-tree consumer"
	);

	// Overwriting with a COUNT index — the only non-consumer kind — drops the
	// last consumer, so the shared space must be reclaimed.
	execute_all(&ds, &session, "DEFINE INDEX OVERWRITE ix ON t COUNT;").await?;
	drain_reclaim_queue(&ds).await;
	assert_eq!(
		count_doc_id_mappings(&ds, "t").await?,
		(0, 0),
		"mappings purged when OVERWRITE drops the last doc-ID consumer"
	);
	Ok(())
}

/// `OVERWRITE` must NOT purge when another doc-ID index still consumes the space.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn overwrite_doc_id_index_keeps_mappings_when_another_consumer_survives() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE ANALYZER simple TOKENIZERS blank;
		 DEFINE INDEX ft1 ON t FIELDS a FULLTEXT ANALYZER simple BM25;
		 DEFINE INDEX ft2 ON t FIELDS b FULLTEXT ANALYZER simple BM25;
		 CREATE t:1 SET a = 'alpha', b = 'one';
		 CREATE t:2 SET a = 'beta',  b = 'two';",
	)
	.await?;
	assert_eq!(count_doc_id_mappings(&ds, "t").await?, (2, 2));

	// Overwrite ft1 with a plain index while ft2 (doc-ID) remains a consumer.
	execute_all(&ds, &session, "DEFINE INDEX OVERWRITE ft1 ON t FIELDS a;").await?;
	assert_eq!(
		count_doc_id_mappings(&ds, "t").await?,
		(2, 2),
		"mappings survive OVERWRITE while another doc-ID index remains"
	);
	Ok(())
}

/// `OVERWRITE` must NOT purge when the replacement is itself a doc-ID index, even
/// if it is the only consumer — the space keeps being used.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn overwrite_last_doc_id_index_with_doc_id_index_keeps_mappings() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE ANALYZER simple TOKENIZERS blank;
		 DEFINE INDEX ix ON t FIELDS a FULLTEXT ANALYZER simple BM25;
		 CREATE t:1 SET a = 'alpha';
		 CREATE t:2 SET a = 'beta';",
	)
	.await?;
	assert_eq!(count_doc_id_mappings(&ds, "t").await?, (2, 2));

	// Overwrite the only doc-ID index with another doc-ID index (still full-text,
	// different analyzer): a consumer still exists, so the space is preserved and
	// the rebuild re-resolves the same ids.
	execute_all(
		&ds,
		&session,
		"DEFINE ANALYZER simple2 TOKENIZERS class;
		 DEFINE INDEX OVERWRITE ix ON t FIELDS a FULLTEXT ANALYZER simple2 BM25;",
	)
	.await?;
	assert_eq!(
		count_doc_id_mappings(&ds, "t").await?,
		(2, 2),
		"mappings kept when the replacement is itself a doc-ID index"
	);
	Ok(())
}

/// Regression guard for the delete-during-build deferral path: a record deleted
/// while a doc-ID index is building must leave the shared doc-ID space in a
/// consistent state. The delete is enqueued for the building index, so the
/// central removal is deferred and the builder's replay reclaims the mapping.
///
/// NB: with `ft1` already Online this drives the *single-builder inline* reclaim
/// (no sibling is building, so the delete's replay reclaims the mapping right
/// away). The two-builders-at-once case — where the replay defers the reclaim
/// through a durable `!dp` marker and a later sweep completes it — is covered by
/// [`concurrent_doc_id_index_build_reclaims_deferred_mapping`] (the sweep's
/// decision matrix, driven directly) and
/// [`deferred_doc_id_reclaim_survives_across_builds`] (the end-to-end wiring
/// through a real replay, a bailing sweep, and a later reclaiming build).
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn delete_during_doc_id_index_build_keeps_shared_mapping_consistent() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	// ft1 is built first, so every record already has a shared doc-ID before ft2
	// starts building.
	execute_all(
		&ds,
		&session,
		"DEFINE ANALYZER simple TOKENIZERS blank;
		 CREATE t:1 SET a = 'alpha', b = 'x';
		 CREATE t:2 SET a = 'beta',  b = 'y';
		 CREATE t:3 SET a = 'gamma', b = 'z';
		 DEFINE INDEX ft1 ON t FIELDS a FULLTEXT ANALYZER simple BM25;",
	)
	.await?;
	assert_eq!(count_doc_id_mappings(&ds, "t").await?, (3, 3));

	// Start a second doc-ID index build and delete a record while it is queued.
	let guard = start_index_build_paused(
		&ds,
		&session,
		"DEFINE INDEX ft2 ON t FIELDS b FULLTEXT ANALYZER simple BM25 CONCURRENTLY",
	)
	.await?;
	execute_all_retrying_conflicts(&ds, &session, "DELETE t:2;").await?;
	drop(guard);
	wait_for_index_ready(&ds, &session, "t", "ft2").await?;

	// Exactly the two surviving records remain mapped — the deleted record's
	// mapping was reclaimed once, not left stale.
	assert_eq!(
		count_doc_id_mappings(&ds, "t").await?,
		(2, 2),
		"delete during an index build must not leave a stale shared doc-ID mapping"
	);
	Ok(())
}

async fn read_shared_doc_id(
	ds: &Datastore,
	docs: &crate::idx::docids::TableDocIds,
	id: &RecordIdKey,
) -> Result<Option<crate::idx::docids::DocId>> {
	let tx = ds.transaction(TransactionType::Read).await?;
	let d = docs.get_doc_id(&tx, id).await?;
	tx.cancel().await?;
	Ok(d)
}

async fn read_shared_record_id(
	ds: &Datastore,
	docs: &crate::idx::docids::TableDocIds,
	doc_id: crate::idx::docids::DocId,
) -> Result<Option<RecordIdKey>> {
	let tx = ds.transaction(TransactionType::Read).await?;
	let id = docs.get_record_id(&tx, doc_id).await?;
	tx.cancel().await?;
	Ok(id)
}

/// Assigns (or resolves) a shared doc-ID for `id` in its own committed
/// transaction.
async fn assign_shared_doc_id(
	ds: &Datastore,
	docs: &crate::idx::docids::TableDocIds,
	id: &RecordIdKey,
) -> Result<crate::idx::docids::DocId> {
	let mut ctx = ds.setup_ctx()?;
	ctx.set_transaction(ds.transaction(TransactionType::Write).await?.into());
	let ctx = ctx.freeze();
	let d = docs.resolve_or_assign(&ctx, id).await?;
	ctx.tx().commit().await?;
	Ok(d)
}

/// Writes a durable `!dp` pending-reclaim marker for `id`, as the delete
/// replay's defer branch would.
async fn seed_pending_reclaim(
	ds: &Datastore,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
	id: &RecordIdKey,
) -> Result<()> {
	let tx = ds.transaction(TransactionType::Write).await?;
	tx.set_key(&DocPendingKey::new(ns, db, Cow::Borrowed(table), RecordIdentity(id.clone())), &())
		.await?;
	tx.commit().await
}

/// Writes a `!dp` marker in the encoding a previous release used for it: the
/// record id under the index format, which is what `!di` still spells, so the
/// bytes are that key's with its route tag swapped. Such a marker names an id
/// this release cannot decode.
async fn seed_pending_reclaim_in_previous_encoding(
	ds: &Datastore,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
	id: &RecordIdKey,
) -> Result<()> {
	let mut key =
		DocLookupKey::new(ns, db, Cow::Borrowed(table), Cow::Borrowed(id)).encode_key()?.to_vec();
	let tag = key
		.windows(3)
		.position(|w| w == b"!di")
		.expect("the erased forward key carries its route tag");
	key[tag..tag + 3].copy_from_slice(b"!dp");
	let tx = ds.transaction(TransactionType::Write).await?;
	tx.set(Key::from(key), Vec::new()).await?;
	tx.commit().await
}

/// Counts the `!dp` pending-reclaim markers on a table.
async fn count_pending_reclaims(
	ds: &Datastore,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
) -> Result<usize> {
	let tx = ds.transaction(TransactionType::Read).await?;
	let rng = DocPendingPrefix::new(ns, db, Cow::Borrowed(table)).range()?;
	let keys = tx.keys(rng, u32::MAX, 0, None).await?;
	tx.cancel().await?;
	Ok(keys.len())
}

/// Deterministic guard for the concurrent-build reclaim of the shared doc-ID
/// space — the two-builders-at-once case. When two doc-ID indexes build at the
/// same time, each defers a deleted record's central mapping removal through a
/// durable `!dp` marker (the sibling may still need the mapping to replay its
/// own copy of the delete); a later sweep with no building sibling must reclaim
/// it, or the `!di`/`!dd` pair is orphaned and a later re-create wrongly reuses
/// the id.
///
/// Drives [`Building::reclaim_deferred_doc_ids`] directly against a simulated
/// sibling build state to cover its whole decision matrix: markers (and
/// mappings) survive while a sibling is still building, are reclaimed once no
/// sibling is building and the record is gone, and a still-present record keeps
/// its live mapping while its stale marker is consumed. Both directions of the
/// mapping are asserted, for a plain id and for one holding a number in a
/// nested value — which the marker must address as precisely as the mapping
/// does, or the reclaim is aimed at a different record. A marker a previous
/// release wrote is translated through the erased mapping its bytes
/// address, so its reclaim is carried out rather than forfeited; one that
/// resolves to nothing is still consumed; and one whose two spellings alias —
/// which decodes cleanly as a record other than its own — is reclaimed through
/// the same chain, because decoding proves nothing about a marker's spelling. The end-to-end wiring
/// (a real replayed delete writing the marker, and a real build completion running the sweep) is
/// covered by [`deferred_doc_id_reclaim_survives_across_builds`].
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn concurrent_doc_id_index_build_reclaims_deferred_mapping() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	// Two full-text indexes on one table => two consumers of the shared !di/!dd
	// space. `ia` is our builder; `ib` stands in for the concurrent sibling.
	execute_all(
		&ds,
		&session,
		"DEFINE TABLE t SCHEMALESS;
		 DEFINE ANALYZER simple TOKENIZERS blank;
		 DEFINE INDEX ia ON t FIELDS a FULLTEXT ANALYZER simple BM25;
		 DEFINE INDEX ib ON t FIELDS b FULLTEXT ANALYZER simple BM25;",
	)
	.await?;

	let (ns, db, table, ia) = get_table_index(&ds, "t", "ia").await?;
	let (_, _, _, ib) = get_table_index(&ds, "t", "ib").await?;
	let ib_ikb = IndexKeyBase::new(ns, db, table.clone(), ib.index_id);
	let docs = crate::idx::docids::TableDocIds::new(ns, db, table.clone());

	// A record that was indexed (shared mapping assigned) then deleted during the
	// concurrent build: the mapping and its pending-reclaim marker exist but the
	// record does not.
	let gone = RecordIdKey::from("gone".to_owned());
	let assigned = assign_shared_doc_id(&ds, &docs, &gone).await?;
	assert_eq!(
		read_shared_doc_id(&ds, &docs, &gone).await?,
		Some(assigned),
		"the deleted record must start with a shared mapping"
	);
	seed_pending_reclaim(&ds, ns, db, &table, &gone).await?;

	let ia_building = new_building_for_index(&ds, &session, ns, db, &table, ia).await?;

	// (1) Sibling `ib` still Building: the sweep must leave both the mapping and
	// the durable marker in place, so `ib` can replay its own copy of the delete
	// and a later build can still complete the reclaim.
	set_durable_build_state(
		&ds,
		&ib_ikb,
		durable_build_state_for_phase(IndexBuildPhase::Building, 1, Some(Uuid::now_v7())),
	)
	.await?;
	ia_building.reclaim_deferred_doc_ids().await?;
	assert_eq!(
		read_shared_doc_id(&ds, &docs, &gone).await?,
		Some(assigned),
		"mapping must survive while a sibling doc-ID index is still building"
	);
	assert_eq!(
		count_pending_reclaims(&ds, ns, db, &table).await?,
		1,
		"the durable marker must survive a sweep that bails on a building sibling"
	);

	// (2) Sibling `ib` now Online: no doc-ID index is building any more and the
	// record is gone, so the marker seeded above (untouched by the bailed sweep)
	// is consumed and both directions of the mapping are reclaimed (no orphan).
	set_durable_build_state(
		&ds,
		&ib_ikb,
		durable_build_state_for_phase(IndexBuildPhase::Online, 1, None),
	)
	.await?;
	ia_building.reclaim_deferred_doc_ids().await?;
	assert_eq!(
		read_shared_doc_id(&ds, &docs, &gone).await?,
		None,
		"the sweep must reclaim the forward mapping once no sibling is building"
	);
	assert_eq!(
		read_shared_record_id(&ds, &docs, assigned).await?,
		None,
		"the sweep must reclaim the reverse mapping too"
	);
	assert_eq!(
		count_pending_reclaims(&ds, ns, db, &table).await?,
		0,
		"the sweep must consume the pending-reclaim marker"
	);

	// delete -> re-create now yields a *new* id (the invariant the reclaim
	// restores): the stale mapping is gone, so `resolve_or_assign` allocates afresh.
	let recreated = assign_shared_doc_id(&ds, &docs, &gone).await?;
	assert_ne!(recreated, assigned, "re-create after reclaim must allocate a fresh doc-ID");

	// (3) Re-creation safety: a record that exists again must keep its mapping
	// even if a stale marker names it — dropping it would strand its index
	// entries. The stale marker itself is consumed.
	execute_all(&ds, &session, "CREATE t:live SET a = 'alpha', b = 'beta';").await?;
	let live = RecordIdKey::from("live".to_owned());
	let live_id = read_shared_doc_id(&ds, &docs, &live).await?;
	assert!(live_id.is_some(), "the live record must have a shared mapping");
	seed_pending_reclaim(&ds, ns, db, &table, &live).await?;
	ia_building.reclaim_deferred_doc_ids().await?;
	assert_eq!(
		read_shared_doc_id(&ds, &docs, &live).await?,
		live_id,
		"a still-present (re-created) record must keep its live mapping"
	);
	assert_eq!(
		count_pending_reclaims(&ds, ns, db, &table).await?,
		0,
		"a stale marker for a live record must still be consumed"
	);

	// (4) A record id holding a number in a nested value. The marker names one
	// record, so it must round-trip the id exactly: under the index format `[1]`
	// and `[1dec]` share a key, and the id read back out of it addresses the
	// other record, whose mapping the reclaim then declines to touch — leaving
	// the marked record's mapping behind while consuming its marker.
	let nested = RecordIdKey::Array(vec![Value::Number(crate::val::Number::Int(1))].into());
	let nested_id = assign_shared_doc_id(&ds, &docs, &nested).await?;
	seed_pending_reclaim(&ds, ns, db, &table, &nested).await?;
	ia_building.reclaim_deferred_doc_ids().await?;
	assert_eq!(
		read_shared_doc_id(&ds, &docs, &nested).await?,
		None,
		"a nested-number id must have its forward mapping reclaimed like any other"
	);
	assert_eq!(
		read_shared_record_id(&ds, &docs, nested_id).await?,
		None,
		"and its reverse mapping too"
	);
	assert_eq!(
		count_pending_reclaims(&ds, ns, db, &table).await?,
		0,
		"its marker must be consumed"
	);

	// (5) A marker in the spelling a previous release used. Its id does not
	// decode as an identity here, so the record it names is recovered through
	// the erased mapping the marker's own bytes address — and the reclaim it was
	// written for is carried out rather than forfeited. `REBUILD INDEX` does not
	// clear the doc-ID space, so a forfeited mapping would survive until the
	// table's last doc-ID index was dropped.
	let legacy = RecordIdKey::Array(vec![Value::Number(crate::val::Number::Int(2))].into());
	let legacy_id = assign_shared_doc_id(&ds, &docs, &legacy).await?;
	seed_pending_reclaim_in_previous_encoding(&ds, ns, db, &table, &legacy).await?;
	let readable = RecordIdKey::from("also-gone".to_owned());
	let readable_id = assign_shared_doc_id(&ds, &docs, &readable).await?;
	seed_pending_reclaim(&ds, ns, db, &table, &readable).await?;
	assert_eq!(count_pending_reclaims(&ds, ns, db, &table).await?, 2);
	ia_building.reclaim_deferred_doc_ids().await?;
	assert_eq!(
		count_pending_reclaims(&ds, ns, db, &table).await?,
		0,
		"both spellings of a marker must be consumed, whichever the scan returned"
	);
	assert_eq!(
		read_shared_doc_id(&ds, &docs, &legacy).await?,
		None,
		"a marker in the previous spelling must still get its mapping reclaimed"
	);
	assert_eq!(read_shared_record_id(&ds, &docs, legacy_id).await?, None, "in both directions");
	assert_eq!(
		read_shared_doc_id(&ds, &docs, &readable).await?,
		None,
		"and the markers beside it are unaffected"
	);
	assert_eq!(read_shared_record_id(&ds, &docs, readable_id).await?, None);

	// A float whose exact expansion the index format cannot hold. Decoding the
	// marker's bytes rounds it to the nearest decimal, which does not encode back
	// to the key its mapping was written at, so only the bytes as they are find it.
	for float in [0.1, 0.7] {
		let id = RecordIdKey::Array(vec![Value::Number(crate::val::Number::Float(float))].into());
		let doc_id = assign_shared_doc_id(&ds, &docs, &id).await?;
		seed_pending_reclaim_in_previous_encoding(&ds, ns, db, &table, &id).await?;
		ia_building.reclaim_deferred_doc_ids().await?;
		assert_eq!(
			read_shared_doc_id(&ds, &docs, &id).await?,
			None,
			"a float id's marker in the previous spelling must reclaim its mapping"
		);
		assert_eq!(read_shared_record_id(&ds, &docs, doc_id).await?, None, "in both directions");
		assert_eq!(count_pending_reclaims(&ds, ns, db, &table).await?, 0);
	}

	// A marker whose record has since lost the type-erased key. `[7]` is gone,
	// and `[7dec]`, which the previous spelling cannot tell from it, is live and
	// took the key over when it was indexed. The chain through that key leads to
	// the live record, so the marked one is reached only as another record the
	// same spelling stands for.
	let displaced = RecordIdKey::Array(vec![Value::Number(crate::val::Number::Int(7))].into());
	let displaced_id = assign_shared_doc_id(&ds, &docs, &displaced).await?;
	execute_all(&ds, &session, "CREATE t:[7dec] SET a = 'alpha', b = 'beta';").await?;
	let twin = RecordIdKey::Array(
		vec![Value::Number(crate::val::Number::Decimal(rust_decimal::Decimal::from(7)))].into(),
	);
	let twin_id = read_shared_doc_id(&ds, &docs, &twin).await?.expect("the live twin is indexed");
	{
		let tx = ds.transaction(TransactionType::Read).await?;
		let erased = DocLookupKey::new(ns, db, Cow::Borrowed(&table), Cow::Borrowed(&displaced));
		let held = tx.get_key(&erased, None).await?;
		tx.cancel().await?;
		assert_eq!(held, Some(twin_id), "the premise: the twin holds the erased key");
	}
	seed_pending_reclaim_in_previous_encoding(&ds, ns, db, &table, &displaced).await?;
	ia_building.reclaim_deferred_doc_ids().await?;
	assert_eq!(
		read_shared_doc_id(&ds, &docs, &displaced).await?,
		None,
		"the marked record's own mapping must be reclaimed"
	);
	assert_eq!(read_shared_record_id(&ds, &docs, displaced_id).await?, None, "in both directions");
	assert_eq!(
		read_shared_doc_id(&ds, &docs, &twin).await?,
		Some(twin_id),
		"and the live record the chain reached keeps its own"
	);
	assert_eq!(count_pending_reclaims(&ds, ns, db, &table).await?, 0);

	// A marker whose mapping is already gone resolves to no record. It must still
	// be consumed, or the sweep meets it again on every pass.
	seed_pending_reclaim_in_previous_encoding(&ds, ns, db, &table, &legacy).await?;
	ia_building.reclaim_deferred_doc_ids().await?;
	assert_eq!(
		count_pending_reclaims(&ds, ns, db, &table).await?,
		0,
		"an unresolvable marker must be consumed, not left to wedge the range"
	);

	// (6) A marker whose two spellings alias. Under the previous release
	// `[2, NONE]` encodes to the same bytes as `[2]` does under this one, so this
	// marker decodes cleanly — as a record it was not written for. Decoding is
	// therefore no proof of a marker's spelling, and the sweep has to follow both
	// readings or this record's mapping is stranded while its marker is consumed.
	let aliased =
		RecordIdKey::Array(vec![Value::Number(crate::val::Number::Int(2)), Value::None].into());
	let aliased_id = assign_shared_doc_id(&ds, &docs, &aliased).await?;
	seed_pending_reclaim_in_previous_encoding(&ds, ns, db, &table, &aliased).await?;
	// The premise: it decodes, and as a different record.
	{
		let tx = ds.transaction(TransactionType::Read).await?;
		let rng = DocPendingPrefix::new(ns, db, Cow::Borrowed(&table)).range()?;
		let keys = tx.keys(rng, u32::MAX, 0, None).await?;
		tx.cancel().await?;
		let decoded = DocPendingKey::decode_key(&keys[0]).expect("the aliased spelling decodes");
		assert!(
			!decoded.id.0.addresses_same_record(&aliased),
			"and decodes as a different record, which is what makes it dangerous"
		);
	}
	ia_building.reclaim_deferred_doc_ids().await?;
	assert_eq!(
		read_shared_doc_id(&ds, &docs, &aliased).await?,
		None,
		"an aliased marker must still reclaim the mapping of the record it names"
	);
	assert_eq!(read_shared_record_id(&ds, &docs, aliased_id).await?, None);
	assert_eq!(count_pending_reclaims(&ds, ns, db, &table).await?, 0);

	Ok(())
}

/// End-to-end wiring of the deferred doc-ID reclaim: a delete replayed by a
/// real builder while a sibling doc-ID index is (durably) Building must write
/// the `!dp` marker in the replay transaction, the builder's own end-of-build
/// sweep must leave it untouched (sibling still building), and a *later* doc-ID
/// index build — here a `REBUILD`, which never saw the delete — must complete
/// the reclaim from the durable marker alone.
///
/// This pins both halves of the production wiring (the defer branch in
/// `apply_appending` and the sweep call at the end of a build) and the
/// keys-survive-across-builds property that makes a late-starting or restarted
/// build a valid reclaimer.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn deferred_doc_id_reclaim_survives_across_builds() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	// ft1 indexes the records first, so every record has a shared doc-ID before
	// ft2 starts building; ft3 is a real Online doc-ID index whose durable build
	// state is forged to Building to stand in for a concurrent sibling.
	execute_all(
		&ds,
		&session,
		"DEFINE ANALYZER simple TOKENIZERS blank;
		 CREATE t:1 SET a = 'alpha', b = 'x';
		 CREATE t:2 SET a = 'beta',  b = 'y';
		 CREATE t:3 SET a = 'gamma', b = 'z';
		 DEFINE INDEX ft1 ON t FIELDS a FULLTEXT ANALYZER simple BM25;
		 DEFINE INDEX ft3 ON t FIELDS b FULLTEXT ANALYZER simple BM25;",
	)
	.await?;
	assert_eq!(count_doc_id_mappings(&ds, "t").await?, (3, 3));
	let (ns, db, table, _) = get_table_index(&ds, "t", "ft3").await?;
	let ft3_ikb = {
		let (_, _, _, ft3) = get_table_index(&ds, "t", "ft3").await?;
		IndexKeyBase::new(ns, db, table.clone(), ft3.index_id)
	};
	// Snapshot ft3's real (Online) build state so it can be restored below.
	let ft3_online = durable_build_state(&ds, &ft3_ikb).await?;

	// Start ft2's build and pause it; forge ft3 as still Building, then delete a
	// record. The delete is enqueued into ft2's replay queue, and when ft2's
	// build resumes, its replay sees a "building" sibling and defers the reclaim
	// through a durable marker; ft2's own end-of-build sweep bails on it too.
	let guard = start_index_build_paused(
		&ds,
		&session,
		"DEFINE INDEX ft2 ON t FIELDS b FULLTEXT ANALYZER simple BM25 CONCURRENTLY",
	)
	.await?;
	set_durable_build_state(
		&ds,
		&ft3_ikb,
		durable_build_state_for_phase(
			IndexBuildPhase::Building,
			ft3_online.generation,
			Some(Uuid::now_v7()),
		),
	)
	.await?;
	execute_all_retrying_conflicts(&ds, &session, "DELETE t:2;").await?;
	drop(guard);
	wait_for_index_ready(&ds, &session, "t", "ft2").await?;

	// The mapping is retained (the "building" sibling may still need it) and the
	// deferral is durable.
	assert_eq!(
		count_doc_id_mappings(&ds, "t").await?,
		(3, 3),
		"the deleted record's mapping must be retained while a sibling builds"
	);
	assert_eq!(
		count_pending_reclaims(&ds, ns, db, &table).await?,
		1,
		"the replayed delete must leave a durable pending-reclaim marker"
	);

	// Restore ft3 to its real Online state: no doc-ID index is building.
	set_durable_build_state(&ds, &ft3_ikb, ft3_online).await?;

	// A later build — a REBUILD that never saw the delete — completes the
	// reclaim from the durable marker alone.
	execute_all(&ds, &session, "REBUILD INDEX ft2 ON t;").await?;
	assert_eq!(
		count_doc_id_mappings(&ds, "t").await?,
		(2, 2),
		"the next doc-ID index build must reclaim the deferred mapping"
	);
	assert_eq!(
		count_pending_reclaims(&ds, ns, db, &table).await?,
		0,
		"the reclaiming sweep must consume the durable marker"
	);
	Ok(())
}

/// A queued delete whose builder never replays it must still be reclaimed: the
/// delete transaction itself writes the durable `!dp` marker at the moment the
/// central removal is deferred (see `doc::index`'s `defer_doc_id_removal`), so
/// the obligation survives the builder and its queues. Here the building index
/// is removed while paused — its queued copy of the delete is retired without
/// ever replaying — and the next doc-ID index build completes the reclaim from
/// the marker alone.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn deferred_doc_id_reclaim_survives_builder_removal() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE ANALYZER simple TOKENIZERS blank;
		 CREATE t:1 SET a = 'alpha', b = 'x';
		 CREATE t:2 SET a = 'beta',  b = 'y';
		 CREATE t:3 SET a = 'gamma', b = 'z';
		 DEFINE INDEX ft1 ON t FIELDS a FULLTEXT ANALYZER simple BM25;",
	)
	.await?;
	assert_eq!(count_doc_id_mappings(&ds, "t").await?, (3, 3));
	let (ns, db, table, _) = get_table_index(&ds, "t", "ft1").await?;

	// ft2's build is paused; the delete is enqueued into its replay queue and
	// the central removal is deferred. The delete transaction must leave the
	// durable marker immediately — before any replay runs.
	let guard = start_index_build_paused(
		&ds,
		&session,
		"DEFINE INDEX ft2 ON t FIELDS b FULLTEXT ANALYZER simple BM25 CONCURRENTLY",
	)
	.await?;
	execute_all_retrying_conflicts(&ds, &session, "DELETE t:2;").await?;
	assert_eq!(
		count_pending_reclaims(&ds, ns, db, &table).await?,
		1,
		"the delete transaction must write the durable marker at deferral time"
	);
	assert_eq!(count_doc_id_mappings(&ds, "t").await?, (3, 3));

	// Remove ft2 while its build is still paused: the queued delete is retired
	// with the index and never replays. ft1 remains a consumer, so the shared
	// space is not purged — the marker must carry the reclaim obligation.
	execute_all_retrying_conflicts(&ds, &session, "REMOVE INDEX ft2 ON t;").await?;
	drop(guard);
	assert_eq!(
		count_pending_reclaims(&ds, ns, db, &table).await?,
		1,
		"the marker must survive a builder that never replayed the delete"
	);
	assert_eq!(count_doc_id_mappings(&ds, "t").await?, (3, 3));

	// The next doc-ID index build completes the reclaim from the marker alone.
	execute_all(&ds, &session, "REBUILD INDEX ft1 ON t;").await?;
	assert_eq!(
		count_doc_id_mappings(&ds, "t").await?,
		(2, 2),
		"the next doc-ID index build must reclaim the never-replayed delete's mapping"
	);
	assert_eq!(count_pending_reclaims(&ds, ns, db, &table).await?, 0);
	Ok(())
}

/// Runs a KNN query and returns the JSON-encoded result rows.
#[cfg(feature = "kv-mem")]
async fn knn_result(ds: &Datastore, session: &Session, sql: &str) -> Result<String> {
	let mut responses = ds.execute(sql, session, None).await?;
	let value = responses.remove(0).result?;
	Ok(value.into_json_value().to_string())
}

/// A record deleted and re-created before HNSW compaction must remain visible
/// to KNN search. The record-keyed pending captures the pre-delete doc-ID; the
/// delete removes that shared mapping, so a pending hit emitted under the
/// captured id could not be resolved back to a record and would be silently
/// dropped. The pending search must emit the surviving vectors under the record
/// key instead (see `search_pendings`).
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn hnsw_pending_search_returns_recreated_record() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	// The full-text index assigns the shared doc-ID at write time, so the HNSW
	// record pending captures it (`doc_id: Some(..)`).
	execute_all(
		&ds,
		&session,
		"DEFINE ANALYZER simple TOKENIZERS blank;
		 DEFINE INDEX ft ON t FIELDS a FULLTEXT ANALYZER simple BM25;
		 DEFINE INDEX hx ON t FIELDS vec HNSW DIMENSION 4 DIST EUCLIDEAN TYPE F32;
		 CREATE t:1 SET a = 'alpha', vec = [1.0, 0.0, 0.0, 0.0];",
	)
	.await?;

	// Delete (drops the shared mapping) then re-create before any compaction:
	// the coalesced record pending still carries the stale captured doc-ID.
	// NB: no KNN query runs before this point — resolving the captured id while
	// its mapping is still live would warm the process-local doc-ID cache and
	// mask the stale-mapping resolution this test pins down.
	execute_all(
		&ds,
		&session,
		"DELETE t:1;
		 CREATE t:1 SET a = 'alpha', vec = [1.0, 0.0, 0.0, 0.0];",
	)
	.await?;
	let knn = "SELECT id FROM t WHERE vec <|1,40|> [1.0, 0.0, 0.0, 0.0];";
	assert!(
		knn_result(&ds, &session, knn).await?.contains("t:1"),
		"a record re-created before compaction must remain visible to KNN"
	);
	Ok(())
}

/// DiskANN counterpart of
/// [`hnsw_pending_search_returns_recreated_record`]: the sharded pending search
/// must emit a re-created record's vectors under the record key, not the stale
/// captured doc-ID.
#[cfg(all(feature = "kv-mem", diskann))]
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn diskann_pending_search_returns_recreated_record() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE ANALYZER simple TOKENIZERS blank;
		 DEFINE INDEX ft ON t FIELDS a FULLTEXT ANALYZER simple BM25;
		 DEFINE INDEX dx ON t FIELDS vec DISKANN DIMENSION 4 DIST EUCLIDEAN TYPE F32;
		 CREATE t:1 SET a = 'alpha', vec = [1.0, 0.0, 0.0, 0.0];",
	)
	.await?;
	// NB: no KNN query runs before the delete — see the HNSW counterpart for
	// why (a warm doc-ID cache would mask the stale-mapping resolution).
	execute_all(
		&ds,
		&session,
		"DELETE t:1;
		 CREATE t:1 SET a = 'alpha', vec = [1.0, 0.0, 0.0, 0.0];",
	)
	.await?;
	let knn = "SELECT id FROM t WHERE vec <|1,40|> [1.0, 0.0, 0.0, 0.0];";
	assert!(
		knn_result(&ds, &session, knn).await?.contains("t:1"),
		"a record re-created before compaction must remain visible to KNN"
	);
	Ok(())
}

/// A shutdown-class commit failure must not poison the durable build state.
///
/// Reproduces the incident behind the PR #553 regression report: the storage
/// engine begins graceful shutdown while a build is mid-scan, the batch
/// commit fails with the engines' shutdown error, and the builder task dies.
/// Durable state must stay `Building` — with no durable error — so the
/// periodic resume scan adopts the build after restart and finishes it.
/// Before this fix the state was durably `Error`, the resume scan skipped it
/// forever, and every write to the table failed on admission with the stale
/// shutdown message.
#[tokio::test(flavor = "multi_thread")]
async fn shutdown_error_does_not_poison_durable_build_state() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:one SET email = 'one@example.com' RETURN NONE;
			CREATE user:two SET email = 'two@example.com' RETURN NONE;
			DEFINE INDEX test ON user FIELDS email CONCURRENTLY;
			",
	)
	.await?;
	wait_for_index_ready(&ds, &session, "user", "test").await?;

	// Fail the rebuild's first initial-scan batch commit with the error the
	// storage engines return once graceful shutdown has begun.
	let _guard = inject_non_retryable_error(
		NonRetryableErrorSite::ConcurrentIndexInitialBatchShutdown,
		ds.id(),
	);
	// A blocking REBUILD surfaces the builder task's failure synchronously.
	let err = ds
		.execute("REBUILD INDEX test ON user", &session, None)
		.await?
		.remove(0)
		.result
		.expect_err("rebuild should fail with the injected shutdown error");
	assert!(err.to_string().contains("shutting down"), "unexpected rebuild error: {err}");

	// The interrupted generation stays adoptable: still `Building`, no
	// durable error, no error report status.
	let (ns, db, table, ix) = get_table_index(&ds, "user", "test").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(state.phase, IndexBuildPhase::Building);
	assert_eq!(state.error, None);
	assert_eq!(state.report_status, Some(IndexBuildReportStatus::Indexing));

	// Writes are still admitted (queued) while the build awaits adoption.
	execute_all_retrying_conflicts(
		&ds,
		&session,
		"CREATE user:three SET email = 'three@example.com' RETURN NONE",
	)
	.await?;

	// Expire the dead builder's lease; the periodic resume scan must adopt
	// the generation and drive the build to `Online`.
	let expired = Utc::now() - chrono::Duration::seconds(BUILD_OWNER_LEASE_SECS + 5);
	let mut stranded = durable_build_state(&ds, &ikb).await?;
	stranded.updated_at = expired;
	stranded.owner_heartbeat_at = Some(expired);
	set_durable_build_state(&ds, &ikb, stranded).await?;
	let resumed = ds
		.resume_stalled_index_builds(
			Duration::from_secs(30),
			tokio_util::sync::CancellationToken::new(),
		)
		.await?;
	assert_eq!(resumed, 1, "the interrupted build should be adopted");
	wait_for_index_ready(&ds, &session, "user", "test").await?;
	Ok(())
}

/// A genuine (non-shutdown) build failure must still publish a durable error:
/// the shutdown classification must not swallow real failures.
#[tokio::test(flavor = "multi_thread")]
async fn non_shutdown_build_error_still_publishes_durable_error() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:one SET email = 'one@example.com' RETURN NONE;
			CREATE user:two SET email = 'two@example.com' RETURN NONE;
			DEFINE INDEX test ON user FIELDS email CONCURRENTLY;
			",
	)
	.await?;
	wait_for_index_ready(&ds, &session, "user", "test").await?;

	let _guard = inject_non_retryable_error(
		NonRetryableErrorSite::ConcurrentIndexInitialBatchCommit,
		ds.id(),
	);
	let err = ds
		.execute("REBUILD INDEX test ON user", &session, None)
		.await?
		.remove(0)
		.result
		.expect_err("rebuild should fail with the injected error");
	assert!(err.to_string().contains("injected non-retryable error"), "unexpected error: {err}");

	let (ns, db, table, ix) = get_table_index(&ds, "user", "test").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(state.phase, IndexBuildPhase::Error);
	assert_eq!(state.report_status, Some(IndexBuildReportStatus::Error));
	assert!(
		state.error.as_deref().is_some_and(|e| e.contains("injected non-retryable error")),
		"durable error should carry the failure reason: {:?}",
		state.error
	);

	// A failed build must not block user writes: the mutation queues under
	// the errored generation instead of failing the statement.
	execute_all_retrying_conflicts(
		&ds,
		&session,
		"CREATE user:three SET email = 'three@example.com' RETURN NONE",
	)
	.await?;

	// A `REBUILD INDEX` recovers from the durable error state by starting a
	// fresh generation (the documented operator remediation), and its rescan
	// indexes the write that was admitted while the build was errored.
	execute_all(&ds, &session, "REBUILD INDEX test ON user").await?;
	assert_eq!(index_building_status(&ds, &session, "user", "test").await?, "ready");
	assert_eq!(
		index_prefix_key_count(&ds, ns, db, &table, ix.index_id).await?,
		3,
		"the rebuild rescan must index the write admitted during the error state"
	);
	Ok(())
}

#[test]
fn shutdown_error_classification() {
	use crate::kvs::is_shutdown_error;
	let direct: anyhow::Error = crate::kvs::Error::Shutdown.into();
	assert!(is_shutdown_error(&direct));
	let wrapped: anyhow::Error = Error::Kvs(crate::kvs::Error::Shutdown).into();
	assert!(is_shutdown_error(&wrapped));
	let internal: anyhow::Error = crate::kvs::Error::Internal("boom".to_string()).into();
	assert!(!is_shutdown_error(&internal));
	let conflict: anyhow::Error = crate::kvs::Error::TransactionConflict("busy".to_string()).into();
	assert!(!is_shutdown_error(&conflict));
}

/// A memory-threshold failure must not poison the durable build state.
///
/// On memory-constrained instances the builder can cross the process memory
/// threshold mid-scan (`Building::is_beyond_threshold`). That is a
/// load-transient condition, so — like a shutdown — it must leave the build
/// adoptable: durable state stays `Building` with its checkpoint, and the
/// resume scan retries the build once the owner lease expires (typically
/// after the pressure has receded or the instance was resized). Before this
/// fix the build was durably `Error` and every write to the table failed on
/// admission.
#[tokio::test(flavor = "multi_thread")]
async fn memory_threshold_error_does_not_poison_durable_build_state() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:one SET email = 'one@example.com' RETURN NONE;
			CREATE user:two SET email = 'two@example.com' RETURN NONE;
			DEFINE INDEX test ON user FIELDS email CONCURRENTLY;
			",
	)
	.await?;
	wait_for_index_ready(&ds, &session, "user", "test").await?;

	// Fail the rebuild's first initial-scan batch commit with the error the
	// builder raises when the process crosses the memory threshold.
	let _guard = inject_non_retryable_error(
		NonRetryableErrorSite::ConcurrentIndexInitialBatchMemoryThreshold,
		ds.id(),
	);
	let err = ds
		.execute("REBUILD INDEX test ON user", &session, None)
		.await?
		.remove(0)
		.result
		.expect_err("rebuild should fail with the injected memory threshold error");
	assert!(err.to_string().contains("memory threshold"), "unexpected rebuild error: {err}");

	// The interrupted generation stays adoptable: still `Building`, no
	// durable error, no error report status.
	let (ns, db, table, ix) = get_table_index(&ds, "user", "test").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(state.phase, IndexBuildPhase::Building);
	assert_eq!(state.error, None);
	assert_eq!(state.report_status, Some(IndexBuildReportStatus::Indexing));

	// Expire the dead builder's lease; the resume scan retries the build.
	let expired = Utc::now() - chrono::Duration::seconds(BUILD_OWNER_LEASE_SECS + 5);
	let mut stranded = durable_build_state(&ds, &ikb).await?;
	stranded.updated_at = expired;
	stranded.owner_heartbeat_at = Some(expired);
	set_durable_build_state(&ds, &ikb, stranded).await?;
	let resumed = ds
		.resume_stalled_index_builds(
			Duration::from_secs(30),
			tokio_util::sync::CancellationToken::new(),
		)
		.await?;
	assert_eq!(resumed, 1, "the interrupted build should be adopted");
	wait_for_index_ready(&ds, &session, "user", "test").await?;
	Ok(())
}

/// The resume scan recovers several stranded builds one at a time.
///
/// A restart with multiple stalled builds (the incident shape: two FULLTEXT
/// indexes on a small instance) must not start every initial scan at once:
/// a pass adopts at most one build, and no pass adopts anything while a
/// local builder task is still running. Every stalled build is still
/// recovered — one scan pass after the previous build finishes.
#[tokio::test(flavor = "multi_thread")]
async fn resume_scan_adopts_one_stalled_build_at_a_time() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:one SET email = 'one@example.com', name = 'One' RETURN NONE;
			CREATE user:two SET email = 'two@example.com', name = 'Two' RETURN NONE;
			DEFINE INDEX test_email ON user FIELDS email;
			DEFINE INDEX test_name ON user FIELDS name;
			",
	)
	.await?;

	// Strand both indexes as expired `Building` generations, as a crash mid
	// build of both would leave them.
	let expired = Utc::now() - chrono::Duration::seconds(BUILD_OWNER_LEASE_SECS + 5);
	let mut ikbs = Vec::new();
	for index in ["test_email", "test_name"] {
		let (ns, db, table, ix) = get_table_index(&ds, "user", index).await?;
		let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
		let tx = ds.transaction(TransactionType::Write).await?;
		tx.del_prefix_key(&IdxRoot {
			ns,
			db,
			tb: Cow::Borrowed(&table),
			ix: ix.index_id,
		})
		.await?;
		tx.set_key(
			&ikb.new_bs_key(),
			&IndexBuildState {
				generation: 2,
				phase: IndexBuildPhase::Building,
				owner: Some(Uuid::new_v4()),
				next_ticket: 0,
				initial_complete: false,
				updated_at: expired,
				owner_heartbeat_at: Some(expired),
				error: None,
				report_status: Some(IndexBuildReportStatus::Indexing),
				initial: Some(0),
				updated: None,
				pending: None,
				initial_cursor: None,
			},
		)
		.await?;
		tx.commit().await?;
		ikbs.push(ikb);
	}

	// Park whichever build gets adopted in its batch-commit retry loop, so
	// the deferral while a build is running can be observed deterministically.
	let guard = inject_retryable_conflicts(
		RetryableConflictSite::ConcurrentIndexInitialBatch,
		ds.id(),
		REPEATED_RETRY_CONFLICTS,
	);

	// The first pass adopts exactly one of the two stranded builds.
	let resumed = ds
		.resume_stalled_index_builds(
			Duration::from_secs(30),
			tokio_util::sync::CancellationToken::new(),
		)
		.await?;
	assert_eq!(resumed, 1, "a pass must adopt at most one stalled build");

	// Wait until the adopted build is demonstrably mid-scan (it consumed an
	// injected conflict), then verify a pass adopts nothing while it runs.
	timeout(Duration::from_secs(10), async {
		while retryable_conflict_count(RetryableConflictSite::ConcurrentIndexInitialBatch, ds.id())
			== REPEATED_RETRY_CONFLICTS
		{
			sleep(Duration::from_millis(5)).await;
		}
	})
	.await
	.map_err(|_| anyhow::anyhow!("adopted build never reached its batch commit"))?;
	let resumed_while_running = ds
		.resume_stalled_index_builds(
			Duration::from_secs(30),
			tokio_util::sync::CancellationToken::new(),
		)
		.await?;
	assert_eq!(resumed_while_running, 0, "no adoption while a local build is running");

	// Un-park the running build and let recovery converge: a later pass
	// adopts the second build, and both indexes come back ready.
	drop(guard);
	let deadline = Instant::now() + Duration::from_secs(30);
	loop {
		let mut online = 0;
		for ikb in &ikbs {
			if durable_build_state(&ds, ikb).await?.phase == IndexBuildPhase::Online {
				online += 1;
			}
		}
		if online == 2 {
			break;
		}
		assert!(Instant::now() < deadline, "stranded builds were not recovered sequentially");
		let _ = ds
			.resume_stalled_index_builds(
				Duration::from_secs(30),
				tokio_util::sync::CancellationToken::new(),
			)
			.await?;
		sleep(Duration::from_millis(20)).await;
	}
	assert_eq!(index_building_status(&ds, &session, "user", "test_email").await?, "ready");
	assert_eq!(index_building_status(&ds, &session, "user", "test_name").await?, "ready");
	Ok(())
}

/// A new-generation takeover installs the next generation BEFORE it waits
/// for prior-generation reservations, and wipes the stale queues after.
///
/// With `Error` builds admitting writers like `Building`, draining before
/// the flip would race a writer that reserves a ticket between the drain and
/// the state commit: its queued mutation would be wiped while its main-table
/// write could land after the new initial scan had already passed the
/// record. Installing the generation first fences those admissions (ticket
/// allocation CASes `!bs` and the fence rejects generation mismatches), so
/// the drain's empty state is stable.
#[tokio::test(flavor = "multi_thread")]
async fn takeover_installs_generation_before_draining_reservations() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:one SET email = 'one@example.com' RETURN NONE;
			DEFINE INDEX test ON user FIELDS email;
			",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "user", "test").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);

	// Seed an errored generation 1 with a live writer reservation (TTL in the
	// future, so the drain must wait) and a stale queue entry.
	let tx = ds.transaction(TransactionType::Write).await?;
	tx.set_key(
		&ikb.new_bs_key(),
		&IndexBuildState {
			generation: 1,
			phase: IndexBuildPhase::Error,
			owner: None,
			next_ticket: 1,
			initial_complete: false,
			updated_at: Utc::now(),
			owner_heartbeat_at: None,
			error: Some("seeded test failure".to_string()),
			report_status: Some(IndexBuildReportStatus::Error),
			initial: None,
			updated: None,
			pending: None,
			initial_cursor: None,
		},
	)
	.await?;
	let br_key = ikb.new_br_key(1, 0);
	tx.set_key(
		&br_key,
		&IndexBuildReservation {
			node: ds.id(),
			expires_at: Utc::now() + chrono::Duration::seconds(BUILD_RESERVATION_TTL_SECS),
		},
	)
	.await?;
	// A different ticket than the live reservation: a queue entry under the
	// same ticket would mark that writer as already committed and let the
	// drain retire the reservation instead of blocking on it.
	tx.set_key(
		&ikb.new_bg_key(1, 5, 0),
		&Appending {
			old_values: None,
			new_values: None,
			id: RecordIdKey::from("one".to_string()),
			count_cond_match: None,
		},
	)
	.await?;
	tx.commit().await?;

	// The takeover must block draining the live reservation...
	let building =
		Arc::new(new_building_for_index(&ds, &session, ns, db, &table, Arc::clone(&ix)).await?);
	let acquire = {
		let building = Arc::clone(&building);
		tokio::spawn(async move { building.acquire_build_state().await })
	};
	// ...but only after the next generation is already installed, fencing
	// off further old-generation admissions while it waits.
	timeout(Duration::from_secs(5), async {
		while durable_build_state(&ds, &ikb).await?.generation != 2 {
			sleep(Duration::from_millis(10)).await;
		}
		Ok::<_, anyhow::Error>(())
	})
	.await
	.map_err(|_| anyhow::anyhow!("takeover never installed the next generation"))??;
	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(state.phase, IndexBuildPhase::Building);
	sleep(Duration::from_millis(200)).await;
	assert!(
		!acquire.is_finished(),
		"acquire must keep draining the live prior-generation reservation"
	);

	// Releasing the writer's reservation lets the takeover finish, and the
	// stale generation-1 queue entries are wiped.
	let tx = ds.transaction(TransactionType::Write).await?;
	tx.del_key(&br_key).await?;
	tx.commit().await?;
	let acquired = timeout(Duration::from_secs(5), acquire)
		.await
		.map_err(|_| {
			anyhow::anyhow!("acquire did not finish after the reservation was released")
		})???
		.expect("takeover should acquire the new generation");
	assert_eq!(acquired.generation, 2);
	let tx = ds.transaction(TransactionType::Read).await?;
	let stale_bg = tx.keys(ikb.new_bg_all_generations_range()?, u32::MAX, 0, None).await?;
	let stale_br = tx.keys(ikb.new_br_all_generations_range()?, u32::MAX, 0, None).await?;
	tx.cancel().await?;
	assert!(stale_bg.is_empty(), "stale generation-1 queue entries should be wiped");
	assert!(stale_br.is_empty(), "no reservations should remain after the takeover");
	Ok(())
}

/// Budget for the under-load phase of [`concurrent_build_under_table_writes`].
///
/// Every arm shares it: a control given a larger budget than the arms it is a
/// control for stops establishing that the harness completes a build at this
/// write rate. Unstarved runs finish the scan in a couple of seconds, so the
/// margin is for loaded CI. These are liveness tests, not throughput tests.
const BUILD_UNDER_WRITES_TIMEOUT: Duration = Duration::from_secs(45);

/// Budget for publishing once the writer has stopped.
const BUILD_PUBLISH_TIMEOUT: Duration = Duration::from_secs(30);

/// Outcome of a [`concurrent_build_under_table_writes`] run.
struct BuildUnderWritesOutcome {
	/// Whether the scan covered every seeded record while the writer ran.
	scan_progressed: bool,
	/// Whether the index published `Online` once the writer stopped.
	published: bool,
	/// Highest `initial` counter the durable state reported.
	max_initial: u64,
	/// Highest writer ticket allocated while the build was still `Building`.
	/// Zero means the writes never produced an index mutation, so they never
	/// entered admission. Sampling stops at `Closing`, whose transition
	/// advances the counter itself to fence in-flight allocations.
	max_ticket: u64,
	/// Writes the writer committed while the build ran.
	committed: u64,
}

/// Drive a concurrent FULLTEXT build while the table it indexes takes writes at
/// a sustained rate, and report whether the initial scan ever completes.
///
/// `writer_sql` decides the shape of the concurrent write. The build is only
/// defined once the writer is demonstrably committing, so the scan starts
/// against established load instead of racing the writer's ramp-up. The writer
/// runs for the whole build and is stopped before the assertions.
async fn concurrent_build_under_table_writes(
	writer_sql: fn(u64) -> String,
	scan_timeout: Duration,
) -> Result<BuildUnderWritesOutcome> {
	/// Records seeded before the build starts. An unstarved build indexes
	/// these in a couple of seconds, so the timeout is a wide margin.
	const RECORDS: usize = 4000;
	/// Writes the writer must commit before the build is defined.
	const WRITES_BEFORE_BUILD: u64 = 50;

	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE doc SCHEMALESS;
			DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;
			",
	)
	.await?;
	execute_all(
		&ds,
		&session,
		&format!(
			"CREATE |doc:1..{RECORDS}| SET text = string::repeat('lorem ipsum dolor sit amet \
			 consectetur adipiscing elit sed do eiusmod tempor incididunt ut labore ', 4) + \
			 <string>id RETURN NONE"
		),
	)
	.await?;

	let stop = Arc::new(AtomicBool::new(false));
	let committed = Arc::new(AtomicU64::new(0));
	let writer = {
		let ds = Arc::clone(&ds);
		let session = session.clone();
		let stop = Arc::clone(&stop);
		let committed = Arc::clone(&committed);
		tokio::spawn(async move {
			let mut n = 0u64;
			while !stop.load(Ordering::Relaxed) {
				n += 1;
				// Statement-level conflicts are expected under this load and are
				// not what these tests are about; only committed writes count as
				// pressure the builder has to make progress against.
				if execute_all(&ds, &session, &writer_sql(n)).await.is_ok() {
					committed.fetch_add(1, Ordering::Relaxed);
				}
				sleep(Duration::from_millis(1)).await;
			}
		})
	};

	timeout(Duration::from_secs(10), async {
		while committed.load(Ordering::Relaxed) < WRITES_BEFORE_BUILD {
			sleep(Duration::from_millis(5)).await;
		}
	})
	.await
	.map_err(|_| anyhow::anyhow!("the writer never reached the pre-build write threshold"))?;

	// The writer touches the table definition's cached index list, so the
	// schema statement can hit statement-level conflicts under this load.
	execute_all_retrying_conflicts(
		&ds,
		&session,
		"DEFINE INDEX ft ON doc FIELDS text FULLTEXT ANALYZER simple BM25(1.2,0.75) HIGHLIGHTS \
		 CONCURRENTLY",
	)
	.await?;

	let (ns, db, table, ix) = get_table_index(&ds, "doc", "ft").await?;
	let ikb = IndexKeyBase::new(ns, db, table, ix.index_id);

	// Phase one, the starvation check: with the writer at full rate the scan
	// must cover at least every seeded record. A starved build sits at zero.
	//
	// The threshold is a count rather than completion because an insert writer
	// keeps extending the scanned range, so "the scan finished" is not a fixed
	// target and a fast enough writer can outrun it indefinitely on slower
	// hardware. Covering the seeded records is bounded and is what
	// distinguishes progress from starvation.
	let mut max_initial = 0u64;
	let mut max_ticket = 0u64;
	let progressed = timeout(scan_timeout, async {
		loop {
			let (state, ticket) = durable_build_state_with_ticket_counter(&ds, &ikb).await?;
			max_initial = max_initial.max(state.initial.unwrap_or(0));
			// Tickets are allocated from the generation's `!bt` counter;
			// `next_ticket` only advances for a generation that predates it.
			// Reading the build state alone would report zero for every
			// admission and make the control assertion below vacuous.
			//
			// Only sample while the build is still `Building`: the transition
			// out of it advances the counter itself, to fence allocations in
			// flight, and that bump is not a writer admission. Phase and
			// counter therefore have to come from one snapshot, which `state`
			// above provides — the fence writes both keys in a single
			// transaction, so two reads can pair a `Building` phase with the
			// post-fence counter and report that bump as an admission.
			if state.phase == IndexBuildPhase::Building {
				max_ticket = max_ticket.max(ticket.unwrap_or(state.next_ticket));
			}
			if max_initial >= RECORDS as u64 || state.phase == IndexBuildPhase::Online {
				return Ok::<_, anyhow::Error>(());
			}
			sleep(Duration::from_millis(20)).await;
		}
	})
	.await;

	stop.store(true, Ordering::Relaxed);
	let _ = writer.await;

	// `timeout` nests the polling result inside its own, so only the outer
	// error means "it did not happen in time". A failure reading durable state
	// is a broken test rather than a starved build, and collapsing the two
	// would let these tests pass without ever observing progress.
	let scan_progressed = match progressed {
		Ok(Ok(())) => true,
		Ok(Err(err)) => return Err(err),
		Err(_elapsed) => false,
	};

	// Phase two: with the writer stopped the build must reach `Online`. The
	// writer is stopped first so the target is bounded — `Closing` under live
	// write pressure is covered deterministically by
	// `closing_transition_fences_in_flight_ticket_allocation`, and folding both
	// into one timing-sensitive test is what made this one fragile on slower
	// runners.
	let published = timeout(BUILD_PUBLISH_TIMEOUT, async {
		loop {
			let state = durable_build_state(&ds, &ikb).await?;
			max_initial = max_initial.max(state.initial.unwrap_or(0));
			if state.phase == IndexBuildPhase::Online {
				return Ok::<_, anyhow::Error>(());
			}
			sleep(Duration::from_millis(20)).await;
		}
	})
	.await;
	let published = match published {
		Ok(Ok(())) => true,
		Ok(Err(err)) => return Err(err),
		Err(_elapsed) => false,
	};

	let committed = committed.load(Ordering::Relaxed);
	assert!(
		committed > 100,
		"the writer only committed {committed} writes — too little load for this test to mean \
		 anything"
	);
	Ok(BuildUnderWritesOutcome {
		scan_progressed,
		published,
		max_initial,
		max_ticket,
		committed,
	})
}

/// Control for the two starvation tests below: writes that do not touch an
/// indexed field must not disturb the build.
///
/// Such a write produces no index mutation, so it never enters writer
/// admission and never allocates a ticket — asserted here, so the test cannot
/// silently become a vacuous "writes are harmless" claim. It establishes that
/// the harness drives a build to completion at this write rate, which is what
/// makes the two failures below attributable to admission rather than to load.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_build_is_unaffected_by_writes_that_miss_the_index() -> Result<()> {
	let outcome = concurrent_build_under_table_writes(
		|n| format!("UPDATE doc:{} SET untracked = {n} RETURN NONE", n % 25 + 1),
		BUILD_UNDER_WRITES_TIMEOUT,
	)
	.await?;
	assert_eq!(
		outcome.max_ticket, 0,
		"writes that miss the index must not enter admission (allocated {} tickets)",
		outcome.max_ticket
	);
	assert!(
		outcome.scan_progressed,
		"the scan stalled under {} writes that miss the index: {}/4000 records",
		outcome.committed, outcome.max_initial
	);
	assert!(outcome.published, "the build did not publish after the writer stopped");
	Ok(())
}

/// A concurrent index build must keep making progress while the table it is
/// indexing takes writes that mutate the indexed field.
///
/// Every write that produces an index mutation allocates a durable admission
/// ticket, and that allocation is a compare-and-swap on the index's single
/// `!bs` build-state key. The builder's initial-scan batch reads and
/// CAS-writes that same key twice in one transaction — the ownership heartbeat
/// before the batch and the progress checkpoint after it — and holds that
/// transaction open across the whole batch, analysis included. Any admitted
/// write committing inside that window invalidates the builder's CAS, so the
/// entire batch is discarded and retried after a fixed backoff.
///
/// Once a batch is long relative to the write inter-arrival time the builder
/// loses every race: the initial scan never commits, the durable counter never
/// advances, and each failed attempt still pays the full analysis and
/// write-set cost for the batch. The stall also makes the generation look
/// abandoned, because the heartbeat rides on the discarded transaction — a
/// starved builder never refreshes `owner_heartbeat_at`, so its lease expires
/// while the task is still running, leaving the generation open to a takeover
/// that restarts the scan from its last committed checkpoint, or from zero
/// when no batch of that generation ever committed.
///
/// Asserted in two phases. With the writer at full rate the scan must cover
/// every seeded record — a starved build sits at zero, which is the defect.
/// The writer is then stopped and the build must reach `Online`, which covers
/// `Closing`, the reservation drain and the publish.
///
/// Progress is a count rather than "the scan finished" because an insert writer
/// keeps extending the scanned range, so completion is not a fixed target and a
/// fast writer can outrun it indefinitely on slower hardware.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_build_publishes_under_indexed_updates() -> Result<()> {
	let outcome = concurrent_build_under_table_writes(
		|n| format!("UPDATE doc:{} SET text = 'changed payload {n}' RETURN NONE", n % 25 + 1),
		BUILD_UNDER_WRITES_TIMEOUT,
	)
	.await?;
	assert!(
		outcome.max_ticket > 0,
		"indexed updates must enter writer admission, otherwise this test exercises nothing"
	);
	assert!(
		outcome.scan_progressed,
		"the scan stalled under {} indexed updates ({} tickets allocated): it indexed {}/4000 \
		 records",
		outcome.committed, outcome.max_ticket, outcome.max_initial
	);
	assert!(outcome.published, "the build did not publish after the writer stopped");
	Ok(())
}

/// The insert-shaped counterpart of
/// [`concurrent_build_publishes_under_indexed_updates`], and the
/// shape reported from production: `RELATE` inserts new edge records, so a
/// RELATION table under normal traffic starves any build against it.
///
/// Kept separate because an insert also extends the scanned key range and
/// creates a fresh doc-ID mapping, so it exercises more of the write path than
/// an in-place update of an indexed field.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_build_publishes_under_indexed_inserts() -> Result<()> {
	let outcome = concurrent_build_under_table_writes(
		|n| format!("CREATE doc:w{n} SET text = 'writer payload {n}' RETURN NONE"),
		BUILD_UNDER_WRITES_TIMEOUT,
	)
	.await?;
	assert!(
		outcome.max_ticket > 0,
		"indexed inserts must enter writer admission, otherwise this test exercises nothing"
	);
	assert!(
		outcome.scan_progressed,
		"the scan stalled under {} indexed inserts ({} tickets allocated): it indexed {}/4000 \
		 records",
		outcome.committed, outcome.max_ticket, outcome.max_initial
	);
	assert!(outcome.published, "the build did not publish after the writer stopped");
	Ok(())
}

/// Read the ticket counter of one build generation, if it exists.
async fn durable_ticket_counter(
	ds: &Datastore,
	ikb: &IndexKeyBase,
	generation: BuildGeneration,
) -> Result<Option<BuildTicket>> {
	let tx = ds.transaction(TransactionType::Read).await?;
	let counter = catch!(tx, tx.get_key(&ikb.new_bt_key(generation), None).await);
	tx.cancel().await?;
	Ok(counter)
}

/// The active generation always owns a `!bt` ticket counter, and installing the
/// next generation removes the previous one.
///
/// Writer admission compare-and-swaps this counter, so both halves matter: the
/// key must exist for a writer to CAS, and the flip must remove it under a
/// conditional delete so an in-flight allocation conflicts instead of landing a
/// reservation against a generation that is no longer current.
#[tokio::test(flavor = "multi_thread")]
async fn generation_flip_rotates_the_ticket_counter() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:one SET email = 'one@example.com' RETURN NONE;
			DEFINE INDEX test ON user FIELDS email;
			",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "user", "test").await?;
	let ikb = IndexKeyBase::new(ns, db, table, ix.index_id);

	let first = durable_build_state(&ds, &ikb).await?.generation;
	assert!(
		durable_ticket_counter(&ds, &ikb, first).await?.is_some(),
		"the active generation must own a ticket counter for admission to CAS"
	);

	execute_all(&ds, &session, "REBUILD INDEX test ON user").await?;

	let second = durable_build_state(&ds, &ikb).await?.generation;
	assert_eq!(second, first.saturating_add(1));
	assert!(
		durable_ticket_counter(&ds, &ikb, second).await?.is_some(),
		"the rebuilt generation must own a ticket counter"
	);
	assert!(
		durable_ticket_counter(&ds, &ikb, first).await?.is_none(),
		"the flip must remove the previous generation's counter, which is what fences writers \
		 still admitting under it"
	);

	// Retiring the index clears every generation's counter along with the
	// rest of the durable build state.
	execute_all(&ds, &session, "REMOVE INDEX test ON user").await?;
	assert!(durable_ticket_counter(&ds, &ikb, second).await?.is_none());
	Ok(())
}

/// A generation that predates the `!bt` counter keeps allocating tickets from
/// `!bs.next_ticket`.
///
/// This is the upgrade path: a build already in flight when the node restarts
/// on a version that owns a counter has no `!bt`, and must keep issuing tickets
/// exactly as before rather than restarting the sequence from zero and reusing
/// reservations. Such a build only moves to the counter at its next generation.
#[tokio::test(flavor = "multi_thread")]
async fn admission_falls_back_to_build_state_ticket_without_a_counter() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:one SET email = 'one@example.com' RETURN NONE;
			DEFINE INDEX test ON user FIELDS email;
			",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "user", "test").await?;
	let ikb = IndexKeyBase::new(ns, db, table, ix.index_id);

	// Recreate a pre-counter build: `Building` with a ticket sequence already
	// part-way through, and no `!bt` for the generation.
	let generation = durable_build_state(&ds, &ikb).await?.generation;
	let mut legacy = durable_build_state_for_phase(IndexBuildPhase::Building, generation, None);
	legacy.next_ticket = 7;
	set_durable_build_state(&ds, &ikb, legacy).await?;
	let tx = ds.transaction(TransactionType::Write).await?;
	tx.del_key(&ikb.new_bt_key(generation)).await?;
	tx.commit().await?;

	execute_all_retrying_conflicts(
		&ds,
		&session,
		"UPDATE user:one SET email = 'changed@example.com' RETURN NONE",
	)
	.await?;

	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(
		state.next_ticket, 8,
		"a generation without a counter must advance the build state's own ticket"
	);
	assert!(
		durable_ticket_counter(&ds, &ikb, generation).await?.is_none(),
		"the legacy path must not create a counter mid-generation: nodes still running the \
		 previous version would keep allocating from `!bs.next_ticket` and collide with it"
	);
	Ok(())
}

/// Entering `Closing` must invalidate a ticket allocation that is already in
/// flight.
///
/// Admission reads the phase, allocates from the generation's counter, and
/// commits its `!br` reservation. If `Closing` could commit in between, the
/// reservation could land after the drain that follows had already seen an
/// empty range, and the writer's fence — which queues on `Closing` — would
/// write a `!bg` entry after the final replay pass. The build would then
/// publish `Online` having never applied that mutation.
///
/// The phase transition therefore rewrites the counter in its own transaction,
/// so the in-flight allocation loses the race and retries against `Closing`.
#[tokio::test(flavor = "multi_thread")]
async fn closing_transition_fences_in_flight_ticket_allocation() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:one SET email = 'one@example.com' RETURN NONE;
			DEFINE INDEX test ON user FIELDS email;
			",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "user", "test").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);

	// Strand a `Building` generation with an expired lease and a counter that
	// has already issued tickets, then take it over so the transition below is
	// owned by this builder.
	const GENERATION: BuildGeneration = 9;
	let expired = Utc::now() - chrono::Duration::seconds(BUILD_OWNER_LEASE_SECS + 5);
	let mut state = durable_build_state_for_phase(IndexBuildPhase::Building, GENERATION, None);
	state.updated_at = expired;
	state.owner_heartbeat_at = Some(expired);
	set_durable_build_state(&ds, &ikb, state).await?;
	let tx = ds.transaction(TransactionType::Write).await?;
	tx.set_key(&ikb.new_bt_key(GENERATION), &5u64).await?;
	tx.commit().await?;

	let build = new_building_for_index(&ds, &session, ns, db, &table, Arc::clone(&ix)).await?;
	let acquired = build
		.acquire_build_state()
		.await?
		.expect("the expired generation should be available for takeover");
	assert_eq!(acquired.generation, GENERATION);

	// A writer mid-admission: it has allocated ticket 5 but not yet committed.
	let writer = ds.transaction(TransactionType::Write).await?;
	let bt = ikb.new_bt_key(GENERATION);
	let ticket = catch!(writer, writer.get_key(&bt, None).await).expect("counter should exist");
	assert_eq!(ticket, 5);
	catch!(writer, writer.put_compare_key(&bt, &(ticket + 1), Some(&ticket)).await);

	// The build closes while that allocation is still open.
	build.mark_durable_closing(GENERATION).await?;
	assert_eq!(durable_build_state(&ds, &ikb).await?.phase, IndexBuildPhase::Closing);

	assert!(
		writer.commit().await.is_err(),
		"a ticket allocation in flight across the `Closing` transition must not commit: its \
		 reservation could land after the drain and be missed"
	);
	Ok(())
}

/// Drive a takeover of a fabricated `Building` generation to completion and
/// return the result, so the publish-time checks can be exercised directly.
#[allow(clippy::too_many_arguments)]
async fn run_takeover_of_generation(
	ds: &Datastore,
	session: &Session,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
	ix: Arc<IndexDefinition>,
	generation: BuildGeneration,
	next_ticket: BuildTicket,
	counter: Option<BuildTicket>,
) -> Result<Result<()>> {
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	let expired = Utc::now() - chrono::Duration::seconds(BUILD_OWNER_LEASE_SECS + 5);
	let mut state = durable_build_state_for_phase(IndexBuildPhase::Building, generation, None);
	state.next_ticket = next_ticket;
	state.initial_complete = false;
	state.updated_at = expired;
	state.owner_heartbeat_at = Some(expired);
	set_durable_build_state(ds, &ikb, state).await?;

	let tx = ds.transaction(TransactionType::Write).await?;
	match counter {
		Some(counter) => tx.set_key(&ikb.new_bt_key(generation), &counter).await?,
		None => tx.del_key(&ikb.new_bt_key(generation)).await?,
	}
	tx.commit().await?;

	let build = new_building_for_index(ds, session, ns, db, table, ix).await?;
	let acquired = build
		.acquire_build_state()
		.await?
		.expect("the expired generation should be available for takeover");
	assert_eq!(acquired.generation, generation);
	Ok(build.run_acquired(acquired).await)
}

/// A generation whose tickets were issued by two different allocators must not
/// be published.
///
/// A `!bt` counter alongside a non-zero `next_ticket` is proof that a node
/// predating the counter allocated against this generation. The two sequences
/// both start at zero and never conflict, so they can issue the same ticket and
/// one writer's queued mutation silently overwrites the other's. Publishing
/// would leave an index reporting `ready` while missing writes, so the build
/// fails and points at `REBUILD INDEX` instead.
#[tokio::test(flavor = "multi_thread")]
async fn cross_version_ticket_allocation_blocks_publishing() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:one SET email = 'one@example.com' RETURN NONE;
			DEFINE INDEX test ON user FIELDS email;
			",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "user", "test").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);

	let result =
		run_takeover_of_generation(&ds, &session, ns, db, &table, ix, 4, 3, Some(7)).await?;

	let err = result.expect_err("a generation with two ticket allocators must not publish");
	let message = err.to_string();
	assert!(
		message.contains("REBUILD INDEX test ON user"),
		"the failure must tell the operator how to recover, got: {message}"
	);
	assert_ne!(
		durable_build_state(&ds, &ikb).await?.phase,
		IndexBuildPhase::Online,
		"the index must not be queryable when its queue may have lost mutations"
	);
	Ok(())
}

/// A generation that predates the counter still publishes normally.
///
/// Such a build legitimately advances `next_ticket`, and has no `!bt`. Treating
/// that as a cross-version collision would fail every build that was already
/// running when the node was upgraded.
#[tokio::test(flavor = "multi_thread")]
async fn legacy_generation_with_advanced_ticket_still_publishes() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:one SET email = 'one@example.com' RETURN NONE;
			DEFINE INDEX test ON user FIELDS email;
			",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "user", "test").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);

	run_takeover_of_generation(&ds, &session, ns, db, &table, ix, 4, 3, None).await??;

	assert_eq!(durable_build_state(&ds, &ikb).await?.phase, IndexBuildPhase::Online);
	Ok(())
}

/// Installing a new generation must invalidate a ticket allocation that is
/// already in flight against the previous one.
///
/// The flip removes the previous generation's counter under a conditional
/// delete, in the same transaction that installs the new state. That is the
/// whole fence: allocation compare-and-swaps that counter, so a writer holding
/// an open allocation loses and retries against the new generation.
///
/// Without it the writer's `!br` can commit *after* the flip — in the window
/// while `wait_for_prior_generation_reservations` is draining — so the drain
/// can return on an empty range that the late reservation then repopulates.
/// The stale-queue wipe that follows destroys the writer's `!bg` while its
/// main-table write survives, and the new scan may already have passed that
/// record.
///
/// The later sweep in `delete_stale_build_queues` also removes the counter, so
/// a test that only checks it is eventually gone passes either way. This one
/// asserts the timing: gone *by the time the flip commits*.
#[tokio::test(flavor = "multi_thread")]
async fn generation_flip_fences_in_flight_ticket_allocation() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:one SET email = 'one@example.com' RETURN NONE;
			DEFINE INDEX test ON user FIELDS email;
			",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "user", "test").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);

	// An errored generation 1 that still owns its counter, plus a live
	// reservation so the takeover's drain blocks after the flip. That pause is
	// the window a late allocation would otherwise slip through.
	let br_key = ikb.new_br_key(1, 0);
	let tx = ds.transaction(TransactionType::Write).await?;
	tx.set_key(
		&ikb.new_bs_key(),
		&IndexBuildState {
			generation: 1,
			phase: IndexBuildPhase::Error,
			owner: None,
			next_ticket: 0,
			initial_complete: false,
			updated_at: Utc::now(),
			owner_heartbeat_at: None,
			error: Some("seeded test failure".to_string()),
			report_status: Some(IndexBuildReportStatus::Error),
			initial: None,
			updated: None,
			pending: None,
			initial_cursor: None,
		},
	)
	.await?;
	tx.set_key(&ikb.new_bt_key(1), &5u64).await?;
	tx.set_key(
		&br_key,
		&IndexBuildReservation {
			node: ds.id(),
			expires_at: Utc::now() + chrono::Duration::seconds(BUILD_RESERVATION_TTL_SECS),
		},
	)
	.await?;
	tx.commit().await?;

	// A writer mid-admission against generation 1: ticket read, CAS staged,
	// not yet committed.
	let writer = ds.transaction(TransactionType::Write).await?;
	let bt = ikb.new_bt_key(1);
	let ticket = catch!(writer, writer.get_key(&bt, None).await).expect("counter should exist");
	catch!(writer, writer.put_compare_key(&bt, &(ticket + 1), Some(&ticket)).await);

	// The takeover installs generation 2 and then blocks draining the live
	// reservation.
	let building =
		Arc::new(new_building_for_index(&ds, &session, ns, db, &table, Arc::clone(&ix)).await?);
	let acquire = {
		let building = Arc::clone(&building);
		tokio::spawn(async move { building.acquire_build_state().await })
	};
	timeout(Duration::from_secs(5), async {
		while durable_build_state(&ds, &ikb).await?.generation != 2 {
			sleep(Duration::from_millis(10)).await;
		}
		Ok::<_, anyhow::Error>(())
	})
	.await
	.map_err(|_| anyhow::anyhow!("takeover never installed the next generation"))??;
	assert!(
		!acquire.is_finished(),
		"the takeover should still be draining, which is the window under test"
	);

	assert!(
		writer.commit().await.is_err(),
		"an allocation in flight across the generation flip must not commit: its reservation \
		 would land after the drain and its queued mutation would be wiped"
	);

	// Let the takeover finish so the task does not outlive the test.
	let tx = ds.transaction(TransactionType::Write).await?;
	tx.del_key(&br_key).await?;
	tx.commit().await?;
	timeout(Duration::from_secs(5), acquire)
		.await
		.map_err(|_| {
			anyhow::anyhow!("takeover did not finish after the reservation was released")
		})???
		.expect("takeover should acquire the new generation");
	Ok(())
}

/// A compaction plan prepared before an index's data was wiped must never
/// apply, however faithfully the wiped state is recreated afterwards.
///
/// This is what the generation guard in `bump_compaction_generation` exists to
/// enforce, and a rebuild's clean phase is the one caller that can defeat it.
/// The generation key lives inside the subspace the wipe clears, so clearing it
/// returns the index to the state a never-compacted index is in — which is
/// exactly the state a plan prepared before the wipe expects, so its
/// conditional write matches again. The per-key guards behind it match too,
/// because a rebuild re-indexes the same records and an HNSW pending is derived
/// wholly from the record and the graph state, carrying no writer identity. The
/// stale plan would then consume the rebuild's pendings and fold them into a
/// graph loaded from the pre-wipe snapshot, leaving the index `Online` and
/// empty.
///
/// The pendings are restored from their own bytes rather than by running a
/// rebuild, because the contract under test is that a pre-wipe plan cannot
/// apply — not that a rebuild happens to reproduce those bytes exactly.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn a_compaction_plan_prepared_before_a_wipe_cannot_apply() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE INDEX hx ON t FIELDS vec HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32;
		 CREATE t:1 SET vec = [1.0, 0.0];
		 CREATE t:2 SET vec = [0.0, 1.0];",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "t", "hx").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	let Index::Hnsw(params) = &ix.index else {
		panic!("the fixture must define an HNSW index");
	};

	// Read phase, over the pendings the writes above left behind. Nothing has
	// compacted this index, so the plan's expected generation is absent — the
	// value a wipe restores.
	let tx = Arc::new(ds.transaction(TransactionType::Read).await?);
	let pendings = catch!(tx, tx.scan_raw(ikb.new_hr_range()?, u32::MAX, 0, None).await);
	let mut ctx = ds.setup_ctx()?;
	ctx.set_transaction(Arc::clone(&tx));
	let ctx = ctx.freeze();
	let plan = IndexOperation::prepare_hnsw_compaction(&ctx, &ikb).await?;
	tx.cancel().await?;
	assert!(plan.has_work(), "the fixture must leave pendings for the plan to capture");
	assert_eq!(pendings.len(), 2, "one pending per indexed record");

	// A rebuild's clean phase, followed by the pendings it would rewrite.
	let tx = ds.transaction(TransactionType::Write).await?;
	catch!(tx, crate::idx::wipe_index_data(&tx, &ikb, &ix.index).await);
	for (key, value) in &pendings {
		catch!(tx, tx.set(Key::from(key.clone()), value.clone()).await);
	}
	tx.commit().await?;

	// Write phase of the stale plan.
	let tx = Arc::new(ds.transaction(TransactionType::Write).await?);
	let mut ctx = ds.setup_ctx()?;
	ctx.set_transaction(Arc::clone(&tx));
	let ctx = ctx.freeze();
	let applied = IndexOperation::apply_hnsw_compaction(
		&ctx,
		ctx.get_index_stores(),
		&ikb,
		params,
		crate::catalog::DOC_IDS_FORMAT_VERSION,
		plan,
	)
	.await?;
	tx.cancel().await?;
	assert!(!applied.applied(), "a plan prepared before the wipe must be rejected, not applied");

	// The pendings must survive for whoever compacts next.
	let tx = ds.transaction(TransactionType::Read).await?;
	let surviving = catch!(tx, tx.count(ikb.new_hr_range()?, None).await);
	tx.cancel().await?;
	assert_eq!(surviving, pendings.len(), "the rejected plan must leave every pending in place");
	Ok(())
}

/// A pre-filtered KNN search (#548) whose WHERE is fully index-coverable
/// performs zero record fetches inside the ANN search, on both the exact and
/// the graph tier: EXPLAIN ANALYZE (unredacted under `Session::owner()`)
/// reports the chosen `prefilter_tier` and no in-traversal `fetched:`
/// counter. The contrast query filters on an indexed column with no declared
/// field kind — not provably exact, so it stays an in-traversal residual and
/// reports its per-candidate record fetches.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn knn_prefilter_performs_zero_in_traversal_fetches() -> Result<()> {
	use surrealdb_types::Value as PublicValue;

	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE FIELD category ON t TYPE string;
		 DEFINE INDEX idx_category ON t FIELDS category;
		 DEFINE INDEX idx_rawcat ON t FIELDS rawcat;
		 DEFINE INDEX hn_pt ON t FIELDS point HNSW DIMENSION 1;",
	)
	.await?;
	// 2200 category-'a' rows push that allow-list over the exact-tier
	// threshold (default 2000) onto the graph tier; the 200 'b' rows stay on
	// the exact tier.
	execute_all(
		&ds,
		&session,
		"FOR $i IN 0..2400 {
			CREATE type::record('t', $i) SET
				point = [ <float> $i ],
				category = IF $i % 12 < 11 { 'a' } ELSE { 'b' },
				rawcat = IF $i % 12 < 11 { 'a' } ELSE { 'b' };
		};",
	)
	.await?;

	let explain = |sql: &'static str| {
		let ds = &ds;
		let session = &session;
		async move {
			let mut results = ds.execute(sql, session, None).await?;
			let value = results.remove(0).result?;
			match value {
				PublicValue::String(plan) => Ok::<String, anyhow::Error>(plan),
				other => anyhow::bail!("unexpected EXPLAIN result: {other:?}"),
			}
		}
	};

	// Exact tier (200 members): zero in-traversal fetches.
	let plan =
		explain("EXPLAIN ANALYZE SELECT id FROM t WHERE category = 'b' AND point <|3,40|> [0f]")
			.await?;
	assert!(plan.contains("prefilter_tier: exact"), "expected exact tier:\n{plan}");
	assert!(!plan.contains("fetched:"), "no in-traversal fetches expected:\n{plan}");

	// Graph tier (2200 members): admission is pure bitmap membership — still
	// zero in-traversal fetches.
	let plan =
		explain("EXPLAIN ANALYZE SELECT id FROM t WHERE category = 'a' AND point <|3,40|> [0f]")
			.await?;
	assert!(plan.contains("prefilter_tier: graph"), "expected graph tier:\n{plan}");
	assert!(!plan.contains("fetched:"), "no in-traversal fetches expected:\n{plan}");

	// Contrast: `rawcat` has no declared field kind, so its equality is not
	// provably exact and remains an in-traversal residual — the filter
	// fetches candidate records and says so.
	let plan =
		explain("EXPLAIN ANALYZE SELECT id FROM t WHERE rawcat = 'b' AND point <|3,40|> [0f]")
			.await?;
	assert!(!plan.contains("prefilter_tier"), "no prefilter expected:\n{plan}");
	assert!(plan.contains("fetched:"), "in-traversal fetches expected:\n{plan}");
	Ok(())
}

/// The bytes a previous release wrote a `!bp` marker at: this release's key up
/// to the record id, then the id under the index format — which is what `!di`
/// still spells, so that key supplies the suffix.
fn primary_append_key_in_previous_encoding(
	ikb: &IndexKeyBase,
	generation: BuildGeneration,
	id: &RecordIdKey,
) -> Result<Vec<u8>> {
	use crate::key::schema::BuildPrimaryGenerationPrefix;
	let (ns, db, table) = (ikb.ns(), ikb.db(), ikb.table());
	let head = BuildPrimaryGenerationPrefix {
		ns,
		db,
		tb: Cow::Borrowed(table),
		ix: ikb.index(),
		generation,
	}
	.encode_bound()?
	.to_vec();
	let erased = DocLookupPrefix::new(ns, db, Cow::Borrowed(table)).encode_bound()?.to_vec();
	let di = DocLookupKey::new(ns, db, Cow::Borrowed(table), Cow::Borrowed(id)).encode_key()?;
	let mut key = head;
	key.extend_from_slice(&di[erased.len()..]);
	Ok(key)
}

/// A `!bp` marker written by a previous release must sit at this release's key
/// for its record before the initial scan reads it.
///
/// That release encoded the record id under the index format, which drops a
/// nested value's numeric variant. The scan looks a marker up by the key it
/// builds for the record, so one left in the old spelling is missed and its
/// queued mutation baselined as though it did not exist. A build taken over
/// from an owner on that release keeps its generation, and with it these keys.
///
/// Also covers a queue wider than one page, because the rebuild carries its
/// cursor across commits and a page stepped past uncommitted leaves records
/// without a marker, and a key whose two spellings alias — which decodes
/// cleanly as a record other than its own.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn primary_append_markers_from_a_previous_release_are_rebuilt() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE TABLE t SCHEMALESS;
		 DEFINE ANALYZER simple TOKENIZERS blank;
		 DEFINE INDEX ia ON t FIELDS a FULLTEXT ANALYZER simple BM25;",
	)
	.await?;
	let (ns, db, table, ia) = get_table_index(&ds, "t", "ia").await?;
	let ix_id = ia.index_id;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix_id);
	let building = new_building_for_index(&ds, &session, ns, db, &table, ia).await?;

	// A queued mutation for a record whose id holds a nested number, with its
	// marker in the spelling the previous release used.
	let generation: BuildGeneration = 1;
	let id = RecordIdKey::Array(vec![Value::Number(crate::val::Number::Int(1))].into());
	let (ticket, mutation_seq) = (7, 0);
	{
		let tx = ds.transaction(TransactionType::Write).await?;
		tx.set_key(
			&ikb.new_bg_key(generation, ticket, mutation_seq),
			&Appending {
				old_values: None,
				new_values: None,
				id: id.clone(),
				count_cond_match: None,
			},
		)
		.await?;
		let legacy = primary_append_key_in_previous_encoding(&ikb, generation, &id)?;
		tx.set(
			Key::from(legacy),
			PrimaryAppendingTicket {
				ticket,
				mutation_seq,
			}
			.kv_encode_value()?,
		)
		.await?;
		tx.commit().await?;
	}

	// The scan cannot see it: the key it builds for this record is this
	// release's spelling, which is not the key on disk.
	{
		let tx = ds.transaction(TransactionType::Read).await?;
		assert!(
			tx.get_key(&ikb.new_bp_key(generation, &id), None).await?.is_none(),
			"the marker must start out invisible to a lookup by record"
		);
		tx.cancel().await?;
	}

	building.build_generation.store(generation, Ordering::Release);
	own_build(&ds, &ikb, &building, generation).await?;
	building.rebuild_primary_appendings().await?;

	let tx = ds.transaction(TransactionType::Read).await?;
	let ptr = tx.get_key(&ikb.new_bp_key(generation, &id), None).await?;
	let ptr = ptr.expect("the marker must now be found by a lookup for its own record");
	assert_eq!(
		(ptr.ticket, ptr.mutation_seq),
		(ticket, mutation_seq),
		"and still point at the same queued mutation"
	);
	let all = tx.keys(ikb.new_bp_range(generation)?, u32::MAX, 0, None).await?;
	assert_eq!(all.len(), 1, "the old spelling must not be left beside the new one");
	tx.cancel().await?;

	// More queued mutations than one page holds, so the rebuild has to carry its
	// cursor across commits. A page it steps past without committing would leave
	// its records without a marker, which is the failure the scan cannot see.
	let span = surrealdb_kvs::consts::INDEXING_BATCH_SIZE as usize + 50;
	{
		let tx = ds.transaction(TransactionType::Write).await?;
		for i in 0..span {
			let id = RecordIdKey::Array(
				vec![Value::Number(crate::val::Number::Int(1000 + i as i64))].into(),
			);
			let (ticket, mutation_seq) = (1000 + i as u64, 0);
			tx.set_key(
				&ikb.new_bg_key(generation, ticket, mutation_seq),
				&Appending {
					old_values: None,
					new_values: None,
					id: id.clone(),
					count_cond_match: None,
				},
			)
			.await?;
			tx.set(
				Key::from(primary_append_key_in_previous_encoding(&ikb, generation, &id)?),
				PrimaryAppendingTicket {
					ticket,
					mutation_seq,
				}
				.kv_encode_value()?,
			)
			.await?;
		}
		tx.commit().await?;
	}
	building.rebuild_primary_appendings().await?;

	let tx = ds.transaction(TransactionType::Read).await?;
	for i in 0..span {
		let id = RecordIdKey::Array(
			vec![Value::Number(crate::val::Number::Int(1000 + i as i64))].into(),
		);
		assert!(
			tx.get_key(&ikb.new_bp_key(generation, &id), None).await?.is_some(),
			"marker {i} must be found by a lookup for its own record"
		);
	}
	let all = tx.keys(ikb.new_bp_range(generation)?, u32::MAX, 0, None).await?;
	assert_eq!(
		all.len(),
		span + 1,
		"every marker must end up in exactly one spelling, across page boundaries"
	);
	tx.cancel().await?;

	// A marker whose two spellings alias. Under the previous release `[3, NONE]`
	// encodes to the same bytes as `[3]` does under this one — the `NONE` tag
	// lands where the number's kind would have been — so this key decodes
	// cleanly, as the wrong record. Decoding is therefore no evidence of which
	// spelling a key is in, and only the queued mutation settles it. `[3]` has
	// no queued mutation, so it must come out of the rebuild with no marker.
	let three = RecordIdKey::Array(vec![Value::Number(crate::val::Number::Int(3))].into());
	let aliased =
		RecordIdKey::Array(vec![Value::Number(crate::val::Number::Int(3)), Value::None].into());
	let (ticket, mutation_seq) = (99, 0);
	{
		let tx = ds.transaction(TransactionType::Write).await?;
		tx.set_key(
			&ikb.new_bg_key(generation, ticket, mutation_seq),
			&Appending {
				old_values: None,
				new_values: None,
				id: aliased.clone(),
				count_cond_match: None,
			},
		)
		.await?;
		let legacy = primary_append_key_in_previous_encoding(&ikb, generation, &aliased)?;
		// The premise: it decodes, and not as the record it was written for.
		let decoded = crate::key::schema::BuildPrimaryKey::decode_key(&legacy)
			.expect("the aliased spelling decodes cleanly");
		assert!(
			!decoded.id.0.addresses_same_record(&aliased),
			"and decodes as a different record, which is what makes it dangerous"
		);
		tx.set(
			Key::from(legacy),
			PrimaryAppendingTicket {
				ticket,
				mutation_seq,
			}
			.kv_encode_value()?,
		)
		.await?;
		tx.commit().await?;
	}
	building.rebuild_primary_appendings().await?;

	let tx = ds.transaction(TransactionType::Read).await?;
	let ptr = tx.get_key(&ikb.new_bp_key(generation, &aliased), None).await?;
	let ptr = ptr.expect("an aliased marker must be rebuilt at its own record's key");
	assert_eq!((ptr.ticket, ptr.mutation_seq), (ticket, mutation_seq));
	// Left where it was, the old key would read as a marker for `[3]`, and the
	// scan would baseline `[3]` against `[3, NONE]`'s queued mutation.
	assert!(
		tx.get_key(&ikb.new_bp_key(generation, &three), None).await?.is_none(),
		"the record the old key decodes as has no queued mutation, so no marker"
	);
	let all = tx.keys(ikb.new_bp_range(generation)?, u32::MAX, 0, None).await?;
	assert_eq!(all.len(), span + 2, "one marker per record with a queued mutation");
	tx.cancel().await?;
	Ok(())
}

/// Two markers in the previous spelling whose keys alias must both survive.
///
/// The key `[1]`'s marker belongs at is the key `[1, NONE]`'s marker occupies,
/// so writing `[1]`'s marker while that one is still there destroys the second
/// record's marker — and the scan then baselines that record as though it had
/// no queued mutation. Reachable on a single-node upgrade.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn aliasing_markers_from_a_previous_release_both_survive_the_rebuild() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE TABLE t SCHEMALESS;
		 DEFINE ANALYZER simple TOKENIZERS blank;
		 DEFINE INDEX ia ON t FIELDS a FULLTEXT ANALYZER simple BM25;",
	)
	.await?;
	let (ns, db, table, ia) = get_table_index(&ds, "t", "ia").await?;
	let ix_id = ia.index_id;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix_id);
	let building = new_building_for_index(&ds, &session, ns, db, &table, ia).await?;
	let generation: BuildGeneration = 1;

	let one = RecordIdKey::Array(vec![Value::Number(crate::val::Number::Int(1))].into());
	let one_none =
		RecordIdKey::Array(vec![Value::Number(crate::val::Number::Int(1)), Value::None].into());
	// The premise: one record's target key is the other's current key.
	assert_eq!(
		*ikb.new_bp_key(generation, &one).encode_key()?,
		*primary_append_key_in_previous_encoding(&ikb, generation, &one_none)?,
		"the two spellings must alias, or this test guards nothing"
	);

	for (id, ticket) in [(&one, 7u64), (&one_none, 9u64)] {
		let tx = ds.transaction(TransactionType::Write).await?;
		tx.set_key(
			&ikb.new_bg_key(generation, ticket, 0),
			&Appending {
				old_values: None,
				new_values: None,
				id: id.clone(),
				count_cond_match: None,
			},
		)
		.await?;
		tx.set(
			Key::from(primary_append_key_in_previous_encoding(&ikb, generation, id)?),
			PrimaryAppendingTicket {
				ticket,
				mutation_seq: 0,
			}
			.kv_encode_value()?,
		)
		.await?;
		tx.commit().await?;
	}

	building.build_generation.store(generation, Ordering::Release);
	own_build(&ds, &ikb, &building, generation).await?;
	building.rebuild_primary_appendings().await?;

	let tx = ds.transaction(TransactionType::Read).await?;
	assert_eq!(
		tx.keys(ikb.new_bp_range(generation)?, u32::MAX, 0, None).await?.len(),
		2,
		"neither marker may be lost"
	);
	for (id, ticket) in [(&one, 7u64), (&one_none, 9u64)] {
		let ptr = tx
			.get_key(&ikb.new_bp_key(generation, id), None)
			.await?
			.expect("each record keeps a marker at its own key");
		assert_eq!(ptr.ticket, ticket, "and it still names that record's own mutation");
	}
	tx.cancel().await?;
	Ok(())
}

/// A record with two queued mutations gets a marker naming the earlier one.
///
/// A marker names a record's *first* queued mutation and the scan baselines
/// against it, so naming the later one would apply a delta the baseline already
/// includes. Here the later one already sits at this release's key — as a write
/// admitted after the markers are cleared leaves it — and the earlier one is in
/// the previous spelling.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn the_rebuild_keeps_the_earlier_of_two_markers_for_one_record() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE TABLE t SCHEMALESS;
		 DEFINE ANALYZER simple TOKENIZERS blank;
		 DEFINE INDEX ia ON t FIELDS a FULLTEXT ANALYZER simple BM25;",
	)
	.await?;
	let (ns, db, table, ia) = get_table_index(&ds, "t", "ia").await?;
	let ix_id = ia.index_id;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix_id);
	let building = new_building_for_index(&ds, &session, ns, db, &table, ia).await?;
	let generation: BuildGeneration = 1;
	let id = RecordIdKey::Array(vec![Value::Number(crate::val::Number::Int(5))].into());

	let tx = ds.transaction(TransactionType::Write).await?;
	for ticket in [5u64, 9u64] {
		tx.set_key(
			&ikb.new_bg_key(generation, ticket, 0),
			&Appending {
				old_values: None,
				new_values: None,
				id: id.clone(),
				count_cond_match: None,
			},
		)
		.await?;
	}
	// The later mutation already sits at this release's key; the earlier one is
	// still in the previous spelling.
	tx.set_key(
		&ikb.new_bp_key(generation, &id),
		&PrimaryAppendingTicket {
			ticket: 9,
			mutation_seq: 0,
		},
	)
	.await?;
	tx.set(
		Key::from(primary_append_key_in_previous_encoding(&ikb, generation, &id)?),
		PrimaryAppendingTicket {
			ticket: 5,
			mutation_seq: 0,
		}
		.kv_encode_value()?,
	)
	.await?;
	tx.commit().await?;

	building.build_generation.store(generation, Ordering::Release);
	own_build(&ds, &ikb, &building, generation).await?;
	building.rebuild_primary_appendings().await?;

	let tx = ds.transaction(TransactionType::Read).await?;
	let ptr = tx
		.get_key(&ikb.new_bp_key(generation, &id), None)
		.await?
		.expect("the record keeps a marker");
	assert_eq!(ptr.ticket, 5, "the earlier queued mutation is the one a marker must name");
	assert_eq!(
		tx.keys(ikb.new_bp_range(generation)?, u32::MAX, 0, None).await?.len(),
		1,
		"and the other spelling is consumed"
	);
	tx.cancel().await?;

	// The same with the two mutations a page apart, so the later one is met in a
	// transaction of its own, after the earlier one's marker has been committed.
	let far = RecordIdKey::Array(vec![Value::Number(crate::val::Number::Int(6))].into());
	let page = surrealdb_kvs::consts::INDEXING_BATCH_SIZE as u64;
	let tx = ds.transaction(TransactionType::Write).await?;
	for (ticket, id) in
		[(20, far.clone()), (21 + page, far.clone())].into_iter().chain((0..page).map(|i| {
			(21 + i, RecordIdKey::Array(vec![Value::Number(crate::val::Number::Int(7))].into()))
		})) {
		tx.set_key(
			&ikb.new_bg_key(generation, ticket, 0),
			&Appending {
				old_values: None,
				new_values: None,
				id,
				count_cond_match: None,
			},
		)
		.await?;
	}
	tx.commit().await?;
	building.rebuild_primary_appendings().await?;

	let tx = ds.transaction(TransactionType::Read).await?;
	let ptr = tx
		.get_key(&ikb.new_bp_key(generation, &far), None)
		.await?
		.expect("the record keeps a marker");
	assert_eq!(ptr.ticket, 20, "a later page must not replace an earlier page's marker");
	tx.cancel().await?;
	Ok(())
}

/// Publishes `building` as the owner of `generation` in the `Building` phase,
/// which the marker rebuild renews as it goes.
async fn own_build(
	ds: &Datastore,
	ikb: &IndexKeyBase,
	building: &Building,
	generation: BuildGeneration,
) -> Result<()> {
	set_durable_build_state(
		ds,
		ikb,
		durable_build_state_for_phase(IndexBuildPhase::Building, generation, Some(building.owner)),
	)
	.await
}

/// Queues a mutation of `id` at `ticket`, and a marker for it in the spelling a
/// previous release used.
async fn queue_with_marker_in_previous_encoding(
	tx: &crate::kvs::Transaction,
	ikb: &IndexKeyBase,
	generation: BuildGeneration,
	ticket: BuildTicket,
	id: &RecordIdKey,
	old_values: Option<Vec<Value>>,
	marker: bool,
) -> Result<()> {
	let appending = Appending {
		old_values,
		new_values: None,
		id: id.clone(),
		count_cond_match: None,
	};
	queue_in_previous_encoding(tx, ikb, generation, ticket, appending, marker).await
}

/// Queues `appending` at `ticket`, and a marker for it in the spelling a
/// previous release used.
async fn queue_in_previous_encoding(
	tx: &crate::kvs::Transaction,
	ikb: &IndexKeyBase,
	generation: BuildGeneration,
	ticket: BuildTicket,
	appending: Appending,
	marker: bool,
) -> Result<()> {
	if marker {
		tx.set(
			Key::from(primary_append_key_in_previous_encoding(ikb, generation, &appending.id)?),
			PrimaryAppendingTicket {
				ticket,
				mutation_seq: 0,
			}
			.kv_encode_value()?,
		)
		.await?;
	}
	tx.set_key(&ikb.new_bg_key(generation, ticket, 0), &appending).await
}

/// The ids `[1]`, `[1, NONE]`, … with `len` links, each of whose keys under this
/// release is the next one's key under the previous release.
fn aliasing_chain(len: usize) -> Vec<RecordIdKey> {
	(0..len)
		.map(|nones| {
			let mut id = vec![Value::Number(crate::val::Number::Int(1))];
			id.extend(std::iter::repeat_n(Value::None, nones));
			RecordIdKey::Array(id.into())
		})
		.collect()
}

/// A chain of markers in the previous spelling, each occupying the key the one
/// before it belongs at, all come out of the rebuild at their own keys.
///
/// No marker is moved, so the chain's length bounds nothing: the markers are
/// cleared and each is written afresh from its queued mutation.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn a_chain_of_aliasing_markers_is_rebuilt() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE TABLE t SCHEMALESS;
		 DEFINE ANALYZER simple TOKENIZERS blank;
		 DEFINE INDEX ia ON t FIELDS a FULLTEXT ANALYZER simple BM25;",
	)
	.await?;
	let (ns, db, table, ia) = get_table_index(&ds, "t", "ia").await?;
	let ix_id = ia.index_id;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix_id);
	let building = new_building_for_index(&ds, &session, ns, db, &table, ia).await?;
	let generation: BuildGeneration = 1;

	let chain = aliasing_chain(5);
	for link in chain.windows(2) {
		assert_eq!(
			*ikb.new_bp_key(generation, &link[0]).encode_key()?,
			*primary_append_key_in_previous_encoding(&ikb, generation, &link[1])?,
			"each link must occupy the key the one before it belongs at"
		);
	}
	let tx = ds.transaction(TransactionType::Write).await?;
	for (ticket, id) in chain.iter().enumerate() {
		queue_with_marker_in_previous_encoding(
			&tx,
			&ikb,
			generation,
			ticket as BuildTicket + 1,
			id,
			None,
			true,
		)
		.await?;
	}
	tx.commit().await?;

	building.build_generation.store(generation, Ordering::Release);
	own_build(&ds, &ikb, &building, generation).await?;
	building.rebuild_primary_appendings().await?;

	let tx = ds.transaction(TransactionType::Read).await?;
	for (ticket, id) in chain.iter().enumerate() {
		let ptr = tx
			.get_key(&ikb.new_bp_key(generation, id), None)
			.await?
			.expect("every link keeps a marker at its own key");
		assert_eq!(ptr.ticket, ticket as BuildTicket + 1, "naming its own mutation");
	}
	assert_eq!(
		tx.keys(ikb.new_bp_range(generation)?, u32::MAX, 0, None).await?.len(),
		chain.len(),
		"and nothing else is left behind"
	);
	tx.cancel().await?;
	Ok(())
}

/// A record whose queued mutation has no marker gets one from the rebuild.
///
/// A write on this release admitted while an older marker for another record
/// occupied its key found the key taken and recorded no marker of its own. Only
/// its queued mutation still says the record has one.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn a_record_left_without_a_marker_gets_one_from_its_queued_mutation() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE TABLE t SCHEMALESS;
		 DEFINE ANALYZER simple TOKENIZERS blank;
		 DEFINE INDEX ia ON t FIELDS a FULLTEXT ANALYZER simple BM25;",
	)
	.await?;
	let (ns, db, table, ia) = get_table_index(&ds, "t", "ia").await?;
	let ix_id = ia.index_id;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix_id);
	let building = new_building_for_index(&ds, &session, ns, db, &table, ia).await?;
	let generation: BuildGeneration = 1;
	let [one, one_none] = <[RecordIdKey; 2]>::try_from(aliasing_chain(2)).expect("two links");

	let tx = ds.transaction(TransactionType::Write).await?;
	// `[1, NONE]`'s marker in the previous spelling, at the key `[1]`'s belongs at.
	queue_with_marker_in_previous_encoding(&tx, &ikb, generation, 9, &one_none, None, true).await?;
	// `[1]` was written after it and found that key taken.
	queue_with_marker_in_previous_encoding(&tx, &ikb, generation, 11, &one, None, false).await?;
	tx.commit().await?;

	building.build_generation.store(generation, Ordering::Release);
	own_build(&ds, &ikb, &building, generation).await?;
	building.rebuild_primary_appendings().await?;

	let tx = ds.transaction(TransactionType::Read).await?;
	for (id, ticket) in [(&one, 11), (&one_none, 9)] {
		let ptr = tx
			.get_key(&ikb.new_bp_key(generation, id), None)
			.await?
			.expect("each record with a queued mutation has a marker");
		assert_eq!(ptr.ticket, ticket, "naming its own mutation");
	}
	tx.cancel().await?;
	Ok(())
}

/// Reads every record of `table` as the initial scan pages them.
async fn scan_records(
	ds: &Datastore,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
) -> Result<Vec<(Vec<u8>, crate::kvs::Val)>> {
	let tx = ds.transaction(TransactionType::Read).await?;
	let rng = RecordPrefix {
		ns,
		db,
		tb: Cow::Borrowed(table),
	}
	.range()?;
	let values = catch!(tx, tx.batch_keys_vals_raw(rng, u32::MAX, None).await).result;
	tx.cancel().await?;
	Ok(values)
}

/// A queued mutation of `id` from one state to another, each present or
/// absent, which is all a COUNT index without a predicate reads of them.
fn mutation(id: &RecordIdKey, old: bool, new: bool) -> Appending {
	Appending {
		old_values: old.then(Vec::new),
		new_values: new.then(Vec::new),
		id: id.clone(),
		count_cond_match: None,
	}
}

/// The ids `[n, s]` the COUNT tests mark: the older spelling of each sorts
/// after the keys of every `[n, …]` record under this release.
fn number_then_string(n: i64, s: &str) -> RecordIdKey {
	RecordIdKey::Array(vec![Value::Number(crate::val::Number::Int(n)), Value::from(s)].into())
}

/// The generation the COUNT tests' builds own.
const COUNT_GENERATION: BuildGeneration = 1;

/// A build of the COUNT index `ic` on table `t`, owning [`COUNT_GENERATION`],
/// over what `sql` defined and wrote.
struct CountBuild {
	ds: Arc<Datastore>,
	session: Session,
	ikb: IndexKeyBase,
	building: Building,
}

impl CountBuild {
	async fn new(sql: &str) -> Result<Self> {
		let (ds, session) = new_index_test_ds().await?;
		execute_all(&ds, &session, sql).await?;
		let (ns, db, table, ic) = get_table_index(&ds, "t", "ic").await?;
		let ikb = IndexKeyBase::new(ns, db, table.clone(), ic.index_id);
		let building = new_building_for_index(&ds, &session, ns, db, &table, ic).await?;
		building.build_generation.store(COUNT_GENERATION, Ordering::Release);
		Ok(Self {
			ds,
			session,
			ikb,
			building,
		})
	}

	/// The key this release marks `id` at.
	fn own_key(&self, id: &RecordIdKey) -> Result<Vec<u8>> {
		Ok(self.ikb.new_bp_key(COUNT_GENERATION, id).encode_key()?.to_vec())
	}

	/// The key an older node marks `id` at.
	fn older_key(&self, id: &RecordIdKey) -> Result<Vec<u8>> {
		primary_append_key_in_previous_encoding(&self.ikb, COUNT_GENERATION, id)
	}

	/// Queues `appending` at `ticket`, marked the way an older node marks it.
	async fn queue_older(
		&self,
		tx: &crate::kvs::Transaction,
		ticket: BuildTicket,
		appending: Appending,
	) -> Result<()> {
		queue_in_previous_encoding(tx, &self.ikb, COUNT_GENERATION, ticket, appending, true).await
	}

	/// Queues `appending` at `ticket`, marked at its record's own key.
	async fn queue_own(
		&self,
		tx: &crate::kvs::Transaction,
		ticket: BuildTicket,
		appending: Appending,
	) -> Result<()> {
		let ptr = PrimaryAppendingTicket {
			ticket,
			mutation_seq: 0,
		};
		tx.set_key(&self.ikb.new_bp_key(COUNT_GENERATION, &appending.id), &ptr).await?;
		self.queue_unmarked(tx, ticket, appending).await
	}

	/// Queues `appending` at `ticket` unmarked, as a writer queues a later
	/// mutation of a record that already has a marker.
	async fn queue_unmarked(
		&self,
		tx: &crate::kvs::Transaction,
		ticket: BuildTicket,
		appending: Appending,
	) -> Result<()> {
		tx.set_key(&self.ikb.new_bg_key(COUNT_GENERATION, ticket, 0), &appending).await
	}

	/// The table's live records, as the initial scan pages them.
	async fn records(&self) -> Result<Vec<(Vec<u8>, crate::kvs::Val)>> {
		scan_records(&self.ds, self.ikb.ns(), self.ikb.db(), self.ikb.table()).await
	}

	/// Runs the initial scan over `batches` of the table's records, in order,
	/// returning what each batch and the tail after them indexed, and the
	/// count they baselined.
	async fn scan_in_batches(
		&self,
		batches: &[&[(Vec<u8>, crate::kvs::Val)]],
	) -> Result<(Vec<usize>, i64)> {
		let mut scan = CountScan::start(&self.building).await?;
		let mut indexed = Vec::new();
		for values in batches {
			indexed.push(scan.batch(values).await?);
		}
		indexed.push(scan.tail().await?);
		let baseline = scan.baseline();
		scan.cancel().await?;
		Ok((indexed, baseline))
	}
}

/// A COUNT build's initial scan and its merge of queued old states, driven a
/// batch at a time in one transaction, as the builder drives them.
struct CountScan<'a> {
	building: &'a Building,
	ctx: crate::ctx::FrozenContext,
	progress: Option<super::replay::CountPrimaryProgress>,
	indexed: usize,
}

impl<'a> CountScan<'a> {
	async fn start(building: &'a Building) -> Result<Self> {
		Ok(Self {
			building,
			ctx: building.new_write_tx_ctx().await?,
			progress: Some(super::replay::CountPrimaryProgress::default()),
			indexed: 0,
		})
	}

	/// Scans `values`, the next batch of records, returning what it indexed.
	async fn batch(&mut self, values: &[(Vec<u8>, crate::kvs::Val)]) -> Result<usize> {
		let tx = self.ctx.tx();
		let n = self
			.building
			.index_initial_batch(
				&self.ctx,
				&tx,
				values,
				self.indexed,
				&mut false,
				&mut self.progress,
			)
			.await?;
		self.indexed += n;
		Ok(n)
	}

	/// Merges the queued old states past the last batch, returning what it
	/// indexed.
	async fn tail(&mut self) -> Result<usize> {
		let tx = self.ctx.tx();
		let n = self
			.building
			.index_remaining_count_primary_appendings(
				&self.ctx,
				&tx,
				&mut self.progress,
				self.indexed,
			)
			.await?;
		self.indexed += n;
		Ok(n)
	}

	/// The count the scan has baselined so far.
	fn baseline(&self) -> i64 {
		let building = self.building;
		self.ctx.tx().pending_count_delta(
			building.ix_key.ns,
			building.ix_key.db,
			building.ikb.table(),
			building.ix.index_id,
		)
	}

	async fn cancel(self) -> Result<()> {
		self.ctx.tx().cancel().await
	}
}

/// The key a COUNT build reads an older node's marker at is the one that node
/// writes it at, and there is one only for an id holding a number.
#[test]
fn a_markers_older_spelling_is_the_key_an_older_node_writes() -> Result<()> {
	let ikb = IndexKeyBase::new(NamespaceId(1), DatabaseId(2), TableName::from("t"), IndexId(3));
	let older = |id: &RecordIdKey| -> Result<Option<Vec<u8>>> {
		let key = ikb.new_bp_key_in_previous_spelling(COUNT_GENERATION, id)?;
		Ok(key.map(|key| key.as_bytes().to_vec()))
	};
	for id in [&aliasing_chain(1)[0], &aliasing_chain(2)[1], &number_then_string(42, "intro")] {
		let written = primary_append_key_in_previous_encoding(&ikb, COUNT_GENERATION, id)?;
		assert_eq!(older(id)?, Some(written), "{id:?}");
	}
	// The bytes v3.2.4 writes for this marker, whose `Bp` key encodes every
	// field under the index format: a change to how that format spells a number
	// would otherwise move the key the scan reads and the helper above alike.
	assert_eq!(
		older(&number_then_string(42, "intro"))?,
		Some(
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*t\x00!bp\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00\x01\
			  \x05\x05\xa0\x19\x1aS\x0f\x01\x00\x00\x06intro\x00\x00"
				.to_vec()
		)
	);
	let plain = [
		RecordIdKey::String("plain".into()),
		RecordIdKey::Number(7),
		RecordIdKey::Array(vec![Value::from("a"), Value::None].into()),
	];
	for id in &plain {
		assert_eq!(older(id)?, None, "{id:?}");
	}
	Ok(())
}

/// A COUNT build merges a marker an older node wrote by the record its queued
/// mutation names, whatever its key decodes as or whether it decodes at all.
///
/// The merge counts the queued old state of records the live scan does not see.
/// An older node spells the id under the index format: `[42, 'intro']` does not
/// decode under this release's spelling at all, and `[1, NONE]` decodes as
/// `[1]`, a live record, which would drop a deleted record from the baseline.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn a_count_build_merges_older_markers_by_their_queued_mutation() -> Result<()> {
	let b = CountBuild::new(
		"DEFINE TABLE t SCHEMALESS;
		 DEFINE INDEX ic ON t COUNT;
		 CREATE t:[1] SET a = 1;",
	)
	.await?;
	let (one_none, intro) =
		(aliasing_chain(2).pop().expect("two links"), number_then_string(42, "intro"));
	// The premise: the aliasing key is the live record's, and sorts in its span.
	assert_eq!(b.own_key(&aliasing_chain(1)[0])?, b.older_key(&one_none)?);

	// Both records were deleted during the build, and neither is live.
	let tx = b.ds.transaction(TransactionType::Write).await?;
	b.queue_older(&tx, 9, mutation(&one_none, true, false)).await?;
	b.queue_older(&tx, 10, mutation(&intro, true, false)).await?;
	tx.commit().await?;

	let values = b.records().await?;
	assert_eq!(values.len(), 1, "only `t:[1]` is live");
	let mut scan = CountScan::start(&b.building).await?;
	assert_eq!(
		scan.batch(&values).await?,
		1,
		"the live record only: `[1, NONE]`'s marker reads as `[1]` but waits for its own span"
	);
	assert_eq!(
		scan.tail().await?,
		2,
		"the deleted `[1, NONE]` in its own span, and the deleted `[42, 'intro']`, whose key \
		 does not decode"
	);
	assert_eq!(scan.baseline(), 3);
	scan.cancel().await
}

/// A marker an older node wrote in a span before its record's is taken up in
/// the record's own span.
///
/// `[1, NONE]`'s marker in the older spelling sits at `[1]`'s key, in the span
/// of the batch that scans `[1]`. `[1, NONE]` is live, and baselined from that
/// marker's queued old state when a batch later scans it: counting that state
/// with `[1]`'s batch as well would count the record twice.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn a_count_build_takes_an_early_marker_up_in_its_records_span() -> Result<()> {
	let b = CountBuild::new(
		"DEFINE TABLE t SCHEMALESS;
		 DEFINE INDEX ic ON t COUNT;
		 CREATE t:[1] SET a = 1;
		 CREATE t:[1, NONE] SET a = 1;",
	)
	.await?;
	let one_none = aliasing_chain(2).pop().expect("two links");

	// An update of the live `[1, NONE]`, queued by an older node.
	let tx = b.ds.transaction(TransactionType::Write).await?;
	b.queue_older(&tx, 9, mutation(&one_none, true, true)).await?;
	tx.commit().await?;

	let values = b.records().await?;
	assert_eq!(values.len(), 2, "`[1]` and `[1, NONE]` are both live");
	let (indexed, baseline) = b.scan_in_batches(&[&values[..1], &values[1..]]).await?;
	assert_eq!(indexed, vec![1, 1, 0], "each live record once, and the marker not at all");
	assert_eq!(baseline, 2);
	Ok(())
}

/// A marker an older node wrote in a span after its record's counts only for a
/// record that is gone.
///
/// The older spelling of `[42, 'intro']` sorts after the keys of every
/// `[42, …]` record, so with `[42, 'k']` ending a batch the markers of
/// `[42, 'intro']` and `[42, 'b']` fall in the next span, after their records'
/// own span. The scan baselined `[42, 'intro']` there from its marker's queued
/// old state, so the marker counts for nothing where it sorts; `[42, 'b']` is
/// gone, so its queued old state is what the baseline needs.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn a_count_build_counts_a_late_marker_only_for_a_record_that_is_gone() -> Result<()> {
	let b = CountBuild::new(
		"DEFINE TABLE t SCHEMALESS;
		 DEFINE INDEX ic ON t COUNT;
		 CREATE t:[42, 'intro'] SET a = 1;
		 CREATE t:[42, 'k'] SET a = 1;
		 CREATE t:[43, 'x'] SET a = 1;",
	)
	.await?;
	let (live, gone) = (number_then_string(42, "intro"), number_then_string(42, "b"));
	// The premise: both older keys land after `[42, 'k']` and before `[43, 'x']`.
	for marked in [&live, &gone] {
		let old = b.older_key(marked)?;
		assert!(old > b.own_key(&number_then_string(42, "k"))?);
		assert!(old < b.own_key(&number_then_string(43, "x"))?);
	}

	// An update of the live record and a delete of the other, queued by an older node.
	let tx = b.ds.transaction(TransactionType::Write).await?;
	b.queue_older(&tx, 9, mutation(&live, true, true)).await?;
	b.queue_older(&tx, 10, mutation(&gone, true, false)).await?;
	tx.commit().await?;

	let values = b.records().await?;
	assert_eq!(values.len(), 3);
	let (indexed, baseline) = b.scan_in_batches(&[&values[..2], &values[2..]]).await?;
	assert_eq!(
		indexed,
		vec![2, 2, 0],
		"the two live `[42, …]`, then `[43, 'x']` and the gone `[42, 'b']`, and nothing twice"
	);
	assert_eq!(baseline, 4);
	Ok(())
}

/// A marker an older node wrote is ignored for a record whose own key holds a
/// marker for it, and only then.
///
/// `[42, 'b']` has both, and must be counted once. The key `[1]`'s marker would
/// sit at holds `[1, NONE]`'s marker in the older spelling instead, which names
/// another record: `[1]`'s own marker, in the older spelling elsewhere, is still
/// the one that counts it.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn a_count_build_trusts_an_own_key_marker_only_for_its_own_record() -> Result<()> {
	let b = CountBuild::new("DEFINE TABLE t SCHEMALESS; DEFINE INDEX ic ON t COUNT;").await?;
	let [one, one_none] = <[RecordIdKey; 2]>::try_from(aliasing_chain(2)).expect("two links");
	let both = number_then_string(42, "b");

	// Every record was deleted during the build; none is live.
	let tx = b.ds.transaction(TransactionType::Write).await?;
	b.queue_older(&tx, 1, mutation(&one, true, false)).await?;
	b.queue_older(&tx, 2, mutation(&one_none, true, false)).await?;
	// `[42, 'b']`'s own marker, and a later mutation marked in the older spelling.
	b.queue_own(&tx, 3, mutation(&both, true, false)).await?;
	b.queue_older(&tx, 4, mutation(&both, true, false)).await?;
	tx.commit().await?;

	let (indexed, baseline) = b.scan_in_batches(&[]).await?;
	assert_eq!(indexed, vec![3], "`[1]`, `[1, NONE]` and `[42, 'b']`, each once");
	assert_eq!(baseline, 3);
	Ok(())
}

/// A COUNT WHERE build baselines a record an older node changed from the
/// predicate state its queued mutation carries, not from the record as scanned.
///
/// Every record matched until an older node updated it to stop matching, and
/// this release then deleted `[42, 'e']` and `[44, 'd']`, marking that at their
/// own keys. Replay subtracts each of them once, so each has to be in the
/// baseline. The older markers sort in a span before their record's
/// (`[1, NONE]`), after it (`[42, 'intro']`, `[42, 'e']`) or in it
/// (`[44, 'd']`); the deleted records' own markers are the later of their two.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn a_count_where_build_baselines_a_changed_record_from_its_queued_state() -> Result<()> {
	let b = CountBuild::new(
		"DEFINE TABLE t SCHEMALESS;
		 DEFINE INDEX ic ON t COUNT WHERE a = 1;
		 CREATE t:[1] SET a = 1;
		 CREATE t:[1, NONE] SET a = 2;
		 CREATE t:[42, 'intro'] SET a = 2;
		 CREATE t:[42, 'k'] SET a = 1;
		 CREATE t:[43, 'x'] SET a = 1;
		 CREATE t:[45, 'z'] SET a = 1;",
	)
	.await?;
	let one_none = aliasing_chain(2).pop().expect("two links");
	let intro = number_then_string(42, "intro");
	let (late, in_span) = (number_then_string(42, "e"), number_then_string(44, "d"));
	let stopped_matching = |id: &RecordIdKey| Appending {
		count_cond_match: Some((true, false)),
		..mutation(id, true, true)
	};
	let deleted = |id: &RecordIdKey| Appending {
		count_cond_match: Some((false, false)),
		..mutation(id, true, false)
	};

	let tx = b.ds.transaction(TransactionType::Write).await?;
	for (ticket, changed) in [(1, &one_none), (2, &intro), (3, &late), (5, &in_span)] {
		b.queue_older(&tx, ticket, stopped_matching(changed)).await?;
	}
	b.queue_own(&tx, 4, deleted(&late)).await?;
	b.queue_own(&tx, 6, deleted(&in_span)).await?;
	tx.commit().await?;

	let values = b.records().await?;
	assert_eq!(values.len(), 6, "every record but `[42, 'e']` and `[44, 'd']` is live");
	let (indexed, baseline) =
		b.scan_in_batches(&[&values[..1], &values[1..4], &values[4..]]).await?;
	assert_eq!(baseline, 8, "each record, as it stood before its queued mutations");
	assert_eq!(
		indexed,
		vec![1, 4, 3, 0],
		"`[1]`; `[1, NONE]`, `[42, 'intro']`, `[42, 'k']` and the gone `[42, 'e']`; \
		 `[43, 'x']`, `[45, 'z']` and the gone `[44, 'd']`"
	);
	Ok(())
}

/// A COUNT build baselines a record from the first of its queued mutations when
/// both an older node and this release marked one.
///
/// An older node deleted `[42, 'b']` and this release recreated it; this release
/// created `[42, 'c']` and an older node then updated it. Each record's own
/// marker is this release's, which is the later mutation for `[42, 'b']` only.
/// `t:plain`, which both releases spell alike, shares their batch.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn a_count_build_baselines_from_the_first_of_two_markers() -> Result<()> {
	let b = CountBuild::new(
		"DEFINE TABLE t SCHEMALESS;
		 DEFINE INDEX ic ON t COUNT;
		 CREATE t:plain SET a = 1;
		 CREATE t:[42, 'b'] SET a = 1;
		 CREATE t:[42, 'c'] SET a = 1;
		 CREATE t:[42, 'k'] SET a = 1;",
	)
	.await?;
	let (recreated, created) = (number_then_string(42, "b"), number_then_string(42, "c"));

	let tx = b.ds.transaction(TransactionType::Write).await?;
	b.queue_older(&tx, 1, mutation(&recreated, true, false)).await?;
	b.queue_own(&tx, 2, mutation(&recreated, false, true)).await?;
	b.queue_own(&tx, 3, mutation(&created, false, true)).await?;
	b.queue_older(&tx, 4, mutation(&created, true, true)).await?;
	tx.commit().await?;

	let values = b.records().await?;
	assert_eq!(values.len(), 4);
	let (indexed, baseline) = b.scan_in_batches(&[&values]).await?;
	assert_eq!(baseline, 3, "all but `[42, 'c']`, which did not exist yet");
	assert_eq!(indexed, vec![4, 0], "the live records once, and the older markers not at all");
	Ok(())
}

/// A COUNT build baselines a record an older node created as absent.
///
/// The older node's marker for `[42, 'n']` sorts after the record's span, so
/// the scan finds it only by reading the older spelling; indexing the record as
/// it stands would count the create twice, once there and once in replay.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn a_count_build_baselines_a_record_an_older_node_created_as_absent() -> Result<()> {
	let b = CountBuild::new(
		"DEFINE TABLE t SCHEMALESS;
		 DEFINE INDEX ic ON t COUNT;
		 CREATE t:[42, 'k'] SET a = 1;
		 CREATE t:[42, 'n'] SET a = 1;",
	)
	.await?;

	let tx = b.ds.transaction(TransactionType::Write).await?;
	b.queue_older(&tx, 1, mutation(&number_then_string(42, "n"), false, true)).await?;
	tx.commit().await?;

	let values = b.records().await?;
	assert_eq!(values.len(), 2);
	let (indexed, baseline) = b.scan_in_batches(&[&values]).await?;
	assert_eq!(baseline, 1, "`[42, 'k']` only");
	assert_eq!(indexed, vec![2, 0]);
	Ok(())
}

/// A marker an older node wrote that sorts after its record's span is skipped
/// when that span settled the record, whatever became of the record since.
///
/// `[42, 'a']`'s span baselined it from its older marker, and `[42, 'c']`'s
/// from the record as it stood; both records are then deleted before the merge
/// reaches their older markers. `[42, 'c']` was first updated by this release,
/// which marked it at its own key before the older node's delete.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn a_count_build_skips_a_late_marker_its_records_span_settled() -> Result<()> {
	let b = CountBuild::new(
		"DEFINE TABLE t SCHEMALESS;
		 DEFINE INDEX ic ON t COUNT;
		 CREATE t:[42, 'a'] SET a = 1;
		 CREATE t:[42, 'c'] SET a = 1;
		 CREATE t:[42, 'k'] SET a = 1;",
	)
	.await?;
	let (taken, updated) = (number_then_string(42, "a"), number_then_string(42, "c"));

	// An older node's update of `[42, 'a']`, before the scan.
	let tx = b.ds.transaction(TransactionType::Write).await?;
	b.queue_older(&tx, 1, mutation(&taken, true, true)).await?;
	tx.commit().await?;

	let values = b.records().await?;
	assert_eq!(values.len(), 3);
	let mut scan = CountScan::start(&b.building).await?;
	assert_eq!(scan.batch(&values).await?, 3);

	// After the batch: the older node deletes `[42, 'a']`, whose older key
	// already holds its marker; this release updates `[42, 'c']` and an older
	// node deletes it.
	let tx = b.ds.transaction(TransactionType::Write).await?;
	b.queue_unmarked(&tx, 2, mutation(&taken, true, false)).await?;
	b.queue_own(&tx, 3, mutation(&updated, true, true)).await?;
	b.queue_older(&tx, 4, mutation(&updated, true, false)).await?;
	tx.commit().await?;
	execute_all(&b.ds, &b.session, "DELETE t:[42, 'a']; DELETE t:[42, 'c'];").await?;

	assert_eq!(scan.tail().await?, 0, "neither older marker counts where it sorts");
	assert_eq!(scan.baseline(), 3, "each record once, as its span had it");
	scan.cancel().await
}

/// A marker an older node wrote that sorts after its record's span still counts
/// when the record's own key marks only a later mutation.
///
/// An older node deleted `[42, 'g']` before its span, which found nothing to
/// count. This release then recreated and deleted it, marking the recreate at
/// the record's own key: that marker stands for nothing before the delete.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn a_count_build_counts_a_late_marker_whose_own_marker_came_after() -> Result<()> {
	let b = CountBuild::new(
		"DEFINE TABLE t SCHEMALESS;
		 DEFINE INDEX ic ON t COUNT;
		 CREATE t:[42, 'k'] SET a = 1;",
	)
	.await?;
	let gone = number_then_string(42, "g");

	let tx = b.ds.transaction(TransactionType::Write).await?;
	b.queue_older(&tx, 1, mutation(&gone, true, false)).await?;
	tx.commit().await?;

	let values = b.records().await?;
	assert_eq!(values.len(), 1, "only `[42, 'k']` is live");
	let mut scan = CountScan::start(&b.building).await?;
	assert_eq!(scan.batch(&values).await?, 1);

	let tx = b.ds.transaction(TransactionType::Write).await?;
	b.queue_own(&tx, 2, mutation(&gone, false, true)).await?;
	b.queue_unmarked(&tx, 3, mutation(&gone, true, false)).await?;
	tx.commit().await?;

	assert_eq!(scan.tail().await?, 1, "`[42, 'g']`, as it stood before the older node's delete");
	assert_eq!(scan.baseline(), 2);
	scan.cancel().await
}

/// A marker an older node wrote in a span before its record's is not counted
/// when, by the record's span, the record's own key marks a mutation of it.
///
/// An older node deleted `[1, NONE]`, marking that at `[1]`'s key, which the
/// merge meets with `[1]`'s batch and sets aside. This release then recreated
/// and deleted the record, marking it at its own key, which the merge meets in
/// the record's span and takes up together with the older marker.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn a_count_build_takes_an_early_marker_up_once_its_record_has_its_own() -> Result<()> {
	let b = CountBuild::new(
		"DEFINE TABLE t SCHEMALESS;
		 DEFINE INDEX ic ON t COUNT;
		 CREATE t:[1] SET a = 1;",
	)
	.await?;
	let one_none = aliasing_chain(2).pop().expect("two links");

	let tx = b.ds.transaction(TransactionType::Write).await?;
	b.queue_older(&tx, 1, mutation(&one_none, true, false)).await?;
	tx.commit().await?;

	let values = b.records().await?;
	assert_eq!(values.len(), 1, "only `[1]` is live");
	let mut scan = CountScan::start(&b.building).await?;
	assert_eq!(scan.batch(&values).await?, 1, "`[1]`, with `[1, NONE]`'s marker set aside");

	let tx = b.ds.transaction(TransactionType::Write).await?;
	b.queue_own(&tx, 2, mutation(&one_none, false, true)).await?;
	b.queue_unmarked(&tx, 3, mutation(&one_none, true, false)).await?;
	tx.commit().await?;

	assert_eq!(scan.tail().await?, 1, "`[1, NONE]` once");
	assert_eq!(scan.baseline(), 2);
	scan.cancel().await
}

/// A marker an older node wrote after its record's span is skipped while the
/// record is there, since that span baselined the record as it stood.
///
/// The scan baselines `[42, 'a']` live, and an older node's update of it is
/// marked only afterwards, at a key that sorts after the span.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn a_count_build_skips_a_late_marker_written_after_its_records_span() -> Result<()> {
	let b = CountBuild::new(
		"DEFINE TABLE t SCHEMALESS;
		 DEFINE INDEX ic ON t COUNT;
		 CREATE t:[42, 'a'] SET a = 1;
		 CREATE t:[42, 'k'] SET a = 1;",
	)
	.await?;
	let values = b.records().await?;
	let mut scan = CountScan::start(&b.building).await?;
	assert_eq!(scan.batch(&values).await?, 2);

	let tx = b.ds.transaction(TransactionType::Write).await?;
	b.queue_older(&tx, 1, mutation(&number_then_string(42, "a"), true, true)).await?;
	tx.commit().await?;

	assert_eq!(scan.tail().await?, 0, "`[42, 'a']` was there when its span was scanned");
	assert_eq!(scan.baseline(), 2);
	scan.cancel().await
}

/// A marker set aside for its record's span defers only to a marker at the
/// record's own key that names the record.
///
/// `[1, NONE]`'s older marker sits at `[1]`'s key, and `[1, NONE]`'s own key
/// holds the older marker of `[1, NONE, NONE]`. Both records are gone.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn a_set_aside_marker_defers_only_to_its_own_records_marker() -> Result<()> {
	let b = CountBuild::new(
		"DEFINE TABLE t SCHEMALESS;
		 DEFINE INDEX ic ON t COUNT;
		 CREATE t:[1] SET a = 1;",
	)
	.await?;
	let [_, one_none, one_none_none] =
		<[RecordIdKey; 3]>::try_from(aliasing_chain(3)).expect("three links");
	// The premise: the two older markers sit at `[1]`'s and `[1, NONE]`'s keys.
	assert_eq!(b.older_key(&one_none_none)?, b.own_key(&one_none)?);

	let tx = b.ds.transaction(TransactionType::Write).await?;
	b.queue_older(&tx, 1, mutation(&one_none, true, false)).await?;
	b.queue_older(&tx, 2, mutation(&one_none_none, true, false)).await?;
	tx.commit().await?;

	let values = b.records().await?;
	assert_eq!(values.len(), 1, "only `[1]` is live");
	let (indexed, baseline) = b.scan_in_batches(&[&values]).await?;
	assert_eq!(indexed, vec![1, 2], "`[1]`, then `[1, NONE]` and `[1, NONE, NONE]`");
	assert_eq!(baseline, 3);
	Ok(())
}

/// A marker met after its record's span defers only to an earlier marker at the
/// record's own key that names the record.
///
/// `[42, 'a']`'s own key holds the older marker of `[42, NONE, 'a']`, another
/// record's earlier mutation, and `[42, 'a']`'s own older marker sorts after
/// the span. Both records are gone.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn a_late_marker_defers_only_to_its_own_records_marker() -> Result<()> {
	let b = CountBuild::new(
		"DEFINE TABLE t SCHEMALESS;
		 DEFINE INDEX ic ON t COUNT;
		 CREATE t:[42, 'k'] SET a = 1;",
	)
	.await?;
	let late = number_then_string(42, "a");
	let alias = RecordIdKey::Array(
		vec![Value::Number(crate::val::Number::Int(42)), Value::None, Value::from("a")].into(),
	);
	// The premise: the other record's older marker sits at `[42, 'a']`'s key.
	assert_eq!(b.older_key(&alias)?, b.own_key(&late)?);

	let tx = b.ds.transaction(TransactionType::Write).await?;
	b.queue_older(&tx, 1, mutation(&alias, true, false)).await?;
	b.queue_older(&tx, 2, mutation(&late, true, false)).await?;
	tx.commit().await?;

	let values = b.records().await?;
	assert_eq!(values.len(), 1, "only `[42, 'k']` is live");
	let (indexed, baseline) = b.scan_in_batches(&[&values]).await?;
	assert_eq!(indexed, vec![2, 1], "`[42, 'k']` and `[42, NONE, 'a']`, then `[42, 'a']`");
	assert_eq!(baseline, 3);
	Ok(())
}

/// Leaves a generation-2 build of `ikb`'s index `Building` under an expired
/// owner with no index data, checkpointed after `cursor` if one is given, so
/// that a takeover picks it up.
async fn strand_count_build(
	ds: &Datastore,
	ikb: &IndexKeyBase,
	cursor: Option<RecordIdKey>,
) -> Result<()> {
	let expired = Utc::now() - chrono::Duration::seconds(BUILD_OWNER_LEASE_SECS + 5);
	let tx = ds.transaction(TransactionType::Write).await?;
	tx.del_prefix_key(&IdxRoot {
		ns: ikb.ns(),
		db: ikb.db(),
		tb: Cow::Borrowed(ikb.table()),
		ix: ikb.index(),
	})
	.await?;
	tx.set_key(
		&ikb.new_bs_key(),
		&IndexBuildState {
			generation: 2,
			phase: IndexBuildPhase::Building,
			owner: Some(Uuid::new_v4()),
			next_ticket: 0,
			initial_complete: false,
			updated_at: expired,
			owner_heartbeat_at: Some(expired),
			error: None,
			report_status: Some(IndexBuildReportStatus::Indexing),
			initial: cursor.as_ref().map(|_| 1),
			updated: None,
			pending: None,
			initial_cursor: cursor,
		},
	)
	.await?;
	tx.commit().await
}

/// Takes over the build of `ix` on `table` and runs it to the end.
async fn take_over_build(
	ds: &Datastore,
	session: &Session,
	(ns, db, table, ix): (NamespaceId, DatabaseId, &TableName, &Arc<IndexDefinition>),
) -> Result<()> {
	let build = new_building_for_index(ds, session, ns, db, table, Arc::clone(ix)).await?;
	let acquired = build
		.acquire_build_state()
		.await?
		.expect("expired build state should be available for takeover");
	build.run_acquired(acquired).await
}

/// A COUNT build taken over from a checkpoint rescans when resuming would leave
/// an older node's marker unmet.
///
/// An older node deleted `[42, 'intro']` before the scan, and its marker sorts
/// after `[42, 'k']`, where the previous owner's last batch ended: the next span
/// would have counted the record. A resume rebuilds that marker at the
/// record's own key, behind the checkpoint, where nothing meets it again.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn a_count_takeover_rescans_rather_than_leave_an_older_marker_unmet() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE TABLE t SCHEMALESS;
		 CREATE t:[42, 'k'] SET a = 1;
		 CREATE t:[43, 'x'] SET a = 1;
		 DEFINE INDEX ic ON t COUNT;",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "t", "ic").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	strand_count_build(&ds, &ikb, Some(number_then_string(42, "k"))).await?;
	let tx = ds.transaction(TransactionType::Write).await?;
	let delete = mutation(&number_then_string(42, "intro"), true, false);
	queue_in_previous_encoding(&tx, &ikb, 2, 1, delete, true).await?;
	tx.commit().await?;

	take_over_build(&ds, &session, (ns, db, &table, &ix)).await?;

	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(state.phase, IndexBuildPhase::Online);
	// A resume would report 1 checkpointed and `[43, 'x']`.
	assert_eq!(state.initial, Some(3), "both live records and `[42, 'intro']`, rescanned");
	assert_eq!(count_query_value(&ds, &session, "SELECT count() FROM t GROUP ALL").await?, 2);
	Ok(())
}

/// A COUNT takeover of a checkpoint a previous release's owner wrote rescans
/// when one of its markers names a record at or before the cursor.
///
/// That owner's merge covered its markers in the index format's order, which
/// puts the gone `[1, 'z']`'s marker after `[1f]`'s key under this release,
/// although the record sorts before `[1f]`. Resuming after `[1f]` would never
/// count it.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn a_count_takeover_rescans_a_checkpoint_that_would_lose_a_record() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE TABLE t SCHEMALESS;
		 CREATE t:[1, 'a'] SET a = 1;
		 CREATE t:[1f] SET a = 1;
		 CREATE t:[2] SET a = 1;
		 DEFINE INDEX ic ON t COUNT;",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "t", "ic").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	let float = RecordIdKey::Array(vec![Value::Number(crate::val::Number::Float(1.0))].into());
	let gone = RecordIdKey::Array(
		vec![Value::Number(crate::val::Number::Int(1)), Value::from("z")].into(),
	);
	// The premise: the gone record sorts before the cursor, its older key after.
	assert!(RecordIdentity(gone.clone()) < RecordIdentity(float.clone()));
	assert!(
		primary_append_key_in_previous_encoding(&ikb, 2, &gone)?
			> ikb.new_bp_key(2, &float).encode_key()?.to_vec()
	);
	strand_count_build(&ds, &ikb, Some(float)).await?;
	let tx = ds.transaction(TransactionType::Write).await?;
	queue_in_previous_encoding(&tx, &ikb, 2, 1, mutation(&gone, true, false), true).await?;
	tx.commit().await?;

	take_over_build(&ds, &session, (ns, db, &table, &ix)).await?;

	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(state.phase, IndexBuildPhase::Online);
	assert_eq!(state.initial, Some(4), "the three live records and `[1, 'z']`, rescanned");
	assert_eq!(count_query_value(&ds, &session, "SELECT count() FROM t GROUP ALL").await?, 3);
	Ok(())
}

/// A COUNT build's batch that fails to commit is retried from the merge's
/// progress before it, so the retry merges the same span.
///
/// `t:b` is deleted while the build is in flight, marked at its own key in the
/// batch's span, and the batch's first commit is refused.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn a_retried_count_batch_merges_its_span_again() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE TABLE t SCHEMALESS;
		 CREATE t:b SET a = 1;
		 CREATE t:k SET a = 1;
		 DEFINE INDEX ic ON t COUNT;",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "t", "ic").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	strand_count_build(&ds, &ikb, None).await?;
	execute_all_retrying_conflicts(&ds, &session, "DELETE t:b RETURN NONE").await?;

	let site = RetryableConflictSite::ConcurrentIndexInitialBatch;
	let _guard = inject_retryable_conflict(site, ds.id());
	take_over_build(&ds, &session, (ns, db, &table, &ix)).await?;

	assert_eq!(retryable_conflict_count(site, ds.id()), 0, "the batch was retried");
	assert_eq!(count_query_value(&ds, &session, "SELECT count() FROM t GROUP ALL").await?, 1);
	Ok(())
}

/// A build of any kind baselines a record from the queued old state an older
/// node marked under its spelling.
///
/// An older node updated `[42, 'intro']` from 'old' to 'new', marking it at a
/// key only that spelling gives the record. Indexing the record as it stands
/// would leave replay removing an 'old' that was never indexed.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn a_build_of_any_kind_baselines_from_an_older_marker() -> Result<()> {
	use surrealdb_types::Value as PublicValue;
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE TABLE t SCHEMALESS;
		 DEFINE ANALYZER simple TOKENIZERS blank;
		 DEFINE INDEX ia ON t FIELDS a FULLTEXT ANALYZER simple BM25;
		 CREATE t:[42, 'intro'] SET a = 'new';",
	)
	.await?;
	let (ns, db, table, ia) = get_table_index(&ds, "t", "ia").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ia.index_id);
	let building = new_building_for_index(&ds, &session, ns, db, &table, ia).await?;
	let generation: BuildGeneration = 1;

	let tx = ds.transaction(TransactionType::Write).await?;
	let update = Appending {
		old_values: Some(vec![Value::from("old")]),
		new_values: Some(vec![Value::from("new")]),
		..mutation(&number_then_string(42, "intro"), true, true)
	};
	queue_in_previous_encoding(&tx, &ikb, generation, 9, update, true).await?;
	tx.commit().await?;
	building.build_generation.store(generation, Ordering::Release);

	let values = scan_records(&ds, ns, db, &table).await?;
	let ctx = building.new_write_tx_ctx().await?;
	let tx = ctx.tx();
	building.index_initial_batch(&ctx, &tx, &values, 0, &mut false, &mut None).await?;
	tx.commit().await?;

	let res = ds.execute("SELECT VALUE id FROM t WHERE a @@ 'old'", &session, None).await?;
	let PublicValue::Array(rows) = res.into_iter().next().expect("one result").result? else {
		anyhow::bail!("unexpected result shape");
	};
	assert_eq!(rows.len(), 1, "`[42, 'intro']` is baselined from its queued old state");
	Ok(())
}

/// The scan baselines a record against a marker only when the marker's queued
/// mutation names that record.
///
/// An older node's marker for `[1, NONE]` sits at the key this release builds
/// for `[1]`. Taking it as `[1]`'s would index `[1]` with another record's
/// queued old state.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn a_marker_naming_another_record_is_not_the_scanned_records_baseline() -> Result<()> {
	use surrealdb_types::Value as PublicValue;
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE TABLE t SCHEMALESS;
		 DEFINE ANALYZER simple TOKENIZERS blank;
		 DEFINE INDEX ia ON t FIELDS a FULLTEXT ANALYZER simple BM25;
		 CREATE t:[1] SET a = 'live';",
	)
	.await?;
	let (ns, db, table, ia) = get_table_index(&ds, "t", "ia").await?;
	let ix_id = ia.index_id;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix_id);
	let building = new_building_for_index(&ds, &session, ns, db, &table, ia).await?;
	let generation: BuildGeneration = 1;
	let one_none = aliasing_chain(2).pop().expect("two links");

	let tx = ds.transaction(TransactionType::Write).await?;
	queue_with_marker_in_previous_encoding(
		&tx,
		&ikb,
		generation,
		9,
		&one_none,
		Some(vec![Value::from("queued")]),
		true,
	)
	.await?;
	tx.commit().await?;
	building.build_generation.store(generation, Ordering::Release);

	let values = scan_records(&ds, ns, db, &table).await?;
	let ctx = building.new_write_tx_ctx().await?;
	let tx = ctx.tx();
	building.index_initial_batch(&ctx, &tx, &values, 0, &mut false, &mut None).await?;
	tx.commit().await?;

	let matches = |sql: &'static str| {
		let ds = &ds;
		let session = &session;
		async move {
			match ds.execute(sql, session, None).await?.remove(0).result? {
				PublicValue::Array(rows) => Ok(rows.len()),
				other => anyhow::bail!("unexpected result: {other:?}"),
			}
		}
	};
	assert_eq!(
		matches("SELECT VALUE id FROM t WHERE a @@ 'queued'").await?,
		0,
		"`t:[1]` must not be indexed with `[1, NONE]`'s queued state"
	);
	assert_eq!(matches("SELECT VALUE id FROM t WHERE a @@ 'live'").await?, 1);
	Ok(())
}

/// Start a `REBUILD INDEX`'s build the way a bare blocking rebuild does: on a
/// write transaction whose owner awaits work queued for after its commit,
/// staging `built`, the definition at the current format the rebuild builds,
/// and leaving the transaction open for the caller to close.
///
/// Returns the transaction, the queue its owner holds, and the local builder.
#[cfg(feature = "kv-mem")]
async fn queue_rebuild_on_an_open_statement(
	ds: &Arc<Datastore>,
	session: &Session,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
	built: Arc<IndexDefinition>,
) -> Result<(
	Arc<crate::kvs::Transaction>,
	surrealdb_datastore::close::AfterCommitWaits,
	IndexBuilding,
)> {
	let waits = surrealdb_datastore::close::AfterCommitWaits::default();
	let txn =
		Arc::new(ds.transaction(TransactionType::Write).await?.with_after_commit_waits(&waits));
	let table_def = catch!(txn, txn.get_tb(ns, db, table, None).await).expect("table should exist");
	catch!(txn, txn.put_tb_index(ns, db, table, &built).await);
	let mut ctx = ds.setup_ctx()?;
	ctx.set_transaction(Arc::clone(&txn));
	let ctx = ctx.freeze();
	let builder = ctx.get_index_builder().expect("index builder should exist").clone();
	let index_id = built.index_id;
	builder
		.run_awaited_after_commit(&ctx, ds.setup_options(session), table_def.table_id, built, &txn)
		.await?;
	let building = local_builder_for_key(ds, ns, db, table, index_id)
		.await?
		.expect("the rebuild registers a local builder");
	Ok((txn, waits, building))
}

/// The definition a `REBUILD INDEX` of `ix` builds: `ix` stamped with the
/// current format version.
#[cfg(feature = "kv-mem")]
fn restamped(ix: &IndexDefinition) -> Arc<IndexDefinition> {
	let mut built = ix.clone();
	built.format_version = crate::catalog::INDEX_FORMAT_VERSION;
	Arc::new(built)
}

/// A rebuild awaited after its commit whose build publishes before its
/// statement rolls back, `legacy` choosing whether the index was persisted at
/// an earlier format. Returns the build state the rollback leaves.
#[cfg(feature = "kv-mem")]
async fn rebuild_published_before_its_rollback(
	legacy: bool,
) -> Result<(Arc<Datastore>, Session, IndexBuildState)> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(&ds, &session, "DEFINE INDEX ix ON t FIELDS v; CREATE t:1 SET v = 1;").await?;
	if legacy {
		downgrade_index_format(&ds, "t", "ix", 0).await?;
	}
	let (ns, db, table, ix) = get_table_index(&ds, "t", "ix").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	let (txn, waits, building) =
		queue_rebuild_on_an_open_statement(&ds, &session, ns, db, &table, restamped(&ix)).await?;
	wait_for_finished_builder(&building).await?;
	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(
		state.phase,
		IndexBuildPhase::Online,
		"the build did not publish before the rollback"
	);
	assert!(
		state.owner.is_some(),
		"the published build must hold its fence while its statement is open"
	);
	txn.cancel().await?;
	assert!(waits.take().await.is_empty(), "a rolled-back statement leaves nothing to await");
	let state = durable_build_state(&ds, &ikb).await?;
	Ok((ds, session, state))
}

/// A rebuild awaited after its commit whose build has already published when
/// its statement rolls back is withdrawn if the definition the rollback keeps
/// does not read the layout the build wrote: an index persisted at an earlier
/// format keeps its own doc-ID layout. Withdrawn, the index is reported
/// `aborted` rather than `ready`, writes to its table still succeed, and the
/// next `REBUILD INDEX` restores it.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_published_rebuild_whose_statement_rolls_back_to_a_legacy_definition_is_withdrawn()
-> Result<()> {
	let (ds, session, state) = rebuild_published_before_its_rollback(true).await?;
	assert_eq!(state.phase, IndexBuildPhase::Error, "the stale build was not withdrawn: {state:?}");
	assert_eq!(state.report_status, Some(IndexBuildReportStatus::Aborted));
	assert_eq!(state.owner, None, "a withdrawn build must not stay fenced");
	let (_, _, _, ix) = get_table_index(&ds, "t", "ix").await?;
	assert_eq!(ix.format_version, 0, "the rollback must keep the legacy definition");
	execute_all(&ds, &session, "CREATE t:2 SET v = 2").await?;
	execute_all(&ds, &session, "REBUILD INDEX ix ON t").await?;
	wait_for_index_ready(&ds, &session, "t", "ix").await?;
	expect_indexed_query_len(&ds, &session, "SELECT id FROM t WITH INDEX ix WHERE v = 2", 1)
		.await?;
	Ok(())
}

/// The same rollback leaves a published build `Online` when the definition it
/// keeps already describes the layout the build wrote: the index was at the
/// current format, so the build serves it.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_published_rebuild_whose_statement_rolls_back_to_the_same_layout_stays_ready()
-> Result<()> {
	let (_ds, _session, state) = rebuild_published_before_its_rollback(false).await?;
	assert_eq!(state.phase, IndexBuildPhase::Online, "a build its definition reads was withdrawn");
	assert_eq!(state.report_status, Some(IndexBuildReportStatus::Ready));
	assert_eq!(state.owner, None, "the rollback must release the fence");
	Ok(())
}

/// A rebuild awaited after its commit whose statement rolls back while its
/// build waits for a writer's reservation, past its initial scan, publishes
/// neither `Closing` nor `Online` afterwards: the build stops short on the
/// abort and its task ends the generation `aborted`.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_rebuild_whose_statement_rolls_back_after_its_scan_is_aborted_not_published() -> Result<()>
{
	let (ds, session) = new_index_test_ds().await?;
	execute_all(&ds, &session, "DEFINE INDEX ix ON t FIELDS v; CREATE t:1 SET v = 1;").await?;
	let (ns, db, table, ix) = get_table_index(&ds, "t", "ix").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	// Hold the build at its start, so the reservation below is in place
	// before it can reach `Closing`.
	let site = RetryableConflictSite::ConcurrentIndexInitialCleanup;
	let paused = inject_retryable_conflicts(site, ds.id(), REPEATED_RETRY_CONFLICTS);
	let (txn, _waits, building) =
		queue_rebuild_on_an_open_statement(&ds, &session, ns, db, &table, restamped(&ix)).await?;
	wait_for_retry_conflict(site, ds.id(), REPEATED_RETRY_CONFLICTS).await?;
	let generation = durable_build_state(&ds, &ikb).await?.generation;
	// A writer's reservation that neither commits nor expires: the build
	// scans, replays, moves to `Closing` and then waits on it.
	let tx = ds.transaction(TransactionType::Write).await?;
	tx.set_key(
		&ikb.new_br_key(generation, 1_000),
		&IndexBuildReservation {
			node: ds.id(),
			expires_at: Utc::now() + chrono::Duration::hours(1),
		},
	)
	.await?;
	tx.commit().await?;
	drop(paused);
	timeout(Duration::from_secs(10), async {
		loop {
			if durable_build_state(&ds, &ikb).await?.phase == IndexBuildPhase::Closing {
				return Ok::<_, anyhow::Error>(());
			}
			sleep(Duration::from_millis(10)).await;
		}
	})
	.await
	.map_err(|_| anyhow::anyhow!("the build never reached Closing"))??;
	txn.cancel().await?;
	wait_for_finished_builder(&building).await?;
	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(state.phase, IndexBuildPhase::Error, "the aborted build was published: {state:?}");
	assert_eq!(state.report_status, Some(IndexBuildReportStatus::Aborted));
	assert_eq!(state.owner, None, "an aborted build must not stay fenced");
	Ok(())
}

/// A rebuild awaited after its commit whose build has published `Online` and
/// is still draining, held in its post-publish HNSW drain, when an abort
/// reaches it before its statement's half of the release gate: the order a
/// rollback produces when its abort lands while it reads the committed
/// catalog. The task then reaches the release gate first, and the statement
/// rolls back after it. `legacy` chooses whether the index was persisted at
/// an earlier format. Returns the build state the rollback leaves.
#[cfg(feature = "kv-mem")]
async fn published_rebuild_aborted_before_its_rollback(legacy: bool) -> Result<IndexBuildState> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE INDEX hx ON t FIELDS vec HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32",
	)
	.await?;
	if legacy {
		downgrade_index_format(&ds, "t", "hx", 0).await?;
	}
	execute_all(&ds, &session, "CREATE t:1 SET vec = [1.0, 0.0]; CREATE t:2 SET vec = [0.0, 1.0];")
		.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "t", "hx").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	let site = RetryableConflictSite::ConcurrentIndexHnswPendingCompaction;
	let draining = inject_retryable_conflicts(site, ds.id(), REPEATED_RETRY_CONFLICTS);
	let (txn, _waits, building) =
		queue_rebuild_on_an_open_statement(&ds, &session, ns, db, &table, restamped(&ix)).await?;
	wait_for_retry_conflict(site, ds.id(), REPEATED_RETRY_CONFLICTS).await?;
	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(state.phase, IndexBuildPhase::Online, "the build is not held after its publish");
	assert!(state.owner.is_some(), "the published build must hold its fence while it drains");
	building.abort();
	wait_for_finished_builder(&building).await?;
	drop(draining);
	txn.cancel().await?;
	durable_build_state(&ds, &ikb).await
}

/// Once a build has published, an abort that reaches it before its
/// statement's rollback does not decide it: the rollback's release does, and
/// for an index already at the current format it keeps the build `Online`.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_published_rebuild_aborted_before_its_rollback_stays_ready() -> Result<()> {
	let state = published_rebuild_aborted_before_its_rollback(false).await?;
	assert_eq!(state.phase, IndexBuildPhase::Online, "the published build was aborted: {state:?}");
	assert_eq!(state.report_status, Some(IndexBuildReportStatus::Ready));
	assert_eq!(state.owner, None, "the rollback must release the fence");
	Ok(())
}

/// The same order on an index persisted at an earlier format: the rollback's
/// release withdraws the published build, as the definition it keeps does not
/// read what the build wrote.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_published_legacy_rebuild_aborted_before_its_rollback_is_withdrawn() -> Result<()> {
	let state = published_rebuild_aborted_before_its_rollback(true).await?;
	assert_eq!(state.phase, IndexBuildPhase::Error, "the stale build was not withdrawn: {state:?}");
	assert_eq!(state.report_status, Some(IndexBuildReportStatus::Aborted));
	assert_eq!(state.owner, None, "a withdrawn build must not stay fenced");
	Ok(())
}

/// A rollback whose close drain has already used its allowance does not read
/// the committed catalog for a published build: it withdraws the build, the
/// side that never leaves `ready` an index its definition may not read,
/// rather than hold the statement's error behind a read.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_rollback_past_its_drain_allowance_withdraws_a_published_rebuild() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(&ds, &session, "DEFINE INDEX ix ON t FIELDS v; CREATE t:1 SET v = 1;").await?;
	let (ns, db, table, ix) = get_table_index(&ds, "t", "ix").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	let (txn, _waits, building) =
		queue_rebuild_on_an_open_statement(&ds, &session, ns, db, &table, restamped(&ix)).await?;
	wait_for_finished_builder(&building).await?;
	assert_eq!(durable_build_state(&ds, &ikb).await?.phase, IndexBuildPhase::Online);
	let drain_started_at = Instant::now()
		.checked_sub(Duration::from_secs(60))
		.expect("the clock has run for a minute");
	super::builder::ReleaseBuildOwnership::rollback_action(Arc::clone(&building))
		.run(drain_started_at)
		.await?;
	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(
		state.phase,
		IndexBuildPhase::Error,
		"the build was kept past the allowance: {state:?}"
	);
	assert_eq!(state.report_status, Some(IndexBuildReportStatus::Aborted));
	assert_eq!(state.owner, None, "a withdrawn build must not stay fenced");
	// The rollback the transaction then runs finds nothing left to release.
	txn.cancel().await?;
	assert_eq!(durable_build_state(&ds, &ikb).await?.phase, IndexBuildPhase::Error);
	Ok(())
}

/// A rebuild awaited after its commit whose build stops on the memory
/// threshold, a transient error that leaves its generation for the resume
/// scan, and whose statement then rolls back. `legacy` chooses whether the
/// index was persisted at an earlier format. The lease is then expired and
/// the resume scan run. Returns how many builds the scan adopted and the build
/// state it leaves.
#[cfg(feature = "kv-mem")]
async fn interrupted_rebuild_rolled_back_then_resume_scan(
	legacy: bool,
) -> Result<(usize, IndexBuildState)> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(&ds, &session, "DEFINE INDEX ix ON t FIELDS v").await?;
	if legacy {
		downgrade_index_format(&ds, "t", "ix", 0).await?;
	}
	execute_all(&ds, &session, "CREATE t:1 SET v = 1; CREATE t:2 SET v = 2;").await?;
	let (ns, db, table, ix) = get_table_index(&ds, "t", "ix").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	let interrupted = inject_non_retryable_error(
		NonRetryableErrorSite::ConcurrentIndexInitialBatchMemoryThreshold,
		ds.id(),
	);
	let (txn, _waits, building) =
		queue_rebuild_on_an_open_statement(&ds, &session, ns, db, &table, restamped(&ix)).await?;
	wait_for_finished_builder(&building).await?;
	drop(interrupted);
	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(
		state.phase,
		IndexBuildPhase::Building,
		"the build was not left for the resume scan"
	);
	txn.cancel().await?;
	// Expire the interrupted builder's lease, so a generation it still owns
	// is one the resume scan adopts.
	let expired = Utc::now() - chrono::Duration::seconds(BUILD_OWNER_LEASE_SECS + 5);
	let mut stranded = durable_build_state(&ds, &ikb).await?;
	stranded.updated_at = expired;
	if stranded.owner.is_some() {
		stranded.owner_heartbeat_at = Some(expired);
	}
	set_durable_build_state(&ds, &ikb, stranded).await?;
	let resumed =
		ds.resume_stalled_index_builds(Duration::from_secs(30), CancellationToken::new()).await?;
	Ok((resumed, durable_build_state(&ds, &ikb).await?))
}

/// A rolled-back rebuild of a legacy-format index whose build stopped on a
/// transient error is withdrawn, not left `Building` for the resume scan to
/// carry on through the committed legacy definition from checkpoints written
/// for the current format.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn an_interrupted_rebuild_rolled_back_to_a_legacy_definition_is_withdrawn_not_resumed()
-> Result<()> {
	let (resumed, state) = interrupted_rebuild_rolled_back_then_resume_scan(true).await?;
	assert_eq!(resumed, 0, "the resume scan adopted the rolled-back build: {state:?}");
	assert_eq!(state.phase, IndexBuildPhase::Error, "the stale build was not withdrawn: {state:?}");
	assert_eq!(state.report_status, Some(IndexBuildReportStatus::Aborted));
	assert_eq!(state.owner, None, "a withdrawn build must not stay fenced");
	Ok(())
}

/// The same interruption and rollback on an index already at the current
/// format leaves the build to the resume scan: the definition the rollback
/// keeps describes what its checkpoints were written for.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn an_interrupted_rebuild_rolled_back_to_the_same_layout_is_resumed() -> Result<()> {
	let (resumed, state) = interrupted_rebuild_rolled_back_then_resume_scan(false).await?;
	assert_eq!(resumed, 1, "the resume scan did not adopt the interrupted build: {state:?}");
	assert_ne!(state.phase, IndexBuildPhase::Error, "the resumable build was withdrawn: {state:?}");
	Ok(())
}

/// The keys of `ix`'s entries with the index's own prefix removed, so that two
/// indexes over the same fields compare equal exactly when they hold the same
/// entries.
#[cfg(feature = "kv-mem")]
async fn index_entry_suffixes(
	ds: &Datastore,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
	ix: IndexId,
) -> Result<std::collections::BTreeSet<Vec<u8>>> {
	let prefix = EntryPrefix {
		ns,
		db,
		tb: Cow::Borrowed(table),
		ix,
	};
	let start = KVSubspace::encode_bound(&prefix)?.to_vec();
	let tx = ds.transaction(TransactionType::Read).await?;
	let keys = catch!(tx, tx.keys(prefix.range()?, u32::MAX, 0, None).await);
	tx.cancel().await?;
	keys.into_iter()
		.map(|key| {
			anyhow::ensure!(key.starts_with(&start), "entry key outside its index prefix");
			Ok(key[start.len()..].to_vec())
		})
		.collect()
}

/// A build comes online while writers keep queueing behind its replay, and the
/// handover to inline maintenance loses and leaves no entry.
///
/// The catch-up hook commits a write before every read of the replay that runs
/// while admissions are open, so no read of it ever finds the queues empty: a
/// replay that waited for that would never reach `Closing`. Other writers run
/// throughout, across `Closing` (where they wait) and past `Online` (where they
/// maintain the index inline). The finished index must then hold exactly the
/// entries of an index built afterwards with no writers.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_build_comes_online_while_writers_keep_queueing() -> Result<()> {
	const ROWS: u64 = 200;
	const WRITERS: u64 = 4;

	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		&format!(
			"DEFINE TABLE t SCHEMALESS;
			 CREATE |t:1..={ROWS}| SET x = 0 RETURN NONE;
			 DEFINE INDEX ix ON t FIELDS x;"
		),
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "t", "ix").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);

	// Strand a fresh generation `Building` under an expired owner, with no
	// index data and its own ticket counter, for a hand-driven takeover.
	let expired = Utc::now() - chrono::Duration::seconds(BUILD_OWNER_LEASE_SECS + 5);
	let mut state = durable_build_state_for_phase(IndexBuildPhase::Building, 2, None);
	state.initial_complete = false;
	state.initial = None;
	state.updated_at = expired;
	state.owner_heartbeat_at = Some(expired);
	let tx = ds.transaction(TransactionType::Write).await?;
	tx.del_prefix_key(&IdxRoot {
		ns,
		db,
		tb: Cow::Borrowed(&table),
		ix: ix.index_id,
	})
	.await?;
	tx.set_key(&ikb.new_bs_key(), &state).await?;
	tx.set_key(&ikb.new_bt_key(2), &(0 as BuildTicket)).await?;
	tx.commit().await?;

	// One write per catch-up read, committed before the read runs.
	let (hook, mut reads) = super::replay::catch_up_hook::register(ds.id());
	let hooked = tokio::spawn({
		let (ds, session) = (Arc::clone(&ds), session.clone());
		async move {
			let mut reads_held = 0usize;
			while let Some(done) = reads.recv().await {
				execute_all_retrying_conflicts(&ds, &session, "UPDATE t:1 SET x += 1 RETURN NONE")
					.await?;
				reads_held += 1;
				let _ = done.send(());
			}
			Ok::<_, anyhow::Error>(reads_held)
		}
	});
	// Writers on every other row, for the whole build and past it.
	let stop = Arc::new(AtomicBool::new(false));
	let writers: Vec<_> = (0..WRITERS)
		.map(|w| {
			let (ds, session, stop) = (Arc::clone(&ds), session.clone(), Arc::clone(&stop));
			tokio::spawn(async move {
				let mut i = 0u64;
				while !stop.load(Ordering::Acquire) {
					let id = 2 + (i * 7 + w * 53) % (ROWS - 1);
					execute_all_retrying_conflicts(
						&ds,
						&session,
						&format!("UPDATE t:{id} SET x += 1 RETURN NONE"),
					)
					.await?;
					i += 1;
				}
				Ok::<_, anyhow::Error>(i)
			})
		})
		.collect();

	let build = new_building_for_index(&ds, &session, ns, db, &table, Arc::clone(&ix)).await?;
	let acquired = build
		.acquire_build_state()
		.await?
		.expect("the expired generation should be available for takeover");
	assert_eq!(acquired.generation, 2);
	timeout(Duration::from_secs(10), build.run_acquired(acquired)).await.map_err(|_| {
		anyhow::anyhow!("the build did not come online while writers kept queueing")
	})??;
	assert_eq!(durable_build_state(&ds, &ikb).await?.phase, IndexBuildPhase::Online);

	drop(hook);
	let reads_held = hooked.await??;
	assert!(reads_held >= 1, "the catch-up replay must have read the queues");
	// Keep writing for a moment against the online index, then stop.
	sleep(Duration::from_millis(100)).await;
	stop.store(true, Ordering::Release);
	let mut written = 0;
	for writer in writers {
		written += writer.await??;
	}
	assert!(written > 0, "the writers must have committed while the build ran");

	execute_all(&ds, &session, "DEFINE INDEX reference ON t FIELDS x;").await?;
	let (_, _, _, reference) = get_table_index(&ds, "t", "reference").await?;
	let built = index_entry_suffixes(&ds, ns, db, &table, ix.index_id).await?;
	let expected = index_entry_suffixes(&ds, ns, db, &table, reference.index_id).await?;
	assert_eq!(expected.len(), ROWS as usize, "one entry per record");
	assert_eq!(built, expected, "the built index must hold exactly the records' current entries");
	Ok(())
}
