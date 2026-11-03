use std::borrow::Cow;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use anyhow::Result;
use chrono::Utc;
use futures::FutureExt;
#[cfg(feature = "kv-mem")]
use surrealdb_datastore::values::fulltext::TermDocsResidual;
use surrealdb_kvs::TransactionType;
#[cfg(feature = "kv-surrealkv")]
use temp_dir::TempDir;
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use web_time::Instant;

use super::builder::{BEST_EFFORT_TRANSITION_RETRIES, BuildCompletion, Building, IndexKey};
use super::state::{build_owner_expired, report_status_from_phase};
use super::*;
use crate::catalog::providers::{
	CatalogProvider, DatabaseProvider, NamespaceProvider, TableProvider,
};
use crate::catalog::{
	DatabaseId, FullTextParams, INDEX_FORMAT_VERSION, Index, IndexDefinition, IndexId, NamespaceId,
	Record, Scoring, TableId,
};
use crate::dbs::Session;
use crate::err::Error;
use crate::expr::Idiom;
use crate::idx::IndexKeyBase;
use crate::idx::index::IndexOperation;
use crate::key::reclaim::{Expunge, ReclaimKind};
use crate::key::schema::{
	DocKeyPrefix, DocLookupIdentityPrefix, DocLookupKey, DocLookupPrefix, DocPendingKey,
	DocPendingPrefix, DocStatsBatchPrefix, EntryPrefix, IdxRoot, IndexCompactionIxPrefix,
	IndexCountKey, IndexCountPrefix, ReclaimKey, ReclaimPrefix, RecordKey, RecordPrefix,
	TermChangeBatchPrefix,
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
use crate::kvs::{
	Datastore, DatastoreError, QueryRequest, is_retryable_transaction_conflict, storage_error,
};
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
		basis: None,
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
) -> Result<Arc<Building>> {
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
	)?
	.signalling(&ds.index_builder().closing_settled);
	tx.cancel().await?;
	Ok(Arc::new(build))
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
/// them fails the fenced arm below. Compaction is fenced separately, by the
/// locked read of the definition in its write guard; see
/// `assert_compaction_write_guard_conflicts_with_a_retirement_that_commits_after_it`.
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

/// The id of the table `table` names now.
#[cfg(feature = "kv-mem")]
async fn current_table_id(
	ds: &Datastore,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
) -> Result<TableId> {
	let tx = ds.transaction(TransactionType::Read).await?;
	let tb = catch!(tx, tx.expect_tb(ns, db, table).await);
	tx.cancel().await?;
	Ok(tb.table_id)
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

/// The keys under index `ix`'s root, leaving out the keys that record the
/// value shapes it stored ([`surrealdb_datastore::index_shapes`]).
async fn index_prefix_key_count(
	ds: &Datastore,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
	ix: IndexId,
) -> Result<usize> {
	Ok(index_entries(ds, ns, db, table, ix).await?.len())
}

/// The keys and values under index `ix`'s root, in key order, leaving out the
/// keys that record the value shapes it stored.
async fn index_entries(
	ds: &Datastore,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
	ix: IndexId,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
	use crate::key::schema::{IndexBuildShapesKey, IndexShapesKey, IndexShapesTrackedKey};
	let tx = ds.transaction(TransactionType::Read).await?;
	let key = IdxRoot {
		ns,
		db,
		tb: Cow::Borrowed(table),
		ix,
	};
	let keys: Vec<(Vec<u8>, Vec<u8>)> = catch!(tx, tx.get_prefix_key(&key, None).await);
	tx.cancel().await?;
	let tb = || Cow::Borrowed(table);
	let shapes = [
		IndexShapesTrackedKey {
			ns,
			db,
			tb: tb(),
			ix,
		}
		.encode_key()?,
		IndexShapesKey {
			ns,
			db,
			tb: tb(),
			ix,
		}
		.encode_key()?,
		IndexBuildShapesKey {
			ns,
			db,
			tb: tb(),
			ix,
		}
		.encode_key()?,
	];
	Ok(keys
		.into_iter()
		.filter(|(k, _)| !shapes.iter().any(|s| s.as_slice() == k.as_slice()))
		.collect())
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

/// The index `test` on `user`, built over one record, and its durable build state.
async fn define_overwritten_index_test() -> Result<(Arc<Datastore>, Session, IndexKeyBase)> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:one SET email = 'one@example.com', account = 'apple' RETURN NONE;
			DEFINE INDEX test ON user FIELDS email COMMENT 'first';
			CREATE user:two SET email = 'two@example.com', account = 'banana' RETURN NONE;
			",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "user", "test").await?;
	let ikb = IndexKeyBase::new(ns, db, table, ix.index_id);
	assert_eq!(durable_build_state(&ds, &ikb).await?.phase, IndexBuildPhase::Online);
	Ok((ds, session, ikb))
}

#[tokio::test(flavor = "multi_thread")]
async fn define_index_overwrite_unchanged_keeps_a_ready_index_without_rebuild() -> Result<()> {
	let (ds, session, ikb) = define_overwritten_index_test().await?;
	let before = durable_build_state(&ds, &ikb).await?;
	for sql in [
		"DEFINE INDEX OVERWRITE test ON user FIELDS email COMMENT 'first';",
		"DEFINE INDEX OVERWRITE test ON user FIELDS email COMMENT 'first' CONCURRENTLY;",
		"DEFINE INDEX OVERWRITE test ON user FIELDS email COMMENT 'second';",
	] {
		execute_all(&ds, &session, sql).await?;
		let (_, _, _, current) = get_table_index(&ds, "user", "test").await?;
		assert_eq!(current.index_id, ikb.index(), "{sql} keeps the index's id");
		assert_eq!(current.format_version, INDEX_FORMAT_VERSION);
		let after = durable_build_state(&ds, &ikb).await?;
		assert_eq!(after.generation, before.generation, "{sql} starts no build");
		assert_eq!(after.phase, IndexBuildPhase::Online);
		assert_eq!(after.initial, before.initial, "{sql} scans nothing");
	}
	let (_, _, _, current) = get_table_index(&ds, "user", "test").await?;
	assert_eq!(current.comment.as_deref(), Some("second"), "the comment is refreshed");
	// The kept index went on being maintained by the write made after its build.
	expect_indexed_query_len(
		&ds,
		&session,
		"SELECT * FROM user WITH INDEX test WHERE email = 'two@example.com'",
		1,
	)
	.await?;
	Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn define_index_overwrite_changed_definition_rebuilds_with_fresh_index_id() -> Result<()> {
	let (ds, session, ikb) = define_overwritten_index_test().await?;
	execute_all(&ds, &session, "DEFINE INDEX OVERWRITE test ON user FIELDS account;").await?;
	let (ns, db, table, current) = get_table_index(&ds, "user", "test").await?;
	assert_ne!(current.index_id, ikb.index());
	let after =
		durable_build_state(&ds, &IndexKeyBase::new(ns, db, table, current.index_id)).await?;
	assert_eq!(after.phase, IndexBuildPhase::Online);
	assert_eq!(after.initial, Some(2), "the rebuild scans every record");
	assert!(!durable_build_state_exists(&ds, &ikb).await?, "the replaced index is retired");
	Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn define_index_overwrite_unchanged_rebuilds_an_index_that_is_not_online() -> Result<()> {
	for phase in [IndexBuildPhase::Error, IndexBuildPhase::Building, IndexBuildPhase::Closing] {
		let (ds, session, ikb) = define_overwritten_index_test().await?;
		let mut state = durable_build_state(&ds, &ikb).await?;
		state.phase = phase;
		state.owner = None;
		state.report_status = Some(report_status_from_phase(phase));
		set_durable_build_state(&ds, &ikb, state).await?;
		execute_all(
			&ds,
			&session,
			"DEFINE INDEX OVERWRITE test ON user FIELDS email COMMENT 'first';",
		)
		.await?;
		let (ns, db, table, current) = get_table_index(&ds, "user", "test").await?;
		assert_ne!(current.index_id, ikb.index(), "an index {phase:?} is rebuilt");
		let after =
			durable_build_state(&ds, &IndexKeyBase::new(ns, db, table, current.index_id)).await?;
		assert_eq!(after.phase, IndexBuildPhase::Online);
		assert_eq!(after.initial, Some(2));
	}
	Ok(())
}

/// An index stamped below its kind's current layout is rebuilt, and one stamped at
/// it is kept, whatever the global format version: a newer version that changes
/// only other kinds' layouts gains this index nothing.
#[tokio::test(flavor = "multi_thread")]
async fn define_index_overwrite_unchanged_keeps_an_index_at_its_kinds_current_layout() -> Result<()>
{
	let (ds, session, ikb) = define_overwritten_index_test().await?;
	let (ns, db, table, current) = get_table_index(&ds, "user", "test").await?;
	let layout = crate::catalog::current_layout_format_version(&current.index);
	assert!(layout < INDEX_FORMAT_VERSION, "a newer version changed only other kinds");
	let mut stamped = current.as_ref().clone();
	stamped.format_version = layout;
	let tx = ds.transaction(TransactionType::Write).await?;
	catch!(tx, tx.put_tb_index(ns, db, &table, &stamped).await);
	tx.commit().await?;
	execute_all(&ds, &session, "DEFINE INDEX OVERWRITE test ON user FIELDS email COMMENT 'first';")
		.await?;
	let (_, _, _, current) = get_table_index(&ds, "user", "test").await?;
	assert_eq!(current.index_id, ikb.index(), "an index at its kind's current layout is kept");
	assert_eq!(current.format_version, layout, "and keeps its stamp");
	Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn define_index_overwrite_unchanged_rebuilds_an_index_of_an_older_format() -> Result<()> {
	let (ds, session, ikb) = define_overwritten_index_test().await?;
	let (ns, db, table, current) = get_table_index(&ds, "user", "test").await?;
	let mut older = current.as_ref().clone();
	older.format_version = crate::catalog::current_layout_format_version(&current.index) - 1;
	let tx = ds.transaction(TransactionType::Write).await?;
	catch!(tx, tx.put_tb_index(ns, db, &table, &older).await);
	tx.commit().await?;
	execute_all(&ds, &session, "DEFINE INDEX OVERWRITE test ON user FIELDS email COMMENT 'first';")
		.await?;
	let (_, _, _, current) = get_table_index(&ds, "user", "test").await?;
	assert_ne!(current.index_id, ikb.index(), "an index of an older format is rebuilt");
	assert_eq!(current.format_version, INDEX_FORMAT_VERSION);
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

/// Runs `sql` and returns its rows' record ids in the order they came back.
#[cfg(feature = "kv-mem")]
async fn ordered_result_ids(ds: &Datastore, session: &Session, sql: &str) -> Result<Vec<String>> {
	use surrealdb_types::ToSql;

	let mut results = ds.execute(sql, session, None).await?;
	let value = results.remove(0).result?;
	let surrealdb_types::Value::Array(rows) = value else {
		anyhow::bail!("query returned non-array value: {value:?}");
	};
	rows.iter()
		.map(|row| {
			let surrealdb_types::Value::RecordId(rid) = row else {
				anyhow::bail!("row is not a record id: {row:?}");
			};
			Ok(rid.to_sql())
		})
		.collect()
}

/// Build `s_time` concurrently over three records holding scalar datetimes,
/// running `during` while the build is parked in the transaction that clears
/// the index and begins tracking its shapes, and return the shapes the index
/// records once its build is `Online`.
///
/// A document write admitted while the build is parked there is queued to
/// `!bg` with a `!bp` marker, so the initial scan indexes the record's queued
/// old value and only the build's replay of the queue writes its new one.
#[cfg(feature = "kv-mem")]
async fn index_shapes_after_concurrent_build(
	during: Option<&str>,
) -> Result<(Arc<Datastore>, Session, Arc<surrealdb_datastore::index_shapes::IndexShapes>)> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE s SCHEMALESS;
			CREATE s:1 SET time = d'2024-01-01' RETURN NONE;
			CREATE s:2 SET time = d'2024-01-02' RETURN NONE;
			CREATE s:3 SET time = d'2024-01-03' RETURN NONE;
			",
	)
	.await?;

	let site = RetryableConflictSite::ConcurrentIndexInitialCleanup;
	let node_id = ds.id();
	let guard = start_index_build_paused(
		&ds,
		&session,
		"DEFINE INDEX s_time ON s FIELDS time CONCURRENTLY",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "s", "s_time").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	if let Some(sql) = during {
		execute_all_retrying_conflicts(&ds, &session, sql).await?;
		let generation = durable_build_state(&ds, &ikb).await?.generation;
		assert!(
			durable_queue_generation_count(&ds, &ikb, generation).await? > 0,
			"the write must be queued for the build to replay"
		);
		assert!(
			retryable_conflict_count(site, node_id) > 0,
			"the build must still be parked ahead of its initial scan when the write is queued"
		);
	}
	drop(guard);
	wait_for_index_ready(&ds, &session, "s", "s_time").await?;
	assert_eq!(durable_build_state(&ds, &ikb).await?.phase, IndexBuildPhase::Online);

	let tx = ds.transaction(TransactionType::Read).await?;
	let shapes = catch!(tx, tx.index_shapes(ns, db, &table, ix.index_id).await);
	tx.cancel().await?;
	Ok((ds, session, shapes))
}

/// Every shape a build records commits before the build publishes the index
/// `Online`, the shapes of the writes it replays included. An array written
/// to the column while the build runs is queued and stored by the replay, and
/// a read planned once the index is `Online` plans the column as one that may
/// hold an array: `ORDER BY ... DESC LIMIT` answers the whole-value order, in
/// which the array sorts after every datetime, as its `WITH NOINDEX` twin does.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_build_records_the_shape_of_a_replayed_write_before_online() -> Result<()> {
	use surrealdb_datastore::index_shapes::ValueShape;

	let (ds, session, shapes) = index_shapes_after_concurrent_build(Some(
		"UPDATE s:1 SET time = [d'2023-01-01'] RETURN NONE",
	))
	.await?;
	assert!(shapes.is_tracked(), "a build tracks its index's shapes: {shapes:?}");
	assert!(
		shapes.may_hold(0, ValueShape::Array),
		"the replayed array must be recorded by the time the build is Online: {shapes:?}"
	);
	assert!(shapes.may_hold(0, ValueShape::Whole), "{shapes:?}");

	let expected = ["s:1", "s:3"];
	assert_eq!(
		ordered_result_ids(&ds, &session, "SELECT VALUE id FROM s ORDER BY time DESC LIMIT 2")
			.await?,
		expected
	);
	assert_eq!(
		ordered_result_ids(
			&ds,
			&session,
			"SELECT VALUE id FROM s WITH NOINDEX ORDER BY time DESC LIMIT 2"
		)
		.await?,
		expected
	);
	Ok(())
}

/// The control for
/// `concurrent_build_records_the_shape_of_a_replayed_write_before_online`: the
/// same build with no write queued records only whole values, so a read
/// planned once the index is `Online` answers `ORDER BY ... DESC LIMIT` from a
/// backward scan of the index.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_build_of_scalars_records_no_array_shape() -> Result<()> {
	use surrealdb_datastore::index_shapes::ValueShape;

	let (ds, session, shapes) = index_shapes_after_concurrent_build(None).await?;
	assert!(shapes.is_tracked(), "a build tracks its index's shapes: {shapes:?}");
	assert!(!shapes.may_hold(0, ValueShape::Array), "{shapes:?}");
	assert!(shapes.may_hold(0, ValueShape::Whole), "{shapes:?}");

	const LATEST: &str = "SELECT VALUE id FROM s ORDER BY time DESC LIMIT 2";
	let mut results = ds.execute(&format!("EXPLAIN {LATEST}"), &session, None).await?;
	let plan = format!("{:?}", results.remove(0).result?);
	assert!(plan.contains("IndexScan"), "an index with no array answers the order: {plan}");
	let expected = ["s:3", "s:2"];
	assert_eq!(ordered_result_ids(&ds, &session, LATEST).await?, expected);
	assert_eq!(
		ordered_result_ids(
			&ds,
			&session,
			"SELECT VALUE id FROM s WITH NOINDEX ORDER BY time DESC LIMIT 2"
		)
		.await?,
		expected
	);
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

/// A transaction that fails before its `COMMIT` rolls the blocking build back
/// with the definition, exactly as an explicit `CANCEL` does.
#[tokio::test(flavor = "multi_thread")]
async fn define_index_blocking_failed_commit_cleans_uncommitted_build_artifacts() -> Result<()> {
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

	let results = ds
		.execute(
			"BEGIN; DEFINE INDEX test ON user FIELDS email; THROW 'abandon'; COMMIT;",
			&session,
			None,
		)
		.await?;
	assert!(
		results.last().is_some_and(|result| result.result.is_err()),
		"the failed transaction does not commit"
	);
	let info = ds.execute("INFO FOR TABLE user", &session, None).await?.remove(0).result?;
	assert_eq!(info.into_json_value()["indexes"], serde_json::json!({}));

	execute_all(&ds, &session, "DEFINE INDEX test ON user FIELDS email").await?;
	let (ns, db, table, current_ix) = get_table_index(&ds, "user", "test").await?;
	let failed_ix = previous_index_id(current_ix.index_id);
	assert_no_index_build_artifacts(&ds, ns, db, &table, failed_ix).await?;
	Ok(())
}

/// A blocking `DEFINE INDEX` returns only once its index is queryable and
/// holds every record, whichever transaction it waited from.
#[tokio::test(flavor = "multi_thread")]
async fn define_index_blocking_returns_a_ready_populated_index() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(&ds, &session, "CREATE |user:40| SET email = rand::string(16) RETURN NONE").await?;
	for sql in [
		"DEFINE INDEX implicit ON user FIELDS email",
		"BEGIN; DEFINE INDEX explicit ON user FIELDS email; COMMIT;",
	] {
		execute_all(&ds, &session, sql).await?;
	}
	for index in ["implicit", "explicit"] {
		let building = index_building_json(&ds, &session, "user", index).await?;
		assert_eq!(building["status"], "ready", "{index}: {building}");
		assert_eq!(building["initial"], 40, "{index}: {building}");
		expect_indexed_query_len(
			&ds,
			&session,
			&format!("SELECT * FROM user WITH INDEX {index} WHERE email >= ''"),
			40,
		)
		.await?;
	}
	Ok(())
}

/// A blocking build that fails once its definition has committed takes the
/// definition away again, leaving what a build that failed inside its
/// transaction would: no index, and no build state beyond the index data
/// already queued for background reclaim.
#[tokio::test(flavor = "multi_thread")]
async fn define_index_blocking_build_failure_after_commit_removes_the_index() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:one SET email = 'same@example.com' RETURN NONE;
			CREATE user:two SET email = 'same@example.com' RETURN NONE;
			",
	)
	.await?;

	let mut results =
		ds.execute("DEFINE INDEX test ON user FIELDS email UNIQUE", &session, None).await?;
	let err = results.remove(0).result.expect_err("duplicates must fail a UNIQUE build");
	assert!(err.to_string().contains("test"), "the error names the index: {err}");
	let info = ds.execute("INFO FOR TABLE user", &session, None).await?.remove(0).result?;
	assert_eq!(info.into_json_value()["indexes"], serde_json::json!({}));
	// Record writes go on unobstructed: nothing is left to admit them to.
	execute_all(&ds, &session, "CREATE user:three SET email = 'same@example.com' RETURN NONE")
		.await?;

	execute_all(&ds, &session, "DEFINE INDEX test ON user FIELDS email").await?;
	let (ns, db, table, current_ix) = get_table_index(&ds, "user", "test").await?;
	let failed_ix = previous_index_id(current_ix.index_id);
	drain_reclaim_queue(&ds).await;
	assert_no_index_build_artifacts(&ds, ns, db, &table, failed_ix).await?;
	Ok(())
}

/// A blocking `DEFINE INDEX` whose commit applied but reported its outcome
/// unknown fails with that error, and what its wait owes is still done: the
/// build, left to run as the definition committed, fails on the duplicates,
/// and its definition is taken away again rather than left in `Error`.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn define_index_blocking_with_an_unknown_commit_outcome_still_removes_a_failed_build()
-> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE TABLE user SCHEMALESS;
		 CREATE user:one SET email = 'same@example.com' RETURN NONE;
		 CREATE user:two SET email = 'same@example.com' RETURN NONE;",
	)
	.await?;

	ds.commit_triggers().set_next_commit_applied_unknown(None);
	let mut results =
		ds.execute("DEFINE INDEX test ON user FIELDS email UNIQUE", &session, None).await?;
	let err = results.remove(0).result.expect_err("the commit's outcome is unknown");
	assert!(
		err.to_string().contains("injected once the commit applied"),
		"the statement reports the unknown outcome: {err}"
	);
	wait_for_published_indexes(&ds, &session, "user", serde_json::json!({})).await
}

/// Start `sql` on its own task, and wait until the build it starts is held at
/// its first batch. Dropping the returned guard lets the build go on.
#[cfg(feature = "kv-mem")]
async fn spawn_with_build_held(
	ds: &Arc<Datastore>,
	session: &Session,
	sql: &'static str,
) -> Result<(RetryableConflictGuard, tokio::task::JoinHandle<Result<()>>)> {
	let site = RetryableConflictSite::ConcurrentIndexInitialCleanup;
	let guard = inject_retryable_conflicts(site, ds.id(), REPEATED_RETRY_CONFLICTS);
	let task = tokio::spawn({
		let ds = Arc::clone(ds);
		let session = session.clone();
		async move { execute_all(&ds, &session, sql).await }
	});
	wait_for_retry_conflict(site, ds.id(), REPEATED_RETRY_CONFLICTS).await?;
	Ok((guard, task))
}

/// The indexes `INFO FOR TABLE` reports for `table` to a separate session.
#[cfg(feature = "kv-mem")]
async fn published_indexes(
	ds: &Datastore,
	session: &Session,
	table: &str,
) -> Result<serde_json::Value> {
	let sql = format!("INFO FOR TABLE {table}");
	let info = ds.execute(&sql, session, None).await?.remove(0).result?;
	Ok(info.into_json_value()["indexes"].clone())
}

/// Wait until the indexes `INFO FOR TABLE` reports for `table` are `expected`.
#[cfg(feature = "kv-mem")]
async fn wait_for_published_indexes(
	ds: &Datastore,
	session: &Session,
	table: &str,
	expected: serde_json::Value,
) -> Result<()> {
	timeout(Duration::from_secs(10), async {
		loop {
			if published_indexes(ds, session, table).await? == expected {
				return Ok(());
			}
			sleep(Duration::from_millis(10)).await;
		}
	})
	.await
	.map_err(|_| anyhow::anyhow!("timed out waiting for the indexes on {table} to be {expected}"))?
}

/// A blocking `DEFINE INDEX` run as a statement of its own commits its
/// definition before it waits for the build, so no transaction is held open
/// across the build's writes. It still returns only once the build is done.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn define_index_blocking_statement_commits_before_awaiting_its_build() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(&ds, &session, "CREATE |user:40| SET email = rand::string(16) RETURN NONE").await?;
	let (guard, define) =
		spawn_with_build_held(&ds, &session, "DEFINE INDEX test ON user FIELDS email").await?;
	// The build is held from before the statement commits, so the commit is
	// waited for rather than assumed.
	wait_for_published_indexes(
		&ds,
		&session,
		"user",
		serde_json::json!({ "test": "DEFINE INDEX test ON user FIELDS email" }),
	)
	.await?;
	assert_ne!(index_building_status(&ds, &session, "user", "test").await?, "ready");
	assert!(!define.is_finished(), "the statement waits for its build");
	drop(guard);
	define.await??;
	assert_eq!(index_building_status(&ds, &session, "user", "test").await?, "ready");
	Ok(())
}

/// A blocking `DEFINE INDEX` whose build is awaited after its commit builds
/// only once that commit has applied. Until then no writer can see the
/// definition, so a record written while the statement is still open is
/// neither indexed by its writer nor queued for the build; the build's scan,
/// which starts after the commit, is what indexes it.
///
/// The statement runs in a transaction this test holds open with the queue of
/// after-commit waits a statement run on its own is given, which takes the same
/// path from `StoreInstr::BuildIndex` and leaves the commit to the test.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn define_index_blocking_awaited_after_its_commit_indexes_a_write_made_before_it()
-> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(&ds, &session, "CREATE t:1 SET v = 'x'").await?;
	let (ns, db, table) = get_table_ids(&ds, "t").await?;

	let waits = surrealdb_datastore::close::AfterCommitWaits::default();
	let txn =
		Arc::new(ds.transaction(TransactionType::Write).await?.with_after_commit_waits(&waits));
	for result in ds
		.run(
			crate::kvs::QueryRequest::new("DEFINE INDEX ix ON t FIELDS v", &session)
				.with_transaction(Arc::clone(&txn)),
		)
		.await?
	{
		result.result?;
	}
	let ix = txn.expect_tb_index(ns, db, &table, "ix").await?;
	let building = local_builder_for_key(&ds, ns, db, &table, ix.index_id)
		.await?
		.expect("the statement starts a local build");
	// A build that ran ahead of the commit is done well within this; one that
	// waits for the commit is still waiting at the end of it.
	let _ = timeout(Duration::from_secs(2), wait_for_finished_builder(&building)).await;
	assert!(!building.is_finished(), "the build waits for its statement's commit");

	execute_all(&ds, &session, "CREATE t:2 SET v = 'x'").await?;

	let queued = waits.take().await;
	assert_eq!(queued.len(), 1, "the build is awaited after the commit");
	txn.commit().await?;
	for wait in queued {
		wait.wait(None).await?;
	}
	assert_eq!(index_building_status(&ds, &session, "t", "ix").await?, "ready");
	expect_indexed_query_len(&ds, &session, "SELECT id FROM t WITH INDEX ix WHERE v = 'x'", 2).await
}

/// Inside `BEGIN ... COMMIT` the build is awaited inside the transaction, so
/// the definition is published only with its finished build.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn define_index_blocking_in_a_transaction_awaits_before_committing() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(&ds, &session, "CREATE |user:40| SET email = rand::string(16) RETURN NONE").await?;
	let (guard, define) = spawn_with_build_held(
		&ds,
		&session,
		"BEGIN; DEFINE INDEX test ON user FIELDS email; COMMIT;",
	)
	.await?;
	assert_eq!(
		published_indexes(&ds, &session, "user").await?,
		serde_json::json!({}),
		"an uncommitted definition is not visible"
	);
	drop(guard);
	define.await??;
	assert_eq!(index_building_status(&ds, &session, "user", "test").await?, "ready");
	Ok(())
}

/// A datastore like [`new_index_test_ds`] with a query or transaction timeout.
#[cfg(feature = "kv-mem")]
async fn new_index_test_ds_with_timeouts(
	query: Option<Duration>,
	transaction: Option<Duration>,
) -> Result<(Arc<Datastore>, Session)> {
	let ds = Datastore::builder()
		.without_maintenance_tasks()
		.with_query_timeout(query)
		.with_transaction_timeout(transaction)
		.build_with_path("memory")
		.await?;
	let session = Session::owner().with_ns("test").with_db("test");
	let tx = ds.transaction(TransactionType::Write).await?;
	tx.ensure_ns_db(None, "test", "test").await?;
	tx.commit().await?;
	Ok((ds, session))
}

/// A blocking build awaited after its commit still answers to the query
/// timeout and the transaction timeout, as it did inside the transaction: a
/// build that outlasts either fails the statement and leaves no index.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn define_index_blocking_past_a_timeout_leaves_no_index() -> Result<()> {
	for (query, transaction, expected) in [
		(Some(Duration::from_millis(300)), None, "query was not executed"),
		(None, Some(Duration::from_millis(300)), "transaction was not completed"),
	] {
		let (ds, session) = new_index_test_ds_with_timeouts(query, transaction).await?;
		execute_all(&ds, &session, "CREATE |user:40| SET email = rand::string(16) RETURN NONE")
			.await?;
		let site = RetryableConflictSite::ConcurrentIndexInitialCleanup;
		let _guard = inject_retryable_conflicts(site, ds.id(), usize::MAX);
		// The build never finishes, so only the timeout ends the statement.
		let mut results = timeout(
			Duration::from_secs(10),
			ds.execute("DEFINE INDEX test ON user FIELDS email", &session, None),
		)
		.await
		.map_err(|_| anyhow::anyhow!("the statement outlived its timeout"))??;
		let err = results.remove(0).result.expect_err("the build outlasts the timeout");
		assert!(err.to_string().contains(expected), "unexpected error: {err}");
		assert_eq!(published_indexes(&ds, &session, "user").await?, serde_json::json!({}));
	}
	Ok(())
}

/// A `DEFINE INDEX` whose operands could write is awaited inside its
/// transaction, so those writes roll back with a build that fails. That holds
/// for a write the operand spells out and for one it only reaches through a
/// closure it hands to a builtin, whose body the statement's syntax does not
/// show.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn define_index_with_writing_operands_rolls_them_back_with_its_build() -> Result<()> {
	for comment in
		["{ DELETE audit; 'unique emails' }", "array::first(array::map([0], $delete_audit))"]
	{
		let (ds, session) = new_index_test_ds().await?;
		execute_all(
			&ds,
			&session,
			"CREATE user:one, user:two SET email = 'same@example.com' RETURN NONE;
			 CREATE audit:one RETURN NONE;",
		)
		.await?;
		let mut results = ds
			.execute(
				&format!(
					"LET $delete_audit = |$x| {{ DELETE audit; RETURN 'unique emails' }};
					 DEFINE INDEX test ON user FIELDS email UNIQUE COMMENT {comment};"
				),
				&session,
				None,
			)
			.await?;
		assert!(results.remove(1).result.is_err(), "the duplicates fail the build ({comment})");
		assert_eq!(published_indexes(&ds, &session, "user").await?, serde_json::json!({}));
		expect_indexed_query_len(&ds, &session, "SELECT * FROM audit", 1).await.map_err(|err| {
			anyhow::anyhow!("the operand's write must roll back ({comment}): {err}")
		})?;
	}
	Ok(())
}

/// `DEFINE INDEX OVERWRITE` waits for its build inside its transaction even as
/// a statement of its own, so readers keep the index it replaces until the
/// replacement is ready, and the swap is a single commit.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn define_index_overwrite_awaits_before_committing_the_swap() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"CREATE |user:40| SET email = rand::string(16), account = rand::string(8) RETURN NONE;
		 DEFINE INDEX test ON user FIELDS account;",
	)
	.await?;
	let (guard, overwrite) =
		spawn_with_build_held(&ds, &session, "DEFINE INDEX OVERWRITE test ON user FIELDS email")
			.await?;
	assert_eq!(
		published_indexes(&ds, &session, "user").await?,
		serde_json::json!({ "test": "DEFINE INDEX test ON user FIELDS account" }),
		"readers keep the replaced definition while its replacement builds"
	);
	assert_eq!(index_building_status(&ds, &session, "user", "test").await?, "ready");
	drop(guard);
	overwrite.await??;
	assert_eq!(
		published_indexes(&ds, &session, "user").await?,
		serde_json::json!({ "test": "DEFINE INDEX test ON user FIELDS email" })
	);
	assert_eq!(index_building_status(&ds, &session, "user", "test").await?, "ready");
	Ok(())
}

/// A `DEFINE INDEX` nested in a block waits for its build inside the block's
/// transaction, so what the block does next sees the finished index, and a
/// write that violates it fails the whole block rather than leaving the write
/// committed beside a failed build.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn define_index_blocking_in_a_block_awaits_before_the_block_goes_on() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(&ds, &session, "CREATE user:one SET email = 'one@example.com' RETURN NONE").await?;
	let (guard, block) = spawn_with_build_held(
		&ds,
		&session,
		"{
			DEFINE INDEX test ON user FIELDS email UNIQUE;
			CREATE user:two SET email = 'one@example.com' RETURN NONE;
		}",
	)
	.await?;
	drop(guard);
	assert!(block.await?.is_err(), "the duplicate fails the block");
	assert_eq!(published_indexes(&ds, &session, "user").await?, serde_json::json!({}));
	expect_indexed_query_len(&ds, &session, "SELECT * FROM user", 1).await?;
	Ok(())
}

/// `CONCURRENTLY` returns without waiting for its build.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn define_index_concurrently_returns_before_its_build_finishes() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(&ds, &session, "CREATE |user:40| SET email = rand::string(16) RETURN NONE").await?;
	let guard = start_index_build_paused(
		&ds,
		&session,
		"DEFINE INDEX test ON user FIELDS email CONCURRENTLY",
	)
	.await?;
	assert_ne!(index_building_status(&ds, &session, "user", "test").await?, "ready");
	drop(guard);
	wait_for_index_ready(&ds, &session, "user", "test").await?;
	Ok(())
}

/// Two vectors in `t`, for the vector-index builds below.
#[cfg(feature = "kv-mem")]
const TWO_VECTORS: &str = "CREATE t:1 SET vec = [1.0, 0.0]; CREATE t:2 SET vec = [0.0, 1.0];";

/// A nearest-neighbour search wide enough to return both of [`TWO_VECTORS`].
#[cfg(feature = "kv-mem")]
const BOTH_NEIGHBOURS: &str = "SELECT id FROM t WHERE vec <|2,40|> [1.0f, 0.0f]";

/// Run `statement`, a blocking build of the vector index `ix` on `t` whose
/// phase-2 pass fails through `site` once the build has published the index,
/// and assert that the statement reports the stopped wait and keeps its index.
///
/// The index is complete from the publish on, so it answers for every row. The
/// writes the failed pass did not fold stay queued, readers merge them, and
/// background compaction folds them.
#[cfg(feature = "kv-mem")]
async fn assert_a_pass_failure_after_online_keeps_the_index(
	setup: Option<&str>,
	statement: &str,
	site: NonRetryableErrorSite,
) -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	// The pass under test folds what a build's scan queues, which the DiskANN sink would not.
	let _sink_ds = super::kind::test_sink::off(ds.id());
	execute_all(&ds, &session, TWO_VECTORS).await?;
	if let Some(setup) = setup {
		execute_all(&ds, &session, setup).await?;
	}
	let guard = inject_non_retryable_error(site, ds.id());
	let err = execute_all(&ds, &session, statement)
		.await
		.expect_err("the stopped wait is the statement's result");
	assert!(
		err.to_string().starts_with("The index was committed and is ready"),
		"`{statement}`: {err}"
	);
	assert_eq!(
		crate::kvs::testing::non_retryable_error_count(site, ds.id()),
		0,
		"`{statement}`: the injected failure must have stopped the pass"
	);
	drop(guard);
	let building = index_building_json(&ds, &session, "t", "ix").await?;
	assert_eq!(building["status"], "ready", "`{statement}` left the index unusable: {building}");
	assert_eq!(
		building["compacting"], true,
		"`{statement}`: the failed pass left its writes to fold: {building}"
	);
	expect_indexed_query_len(&ds, &session, BOTH_NEIGHBOURS, 2).await?;
	Datastore::index_compaction(Arc::clone(&ds), Duration::from_secs(1), CancellationToken::new())
		.await?;
	let building = index_building_json(&ds, &session, "t", "ix").await?;
	assert_eq!(
		building["compacting"], false,
		"`{statement}`: background compaction folds what the pass left: {building}"
	);
	expect_indexed_query_len(&ds, &session, BOTH_NEIGHBOURS, 2).await
}

/// Every way a blocking statement can wait for a vector-index build of `kind`:
/// after its commit (`DEFINE`, `REBUILD`), and inside its transaction (`BEGIN
/// ... COMMIT`, `OVERWRITE`). None of them takes its index back over a pass
/// that fails once the index is online.
#[cfg(feature = "kv-mem")]
async fn assert_every_wait_survives_a_pass_failure_after_online(
	kind: &str,
	site: NonRetryableErrorSite,
) -> Result<()> {
	let define = format!("DEFINE INDEX ix ON t FIELDS vec {kind}");
	assert_a_pass_failure_after_online_keeps_the_index(None, &define, site).await?;
	assert_a_pass_failure_after_online_keeps_the_index(
		None,
		&format!("BEGIN; {define}; COMMIT;"),
		site,
	)
	.await?;
	assert_a_pass_failure_after_online_keeps_the_index(
		Some(&define),
		"REBUILD INDEX ix ON t",
		site,
	)
	.await?;
	// An overwrite that restates the index's definition keeps the index and builds nothing, so
	// the index it replaces measures distance another way.
	assert_a_pass_failure_after_online_keeps_the_index(
		Some(&define.replace("EUCLIDEAN", "COSINE")),
		&format!("DEFINE INDEX OVERWRITE ix ON t FIELDS vec {kind}"),
		site,
	)
	.await
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn define_index_hnsw_pass_failure_after_online_keeps_the_index() -> Result<()> {
	assert_every_wait_survives_a_pass_failure_after_online(
		"HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32",
		NonRetryableErrorSite::HnswCompaction,
	)
	.await
}

#[cfg(all(feature = "kv-mem", diskann))]
#[tokio::test(flavor = "multi_thread")]
async fn define_index_diskann_pass_failure_after_online_keeps_the_index() -> Result<()> {
	assert_every_wait_survives_a_pass_failure_after_online(
		"DISKANN DIMENSION 2 DIST EUCLIDEAN TYPE F32",
		NonRetryableErrorSite::DiskAnnCompaction,
	)
	.await
}

/// Run `statement`, a blocking build of the vector index `ix` on `t` waited for
/// after its commit, and let the statement's query timeout expire while its
/// phase-2 passes fold what the build queued. The statement succeeds: the index
/// was online by then, so it stays, and background compaction finishes the
/// fold.
#[cfg(feature = "kv-mem")]
async fn assert_a_timeout_once_online_keeps_the_index(
	setup: Option<&'static str>,
	statement: &'static str,
) -> Result<()> {
	let (ds, session) = new_index_test_ds_with_timeouts(Some(Duration::from_secs(2)), None).await?;
	execute_all(&ds, &session, TWO_VECTORS).await?;
	if let Some(setup) = setup {
		execute_all(&ds, &session, setup).await?;
	}
	let site = RetryableConflictSite::HnswCompaction;
	let guard = inject_retryable_conflicts(site, ds.id(), REPEATED_RETRY_CONFLICTS);
	let task = tokio::spawn({
		let ds = Arc::clone(&ds);
		let session = session.clone();
		async move { execute_all(&ds, &session, statement).await }
	});
	// A pass runs only once the index is online.
	wait_for_retry_conflict(site, ds.id(), REPEATED_RETRY_CONFLICTS).await?;
	task.await?.map_err(|err| anyhow::anyhow!("`{statement}` must succeed: {err}"))?;
	assert!(
		retryable_conflict_count(site, ds.id()) > 0,
		"`{statement}` must have returned while its passes were still retrying"
	);
	let indexes = published_indexes(&ds, &session, "t").await?;
	assert!(indexes.get("ix").is_some(), "the index stays defined: {indexes}");
	assert_eq!(index_building_status(&ds, &session, "t", "ix").await?, "ready");
	expect_indexed_query_len(&ds, &session, BOTH_NEIGHBOURS, 2).await?;
	drop(guard);
	Datastore::index_compaction(Arc::clone(&ds), Duration::from_secs(1), CancellationToken::new())
		.await?;
	wait_for_index_drained(&ds, &session, "ix").await?;
	expect_indexed_query_len(&ds, &session, BOTH_NEIGHBOURS, 2).await
}

/// Both statements that wait after their commit: a `DEFINE`, whose failure
/// would remove the definition, and a `REBUILD`, whose would not, so only the
/// wait itself can report it as the success it is.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn define_index_blocking_timing_out_once_its_index_is_online_keeps_it() -> Result<()> {
	const DEFINE: &str = "DEFINE INDEX ix ON t FIELDS vec HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32";
	assert_a_timeout_once_online_keeps_the_index(None, DEFINE).await?;
	assert_a_timeout_once_online_keeps_the_index(Some(DEFINE), "REBUILD INDEX ix ON t").await
}

/// Run `statement`, a blocking build of the vector index `ix` on `t` whose
/// publish commit reports an error after it applied, and assert that the
/// statement succeeds with the index online. The builder reads the durable
/// state back and carries on as published, so it releases its ownership as any
/// build that published does, and the statement's phase 2 folds what it
/// queued.
#[cfg(feature = "kv-mem")]
async fn assert_a_publish_applied_despite_an_error_keeps_the_index(
	setup: Option<&str>,
	statement: &str,
) -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(&ds, &session, TWO_VECTORS).await?;
	if let Some(setup) = setup {
		execute_all(&ds, &session, setup).await?;
	}
	let site = NonRetryableErrorSite::ConcurrentIndexOnlineCommitAppliedUnknown;
	let guard = inject_non_retryable_error(site, ds.id());
	execute_all(&ds, &session, statement).await.map_err(|err| {
		anyhow::anyhow!("`{statement}` published its index and must succeed: {err}")
	})?;
	drop(guard);
	let (ns, db, table, ix) = get_table_index(&ds, "t", "ix").await?;
	let state = durable_build_state(&ds, &IndexKeyBase::new(ns, db, table, ix.index_id)).await?;
	assert_eq!(state.phase, IndexBuildPhase::Online, "`{statement}`: {state:?}");
	assert_eq!(state.owner, None, "`{statement}` releases its ownership: {state:?}");
	let building = index_building_json(&ds, &session, "t", "ix").await?;
	assert_eq!(building["compacting"], false, "`{statement}` folded what it queued: {building}");
	expect_indexed_query_len(&ds, &session, BOTH_NEIGHBOURS, 2).await
}

/// A publish whose commit reports an error after applying is a publish, for
/// every way a blocking statement waits: after its commit with a definition to
/// take back (`DEFINE`) or without one (`REBUILD`), and inside its
/// transaction.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn define_index_blocking_whose_publish_applied_despite_an_error_keeps_the_index() -> Result<()>
{
	const DEFINE: &str = "DEFINE INDEX ix ON t FIELDS vec HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32";
	assert_a_publish_applied_despite_an_error_keeps_the_index(None, DEFINE).await?;
	assert_a_publish_applied_despite_an_error_keeps_the_index(
		Some(DEFINE),
		"REBUILD INDEX ix ON t",
	)
	.await?;
	assert_a_publish_applied_despite_an_error_keeps_the_index(
		None,
		&format!("BEGIN; {DEFINE}; COMMIT;"),
	)
	.await
}

/// A blocking `DEFINE INDEX` whose caller stops waiting once the definition
/// has committed still has the definition of a failed build removed: the
/// removal does not depend on anyone still awaiting the statement.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn define_index_blocking_abandoned_after_its_commit_still_removes_a_failed_build()
-> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"CREATE user:one, user:two SET email = 'same@example.com' RETURN NONE",
	)
	.await?;
	let (guard, define) =
		spawn_with_build_held(&ds, &session, "DEFINE INDEX test ON user FIELDS email UNIQUE")
			.await?;
	wait_for_published_indexes(
		&ds,
		&session,
		"user",
		serde_json::json!({ "test": "DEFINE INDEX test ON user FIELDS email UNIQUE" }),
	)
	.await?;
	define.abort();
	assert!(define.await.is_err_and(|err| err.is_cancelled()), "the caller stopped waiting");
	// The build goes on without its caller, and fails on the duplicate.
	drop(guard);
	wait_for_published_indexes(&ds, &session, "user", serde_json::json!({})).await
}

/// A blocking `DEFINE INDEX` whose index another statement removes while it
/// builds does not report success: its index never came online.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn define_index_blocking_whose_index_is_removed_during_the_build_fails() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(&ds, &session, "CREATE |user:40| SET email = rand::string(16) RETURN NONE").await?;
	let (guard, define) =
		spawn_with_build_held(&ds, &session, "DEFINE INDEX test ON user FIELDS email").await?;
	wait_for_published_indexes(
		&ds,
		&session,
		"user",
		serde_json::json!({ "test": "DEFINE INDEX test ON user FIELDS email" }),
	)
	.await?;
	execute_all_retrying_conflicts(&ds, &session, "REMOVE INDEX test ON user").await?;
	drop(guard);
	let err = define.await?.expect_err("the index never came online");
	assert!(err.to_string().contains("removed or replaced"), "unexpected error: {err}");
	assert_eq!(published_indexes(&ds, &session, "user").await?, serde_json::json!({}));
	Ok(())
}

/// A blocking `DEFINE INDEX` whose table another statement removes while it
/// builds after its commit fails, and leaves neither the table, the index nor
/// its build state behind.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn define_index_blocking_whose_table_is_removed_during_the_build_fails() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(&ds, &session, "CREATE |user:40| SET email = rand::string(16) RETURN NONE").await?;
	let (guard, define) =
		spawn_with_build_held(&ds, &session, "DEFINE INDEX test ON user FIELDS email").await?;
	wait_for_published_indexes(
		&ds,
		&session,
		"user",
		serde_json::json!({ "test": "DEFINE INDEX test ON user FIELDS email" }),
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "user", "test").await?;
	execute_all_retrying_conflicts(&ds, &session, "REMOVE TABLE user").await?;
	drop(guard);
	let err = define.await?.expect_err("the index never came online");
	assert!(err.to_string().contains("removed or replaced"), "unexpected error: {err}");
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	assert!(!durable_build_state_exists(&ds, &ikb).await?, "the build state was left behind");
	let tx = ds.transaction(TransactionType::Read).await?;
	let left = catch!(tx, tx.get_tb(ns, db, &table, None).await);
	tx.cancel().await?;
	assert!(left.is_none(), "the removed table is defined again: {left:?}");
	Ok(())
}

/// The removal of a failed build's definition finds nothing to undo once the
/// table the definition was on is gone: the index went with it. It neither
/// fails nor stages anything.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn removing_a_failed_definition_of_a_removed_table_has_nothing_to_undo() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"CREATE user:1 SET email = 'a'; DEFINE INDEX test ON user FIELDS email;",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "user", "test").await?;
	let tx = ds.transaction(TransactionType::Read).await?;
	let table_def = catch!(tx, tx.expect_tb(ns, db, &table).await);
	tx.cancel().await?;
	let generation =
		durable_build_state(&ds, &IndexKeyBase::new(ns, db, table, ix.index_id)).await?.generation;
	execute_all(&ds, &session, "REMOVE TABLE user").await?;
	let ctx = ds.setup_ctx()?.freeze();
	let undo = super::after_commit::RemoveFailedDefinition::new(
		ctx,
		("test".to_owned(), "test".to_owned()),
		&table_def,
		&ix,
	);
	assert_eq!(undo.run(generation).await?, super::after_commit::Undone::Gone);
	Ok(())
}

/// Hold the report the builder of the blocking statement on `ds` owes that
/// statement, once the builder has stopped, until the returned guard is
/// dropped. The builder ends its generation first, so the statement hears the
/// outcome only once the test has arranged what the durable state holds by
/// then.
#[cfg(feature = "kv-mem")]
fn hold_build_report(ds: &Datastore) -> RetryableConflictGuard {
	let site = RetryableConflictSite::ConcurrentIndexGenerationEnd;
	inject_retryable_conflicts(site, ds.id(), REPEATED_RETRY_CONFLICTS)
}

/// Wait until the builder on `ds` has stopped and reached the report
/// [`hold_build_report`] holds.
#[cfg(feature = "kv-mem")]
async fn wait_for_held_build_report(ds: &Datastore) -> Result<()> {
	let site = RetryableConflictSite::ConcurrentIndexGenerationEnd;
	wait_for_retry_conflict(site, ds.id(), REPEATED_RETRY_CONFLICTS).await
}

/// Wait until the durable build state `ikb` names is `Online`.
#[cfg(feature = "kv-mem")]
async fn wait_for_durable_online(ds: &Datastore, ikb: &IndexKeyBase) -> Result<()> {
	timeout(Duration::from_secs(30), async {
		while durable_build_state(ds, ikb).await?.phase != IndexBuildPhase::Online {
			sleep(Duration::from_millis(20)).await;
		}
		anyhow::Ok(())
	})
	.await
	.map_err(|_| anyhow::anyhow!("the build did not publish the index"))?
}

/// A blocking statement on node A of two whose build node B has taken over
/// and published, and whose own builder has stopped on finding its ownership
/// gone, with the report that builder owes the statement still held.
#[cfg(feature = "kv-mem")]
struct BuildTakenOverByNodeB {
	ds_a: Arc<Datastore>,
	ds_b: Datastore,
	session: Session,
	/// The build state of the index `test` on `user`.
	ikb: IndexKeyBase,
	/// The statement, still waiting for its build.
	statement: tokio::task::JoinHandle<Result<()>>,
	/// Dropping this lets node A's builder report to the statement.
	report: RetryableConflictGuard,
}

/// Run `statement`, a blocking build of the b-tree index `test` on `user`
/// waited for after its commit, on node A, and hold its builder until its
/// lease lapses. Node B then takes the build over and publishes it, and A's
/// builder stops on finding its ownership gone, holding its report to the
/// statement (see [`BuildTakenOverByNodeB`]).
#[cfg(feature = "kv-mem")]
async fn take_a_held_build_over_on_node_b(
	setup: Option<&str>,
	statement: &'static str,
) -> Result<BuildTakenOverByNodeB> {
	let (ds_a, ds_b, session) = new_distributed_index_test_ds().await?;
	execute_all(&ds_a, &session, "CREATE |user:40| SET email = rand::string(16) RETURN NONE")
		.await?;
	if let Some(setup) = setup {
		execute_all(&ds_a, &session, setup).await?;
	}
	let (held, task) = spawn_with_build_held(&ds_a, &session, statement).await?;
	wait_for_published_indexes(
		&ds_a,
		&session,
		"user",
		serde_json::json!({ "test": "DEFINE INDEX test ON user FIELDS email" }),
	)
	.await?;
	let report = hold_build_report(&ds_a);
	// Node A's builder is held, so its lease can lapse and node B take over.
	let (ns, db, table, ix) = get_table_index(&ds_a, "user", "test").await?;
	let ikb = IndexKeyBase::new(ns, db, table, ix.index_id);
	let mut state = durable_build_state(&ds_a, &ikb).await?;
	assert_ne!(state.phase, IndexBuildPhase::Online, "`{statement}`: node A's build is held");
	let expired = Utc::now() - chrono::Duration::seconds(BUILD_OWNER_LEASE_SECS + 5);
	state.updated_at = expired;
	state.owner_heartbeat_at = Some(expired);
	set_durable_build_state(&ds_a, &ikb, state).await?;
	let adopted =
		ds_b.resume_stalled_index_builds(Duration::from_secs(30), CancellationToken::new()).await?;
	assert_eq!(adopted.items, 1, "`{statement}`: node B takes the lapsed build over");
	wait_for_durable_online(&ds_b, &ikb)
		.await
		.map_err(|err| anyhow::anyhow!("`{statement}`: node B's takeover: {err}"))?;
	// Node A's builder, still held, finds its ownership gone and stops.
	drop(held);
	wait_for_held_build_report(&ds_a).await?;
	Ok(BuildTakenOverByNodeB {
		ds_a,
		ds_b,
		session,
		ikb,
		statement: task,
		report,
	})
}

/// Run `statement` through [`take_a_held_build_over_on_node_b`], then let
/// node A's builder report the failure it saw. The statement succeeds with
/// the index online: what decides its outcome is the durable state, not what
/// its own builder saw.
#[cfg(feature = "kv-mem")]
async fn assert_a_build_another_node_publishes_keeps_its_index(
	setup: Option<&str>,
	statement: &'static str,
) -> Result<()> {
	let build = take_a_held_build_over_on_node_b(setup, statement).await?;
	drop(build.report);
	build
		.statement
		.await?
		.map_err(|err| anyhow::anyhow!("`{statement}`: the statement's index is online: {err}"))?;
	assert_eq!(index_building_status(&build.ds_a, &build.session, "user", "test").await?, "ready");
	expect_indexed_query_len(
		&build.ds_a,
		&build.session,
		"SELECT * FROM user WITH INDEX test WHERE email >= ''",
		40,
	)
	.await
}

/// A blocking statement whose build another node takes over and publishes
/// succeeds, although its own builder then fails with its ownership lost: a
/// `DEFINE`, whose removal decides on the durable state before it takes the
/// definition back, and a `REBUILD`, which has nothing to take back and reads
/// the durable state on its own.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn define_index_blocking_whose_build_another_node_publishes_keeps_it() -> Result<()> {
	const DEFINE: &str = "DEFINE INDEX test ON user FIELDS email";
	assert_a_build_another_node_publishes_keeps_its_index(None, DEFINE).await?;
	assert_a_build_another_node_publishes_keeps_its_index(
		Some(DEFINE),
		"REBUILD INDEX test ON user",
	)
	.await
}

/// A blocking `DEFINE INDEX` whose build another node took over and
/// published, and which a `REBUILD INDEX` has taken on since, leaves the index
/// to that rebuild when its own builder reports the failure it saw. The
/// statement fails, as its own build did, but its removal is fenced to the
/// build generation the statement awaited: it takes back neither the
/// definition nor the build state of the newer generation.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn define_index_blocking_leaves_an_index_a_later_rebuild_took_on() -> Result<()> {
	const DEFINE: &str = "DEFINE INDEX test ON user FIELDS email";
	let build = take_a_held_build_over_on_node_b(None, DEFINE).await?;
	let published = durable_build_state(&build.ds_b, &build.ikb).await?.generation;
	// Node B's takeover publishes before its task ends, and a build of the
	// index that node starts meanwhile is refused as one already running.
	let ikb = &build.ikb;
	let takeover = local_builder_for_key(&build.ds_b, ikb.ns(), ikb.db(), ikb.table(), ikb.index())
		.await?
		.expect("node B's takeover registers a local builder");
	wait_for_finished_builder(&takeover).await?;
	// Node B rebuilds the index, held at its first batch.
	let rebuild = start_index_build_paused(
		&build.ds_b,
		&build.session,
		"REBUILD INDEX test ON user CONCURRENTLY",
	)
	.await?;
	let state = durable_build_state(&build.ds_b, &build.ikb).await?;
	assert_eq!(state.generation, published + 1, "the rebuild builds a generation of its own");
	drop(build.report);
	build.statement.await?.expect_err("the statement's own builder lost its build");
	assert_eq!(
		published_indexes(&build.ds_a, &build.session, "user").await?,
		serde_json::json!({ "test": DEFINE }),
		"the definition stays with the rebuild that took it on"
	);
	let state = durable_build_state(&build.ds_b, &build.ikb).await?;
	assert_eq!(
		(state.generation, state.phase),
		(published + 1, IndexBuildPhase::Building),
		"the rebuild's build is left to it: {state:?}"
	);
	drop(rebuild);
	wait_for_index_ready(&build.ds_b, &build.session, "user", "test").await?;
	expect_indexed_query_len(
		&build.ds_b,
		&build.session,
		"SELECT * FROM user WITH INDEX test WHERE email >= ''",
		40,
	)
	.await
}

/// A blocking `REBUILD INDEX` whose build another node took over and failed,
/// and which a later `REBUILD INDEX` has since published, reports the failure
/// of the build it awaited: a publish counts for the statement only if it is
/// of the generation the statement awaited.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn rebuild_index_blocking_does_not_take_a_later_generation_for_its_own() -> Result<()> {
	const DEFINE: &str = "DEFINE INDEX test ON user FIELDS email";
	let (ds_a, ds_b, session) = new_distributed_index_test_ds().await?;
	execute_all(
		&ds_a,
		&session,
		&format!("CREATE |user:40| SET email = rand::string(16) RETURN NONE; {DEFINE};"),
	)
	.await?;
	let (held, rebuild) =
		spawn_with_build_held(&ds_a, &session, "REBUILD INDEX test ON user").await?;
	let report = hold_build_report(&ds_a);
	// Node A's builder is held, so its lease can lapse, and node B takes the
	// build over and fails it.
	let (ns, db, table, ix) = get_table_index(&ds_a, "user", "test").await?;
	let ikb = IndexKeyBase::new(ns, db, table, ix.index_id);
	let mut state = durable_build_state(&ds_a, &ikb).await?;
	let awaited = state.generation;
	let expired = Utc::now() - chrono::Duration::seconds(BUILD_OWNER_LEASE_SECS + 5);
	state.updated_at = expired;
	state.owner_heartbeat_at = Some(expired);
	set_durable_build_state(&ds_a, &ikb, state).await?;
	{
		let site = NonRetryableErrorSite::ConcurrentIndexInitialBatchCommit;
		let _failing = inject_non_retryable_error(site, ds_b.id());
		let adopted = ds_b
			.resume_stalled_index_builds(Duration::from_secs(30), CancellationToken::new())
			.await?;
		assert_eq!(adopted.items, 1, "node B takes the lapsed build over");
		timeout(Duration::from_secs(10), async {
			while durable_build_state(&ds_b, &ikb).await?.phase != IndexBuildPhase::Error {
				sleep(Duration::from_millis(20)).await;
			}
			anyhow::Ok(())
		})
		.await
		.map_err(|_| anyhow::anyhow!("node B's takeover did not fail the build"))??;
	}
	drop(held);
	wait_for_held_build_report(&ds_a).await?;
	// A later rebuild publishes the next generation.
	execute_all(&ds_b, &session, "REBUILD INDEX test ON user").await?;
	let state = durable_build_state(&ds_b, &ikb).await?;
	assert_eq!(
		(state.generation, state.phase),
		(awaited + 1, IndexBuildPhase::Online),
		"the later rebuild published a generation of its own: {state:?}"
	);
	drop(report);
	rebuild.await?.expect_err("the build the statement awaited failed");
	assert_eq!(index_building_status(&ds_a, &session, "user", "test").await?, "ready");
	Ok(())
}

/// A blocking `REBUILD INDEX` whose table another node removes and defines
/// again while it builds reports its build's failure, although by then an
/// unrelated index on the new table holds the same build-state key and has
/// published the generation the statement awaited: that key names a table
/// only by its name, and index ids start afresh on a new table, so a publish
/// counts for the statement only on the table it was started on.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn rebuild_index_blocking_does_not_take_an_index_on_a_new_table_for_its_own() -> Result<()> {
	const DATA: &str = "CREATE |user:40| SET email = rand::string(16) RETURN NONE;";
	const DEFINE: &str = "DEFINE INDEX test ON user FIELDS email";
	let (ds_a, ds_b, session) = new_distributed_index_test_ds().await?;
	execute_all(&ds_a, &session, &format!("{DATA} {DEFINE};")).await?;
	let (ns, db, table, ix) = get_table_index(&ds_a, "user", "test").await?;
	let removed_table = current_table_id(&ds_a, ns, db, &table).await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	let (held, rebuild) =
		spawn_with_build_held(&ds_a, &session, "REBUILD INDEX test ON user").await?;
	let awaited = durable_build_state(&ds_a, &ikb).await?.generation;
	let report = hold_build_report(&ds_a);
	// Node A's builder, finding its build state gone with the table, stops.
	execute_all_retrying_conflicts(&ds_b, &session, "REMOVE TABLE user").await?;
	drop(held);
	wait_for_held_build_report(&ds_a).await?;
	// The table is defined again, and its first index, built and rebuilt as
	// often as the removed one was, publishes the generation the statement
	// awaited under the same key.
	execute_all(&ds_b, &session, &format!("{DATA} {DEFINE}; REBUILD INDEX test ON user;")).await?;
	let (_, _, _, reused) = get_table_index(&ds_b, "user", "test").await?;
	assert_eq!(reused.index_id, ix.index_id, "index ids start afresh on the new table");
	assert_ne!(
		current_table_id(&ds_b, ns, db, &table).await?,
		removed_table,
		"the new table is a table of its own"
	);
	let state = durable_build_state(&ds_b, &ikb).await?;
	assert_eq!((state.generation, state.phase), (awaited, IndexBuildPhase::Online), "{state:?}");
	drop(report);
	rebuild.await?.expect_err("the statement's index went with its table");
	assert_eq!(index_building_status(&ds_b, &session, "user", "test").await?, "ready");
	Ok(())
}

/// A standalone blocking `DEFINE INDEX` on a table the statement creates waits
/// for its build inside its transaction: a build that fails takes the table
/// back with the definition, and one that succeeds commits both.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn define_index_blocking_on_a_table_it_creates_rolls_both_back() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	{
		let site = NonRetryableErrorSite::ConcurrentIndexCountTailCommitted;
		let _guard = inject_non_retryable_error(site, ds.id());
		expect_statement_error(&ds, &session, "DEFINE INDEX ix ON fresh COUNT").await?;
	}
	let info = ds.execute("INFO FOR DB", &session, None).await?.remove(0).result?;
	assert_eq!(
		info.into_json_value()["tables"],
		serde_json::json!({}),
		"a failed define must not leave behind the table it created"
	);
	execute_all(&ds, &session, "DEFINE INDEX ix ON fresh COUNT").await?;
	assert_eq!(index_building_status(&ds, &session, "fresh", "ix").await?, "ready");
	Ok(())
}

/// What a `tracing` subscriber writes, kept for a test to read back.
#[cfg(feature = "kv-mem")]
#[derive(Clone, Default)]
struct CapturedLog(Arc<std::sync::Mutex<Vec<u8>>>);

#[cfg(feature = "kv-mem")]
impl CapturedLog {
	fn contents(&self) -> String {
		String::from_utf8_lossy(&self.0.lock().expect("the log lock is never poisoned"))
			.into_owned()
	}
}

#[cfg(feature = "kv-mem")]
impl std::io::Write for CapturedLog {
	fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
		self.0.lock().expect("the log lock is never poisoned").extend_from_slice(buf);
		Ok(buf.len())
	}

	fn flush(&mut self) -> std::io::Result<()> {
		Ok(())
	}
}

#[cfg(feature = "kv-mem")]
impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedLog {
	type Writer = CapturedLog;

	fn make_writer(&'a self) -> Self::Writer {
		self.clone()
	}
}

/// A blocking `DEFINE INDEX` that waits for its build after its commit is
/// slow-logged once that wait is over: its caller waits for the build too, so
/// the build's time is the statement's.
///
/// The statement's own work takes milliseconds; only its build, held here for
/// longer than the threshold, can take it over.
///
/// On a current-thread runtime, so the subscriber this test installs for its
/// own thread sees the statement wherever the runtime polls it.
#[cfg(feature = "kv-mem")]
#[tokio::test]
async fn define_index_blocking_is_slow_logged_with_its_build() -> Result<()> {
	const SLOW: Duration = Duration::from_millis(500);
	// Each injected conflict holds the build for its retry pause, which for a build a statement
	// awaits doubles from at least 1ms to at least 50ms: twenty in a row hold it for over 700ms.
	const HELD_BY_CONFLICTS: usize = 20;
	let log = CapturedLog::default();
	let subscriber = tracing_subscriber::fmt()
		.with_writer(log.clone())
		.with_max_level(tracing::Level::WARN)
		.with_ansi(false)
		.finish();
	let _default = tracing::subscriber::set_default(subscriber);

	let ds = Datastore::builder()
		.without_maintenance_tasks()
		.with_slow_log(SLOW, Vec::new(), Vec::new())
		.build_with_path("memory")
		.await?;
	let session = Session::owner().with_ns("test").with_db("test");
	let tx = ds.transaction(TransactionType::Write).await?;
	tx.ensure_ns_db(None, "test", "test").await?;
	tx.commit().await?;
	execute_all(&ds, &session, "CREATE |user:40| SET email = rand::string(16) RETURN NONE").await?;
	let site = RetryableConflictSite::ConcurrentIndexInitialCleanup;
	let _guard = inject_retryable_conflicts(site, ds.id(), HELD_BY_CONFLICTS);
	execute_all(&ds, &session, "DEFINE INDEX test ON user FIELDS email").await?;
	assert_eq!(retryable_conflict_count(site, ds.id()), 0, "the build was held as intended");

	let log = log.contents();
	assert!(
		log.lines().any(|line| {
			line.contains("Slow query detected") && line.contains("DEFINE INDEX test ON user")
		}),
		"the wait for the build must count toward the statement's slow-log time: {log}"
	);
	Ok(())
}

/// A blocking `DEFINE INDEX` that builds inside its own statement's
/// transaction, as one nested in an `IF` does, is slow-logged once its
/// materialise wait after the commit is over: its caller waits for that too.
///
/// The build and the statement take milliseconds; only the wait, its passes
/// held here for longer than the threshold, can take it over.
///
/// On a current-thread runtime, as
/// [`define_index_blocking_is_slow_logged_with_its_build`] is.
#[cfg(feature = "kv-mem")]
#[tokio::test]
async fn a_statement_building_in_its_transaction_is_slow_logged_with_its_materialise_wait()
-> Result<()> {
	const SLOW: Duration = Duration::from_millis(500);
	// Each injected conflict holds a pass for its retry pause of 100ms.
	const HELD_BY_CONFLICTS: usize = 8;
	let log = CapturedLog::default();
	let subscriber = tracing_subscriber::fmt()
		.with_writer(log.clone())
		.with_max_level(tracing::Level::WARN)
		.with_ansi(false)
		.finish();
	let _default = tracing::subscriber::set_default(subscriber);

	let ds = Datastore::builder()
		.without_maintenance_tasks()
		.with_slow_log(SLOW, Vec::new(), Vec::new())
		.build_with_path("memory")
		.await?;
	let session = Session::owner().with_ns("test").with_db("test");
	let tx = ds.transaction(TransactionType::Write).await?;
	tx.ensure_ns_db(None, "test", "test").await?;
	tx.commit().await?;
	execute_all(&ds, &session, "CREATE |user:40| SET email = rand::string(16) RETURN NONE").await?;
	let site = RetryableConflictSite::CountCompaction;
	let _guard = inject_retryable_conflicts(site, ds.id(), HELD_BY_CONFLICTS);
	execute_all(&ds, &session, "IF true { DEFINE INDEX cx ON user COUNT }").await?;
	assert_eq!(retryable_conflict_count(site, ds.id()), 0, "the wait was held as intended");

	let log = log.contents();
	assert!(
		log.lines().any(|line| {
			line.contains("Slow query detected") && line.contains("DEFINE INDEX cx ON user")
		}),
		"the materialise wait must count toward the statement's slow-log time: {log}"
	);
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

/// An index read as settled online is served to the transactions of its catalog and build epochs
/// without a read. Every other state is read from the store each time, never kept, and a build
/// state commit moves the build epoch, so an index that goes `Online` -> `Building` mid-epoch is
/// not served as online and one that goes back `Online` is used by the next plan.
#[tokio::test(flavor = "multi_thread")]
async fn filter_online_indexes_serves_only_settled_online_indexes_of_its_epochs() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:one SET email = 'one@example.com' RETURN NONE;
			DEFINE INDEX one ON user FIELDS email;
			",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "user", "one").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	let indexes: Arc<[IndexDefinition]> = Arc::from(vec![ix.as_ref().clone()]);
	let online = durable_build_state_for_phase(IndexBuildPhase::Online, 1, None);
	set_durable_build_state(&ds, &ikb, online.clone()).await?;

	// One read settles it; the next transaction of the epochs reads nothing.
	let first = ds.transaction(TransactionType::Read).await?;
	assert_eq!(filter_online_indexes(&first, ns, db, Arc::clone(&indexes)).await?.len(), 1);
	assert_eq!(first.metrics_snapshot_for_test().ops_get, 1);
	assert!(first.index_known_online(ns, db, &table, ix.index_id));
	let same = ds.transaction(TransactionType::Read).await?;
	assert_eq!(filter_online_indexes(&same, ns, db, Arc::clone(&indexes)).await?.len(), 1);
	assert_eq!(same.metrics_snapshot_for_test().ops_get, 0, "served from the epochs' entry");
	let settled = same.build_epoch();
	assert!(settled.is_some());

	// A builder commits `Building` after those reads cached the online set.
	let owner = Some(ds.id());
	let building = durable_build_state_for_phase(IndexBuildPhase::Building, 2, owner);
	set_durable_build_state(&ds, &ikb, building).await?;
	assert!(!same.index_known_online(ns, db, &table, ix.index_id), "open across the commit");
	let later = ds.transaction(TransactionType::Read).await?;
	assert_ne!(later.build_epoch(), settled, "a phase change moves the build epoch");
	assert!(filter_online_indexes(&later, ns, db, Arc::clone(&indexes)).await?.is_empty());
	assert!(!later.index_known_online(ns, db, &table, ix.index_id));
	later.cancel().await?;

	// No other state is ever kept, however often it is read.
	for (phase, owner) in [
		(IndexBuildPhase::Closing, Some(ds.id())),
		(IndexBuildPhase::Error, None),
		(IndexBuildPhase::Online, Some(ds.id())),
	] {
		let state = durable_build_state_for_phase(phase, 3, owner);
		set_durable_build_state(&ds, &ikb, state).await?;
		for _ in 0..2 {
			let tx = ds.transaction(TransactionType::Read).await?;
			let kept = filter_online_indexes(&tx, ns, db, Arc::clone(&indexes)).await?;
			assert_eq!(kept.len(), usize::from(phase == IndexBuildPhase::Online), "{phase:?}");
			assert!(!tx.index_known_online(ns, db, &table, ix.index_id), "{phase:?} was kept");
			assert_eq!(tx.metrics_snapshot_for_test().ops_get, 1, "{phase:?} is read each time");
			tx.cancel().await?;
		}
	}

	// The build completes: the epoch moves again and the next plan uses the index.
	let before = ds.transaction(TransactionType::Read).await?;
	let before_epoch = before.build_epoch();
	set_durable_build_state(
		&ds,
		&ikb,
		durable_build_state_for_phase(IndexBuildPhase::Online, 3, None),
	)
	.await?;
	assert!(!before.index_known_online(ns, db, &table, ix.index_id));
	let after = ds.transaction(TransactionType::Read).await?;
	assert_ne!(after.build_epoch(), before_epoch, "going online moves the build epoch");
	assert_eq!(filter_online_indexes(&after, ns, db, Arc::clone(&indexes)).await?.len(), 1);
	assert!(after.index_known_online(ns, db, &table, ix.index_id));
	for tx in [first, same, before, after] {
		tx.cancel().await?;
	}
	Ok(())
}

/// Decide the write of one row of `table` to `ix` in `tx`, as a document write does.
async fn consume_one(
	ds: &Datastore,
	tx: &Arc<crate::kvs::Transaction>,
	table: &TableName,
	ix: &IndexDefinition,
) -> Result<ConsumeResult> {
	let db_def = tx.get_db_by_name("test", "test", None).await?.expect("database should exist");
	let mut ctx = ds.setup_ctx()?;
	ctx.set_transaction(Arc::clone(tx));
	let ctx = ctx.freeze();
	let rid = RecordId {
		table: table.clone(),
		key: RecordIdKey::from("one".to_owned()),
	};
	ctx.get_index_builder()
		.expect("index builder should be present")
		.consume(
			db_def.as_ref(),
			&ctx,
			ix,
			IndexMutation {
				old_values: None,
				new_values: None,
				rid: &rid,
				count_cond_match: None,
			},
		)
		.await
}

/// The admission of a write to an online index opens a transaction of its own only when the user
/// transaction cannot name the build state it would read: the first decision of an epoch settles
/// it, a later user transaction of the epoch opens none, and one that was open across a build
/// state commit, or opened after one that left the index building, decides for real.
#[tokio::test(flavor = "multi_thread")]
async fn online_index_admission_is_served_only_within_its_epochs() -> Result<()> {
	let observer = Arc::new(CompletedTransactions::default());
	let ds = Datastore::builder()
		.with_observer(Arc::clone(&observer) as Arc<dyn crate::observe::ExecutionObserver>)
		.without_maintenance_tasks()
		.build_with_path("memory")
		.await?;
	let session = Session::owner().with_ns("test").with_db("test");
	let tx = ds.transaction(TransactionType::Write).await?;
	tx.ensure_ns_db(None, "test", "test").await?;
	tx.commit().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE TABLE user SCHEMALESS; DEFINE INDEX test ON user FIELDS email;",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "user", "test").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);

	let decide = async |tx: &Arc<crate::kvs::Transaction>| -> Result<(u64, ConsumeResult)> {
		let before = observer.count();
		let result = consume_one(&ds, tx, &table, &ix).await?;
		Ok((observer.count() - before, result))
	};

	let tx = Arc::new(ds.transaction(TransactionType::Write).await?);
	let (opened, result) = decide(&tx).await?;
	assert!(opened >= 1 && matches!(result, ConsumeResult::Ignored(..)), "the first decides");
	tx.cancel().await?;

	let tx = Arc::new(ds.transaction(TransactionType::Write).await?);
	let (opened, result) = decide(&tx).await?;
	assert_eq!(opened, 0, "a later transaction of the epochs opens none");
	assert!(matches!(result, ConsumeResult::Ignored(..)));
	tx.cancel().await?;

	// A builder commit lands after the user transaction opened.
	let straddling = Arc::new(ds.transaction(TransactionType::Write).await?);
	set_durable_build_state(
		&ds,
		&ikb,
		durable_build_state_for_phase(IndexBuildPhase::Online, 1, None),
	)
	.await?;
	let (opened, result) = decide(&straddling).await?;
	assert!(opened >= 1, "a build state commit since it opened forces a real admission");
	assert!(matches!(result, ConsumeResult::Ignored(..)));
	straddling.cancel().await?;

	// The index is building when the user transaction opens.
	let building = durable_build_state_for_phase(IndexBuildPhase::Building, 2, Some(ds.id()));
	set_durable_build_state(&ds, &ikb, building).await?;
	// The generation owns a ticket counter, so an admission writes no `!bs` and moves no epoch:
	// only the state of the index keeps it from being served.
	let tx = ds.transaction(TransactionType::Write).await?;
	tx.set_key(&ikb.new_bt_key(2), &0u64).await?;
	tx.commit().await?;
	for _ in 0..2 {
		let tx = Arc::new(ds.transaction(TransactionType::Write).await?);
		assert!(tx.build_epoch().is_some() && tx.catalog_epoch().is_some());
		let (opened, result) = decide(&tx).await?;
		assert!(opened >= 1, "a building index is admitted for real every time");
		assert!(!matches!(result, ConsumeResult::Ignored(..)), "a building index is not online");
		tx.cancel().await?;
	}

	// An `Online` state a build owner still holds is admitted as online but never kept.
	let held = durable_build_state_for_phase(IndexBuildPhase::Online, 3, Some(ds.id()));
	set_durable_build_state(&ds, &ikb, held).await?;
	for _ in 0..2 {
		let tx = Arc::new(ds.transaction(TransactionType::Write).await?);
		let (opened, result) = decide(&tx).await?;
		assert!(opened >= 1, "an online index with a build owner is read every time");
		assert!(matches!(result, ConsumeResult::Ignored(..)));
		tx.cancel().await?;
	}
	Ok(())
}

/// A definition replaced after its online state was kept is retired, not online.
#[tokio::test(flavor = "multi_thread")]
async fn online_index_admission_does_not_keep_a_replaced_definition() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE TABLE user SCHEMALESS; DEFINE INDEX test ON user FIELDS email;",
	)
	.await?;
	let (_, _, table, old_ix) = get_table_index(&ds, "user", "test").await?;
	let tx = Arc::new(ds.transaction(TransactionType::Write).await?);
	assert!(matches!(consume_one(&ds, &tx, &table, &old_ix).await?, ConsumeResult::Ignored(..)));
	tx.cancel().await?;
	execute_all(&ds, &session, "DEFINE INDEX OVERWRITE test ON user FIELDS account").await?;
	let tx = Arc::new(ds.transaction(TransactionType::Write).await?);
	assert!(matches!(consume_one(&ds, &tx, &table, &old_ix).await?, ConsumeResult::Retired));
	tx.cancel().await?;
	Ok(())
}

/// A datastore whose completed transactions are counted, with `user` defined and its index `one`
/// online, and that index's online state settled for the current epochs by one plan and one
/// write admission.
async fn new_observed_ds_with_online_index()
-> Result<(Arc<Datastore>, Arc<CompletedTransactions>, Session)> {
	let observer = Arc::new(CompletedTransactions::default());
	let ds = Datastore::builder()
		.with_observer(Arc::clone(&observer) as Arc<dyn crate::observe::ExecutionObserver>)
		.without_maintenance_tasks()
		.build_with_path("memory")
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
			CREATE user:one SET email = 'one@example.com', account = 1 RETURN NONE;
			DEFINE INDEX one ON user FIELDS email;
			",
	)
	.await?;
	let (ns, db, _, ix) = get_table_index(&ds, "user", "one").await?;
	let indexes: Arc<[IndexDefinition]> = Arc::from(vec![ix.as_ref().clone()]);
	let tx = ds.transaction(TransactionType::Read).await?;
	assert_eq!(filter_online_indexes(&tx, ns, db, indexes).await?.len(), 1);
	tx.cancel().await?;
	let tx = Arc::new(ds.transaction(TransactionType::Write).await?);
	let table = TableName::from("user");
	assert!(matches!(consume_one(&ds, &tx, &table, &ix).await?, ConsumeResult::Ignored(..)));
	tx.cancel().await?;
	Ok((ds, observer, session))
}

/// A `DEFINE INDEX ... CONCURRENTLY` that commits after other sessions' reads cached the online
/// set is not served as online: the next plan reads the new index's build state and leaves it
/// out, and the next write admission of it queues instead of indexing normally, while the index
/// that was online stays so.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_concurrent_define_after_cached_online_reads_is_not_served_online() -> Result<()> {
	let (ds, observer, session) = new_observed_ds_with_online_index().await?;
	let (ns, db, table, one) = get_table_index(&ds, "user", "one").await?;
	let cached = ds.transaction(TransactionType::Read).await?;
	assert!(cached.index_known_online(ns, db, &table, one.index_id), "the set is cached");
	cached.cancel().await?;

	let _guard = start_index_build_paused(
		&ds,
		&session,
		"DEFINE INDEX two ON user FIELDS account CONCURRENTLY",
	)
	.await?;
	let (_, _, _, two) = get_table_index(&ds, "user", "two").await?;
	let both: Arc<[IndexDefinition]> = Arc::from(vec![one.as_ref().clone(), two.as_ref().clone()]);

	for (kind, label) in [(TransactionType::Read, "read"), (TransactionType::Write, "write")] {
		let tx = ds.transaction(kind).await?;
		let online = filter_online_indexes(&tx, ns, db, Arc::clone(&both)).await?;
		let names: Vec<_> = online.iter().map(|ix| ix.name.to_string()).collect();
		assert_eq!(names, ["one"], "{label}: the building index is not online");
		assert!(!tx.index_known_online(ns, db, &table, two.index_id), "{label}");
		tx.cancel().await?;
	}

	let tx = Arc::new(ds.transaction(TransactionType::Write).await?);
	let before = observer.count();
	let result = consume_one(&ds, &tx, &table, &two).await?;
	assert!(observer.count() > before, "the building index is admitted by a read of its state");
	assert!(!matches!(result, ConsumeResult::Ignored(..)), "a building index is not online");
	assert!(matches!(consume_one(&ds, &tx, &table, &one).await?, ConsumeResult::Ignored(..)));
	tx.cancel().await?;
	Ok(())
}

/// A `DEFINE INDEX` or a `REBUILD INDEX` run inside a user transaction leaves that transaction
/// naming neither epoch, so its plans and its write admissions read the build state themselves
/// rather than take what other transactions of the epochs kept.
#[tokio::test(flavor = "multi_thread")]
async fn a_define_or_rebuild_in_a_transaction_turns_off_the_epoch_lookups() -> Result<()> {
	for sql in ["DEFINE INDEX two ON user FIELDS account", "REBUILD INDEX one ON user"] {
		let (ds, observer, session) = new_observed_ds_with_online_index().await?;
		let (ns, db, table, one) = get_table_index(&ds, "user", "one").await?;
		let indexes: Arc<[IndexDefinition]> = Arc::from(vec![one.as_ref().clone()]);

		let tx = Arc::new(ds.transaction(TransactionType::Write).await?);
		assert!(tx.build_epoch().is_some() && tx.catalog_epoch().is_some(), "{sql}");
		assert!(tx.index_known_online(ns, db, &table, one.index_id), "{sql}: before");
		for result in
			ds.run(QueryRequest::new(sql, &session).with_transaction(Arc::clone(&tx))).await?
		{
			result.result?;
		}
		assert_eq!(tx.build_epoch(), None, "{sql}");
		assert_eq!(tx.catalog_epoch(), None, "{sql}");
		assert!(!tx.index_known_online(ns, db, &table, one.index_id), "{sql}: after");

		let reads = tx.metrics_snapshot_for_test().ops_get;
		let online = filter_online_indexes(&tx, ns, db, indexes).await?;
		assert!(tx.metrics_snapshot_for_test().ops_get > reads, "{sql}: the plan reads for real");
		assert!(online.len() <= 1);

		let before = observer.count();
		consume_one(&ds, &tx, &table, &one).await?;
		assert!(observer.count() > before, "{sql}: the admission reads for real");
		tx.cancel().await?;
	}
	Ok(())
}

/// A writer on a backend whose plain reads join its conflict set reads the build state of its
/// plan itself, though the epochs' entry holds it; a writer that is not tracked is served.
#[tokio::test(flavor = "multi_thread")]
async fn a_tracked_writer_reads_the_build_state_of_its_plan() -> Result<()> {
	let (ds, _, _) = new_observed_ds_with_online_index().await?;
	let (ns, db, table, one) = get_table_index(&ds, "user", "one").await?;
	let indexes: Arc<[IndexDefinition]> = Arc::from(vec![one.as_ref().clone()]);

	let tracked =
		ds.transaction_at(TransactionType::Write, crate::kvs::Isolation::Serializable).await?;
	assert!(tracked.plain_reads_tracked() && !tracked.plans_from_known_online());
	assert!(tracked.index_known_online(ns, db, &table, one.index_id), "the entry is present");
	assert_eq!(filter_online_indexes(&tracked, ns, db, Arc::clone(&indexes)).await?.len(), 1);
	assert_eq!(tracked.metrics_snapshot_for_test().ops_get, 1, "a tracked writer reads");
	tracked.cancel().await?;

	let snapshot = ds.transaction(TransactionType::Write).await?;
	assert!(snapshot.plans_from_known_online());
	assert_eq!(filter_online_indexes(&snapshot, ns, db, indexes).await?.len(), 1);
	assert_eq!(snapshot.metrics_snapshot_for_test().ops_get, 0, "a snapshot writer is served");
	snapshot.cancel().await?;
	Ok(())
}

/// A datastore of `t` with an online index `ix` of `define`'s kind, whose completed transactions
/// are counted, and whose online state is settled for the current epochs by one plan and one
/// write admission, so a user transaction opened now skips its admission of `ix`.
#[cfg(feature = "kv-mem")]
struct AdmissionRace {
	ds: Arc<Datastore>,
	observer: Arc<CompletedTransactions>,
	session: Session,
	define: &'static str,
}

/// The two kinds of index the admission race is run over: one whose entries a second application
/// of a write overwrites, and one whose entries it doubles.
#[cfg(feature = "kv-mem")]
const ADMISSION_RACE_INDEXES: [&str; 2] =
	["DEFINE INDEX ix ON t FIELDS f", "DEFINE INDEX ix ON t COUNT"];

/// The write the race decides the admission of: a record that sorts after every seeded one.
#[cfg(feature = "kv-mem")]
const ADMISSION_RACE_WRITE: &str = "CREATE t:zzz SET f = 'vzzz' RETURN NONE";

#[cfg(feature = "kv-mem")]
impl AdmissionRace {
	async fn new(path: &str, define: &'static str) -> Result<Self> {
		let observer = Arc::new(CompletedTransactions::default());
		let ds = Datastore::builder()
			.with_observer(Arc::clone(&observer) as Arc<dyn crate::observe::ExecutionObserver>)
			.without_maintenance_tasks()
			.build_with_path(path)
			.await?;
		let session = Session::owner().with_ns("test").with_db("test");
		let tx = ds.transaction(TransactionType::Write).await?;
		tx.ensure_ns_db(None, "test", "test").await?;
		tx.commit().await?;
		let seed: String =
			(0..5).map(|i| format!("CREATE t:r{i} SET f = 'v{i}' RETURN NONE;")).collect();
		execute_all(&ds, &session, &format!("DEFINE TABLE t SCHEMALESS; {seed} {define};")).await?;
		wait_for_index_ready(&ds, &session, "t", "ix").await?;
		let race = Self {
			ds,
			observer,
			session,
			define,
		};
		let (ns, db, table, ix) = get_table_index(&race.ds, "t", "ix").await?;
		let indexes: Arc<[IndexDefinition]> = Arc::from(vec![ix.as_ref().clone()]);
		let tx = race.ds.transaction(TransactionType::Read).await?;
		assert_eq!(filter_online_indexes(&tx, ns, db, indexes).await?.len(), 1);
		tx.cancel().await?;
		let tx = Arc::new(race.ds.transaction(TransactionType::Write).await?);
		assert!(matches!(
			consume_one(&race.ds, &tx, &table, &ix).await?,
			ConsumeResult::Ignored(..)
		));
		tx.cancel().await?;
		race.assert_known_online(true).await?;
		Ok(race)
	}

	/// Whether a transaction opened now is served the online state of `ix`.
	async fn assert_known_online(&self, expected: bool) -> Result<()> {
		let (ns, db, table, ix) = get_table_index(&self.ds, "t", "ix").await?;
		let tx = self.ds.transaction(TransactionType::Read).await?;
		assert_eq!(tx.index_known_online(ns, db, &table, ix.index_id), expected);
		tx.cancel().await?;
		Ok(())
	}

	/// Run `sql` in `tx`, and answer how many transactions completed meanwhile: the
	/// admissions it opened.
	async fn write_in(&self, tx: &Arc<crate::kvs::Transaction>, sql: &str) -> Result<u64> {
		let before = self.observer.count();
		for result in self
			.ds
			.run(QueryRequest::new(sql, &self.session).with_transaction(Arc::clone(tx)))
			.await?
		{
			result.result?;
		}
		Ok(self.observer.count() - before)
	}

	/// Start a blocking `REBUILD INDEX` on its own task.
	fn rebuild(&self) -> tokio::task::JoinHandle<Result<()>> {
		tokio::spawn({
			let (ds, session) = (Arc::clone(&self.ds), self.session.clone());
			async move { execute_all(&ds, &session, "REBUILD INDEX ix ON t").await }
		})
	}

	/// The phase of the index's durable build state.
	async fn phase(&self) -> Result<IndexBuildPhase> {
		let (ns, db, table, ix) = get_table_index(&self.ds, "t", "ix").await?;
		let ikb = IndexKeyBase::new(ns, db, table, ix.index_id);
		Ok(durable_build_state(&self.ds, &ikb).await?.phase)
	}

	/// Wait until the index's durable build state is `Online` with no build owner and its builder
	/// task on this datastore, if any, has exited: the build that was running has finished. An
	/// index status of `ready` read before then can be the previous build's.
	async fn wait_for_build_to_settle(&self) -> Result<()> {
		let (ns, db, table, ix) = get_table_index(&self.ds, "t", "ix").await?;
		let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
		timeout(Duration::from_secs(30), async {
			loop {
				let state = durable_build_state(&self.ds, &ikb).await?;
				if state.phase == IndexBuildPhase::Online && state.owner.is_none() {
					return Ok::<(), anyhow::Error>(());
				}
				sleep(Duration::from_millis(10)).await;
			}
		})
		.await
		.map_err(|_| anyhow::anyhow!("timed out waiting for the build to settle"))??;
		match local_builder_for_key(&self.ds, ns, db, &table, ix.index_id).await? {
			Some(building) => wait_for_finished_builder(&building).await,
			None => Ok(()),
		}
	}

	/// The index answers for the written record exactly once, and still does after a fresh
	/// `REBUILD INDEX` over the final table; a plain index also holds exactly the entries that
	/// rebuild produces. A COUNT index keeps its total as a base and deltas whose split between
	/// the two depends on compaction, so only its answer is compared.
	async fn assert_matches_a_fresh_rebuild(&self, case: &str) -> Result<()> {
		let answer = async || -> Result<()> {
			wait_for_index_ready(&self.ds, &self.session, "t", "ix").await?;
			if self.define.ends_with("COUNT") {
				let (indexed, expected) =
					RebuiltIndexKind::Count.answers(&self.ds, &self.session).await?;
				assert_eq!(indexed, expected, "{case}: the count");
				assert_eq!(indexed, serde_json::json!(6), "{case}: the count");
			} else {
				expect_indexed_query_len(
					&self.ds,
					&self.session,
					"SELECT id FROM t WITH INDEX ix WHERE f = 'vzzz'",
					1,
				)
				.await?;
			}
			Ok(())
		};
		let entries = async || -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
			let (ns, db, table, ix) = get_table_index(&self.ds, "t", "ix").await?;
			index_entries(&self.ds, ns, db, &table, ix.index_id).await
		};
		answer().await?;
		let built = entries().await?;
		execute_all(&self.ds, &self.session, "REBUILD INDEX ix ON t").await?;
		answer().await?;
		if !self.define.ends_with("COUNT") {
			assert_eq!(built, entries().await?, "{case}: the entries of a fresh rebuild");
		}
		Ok(())
	}
}

/// A write whose admission was skipped, because its transaction named the epochs of the online
/// state it read, is still accounted for by a `REBUILD INDEX` that starts while the transaction is
/// open: the rebuild commits its generation, then reads only once every write transaction opened
/// before that commit has ended, so the write has committed before the wipe and the scan, which
/// index it once.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_writer_that_skips_admission_is_accounted_for_by_a_later_rebuild() -> Result<()> {
	for define in ADMISSION_RACE_INDEXES {
		for engine in storage_engines()? {
			let case = format!("{} {define}", engine.label);
			let race = AdmissionRace::new(&engine.path, define).await?;
			let writer = Arc::new(race.ds.transaction(TransactionType::Write).await?);
			assert!(writer.build_epoch().is_some() && writer.catalog_epoch().is_some(), "{case}");
			assert_eq!(race.write_in(&writer, ADMISSION_RACE_WRITE).await?, 0, "{case}: skipped");

			let waited = super::builder::write_barrier_waits::count(race.ds.id());
			let rebuild = race.rebuild();
			wait_for_a_build_at_the_write_barrier(race.ds.id(), waited).await?;
			assert_eq!(race.phase().await?, IndexBuildPhase::Building, "{case}");
			sleep(Duration::from_millis(200)).await;
			assert!(!rebuild.is_finished(), "{case}: the rebuild waits for the open writer");

			writer.commit().await?;
			timeout(Duration::from_secs(30), rebuild)
				.await
				.map_err(|_| anyhow::anyhow!("{case}: timed out waiting for the rebuild"))???;
			race.assert_matches_a_fresh_rebuild(&case).await?;
			race.ds.shutdown().await?;
		}
	}
	Ok(())
}

/// A writer that opens after a `REBUILD INDEX` committed its generation is not served the online
/// state kept before it: its admission reads the index as building and queues the write for the
/// build, which indexes it once.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_writer_opened_after_a_rebuild_commits_its_generation_is_queued() -> Result<()> {
	for define in ADMISSION_RACE_INDEXES {
		for engine in storage_engines()? {
			let case = format!("{} {define}", engine.label);
			let race = AdmissionRace::new(&engine.path, define).await?;
			let guard = start_index_build_paused(
				&race.ds,
				&race.session,
				"REBUILD INDEX ix ON t CONCURRENTLY",
			)
			.await?;
			assert_eq!(race.phase().await?, IndexBuildPhase::Building, "{case}");
			race.assert_known_online(false).await?;

			let writer = Arc::new(race.ds.transaction(TransactionType::Write).await?);
			assert!(race.write_in(&writer, ADMISSION_RACE_WRITE).await? >= 1, "{case}: admitted");
			writer.commit().await?;
			drop(guard);
			race.wait_for_build_to_settle().await?;
			race.assert_matches_a_fresh_rebuild(&case).await?;
			race.ds.shutdown().await?;
		}
	}
	Ok(())
}

/// A writer that opened before a `REBUILD INDEX` committed its generation, and makes its first
/// write to the table only after, is not served the online state kept for the epochs it opened
/// in: the build epoch moved, so its admission reads the index as building and queues the write.
/// The rebuild waits for the writer's transaction before it reads, and indexes the write once.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_writer_open_across_a_rebuilds_generation_commit_is_queued() -> Result<()> {
	for define in ADMISSION_RACE_INDEXES {
		for engine in storage_engines()? {
			let case = format!("{} {define}", engine.label);
			let race = AdmissionRace::new(&engine.path, define).await?;
			let writer = Arc::new(race.ds.transaction(TransactionType::Write).await?);
			assert!(writer.build_epoch().is_some() && writer.catalog_epoch().is_some(), "{case}");

			let waited = super::builder::write_barrier_waits::count(race.ds.id());
			let rebuild = race.rebuild();
			wait_for_a_build_at_the_write_barrier(race.ds.id(), waited).await?;
			assert_eq!(race.phase().await?, IndexBuildPhase::Building, "{case}");
			assert!(writer.build_epoch().is_none(), "{case}: the commit moved the build epoch");

			assert!(race.write_in(&writer, ADMISSION_RACE_WRITE).await? >= 1, "{case}: admitted");
			assert!(!rebuild.is_finished(), "{case}: the rebuild waits for the open writer");
			writer.commit().await?;
			timeout(Duration::from_secs(30), rebuild)
				.await
				.map_err(|_| anyhow::anyhow!("{case}: timed out waiting for the rebuild"))???;
			race.assert_matches_a_fresh_rebuild(&case).await?;
			race.ds.shutdown().await?;
		}
	}
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

/// The compactor's write guard refuses an index whose definition has been
/// retired, so a pass dispatched before a `REMOVE INDEX` writes nothing under
/// the index's prefix once it has committed.
#[tokio::test(flavor = "multi_thread")]
async fn compaction_write_guard_rejects_a_committed_index_removal() -> Result<()> {
	use crate::kvs::index::materialise::{CompactionWriteGuard, compaction_write_guard};
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

	let tx = ds.transaction(TransactionType::Write).await?;
	let guard = catch!(tx, compaction_write_guard(&tx, &ikb).await);
	assert!(matches!(guard, CompactionWriteGuard::Live(_)), "a live index passes the guard");
	tx.cancel().await?;

	execute_all(&ds, &session, "REMOVE INDEX test ON user").await?;

	let tx = ds.transaction(TransactionType::Write).await?;
	let guard = catch!(tx, compaction_write_guard(&tx, &ikb).await);
	assert!(
		matches!(guard, CompactionWriteGuard::Retired),
		"a retired index must not accept compaction writes"
	);
	tx.cancel().await?;
	Ok(())
}

/// A compaction plan read before a `REBUILD INDEX` does not apply after it:
/// the rebuild's wipe advances the index's compaction generation, so a plan
/// that captured the previous build's deltas reports a lost race rather than
/// folding them into the rebuilt index.
#[tokio::test(flavor = "multi_thread")]
async fn a_compaction_plan_read_before_a_rebuild_does_not_apply_after_it() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(&ds, &session, "DEFINE INDEX cx ON t COUNT; CREATE t:1; CREATE t:2;").await?;
	let (ns, db, table, ix) = get_table_index(&ds, "t", "cx").await?;
	let ikb = IndexKeyBase::new(ns, db, table, ix.index_id);
	assert!(count_deltas(&ds, &ikb).await? > 0, "the writes leave deltas to plan");

	let tx = ds.transaction(TransactionType::Read).await?;
	let plan = catch!(tx, IndexOperation::prepare_count_compaction(&ikb, &tx).await);
	tx.cancel().await?;
	assert!(plan.has_work());

	execute_all(&ds, &session, "REBUILD INDEX cx ON t").await?;

	let tx = ds.transaction(TransactionType::Write).await?;
	let applied = catch!(tx, IndexOperation::apply_count_compaction(&ikb, &tx, plan).await);
	tx.cancel().await?;
	assert_eq!(applied, crate::idx::CompactionApply::LostRace);
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
			basis: None,
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

/// Number of HNSW record-keyed pending entries queued for one index, in `!hr`
/// and every `!hw` shard.
#[cfg(feature = "kv-mem")]
async fn count_hnsw_pendings(ds: &Datastore, ikb: &IndexKeyBase) -> Result<usize> {
	let tx = ds.transaction(TransactionType::Read).await?;
	let count = catch!(tx, tx.count(ikb.new_hr_range()?, None).await)
		+ catch!(tx, tx.count(ikb.new_hw_range()?, None).await);
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
			nulls_not_distinct: false,
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
	let build_a = Arc::new(Building::new(
		&ctx,
		ds.transaction_factory().clone(),
		opt.clone(),
		table_id,
		Arc::clone(&index),
		Arc::clone(&index_key),
	)?);
	let build_b = Arc::new(Building::new(
		&ctx,
		ds.transaction_factory().clone(),
		opt,
		table_id,
		index,
		index_key,
	)?);
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
				basis: None,
			},
		)
		.await?;
		// The owner waited at the write barrier for its generation, so the takeover keeps its
		// completed scan (see `a_takeover_rescans_a_completed_scan_without_barrier_proof`).
		tx.set_key(&ikb.new_bw_key(), &generation).await?;
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
			basis: None,
		},
	)
	.await?;
	// The crashed owner waited at the write barrier for its generation before it scanned, so the
	// takeover keeps its checkpoint (see
	// `a_takeover_rescans_a_completed_scan_without_barrier_proof`).
	tx.set_key(&ikb.new_bw_key(), &2).await?;
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
			basis: None,
		},
	)
	.await?;
	// The crashed owner waited at the write barrier for its generation before it scanned, so the
	// takeover keeps its checkpoint (see
	// `a_takeover_rescans_a_completed_scan_without_barrier_proof`).
	tx.set_key(&ikb.new_bw_key(), &2).await?;
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
			basis: None,
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
			basis: None,
		},
	)
	.await?;
	// The crashed owner waited at the write barrier for its generation before it scanned, so the
	// takeover keeps its checkpoint (see
	// `a_takeover_rescans_a_completed_scan_without_barrier_proof`).
	tx.set_key(&ikb.new_bw_key(), &2).await?;
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
			basis: None,
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
			basis: None,
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
	assert_eq!(resumed.items, 1, "scan should adopt the stalled build");

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
	assert_eq!(resumed_again.items, 0, "healthy index must not be re-adopted");

	Ok(())
}

/// A resume scan works from a snapshot of the catalog, which can be stale by
/// the time it takes a candidate up: the table may have been removed and
/// defined again, under an index id the snapshot also names. A candidate whose
/// table id or definition the catalog no longer holds is not adopted, however
/// its build state reads; the same candidate, current, is.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn resume_does_not_adopt_a_build_for_a_stale_definition() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE TABLE user SCHEMALESS;
		 CREATE user:one SET email = 'one@example.com', name = 'one' RETURN NONE;
		 DEFINE INDEX test ON user FIELDS email;",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "user", "test").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	let tb = current_table_id(&ds, ns, db, &table).await?;
	let expired = Utc::now() - chrono::Duration::seconds(BUILD_OWNER_LEASE_SECS + 5);
	let mut stalled = durable_build_state(&ds, &ikb).await?;
	stalled.generation += 1;
	stalled.phase = IndexBuildPhase::Building;
	stalled.owner = Some(Uuid::new_v4());
	stalled.initial_complete = false;
	stalled.updated_at = expired;
	stalled.owner_heartbeat_at = Some(expired);
	set_durable_build_state(&ds, &ikb, stalled.clone()).await?;

	let ctx = ds.setup_ctx()?.freeze();
	let builder = ctx.get_index_builder().expect("index builder should exist").clone();
	let opt = ds.setup_options(&session);
	let other_table = TableId(tb.0 + 1);
	let other_definition = Arc::new(IndexDefinition {
		cols: vec![Idiom::field("name")],
		..(*ix).clone()
	});
	for (tb, ix, what) in [
		(other_table, Arc::clone(&ix), "a table id the catalog no longer holds"),
		(tb, other_definition, "a definition the catalog no longer holds"),
	] {
		let adopted =
			builder.resume_stalled(&ctx, opt.clone(), ns, db, tb, ix, ResumeScope::Any).await?;
		assert!(!adopted, "a candidate with {what} must not be adopted");
		assert_eq!(
			durable_build_state(&ds, &ikb).await?.owner,
			stalled.owner,
			"a candidate with {what} must leave the stalled build alone"
		);
	}
	assert!(
		builder.resume_stalled(&ctx, opt, ns, db, tb, Arc::clone(&ix), ResumeScope::Any).await?,
		"the current candidate is adopted"
	);
	let building = local_builder_for_key(&ds, ns, db, &table, ix.index_id)
		.await?
		.expect("the adopted build registers a local builder");
	wait_for_finished_builder(&building).await?;
	assert_eq!(durable_build_state(&ds, &ikb).await?.phase, IndexBuildPhase::Online);
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
			basis: None,
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
		basis: None,
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
		basis: None,
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
		basis: None,
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
	let outcome = run_recheck_cached_admission(&ds, ns, db, &ikb, ix.as_ref(), 1, None)
		.await?
		.expect("recheck on matching Building state should succeed");
	assert!(matches!(outcome, CachedAdmission::Valid));

	// Closing is still queueable.
	seed_build_state(&ds, &ikb, IndexBuildPhase::Closing, 1).await?;
	let outcome = run_recheck_cached_admission(&ds, ns, db, &ikb, ix.as_ref(), 1, None)
		.await?
		.expect("recheck on matching Closing state should succeed");
	assert!(matches!(outcome, CachedAdmission::Valid));

	// Generation rotation — recheck aborts.
	seed_build_state(&ds, &ikb, IndexBuildPhase::Building, 2).await?;
	let err = run_recheck_cached_admission(&ds, ns, db, &ikb, ix.as_ref(), 1, None)
		.await?
		.expect_err("generation mismatch must abort cached admission");
	assert!(err.to_string().contains("generation"), "unexpected error: {err}");

	// Online phase — cached writers cannot trust the cached ticket.
	seed_build_state(&ds, &ikb, IndexBuildPhase::Online, 1).await?;
	let err = run_recheck_cached_admission(&ds, ns, db, &ikb, ix.as_ref(), 1, None)
		.await?
		.expect_err("online phase must abort cached admission");
	assert!(err.to_string().contains("online"), "unexpected error: {err}");

	// Error phase — a non-UNIQUE index keeps queueing. The queued mutations
	// die with the errored generation's wipe and the records are rescanned by
	// the recovering `REBUILD INDEX`.
	seed_build_state(&ds, &ikb, IndexBuildPhase::Error, 1).await?;
	let outcome = run_recheck_cached_admission(&ds, ns, db, &ikb, ix.as_ref(), 1, None)
		.await?
		.expect("recheck on matching Error state should keep queueing");
	assert!(matches!(outcome, CachedAdmission::Valid));

	// Missing state under a definition the catalog still carries — recheck
	// aborts: a build vanished under a live index, which this cannot repair.
	let tx = ds.transaction(TransactionType::Write).await?;
	tx.del_key(&ikb.new_bs_key()).await?;
	tx.commit().await?;
	let err = run_recheck_cached_admission(&ds, ns, db, &ikb, ix.as_ref(), 1, None)
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

	let outcome = run_recheck_cached_admission(&ds, ns, db, &ikb, cached_ix.as_ref(), 1, None)
		.await?
		.expect("a retired index must not fail the write that was mid-transaction");
	assert!(
		matches!(outcome, CachedAdmission::Retired),
		"the recheck must report the index retired so the mutation skips it"
	);
	Ok(())
}

/// A write that adds a key to a UNIQUE index whose build failed is refused,
/// naming the index, while a write that adds no key to it is admitted. Under
/// `NULLS NOT DISTINCT`, `NULL` is a unique key and is refused too, while
/// `NONE` remains exempt.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_unique_index_in_error_refuses_a_write_that_adds_a_key() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE INDEX ux ON t FIELDS n UNIQUE; CREATE t:1 SET n = 1; \
		 DEFINE INDEX ux_nnd ON u FIELDS n UNIQUE NULLS NOT DISTINCT; \
		 CREATE u:0 SET n = NULL; CREATE u:1 SET n = 1;",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "t", "ux").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	seed_build_state(&ds, &ikb, IndexBuildPhase::Error, 1).await?;

	let err = execute_all(&ds, &session, "CREATE t:2 SET n = 2")
		.await
		.expect_err("a write adding a key to a failed UNIQUE index must be refused");
	let message = err.to_string();
	assert!(
		message.contains("`ux`") && message.contains("failed to build"),
		"unexpected error: {message}"
	);
	execute_all(
		&ds,
		&session,
		"CREATE t:3 SET m = 3; CREATE t:4 SET n = NULL; CREATE t:5 SET n = [];",
	)
	.await?;

	let (ns, db, table_u, ix_u) = get_table_index(&ds, "u", "ux_nnd").await?;
	let ikb_u = IndexKeyBase::new(ns, db, table_u, ix_u.index_id);
	seed_build_state(&ds, &ikb_u, IndexBuildPhase::Error, 1).await?;

	let err = execute_all(&ds, &session, "CREATE u:2 SET n = NULL")
		.await
		.expect_err("a write adding NULL to a failed UNIQUE NULLS NOT DISTINCT index must fail");
	let message = err.to_string();
	assert!(
		message.contains("`ux_nnd`") && message.contains("failed to build"),
		"unexpected error: {message}"
	);
	let err = execute_all(&ds, &session, "CREATE u:5 SET n = []")
		.await
		.expect_err("Indexable keys [] as NULL, so UNIQUE NULLS NOT DISTINCT must refuse it");
	let message = err.to_string();
	assert!(
		message.contains("`ux_nnd`") && message.contains("failed to build"),
		"unexpected error: {message}"
	);
	execute_all(
		&ds,
		&session,
		"CREATE u:3 SET m = 3; CREATE u:4 SET n = NONE; UPDATE u:0 SET m = 10;",
	)
	.await?;
	Ok(())
}

/// The admission a transaction cached while the build was building is
/// rechecked against the errored state: it refuses a mutation that adds a
/// key, and admits one that adds none.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_cached_unique_admission_refuses_a_key_once_the_build_fails() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(&ds, &session, "DEFINE INDEX ux ON t FIELDS n UNIQUE").await?;
	let (ns, db, table, ix) = get_table_index(&ds, "t", "ux").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	seed_build_state(&ds, &ikb, IndexBuildPhase::Error, 1).await?;

	let rid = RecordId {
		table: table.clone(),
		key: RecordIdKey::from("two".to_owned()),
	};
	let adds_key = IndexMutation {
		old_values: None,
		new_values: Some(vec![Value::from(2i64)]),
		rid: &rid,
		count_cond_match: None,
	};
	let err = run_recheck_cached_admission(&ds, ns, db, &ikb, ix.as_ref(), 1, Some(&adds_key))
		.await?
		.expect_err(
			"a cached admission must refuse a mutation adding a key to a failed UNIQUE index",
		);
	assert!(err.to_string().contains("failed to build"), "unexpected error: {err}");

	let adds_no_key = IndexMutation {
		old_values: None,
		new_values: None,
		rid: &rid,
		count_cond_match: None,
	};
	let outcome =
		run_recheck_cached_admission(&ds, ns, db, &ikb, ix.as_ref(), 1, Some(&adds_no_key))
			.await?
			.expect("a mutation that adds no key keeps queueing");
	assert!(matches!(outcome, CachedAdmission::Valid));
	Ok(())
}

/// A `DEFINE INDEX` whose save point is rolled back inside a transaction that
/// then commits — an event that defines an index and throws, inside a row whose
/// error a caller catches — still has its separately committed build state
/// cleaned up: the save point rollback runs the cleanup, rather than leaving it
/// for a transaction rollback that never comes.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn save_point_rollback_cleans_uncommitted_index_build_artifacts() -> Result<()> {
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
	let ix = IndexId(43);
	seed_uncommitted_index_build_artifacts(&ds, ns, db, &table, ix).await?;

	let user_tx = ds.transaction(TransactionType::Write).await?;
	let ctx = ds.setup_ctx()?;
	let builder = ctx.get_index_builder().expect("index builder should exist").clone();
	user_tx.new_save_point().await?;
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
	user_tx.rollback_to_save_point().await?;
	assert_no_index_build_artifacts(&ds, ns, db, &table, ix).await?;
	user_tx.commit().await?;
	assert_no_index_build_artifacts(&ds, ns, db, &table, ix).await?;
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
		basis: None,
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
	let outcome =
		builder.recheck_cached_admission(&frozen, &ikb, ix.as_ref(), ns, db, 1, None).await;
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
	let outcome =
		builder.recheck_cached_admission(&frozen, &ikb, old_ix.as_ref(), ns, db, 1, None).await;
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
	mutation: Option<&IndexMutation<'_>>,
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
	let result = builder
		.recheck_cached_admission(&frozen, ikb, ix, ns, db, cached_generation, mutation)
		.await;
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
		basis: None,
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

/// Memory pressure on the node whose blocking statement waits for another
/// node's build does not end the wait: the waiting statement holds none of
/// the build's memory, and the build carries on regardless. Only the query's
/// deadline or cancellation ends it.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn distributed_blocking_wait_on_a_remote_owner_ignores_memory_pressure() -> Result<()> {
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
	let node_b = ds_b.id();
	let session_rebuild = session.clone();
	let rebuild = tokio::spawn(async move {
		execute_all(&ds_b, &session_rebuild, "REBUILD INDEX test ON user").await
	});
	sleep(Duration::from_millis(200)).await;
	assert!(!rebuild.is_finished(), "the rebuild did not wait for the remote build");
	// Node B is over its memory threshold for several of the wait's polls.
	let pressure = crate::kvs::testing::inject_non_retryable_errors(
		NonRetryableErrorSite::ContextDeepCheckMemoryThreshold,
		node_b,
		REPEATED_RETRY_CONFLICTS,
	);
	sleep(Duration::from_millis(500)).await;
	drop(pressure);
	if rebuild.is_finished() {
		panic!("memory pressure ended the wait: {:?}", rebuild.await);
	}
	drop(guard);
	timeout(Duration::from_secs(10), rebuild)
		.await
		.map_err(|_| anyhow::anyhow!("timed out waiting for blocking rebuild"))???;
	assert_eq!(index_building_status(&ds_a, &session, "user", "test").await?, "ready");
	Ok(())
}

/// Two nodes as [`new_distributed_index_test_ds`] gives, both with the query
/// timeout `query`.
#[cfg(feature = "kv-mem")]
async fn new_distributed_index_test_ds_with_query_timeout(
	query: Duration,
) -> Result<(Arc<Datastore>, Datastore, Session)> {
	let (ds_a, session) = new_index_test_ds_with_timeouts(Some(query), None).await?;
	let ds_b = ds_a.fork_for_test_with_node_id(uuid::Uuid::new_v4());
	ds_a.insert_node().await?;
	ds_b.insert_node().await?;
	Ok((ds_a, ds_b, session))
}

/// Run `sql` on `ds` as a request `cancel` can cancel, returning each
/// statement's error, if any.
#[cfg(feature = "kv-mem")]
async fn run_cancellable(
	ds: &Datastore,
	session: &Session,
	sql: &str,
	cancel: crate::ctx::CancelHandle,
) -> Result<Vec<Option<String>>> {
	let request = crate::kvs::QueryRequest::new(sql, session).with_cancel(cancel);
	Ok(ds.run(request).await?.into_iter().map(|r| r.result.err().map(|e| e.to_string())).collect())
}

/// A blocking `REBUILD INDEX` waiting after its commit for another node's
/// build reports, when its query times out or is cancelled, that its rebuild
/// committed and goes on in the background, as a rebuild of its own build
/// does: never that the query did not run.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn distributed_blocking_rebuild_reports_its_commit_when_its_wait_ends() -> Result<()> {
	for cancelled in [false, true] {
		let (ds_a, ds_b, session) =
			new_distributed_index_test_ds_with_query_timeout(Duration::from_millis(600)).await?;
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
		let cancel = crate::ctx::CancelHandle::new();
		let rebuild = tokio::spawn({
			let (cancel, session) = (cancel.clone(), session.clone());
			async move { run_cancellable(&ds_b, &session, "REBUILD INDEX test ON user", cancel).await }
		});
		if cancelled {
			sleep(Duration::from_millis(250)).await;
			cancel.trip();
		}
		let errors = timeout(Duration::from_secs(10), rebuild)
			.await
			.map_err(|_| anyhow::anyhow!("the rebuild did not stop waiting"))???;
		let expected = if cancelled {
			"The index rebuild was committed, but the query was cancelled"
		} else {
			"The index rebuild was committed, but its build did not finish"
		};
		assert!(
			matches!(errors.as_slice(), [Some(err)] if err.starts_with(expected)),
			"cancelled: {cancelled}, the rebuild reported {errors:?}"
		);
		drop(guard);
		wait_for_index_ready(&ds_a, &session, "user", "test").await?;
	}
	Ok(())
}

/// A blocking `REBUILD INDEX` waiting inside its transaction for another
/// node's build, which takes the build over once that node's lease expires,
/// still stops waiting when its query is cancelled: the build it took over
/// goes on without it.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_wait_that_takes_over_a_remote_build_still_ends_on_cancellation() -> Result<()> {
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
	let now = Utc::now();
	let mut state = durable_build_state(&ds_a, &ikb).await?;
	state.generation += 1;
	state.phase = IndexBuildPhase::Building;
	state.owner = Some(uuid::Uuid::new_v4());
	state.initial_complete = false;
	state.initial_cursor = None;
	state.updated_at = now;
	state.owner_heartbeat_at = Some(now);
	state.report_status = Some(IndexBuildReportStatus::Indexing);
	set_durable_build_state(&ds_a, &ikb, state).await?;
	// Node B's build of the generation it takes over is held at its first
	// initial-scan batch.
	let site = RetryableConflictSite::ConcurrentIndexInitialBatch;
	let hold = inject_retryable_conflicts(site, ds_b.id(), REPEATED_RETRY_CONFLICTS);
	let node_b = ds_b.id();
	let cancel = crate::ctx::CancelHandle::new();
	let rebuild = tokio::spawn({
		let (cancel, session) = (cancel.clone(), session.clone());
		async move {
			run_cancellable(&ds_b, &session, "BEGIN; REBUILD INDEX test ON user; COMMIT;", cancel)
				.await
		}
	});
	sleep(Duration::from_millis(200)).await;
	assert!(!rebuild.is_finished(), "the rebuild did not wait for the remote build");
	let expired = Utc::now() - chrono::Duration::seconds(BUILD_OWNER_LEASE_SECS + 5);
	let mut state = durable_build_state(&ds_a, &ikb).await?;
	state.updated_at = expired;
	state.owner_heartbeat_at = Some(expired);
	set_durable_build_state(&ds_a, &ikb, state).await?;
	wait_for_retry_conflict(site, node_b, REPEATED_RETRY_CONFLICTS).await?;
	cancel.trip();
	let outcome = timeout(Duration::from_secs(3), rebuild).await;
	drop(hold);
	let errors = outcome.map_err(|_| anyhow::anyhow!("cancellation did not end the wait"))???;
	assert!(
		errors.iter().any(|err| err.as_deref().is_some_and(|err| err.contains("cancelled"))),
		"the rebuild was not cancelled: {errors:?}"
	);
	Ok(())
}

/// A blocking build of `user.test` started while a live owner on another node
/// builds generation `joined`: the statement's transaction, still open, the
/// completion it would await, not yet polled, and `joined`.
#[cfg(feature = "kv-mem")]
async fn join_a_remote_build(
	ds: &Arc<Datastore>,
	session: &Session,
) -> Result<(IndexKeyBase, Arc<crate::kvs::Transaction>, BuildCompletion, BuildGeneration)> {
	execute_all(
		ds,
		session,
		"DEFINE TABLE user SCHEMALESS;
		 CREATE user:one SET email = 'one@example.com' RETURN NONE;
		 DEFINE INDEX test ON user FIELDS email;",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(ds, "user", "test").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	let joined = durable_build_state(ds, &ikb).await?.generation + 1;
	let state =
		durable_build_state_for_phase(IndexBuildPhase::Building, joined, Some(Uuid::new_v4()));
	set_durable_build_state(ds, &ikb, state).await?;
	let (txn, completion) = start_index_build_inside_an_open_statement_transaction(
		ds, session, ns, db, &table, ix, true,
	)
	.await?;
	let completion = completion.expect("a blocking build has a completion");
	assert!(!completion.started_here(), "the build is the remote owner's");
	assert_eq!(completion.generation(), joined, "the wait joined the remote generation");
	Ok((ikb, txn, completion, joined))
}

/// Await `completion` with a guard that turns a wait that never ends into a
/// failure rather than a hang.
#[cfg(feature = "kv-mem")]
async fn settle_remote_wait(completion: BuildCompletion) -> Result<Result<()>> {
	timeout(Duration::from_secs(10), completion)
		.await
		.map_err(|_| anyhow::anyhow!("the wait did not end with the generation it joined"))
}

/// A wait on a remote build that first looks once the generation it joined
/// has gone `Online` and a later build has begun does not wait on that later
/// build: the generation it joined has ended. Nothing durable says how it
/// ended, so the wait reports that it was superseded.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_remote_wait_does_not_wait_on_a_later_generation() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	let (ikb, txn, completion, joined) = join_a_remote_build(&ds, &session).await?;
	let online = durable_build_state_for_phase(IndexBuildPhase::Online, joined, None);
	set_durable_build_state(&ds, &ikb, online).await?;
	let later =
		durable_build_state_for_phase(IndexBuildPhase::Building, joined + 1, Some(Uuid::new_v4()));
	set_durable_build_state(&ds, &ikb, later).await?;

	let err =
		settle_remote_wait(completion).await?.expect_err("the joined generation ended unseen");
	assert!(err.to_string().contains("superseded"), "unexpected error: {err}");
	txn.cancel().await?;
	Ok(())
}

/// A wait on a remote build never reports a later build's failure as its own.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_remote_wait_does_not_report_a_later_generation_error() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	let (ikb, txn, completion, joined) = join_a_remote_build(&ds, &session).await?;
	let mut later = durable_build_state_for_phase(IndexBuildPhase::Error, joined + 1, None);
	later.error = Some("the later build's own failure".into());
	set_durable_build_state(&ds, &ikb, later).await?;

	let err =
		settle_remote_wait(completion).await?.expect_err("the joined generation ended unseen");
	assert!(
		!err.to_string().contains("the later build's own failure"),
		"the wait reported a later build's error: {err}"
	);
	assert!(err.to_string().contains("superseded"), "unexpected error: {err}");
	txn.cancel().await?;
	Ok(())
}

/// A wait on a remote build that sees the generation it joined `Online`
/// succeeds.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_remote_wait_succeeds_once_its_generation_is_online() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	let (ikb, txn, completion, joined) = join_a_remote_build(&ds, &session).await?;
	let online = durable_build_state_for_phase(IndexBuildPhase::Online, joined, None);
	set_durable_build_state(&ds, &ikb, online).await?;

	settle_remote_wait(completion).await??;
	txn.cancel().await?;
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
			basis: None,
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
	// A write on any node that adds a key to the failed UNIQUE index is refused
	// by that node: nothing enforces the constraint until a recovering `REBUILD
	// INDEX` succeeds or the index is removed.
	let refused = execute_all_retrying_conflicts(
		&ds_b,
		&session,
		"CREATE user:three SET account = 'tesla', email = 'three@surrealdb.com' RETURN NONE",
	)
	.await
	.expect_err("a write adding a key to a failed UNIQUE index must be refused on every node");
	assert!(
		refused.to_string().contains("`test`") && refused.to_string().contains("failed to build"),
		"unexpected refusal: {refused}"
	);
	// A write that adds no key to the index is still admitted.
	execute_all_retrying_conflicts(
		&ds_b,
		&session,
		"CREATE user:four SET note = 'no key' RETURN NONE",
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

/// Commits `txn` so that it applies and then reports its outcome unknown,
/// running its rollback actions as such a commit does, with `between` run in
/// between, once the commit has applied, when given.
#[cfg(feature = "kv-mem")]
async fn commit_with_an_unknown_outcome(
	ds: &Arc<Datastore>,
	session: &Session,
	txn: &crate::kvs::Transaction,
	between: Option<&'static str>,
) -> Result<()> {
	let between = between.map(|sql| {
		let (ds, session) = (Arc::clone(ds), session.clone());
		Box::new(move || {
			Box::pin(async move {
				execute_all(&ds, &session, sql).await.expect("the statement in between commits");
			}) as std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
		}) as surrealdb_datastore::triggers::BeforeCommitHook
	});
	ds.commit_triggers().set_next_commit_applied_unknown(between);
	let err = txn.commit().await.expect_err("the commit reports its outcome unknown");
	assert!(crate::kvs::is_indeterminate_commit(&err), "the commit's outcome is unknown: {err}");
	Ok(())
}

/// A new index's `CONCURRENTLY` build whose statement's commit applied but
/// reported its outcome unknown is not aborted by that statement's rollback
/// actions: the build goes on and publishes, and the index answers queries.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_build_whose_definition_committed_is_not_aborted_by_its_rollback() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"CREATE user:1 SET email = 'a@example.com'; CREATE user:2 SET email = 'b@example.com';",
	)
	.await?;
	let (ns, db, table) = get_table_ids(&ds, "user").await?;
	let ix = new_index_definition(&ds, ns, db, &table, "ex", &["email"], Index::Idx).await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	let (txn, completion) = start_index_build_inside_an_open_statement_transaction(
		&ds,
		&session,
		ns,
		db,
		&table,
		Arc::clone(&ix),
		false,
	)
	.await?;
	assert!(completion.is_none(), "a CONCURRENTLY build is only spawned");
	let building = local_builder_for_key(&ds, ns, db, &table, ix.index_id)
		.await?
		.expect("the build registers a local builder");

	commit_with_an_unknown_outcome(&ds, &session, &txn, None).await?;

	wait_for_finished_builder(&building).await?;
	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(state.phase, IndexBuildPhase::Online, "the build was aborted: {state:?}");
	expect_indexed_query_len(&ds, &session, "SELECT * FROM user WHERE email = 'a@example.com'", 1)
		.await
}

/// A rebuild whose statement's commit applied but reported its outcome
/// unknown is not aborted by that statement's rollback actions: the new
/// generation goes on and publishes.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_rebuild_whose_statement_committed_is_not_aborted_by_its_rollback() -> Result<()> {
	assert_a_committed_rebuild_is_not_aborted(None).await
}

/// The rebuild of [`a_rebuild_whose_statement_committed_is_not_aborted_by_its_rollback`]
/// keeps its build when another index statement on the table commits between
/// its own commit and its rollback actions, as one may while a commit's outcome
/// is unknown: that statement rewrites the table's index-cache stamp, which is
/// no record of whether the rebuild committed.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_committed_rebuild_is_not_aborted_after_a_later_index_statement() -> Result<()> {
	assert_a_committed_rebuild_is_not_aborted(Some("DEFINE INDEX other ON user FIELDS name")).await
}

/// Rebuild `user.test` in an open statement transaction, and commit it so that
/// it applies, `between` commits if given, and the commit then reports its
/// outcome unknown and runs the statement's rollback actions: the rebuild must
/// go on and publish.
#[cfg(feature = "kv-mem")]
async fn assert_a_committed_rebuild_is_not_aborted(between: Option<&'static str>) -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"CREATE user:1 SET email = 'a@example.com'; CREATE user:2 SET email = 'b@example.com';
		 DEFINE INDEX test ON user FIELDS email;",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "user", "test").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	let before = durable_build_state(&ds, &ikb).await?.generation;

	// The statement: it restamps the table's index cache, then builds.
	let txn = Arc::new(ds.transaction(TransactionType::Write).await?);
	let table_def =
		catch!(txn, txn.get_tb(ns, db, &table, None).await).expect("table should exist");
	let restamped = crate::catalog::TableDefinition {
		cache_indexes_ts: Uuid::now_v7(),
		..(*table_def).clone()
	};
	catch!(txn, txn.replace_tb("test", "test", &restamped).await);
	txn.clear_cache();
	let mut ctx = ds.setup_ctx()?;
	ctx.set_transaction(Arc::clone(&txn));
	let ctx = ctx.freeze();
	let builder = ctx.get_index_builder().expect("index builder should exist").clone();
	let completion = catch!(
		txn,
		builder
			.build(&ctx, ds.setup_options(&session), table_def.table_id, Arc::clone(&ix), false)
			.await
	);
	assert!(completion.is_none(), "a CONCURRENTLY build is only spawned");
	let building = local_builder_for_key(&ds, ns, db, &table, ix.index_id)
		.await?
		.expect("the build registers a local builder");

	commit_with_an_unknown_outcome(&ds, &session, &txn, between).await?;

	wait_for_finished_builder(&building).await?;
	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(state.phase, IndexBuildPhase::Online, "the rebuild was aborted: {state:?}");
	assert!(state.generation > before, "the rebuild published a generation of its own");
	expect_indexed_query_len(&ds, &session, "SELECT * FROM user WHERE email = 'a@example.com'", 1)
		.await
}

/// A commit whose outcome is unknown runs the rollback actions of a transaction
/// that may well have committed. The cleanup of an uncommitted build must then
/// leave the build alone when its definition is in the catalog: deleting its
/// build state and data would leave a committed definition read as a ready
/// index with nothing in it. Here the definition has committed and a later
/// transaction's failed commit runs the cleanup for it, as the statement's own
/// unknown-outcome commit would.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn cleanup_of_an_uncommitted_build_keeps_a_build_whose_definition_committed() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE TABLE user SCHEMALESS;
		 CREATE user:one SET email = 'one@example.com' RETURN NONE;
		 CREATE user:two SET email = 'two@example.com' RETURN NONE;
		 DEFINE INDEX test ON user FIELDS email;",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "user", "test").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	let generation = durable_build_state(&ds, &ikb).await?.generation;
	let lookup = "SELECT * FROM user WHERE email = 'one@example.com'";
	expect_indexed_query_len(&ds, &session, lookup, 1).await?;

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
			ix.index_id,
		))
		.await;
	let conflicting_tx = ds.transaction(TransactionType::Write).await?;
	(*conflicting_tx)
		.set("commit-conflict".as_bytes().into(), b"conflicting-write".as_slice())
		.await?;
	conflicting_tx.commit().await?;
	user_tx.commit().await.expect_err("the commit conflicts");

	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(
		(state.generation, state.phase),
		(generation, IndexBuildPhase::Online),
		"the build of a committed definition must keep its state"
	);
	expect_indexed_query_len(&ds, &session, lookup, 1).await
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

/// The ids `t`'s full-text index `ft` matches for `term`.
async fn fulltext_matches(ds: &Datastore, session: &Session, term: &str) -> Result<Vec<String>> {
	use surrealdb_types::ToSql;
	let sql = format!("SELECT VALUE id FROM t WITH INDEX ft WHERE content @@ '{term}' ORDER BY id");
	let mut results = ds.execute(&sql, session, None).await?;
	let value = results.remove(0).result?;
	let surrealdb_types::Value::Array(rows) = value else {
		anyhow::bail!("query returned non-array value: {value:?}");
	};
	rows.into_iter()
		.map(|row| {
			if let surrealdb_types::Value::RecordId(rid) = row {
				Ok(rid.to_sql())
			} else {
				Err(anyhow::anyhow!("query returned a non-record value: {row:?}"))
			}
		})
		.collect()
}

/// Run `stale` in a writer whose snapshot predates the final replay of a
/// full-text build of `t.content`, and require its commit to conflict.
///
/// `setup` runs before the index is defined and `queued` while the build is
/// `Building`, so the build queues it. The writer opens its transaction and
/// reads `t` before the build replays `queued`, closes and publishes; `stale`
/// then runs in it against the `Online` index. Its snapshot holds `queued` on
/// the record but none of the index state the replay wrote for it.
async fn fulltext_write_from_a_snapshot_before_the_final_replay_conflicts(
	ds: &Arc<Datastore>,
	session: &Session,
	setup: &str,
	queued: &str,
	stale: &str,
) -> Result<()> {
	execute_all(
		ds,
		session,
		&format!(
			"DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase;
			DEFINE TABLE t SCHEMALESS;
			{setup}
			DEFINE INDEX ft ON t FIELDS content FULLTEXT ANALYZER simple BM25;"
		),
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(ds, "t", "ft").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);

	// A `Building` generation whose initial scan is complete, taken over by a
	// builder this test drives by hand.
	const GENERATION: BuildGeneration = 3;
	let expired = Utc::now() - chrono::Duration::seconds(BUILD_OWNER_LEASE_SECS + 5);
	let mut state = durable_build_state_for_phase(IndexBuildPhase::Building, GENERATION, None);
	state.updated_at = expired;
	state.owner_heartbeat_at = Some(expired);
	set_durable_build_state(ds, &ikb, state).await?;
	let tx = ds.transaction(TransactionType::Write).await?;
	tx.set_key(&ikb.new_bt_key(GENERATION), &0u64).await?;
	tx.commit().await?;
	let build = new_building_for_index(ds, session, ns, db, &table, Arc::clone(&ix)).await?;
	let acquired = build
		.acquire_build_state()
		.await?
		.expect("the expired generation should be available for takeover");
	assert_eq!(acquired.generation, GENERATION);

	execute_all(ds, session, queued).await?;

	let writer = Arc::new(ds.transaction(TransactionType::Write).await?);
	for result in ds
		.run(
			crate::kvs::QueryRequest::new("SELECT * FROM t", session)
				.with_transaction(Arc::clone(&writer)),
		)
		.await?
	{
		result.result.map_err(|e| anyhow::anyhow!("{e}"))?;
	}

	let mut updates = 0;
	let mut last_check = Instant::now();
	build
		.index_appending_loop(super::replay::ReplayUntil::Drained, 1, &mut updates, &mut last_check)
		.await?;
	assert_eq!(updates, 1, "the queued write is replayed");
	build.mark_durable_closing(GENERATION).await?;
	build
		.index_appending_loop(super::replay::ReplayUntil::Drained, 1, &mut updates, &mut last_check)
		.await?;
	build.mark_durable_online(GENERATION, 1, updates, true).await?;

	for result in ds
		.run(crate::kvs::QueryRequest::new(stale, session).with_transaction(Arc::clone(&writer)))
		.await?
	{
		result.result.map_err(|e| anyhow::anyhow!("{e}"))?;
	}
	match writer.commit().await {
		Err(err) => assert!(
			is_retryable_transaction_conflict(&err),
			"the stale writer must fail with a retryable conflict, not {err:#}"
		),
		Ok(()) => panic!(
			"`{stale}` committed from a snapshot that predates the final replay; \
			 'bravo' matches {:?}",
			fulltext_matches(ds, session, "bravo").await?
		),
	}
	Ok(())
}

/// An update of a record the build indexed from the snapshot of a writer that
/// predates the replay of the record's last change.
#[tokio::test(flavor = "multi_thread")]
async fn fulltext_update_from_a_snapshot_before_the_final_replay_conflicts() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	fulltext_write_from_a_snapshot_before_the_final_replay_conflicts(
		&ds,
		&session,
		"CREATE t:1 SET content = 'alpha' RETURN NONE;",
		"UPDATE t:1 SET content = 'bravo' RETURN NONE",
		"UPDATE t:1 SET content = 'charlie' RETURN NONE",
	)
	.await?;
	execute_all(&ds, &session, "UPDATE t:1 SET content = 'charlie' RETURN NONE").await?;
	assert!(fulltext_matches(&ds, &session, "alpha").await?.is_empty());
	assert!(fulltext_matches(&ds, &session, "bravo").await?.is_empty());
	assert_eq!(fulltext_matches(&ds, &session, "charlie").await?, ["t:1"]);
	Ok(())
}

/// An update of a record whose creation the build queued, from a writer whose
/// snapshot holds the record but no doc-ID for it.
#[tokio::test(flavor = "multi_thread")]
async fn fulltext_update_of_a_queued_create_from_a_stale_snapshot_conflicts() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	fulltext_write_from_a_snapshot_before_the_final_replay_conflicts(
		&ds,
		&session,
		"CREATE t:1 SET content = 'alpha' RETURN NONE;",
		"CREATE t:2 SET content = 'bravo' RETURN NONE",
		"UPDATE t:2 SET content = 'charlie' RETURN NONE",
	)
	.await?;
	execute_all(&ds, &session, "UPDATE t:2 SET content = 'charlie' RETURN NONE").await?;
	assert!(fulltext_matches(&ds, &session, "bravo").await?.is_empty());
	assert_eq!(fulltext_matches(&ds, &session, "charlie").await?, ["t:2"]);
	Ok(())
}

/// A delete of a record whose creation the build queued, from a writer whose
/// snapshot holds no doc-ID for it. Committed, it would leave the record's
/// mapping and postings behind, and the record created again under the same id
/// would adopt the doc-ID and match the deleted content's terms.
#[tokio::test(flavor = "multi_thread")]
async fn fulltext_delete_of_a_queued_create_from_a_stale_snapshot_conflicts() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	fulltext_write_from_a_snapshot_before_the_final_replay_conflicts(
		&ds,
		&session,
		"CREATE t:1 SET content = 'alpha' RETURN NONE;",
		"CREATE t:2 SET content = 'bravo' RETURN NONE",
		"DELETE t:2 RETURN NONE",
	)
	.await?;
	execute_all(&ds, &session, "DELETE t:2 RETURN NONE").await?;
	execute_all(&ds, &session, "CREATE t:2 SET content = 'charlie' RETURN NONE").await?;
	assert!(fulltext_matches(&ds, &session, "bravo").await?.is_empty());
	assert_eq!(fulltext_matches(&ds, &session, "charlie").await?, ["t:2"]);
	assert_eq!(fulltext_matches(&ds, &session, "alpha").await?, ["t:1"]);
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
		// The build folds nothing; no background task runs here, so the
		// compaction a deployment runs in the background runs here.
		Datastore::index_compaction(
			Arc::clone(&ds),
			Duration::from_secs(1),
			CancellationToken::new(),
		)
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

/// Records changed twice while background compaction is folding pendings into
/// the graph: every pass applies its plan and then fails its commit, so it is
/// re-planned from the pendings as they stand, until the conflicts stop. Each
/// record keeps a single pending entry however often it changes, and the pass
/// that finally commits leaves the graph answering for the final rows.
#[cfg(feature = "kv-mem")]
async fn assert_compaction_folds_records_modified_twice(
	define: &str,
	site: RetryableConflictSite,
) -> Result<()> {
	let (ds_a, ds_b, session) = new_distributed_index_test_ds().await?;
	// The pass under test folds what a build's scan queues, which the DiskANN sink would not.
	let _sink_ds_a = super::kind::test_sink::off(ds_a.id());
	let _sink_ds_b = super::kind::test_sink::off(ds_b.id());
	let seed: String = TWICE_MODIFIED_SEED
		.iter()
		.map(|(id, value)| format!("CREATE t:{id} SET f = {} RETURN NONE;", point(value)))
		.collect();
	execute_all(&ds_a, &session, &format!("DEFINE TABLE t SCHEMALESS; {seed}")).await?;
	execute_all(&ds_a, &session, define).await?;
	wait_for_index_ready(&ds_a, &session, "t", "ix").await?;
	let node_id = ds_a.id();
	let guard = inject_retryable_conflicts(site, node_id, REPEATED_RETRY_CONFLICTS);
	let stop = CancellationToken::new();
	let compactor = tokio::spawn({
		let (ds_a, stop) = (Arc::clone(&ds_a), stop.clone());
		async move {
			while !stop.is_cancelled() {
				let _ = Datastore::index_compaction(
					Arc::clone(&ds_a),
					Duration::from_secs(1),
					stop.clone(),
				)
				.await;
				sleep(Duration::from_millis(10)).await;
			}
		}
	});
	wait_for_retry_conflict(site, node_id, REPEATED_RETRY_CONFLICTS).await?;
	for step in TWICE_MODIFIED_STEPS {
		execute_all_retrying_conflicts(&ds_b, &session, &spell(step, point)).await?;
	}
	assert!(
		retryable_conflict_count(site, node_id) > 0,
		"compaction must still be retrying when the last write commits"
	);
	drop(guard);
	stop.cancel();
	compactor.await?;
	Datastore::index_compaction(
		Arc::clone(&ds_a),
		Duration::from_secs(1),
		CancellationToken::new(),
	)
	.await?;
	wait_for_index_drained(&ds_a, &session, "ix").await?;
	let history: Vec<String> = TWICE_MODIFIED_HISTORY.iter().map(|name| point(name)).collect();
	let finals: Vec<&str> = TWICE_MODIFIED_FINAL.iter().map(|(id, _)| *id).collect();
	assert_nearest_neighbours_match_brute_force(
		&ds_a,
		&session,
		"after compaction",
		&history,
		&finals,
	)
	.await
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn hnsw_compaction_folds_records_modified_twice() -> Result<()> {
	assert_compaction_folds_records_modified_twice(
		"DEFINE INDEX ix ON t FIELDS f HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32 EFC 16 M 4 CONCURRENTLY",
		RetryableConflictSite::HnswCompaction,
	)
	.await
}

#[cfg(all(feature = "kv-mem", diskann))]
#[tokio::test(flavor = "multi_thread")]
async fn diskann_compaction_folds_records_modified_twice() -> Result<()> {
	assert_compaction_folds_records_modified_twice(
		"DEFINE INDEX ix ON t FIELDS f DISKANN DIMENSION 2 DIST EUCLIDEAN TYPE F32 CONCURRENTLY",
		RetryableConflictSite::DiskAnnCompaction,
	)
	.await
}

/// A storage engine a reproduction opens, at a fresh location that lives as
/// long as the datastore does.
#[cfg(feature = "kv-mem")]
struct StorageEngine {
	/// The name failures are reported under.
	label: &'static str,
	/// The path the datastore is opened at.
	path: String,
	#[cfg(feature = "kv-surrealkv")]
	_dir: Option<TempDir>,
}

/// Every storage engine this build carries, each at a fresh location. Which
/// reads an engine validates at commit decides whether a write racing a build
/// can slip past it, so the reproductions below run on all of them.
#[cfg(feature = "kv-mem")]
fn storage_engines() -> Result<Vec<StorageEngine>> {
	#[allow(unused_mut)]
	let mut engines = vec![StorageEngine {
		label: "memory",
		path: "memory".to_owned(),
		#[cfg(feature = "kv-surrealkv")]
		_dir: None,
	}];
	#[cfg(feature = "kv-surrealkv")]
	{
		let dir = TempDir::new()?;
		engines.push(StorageEngine {
			label: "surrealkv",
			path: format!("surrealkv://{}", dir.path().to_string_lossy()),
			_dir: Some(dir),
		});
	}
	Ok(engines)
}

/// Two nodes sharing one datastore opened at `path`, as
/// [`new_distributed_index_test_ds`] builds them in memory.
#[cfg(feature = "kv-mem")]
async fn new_distributed_index_test_ds_at(
	path: &str,
) -> Result<(Arc<Datastore>, Datastore, Session)> {
	let ds_a = Datastore::builder().without_maintenance_tasks().build_with_path(path).await?;
	let session = Session::owner().with_ns("test").with_db("test");
	let tx = ds_a.transaction(TransactionType::Write).await?;
	tx.ensure_ns_db(None, "test", "test").await?;
	tx.commit().await?;
	let ds_b = ds_a.fork_for_test_with_node_id(Uuid::new_v4());
	ds_a.insert_node().await?;
	ds_b.insert_node().await?;
	Ok((ds_a, ds_b, session))
}

/// Defines `t` and the analyzer a full-text `ix` uses, and seeds `t` from
/// [`TWICE_MODIFIED_SEED`]: five records, which the first initial-scan batch
/// reads all of.
#[cfg(feature = "kv-mem")]
async fn seed_raced_table(ds: &Datastore, session: &Session) -> Result<()> {
	let seed: String = TWICE_MODIFIED_SEED
		.iter()
		.map(|(id, value)| format!("CREATE t:{id} SET f = '{value}' RETURN NONE;"))
		.collect();
	execute_all(
		ds,
		session,
		&format!(
			"DEFINE TABLE t SCHEMALESS; DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase; {seed}"
		),
	)
	.await
}

/// A `count()` query's total. A query that groups no rows answers with no row
/// at all, which is a total of zero.
#[cfg(feature = "kv-mem")]
async fn count_rows(ds: &Datastore, session: &Session, sql: &str) -> Result<i64> {
	let rows = query_json(ds, session, sql).await?;
	if rows.as_array().is_some_and(Vec::is_empty) {
		return Ok(0);
	}
	rows.pointer("/0/count")
		.and_then(serde_json::Value::as_i64)
		.ok_or_else(|| anyhow::anyhow!("{sql} answered without a count: {rows}"))
}

/// The key base of the index a build running on `ds` is building, found
/// through the node's local builder: a blocking statement has not committed
/// the definition the catalog would find it by.
#[cfg(feature = "kv-mem")]
async fn running_build_key_base(ds: &Datastore, table: &str, index: &str) -> Result<IndexKeyBase> {
	let ctx = ds.setup_ctx()?;
	let builder = ctx.get_index_builder().expect("the datastore has an index builder");
	let indexes = builder.indexes.read().await;
	indexes
		.values()
		.find(|building| {
			building.ix.table_name.as_str() == table && building.ix.name.as_str() == index
		})
		.map(|building| building.ikb.clone())
		.ok_or_else(|| anyhow::anyhow!("no local build of {index} on {table}"))
}

/// An index kind a write can go missing from, and the question that shows it.
#[cfg(feature = "kv-mem")]
#[derive(Clone, Copy, Debug)]
enum RacedIndexKind {
	/// An equality lookup through a standard index.
	Standard,
	/// The table count a COUNT index answers. It answers `count()` over the
	/// whole table with or without `WITH NOINDEX`, so the scan side counts the
	/// rows by length instead.
	Count,
	/// The count query a conditional COUNT index answers.
	CountWhere,
	/// A term match through a full-text index.
	FullText,
}

#[cfg(feature = "kv-mem")]
impl RacedIndexKind {
	const ALL: [Self; 4] = [Self::Standard, Self::Count, Self::CountWhere, Self::FullText];

	/// The definition of `ix` on `t`, without `CONCURRENTLY`.
	fn define(self) -> &'static str {
		match self {
			Self::Standard => "DEFINE INDEX ix ON t FIELDS f",
			Self::Count => "DEFINE INDEX ix ON t COUNT",
			Self::CountWhere => "DEFINE INDEX ix ON t COUNT WHERE f = 'v1'",
			Self::FullText => "DEFINE INDEX ix ON t FIELDS f FULLTEXT ANALYZER simple BM25",
		}
	}

	/// The published index's answer, then a table scan's answer to the same
	/// question.
	async fn answers(
		self,
		ds: &Datastore,
		session: &Session,
	) -> Result<(serde_json::Value, serde_json::Value)> {
		const HOLDERS: &str =
			"SELECT VALUE record::id(id) FROM t WITH NOINDEX WHERE f = 'v1' ORDER BY id";
		Ok(match self {
			Self::Standard => (
				query_json(ds, session, &HOLDERS.replace("WITH NOINDEX", "WITH INDEX ix")).await?,
				query_json(ds, session, HOLDERS).await?,
			),
			Self::Count => (
				count_rows(ds, session, "SELECT count() FROM t GROUP ALL").await?.into(),
				query_json(ds, session, "RETURN array::len(SELECT VALUE id FROM t WITH NOINDEX)")
					.await?,
			),
			Self::CountWhere => (
				count_rows(ds, session, "SELECT count() FROM t WHERE f = 'v1' GROUP ALL")
					.await?
					.into(),
				query_json(
					ds,
					session,
					"RETURN array::len(SELECT VALUE id FROM t WITH NOINDEX WHERE f = 'v1')",
				)
				.await?,
			),
			Self::FullText => (
				query_json(
					ds,
					session,
					"SELECT VALUE record::id(id) FROM t WITH INDEX ix WHERE f @@ 'v1' ORDER BY id",
				)
				.await?,
				query_json(ds, session, HOLDERS).await?,
			),
		})
	}
}

/// A build of `ix` on `t`, held in its initial scan while its first batch
/// commits: every record that batch read has been read, and no later one has.
/// The first batch holds sixteen records, so on a smaller table it is every
/// record. The statement that started the build runs in a task of its own, so
/// a blocking statement is held with its build, its definition still
/// uncommitted.
#[cfg(feature = "kv-mem")]
struct HeldScan {
	statement: tokio::task::JoinHandle<Result<()>>,
	guard: RetryableConflictGuard,
	node_id: Uuid,
	/// The build's index, found through the running build rather than the
	/// catalog, which a blocking statement has not committed to yet.
	ikb: IndexKeyBase,
}

#[cfg(feature = "kv-mem")]
impl HeldScan {
	/// Runs `sql`, which starts a build of `ix`, on `ds`, and returns once the
	/// build is held.
	async fn start(ds: &Arc<Datastore>, session: &Session, sql: &str) -> Result<Self> {
		let site = BuildPause::InScanBatch.site();
		let node_id = ds.id();
		let guard = inject_retryable_conflicts(site, node_id, REPEATED_RETRY_CONFLICTS);
		let statement = tokio::spawn({
			let ds = Arc::clone(ds);
			let session = session.clone();
			let sql = sql.to_owned();
			async move { execute_all(&ds, &session, &sql).await }
		});
		wait_for_retry_conflict(site, node_id, REPEATED_RETRY_CONFLICTS).await?;
		let ikb = running_build_key_base(ds, "t", "ix").await?;
		Ok(Self {
			statement,
			guard,
			node_id,
			ikb,
		})
	}

	/// Releases the build, then waits for the statement that started it and
	/// for the index to publish.
	async fn finish(self, ds: &Datastore, session: &Session) -> Result<()> {
		assert!(
			retryable_conflict_count(BuildPause::InScanBatch.site(), self.node_id) > 0,
			"the build must still be held when the last write commits"
		);
		drop(self.guard);
		timeout(Duration::from_secs(30), self.statement).await.map_err(|_| {
			anyhow::anyhow!("timed out waiting for the statement that started the build")
		})???;
		wait_for_index_ready(ds, session, "t", "ix").await
	}
}

/// Writes another node makes to `t` once a build's initial scan has read every
/// record: a scanned record's indexed value changes, two scanned records go,
/// and a record appears where the scan has already passed.
#[cfg(feature = "kv-mem")]
const WRITES_BEHIND_THE_SCAN: [&str; 4] = [
	"UPDATE t:twice SET f = 'v1' RETURN NONE",
	"DELETE t:gone RETURN NONE",
	"DELETE t:back RETURN NONE",
	"CREATE t:fresh SET f = 'v1' RETURN NONE",
];

/// A blocking `DEFINE INDEX` publishes an index that answers as a table scan
/// does, whatever another node writes to the table while the statement builds
/// it. A record the initial scan has already read and that is then changed or
/// removed, and a record created behind the scan, all reach the index. Checked
/// for every kind such a write can go missing from, on every storage engine.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn blocking_define_index_misses_writes_to_scanned_records() -> Result<()> {
	let mut wrong = Vec::new();
	for kind in RacedIndexKind::ALL {
		for engine in storage_engines()? {
			let (ds_a, ds_b, session) = new_distributed_index_test_ds_at(&engine.path).await?;
			seed_raced_table(&ds_a, &session).await?;
			let held = HeldScan::start(&ds_a, &session, kind.define()).await?;
			for write in WRITES_BEHIND_THE_SCAN {
				execute_all_retrying_conflicts(&ds_b, &session, write).await?;
			}
			let queued = durable_appending_count(&ds_a, &held.ikb).await?;
			held.finish(&ds_a, &session).await?;
			let (indexed, scanned) = kind.answers(&ds_a, &session).await?;
			if indexed != scanned {
				wrong.push(format!(
					"{kind:?} on {}: the index answers {indexed} where a scan answers {scanned}; \
					 {queued} of the {} writes were queued for the build",
					engine.label,
					WRITES_BEHIND_THE_SCAN.len()
				));
			}
			ds_a.shutdown().await?;
		}
	}
	assert!(wrong.is_empty(), "\n{}", wrong.join("\n"));
	Ok(())
}

/// A new index's build publishes an index that answers as a table scan does, including for a
/// transaction that began before the definition and writes records once the definition has
/// committed. That transaction neither maintains nor queues for an index its snapshot does not
/// hold, so the build reads only once it has ended, and it commits. Checked for `CONCURRENTLY`
/// and for a definition inside `BEGIN … COMMIT`, whose build reads inside the defining
/// transaction, for every kind such a write can go missing from, on every storage engine.
///
/// The table already carries a standard index on another field, as a table being given one more
/// usually does. That index takes doc-IDs from the table, so removing a record does not re-read
/// the table definition under a lock, which would otherwise refuse the older delete on its own.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_define_index_misses_writes_from_an_older_snapshot() -> Result<()> {
	let mut wrong = Vec::new();
	for form in ["{} CONCURRENTLY", "BEGIN; {}; COMMIT;"] {
		for kind in RacedIndexKind::ALL {
			for engine in storage_engines()? {
				let (ds_a, ds_b, session) = new_distributed_index_test_ds_at(&engine.path).await?;
				seed_raced_table(&ds_a, &session).await?;
				execute_all(&ds_a, &session, "DEFINE INDEX other ON t FIELDS g").await?;
				// Begun before the definition exists, so its snapshot never holds it.
				let older = Arc::new(ds_b.transaction(TransactionType::Write).await?);
				let define = form.replace("{}", kind.define());
				let waited = super::builder::write_barrier_waits::count(ds_a.id());
				let statement = tokio::spawn({
					let (ds, session) = (Arc::clone(&ds_a), session.clone());
					async move { execute_all(&ds, &session, &define).await }
				});
				wait_for_a_build_at_the_write_barrier(ds_a.id(), waited).await?;
				for write in WRITES_BEHIND_THE_SCAN {
					let request =
						QueryRequest::new(write, &session).with_transaction(Arc::clone(&older));
					for result in ds_b.run(request).await? {
						result.result?;
					}
				}
				if let Err(err) = older.commit().await {
					wrong.push(format!(
						"`{form}` with {kind:?} on {}: the transaction the build waited for did \
						 not commit: {err}",
						engine.label
					));
				}
				timeout(Duration::from_secs(30), statement)
					.await
					.map_err(|_| anyhow::anyhow!("timed out waiting for the definition"))???;
				wait_for_index_ready(&ds_a, &session, "t", "ix").await?;
				let (indexed, scanned) = kind.answers(&ds_a, &session).await?;
				if indexed != scanned {
					wrong.push(format!(
						"`{form}` with {kind:?} on {}: the index answers {indexed} where a scan \
						 answers {scanned}",
						engine.label
					));
				}
				ds_a.shutdown().await?;
			}
		}
	}
	assert!(wrong.is_empty(), "\n{}", wrong.join("\n"));
	Ok(())
}

/// [`WRITES_BEHIND_THE_SCAN`], made one after another by a task of its own, each made again when
/// it meets a transaction conflict.
#[cfg(feature = "kv-mem")]
fn spawn_writes_behind_the_scan(
	ds: &Arc<Datastore>,
	session: &Session,
) -> tokio::task::JoinHandle<Result<()>> {
	tokio::spawn({
		let (ds, session) = (Arc::clone(ds), session.clone());
		async move {
			for write in WRITES_BEHIND_THE_SCAN {
				execute_all_retrying_conflicts(&ds, &session, write).await?;
			}
			Ok(())
		}
	})
}

/// How long a write that is held back is given to commit before it is taken to be waiting.
#[cfg(feature = "kv-mem")]
const HELD_WRITE_GRACE: Duration = Duration::from_millis(400);

/// A `DEFINE INDEX` inside `BEGIN … COMMIT` publishes an index that answers as a table scan does,
/// whatever another session on the same node writes while the transaction is open. The build reads
/// before the transaction commits, so a write that opens after the build began waiting for the
/// writes older than itself cannot see the index, and would be left out of it: it waits for the
/// transaction to end, is refused as a conflict once that has committed, and its retry sees the
/// index. Checked for every kind such a write can go missing from.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn define_index_in_a_transaction_holds_back_a_concurrent_sessions_writes() -> Result<()> {
	let mut wrong = Vec::new();
	for kind in RacedIndexKind::ALL {
		let (ds, session) = new_index_test_ds().await?;
		seed_raced_table(&ds, &session).await?;
		let sql = format!("BEGIN; {}; COMMIT;", kind.define());
		let held = HeldScan::start(&ds, &session, &sql).await?;
		let writes = spawn_writes_behind_the_scan(&ds, &session);
		sleep(HELD_WRITE_GRACE).await;
		assert!(
			!held.statement.is_finished(),
			"the defining transaction must still be open when the writes would commit"
		);
		if writes.is_finished() {
			wrong.push(format!(
				"{kind:?}: the writes committed while the defining transaction was open"
			));
		}
		held.finish(&ds, &session).await?;
		timeout(Duration::from_secs(30), writes)
			.await
			.map_err(|_| anyhow::anyhow!("timed out waiting for the held back writes"))???;
		let (indexed, scanned) = kind.answers(&ds, &session).await?;
		if indexed != scanned {
			wrong.push(format!(
				"{kind:?}: the index answers {indexed} where a scan answers {scanned}"
			));
		}
		ds.shutdown().await?;
	}
	assert!(wrong.is_empty(), "\n{}", wrong.join("\n"));
	Ok(())
}

/// A write held back by a definition whose transaction is then rolled back goes on and commits:
/// the index it could not see does not exist, so it is left out of nothing.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_write_held_back_by_a_definition_that_rolls_back_commits() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	seed_raced_table(&ds, &session).await?;
	let HeldScan {
		statement,
		guard,
		..
	} = HeldScan::start(&ds, &session, "BEGIN; DEFINE INDEX ix ON t FIELDS f; CANCEL;").await?;
	let writes = spawn_writes_behind_the_scan(&ds, &session);
	sleep(HELD_WRITE_GRACE).await;
	assert!(!writes.is_finished(), "the writes wait for the defining transaction");
	drop(guard);
	let cancelled = timeout(Duration::from_secs(30), statement)
		.await
		.map_err(|_| anyhow::anyhow!("timed out waiting for the transaction to be cancelled"))??;
	if let Err(err) = cancelled {
		assert!(err.to_string().contains("cancelled transaction"), "{err}");
	}
	timeout(Duration::from_secs(30), writes)
		.await
		.map_err(|_| anyhow::anyhow!("timed out waiting for the held back writes"))???;
	let records =
		query_json(&ds, &session, "SELECT VALUE record::id(id) FROM t ORDER BY id").await?;
	let records: Vec<_> =
		records.as_array().into_iter().flatten().filter_map(|id| id.as_str()).collect();
	assert!(records.contains(&"fresh"), "{records:?}");
	assert!(!records.contains(&"gone") && !records.contains(&"back"), "{records:?}");
	ds.shutdown().await?;
	Ok(())
}

/// A definition inside a transaction holds back the writes of its table only: another table's
/// writes, and every read, go on while it is open.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_definition_in_a_transaction_holds_back_only_the_writes_of_its_table() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	seed_raced_table(&ds, &session).await?;
	execute_all(&ds, &session, "DEFINE TABLE other SCHEMALESS").await?;
	let held =
		HeldScan::start(&ds, &session, "BEGIN; DEFINE INDEX ix ON t FIELDS f; COMMIT;").await?;
	let others = timeout(Duration::from_secs(10), async {
		execute_all(&ds, &session, "CREATE other:1 SET f = 'v1' RETURN NONE").await?;
		execute_all(&ds, &session, "SELECT * FROM t").await?;
		execute_all(&ds, &session, "SELECT * FROM other").await
	})
	.await
	.map_err(|_| anyhow::anyhow!("a statement waited for a definition of another table"))?;
	assert!(!held.statement.is_finished(), "the defining transaction must still be open");
	held.finish(&ds, &session).await?;
	others?;
	ds.shutdown().await?;
	Ok(())
}

/// A write that waits for a definition is refused as a retryable conflict once it has waited the
/// bound, though the definition's transaction is still open, rather than holding the table's writes
/// back for as long as a client leaves that transaction open.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_write_waits_for_a_definition_in_a_transaction_for_a_bounded_time() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	ds.transaction_factory().live_writes().set_definition_hold_wait(Duration::from_millis(300));
	seed_raced_table(&ds, &session).await?;
	let held =
		HeldScan::start(&ds, &session, "BEGIN; DEFINE INDEX ix ON t FIELDS f; COMMIT;").await?;
	let began = Instant::now();
	let err = execute_all(&ds, &session, "UPDATE t:twice SET f = 'v1' RETURN NONE")
		.await
		.expect_err("the definition's transaction stays open");
	assert!(began.elapsed() >= Duration::from_millis(300), "{:?}", began.elapsed());
	assert!(is_retryable_statement_conflict(&err), "{err}");
	assert!(err.to_string().contains("stayed open"), "{err}");
	assert!(!held.statement.is_finished(), "the defining transaction must still be open");
	held.finish(&ds, &session).await?;
	execute_all(&ds, &session, "UPDATE t:twice SET f = 'v1' RETURN NONE").await?;
	ds.shutdown().await?;
	Ok(())
}

/// Records `t:r00` to `t:r39`, each holding its own value of `f`: more than the
/// sixteen an initial scan's first batch reads, so a record sorting after them
/// is only read by a later batch.
#[cfg(feature = "kv-mem")]
const REBUILT_RECORDS: usize = 40;

/// Defines `t`, seeds it with [`REBUILT_RECORDS`] records, and builds `define`,
/// an index named `ix`, over them, returning once it is online, so that every
/// later write maintains it directly.
#[cfg(feature = "kv-mem")]
async fn seed_online_index(ds: &Datastore, session: &Session, define: &str) -> Result<()> {
	let seed: String = (0..REBUILT_RECORDS)
		.map(|i| format!("CREATE t:r{i:02} SET f = 'v{i:02}' RETURN NONE;"))
		.collect();
	execute_all(ds, session, &format!("DEFINE TABLE t SCHEMALESS; {seed} {define};")).await?;
	wait_for_index_ready(ds, session, "t", "ix").await
}

/// An index kind whose entries a rebuild can apply twice: one whose writes are
/// appended under keys of their own, or inserted only where no entry exists,
/// so a second application neither overwrites nor collides with the first.
#[cfg(feature = "kv-mem")]
#[derive(Clone, Copy, Debug)]
enum RebuiltIndexKind {
	/// The table count a COUNT index answers, against the rows.
	Count,
	/// A lookup of a written value through a UNIQUE index, against a scan.
	Unique,
}

#[cfg(feature = "kv-mem")]
impl RebuiltIndexKind {
	/// The definition of `ix` on `t`.
	fn define(self) -> &'static str {
		match self {
			Self::Count => "DEFINE INDEX ix ON t COUNT",
			Self::Unique => "DEFINE INDEX ix ON t FIELDS f UNIQUE",
		}
	}

	/// The rebuilt index's answer, then the answer it has to match.
	async fn answers(
		self,
		ds: &Datastore,
		session: &Session,
	) -> Result<(serde_json::Value, serde_json::Value)> {
		Ok(match self {
			Self::Count => (
				count_rows(ds, session, "SELECT count() FROM t GROUP ALL").await?.into(),
				query_json(ds, session, "RETURN array::len(SELECT VALUE id FROM t WITH NOINDEX)")
					.await?,
			),
			Self::Unique => {
				let lookup = "SELECT VALUE record::id(id) FROM t WITH INDEX ix WHERE f = 'vzzz'";
				(
					query_json(ds, session, lookup).await?,
					query_json(ds, session, &lookup.replace("WITH INDEX ix", "WITH NOINDEX"))
						.await?,
				)
			}
		})
	}
}

/// Writes a rebuild of `ix` never waits for: begun while `ix` is online, so
/// each maintains the index in its own transaction and holds no reservation.
/// One creates a record sorting after every other; one deletes a record past
/// the scan's first batch.
#[cfg(feature = "kv-mem")]
const DIRECT_WRITES: [&str; 2] =
	["CREATE t:zzz SET f = 'vzzz' RETURN NONE", "DELETE t:r39 RETURN NONE"];

/// Waits until a build on `node` has found write transactions older than its commit still open
/// at the write barrier more than `after` times.
#[cfg(feature = "kv-mem")]
async fn wait_for_a_build_at_the_write_barrier(node: Uuid, after: usize) -> Result<()> {
	timeout(Duration::from_secs(20), async {
		while super::builder::write_barrier_waits::count(node) <= after {
			sleep(Duration::from_millis(10)).await;
		}
	})
	.await
	.map_err(|_| anyhow::anyhow!("no build waited at the write barrier"))
}

/// `REBUILD INDEX` publishes an index that holds each record once when a write that maintains the
/// index directly is still open as the rebuild installs its generation. The write was admitted
/// while the index was online, so it holds no reservation, and what it stored would survive the
/// wipe for the scan to index a second time; the rebuild therefore wipes and scans only once that
/// write has ended, and the write commits. Checked for the statement on its own, `CONCURRENTLY`
/// and inside `BEGIN … COMMIT`, for every kind whose entries would double — a UNIQUE index
/// included, whose rebuild would otherwise fail on the record it races — on every storage engine,
/// and for an index with no durable build state, as one defined before that state existed, which
/// the rebuild creates.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn rebuild_index_double_applies_direct_writes_that_outlive_its_wipe() -> Result<()> {
	let [create, delete] = DIRECT_WRITES;
	let cases = [
		(RebuiltIndexKind::Count, create),
		(RebuiltIndexKind::Count, delete),
		(RebuiltIndexKind::Unique, create),
	];
	let mut wrong = Vec::new();
	for rebuild in [
		"REBUILD INDEX ix ON t CONCURRENTLY",
		"REBUILD INDEX ix ON t",
		"BEGIN; REBUILD INDEX ix ON t; COMMIT;",
	] {
		for ((kind, write), legacy) in cases.into_iter().flat_map(|c| [(c, false), (c, true)]) {
			for engine in storage_engines()? {
				let (ds_a, ds_b, session) = new_distributed_index_test_ds_at(&engine.path).await?;
				seed_online_index(&ds_a, &session, kind.define()).await?;
				if legacy {
					let (ns, db, table, ix) = get_table_index(&ds_a, "t", "ix").await?;
					let ikb = IndexKeyBase::new(ns, db, table, ix.index_id);
					let tx = ds_a.transaction(TransactionType::Write).await?;
					catch!(tx, tx.del_key(&ikb.new_bs_key()).await);
					tx.commit().await?;
				}
				let kind = (kind, legacy);
				let writer = Arc::new(ds_b.transaction(TransactionType::Write).await?);
				let request =
					QueryRequest::new(write, &session).with_transaction(Arc::clone(&writer));
				for result in ds_b.run(request).await? {
					result.result?;
				}
				let waited = super::builder::write_barrier_waits::count(ds_a.id());
				let statement = tokio::spawn({
					let (ds, session) = (Arc::clone(&ds_a), session.clone());
					async move { execute_all(&ds, &session, rebuild).await }
				});
				wait_for_a_build_at_the_write_barrier(ds_a.id(), waited).await?;
				match writer.commit().await {
					Ok(()) => {}
					// A delete re-reads the table's definition under a lock, which the rebuild's
					// rewrite of that definition refuses; the client retries, and it is queued.
					Err(err) if write == delete && is_retryable_transaction_conflict(&err) => {
						execute_all_retrying_conflicts(&ds_b, &session, write).await?;
					}
					Err(err) => wrong.push(format!(
						"`{rebuild}` with {kind:?} after `{write}` on {}: the write the rebuild \
						 waited for did not commit: {err}",
						engine.label
					)),
				}
				let built = async {
					timeout(Duration::from_secs(30), statement)
						.await
						.map_err(|_| anyhow::anyhow!("timed out waiting for the rebuild"))???;
					wait_for_index_ready(&ds_a, &session, "t", "ix").await
				};
				let observed = match built.await {
					Err(err) => Some(format!("the rebuild did not publish: {err}")),
					Ok(()) => {
						let (rebuilt, expected) = kind.0.answers(&ds_a, &session).await?;
						(rebuilt != expected).then(|| {
							format!("the index answers {rebuilt} where {expected} is expected")
						})
					}
				};
				if let Some(observed) = observed {
					wrong.push(format!(
						"`{rebuild}` with {kind:?} after `{write}` on {}: {observed}",
						engine.label
					));
				}
				ds_a.shutdown().await?;
			}
		}
	}
	assert!(wrong.is_empty(), "\n{}", wrong.join("\n"));
	Ok(())
}

/// Seeds `t` with a COUNT index on `ds`, registers `ds` and a second process writing the same
/// store as nodes, and answers that second node's write barriers in the background until the
/// returned token is cancelled. Both leave their writes unfenced against catalog changes, as on a
/// store whose builds rely on the other nodes' answers.
#[cfg(feature = "kv-mem")]
async fn two_nodes_with_a_count_index()
-> Result<(Arc<Datastore>, Arc<Datastore>, Session, CancellationToken)> {
	let (ds_a, session) = new_index_test_ds().await?;
	let ds_b = Arc::new(ds_a.fork_for_test_as_another_process(Uuid::new_v4()));
	ds_a.assume_unfenced_catalog_for_test();
	ds_b.assume_unfenced_catalog_for_test();
	// Both answer write barriers — node B below, node A where a test answers for it — so both
	// record that they do as they register.
	ds_a.begin_answering_write_barriers();
	ds_b.begin_answering_write_barriers();
	ds_a.insert_node().await?;
	ds_b.insert_node().await?;
	let answering = CancellationToken::new();
	tokio::spawn({
		let (ds, answering) = (Arc::clone(&ds_b), answering.clone());
		async move {
			while !answering.is_cancelled() {
				let _ = ds.answer_write_barrier(&answering).await;
				sleep(Duration::from_millis(20)).await;
			}
		}
	});
	execute_all(
		&ds_a,
		&session,
		"DEFINE TABLE t SCHEMALESS; CREATE t:1; CREATE t:2; DEFINE INDEX ix ON t COUNT;",
	)
	.await?;
	wait_for_index_ready(&ds_a, &session, "t", "ix").await?;
	Ok((ds_a, ds_b, session, answering))
}

/// The index's table count, and the count of its rows.
#[cfg(feature = "kv-mem")]
async fn count_against_rows(ds: &Datastore, session: &Session) -> Result<(i64, i64)> {
	Ok((
		count_rows(ds, session, "SELECT count() FROM t GROUP ALL").await?,
		query_json(ds, session, "RETURN array::len(SELECT VALUE id FROM t WITH NOINDEX)")
			.await?
			.as_i64()
			.unwrap_or_default(),
	))
}

/// On a store several nodes write, a rebuild on one node reads only once every other live node
/// has answered its write barrier, which a node does only once its own write transactions older
/// than the barrier have ended: a write another node holds open across the rebuild commits, and
/// is counted once.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_rebuild_waits_for_another_nodes_older_writes() -> Result<()> {
	let (ds_a, ds_b, session, answering) = two_nodes_with_a_count_index().await?;
	let writer = Arc::new(ds_b.transaction(TransactionType::Write).await?);
	let request =
		QueryRequest::new("CREATE t:3 RETURN NONE", &session).with_transaction(Arc::clone(&writer));
	for result in ds_b.run(request).await? {
		result.result?;
	}
	let waited = super::builder::write_barrier_waits::count(ds_a.id());
	let rebuild = tokio::spawn({
		let (ds, session) = (Arc::clone(&ds_a), session.clone());
		async move { execute_all(&ds, &session, "REBUILD INDEX ix ON t").await }
	});
	wait_for_a_build_at_the_write_barrier(ds_a.id(), waited).await?;
	sleep(Duration::from_millis(300)).await;
	assert!(!rebuild.is_finished(), "the rebuild must wait for the other node's open write");
	writer.commit().await?;
	timeout(Duration::from_secs(20), rebuild)
		.await
		.map_err(|_| anyhow::anyhow!("timed out waiting for the rebuild"))???;
	wait_for_index_ready(&ds_a, &session, "t", "ix").await?;
	let (indexed, rows) = count_against_rows(&ds_a, &session).await?;
	assert_eq!(indexed, rows, "the index counts each row once");
	answering.cancel();
	Ok(())
}

/// Registers a datastore and a second process writing the same store as nodes that answer write
/// barriers, of which only the first ever answers one: the second stands for a live node whose
/// answer a build waits for and never gets.
#[cfg(feature = "kv-mem")]
async fn two_nodes_the_second_never_answering() -> Result<(Arc<Datastore>, Arc<Datastore>, Session)>
{
	let (ds_a, session) = new_index_test_ds().await?;
	let ds_b = Arc::new(ds_a.fork_for_test_as_another_process(Uuid::new_v4()));
	ds_a.begin_answering_write_barriers();
	ds_b.begin_answering_write_barriers();
	ds_a.insert_node().await?;
	ds_b.insert_node().await?;
	execute_all(&ds_a, &session, "DEFINE TABLE t SCHEMALESS; CREATE t:1 SET n = 1;").await?;
	Ok((ds_a, ds_b, session))
}

/// On a store that fences catalog changes, a new index built once its definition has committed
/// asks no other node to answer its write barrier. A write another node opened before the
/// definition and commits after it is refused at its commit, as a retryable conflict, rather than
/// waited for, and its retry is indexed.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_definition_built_after_its_commit_asks_no_other_node_on_a_fenced_store() -> Result<()> {
	let (ds_a, ds_b, session) = two_nodes_the_second_never_answering().await?;
	let writer = Arc::new(ds_b.transaction(TransactionType::Write).await?);
	let request = QueryRequest::new("CREATE t:2 SET n = 2 RETURN NONE", &session)
		.with_transaction(Arc::clone(&writer));
	for result in ds_b.run(request).await? {
		result.result?;
	}
	timeout(Duration::from_secs(20), execute_all(&ds_a, &session, "DEFINE INDEX ix ON t FIELDS n"))
		.await
		.map_err(|_| {
			anyhow::anyhow!("the build waited for a node whose writes the store fences")
		})??;
	let err =
		writer.commit().await.expect_err("a write older than the definition commits after it");
	assert!(crate::kvs::is_retryable_transaction_conflict(&err), "{err}");
	execute_all(&ds_b, &session, "CREATE t:2 SET n = 2").await?;
	wait_for_index_ready(&ds_a, &session, "t", "ix").await?;
	for n in [1, 2] {
		let sql = format!("RETURN array::len(SELECT VALUE id FROM t WITH INDEX ix WHERE n = {n})");
		assert_eq!(query_json(&ds_a, &session, &sql).await?, serde_json::json!(1), "n = {n}");
	}
	Ok(())
}

/// On a store that fences catalog changes, a new index built inside the transaction that defines
/// it still waits for every other live node to answer its write barrier: it reads before its
/// definition commits, so a write another node commits meanwhile conflicts with nothing.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_definition_built_inside_its_transaction_asks_the_other_nodes_on_a_fenced_store()
-> Result<()> {
	let (ds_a, ds_b, session) = two_nodes_the_second_never_answering().await?;
	let waited = super::builder::write_barrier_waits::count(ds_a.id());
	let define = tokio::spawn({
		let (ds, session) = (Arc::clone(&ds_a), session.clone());
		async move { execute_all(&ds, &session, "BEGIN; DEFINE INDEX ix ON t FIELDS n; COMMIT;").await }
	});
	wait_for_a_build_at_the_write_barrier(ds_a.id(), waited).await?;
	sleep(Duration::from_millis(500)).await;
	assert!(!define.is_finished(), "the build waits for the other node's answer");
	let answering = CancellationToken::new();
	tokio::spawn({
		let (ds, answering) = (Arc::clone(&ds_b), answering.clone());
		async move {
			while !answering.is_cancelled() {
				let _ = ds.answer_write_barrier(&answering).await;
				sleep(Duration::from_millis(20)).await;
			}
		}
	});
	timeout(Duration::from_secs(20), define)
		.await
		.map_err(|_| anyhow::anyhow!("timed out waiting for the definition"))???;
	answering.cancel();
	Ok(())
}

/// On a store that fences catalog changes, a definition built after its commit still waits for
/// every other live node to answer its write barrier while one of them was last refreshed by a
/// release without the fence, whose writes read catalog versions plainly: one that never marked
/// its heartbeat as fenced, and one whose heartbeat a release with the barrier but without the
/// fence recorded after the mark.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_definition_built_after_its_commit_asks_a_node_on_a_release_without_the_fence()
-> Result<()> {
	use crate::dbs::node::{Node, Timestamp};
	for case in ["never marked", "refreshed without the mark"] {
		let (ds_a, session) = new_index_test_ds().await?;
		let ds_b = Arc::new(ds_a.fork_for_test_as_another_process(Uuid::new_v4()));
		if case == "never marked" {
			ds_b.assume_unfenced_catalog_for_test();
		}
		ds_a.begin_answering_write_barriers();
		ds_b.begin_answering_write_barriers();
		ds_a.insert_node().await?;
		ds_b.insert_node().await?;
		if case == "refreshed without the mark" {
			let tx = ds_a.transaction(TransactionType::Write).await?;
			let now = crate::kvs::clock::SystemClock::new().now().value;
			let fresh = Node::new(
				ds_b.id(),
				Timestamp {
					value: now + 1,
				},
				false,
			);
			tx.set_key(
				&crate::key::schema::NodeKey {
					nd: ds_b.id(),
				},
				&fresh,
			)
			.await?;
			tx.set_key(
				&crate::key::schema::WriteBarrierHeartbeatKey {
					nd: ds_b.id(),
				},
				&fresh.heartbeat.value,
			)
			.await?;
			tx.commit().await?;
		}
		execute_all(&ds_a, &session, "DEFINE TABLE t SCHEMALESS; CREATE t:1 SET n = 1;").await?;
		let waited = super::builder::write_barrier_waits::count(ds_a.id());
		let define = tokio::spawn({
			let (ds, session) = (Arc::clone(&ds_a), session.clone());
			async move { execute_all(&ds, &session, "DEFINE INDEX ix ON t FIELDS n").await }
		});
		wait_for_a_build_at_the_write_barrier(ds_a.id(), waited).await?;
		sleep(Duration::from_millis(500)).await;
		assert!(!define.is_finished(), "{case}: the build waits for the other node's answer");
		let answering = CancellationToken::new();
		tokio::spawn({
			let (ds, answering) = (Arc::clone(&ds_b), answering.clone());
			async move {
				while !answering.is_cancelled() {
					let _ = ds.answer_write_barrier(&answering).await;
					sleep(Duration::from_millis(20)).await;
				}
			}
		});
		timeout(Duration::from_secs(20), define)
			.await
			.map_err(|_| anyhow::anyhow!("{case}: timed out waiting for the definition"))???;
		answering.cancel();
	}
	Ok(())
}

/// A rebuild does not wait for a node it cannot count on to answer: one whose heartbeat has lapsed
/// past the exclusion window, which refuses its own write commits; one that has never answered a
/// barrier; and one whose current heartbeat a release without the barrier recorded, leaving the
/// answer an earlier process of it recorded unmaintained.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_rebuild_does_not_wait_for_a_lapsed_or_older_node() -> Result<()> {
	use crate::dbs::node::{Node, Timestamp};
	for case in ["lapsed", "unanswered", "older release"] {
		let (ds_a, ds_b, session, answering) = two_nodes_with_a_count_index().await?;
		answering.cancel();
		let tx = ds_a.transaction(TransactionType::Write).await?;
		let now = crate::kvs::clock::SystemClock::new().now().value;
		let node_key = crate::key::schema::NodeKey {
			nd: ds_b.id(),
		};
		match case {
			"lapsed" => {
				let stale = Node::new(
					ds_b.id(),
					Timestamp {
						value: now.saturating_sub(90_000),
					},
					false,
				);
				tx.set_key(&node_key, &stale).await?;
			}
			"unanswered" => {
				tx.del_key(&crate::key::schema::WriteBarrierAckKey {
					nd: ds_b.id(),
				})
				.await?;
			}
			_ => {
				// A heartbeat that writes the node's row and nothing else, as an older release's.
				let fresh = Node::new(
					ds_b.id(),
					Timestamp {
						value: now + 1,
					},
					false,
				);
				tx.set_key(&node_key, &fresh).await?;
			}
		}
		tx.commit().await?;
		let writer = Arc::new(ds_b.transaction(TransactionType::Write).await?);
		timeout(Duration::from_secs(20), execute_all(&ds_a, &session, "REBUILD INDEX ix ON t"))
			.await
			.map_err(|_| anyhow::anyhow!("the rebuild waited for a node it cannot count on"))??;
		writer.cancel().await?;
	}
	Ok(())
}

/// On a store several nodes write, a transaction whose own blocking build ran while its own node
/// answered that build's barrier commits: the answer covered only barriers its own node's builds
/// raised, which wait for that node's transactions directly.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_transaction_is_not_refused_for_its_own_nodes_answer_to_its_build() -> Result<()> {
	let (ds_a, _ds_b, session, answering) = two_nodes_with_a_count_index().await?;
	tokio::spawn({
		let (ds, answering) = (Arc::clone(&ds_a), answering.clone());
		async move {
			while !answering.is_cancelled() {
				let _ = ds.answer_write_barrier(&answering).await;
				sleep(Duration::from_millis(20)).await;
			}
		}
	});
	execute_all(&ds_a, &session, "DEFINE TABLE a SCHEMALESS;").await?;
	let site = RetryableConflictSite::ConcurrentIndexInitialCleanup;
	let guard = inject_retryable_conflicts(site, ds_a.id(), REPEATED_RETRY_CONFLICTS);
	let txn = Arc::new(ds_a.transaction(TransactionType::Write).await?);
	let own = tokio::spawn({
		let (ds, session, txn) = (Arc::clone(&ds_a), session.clone(), Arc::clone(&txn));
		async move {
			let request =
				QueryRequest::new("DEFINE INDEX ia ON a FIELDS f", &session).with_transaction(txn);
			for result in ds.run(request).await? {
				result.result?;
			}
			Ok::<_, anyhow::Error>(())
		}
	});
	wait_for_retry_conflict(site, ds_a.id(), REPEATED_RETRY_CONFLICTS).await?;
	// The build is held past its wait: its own node answers the barrier it raised.
	timeout(Duration::from_secs(20), async {
		loop {
			let tx = ds_a.transaction(TransactionType::Read).await?;
			let asked = tx.get_key(&crate::key::schema::WriteBarrierKey {}, None).await?;
			let answered = tx
				.get_key(
					&crate::key::schema::WriteBarrierAckKey {
						nd: ds_a.id(),
					},
					None,
				)
				.await?;
			tx.cancel().await?;
			if asked.is_some() && answered >= asked {
				return Ok::<_, anyhow::Error>(());
			}
			sleep(Duration::from_millis(20)).await;
		}
	})
	.await
	.map_err(|_| anyhow::anyhow!("the node did not answer its own build's barrier"))??;
	drop(guard);
	timeout(Duration::from_secs(20), own)
		.await
		.map_err(|_| anyhow::anyhow!("timed out waiting for the own build"))???;
	txn.commit().await?;
	answering.cancel();
	Ok(())
}

/// Commit a client-owned transaction on node A of two that runs `writes` and then
/// `DEFINE INDEX ia ON a`, whose build is held while node B rebuilds `ix ON t`, which reads only
/// once node A has answered its write barrier. Answers the commit's outcome. The `writes` write no
/// record of `a`, as a blocking build is refused in a transaction that wrote its table's records;
/// `c` is a table they may write besides `t`.
#[cfg(feature = "kv-mem")]
async fn commit_past_another_nodes_build(writes: &str) -> Result<Result<()>> {
	let (ds_a, ds_b, session, answering) = two_nodes_with_a_count_index().await?;
	tokio::spawn({
		let (ds, answering) = (Arc::clone(&ds_a), answering.clone());
		async move {
			while !answering.is_cancelled() {
				let _ = ds.answer_write_barrier(&answering).await;
				sleep(Duration::from_millis(20)).await;
			}
		}
	});
	execute_all(&ds_a, &session, "DEFINE TABLE a SCHEMALESS; DEFINE TABLE c SCHEMALESS;").await?;
	let site = RetryableConflictSite::ConcurrentIndexInitialCleanup;
	let guard = inject_retryable_conflicts(site, ds_a.id(), REPEATED_RETRY_CONFLICTS);
	let txn = Arc::new(ds_a.transaction(TransactionType::Write).await?);
	for result in
		ds_a.run(QueryRequest::new(writes, &session).with_transaction(Arc::clone(&txn))).await?
	{
		result.result?;
	}
	let own = tokio::spawn({
		let (ds, session, txn) = (Arc::clone(&ds_a), session.clone(), Arc::clone(&txn));
		async move {
			let request =
				QueryRequest::new("DEFINE INDEX ia ON a FIELDS f", &session).with_transaction(txn);
			for result in ds.run(request).await? {
				result.result?;
			}
			Ok::<_, anyhow::Error>(())
		}
	});
	wait_for_retry_conflict(site, ds_a.id(), REPEATED_RETRY_CONFLICTS).await?;
	// Node A answers node B's barrier while the transaction is set aside for its own build.
	timeout(Duration::from_secs(20), async {
		execute_all(&ds_b, &session, "REBUILD INDEX ix ON t").await?;
		wait_for_index_ready(&ds_b, &session, "t", "ix").await
	})
	.await
	.map_err(|_| anyhow::anyhow!("timed out waiting for the other node's rebuild"))??;
	drop(guard);
	timeout(Duration::from_secs(20), own)
		.await
		.map_err(|_| anyhow::anyhow!("timed out waiting for the own build"))???;
	let committed = txn.commit().await;
	answering.cancel();
	Ok(committed)
}

/// On a store several nodes write, a transaction whose own blocking build ran while its node
/// answered another node's build's barrier commits when it writes nothing of that build's table,
/// and is refused with a retryable conflict when it does: the answer let that build read without
/// waiting for the transaction.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_transaction_another_nodes_build_passed_is_refused_only_for_its_table() -> Result<()> {
	commit_past_another_nodes_build("CREATE c:1")
		.await?
		.expect("a build of another table on another node passed it");
	let refused = commit_past_another_nodes_build("CREATE t:9")
		.await?
		.expect_err("a build on another node of a table it writes passed it");
	assert!(is_retryable_transaction_conflict(&refused), "{refused}");
	Ok(())
}

/// On a store several nodes write, a node whose heartbeat has lapsed refuses its own write
/// commits, still commits the heartbeat that ends the lapse, and after it refuses a transaction
/// that was open across the lapse, while a fresh one commits.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_node_whose_heartbeat_lapsed_refuses_the_writes_it_held_across_the_lapse() -> Result<()> {
	let (ds_a, session) = new_index_test_ds().await?;
	let ds_b = ds_a.fork_for_test_as_another_process(Uuid::new_v4());
	ds_b.insert_node().await?;
	execute_all(&ds_a, &session, "DEFINE TABLE t SCHEMALESS;").await?;
	let fence = ds_b.transaction_factory().write_fence();
	fence.heartbeat(web_time::Instant::now(), Duration::from_millis(200));
	let across = ds_b.transaction(TransactionType::Write).await?;
	sleep(Duration::from_millis(300)).await;

	let lapsed = ds_b.transaction(TransactionType::Write).await?;
	lapsed.set_key(&crate::key::schema::WriteBarrierKey {}, &7).await?;
	let refused = lapsed.commit().await.expect_err("a lapsed node commits no write");
	assert!(is_retryable_transaction_conflict(&refused), "{refused}");

	ds_b.update_node().await?;
	fence.heartbeat(web_time::Instant::now(), Duration::from_secs(10));

	across.set_key(&crate::key::schema::WriteBarrierKey {}, &8).await?;
	let refused = across.commit().await.expect_err("a write open across the lapse is refused");
	assert!(is_retryable_transaction_conflict(&refused), "{refused}");
	execute_all(&ds_b, &session, "CREATE t:1 RETURN NONE").await?;
	Ok(())
}

/// A transaction that awaited a blocking build inside it counts again among the write
/// transactions a later build waits for: a definition elsewhere, made once that build has ended,
/// waits for the transaction, which then writes to the newly indexed table and commits, and the
/// new index holds the write.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_transaction_counts_again_once_its_own_build_has_ended() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(&ds, &session, "DEFINE TABLE a SCHEMALESS; DEFINE TABLE b SCHEMALESS;").await?;
	let txn = Arc::new(ds.transaction(TransactionType::Write).await?);
	let request = QueryRequest::new("DEFINE INDEX ia ON a FIELDS f", &session)
		.with_transaction(Arc::clone(&txn));
	for result in ds.run(request).await? {
		result.result?;
	}
	let waited = super::builder::write_barrier_waits::count(ds.id());
	let define = tokio::spawn({
		let (ds, session) = (Arc::clone(&ds), session.clone());
		async move { execute_all(&ds, &session, "DEFINE INDEX ib ON b FIELDS f CONCURRENTLY").await }
	});
	wait_for_a_build_at_the_write_barrier(ds.id(), waited).await?;
	let request = QueryRequest::new("CREATE b:1 SET f = 'x' RETURN NONE", &session)
		.with_transaction(Arc::clone(&txn));
	for result in ds.run(request).await? {
		result.result?;
	}
	txn.commit().await?;
	timeout(Duration::from_secs(20), define)
		.await
		.map_err(|_| anyhow::anyhow!("timed out waiting for the definition"))???;
	wait_for_index_ready(&ds, &session, "b", "ib").await?;
	expect_indexed_query_len(&ds, &session, "SELECT id FROM b WITH INDEX ib WHERE f = 'x'", 1).await
}

/// Commit a client-owned transaction that runs `writes` and then `DEFINE INDEX ia ON a`, whose
/// build is held while `DEFINE INDEX ib ON b CONCURRENTLY`, run in `elsewhere`, begins its wait
/// for older write transactions, and a newer writer keeps that wait running past the
/// transaction's return. Answers the commit's outcome. The `writes` write no record of `a`, as a
/// blocking build is refused in a transaction that wrote its table's records.
async fn commit_past_another_build(
	ds: &Arc<Datastore>,
	session: &Session,
	writes: &str,
	elsewhere: &Session,
) -> Result<Result<()>> {
	let site = RetryableConflictSite::ConcurrentIndexInitialCleanup;
	let guard = inject_retryable_conflicts(site, ds.id(), REPEATED_RETRY_CONFLICTS);
	let txn = Arc::new(ds.transaction(TransactionType::Write).await?);
	for result in
		ds.run(QueryRequest::new(writes, session).with_transaction(Arc::clone(&txn))).await?
	{
		result.result?;
	}
	let own = tokio::spawn({
		let (ds, session, txn) = (Arc::clone(ds), session.clone(), Arc::clone(&txn));
		async move {
			let request =
				QueryRequest::new("DEFINE INDEX ia ON a FIELDS f", &session).with_transaction(txn);
			for result in ds.run(request).await? {
				result.result?;
			}
			Ok::<_, anyhow::Error>(())
		}
	});
	wait_for_retry_conflict(site, ds.id(), REPEATED_RETRY_CONFLICTS).await?;
	// Another build begins its wait while the transaction awaits its own build, held past its
	// own wait, and a newer writer keeps that wait running past the transaction's return.
	let newer = ds.transaction(TransactionType::Write).await?;
	let (begun, _) = ds.transaction_factory().live_writes().waits();
	let waited = super::builder::write_barrier_waits::count(ds.id());
	let other = tokio::spawn({
		let (ds, elsewhere) = (Arc::clone(ds), elsewhere.clone());
		async move { execute_all(&ds, &elsewhere, "DEFINE INDEX ib ON b FIELDS f CONCURRENTLY").await }
	});
	timeout(Duration::from_secs(20), async {
		while ds.transaction_factory().live_writes().waits().0 <= begun {
			sleep(Duration::from_millis(10)).await;
		}
	})
	.await
	.map_err(|_| anyhow::anyhow!("the other build's wait did not begin"))?;
	drop(guard);
	timeout(Duration::from_secs(20), own)
		.await
		.map_err(|_| anyhow::anyhow!("timed out waiting for the own build"))???;
	wait_for_a_build_at_the_write_barrier(ds.id(), waited).await?;
	let committed = txn.commit().await;
	newer.cancel().await?;
	timeout(Duration::from_secs(20), other)
		.await
		.map_err(|_| anyhow::anyhow!("timed out waiting for the other definition"))???;
	Ok(committed)
}

/// A transaction whose own blocking build ran while another build began its wait for older write
/// transactions, and that writes the other build's table, is refused at commit with a retryable
/// conflict, even when that wait is still running once the transaction is counted back in: the
/// wait read its epoch while the transaction was set aside, and may end without it.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_transaction_a_build_passed_while_it_awaited_its_own_is_refused() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE TABLE a SCHEMALESS; DEFINE TABLE b SCHEMALESS; CREATE b:1 SET f = 'x';",
	)
	.await?;
	let refused = commit_past_another_build(&ds, &session, "CREATE b:2 SET f = 'y'", &session)
		.await?
		.expect_err("the transaction a build of a table it writes passed is refused");
	assert!(is_retryable_transaction_conflict(&refused), "{refused}");
	Ok(())
}

/// A transaction whose own blocking build ran while another build began its wait for older write
/// transactions commits when it writes nothing of the other build's table, whether that table is
/// in the same database or another one: the other build reads nothing the transaction commits.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_transaction_a_build_of_another_table_passed_commits() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	let other = Session::owner().with_ns("test").with_db("other");
	let tx = ds.transaction(TransactionType::Write).await?;
	tx.ensure_ns_db(None, "test", "other").await?;
	tx.commit().await?;
	let tables = "
		DEFINE TABLE a SCHEMALESS;
		DEFINE TABLE b SCHEMALESS;
		DEFINE TABLE c SCHEMALESS;
		CREATE b:1 SET f = 'x';
	";
	execute_all(&ds, &session, tables).await?;
	execute_all(&ds, &other, tables).await?;
	// The other build in another database, as concurrent tests each in their own database run.
	commit_past_another_build(&ds, &session, "CREATE c:1", &other)
		.await?
		.expect("a build in another database passed it");
	// The other build runs on after its statement returns, and removing its index while it runs
	// can conflict with it.
	wait_for_index_ready(&ds, &other, "b", "ib").await?;
	execute_all(&ds, &other, "REMOVE INDEX ib ON b").await?;
	execute_all(&ds, &session, "REMOVE INDEX ia ON a").await?;
	// The other build of another table in the same database.
	commit_past_another_build(&ds, &session, "CREATE c:2", &session)
		.await?
		.expect("a build of another table passed it");
	Ok(())
}

/// A transaction whose statement is dropped while it awaits its own blocking build counts again
/// among the write transactions a later build waits for, and, its own build's wait having ended
/// unrecorded, is refused at commit.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_transaction_whose_build_wait_is_dropped_counts_again() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(&ds, &session, "DEFINE TABLE a SCHEMALESS; DEFINE TABLE b SCHEMALESS;").await?;
	let site = RetryableConflictSite::ConcurrentIndexInitialCleanup;
	let guard = inject_retryable_conflicts(site, ds.id(), REPEATED_RETRY_CONFLICTS);
	let txn = Arc::new(ds.transaction(TransactionType::Write).await?);
	let own = tokio::spawn({
		let (ds, session, txn) = (Arc::clone(&ds), session.clone(), Arc::clone(&txn));
		async move {
			let request =
				QueryRequest::new("DEFINE INDEX ia ON a FIELDS f", &session).with_transaction(txn);
			ds.run(request).await.map(drop)
		}
	});
	wait_for_retry_conflict(site, ds.id(), REPEATED_RETRY_CONFLICTS).await?;
	own.abort();
	let _ = own.await;
	drop(guard);
	let waited = super::builder::write_barrier_waits::count(ds.id());
	let define = tokio::spawn({
		let (ds, session) = (Arc::clone(&ds), session.clone());
		async move { execute_all(&ds, &session, "DEFINE INDEX ib ON b FIELDS f CONCURRENTLY").await }
	});
	wait_for_a_build_at_the_write_barrier(ds.id(), waited).await?;
	let refused = txn.commit().await.expect_err("a transaction whose build wait was dropped");
	assert!(is_retryable_transaction_conflict(&refused), "{refused}");
	timeout(Duration::from_secs(20), define)
		.await
		.map_err(|_| anyhow::anyhow!("timed out waiting for the definition"))???;
	Ok(())
}

/// A node shutting down on a store several nodes write closes its write fence before it archives
/// its node: an open write's commit is refused, while the node's answer to every barrier and the
/// archive still commit, so builds elsewhere stop waiting for it.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_node_closing_its_write_fence_refuses_writes_but_archives() -> Result<()> {
	let (ds_a, _) = new_index_test_ds().await?;
	let ds_b = ds_a.fork_for_test_as_another_process(Uuid::new_v4());
	ds_b.insert_node().await?;
	let open = ds_b.transaction(TransactionType::Write).await?;
	open.set_key(&crate::key::schema::WriteBarrierKey {}, &9).await?;
	ds_b.transaction_factory().write_fence().close();
	let refused = open.commit().await.expect_err("a closed fence refuses the write");
	assert!(is_retryable_transaction_conflict(&refused), "{refused}");
	ds_b.retire_write_barrier_answer().await?;
	ds_b.delete_node().await?;
	let answered = ds_a.transaction(TransactionType::Read).await?;
	let answer = answered
		.get_key(
			&crate::key::schema::WriteBarrierAckKey {
				nd: ds_b.id(),
			},
			None,
		)
		.await?;
	answered.cancel().await?;
	assert_eq!(answer, Some(u64::MAX), "a node that left answers every barrier");
	Ok(())
}

/// A takeover of a generation whose initial scan has completed, or checkpointed a cursor, with no
/// proof that its owner waited at the write barrier — as a release without the barrier leaves it —
/// rescans the whole table rather than publishing, or resuming after, what that scan wrote.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_takeover_rescans_a_completed_scan_without_barrier_proof() -> Result<()> {
	for checkpointed in [false, true] {
		takeover_of_a_scan_without_barrier_proof(checkpointed).await?;
	}
	Ok(())
}

/// [`a_takeover_rescans_a_completed_scan_without_barrier_proof`], for a scan that completed, or
/// one that `checkpointed` a cursor past every record.
#[cfg(feature = "kv-mem")]
async fn takeover_of_a_scan_without_barrier_proof(checkpointed: bool) -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE TABLE t SCHEMALESS; CREATE t:1; CREATE t:2; CREATE t:3; DEFINE INDEX ix ON t COUNT;",
	)
	.await?;
	wait_for_index_ready(&ds, &session, "t", "ix").await?;
	let (ns, db, table, ix) = get_table_index(&ds, "t", "ix").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	// An owner without the barrier takes a new generation, wipes the index, reports its scan
	// complete having missed every record, and dies.
	let building = new_building_for_index(&ds, &session, ns, db, &table, ix).await?;
	let acquired = building.acquire_build_state().await?.expect("the build takes a new generation");
	let ctx = building.new_write_tx_ctx().await?;
	let tx = ctx.tx();
	building
		.maintain_build_ownership(&tx, acquired.generation, &[IndexBuildPhase::Building])
		.await?;
	crate::idx::wipe_index_data(&tx, &ikb, &building.ix.index).await?;
	tx.commit().await?;
	drop(building);
	let expired = Utc::now() - chrono::Duration::seconds(BUILD_OWNER_LEASE_SECS + 5);
	let mut state = durable_build_state(&ds, &ikb).await?;
	if checkpointed {
		state.initial_cursor = Some(RecordIdKey::Number(3));
	} else {
		state.initial_complete = true;
	}
	state.initial = Some(0);
	state.updated_at = expired;
	state.owner_heartbeat_at = Some(expired);
	set_durable_build_state(&ds, &ikb, state).await?;

	let resumed =
		ds.resume_stalled_index_builds(Duration::from_secs(30), CancellationToken::new()).await?;
	assert_eq!(resumed.items, 1, "the resume scan adopts the generation");
	wait_for_index_ready(&ds, &session, "t", "ix").await?;
	let (indexed, rows) = count_against_rows(&ds, &session).await?;
	assert_eq!(
		indexed, rows,
		"checkpointed: {checkpointed}: the takeover rescans what the unproven scan missed"
	);
	Ok(())
}

/// A build stops waiting for a write transaction older than it once its wait has run its bound,
/// so a client holding a write transaction open cannot stall it: the transaction is then refused
/// at commit, and the index answers as a table scan does.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_build_stops_waiting_for_a_write_held_open_and_refuses_its_commit() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(&ds, &session, "DEFINE TABLE t SCHEMALESS; CREATE t:1 SET f = 'x';").await?;
	let _bounded =
		crate::kvs::write_barrier::test_wait::bound_waits(ds.id(), Duration::from_millis(200));
	let held = Arc::new(ds.transaction(TransactionType::Write).await?);
	let request = QueryRequest::new("CREATE t:2 SET f = 'x' RETURN NONE", &session)
		.with_transaction(Arc::clone(&held));
	for result in ds.run(request).await? {
		result.result?;
	}
	timeout(
		Duration::from_secs(20),
		execute_all(&ds, &session, "DEFINE INDEX ix ON t FIELDS f CONCURRENTLY"),
	)
	.await
	.map_err(|_| anyhow::anyhow!("timed out defining the index"))??;
	wait_for_index_ready(&ds, &session, "t", "ix").await?;
	let refused = held.commit().await.expect_err("the held write is fenced");
	assert!(is_retryable_transaction_conflict(&refused), "{refused}");
	expect_indexed_query_len(&ds, &session, "SELECT id FROM t WITH INDEX ix WHERE f = 'x'", 1).await
}

/// A write transaction held open, which a build on a table stopped waiting for and fenced, holds
/// up no later build on that table: the later one does not wait out its bound again.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_later_build_does_not_wait_again_for_a_write_its_table_fenced() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE TABLE t SCHEMALESS; DEFINE TABLE u SCHEMALESS; CREATE t:1 SET f = 'x', g = 'y';",
	)
	.await?;
	// A write of another table, held open across both builds.
	let held = Arc::new(ds.transaction(TransactionType::Write).await?);
	let request = QueryRequest::new("CREATE u:1 SET f = 'x' RETURN NONE", &session)
		.with_transaction(Arc::clone(&held));
	for result in ds.run(request).await? {
		result.result?;
	}
	{
		let _bounded =
			crate::kvs::write_barrier::test_wait::bound_waits(ds.id(), Duration::from_millis(200));
		timeout(
			Duration::from_secs(20),
			execute_all(&ds, &session, "DEFINE INDEX ix ON t FIELDS f CONCURRENTLY"),
		)
		.await
		.map_err(|_| anyhow::anyhow!("timed out defining the first index"))??;
		wait_for_index_ready(&ds, &session, "t", "ix").await?;
	}
	// The next build on `t` waits with the full bound, which it must not need.
	timeout(Duration::from_secs(20), async {
		execute_all(&ds, &session, "DEFINE INDEX iy ON t FIELDS g CONCURRENTLY").await?;
		wait_for_index_ready(&ds, &session, "t", "iy").await
	})
	.await
	.map_err(|_| anyhow::anyhow!("the second build waited again for the fenced write"))??;
	held.commit().await?;
	expect_indexed_query_len(&ds, &session, "SELECT id FROM t WITH INDEX iy WHERE g = 'y'", 1).await
}

/// A build whose generation another has replaced records no barrier proof over the newer
/// generation's, so a takeover of that generation cannot skip its rescan on the older build's word.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_superseded_build_records_no_barrier_proof() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(&ds, &session, "DEFINE TABLE t SCHEMALESS; CREATE t:1 SET f = 'x';").await?;
	execute_all(&ds, &session, "DEFINE INDEX ix ON t FIELDS f;").await?;
	wait_for_index_ready(&ds, &session, "t", "ix").await?;
	let (ns, db, table, ix) = get_table_index(&ds, "t", "ix").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	let building = new_building_for_index(&ds, &session, ns, db, &table, ix).await?;
	let acquired = building.acquire_build_state().await?.expect("the build takes a new generation");
	let read_proof = || async {
		let tx = ds.transaction(TransactionType::Read).await?;
		let proof = tx.get_key(&ikb.new_bw_key(), None).await?;
		tx.cancel().await?;
		anyhow::Ok(proof)
	};
	let before = read_proof().await?;
	// Another owner replaces the generation.
	let now = Utc::now();
	let mut state = durable_build_state(&ds, &ikb).await?;
	state.generation += 1;
	state.owner = Some(Uuid::new_v4());
	state.updated_at = now;
	state.owner_heartbeat_at = Some(now);
	set_durable_build_state(&ds, &ikb, state).await?;
	let err = building
		.record_barrier_proof(acquired.generation)
		.await
		.expect_err("a build that lost its generation records no proof");
	assert!(!is_retryable_transaction_conflict(&err), "{err}");
	assert_eq!(read_proof().await?, before, "the proof is left as it was");
	Ok(())
}

/// A wait that has run its bound refuses only the held writes of the table it builds an index on,
/// whichever path wrote them: a write held open on another table, of another tenant's or not,
/// still commits.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_build_that_stops_waiting_refuses_only_held_writes_of_its_table() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE TABLE t SCHEMALESS; DEFINE TABLE u SCHEMALESS; CREATE t:1 SET f = 'x';
		 CREATE u:1 SET f = 'x'; DEFINE INDEX other ON t FIELDS g;",
	)
	.await?;
	let (ns, db, table, _) = get_table_index(&ds, "t", "other").await?;
	let _bounded =
		crate::kvs::write_barrier::test_wait::bound_waits(ds.id(), Duration::from_millis(200));
	// A write of another table, held open.
	let other_table = Arc::new(ds.transaction(TransactionType::Write).await?);
	let request = QueryRequest::new("CREATE u:2 SET f = 'x' RETURN NONE", &session)
		.with_transaction(Arc::clone(&other_table));
	for result in ds.run(request).await? {
		result.result?;
	}
	// A write in the built table's subtree that no statement made, held open.
	let raw = ds.transaction(TransactionType::Write).await?;
	let mut key = IdxRoot {
		ns,
		db,
		tb: Cow::Borrowed(&table),
		ix: IndexId(u32::MAX),
	}
	.encode_bound()?
	.as_slice()
	.to_vec();
	key.push(b'*');
	raw.set(Key::from(key), b"x".to_vec()).await?;
	timeout(
		Duration::from_secs(20),
		execute_all(&ds, &session, "DEFINE INDEX ix ON t FIELDS f CONCURRENTLY"),
	)
	.await
	.map_err(|_| anyhow::anyhow!("timed out defining the index"))??;
	wait_for_index_ready(&ds, &session, "t", "ix").await?;
	let refused = raw.commit().await.expect_err("the held write of the built table is fenced");
	assert!(is_retryable_transaction_conflict(&refused), "{refused}");
	other_table.commit().await?;
	expect_indexed_query_len(&ds, &session, "SELECT id FROM u WHERE f = 'x'", 2).await
}

/// A node records that it answers write barriers — its answer as it registers, and the mark
/// beside each heartbeat — only once it answers them, so a node running no responder is taken by
/// every build for one that predates the barrier, never waited for nor counted on.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn only_a_node_that_answers_write_barriers_records_that_it_does() -> Result<()> {
	let (ds, _) = new_index_test_ds().await?;
	let node = ds.fork_for_test_with_node_id(Uuid::new_v4());
	let recorded = |nd: Uuid| {
		let ds = Arc::clone(&ds);
		async move {
			let tx = ds.transaction(TransactionType::Read).await?;
			let answer = tx
				.get_key(
					&crate::key::schema::WriteBarrierAckKey {
						nd,
					},
					None,
				)
				.await?
				.is_some();
			let mark = tx
				.get_key(
					&crate::key::schema::WriteBarrierHeartbeatKey {
						nd,
					},
					None,
				)
				.await?
				.is_some();
			tx.cancel().await?;
			anyhow::Ok((answer, mark))
		}
	};
	node.insert_node().await?;
	node.update_node().await?;
	assert_eq!(recorded(node.id()).await?, (false, false), "a node that answers no barrier");
	node.begin_answering_write_barriers();
	node.update_node().await?;
	assert_eq!(recorded(node.id()).await?.1, true, "its heartbeats are marked once it answers");
	let restarted = ds.fork_for_test_with_node_id(Uuid::new_v4());
	restarted.begin_answering_write_barriers();
	restarted.insert_node().await?;
	assert_eq!(recorded(restarted.id()).await?, (true, true), "a node that answers from the start");
	Ok(())
}

/// A node registered before its maintenance tasks start records that it answers write barriers —
/// its answer, and a marked heartbeat — before the start returns, so it serves no write while a
/// build elsewhere would take it for a node without the barrier.
#[cfg(all(feature = "kv-mem", feature = "maintenance-tasks"))]
#[tokio::test(flavor = "multi_thread")]
async fn a_deferred_start_records_its_barrier_support_before_it_returns() -> Result<()> {
	let (ds, _) = new_index_test_ds().await?;
	ds.insert_node().await?;
	let recorded = || async {
		let tx = ds.transaction(TransactionType::Read).await?;
		let answer = tx
			.get_key(
				&crate::key::schema::WriteBarrierAckKey {
					nd: ds.id(),
				},
				None,
			)
			.await?;
		let mark = tx
			.get_key(
				&crate::key::schema::WriteBarrierHeartbeatKey {
					nd: ds.id(),
				},
				None,
			)
			.await?;
		let node = tx
			.get_key(
				&crate::key::schema::NodeKey {
					nd: ds.id(),
				},
				None,
			)
			.await?
			.expect("the node");
		tx.cancel().await?;
		anyhow::Ok((answer.is_some(), mark == Some(node.heartbeat.value)))
	};
	assert_eq!(recorded().await?, (false, false), "registered with no responder");
	ds.start_maintenance_tasks_checked().await?;
	assert_eq!(recorded().await?, (true, true), "recorded before the start returned");
	ds.shutdown().await?;
	Ok(())
}

/// Node cleanup keeps an archived node's row while a build may still wait for its write barrier
/// answer — until its heartbeat is `exclude_after` old — since a build finds a node
/// only through its row.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn node_cleanup_keeps_an_archived_node_a_build_may_still_wait_for() -> Result<()> {
	let (ds, _) = new_index_test_ds().await?;
	let exclude =
		crate::kvs::write_barrier::exclude_after(surrealdb_datastore::triggers::WRITE_COMMIT_BOUND);
	let archived = |age: Duration| {
		let ds = Arc::clone(&ds);
		async move {
			let id = Uuid::new_v4();
			let tx = ds.transaction(TransactionType::Write).await?;
			let node =
				crate::catalog::node::Node::new_with_endpoint(id, ds.clock_now() - age, true, None);
			tx.set_key(
				&crate::key::schema::NodeKey {
					nd: id,
				},
				&node,
			)
			.await?;
			tx.commit().await?;
			anyhow::Ok(id)
		}
	};
	let recent = archived(exclude - Duration::from_secs(30)).await?;
	let old = archived(exclude + Duration::from_secs(10)).await?;
	ds.remove_nodes().await?;
	let tx = ds.transaction(TransactionType::Read).await?;
	let kept = tx
		.get_key(
			&crate::key::schema::NodeKey {
				nd: recent,
			},
			None,
		)
		.await?
		.is_some();
	let removed = tx
		.get_key(
			&crate::key::schema::NodeKey {
				nd: old,
			},
			None,
		)
		.await?
		.is_none();
	tx.cancel().await?;
	assert!(kept, "an archived node within the window is kept");
	assert!(removed, "one past it is removed");
	Ok(())
}

/// A node membership refresh interval longer than the write fence can run with is refused on a
/// store other nodes write, and accepted on one the datastore writes alone.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_heartbeat_interval_too_long_for_the_write_fence_is_refused_on_a_shared_store()
-> Result<()> {
	let (ds, _) = new_index_test_ds().await?;
	let long = Duration::from_secs(15);
	ds.check_heartbeat_interval(long).await?;
	let other = ds.fork_for_test_as_another_process(Uuid::new_v4());
	assert!(other.check_heartbeat_interval(long).await.is_err());
	other.check_heartbeat_interval(Duration::from_secs(3)).await?;
	Ok(())
}

/// Defines a full-text `ft` on `t.content` and moves it to format 0 while it
/// is still empty, as a binary predating the shared doc-ID space would have
/// defined it, then creates records `t:1` to `t:{count}` whose text holds
/// `bravo`, which the index takes in that format's layout.
#[cfg(feature = "kv-mem")]
async fn seed_legacy_fulltext_index(ds: &Datastore, session: &Session, count: usize) -> Result<()> {
	execute_all(
		ds,
		session,
		"DEFINE ANALYZER simple TOKENIZERS blank;
		 DEFINE INDEX ft ON t FIELDS content FULLTEXT ANALYZER simple BM25;",
	)
	.await?;
	downgrade_index_format(ds, "t", "ft", 0).await?;
	let creates: String = (1..=count)
		.map(|i| format!("CREATE t:{i} SET content = 'bravo {i}' RETURN NONE;"))
		.collect();
	execute_all(ds, session, &creates).await
}

/// The ids [`BRAVO`] has to answer when `t:1` to `t:{count}` hold `bravo`,
/// sorted as [`planner_result_ids`] sorts them.
#[cfg(feature = "kv-mem")]
fn bravo_carriers(count: usize) -> Vec<String> {
	let mut ids: Vec<String> = (1..=count).map(|i| format!("t:{i}")).collect();
	ids.sort();
	ids
}

/// How the query engine's answer to `sql` differs from `expected`: one line when it does not
/// answer it, none when it does.
#[cfg(feature = "kv-mem")]
async fn engines_answering_otherwise(
	ds: &Datastore,
	sql: &str,
	expected: &[String],
) -> Vec<String> {
	match planner_result_ids(ds, sql).await {
		Ok(ids) if ids == expected => Vec::new(),
		Ok(ids) => vec![format!(
			"the engine answers `{sql}` with {} of the {} records: {ids:?}",
			ids.len(),
			expected.len()
		)],
		Err(err) => vec![format!("the engine fails `{sql}`: {err}")],
	}
}

/// A full-text match every record created by [`seed_legacy_fulltext_index`]
/// answers.
#[cfg(feature = "kv-mem")]
const BRAVO: &str = "SELECT VALUE id FROM t WHERE content @@ 'bravo'";

/// Runs `check` against a fresh datastore on every storage engine, and returns
/// what it finds wrong, each line prefixed with the engine's name.
#[cfg(feature = "kv-mem")]
async fn on_every_engine<F, Fut>(check: F) -> Result<Vec<String>>
where
	F: Fn(Arc<Datastore>, Session) -> Fut,
	Fut: std::future::Future<Output = Result<Vec<String>>>,
{
	let mut wrong = Vec::new();
	for engine in storage_engines()? {
		let (ds, _other_node, session) = new_distributed_index_test_ds_at(&engine.path).await?;
		for line in check(Arc::clone(&ds), session).await? {
			wrong.push(format!("{}: {line}", engine.label));
		}
		ds.shutdown().await?;
	}
	Ok(wrong)
}

/// A `REBUILD INDEX` that would move a legacy index onto the current layout,
/// rolled back, leaves the index as it found it. The definition keeps its
/// format, so the data it reads, and the doc-ID space it does not consume, must
/// stay in that format's layout. Checked on every storage engine.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn cancelled_format_rebuild_leaves_its_layout_under_the_old_definition() -> Result<()> {
	let wrong = on_every_engine(|ds, session| async move {
		seed_legacy_fulltext_index(&ds, &session, 3).await?;
		let before = engines_answering_otherwise(&ds, BRAVO, &bravo_carriers(3)).await;
		assert!(before.is_empty(), "the legacy index must answer before the rebuild: {before:?}");

		execute_cancelled_transaction(&ds, &session, "BEGIN; REBUILD INDEX ft ON t; CANCEL;")
			.await?;
		let (_, _, _, ix) = get_table_index(&ds, "t", "ft").await?;
		assert_eq!(ix.format_version, 0, "the cancel keeps the legacy definition");

		let mut wrong = engines_answering_otherwise(&ds, BRAVO, &bravo_carriers(3)).await;
		execute_all(&ds, &session, "CREATE t:4 SET content = 'bravo 4'; DELETE t:1;").await?;
		let after_writes: Vec<String> =
			bravo_carriers(4).into_iter().filter(|id| id != "t:1").collect();
		wrong.extend(engines_answering_otherwise(&ds, BRAVO, &after_writes).await);
		let mappings = count_doc_id_mappings(&ds, "t").await?;
		if mappings != (0, 0) {
			wrong.push(format!(
				"the table holds {mappings:?} shared doc-ID mappings, which no index on it consumes"
			));
		}
		Ok(wrong)
	})
	.await?;
	assert!(wrong.is_empty(), "\n{}", wrong.join("\n"));
	Ok(())
}

/// A script that catches the failure of a blocking `REBUILD INDEX` that would move a legacy
/// index onto the current layout takes the rebuild back: the transaction still commits, every
/// statement keeps its own result, and the index keeps the format and layout it had. The
/// rebuild's wait for its staged generation, which the owner awaits after the commit, goes with
/// the savepoint the failed call ran behind, so the commit does not start a generation that no
/// longer exists.
#[cfg(all(feature = "kv-mem", feature = "scripting"))]
#[tokio::test(flavor = "multi_thread")]
async fn caught_format_rebuild_leaves_no_wait_for_its_staged_generation() -> Result<()> {
	let ds = Datastore::builder()
		.without_maintenance_tasks()
		.with_capabilities(crate::dbs::Capabilities::all())
		.build_with_path("memory")
		.await?;
	let session = Session::owner().with_ns("test").with_db("test");
	let tx = ds.transaction(TransactionType::Write).await?;
	tx.ensure_ns_db(None, "test", "test").await?;
	tx.commit().await?;
	seed_legacy_fulltext_index(&ds, &session, 3).await?;
	let results = ds
		.execute(
			r#"BEGIN;
			RETURN function() {
				try {
					await surrealdb.query(`{ REBUILD INDEX ft ON t; THROW 'boom'; }`);
				} catch (e) {
					return 'caught';
				}
				return 'not caught';
			};
			CREATE t:4 SET content = 'bravo 4';
			COMMIT;"#,
			&session,
			None,
		)
		.await?;
	let outcomes: Vec<String> = results
		.into_iter()
		.map(|result| match result.result {
			Ok(value) => surrealdb_types::ToSql::to_sql(&value),
			Err(err) => format!("error: {err}"),
		})
		.collect();
	assert_eq!(
		outcomes,
		["NONE", "'caught'", "[{ content: 'bravo 4', id: t:4 }]", "NONE"],
		"the statements keep their own results"
	);
	let (_, _, _, ix) = get_table_index(&ds, "t", "ft").await?;
	assert_eq!(ix.format_version, 0, "the rolled-back rebuild leaves the legacy definition");
	let wrong = engines_answering_otherwise(&ds, BRAVO, &bravo_carriers(4)).await;
	assert!(wrong.is_empty(), "the legacy index still answers: {wrong:?}");
	Ok(())
}

/// While a `REBUILD INDEX` that moves a legacy index onto the current layout
/// has published its build but not yet committed its definition, every other
/// session still reads and maintains the index through the legacy definition.
/// Their queries must find the records, and a record they write must be found
/// once the rebuild commits. Checked on every storage engine.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn format_rebuild_publishes_its_layout_before_its_definition() -> Result<()> {
	let wrong = on_every_engine(|ds, session| async move {
		seed_legacy_fulltext_index(&ds, &session, 3).await?;
		let statement = Arc::new(ds.transaction(TransactionType::Write).await?);
		let request = QueryRequest::new("REBUILD INDEX ft ON t", &session)
			.with_transaction(Arc::clone(&statement));
		for result in ds.run(request).await? {
			result.result?;
		}
		// The blocking build has published, and its statement is still open.
		let mut wrong = engines_answering_otherwise(&ds, BRAVO, &bravo_carriers(3))
			.await
			.into_iter()
			.map(|line| format!("before the rebuild commits, {line}"))
			.collect::<Vec<_>>();
		execute_all(&ds, &session, "CREATE t:4 SET content = 'bravo 4'").await?;
		statement.commit().await?;
		let (_, _, _, ix) = get_table_index(&ds, "t", "ft").await?;
		assert_eq!(ix.format_version, INDEX_FORMAT_VERSION, "the commit stamps the current format");
		wrong.extend(
			engines_answering_otherwise(&ds, BRAVO, &bravo_carriers(4))
				.await
				.into_iter()
				.map(|line| format!("after the rebuild commits, {line}")),
		);
		Ok(wrong)
	})
	.await?;
	assert!(wrong.is_empty(), "\n{}", wrong.join("\n"));
	Ok(())
}

/// A `REBUILD INDEX` that moves a legacy index onto the current layout, and
/// whose node dies after its build has checkpointed part of its scan, leaves an
/// index that one layout answers for in full once the resume scan finishes the
/// build. The statement commits the new format together with the generation its
/// build builds, so the build that resumes reads the definition its checkpoint
/// was made under. Checked on every storage engine.
///
/// The node's death is written as the durable state it leaves: the statement
/// committed, the build it started claimed the generation, wiped the index,
/// indexed the first ten records and checkpointed after them, and its owner's
/// lease lapsed.
///
/// The resumed scan waits for a write transaction open across the resume, as a scan resumed from
/// a checkpoint an owner without the write barrier made must.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn resumed_format_rebuild_indexes_the_rest_in_the_old_layout() -> Result<()> {
	const RECORDS: usize = 20;
	const CHECKPOINTED: usize = 10;
	let wrong = on_every_engine(|ds, session| async move {
		seed_legacy_fulltext_index(&ds, &session, RECORDS).await?;
		{
			let _unstarted = super::unstarted::leave_unstarted(ds.id());
			execute_all(&ds, &session, "REBUILD INDEX ft ON t").await?;
		}
		let (ns, db, table, ix) = get_table_index(&ds, "t", "ft").await?;
		assert_eq!(ix.format_version, INDEX_FORMAT_VERSION, "the rebuild commits its format");
		let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
		let staged = durable_build_state(&ds, &ikb).await?;
		assert_eq!(
			(staged.phase, staged.owner),
			(IndexBuildPhase::Building, None),
			"the rebuild commits its generation unclaimed"
		);

		let building = new_building_for_index(&ds, &session, ns, db, &table, ix).await?;
		let acquired = building
			.claim_staged_generation(staged.generation)
			.await?
			.expect("the build claims the generation its statement staged");
		let values = scan_records(&ds, ns, db, &table).await?;
		for batch in [None, Some(&values[..CHECKPOINTED])] {
			let ctx = building.new_write_tx_ctx().await?;
			let tx = ctx.tx();
			building
				.maintain_build_ownership(&tx, acquired.generation, &[IndexBuildPhase::Building])
				.await?;
			match batch {
				None => crate::idx::wipe_index_data(&tx, &ikb, &building.ix.index).await?,
				Some(batch) => {
					building
						.index_initial_batch(
							&ctx,
							&tx,
							batch,
							0,
							&mut false,
							&mut None,
							&super::kind::InitialBatchSink::Pending,
						)
						.await?;
				}
			}
			tx.commit().await?;
		}
		let expired = Utc::now() - chrono::Duration::seconds(BUILD_OWNER_LEASE_SECS + 5);
		let mut state = durable_build_state(&ds, &ikb).await?;
		state.initial_cursor =
			Some(RecordKey::decode_key(&values[CHECKPOINTED - 1].0)?.id.into_owned());
		state.initial = Some(CHECKPOINTED as u64);
		state.updated_at = expired;
		state.owner_heartbeat_at = Some(expired);
		set_durable_build_state(&ds, &ikb, state).await?;
		drop(building);

		// A write transaction open across the resume: the build that dies may never have waited
		// for it, so the resumed scan does.
		let open = ds.transaction(TransactionType::Write).await?;
		let waited = super::builder::write_barrier_waits::count(ds.id());
		let resumed = ds
			.resume_stalled_index_builds(Duration::from_secs(30), CancellationToken::new())
			.await?;
		assert_eq!(resumed.items, 1, "the resume scan adopts the stalled rebuild");
		wait_for_a_build_at_the_write_barrier(ds.id(), waited).await?;
		open.cancel().await?;
		wait_for_index_ready(&ds, &session, "t", "ft").await?;
		Ok(engines_answering_otherwise(&ds, BRAVO, &bravo_carriers(RECORDS)).await)
	})
	.await?;
	assert!(wrong.is_empty(), "\n{}", wrong.join("\n"));
	Ok(())
}

/// Within one transaction, writes after a `REBUILD INDEX` that moves a legacy index onto the
/// current layout, and a second such rebuild, leave an index that the current layout answers for
/// in full once the transaction commits: the rebuild is staged once, and its build reads the
/// transaction's writes once they have committed. Checked on every storage engine.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_format_rebuild_in_a_transaction_builds_once_after_its_writes() -> Result<()> {
	let wrong = on_every_engine(|ds, session| async move {
		seed_legacy_fulltext_index(&ds, &session, 3).await?;
		let (ns, db, table, ix) = get_table_index(&ds, "t", "ft").await?;
		let ikb = IndexKeyBase::new(ns, db, table, ix.index_id);
		let before = durable_build_state(&ds, &ikb).await?.generation;
		execute_all(
			&ds,
			&session,
			"BEGIN;
			 REBUILD INDEX ft ON t;
			 CREATE t:4 SET content = 'bravo 4';
			 DELETE t:1;
			 REBUILD INDEX ft ON t;
			 COMMIT;",
		)
		.await?;
		wait_for_index_ready(&ds, &session, "t", "ft").await?;
		let mut wrong = Vec::new();
		let after = durable_build_state(&ds, &ikb).await?.generation;
		if after != before + 1 {
			wrong.push(format!("the transaction built generations {before} to {after}"));
		}
		let carriers: Vec<String> =
			bravo_carriers(4).into_iter().filter(|id| id != "t:1").collect();
		wrong.extend(engines_answering_otherwise(&ds, BRAVO, &carriers).await);
		Ok(wrong)
	})
	.await?;
	assert!(wrong.is_empty(), "\n{}", wrong.join("\n"));
	Ok(())
}

/// A `REBUILD INDEX` that moves an index onto the current layout, committed while a local build
/// of the generation it replaces is still running, stops that build and builds its own: the
/// statement, which has committed, succeeds, and the index answers in the current layout.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_format_rebuild_replaces_a_local_build_still_running() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	seed_legacy_fulltext_index(&ds, &session, 3).await?;
	execute_all(&ds, &session, "REBUILD INDEX ft ON t").await?;
	let guard =
		start_index_build_paused(&ds, &session, "REBUILD INDEX ft ON t CONCURRENTLY").await?;
	downgrade_index_format(&ds, "t", "ft", 0).await?;
	let statement = tokio::spawn({
		let (ds, session) = (Arc::clone(&ds), session.clone());
		async move { execute_all(&ds, &session, "REBUILD INDEX ft ON t").await }
	});
	sleep(Duration::from_millis(500)).await;
	drop(guard);
	timeout(Duration::from_secs(60), statement)
		.await
		.map_err(|_| anyhow::anyhow!("timed out waiting for the rebuild"))???;
	wait_for_index_ready(&ds, &session, "t", "ft").await?;
	let wrong = engines_answering_otherwise(&ds, BRAVO, &bravo_carriers(3)).await;
	assert!(wrong.is_empty(), "\n{}", wrong.join("\n"));
	Ok(())
}

/// The start of a committed `REBUILD INDEX` that moves an index onto the current layout joins a
/// build of the generation it staged that the resume scan adopted first on this node, rather than
/// stopping it as the build of the generation it replaced.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_format_rebuild_joins_its_generation_adopted_by_the_resume_scan() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	seed_legacy_fulltext_index(&ds, &session, 3).await?;
	{
		let _unstarted = super::unstarted::leave_unstarted(ds.id());
		execute_all(&ds, &session, "REBUILD INDEX ft ON t").await?;
	}
	let (ns, db, table, ix) = get_table_index(&ds, "t", "ft").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	let mut staged = durable_build_state(&ds, &ikb).await?;
	let generation = staged.generation;
	let expired = Utc::now() - chrono::Duration::seconds(BUILD_OWNER_LEASE_SECS + 5);
	staged.updated_at = expired;
	set_durable_build_state(&ds, &ikb, staged).await?;
	// The resume scan adopts the generation, and its build is held in its cleanup.
	let site = RetryableConflictSite::ConcurrentIndexInitialCleanup;
	let guard = inject_retryable_conflicts(site, ds.id(), REPEATED_RETRY_CONFLICTS);
	let resumed =
		ds.resume_stalled_index_builds(Duration::from_secs(30), CancellationToken::new()).await?;
	assert_eq!(resumed.items, 1, "the resume scan adopts the staged generation");
	wait_for_retry_conflict(site, ds.id(), REPEATED_RETRY_CONFLICTS).await?;

	let ctx = ds.setup_ctx()?.freeze();
	let builder = ctx.get_index_builder().expect("index builder").clone();
	let tb = {
		let tx = ds.transaction(TransactionType::Read).await?;
		let tb = catch!(tx, tx.get_tb(ns, db, &table, None).await).expect("table").table_id;
		tx.cancel().await?;
		tb
	};
	let completion = builder
		.start_staged_rebuild(&ctx, ds.setup_options(&session), ns, db, tb, ix, generation, true)
		.await?
		.expect("a blocking start answers a completion");
	drop(guard);
	timeout(Duration::from_secs(30), completion)
		.await
		.map_err(|_| anyhow::anyhow!("timed out waiting for the joined build"))??;
	wait_for_index_ready(&ds, &session, "t", "ft").await?;
	assert_eq!(durable_build_state(&ds, &ikb).await?.generation, generation);
	let wrong = engines_answering_otherwise(&ds, BRAVO, &bravo_carriers(3)).await;
	assert!(wrong.is_empty(), "\n{}", wrong.join("\n"));
	Ok(())
}

/// The start of a committed `REBUILD INDEX` that moves an index onto the current layout leaves a
/// local build of a later generation, which a later statement started first, to finish, rather
/// than stopping it as the build of the generation it replaced.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_format_rebuild_start_leaves_a_later_generation_building() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	seed_legacy_fulltext_index(&ds, &session, 3).await?;
	// The rebuild that moves the format, whose start comes late, staged this generation.
	execute_all(&ds, &session, "REBUILD INDEX ft ON t").await?;
	let (ns, db, table, ix) = get_table_index(&ds, "t", "ft").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	let staged = durable_build_state(&ds, &ikb).await?.generation;
	// A later rebuild, which leaves the format as it is, builds the next generation, held in its
	// cleanup.
	let site = RetryableConflictSite::ConcurrentIndexInitialCleanup;
	let guard = inject_retryable_conflicts(site, ds.id(), REPEATED_RETRY_CONFLICTS);
	execute_all(&ds, &session, "REBUILD INDEX ft ON t CONCURRENTLY").await?;
	wait_for_retry_conflict(site, ds.id(), REPEATED_RETRY_CONFLICTS).await?;
	let later = durable_build_state(&ds, &ikb).await?.generation;
	assert!(later > staged, "the later rebuild builds a generation of its own");

	let ctx = ds.setup_ctx()?.freeze();
	let builder = ctx.get_index_builder().expect("index builder").clone();
	let tb = {
		let tx = ds.transaction(TransactionType::Read).await?;
		let tb = catch!(tx, tx.get_tb(ns, db, &table, None).await).expect("table").table_id;
		tx.cancel().await?;
		tb
	};
	let started = builder
		.start_staged_rebuild(&ctx, ds.setup_options(&session), ns, db, tb, ix, staged, false)
		.await?;
	assert!(started.is_none(), "a background start answers no completion");
	drop(guard);
	timeout(Duration::from_secs(30), wait_for_index_ready(&ds, &session, "t", "ft"))
		.await
		.map_err(|_| anyhow::anyhow!("the later generation's build did not finish"))??;
	assert_eq!(durable_build_state(&ds, &ikb).await?.generation, later);
	let wrong = engines_answering_otherwise(&ds, BRAVO, &bravo_carriers(3)).await;
	assert!(wrong.is_empty(), "\n{}", wrong.join("\n"));
	Ok(())
}

/// A blocking `REBUILD INDEX` that follows, in the same transaction, a `REBUILD INDEX …
/// CONCURRENTLY` moving the index onto the current layout waits for the build the earlier
/// statement staged, as it would for one it staged itself.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_blocking_format_rebuild_after_a_concurrent_one_waits_for_its_build() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	seed_legacy_fulltext_index(&ds, &session, 3).await?;
	let site = RetryableConflictSite::ConcurrentIndexInitialCleanup;
	let guard = inject_retryable_conflicts(site, ds.id(), REPEATED_RETRY_CONFLICTS);
	let statement = tokio::spawn({
		let (ds, session) = (Arc::clone(&ds), session.clone());
		async move {
			execute_all(
				&ds,
				&session,
				"BEGIN; REBUILD INDEX ft ON t CONCURRENTLY; REBUILD INDEX ft ON t; COMMIT;",
			)
			.await
		}
	});
	wait_for_retry_conflict(site, ds.id(), REPEATED_RETRY_CONFLICTS).await?;
	sleep(Duration::from_millis(300)).await;
	assert!(!statement.is_finished(), "the blocking rebuild returned before its build finished");
	drop(guard);
	timeout(Duration::from_secs(60), statement)
		.await
		.map_err(|_| anyhow::anyhow!("timed out waiting for the rebuild"))???;
	assert_eq!(index_building_status(&ds, &session, "t", "ft").await?, "ready");
	let wrong = engines_answering_otherwise(&ds, BRAVO, &bravo_carriers(3)).await;
	assert!(wrong.is_empty(), "\n{}", wrong.join("\n"));
	Ok(())
}

/// A blocking `REBUILD INDEX` moving an index onto the current layout, inside `BEGIN … COMMIT` or
/// a compound statement, reports a build that fails after the commit as its own statement's
/// error, and every other statement keeps its result.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_format_rebuild_in_a_block_reports_its_failed_build() -> Result<()> {
	for (sql, slot) in
		[("BEGIN; REBUILD INDEX ft ON t; COMMIT;", 1), ("IF true { REBUILD INDEX ft ON t }", 0)]
	{
		let (ds, session) = new_index_test_ds().await?;
		seed_legacy_fulltext_index(&ds, &session, 3).await?;
		let _failing = inject_non_retryable_error(
			NonRetryableErrorSite::ConcurrentIndexInitialBatchCommit,
			ds.id(),
		);
		let results = timeout(Duration::from_secs(60), ds.execute(sql, &session, None))
			.await
			.map_err(|_| anyhow::anyhow!("{sql}: timed out"))??;
		for (at, result) in results.into_iter().enumerate() {
			if at == slot {
				result
					.result
					.expect_err(&format!("{sql}: the statement reports its build's failure"));
			} else {
				assert_eq!(result.result?, crate::types::PublicValue::None, "{sql}: slot {at}");
			}
		}
	}
	Ok(())
}

/// A blocking `REBUILD INDEX` moving an index onto the current layout inside `BEGIN … COMMIT`
/// awaits its build outside the block's transaction timer: a build that outlasts what the block
/// has left ends the statement's wait, never the committed block, whose `COMMIT` succeeds.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_format_rebuild_in_a_block_never_times_its_commit_out() -> Result<()> {
	let (ds, session) =
		new_index_test_ds_with_timeouts(None, Some(Duration::from_millis(500))).await?;
	seed_legacy_fulltext_index(&ds, &session, 3).await?;
	let site = RetryableConflictSite::ConcurrentIndexInitialCleanup;
	let guard = inject_retryable_conflicts(site, ds.id(), REPEATED_RETRY_CONFLICTS);
	let results = timeout(
		Duration::from_secs(30),
		run_cancellable(
			&ds,
			&session,
			"BEGIN; REBUILD INDEX ft ON t; COMMIT;",
			crate::ctx::CancelHandle::new(),
		),
	)
	.await
	.map_err(|_| anyhow::anyhow!("timed out running the block"))??;
	drop(guard);
	assert_eq!(results.len(), 3, "{results:?}");
	assert_eq!(results[2], None, "the block's commit stands: {results:?}");
	assert!(
		results.iter().flatten().all(|err| !err.contains("The transaction timed out")),
		"{results:?}"
	);
	let (_, _, _, ix) = get_table_index(&ds, "t", "ft").await?;
	assert_ne!(ix.format_version, 0, "the rebuild's definition committed");
	wait_for_index_ready(&ds, &session, "t", "ft").await?;
	let wrong = engines_answering_otherwise(&ds, BRAVO, &bravo_carriers(3)).await;
	assert!(wrong.is_empty(), "\n{}", wrong.join("\n"));
	Ok(())
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

/// The HNSW pass a blocking rebuild's phase 2 runs contends with the
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
	let site = RetryableConflictSite::HnswCompaction;
	let node_id = ds.id();
	let _guard = inject_retryable_conflict(site, node_id);

	execute_all(&ds, &session, "REBUILD INDEX hx ON t").await?;

	assert_eq!(
		retryable_conflict_count(site, node_id),
		0,
		"the rebuild must consume the injected conflict by retrying its pass"
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
	// The pass under test folds what a build's scan queues, which the DiskANN sink would not.
	let _sink_ds = super::kind::test_sink::off(ds.id());
	execute_all(
		&ds,
		&session,
		"DEFINE INDEX dx ON t FIELDS vec DISKANN DIMENSION 2 DIST EUCLIDEAN TYPE F32;
		 CREATE t:1 SET vec = [1.0, 0.0];
		 CREATE t:2 SET vec = [0.0, 1.0];",
	)
	.await?;
	let site = RetryableConflictSite::DiskAnnCompaction;
	let node_id = ds.id();
	let _guard = inject_retryable_conflict(site, node_id);

	execute_all(&ds, &session, "REBUILD INDEX dx ON t").await?;

	assert_eq!(
		retryable_conflict_count(site, node_id),
		0,
		"the rebuild must consume the injected conflict by retrying its pass"
	);
	expect_indexed_query_len(&ds, &session, "SELECT id FROM t WHERE vec <|2,40|> [1.0f, 0.0f]", 2)
		.await
}

/// The `!dw` pendings a DiskANN index holds, over every shard.
#[cfg(all(feature = "kv-mem", diskann))]
async fn count_diskann_pendings(ds: &Datastore, ikb: &IndexKeyBase) -> Result<usize> {
	use surrealdb_datastore::values::diskann::DISKANN_PENDING_STATE_SHARDS;
	let tx = ds.transaction(TransactionType::Read).await?;
	let mut count = 0;
	for shard in 0..DISKANN_PENDING_STATE_SHARDS {
		count += catch!(tx, tx.count(ikb.new_dw_shard_range(shard)?, None).await);
	}
	tx.cancel().await?;
	Ok(count)
}

/// A DISKANN build sends the records its scan reads straight into the graph through its
/// initial-batch sink. With no compaction running, the graph holds every record's vector and the
/// index holds no pending: nothing went through `!dw`. With the sink capped at 7 vectors, every
/// scan batch is cut short of its end and its rest left to the next transaction, and every record
/// is still indexed exactly once.
#[cfg(all(feature = "kv-mem", diskann))]
#[tokio::test(flavor = "multi_thread")]
async fn diskann_build_sinks_its_scan_without_pendings() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	let _cap = super::kind::test_sink::set_cap(ds.id(), 7);
	let rows: String =
		(0..300).map(|i| format!("CREATE t:{i} SET vec = [{i}.0, 0.0] RETURN NONE;")).collect();
	execute_all(&ds, &session, &format!("DEFINE TABLE t SCHEMALESS; {rows}")).await?;
	execute_all(
		&ds,
		&session,
		"DEFINE INDEX dx ON t FIELDS vec DISKANN DIMENSION 2 DIST EUCLIDEAN TYPE F32 CONCURRENTLY",
	)
	.await?;
	wait_for_index_ready(&ds, &session, "t", "dx").await?;
	let (ns, db, table, ix) = get_table_index(&ds, "t", "dx").await?;
	let ikb = IndexKeyBase::new(ns, db, table, ix.index_id);
	assert_eq!(count_diskann_pendings(&ds, &ikb).await?, 0, "the build queued pendings");
	let tx = ds.transaction(TransactionType::Read).await?;
	let elements = catch!(tx, tx.count(ikb.new_de_range()?, None).await);
	tx.cancel().await?;
	assert_eq!(elements, 300, "every record's vector is in the graph, once");
	let info = ds.execute("INFO FOR INDEX dx ON t", &session, None).await?.remove(0).result?;
	let info = info.into_json_value();
	assert_eq!(
		info.pointer("/building/initial").and_then(|v| v.as_u64()),
		Some(300),
		"the scan indexed a record other than once: {info}"
	);
	expect_indexed_query_len(
		&ds,
		&session,
		"SELECT id FROM t WHERE vec <|300,400|> [0.0, 0.0]",
		300,
	)
	.await
}

/// #1401: the initial-batch sink hook is applied once for each batch a build's scan commits. A
/// kind with no sink of its own takes the default, which writes nothing. A batch whose commit
/// conflicts is retried in a fresh transaction, which applies the hook again; only the attempt
/// that commits counts as a batch.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn the_default_sink_is_applied_once_per_committed_initial_batch() -> Result<()> {
	for retries in [0usize, 2] {
		let (ds, session) = new_index_test_ds().await?;
		let _scan = super::kind::test_sink::set_scan_batch(ds.id(), 8);
		let rows: String =
			(0..40).map(|i| format!("CREATE t:{i} SET n = {i} RETURN NONE;")).collect();
		execute_all(&ds, &session, &format!("DEFINE TABLE t SCHEMALESS; {rows}")).await?;
		let site = RetryableConflictSite::ConcurrentIndexInitialBatch;
		let _conflicts = (retries > 0).then(|| inject_retryable_conflicts(site, ds.id(), retries));
		execute_all(&ds, &session, "DEFINE INDEX ix ON t FIELDS n CONCURRENTLY").await?;
		wait_for_index_ready(&ds, &session, "t", "ix").await?;
		let counts = super::kind::test_sink::counts(ds.id());
		assert_eq!(counts.committed, 5, "40 records in batches of 8");
		assert_eq!(
			counts.applied,
			counts.committed + retries,
			"the sink was not applied once per attempt at each batch"
		);
	}
	Ok(())
}

/// #1401: the hooks that keep no state across batches — the doc-ID deferral, the full-text
/// batch context and the initial-batch sink — index a table scanned in two disjoint key ranges
/// as they index it scanned in one. Each build is held while deletes queue, so the deferral and
/// its reclaim run too; both builds answer every query alike and leave the same doc-ID
/// mappings, with nothing left deferred.
#[cfg(all(feature = "kv-mem", diskann))]
#[tokio::test(flavor = "multi_thread")]
async fn stateless_hooks_index_a_table_tiled_by_two_ranges_as_one() -> Result<()> {
	let mut outcomes = Vec::new();
	for batch in [100u32, 20] {
		let (ds, session) = new_index_test_ds().await?;
		let _scan = super::kind::test_sink::set_scan_batch(ds.id(), batch);
		let rows: String = (0..40)
			.map(|i| {
				format!(
					"CREATE t:{i} SET body = 'word{} common', vec = [{i}.0, 0.0] RETURN NONE;",
					i % 5
				)
			})
			.collect();
		execute_all(
			&ds,
			&session,
			&format!(
				"DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase; DEFINE TABLE t \
				 SCHEMALESS; {rows}"
			),
		)
		.await?;
		let guard = start_index_build_paused(
			&ds,
			&session,
			"DEFINE INDEX ft ON t FIELDS body FULLTEXT ANALYZER simple BM25 CONCURRENTLY",
		)
		.await?;
		execute_all_retrying_conflicts(&ds, &session, "DELETE t:3; DELETE t:27;").await?;
		drop(guard);
		wait_for_index_ready(&ds, &session, "t", "ft").await?;
		let guard = start_index_build_paused(
			&ds,
			&session,
			"DEFINE INDEX dx ON t FIELDS vec DISKANN DIMENSION 2 DIST EUCLIDEAN TYPE F32 CONCURRENTLY",
		)
		.await?;
		execute_all_retrying_conflicts(&ds, &session, "DELETE t:5;").await?;
		drop(guard);
		wait_for_index_ready(&ds, &session, "t", "dx").await?;
		Datastore::index_compaction(
			Arc::clone(&ds),
			Duration::from_secs(10),
			CancellationToken::new(),
		)
		.await?;
		let (ns, db, table, _) = get_table_index(&ds, "t", "dx").await?;
		outcomes.push((
			query_json(&ds, &session, "SELECT VALUE id FROM t WHERE body @@ 'word1' ORDER BY id")
				.await?,
			query_json(&ds, &session, "SELECT VALUE id FROM t WHERE body @@ 'common' ORDER BY id")
				.await?,
			query_json(&ds, &session, "SELECT VALUE id FROM t WHERE vec <|40,100|> [0.0, 0.0]")
				.await?,
			count_doc_id_mappings(&ds, "t").await?,
			count_pending_reclaims(&ds, ns, db, &table).await?,
		));
	}
	assert_eq!(outcomes[0].4, 0, "a deferred doc-ID was left unreclaimed");
	assert_eq!(outcomes[0], outcomes[1], "two ranges indexed the table unlike one");
	Ok(())
}

/// Writes that land while a DISKANN build runs keep the pending path: the sink takes only the
/// records the scan reads with no queued mutation, and the materialiser folds the rest after the
/// build is `Online`. Every write is answered, and the folded index holds no pending.
#[cfg(all(feature = "kv-mem", diskann))]
#[tokio::test(flavor = "multi_thread")]
async fn diskann_build_with_concurrent_writes_converges() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	let rows: String =
		(0..200).map(|i| format!("CREATE t:{i} SET vec = [{i}.0, 0.0] RETURN NONE;")).collect();
	execute_all(&ds, &session, &format!("DEFINE TABLE t SCHEMALESS; {rows}")).await?;
	// Held before its scan, so every write below is queued by the build, and the scan meets
	// those records with a queued mutation beside the ones it sinks.
	let guard = start_index_build_paused(
		&ds,
		&session,
		"DEFINE INDEX dx ON t FIELDS vec DISKANN DIMENSION 2 DIST EUCLIDEAN TYPE F32 CONCURRENTLY",
	)
	.await?;
	let writes: String = (0..200)
		.step_by(2)
		.map(|i| format!("UPDATE t:{i} SET vec = [{i}.0, 1.0] RETURN NONE;"))
		.chain((200..260).map(|i| format!("CREATE t:{i} SET vec = [{i}.0, 0.0] RETURN NONE;")))
		.collect();
	execute_all_retrying_conflicts(&ds, &session, &writes).await?;
	drop(guard);
	wait_for_index_ready(&ds, &session, "t", "dx").await?;
	let building = index_building_json(&ds, &session, "t", "dx").await?;
	assert!(
		building.get("updated").and_then(|updated| updated.as_u64()).is_some_and(|n| n > 0),
		"no write was queued by the build: {building}"
	);
	Datastore::index_compaction(Arc::clone(&ds), Duration::from_secs(10), CancellationToken::new())
		.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "t", "dx").await?;
	let ikb = IndexKeyBase::new(ns, db, table, ix.index_id);
	assert_eq!(count_diskann_pendings(&ds, &ikb).await?, 0, "pendings were left unfolded");
	expect_indexed_query_len(
		&ds,
		&session,
		"SELECT id FROM t WHERE vec <|260,400|> [0.0, 0.0]",
		260,
	)
	.await?;
	expect_indexed_query_len(&ds, &session, "SELECT id FROM t WHERE vec <|1,40|> [4.0, 1.0]", 1)
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

/// A blocking `DEFINE INDEX … DISKANN` whose statement goes away during its
/// phase-2 wait — its future dropped, or its query cancelled — while the wait's
/// pass folds the build's backlog.
///
/// Each frame of the pass runs in a task of its own, so the frame in progress
/// ends on its own: it commits, or the cancellation stops it and it commits
/// nothing. Either way no plan is left half-applied, and a later pass folds
/// whatever is left, into a graph that answers every query.
#[cfg(all(feature = "kv-mem", diskann))]
#[tokio::test(flavor = "multi_thread")]
async fn a_diskann_frame_outlives_its_phase_two_wait() -> Result<()> {
	for cancelled in [false, true] {
		let (ds, session) = new_index_test_ds().await?;
		// The pass under test folds what a build's scan queues, which the DiskANN sink would not.
		let _sink_ds = super::kind::test_sink::off(ds.id());
		execute_all(
			&ds,
			&session,
			"CREATE |t:1500| SET vec = [rand::float(), rand::float(), rand::float(), rand::float(),
				rand::float(), rand::float(), rand::float(), rand::float()] RETURN NONE",
		)
		.await?;
		let cancel = crate::ctx::CancelHandle::new();
		let run = tokio::spawn({
			let (ds, session, cancel) = (Arc::clone(&ds), session.clone(), cancel.clone());
			async move {
				run_cancellable(
					&ds,
					&session,
					"DEFINE INDEX dx ON t FIELDS vec DISKANN DIMENSION 8 DIST EUCLIDEAN TYPE F32",
					cancel,
				)
				.await
			}
		});
		// The definition is visible once it has committed; phase 2 is under way
		// once phase 1 has transitioned the index to Online (`ready`) with a
		// backlog to fold (`compacting`). The backlog alone does not say so: the
		// build's scan queues it during phase 1, and a cancellation then takes
		// the definition back.
		timeout(Duration::from_secs(30), async {
			while get_table_index(&ds, "t", "dx").await.is_err()
				|| index_building_status(&ds, &session, "t", "dx").await.ok().as_deref()
					!= Some("ready")
				|| !compacting(&ds, &session, "dx").await.unwrap_or(false)
			{
				sleep(Duration::from_millis(5)).await;
			}
		})
		.await?;
		sleep(Duration::from_millis(20)).await;
		if cancelled {
			cancel.trip();
			let errors = timeout(Duration::from_secs(30), run).await???;
			assert!(
				errors
					.iter()
					.all(|e| e.as_ref().is_none_or(|e| e.starts_with("The index was committed"))),
				"{errors:?}"
			);
		} else {
			run.abort();
			let _ = run.await;
		}
		timeout(Duration::from_secs(60), async {
			while compacting(&ds, &session, "dx").await? {
				Datastore::index_compaction(
					Arc::clone(&ds),
					Duration::from_secs(1),
					CancellationToken::new(),
				)
				.await?;
			}
			Ok::<_, anyhow::Error>(())
		})
		.await??;
		let rows = ds
			.execute(
				"SELECT id FROM t WHERE vec <|10,100|> [0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5]",
				&session,
				None,
			)
			.await?
			.remove(0)
			.result?;
		let crate::types::PublicValue::Array(rows) = rows else {
			panic!("a KNN query answers rows: {rows:?}");
		};
		assert_eq!(rows.len(), 10, "cancelled={cancelled}");
		assert_eq!(index_building_status(&ds, &session, "t", "dx").await?, "ready");
	}
	Ok(())
}

/// The full-text half of [`info_for_index_reports_an_undrained_compaction_queue`].
///
/// A full-text read folds its term and statistics deltas into the compacted
/// state it reads, so a backlog costs every query. `compacting` reports it
/// until a compaction folds it, and then reports a settled index although the
/// compacted bitmaps and the statistics root remain.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn info_for_index_reports_an_undrained_fulltext_backlog() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase;
		 DEFINE INDEX ft ON t FIELDS text FULLTEXT ANALYZER simple BM25;
		 CREATE t:1 SET text = 'alpha beta';
		 CREATE t:2 SET text = 'beta gamma';",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "t", "ft").await?;
	let ikb = IndexKeyBase::new(ns, db, table, ix.index_id);
	assert!(fulltext_deltas(&ds, &ikb).await? > 0, "the writes must leave deltas to fold");

	let building = index_building_json(&ds, &session, "t", "ft").await?;
	assert_eq!(
		building.get("compacting").and_then(|v| v.as_bool()),
		Some(true),
		"the writes' deltas are not folded yet: {building}"
	);

	Datastore::index_compaction(Arc::clone(&ds), Duration::from_secs(1), CancellationToken::new())
		.await?;

	assert_eq!(fulltext_deltas(&ds, &ikb).await?, 0);
	let building = index_building_json(&ds, &session, "t", "ft").await?;
	assert_eq!(
		building.get("compacting").and_then(|v| v.as_bool()),
		Some(false),
		"the deltas are folded, and the compacted state is not a backlog: {building}"
	);
	expect_indexed_query_len(&ds, &session, "SELECT id FROM t WHERE text @@ 'beta'", 2).await?;
	Ok(())
}

/// The count half of [`info_for_index_reports_an_undrained_compaction_queue`].
///
/// A count sums every `!iu` delta beside its aggregate. The aggregate, at the
/// `None` bound, is compacted state, so an index holding only it is settled.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn info_for_index_reports_an_undrained_count_backlog() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE INDEX cx ON t COUNT;
		 CREATE t:1;
		 CREATE t:2;",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "t", "cx").await?;
	let ikb = IndexKeyBase::new(ns, db, table, ix.index_id);
	assert!(count_deltas(&ds, &ikb).await? > 0, "the writes must leave deltas to fold");

	let building = index_building_json(&ds, &session, "t", "cx").await?;
	assert_eq!(
		building.get("compacting").and_then(|v| v.as_bool()),
		Some(true),
		"the writes' deltas are not folded yet: {building}"
	);

	Datastore::index_compaction(Arc::clone(&ds), Duration::from_secs(1), CancellationToken::new())
		.await?;

	assert_eq!(count_deltas(&ds, &ikb).await?, 0);
	let building = index_building_json(&ds, &session, "t", "cx").await?;
	assert_eq!(
		building.get("compacting").and_then(|v| v.as_bool()),
		Some(false),
		"only the aggregate remains: {building}"
	);
	Ok(())
}

/// The compacted documents and residual of one term of a full-text index.
#[cfg(feature = "kv-mem")]
async fn term_docs_and_residual(
	ds: &Datastore,
	ikb: &IndexKeyBase,
	term: &str,
) -> Result<(Option<roaring::RoaringTreemap>, Option<TermDocsResidual>)> {
	let residual = crate::key::schema::TermDocsResidualKey {
		ns: ikb.ns(),
		db: ikb.db(),
		tb: Cow::Borrowed(ikb.table()),
		ix: ikb.index(),
		term: Cow::Borrowed(term),
	};
	let tx = ds.transaction(TransactionType::Read).await?;
	let docs = catch!(tx, compacted_term_docs(&tx, ikb, term).await);
	let residual = catch!(tx, tx.get_key(&residual, None).await);
	tx.cancel().await?;
	Ok((docs, residual))
}

/// The documents `term`'s compacted state holds, as the index's format keeps
/// them: the term's bitmap (`!td`), or the documents of its posting blocks
/// (`!tk`, `!tp`) on an index that keeps its postings in blocks. `None` where
/// the term holds neither.
#[cfg(feature = "kv-mem")]
async fn compacted_term_docs(
	tx: &crate::kvs::Transaction,
	ikb: &IndexKeyBase,
	term: &str,
) -> Result<Option<roaring::RoaringTreemap>> {
	let bitmap = crate::key::schema::TermDocsKey {
		ns: ikb.ns(),
		db: ikb.db(),
		tb: Cow::Borrowed(ikb.table()),
		ix: ikb.index(),
		term: Cow::Borrowed(term),
	};
	if let Some(docs) = tx.get_key(&bitmap, None).await? {
		return Ok(Some(docs));
	}
	let listed = crate::key::schema::TermBlocksKey {
		ns: ikb.ns(),
		db: ikb.db(),
		tb: Cow::Borrowed(ikb.table()),
		ix: ikb.index(),
		term: Cow::Borrowed(term),
	};
	let Some(listed) = tx.get_key(&listed, None).await? else {
		return Ok(None);
	};
	let mut docs = roaring::RoaringTreemap::new();
	for summary in &listed.blocks {
		let key = crate::key::schema::TermBlockKey {
			ns: ikb.ns(),
			db: ikb.db(),
			tb: Cow::Borrowed(ikb.table()),
			ix: ikb.index(),
			term: Cow::Borrowed(term),
			start: summary.start,
		};
		let block = tx
			.get_key(&key, None)
			.await?
			.ok_or_else(|| anyhow::anyhow!("a listed posting block is missing"))?;
		let entries =
			block.entries().ok_or_else(|| anyhow::anyhow!("failed to decode a posting block"))?;
		docs.extend(entries.into_iter().map(|(doc, _, _)| doc));
	}
	Ok(Some(docs))
}

/// A full-text index whose backlog is folded, with no request queued, and a
/// residual written for `term` with nothing beside it: the state a compactor
/// that predates the residual family leaves, once it has folded the term's
/// deltas into its compacted state and deleted the request that led to its
/// pass.
///
/// The residual takes `doc`, which the term's compacted state holds, back out
/// of the term, so a fold that ignored it would answer differently.
#[cfg(feature = "kv-mem")]
async fn strand_a_residual(ds: &Datastore, ikb: &IndexKeyBase, term: &str, doc: u64) -> Result<()> {
	let residual = crate::key::schema::TermDocsResidualKey {
		ns: ikb.ns(),
		db: ikb.db(),
		tb: Cow::Borrowed(ikb.table()),
		ix: ikb.index(),
		term: Cow::Borrowed(term),
	};
	let tx = ds.transaction(TransactionType::Write).await?;
	let held = catch!(tx, compacted_term_docs(&tx, ikb, term).await).unwrap_or_default();
	assert!(held.contains(doc), "the stranded residual must take back a document the term holds");
	catch!(tx, tx.set_key(&residual, &TermDocsResidual::new([(doc, -1)])).await);
	tx.commit().await?;
	Ok(())
}

/// A full-text index over three records, its backlog folded and no request
/// left queued.
#[cfg(feature = "kv-mem")]
async fn settled_fulltext_index(ds: &Arc<Datastore>, session: &Session) -> Result<IndexKeyBase> {
	execute_all(
		ds,
		session,
		"DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase;
		 DEFINE INDEX ft ON t FIELDS text FULLTEXT ANALYZER simple;
		 CREATE t:1 SET text = 'alpha beta';
		 CREATE t:2 SET text = 'beta gamma';
		 CREATE t:3 SET text = 'beta delta';",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(ds, "t", "ft").await?;
	let ikb = IndexKeyBase::new(ns, db, table, ix.index_id);
	Datastore::index_compaction(Arc::clone(ds), Duration::from_secs(60), CancellationToken::new())
		.await?;
	assert_eq!(fulltext_deltas(ds, &ikb).await?, 0);
	assert_eq!(compaction_requests(ds, &ikb).await?, 0);
	let building = index_building_json(ds, session, "t", "ft").await?;
	assert_eq!(building.get("compacting").and_then(|v| v.as_bool()), Some(false), "{building}");
	Ok(ikb)
}

/// A residual stranded with no delta beside it and no request queued is found
/// by the next drain of the compaction queue, through its sweep of the
/// defined indexes, and folded into its term's bitmap.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn index_compaction_sweeps_up_a_stranded_residual() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	let ikb = settled_fulltext_index(&ds, &session).await?;
	let (held, _) = term_docs_and_residual(&ds, &ikb, "beta").await?;
	let held = held.expect("the folded term holds documents");
	let doc = held.iter().next().expect("the bitmap holds a document");
	strand_a_residual(&ds, &ikb, "beta", doc).await?;

	let building = index_building_json(&ds, &session, "t", "ft").await?;
	assert_eq!(
		building.get("compacting").and_then(|v| v.as_bool()),
		Some(true),
		"a residual is a backlog: {building}"
	);
	expect_indexed_query_len(&ds, &session, "SELECT id FROM t WHERE text @@ 'beta'", 2).await?;

	// An already-running datastore has swept once; this drain is the next
	// lease period's.
	ds.expire_compaction_sweep();
	Datastore::index_compaction(Arc::clone(&ds), Duration::from_secs(60), CancellationToken::new())
		.await?;

	let (docs, residual) = term_docs_and_residual(&ds, &ikb, "beta").await?;
	assert!(residual.is_none(), "the residual must be folded and removed");
	let mut expected = held;
	expected.remove(doc);
	assert_eq!(docs, Some(expected), "the compacted documents must hold the residual's total");
	assert_eq!(compaction_requests(&ds, &ikb).await?, 0, "the sweep's request is drained");
	let building = index_building_json(&ds, &session, "t", "ft").await?;
	assert_eq!(building.get("compacting").and_then(|v| v.as_bool()), Some(false), "{building}");
	expect_indexed_query_len(&ds, &session, "SELECT id FROM t WHERE text @@ 'beta'", 2).await?;
	Ok(())
}

/// The sweep runs at most once per lease period: a residual stranded after a
/// drain swept waits for the next period, and is folded by the first drain
/// in it.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn index_compaction_sweeps_at_most_once_per_lease_period() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	// The drain that settled the index swept once it had drained the queue.
	let ikb = settled_fulltext_index(&ds, &session).await?;
	let (held, _) = term_docs_and_residual(&ds, &ikb, "beta").await?;
	let doc = held.and_then(|held| held.iter().next()).expect("the bitmap holds a document");
	strand_a_residual(&ds, &ikb, "beta", doc).await?;

	Datastore::index_compaction(Arc::clone(&ds), Duration::from_secs(60), CancellationToken::new())
		.await?;
	let (_, residual) = term_docs_and_residual(&ds, &ikb, "beta").await?;
	assert!(residual.is_some(), "a drain within the period must not sweep again");
	assert_eq!(compaction_requests(&ds, &ikb).await?, 0);

	ds.expire_compaction_sweep();
	Datastore::index_compaction(Arc::clone(&ds), Duration::from_secs(60), CancellationToken::new())
		.await?;
	let (_, residual) = term_docs_and_residual(&ds, &ikb, "beta").await?;
	assert!(residual.is_none(), "the next period's drain sweeps, and folds the residual");
	Ok(())
}

/// A drain that empties the queue sweeps in the same call: a request for one
/// index is drained, and a residual stranded on another index with no
/// request is then found and folded, before the call returns.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_drain_that_empties_the_queue_sweeps_in_the_same_call() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	let ikb = settled_fulltext_index(&ds, &session).await?;
	let (held, _) = term_docs_and_residual(&ds, &ikb, "beta").await?;
	let doc = held.and_then(|held| held.iter().next()).expect("the bitmap holds a document");
	strand_a_residual(&ds, &ikb, "beta", doc).await?;
	execute_all(&ds, &session, "DEFINE INDEX cx ON c COUNT; CREATE c:1; CREATE c:2;").await?;
	let (ns, db, table, cx) = get_table_index(&ds, "c", "cx").await?;
	let cx = IndexKeyBase::new(ns, db, table, cx.index_id);
	assert!(compaction_requests(&ds, &cx).await? > 0, "the count index has requests queued");
	assert_eq!(compaction_requests(&ds, &ikb).await?, 0, "the residual has none");

	ds.expire_compaction_sweep();
	Datastore::index_compaction(Arc::clone(&ds), Duration::from_secs(60), CancellationToken::new())
		.await?;

	assert_eq!(compaction_requests(&ds, &cx).await?, 0, "the queue was drained");
	assert_eq!(count_deltas(&ds, &cx).await?, 0);
	let (_, residual) = term_docs_and_residual(&ds, &ikb, "beta").await?;
	assert!(residual.is_none(), "the same call swept and folded the residual");
	Ok(())
}

/// A sweep over a store with nothing left to fold queues no request.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn index_compaction_sweep_over_a_settled_store_queues_nothing() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	let ikb = settled_fulltext_index(&ds, &session).await?;
	execute_all(&ds, &session, "DEFINE INDEX cx ON t COUNT; DEFINE INDEX bx ON t FIELDS text;")
		.await?;
	Datastore::index_compaction(Arc::clone(&ds), Duration::from_secs(60), CancellationToken::new())
		.await?;
	let (ns, db, table, cx) = get_table_index(&ds, "t", "cx").await?;
	let cx = IndexKeyBase::new(ns, db, table, cx.index_id);
	assert_eq!(count_deltas(&ds, &cx).await?, 0);

	ds.expire_compaction_sweep();
	let (batches, errors) = Datastore::index_compaction(
		Arc::clone(&ds),
		Duration::from_secs(60),
		CancellationToken::new(),
	)
	.await?;
	assert_eq!((batches, errors), (0, 0), "a sweep that queued nothing drains nothing");
	assert_eq!(compaction_requests(&ds, &ikb).await?, 0);
	assert_eq!(compaction_requests(&ds, &cx).await?, 0);
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
	// build looks like from another node while the statement that started it
	// is still open.
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

/// Start a build the way a statement does: inside a write transaction that
/// stays open across it and is the caller's to close.
///
/// An index the catalog already holds is built against its committed
/// definition, as `REBUILD INDEX` does. A new one is first staged in the open
/// transaction, as `DEFINE INDEX` does — its definition written and the table's
/// index-cache stamp refreshed — so no other session, the compactor included,
/// can see it until the caller commits.
///
/// Returns as soon as the build is started: with the completion a blocking
/// build resolves, and without one for a build only spawned.
#[cfg(feature = "kv-mem")]
async fn start_index_build_inside_an_open_statement_transaction(
	ds: &Arc<Datastore>,
	session: &Session,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
	ix: Arc<IndexDefinition>,
	blocking: bool,
) -> Result<(Arc<crate::kvs::Transaction>, Option<BuildCompletion>)> {
	let txn = Arc::new(ds.transaction(TransactionType::Write).await?);
	let table_def = catch!(txn, txn.get_tb(ns, db, table, None).await).expect("table should exist");
	let mut ctx = ds.setup_ctx()?;
	ctx.set_transaction(Arc::clone(&txn));
	let ctx = ctx.freeze();
	let opt = ds.setup_options(session);
	if catch!(txn, txn.get_tb_index_by_id(ns, db, table, ix.index_id, None).await).is_none() {
		catch!(txn, txn.put_tb_index(ns, db, table, &ix).await);
		let (ns_name, db_name) = catch!(txn, opt.ns_db());
		let restamped = crate::catalog::TableDefinition {
			cache_indexes_ts: Uuid::now_v7(),
			..(*table_def).clone()
		};
		catch!(txn, txn.replace_tb(ns_name, db_name, &restamped).await);
		txn.clear_cache();
	}
	let builder = ctx.get_index_builder().expect("index builder should exist").clone();
	let completion = catch!(txn, builder.build(&ctx, opt, table_def.table_id, ix, blocking).await);
	Ok((txn, completion))
}

/// Run a build the way a statement does, as
/// [`start_index_build_inside_an_open_statement_transaction`] starts it.
///
/// A `blocking` build returns once the build task has exited, so a caller can
/// assert on what the finished build left behind while its statement is still
/// uncommitted. Any other build returns once its task is spawned, as the task
/// waits for the statement's transaction to close before it reads the catalog.
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
	let index_id = ix.index_id;
	let (txn, completion) = start_index_build_inside_an_open_statement_transaction(
		ds, session, ns, db, table, ix, blocking,
	)
	.await?;
	if let Some(completion) = completion {
		completion.await?;
	} else {
		local_builder_for_key(ds, ns, db, table, index_id)
			.await?
			.expect("the build registers a local builder");
	}
	Ok(txn)
}

/// The kinds of index whose build is not fenced off background compaction.
#[cfg(feature = "kv-mem")]
#[derive(Clone, Copy, Debug)]
enum UnfencedKind {
	Count,
	FullText,
}

/// The `!ic` compaction requests queued for one index.
#[cfg(feature = "kv-mem")]
async fn compaction_requests(ds: &Datastore, ikb: &IndexKeyBase) -> Result<usize> {
	let range = crate::key::schema::IndexCompactionIxPrefix {
		ns: ikb.ns(),
		db: ikb.db(),
		tb: Cow::Borrowed(ikb.table()),
		ix: ikb.index(),
	}
	.range()?;
	let tx = ds.transaction(TransactionType::Read).await?;
	let keys = catch!(tx, tx.keys(range, u32::MAX, 0, None).await);
	tx.cancel().await?;
	Ok(keys.len())
}

/// The `!iu` deltas a count compaction folds: every entry but the aggregate.
#[cfg(feature = "kv-mem")]
async fn count_deltas(ds: &Datastore, ikb: &IndexKeyBase) -> Result<usize> {
	let range = IndexCountPrefix {
		ns: ikb.ns(),
		db: ikb.db(),
		tb: Cow::Borrowed(ikb.table()),
		ix: ikb.index(),
	}
	.range()?;
	let tx = ds.transaction(TransactionType::Read).await?;
	let keys = catch!(tx, tx.keys(range, u32::MAX, 0, None).await);
	tx.cancel().await?;
	let mut deltas = 0;
	for key in keys {
		if IndexCountKey::decode_key(&key)?.uid.is_some() {
			deltas += 1;
		}
	}
	Ok(deltas)
}

/// The `!tx` and `!dx` deltas a full-text compaction folds.
#[cfg(feature = "kv-mem")]
async fn fulltext_deltas(ds: &Datastore, ikb: &IndexKeyBase) -> Result<usize> {
	let terms = crate::key::schema::TermChangeBatchPrefix {
		ns: ikb.ns(),
		db: ikb.db(),
		tb: Cow::Borrowed(ikb.table()),
		ix: ikb.index(),
	}
	.range()?;
	let docs = crate::key::schema::DocStatsBatchPrefix {
		ns: ikb.ns(),
		db: ikb.db(),
		tb: Cow::Borrowed(ikb.table()),
		ix: ikb.index(),
	}
	.range()?;
	let tx = ds.transaction(TransactionType::Read).await?;
	let terms = catch!(tx, tx.keys(terms, u32::MAX, 0, None).await).len();
	let docs = catch!(tx, tx.keys(docs, u32::MAX, 0, None).await).len();
	tx.cancel().await?;
	Ok(terms + docs)
}

/// A full-text or count build runs under a live owner while the statement
/// defining its index is still open, and what it writes queues compaction
/// requests for an index no other transaction can read yet. A drain in that
/// window keeps one of those requests, and only one. The statement's commit
/// releases the build, which queues the one request a release owes beside the
/// kept one, and the next drain folds the build's deltas.
#[cfg(feature = "kv-mem")]
async fn assert_uncommitted_definition_keeps_a_request(kind: UnfencedKind) -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase;
		 DEFINE TABLE tpl SCHEMALESS;
		 DEFINE INDEX ft ON tpl FIELDS text FULLTEXT ANALYZER simple BM25;
		 DEFINE TABLE t SCHEMALESS;
		 CREATE t:1 SET text = 'alpha beta';
		 CREATE t:2 SET text = 'beta gamma';
		 CREATE t:3 SET text = 'gamma delta';",
	)
	.await?;
	let (ns, db, table) = get_table_ids(&ds, "t").await?;
	let (_, _, _, template) = get_table_index(&ds, "tpl", "ft").await?;
	let ix = Arc::new(IndexDefinition {
		index_id: IndexId(1000),
		name: "ix".into(),
		table_name: table.clone(),
		cols: template.cols.clone(),
		index: match kind {
			UnfencedKind::Count => Index::Count(None),
			UnfencedKind::FullText => template.index.clone(),
		},
		count_cond: None,
		comment: None,
		prepare_remove: false,
		format_version: template.format_version,
		nulls_not_distinct: false,
	});
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);

	// The statement: it stages the definition, then builds against it.
	let txn = Arc::new(ds.transaction(TransactionType::Write).await?);
	catch!(txn, txn.put_tb_index(ns, db, &table, &ix).await);
	let table_def =
		catch!(txn, txn.get_tb(ns, db, &table, None).await).expect("table should exist");
	let mut ctx = ds.setup_ctx()?;
	ctx.set_transaction(Arc::clone(&txn));
	let ctx = ctx.freeze();
	let builder = ctx.get_index_builder().expect("index builder should exist").clone();
	let completion = builder
		.build(&ctx, ds.setup_options(&session), table_def.table_id, Arc::clone(&ix), true)
		.await?
		.expect("a blocking build reports its result");
	completion.await?;

	{
		let tx = ds.transaction(TransactionType::Read).await?;
		let visible = catch!(tx, tx.get_tb_index_by_id(ns, db, &table, ix.index_id, None).await);
		let owned = catch!(tx, super::state::build_owner_is_live(&tx, &ikb).await);
		tx.cancel().await?;
		assert!(visible.is_none(), "the definition must not be readable before its commit");
		assert!(owned, "the build must hold its index until its statement closes");
	}
	let backlog = || async {
		match kind {
			UnfencedKind::Count => count_deltas(&ds, &ikb).await,
			UnfencedKind::FullText => fulltext_deltas(&ds, &ikb).await,
		}
	};
	let built = backlog().await?;
	assert!(built > 0, "the build must leave deltas for a compaction to fold: {kind:?}");
	// Requests beside the build's own, so keeping one is told apart from
	// keeping them all.
	{
		let tx = ds.transaction(TransactionType::Write).await?;
		for _ in 0..3 {
			catch!(tx, tx.set_key(&ikb.new_ic_key(ds.id()), &()).await);
		}
		tx.commit().await?;
	}

	Datastore::index_compaction(Arc::clone(&ds), Duration::from_secs(60), CancellationToken::new())
		.await?;
	assert_eq!(
		compaction_requests(&ds, &ikb).await?,
		1,
		"a drain during the open statement must keep exactly one request: {kind:?}"
	);
	assert_eq!(backlog().await?, built, "nothing can be folded before the commit: {kind:?}");

	txn.commit().await?;
	assert_eq!(
		compaction_requests(&ds, &ikb).await?,
		2,
		"the kept request and the release's own must be the only ones after the commit: {kind:?}"
	);
	Datastore::index_compaction(Arc::clone(&ds), Duration::from_secs(60), CancellationToken::new())
		.await?;
	assert_eq!(backlog().await?, 0, "the next drain must fold the backlog: {kind:?}");
	if let UnfencedKind::Count = kind {
		assert_eq!(
			count_index_delta_keys(&ds, ns, db, &table, ix.index_id).await?,
			1,
			"a folded count index is its single aggregate"
		);
	}
	assert_eq!(compaction_requests(&ds, &ikb).await?, 0);
	Ok(())
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn index_compaction_keeps_a_request_for_a_count_index_defined_by_an_open_statement()
-> Result<()> {
	assert_uncommitted_definition_keeps_a_request(UnfencedKind::Count).await
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn index_compaction_keeps_a_request_for_a_fulltext_index_defined_by_an_open_statement()
-> Result<()> {
	assert_uncommitted_definition_keeps_a_request(UnfencedKind::FullText).await
}

/// `CONCURRENTLY` returns as soon as the build task is spawned, while its
/// statement transaction, the one staging the definition the build is made
/// against, is still uncommitted. The build holds ownership and reads nothing
/// until that transaction closes, then builds and releases ownership.
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
	let building = local_builder_for_key(&ds, ns, db, &table, ix.index_id)
		.await?
		.expect("the build registers a local builder");

	sleep(Duration::from_millis(300)).await;
	assert!(!building.is_finished(), "a concurrent build must wait for its statement to close");
	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(state.phase, IndexBuildPhase::Building);
	assert!(
		state.owner.is_some(),
		"a concurrent build must hold the fence until its statement transaction closes"
	);

	txn.commit().await?;
	wait_for_finished_builder(&building).await?;

	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(state.phase, IndexBuildPhase::Online);
	assert_eq!(state.owner, None, "the build must release the fence once both halves arrive");
	Ok(())
}

/// A statement transaction dropped without being committed or cancelled runs
/// neither of its hooks, so the half of the gate a concurrent build waits for
/// never arrives. The build stops waiting once that transaction is gone,
/// without building, and ends `aborted` with its ownership cleared, as a
/// rolled-back statement's build does.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_concurrent_build_whose_statement_transaction_is_dropped_stops_waiting() -> Result<()> {
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
	let building = local_builder_for_key(&ds, ns, db, &table, ix.index_id)
		.await?
		.expect("the build registers a local builder");

	drop(txn);
	wait_for_finished_builder(&building).await?;

	let state = durable_build_state(&ds, &ikb).await?;
	assert!(!state.initial_complete, "the build must not have scanned: {state:?}");
	assert_eq!(state.initial, None, "the build must not have scanned: {state:?}");
	assert_eq!(
		state.phase,
		IndexBuildPhase::Error,
		"a build whose statement never committed must end as a rolled-back one does: {state:?}"
	);
	assert_eq!(state.report_status, Some(IndexBuildReportStatus::Aborted));
	assert_eq!(state.owner, None, "a build no task runs must not keep its ownership");
	Ok(())
}

/// Rolling back the statement that spawned a concurrent build aborts the build
/// before it scans and clears durable ownership.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_concurrent_build_whose_statement_rolls_back_aborts_without_building() -> Result<()> {
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
	let building = local_builder_for_key(&ds, ns, db, &table, ix.index_id)
		.await?
		.expect("the build registers a local builder");

	txn.cancel().await?;
	wait_for_finished_builder(&building).await?;

	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(state.phase, IndexBuildPhase::Error, "the rolled-back build must be aborted");
	assert_eq!(state.report_status, Some(IndexBuildReportStatus::Aborted));
	assert_eq!(state.owner, None, "aborting must release durable build ownership");
	Ok(())
}

/// A `CONCURRENTLY` build waiting for its statement's transaction keeps its
/// owner lease, however long that transaction stays open: a lapsed lease would
/// let another builder take the generation over and build it from the
/// definition committed before the statement.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_concurrent_build_renews_its_lease_while_its_statement_stays_open() -> Result<()> {
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
	let building = local_builder_for_key(&ds, ns, db, &table, ix.index_id)
		.await?
		.expect("the build registers a local builder");
	// As if the transaction had been open for longer than the lease.
	let mut state = durable_build_state(&ds, &ikb).await?;
	let expired = Utc::now() - chrono::Duration::seconds(BUILD_OWNER_LEASE_SECS + 5);
	state.updated_at = expired;
	state.owner_heartbeat_at = Some(expired);
	set_durable_build_state(&ds, &ikb, state).await?;

	timeout(Duration::from_secs(10), async {
		while build_owner_expired(&durable_build_state(&ds, &ikb).await?, Utc::now()) {
			sleep(BUILD_WAITING_HEARTBEAT).await;
		}
		anyhow::Ok(())
	})
	.await
	.map_err(|_| anyhow::anyhow!("the waiting build never renewed its lease"))??;
	assert!(!building.is_finished(), "the build must still be waiting for its statement");
	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(state.phase, IndexBuildPhase::Building);
	assert!(state.owner.is_some(), "the renewal keeps the build's ownership");

	txn.commit().await?;

	wait_for_finished_builder(&building).await?;
	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(state.phase, IndexBuildPhase::Online);
	assert_eq!(state.owner, None, "the commit and the finished build must release the fence");
	Ok(())
}

/// A `CONCURRENTLY` build started inside a save point scope still waits for its
/// statement's transaction when that scope rolls back. The rollback undoes the
/// scope's writes; the transaction the build waits for is still open.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_concurrent_build_started_in_a_rolled_back_save_point_waits_for_the_commit() -> Result<()>
{
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

	let txn = Arc::new(ds.transaction(TransactionType::Write).await?);
	{
		let table_def =
			catch!(txn, txn.get_tb(ns, db, &table, None).await).expect("table should exist");
		let mut ctx = ds.setup_ctx()?;
		ctx.set_transaction(Arc::clone(&txn));
		let ctx = ctx.freeze();
		let builder = ctx.get_index_builder().expect("index builder should exist").clone();
		txn.new_save_point().await?;
		builder
			.build(&ctx, ds.setup_options(&session), table_def.table_id, Arc::clone(&ix), false)
			.await?;
		txn.rollback_to_save_point().await?;
	}
	let building = local_builder_for_key(&ds, ns, db, &table, ix.index_id)
		.await?
		.expect("the build registers a local builder");
	// Ample time for a build of one record that does not wait to finish.
	sleep(Duration::from_millis(200)).await;
	assert!(!building.is_finished(), "the build must wait for its statement's transaction");

	txn.commit().await?;

	wait_for_finished_builder(&building).await?;
	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(state.phase, IndexBuildPhase::Online);
	assert_eq!(state.owner, None, "the commit and the finished build must release the fence");
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

/// The fence outlives the build: the build leaves its pendings, and a write
/// committing before the statement's commit queues one more, all of which the
/// fenced compaction turns away. Once the statement commits and the fence
/// comes down, the next drain has a request to act on and folds them.
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
	let built = count_hnsw_pendings(&ds, &ikb).await?;
	assert!(built > 0, "the build folds nothing, so it reports with its pendings queued");

	// A write in the window the fence outlives: the build has published, and
	// the statement that publishes its definition is still open.
	execute_all(&ds, &session, "CREATE t:2 SET vec = [0.0, 1.0]").await?;
	let pendings = count_hnsw_pendings(&ds, &ikb).await?;
	assert!(pendings > built, "the write must queue a pending for compaction to consume");

	// Consumes the request that write queued without acting on it.
	Datastore::index_compaction(Arc::clone(&ds), Duration::from_secs(1), CancellationToken::new())
		.await?;
	assert_eq!(
		count_hnsw_pendings(&ds, &ikb).await?,
		pendings,
		"the fence must keep compaction off the pendings"
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
/// otherwise a rolled-back rebuild strands the index with no maintenance. The
/// committed definition describes the layout the build wrote, so the
/// generation it published still serves it and stays `Online`.
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
	assert_eq!(
		state.phase,
		IndexBuildPhase::Online,
		"a build of the committed definition must stay published"
	);
	Ok(())
}

/// A rolled-back statement whose build was made against a definition other
/// than the committed one withdraws the generation it published: an index
/// committed at format 0 reads its legacy doc-ID layout, not the one a build
/// at the current format wrote, so `Online` would report it ready while it
/// answered from data it cannot find.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_rolled_back_build_of_an_uncommitted_definition_is_withdrawn() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE INDEX hx ON t FIELDS vec HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32;",
	)
	.await?;
	// The build is handed the current-format definition, as a rebuild of a
	// legacy index is, while the catalog keeps the legacy one.
	let (ns, db, table, ix) = get_table_index(&ds, "t", "hx").await?;
	downgrade_index_format(&ds, "t", "hx", 0).await?;
	execute_all(&ds, &session, "CREATE t:1 SET vec = [1.0, 0.0];").await?;
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
	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(state.phase, IndexBuildPhase::Online, "the build publishes before its statement");
	txn.cancel().await?;

	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(state.owner, None, "a rolled-back statement must not leave the index fenced");
	assert_eq!(
		state.phase,
		IndexBuildPhase::Error,
		"a build of a definition the rollback took back must not stay published"
	);
	assert_eq!(state.report_status, Some(IndexBuildReportStatus::Aborted));
	Ok(())
}

/// Lapse whatever owner lease the build state of `ikb` holds, as time would,
/// and run the resume scan, which settles a lapsed owner or carries on a
/// stalled build through a builder of its own. Answers how many builds the
/// scan carried on.
#[cfg(feature = "kv-mem")]
async fn lapse_and_run_the_resume_scan(ds: &Arc<Datastore>, ikb: &IndexKeyBase) -> Result<u64> {
	let expired = Utc::now() - chrono::Duration::seconds(BUILD_OWNER_LEASE_SECS + 5);
	let mut lapsed = durable_build_state(ds, ikb).await?;
	lapsed.updated_at = expired;
	lapsed.owner_heartbeat_at = lapsed.owner_heartbeat_at.map(|_| expired);
	set_durable_build_state(ds, ikb, lapsed).await?;
	ds.resume_stalled_index_builds(Duration::from_secs(30), CancellationToken::new())
		.await
		.map(|pass| pass.items)
}

/// An HNSW index stamped at format 0, published `Online` by a build at the
/// current format whose statement then rolled back, with the withdrawal that
/// rollback owes cut short by the datastore's shutdown: the generation is left
/// `Online`, owned by a build no process will settle.
#[cfg(feature = "kv-mem")]
async fn published_build_whose_withdrawal_was_cut_short() -> Result<(Arc<Datastore>, IndexKeyBase)>
{
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE INDEX hx ON t FIELDS vec HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32;",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "t", "hx").await?;
	downgrade_index_format(&ds, "t", "hx", 0).await?;
	execute_all(&ds, &session, "CREATE t:1 SET vec = [1.0, 0.0];").await?;
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
	let shutdown = inject_non_retryable_error(
		NonRetryableErrorSite::ConcurrentIndexWithdrawalShutdown,
		ds.id(),
	);
	txn.cancel().await?;
	drop(shutdown);
	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(state.phase, IndexBuildPhase::Online, "the withdrawal was cut short: {state:?}");
	assert!(state.owner.is_some(), "the cut-short withdrawal left the owner set: {state:?}");
	Ok((ds, ikb))
}

/// Assert that the build state of `ikb` is withdrawn: `Error`, reported
/// `aborted`, with no owner.
#[cfg(feature = "kv-mem")]
async fn assert_withdrawn(ds: &Datastore, ikb: &IndexKeyBase) -> Result<()> {
	let state = durable_build_state(ds, ikb).await?;
	assert_eq!(
		(state.phase, state.report_status, state.owner),
		(IndexBuildPhase::Error, Some(IndexBuildReportStatus::Aborted), None),
		"a build of a definition the rollback took back stayed published: {state:?}"
	);
	Ok(())
}

/// A published build whose statement rolled back to a definition that does
/// not describe it, and whose withdrawal the datastore's shutdown cut short,
/// is withdrawn once its owner's lease lapses: the build state records what
/// the build was made against, so the resume scan, which settles the lapsed
/// owner, finds that the committed definition does not describe it, without
/// the process that started the build.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_published_build_whose_withdrawal_was_cut_short_is_withdrawn_once_its_lease_lapses()
-> Result<()> {
	let (ds, ikb) = published_build_whose_withdrawal_was_cut_short().await?;
	lapse_and_run_the_resume_scan(&ds, &ikb).await?;
	assert_withdrawn(&ds, &ikb).await?;
	// A compaction pass afterwards leaves the withdrawn generation as it is.
	Datastore::index_compaction(Arc::clone(&ds), Duration::from_secs(1), CancellationToken::new())
		.await?;
	assert_withdrawn(&ds, &ikb).await
}

/// [`a_published_build_whose_withdrawal_was_cut_short_is_withdrawn_once_its_lease_lapses`],
/// settled by a compaction pass rather than the resume scan: the pass withdraws
/// the generation in a transaction of its own, rather than fold the build's
/// pendings through the committed definition that does not read them.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_compaction_pass_withdraws_a_published_build_whose_withdrawal_was_cut_short() -> Result<()>
{
	let (ds, ikb) = published_build_whose_withdrawal_was_cut_short().await?;
	assert!(
		compaction_requests(&ds, &ikb).await? > 0,
		"the build must leave compaction requests for the pass to visit"
	);
	let expired = Utc::now() - chrono::Duration::seconds(BUILD_OWNER_LEASE_SECS + 5);
	let mut lapsed = durable_build_state(&ds, &ikb).await?;
	lapsed.updated_at = expired;
	lapsed.owner_heartbeat_at = Some(expired);
	set_durable_build_state(&ds, &ikb, lapsed).await?;
	Datastore::index_compaction(Arc::clone(&ds), Duration::from_secs(1), CancellationToken::new())
		.await?;
	assert_withdrawn(&ds, &ikb).await
}

/// [`a_published_build_whose_withdrawal_was_cut_short_is_withdrawn_once_its_lease_lapses`]
/// on a store held at the `Legacy` feature level, where a lapsed `Online` owner
/// with no recorded basis keeps fencing: one whose state records its basis was
/// written by a builder of this release, so its lapse is settled all the same.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_published_build_whose_withdrawal_was_cut_short_is_withdrawn_on_a_legacy_store()
-> Result<()> {
	let (ds, ikb) = published_build_whose_withdrawal_was_cut_short().await?;
	hold_the_store_at_legacy(&ds).await?;
	lapse_and_run_the_resume_scan(&ds, &ikb).await?;
	assert_withdrawn(&ds, &ikb).await
}

/// An HNSW index stamped at format 0, whose build at the current format, started
/// by a statement still open, stopped on the memory threshold before it
/// published, and whose statement then rolled back with the withdrawal that
/// rollback owes cut short by the datastore's shutdown: the generation is left
/// `Building`, owned by a build no process will settle.
#[cfg(feature = "kv-mem")]
async fn stopped_build_whose_withdrawal_was_cut_short() -> Result<(Arc<Datastore>, IndexKeyBase)> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE INDEX hx ON t FIELDS vec HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32;",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "t", "hx").await?;
	downgrade_index_format(&ds, "t", "hx", 0).await?;
	execute_all(&ds, &session, "CREATE t:1 SET vec = [1.0, 0.0];").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);

	let threshold = inject_non_retryable_error(
		NonRetryableErrorSite::ConcurrentIndexInitialBatchMemoryThreshold,
		ds.id(),
	);
	let (txn, completion) = start_index_build_inside_an_open_statement_transaction(
		&ds,
		&session,
		ns,
		db,
		&table,
		Arc::clone(&ix),
		true,
	)
	.await?;
	completion
		.expect("a blocking build has a completion")
		.await
		.expect_err("the build stops on the injected memory threshold");
	drop(threshold);
	let shutdown = inject_non_retryable_error(
		NonRetryableErrorSite::ConcurrentIndexWithdrawalShutdown,
		ds.id(),
	);
	txn.cancel().await?;
	drop(shutdown);
	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(state.phase, IndexBuildPhase::Building, "the withdrawal was cut short: {state:?}");
	Ok((ds, ikb))
}

/// [`a_published_build_whose_withdrawal_was_cut_short_is_withdrawn_once_its_lease_lapses`]
/// for a build that stopped on the memory threshold before it published: the
/// resume scan withdraws it rather than carry it on from checkpoints written
/// for a definition that never committed.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_stopped_build_whose_withdrawal_was_cut_short_is_not_resumed() -> Result<()> {
	let (ds, ikb) = stopped_build_whose_withdrawal_was_cut_short().await?;
	let resumed = lapse_and_run_the_resume_scan(&ds, &ikb).await?;
	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(resumed, 0, "the resume scan carried on the rolled-back build: {state:?}");
	assert_withdrawn(&ds, &ikb).await
}

/// The resume scan's withdrawal of a stale stopped build fences the
/// statements still open on its table, as clearing a lapsed `Online` owner
/// does: the withdrawal decides from the committed catalog, so a statement
/// that could still commit what the build was made against must not commit
/// after it. Shown with a statement that restamps the table's definition, as
/// `DEFINE INDEX` and `REBUILD INDEX` do.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn withdrawing_a_stale_stopped_build_fences_the_open_statements_on_its_table() -> Result<()> {
	let (ds, ikb) = stopped_build_whose_withdrawal_was_cut_short().await?;
	let session = Session::owner().with_ns("test").with_db("test");
	let (ns, db, table) = (ikb.ns(), ikb.db(), ikb.table().clone());
	let statement = Arc::new(ds.transaction(TransactionType::Write).await?);
	let table_def = catch!(statement, statement.get_tb(ns, db, &table, None).await)
		.expect("table should exist");
	let opt = ds.setup_options(&session);
	let (ns_name, db_name) = catch!(statement, opt.ns_db());
	catch!(
		statement,
		statement
			.replace_tb(
				ns_name,
				db_name,
				&crate::catalog::TableDefinition {
					cache_indexes_ts: Uuid::now_v7(),
					..(*table_def).clone()
				},
			)
			.await
	);

	assert_eq!(lapse_and_run_the_resume_scan(&ds, &ikb).await?, 0);
	assert_withdrawn(&ds, &ikb).await?;
	let err = statement
		.commit()
		.await
		.expect_err("a statement open on the table must not commit after the withdrawal");
	assert!(is_retryable_transaction_conflict(&err), "expected a conflict, got: {err}");
	Ok(())
}

/// A generation recorded with no basis — as every generation written before
/// the build state carried one is — is never withdrawn on that account: what
/// it was built against is unknown, so the scan that settles its lapsed owner
/// clears the owner and leaves it `Online`, as for any lapsed owner.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_lapsed_build_with_no_recorded_basis_is_not_withdrawn() -> Result<()> {
	let (ds, ikb) = published_build_whose_withdrawal_was_cut_short().await?;
	let mut unrecorded = durable_build_state(&ds, &ikb).await?;
	unrecorded.basis = None;
	set_durable_build_state(&ds, &ikb, unrecorded).await?;

	lapse_and_run_the_resume_scan(&ds, &ikb).await?;
	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(
		(state.phase, state.owner),
		(IndexBuildPhase::Online, None),
		"a generation with no recorded basis was withdrawn: {state:?}"
	);
	Ok(())
}

/// A build records what it is made against as it takes ownership, and a
/// lapsed owner of a generation whose basis the committed catalog describes is
/// only cleared: the generation stays `Online`. On a store held at the
/// `Legacy` feature level too, when `legacy`: a recorded basis shows the owner
/// is a builder of this release.
#[cfg(feature = "kv-mem")]
async fn assert_a_lapsed_build_whose_basis_the_catalog_describes_stays_online(
	legacy: bool,
) -> Result<()> {
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
	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(
		state.basis,
		Some(IndexBuildBasis {
			format_version: ix.format_version,
			analyzer: None,
		}),
		"the build did not record what it is made against: {state:?}"
	);
	assert!(state.owner.is_some(), "the open statement still holds the build's ownership");
	if legacy {
		hold_the_store_at_legacy(&ds).await?;
	}

	lapse_and_run_the_resume_scan(&ds, &ikb).await?;
	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(
		(state.phase, state.owner),
		(IndexBuildPhase::Online, None),
		"a generation whose basis the catalog describes was not kept online: {state:?}"
	);
	drop(txn);
	Ok(())
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_lapsed_build_whose_basis_the_catalog_describes_stays_online() -> Result<()> {
	assert_a_lapsed_build_whose_basis_the_catalog_describes_stays_online(false).await
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_lapsed_build_whose_basis_the_catalog_describes_stays_online_on_a_legacy_store()
-> Result<()> {
	assert_a_lapsed_build_whose_basis_the_catalog_describes_stays_online(true).await
}

/// A rollback whose close drain has already used its allowance does not read
/// the committed catalog for a published build: it withdraws the build, the
/// side that never leaves `ready` an index its definition may not read, rather
/// than hold the statement's error behind a read. Shown for a rebuild of an
/// unchanged definition, which a read within the allowance would keep.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_rollback_past_its_drain_allowance_withdraws_a_published_rebuild() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(&ds, &session, "DEFINE INDEX ix ON t FIELDS v; CREATE t:1 SET v = 1;").await?;
	let (ns, db, table, ix) = get_table_index(&ds, "t", "ix").await?;
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
	assert_eq!(durable_build_state(&ds, &ikb).await?.phase, IndexBuildPhase::Online);
	let building = local_builder_for_key(&ds, ns, db, &table, ix.index_id)
		.await?
		.expect("the build registers a local builder");

	let drain_started_at = Instant::now()
		.checked_sub(Duration::from_secs(60))
		.expect("the clock has run for a minute");
	super::builder::ReleaseBuildOwnership::rollback_action(Arc::clone(&building))
		.run(drain_started_at)
		.await?;
	assert_withdrawn(&ds, &ikb).await?;
	// The rollback the transaction then runs finds nothing left to release.
	txn.cancel().await?;
	assert_withdrawn(&ds, &ikb).await
}

/// A rollback whose withdrawal of a published build meets conflicts past the
/// close's allowance stops retrying at the allowance, rather than hold the
/// close behind them. The generation it leaves `Online` records its basis, so
/// once its owner's lease lapses the resume scan withdraws it.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_rollback_gives_up_a_withdrawal_at_its_allowance_and_recovery_finishes_it() -> Result<()>
{
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE INDEX hx ON t FIELDS vec HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32;",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "t", "hx").await?;
	downgrade_index_format(&ds, "t", "hx", 0).await?;
	execute_all(&ds, &session, "CREATE t:1 SET vec = [1.0, 0.0];").await?;
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
	assert_eq!(durable_build_state(&ds, &ikb).await?.phase, IndexBuildPhase::Online);
	let building = local_builder_for_key(&ds, ns, db, &table, ix.index_id)
		.await?
		.expect("the build registers a local builder");

	// The close's allowance ends a second from now, and every build-state
	// transition conflicts for far longer.
	let drain_started_at = Instant::now()
		.checked_sub(super::BUILD_ABORT_STOP_BUDGET - Duration::from_secs(1))
		.expect("the clock has run for the allowance");
	let site = RetryableConflictSite::ConcurrentIndexBuildStateRead;
	let conflicts = inject_retryable_conflicts(site, ds.id(), REPEATED_RETRY_CONFLICTS);
	let rollback = timeout(
		Duration::from_secs(10),
		super::builder::ReleaseBuildOwnership::rollback_action(Arc::clone(&building))
			.run(drain_started_at),
	)
	.await;
	drop(conflicts);
	rollback.map_err(|_| anyhow::anyhow!("the rollback outran its close's allowance"))??;
	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(
		(state.phase, state.owner.is_some()),
		(IndexBuildPhase::Online, true),
		"the withdrawal the allowance cut short leaves the generation for recovery: {state:?}"
	);

	lapse_and_run_the_resume_scan(&ds, &ikb).await?;
	assert_withdrawn(&ds, &ikb).await?;
	txn.cancel().await?;
	assert_withdrawn(&ds, &ikb).await
}

/// A rollback of a statement whose commit's outcome is unknown reads the
/// committed catalog once: the read that finds the catalog describing the
/// build lets it go on, and its release keeps it `Online` without reading
/// again, so no later read, nor the close's allowance running out before it,
/// can withdraw a build that read confirmed.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_rollback_of_a_commit_with_an_unknown_outcome_reads_the_catalog_once() -> Result<()> {
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
	assert_eq!(durable_build_state(&ds, &ikb).await?.phase, IndexBuildPhase::Online);

	let before = super::builder::committed_catalog_reads::count(ds.id());
	commit_with_an_unknown_outcome(&ds, &session, &txn, None).await?;
	let reads = super::builder::committed_catalog_reads::count(ds.id()) - before;
	assert_eq!(reads, 1, "the rollback read the committed catalog {reads} times");
	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(
		(state.phase, state.owner),
		(IndexBuildPhase::Online, None),
		"a build the catalog confirmed was not kept online: {state:?}"
	);
	Ok(())
}

/// The withdrawal of [`a_rolled_back_build_of_an_uncommitted_definition_is_withdrawn`]
/// outlasts more conflicts than a best-effort transition retries: it is the
/// only write that takes the generation out of `Online`, and a lapsed owner
/// cleared later keeps the generation `Online`. However long the lease has
/// lapsed afterwards, the resume scan finds the generation withdrawn.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_withdrawal_outlasts_the_conflicts_a_release_gives_up_on() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE INDEX hx ON t FIELDS vec HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32;",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "t", "hx").await?;
	downgrade_index_format(&ds, "t", "hx", 0).await?;
	execute_all(&ds, &session, "CREATE t:1 SET vec = [1.0, 0.0];").await?;
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
	assert_eq!(durable_build_state(&ds, &ikb).await?.phase, IndexBuildPhase::Online);
	// One conflict for each attempt a best-effort transition makes, and one
	// more.
	let site = RetryableConflictSite::ConcurrentIndexBuildStateRead;
	let conflicts = inject_retryable_conflicts(site, ds.id(), BEST_EFFORT_TRANSITION_RETRIES + 2);
	txn.cancel().await?;
	drop(conflicts);

	let expired = Utc::now() - chrono::Duration::seconds(BUILD_OWNER_LEASE_SECS + 5);
	let mut lapsed = durable_build_state(&ds, &ikb).await?;
	lapsed.updated_at = expired;
	lapsed.owner_heartbeat_at = lapsed.owner_heartbeat_at.map(|_| expired);
	set_durable_build_state(&ds, &ikb, lapsed).await?;
	ds.resume_stalled_index_builds(Duration::from_secs(30), CancellationToken::new()).await?;

	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(
		(state.phase, state.report_status, state.owner),
		(IndexBuildPhase::Error, Some(IndexBuildReportStatus::Aborted), None),
		"a build of a definition the rollback took back must not stay published: {state:?}"
	);
	Ok(())
}

/// A build whose statement rolls back while its `Online` publish retries a
/// conflict never commits `Online`: each attempt reads the abort first, so the
/// publish declines and the build ends `aborted`. The definition is unchanged,
/// so nothing at the release gate would withdraw a generation it published.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_build_aborted_while_retrying_its_publish_never_publishes() -> Result<()> {
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

	let site = RetryableConflictSite::ConcurrentIndexOnlinePublish;
	let held = inject_retryable_conflicts(site, ds.id(), REPEATED_RETRY_CONFLICTS);
	let (txn, _completion) = start_index_build_inside_an_open_statement_transaction(
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
	wait_for_retry_conflict(site, ds.id(), REPEATED_RETRY_CONFLICTS).await?;
	txn.cancel().await?;
	assert_ne!(
		durable_build_state(&ds, &ikb).await?.phase,
		IndexBuildPhase::Online,
		"the build must still be held inside its publish"
	);
	drop(held);
	wait_for_finished_builder(&building).await?;

	assert!(!building.is_online(), "the aborted build committed its publish");
	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(state.phase, IndexBuildPhase::Error, "the aborted build published: {state:?}");
	assert_eq!(state.report_status, Some(IndexBuildReportStatus::Aborted));
	assert_eq!(state.owner, None, "the aborted build must not leave the index fenced");
	Ok(())
}

/// Whichever half of the release gate arrives second withdraws a build whose
/// statement rolled back to a definition that does not describe it. Here the
/// statement rolls back after the build has published `Online`, while the
/// build is held in the doc-ID reclaim sweep it runs before arriving at the
/// gate, so the statement arrives first and the build withdraws the generation
/// itself.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_build_published_before_its_statement_rolled_back_is_withdrawn_by_its_task() -> Result<()>
{
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE INDEX hx ON t FIELDS vec HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32;",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "t", "hx").await?;
	downgrade_index_format(&ds, "t", "hx", 0).await?;
	execute_all(&ds, &session, "CREATE t:1 SET vec = [1.0, 0.0];").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);

	let park = super::doc_ids::reclaim_sweep_park::park(ds.id());
	let (txn, _completion) = start_index_build_inside_an_open_statement_transaction(
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
	timeout(Duration::from_secs(10), park.parked())
		.await
		.map_err(|_| anyhow::anyhow!("the build did not reach its reclaim sweep"))?;
	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(state.phase, IndexBuildPhase::Online, "the build publishes before its sweep");
	assert_eq!(state.owner, Some(building.owner), "the sweep runs under the build's ownership");

	txn.cancel().await?;
	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(
		(state.phase, state.owner),
		(IndexBuildPhase::Online, Some(building.owner)),
		"the statement arrives first, so it leaves the release to the build"
	);
	drop(park);
	wait_for_finished_builder(&building).await?;

	assert!(building.is_online(), "the build published before its statement rolled back");
	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(state.owner, None, "the build's release must not leave the index fenced");
	assert_eq!(
		state.phase,
		IndexBuildPhase::Error,
		"a build of a definition the rollback took back must not stay published"
	);
	assert_eq!(state.report_status, Some(IndexBuildReportStatus::Aborted));
	Ok(())
}

/// A statement transaction dropped without being committed or cancelled
/// leaves the catalog as a rollback does, so the keeper of a build published
/// against a definition the catalog does not hold withdraws the generation as
/// it releases in the statement's place.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_dropped_statement_transaction_withdraws_a_build_of_an_uncommitted_definition()
-> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE INDEX hx ON t FIELDS vec HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32;",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "t", "hx").await?;
	downgrade_index_format(&ds, "t", "hx", 0).await?;
	execute_all(&ds, &session, "CREATE t:1 SET vec = [1.0, 0.0];").await?;
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
	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(state.phase, IndexBuildPhase::Online, "the build publishes before its statement");
	assert!(state.owner.is_some(), "the open statement still holds the build's ownership");

	drop(txn);

	timeout(Duration::from_secs(10), async {
		while durable_build_state(&ds, &ikb).await?.owner.is_some() {
			sleep(Duration::from_millis(20)).await;
		}
		anyhow::Ok(())
	})
	.await
	.map_err(|_| anyhow::anyhow!("the keeper did not settle the dropped statement's build"))??;
	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(
		state.phase,
		IndexBuildPhase::Error,
		"a build of a definition the dropped statement never committed must not stay published"
	);
	assert_eq!(state.report_status, Some(IndexBuildReportStatus::Aborted));
	Ok(())
}

/// A full-text index over `t.f` analysed by `simple`, which lowercases, with
/// `t:1` holding `Hello World`, so `f @@ 'hello'` finds `t:1`.
#[cfg(feature = "kv-mem")]
async fn fulltext_index_over_a_lowercasing_analyzer() -> Result<(Arc<Datastore>, Session)> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase;
		 DEFINE INDEX ft ON t FIELDS f FULLTEXT ANALYZER simple BM25;
		 CREATE t:1 SET f = 'Hello World'; CREATE t:2 SET f = 'goodbye';",
	)
	.await?;
	assert_eq!(planner_result_ids(&ds, "SELECT id FROM t WHERE f @@ 'hello'").await?, ["t:1"]);
	Ok((ds, session))
}

/// A blocking rebuild of a full-text index analyses with the analyzer as its
/// statement's transaction reads it. A transaction that redefines that
/// analyzer, rebuilds the index and is cancelled leaves the committed analyzer
/// in place, which does not produce the terms the build indexed: the build is
/// withdrawn rather than kept `ready` while `@@` misses the rows it matched.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_cancelled_rebuild_under_a_redefined_analyzer_withdraws_the_build() -> Result<()> {
	let (ds, session) = fulltext_index_over_a_lowercasing_analyzer().await?;
	ds.execute(
		"BEGIN; DEFINE ANALYZER OVERWRITE simple TOKENIZERS blank; REBUILD INDEX ft ON t; CANCEL;",
		&session,
		None,
	)
	.await?;

	let report = index_building_json(&ds, &session, "t", "ft").await?;
	let found = planner_result_ids(&ds, "SELECT id FROM t WHERE f @@ 'hello'")
		.await
		.map_err(|err| err.to_string());
	assert_eq!(
		report["status"], "aborted",
		"the rebuild under the cancelled analyzer is not reported as aborted, and `@@ 'hello'` \
		 returned {found:?}: {report}"
	);
	assert_eq!(
		found,
		Err("There was no suitable index supporting the expression: f @@ 'hello'".to_owned()),
		"`@@` over the withdrawn index does not refuse to answer"
	);
	Ok(())
}

/// The counterpart of
/// [`a_cancelled_rebuild_under_a_redefined_analyzer_withdraws_the_build`]: a
/// cancelled rebuild whose analyzer is unchanged built what the committed
/// definition and analyzer read, so the index stays `ready` and `@@` still
/// matches.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_cancelled_rebuild_under_an_unchanged_analyzer_keeps_the_index_ready() -> Result<()> {
	let (ds, session) = fulltext_index_over_a_lowercasing_analyzer().await?;
	ds.execute("BEGIN; REBUILD INDEX ft ON t; CANCEL;", &session, None).await?;

	assert_eq!(index_building_status(&ds, &session, "t", "ft").await?, "ready");
	assert_eq!(planner_result_ids(&ds, "SELECT id FROM t WHERE f @@ 'hello'").await?, ["t:1"]);
	Ok(())
}

/// [`a_cancelled_rebuild_under_a_redefined_analyzer_withdraws_the_build`],
/// with the index's removal staged after the rebuild. The removal stops the
/// rebuild's build, which has already published, and the cancel rolls the
/// removal back with the rebuild and the analyzer. That earlier stop must not
/// keep the rollback from withdrawing the build: `@@` needs an index to
/// answer, so over the withdrawn index it fails with an error naming the
/// expression.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_cancelled_rebuild_and_removal_under_a_redefined_analyzer_withdraws_the_build()
-> Result<()> {
	let (ds, session) = fulltext_index_over_a_lowercasing_analyzer().await?;
	ds.execute(
		"BEGIN; DEFINE ANALYZER OVERWRITE simple TOKENIZERS blank; REBUILD INDEX ft ON t; \
		 REMOVE INDEX ft ON t; CANCEL;",
		&session,
		None,
	)
	.await?;

	let report = index_building_json(&ds, &session, "t", "ft").await?;
	let found = planner_result_ids(&ds, "SELECT id FROM t WHERE f @@ 'hello'")
		.await
		.map_err(|err| err.to_string());
	assert_eq!(
		report["status"], "aborted",
		"the cancelled rebuild is not reported as aborted, and `@@ 'hello'` returned \
		 {found:?}: {report}"
	);
	assert_eq!(
		found,
		Err("There was no suitable index supporting the expression: f @@ 'hello'".to_owned()),
		"`@@` over the withdrawn index does not refuse to answer"
	);
	Ok(())
}

/// A build that stops on the memory threshold leaves its generation
/// `Building` for the resume scan to carry on. When its statement then rolls
/// back to a definition that does not describe the build — here the format-0
/// one a rebuild at the current format replaces — the generation is withdrawn
/// instead, so the resume scan never carries on from checkpoints written for
/// a definition that did not commit.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_rolled_back_build_stopped_by_the_memory_threshold_is_withdrawn() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE INDEX hx ON t FIELDS vec HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32;",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "t", "hx").await?;
	downgrade_index_format(&ds, "t", "hx", 0).await?;
	execute_all(&ds, &session, "CREATE t:1 SET vec = [1.0, 0.0];").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);

	let threshold = inject_non_retryable_error(
		NonRetryableErrorSite::ConcurrentIndexInitialBatchMemoryThreshold,
		ds.id(),
	);
	let (txn, completion) = start_index_build_inside_an_open_statement_transaction(
		&ds,
		&session,
		ns,
		db,
		&table,
		Arc::clone(&ix),
		true,
	)
	.await?;
	let err = completion
		.expect("a blocking build has a completion")
		.await
		.expect_err("the build stops on the injected memory threshold");
	drop(threshold);
	assert!(err.to_string().contains("memory threshold"), "unexpected build error: {err}");
	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(state.phase, IndexBuildPhase::Building, "left for the resume scan: {state:?}");

	txn.cancel().await?;

	// Lapse whatever lease the rollback left, as time would, and let the
	// resume scan look: nothing is left for it to carry on.
	let expired = Utc::now() - chrono::Duration::seconds(BUILD_OWNER_LEASE_SECS + 5);
	let mut lapsed = durable_build_state(&ds, &ikb).await?;
	lapsed.updated_at = expired;
	lapsed.owner_heartbeat_at = lapsed.owner_heartbeat_at.map(|_| expired);
	set_durable_build_state(&ds, &ikb, lapsed.clone()).await?;
	let resumed =
		ds.resume_stalled_index_builds(Duration::from_secs(30), CancellationToken::new()).await?;
	assert_eq!(resumed.items, 0, "the resume scan carried on the rolled-back build: {lapsed:?}");
	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(
		(state.phase, state.report_status, state.owner),
		(IndexBuildPhase::Error, Some(IndexBuildReportStatus::Aborted), None),
		"a stopped build of a definition the rollback took back must be withdrawn: {state:?}"
	);
	Ok(())
}

/// A build aborted while it waits in `Closing` for an admitted writer never
/// publishes `Online`: the stages it cut short leave queued work the index
/// would be missing. The statement awaiting the build inside its transaction
/// rolls back during that wait, against an unchanged definition, so no
/// withdrawal at the release gate would take back a generation it published.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_build_aborted_while_closing_never_publishes() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE INDEX ix ON t FIELDS v; CREATE t:1 SET v = 1; CREATE t:2 SET v = 2;",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "t", "ix").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);

	// Hold the build in its initial cleanup, so a writer's reservation can be
	// planted against its generation before it reaches `Closing`.
	let site = RetryableConflictSite::ConcurrentIndexInitialCleanup;
	let guard = inject_retryable_conflicts(site, ds.id(), REPEATED_RETRY_CONFLICTS);
	let (txn, _completion) = start_index_build_inside_an_open_statement_transaction(
		&ds,
		&session,
		ns,
		db,
		&table,
		Arc::clone(&ix),
		true,
	)
	.await?;
	wait_for_retry_conflict(site, ds.id(), REPEATED_RETRY_CONFLICTS).await?;
	let generation = durable_build_state(&ds, &ikb).await?.generation;
	let tx = ds.transaction(TransactionType::Write).await?;
	tx.set_key(
		&ikb.new_br_key(generation, u64::MAX / 2),
		&IndexBuildReservation {
			node: ds.id(),
			expires_at: Utc::now() + chrono::Duration::hours(1),
		},
	)
	.await?;
	tx.commit().await?;
	drop(guard);

	timeout(Duration::from_secs(10), async {
		while durable_build_state(&ds, &ikb).await?.phase != IndexBuildPhase::Closing {
			sleep(Duration::from_millis(10)).await;
		}
		anyhow::Ok(())
	})
	.await
	.map_err(|_| anyhow::anyhow!("the build did not reach Closing"))??;
	// Let the build enter its wait for the planted reservation.
	sleep(Duration::from_millis(200)).await;
	txn.cancel().await?;

	let building = local_builder_for_key(&ds, ns, db, &table, ix.index_id)
		.await?
		.expect("the build registers a local builder");
	wait_for_finished_builder(&building).await?;
	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(state.phase, IndexBuildPhase::Error, "the aborted build published: {state:?}");
	assert_eq!(state.report_status, Some(IndexBuildReportStatus::Aborted));
	assert_eq!(state.owner, None, "the aborted build must not leave the index fenced");
	Ok(())
}

/// A vector build folds nothing: it publishes its generation `Online` with
/// every pending its scan wrote still queued, which is the materialiser's to
/// fold. The build's ownership fences background compaction until the statement
/// that started it closes its transaction, so the requests compaction turns
/// away meanwhile have to be queued again by the release.
#[cfg(feature = "kv-mem")]
async fn assert_a_vector_build_leaves_its_pendings_behind_its_fence(kind: &str) -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	// The pass under test folds what a build's scan queues, which the DiskANN sink would not.
	let _sink_ds = super::kind::test_sink::off(ds.id());
	execute_all(&ds, &session, &format!("DEFINE INDEX ix ON t FIELDS vec {kind}; {TWO_VECTORS}"))
		.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "t", "ix").await?;
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
	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(state.phase, IndexBuildPhase::Online, "{state:?}");
	assert!(state.owner.is_some(), "the open statement still holds the build's ownership");
	let building = index_building_json(&ds, &session, "t", "ix").await?;
	assert_eq!(building["compacting"], true, "the build leaves its writes to fold: {building}");

	// Turned away while the ownership fences the index.
	Datastore::index_compaction(Arc::clone(&ds), Duration::from_secs(1), CancellationToken::new())
		.await?;
	let building = index_building_json(&ds, &session, "t", "ix").await?;
	assert_eq!(building["compacting"], true, "the fence keeps compaction off: {building}");

	txn.commit().await?;
	assert_eq!(durable_build_state(&ds, &ikb).await?.owner, None, "the commit releases the fence");
	Datastore::index_compaction(Arc::clone(&ds), Duration::from_secs(1), CancellationToken::new())
		.await?;
	let building = index_building_json(&ds, &session, "t", "ix").await?;
	assert_eq!(
		building["compacting"], false,
		"the release must queue the compaction the fence turned away: {building}"
	);
	expect_indexed_query_len(&ds, &session, BOTH_NEIGHBOURS, 2).await
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn an_hnsw_build_leaves_its_pendings_behind_its_fence() -> Result<()> {
	assert_a_vector_build_leaves_its_pendings_behind_its_fence(
		"HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32",
	)
	.await
}

#[cfg(all(feature = "kv-mem", diskann))]
#[tokio::test(flavor = "multi_thread")]
async fn a_diskann_build_leaves_its_pendings_behind_its_fence() -> Result<()> {
	assert_a_vector_build_leaves_its_pendings_behind_its_fence(
		"DISKANN DIMENSION 2 DIST EUCLIDEAN TYPE F32",
	)
	.await
}

/// A blocking wait on a build another node owns records the publish it sees,
/// so it settles as a wait on a build of its own that published.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_wait_on_a_remote_owner_records_the_publish_it_sees() -> Result<()> {
	let (ds_a, ds_b, session) = new_distributed_index_test_ds().await?;
	execute_all(
		&ds_a,
		&session,
		&format!(
			"{TWO_VECTORS} DEFINE INDEX ix ON t FIELDS vec HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32;"
		),
	)
	.await?;
	// Node A rebuilds the index, held at its first batch so the build stays A's.
	let guard =
		start_index_build_paused(&ds_a, &session, "REBUILD INDEX ix ON t CONCURRENTLY").await?;
	let (ns, db, table, ix) = get_table_index(&ds_b, "t", "ix").await?;
	let txn = Arc::new(ds_b.transaction(TransactionType::Write).await?);
	let table_def =
		catch!(txn, txn.get_tb(ns, db, &table, None).await).expect("table should exist");
	let mut ctx = ds_b.setup_ctx()?;
	ctx.set_transaction(Arc::clone(&txn));
	let ctx = ctx.freeze();
	let builder = ctx.get_index_builder().expect("index builder should exist").clone();
	let mut completion = builder
		.build(&ctx, ds_b.setup_options(&session), table_def.table_id, ix, true)
		.await?
		.expect("a blocking build another node owns is waited for");
	assert!(!completion.is_online(), "node A's build is still held");
	drop(guard);
	(&mut completion).await?;
	assert!(completion.is_online(), "the wait saw node A publish the index");
	txn.cancel().await?;
	Ok(())
}

/// A blocking wait on a build another node owns waits for that build's
/// generation, so the durable check a wait makes before it reports a failure
/// counts that generation's publish, although the wait itself never saw it.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_wait_on_a_remote_owner_counts_the_publish_of_its_generation() -> Result<()> {
	let (ds_a, ds_b, session) = new_distributed_index_test_ds().await?;
	execute_all(
		&ds_a,
		&session,
		"CREATE |user:40| SET email = rand::string(16) RETURN NONE;
		 DEFINE INDEX test ON user FIELDS email;",
	)
	.await?;
	// Node A rebuilds the index, held at its first batch so the build stays A's.
	let guard =
		start_index_build_paused(&ds_a, &session, "REBUILD INDEX test ON user CONCURRENTLY")
			.await?;
	let (ns, db, table, ix) = get_table_index(&ds_b, "user", "test").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	let remote = durable_build_state(&ds_b, &ikb).await?.generation;
	let txn = Arc::new(ds_b.transaction(TransactionType::Write).await?);
	let table_def =
		catch!(txn, txn.get_tb(ns, db, &table, None).await).expect("table should exist");
	let mut ctx = ds_b.setup_ctx()?;
	ctx.set_transaction(Arc::clone(&txn));
	let ctx = ctx.freeze();
	let builder = ctx.get_index_builder().expect("index builder should exist").clone();
	let completion = builder
		.build(&ctx, ds_b.setup_options(&session), table_def.table_id, ix, true)
		.await?
		.expect("a blocking build another node owns is waited for");
	assert_eq!(completion.generation(), remote, "the wait waits for node A's generation");
	assert!(!completion.published().await?, "node A's build is still held");
	drop(guard);
	wait_for_durable_online(&ds_a, &ikb).await?;
	assert!(!completion.is_online(), "the wait has not looked since");
	assert!(completion.published().await?, "node A published the generation the wait waits for");
	txn.cancel().await?;
	Ok(())
}

/// Recording an error for a generation that has published its index leaves it
/// `Online`: a failure after the publish never reaches back into an index
/// already queryable.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn an_error_recorded_after_the_publish_keeps_the_generation_online() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(&ds, &session, "DEFINE INDEX ix ON t FIELDS n; CREATE t:1 SET n = 1;").await?;
	let (ns, db, table, ix) = get_table_index(&ds, "t", "ix").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	let building = new_building_for_index(&ds, &session, ns, db, &table, ix).await?;
	let acquired =
		building.acquire_build_state().await?.expect("an Online index admits a new generation");
	building.mark_durable_online(acquired.generation, 1, 0, false).await?;

	let _ = building.mark_durable_error(acquired.generation, "injected failure".into()).await;
	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(
		state.phase,
		IndexBuildPhase::Online,
		"an error after the publish must not demote the generation: {state:?}"
	);
	assert_eq!(state.report_status, Some(IndexBuildReportStatus::Ready), "{state:?}");
	expect_indexed_query_len(&ds, &session, "SELECT * FROM t WHERE n = 1", 1).await
}

/// The close of a rolled-back statement releases ownership through the same
/// transaction as a commit's, so it queues the compaction request the release
/// owes and wakes the compactor.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_rolled_back_statement_release_requeues_compaction() -> Result<()> {
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
	let txn =
		build_index_inside_an_open_statement_transaction(&ds, &session, ns, db, &table, ix, true)
			.await?;
	Datastore::index_compaction(Arc::clone(&ds), Duration::from_secs(1), CancellationToken::new())
		.await?;
	let kept = count_compaction_requests(&ds, &ikb).await?;
	assert_eq!(kept, 1, "a fenced drain must keep exactly one request");
	// A wake-up with no task waiting is stored for the next waiter. Take any an
	// earlier commit left, so the one read below can only be the release's.
	let triggers = Arc::clone(ds.commit_triggers());
	let _ = triggers.index_compaction.notified().now_or_never();

	txn.cancel().await?;

	assert_eq!(
		durable_build_state(&ds, &ikb).await?.owner,
		None,
		"a rolled-back statement must release ownership"
	);
	assert_eq!(
		count_compaction_requests(&ds, &ikb).await?,
		kept + 1,
		"the rollback's release must queue exactly the one request it owes"
	);
	assert!(
		triggers.index_compaction.notified().now_or_never().is_some(),
		"the rollback's release must wake the compactor"
	);
	Ok(())
}

/// A transition whose outcome is decided before it writes — here, one made by
/// a builder that does not own the generation — reports that outcome even when
/// the cancel ending it fails.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_build_state_transition_keeps_its_outcome_when_its_cancel_fails() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(&ds, &session, "DEFINE INDEX ix ON t FIELDS n; CREATE t:1 SET n = 1;").await?;
	let (ns, db, table, ix) = get_table_index(&ds, "t", "ix").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	let generation = durable_build_state(&ds, &ikb).await?.generation;
	let stranger = new_building_for_index(&ds, &session, ns, db, &table, ix).await?;

	let _fault =
		inject_non_retryable_error(NonRetryableErrorSite::ConcurrentIndexBuildStateCancel, ds.id());
	let err = stranger
		.mark_durable_report(generation, IndexBuildReportStatus::Ready, None, None, None)
		.await
		.expect_err("a builder that does not own the generation cannot update it");

	assert!(
		matches!(
			err.downcast_ref::<DatastoreError>(),
			Some(DatastoreError::IndexingBuildingCancelled { .. })
		),
		"the transition must report lost ownership, not the failed cancel: {err}"
	);
	Ok(())
}

/// Acquire a fresh build generation of the index `ix` on `t` for a builder
/// that is never spawned, then have it record `error` with `conflicts`
/// retryable conflicts raised in place of the `!bs` reads that open its
/// attempts. Reports the state left and the conflicts left unconsumed.
#[cfg(feature = "kv-mem")]
async fn record_a_build_error_through_conflicting_state_reads(
	conflicts: usize,
) -> Result<(IndexBuildState, usize)> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(&ds, &session, "DEFINE INDEX ix ON t FIELDS n; CREATE t:1 SET n = 1;").await?;
	let (ns, db, table, ix) = get_table_index(&ds, "t", "ix").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	let building = new_building_for_index(&ds, &session, ns, db, &table, ix).await?;
	let acquired =
		building.acquire_build_state().await?.expect("an Online index admits a new generation");

	let site = RetryableConflictSite::ConcurrentIndexBuildStateRead;
	let _conflicts = inject_retryable_conflicts(site, ds.id(), conflicts);
	// The build task ignores the outcome, so a give-up is not asserted on here.
	let _ = building.mark_durable_error(acquired.generation, "injected build failure".into()).await;

	Ok((durable_build_state(&ds, &ikb).await?, retryable_conflict_count(site, ds.id())))
}

/// Recording a build error is a transition like any other: a conflict on the
/// read that opens it is retried, and the error is recorded.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn recording_a_build_error_retries_a_conflicting_state_read() -> Result<()> {
	let (state, left) = record_a_build_error_through_conflicting_state_reads(1).await?;
	assert_eq!(left, 0, "recording the error must have met the injected conflict");
	assert_eq!(state.phase, IndexBuildPhase::Error, "the retried attempt must record the error");
	assert_eq!(state.owner, None, "a recorded error clears the owner");
	Ok(())
}

/// Recording a build error runs after the build has already failed, and nobody
/// waits on its outcome, so it gives up at the best-effort retry bound rather
/// than retrying forever. The `Building` generation it leaves stays owned, and
/// the resume scan adopts it once the owner's lease expires.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn recording_a_build_error_gives_up_at_its_retry_bound() -> Result<()> {
	let (state, left) =
		record_a_build_error_through_conflicting_state_reads(BEST_EFFORT_TRANSITION_RETRIES + 2)
			.await?;
	assert_eq!(left, 1, "recording the error must make exactly one attempt plus its retries");
	assert_eq!(state.phase, IndexBuildPhase::Building, "an error that gave up records nothing");
	assert!(state.owner.is_some(), "an error that gave up leaves the owner");
	Ok(())
}

/// The exclusion is scoped to the index kinds whose builders drain their own
/// pendings. A full-text or count builder has no such drain, so its index is
/// compacted under a live owner rather than left until the owner releases it.
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

/// Number of compaction requests (`!ic`) queued for one index.
#[cfg(feature = "kv-mem")]
async fn count_compaction_requests(ds: &Datastore, ikb: &IndexKeyBase) -> Result<usize> {
	let rng =
		IndexCompactionIxPrefix::new(ikb.ns(), ikb.db(), Cow::Borrowed(ikb.table()), ikb.index())
			.range()?;
	let tx = ds.transaction(TransactionType::Read).await?;
	let count = catch!(tx, tx.count(rng, None).await);
	tx.cancel().await?;
	Ok(count)
}

/// Every `!iu` entry of a COUNT index: the deltas its writes left, each with a
/// `uid`, and the aggregate compaction folds them into, at the `None` bound.
#[cfg(feature = "kv-mem")]
async fn count_index_entries(
	ds: &Datastore,
	ikb: &IndexKeyBase,
) -> Result<Vec<IndexCountKey<'static>>> {
	let rng = IndexCountPrefix {
		ns: ikb.ns(),
		db: ikb.db(),
		tb: Cow::Borrowed(ikb.table()),
		ix: ikb.index(),
	}
	.range()?;
	let tx = ds.transaction(TransactionType::Read).await?;
	let keys = catch!(tx, tx.keys(rng, u32::MAX, 0, None).await);
	tx.cancel().await?;
	keys.iter().map(|key| Ok(IndexCountKey::decode_key(key)?.into_owned())).collect()
}

/// The full-text deltas an index has not folded yet: its `!tx` term batches
/// and its `!dx` document-statistics batches.
#[cfg(feature = "kv-mem")]
async fn count_fulltext_deltas(ds: &Datastore, ikb: &IndexKeyBase) -> Result<(usize, usize)> {
	let terms = TermChangeBatchPrefix {
		ns: ikb.ns(),
		db: ikb.db(),
		tb: Cow::Borrowed(ikb.table()),
		ix: ikb.index(),
	}
	.range()?;
	let docs = DocStatsBatchPrefix {
		ns: ikb.ns(),
		db: ikb.db(),
		tb: Cow::Borrowed(ikb.table()),
		ix: ikb.index(),
	}
	.range()?;
	let tx = ds.transaction(TransactionType::Read).await?;
	let terms = catch!(tx, tx.count(terms, None).await);
	let docs = catch!(tx, tx.count(docs, None).await);
	tx.cancel().await?;
	Ok((terms, docs))
}

/// A definition for a new index on `table`, under a fresh index id and at the
/// current format, as `DEFINE INDEX` stamps one. The definition is only
/// returned; [`build_index_inside_an_open_statement_transaction`] stages it.
#[cfg(feature = "kv-mem")]
async fn new_index_definition(
	ds: &Datastore,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
	name: &str,
	cols: &[&str],
	index: Index,
) -> Result<Arc<IndexDefinition>> {
	let index_id = ds.sequences().next_index_id(None, ns, db, table.clone()).await?;
	Ok(Arc::new(IndexDefinition {
		index_id,
		name: name.into(),
		table_name: table.clone(),
		cols: cols.iter().map(|col| Idiom::field(*col)).collect(),
		index,
		count_cond: None,
		comment: None,
		prepare_remove: false,
		format_version: INDEX_FORMAT_VERSION,
		nulls_not_distinct: false,
	}))
}

/// Build a new index the way the statement defining it does, and run a
/// compaction pass before that statement commits.
///
/// A background compactor can run that pass at any point in the window. It
/// reads the requests the build's batches queued, finds no committed
/// definition to compact them against, and keeps one of them for a later
/// pass. Returns the statement's transaction, still open.
#[cfg(feature = "kv-mem")]
async fn build_a_new_index_past_a_compaction_that_cannot_see_it(
	ds: &Arc<Datastore>,
	session: &Session,
	ikb: &IndexKeyBase,
	ix: Arc<IndexDefinition>,
) -> Result<Arc<crate::kvs::Transaction>> {
	let txn = build_index_inside_an_open_statement_transaction(
		ds,
		session,
		ikb.ns(),
		ikb.db(),
		ikb.table(),
		ix,
		true,
	)
	.await?;
	assert!(
		count_compaction_requests(ds, ikb).await? > 0,
		"the fixture's build must queue compaction requests for the pass to consume"
	);

	Datastore::index_compaction(Arc::clone(ds), Duration::from_secs(1), CancellationToken::new())
		.await?;

	assert_eq!(
		count_compaction_requests(ds, ikb).await?,
		1,
		"a pass that finds no definition under a live owner must keep exactly one request"
	);
	Ok(txn)
}

/// A new index is invisible to every other session until the statement that
/// defines it commits, and the compactor is one of them. A pass in that window
/// cannot act on the requests the build's batches queued and keeps one of
/// them; once the statement commits and its release queues the request it
/// owes, the next drain folds the build's deltas.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn releasing_build_ownership_requeues_compaction_for_a_new_count_index() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(&ds, &session, "CREATE t:1; CREATE t:2; CREATE t:3;").await?;
	let (ns, db, table) = get_table_ids(&ds, "t").await?;
	let ix = new_index_definition(&ds, ns, db, &table, "cx", &[], Index::Count(None)).await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);

	let txn =
		build_a_new_index_past_a_compaction_that_cannot_see_it(&ds, &session, &ikb, ix).await?;
	assert!(
		count_index_entries(&ds, &ikb).await?.iter().any(|iu| iu.uid.is_some()),
		"the pass that found no definition must leave the build's deltas unfolded"
	);

	txn.commit().await?;
	Datastore::index_compaction(Arc::clone(&ds), Duration::from_secs(1), CancellationToken::new())
		.await?;

	let entries = count_index_entries(&ds, &ikb).await?;
	assert_eq!(entries.len(), 1, "compaction must fold every delta into one entry: {entries:?}");
	assert_eq!(entries[0].uid, None, "the entry left must be the aggregate: {entries:?}");
	assert_eq!(
		(entries[0].pos, entries[0].count),
		(true, 3),
		"the aggregate must count every record: {entries:?}"
	);
	Ok(())
}

/// The full-text half of
/// [`releasing_build_ownership_requeues_compaction_for_a_new_count_index`].
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn releasing_build_ownership_requeues_compaction_for_a_new_fulltext_index() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;
		 CREATE t:1 SET text = 'hello world';
		 CREATE t:2 SET text = 'hello there';",
	)
	.await?;
	let (ns, db, table) = get_table_ids(&ds, "t").await?;
	let ix = new_index_definition(
		&ds,
		ns,
		db,
		&table,
		"fx",
		&["text"],
		Index::FullText(FullTextParams {
			analyzer: "simple".into(),
			highlight: false,
			scoring: Scoring::default(),
		}),
	)
	.await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);

	let txn =
		build_a_new_index_past_a_compaction_that_cannot_see_it(&ds, &session, &ikb, ix).await?;
	let (terms, docs) = count_fulltext_deltas(&ds, &ikb).await?;
	assert!(
		terms > 0 && docs > 0,
		"the pass that found no definition must leave the build's deltas unfolded"
	);

	txn.commit().await?;
	Datastore::index_compaction(Arc::clone(&ds), Duration::from_secs(1), CancellationToken::new())
		.await?;

	assert_eq!(
		count_fulltext_deltas(&ds, &ikb).await?,
		(0, 0),
		"compaction must fold every term and document-statistics delta"
	);
	Ok(())
}

/// The release and the compaction request it owes commit in one transaction,
/// so there is never one without the other. A release whose commit fails
/// leaves the owner in place and queues nothing, and a later release still
/// queues the request. The statement whose close ran the release commits
/// either way.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_release_that_fails_to_commit_keeps_ownership_and_queues_nothing() -> Result<()> {
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
	Datastore::index_compaction(Arc::clone(&ds), Duration::from_secs(1), CancellationToken::new())
		.await?;
	let kept = count_compaction_requests(&ds, &ikb).await?;
	assert_eq!(kept, 1, "a fenced drain must keep exactly one request");
	let building = local_builder_for_key(&ds, ns, db, &table, ix.index_id)
		.await?
		.expect("the build registers a local builder");

	// Held to the end of the test: a release that never reached the fault would
	// leave it armed for the release below, which would then fail too.
	let _fault = inject_non_retryable_error(
		NonRetryableErrorSite::ConcurrentIndexOwnershipReleaseCommit,
		ds.id(),
	);
	txn.commit().await?;

	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(
		state.owner,
		Some(building.owner),
		"a release that did not commit must leave the owner in place"
	);
	assert_eq!(
		count_compaction_requests(&ds, &ikb).await?,
		kept,
		"a release that did not commit must queue nothing"
	);

	building.release_build_ownership(state.generation, None).await;

	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(state.owner, None, "a release that commits must clear the owner");
	assert_eq!(
		count_compaction_requests(&ds, &ikb).await?,
		kept + 1,
		"a release that commits must queue exactly the one request it owes"
	);
	Ok(())
}

/// A release runs as its statement's close action, so it may hold that close
/// only briefly. It retries a conflict a bounded number of times, then gives
/// up: the owner stays in place, nothing is queued, and the statement commits.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_release_that_keeps_conflicting_gives_up_and_queues_nothing() -> Result<()> {
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
	Datastore::index_compaction(Arc::clone(&ds), Duration::from_secs(1), CancellationToken::new())
		.await?;
	let kept = count_compaction_requests(&ds, &ikb).await?;
	assert_eq!(kept, 1, "a fenced drain must keep exactly one request");
	let building = local_builder_for_key(&ds, ns, db, &table, ix.index_id)
		.await?
		.expect("the build registers a local builder");

	// One conflict for the first attempt and each retry, and one to spare.
	let site = RetryableConflictSite::ConcurrentIndexOwnershipRelease;
	let _conflicts = inject_retryable_conflicts(site, ds.id(), BEST_EFFORT_TRANSITION_RETRIES + 2);
	txn.commit().await?;

	assert_eq!(
		retryable_conflict_count(site, ds.id()),
		1,
		"the release must make exactly one attempt plus {BEST_EFFORT_TRANSITION_RETRIES} retries"
	);
	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(state.owner, Some(building.owner), "a release that gave up must leave the owner");
	assert_eq!(
		count_compaction_requests(&ds, &ikb).await?,
		kept,
		"a release that gave up must queue nothing"
	);
	Ok(())
}

/// Build a new index `CONCURRENTLY`, so its defining statement commits while
/// the build task is still running and the task, not the statement, releases
/// ownership.
///
/// The task reads nothing until the statement commits, and is then held at its
/// `Online` publish, after every batch. A compaction pass in that window
/// consumes the requests the batches queued; only then is the task let
/// through to publish and release. The release must queue exactly `owed`
/// compaction requests, and wake the compactor exactly when it queues one.
#[cfg(feature = "kv-mem")]
async fn build_a_new_index_released_by_its_task(
	ds: &Arc<Datastore>,
	session: &Session,
	ikb: &IndexKeyBase,
	ix: Arc<IndexDefinition>,
	owed: usize,
) -> Result<()> {
	let site = RetryableConflictSite::ConcurrentIndexOnlinePublish;
	let held = inject_retryable_conflicts(site, ds.id(), REPEATED_RETRY_CONFLICTS);
	let (txn, completion) = start_index_build_inside_an_open_statement_transaction(
		ds,
		session,
		ikb.ns(),
		ikb.db(),
		ikb.table(),
		ix,
		false,
	)
	.await?;
	assert!(completion.is_none(), "a CONCURRENTLY build is only spawned");
	let building = local_builder_for_key(ds, ikb.ns(), ikb.db(), ikb.table(), ikb.index())
		.await?
		.expect("the build registers a local builder");
	txn.commit().await?;
	wait_for_retry_conflict(site, ds.id(), REPEATED_RETRY_CONFLICTS).await?;
	if owed > 0 {
		assert!(
			count_compaction_requests(ds, ikb).await? > 0,
			"the fixture's build must queue compaction requests for the pass to consume"
		);
	}
	Datastore::index_compaction(Arc::clone(ds), Duration::from_secs(1), CancellationToken::new())
		.await?;
	assert_eq!(
		count_compaction_requests(ds, ikb).await?,
		0,
		"the pass must consume the requests the batches queued"
	);
	assert!(!building.is_finished(), "the task must still be held at its publish");
	// A wake-up with no task waiting is stored for the next waiter. Take any the
	// batches left, so the one read below can only be the release's.
	let triggers = Arc::clone(ds.commit_triggers());
	let _ = triggers.index_compaction.notified().now_or_never();
	drop(held);
	wait_for_finished_builder(&building).await?;
	let woken = triggers.index_compaction.notified().now_or_never().is_some();

	let state = durable_build_state(ds, ikb).await?;
	assert_eq!(state.phase, IndexBuildPhase::Online);
	assert_eq!(state.owner, None, "the task arrives at the gate second, so it releases");
	assert_eq!(
		count_compaction_requests(ds, ikb).await?,
		owed,
		"the task's release must queue exactly the requests it owes"
	);
	assert_eq!(
		woken,
		owed > 0,
		"the task's release must wake the compactor exactly when it queues"
	);
	Datastore::index_compaction(Arc::clone(ds), Duration::from_secs(1), CancellationToken::new())
		.await?;
	Ok(())
}

/// The release a build task performs owes the same request as the one its
/// statement's close performs: see
/// [`releasing_build_ownership_requeues_compaction_for_a_new_count_index`].
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_concurrent_count_build_released_by_its_task_requeues_compaction() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(&ds, &session, "CREATE t:1; CREATE t:2; CREATE t:3;").await?;
	let (ns, db, table) = get_table_ids(&ds, "t").await?;
	let ix = new_index_definition(&ds, ns, db, &table, "cx", &[], Index::Count(None)).await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);

	build_a_new_index_released_by_its_task(&ds, &session, &ikb, ix, 1).await?;

	let entries = count_index_entries(&ds, &ikb).await?;
	assert_eq!(entries.len(), 1, "compaction must fold every delta into one entry: {entries:?}");
	assert_eq!(entries[0].uid, None, "the entry left must be the aggregate: {entries:?}");
	assert_eq!(
		(entries[0].pos, entries[0].count),
		(true, 3),
		"the aggregate must count every record: {entries:?}"
	);
	Ok(())
}

/// The full-text half of
/// [`a_concurrent_count_build_released_by_its_task_requeues_compaction`].
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_concurrent_fulltext_build_released_by_its_task_requeues_compaction() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;
		 CREATE t:1 SET text = 'hello world';
		 CREATE t:2 SET text = 'hello there';",
	)
	.await?;
	let (ns, db, table) = get_table_ids(&ds, "t").await?;
	let ix = new_index_definition(
		&ds,
		ns,
		db,
		&table,
		"fx",
		&["text"],
		Index::FullText(FullTextParams {
			analyzer: "simple".into(),
			highlight: false,
			scoring: Scoring::default(),
		}),
	)
	.await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);

	build_a_new_index_released_by_its_task(&ds, &session, &ikb, ix, 1).await?;

	assert_eq!(
		count_fulltext_deltas(&ds, &ikb).await?,
		(0, 0),
		"compaction must fold every term and document-statistics delta"
	);
	Ok(())
}

/// A b-tree index's writes queue no compaction work, so the release its build
/// task performs clears the owner and queues nothing, for a standard and a
/// unique index alike.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_concurrent_btree_build_released_by_its_task_queues_nothing() -> Result<()> {
	for index in [Index::Idx, Index::Uniq] {
		let (ds, session) = new_index_test_ds().await?;
		execute_all(&ds, &session, "CREATE t:1 SET n = 1; CREATE t:2 SET n = 2;").await?;
		let (ns, db, table) = get_table_ids(&ds, "t").await?;
		let ix = new_index_definition(&ds, ns, db, &table, "bx", &["n"], index).await?;
		let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);

		build_a_new_index_released_by_its_task(&ds, &session, &ikb, ix, 0).await?;
	}
	Ok(())
}

/// Commit a blocking HNSW build's statement, so its close releases ownership,
/// with `conflicts` retryable conflicts raised in place of the `!bs` reads
/// that open the release's attempts. Reports whether the owner is left set,
/// the compaction requests the release queued, and the conflicts left
/// unconsumed.
#[cfg(feature = "kv-mem")]
async fn release_through_conflicting_state_reads(conflicts: usize) -> Result<(bool, usize, usize)> {
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
	let txn =
		build_index_inside_an_open_statement_transaction(&ds, &session, ns, db, &table, ix, true)
			.await?;
	Datastore::index_compaction(Arc::clone(&ds), Duration::from_secs(1), CancellationToken::new())
		.await?;
	let kept = count_compaction_requests(&ds, &ikb).await?;
	assert_eq!(kept, 1, "a fenced drain must keep exactly one request");

	let site = RetryableConflictSite::ConcurrentIndexBuildStateRead;
	let _conflicts = inject_retryable_conflicts(site, ds.id(), conflicts);
	txn.commit().await?;

	let owned = durable_build_state(&ds, &ikb).await?.owner.is_some();
	Ok((
		owned,
		count_compaction_requests(&ds, &ikb).await? - kept,
		retryable_conflict_count(site, ds.id()),
	))
}

/// A conflict on the read that opens a transition is retried like any other:
/// the release that meets one still clears the owner and queues its request.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_release_retries_a_conflicting_state_read() -> Result<()> {
	let (owned, requests, left) = release_through_conflicting_state_reads(1).await?;
	assert_eq!(left, 0, "the release must have met the injected conflict");
	assert!(!owned, "a release that retried the read must clear the owner");
	assert_eq!(requests, 1, "a release that retried the read must queue its request");
	Ok(())
}

/// Conflicting reads count against the release's retry bound as any conflict
/// does: past it, the release gives up, leaving the owner and queuing nothing.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_release_bounds_conflicting_state_reads() -> Result<()> {
	let (owned, requests, left) =
		release_through_conflicting_state_reads(BEST_EFFORT_TRANSITION_RETRIES + 2).await?;
	assert_eq!(left, 1, "the release must make exactly one attempt plus its retries");
	assert!(owned, "a release that gave up must leave the owner");
	assert_eq!(requests, 0, "a release that gave up must queue nothing");
	Ok(())
}

/// Build `index`, defined on `t` by `define`, inside an open statement
/// transaction, and commit it. Reports how many compaction requests the
/// release that commit ran queued, and whether it woke the compactor.
#[cfg(feature = "kv-mem")]
async fn compaction_queued_by_a_release(define: &str, index: &str) -> Result<(usize, bool)> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;
		 CREATE t:1 SET n = 1, text = 'hello world', vec = [1.0, 0.0];
		 CREATE t:2 SET n = 2, text = 'hello there', vec = [0.0, 1.0];",
	)
	.await?;
	execute_all(&ds, &session, define).await?;
	let (ns, db, table, ix) = get_table_index(&ds, "t", index).await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	let txn =
		build_index_inside_an_open_statement_transaction(&ds, &session, ns, db, &table, ix, true)
			.await?;
	// Consume every request queued before the release that a pass can act on,
	// so the queue ends up holding only what the release adds beside the one
	// request a fenced pass keeps.
	Datastore::index_compaction(Arc::clone(&ds), Duration::from_secs(1), CancellationToken::new())
		.await?;
	let kept = count_compaction_requests(&ds, &ikb).await?;
	assert!(kept <= 1, "{define}: a drain must keep at most one request, got {kept}");
	// A wake-up with no task waiting is stored for the next waiter. Take any an
	// earlier commit left, so the one read below can only be the release's.
	let triggers = Arc::clone(ds.commit_triggers());
	let _ = triggers.index_compaction.notified().now_or_never();

	txn.commit().await?;

	let woken = triggers.index_compaction.notified().now_or_never().is_some();
	Ok((count_compaction_requests(&ds, &ikb).await? - kept, woken))
}

/// Every kind whose writes queue compaction work gets exactly one request from
/// the release, and the commit that carries it wakes the compactor. A b-tree
/// index queues nothing, so its release neither queues nor wakes.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn releasing_build_ownership_queues_one_compaction_request_for_each_queued_kind() -> Result<()>
{
	let kinds = [
		("ix", "DEFINE INDEX ix ON t FIELDS n", 0),
		("ux", "DEFINE INDEX ux ON t FIELDS n UNIQUE", 0),
		("cx", "DEFINE INDEX cx ON t COUNT", 1),
		("fx", "DEFINE INDEX fx ON t FIELDS text FULLTEXT ANALYZER simple BM25", 1),
		("hx", "DEFINE INDEX hx ON t FIELDS vec HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32", 1),
		#[cfg(diskann)]
		("dx", "DEFINE INDEX dx ON t FIELDS vec DISKANN DIMENSION 2 DIST EUCLIDEAN TYPE F32", 1),
	];
	for (index, define, expected) in kinds {
		let (requests, woken) = compaction_queued_by_a_release(define, index).await?;
		assert_eq!(requests, expected, "{define}: requests queued by the release");
		assert_eq!(woken, expected > 0, "{define}: whether the release woke the compactor");
	}
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
/// `Datastore` (and its storage handles, leaking file descriptors per
/// SurrealKV instance) for the lifetime of the process.
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
// A table's shared doc-ID space (`!dj` record→doc, `!dd` doc→record) exists iff
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
	let dj = DocLookupIdentityPrefix::new(ns.namespace_id, db.database_id, Cow::Borrowed(&tb))
		.range()?;
	let dd = DocKeyPrefix::new(ns.namespace_id, db.database_id, Cow::Borrowed(&tb)).range()?;
	let dj_keys = tx.keys(dj, u32::MAX, 0, None).await?;
	let dd_keys = tx.keys(dd, u32::MAX, 0, None).await?;
	tx.cancel().await?;
	Ok((dj_keys.len(), dd_keys.len()))
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
/// record fetches. The conjuncts are full-text matches, whose postings are
/// exactly their truth set; a b-tree conjunct is exact only over a column
/// holding one entry per record. Proved behaviorally: after deleting the
/// record *values* directly at the KV layer (leaving index entries and doc-ID
/// mappings intact), the bitmap count still reports the indexed cardinality,
/// while a count with no index to read — which must fetch records — finds
/// none. Finding none is no row rather than a count of zero: the count comes
/// from the rows read, and an aggregation over no row reports nothing.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn bitmap_count_performs_zero_record_fetches() -> Result<()> {
	use surrealdb_types::Value as PublicValue;

	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase;
		 DEFINE INDEX ft_a ON t FIELDS a FULLTEXT ANALYZER simple BM25;
		 DEFINE INDEX ft_b ON t FIELDS b FULLTEXT ANALYZER simple BM25;
		 CREATE t:1 SET a = 'x', b = 'yes';
		 CREATE t:2 SET a = 'x', b = 'yes';
		 CREATE t:3 SET a = 'x', b = 'no';
		 CREATE t:4 SET a = 'y', b = 'yes';",
	)
	.await?;

	// `GROUP ALL` reports one row whatever reaches it, so the count is always
	// a number and a statement that answered with no row at all is a failure.
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
				other @ (PublicValue::None
				| PublicValue::Null
				| PublicValue::Bool(_)
				| PublicValue::Number(_)
				| PublicValue::String(_)
				| PublicValue::Bytes(_)
				| PublicValue::Duration(_)
				| PublicValue::Datetime(_)
				| PublicValue::Date(_)
				| PublicValue::Time(_)
				| PublicValue::Timestamp(_)
				| PublicValue::Interval(_)
				| PublicValue::Uuid(_)
				| PublicValue::Geometry(_)
				| PublicValue::Table(_)
				| PublicValue::RecordId(_)
				| PublicValue::File(_)
				| PublicValue::Range(_)
				| PublicValue::Regex(_)
				| PublicValue::Array(_)
				| PublicValue::Object(_)
				| PublicValue::Set(_)) => anyhow::bail!("unexpected count result: {other:?}"),
			}
		}
	};

	const BITMAP_COUNT: &str = "SELECT count() FROM t WHERE a @1@ 'x' AND b @2@ 'yes' GROUP ALL";
	const NOINDEX_COUNT: &str = "SELECT count() FROM t WHERE a = 'x' AND b = 'yes' GROUP ALL";
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
	// indexed cardinality. The NOINDEX count reads records and finds none, so
	// its group holds nothing to count.
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

/// Runs `sql` and returns every row's record ID in sorted `table:key` form.
/// Rows may be bare record IDs (`SELECT VALUE id`) or objects carrying an `id`
/// field.
#[cfg(feature = "kv-mem")]
async fn planner_result_ids(ds: &Datastore, sql: &str) -> Result<Vec<String>> {
	use surrealdb_types::ToSql;

	let session = Session::owner().with_ns("test").with_db("test");
	let mut results = ds.execute(sql, &session, None).await?;
	let value = results.remove(0).result?;
	let surrealdb_types::Value::Array(rows) = value else {
		anyhow::bail!("query returned non-array value: {value:?}");
	};
	let mut ids = Vec::with_capacity(rows.len());
	for row in rows.iter() {
		let id = if let surrealdb_types::Value::Object(obj) = row {
			obj.get("id")
		} else {
			Some(row)
		};
		match id {
			Some(surrealdb_types::Value::RecordId(rid)) => ids.push(rid.to_sql()),
			other => anyhow::bail!("row without a record id: {other:?}"),
		}
	}
	ids.sort();
	Ok(ids)
}

/// Asserts that `sql` is answered with exactly `expected` (sorted `table:key`
/// form).
#[cfg(feature = "kv-mem")]
async fn assert_planner_returns(ds: &Datastore, sql: &str, expected: &[&str]) -> Result<()> {
	let ids = planner_result_ids(ds, sql).await?;
	assert_eq!(ids, expected, "{sql}");
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

/// Every entry and unique slot of `table`'s index `index`, as raw keys, in key
/// order.
#[cfg(feature = "kv-mem")]
async fn index_entry_keys(ds: &Datastore, table: &str, index: &str) -> Result<Vec<Vec<u8>>> {
	let (ns, db, _, ix) = get_table_index(ds, table, index).await?;
	let range = crate::idx::kernels::btree::IndexKeyspace::of(ns, db, &ix).all()?;
	let tx = ds.transaction(TransactionType::Read).await?;
	let keys = catch!(tx, tx.keys(range, 10_000, 0, None).await);
	tx.cancel().await?;
	Ok(keys)
}

/// Whether `key` holds `text` as a storekey string: its bytes, then the
/// terminator.
#[cfg(feature = "kv-mem")]
fn holds_text(key: &[u8], text: &str) -> bool {
	let mut needle = text.as_bytes().to_vec();
	needle.push(0);
	key.windows(needle.len()).any(|w| w == needle)
}

/// An index stamped below the Datetime-order format spells a `Datetime` as
/// RFC 3339 text in every key it builds — writes, guarded deletes, unique
/// slots and seeks alike — so the entries the current binary writes to it sit
/// beside the ones an older binary wrote, in one spelling: a duplicate is
/// refused, an update or delete leaves no stale entry, and a seek finds the
/// row. An index at the current stamp spells it as integers, and `REBUILD
/// INDEX` moves an index onto that stamp.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn btree_index_keeps_the_datetime_spelling_of_its_stamp() -> Result<()> {
	use crate::catalog::{DATETIME_ORDER_FORMAT_VERSION, INDEX_FORMAT_VERSION};

	const WHOLE: &str = "2026-08-28T09:00:00Z";
	// As the text spelling writes it: the fraction in groups of three digits.
	const FRACTION: &str = "2026-08-28T09:00:00.500Z";
	const LATER: &str = "2026-08-29T00:00:00Z";

	for (unique, kind) in [(true, " UNIQUE"), (false, "")] {
		let (ds, session) = new_index_test_ds().await?;
		execute_all(&ds, &session, &format!("DEFINE INDEX ix ON t FIELDS at{kind};")).await?;
		downgrade_index_format(&ds, "t", "ix", DATETIME_ORDER_FORMAT_VERSION - 1).await?;
		execute_all(
			&ds,
			&session,
			&format!("CREATE t:1 SET at = d'{WHOLE}'; CREATE t:2 SET at = d'{FRACTION}';"),
		)
		.await?;

		// The keys are the text spelling, which is not the current one.
		let keys = index_entry_keys(&ds, "t", "ix").await?;
		assert_eq!(keys.len(), 2, "{kind}");
		assert!(keys.iter().any(|k| holds_text(k, WHOLE)), "{kind}: {keys:?}");
		assert!(keys.iter().any(|k| holds_text(k, FRACTION)), "{kind}: {keys:?}");
		let (ns, db, tb, ix) = get_table_index(&ds, "t", "ix").await?;
		let at = Value::Datetime(WHOLE.parse().expect("a datetime"));
		let id = RecordIdKey::from(1);
		let text = crate::idx::kernels::btree::IndexKeyspace::of(ns, db, &ix).stored_key(
			unique,
			std::slice::from_ref(&at),
			&id,
		)?;
		let current = crate::idx::kernels::btree::IndexKeyspace::new(
			ns,
			db,
			&tb,
			ix.index_id,
			INDEX_FORMAT_VERSION,
		)
		.stored_key(unique, std::slice::from_ref(&at), &id)?;
		assert!(keys.contains(&text.key.to_vec()), "{kind}: the written key is the text one");
		assert_ne!(text.key, current.key, "{kind}: the two spellings differ");

		// A seek on the index finds the row.
		let seek = format!("SELECT * FROM t WHERE at = d'{WHOLE}'");
		assert_eq!(query_array_len(&ds, &session, &seek).await?, 1, "{kind}");

		// A duplicate instant, in either spelling of it, is refused.
		if unique {
			for dup in [WHOLE, "2026-08-28T11:00:00+02:00"] {
				let sql = format!("CREATE t:3 SET at = d'{dup}';");
				let err = execute_all(&ds, &session, &sql).await.expect_err("a duplicate");
				assert!(err.to_string().contains("already contains"), "{dup}: {err}");
			}
		}

		// An update and a delete remove the entries they replace.
		execute_all(&ds, &session, &format!("UPDATE t:1 SET at = d'{LATER}'; DELETE t:2;")).await?;
		let keys = index_entry_keys(&ds, "t", "ix").await?;
		assert_eq!(keys.len(), 1, "{kind}: {keys:?}");
		assert!(holds_text(&keys[0], LATER), "{kind}: {keys:?}");
		assert_eq!(query_array_len(&ds, &session, &seek).await?, 0, "{kind}");
		// The freed instant takes a new row.
		execute_all(&ds, &session, &format!("CREATE t:3 SET at = d'{WHOLE}';")).await?;
		assert_eq!(query_array_len(&ds, &session, &seek).await?, 1, "{kind}");

		// A rebuild restamps the index, and its keys move to integers.
		execute_all(&ds, &session, "REBUILD INDEX ix ON t;").await?;
		wait_for_index_ready(&ds, &session, "t", "ix").await?;
		let (_, _, _, ix) = get_table_index(&ds, "t", "ix").await?;
		assert_eq!(ix.format_version, INDEX_FORMAT_VERSION, "{kind}");
		let keys = index_entry_keys(&ds, "t", "ix").await?;
		assert_eq!(keys.len(), 2, "{kind}: {keys:?}");
		assert!(!keys.iter().any(|k| holds_text(k, WHOLE) || holds_text(k, LATER)), "{kind}");
		assert_eq!(query_array_len(&ds, &session, &seek).await?, 1, "{kind}");
	}
	Ok(())
}

/// A full-text index whose stored format predates the shared table-level
/// doc-ID space (simulated by downgrading `format_version` to 0) serves
/// MATCHES. Its maintenance stays on the private per-index doc-ID layout — the table allocates no
/// shared doc-ID mappings for it — until an explicit REBUILD moves it onto the shared space, which
/// changes no answer.
///
/// MATCHES is only answerable from a full-text index, so every assertion here
/// reads the index under test.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn legacy_fulltext_index_serves_matches() -> Result<()> {
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
	assert_planner_returns(&ds, MATCHES, CARRIERS).await?;

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
	assert_planner_returns(&ds, MATCHES, CARRIERS).await?;

	execute_all(&ds, &session, "REBUILD INDEX ft ON t;").await?;
	let (_, _, _, ix) = get_table_index(&ds, "t", "ft").await?;
	assert!(ix.uses_shared_doc_ids(), "the rebuild must stamp the current format");
	assert_eq!(
		count_doc_id_mappings(&ds, "t").await?,
		(3, 3),
		"the rebuilt index allocates one shared doc-ID per surviving record"
	);
	assert_planner_returns(&ds, MATCHES, CARRIERS).await?;
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
/// of the two the query performs.
#[cfg(feature = "kv-mem")]
async fn legacy_vector_index_knn(define_index: &str) -> Result<()> {
	// An explicit `ef` plans through the ANN index.
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
	assert_planner_returns(&ds, KNN, NEAREST_TWO).await?;

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
	assert_planner_returns(&ds, KNN, NEAREST_TWO).await?;

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
	assert_planner_returns(&ds, KNN, NEAREST_TWO).await?;

	// A MATCHES the full-text index cannot answer on its own is taken through a
	// top-K over a filtered scan, which restricts the search to the matching
	// records.
	assert_planner_returns(&ds, OR_MATCHES_KNN, NEAREST_TWO_MATCHING).await?;

	// A top-level MATCHES is served by the full-text index. The ANN search
	// is kept and evaluates MATCHES per candidate — a legacy
	// vector index cannot join the shared-doc-ID bitmap prefilter, so this is
	// the path that keeps the search restricted to the matching records.
	assert_planner_returns(&ds, MATCHES_KNN, NEAREST_TWO_MATCHING).await?;
	// KNN nested under OR: the planner rejects the shape outright before
	// union analysis, so a multi-index OR union can never contain a KNN
	// branch — legacy or current format.
	const UNION_MATCHES_KNN: &str = "SELECT VALUE id FROM t \
		WHERE (content @@ 'bravo' AND pt <|2,40|> [0.0, 0.0]) OR content @@ 'zulu'";
	let err = planner_result_ids(&ds, UNION_MATCHES_KNN)
		.await
		.expect_err("KNN under OR must be rejected by the planner");
	assert!(err.to_string().contains("top level of the WHERE clause"), "unexpected error: {err}");

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
	assert_planner_returns(&ds, "RETURN fn::knn_plain()", NEAREST_TWO).await?;
	assert_planner_returns(&ds, "RETURN fn::knn_matches()", NEAREST_TWO_MATCHING).await?;

	// The rebuild moves the vector index onto the shared space it was ignoring.
	execute_all(&ds, &session, "REBUILD INDEX vx ON t;").await?;
	let (_, _, _, ix) = get_table_index(&ds, "t", "vx").await?;
	assert!(ix.uses_shared_doc_ids(), "the rebuild must stamp the current format");
	assert_eq!(count_doc_id_mappings(&ds, "t").await?, (5, 5));
	assert_planner_returns(&ds, KNN, NEAREST_TWO).await?;
	// The planner now restricts the ANN search with the shared-doc-ID
	// bitmap prefilter instead, and answers both MATCHES shapes identically.
	assert_planner_returns(&ds, MATCHES_KNN, NEAREST_TWO_MATCHING).await?;
	assert_planner_returns(&ds, OR_MATCHES_KNN, NEAREST_TWO_MATCHING).await?;
	// A runtime-resolved source never carries a bitmap prefilter, so on that
	// path the MATCHES conjunct stays evaluable during traversal for the
	// rebuilt, current-format index too: the top-k is still chosen among the
	// matching records.
	assert_planner_returns(&ds, "RETURN fn::knn_plain()", NEAREST_TWO).await?;
	assert_planner_returns(&ds, "RETURN fn::knn_matches()", NEAREST_TWO_MATCHING).await?;
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
	assert_planner_returns(&ds, MATCHES_KNN, NEAREST_TWO_MATCHING).await?;
	// A coverable conjunct produces an allow-list that cannot include the
	// MATCHES: the compiled condition must stay active alongside it.
	assert_planner_returns(&ds, COVERED_AND_MATCHES_KNN, NEAREST_TWO_MATCHING).await?;
	Ok(())
}

/// An HNSW index whose stored format predates the shared table-level doc-ID
/// space (simulated by downgrading `format_version` to 0) serves KNN — on its own and alongside a
/// MATCHES conjunct answered by a current-format full-text index on the same table — and keeps its
/// private doc-ID layout until an explicit REBUILD. See
/// [`legacy_vector_index_knn`] for the fixture's distances.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn legacy_hnsw_index_serves_knn() -> Result<()> {
	legacy_vector_index_knn(
		"DEFINE INDEX vx ON t FIELDS pt HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32 EFC 16 M 4;",
	)
	.await
}

/// DiskANN counterpart of [`legacy_hnsw_index_serves_knn`]:
/// a pre-shared-doc-ID DiskANN index serves KNN, on
/// its own and with a MATCHES conjunct, and keeps its private doc-ID layout
/// until an explicit REBUILD.
#[cfg(all(feature = "kv-mem", diskann))]
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn legacy_diskann_index_serves_knn() -> Result<()> {
	legacy_vector_index_knn(
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
/// duplication): `resolve_or_assign` is serialized by the per-record `!dj` write
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
/// record id under the index format, as the type-erased `!di` key spells it, so the
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
/// it, or the `!dj`/`!dd` pair is orphaned and a later re-create wrongly reuses
/// the id.
///
/// Drives [`Building::reclaim_deferred_doc_ids`] directly against a simulated
/// sibling build state to cover its whole decision matrix: markers (and
/// mappings) survive while a sibling is still building, are reclaimed once no
/// sibling is building and the record is gone, and a still-present record keeps
/// its live mapping while its stale marker is consumed. Both directions of the
/// mapping are asserted, for a plain id and for one holding a number in a
/// nested value — which the marker must address as precisely as the mapping
/// does, or the reclaim is aimed at a different record. A marker a 3.3
/// pre-release wrote is read as every record its spelling stands for, so its
/// reclaim is carried out rather than forfeited; one that resolves to nothing
/// is still consumed; and one whose two spellings alias — which decodes
/// cleanly as a record other than its own — is reclaimed through its other
/// readings, because decoding proves nothing about a marker's spelling. The end-to-end wiring
/// (a real replayed delete writing the marker, and a real build completion running the sweep) is
/// covered by [`deferred_doc_id_reclaim_survives_across_builds`].
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn concurrent_doc_id_index_build_reclaims_deferred_mapping() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	// Two full-text indexes on one table => two consumers of the shared !dj/!dd
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

	// (5) A marker in the spelling a 3.3 pre-release used. Its id does not
	// decode as an identity here, so it is read as every record that spelling
	// stands for — and the reclaim it was written for is carried out rather than
	// forfeited. `REBUILD INDEX` does not
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
	// marker's bytes rounds it to the nearest decimal, and the float reading of
	// that decimal is the record the marker was written for.
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

	// A marker whose spelling a live record shares. `[7]` is gone, and `[7dec]`,
	// which the pre-release spelling cannot tell from it, is live: both are
	// readings of the marker, and only the absent one is reclaimed.
	let displaced = RecordIdKey::Array(vec![Value::Number(crate::val::Number::Int(7))].into());
	let displaced_id = assign_shared_doc_id(&ds, &docs, &displaced).await?;
	execute_all(&ds, &session, "CREATE t:[7dec] SET a = 'alpha', b = 'beta';").await?;
	let twin = RecordIdKey::Array(
		vec![Value::Number(crate::val::Number::Decimal(rust_decimal::Decimal::from(7)))].into(),
	);
	let twin_id = read_shared_doc_id(&ds, &docs, &twin).await?.expect("the live twin is indexed");
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
		"and the live record the spelling also reaches keeps its own"
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

	// (6) A marker whose two spellings alias. Under a 3.3 pre-release
	// `[2, NONE]` encodes to the same bytes as `[2]` does under this release, so
	// this marker decodes cleanly — as a record it was not written for. Decoding
	// is therefore no proof of a marker's spelling, and the sweep has to take
	// every reading or this record's mapping is stranded while its marker is
	// consumed.
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
	assert_eq!(resumed.items, 1, "the interrupted build should be adopted");
	wait_for_index_ready(&ds, &session, "user", "test").await?;
	Ok(())
}

/// [`shutdown_error_does_not_poison_durable_build_state`] for a DISKANN build,
/// whose scan batches go through its initial-batch sink: the batch's frame is
/// undone when its commit fails, the build stays adoptable, and the resumed
/// build answers for every record.
#[cfg(diskann)]
#[tokio::test(flavor = "multi_thread")]
async fn shutdown_error_in_a_diskann_sink_batch_does_not_poison_durable_build_state() -> Result<()>
{
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE t SCHEMALESS;
			CREATE t:1 SET vec = [1.0, 0.0] RETURN NONE;
			CREATE t:2 SET vec = [0.0, 1.0] RETURN NONE;
			DEFINE INDEX dx ON t FIELDS vec DISKANN DIMENSION 2 DIST EUCLIDEAN TYPE F32 CONCURRENTLY;
			",
	)
	.await?;
	wait_for_index_ready(&ds, &session, "t", "dx").await?;
	let _guard = inject_non_retryable_error(
		NonRetryableErrorSite::ConcurrentIndexInitialBatchShutdown,
		ds.id(),
	);
	let err = ds
		.execute("REBUILD INDEX dx ON t", &session, None)
		.await?
		.remove(0)
		.result
		.expect_err("rebuild should fail with the injected shutdown error");
	assert!(err.to_string().contains("shutting down"), "unexpected rebuild error: {err}");
	let (ns, db, table, ix) = get_table_index(&ds, "t", "dx").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(state.phase, IndexBuildPhase::Building);
	assert_eq!(state.error, None);
	let tx = ds.transaction(TransactionType::Read).await?;
	let elements = catch!(tx, tx.count(ikb.new_de_range()?, None).await);
	tx.cancel().await?;
	assert_eq!(elements, 0, "the failed batch's graph committed");
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
	assert_eq!(resumed.items, 1, "the interrupted build should be adopted");
	wait_for_index_ready(&ds, &session, "t", "dx").await?;
	expect_indexed_query_len(&ds, &session, "SELECT id FROM t WHERE vec <|2,40|> [1.0, 0.0]", 2)
		.await
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
	assert_eq!(resumed.items, 1, "the interrupted build should be adopted");
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
				basis: None,
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
	assert_eq!(resumed.items, 1, "a pass must adopt at most one stalled build");

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
	assert_eq!(resumed_while_running.items, 0, "no adoption while a local build is running");

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

/// A candidate the scan gave back to the walk is checked against the catalog
/// as it stands before its takeover: an index marked for removal since the
/// walk reached it is dropped, as the walk passes one it reaches so marked.
///
/// A candidate given back is reached again from the walk's record of it, a
/// pass or more after the catalog was read, so what it names may have gone.
/// An index pending removal keeps its stranded build state until the reaper
/// clears it, and adopting that would build an index about to be cleared.
#[tokio::test(flavor = "multi_thread")]
async fn resume_scan_drops_a_retained_candidate_marked_for_removal() -> Result<()> {
	use crate::kvs::tasks::Progress;

	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE user SCHEMALESS;
			CREATE user:one SET email = 'one@example.com', name = 'One' RETURN NONE;
			DEFINE INDEX test_email ON user FIELDS email;
			DEFINE INDEX test_name ON user FIELDS name;
			",
	)
	.await?;

	// Strand both indexes as expired `Building` generations.
	let expired = Utc::now() - chrono::Duration::seconds(BUILD_OWNER_LEASE_SECS + 5);
	let stranded_owner = Uuid::new_v4();
	let mut indexes = Vec::new();
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
				owner: Some(stranded_owner),
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
				basis: None,
			},
		)
		.await?;
		tx.commit().await?;
		indexes.push((ns, db, table, ix.index_id, ikb));
	}

	// The first pass adopts the first index in catalog order and gives the
	// second back to the walk for the next pass.
	let scan = || ds.resume_stalled_index_builds(Duration::from_secs(30), CancellationToken::new());
	let first = scan().await?;
	assert_eq!(first.items, 1, "a pass adopts one stalled build");
	assert_eq!(first.progress, Progress::More, "the other candidate is kept for the next pass");
	let (ns, db, table, ix, _) = &indexes[0];
	let adopted = local_builder_for_key(&ds, *ns, *db, table, *ix)
		.await?
		.ok_or_else(|| anyhow::anyhow!("the adopted build should have a local builder"))?;
	wait_for_finished_builder(&adopted).await?;
	wait_for_index_ready(&ds, &session, "user", "test_email").await?;

	// Before the next pass, the retained candidate's index is marked for
	// removal; its stranded build state stays until the reaper clears it.
	execute_all(&ds, &session, "ALTER INDEX test_name ON user PREPARE REMOVE").await?;
	let (_, _, _, _, retained) = &indexes[1];
	assert!(durable_build_state_exists(&ds, retained).await?);

	// The next pass reaches the retained candidate first, and drops it.
	let second = scan().await?;
	assert_eq!(second.items, 0, "an index marked for removal is not adopted");
	let state = durable_build_state(&ds, retained).await?;
	assert_eq!(state.phase, IndexBuildPhase::Building);
	assert_eq!(state.owner, Some(stranded_owner), "no builder took the stranded generation over");
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
			basis: None,
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
			basis: None,
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
	let mut pendings = catch!(tx, tx.scan_raw(ikb.new_hr_range()?, u32::MAX, 0, None).await);
	pendings.extend(catch!(tx, tx.scan_raw(ikb.new_hw_range()?, u32::MAX, 0, None).await));
	let mut ctx = ds.setup_ctx()?;
	ctx.set_transaction(Arc::clone(&tx));
	let ctx = ctx.freeze();
	let plan = IndexOperation::prepare_hnsw_compaction(&ctx, &ikb, ix.format_version).await?;
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
	let surviving = count_hnsw_pendings(&ds, &ikb).await?;
	assert_eq!(surviving, pendings.len(), "the rejected plan must leave every pending in place");
	Ok(())
}

/// A pre-filtered KNN search (#548) narrows the ANN search to the allow-list
/// its b-tree conjunct builds, on both the exact and the graph tier: EXPLAIN
/// ANALYZE (unredacted under `Session::owner()`) reports the chosen
/// `prefilter_tier`. A b-tree column whose index has recorded storing no array
/// answers its equality exactly, typed (`category`) or not (`plaincat`): the
/// allow-list is the conjunct's truth set and no candidate is fetched to judge
/// it. A column whose index has stored an array (`rawcat`) lists a superset, so
/// its conjunct is still judged on each candidate. A declared kind plays no
/// part.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn knn_prefilter_narrows_the_search_to_the_allow_list() -> Result<()> {
	use surrealdb_types::Value as PublicValue;

	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE FIELD category ON t TYPE string;
		 DEFINE INDEX idx_category ON t FIELDS category;
		 DEFINE INDEX idx_rawcat ON t FIELDS rawcat;
		 DEFINE INDEX idx_plaincat ON t FIELDS plaincat;
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
				plaincat = IF $i % 12 < 11 { 'a' } ELSE { 'b' },
				rawcat = IF $i == 0 { ['a'] } ELSE IF $i % 12 < 11 { 'a' } ELSE { 'b' };
		};",
	)
	.await?;

	let explain = |sql: &'static str| {
		let ds = &ds;
		let session = &session;
		async move {
			let mut results = ds.execute(sql, session, None).await?;
			let value = results.remove(0).result?;
			let PublicValue::String(plan) = value else {
				anyhow::bail!("unexpected EXPLAIN result: {value:?}");
			};
			Ok::<String, anyhow::Error>(plan)
		}
	};

	// Exact tier (200 members): zero in-traversal fetches.
	let plan =
		explain("EXPLAIN ANALYZE SELECT id FROM t WHERE category = 'b' AND point <|3,40|> [0f]")
			.await?;
	assert!(plan.contains("prefilter_tier: exact"), "expected exact tier:\n{plan}");
	assert!(!plan.contains("fetched:"), "no in-traversal fetches expected:\n{plan}");

	// Graph tier (2200 members): the allow-list gates admission, and being
	// exact it needs no fetch to judge the conjunct.
	let plan =
		explain("EXPLAIN ANALYZE SELECT id FROM t WHERE category = 'a' AND point <|3,40|> [0f]")
			.await?;
	assert!(plan.contains("prefilter_tier: graph"), "expected graph tier:\n{plan}");
	assert!(!plan.contains("fetched:"), "no in-traversal fetches expected:\n{plan}");

	// `plaincat` has no declared field kind, but its index has stored no
	// array in it, so its equality is exact and joins the allow-list too.
	let plan =
		explain("EXPLAIN ANALYZE SELECT id FROM t WHERE plaincat = 'b' AND point <|3,40|> [0f]")
			.await?;
	assert!(plan.contains("prefilter_tier: exact"), "expected exact tier:\n{plan}");
	assert!(!plan.contains("fetched:"), "no in-traversal fetches expected:\n{plan}");

	// Contrast: `rawcat`'s index has stored an array, so its equality is a
	// superset and the conjunct is still judged.
	let plan =
		explain("EXPLAIN ANALYZE SELECT id FROM t WHERE rawcat = 'b' AND point <|3,40|> [0f]")
			.await?;
	assert!(plan.contains("prefilter_tier: exact"), "expected exact tier:\n{plan}");
	assert!(plan.contains("predicate: rawcat = 'b'"), "the conjunct is still judged:\n{plan}");
	Ok(())
}

/// The bytes a previous release wrote a `!bp` marker at: this release's key up
/// to the record id, then the id under the index format — as the type-erased `!di`
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
	building: Arc<Building>,
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
	progress: Option<super::count::CountPrimaryProgress>,
	indexed: usize,
}

impl<'a> CountScan<'a> {
	async fn start(building: &'a Building) -> Result<Self> {
		Ok(Self {
			building,
			ctx: building.new_write_tx_ctx().await?,
			progress: Some(super::count::CountPrimaryProgress::default()),
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
				&super::kind::InitialBatchSink::Pending,
			)
			.await?
			.indexed;
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
			basis: None,
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
	building
		.index_initial_batch(
			&ctx,
			&tx,
			&values,
			0,
			&mut false,
			&mut None,
			&super::kind::InitialBatchSink::Pending,
		)
		.await?;
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
	building
		.index_initial_batch(
			&ctx,
			&tx,
			&values,
			0,
			&mut false,
			&mut None,
			&super::kind::InitialBatchSink::Pending,
		)
		.await?;
	tx.commit().await?;

	let matches = |sql: &'static str| {
		let ds = &ds;
		let session = &session;
		async move {
			let res = ds.execute(sql, session, None).await?.remove(0).result?;
			let PublicValue::Array(rows) = res else {
				anyhow::bail!("unexpected result: {res:?}");
			};
			Ok(rows.len())
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

/// Counts the transactions that complete.
#[derive(Default)]
struct CompletedTransactions(AtomicU64);

impl crate::observe::ExecutionObserver for CompletedTransactions {
	fn on_transaction_complete(&self, _event: &crate::observe::TransactionEvent) {
		self.0.fetch_add(1, Ordering::SeqCst);
	}
}

impl CompletedTransactions {
	fn count(&self) -> u64 {
		self.0.load(Ordering::SeqCst)
	}
}

/// A user transaction that updated a row while the index was online keeps maintaining the index
/// directly for its later writes, and commits, while a rebuild started elsewhere in between waits
/// at the write barrier for it to end before it wipes and scans. The rebuilt index holds every
/// value the transaction wrote and none it replaced.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn online_decision_holds_for_a_transaction_that_straddles_a_remote_rebuild() -> Result<()> {
	let (ds_a, ds_b, session) = new_distributed_index_test_ds().await?;
	execute_all(
		&ds_a,
		&session,
		"
			DEFINE TABLE u SCHEMALESS;
			CREATE u:one SET e = 'old1' RETURN NONE;
			CREATE u:two SET e = 'old2' RETURN NONE;
			DEFINE INDEX ix ON u FIELDS e;
			",
	)
	.await?;
	let ds_a = Arc::new(ds_a);

	let writer = {
		let (ds, session) = (Arc::clone(&ds_a), session.clone());
		tokio::spawn(async move {
			execute_all(
				&ds,
				&session,
				"
					BEGIN;
					UPDATE u:one SET e = 'new1' RETURN NONE;
					SLEEP 3s;
					UPDATE u:two SET e = 'new2' RETURN NONE;
					COMMIT;
					",
			)
			.await
		})
	};
	sleep(Duration::from_millis(500)).await;

	let ds_b = Arc::new(ds_b);
	let waited = super::builder::write_barrier_waits::count(ds_b.id());
	let rebuild = {
		let (ds, session) = (Arc::clone(&ds_b), session.clone());
		tokio::spawn(async move { execute_all(&ds, &session, "REBUILD INDEX ix ON u").await })
	};
	wait_for_a_build_at_the_write_barrier(ds_b.id(), waited).await?;
	assert!(!writer.is_finished(), "the build must wait while the transaction is still open");

	timeout(Duration::from_secs(20), writer)
		.await
		.map_err(|_| anyhow::anyhow!("timed out waiting for the straddling transaction"))???;
	timeout(Duration::from_secs(20), rebuild)
		.await
		.map_err(|_| anyhow::anyhow!("timed out waiting for the rebuild"))???;
	wait_for_index_ready(&ds_a, &session, "u", "ix").await?;

	for (value, expected) in [("new1", 1), ("new2", 1), ("old1", 0), ("old2", 0)] {
		expect_indexed_query_len(
			&ds_a,
			&session,
			&format!("SELECT id FROM u WITH INDEX ix WHERE e = '{value}'"),
			expected,
		)
		.await?;
	}
	Ok(())
}

/// A transaction that writes to an indexed table, rebuilds the index and
/// writes again leaves the index agreeing with the rows, whether it commits
/// or it does not: the rebuild's own writes conflict with its own, or the
/// write after the rebuild changes a record the rebuild indexed in a
/// transaction of its own, which is refused where it is made.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn same_transaction_rebuild_between_writes_leaves_index_matching_rows() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE u SCHEMALESS;
			CREATE u:one SET e = 'old1' RETURN NONE;
			CREATE u:two SET e = 'old2' RETURN NONE;
			DEFINE INDEX ix ON u FIELDS e;
			",
	)
	.await?;
	let results = ds
		.execute(
			"
			BEGIN;
			UPDATE u:one SET e = 'new1' RETURN NONE;
			REBUILD INDEX ix ON u;
			UPDATE u:two SET e = 'new2' RETURN NONE;
			COMMIT;
			",
			&session,
			None,
		)
		.await?;
	let committed = match transaction_failure(results) {
		None => true,
		Some(error)
			if error.contains("Transaction conflict:")
				|| error.contains("index `ix` was defined by this transaction") =>
		{
			false
		}
		Some(error) => anyhow::bail!("the transaction failed: {error}"),
	};
	wait_for_index_ready(&ds, &session, "u", "ix").await?;
	let (new, old) = if committed {
		(1, 0)
	} else {
		(0, 1)
	};
	for (value, expected) in [("new1", new), ("new2", new), ("old1", old), ("old2", old)] {
		expect_indexed_query_len(
			&ds,
			&session,
			&format!("SELECT id FROM u WITH INDEX ix WHERE e = '{value}'"),
			expected,
		)
		.await?;
		expect_indexed_query_len(
			&ds,
			&session,
			&format!("SELECT id FROM u WHERE e = '{value}'"),
			expected,
		)
		.await?;
	}
	Ok(())
}

/// The writes after the first to an online index in one transaction decide
/// nothing again: they open no transaction of their own.
#[tokio::test(flavor = "multi_thread")]
async fn online_index_admission_opens_one_transaction_per_user_transaction() -> Result<()> {
	let observer = Arc::new(CompletedTransactions::default());
	let ds = Datastore::builder()
		.with_observer(Arc::clone(&observer) as Arc<dyn crate::observe::ExecutionObserver>)
		.without_maintenance_tasks()
		.build_with_path("memory")
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
			DEFINE INDEX test ON user FIELDS email;
			",
	)
	.await?;
	let (_, _, table, ix) = get_table_index(&ds, "user", "test").await?;

	let tx = Arc::new(ds.transaction(TransactionType::Write).await?);
	let db_def = tx.get_db_by_name("test", "test", None).await?.expect("database should exist");
	let mut ctx = ds.setup_ctx()?;
	ctx.set_transaction(Arc::clone(&tx));
	let ctx = ctx.freeze();
	let builder = ctx.get_index_builder().expect("index builder should be present");
	let rid = RecordId {
		table,
		key: RecordIdKey::from("one".to_owned()),
	};
	let consume = async || {
		builder
			.consume(
				db_def.as_ref(),
				&ctx,
				&ix,
				IndexMutation {
					old_values: None,
					new_values: None,
					rid: &rid,
					count_cond_match: None,
				},
			)
			.await
	};

	let before = observer.count();
	assert!(matches!(consume().await?, ConsumeResult::Ignored(..)));
	let first = observer.count() - before;
	assert!(first >= 1, "the first write decides in a transaction of its own");

	let before = observer.count();
	for _ in 0..3 {
		assert!(matches!(consume().await?, ConsumeResult::Ignored(..)));
	}
	assert_eq!(observer.count(), before, "a later write to the same index opens no transaction");
	tx.cancel().await?;
	Ok(())
}

/// The error that failed a transaction, skipping the statements that report
/// only that it failed.
fn transaction_failure(results: Vec<crate::dbs::QueryResult>) -> Option<String> {
	results
		.into_iter()
		.filter_map(|result| result.result.err())
		.map(|error| error.to_string())
		.find(|error| !error.contains("not executed due to a failed transaction"))
}

/// Two writes in one transaction that collide on a unique index fail with the
/// collision, and leave neither a row nor an index entry behind.
async fn unique_collision_in_one_transaction(ds: &Datastore, session: &Session) -> Result<()> {
	execute_all(
		ds,
		session,
		"
			DEFINE TABLE u SCHEMALESS;
			DEFINE INDEX ux ON u FIELDS e UNIQUE;
			",
	)
	.await?;
	let results = ds
		.execute("BEGIN; CREATE u:a SET e = 'x'; CREATE u:b SET e = 'x'; COMMIT;", session, None)
		.await?;
	let error = transaction_failure(results).expect("the collision must fail the transaction");
	assert!(error.contains("already contains"), "unexpected error: {error}");
	expect_indexed_query_len(ds, session, "SELECT id FROM u", 0).await?;
	expect_indexed_query_len(ds, session, "SELECT id FROM u WITH INDEX ux WHERE e = 'x'", 0)
		.await?;

	let rows = (0..1000)
		.map(|n| {
			let e = if n == 500 {
				100
			} else {
				n
			};
			format!("{{ id: u:{n}, e: 'v{e}' }}")
		})
		.collect::<Vec<_>>()
		.join(", ");
	let results = ds.execute(&format!("INSERT INTO u [{rows}]"), session, None).await?;
	let error = transaction_failure(results).expect("the duplicate row must fail the insert");
	assert!(error.contains("already contains"), "unexpected error: {error}");
	expect_indexed_query_len(ds, session, "SELECT id FROM u", 0).await?;
	expect_indexed_query_len(ds, session, "SELECT id FROM u WITH INDEX ux WHERE e = 'v100'", 0)
		.await?;
	Ok(())
}

/// Two transactions that each create the same unique value, overlapping, leave
/// the first commit's row alone and fail the other.
async fn unique_collision_across_transactions(ds: Arc<Datastore>, session: Session) -> Result<()> {
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE u SCHEMALESS;
			DEFINE INDEX ux ON u FIELDS e UNIQUE;
			",
	)
	.await?;
	let run = |id: &'static str| {
		let (ds, session) = (Arc::clone(&ds), session.clone());
		tokio::spawn(async move {
			let sql = format!("BEGIN; CREATE u:{id} SET e = 'x'; SLEEP 500ms; COMMIT;");
			let results = ds.execute(&sql, &session, None).await?;
			Ok::<_, anyhow::Error>(transaction_failure(results))
		})
	};
	let (first, second) = (run("a"), run("b"));
	let outcomes = [first.await??, second.await??];
	let errors: Vec<_> = outcomes.iter().flatten().collect();
	assert_eq!(errors.len(), 1, "exactly one of the overlapping commits fails: {outcomes:?}");
	assert!(
		errors[0].contains("already contains") || errors[0].contains("Transaction conflict:"),
		"unexpected error: {}",
		errors[0]
	);
	println!("unique collision across transactions: {}", errors[0]);
	expect_indexed_query_len(&ds, &session, "SELECT id FROM u", 1).await?;
	expect_indexed_query_len(&ds, &session, "SELECT id FROM u WITH INDEX ux WHERE e = 'x'", 1)
		.await?;
	Ok(())
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn unique_collision_in_one_transaction_mem() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	unique_collision_in_one_transaction(&ds, &session).await
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn unique_collision_across_transactions_mem() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	unique_collision_across_transactions(ds, session).await
}

#[cfg(feature = "kv-surrealkv")]
async fn new_surrealkv_index_test_ds(dir: &TempDir) -> Result<(Arc<Datastore>, Session)> {
	let ds = Datastore::builder()
		.without_maintenance_tasks()
		.build_with_path(&format!("surrealkv://{}", dir.path().to_string_lossy()))
		.await?;
	let session = Session::owner().with_ns("test").with_db("test");
	let tx = ds.transaction(TransactionType::Write).await?;
	tx.ensure_ns_db(None, "test", "test").await?;
	tx.commit().await?;
	Ok((ds, session))
}

#[cfg(feature = "kv-surrealkv")]
#[tokio::test(flavor = "multi_thread")]
async fn unique_collision_in_one_transaction_surrealkv() -> Result<()> {
	let dir = TempDir::new()?;
	let (ds, session) = new_surrealkv_index_test_ds(&dir).await?;
	unique_collision_in_one_transaction(&ds, &session).await
}

#[cfg(feature = "kv-surrealkv")]
#[tokio::test(flavor = "multi_thread")]
async fn unique_collision_across_transactions_surrealkv() -> Result<()> {
	let dir = TempDir::new()?;
	let (ds, session) = new_surrealkv_index_test_ds(&dir).await?;
	unique_collision_across_transactions(ds, session).await
}

/// What `table`'s index `index` compiles to in a build, and holds for `image`:
/// whether the entry reads the record's own id, the values it indexes, and the
/// answer of its guard.
#[cfg(feature = "kv-mem")]
async fn compiled_entry_over(
	ds: &Datastore,
	session: &Session,
	table: &str,
	index: &str,
	image: &Value,
) -> Result<(bool, Option<Vec<Value>>, Option<bool>)> {
	let (ns, db, table, ix) = get_table_index(ds, table, index).await?;
	let building = new_building_for_index(ds, session, ns, db, &table, ix).await?;
	let ctx = building.new_write_tx_ctx().await?;
	let exec = building.exec_ctx(&ctx).await?;
	let entry = building.compiled_index(&exec).await?;
	let values = entry.values(image, &exec).await?;
	let guard = entry.guard_matches(image, &exec).await?;
	ctx.tx().cancel().await?;
	Ok((entry.reads_record_id(), values, guard))
}

/// A build compiles a column or a guard that names a component of the table's
/// record id as the write path compiles it, from the structure the `id` field
/// declares, and compiles every other column as it is stored.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn a_build_compiles_the_components_of_a_named_record_id() -> Result<()> {
	use crate::val::Object;
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE TABLE t SCHEMALESS;
		 DEFINE FIELD id ON t TYPE [feed: string, n: int];
		 DEFINE INDEX by_n ON t FIELDS n;
		 DEFINE INDEX by_element ON t FIELDS id[1];
		 DEFINE INDEX by_feed_v ON t FIELDS feed, v;
		 DEFINE INDEX by_v ON t FIELDS v;
		 DEFINE INDEX only_f ON t COUNT WHERE feed = 'f';
		 DEFINE INDEX only_f_this ON t COUNT WHERE $this.feed = 'f';
		 DEFINE INDEX only_f_doc ON t COUNT WHERE @.feed = 'f';
		 DEFINE TABLE plain SCHEMALESS;
		 DEFINE INDEX plain_n ON plain FIELDS n;
		 DEFINE INDEX plain_count ON plain COUNT WHERE n = 5;",
	)
	.await?;
	let image = |table: &str, key: Vec<Value>, stored: &[(&str, Value)]| {
		let id = RecordId {
			table: table.into(),
			key: RecordIdKey::Array(key.into()),
		};
		let mut fields = vec![("id".to_owned(), Value::RecordId(id))];
		fields.extend(stored.iter().map(|(name, value)| ((*name).to_owned(), value.clone())));
		Value::Object(Object::from_iter(fields))
	};
	let keyed = image("t", vec![Value::from("f"), Value::from(2)], &[("v", Value::from(7))]);

	assert_eq!(
		compiled_entry_over(&ds, &session, "t", "by_n", &keyed).await?,
		(true, Some(vec![Value::from(2)]), None),
		"a component is read off the id"
	);
	assert_eq!(
		compiled_entry_over(&ds, &session, "t", "by_element", &keyed).await?,
		(true, Some(vec![Value::from(2)]), None),
		"`id[1]` is the same column as the component it addresses"
	);
	assert_eq!(
		compiled_entry_over(&ds, &session, "t", "by_feed_v", &keyed).await?,
		(true, Some(vec![Value::from("f"), Value::from(7)]), None),
		"a component and a stored field in one index"
	);
	assert_eq!(
		compiled_entry_over(&ds, &session, "t", "by_v", &keyed).await?,
		(false, Some(vec![Value::from(7)]), None),
		"an index over a stored field reads no part of the id"
	);
	let (reads_id, _, guard) = compiled_entry_over(&ds, &session, "t", "only_f", &keyed).await?;
	assert!(reads_id, "a guard over a component reads the id");
	assert_eq!(guard, Some(true));
	let elsewhere = image("t", vec![Value::from("g"), Value::from(2)], &[]);
	let (_, _, guard) = compiled_entry_over(&ds, &session, "t", "only_f", &elsewhere).await?;
	assert_eq!(guard, Some(false));
	for index in ["only_f_this", "only_f_doc"] {
		let (_, _, guard) = compiled_entry_over(&ds, &session, "t", index, &keyed).await?;
		assert_eq!(guard, Some(true), "{index}: a guard reading a component through the record");
		let (_, _, guard) = compiled_entry_over(&ds, &session, "t", index, &elsewhere).await?;
		assert_eq!(guard, Some(false), "{index}");
	}

	let unkeyed = image("plain", vec![Value::from("f"), Value::from(2)], &[("n", Value::from(5))]);
	assert_eq!(
		compiled_entry_over(&ds, &session, "plain", "plain_n", &unkeyed).await?,
		(false, Some(vec![Value::from(5)]), None),
		"a table whose id names no components indexes the stored field"
	);
	// Neither a column nor a guard of such a table reads the id, so the build
	// evaluates its entries over the stored value as it decodes.
	let (reads_id, _, guard) =
		compiled_entry_over(&ds, &session, "plain", "plain_count", &unkeyed).await?;
	assert!(!reads_id, "a guard over a table whose id names no components reads no part of it");
	assert_eq!(guard, Some(true));
	Ok(())
}

/// The integers a `SELECT VALUE` query returns, in the order it returns them.
#[cfg(feature = "kv-mem")]
async fn int_rows(ds: &Datastore, session: &Session, sql: &str) -> Result<Vec<i64>> {
	let mut results = ds.execute(sql, session, None).await?;
	let value = results.remove(0).result?;
	let surrealdb_types::Value::Array(rows) = value else {
		anyhow::bail!("query returned non-array value: {value:?}");
	};
	rows.iter()
		.map(|row| {
			let surrealdb_types::Value::Number(n) = row else {
				anyhow::bail!("row is not a number: {row:?}");
			};
			n.to_int().ok_or_else(|| anyhow::anyhow!("row is not an integer: {row:?}"))
		})
		.collect()
}

/// The text `EXPLAIN` renders for `sql`.
#[cfg(feature = "kv-mem")]
async fn plan_text(ds: &Datastore, session: &Session, sql: &str) -> Result<String> {
	let mut results = ds.execute(&format!("EXPLAIN {sql}"), session, None).await?;
	let res = results.remove(0).result?;
	let surrealdb_types::Value::String(plan) = res else {
		anyhow::bail!("unexpected EXPLAIN result: {res:?}");
	};
	Ok(plan)
}

/// Seeds `t`, whose id names `[feed: string, n: int]`, with `['f', 1]`,
/// `['f', 2]`, `['g', 3]` and `['g', 4]`, starts `define` (an index named
/// `ix`) held before its initial scan, and while it is held creates
/// `['f', 11]` and `['g', 12]`, deletes `['f', 1]` and updates `['g', 3]`.
/// Returns once the build is ready, the held writes having reached the index
/// through the build's queue.
#[cfg(feature = "kv-mem")]
async fn keyed_build_with_held_writes(define: &str) -> Result<(Arc<Datastore>, Session)> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE TABLE t SCHEMALESS;
		 DEFINE FIELD id ON t TYPE [feed: string, n: int];
		 CREATE t SET feed = 'f', n = 1 RETURN NONE;
		 CREATE t SET feed = 'f', n = 2 RETURN NONE;
		 CREATE t SET feed = 'g', n = 3 RETURN NONE;
		 CREATE t SET feed = 'g', n = 4 RETURN NONE;",
	)
	.await?;
	let guard = start_index_build_paused(&ds, &session, define).await?;
	for write in [
		"CREATE t SET feed = 'f', n = 11 RETURN NONE",
		"CREATE t SET feed = 'g', n = 12 RETURN NONE",
		"DELETE t:['f', 1] RETURN NONE",
		"UPDATE t:['g', 3] SET v = 1 RETURN NONE",
	] {
		execute_all_retrying_conflicts(&ds, &session, write).await?;
	}
	drop(guard);
	wait_for_index_ready(&ds, &session, "t", "ix").await?;
	let building = index_building_json(&ds, &session, "t", "ix").await?;
	assert!(
		building.get("updated").and_then(|updated| updated.as_u64()).is_some_and(|n| n > 0),
		"the held writes are replayed from the build's queue: {building}"
	);
	Ok((ds, session))
}

/// A write held in a build's queue indexes a component of a named record id
/// as the build's scan does: once the build is ready, the index over the
/// component `n` lists each row the table ends with once, and none it no
/// longer holds.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_write_queued_during_a_build_indexes_a_component_of_a_named_record_id() -> Result<()> {
	let (ds, session) =
		keyed_build_with_held_writes("DEFINE INDEX ix ON t FIELDS n CONCURRENTLY").await?;
	let all = "SELECT VALUE n FROM t WITH INDEX ix WHERE n >= 0 ORDER BY n";
	let plan = plan_text(&ds, &session, all).await?;
	assert!(plan.contains("IndexScan"), "the rows are read through the index: {plan}");
	assert_eq!(int_rows(&ds, &session, all).await?, vec![2, 3, 4, 11, 12]);
	for (n, expected) in [(1, vec![]), (3, vec![3]), (11, vec![11]), (12, vec![12])] {
		let sql = format!("SELECT VALUE n FROM t WITH INDEX ix WHERE n = {n}");
		assert_eq!(int_rows(&ds, &session, &sql).await?, expected, "{sql}");
	}
	Ok(())
}

/// A `COUNT` index guarded by a predicate over a component counts a write
/// held in its build's queue as its scan counts a stored row: the count read
/// from the index is that of the rows the guard admits once the build is
/// ready.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_write_queued_during_a_build_is_counted_by_a_guard_over_a_component() -> Result<()> {
	let (ds, session) =
		keyed_build_with_held_writes("DEFINE INDEX ix ON t COUNT WHERE feed = 'f' CONCURRENTLY")
			.await?;
	let count = "SELECT count() FROM t WHERE feed = 'f' GROUP ALL";
	let plan = plan_text(&ds, &session, count).await?;
	assert!(plan.contains("IndexCountScan"), "the count is read from the index: {plan}");
	// `['f', 2]` and `['f', 11]`: the deleted `['f', 1]` is not counted.
	assert_eq!(count_query_value(&ds, &session, count).await?, 2);
	assert_eq!(
		count_query_value(
			&ds,
			&session,
			"SELECT count() FROM t WITH NOINDEX WHERE feed = 'f' GROUP ALL"
		)
		.await?,
		2
	);
	Ok(())
}

// What a build started by a statement sees of that statement's transaction.
//
// `DEFINE INDEX` starts its build while its transaction is still open, and the
// build reads in transactions of its own. A blocking statement holds its
// transaction open by waiting on the build; the `CONCURRENTLY` tests hold it
// open with `SLEEP`, so a build that reads before the commit always does.

/// Run `sql` and fail with every statement's error, not only the first: inside
/// `BEGIN … COMMIT` the first is the "not executed" stand-in a failed statement
/// leaves on the statements before it.
async fn execute_all_reporting(ds: &Datastore, session: &Session, sql: &str) -> Result<()> {
	let errors: Vec<String> = ds
		.execute(sql, session, None)
		.await?
		.into_iter()
		.filter_map(|result| result.result.err().map(|err| err.to_string()))
		.collect();
	anyhow::ensure!(errors.is_empty(), "statements failed: {errors:#?}");
	Ok(())
}

/// A table of a thousand records, enough for the build to read it in several
/// batches, beside one record a full-text query can name.
async fn seed_defining_transaction_users(ds: &Datastore, session: &Session) -> Result<()> {
	execute_all(
		ds,
		session,
		"
		DEFINE TABLE user SCHEMALESS;
		CREATE |user:1000| SET email = string::concat('user', rand::string(8), '@example.com') RETURN NONE;
		CREATE user:needle SET email = 'needle@example.com' RETURN NONE;
		",
	)
	.await
}

/// How many records index `test` on `user` answers for each address the
/// staged-write tests write, keyed by the address's local part.
async fn indexed_email_rows(
	ds: &Datastore,
	session: &Session,
) -> Result<Vec<(&'static str, usize)>> {
	let mut rows = Vec::new();
	for email in ["kept", "added", "after", "moved", "before", "gone", "dup"] {
		let sql =
			format!("SELECT id FROM user WITH INDEX test WHERE email = '{email}@example.com'");
		rows.push((email, query_array_len(ds, session, &sql).await?));
	}
	Ok(rows)
}

/// Wait until the build of `index` on `table` stops, answering its report.
async fn wait_for_index_build_end(
	ds: &Datastore,
	session: &Session,
	table: &str,
	index: &str,
) -> Result<serde_json::Value> {
	timeout(Duration::from_secs(10), async {
		loop {
			let building = index_building_json(ds, session, table, index).await?;
			match building.get("status").and_then(|status| status.as_str()) {
				Some("ready" | "error" | "aborted") => return Ok(building),
				_ => sleep(Duration::from_millis(20)).await,
			}
		}
	})
	.await
	.map_err(|_| anyhow::anyhow!("timed out waiting for the index build to stop"))?
}

#[tokio::test(flavor = "multi_thread")]
async fn a_blocking_full_text_build_uses_the_analyzer_its_transaction_defines() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	seed_defining_transaction_users(&ds, &session).await?;
	execute_all_reporting(
		&ds,
		&session,
		"
		BEGIN;
		DEFINE ANALYZER simple TOKENIZERS blank,class;
		DEFINE INDEX test ON user FIELDS email FULLTEXT ANALYZER simple BM25 HIGHLIGHTS;
		COMMIT;
		",
	)
	.await?;
	let building = index_building_json(&ds, &session, "user", "test").await?;
	assert_eq!(building["status"], "ready", "{building}");
	assert_eq!(building["initial"], 1001, "{building}");
	expect_indexed_query_len(&ds, &session, "SELECT id FROM user WHERE email @@ 'needle'", 1)
		.await?;
	Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_concurrently_build_reads_the_analyzer_its_transaction_defines() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	seed_defining_transaction_users(&ds, &session).await?;
	execute_all_reporting(
		&ds,
		&session,
		"
		BEGIN;
		DEFINE ANALYZER simple TOKENIZERS blank,class;
		DEFINE INDEX test ON user FIELDS email FULLTEXT ANALYZER simple BM25 HIGHLIGHTS CONCURRENTLY;
		SLEEP 500ms;
		COMMIT;
		",
	)
	.await?;
	let building = wait_for_index_build_end(&ds, &session, "user", "test").await?;
	assert_eq!(building["status"], "ready", "{building}");
	assert_eq!(building["initial"], 1001, "{building}");
	expect_indexed_query_len(&ds, &session, "SELECT id FROM user WHERE email @@ 'needle'", 1)
		.await?;
	Ok(())
}

/// A decommission committed with the definition is visible to the build's
/// first check, so the build stops instead of publishing an index that is
/// already being removed.
#[tokio::test(flavor = "multi_thread")]
async fn a_concurrently_build_stops_on_a_prepare_remove_committed_with_it() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	seed_defining_transaction_users(&ds, &session).await?;
	execute_all_reporting(
		&ds,
		&session,
		"
		BEGIN;
		DEFINE INDEX test ON user FIELDS email CONCURRENTLY;
		SLEEP 500ms;
		ALTER INDEX test ON user PREPARE REMOVE;
		COMMIT;
		",
	)
	.await?;
	let building = wait_for_index_build_end(&ds, &session, "user", "test").await?;
	assert_eq!(building["status"], "error", "{building}");
	assert_eq!(
		building["error"], "Index building has been cancelled: Prepare remove.",
		"{building}"
	);
	Ok(())
}

/// Records the defining transaction writes before `DEFINE INDEX` are never
/// queued for the build: no index existed when they were written. The build's
/// scan has to read them, which it can only do once they are committed.
#[tokio::test(flavor = "multi_thread")]
async fn a_concurrently_build_indexes_what_its_transaction_wrote_before_it() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
		DEFINE TABLE user SCHEMALESS;
		CREATE user:kept SET email = 'kept@example.com' RETURN NONE;
		CREATE user:changed SET email = 'before@example.com' RETURN NONE;
		CREATE user:gone SET email = 'gone@example.com' RETURN NONE;
		CREATE user:moved SET email = 'dup@example.com' RETURN NONE;
		",
	)
	.await?;
	execute_all_reporting(
		&ds,
		&session,
		"
		BEGIN;
		CREATE user:added SET email = 'added@example.com' RETURN NONE;
		UPDATE user:changed SET email = 'after@example.com' RETURN NONE;
		DELETE user:gone;
		UPDATE user:moved SET email = 'moved@example.com' RETURN NONE;
		DEFINE INDEX test ON user FIELDS email UNIQUE CONCURRENTLY;
		SLEEP 500ms;
		COMMIT;
		",
	)
	.await?;
	wait_for_index_ready(&ds, &session, "user", "test").await?;
	assert_eq!(
		indexed_email_rows(&ds, &session).await?,
		[
			("kept", 1),
			("added", 1),
			("after", 1),
			("moved", 1),
			("before", 0),
			("gone", 0),
			("dup", 0)
		]
	);
	// The value `user:moved` gave up is free: a stale entry would refuse it.
	execute_all_reporting(
		&ds,
		&session,
		"CREATE user:taker SET email = 'dup@example.com' RETURN NONE;",
	)
	.await?;
	Ok(())
}

/// Whether `error` refuses a blocking build of index `test` on `table` because
/// the transaction has written the table's records.
fn is_blocking_build_refusal(error: &str, table: &str) -> bool {
	error.contains(&format!("Cannot define index `test` on `{table}`"))
		&& error.contains("CONCURRENTLY")
}

/// The local part of every address the records of `user` hold, in order.
async fn user_emails(ds: &Datastore, session: &Session) -> Result<Vec<String>> {
	let sql = "SELECT VALUE email FROM user ORDER BY email";
	let json = ds.execute(sql, session, None).await?.remove(0).result?.into_json_value();
	Ok(json
		.as_array()
		.into_iter()
		.flatten()
		.filter_map(|email| Some(email.as_str()?.split('@').next()?.to_owned()))
		.collect())
}

/// The names of the indexes `table` defines.
async fn table_index_names(ds: &Datastore, session: &Session, table: &str) -> Result<Vec<String>> {
	let sql = format!("INFO FOR TABLE {table}");
	let json = ds.execute(&sql, session, None).await?.remove(0).result?.into_json_value();
	Ok(json
		.pointer("/indexes")
		.and_then(|indexes| indexes.as_object())
		.map(|indexes| indexes.keys().cloned().collect())
		.unwrap_or_default())
}

/// A blocking build reads the table as last committed, so it cannot see what
/// its own transaction wrote before it: built there, an index would miss the
/// records the transaction created or changed, and keep entries for the
/// values it changed or deleted, which a UNIQUE index then refuses to anyone.
/// `DEFINE INDEX` without `CONCURRENTLY` is refused in a transaction that has
/// written the table's records, and the transaction keeps nothing. Committing
/// the writes first and then defining the index indexes them; the
/// `CONCURRENTLY` counterpart above defines it in the same transaction.
#[tokio::test(flavor = "multi_thread")]
async fn a_blocking_build_is_refused_after_its_transaction_wrote_the_table() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
		DEFINE TABLE user SCHEMALESS;
		CREATE user:kept SET email = 'kept@example.com' RETURN NONE;
		CREATE user:changed SET email = 'before@example.com' RETURN NONE;
		CREATE user:gone SET email = 'gone@example.com' RETURN NONE;
		CREATE user:moved SET email = 'dup@example.com' RETURN NONE;
		",
	)
	.await?;
	let writes = "
		CREATE user:added SET email = 'added@example.com' RETURN NONE;
		UPDATE user:changed SET email = 'after@example.com' RETURN NONE;
		DELETE user:gone;
		UPDATE user:moved SET email = 'moved@example.com' RETURN NONE;
	";
	let err = execute_all_reporting(
		&ds,
		&session,
		&format!("BEGIN; {writes} DEFINE INDEX test ON user FIELDS email UNIQUE; COMMIT;"),
	)
	.await
	.expect_err("the definition is refused");
	assert!(is_blocking_build_refusal(&err.to_string(), "user"), "{err:#}");
	// The transaction kept nothing it wrote, the definition included.
	assert_eq!(user_emails(&ds, &session).await?, ["before", "dup", "gone", "kept"]);
	assert_eq!(table_index_names(&ds, &session, "user").await?, Vec::<String>::new());
	// Committed first, the writes are what the build reads.
	execute_all_reporting(&ds, &session, &format!("BEGIN; {writes} COMMIT;")).await?;
	execute_all_reporting(&ds, &session, "DEFINE INDEX test ON user FIELDS email UNIQUE;").await?;
	wait_for_index_ready(&ds, &session, "user", "test").await?;
	assert_eq!(
		indexed_email_rows(&ds, &session).await?,
		[
			("kept", 1),
			("added", 1),
			("after", 1),
			("moved", 1),
			("before", 0),
			("gone", 0),
			("dup", 0)
		]
	);
	// The value `user:moved` gave up is free.
	execute_all_reporting(
		&ds,
		&session,
		"CREATE user:taker SET email = 'dup@example.com' RETURN NONE;",
	)
	.await?;
	Ok(())
}

/// Every statement that writes or deletes a record of the table before a
/// blocking `DEFINE INDEX` in its transaction refuses the definition: the
/// table's own writes and deletes, a whole-table delete, a removal of the
/// table, an edge `RELATE` writes into its edge table, the rows a view's
/// definition computes, and the records of a table the transaction itself
/// creates.
#[tokio::test(flavor = "multi_thread")]
async fn every_write_of_the_tables_records_refuses_a_blocking_build() -> Result<()> {
	let cases = [
		("user", "CREATE user:new SET email = 'new@example.com'"),
		("user", "UPDATE user:kept SET email = 'new@example.com'"),
		("user", "UPSERT user:new SET email = 'new@example.com'"),
		("user", "INSERT INTO user { id: user:new, email: 'new@example.com' }"),
		("user", "FOR $i IN 1..=3 { CREATE user SET email = 'new@example.com' }"),
		("user", "DELETE user:kept"),
		("user", "DELETE user"),
		("user", "REMOVE TABLE user; DEFINE TABLE user SCHEMALESS"),
		("likes", "RELATE user:kept->likes->user:kept SET email = 'new@example.com'"),
		("adult", "DEFINE TABLE adult AS SELECT * FROM user"),
		("fresh", "CREATE fresh:1 SET email = 'new@example.com'"),
	];
	for (table, write) in cases {
		let (ds, session) = new_index_test_ds().await?;
		execute_all(
			&ds,
			&session,
			"DEFINE TABLE user SCHEMALESS; CREATE user:kept SET email = 'kept@example.com';",
		)
		.await?;
		let sql = format!("BEGIN; {write}; DEFINE INDEX test ON {table} FIELDS email; COMMIT;");
		let err = execute_all_reporting(&ds, &session, &sql).await.expect_err(write);
		assert!(is_blocking_build_refusal(&err.to_string(), table), "{write}: {err:#}");
		assert_eq!(user_emails(&ds, &session).await?, ["kept"], "{write}");
	}
	Ok(())
}

/// A transaction that has written none of the table's records, or whose
/// definition builds nothing, defines the index: one that only read the
/// table, one that wrote another table, and one whose `IF NOT EXISTS` finds
/// the index defined.
#[tokio::test(flavor = "multi_thread")]
async fn a_blocking_build_is_not_refused_without_a_write_it_would_miss() -> Result<()> {
	for (write, define) in [
		("SELECT * FROM user", ""),
		("CREATE other:1 SET email = 'new@example.com'", ""),
		("CREATE user:new SET email = 'new@example.com'", "IF NOT EXISTS"),
	] {
		let (ds, session) = new_index_test_ds().await?;
		execute_all(
			&ds,
			&session,
			"DEFINE TABLE user SCHEMALESS; CREATE user:kept SET email = 'kept@example.com';",
		)
		.await?;
		if !define.is_empty() {
			execute_all(&ds, &session, "DEFINE INDEX test ON user FIELDS email;").await?;
		}
		let sql = format!(
			"BEGIN; {write}; DEFINE INDEX {define} test ON user FIELDS email UNIQUE; COMMIT;"
		);
		execute_all_reporting(&ds, &session, &sql).await?;
		wait_for_index_ready(&ds, &session, "user", "test").await?;
		let kept = "SELECT id FROM user WITH INDEX test WHERE email = 'kept@example.com'";
		assert_eq!(query_array_len(&ds, &session, kept).await?, 1, "{write}");
	}
	Ok(())
}

/// A definition that builds nothing is not refused, and one that builds is: an
/// `OVERWRITE` that restates the definition of a ready index keeps the index
/// and its entries, so the records its transaction wrote are indexed as they
/// are written and nothing is left out, while an `OVERWRITE` that changes the
/// definition builds over the committed records and is refused over the writes
/// its transaction made, as a first definition is.
#[tokio::test(flavor = "multi_thread")]
async fn an_overwrite_is_refused_after_a_write_only_when_it_builds() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
		DEFINE TABLE user SCHEMALESS;
		CREATE user:kept SET email = 'kept@example.com' RETURN NONE;
		DEFINE INDEX test ON user FIELDS email;
		",
	)
	.await?;
	wait_for_index_ready(&ds, &session, "user", "test").await?;
	let initial = |building: serde_json::Value| building.get("initial").cloned();
	let first_build = initial(index_building_json(&ds, &session, "user", "test").await?);
	assert_eq!(first_build, Some(serde_json::json!(1)));
	// Restated, the definition keeps the index: no build runs, so there is nothing for the
	// transaction's write to miss, and the index answers for the record it wrote. The build
	// state still counts the records of the first build alone.
	execute_all_reporting(
		&ds,
		&session,
		"
		BEGIN;
		CREATE user:new SET email = 'new@example.com' RETURN NONE;
		DEFINE INDEX OVERWRITE test ON user FIELDS email COMMENT 'restated';
		COMMIT;
		",
	)
	.await?;
	assert_eq!(initial(index_building_json(&ds, &session, "user", "test").await?), first_build);
	let new = "SELECT id FROM user WITH INDEX test WHERE email = 'new@example.com'";
	assert_eq!(query_array_len(&ds, &session, new).await?, 1);
	// Changed, the definition builds, and it is refused over what the transaction wrote.
	let err = execute_all_reporting(
		&ds,
		&session,
		"
		BEGIN;
		CREATE user:newer SET email = 'newer@example.com' RETURN NONE;
		DEFINE INDEX OVERWRITE test ON user FIELDS email UNIQUE;
		COMMIT;
		",
	)
	.await
	.expect_err("a definition that builds is refused");
	assert!(is_blocking_build_refusal(&err.to_string(), "user"), "{err:#}");
	// The transaction kept nothing, and the index is the one the restatement left.
	assert_eq!(user_emails(&ds, &session).await?, ["kept", "new"]);
	let sql = "RETURN (INFO FOR TABLE user).indexes.test";
	let defined = ds.execute(sql, &session, None).await?.remove(0).result?.into_json_value();
	let defined = defined.as_str().unwrap_or_default().to_owned();
	assert!(defined.contains("COMMENT 'restated'") && !defined.contains("UNIQUE"), "{defined}");
	assert_eq!(query_array_len(&ds, &session, new).await?, 1);
	Ok(())
}

/// Removing a table removes its records, so a blocking build after the removal
/// in the same transaction would index records the removal took: the definition
/// is refused when the table held a record, and defined when it held none.
#[tokio::test(flavor = "multi_thread")]
async fn a_blocking_build_after_a_table_removal_is_refused_only_for_the_records_removed()
-> Result<()> {
	let sql = "
		BEGIN;
		REMOVE TABLE user;
		DEFINE TABLE user SCHEMALESS;
		DEFINE INDEX test ON user FIELDS email;
		COMMIT;
	";
	for (setup, held_a_record) in [
		("DEFINE TABLE user SCHEMALESS; CREATE user:kept SET email = 'kept@example.com';", true),
		("DEFINE TABLE user SCHEMALESS;", false),
	] {
		let (ds, session) = new_index_test_ds().await?;
		execute_all(&ds, &session, setup).await?;
		let outcome = execute_all_reporting(&ds, &session, sql).await;
		if held_a_record {
			let err = outcome.expect_err("the removal took a record");
			assert!(is_blocking_build_refusal(&err.to_string(), "user"), "{err:#}");
			assert_eq!(user_emails(&ds, &session).await?, ["kept"]);
			assert_eq!(table_index_names(&ds, &session, "user").await?, Vec::<String>::new());
		} else {
			outcome?;
			wait_for_index_ready(&ds, &session, "user", "test").await?;
			assert_eq!(table_index_names(&ds, &session, "user").await?, ["test"]);
		}
	}
	Ok(())
}

/// A write a save point rolled back is no write the build would miss: an
/// `INSERT IGNORE` whose row a UNIQUE index refuses writes the record, then
/// undoes it, and a blocking build after it in the transaction is allowed.
#[tokio::test(flavor = "multi_thread")]
async fn a_write_its_save_point_undid_does_not_refuse_a_blocking_build() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
		DEFINE TABLE user SCHEMALESS;
		DEFINE INDEX email ON user FIELDS email UNIQUE;
		CREATE user:kept SET email = 'kept@example.com';
		",
	)
	.await?;
	execute_all_reporting(
		&ds,
		&session,
		"
		BEGIN;
		INSERT IGNORE INTO user { id: user:new, email: 'kept@example.com' };
		DEFINE INDEX test ON user FIELDS email;
		COMMIT;
		",
	)
	.await?;
	wait_for_index_ready(&ds, &session, "user", "test").await?;
	assert_eq!(user_emails(&ds, &session).await?, ["kept"]);
	let kept = "SELECT id FROM user WITH INDEX test WHERE email = 'kept@example.com'";
	assert_eq!(query_array_len(&ds, &session, kept).await?, 1);
	Ok(())
}

/// A client-owned transaction outlives a statement that fails, and its client
/// can still commit it. The refusal comes before the definition writes
/// anything, so the commit keeps the transaction's writes and no index.
#[tokio::test(flavor = "multi_thread")]
async fn a_client_owned_transaction_commits_nothing_of_a_refused_definition() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE TABLE user SCHEMALESS; CREATE user:kept SET email = 'kept@example.com';",
	)
	.await?;
	let tx = Arc::new(ds.transaction(TransactionType::Write).await?);
	let sql = "
		CREATE user:new SET email = 'new@example.com' RETURN NONE;
		DEFINE INDEX test ON user FIELDS email UNIQUE;
	";
	let request = crate::kvs::QueryRequest::new(sql, &session).with_transaction(Arc::clone(&tx));
	let errors: Vec<String> = ds
		.run(request)
		.await?
		.into_iter()
		.filter_map(|result| result.result.err().map(|err| err.to_string()))
		.collect();
	assert!(
		matches!(errors.as_slice(), [error] if is_blocking_build_refusal(error, "user")),
		"{errors:#?}"
	);
	tx.commit().await?;
	assert_eq!(user_emails(&ds, &session).await?, ["kept", "new"]);
	assert_eq!(table_index_names(&ds, &session, "user").await?, Vec::<String>::new());
	Ok(())
}

/// A rolled-back definition stops its build promptly, whatever the build was
/// doing: the rollback waits for the build to stop before deleting what it
/// wrote, for at most `BUILD_ABORT_STOP_BUDGET`.
#[tokio::test(flavor = "multi_thread")]
async fn a_rolled_back_definition_stops_its_concurrently_build_promptly() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	seed_defining_transaction_users(&ds, &session).await?;
	let started = Instant::now();
	execute_cancelled_transaction(
		&ds,
		&session,
		"BEGIN; DEFINE INDEX test ON user FIELDS email CONCURRENTLY; SLEEP 200ms; CANCEL;",
	)
	.await?;
	assert!(
		started.elapsed() < BUILD_ABORT_STOP_BUDGET / 3,
		"the rollback waited {:?} for the build to stop",
		started.elapsed()
	);
	execute_all(&ds, &session, "DEFINE INDEX test ON user FIELDS email CONCURRENTLY").await?;
	wait_for_index_ready(&ds, &session, "user", "test").await?;
	let (ns, db, table, current_ix) = get_table_index(&ds, "user", "test").await?;
	assert_no_index_build_artifacts(&ds, ns, db, &table, previous_index_id(current_ix.index_id))
		.await?;
	Ok(())
}

/// A planner shares the value shapes of an index through the node's cache only
/// once the index's build has finished, or never ran: a build records shapes
/// without stamping the catalog version, so an index still building is read at
/// each planner's snapshot. A shape recorded with a stamp reaches the shared
/// entry of the new version.
#[cfg(feature = "kv-mem")]
#[test_log::test(tokio::test)]
async fn planning_shares_the_shapes_of_finished_builds_only() -> Result<()> {
	use surrealdb_datastore::cache::ds::DatastoreCache;
	use surrealdb_datastore::index_shapes::ValueShape;

	let ds = Datastore::builder().build_with_path("memory").await?;
	let cache = DatastoreCache::new(64);
	let (ns, db) = (NamespaceId(1), DatabaseId(1));
	let tb = TableName::from("t");
	let index = |id: u32, name: &str| IndexDefinition {
		index_id: IndexId(id),
		name: name.into(),
		table_name: tb.clone(),
		cols: vec![crate::expr::Idiom::field("v")],
		index: Index::Idx,
		count_cond: None,
		prepare_remove: false,
		format_version: crate::catalog::INDEX_FORMAT_VERSION,
		comment: None,
		nulls_not_distinct: false,
	};
	let (finished, building) = (index(1, "finished"), index(2, "building"));
	let indexes = [finished.clone(), building.clone()];
	let array = [(0u16, ValueShape::Array)];

	let tx = ds.transaction(TransactionType::Write).await?;
	tx.bump_catalog_version(ns, db, &tb).await?;
	tx.track_index_shapes(ns, db, &tb, finished.index_id, &[]).await?;
	tx.track_index_shapes(ns, db, &tb, building.index_id, &[]).await?;
	let ikb = IndexKeyBase::new(ns, db, tb.clone(), building.index_id);
	tx.set_key(
		&ikb.new_bs_key(),
		&durable_build_state_for_phase(IndexBuildPhase::Building, 1, Some(Uuid::now_v7())),
	)
	.await?;
	tx.commit().await?;

	// The first planner loads the entry, which holds the finished index only.
	let tx = ds.transaction(TransactionType::Read).await?;
	let shapes = tx.planning_index_shapes(Some(&cache), ns, db, &tb, &indexes).await?;
	assert!(!shapes.of(finished.index_id).expect("read").may_hold(0, ValueShape::Array));
	assert!(!shapes.of(building.index_id).expect("read").may_hold(0, ValueShape::Array));
	tx.cancel().await?;

	// Shapes recorded without a stamp, as a build records them.
	let tx = ds.transaction(TransactionType::Write).await?;
	tx.record_index_shapes(
		ns,
		db,
		&tb,
		finished.index_id,
		&array,
		surrealdb_datastore::index_shapes::ShapeRecorder::Build,
	)
	.await?;
	tx.record_index_shapes(
		ns,
		db,
		&tb,
		building.index_id,
		&array,
		surrealdb_datastore::index_shapes::ShapeRecorder::Build,
	)
	.await?;
	tx.commit().await?;

	// The building index is read at the snapshot; the finished one is served
	// from the entry of the unchanged version.
	let tx = ds.transaction(TransactionType::Read).await?;
	let shapes = tx.planning_index_shapes(Some(&cache), ns, db, &tb, &indexes).await?;
	assert!(
		shapes.of(building.index_id).expect("read").may_hold(0, ValueShape::Array),
		"an index still building is never served from the shared entry"
	);
	assert!(
		!shapes.of(finished.index_id).expect("read").may_hold(0, ValueShape::Array),
		"a finished index is served from the shared entry of its version"
	);
	tx.cancel().await?;

	// A stamp moves planners to a new version, whose entry is read afresh.
	let tx = ds.transaction(TransactionType::Write).await?;
	tx.bump_catalog_version(ns, db, &tb).await?;
	tx.commit().await?;
	let tx = ds.transaction(TransactionType::Read).await?;
	let shapes = tx.planning_index_shapes(Some(&cache), ns, db, &tb, &indexes).await?;
	assert!(shapes.of(finished.index_id).expect("read").may_hold(0, ValueShape::Array));
	tx.cancel().await?;
	Ok(())
}

/// A blocking build carries the registration its waiting statement lends it,
/// so the places its parallel work takes count as that statement's. A
/// `CONCURRENTLY` build, or a blocking one whose statement lends none, takes
/// its places through a build ticket of its own.
#[tokio::test(flavor = "multi_thread")]
async fn a_blocking_build_counts_its_places_as_the_waiting_statement() -> Result<()> {
	use super::builder::carried_places;
	use crate::catalog::TableId;
	use crate::ctx::{FrozenContext, IndexPlaces};
	use crate::exec::config::ExecConfig;
	use crate::exec::planner::placement::Policy;
	use crate::exec::vm::morsel::places::Pool;
	use crate::idx::env::IndexEnv;

	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE TABLE user SCHEMALESS; DEFINE INDEX test ON user FIELDS email;",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "user", "test").await?;
	let pool = Pool::new(4);
	let statement = pool.register();
	let lent =
		IndexPlaces::statement(statement.clone(), Policy::from_config(&ExecConfig::default()));
	let mut ctx = ds.setup_ctx()?;
	ctx.set_index_places(lent.clone());
	let ctx = ctx.freeze();
	assert!(carried_places(&ctx, true).is_some_and(|carried| carried.same(&lent)));
	assert!(carried_places(&ctx, false).is_none(), "a CONCURRENTLY build carries none");
	assert!(carried_places(&ds.setup_ctx()?.freeze(), true).is_none());
	let new_build = |ctx: &FrozenContext, carried| {
		Building::with_places(
			ctx,
			ds.transaction_factory().clone(),
			ds.setup_options(&session),
			TableId(0),
			Arc::clone(&ix),
			Arc::new(IndexKey::new(ns, db, &table, ix.index_id)),
			carried,
		)
	};

	let carrying = new_build(&ctx, carried_places(&ctx, true))?;
	let batch = carrying.new_write_tx_ctx().await?;
	let places = IndexEnv::places(&*batch);
	assert!(places.same(&lent), "the build takes places through the statement's registration");
	let grant = places.acquire_places(3);
	assert_eq!(grant.len(), 3);
	assert_eq!(statement.held(), 3, "the build's places are the statement's");
	assert_eq!(pool.registrations(), 1, "the build registered nothing of its own");
	drop((grant, places));
	batch.tx().cancel().await?;
	drop(batch);
	// A finished build stays in the builder's map, holding no registration.
	carrying.release_places();
	drop((ctx, lent, statement));
	assert_eq!(pool.registrations(), 0, "a finished build keeps no registration");

	let own = new_build(&ds.setup_ctx()?.freeze(), None)?;
	let places = own.places();
	assert!(own.places().same(&places), "one build ticket for the whole build");
	let batch = own.new_write_tx_ctx().await?;
	assert!(IndexEnv::places(&*batch).same(&places), "every batch carries the build's ticket");
	batch.tx().cancel().await?;
	own.release_places();
	assert!(!own.places().same(&places), "a finished build holds no ticket");
	Ok(())
}

/// A blocking `DEFINE INDEX` and a blocking `REBUILD INDEX`, run through the
/// executor, lend the statement's registration to the build they wait on, so
/// the build's places count as the statement's. A `CONCURRENTLY` build has no
/// statement waiting on it and is lent none.
#[tokio::test(flavor = "multi_thread")]
async fn a_blocking_statement_lends_its_registration_to_its_build() -> Result<()> {
	use super::builder::lent_places;

	let (ds, session) = new_index_test_ds().await?;
	let node = ds.id();
	execute_all(
		&ds,
		&session,
		"DEFINE TABLE user SCHEMALESS; CREATE user:one SET email = 'one@example.com';",
	)
	.await?;
	execute_all(&ds, &session, "DEFINE INDEX blocking ON user FIELDS email;").await?;
	assert_eq!(lent_places::take(node, "blocking"), Some(true), "DEFINE INDEX lends");
	execute_all(&ds, &session, "REBUILD INDEX blocking ON user;").await?;
	assert_eq!(lent_places::take(node, "blocking"), Some(true), "REBUILD INDEX lends");
	execute_all(&ds, &session, "DEFINE INDEX concurrent ON user FIELDS email CONCURRENTLY;")
		.await?;
	assert_eq!(lent_places::take(node, "concurrent"), Some(false), "CONCURRENTLY lends none");
	Ok(())
}

/// A governor that decides as a [`FairGovernor`] decides and counts the
/// leases it opens and closes.
///
/// [`FairGovernor`]: crate::exec::memory::FairGovernor
struct CountingGovernor {
	inner: crate::exec::memory::FairGovernor,
	opened: std::sync::atomic::AtomicUsize,
	closed: std::sync::atomic::AtomicUsize,
}

impl crate::exec::memory::MemoryGovernor for CountingGovernor {
	fn opened(&self, lease: &Arc<crate::exec::memory::Lease>) {
		self.opened.fetch_add(1, Ordering::SeqCst);
		self.inner.opened(lease);
	}

	fn grow(&self, lease: &crate::exec::memory::Lease, bytes: usize) -> bool {
		self.inner.grow(lease, bytes)
	}

	fn closed(&self, lease: &crate::exec::memory::Lease) {
		self.closed.fetch_add(1, Ordering::SeqCst);
		self.inner.closed(lease);
	}

	fn admit(
		&self,
		lease: &crate::exec::memory::Lease,
		bytes: usize,
		may_wait: bool,
	) -> crate::exec::memory::Admit {
		self.inner.admit(lease, bytes, may_wait)
	}

	fn changed(&self, epoch: u64) -> crate::exec::memory::BoxFut<'_, ()> {
		self.inner.changed(epoch)
	}

	fn released(&self, lease: &crate::exec::memory::Lease) {
		self.inner.released(lease);
	}

	fn overcommitted(&self, lease: &crate::exec::memory::Lease, bytes: usize) {
		crate::exec::memory::MemoryGovernor::overcommitted(&self.inner, lease, bytes);
	}

	fn unwait(&self, lease: &crate::exec::memory::Lease) {
		self.inner.unwait(lease);
	}

	fn regrant(&self, lease: &crate::exec::memory::Lease, bytes: usize) {
		self.inner.regrant(lease, bytes);
	}

	fn limit(&self) -> Option<usize> {
		crate::exec::memory::MemoryGovernor::limit(&self.inner)
	}
}

/// A build the builder remembers once it has finished holds no lease from the
/// node's memory governor: neither the lease of the query that defined the
/// index, which ended before the build is forgotten, nor one its batches
/// opened. Every lease opened is closed, so the governor grants nothing.
#[tokio::test]
async fn a_finished_build_holds_no_memory_lease() -> Result<()> {
	let governor = Arc::new(CountingGovernor {
		inner: crate::exec::memory::FairGovernor::new(64 << 20),
		opened: Default::default(),
		closed: Default::default(),
	});
	let ds = Datastore::builder()
		.without_maintenance_tasks()
		.with_memory_governor(Arc::clone(&governor) as Arc<dyn crate::exec::memory::MemoryGovernor>)
		.build_with_path("memory")
		.await?;
	let session = Session::owner().with_ns("test").with_db("test");
	let tx = ds.transaction(TransactionType::Write).await?;
	tx.ensure_ns_db(None, "test", "test").await?;
	tx.commit().await?;
	// The rows are written by the query that defines the index, so the
	// query's writes open its lease before the build starts.
	execute_all(
		&ds,
		&session,
		"DEFINE TABLE t;
		 FOR $i IN 0..200 { CREATE type::record('t', $i) SET n = $i };
		 DEFINE INDEX ix ON t FIELDS n;",
	)
	.await?;
	let (ns, db, tb, ix) = get_table_index(&ds, "t", "ix").await?;
	let building = local_builder_for_key(&ds, ns, db, &tb, ix.index_id)
		.await?
		.expect("the builder remembers the build");
	wait_for_finished_builder(&building).await?;
	assert!(governor.opened.load(Ordering::SeqCst) > 0, "the defining query opened no lease");
	let deadline = Instant::now() + Duration::from_secs(5);
	while governor.inner.granted() > 0 && Instant::now() < deadline {
		sleep(Duration::from_millis(10)).await;
	}
	assert_eq!(
		governor.inner.granted(),
		0,
		"a lease stayed open: {} opened, {} closed",
		governor.opened.load(Ordering::SeqCst),
		governor.closed.load(Ordering::SeqCst),
	);
	assert_eq!(governor.opened.load(Ordering::SeqCst), governor.closed.load(Ordering::SeqCst));
	// The build is still remembered: what it held was released while it was.
	assert!(local_builder_for_key(&ds, ns, db, &tb, ix.index_id).await?.is_some());
	// The build's context carries nothing of the defining statement's query.
	assert!(building.ctx.memory().is_none());
	assert!(building.ctx.statement_counters().is_none());
	// A batch is bounded by a query memory of its own, whose lease closes when
	// the batch's context drops.
	let batch = building.new_write_tx_ctx().await?;
	let memory = Arc::clone(batch.memory().expect("a batch is bounded by the node's governor"));
	let lease = Arc::clone(memory.lease());
	assert_eq!(governor.inner.granted(), lease.grant());
	batch.tx().cancel().await?;
	drop((batch, memory, lease));
	assert_eq!(governor.inner.granted(), 0, "the batch's lease outlived its context");
	assert_eq!(governor.opened.load(Ordering::SeqCst), governor.closed.load(Ordering::SeqCst));
	Ok(())
}

/// The materialise lock this node's passes over `ikb` take, which a test
/// holds to keep a statement's phase-2 wait from running a pass.
#[cfg(feature = "kv-mem")]
fn materialise_lock(ds: &Datastore, ikb: &IndexKeyBase) -> Result<Arc<tokio::sync::Mutex<()>>> {
	let ctx = ds.setup_ctx()?.freeze();
	Ok(ctx.get_index_builder().expect("index builder should exist").materialise_lock(ikb))
}

/// Whether `task` is still running after `bound`: given that long to finish,
/// it did not.
#[cfg(feature = "kv-mem")]
async fn still_running<T>(task: &tokio::task::JoinHandle<T>, bound: Duration) -> bool {
	let deadline = tokio::time::Instant::now() + bound;
	while tokio::time::Instant::now() < deadline {
		if task.is_finished() {
			return false;
		}
		sleep(Duration::from_millis(20)).await;
	}
	!task.is_finished()
}

/// Wait until a build of `ikb` later than generation `after` is `Online`.
#[cfg(feature = "kv-mem")]
async fn wait_for_a_later_build_online(
	ds: &Datastore,
	ikb: &IndexKeyBase,
	after: BuildGeneration,
) -> Result<()> {
	timeout(Duration::from_secs(10), async {
		loop {
			let state = durable_build_state(ds, ikb).await?;
			if state.generation > after && state.phase == IndexBuildPhase::Online {
				return Ok::<_, anyhow::Error>(());
			}
			sleep(Duration::from_millis(20)).await;
		}
	})
	.await?
}

/// `INFO FOR INDEX`'s `compacting` for index `index` of table `t`.
#[cfg(feature = "kv-mem")]
async fn compacting(ds: &Datastore, session: &Session, index: &str) -> Result<bool> {
	let building = index_building_json(ds, session, "t", index).await?;
	Ok(building.get("compacting").and_then(|v| v.as_bool()).expect("compacting is reported"))
}

/// The rows of table `t`, then a full-text index `ft` over them and its key
/// base, on a datastore with no background compaction.
#[cfg(feature = "kv-mem")]
async fn table_with_a_fulltext_index(ds: &Datastore, session: &Session) -> Result<IndexKeyBase> {
	execute_all(
		ds,
		session,
		"DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase;
		 CREATE t:1 SET text = 'alpha beta';
		 CREATE t:2 SET text = 'beta gamma';
		 CREATE t:3 SET text = 'gamma delta';
		 DEFINE INDEX ft ON t FIELDS text FULLTEXT ANALYZER simple BM25;",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(ds, "t", "ft").await?;
	Ok(IndexKeyBase::new(ns, db, table, ix.index_id))
}

/// A blocking `DEFINE INDEX` over existing rows returns with its index
/// materialised, for every queued kind, on a datastore that runs no
/// background compaction: only the statement's own phase-2 wait can have
/// folded the backlog the build left.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn blocking_define_index_returns_materialised_for_every_queued_kind() -> Result<()> {
	let mut kinds = vec![
		(
			"ft",
			"DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase; DEFINE INDEX ft ON t FIELDS text FULLTEXT ANALYZER simple BM25",
		),
		("cx", "DEFINE INDEX cx ON t COUNT"),
		("hx", "DEFINE INDEX hx ON t FIELDS vec HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32"),
	];
	#[cfg(diskann)]
	kinds.push((
		"dx",
		"DEFINE INDEX dx ON t FIELDS vec DISKANN DIMENSION 2 DIST EUCLIDEAN TYPE F32",
	));
	for (name, define) in kinds {
		let (ds, session) = new_index_test_ds().await?;
		execute_all(
			&ds,
			&session,
			"CREATE t:1 SET text = 'alpha beta', vec = [1.0, 0.0];
			 CREATE t:2 SET text = 'beta gamma', vec = [0.0, 1.0];
			 CREATE t:3 SET text = 'gamma delta', vec = [1.0, 1.0];",
		)
		.await?;
		execute_all(&ds, &session, define).await?;
		let (ns, db, table, ix) = get_table_index(&ds, "t", name).await?;
		let ikb = IndexKeyBase::new(ns, db, table, ix.index_id);
		match name {
			"ft" => assert_eq!(fulltext_deltas(&ds, &ikb).await?, 0, "no !tx or !dx is left"),
			"cx" => assert_eq!(count_deltas(&ds, &ikb).await?, 0, "only the aggregate is left"),
			"hx" => assert_eq!(count_hnsw_pendings(&ds, &ikb).await?, 0, "no !hr is left"),
			_ => {}
		}
		assert!(!compacting(&ds, &session, name).await?, "{name}: the index is materialised");
	}
	Ok(())
}

/// A blocking index statement in `BEGIN … COMMIT` builds inside the block's
/// transaction and is materialised once the block has committed: the block
/// returns with the backlog folded, the statement's slot and `COMMIT`'s both
/// `NONE`.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_committed_block_returns_its_index_materialised() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase;
		 CREATE t:1 SET text = 'alpha beta';
		 CREATE t:2 SET text = 'beta gamma';",
	)
	.await?;
	let results = ds
		.execute(
			"BEGIN; DEFINE INDEX ft ON t FIELDS text FULLTEXT ANALYZER simple BM25; COMMIT;",
			&session,
			None,
		)
		.await?;
	assert_eq!(results.len(), 3, "BEGIN, DEFINE and COMMIT each take a slot");
	for result in results {
		assert_eq!(result.result?, crate::types::PublicValue::None);
	}
	let (ns, db, table, ix) = get_table_index(&ds, "t", "ft").await?;
	let ikb = IndexKeyBase::new(ns, db, table, ix.index_id);
	assert_eq!(fulltext_deltas(&ds, &ikb).await?, 0);
	assert!(!compacting(&ds, &session, "ft").await?);
	Ok(())
}

/// A blocking `REBUILD INDEX` does not return while its index's pass cannot
/// run: its phase-2 wait holds it after the index is online, and lets it go
/// with the backlog folded once the pass runs.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_blocking_rebuild_waits_for_its_pass() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	let ikb = table_with_a_fulltext_index(&ds, &session).await?;
	let built = durable_build_state(&ds, &ikb).await?.generation;
	let lock = materialise_lock(&ds, &ikb)?;
	let held = lock.lock().await;
	let rebuild = tokio::spawn({
		let (ds, session) = (Arc::clone(&ds), session.clone());
		async move { execute_all(&ds, &session, "REBUILD INDEX ft ON t").await }
	});
	wait_for_a_later_build_online(&ds, &ikb, built).await?;
	assert!(compacting(&ds, &session, "ft").await?, "the rebuild left a backlog to fold");
	assert!(
		still_running(&rebuild, Duration::from_secs(1)).await,
		"the rebuild returned before its pass ran"
	);
	drop(held);
	timeout(Duration::from_secs(10), rebuild).await???;
	assert_eq!(fulltext_deltas(&ds, &ikb).await?, 0);
	assert!(!compacting(&ds, &session, "ft").await?);
	Ok(())
}

/// A blocking `REBUILD INDEX` that joined another node's build waits, once
/// that build is online, for its index to be materialised as a build of its
/// own does: it holds while its own node's pass cannot run.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn distributed_blocking_rebuild_waits_for_a_remote_owners_index_to_materialise() -> Result<()>
{
	let mut kinds = vec![(
		"hx",
		"DEFINE INDEX hx ON t FIELDS vec HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32 CONCURRENTLY",
	)];
	#[cfg(diskann)]
	kinds.push((
		"dx",
		"DEFINE INDEX dx ON t FIELDS vec DISKANN DIMENSION 2 DIST EUCLIDEAN TYPE F32 CONCURRENTLY",
	));
	for (name, define) in kinds {
		let (ds_a, ds_b, session) = new_distributed_index_test_ds().await?;
		execute_all(
			&ds_a,
			&session,
			"CREATE t:1 SET vec = [1.0, 0.0];
			 CREATE t:2 SET vec = [0.0, 1.0];
			 CREATE t:3 SET vec = [1.0, 1.0];",
		)
		.await?;
		let guard = start_index_build_paused(&ds_a, &session, define).await?;
		let (ns, db, table, ix) = get_table_index(&ds_a, "t", name).await?;
		let ikb = IndexKeyBase::new(ns, db, table, ix.index_id);
		let ds_b = Arc::new(ds_b);
		let lock = materialise_lock(&ds_b, &ikb)?;
		let held = lock.lock().await;
		let rebuild = tokio::spawn({
			let (ds_b, session, sql) =
				(Arc::clone(&ds_b), session.clone(), format!("REBUILD INDEX {name} ON t"));
			async move { execute_all(&ds_b, &session, &sql).await }
		});
		drop(guard);
		wait_for_index_ready(&ds_a, &session, "t", name).await?;
		assert!(
			still_running(&rebuild, Duration::from_secs(1)).await,
			"{name}: the rebuild returned before its own node's pass ran"
		);
		drop(held);
		timeout(Duration::from_secs(10), rebuild).await???;
		assert!(!compacting(&ds_a, &session, name).await?, "{name}");
	}
	Ok(())
}

/// A phase-2 wait that reaches its deadline returns success, with the index
/// online and answering correctly and its backlog left for background
/// compaction: standalone, against the transaction timeout's remainder, and in
/// a block whose phase 2 outlasts the block's timer, which no slot then
/// reports as timed out.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_phase_two_deadline_keeps_the_committed_index() -> Result<()> {
	for sql in ["REBUILD INDEX ft ON t", "BEGIN; REBUILD INDEX ft ON t; COMMIT;"] {
		let (ds, session) =
			new_index_test_ds_with_timeouts(None, Some(Duration::from_millis(500))).await?;
		let ikb = table_with_a_fulltext_index(&ds, &session).await?;
		let lock = materialise_lock(&ds, &ikb)?;
		let held = lock.lock().await;
		let results = timeout(Duration::from_secs(10), ds.execute(sql, &session, None)).await??;
		for result in results {
			assert_eq!(result.result?, crate::types::PublicValue::None, "{sql}");
		}
		assert_eq!(index_building_status(&ds, &session, "t", "ft").await?, "ready", "{sql}");
		assert!(compacting(&ds, &session, "ft").await?, "{sql}: the backlog is left queued");
		expect_indexed_query_len(&ds, &session, "SELECT id FROM t WHERE text @@ 'beta'", 2).await?;
		assert_eq!(
			query_array_len(&ds, &session, "SELECT id FROM t WITH NOINDEX WHERE text @@ 'beta'")
				.await?,
			2
		);
		drop(held);
		Datastore::index_compaction(
			Arc::clone(&ds),
			Duration::from_secs(60),
			CancellationToken::new(),
		)
		.await?;
		assert!(!compacting(&ds, &session, "ft").await?, "{sql}");
	}
	Ok(())
}

/// A pass that fails on a retryable conflict it could not retry away is not
/// the end of the wait: the next pass folds the backlog, and the statement
/// returns materialised.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_phase_two_wait_retries_a_pass_that_failed_on_a_conflict() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	table_with_a_fulltext_index(&ds, &session).await?;
	let conflicts = crate::kvs::ds::INDEX_COMPACTION_PASS_MAX_RETRIES + 1;
	let guard =
		inject_retryable_conflicts(RetryableConflictSite::FullTextCompaction, ds.id(), conflicts);
	execute_all(&ds, &session, "REBUILD INDEX ft ON t").await?;
	assert_eq!(
		retryable_conflict_count(RetryableConflictSite::FullTextCompaction, ds.id()),
		0,
		"every injected conflict was met"
	);
	drop(guard);
	assert!(!compacting(&ds, &session, "ft").await?);
	Ok(())
}

/// A pass that fails on an error no retry cures ends the wait with
/// `IndexMaterialiseWaitStopped` as a plain query error, on the slot of the
/// statement that queued it, after a commit that stands: standalone, and in a
/// block, where `COMMIT` keeps its `NONE`.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_phase_two_pass_error_lands_on_its_statements_slot() -> Result<()> {
	for (sql, slot) in [("REBUILD INDEX ft ON t", 0), ("BEGIN; REBUILD INDEX ft ON t; COMMIT;", 1)]
	{
		let (ds, session) = new_index_test_ds().await?;
		table_with_a_fulltext_index(&ds, &session).await?;
		let _guard = inject_non_retryable_error(NonRetryableErrorSite::FullTextCompaction, ds.id());
		let results = ds.execute(sql, &session, None).await?;
		for (at, result) in results.into_iter().enumerate() {
			if at == slot {
				let err = result.result.expect_err("the statement reports its wait's error");
				assert!(
					err.to_string().starts_with("The index was committed and is ready"),
					"{sql}: {err}"
				);
				assert!(
					!matches!(err.query_details(), Some(surrealdb_types::QueryError::Cancelled)),
					"{sql}: a pass error is not a cancellation"
				);
			} else {
				assert_eq!(result.result?, crate::types::PublicValue::None, "{sql}: slot {at}");
			}
		}
		assert_eq!(index_building_status(&ds, &session, "t", "ft").await?, "ready", "{sql}");
	}
	Ok(())
}

/// Cancelling the query during phase 2 ends the wait with
/// `IndexMaterialiseWaitStopped` of the cancelled kind, on the index
/// statement's slot; the index stays committed and online.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_cancelled_phase_two_wait_reports_its_commit() -> Result<()> {
	for (sql, slot) in [("REBUILD INDEX ft ON t", 0), ("BEGIN; REBUILD INDEX ft ON t; COMMIT;", 1)]
	{
		let (ds, session) = new_index_test_ds().await?;
		let ikb = table_with_a_fulltext_index(&ds, &session).await?;
		let built = durable_build_state(&ds, &ikb).await?.generation;
		let lock = materialise_lock(&ds, &ikb)?;
		let held = lock.lock().await;
		let cancel = crate::ctx::CancelHandle::new();
		let run = tokio::spawn({
			let (ds, session, cancel) = (Arc::clone(&ds), session.clone(), cancel.clone());
			async move { run_cancellable(&ds, &session, sql, cancel).await }
		});
		wait_for_a_later_build_online(&ds, &ikb, built).await?;
		cancel.trip();
		let errors = timeout(Duration::from_secs(10), run).await???;
		drop(held);
		for (at, error) in errors.iter().enumerate() {
			if at == slot {
				assert!(
					matches!(error, Some(err) if err.starts_with("The index was committed and is ready")),
					"{sql}: {errors:?}"
				);
			} else {
				assert_eq!(error, &None, "{sql}: slot {at}");
			}
		}
		assert_eq!(index_building_status(&ds, &session, "t", "ft").await?, "ready", "{sql}");
	}
	Ok(())
}

/// A block with two blocking index statements resolves both waits: an error
/// in the first lands on the first's slot, and the second index still
/// materialises with its slot `NONE`.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn every_wait_in_a_block_resolves_onto_its_own_slot() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase;
		 CREATE t:1 SET text = 'alpha beta', title = 'one';
		 CREATE t:2 SET text = 'beta gamma', title = 'two';",
	)
	.await?;
	let _guard = inject_non_retryable_error(NonRetryableErrorSite::FullTextCompaction, ds.id());
	let results = ds
		.execute(
			"BEGIN;
			 DEFINE INDEX ft ON t FIELDS text FULLTEXT ANALYZER simple BM25;
			 DEFINE INDEX ft2 ON t FIELDS title FULLTEXT ANALYZER simple BM25;
			 COMMIT;",
			&session,
			None,
		)
		.await?;
	let outcomes: Vec<_> =
		results.into_iter().map(|r| r.result.map_err(|e| e.to_string())).collect();
	assert!(
		matches!(&outcomes[1], Err(err) if err.starts_with("The index was committed and is ready")),
		"{outcomes:?}"
	);
	assert_eq!(outcomes[2], Ok(crate::types::PublicValue::None), "{outcomes:?}");
	assert_eq!(outcomes[3], Ok(crate::types::PublicValue::None), "{outcomes:?}");
	let (ns, db, table, ix) = get_table_index(&ds, "t", "ft2").await?;
	assert_eq!(fulltext_deltas(&ds, &IndexKeyBase::new(ns, db, table, ix.index_id)).await?, 0);
	assert!(compacting(&ds, &session, "ft").await?, "the first index's backlog stays queued");
	Ok(())
}

/// A statement event that records the outcome, error class and order of
/// every statement a datastore runs.
#[cfg(feature = "kv-mem")]
#[derive(Default)]
struct StatementEvents(
	std::sync::Mutex<
		Vec<(crate::observe::StatementType, crate::observe::Outcome, Option<&'static str>)>,
	>,
);

#[cfg(feature = "kv-mem")]
impl crate::observe::ExecutionObserver for StatementEvents {
	fn on_statement_complete(&self, event: &crate::observe::StatementEvent) {
		self.0.lock().expect("events lock").push((
			event.safe.kind,
			event.safe.outcome,
			event.safe.error_class,
		));
	}
}

/// In a block, an index statement's event is emitted once its phase-2 wait
/// settles, so it reports what its slot ends with: emitted after `COMMIT`'s,
/// with the error class of a wait that stopped; and with its phase-1 outcome
/// when the block ends before its commit.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_block_statement_event_matches_its_slot() -> Result<()> {
	use crate::observe::{Outcome, StatementType};
	for (inject, sql, expected) in [
		(
			false,
			"BEGIN; REBUILD INDEX ft ON t; COMMIT;",
			vec![
				(StatementType::Commit, Outcome::Success),
				(StatementType::Rebuild, Outcome::Success),
			],
		),
		(
			true,
			"BEGIN; REBUILD INDEX ft ON t; COMMIT;",
			vec![
				(StatementType::Commit, Outcome::Success),
				(StatementType::Rebuild, Outcome::Error),
			],
		),
		(
			false,
			"BEGIN; REBUILD INDEX ft ON t; CANCEL;",
			vec![
				(StatementType::Cancel, Outcome::Success),
				(StatementType::Rebuild, Outcome::Success),
			],
		),
	] {
		let events = Arc::new(StatementEvents::default());
		let observer: Arc<dyn crate::observe::ExecutionObserver> =
			Arc::<StatementEvents>::clone(&events);
		let ds = Datastore::builder()
			.without_maintenance_tasks()
			.with_observer(observer)
			.build_with_path("memory")
			.await?;
		let session = Session::owner().with_ns("test").with_db("test");
		let tx = ds.transaction(TransactionType::Write).await?;
		tx.ensure_ns_db(None, "test", "test").await?;
		tx.commit().await?;
		table_with_a_fulltext_index(&ds, &session).await?;
		events.0.lock().expect("events lock").clear();
		let _guard = inject.then(|| {
			inject_non_retryable_error(NonRetryableErrorSite::FullTextCompaction, ds.id())
		});
		ds.execute(sql, &session, None).await?;
		let seen: Vec<_> = events
			.0
			.lock()
			.unwrap()
			.iter()
			.filter(|(kind, ..)| {
				matches!(
					kind,
					StatementType::Commit | StatementType::Cancel | StatementType::Rebuild
				)
			})
			.map(|(kind, outcome, _)| (*kind, *outcome))
			.collect();
		assert_eq!(seen, expected, "{sql}, injected: {inject}");
		if inject {
			let class = events
				.0
				.lock()
				.unwrap()
				.iter()
				.find(|(kind, ..)| *kind == StatementType::Rebuild)
				.and_then(|(.., class)| *class);
			assert!(class.is_some(), "a stopped wait carries an error class");
		}
	}
	Ok(())
}

/// Two full-text indexes `ft` and `ft2` on table `t`, rebuilt by `sql` with
/// `ft`'s pass held, so that phase 2 is under way when the test acts: both
/// rebuilds are online and the first wait is blocked on its lock. Returns the
/// running request, the held lock and the two key bases.
#[cfg(feature = "kv-mem")]
async fn two_rebuilds_with_the_first_pass_held(
	ds: &Arc<Datastore>,
	session: &Session,
	cancel: crate::ctx::CancelHandle,
) -> Result<(
	tokio::task::JoinHandle<Result<Vec<Option<String>>>>,
	tokio::sync::OwnedMutexGuard<()>,
	IndexKeyBase,
	IndexKeyBase,
)> {
	let first = table_with_a_fulltext_index(ds, session).await?;
	execute_all(ds, session, "DEFINE INDEX ft2 ON t FIELDS text FULLTEXT ANALYZER simple BM25;")
		.await?;
	let (ns, db, table, ix) = get_table_index(ds, "t", "ft2").await?;
	let second = IndexKeyBase::new(ns, db, table, ix.index_id);
	let built = (
		durable_build_state(ds, &first).await?.generation,
		durable_build_state(ds, &second).await?.generation,
	);
	let held = materialise_lock(ds, &first)?.lock_owned().await;
	let run = tokio::spawn({
		let (ds, session) = (Arc::clone(ds), session.clone());
		async move {
			run_cancellable(
				&ds,
				&session,
				"BEGIN; REBUILD INDEX ft ON t; REBUILD INDEX ft2 ON t; COMMIT;",
				cancel,
			)
			.await
		}
	});
	wait_for_a_later_build_online(ds, &first, built.0).await?;
	wait_for_a_later_build_online(ds, &second, built.1).await?;
	Ok((run, held, first, second))
}

/// A cancellation during a block's phase 2 is seen by every wait still to
/// settle: each index statement reports `IndexMaterialiseWaitStopped`, and
/// `COMMIT` keeps its `NONE`.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_cancellation_stops_every_wait_in_a_block() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	let cancel = crate::ctx::CancelHandle::new();
	let (run, held, ..) =
		two_rebuilds_with_the_first_pass_held(&ds, &session, cancel.clone()).await?;
	assert!(still_running(&run, Duration::from_millis(300)).await, "phase 2 is under way");
	cancel.trip();
	let errors = timeout(Duration::from_secs(10), run).await???;
	drop(held);
	let stopped = |error: &Option<String>| matches!(error, Some(err) if err.starts_with("The index was committed and is ready"));
	assert_eq!(errors.len(), 4, "{errors:?}");
	assert!(stopped(&errors[1]) && stopped(&errors[2]), "{errors:?}");
	assert_eq!((&errors[0], &errors[3]), (&None, &None), "{errors:?}");
	Ok(())
}

/// A block's waits share one deadline, what is left of the transaction
/// timeout: one that reaches it ends with success, so does every wait after
/// it, and both indexes stay online with their backlog queued for background
/// compaction.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_blocks_waits_share_one_deadline() -> Result<()> {
	let (ds, session) =
		new_index_test_ds_with_timeouts(None, Some(Duration::from_millis(800))).await?;
	let (run, held, first, second) =
		two_rebuilds_with_the_first_pass_held(&ds, &session, crate::ctx::CancelHandle::new())
			.await?;
	let errors = timeout(Duration::from_secs(10), run).await???;
	assert_eq!(errors, vec![None, None, None, None], "no slot reports a timeout");
	assert!(compacting(&ds, &session, "ft").await?);
	assert!(compacting(&ds, &session, "ft2").await?, "the second wait returned at the deadline");
	assert_eq!(index_building_status(&ds, &session, "t", "ft").await?, "ready");
	assert_eq!(index_building_status(&ds, &session, "t", "ft2").await?, "ready");
	drop(held);
	Datastore::index_compaction(Arc::clone(&ds), Duration::from_secs(60), CancellationToken::new())
		.await?;
	assert_eq!(fulltext_deltas(&ds, &first).await?, 0);
	assert_eq!(fulltext_deltas(&ds, &second).await?, 0);
	Ok(())
}

/// A vector build folds nothing: once a blocking `REBUILD INDEX`'s build task
/// has finished, with its statement held in phase 2, the index holds a pending
/// for every row the scan read. The statement's own pass folds them once it
/// can run.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_vector_build_folds_nothing_and_leaves_its_pendings_to_phase_two() -> Result<()> {
	let mut kinds = vec!["HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32"];
	#[cfg(diskann)]
	kinds.push("DISKANN DIMENSION 2 DIST EUCLIDEAN TYPE F32");
	for kind in kinds {
		let (ds, session) = new_index_test_ds().await?;
		// The pass under test folds what a build's scan queues, which the DiskANN sink would not.
		let _sink_ds = super::kind::test_sink::off(ds.id());
		execute_all(
			&ds,
			&session,
			&format!(
				"CREATE t:1 SET vec = [1.0, 0.0];
				 CREATE t:2 SET vec = [0.0, 1.0];
				 CREATE t:3 SET vec = [1.0, 1.0];
				 DEFINE INDEX ix ON t FIELDS vec {kind};"
			),
		)
		.await?;
		let (ns, db, table, ix) = get_table_index(&ds, "t", "ix").await?;
		let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
		assert!(!compacting(&ds, &session, "ix").await?, "{kind}: the define materialised it");
		let built = durable_build_state(&ds, &ikb).await?.generation;
		let lock = materialise_lock(&ds, &ikb)?;
		let held = lock.lock().await;
		let rebuild = tokio::spawn({
			let (ds, session) = (Arc::clone(&ds), session.clone());
			async move { execute_all(&ds, &session, "REBUILD INDEX ix ON t").await }
		});
		wait_for_a_later_build_online(&ds, &ikb, built).await?;
		let building = local_builder_for_key(&ds, ns, db, &table, ix.index_id)
			.await?
			.expect("the rebuild registers a local builder");
		wait_for_finished_builder(&building).await?;
		assert!(!rebuild.is_finished(), "{kind}: the statement is held in phase 2");
		if kind.starts_with("HNSW") {
			assert_eq!(count_hnsw_pendings(&ds, &ikb).await?, 3, "a pending for every row");
		}
		assert!(compacting(&ds, &session, "ix").await?, "{kind}: the build applied no plan");
		drop(held);
		timeout(Duration::from_secs(10), rebuild).await???;
		assert!(!compacting(&ds, &session, "ix").await?, "{kind}: phase 2 folded them");
		if kind.starts_with("HNSW") {
			assert_eq!(count_hnsw_pendings(&ds, &ikb).await?, 0);
		}
	}
	Ok(())
}

/// A build whose statement has already closed releases its ownership in the
/// transaction that publishes it `Online`, beside the compaction request a
/// release owes, so no separate release transaction runs: the release
/// transaction's own failure, armed here, is never met.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_build_whose_statement_has_closed_releases_with_its_publish() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(&ds, &session, "DEFINE INDEX cx ON t COUNT; CREATE t:1; CREATE t:2;").await?;
	let (ns, db, table, ix) = get_table_index(&ds, "t", "cx").await?;
	let ikb = IndexKeyBase::new(ns, db, table, ix.index_id);
	let site = NonRetryableErrorSite::ConcurrentIndexOwnershipReleaseCommit;
	let _guard = inject_non_retryable_error(site, ds.id());
	// Standalone, so the build is awaited after the statement's commit, and
	// the statement half of the release gate has arrived before it publishes.
	execute_all(&ds, &session, "REBUILD INDEX cx ON t").await?;
	assert_eq!(
		crate::kvs::testing::non_retryable_error_count(site, ds.id()),
		1,
		"no release transaction ran"
	);
	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(state.phase, IndexBuildPhase::Online, "{state:?}");
	assert_eq!(state.owner, None, "the publish released the ownership: {state:?}");
	assert!(!compacting(&ds, &session, "cx").await?);
	Ok(())
}

/// A build whose statement is still open publishes `Online` with its owner
/// set, and the statement's commit releases it.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_build_whose_statement_is_open_keeps_its_owner_past_the_publish() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(&ds, &session, "DEFINE INDEX cx ON t COUNT; CREATE t:1; CREATE t:2;").await?;
	let (ns, db, table, ix) = get_table_index(&ds, "t", "cx").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	let txn =
		build_index_inside_an_open_statement_transaction(&ds, &session, ns, db, &table, ix, true)
			.await?;
	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(state.phase, IndexBuildPhase::Online, "{state:?}");
	assert!(state.owner.is_some(), "the open statement holds the ownership: {state:?}");
	txn.commit().await?;
	assert_eq!(durable_build_state(&ds, &ikb).await?.owner, None, "the commit releases it");
	Ok(())
}

/// An HNSW index `hx` on `t` with `rows` records written after its build, so
/// their pendings are queued with no compaction run, and its key base.
#[cfg(feature = "kv-mem")]
async fn hnsw_index_with_pendings(ds: &Datastore, session: &Session) -> Result<IndexKeyBase> {
	execute_all(
		ds,
		session,
		"DEFINE INDEX hx ON t FIELDS vec HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32;
		 CREATE t:1 SET vec = [1.0, 0.0];
		 CREATE t:2 SET vec = [0.0, 1.0];
		 CREATE t:3 SET vec = [1.0, 1.0];",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(ds, "t", "hx").await?;
	let ikb = IndexKeyBase::new(ns, db, table, ix.index_id);
	assert_eq!(count_hnsw_pendings(ds, &ikb).await?, 3, "the writes leave their pendings");
	Ok(ikb)
}

/// Rewrite `ikb`'s published build state so a builder that no longer runs
/// owns it, with a heartbeat a lease and more ago: the state a node that died,
/// or a release that failed, leaves behind.
#[cfg(feature = "kv-mem")]
async fn strand_the_online_owner(ds: &Datastore, ikb: &IndexKeyBase) -> Result<Uuid> {
	let mut state = durable_build_state(ds, ikb).await?;
	assert_eq!(state.phase, IndexBuildPhase::Online);
	let owner = Uuid::now_v7();
	let lapsed = Utc::now() - chrono::Duration::seconds(BUILD_OWNER_LEASE_SECS + 5);
	state.owner = Some(owner);
	state.owner_heartbeat_at = Some(lapsed);
	state.updated_at = lapsed;
	set_durable_build_state(ds, ikb, state).await?;
	Ok(owner)
}

/// Let `ikb`'s `Online` owner's heartbeat lapse, keeping the owner.
#[cfg(feature = "kv-mem")]
async fn lapse_the_online_owner(ds: &Datastore, ikb: &IndexKeyBase) -> Result<()> {
	let mut state = durable_build_state(ds, ikb).await?;
	assert_eq!(state.phase, IndexBuildPhase::Online);
	assert!(state.owner.is_some(), "the build still owns the index: {state:?}");
	let lapsed = Utc::now() - chrono::Duration::seconds(BUILD_OWNER_LEASE_SECS + 5);
	state.owner_heartbeat_at = Some(lapsed);
	state.updated_at = lapsed;
	set_durable_build_state(ds, ikb, state).await
}

/// Hold the store at the `Legacy` feature level, as one upgraded from a
/// release before feature levels and not yet raised is.
#[cfg(feature = "kv-mem")]
async fn hold_the_store_at_legacy(ds: &Datastore) -> Result<()> {
	let tx = ds.transaction(TransactionType::Write).await?;
	catch!(tx, tx.del_key(&crate::key::schema::FeatureLevelKey {}).await);
	tx.commit().await
}

/// A stranded `Online` owner no longer keeps its index from being compacted:
/// the pass folds every pending, and its write guard clears the owner.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_stranded_online_owner_is_compacted_and_cleared() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	let ikb = hnsw_index_with_pendings(&ds, &session).await?;
	strand_the_online_owner(&ds, &ikb).await?;

	Datastore::index_compaction(Arc::clone(&ds), Duration::from_secs(1), CancellationToken::new())
		.await?;

	assert_eq!(count_hnsw_pendings(&ds, &ikb).await?, 0, "every pending is folded");
	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(state.owner, None, "the guard cleared the lapsed owner: {state:?}");
	assert_eq!(state.phase, IndexBuildPhase::Online);
	expect_indexed_query_len(&ds, &session, "SELECT id FROM t WHERE vec <|3,40|> [1.0f, 0.0f]", 3)
		.await
}

/// The resume scan clears a stranded `Online` owner, queuing exactly the one
/// compaction request a release owes.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn the_resume_scan_clears_a_stranded_online_owner() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	let ikb = hnsw_index_with_pendings(&ds, &session).await?;
	strand_the_online_owner(&ds, &ikb).await?;
	let requests = compaction_requests(&ds, &ikb).await?;

	let resumed =
		ds.resume_stalled_index_builds(Duration::from_secs(30), CancellationToken::new()).await?;

	assert_eq!(resumed.items, 0, "an Online generation is cleared, not resumed");
	let state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(state.owner, None, "the scan cleared the lapsed owner: {state:?}");
	assert_eq!(compaction_requests(&ds, &ikb).await?, requests + 1);
	Ok(())
}

/// On a store whose feature level is not raised — a cluster that may still
/// run a builder that folds after `Online` without a heartbeat — a lapsed
/// `Online` owner whose state records no basis, as an earlier release's
/// builder leaves it, keeps fencing: neither the compactor nor the resume scan
/// clears it.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_lapsed_online_owner_keeps_fencing_on_a_legacy_store() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	let ikb = hnsw_index_with_pendings(&ds, &session).await?;
	let owner = strand_the_online_owner(&ds, &ikb).await?;
	let mut unrecorded = durable_build_state(&ds, &ikb).await?;
	unrecorded.basis = None;
	set_durable_build_state(&ds, &ikb, unrecorded).await?;
	hold_the_store_at_legacy(&ds).await?;

	Datastore::index_compaction(Arc::clone(&ds), Duration::from_secs(1), CancellationToken::new())
		.await?;
	ds.resume_stalled_index_builds(Duration::from_secs(30), CancellationToken::new()).await?;

	assert_eq!(count_hnsw_pendings(&ds, &ikb).await?, 3, "the fence still holds");
	assert_eq!(durable_build_state(&ds, &ikb).await?.owner, Some(owner));
	Ok(())
}

/// A statement transaction dropped without being committed or cancelled runs
/// neither close action, so its build's keeper releases the ownership in its
/// place, with the compaction request a release owes.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_dropped_statement_transaction_releases_its_build() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(&ds, &session, "DEFINE INDEX cx ON t COUNT; CREATE t:1; CREATE t:2;").await?;
	let (ns, db, table, ix) = get_table_index(&ds, "t", "cx").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	let txn =
		build_index_inside_an_open_statement_transaction(&ds, &session, ns, db, &table, ix, true)
			.await?;
	assert!(durable_build_state(&ds, &ikb).await?.owner.is_some());
	let requests = compaction_requests(&ds, &ikb).await?;

	drop(txn);

	timeout(Duration::from_secs(10), async {
		while durable_build_state(&ds, &ikb).await?.owner.is_some() {
			sleep(Duration::from_millis(20)).await;
		}
		anyhow::Ok(())
	})
	.await
	.map_err(|_| anyhow::anyhow!("the keeper did not release the dropped statement's build"))??;
	assert_eq!(compaction_requests(&ds, &ikb).await?, requests + 1);
	Ok(())
}

/// A release whose commit fails leaves the owner set; once its heartbeat has
/// lapsed, compaction proceeds and clears it.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_release_does_not_strand_the_owner() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	let ikb = hnsw_index_with_pendings(&ds, &session).await?;
	let (ns, db, table, ix) = get_table_index(&ds, "t", "hx").await?;
	let txn =
		build_index_inside_an_open_statement_transaction(&ds, &session, ns, db, &table, ix, true)
			.await?;
	let site = NonRetryableErrorSite::ConcurrentIndexOwnershipReleaseCommit;
	let guard = inject_non_retryable_error(site, ds.id());
	txn.commit().await?;
	drop(guard);
	assert!(
		durable_build_state(&ds, &ikb).await?.owner.is_some(),
		"the failed release left the owner set"
	);
	assert!(count_hnsw_pendings(&ds, &ikb).await? > 0);

	lapse_the_online_owner(&ds, &ikb).await?;
	Datastore::index_compaction(Arc::clone(&ds), Duration::from_secs(1), CancellationToken::new())
		.await?;
	assert_eq!(count_hnsw_pendings(&ds, &ikb).await?, 0);
	assert_eq!(durable_build_state(&ds, &ikb).await?.owner, None);
	Ok(())
}

/// The statement a lapsed `Online` owner's build waits on cannot commit once
/// a compaction pass has cleared that owner: of the two, whichever commits
/// second fails. Shown for a statement that restamps the table's definition,
/// as `DEFINE INDEX` and `REBUILD INDEX` do, in both orders.
#[cfg(feature = "kv-mem")]
async fn assert_clearing_a_lapsed_owner_fences_its_statement(ds: Arc<Datastore>) -> Result<()> {
	let session = Session::owner().with_ns("test").with_db("test");
	for statement_first in [false, true] {
		let ikb = hnsw_index_with_pendings(&ds, &session).await?;
		let (ns, db, table, ix) = get_table_index(&ds, "t", "hx").await?;
		let txn = Arc::new(ds.transaction(TransactionType::Write).await?);
		let table_def =
			catch!(txn, txn.get_tb(ns, db, &table, None).await).expect("table should exist");
		let opt = ds.setup_options(&session);
		let (ns_name, db_name) = catch!(txn, opt.ns_db());
		catch!(
			txn,
			txn.replace_tb(
				ns_name,
				db_name,
				&crate::catalog::TableDefinition {
					cache_indexes_ts: Uuid::now_v7(),
					..(*table_def).clone()
				},
			)
			.await
		);
		let mut ctx = ds.setup_ctx()?;
		ctx.set_transaction(Arc::clone(&txn));
		let ctx = ctx.freeze();
		let builder = ctx.get_index_builder().expect("index builder should exist").clone();
		let completion = catch!(
			txn,
			builder.build(&ctx, ds.setup_options(&session), table_def.table_id, ix, true).await
		)
		.expect("a blocking build reports its result");
		completion.await?;
		lapse_the_online_owner(&ds, &ikb).await?;

		if statement_first {
			txn.commit().await?;
			assert_eq!(durable_build_state(&ds, &ikb).await?.owner, None, "the commit released");
			Datastore::index_compaction(
				Arc::clone(&ds),
				Duration::from_secs(1),
				CancellationToken::new(),
			)
			.await?;
			assert_eq!(count_hnsw_pendings(&ds, &ikb).await?, 0);
		} else {
			Datastore::index_compaction(
				Arc::clone(&ds),
				Duration::from_secs(1),
				CancellationToken::new(),
			)
			.await?;
			assert_eq!(durable_build_state(&ds, &ikb).await?.owner, None, "the pass cleared it");
			let err =
				txn.commit().await.expect_err("the cleared owner's statement must not commit");
			assert!(is_retryable_transaction_conflict(&err), "expected a conflict, got: {err}");
		}
		execute_all(&ds, &session, "REMOVE TABLE t").await?;
	}
	Ok(())
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn clearing_a_lapsed_owner_fences_its_statement() -> Result<()> {
	let (ds, _) = new_index_test_ds().await?;
	assert_clearing_a_lapsed_owner_fences_its_statement(ds).await
}

/// SurrealKV counterpart of [`clearing_a_lapsed_owner_fences_its_statement`],
/// on a backend that validates a transaction's locked reads at commit.
#[cfg(feature = "kv-surrealkv")]
#[tokio::test(flavor = "multi_thread")]
async fn clearing_a_lapsed_owner_fences_its_statement_on_surrealkv() -> Result<()> {
	let dir = TempDir::new()?;
	let ds = Datastore::builder()
		.without_maintenance_tasks()
		.build_with_path(&format!("surrealkv://{}", dir.path().to_string_lossy()))
		.await?;
	let tx = ds.transaction(TransactionType::Write).await?;
	tx.ensure_ns_db(None, "test", "test").await?;
	tx.commit().await?;
	assert_clearing_a_lapsed_owner_fences_its_statement(ds).await
}

/// The keeper's heartbeats, committed beside an open statement, do not
/// conflict that statement: held open across one, it still commits.
#[cfg(feature = "kv-surrealkv")]
#[tokio::test(flavor = "multi_thread")]
async fn a_keepers_heartbeat_leaves_its_statement_able_to_commit() -> Result<()> {
	let dir = TempDir::new()?;
	let ds = Datastore::builder()
		.without_maintenance_tasks()
		.build_with_path(&format!("surrealkv://{}", dir.path().to_string_lossy()))
		.await?;
	let session = Session::owner().with_ns("test").with_db("test");
	let tx = ds.transaction(TransactionType::Write).await?;
	tx.ensure_ns_db(None, "test", "test").await?;
	tx.commit().await?;
	execute_all(&ds, &session, "DEFINE INDEX cx ON t COUNT; CREATE t:1;").await?;
	let (ns, db, table, ix) = get_table_index(&ds, "t", "cx").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	let _cadence = super::builder::statement_heartbeat::set(ds.id(), Duration::from_millis(100));
	let txn =
		build_index_inside_an_open_statement_transaction(&ds, &session, ns, db, &table, ix, true)
			.await?;
	let published = durable_build_state(&ds, &ikb).await?.owner_heartbeat_at;
	timeout(Duration::from_secs(10), async {
		while durable_build_state(&ds, &ikb).await?.owner_heartbeat_at == published {
			sleep(Duration::from_millis(200)).await;
		}
		anyhow::Ok(())
	})
	.await
	.map_err(|_| anyhow::anyhow!("the keeper did not heartbeat"))??;
	txn.commit().await?;
	assert_eq!(durable_build_state(&ds, &ikb).await?.owner, None, "the commit released");
	Ok(())
}

/// A keeper whose heartbeat finds its ownership cleared cancels the statement
/// its build waits on, so that statement ends rather than committing a
/// definition whose build no longer owns the index.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_keeper_that_finds_its_ownership_cleared_cancels_its_statement() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(&ds, &session, "DEFINE INDEX cx ON t COUNT; CREATE t:1; CREATE t:2;").await?;
	let (ns, db, table, ix) = get_table_index(&ds, "t", "cx").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	let _cadence = super::builder::statement_heartbeat::set(ds.id(), Duration::from_millis(100));
	let txn =
		build_index_inside_an_open_statement_transaction(&ds, &session, ns, db, &table, ix, true)
			.await?;
	lapse_the_online_owner(&ds, &ikb).await?;
	ds.resume_stalled_index_builds(Duration::from_secs(30), CancellationToken::new()).await?;
	assert_eq!(durable_build_state(&ds, &ikb).await?.owner, None, "the scan cleared it");

	// The keeper finds the ownership gone at its next heartbeat.
	timeout(Duration::from_secs(10), async {
		while !txn.closed() {
			sleep(Duration::from_millis(100)).await;
		}
	})
	.await
	.map_err(|_| anyhow::anyhow!("the keeper did not cancel its statement"))?;
	assert!(txn.commit().await.is_err(), "a cancelled statement cannot commit");
	Ok(())
}

/// An aborted build's keeper exits without writing: whatever aborted it
/// settles the ownership, so the keeper releases nothing and queues nothing.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn an_aborted_keeper_writes_nothing() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(&ds, &session, "DEFINE INDEX cx ON t COUNT; CREATE t:1; CREATE t:2;").await?;
	let (ns, db, table, ix) = get_table_index(&ds, "t", "cx").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	let index_id = ix.index_id;
	let txn =
		build_index_inside_an_open_statement_transaction(&ds, &session, ns, db, &table, ix, true)
			.await?;
	let owner = durable_build_state(&ds, &ikb).await?.owner;
	assert!(owner.is_some());
	let requests = compaction_requests(&ds, &ikb).await?;

	let building = local_builder_for_key(&ds, ns, db, &table, index_id)
		.await?
		.expect("the build registers a local builder");
	building.abort();
	// The keeper polls at the build's closing cadence; give it many polls.
	sleep(Duration::from_millis(500)).await;

	assert_eq!(durable_build_state(&ds, &ikb).await?.owner, owner, "the keeper released nothing");
	assert_eq!(compaction_requests(&ds, &ikb).await?, requests, "the keeper queued nothing");
	txn.cancel().await?;
	Ok(())
}

/// One block that defines an index and then rebuilds it, held open across
/// keeper heartbeats, commits: the first build's keeper stops once the second
/// build replaces its generation, rather than cancelling the block.
#[cfg(feature = "kv-mem")]
async fn assert_a_block_that_rebuilds_its_own_index_commits(ds: &Datastore) -> Result<()> {
	let session = Session::owner().with_ns("test").with_db("test");
	let _cadence = super::builder::statement_heartbeat::set(ds.id(), Duration::from_millis(50));
	execute_all(ds, &session, "CREATE t:1; CREATE t:2;").await?;
	let results = ds
		.execute(
			"BEGIN; DEFINE INDEX cx ON t COUNT; REBUILD INDEX cx ON t; SLEEP 500ms; COMMIT;",
			&session,
			None,
		)
		.await?;
	let outcomes: Vec<_> =
		results.into_iter().map(|r| r.result.map_err(|e| e.to_string())).collect();
	assert!(outcomes.iter().all(Result::is_ok), "{outcomes:#?}");
	assert_eq!(index_building_status(ds, &session, "t", "cx").await?, "ready");
	Ok(())
}

/// A retirement staged in the transaction of the statement that started a
/// build stops that build's keeper at once, so no heartbeat of `!bs` is
/// committed beside a transaction that deletes it. A retirement staged in any
/// other transaction leaves the keeper to the retirement's commit.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_retirement_in_its_statement_stops_the_builds_keeper() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(&ds, &session, "DEFINE INDEX cx ON t COUNT; CREATE t:1;").await?;
	let (ns, db, table, ix) = get_table_index(&ds, "t", "cx").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	let index_id = ix.index_id;
	let _cadence = super::builder::statement_heartbeat::set(ds.id(), Duration::from_millis(50));
	let txn =
		build_index_inside_an_open_statement_transaction(&ds, &session, ns, db, &table, ix, true)
			.await?;
	let builder = ds.setup_ctx()?.freeze().get_index_builder().expect("index builder").clone();
	let building = local_builder_for_key(&ds, ns, db, &table, index_id)
		.await?
		.expect("the build registers a local builder");

	let other = Arc::new(ds.transaction(TransactionType::Write).await?);
	builder.stop_build_of_statement(ns, db, &table, index_id, &other).await;
	other.cancel().await?;
	assert!(!building.is_aborted().await, "another transaction's retirement waits for its commit");

	builder.stop_build_of_statement(ns, db, &table, index_id, &txn).await;
	assert!(building.is_aborted().await, "the statement's own retirement stops its build");
	sleep(Duration::from_millis(200)).await;
	let stopped = durable_build_state(&ds, &ikb).await?.owner_heartbeat_at;
	sleep(Duration::from_millis(500)).await;
	assert_eq!(
		durable_build_state(&ds, &ikb).await?.owner_heartbeat_at,
		stopped,
		"the stopped keeper heartbeats no more"
	);
	txn.cancel().await?;
	Ok(())
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_block_that_rebuilds_its_own_index_commits() -> Result<()> {
	let (ds, _) = new_index_test_ds().await?;
	assert_a_block_that_rebuilds_its_own_index_commits(&ds).await
}

/// SurrealKV counterpart of [`a_block_that_rebuilds_its_own_index_commits`].
#[cfg(feature = "kv-surrealkv")]
#[tokio::test(flavor = "multi_thread")]
async fn a_block_that_rebuilds_its_own_index_commits_on_surrealkv() -> Result<()> {
	let dir = TempDir::new()?;
	let ds = Datastore::builder()
		.without_maintenance_tasks()
		.build_with_path(&format!("surrealkv://{}", dir.path().to_string_lossy()))
		.await?;
	let tx = ds.transaction(TransactionType::Write).await?;
	tx.ensure_ns_db(None, "test", "test").await?;
	tx.commit().await?;
	assert_a_block_that_rebuilds_its_own_index_commits(&ds).await
}

/// A statement's materialise wait keeps its place in the index's lock queue
/// across its done checks.
///
/// A background pass waits for the same lock. One that starts waiting while
/// the statement already waits must not get the lock first, or a stream of
/// background passes keeps the statement waiting until its deadline.
#[tokio::test(start_paused = true)]
async fn a_materialise_wait_keeps_its_place_in_the_lock_queue() {
	let lock = Arc::new(tokio::sync::Mutex::new(()));
	let order = Arc::new(std::sync::Mutex::new(Vec::new()));
	let held = Arc::clone(&lock).lock_owned().await;
	let wait = {
		let (lock, order) = (Arc::clone(&lock), Arc::clone(&order));
		tokio::spawn(async move {
			let held = materialise::lock_unless(&lock, || Duration::from_millis(10), || None::<()>)
				.await
				.expect("the wait never ends");
			order.lock().expect("order").push("wait");
			drop(held);
		})
	};
	// The wait queues, and checks once while the lock stays held.
	sleep(Duration::from_millis(15)).await;
	let pass = {
		let (lock, order) = (Arc::clone(&lock), Arc::clone(&order));
		tokio::spawn(async move {
			let held = lock.lock().await;
			order.lock().expect("order").push("pass");
			drop(held);
		})
	};
	// The wait checks again, with the pass queued behind it.
	sleep(Duration::from_millis(15)).await;
	drop(held);
	wait.await.expect("wait task");
	pass.await.expect("pass task");
	assert_eq!(*order.lock().expect("order"), ["wait", "pass"]);
}

/// A doc-ID index build heartbeats the ownership it holds past `Online` while
/// its deferred doc-ID reclaim sweep runs.
///
/// The sweep has no bound on how long it runs, and the ownership is what keeps
/// compaction from resolving doc IDs through the mappings it reclaims. A lease
/// left to lapse under a long sweep is cleared as stranded, and that fence
/// goes with it.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_doc_id_build_heartbeats_its_ownership_through_the_reclaim_sweep() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE ANALYZER simple TOKENIZERS blank; CREATE user:1 SET bio = 'alpha' RETURN NONE;",
	)
	.await?;
	let _cadence = super::builder::statement_heartbeat::set(ds.id(), Duration::from_millis(50));
	let park = super::doc_ids::reclaim_sweep_park::park(ds.id());
	execute_all(
		&ds,
		&session,
		"DEFINE INDEX test ON user FIELDS bio FULLTEXT ANALYZER simple CONCURRENTLY",
	)
	.await?;
	timeout(Duration::from_secs(30), park.parked()).await?;
	let (ns, db, table, ix) = get_table_index(&ds, "user", "test").await?;
	let ikb = IndexKeyBase::new(ns, db, table, ix.index_id);
	let mut state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(state.phase, IndexBuildPhase::Online, "the sweep runs after the publish");
	assert!(state.owner.is_some(), "the build holds its ownership through the sweep");
	// Age the lease past its end, as a sweep longer than the lease would.
	let expired = Utc::now() - chrono::Duration::seconds(BUILD_OWNER_LEASE_SECS + 5);
	state.owner_heartbeat_at = Some(expired);
	set_durable_build_state(&ds, &ikb, state).await?;
	let deadline = Instant::now() + Duration::from_secs(10);
	loop {
		let state = durable_build_state(&ds, &ikb).await?;
		if state.owner_heartbeat_at.is_some_and(|at| at > expired) {
			break;
		}
		anyhow::ensure!(Instant::now() < deadline, "no heartbeat while the sweep ran");
		sleep(Duration::from_millis(20)).await;
	}
	// The sweep ends, and the build releases its ownership.
	drop(park);
	let deadline = Instant::now() + Duration::from_secs(30);
	while durable_build_state(&ds, &ikb).await?.owner.is_some() {
		anyhow::ensure!(Instant::now() < deadline, "the build kept its ownership after the sweep");
		sleep(Duration::from_millis(20)).await;
	}
	Ok(())
}

/// The keys stored under the index `ikb` names: its index data.
#[cfg(feature = "kv-mem")]
async fn index_data_keys(ds: &Datastore, ikb: &IndexKeyBase) -> Result<usize> {
	let range = crate::key::schema::IdxRoot {
		ns: ikb.ns(),
		db: ikb.db(),
		tb: Cow::Borrowed(ikb.table()),
		ix: ikb.index(),
	}
	.range()?;
	let tx = ds.transaction(TransactionType::Read).await?;
	let keys = catch!(tx, tx.keys_raw(range, u32::MAX, 0, None).await);
	tx.cancel().await?;
	Ok(keys.len())
}

/// Leaves build state, index data and one compaction request for an index of
/// `t` whose definition was never committed, owned by a build in `phase`
/// whose lease lapsed when `lapsed`, and answers the index's key base.
#[cfg(feature = "kv-mem")]
async fn orphaned_build(
	ds: &Datastore,
	session: &Session,
	phase: IndexBuildPhase,
	lapsed: bool,
) -> Result<IndexKeyBase> {
	execute_all(ds, session, "DEFINE INDEX cx ON t COUNT; CREATE t:1;").await?;
	let (ns, db, table, cx) = get_table_index(ds, "t", "cx").await?;
	let ikb = IndexKeyBase::new(ns, db, table, IndexId(cx.index_id.0 + 1));
	let mut state = durable_build_state_for_phase(phase, 1, Some(Uuid::now_v7()));
	if lapsed {
		let at = Utc::now() - chrono::Duration::seconds(BUILD_OWNER_LEASE_SECS + 5);
		state.owner_heartbeat_at = Some(at);
		state.updated_at = at;
	}
	set_durable_build_state(ds, &ikb, state).await?;
	let range = crate::key::schema::IdxRoot {
		ns: ikb.ns(),
		db: ikb.db(),
		tb: Cow::Borrowed(ikb.table()),
		ix: ikb.index(),
	}
	.range()?;
	let mut data = range.bounds().0.to_vec();
	data.extend_from_slice(b"orphaned");
	let tx = ds.transaction(TransactionType::Write).await?;
	catch!(tx, tx.set(Key::from(data), b"data".to_vec()).await);
	catch!(tx, tx.set_key(&ikb.new_ic_key(ds.id()), &()).await);
	tx.commit().await?;
	Ok(ikb)
}

/// Whether the build `ikb` names still has its state, data and request.
#[cfg(feature = "kv-mem")]
async fn orphan_left(ds: &Datastore, ikb: &IndexKeyBase) -> Result<(bool, usize, usize)> {
	Ok((
		durable_build_state_exists(ds, ikb).await?,
		index_data_keys(ds, ikb).await?,
		compaction_requests(ds, ikb).await?,
	))
}

/// A build of a definition that never committed, whose owner's lease lapsed,
/// is retired by the next compaction pass over it: its build state, queues and
/// index data are deleted, and its request is dropped. Nothing else reaches
/// it: the resume scan visits only the indexes the catalog defines.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_lapsed_build_of_an_uncommitted_definition_is_retired() -> Result<()> {
	for phase in [IndexBuildPhase::Building, IndexBuildPhase::Closing, IndexBuildPhase::Online] {
		let (ds, session) = new_index_test_ds().await?;
		let ikb = orphaned_build(&ds, &session, phase, true).await?;
		assert_eq!(orphan_left(&ds, &ikb).await?, (true, 1, 1), "{phase:?}: the orphan");
		ds.resume_stalled_index_builds(Duration::from_secs(30), CancellationToken::new()).await?;
		assert_eq!(orphan_left(&ds, &ikb).await?, (true, 1, 1), "{phase:?}: not the scan's");
		Datastore::index_compaction(
			Arc::clone(&ds),
			Duration::from_secs(1),
			CancellationToken::new(),
		)
		.await?;
		assert_eq!(orphan_left(&ds, &ikb).await?, (false, 0, 0), "{phase:?}: retired");
	}
	Ok(())
}

/// A build of a definition that has not committed keeps its state, data and
/// request while its owner's lease holds, and on a store whose feature level
/// does not allow a lapsed owner to be cleared.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn an_uncommitted_definitions_build_is_kept_while_its_lease_holds_or_on_legacy() -> Result<()>
{
	let (ds, session) = new_index_test_ds().await?;
	let ikb = orphaned_build(&ds, &session, IndexBuildPhase::Building, false).await?;
	Datastore::index_compaction(Arc::clone(&ds), Duration::from_secs(1), CancellationToken::new())
		.await?;
	assert_eq!(orphan_left(&ds, &ikb).await?, (true, 1, 1), "a live lease keeps it");

	let (ds, session) = new_index_test_ds().await?;
	let ikb = orphaned_build(&ds, &session, IndexBuildPhase::Online, true).await?;
	hold_the_store_at_legacy(&ds).await?;
	Datastore::index_compaction(Arc::clone(&ds), Duration::from_secs(1), CancellationToken::new())
		.await?;
	assert_eq!(orphan_left(&ds, &ikb).await?, (true, 1, 1), "a legacy store keeps it");
	Ok(())
}

/// Retiring a lapsed build whose statement is still open fences that
/// statement: the definition it holds can no longer commit.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn retiring_an_uncommitted_build_fences_its_statement() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(&ds, &session, "DEFINE INDEX cx ON t COUNT; CREATE t:1; CREATE t:2;").await?;
	let (ns, db, table, cx) = get_table_index(&ds, "t", "cx").await?;
	let ix = Arc::new(IndexDefinition {
		index_id: IndexId(cx.index_id.0 + 1),
		name: "cy".into(),
		..(*cx).clone()
	});
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	let txn =
		build_index_inside_an_open_statement_transaction(&ds, &session, ns, db, &table, ix, true)
			.await?;
	lapse_the_online_owner(&ds, &ikb).await?;
	let tx = ds.transaction(TransactionType::Write).await?;
	catch!(tx, tx.set_key(&ikb.new_ic_key(ds.id()), &()).await);
	tx.commit().await?;
	Datastore::index_compaction(Arc::clone(&ds), Duration::from_secs(1), CancellationToken::new())
		.await?;
	assert_eq!(orphan_left(&ds, &ikb).await?, (false, 0, 0), "the build is retired");
	assert!(txn.commit().await.is_err(), "the statement that defined it cannot commit");
	assert!(get_table_index(&ds, "t", "cy").await.is_err(), "and its definition is absent");
	Ok(())
}

/// Starts the blocking `define` over a table `t` of `rows` records, scanned one record a batch,
/// and holds the build at a batch commit after at least one batch has committed. The records
/// scanned by then are indexed from the state before any later write; the rest are still to be
/// read. Dropping the returned guard lets the build go on.
#[cfg(feature = "kv-mem")]
async fn spawn_blocking_build_held_mid_scan(
	ds: &Arc<Datastore>,
	session: &Session,
	define: &'static str,
) -> Result<(
	RetryableConflictGuard,
	(super::kind::test_sink::ScanGuard, super::kind::test_sink::HoldGuard),
	tokio::task::JoinHandle<Result<()>>,
)> {
	let scan = super::kind::test_sink::set_scan_batch(ds.id(), 1);
	let committed_before = super::kind::test_sink::counts(ds.id()).committed;
	// Armed before the build starts, and deferred by the sink until its first batch has
	// committed, so no scan speed lets the build reach the end first.
	let hold = super::kind::test_sink::hold_after_commits(ds.id(), committed_before + 1);
	let site = RetryableConflictSite::ConcurrentIndexInitialBatch;
	let guard = inject_retryable_conflicts(site, ds.id(), REPEATED_RETRY_CONFLICTS);
	let task = tokio::spawn({
		let ds = Arc::clone(ds);
		let session = session.clone();
		async move { execute_all(&ds, &session, define).await }
	});
	wait_for_retry_conflict(site, ds.id(), REPEATED_RETRY_CONFLICTS).await?;
	assert!(!task.is_finished(), "the build must still be scanning");
	Ok((guard, (scan, hold), task))
}

/// A datastore about to shut down lapses the owner lease of each build it is running, while it
/// can still commit, and leaves the build as it was: its generation, owner, phase and checkpoint.
/// The build writes nothing more, stopping as a shutdown stops it, so the lease stays lapsed, and
/// a resume pass takes the build over at once rather than a lease later and finishes it.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn shutdown_lapses_the_lease_of_a_running_build() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE TABLE t SCHEMALESS; CREATE |t:1..=300| SET flag = false RETURN NONE;",
	)
	.await?;
	// A committed build, scanning one record a batch, held at a batch commit once its first
	// batch has committed.
	let scan = super::kind::test_sink::set_scan_batch(ds.id(), 1);
	let committed_before = super::kind::test_sink::counts(ds.id()).committed;
	let hold = super::kind::test_sink::hold_after_commits(ds.id(), committed_before + 1);
	let site = RetryableConflictSite::ConcurrentIndexInitialBatch;
	let guard = inject_retryable_conflicts(site, ds.id(), REPEATED_RETRY_CONFLICTS);
	execute_all(&ds, &session, "DEFINE INDEX ix ON t FIELDS flag CONCURRENTLY").await?;
	wait_for_retry_conflict(site, ds.id(), REPEATED_RETRY_CONFLICTS).await?;
	let building = ds
		.index_builder()
		.indexes
		.read()
		.await
		.values()
		.next()
		.cloned()
		.expect("the build runs on this datastore");
	let before = durable_build_state(&ds, &building.ikb).await?;
	assert!(
		!super::state::build_owner_expired(&before, Utc::now()),
		"a running build holds its lease"
	);
	assert_eq!(ds.index_builder().lapse_local_build_leases().await, 1);
	let after = durable_build_state(&ds, &building.ikb).await?;
	assert!(super::state::build_owner_expired(&after, Utc::now()), "the lease has lapsed");
	assert_eq!(
		(after.generation, after.owner, after.phase, after.initial_cursor.clone()),
		(before.generation, before.owner, before.phase, before.initial_cursor.clone()),
		"nothing but the lease changes"
	);
	drop(guard);
	wait_for_finished_builder(&building).await?;
	let left = durable_build_state(&ds, &building.ikb).await?;
	assert!(super::state::build_owner_expired(&left, Utc::now()), "the lease stays lapsed");
	assert_eq!(left.phase, IndexBuildPhase::Building);
	drop((scan, hold));
	// The in-memory store cannot be reopened; its builder forgets the shutdown as a restarted
	// datastore's fresh builder would.
	ds.index_builder().restart_after_shutdown();
	let resumed = ds
		.resume_stalled_index_builds(
			Duration::from_secs(30),
			tokio_util::sync::CancellationToken::new(),
		)
		.await?;
	assert_eq!(resumed.items, 1, "the scan takes the build over at once");
	let deadline = Instant::now() + Duration::from_secs(30);
	while durable_build_state(&ds, &building.ikb).await?.phase != IndexBuildPhase::Online {
		assert!(Instant::now() < deadline, "the resumed build did not finish");
		sleep(Duration::from_millis(50)).await;
	}
	Ok(())
}

/// A build that registers once the datastore has begun to shut down, having taken its generation
/// while the shutdown lapsed the builds it found, stops writing and has its lease lapsed as it
/// registers, so it is left for a resume pass as those builds are.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_build_registered_during_shutdown_has_its_lease_lapsed() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE TABLE t SCHEMALESS; CREATE |t:1..=300| SET flag = false RETURN NONE;",
	)
	.await?;
	assert_eq!(ds.index_builder().lapse_local_build_leases().await, 0, "no build runs yet");
	execute_all(&ds, &session, "DEFINE INDEX ix ON t FIELDS flag CONCURRENTLY").await?;
	let building = ds
		.index_builder()
		.indexes
		.read()
		.await
		.values()
		.next()
		.cloned()
		.expect("the build registers on this datastore");
	wait_for_finished_builder(&building).await?;
	let left = durable_build_state(&ds, &building.ikb).await?;
	assert_ne!(left.phase, IndexBuildPhase::Online, "the build stopped before publishing");
	assert!(super::state::build_owner_expired(&left, Utc::now()), "its lease is lapsed");
	Ok(())
}

/// An UPDATE that commits while a blocking `DEFINE INDEX` is mid-scan reaches the finished
/// index, whether the build had already scanned the record (it holds the old value, which must
/// go) or not (it reads the new one). The index answers the same as a scan.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn blocking_define_index_keeps_an_update_committed_mid_scan() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE TABLE t SCHEMALESS; CREATE |t:1..=300| SET flag = false RETURN NONE;",
	)
	.await?;
	let (guard, _scan, define) =
		spawn_blocking_build_held_mid_scan(&ds, &session, "DEFINE INDEX ix ON t FIELDS flag")
			.await?;
	// `t:1` is the first record the scan reads; `t:300` is the last.
	execute_all_retrying_conflicts(
		&ds,
		&session,
		"UPDATE t:1 SET flag = true RETURN NONE; UPDATE t:300 SET flag = true RETURN NONE;",
	)
	.await?;
	drop(guard);
	define.await??;
	for flag in ["true", "false"] {
		let via = |clause: &str| {
			format!("SELECT VALUE id FROM t {clause} WHERE flag = {flag} ORDER BY id")
		};
		let indexed = query_json(&ds, &session, &via("WITH INDEX ix")).await?;
		let scanned = query_json(&ds, &session, &via("WITH NOINDEX")).await?;
		assert_eq!(indexed, scanned, "the index disagrees with a scan for flag = {flag}");
		let expected = if flag == "true" {
			2
		} else {
			298
		};
		assert_eq!(indexed.as_array().map(Vec::len), Some(expected), "flag = {flag}");
	}
	Ok(())
}

/// The UNIQUE counterpart: the value an UPDATE moved a scanned record off is free again and the
/// value it moved to is taken, once the build is done.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn blocking_unique_define_index_keeps_an_update_committed_mid_scan() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE TABLE t SCHEMALESS;
		 FOR $i IN 1..=300 { CREATE type::record('t', $i) SET code = 'c' + <string> $i RETURN NONE; };",
	)
	.await?;
	let (guard, _scan, define) = spawn_blocking_build_held_mid_scan(
		&ds,
		&session,
		"DEFINE INDEX ux ON t FIELDS code UNIQUE",
	)
	.await?;
	execute_all_retrying_conflicts(&ds, &session, "UPDATE t:1 SET code = 'moved' RETURN NONE;")
		.await?;
	drop(guard);
	define.await??;
	assert!(
		execute_all(&ds, &session, "CREATE t:dup SET code = 'moved' RETURN NONE").await.is_err(),
		"the value the record moved to is taken"
	);
	execute_all(&ds, &session, "CREATE t:reuse SET code = 'c1' RETURN NONE").await?;
	assert_eq!(
		query_json(&ds, &session, "SELECT VALUE id FROM t WITH INDEX ux WHERE code = 'moved'")
			.await?,
		serde_json::json!(["t:1"])
	);
	assert_eq!(
		query_json(&ds, &session, "SELECT VALUE id FROM t WITH INDEX ux WHERE code = 'c1'").await?,
		serde_json::json!(["t:reuse"])
	);
	Ok(())
}

/// Creates issued from several tasks at once against a table with a full-text index and its
/// analyzer all commit, and every document is findable.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parallel_creates_over_a_fulltext_index_all_commit() -> Result<()> {
	const WRITERS: usize = 8;
	const CREATES: usize = 25;
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase,snowball(english);
		 DEFINE TABLE doc SCHEMALESS;
		 DEFINE INDEX ft ON doc FIELDS body FULLTEXT ANALYZER simple BM25;",
	)
	.await?;
	let mut tasks = Vec::new();
	for writer in 0..WRITERS {
		let (ds, session) = (Arc::clone(&ds), session.clone());
		tasks.push(tokio::spawn(async move {
			for n in 0..CREATES {
				let sql = format!("CREATE doc SET body = 'hello world {n} from {writer}'");
				execute_all(&ds, &session, &sql).await?;
			}
			Ok::<_, anyhow::Error>(())
		}));
	}
	for task in tasks {
		task.await??;
	}
	assert_eq!(
		query_json(&ds, &session, "SELECT VALUE count() FROM doc WHERE body @@ 'hello' GROUP ALL")
			.await?,
		serde_json::json!([WRITERS * CREATES])
	);
	Ok(())
}

/// Times a synchronous compound `UNIQUE` definition on a relation table, alone and after five
/// `CONCURRENTLY` definitions on the same table in the same query, over a fresh database each
/// trial, and prints the median, minimum and maximum of each. Run in release mode, with
/// `--ignored --nocapture`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a timing harness, not a test"]
async fn sync_unique_after_concurrent_definitions_timing() -> Result<()> {
	const MODELS: usize = 170;
	const TRIALS: usize = 7;
	const CONCURRENT: &str = "
		DEFINE INDEX OVERWRITE idx_sr ON has_attribute FIELDS source_rel CONCURRENTLY;
		DEFINE INDEX OVERWRITE idx_sr_in ON has_attribute FIELDS source_rel, in CONCURRENTLY;
		DEFINE INDEX OVERWRITE idx_sr_out ON has_attribute FIELDS source_rel, out CONCURRENTLY;
		DEFINE INDEX OVERWRITE idx_in ON has_attribute FIELDS in CONCURRENTLY;
		DEFINE INDEX OVERWRITE idx_out ON has_attribute FIELDS out CONCURRENTLY;";
	const UNIQUE: &str =
		"DEFINE INDEX OVERWRITE idx_sr_in_out ON has_attribute FIELDS source_rel, in, out UNIQUE;";
	// The datastore path, `memory` unless `SURREAL_TIMING_PATH` names another (a `surrealkv://`
	// path needs the `kv-surrealkv` feature).
	let path = std::env::var("SURREAL_TIMING_PATH").unwrap_or_else(|_| "memory".to_owned());
	let ds = Datastore::builder().build_with_path(&path).await?;
	let summary = |name: &str, mut times: Vec<Duration>| {
		times.sort();
		println!(
			"{name}: median {:?} min {:?} max {:?}",
			times[times.len() / 2],
			times[0],
			times[times.len() - 1]
		);
	};
	let mut alone = Vec::new();
	let mut after = Vec::new();
	let mut batch = Vec::new();
	for trial in 0..TRIALS * 2 {
		let session = Session::owner().with_ns("timing").with_db(&format!("db{trial}"));
		let tx = ds.transaction(TransactionType::Write).await?;
		tx.ensure_ns_db(None, "timing", &format!("db{trial}")).await?;
		tx.commit().await?;
		execute_all(
			&ds,
			&session,
			&format!(
				"DEFINE TABLE resource SCHEMALESS;
				 DEFINE TABLE attribute SCHEMALESS;
				 DEFINE TABLE has_attribute TYPE RELATION IN resource OUT attribute SCHEMALESS;
				 FOR $model IN 0..{MODELS} {{
					LET $resource = type::record('resource', $model);
					UPSERT $resource SET name = string::concat('model_', <string>$model);
					FOR $column IN 0..8 {{
						LET $attribute = type::record('attribute', $model * 8 + $column);
						UPSERT $attribute SET name = string::concat('column_', <string>$column);
						RELATE $resource->has_attribute->$attribute SET source_rel = 'HAS_COLUMN';
					}};
				 }};"
			),
		)
		.await?;
		let results = if trial % 2 == 0 {
			let started = Instant::now();
			let results = ds.execute(UNIQUE, &session, None).await?;
			alone.push(started.elapsed());
			results
		} else {
			let started = Instant::now();
			let results = ds.execute(&format!("{CONCURRENT}\n{UNIQUE}"), &session, None).await?;
			batch.push(started.elapsed());
			after.push(results.last().map(|result| result.time).unwrap_or_default());
			results
		};
		for result in results {
			result.result?;
		}
		// The concurrent builds finish before the next trial.
		sleep(Duration::from_millis(500)).await;
	}
	summary("sync UNIQUE alone", alone);
	summary("sync UNIQUE after five CONCURRENTLY (its statement)", after);
	summary("whole six-statement batch", batch);
	Ok(())
}

/// A write that commits while a `DEFINE INDEX … CONCURRENTLY` statement's
/// transaction is still open cannot see the definition, so it neither indexes
/// the record nor queues it for the build. The build must therefore read the
/// table only once that transaction has committed and every write that began
/// before the commit has ended, or a record it scanned first would keep its old
/// entry and never gain its new one (surrealdb/surrealdb#7550).
#[tokio::test(flavor = "multi_thread")]
async fn writes_inside_an_open_concurrent_definition_reach_the_index() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE t SCHEMALESS;
			FOR $i IN 1..=200 { CREATE type::record('t', $i) SET x = 'old' RETURN NONE; };
			",
	)
	.await?;

	let define = tokio::spawn({
		let (ds, session) = (Arc::clone(&ds), session.clone());
		async move {
			execute_all(
				&ds,
				&session,
				"BEGIN; DEFINE INDEX ix ON t FIELDS x CONCURRENTLY; SLEEP 5s; COMMIT;",
			)
			.await
		}
	});
	// Long enough for a build that did not wait for its statement to have
	// scanned all 200 rows.
	sleep(Duration::from_millis(500)).await;
	execute_all(&ds, &session, "UPDATE t:1..=10 SET x = 'new' RETURN NONE").await?;
	assert!(!define.is_finished(), "the write must land while the definition is uncommitted");
	define.await??;
	wait_for_index_ready(&ds, &session, "t", "ix").await?;

	expect_indexed_query_len(&ds, &session, "SELECT id FROM t WITH INDEX ix WHERE x = 'new'", 10)
		.await?;
	expect_indexed_query_len(&ds, &session, "SELECT id FROM t WITH INDEX ix WHERE x = 'old'", 190)
		.await?;
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

/// Whether `t`'s index `ix` holds an entry for value `x` and record `t:<id>`.
async fn has_entry(
	ds: &Datastore,
	(ns, db, ix): (NamespaceId, DatabaseId, &IndexDefinition),
	x: &str,
	id: i64,
) -> Result<bool> {
	let key = crate::idx::kernels::btree::IndexKeyspace::of(ns, db, ix).stored_key(
		false,
		&[Value::from(x)],
		&RecordIdKey::Number(id),
	)?;
	let tx = ds.transaction(TransactionType::Read).await?;
	let entry = catch!(tx, tx.get_key(&key, None).await);
	tx.cancel().await?;
	Ok(entry.is_some())
}

/// A write that indexes normally once a build is `Online` must not leave the
/// entry the build's last replay wrote for the record's previous value.
///
/// A writer held while the build is `Closing` began its transaction — and took
/// its snapshot — before the final replay pass committed. Once it observes
/// `Online` it moves the record's entries itself, from its snapshot: the entry
/// for the old value that replay wrote is absent there, so the delete finds
/// nothing. The write must then conflict with the replay rather than commit,
/// or the old value's entry survives beside the new one and an index scan
/// returns the record for a value it no longer holds.
#[tokio::test(flavor = "multi_thread")]
async fn normal_write_from_a_snapshot_before_the_final_replay_conflicts() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE t SCHEMALESS;
			CREATE t:1 SET x = 'a' RETURN NONE;
			DEFINE INDEX ix ON t FIELDS x;
			",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "t", "ix").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	let entries = (ns, db, &*ix);
	assert!(has_entry(&ds, entries, "a", 1).await?);

	// A `Building` generation whose initial scan is complete, taken over by a
	// builder this test drives by hand.
	const GENERATION: BuildGeneration = 3;
	let expired = Utc::now() - chrono::Duration::seconds(BUILD_OWNER_LEASE_SECS + 5);
	let mut state = durable_build_state_for_phase(IndexBuildPhase::Building, GENERATION, None);
	state.updated_at = expired;
	state.owner_heartbeat_at = Some(expired);
	set_durable_build_state(&ds, &ikb, state).await?;
	let tx = ds.transaction(TransactionType::Write).await?;
	tx.set_key(&ikb.new_bt_key(GENERATION), &0u64).await?;
	tx.commit().await?;
	let build = new_building_for_index(&ds, &session, ns, db, &table, Arc::clone(&ix)).await?;
	let acquired = build
		.acquire_build_state()
		.await?
		.expect("the expired generation should be available for takeover");
	assert_eq!(acquired.generation, GENERATION);

	// The build queues `a -> b` rather than indexing it.
	execute_all(&ds, &session, "UPDATE t:1 SET x = 'b' RETURN NONE").await?;
	assert!(has_entry(&ds, entries, "a", 1).await?, "a queued write leaves the index alone");

	// A writer whose snapshot sees `b` on the record but not yet in the index.
	let writer = Arc::new(ds.transaction(TransactionType::Write).await?);
	let seen = ds
		.run(
			crate::kvs::QueryRequest::new("SELECT VALUE x FROM ONLY t:1", &session)
				.with_transaction(Arc::clone(&writer)),
		)
		.await?;
	let seen = seen[0].result.as_ref().map_err(|e| anyhow::anyhow!("{e}"))?;
	assert_eq!(seen.clone().into_json_value(), serde_json::json!("b"));

	// The build replays `a -> b`, closes and publishes.
	let mut updates = 0;
	let mut last_check = Instant::now();
	build
		.index_appending_loop(super::replay::ReplayUntil::Drained, 1, &mut updates, &mut last_check)
		.await?;
	assert_eq!(updates, 1, "the queued write is replayed");
	build.mark_durable_closing(GENERATION).await?;
	build
		.index_appending_loop(super::replay::ReplayUntil::Drained, 1, &mut updates, &mut last_check)
		.await?;
	build.mark_durable_online(GENERATION, 1, updates, true).await?;
	assert!(has_entry(&ds, entries, "b", 1).await?, "the replay indexes `b`");
	assert!(!has_entry(&ds, entries, "a", 1).await?, "the replay removes `a`");

	// The writer now sees `Online` and moves `b -> c` from its snapshot, where
	// the `b` entry the replay wrote does not exist yet.
	let results = ds
		.run(
			crate::kvs::QueryRequest::new("UPDATE t:1 SET x = 'c' RETURN NONE", &session)
				.with_transaction(Arc::clone(&writer)),
		)
		.await?;
	for result in results {
		result.result.map_err(|e| anyhow::anyhow!("{e}"))?;
	}
	let commit = writer.commit().await;
	match &commit {
		Err(err) => assert!(
			is_retryable_transaction_conflict(err),
			"the stale writer must fail with a retryable conflict, not {err:#}"
		),
		Ok(()) => panic!(
			"a write whose snapshot predates the final replay committed; `b` still indexed: {}",
			has_entry(&ds, entries, "b", 1).await?
		),
	}

	// Retried from a fresh snapshot, the write leaves the index exact.
	execute_all(&ds, &session, "UPDATE t:1 SET x = 'c' RETURN NONE").await?;
	assert!(has_entry(&ds, entries, "c", 1).await?);
	assert!(
		!has_entry(&ds, entries, "b", 1).await?,
		"no entry left for a value t:1 no longer holds"
	);
	assert!(!has_entry(&ds, entries, "a", 1).await?);
	Ok(())
}

/// A write held in admission while a build is `Closing` is released as soon
/// as a build in this process publishes `Online`, without waiting out a poll.
#[tokio::test(flavor = "multi_thread")]
async fn a_write_held_by_closing_is_released_when_the_build_publishes() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE t SCHEMALESS;
			CREATE t:1 SET x = 'a' RETURN NONE;
			DEFINE INDEX ix ON t FIELDS x;
			",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "t", "ix").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	let entries = (ns, db, &*ix);

	// A `Building` generation whose initial scan is complete, taken over by a
	// builder this test drives by hand, and moved to `Closing`.
	const GENERATION: BuildGeneration = 3;
	let expired = Utc::now() - chrono::Duration::seconds(BUILD_OWNER_LEASE_SECS + 5);
	let mut state = durable_build_state_for_phase(IndexBuildPhase::Building, GENERATION, None);
	state.updated_at = expired;
	state.owner_heartbeat_at = Some(expired);
	set_durable_build_state(&ds, &ikb, state).await?;
	let tx = ds.transaction(TransactionType::Write).await?;
	tx.set_key(&ikb.new_bt_key(GENERATION), &0u64).await?;
	tx.commit().await?;
	let build = new_building_for_index(&ds, &session, ns, db, &table, Arc::clone(&ix)).await?;
	let acquired = build
		.acquire_build_state()
		.await?
		.expect("the expired generation should be available for takeover");
	assert_eq!(acquired.generation, GENERATION);
	build.mark_durable_closing(GENERATION).await?;

	// Only the signal can end the writer's wait within the test's bound.
	*ds.index_builder().closing_poll.lock() = Some(Duration::from_secs(60));
	let writer = tokio::spawn({
		let (ds, session) = (Arc::clone(&ds), session.clone());
		async move { execute_all(&ds, &session, "UPDATE t:1 SET x = 'b' RETURN NONE").await }
	});
	timeout(Duration::from_secs(10), async {
		while ds.index_builder().closing_waits.load(Ordering::Relaxed) == 0 {
			sleep(Duration::from_millis(10)).await;
		}
	})
	.await
	.map_err(|_| anyhow::anyhow!("the writer never waited on the Closing build"))?;
	assert!(!writer.is_finished(), "an indexed write waits while the build is Closing");

	build.mark_durable_online(GENERATION, 1, 0, true).await?;
	timeout(Duration::from_secs(10), writer)
		.await
		.map_err(|_| anyhow::anyhow!("the writer was not released when the build published"))???;
	assert!(has_entry(&ds, entries, "b", 1).await?);
	assert!(!has_entry(&ds, entries, "a", 1).await?);
	Ok(())
}

/// While a build runs in this process, the resume scan adopts no stalled build
/// that still has a scan to run, but does adopt one stranded in `Closing`:
/// every write that changes that index's fields waits in admission until the
/// build leaves `Closing`.
#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
async fn a_running_build_defers_stalled_scans_but_not_a_stalled_closing() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"
			DEFINE TABLE t SCHEMALESS;
			CREATE t:1 SET vec = [1.0, 0.0], x = 'a', y = 'a' RETURN NONE;
			DEFINE INDEX hx ON t FIELDS vec HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32;
			DEFINE INDEX ix ON t FIELDS x;
			DEFINE INDEX iy ON t FIELDS y;
			",
	)
	.await?;
	// A local build that stays unfinished until its statement commits.
	let (ns, db, table, hx) = get_table_index(&ds, "t", "hx").await?;
	let txn = build_index_inside_an_open_statement_transaction(
		&ds,
		&session,
		ns,
		db,
		&table,
		Arc::clone(&hx),
		false,
	)
	.await?;
	assert!(ds.index_builder().has_unfinished_build().await);

	let expired = Utc::now() - chrono::Duration::seconds(BUILD_OWNER_LEASE_SECS + 5);
	let strand = |phase: IndexBuildPhase, ix: IndexId| {
		let ds = Arc::clone(&ds);
		let table = table.clone();
		async move {
			let ikb = IndexKeyBase::new(ns, db, table, ix);
			let mut state = durable_build_state_for_phase(phase, 2, None);
			state.owner = Some(Uuid::new_v4());
			state.updated_at = expired;
			state.owner_heartbeat_at = Some(expired);
			set_durable_build_state(&ds, &ikb, state).await?;
			let tx = ds.transaction(TransactionType::Write).await?;
			tx.set_key(&ikb.new_bt_key(2), &0u64).await?;
			// The proof that the stranded owner waited at the write barrier,
			// without which a takeover rescans what it had scanned.
			tx.set_key(&ikb.new_bw_key(), &2).await?;
			tx.commit().await?;
			anyhow::Ok(ikb)
		}
	};
	let (_, _, _, iy) = get_table_index(&ds, "t", "iy").await?;
	let iy_ikb = strand(IndexBuildPhase::Building, iy.index_id).await?;
	let resumed =
		ds.resume_stalled_index_builds(Duration::from_secs(30), CancellationToken::new()).await?;
	assert_eq!(resumed.items, 0, "a stalled scan waits for the running build");

	let (_, _, _, ix) = get_table_index(&ds, "t", "ix").await?;
	let ix_ikb = strand(IndexBuildPhase::Closing, ix.index_id).await?;
	let resumed =
		ds.resume_stalled_index_builds(Duration::from_secs(30), CancellationToken::new()).await?;
	assert_eq!(resumed.items, 1, "a stalled Closing build is adopted while another build runs");
	timeout(Duration::from_secs(30), async {
		while durable_build_state(&ds, &ix_ikb).await?.phase != IndexBuildPhase::Online {
			sleep(Duration::from_millis(20)).await;
		}
		anyhow::Ok(())
	})
	.await
	.map_err(|_| anyhow::anyhow!("the adopted Closing build never came online"))??;
	assert_eq!(
		durable_build_state(&ds, &iy_ikb).await?.phase,
		IndexBuildPhase::Building,
		"the stalled scan is still left for a later pass"
	);

	txn.commit().await?;
	Ok(())
}

/// A DiskANN definition with parameters no index can serve is refused in its own
/// transaction, `CONCURRENTLY` included, so it never commits a definition that
/// would fail every later write and query on its table.
#[cfg(diskann)]
#[tokio::test(flavor = "multi_thread")]
async fn define_index_refuses_unservable_diskann_parameters() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	for sql in [
		"DEFINE INDEX ix ON t FIELDS v DISKANN DIMENSION 33026 DIST EUCLIDEAN TYPE U8;",
		"DEFINE INDEX ix ON t FIELDS v DISKANN DIMENSION 33026 DIST EUCLIDEAN TYPE U8 CONCURRENTLY;",
	] {
		let err = ds.execute(sql, &session, None).await?.remove(0).result.expect_err(sql);
		assert!(err.to_string().contains("support DIMENSION up to 33025"), "{sql}: {err}");
		let res = ds
			.execute("(INFO FOR TABLE t).indexes.keys();", &session, None)
			.await?
			.remove(0)
			.result?;
		assert_eq!(surrealdb_types::ToSql::to_sql(&res), "[]", "{sql} left an index");
	}
	execute_all(
		&ds,
		&session,
		"DEFINE INDEX ix ON t FIELDS v DISKANN DIMENSION 33025 DIST EUCLIDEAN TYPE U8 CONCURRENTLY;",
	)
	.await?;
	Ok(())
}

/// Removing an index drops its pending-deletion counter so the map does not
/// accumulate entries for dropped indexes and a newly defined index cannot
/// inherit a retired one's count.
#[cfg(all(feature = "kv-mem", feature = "vector-index"))]
#[tokio::test]
async fn removing_an_index_forgets_its_counted_pending_deletions() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(
		&ds,
		&session,
		"DEFINE TABLE t; DEFINE INDEX hx ON t FIELDS v HNSW DIMENSION 2 DIST EUCLIDEAN;",
	)
	.await?;
	let (ns, db, table, ix) = get_table_index(&ds, "t", "hx").await?;
	let ikb = IndexKeyBase::new(ns, db, table, ix.index_id);

	let builder = ds.index_builder();
	builder.note_pending_deleted(&ikb, 1_024);

	execute_all(&ds, &session, "REMOVE INDEX hx ON t;").await?;

	assert!(
		!builder.take_pending_range_compaction(&ikb),
		"REMOVE INDEX must drop the index's pending-deletion count"
	);
	Ok(())
}

/// The format version `DEFINE INDEX` and `REBUILD INDEX` stamp is admitted to the
/// store's feature level: the current one above `Legacy`, and no higher than
/// [`crate::catalog::LEGACY_LEVEL_INDEX_FORMAT_VERSION`] on a store held at
/// `Legacy`, which releases before 4.0 can still serve.
#[cfg(feature = "kv-mem")]
#[tokio::test]
async fn index_format_stamp_is_admitted_to_the_feature_level() -> Result<()> {
	use crate::catalog::{INDEX_FORMAT_VERSION, LEGACY_LEVEL_INDEX_FORMAT_VERSION};

	async fn stamp(ds: &Datastore, index: &str) -> Result<u16> {
		Ok(stamp_and_id(ds, index).await?.0)
	}

	async fn stamp_and_id(ds: &Datastore, index: &str) -> Result<(u16, crate::catalog::IndexId)> {
		let tx = ds.transaction(TransactionType::Read).await?;
		let ns = tx.get_ns_by_name("test", None).await?.expect("namespace should exist");
		let db = tx.get_db_by_name("test", "test", None).await?.expect("database should exist");
		let ix = tx
			.get_tb_index(ns.namespace_id, db.database_id, &TableName::from("t"), index, None)
			.await?
			.expect("index should exist");
		tx.cancel().await?;
		Ok((ix.format_version, ix.index_id))
	}

	/// An unchanged `DEFINE INDEX OVERWRITE` keeps an index whose stamp is the one
	/// the store admits now, and keeps its stamp.
	async fn overwrite_keeps(ds: &Datastore, session: &Session) -> Result<(u16, bool)> {
		let (_, before) = stamp_and_id(ds, "hx").await?;
		let sql = "DEFINE INDEX OVERWRITE hx ON t FIELDS v HNSW DIMENSION 2 DIST EUCLIDEAN;";
		for r in ds.execute(sql, session, None).await? {
			r.result?;
		}
		let (stamped, after) = stamp_and_id(ds, "hx").await?;
		Ok((stamped, after == before))
	}

	async fn define_and_rebuild(ds: &Datastore, session: &Session) -> Result<(u16, u16)> {
		for sql in [
			"DEFINE TABLE t SCHEMALESS;",
			"DEFINE INDEX hx ON t FIELDS v HNSW DIMENSION 2 DIST EUCLIDEAN;",
		] {
			for r in ds.execute(sql, session, None).await? {
				r.result?;
			}
		}
		let defined = stamp(ds, "hx").await?;
		for r in ds.execute("REBUILD INDEX hx ON t;", session, None).await? {
			r.result?;
		}
		Ok((defined, stamp(ds, "hx").await?))
	}

	let (ds, session) = new_index_test_ds().await?;
	assert!(!ds.feature_level().await?.level.is_legacy(), "a new store starts above Legacy");
	assert_eq!(
		define_and_rebuild(&ds, &session).await?,
		(INDEX_FORMAT_VERSION, INDEX_FORMAT_VERSION)
	);
	assert_eq!(overwrite_keeps(&ds, &session).await?, (INDEX_FORMAT_VERSION, true));
	// Above `Legacy`, an HNSW index stamped with the legacy-level format lacks the
	// layout this format gives it, so an unchanged overwrite rebuilds it.
	{
		let tx = ds.transaction(TransactionType::Write).await?;
		let ns = tx.get_ns_by_name("test", None).await?.expect("namespace should exist");
		let db = tx.get_db_by_name("test", "test", None).await?.expect("database should exist");
		let table = TableName::from("t");
		let mut ix = tx
			.get_tb_index(ns.namespace_id, db.database_id, &table, "hx", None)
			.await?
			.expect("index should exist")
			.as_ref()
			.clone();
		ix.format_version = LEGACY_LEVEL_INDEX_FORMAT_VERSION;
		tx.put_tb_index(ns.namespace_id, db.database_id, &table, &ix).await?;
		tx.commit().await?;
	}
	assert_eq!(overwrite_keeps(&ds, &session).await?, (INDEX_FORMAT_VERSION, false));

	// A store an earlier release wrote, opened by this build with the opt-in to upgrade it, and
	// held at Legacy.
	let legacy = Datastore::builder()
		.without_maintenance_tasks()
		.without_startup_transactions()
		.with_config(
			surrealdb_cnf::ConfigMap::empty()
				.with_key_value("experimental_upgrade_from_3x", "true"),
		)
		.with_legacy_feature_level()
		.build_with_path("memory")
		.await?;
	let tx = legacy.transaction(TransactionType::Write).await?;
	tx.set_key(&crate::key::schema::VersionKey {}, &crate::kvs::version::MajorVersion::latest())
		.await?;
	tx.set_key(
		&crate::key::schema::StorageVersionKey {},
		&crate::kvs::version::StorageVersion::parse("3.3.0")?,
	)
	.await?;
	tx.commit().await?;
	legacy.check_version().await?;
	let tx = legacy.transaction(TransactionType::Write).await?;
	tx.ensure_ns_db(None, "test", "test").await?;
	tx.commit().await?;
	assert!(legacy.feature_level().await?.level.is_legacy(), "the store is held at Legacy");
	assert_eq!(
		define_and_rebuild(&legacy, &session).await?,
		(LEGACY_LEVEL_INDEX_FORMAT_VERSION, LEGACY_LEVEL_INDEX_FORMAT_VERSION)
	);
	assert_eq!(
		overwrite_keeps(&legacy, &session).await?,
		(LEGACY_LEVEL_INDEX_FORMAT_VERSION, true),
		"an index at the format the store admits is current there"
	);
	Ok(())
}

/// The number of rows `sql` returns, run on the caller's transaction `tx`.
async fn rows_in_transaction(
	ds: &Datastore,
	session: &Session,
	tx: &Arc<crate::kvs::Transaction>,
	sql: &str,
) -> Result<usize> {
	let mut results =
		ds.run(QueryRequest::new(sql, session).with_transaction(Arc::clone(tx))).await?;
	let value = results.pop().expect("one result").result?.into_json_value();
	Ok(value.as_array().map_or(0, Vec::len))
}

/// A transaction whose snapshot shows an index online reads its own writes
/// through it, even when a build that started after the transaction began
/// queued those writes instead of indexing them, or when the index was
/// removed in between.
#[tokio::test(flavor = "multi_thread")]
async fn a_transaction_reads_its_own_write_through_an_index_changed_after_it_began() -> Result<()> {
	for change in ["REBUILD INDEX ix ON t CONCURRENTLY", "REMOVE INDEX ix ON t"] {
		let (ds, session) = new_index_test_ds().await?;
		execute_all(
			&ds,
			&session,
			"DEFINE TABLE t; DEFINE INDEX ix ON t FIELDS k UNIQUE; CREATE t:1 SET k = 'a';",
		)
		.await?;
		let tx = Arc::new(ds.transaction(TransactionType::Write).await?);
		let read = "SELECT * FROM t WHERE k = 'b'";
		assert_eq!(rows_in_transaction(&ds, &session, &tx, read).await?, 0, "{change}");
		execute_all(&ds, &session, change).await?;
		rows_in_transaction(&ds, &session, &tx, "CREATE t:2 SET k = 'b'").await?;
		assert_eq!(
			rows_in_transaction(&ds, &session, &tx, "SELECT * FROM t:2").await?,
			1,
			"{change}: the record"
		);
		assert_eq!(
			rows_in_transaction(&ds, &session, &tx, read).await?,
			1,
			"{change}: the record by its indexed field"
		);
		tx.cancel().await?;
	}
	Ok(())
}

/// A datastore whose statements leave index builds to bounded passes.
#[cfg(feature = "kv-mem")]
async fn new_index_pass_test_ds() -> Result<(Arc<Datastore>, Session)> {
	let ds = Datastore::builder()
		.without_maintenance_tasks()
		.without_index_build_tasks()
		.build_with_path("memory")
		.await?;
	let session = Session::owner().with_ns("test").with_db("test");
	let tx = ds.transaction(TransactionType::Write).await?;
	tx.ensure_ns_db(None, "test", "test").await?;
	tx.commit().await?;
	Ok((ds, session))
}

/// The writes made while the pass-driven build in [`passes_build_the_index_an_ordinary_build_does`]
/// runs, the `step`th of them: an update that moves a record between keys, a new record, and a
/// deleted one.
#[cfg(feature = "kv-mem")]
fn write_during_build(step: usize) -> String {
	format!(
		"UPDATE t:{a} SET n = {n}; CREATE t:{c} SET n = {m}; DELETE t:{d};",
		a = step % 40,
		n = (step * 3) % 7,
		c = 1000 + step,
		m = step % 7,
		d = 40 + (step % 20),
	)
}

/// Runs passes of `budget` batches until none is left, between passes running `between`, and
/// answers how many passes ran.
#[cfg(feature = "kv-mem")]
async fn run_passes_to_end(
	ds: &Datastore,
	session: &Session,
	budget: usize,
	mut between: impl FnMut(usize) -> Option<String>,
) -> Result<usize> {
	let mut passes = 0;
	loop {
		let run = ds.run_index_builds(budget).await?;
		passes += 1;
		assert!(
			run.batches <= budget,
			"a pass ran {} batches over a budget of {budget}",
			run.batches
		);
		if !run.more {
			return Ok(passes);
		}
		assert!(passes < 1000, "the build did not finish under passes of {budget} batches");
		if let Some(sql) = between(passes) {
			execute_all(ds, session, &sql).await?;
		}
	}
}

/// A build left to bounded passes, with writes between every pass, ends with the index an
/// ordinary build of the same records writes, reporting `ready` only once the passes are done.
#[cfg(feature = "kv-mem")]
async fn assert_passes_build_the_index_an_ordinary_build_does(
	define: &str,
	budget: usize,
) -> Result<()> {
	let (ds, session) = new_index_pass_test_ds().await?;
	let _scan = super::kind::test_sink::set_scan_batch(ds.id(), 4);
	execute_all(&ds, &session, "FOR $i IN 0..60 { CREATE type::record('t', $i) SET n = $i % 7 };")
		.await?;
	execute_all(&ds, &session, define).await?;
	assert_ne!(
		index_building_status(&ds, &session, "t", "ix").await?,
		"ready",
		"the statement returns before any pass has run"
	);
	let mut writes = 0;
	let passes = run_passes_to_end(&ds, &session, budget, |_| {
		writes += 1;
		Some(write_during_build(writes))
	})
	.await?;
	assert!(passes > 2, "the build ran in {passes} passes, so no pass was bounded");
	assert_eq!(index_building_status(&ds, &session, "t", "ix").await?, "ready");

	// The same records, written directly, and indexed by an ordinary build.
	let (reference, session_ref) = new_index_test_ds().await?;
	execute_all(
		&reference,
		&session_ref,
		"FOR $i IN 0..60 { CREATE type::record('t', $i) SET n = $i % 7 };",
	)
	.await?;
	let mut sql = String::new();
	for step in 1..=writes {
		sql.push_str(&write_during_build(step));
	}
	execute_all(&reference, &session_ref, &sql).await?;
	execute_all(&reference, &session_ref, "DEFINE INDEX ix ON t FIELDS n;").await?;
	assert_eq!(
		index_entry_keys(&ds, "t", "ix").await?,
		index_entry_keys(&reference, "t", "ix").await?,
		"the passes wrote the index an ordinary build writes"
	);
	for n in 0..7 {
		let sql = format!("SELECT VALUE id FROM t WHERE n = {n} ORDER BY id");
		let unindexed = format!("SELECT VALUE id FROM t WITH NOINDEX WHERE n = {n} ORDER BY id");
		let mut indexed = ds.execute(&sql, &session, None).await?;
		let mut scanned = ds.execute(&unindexed, &session, None).await?;
		assert_eq!(indexed.remove(0).result?, scanned.remove(0).result?, "n = {n}");
	}
	Ok(())
}

#[cfg(feature = "kv-mem")]
#[test_log::test(tokio::test)]
async fn passes_build_the_index_an_ordinary_build_does() -> Result<()> {
	assert_passes_build_the_index_an_ordinary_build_does(
		"DEFINE INDEX ix ON t FIELDS n CONCURRENTLY;",
		3,
	)
	.await
}

/// A blocking `DEFINE INDEX` on a populated table returns with the index building, as
/// `CONCURRENTLY` does, and passes of one batch each, with writes queued between every one of
/// them, still carry it to the end.
#[cfg(feature = "kv-mem")]
#[test_log::test(tokio::test)]
async fn a_blocking_define_is_left_to_passes_of_one_batch() -> Result<()> {
	assert_passes_build_the_index_an_ordinary_build_does("DEFINE INDEX ix ON t FIELDS n;", 1).await
}

/// A pass stopped after any batch — handed back as a pass that spends its budget does, or left
/// claimed by a pass whose process died — is resumed by later passes, which end with the index
/// whole.
#[cfg(feature = "kv-mem")]
#[test_log::test(tokio::test)]
async fn a_pass_stopped_after_any_batch_resumes() -> Result<()> {
	for stop_after in [1, 2, 3, 5, 8, 13] {
		let (ds, session) = new_index_pass_test_ds().await?;
		let _scan = super::kind::test_sink::set_scan_batch(ds.id(), 3);
		execute_all(
			&ds,
			&session,
			"FOR $i IN 0..40 { CREATE type::record('t', $i) SET n = $i % 5 };",
		)
		.await?;
		execute_all(&ds, &session, "DEFINE INDEX ix ON t FIELDS n CONCURRENTLY;").await?;
		let first = ds.run_index_builds(stop_after).await?;
		assert_eq!(first.batches, stop_after, "the first pass spends its budget");
		assert!(first.more, "and hands the build back");

		// A pass that claims the build and dies holds it, as far as the store says, until its
		// lease lapses.
		let (ns, db, table, ix) = get_table_index(&ds, "t", "ix").await?;
		let ikb = IndexKeyBase::new(ns, db, table, ix.index_id);
		let mut state = durable_build_state(&ds, &ikb).await?;
		assert_eq!(state.owner, None, "a handed-back build names no owner");
		state.owner = Some(Uuid::now_v7());
		state.owner_heartbeat_at = Some(Utc::now());
		set_durable_build_state(&ds, &ikb, state.clone()).await?;
		execute_all(&ds, &session, &write_during_build(stop_after)).await?;
		let held = ds.run_index_builds(100).await?;
		assert_eq!((held.batches, held.more), (0, false), "a held build is not claimed");
		assert!(
			held.retry_after.is_some_and(|after| after > Duration::from_secs(50)),
			"the pass says when the lease lapses: {held:?}"
		);
		assert_ne!(index_building_status(&ds, &session, "t", "ix").await?, "ready");

		state.owner_heartbeat_at = Some(Utc::now() - chrono::Duration::seconds(61));
		set_durable_build_state(&ds, &ikb, state).await?;
		run_passes_to_end(&ds, &session, 2, |_| None).await?;
		assert_eq!(index_building_status(&ds, &session, "t", "ix").await?, "ready");
		for n in 0..5 {
			let sql = format!("SELECT VALUE id FROM t WHERE n = {n} ORDER BY id");
			let unindexed =
				format!("SELECT VALUE id FROM t WITH NOINDEX WHERE n = {n} ORDER BY id");
			let mut indexed = ds.execute(&sql, &session, None).await?;
			let mut scanned = ds.execute(&unindexed, &session, None).await?;
			assert_eq!(
				indexed.remove(0).result?,
				scanned.remove(0).result?,
				"n = {n}, stopped after {stop_after}"
			);
		}
	}
	Ok(())
}

/// A pass over a parallel initial scan that spends its budget part-way through a round keeps the
/// batches of that round that committed: the next pass resumes after them instead of wiping the
/// index and scanning again, so passes smaller than a round still carry the build to the end.
#[cfg(feature = "kv-mem")]
#[test_log::test(tokio::test)]
async fn a_pass_that_spends_its_budget_mid_round_keeps_the_round() -> Result<()> {
	let ds = Datastore::builder()
		.with_config(
			surrealdb_cnf::ConfigMap::empty().with_key_value("index_build_parallelism", "4"),
		)
		.without_maintenance_tasks()
		.without_index_build_tasks()
		.build_with_path("memory")
		.await?;
	let session = Session::owner().with_ns("test").with_db("test");
	let tx = ds.transaction(TransactionType::Write).await?;
	tx.ensure_ns_db(None, "test", "test").await?;
	tx.commit().await?;
	let _scan = super::kind::test_sink::set_scan_batch(ds.id(), 3);
	execute_all(&ds, &session, "FOR $i IN 0..60 { CREATE type::record('t', $i) SET n = $i % 5 };")
		.await?;
	execute_all(&ds, &session, "DEFINE INDEX ix ON t FIELDS n CONCURRENTLY;").await?;
	let passes = run_passes_to_end(&ds, &session, 1, |_| None).await?;
	assert!(passes > 1, "the build ran in {passes} passes, so no pass was bounded");
	assert_eq!(index_building_status(&ds, &session, "t", "ix").await?, "ready");
	for n in 0..5 {
		let sql = format!("SELECT VALUE id FROM t WHERE n = {n} ORDER BY id");
		let unindexed = format!("SELECT VALUE id FROM t WITH NOINDEX WHERE n = {n} ORDER BY id");
		let mut indexed = ds.execute(&sql, &session, None).await?;
		let mut scanned = ds.execute(&unindexed, &session, None).await?;
		assert_eq!(indexed.remove(0).result?, scanned.remove(0).result?, "n = {n}");
	}
	Ok(())
}

/// A statement that leaves a build to passes wakes `index_build` once it commits, and writes a
/// key the build-state predicate matches; a record key does not match.
#[cfg(feature = "kv-mem")]
#[test_log::test(tokio::test)]
async fn a_deferred_build_is_signalled_and_its_state_key_recognised() -> Result<()> {
	let (ds, session) = new_index_pass_test_ds().await?;
	execute_all(&ds, &session, "CREATE t:1 SET n = 1; DEFINE INDEX ix ON t FIELDS n CONCURRENTLY;")
		.await?;
	timeout(Duration::from_secs(5), ds.commit_triggers().index_build.notified())
		.await
		.map_err(|_| anyhow::anyhow!("the statement did not wake the index build passes"))?;
	let (ns, db, table, ix) = get_table_index(&ds, "t", "ix").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	assert!(Datastore::is_index_build_state_key(&ikb.new_bs_key().encode_key()?));
	let record = crate::key::schema::RecordKey {
		ns,
		db,
		tb: Cow::Borrowed(&table),
		id: Cow::Owned(RecordIdKey::Number(1)),
	}
	.encode_key()?;
	assert!(!Datastore::is_index_build_state_key(&record));
	assert!(!Datastore::is_index_build_state_key(&ikb.new_bt_key(1).encode_key()?));
	let run = ds.run_index_builds(10).await?;
	assert!(!run.more, "{run:?}");
	assert_eq!(index_building_status(&ds, &session, "t", "ix").await?, "ready");
	Ok(())
}

/// On a datastore that leaves its background work to its host, the writes to a built index stay
/// queued for compaction until the host runs [`Datastore::run_index_compaction`]; each write
/// wakes `index_compaction`, and calls run while they report more drain the queue. The full-text,
/// `COUNT` and HNSW kinds are covered by `host_driven_compaction_drains_each_queued_kind`. `define`
/// is the index's definition on table `t`, whose records carry a two-dimensional `vec`.
#[cfg(feature = "kv-mem")]
async fn host_passes_drain_the_compaction_queue(define: &str, index: &str) -> Result<()> {
	let (ds, session) = new_index_pass_test_ds().await?;
	execute_all(&ds, &session, define).await?;
	while ds.run_index_builds(100).await?.more {}
	assert_eq!(index_building_status(&ds, &session, "t", index).await?, "ready");
	execute_all(
		&ds,
		&session,
		"CREATE t:1 SET vec = [1.0, 0.0];
		 CREATE t:2 SET vec = [0.0, 1.0];
		 CREATE t:3 SET vec = [1.0, 1.0];",
	)
	.await?;
	timeout(Duration::from_secs(5), ds.commit_triggers().index_compaction.notified())
		.await
		.map_err(|_| anyhow::anyhow!("the writes did not wake index compaction"))?;
	let building = index_building_json(&ds, &session, "t", index).await?;
	assert_eq!(building.get("compacting").and_then(|v| v.as_bool()), Some(true), "{building}");

	let mut passes = 0;
	let mut calls = 0;
	loop {
		let run = ds.run_index_compaction(8, Duration::from_secs(60)).await?;
		assert_eq!(run.retry_after, None, "{run:?}");
		passes += run.passes;
		calls += 1;
		if !run.more {
			break;
		}
		assert!(calls < 100, "the calls did not drain the queue: {run:?}");
	}
	assert!(passes > 0, "the calls ran no compaction pass");
	let building = index_building_json(&ds, &session, "t", index).await?;
	assert_eq!(building.get("compacting").and_then(|v| v.as_bool()), Some(false), "{building}");

	let idle = ds.run_index_compaction(8, Duration::from_secs(60)).await?;
	assert_eq!((idle.passes, idle.more), (0, false), "{idle:?}");
	Ok(())
}

#[cfg(all(feature = "kv-mem", diskann))]
#[test_log::test(tokio::test)]
async fn host_passes_compact_a_diskann_index() -> Result<()> {
	host_passes_drain_the_compaction_queue(
		"DEFINE INDEX dx ON t FIELDS vec DISKANN DIMENSION 2 DIST EUCLIDEAN TYPE F32;",
		"dx",
	)
	.await
}

/// A build whose statement is cancelled is not left for passes, and a datastore with nothing
/// pending reads as such.
#[cfg(feature = "kv-mem")]
#[test_log::test(tokio::test)]
async fn a_cancelled_definition_leaves_no_build_for_passes() -> Result<()> {
	let (ds, session) = new_index_pass_test_ds().await?;
	let idle = ds.run_index_builds(10).await?;
	assert_eq!((idle.batches, idle.more, idle.retry_after), (0, false, None));
	execute_all(&ds, &session, "CREATE t:1 SET n = 1;").await?;
	execute_cancelled_transaction(
		&ds,
		&session,
		"BEGIN; DEFINE INDEX ix ON t FIELDS n CONCURRENTLY; CANCEL;",
	)
	.await?;
	let run = ds.run_index_builds(10).await?;
	assert_eq!((run.batches, run.more, run.retry_after), (0, false, None), "{run:?}");
	assert!(get_table_index(&ds, "t", "ix").await.is_err());
	Ok(())
}

/// `REBUILD INDEX` on a datastore that leaves builds to passes returns with the index building,
/// and passes bring it back to `ready` with the index whole; one whose transaction is cancelled
/// leaves no build for passes and the ready index as it was.
#[cfg(feature = "kv-mem")]
#[test_log::test(tokio::test)]
async fn a_rebuild_is_left_to_passes_and_a_cancelled_one_is_not() -> Result<()> {
	let (ds, session) = new_index_pass_test_ds().await?;
	let _scan = super::kind::test_sink::set_scan_batch(ds.id(), 4);
	execute_all(&ds, &session, "FOR $i IN 0..30 { CREATE type::record('t', $i) SET n = $i % 3 };")
		.await?;
	execute_all(&ds, &session, "DEFINE INDEX ix ON t FIELDS n;").await?;
	run_passes_to_end(&ds, &session, 100, |_| None).await?;
	assert_eq!(index_building_status(&ds, &session, "t", "ix").await?, "ready");
	let built = index_entry_keys(&ds, "t", "ix").await?;

	execute_cancelled_transaction(&ds, &session, "BEGIN; REBUILD INDEX ix ON t; CANCEL;").await?;
	let run = ds.run_index_builds(100).await?;
	assert_eq!((run.batches, run.more), (0, false), "{run:?}");

	execute_all(&ds, &session, "REBUILD INDEX ix ON t;").await?;
	assert_ne!(index_building_status(&ds, &session, "t", "ix").await?, "ready");
	let passes = run_passes_to_end(&ds, &session, 2, |_| None).await?;
	assert!(passes > 1, "{passes}");
	assert_eq!(index_building_status(&ds, &session, "t", "ix").await?, "ready");
	assert_eq!(index_entry_keys(&ds, "t", "ix").await?, built);
	Ok(())
}

/// The writes made between passes in [`assert_passes_build_a_kind`]: one record moved between
/// values, one created and one deleted.
#[cfg(feature = "kv-mem")]
fn kind_write_during_build(step: usize) -> String {
	let word = ["alpha", "beta", "gamma"][step % 3];
	format!(
		"UPDATE t:{a} SET n = {n}, body = '{word} moved'; CREATE t:{c} SET n = {m}, body = '{word} new'; DELETE t:{d};",
		a = step % 20,
		n = step % 5,
		c = 1000 + step,
		m = (step * 2) % 5,
		d = 20 + (step % 20),
	)
}

/// Builds the index `define` declares on `t` with passes of two batches, writing between every
/// pass, and checks it ends `ready`, released by its last owner, answering each of `queries`
/// as `WITH NOINDEX` answers it.
#[cfg(feature = "kv-mem")]
async fn assert_passes_build_a_kind(define: &str, queries: &[&str]) -> Result<()> {
	let (ds, session) = new_index_pass_test_ds().await?;
	let _scan = super::kind::test_sink::set_scan_batch(ds.id(), 4);
	execute_all(
		&ds,
		&session,
		"DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;
		 FOR $i IN 0..40 {
			CREATE type::record('t', $i) SET n = $i % 5, body = ['alpha', 'beta', 'gamma'][$i % 3] + ' old';
		 };",
	)
	.await?;
	execute_all(&ds, &session, define).await?;
	let mut writes = 0;
	let passes = run_passes_to_end(&ds, &session, 2, |_| {
		writes += 1;
		Some(kind_write_during_build(writes))
	})
	.await?;
	assert!(passes > 2, "{define}: the build ran in {passes} passes");
	assert_eq!(index_building_status(&ds, &session, "t", "ix").await?, "ready", "{define}");
	let (ns, db, table, ix) = get_table_index(&ds, "t", "ix").await?;
	let state = durable_build_state(&ds, &IndexKeyBase::new(ns, db, table, ix.index_id)).await?;
	assert_eq!(state.owner, None, "{define}: the last pass released the build");
	for query in queries {
		let unindexed = query.replacen("FROM t", "FROM t WITH NOINDEX", 1);
		let mut indexed = ds.execute(query, &session, None).await?;
		let mut scanned = ds.execute(&unindexed, &session, None).await?;
		assert_eq!(indexed.remove(0).result?, scanned.remove(0).result?, "{query}");
	}
	Ok(())
}

#[cfg(feature = "kv-mem")]
#[test_log::test(tokio::test)]
async fn passes_build_each_index_kind_and_release_it() -> Result<()> {
	assert_passes_build_a_kind(
		"DEFINE INDEX ix ON t FIELDS n CONCURRENTLY;",
		&["SELECT VALUE id FROM t WHERE n = 2 ORDER BY id"],
	)
	.await?;
	assert_passes_build_a_kind(
		"DEFINE INDEX ix ON t FIELDS body FULLTEXT ANALYZER simple BM25 CONCURRENTLY;",
		&[
			"SELECT VALUE id FROM t WHERE body @@ 'alpha' ORDER BY id",
			"SELECT VALUE id FROM t WHERE body @@ 'gamma' ORDER BY id",
		],
	)
	.await?;
	assert_passes_build_a_kind(
		"DEFINE INDEX ix ON t COUNT CONCURRENTLY;",
		&["SELECT count() FROM t GROUP ALL"],
	)
	.await
}

/// A unique index left to passes refuses no duplicate written while it is pending: the duplicate
/// commits, and the build that replays it ends in `error`, as a `CONCURRENTLY` unique build over
/// such writes does.
#[cfg(feature = "kv-mem")]
#[test_log::test(tokio::test)]
async fn a_pending_unique_index_admits_a_duplicate_and_its_build_fails() -> Result<()> {
	let (ds, session) = new_index_pass_test_ds().await?;
	execute_all(&ds, &session, "DEFINE INDEX ix ON t FIELDS email UNIQUE;").await?;
	let mut first = ds.execute("CREATE t:1 SET email = 'a'", &session, None).await?;
	let mut second = ds.execute("CREATE t:2 SET email = 'a'", &session, None).await?;
	let first = first.remove(0).result.map(|_| ()).map_err(|e| e.to_string());
	let second = second.remove(0).result.map(|_| ()).map_err(|e| e.to_string());
	assert_eq!((first, second), (Ok(()), Ok(())), "both writes commit while the build is pending");
	run_passes_to_end(&ds, &session, 10, |_| None).await?;
	assert_eq!(index_building_status(&ds, &session, "t", "ix").await?, "error");
	Ok(())
}

/// A duplicate written while a unique build runs on its own task commits, and the build that
/// replays it ends in `error`: a running build admits writes without judging them, which is
/// what a build left to passes does too
/// ([`a_pending_unique_index_admits_a_duplicate_and_its_build_fails`]).
#[cfg(feature = "kv-mem")]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn a_running_unique_build_admits_a_duplicate_and_fails() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(&ds, &session, "CREATE t:1 SET email = 'a';").await?;
	let guard = start_index_build_paused(
		&ds,
		&session,
		"DEFINE INDEX ix ON t FIELDS email UNIQUE CONCURRENTLY;",
	)
	.await?;
	let mut second = ds.execute("CREATE t:2 SET email = 'a'", &session, None).await?;
	let second = second.remove(0).result.map(|_| ()).map_err(|e| e.to_string());
	drop(guard);
	let status = timeout(Duration::from_secs(10), async {
		loop {
			let status = index_building_status(&ds, &session, "t", "ix").await?;
			if status == "ready" || status == "error" {
				return Ok::<_, anyhow::Error>(status);
			}
			sleep(Duration::from_millis(20)).await;
		}
	})
	.await??;
	assert_eq!((second, status.as_str()), (Ok(()), "error"));
	Ok(())
}

/// A datastore that leaves builds to passes, but has a blocking statement run up to `batches`
/// batches of its own build once it commits.
#[cfg(feature = "kv-mem")]
async fn new_inline_pass_test_ds(batches: usize) -> Result<(Arc<Datastore>, Session)> {
	let ds = Datastore::builder()
		.without_maintenance_tasks()
		.without_index_build_tasks()
		.with_inline_index_build_batches(batches)
		.build_with_path("memory")
		.await?;
	let session = Session::owner().with_ns("test").with_db("test");
	let tx = ds.transaction(TransactionType::Write).await?;
	tx.ensure_ns_db(None, "test", "test").await?;
	tx.commit().await?;
	Ok((ds, session))
}

/// A blocking `DEFINE INDEX` whose build ends within the inline budget returns with the index
/// ready, as where builds run on tasks: a unique index refuses a duplicate from then on, and a
/// unique build over duplicates fails the statement and takes its definition back.
#[cfg(feature = "kv-mem")]
#[test_log::test(tokio::test)]
async fn a_blocking_define_runs_its_build_within_the_inline_budget() -> Result<()> {
	let (ds, session) = new_inline_pass_test_ds(100).await?;
	let _scan = super::kind::test_sink::set_scan_batch(ds.id(), 4);
	execute_all(&ds, &session, "FOR $i IN 0..20 { CREATE type::record('t', $i) SET email = $i };")
		.await?;
	execute_all(&ds, &session, "DEFINE INDEX ix ON t FIELDS email UNIQUE;").await?;
	assert_eq!(index_building_status(&ds, &session, "t", "ix").await?, "ready");
	let mut dup = ds.execute("CREATE t:99 SET email = 3", &session, None).await?;
	let err = dup.remove(0).result.expect_err("the ready unique index refuses a duplicate");
	assert!(err.to_string().contains("already contains"), "{err}");
	let run = ds.run_index_builds(10).await?;
	assert_eq!((run.batches, run.more), (0, false), "{run:?}");

	execute_all(&ds, &session, "CREATE u:1 SET email = 'a'; CREATE u:2 SET email = 'a';").await?;
	let mut define =
		ds.execute("DEFINE INDEX ux ON u FIELDS email UNIQUE;", &session, None).await?;
	let err = define.remove(0).result.expect_err("the build over duplicates fails the statement");
	assert!(err.to_string().contains("already contains"), "{err}");
	assert!(get_table_index(&ds, "u", "ux").await.is_err(), "its definition is taken back");
	Ok(())
}

/// A blocking `DEFINE INDEX` whose build outlasts the inline budget returns with the index
/// building and wakes the passes, which finish it.
#[cfg(feature = "kv-mem")]
#[test_log::test(tokio::test)]
async fn a_blocking_define_leaves_what_outlasts_the_inline_budget_to_passes() -> Result<()> {
	let (ds, session) = new_inline_pass_test_ds(2).await?;
	let _scan = super::kind::test_sink::set_scan_batch(ds.id(), 4);
	execute_all(&ds, &session, "FOR $i IN 0..40 { CREATE type::record('t', $i) SET n = $i % 5 };")
		.await?;
	execute_all(&ds, &session, "DEFINE INDEX ix ON t FIELDS n;").await?;
	assert_ne!(index_building_status(&ds, &session, "t", "ix").await?, "ready");
	timeout(Duration::from_secs(5), ds.commit_triggers().index_build.notified())
		.await
		.map_err(|_| anyhow::anyhow!("the statement did not wake the passes"))?;
	run_passes_to_end(&ds, &session, 2, |_| None).await?;
	assert_eq!(index_building_status(&ds, &session, "t", "ix").await?, "ready");
	Ok(())
}

/// The writes made between passes in [`assert_passes_build_a_vector_index`]: a vector moved, one
/// created and one deleted.
#[cfg(feature = "kv-mem")]
fn vector_write_during_build(step: usize) -> String {
	format!(
		"UPDATE t:{a} SET vec = [{x}f, {y}f]; CREATE t:{c} SET vec = [{y}f, {x}f]; DELETE t:{d};",
		a = step % 20,
		x = (step * 7 % 31) as f64 + 0.25,
		y = (step * 3 % 17) as f64 + 0.5,
		c = 1000 + step,
		d = 20 + (step % 20),
	)
}

/// Builds the vector index `kind` declares with passes of two batches, writing between every
/// pass, and checks it answers nearest-neighbour queries as an ordinary build over the same
/// records does, and as the exact search does.
#[cfg(feature = "kv-mem")]
async fn assert_passes_build_a_vector_index(kind: &str) -> Result<()> {
	const SEED: &str = "FOR $i IN 0..40 {
		CREATE type::record('t', $i) SET vec = [<float> $i + 0.1, <float> ($i * $i % 13) + 0.3];
	};";
	let queries = ["[3.3f, 1.7f]", "[17.9f, 6.2f]", "[39.1f, 12.4f]"];
	let (ds, session) = new_index_pass_test_ds().await?;
	let _scan = super::kind::test_sink::set_scan_batch(ds.id(), 4);
	execute_all(&ds, &session, SEED).await?;
	execute_all(&ds, &session, &format!("DEFINE INDEX ix ON t FIELDS vec {kind} CONCURRENTLY;"))
		.await?;
	let mut writes = 0;
	let passes = run_passes_to_end(&ds, &session, 2, |_| {
		writes += 1;
		Some(vector_write_during_build(writes))
	})
	.await?;
	assert!(passes > 2, "{kind}: the build ran in {passes} passes");
	assert_eq!(index_building_status(&ds, &session, "t", "ix").await?, "ready", "{kind}");

	let (reference, session_ref) = new_index_test_ds().await?;
	execute_all(&reference, &session_ref, SEED).await?;
	let sql: String = (1..=writes).map(vector_write_during_build).collect();
	execute_all(&reference, &session_ref, &sql).await?;
	execute_all(&reference, &session_ref, &format!("DEFINE INDEX ix ON t FIELDS vec {kind};"))
		.await?;
	for at in queries {
		let knn = format!("SELECT VALUE id FROM t WITH INDEX ix WHERE vec <|4,40|> {at}");
		let exact = format!("SELECT VALUE id FROM t WITH NOINDEX WHERE vec <|4,EUCLIDEAN|> {at}");
		let passed = ds.execute(&knn, &session, None).await?.remove(0).result?;
		let ordinary = reference.execute(&knn, &session_ref, None).await?.remove(0).result?;
		let scanned = ds.execute(&exact, &session, None).await?.remove(0).result?;
		assert_eq!(passed, ordinary, "{kind} at {at}: passes and an ordinary build agree");
		assert_eq!(passed, scanned, "{kind} at {at}: the index answers the exact search");
	}
	Ok(())
}

#[cfg(feature = "kv-mem")]
#[test_log::test(tokio::test)]
async fn passes_build_an_hnsw_index() -> Result<()> {
	assert_passes_build_a_vector_index("HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32").await
}

#[cfg(all(feature = "kv-mem", diskann))]
#[test_log::test(tokio::test)]
async fn passes_build_a_diskann_index() -> Result<()> {
	assert_passes_build_a_vector_index("DISKANN DIMENSION 2 DIST EUCLIDEAN TYPE F32").await
}

/// A `REBUILD INDEX` that moves an index to the current format stages its generation in its
/// transaction and, on a datastore that leaves builds to passes, leaves the committed generation
/// to them: they build it in the new layout, and the index answers as before.
#[cfg(feature = "kv-mem")]
#[test_log::test(tokio::test)]
async fn passes_build_a_format_moving_rebuild() -> Result<()> {
	let (ds, session) = new_index_pass_test_ds().await?;
	let _scan = super::kind::test_sink::set_scan_batch(ds.id(), 2);
	execute_all(
		&ds,
		&session,
		"DEFINE ANALYZER simple TOKENIZERS blank;
		 DEFINE INDEX ft ON t FIELDS content FULLTEXT ANALYZER simple BM25;",
	)
	.await?;
	run_passes_to_end(&ds, &session, 10, |_| None).await?;
	downgrade_index_format(&ds, "t", "ft", 0).await?;
	let creates: String =
		(1..=12).map(|i| format!("CREATE t:{i} SET content = 'bravo {i}' RETURN NONE;")).collect();
	execute_all(&ds, &session, &creates).await?;
	execute_all(&ds, &session, "REBUILD INDEX ft ON t;").await?;
	let (_, _, _, ix) = get_table_index(&ds, "t", "ft").await?;
	assert_eq!(ix.format_version, INDEX_FORMAT_VERSION, "the rebuild commits its format");
	assert_ne!(index_building_status(&ds, &session, "t", "ft").await?, "ready");
	let passes = run_passes_to_end(&ds, &session, 2, |_| None).await?;
	assert!(passes > 1, "{passes}");
	assert_eq!(index_building_status(&ds, &session, "t", "ft").await?, "ready");
	assert_eq!(planner_result_ids(&ds, BRAVO).await?, bravo_carriers(12));
	Ok(())
}

/// Inserts records `t:{from}` to `t:{from + count - 1}`, `n` cycling through seven values, ten
/// thousand to a statement.
#[cfg(feature = "kv-mem")]
async fn insert_numbered(
	ds: &Datastore,
	session: &Session,
	from: usize,
	count: usize,
) -> Result<()> {
	let mut at = from;
	while at < from + count {
		let to = (at + 10_000).min(from + count);
		execute_all(
			ds,
			session,
			&format!(
				"INSERT INTO t (array::range({at}, {to}).map(|$i| {{ id: $i, n: $i % 7 }})) RETURN NONE;"
			),
		)
		.await?;
		at = to;
	}
	Ok(())
}

/// The time a pass spends on the steps its budget does not bound, at a million records: the
/// wipe that starts a restarted scan, as a `REBUILD INDEX` of a built index runs it, and a
/// `COUNT` index's tail over the records created past the scan's end while it ran. A
/// measurement, run by hand: `cargo test --profile devopt ... -- --ignored --nocapture`.
#[cfg(feature = "kv-mem")]
#[ignore]
#[test_log::test(tokio::test)]
async fn measure_the_steps_a_pass_budget_does_not_bound() -> Result<()> {
	const RECORDS: usize = 1_000_000;
	const PAST_END: usize = 100_000;
	let (ds, session) = new_index_pass_test_ds().await?;
	let started = Instant::now();
	insert_numbered(&ds, &session, 0, RECORDS).await?;
	eprintln!("inserted {RECORDS} records in {:?}", started.elapsed());

	// The wipe: a REBUILD of a built index restarts its scan, wiping the entries first.
	execute_all(&ds, &session, "DEFINE INDEX ix ON t FIELDS n CONCURRENTLY;").await?;
	let started = Instant::now();
	run_passes_to_end(&ds, &session, usize::MAX, |_| None).await?;
	eprintln!("built ix over {RECORDS} records in {:?}", started.elapsed());
	execute_all(&ds, &session, "REBUILD INDEX ix ON t;").await?;
	let started = Instant::now();
	let first = ds.run_index_builds(1).await?;
	eprintln!(
		"REBUILD: first pass (wipe of {RECORDS} entries + one batch) took {:?}: {first:?}",
		started.elapsed()
	);
	run_passes_to_end(&ds, &session, usize::MAX, |_| None).await?;
	execute_all(&ds, &session, "REMOVE INDEX ix ON t;").await?;

	// The COUNT tail: records created past the scan's end while it runs.
	execute_all(&ds, &session, "DEFINE INDEX cx ON t COUNT CONCURRENTLY;").await?;
	let first = ds.run_index_builds(1).await?;
	assert!(first.more);
	insert_numbered(&ds, &session, 10 * RECORDS, PAST_END).await?;
	let mut slowest = Duration::ZERO;
	let mut passes = 0;
	loop {
		let started = Instant::now();
		let run = ds.run_index_builds(64).await?;
		slowest = slowest.max(started.elapsed());
		passes += 1;
		if !run.more {
			break;
		}
	}
	eprintln!(
		"COUNT over {RECORDS} records, {PAST_END} created past the scan's end: {passes} passes \
		 of 64 batches, slowest {slowest:?}"
	);
	let count = query_json(&ds, &session, "SELECT VALUE count() FROM t GROUP ALL").await?;
	assert_eq!(count, serde_json::json!([RECORDS + PAST_END]));
	Ok(())
}

/// A pass wipes a rebuilt index's entries a page to a batch of its budget, so a `REBUILD INDEX`
/// of a large index starts with passes no longer than any other, and later passes carry the wipe
/// on and rebuild the index whole.
#[cfg(feature = "kv-mem")]
#[test_log::test(tokio::test)]
async fn a_rebuild_wipes_its_index_a_page_to_a_batch() -> Result<()> {
	let (ds, session) = new_index_pass_test_ds().await?;
	insert_numbered(&ds, &session, 0, 25_000).await?;
	execute_all(&ds, &session, "DEFINE INDEX ix ON t FIELDS n;").await?;
	run_passes_to_end(&ds, &session, usize::MAX, |_| None).await?;
	let (ns, db, table, ix) = get_table_index(&ds, "t", "ix").await?;
	let held = || index_prefix_key_count(&ds, ns, db, &table, ix.index_id);
	let built = held().await?;
	assert!(built >= 25_000, "{built}");
	let first = index_entry_keys(&ds, "t", "ix").await?;

	execute_all(&ds, &session, "REBUILD INDEX ix ON t;").await?;
	let mut left = built;
	for _ in 0..2 {
		let run = ds.run_index_builds(1).await?;
		assert_eq!((run.batches, run.more), (1, true), "{run:?}");
		let now = held().await?;
		// A page reads 10,000 keys and keeps the compaction generations among them.
		assert!((left - 10_000..=left - 9_990).contains(&now), "{left} -> {now}");
		left = now;
	}
	let passes = run_passes_to_end(&ds, &session, 4, |_| None).await?;
	assert!(passes > 1, "{passes}");
	assert_eq!(index_building_status(&ds, &session, "t", "ix").await?, "ready");
	assert_eq!(held().await?, built, "the rebuilt index holds as many keys");
	assert_eq!(index_entry_keys(&ds, "t", "ix").await?, first);
	Ok(())
}

/// `INFO FOR INDEX` reports `building.compacting`, `building.queued`, `building.armed_shards`,
/// and `building.active` independently across pending writes, a write transaction's own
/// buffered shards, compaction passes running on this node, and standalone `/!ic` queue
/// records.
#[cfg(feature = "kv-mem")]
#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn info_for_index_reports_active_queued_compacting_and_armed_shards() -> Result<()> {
	let (ds, session) = new_index_test_ds().await?;
	execute_all(&ds, &session, "DEFINE INDEX ix ON t FIELDS v HNSW DIMENSION 2;").await?;
	for i in 0..16 {
		execute_all(&ds, &session, &format!("CREATE t:{i} SET v = [{i}.0, 1.0];")).await?;
	}
	let (ns, db, table, ix) = get_table_index(&ds, "t", "ix").await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);

	let before = index_building_json(&ds, &session, "t", "ix").await?;
	assert_eq!(before["status"], "ready");
	assert_eq!(before["compacting"], true);
	assert_eq!(before["queued"], true);
	let stored_shards = before["armed_shards"].as_u64().unwrap_or(0);
	assert!(stored_shards > 1, "the writes must arm more than one stored shard: {before}");
	assert_eq!(before["active"], false);

	// A write transaction that wrote into the index counts the one shard it buffered, not
	// the stored ones, before its commit arms the stored guard.
	let results = ds
		.execute(
			"BEGIN; CREATE t:100 SET v = [100.0, 1.0]; \
			 (INFO FOR INDEX ix ON t).building; COMMIT;",
			&session,
			None,
		)
		.await?;
	// `BEGIN`, `CREATE`, the `INFO` and `COMMIT` each answer.
	let in_tx = results.into_iter().nth(2).expect("the INFO result").result?.into_json_value();
	assert_eq!(in_tx["compacting"], true, "{in_tx}");
	assert_eq!(in_tx["armed_shards"], 1, "{in_tx}");
	assert_eq!(in_tx["queued"], true, "{in_tx}");

	// Holding the per-index materialisation lock is not a pass; a pass entered on this node
	// is, until it ends.
	let builder = ds.index_builder();
	let lock = builder.materialise_lock(&ikb);
	let held = lock.lock().await;
	assert_eq!(index_building_json(&ds, &session, "t", "ix").await?["active"], false);
	let pass = builder.enter_pass(&ikb);
	assert_eq!(index_building_json(&ds, &session, "t", "ix").await?["active"], true);
	drop(pass);
	assert_eq!(index_building_json(&ds, &session, "t", "ix").await?["active"], false);
	drop(held);
	drop(lock);

	let (iterations, errors) = Datastore::index_compaction(
		Arc::clone(&ds),
		Duration::from_secs(1),
		CancellationToken::new(),
	)
	.await?;
	assert_eq!(errors, 0);
	assert!(iterations >= 1);
	let after = index_building_json(&ds, &session, "t", "ix").await?;
	assert_eq!(after["status"], "ready");
	assert_eq!(after["compacting"], false);
	assert_eq!(after["queued"], false);
	assert_eq!(after["armed_shards"], 0);
	assert_eq!(after["active"], false);

	// A standalone `/!ic` record with no armed guard shards reports `queued == true` while
	// `compacting` and `armed_shards` remain `false` / `0`.
	let tx = ds.transaction(TransactionType::Write).await?;
	tx.buffer_compaction_trigger(ns, db, &table, ix.index_id, ds.id());
	tx.commit().await?;
	let queued_only = index_building_json(&ds, &session, "t", "ix").await?;
	assert_eq!(queued_only["compacting"], false);
	assert_eq!(queued_only["queued"], true);
	assert_eq!(queued_only["armed_shards"], 0);
	assert_eq!(queued_only["active"], false);
	Ok(())
}

/// A build whose initial scan runs on several tasks reads a plain-field column
/// off each stored record in place, and falls back to the decoded record where
/// the record does not hold the path through objects alone. Either way the
/// index holds exactly the entries a serial scan builds, and the ones the
/// writes of the same records build into an index defined before them.
#[cfg(feature = "kv-mem")]
#[test_log::test(tokio::test)]
async fn a_parallel_scan_indexes_plain_fields_as_a_serial_scan_does() -> Result<()> {
	const SEED: &str = "
		CREATE |t:1..=300| SET a = rand::int(0, 9), n = { b: 'x', c: { d: 1 } };
		CREATE t:missing SET n = { c: { d: 2 } };
		CREATE t:nulls SET a = NULL, n = NULL;
		CREATE t:array SET a = [1, 2], n = [{ b: 'y' }, { b: 'z' }];
		CREATE t:scalar SET a = 'text', n = 'not an object';
		CREATE t:deep SET a = { v: 1 }, n = { b: { e: [1, 2] }, c: { d: 3 } };
		CREATE t:link SET a = 5, n = t:deep;
	";
	const INDEX: &str = "ix ON t FIELDS a, n.b, n.c.d";
	let parallel = |tasks: &str| {
		Datastore::builder()
			.without_maintenance_tasks()
			.with_config(
				surrealdb_cnf::ConfigMap::empty().with_key_value("index_build_parallelism", tasks),
			)
			.build_with_path("memory")
	};
	let session = Session::owner().with_ns("test").with_db("test");
	let mut built = Vec::new();
	for (tasks, define_first) in [("4", false), ("serial", false), ("4", true)] {
		let ds = parallel(tasks).await?;
		let tx = ds.transaction(TransactionType::Write).await?;
		tx.ensure_ns_db(None, "test", "test").await?;
		tx.commit().await?;
		if define_first {
			execute_all(&ds, &session, &format!("DEFINE INDEX {INDEX}")).await?;
		}
		execute_all(&ds, &session, SEED).await?;
		// The records are the same in every datastore.
		execute_all(&ds, &session, "UPDATE |t:1..=300| SET a = <int> record::id(id) % 10").await?;
		if !define_first {
			execute_all(&ds, &session, &format!("DEFINE INDEX {INDEX} CONCURRENTLY")).await?;
			wait_for_index_ready(&ds, &session, "t", "ix").await?;
		}
		built.push(index_entry_keys(&ds, "t", "ix").await?);
	}
	assert!(built[0].len() > 300, "{}", built[0].len());
	assert_eq!(built[0], built[1], "a parallel scan builds what a serial scan does");
	assert_eq!(built[0], built[2], "a build holds what the writes of its records do");
	Ok(())
}
