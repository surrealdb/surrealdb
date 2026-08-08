use std::borrow::Cow;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use anyhow::Result;
use chrono::Utc;
use surrealdb_kvs::TransactionType;
use tokio::time::{sleep, timeout};
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
use crate::key::schema::{
	DocKeyPrefix, DocLookupPrefix, DocPendingKey, DocPendingPrefix, EntryPrefix, IdxRoot,
	IndexCountKey, IndexCountPrefix, RecordPrefix,
};
use crate::key::{KVKey, KVKeyDecode, KVSubspace, KVValue, Key};
use crate::kvs::index::CleanUncommittedBuild;
use crate::kvs::testing::{
	NonRetryableErrorSite, RetryableConflictGuard, RetryableConflictSite,
	inject_non_retryable_error, inject_retryable_conflict, inject_retryable_conflicts,
	retryable_conflict_count,
};
use crate::kvs::tx::{
	CachedIndexBuildReservationKey, CachedIndexBuildReservationLookup, IndexBuildReservationRelease,
};
use crate::kvs::{Datastore, DatastoreError, is_retryable_transaction_conflict, storage_error};
use crate::val::{RecordId, RecordIdKey, TableName, Value};

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
		builder: _,
	} = start_paused_remove_build().await?;
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	seed_durable_queue_generation(&ds, &ikb, 99).await?;
	assert_eq!(durable_queue_all_generations_count(&ds, &ikb).await?, 3);

	execute_all_retrying_conflicts(&ds, &session, sql).await?;
	wait_for_no_local_builder(&ds, ns, db, &table, ix.index_id).await?;
	drop(guard);
	sleep(Duration::from_millis(200)).await;

	assert!(!durable_build_state_exists(&ds, &ikb).await?);
	assert_eq!(durable_queue_all_generations_count(&ds, &ikb).await?, 0);
	assert_eq!(index_prefix_key_count(&ds, ns, db, &table, ix.index_id).await?, 0);
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
	let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
	assert!(local_builder_for_key(&ds, ns, db, &table, ix.index_id).await?.is_some());

	execute_all_retrying_conflicts(&ds, &session, "REMOVE INDEX test ON user").await?;
	wait_for_no_local_builder(&ds, ns, db, &table, ix.index_id).await?;
	drop(guard);
	sleep(Duration::from_millis(200)).await;

	assert!(!durable_build_state_exists(&ds, &ikb).await?);
	assert_eq!(index_prefix_key_count(&ds, ns, db, &table, ix.index_id).await?, 0);
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
	assert!(local_builder_for_key(&ds, ns, db, &table, old_ix.index_id).await?.is_some());

	execute_all_retrying_conflicts(
		&ds,
		&session,
		"DEFINE INDEX OVERWRITE test ON user FIELDS account CONCURRENTLY",
	)
	.await?;
	wait_for_no_local_builder(&ds, ns, db, &table, old_ix.index_id).await?;
	drop(guard);
	wait_for_index_ready(&ds, &session, "user", "test").await?;

	let (_, _, _, new_ix) = get_table_index(&ds, "user", "test").await?;
	assert_ne!(new_ix.index_id, old_ix.index_id);
	assert_eq!(index_prefix_key_count(&ds, ns, db, &table, old_ix.index_id).await?, 0);
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

	let tx = ds.transaction(TransactionType::Read).await?;
	assert!(build.compaction_write_still_owns_index(&tx, generation).await?);
	tx.cancel().await?;

	execute_all(&ds, &session, "REMOVE INDEX test ON user").await?;

	let tx = ds.transaction(TransactionType::Read).await?;
	assert!(
		!build.compaction_write_still_owns_index(&tx, generation).await?,
		"retired indexes must not accept post-online builder compaction writes"
	);
	tx.cancel().await?;
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

	let tx = ds.transaction(TransactionType::Read).await?;
	assert!(build.compaction_write_still_owns_index(&tx, generation).await?);
	tx.cancel().await?;

	execute_all(&ds, &session, "REBUILD INDEX test ON user").await?;

	let (_, _, _, rebuilt_ix) = get_table_index(&ds, "user", "test").await?;
	assert_eq!(rebuilt_ix.index_id, ix.index_id);
	assert_eq!(rebuilt_ix.name, ix.name);

	let rebuilt_state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(rebuilt_state.generation, generation.saturating_add(1));
	assert_eq!(rebuilt_state.phase, IndexBuildPhase::Online);

	let tx = ds.transaction(TransactionType::Read).await?;
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
	run_recheck_cached_admission(&ds, &ikb, ix.as_ref(), 1)
		.await?
		.expect("recheck on matching Building state should succeed");

	// Closing is still queueable.
	seed_build_state(&ds, &ikb, IndexBuildPhase::Closing, 1).await?;
	run_recheck_cached_admission(&ds, &ikb, ix.as_ref(), 1)
		.await?
		.expect("recheck on matching Closing state should succeed");

	// Generation rotation — recheck aborts.
	seed_build_state(&ds, &ikb, IndexBuildPhase::Building, 2).await?;
	let err = run_recheck_cached_admission(&ds, &ikb, ix.as_ref(), 1)
		.await?
		.expect_err("generation mismatch must abort cached admission");
	assert!(err.to_string().contains("generation"), "unexpected error: {err}");

	// Online phase — cached writers cannot trust the cached ticket.
	seed_build_state(&ds, &ikb, IndexBuildPhase::Online, 1).await?;
	let err = run_recheck_cached_admission(&ds, &ikb, ix.as_ref(), 1)
		.await?
		.expect_err("online phase must abort cached admission");
	assert!(err.to_string().contains("online"), "unexpected error: {err}");

	// Error phase — keeps queueing: a failed build must not abort user
	// writes. The queued mutations die with the errored generation's wipe
	// and the records are rescanned by the recovering `REBUILD INDEX`.
	seed_build_state(&ds, &ikb, IndexBuildPhase::Error, 1).await?;
	run_recheck_cached_admission(&ds, &ikb, ix.as_ref(), 1)
		.await?
		.expect("recheck on matching Error state should keep queueing");

	// Missing state — recheck aborts.
	let tx = ds.transaction(TransactionType::Write).await?;
	tx.del_key(&ikb.new_bs_key()).await?;
	tx.commit().await?;
	let err = run_recheck_cached_admission(&ds, &ikb, ix.as_ref(), 1)
		.await?
		.expect_err("missing build state must abort cached admission");
	assert!(err.to_string().contains("no longer exists"), "unexpected error: {err}");

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

#[cfg(feature = "kv-mem")]
async fn run_recheck_cached_admission(
	ds: &Datastore,
	ikb: &IndexKeyBase,
	ix: &IndexDefinition,
	cached_generation: u64,
) -> Result<Result<()>> {
	let tx = ds.transaction(TransactionType::Read).await?;
	let mut ctx = ds.setup_ctx()?;
	let tx = Arc::new(tx);
	ctx.set_transaction(Arc::clone(&tx));
	let frozen = ctx.freeze();
	let builder = frozen
		.get_index_builder()
		.expect("index builder should be available on the configured Datastore")
		.clone();
	let result = builder.recheck_cached_admission(&frozen, ikb, ix, cached_generation).await;
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

/// Smallest key strictly greater than every key sharing `prefix`. The `!di` /
/// `!dd` sigils end in an ASCII letter well below `0xFF`, so incrementing the
/// final byte yields a valid exclusive upper bound for a prefix scan.
/// Returns `(record→doc count, doc→record count)` for `table`'s shared doc-ID
/// space. Each prefix is scanned over its own range, so the `!dh`/`!ds` sequence
/// keys (which deliberately survive a purge) are excluded from the counts.
#[cfg(feature = "kv-mem")]
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
	{
		let tx = ds.transaction(TransactionType::Write).await?;
		let ns = tx.get_ns_by_name("test", None).await?.expect("namespace should exist");
		let db = tx.get_db_by_name("test", "test", None).await?.expect("database should exist");
		let tb: TableName = "t".into();
		let ix = tx
			.get_tb_index(ns.namespace_id, db.database_id, &tb, "idx_a", None)
			.await?
			.expect("index should exist");
		let mut old = (*ix).clone();
		old.format_version = BTREE_ENTRY_DOC_IDS_FORMAT_VERSION - 1;
		assert!(!old.uses_doc_ids());
		tx.put_tb_index(ns.namespace_id, db.database_id, &tb, &old).await?;
		// Bump the table definition so cached index lists are refreshed.
		let tb_def = tx.expect_tb(ns.namespace_id, db.database_id, &tb).await?;
		tx.put_tb("test", "test", &tb_def).await?;
		tx.commit().await?;
	}

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
	tx.set_key(&DocPendingKey::new(ns, db, Cow::Borrowed(table), Cow::Borrowed(id)), &()).await?;
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
/// mapping are asserted. The end-to-end wiring (a real replayed delete writing
/// the marker, and a real build completion running the sweep) is covered by
/// [`deferred_doc_id_reclaim_survives_across_builds`].
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
	let applied =
		IndexOperation::apply_hnsw_compaction(&ctx, ctx.get_index_stores(), &ikb, params, plan)
			.await?;
	tx.cancel().await?;
	assert!(!applied, "a plan prepared before the wipe must be rejected, not applied");

	// The pendings must survive for whoever compacts next.
	let tx = ds.transaction(TransactionType::Read).await?;
	let surviving = catch!(tx, tx.count(ikb.new_hr_range()?, None).await);
	tx.cancel().await?;
	assert_eq!(surviving, pendings.len(), "the rejected plan must leave every pending in place");
	Ok(())
}
