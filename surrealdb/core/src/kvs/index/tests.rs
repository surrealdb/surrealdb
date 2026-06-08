use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use anyhow::Result;
use chrono::Utc;
use tokio::time::{sleep, timeout};
use uuid::Uuid;
use web_time::Instant;

use super::builder::{Building, IndexKey};
use super::state::{build_owner_expired, report_status_from_phase};
use super::*;
use crate::catalog::providers::{
	CatalogProvider, DatabaseProvider, NamespaceProvider, TableProvider,
};
use crate::catalog::{DatabaseId, Index, IndexDefinition, IndexId, NamespaceId};
use crate::dbs::Session;
use crate::err::Error;
use crate::idx::IndexKeyBase;
use crate::key::index::all as index_all;
use crate::kvs::LockType::Optimistic;
use crate::kvs::testing::{
	NonRetryableErrorSite, RetryableConflictGuard, RetryableConflictSite,
	inject_non_retryable_error, inject_retryable_conflict, inject_retryable_conflicts,
	retryable_conflict_count,
};
use crate::kvs::tx::{
	CachedIndexBuildReservationKey, CachedIndexBuildReservationLookup, IndexBuildReservationRelease,
};
use crate::kvs::{
	Datastore, KVKey, KVValue, Key, TransactionType, is_retryable_transaction_conflict,
};
use crate::val::{RecordId, RecordIdKey, TableName, Value};

const REPEATED_RETRY_CONFLICTS: usize = 1000;

async fn new_index_test_ds() -> Result<(Datastore, Session)> {
	let ds = Datastore::new("memory").await?;
	let session = Session::owner().with_ns("test").with_db("test");
	let tx = ds.transaction(TransactionType::Write, Optimistic).await?;
	tx.ensure_ns_db(None, "test", "test").await?;
	tx.commit().await?;
	Ok((ds, session))
}

#[cfg(feature = "kv-mem")]
async fn new_distributed_index_test_ds() -> Result<(Datastore, Datastore, Session)> {
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
	let tx = ds.transaction(TransactionType::Read, Optimistic).await?;
	let state = catch!(tx, tx.get(&ikb.new_bs_key(), None).await)
		.ok_or_else(|| anyhow::anyhow!("durable build state should exist"))?;
	tx.cancel().await?;
	Ok(state)
}

async fn set_durable_build_state(
	ds: &Datastore,
	ikb: &IndexKeyBase,
	state: IndexBuildState,
) -> Result<()> {
	let tx = ds.transaction(TransactionType::Write, Optimistic).await?;
	tx.set(&ikb.new_bs_key(), &state).await?;
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
	}
}

async fn durable_build_state_exists(ds: &Datastore, ikb: &IndexKeyBase) -> Result<bool> {
	let tx = ds.transaction(TransactionType::Read, Optimistic).await?;
	let state: Option<IndexBuildState> = catch!(tx, tx.get(&ikb.new_bs_key(), None).await);
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
	let tx = ds.transaction(TransactionType::Read, Optimistic).await?;
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
	ds: Datastore,
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
	let tx = ds.transaction(TransactionType::Read, Optimistic).await?;
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
	let tx = ds.transaction(TransactionType::Read, Optimistic).await?;
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
	let tx = ds.transaction(TransactionType::Read, Optimistic).await?;
	let key = index_all::new(ns, db, table, ix);
	let keys: Vec<(Key, Vec<u8>)> = catch!(tx, tx.getp(&key, None).await);
	tx.cancel().await?;
	Ok(keys.len())
}

async fn seed_durable_queue_generation(
	ds: &Datastore,
	ikb: &IndexKeyBase,
	generation: BuildGeneration,
) -> Result<()> {
	let tx = ds.transaction(TransactionType::Write, Optimistic).await?;
	let id = RecordIdKey::from(format!("stale-{generation}"));
	let ticket = generation;
	let mutation_seq = 0;
	tx.set(
		&ikb.new_bg_key(generation, ticket, mutation_seq),
		&Appending::new(None, None, id.clone()),
	)
	.await?;
	tx.set(
		&ikb.new_bp_key(generation, id),
		&PrimaryAppendingTicket {
			ticket,
			mutation_seq,
		},
	)
	.await?;
	tx.set(
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
	let tx = ds.transaction(TransactionType::Write, Optimistic).await?;
	tx.set(
		&ikb.new_bs_key(),
		&durable_build_state_for_phase(IndexBuildPhase::Building, 1, Some(ds.id())),
	)
	.await?;
	let id = RecordIdKey::from("orphan".to_owned());
	let ticket = 1;
	let mutation_seq = 0;
	tx.set(&ikb.new_bg_key(1, ticket, mutation_seq), &Appending::new(None, None, id.clone()))
		.await?;
	tx.set(
		&ikb.new_bp_key(1, id),
		&PrimaryAppendingTicket {
			ticket,
			mutation_seq,
		},
	)
	.await?;
	tx.set(
		&ikb.new_br_key(1, ticket),
		&IndexBuildReservation {
			node: ds.id(),
			expires_at: Utc::now() + chrono::Duration::seconds(BUILD_RESERVATION_TTL_SECS),
		},
	)
	.await?;
	let mut index_data_key = index_all::new(ns, db, table, ix).encode_key()?;
	index_data_key.extend_from_slice(b"orphan");
	tx.set(&index_data_key, &b"orphan".to_vec()).await?;
	tx.commit().await?;
	Ok(())
}

async fn durable_queue_generation_count(
	ds: &Datastore,
	ikb: &IndexKeyBase,
	generation: BuildGeneration,
) -> Result<usize> {
	let tx = ds.transaction(TransactionType::Read, Optimistic).await?;
	let bg = catch!(tx, tx.keys(ikb.new_bg_range(generation)?, u32::MAX, 0, None).await);
	let bp = catch!(tx, tx.keys(ikb.new_bp_range(generation)?, u32::MAX, 0, None).await);
	let br = catch!(tx, tx.keys(ikb.new_br_range(generation)?, u32::MAX, 0, None).await);
	tx.cancel().await?;
	Ok(bg.len() + bp.len() + br.len())
}

async fn durable_queue_all_generations_count(ds: &Datastore, ikb: &IndexKeyBase) -> Result<usize> {
	let tx = ds.transaction(TransactionType::Read, Optimistic).await?;
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

	let tx = ds.transaction(TransactionType::Read, Optimistic).await?;
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

	let tx = ds.transaction(TransactionType::Read, Optimistic).await?;
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
	let tx = ds.transaction(TransactionType::Write, Optimistic).await?;
	tx.set(
		&online_ikb.new_bs_key(),
		&durable_build_state_for_phase(IndexBuildPhase::Online, 1, None),
	)
	.await?;
	tx.set(
		&building_ikb.new_bs_key(),
		&durable_build_state_for_phase(IndexBuildPhase::Building, 1, Some(ds.id())),
	)
	.await?;
	tx.del(&legacy_ikb.new_bs_key()).await?;
	tx.commit().await?;

	execute_all(&ds, &session, "DEFINE INDEX OVERWRITE stale ON user FIELDS score").await?;

	let tx = ds.transaction(TransactionType::Read, Optimistic).await?;
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

	let tx = ds.transaction(TransactionType::Write, Optimistic).await?;
	for ix in [&one_ix, &two_ix, &three_ix] {
		let ikb = IndexKeyBase::new(ns, db, table.clone(), ix.index_id);
		tx.set(&ikb.new_bs_key(), &durable_build_state_for_phase(IndexBuildPhase::Online, 1, None))
			.await?;
	}
	tx.commit().await?;

	let tx = ds.transaction(TransactionType::Read, Optimistic).await?;
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

	let tx = Arc::new(ds.transaction(TransactionType::Write, Optimistic).await?);
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

	let tx = ds.transaction(TransactionType::Read, Optimistic).await?;
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

	let tx = ds.transaction(TransactionType::Read, Optimistic).await?;
	assert!(build.compaction_write_still_owns_index(&tx, generation).await?);
	tx.cancel().await?;

	execute_all(&ds, &session, "REMOVE INDEX test ON user").await?;

	let tx = ds.transaction(TransactionType::Read, Optimistic).await?;
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

	let tx = ds.transaction(TransactionType::Read, Optimistic).await?;
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

	let tx = ds.transaction(TransactionType::Read, Optimistic).await?;
	assert!(build.compaction_write_still_owns_index(&tx, generation).await?);
	tx.cancel().await?;

	execute_all(&ds, &session, "REBUILD INDEX test ON user").await?;

	let (_, _, _, rebuilt_ix) = get_table_index(&ds, "user", "test").await?;
	assert_eq!(rebuilt_ix.index_id, ix.index_id);
	assert_eq!(rebuilt_ix.name, ix.name);

	let rebuilt_state = durable_build_state(&ds, &ikb).await?;
	assert_eq!(rebuilt_state.generation, generation.saturating_add(1));
	assert_eq!(rebuilt_state.phase, IndexBuildPhase::Online);

	let tx = ds.transaction(TransactionType::Read, Optimistic).await?;
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
	let tx = ds.transaction(TransactionType::Write, Optimistic).await?;
	tx.set(
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
		},
	)
	.await?;
	tx.commit().await?;

	let tx = ds.transaction(TransactionType::Read, Optimistic).await?;
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
		let tx = ds.transaction(TransactionType::Write, Optimistic).await?;
		let ns = tx.get_ns_by_name("test", None).await?.expect("namespace should exist");
		let db = tx.get_db_by_name("test", "test", None).await?.expect("database should exist");
		let table = tx
			.get_tb(ns.namespace_id, db.database_id, &table_name, None)
			.await?
			.expect("table should exist");
		let index = Arc::new(IndexDefinition {
			index_id: IndexId(1),
			name: "test".into(),
			table_name: table_name.clone(),
			cols: Vec::new(),
			index: Index::Count(None),
			comment: None,
			prepare_remove: false,
		});
		tx.put_tb_index(ns.namespace_id, db.database_id, &table.name, &index).await?;
		tx.commit().await?;
		(ns.namespace_id, db.database_id, table.table_id, index)
	};

	let index_key = Arc::new(IndexKey::new(ns_id, db_id, &table_name, index.index_id));
	let opt = ds.setup_options(&session);
	let mut ctx = ds.setup_ctx()?;
	let read_tx = Arc::new(ds.transaction(TransactionType::Read, Optimistic).await?);
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
		let tx = ds.transaction(TransactionType::Write, Optimistic).await?;
		tx.set(
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
	};
	let tx = ds_a.transaction(TransactionType::Write, Optimistic).await?;
	tx.set(&ikb.new_bs_key(), &stale_state).await?;
	tx.commit().await?;

	for record in ["one", "two", "three"] {
		execute_all(
			&ds_b,
			&session,
			&format!("CREATE user:{record} SET email = '{record}@example.com' RETURN NONE"),
		)
		.await?;
	}

	let tx = ds_a.transaction(TransactionType::Read, Optimistic).await?;
	let admitted_state: IndexBuildState =
		tx.get(&ikb.new_bs_key(), None).await?.expect("build state should exist");
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

	let tx = ds_a.transaction(TransactionType::Read, Optimistic).await?;
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

	let tx = ds_a.transaction(TransactionType::Read, Optimistic).await?;
	let taken_over: IndexBuildState =
		tx.get(&ikb.new_bs_key(), None).await?.expect("build state should exist");
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
	};
	let tx = ds.transaction(TransactionType::Write, Optimistic).await?;
	tx.set(&ikb.new_bs_key(), &building).await?;
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
	let tx = ds.transaction(TransactionType::Read, Optimistic).await?;
	let after: IndexBuildState =
		tx.get(&ikb.new_bs_key(), None).await?.expect("build state should exist");
	tx.cancel().await?;
	assert_eq!(
		after.next_ticket, 1,
		"a single user transaction must consume exactly one durable ticket regardless of mutation count"
	);

	// Five `!bg` entries, one per mutation, all sharing the same `(generation, ticket)`.
	let tx = ds.transaction(TransactionType::Read, Optimistic).await?;
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
	};
	let tx = ds.transaction(TransactionType::Write, Optimistic).await?;
	tx.set(&ikb.new_bs_key(), &building).await?;
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

	let tx = ds.transaction(TransactionType::Read, Optimistic).await?;
	let after: IndexBuildState =
		tx.get(&ikb.new_bs_key(), None).await?.expect("build state should exist");
	let bg_keys = tx.keys(ikb.new_bg_range(generation)?, u32::MAX, 0, None).await?;
	let bp_keys = tx.keys(ikb.new_bp_range(generation)?, u32::MAX, 0, None).await?;
	let br_keys = tx.keys(ikb.new_br_range(generation)?, u32::MAX, 0, None).await?;
	tx.cancel().await?;
	assert_eq!(after.next_ticket, 1, "cancelled batch still consumes one durable ticket");
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
	let tx = ds.transaction(TransactionType::Write, Optimistic).await?;
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
	let tx = ds.transaction(TransactionType::Write, Optimistic).await?;
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
	let downcast =
		err.downcast_ref::<Error>().expect("error should be the typed IndexingBuildingCancelled");
	assert!(
		matches!(downcast, Error::IndexingBuildingCancelled { .. }),
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
	// generation rotation, an Online/Error transition, or vanished build
	// state must abort the user transaction with `IndexingBuildingCancelled`
	// — otherwise later mutations write `!bg` against a generation no
	// builder will replay.
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

	// Error phase — same.
	seed_build_state(&ds, &ikb, IndexBuildPhase::Error, 1).await?;
	let err = run_recheck_cached_admission(&ds, &ikb, ix.as_ref(), 1)
		.await?
		.expect_err("error phase must abort cached admission");
	assert!(
		matches!(err.downcast_ref::<Error>(), Some(Error::IndexingBuildingCancelled { .. })),
		"unexpected error: {err}"
	);

	// Missing state — recheck aborts.
	let tx = ds.transaction(TransactionType::Write, Optimistic).await?;
	tx.del(&ikb.new_bs_key()).await?;
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
	};
	if matches!(phase, IndexBuildPhase::Error) {
		state.error = Some("seeded test failure".to_string());
		state.report_status = Some(IndexBuildReportStatus::Error);
	}
	let tx = ds.transaction(TransactionType::Write, Optimistic).await?;
	tx.set(&ikb.new_bs_key(), &state).await?;
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
	let tx = ds.transaction(TransactionType::Read, Optimistic).await?;
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
	// writers on *other* nodes before wiping durable build state. Without the
	// drain, the wipe destroys the writer's anchor and the new build's
	// initial scan can start before the writer's commit, missing main-table
	// writes.
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
	};
	let seed_tx = ds_a.transaction(TransactionType::Write, Optimistic).await?;
	seed_tx.set(&ikb.new_bs_key(), &errored).await?;
	let reservation = IndexBuildReservation {
		node: ds_a.id(),
		expires_at: Utc::now() + chrono::Duration::seconds(BUILD_RESERVATION_TTL_SECS),
	};
	let br_key = ikb.new_br_key(1, 0);
	seed_tx.set(&br_key, &reservation).await?;
	seed_tx.commit().await?;

	// `ds_b` is the would-be takeover node. Its drain must block on ds_a's
	// live `!br`.
	let building = new_building_for_index(&ds_b, &session, ns, db, &table, Arc::clone(&ix)).await?;
	let timeout_result =
		timeout(Duration::from_millis(200), building.wait_for_prior_generation_reservations())
			.await;
	assert!(
		timeout_result.is_err(),
		"drain must block while another node's !br is alive in durable membership"
	);

	// Simulate ds_a's deferred release firing (e.g. user transaction committed
	// or cancelled, removing its `!br`).
	let release_tx = ds_a.transaction(TransactionType::Write, Optimistic).await?;
	release_tx.del(&br_key).await?;
	release_tx.commit().await?;

	// Drain should now return promptly.
	timeout(Duration::from_secs(5), building.wait_for_prior_generation_reservations())
		.await
		.expect("drain must complete after !br is removed")?;

	// Takeover proceeds with generation 2 and leaves no stranded reservations.
	let acquired =
		building.acquire_build_state().await?.expect("takeover should succeed after drain");
	assert_eq!(acquired.generation, 2);
	assert!(matches!(acquired.phase, IndexBuildPhase::Building));
	let tx = ds_a.transaction(TransactionType::Read, Optimistic).await?;
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
	let seed_tx = ds.transaction(TransactionType::Write, Optimistic).await?;
	seed_tx.set(&br_key, &reservation).await?;
	seed_tx.commit().await?;

	let building = new_building_for_index(&ds, &session, ns, db, &table, Arc::clone(&ix)).await?;
	timeout(Duration::from_secs(5), building.wait_for_prior_generation_reservations())
		.await
		.expect("drain must complete promptly for a dead writer's reservation")?;

	let tx = ds.transaction(TransactionType::Read, Optimistic).await?;
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
	let tx = ds_a.transaction(TransactionType::Write, Optimistic).await?;
	tx.set(
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
	let tx = ds_a.transaction(TransactionType::Write, Optimistic).await?;
	let mut state: IndexBuildState =
		tx.get(&ikb.new_bs_key(), None).await?.expect("build state should exist");
	state.updated_at = expired;
	state.owner_heartbeat_at = Some(expired);
	tx.set(&ikb.new_bs_key(), &state).await?;
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
	let mut results = ds_b
		.execute(
			"CREATE user:three SET account = 'tesla', email = 'three@surrealdb.com' RETURN NONE",
			&session,
			None,
		)
		.await?;
	let write_error =
		results.remove(0).result.expect_err("write against errored durable index should fail");
	let write_error = write_error.to_string();
	assert!(
		write_error.contains(error_reason),
		"write error should include durable build reason {error_reason:?}, got {write_error:?}"
	);
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

	let seed_tx = ds.transaction(TransactionType::Write, Optimistic).await?;
	seed_tx.set(&br, &reservation).await?;
	seed_tx.set(&"commit-conflict", &b"initial".to_vec()).await?;
	seed_tx.commit().await?;

	let user_tx = ds.transaction(TransactionType::Write, Optimistic).await?;
	user_tx.set(&"commit-conflict", &b"user-write".to_vec()).await?;
	user_tx
		.register_index_build_reservation_release(IndexBuildReservationRelease::new(
			ds.transaction_factory().clone(),
			ds.sequences().clone(),
			ds.id(),
			br_key,
			br_val,
		))
		.await;

	let conflicting_tx = ds.transaction(TransactionType::Write, Optimistic).await?;
	conflicting_tx.set(&"commit-conflict", &b"conflicting-write".to_vec()).await?;
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

	let user_tx = ds.transaction(TransactionType::Write, Optimistic).await?;
	user_tx.set(&"commit-conflict", &b"user-write".to_vec()).await?;
	let ctx = ds.setup_ctx()?;
	let builder = ctx.get_index_builder().expect("index builder should exist").clone();
	user_tx
		.register_uncommitted_index_build_cleanup(
			builder.clone(),
			builder.transaction_factory(),
			ns,
			db,
			table.clone(),
			ix,
		)
		.await;

	let conflicting_tx = ds.transaction(TransactionType::Write, Optimistic).await?;
	conflicting_tx.set(&"commit-conflict", &b"conflicting-write".to_vec()).await?;
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

	let seed_tx = ds.transaction(TransactionType::Write, Optimistic).await?;
	seed_tx.set(&br, &reservation).await?;
	seed_tx.commit().await?;

	let user_tx = ds.transaction(TransactionType::Read, Optimistic).await?;
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
		Value::None.into(),
		current.into(),
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
		matches!(err.downcast_ref(), Some(Error::Kvs(crate::kvs::Error::TransactionReadonly))),
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
