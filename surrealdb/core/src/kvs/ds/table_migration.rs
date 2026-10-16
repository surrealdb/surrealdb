//! The table migration job: rewrites the rows a table's migration steps have not reached, and
//! verifies whether the stored values of fields queued for verification conform to their
//! definitions.
//!
//! A statement that gives a table work writes a request at `/!tq`
//! ([`crate::key::table_migration::TableMigrationRequest`]) under an id of its own, in the
//! transaction that makes the change. The job keeps its progress through each table at `/!tm`
//! ([`TableMigrationState`]), a key no statement writes, so a statement and a page of the job never
//! write the same queue key and neither can fail the other's commit through it. The job holds one
//! lease ([`TaskLeaseType::TableMigration`]) for every table, visits each table with requests and
//! then each whose progress asks for work, and works through each table in laps of pages:
//!
//! - A page that finds requests folds them into the progress, in the order made, deletes them, and
//!   commits nothing else. A request committed after the page's snapshot is not deleted, so a later
//!   page folds it.
//! - Every other page runs in one write transaction. It reads the table's generations and steps
//!   from its own snapshot, then up to a page of records after the progress's cursor. A record
//!   stamped before the table's newest step is brought through the steps it has not passed, and,
//!   when the progress asks for a rewrite, stored again in the layout of the table's head
//!   generation, stamped with it. The page then advances the progress by compare-and-set on the
//!   state it read, so a page whose progress was replaced (by a node that took the lease over) or
//!   deleted (`REMOVE TABLE`) meanwhile commits nothing.
//! - At the end of a lap the rewrite is done if the table's newest step is the one the lap began
//!   under. Otherwise another lap starts. A step minted after the last page's snapshot comes with a
//!   request that page did not delete, so the next page starts a lap for it. The steps stay in
//!   their generations' descriptors: a row stamped before one, such as one a transaction that began
//!   earlier committed after the lap passed it, is replayed whenever it is read.
//! - A verification judges each record's value, brought through any pending step, without writing
//!   it, against the test a migration step of the field would apply: its type with the definitions
//!   beneath it folded in, so a key no definition declares beneath an object of a `SCHEMAFULL`
//!   table fails it, and its `ASSERT` over `$value` ([`crate::exec::migrate::verification_test`]).
//!   A field whose values all conformed has its claim made at the end of the lap, when that test is
//!   the field's whole definition, the table's claims can state it
//!   ([`surrealdb_catalog::birth_claims_of`]), the rewrite is done, and the lap began after every
//!   transaction that could still commit an unjudged row had ended: on a store this datastore
//!   writes alone, the job waits for the transactions older than the fold of the request before the
//!   lap's first page; on any other store a pass reports its counts and never makes a claim. A
//!   field that did not conform withdraws the table's claims if one is on the definition the lap
//!   judged.
//! - The check for undeclared keys judges each record, brought through any pending step, by its
//!   top-level keys: one other than `id`, `in` and `out` that no definition's path begins with is a
//!   key a write of the row refuses ([`crate::exec::migrate::holds_undeclared_key`]). The check
//!   states no claim. It reports the rows it counted, unless the lap judged every row and counted
//!   none, which leaves nothing to report. A lap that has passed rows under names the table no
//!   longer declares starts over.
//!
//! The job writes only record keys, never through `put_record` or `set_record`, which would store
//! the record through the transaction's record cache. It runs no events, live queries or
//! changefeed writes: a rewrite changes what a row costs to read, never what it reads as.

use std::borrow::Cow;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio_util::sync::CancellationToken;
use web_time::Instant;

use super::Datastore;
use crate::catalog::providers::{DatabaseProvider, NamespaceProvider, TableProvider};
use crate::catalog::{
	Conformance, DatabaseId, FieldCandidate, Generation, NamespaceId, TableLayouts, birth_claims_of,
};
use crate::exec::ExecutionContext;
use crate::exec::migrate::{Replay, into_error};
use crate::iam::{Auth, Role};
use crate::key::schema::{
	RecordKey, RecordPrefix, TableMigrationKey, TableMigrationPrefix, TableMigrationRequestKey,
	TableMigrationRequestPrefix,
};
use crate::key::table_migration::{TableMigrationRequest, TableMigrationState};
use crate::key::{KVKeyDecode, Key, Resumable};
use crate::kvs::tasklease::{LeaseHandler, TaskLeaseType};
use crate::kvs::{DatastoreError, Direction, Transaction, TransactionType};
use crate::val::{RecordId, TableName, Value};

/// The most records a page reads.
const PAGE_ROWS: u32 = 256;

/// The most queue entries read at once. A pass pages through each queue in batches of this size,
/// so an entry that asks for nothing never keeps a later one from being visited, and a page folds
/// at most this many of a table's requests.
const QUEUE_BATCH: u32 = 1024;

/// How long a pass waits between checks for the transactions older than a verification to end.
const SETTLE_POLL: Duration = Duration::from_millis(10);

/// How a pass over one table ended.
enum TableOutcome {
	/// The table has nothing left for this pass.
	Done,
	/// Another node holds the job's lease.
	LeaseLost,
}

/// A table the job works on, with the names a catalog write addresses it by.
struct Target {
	ns: NamespaceId,
	db: DatabaseId,
	tb: TableName,
	ns_name: String,
	db_name: String,
}

impl Target {
	fn key(&self) -> TableMigrationKey<'_> {
		TableMigrationKey {
			ns: self.ns,
			db: self.db,
			tb: Cow::Borrowed(&self.tb),
		}
	}
}

impl Datastore {
	/// Work through every table queued for the table migration job, under the job's lease:
	/// rewrite the rows its steps have not reached, and verify the fields queued for
	/// verification. The tables with requests are visited first, then the tables whose progress
	/// asks for work that this pass has not visited.
	pub async fn table_migration(
		dbs: Arc<Datastore>,
		interval: Duration,
		canceller: CancellationToken,
	) -> Result<()> {
		let lh = LeaseHandler::new_with_canceller(
			dbs.sequences.clone(),
			dbs.id,
			dbs.transaction_factory.clone(),
			TaskLeaseType::TableMigration,
			interval * 2,
			canceller.clone(),
		)?;
		if !lh.has_lease().await? {
			return Ok(());
		}
		let mut visited = HashSet::new();
		// The tables with requests, each once: a table's requests sort together.
		let mut range = TableMigrationRequestPrefix {}.range()?;
		loop {
			let keys = {
				let txn = dbs.transaction(TransactionType::Read).await?;
				let res = txn.keys(range.clone(), QUEUE_BATCH, 0, None).await;
				let _ = txn.cancel().await;
				res?
			};
			let Some(last) = keys.last() else {
				break;
			};
			let last = last.clone();
			let full = keys.len() >= QUEUE_BATCH as usize;
			for key in keys {
				Self::ensure_not_cancelled(&canceller)?;
				let key = match TableMigrationRequestKey::decode_key(&key) {
					Ok(key) => key,
					Err(e) => {
						warn!("Skipping undecodable table migration request: {e}");
						continue;
					}
				};
				let table = (key.ns, key.db, key.tb.into_owned());
				if visited.contains(&table) {
					continue;
				}
				visited.insert(table.clone());
				let (ns, db, tb) = table;
				if !Self::visit_table(&dbs, &lh, &canceller, ns, db, tb, interval).await? {
					return Ok(());
				}
			}
			if !full {
				break;
			}
			range = range.resume_after(&last, Direction::Forward);
		}
		// The tables whose progress asks for work, as a pass that stopped left them.
		let mut range = TableMigrationPrefix {}.range()?;
		loop {
			let entries = {
				let txn = dbs.transaction(TransactionType::Read).await?;
				let res = txn.scan(range.clone(), QUEUE_BATCH, 0, None).await;
				let _ = txn.cancel().await;
				res?
			};
			let Some((last, _)) = entries.last() else {
				return Ok(());
			};
			let last = last.clone();
			let full = entries.len() >= QUEUE_BATCH as usize;
			for (key, state) in entries {
				Self::ensure_not_cancelled(&canceller)?;
				if !state.has_work() {
					continue;
				}
				let key = match TableMigrationKey::decode_key(&key) {
					Ok(key) => key,
					Err(e) => {
						warn!("Skipping undecodable table migration queue entry: {e}");
						continue;
					}
				};
				let table = (key.ns, key.db, key.tb.into_owned());
				if visited.contains(&table) {
					continue;
				}
				let (ns, db, tb) = table;
				if !Self::visit_table(&dbs, &lh, &canceller, ns, db, tb, interval).await? {
					return Ok(());
				}
			}
			if !full {
				return Ok(());
			}
			range = range.resume_after(&last, Direction::Forward);
		}
	}

	/// Work through table `tb` of database `db` of namespace `ns`, as [`Self::migrate_table`]
	/// does. Answers whether the pass goes on: `false` once another node holds the job's lease. A
	/// table whose pass fails is logged and left for the next pass.
	#[allow(clippy::too_many_arguments)]
	async fn visit_table(
		dbs: &Arc<Datastore>,
		lh: &LeaseHandler,
		canceller: &CancellationToken,
		ns: NamespaceId,
		db: DatabaseId,
		tb: TableName,
		interval: Duration,
	) -> Result<bool> {
		let Some(target) = Self::migration_target(dbs, ns, db, tb).await? else {
			return Ok(true);
		};
		match Self::migrate_table(dbs, lh, canceller, &target, interval).await {
			Ok(TableOutcome::Done) => Ok(true),
			Ok(TableOutcome::LeaseLost) => Ok(false),
			Err(e) => {
				if canceller.is_cancelled() {
					return Err(e);
				}
				warn!(
					"Table migration of `{}` in {}/{} failed: {e}",
					target.tb, target.ns_name, target.db_name
				);
				Ok(true)
			}
		}
	}

	/// The table a queue entry names, with its namespace and database names; `None` when the
	/// namespace or database is gone, in which case the table's progress and requests are deleted.
	async fn migration_target(
		dbs: &Datastore,
		ns: NamespaceId,
		db: DatabaseId,
		tb: TableName,
	) -> Result<Option<Target>> {
		let txn = dbs.transaction(TransactionType::Write).await?;
		let ns_name = txn
			.all_ns(None)
			.await?
			.iter()
			.find(|n| n.namespace_id == ns)
			.map(|n| n.name.to_string());
		let db_name = match ns_name {
			Some(_) => txn
				.all_db(ns, None)
				.await?
				.iter()
				.find(|d| d.database_id == db)
				.map(|d| d.name.to_string()),
			None => None,
		};
		match (ns_name, db_name) {
			(Some(ns_name), Some(db_name)) => {
				let _ = txn.cancel().await;
				Ok(Some(Target {
					ns,
					db,
					tb,
					ns_name,
					db_name,
				}))
			}
			_ => {
				let key = TableMigrationKey {
					ns,
					db,
					tb: Cow::Borrowed(&tb),
				};
				txn.del_key(&key).await?;
				for (request, _) in txn.tb_migration_requests(ns, db, &tb, u32::MAX).await? {
					txn.del(Key::from(request)).await?;
				}
				txn.commit().await?;
				Ok(None)
			}
		}
	}

	/// Work through `target` in pages until its progress asks for nothing more and it has no
	/// request left, the lease is lost, or a verification must wait for older transactions longer
	/// than one interval.
	async fn migrate_table(
		dbs: &Arc<Datastore>,
		lh: &LeaseHandler,
		canceller: &CancellationToken,
		target: &Target,
		interval: Duration,
	) -> Result<TableOutcome> {
		let mut page = PAGE_ROWS;
		loop {
			Self::ensure_not_cancelled(canceller)?;
			if !lh.try_maintain_lease().await? {
				return Ok(TableOutcome::LeaseLost);
			}
			let txn = Arc::new(dbs.transaction(TransactionType::Write).await?);
			let progress = txn.get_key(&target.key(), None).await?;
			let requests =
				txn.tb_migration_requests(target.ns, target.db, &target.tb, QUEUE_BATCH).await?;
			if requests.is_empty() && !progress.as_ref().is_some_and(TableMigrationState::has_work)
			{
				let _ = txn.cancel().await;
				return Ok(TableOutcome::Done);
			}
			if txn.get_tb(target.ns, target.db, &target.tb, None).await?.is_none() {
				// The table is gone: nothing is left to rewrite or verify.
				for (request, _) in &requests {
					txn.del(Key::from(request.as_slice())).await?;
				}
				if let Some(progress) = &progress {
					txn.del_compare_key(&target.key(), Some(progress)).await?;
				}
				Self::commit_page(&txn).await?;
				return Ok(TableOutcome::Done);
			}
			if !requests.is_empty() {
				// The requests are folded in a page of their own, so a verification a request
				// asks for is in the progress before the lap waits for the transactions older
				// than it.
				match Self::fold_requests(&txn, target, progress.as_ref(), &requests).await {
					Ok(()) => Self::commit_page(&txn).await?,
					// Another node's job replaced the progress; the next page reads it again.
					Err(e) if crate::kvs::is_conditional_write_conflict(&e) => {
						let _ = txn.cancel().await;
					}
					Err(e) => {
						let _ = txn.cancel().await;
						return Err(e);
					}
				}
				continue;
			}
			let Some(state) = progress else {
				let _ = txn.cancel().await;
				return Ok(TableOutcome::Done);
			};
			if state.verifying() && !state.settled && txn.sole_writer() {
				// The lap judges rows only once no transaction that began before the
				// verification was folded can still commit one behind its cursor. Every row
				// committed afterwards was written by a transaction that saw the new
				// definition, and coerced to it.
				let _ = txn.cancel().await;
				// The page's own transaction is older than the wait: it is counted out only once
				// dropped.
				drop(txn);
				if !Self::older_transactions_ended(dbs, interval).await {
					return Ok(TableOutcome::Done);
				}
				// A request committed during the wait is left for the next page, which folds it
				// into the settled progress: a verification it asks for is unsettled again.
				let txn = dbs.transaction(TransactionType::Write).await?;
				let settled = TableMigrationState {
					settled: true,
					..state.clone()
				};
				match txn.put_compare_key(&target.key(), &settled, Some(&state)).await {
					Ok(()) => Self::commit_page(&txn).await?,
					// The progress moved meanwhile; the next page reads it again.
					Err(e) if crate::kvs::is_conditional_write_conflict(&e) => {
						let _ = txn.cancel().await;
					}
					Err(e) => {
						let _ = txn.cancel().await;
						return Err(e);
					}
				}
				continue;
			}
			let outcome = Self::migration_page(dbs, &txn, target, &state, page).await;
			match outcome {
				Ok(more) => match Self::commit_page(&txn).await {
					Ok(()) => {
						if !more {
							return Ok(TableOutcome::Done);
						}
						page = (page * 2).min(PAGE_ROWS);
					}
					Err(e) if surrealdb_datastore::is_retryable_transaction_conflict(&e) => {
						// A user write of a row the page rewrote beat the page; take a fresh
						// snapshot with a smaller page.
						page = (page / 2).max(1);
						common::time::sleep(SETTLE_POLL).await;
					}
					Err(e) => return Err(e),
				},
				// The progress moved under the page: a node that took the lease over replaced it,
				// or `REMOVE TABLE` deleted it. The next page reads it again.
				Err(e) if crate::kvs::is_conditional_write_conflict(&e) => {
					let _ = txn.cancel().await;
				}
				Err(e) => {
					let _ = txn.cancel().await;
					return Err(e);
				}
			}
			yield_now!();
		}
	}

	/// Fold `requests`, the first of `target`'s in the order made, into `progress`, the progress
	/// `txn` read, and delete them, leaving the writes for the caller to commit. The progress is
	/// written by compare-and-set on `progress`, and deleted when the fold leaves none.
	async fn fold_requests(
		txn: &Transaction,
		target: &Target,
		progress: Option<&TableMigrationState>,
		requests: &[(Vec<u8>, TableMigrationRequest)],
	) -> Result<()> {
		let folded =
			requests.iter().fold(progress.cloned(), |state, (_, request)| request.apply(state));
		for (request, _) in requests {
			txn.del(Key::from(request.as_slice())).await?;
		}
		match (&folded, progress) {
			(Some(folded), _) => txn.put_compare_key(&target.key(), folded, progress).await,
			(None, Some(progress)) => txn.del_compare_key(&target.key(), Some(progress)).await,
			(None, None) => Ok(()),
		}
	}

	/// Commit a page; a page whose progress moved under it commits nothing and is not an error,
	/// since the next page reads the progress again.
	async fn commit_page(txn: &Transaction) -> Result<()> {
		match txn.commit().await {
			Ok(()) => Ok(()),
			Err(e) if crate::kvs::is_conditional_write_conflict(&e) => {
				let _ = txn.cancel().await;
				Ok(())
			}
			Err(e) => {
				let _ = txn.cancel().await;
				Err(e)
			}
		}
	}

	/// Wait, for at most `limit`, until every transaction opened through this datastore before
	/// the call has ended.
	async fn older_transactions_ended(dbs: &Datastore, limit: Duration) -> bool {
		let live = dbs.commit_triggers().live_transactions();
		let deadline = Instant::now() + limit;
		let bucket = loop {
			if let Some(bucket) = live.advance() {
				break bucket;
			}
			if Instant::now() >= deadline {
				return false;
			}
			common::time::sleep(SETTLE_POLL).await;
		};
		while !live.drained(bucket) {
			if Instant::now() >= deadline {
				return false;
			}
			common::time::sleep(SETTLE_POLL).await;
		}
		true
	}

	/// The context a page evaluates migration steps in: the table's database, privileged, with no
	/// session.
	async fn migration_context(
		dbs: &Datastore,
		txn: &Arc<Transaction>,
		target: &Target,
	) -> Result<ExecutionContext> {
		dbs.privileged_database_context(txn, &target.ns_name, &target.db_name).await
	}

	/// The context a privileged evaluation over `txn` runs in: database `db_name` of namespace
	/// `ns_name`, with the root owner's rights, permissions suppressed and no session. Both must
	/// be defined.
	pub(crate) async fn privileged_database_context(
		&self,
		txn: &Arc<Transaction>,
		ns_name: &str,
		db_name: &str,
	) -> Result<ExecutionContext> {
		let mut ctx = self.setup_ctx()?;
		ctx.set_transaction(Arc::clone(txn));
		let ctx = ctx.freeze();
		let ns = txn.expect_ns_by_name(ns_name).await?;
		let db = txn.expect_db_by_name(ns_name, db_name).await?;
		let opt = crate::dbs::Options::new(&self.config.exec)
			.with_ns(Some(ns_name.into()))
			.with_db(Some(db_name.into()))
			.with_auth(Arc::new(Auth::for_root(Role::Owner)));
		Ok(ExecutionContext::privileged_database(ctx, opt, ns, db))
	}

	/// Run one page of `target`'s lap in `txn`, which `state` was read in, leaving the advanced
	/// progress written for the caller to commit. Answers whether the progress still asks for
	/// work.
	async fn migration_page(
		dbs: &Datastore,
		txn: &Arc<Transaction>,
		target: &Target,
		state: &TableMigrationState,
		page: u32,
	) -> Result<bool> {
		let exec = Self::migration_context(dbs, txn, target).await?;
		let layouts = txn.table_layouts(target.ns, target.db, &target.tb, None).await?;
		let replay = Replay::of(&layouts, target.ns, target.db);
		let fields = txn.all_tb_fields(target.ns, target.db, &target.tb, None).await?;
		let mut next = state.clone();
		// The check for undeclared keys judges rows against the names the table declares at the
		// top level now. Rows a lap passed under other names were judged against names that no
		// longer stand, so the lap starts over.
		let declared = crate::exec::migrate::declared_roots(fields.iter());
		if next
			.undeclared
			.as_ref()
			.is_some_and(|check| !check.finished && check.declared != declared)
		{
			if next.cursor.is_some() {
				next.restart();
			}
			if let Some(check) = next.undeclared.as_mut() {
				check.declared = declared;
			}
		}
		if next.lap == 0 {
			next.lap = 1;
		}
		// The lap's target is read on its first page, and only there. `0` is a target too (no
		// step stood when the lap began), so a step minted while the lap is under way leaves the
		// target behind the table's newest step and the lap's end starts another: the rows this
		// lap passed before the step was minted are rewritten by the next.
		if next.cursor.is_none() {
			next.target = newest_step(&layouts);
		}
		// The definitions of the fields under verification, by the position of each in the
		// progress; a field whose definition changed since the pass started is dropped from it, as
		// the definition is no longer the one the pass judges.
		next.verify.retain(|verified| {
			verified.finished
				|| fields.iter().any(|fd| {
					fd.name.to_raw_string() == verified.name
						&& crate::exec::migrate::definition_digest(fd)
							.is_ok_and(|digest| digest == verified.definition)
				})
		});
		// For each field under verification, the key a row holds its value under, what the value
		// is judged against, and whether that is the field's whole definition
		// ([`crate::exec::migrate::verification_test`]). The key is the field's name itself: the
		// canonical text `verified.name` holds backticks a name that would not parse bare, and
		// is the key of no row.
		let schemafull = txn
			.get_tb(target.ns, target.db, &target.tb, None)
			.await?
			.is_some_and(|tb| tb.schemafull);
		let mut tests = Vec::with_capacity(next.verify.len());
		for verified in &next.verify {
			let field = fields.iter().find(|fd| fd.name.to_raw_string() == verified.name);
			tests.push(match field.and_then(|field| Some((field, field.name.field_name()?))) {
				Some((field, key)) if !verified.finished && field.field_kind.is_some() => Some((
					key.to_owned(),
					crate::exec::migrate::verification_test(&exec, &fields, field, schemafull)
						.await
						.map_err(into_error)?,
				)),
				_ => None,
			});
		}
		let frame = crate::exec::migrate::migration_frame(&exec);

		let mut range = RecordPrefix {
			ns: target.ns,
			db: target.db,
			tb: Cow::Borrowed(&target.tb),
		}
		.range()?;
		if let Some(cursor) = &next.cursor {
			range = range.resume_after(cursor, Direction::Forward);
		}
		let rows = txn.scan_raw(range, page.max(1), 0, None).await?;
		if rows.is_empty() {
			let whole: Vec<bool> = tests
				.iter()
				.map(|test| test.as_ref().is_some_and(|(_, (_, whole))| *whole))
				.collect();
			return Self::end_lap(txn, target, state, next, &layouts, &fields, &whole).await;
		}
		let writer = if next.rewrite {
			Some(txn.record_writer(target.ns, target.db, &target.tb).await?)
		} else {
			None
		};
		for (key, value) in &rows {
			next.scanned += 1;
			let decoded = RecordKey::decode_key(key)?;
			let rid = RecordId {
				table: decoded.tb.into_owned(),
				key: decoded.id.into_owned(),
			};
			let (mut record, stamp) = txn
				.decode_stamped_record(target.ns, target.db, &target.tb, value, rid.clone(), None)
				.await?;
			if let Some(replay) = replay.as_ref().filter(|replay| replay.behind(stamp)) {
				let fallbacks = replay
					.apply_counted(&exec, &mut record.data, stamp)
					.await
					.map_err(into_error)?;
				if let Some(writer) = &writer {
					let bytes =
						writer.encode(record.metadata.as_ref(), &record.data, Some(&rid))?;
					txn.set(Key::from(key.as_slice()), bytes).await?;
					txn.forget_record(target.ns, target.db, &target.tb, &rid.key);
					next.rewritten += 1;
					next.fallbacks += fallbacks;
				}
			}
			// A row that is not an object has no fields to judge.
			let Value::Object(object) = &record.data else {
				continue;
			};
			for (verified, test) in next.verify.iter_mut().zip(&tests) {
				if verified.finished {
					continue;
				}
				let Some((key, (test, _))) = test else {
					continue;
				};
				// An object that holds no value at the field is judged as `NONE`: the report
				// lists the rows a later write would refuse, and a field whose type refuses `NONE`
				// states no claim over one.
				let value = object.get(key.as_str()).unwrap_or(&Value::None);
				if !test.conforms(&frame, value).await.map_err(into_error)? {
					verified.failures += 1;
				}
			}
			if let Some(check) = next.undeclared.as_mut().filter(|check| !check.finished)
				&& crate::exec::migrate::holds_undeclared_key(object, &check.declared)
			{
				check.failures += 1;
			}
		}
		next.cursor = rows.last().map(|(key, _)| key.clone());
		txn.put_compare_key(&target.key(), &next, Some(state)).await?;
		Ok(true)
	}

	/// End `target`'s lap in `txn`: stop rewriting when the lap's target is still the newest
	/// step, or start another lap; make the claim of each verified field that conformed
	/// throughout, where the lap could judge every row against the field's whole definition
	/// (`whole`, by the position of each in the progress) and the rewrite is done, and drop a
	/// field that conformed even where the table's claims cannot state it; withdraw the table's
	/// claims when a field that did not conform holds one on the definition the lap judged; drop
	/// the check for undeclared keys when the lap judged every row and counted none, and finish it
	/// with its count otherwise; and delete the progress once it asks for nothing and reports
	/// nothing.
	///
	/// The rewrite is judged done from the generations `txn` read. A step minted by a transaction
	/// that commits after that read comes with a request this page does not delete, so the next
	/// page starts another lap for it.
	#[allow(clippy::too_many_arguments)]
	async fn end_lap(
		txn: &Arc<Transaction>,
		target: &Target,
		state: &TableMigrationState,
		mut next: TableMigrationState,
		layouts: &TableLayouts,
		fields: &[crate::catalog::FieldDefinition],
		whole: &[bool],
	) -> Result<bool> {
		let mut again = false;
		if next.rewrite {
			if newest_step(layouts) == next.target {
				next.rewrite = false;
			} else {
				again = true;
			}
		}
		// The lap judged every row the table can hold: it began after every transaction that
		// could still commit one behind its cursor had ended, and no newer step is left to bring
		// the rows through.
		let judged_all = next.settled && txn.sole_writer() && !again;
		// A claim is stored in the stored grammar, which a store still at the legacy feature level
		// does not write.
		let promotable = judged_all && !txn.write_level().is_legacy();
		let claimable = if promotable && next.verify.iter().any(|f| !f.finished && f.failures == 0)
		{
			claimable_fields(fields)
		} else {
			Vec::new()
		};
		// A row the lap found not to conform refutes any claim on the definition it judged. The
		// claims are withdrawn before this lap's promotions, which a withdrawal would undo.
		if !again {
			let held = txn.table_conformance(target.ns, target.db, &target.tb, None).await?;
			let refuted = held.is_some_and(|held| {
				next.verify.iter().any(|verified| {
					!verified.finished
						&& verified.failures > 0
						&& held
							.claim(&verified.name)
							.is_some_and(|claim| claim.definition == verified.definition)
				})
			});
			if refuted {
				txn.forfeit_conformance(target.ns, target.db, &target.tb).await?;
			}
		}
		let mut kept = Vec::with_capacity(next.verify.len());
		let verify = std::mem::take(&mut next.verify);
		for (at, mut verified) in verify.into_iter().enumerate() {
			if verified.finished || again {
				kept.push(verified);
				continue;
			}
			if promotable && verified.failures == 0 {
				// The lap judged every row and none failed: the field conforms. It states a claim
				// when the lap judged its whole definition and the table's claims can state it;
				// either way nothing is left to report.
				if whole.get(at).copied().unwrap_or(false)
					&& claimable.iter().any(|(name, digest)| {
						*name == verified.name && *digest == verified.definition
					}) {
					match txn
						.promote_conformance(
							target.ns,
							target.db,
							&target.tb,
							&verified.name,
							verified.definition,
							Conformance::Guaranteed,
						)
						.await
					{
						Ok(()) => {}
						// The definition changed after the pass read it: nothing is claimed.
						Err(e)
							if e.downcast_ref::<DatastoreError>().is_some_and(|e| {
								matches!(e, DatastoreError::ConformanceStale { .. })
							}) => {}
						Err(e) => return Err(e),
					}
				}
				continue;
			}
			verified.finished = true;
			kept.push(verified);
		}
		next.verify = kept;
		// The check for undeclared keys states no claim: a lap that judged every row and counted
		// none leaves nothing to report.
		if !again && let Some(check) = next.undeclared.as_mut().filter(|check| !check.finished) {
			check.finished = true;
			if judged_all && check.failures == 0 {
				next.undeclared = None;
			}
		}
		if again {
			next.restart();
		}
		if next.is_spent() {
			txn.del_compare_key(&target.key(), Some(state)).await?;
			return Ok(false);
		}
		if !again {
			// The lap is over; a finished verification keeps its counts for `INFO`, and the
			// cursor starts the next lap from the first record.
			next.cursor = None;
		}
		txn.put_compare_key(&target.key(), &next, Some(state)).await?;
		Ok(next.has_work())
	}
}

/// The newest generation of the table's ring that carries a migration step, as the number the
/// progress records; `0` when none does.
fn newest_step(layouts: &TableLayouts) -> u16 {
	layouts.newest_migration().map_or(0, Generation::get)
}

/// The fields of a table, by name with the digest of their definitions, that the table's claims
/// can state conform ([`birth_claims_of`]): none when a definition cannot be compiled. A
/// component of a named record id is read from the key and has no stored form, so it is not a
/// candidate.
fn claimable_fields(fields: &[crate::catalog::FieldDefinition]) -> Vec<(String, i64)> {
	let stored: Vec<_> = fields
		.iter()
		.filter(|fd| !fd.is_key_part())
		.map(|fd| (fd.name.to_raw_string(), fd.to_stored()))
		.collect();
	let mut compiled = Vec::with_capacity(stored.len());
	for (name, definition) in &stored {
		let Some(candidate) = FieldCandidate::compile(name, definition) else {
			return Vec::new();
		};
		compiled.push(candidate);
	}
	let candidates: Vec<&FieldCandidate> = compiled.iter().collect();
	birth_claims_of(&candidates).into_iter().map(|claim| (claim.field, claim.definition)).collect()
}
