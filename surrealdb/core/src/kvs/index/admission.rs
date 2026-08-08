use anyhow::Result;
use chrono::Utc;
use common::time::{sleep, timeout};
use surrealdb_strand::TableName;
use web_time::Instant;

use super::builder::{IndexKey, IndexMutation};
use super::state::{catalog_still_references_index, is_condition_not_met};
use super::{
	Appending, BUILD_CLOSING_SLEEP, BUILD_RESERVATION_TTL_SECS, ConsumeResult, DurableAdmission,
	DurableAdmissionDecision, DurableAdmissionFence, IndexBuildPhase, IndexBuildReservation,
	IndexBuilder, IndexBuilding, PrimaryAppendingTicket,
};
use crate::catalog::{DatabaseDefinition, DatabaseId, IndexDefinition, IndexId, NamespaceId};
use crate::ctx::FrozenContext;
use crate::err::Error;
use crate::idx::IndexKeyBase;
use crate::key::{KVKey, KVValue};
#[cfg(test)]
use crate::kvs::testing::{NonRetryableErrorSite, maybe_inject_non_retryable_error};
use crate::kvs::tx::{
	CachedIndexBuildReservationKey, CachedIndexBuildReservationLookup, IndexBuildReservationRelease,
};
use crate::kvs::{DatastoreError, TransactionType};

/// Resolved slot in a per-(user-txn, index) reservation under which a single
/// indexed mutation will be written.
struct AdmittedMutation {
	generation: super::BuildGeneration,
	ticket: super::BuildTicket,
	mutation_seq: super::BuildTicketMutationSeq,
	initial_complete: bool,
}

impl AdmittedMutation {
	fn from_lookup(lookup: CachedIndexBuildReservationLookup) -> Self {
		match lookup {
			CachedIndexBuildReservationLookup::FirstUse {
				generation,
				ticket,
				mutation_seq,
				initial_complete,
				..
			}
			| CachedIndexBuildReservationLookup::Reused {
				generation,
				ticket,
				mutation_seq,
				initial_complete,
			} => Self {
				generation,
				ticket,
				mutation_seq,
				initial_complete,
			},
		}
	}
}

impl IndexBuilder {
	/// Either enqueue a document mutation for a durable build or let it index now.
	///
	/// Writer admission is split into a short CAS transaction that reserves a
	/// durable ticket and the user write transaction that writes the replayable
	/// mutation. A committed reservation carries a prepared close-time release,
	/// which is registered before fence or queue work so failed writes cannot
	/// leave a live-node ticket blocking `Closing`. The follow-up fence closes
	/// the race where the build reaches `Online` or `Error` after the ticket was
	/// reserved but before the user transaction writes its `!bg` entry.
	///
	/// One reservation is allocated per user transaction per index. The first
	/// admitted mutation runs the short reservation transaction; subsequent
	/// mutations on the same index reuse the cached ticket and consume fresh
	/// `mutation_seq` slots without paying another reservation commit.
	pub(crate) async fn consume(
		&self,
		db: &DatabaseDefinition,
		ctx: &FrozenContext,
		ix: &IndexDefinition,
		mutation: IndexMutation<'_>,
	) -> Result<ConsumeResult> {
		let table_name = &ix.table_name;
		let ikb =
			IndexKeyBase::new(db.namespace_id, db.database_id, table_name.clone(), ix.index_id);

		let cache_key = CachedIndexBuildReservationKey {
			ns: db.namespace_id,
			db: db.database_id,
			tb: table_name.clone(),
			ix: ix.index_id,
		};

		// Reuse an admission cached earlier in this user transaction; this is
		// the common path for bulk INSERT/UPDATE/DELETE statements that touch
		// many records on the same index. Revalidate against the live `!bs`
		// state before reusing: skipping that recheck would let a generation
		// rotation or phase transition between mutations strand
		// `!bg(old_gen, *)` entries that no builder would replay.
		if let Some(lookup) = ctx.tx().lookup_cached_index_build_reservation(&cache_key).await? {
			let admitted = AdmittedMutation::from_lookup(lookup);
			self.recheck_cached_admission(ctx, &ikb, ix, admitted.generation).await?;
			return self.write_admitted_mutation(ctx, &ikb, mutation, admitted).await;
		}

		match self.reserve_durable_admission(ctx, &ikb).await? {
			DurableAdmissionDecision::Admit(admission) => {
				// Admission has already committed `!br`. Publish the prepared
				// release and ticket into the per-user-txn cache before fence
				// or queue work so any later error still releases the ticket
				// when the user transaction closes — and so subsequent
				// mutations in this user transaction skip the reservation.
				let release = admission.release.clone();
				ctx.tx().register_index_build_reservation_release(release.clone()).await;
				let first_use = ctx
					.tx()
					.insert_cached_index_build_reservation(
						cache_key.clone(),
						admission.generation,
						admission.ticket,
						admission.initial_complete,
					)
					.await;
				#[cfg(test)]
				maybe_inject_non_retryable_error(
					NonRetryableErrorSite::ConcurrentIndexAfterReservationRegistration,
					ctx.node_id(),
				)?;
				if matches!(
					self.fence_durable_admission(ctx, &ikb, ix, &admission, release).await?,
					DurableAdmissionFence::IndexNormally
				) {
					// The fence saw the build go online and released `!br`.
					// Drop the cache entry so the next mutation in this user
					// transaction re-enters reservation and rediscovers the
					// online state via `DurableAdmissionDecision::IndexNormally`
					// instead of writing orphan `!bg` under the released ticket.
					ctx.tx().remove_cached_index_build_reservation(&cache_key).await;
					return Ok(ConsumeResult::Ignored(mutation.old_values, mutation.new_values));
				}
				let admitted = AdmittedMutation::from_lookup(first_use);
				self.write_admitted_mutation(ctx, &ikb, mutation, admitted).await
			}
			DurableAdmissionDecision::IndexNormally => {
				Ok(ConsumeResult::Ignored(mutation.old_values, mutation.new_values))
			}
			DurableAdmissionDecision::MissingState => {
				let tx = ctx.tx();
				if catalog_still_references_index(
					&tx,
					db.namespace_id,
					db.database_id,
					&ix.table_name,
					&ix.name,
					ix.index_id,
				)
				.await?
				{
					Ok(ConsumeResult::Ignored(mutation.old_values, mutation.new_values))
				} else {
					Ok(ConsumeResult::Retired)
				}
			}
		}
	}

	/// Write the `!bg` entry (and optionally a `!bp` marker) for a mutation
	/// that has been admitted under a (cached or freshly allocated) reservation.
	async fn write_admitted_mutation(
		&self,
		ctx: &FrozenContext,
		ikb: &IndexKeyBase,
		mutation: IndexMutation<'_>,
		admitted: AdmittedMutation,
	) -> Result<ConsumeResult> {
		let IndexMutation {
			old_values,
			new_values,
			rid,
			count_cond_match,
		} = mutation;
		let appending = Appending {
			old_values,
			new_values,
			id: rid.key.clone(),
			count_cond_match,
		};
		let tx = ctx.tx();
		tx.set_key(
			&ikb.new_bg_key(admitted.generation, admitted.ticket, admitted.mutation_seq),
			&appending,
		)
		.await?;
		if !admitted.initial_complete {
			let bp = ikb.new_bp_key(admitted.generation, &rid.key);
			if tx.get_key(&bp, None).await?.is_none() {
				tx.set_key(
					&bp,
					&PrimaryAppendingTicket {
						ticket: admitted.ticket,
						mutation_seq: admitted.mutation_seq,
					},
				)
				.await?;
			}
		}
		Ok(ConsumeResult::Enqueued)
	}

	/// Re-read durable build state before writing a queued mutation.
	///
	/// If the generation changed, the reservation is released and the write
	/// fails. If the build became online, the reservation is released and
	/// the caller indexes normally in the user transaction. A build that
	/// errored keeps queueing (see `reserve_durable_admission`). This
	/// deliberately avoids `ctx.tx()`: a user transaction can define a
	/// concurrent index and then write to the same table before its snapshot
	/// can see the builder's separately-committed `!bs` record.
	async fn fence_durable_admission(
		&self,
		ctx: &FrozenContext,
		ikb: &IndexKeyBase,
		ix: &IndexDefinition,
		admission: &DurableAdmission,
		release: IndexBuildReservationRelease,
	) -> Result<DurableAdmissionFence> {
		let tx =
			self.tf.transaction(TransactionType::Read, ctx.try_get_sequences()?.clone()).await?;
		let state = catch!(tx, tx.get_key(&ikb.new_bs_key(), None).await);
		tx.cancel().await?;
		let Some(state) = state else {
			release.release().await?;
			return Err(DatastoreError::IndexingBuildingCancelled {
				reason: format!("Index {} build state no longer exists", ix.name),
			}
			.into());
		};
		if state.generation != admission.generation {
			release.release().await?;
			return Err(DatastoreError::IndexingBuildingCancelled {
				reason: format!("Index {} build generation changed", ix.name),
			}
			.into());
		}
		match state.phase {
			// `Error` queues like `Building`: the ticket was reserved against
			// this same generation, and the mutation either dies with the
			// generation's queue wipe (the record is rescanned) or is
			// re-queued under the recovering build — see the matching arm in
			// `reserve_durable_admission`.
			IndexBuildPhase::Building | IndexBuildPhase::Closing | IndexBuildPhase::Error => {
				Ok(DurableAdmissionFence::Queue)
			}
			IndexBuildPhase::Online => {
				release.release().await?;
				Ok(DurableAdmissionFence::IndexNormally)
			}
		}
	}

	/// Revalidate a cached admission against the live durable build state.
	///
	/// The first mutation in a user transaction pays the full fence (a fresh
	/// short transaction reads `!bs`). Subsequent mutations reuse the cached
	/// ticket, but the build can still rotate or transition `Online` in
	/// between — and a new-build takeover wipes prior-generation `!br` via
	/// [`super::state::delete_stale_build_queues`], removing the anchor the
	/// protocol relies on. Without this recheck, the next mutation would
	/// write `!bg(old_gen, *)` that no builder will replay.
	///
	/// On `Online`, generation mismatch, or missing state, the user
	/// transaction is aborted with `IndexingBuildingCancelled`; an errored
	/// build keeps queueing (see `reserve_durable_admission`). The cached
	/// ticket's `!br` is removed by the close-path release; an idempotent
	/// `delc(key, val)` handles the case where it was already wiped.
	pub(super) async fn recheck_cached_admission(
		&self,
		ctx: &FrozenContext,
		ikb: &IndexKeyBase,
		ix: &IndexDefinition,
		cached_generation: super::BuildGeneration,
	) -> Result<()> {
		let tx =
			self.tf.transaction(TransactionType::Read, ctx.try_get_sequences()?.clone()).await?;
		let state = catch!(tx, tx.get_key(&ikb.new_bs_key(), None).await);
		tx.cancel().await?;
		let Some(state) = state else {
			return Err(DatastoreError::IndexingBuildingCancelled {
				reason: format!("Index {} build state no longer exists", ix.name),
			}
			.into());
		};
		if state.generation != cached_generation {
			return Err(DatastoreError::IndexingBuildingCancelled {
				reason: format!("Index {} build generation changed mid-transaction", ix.name),
			}
			.into());
		}
		match state.phase {
			// `Error` keeps queueing under the cached ticket: earlier
			// mutations in this user transaction are already queued to this
			// generation, and the whole queue is wiped-and-rescanned by the
			// recovering `REBUILD INDEX`, so consistency does not depend on
			// replay. Aborting here would fail the user's write for a
			// background build failure.
			IndexBuildPhase::Building | IndexBuildPhase::Closing | IndexBuildPhase::Error => Ok(()),
			IndexBuildPhase::Online => Err(DatastoreError::IndexingBuildingCancelled {
				reason: format!(
					"Index {} became online mid-transaction; queued mutations would be lost",
					ix.name
				),
			}
			.into()),
		}
	}

	/// Allocate a durable writer ticket while the index is building.
	///
	/// `Closing` rejects new tickets but may still have admitted writers in
	/// flight, so callers poll durable state until the build becomes `Online`
	/// or `Error`, or the request context is cancelled or timed out. `Error`
	/// admits like `Building` so a failed build never blocks user writes; the
	/// queued mutations are wiped and the table rescanned when a `REBUILD
	/// INDEX` recovers the index.
	async fn reserve_durable_admission(
		&self,
		ctx: &FrozenContext,
		ikb: &IndexKeyBase,
	) -> Result<DurableAdmissionDecision> {
		loop {
			if let Some(reason) = ctx.done(true)? {
				return Err(Error::from(reason).into());
			}
			let tx = self
				.tf
				.transaction(TransactionType::Write, ctx.try_get_sequences()?.clone())
				.await?;
			let state_key = ikb.new_bs_key();
			let Some(state) = tx.get_key(&state_key, None).await? else {
				tx.cancel().await?;
				return Ok(DurableAdmissionDecision::MissingState);
			};
			match state.phase {
				IndexBuildPhase::Online => {
					tx.cancel().await?;
					return Ok(DurableAdmissionDecision::IndexNormally);
				}
				IndexBuildPhase::Closing => {
					tx.cancel().await?;
					sleep(BUILD_CLOSING_SLEEP).await;
					if let Some(reason) = ctx.done(true)? {
						return Err(Error::from(reason).into());
					}
					continue;
				}
				// A failed build must not fail user writes: mutations keep
				// queueing under the errored generation. Nothing replays them
				// (the generation is terminal), but the reservation still
				// fences the recovery path — a `REBUILD INDEX` starts a new
				// generation, and its takeover drains in-flight `!br` before
				// wiping the stale queues and rescanning the table, so every
				// write admitted here is either rescanned or re-queued under
				// the new generation. Failing the write instead would turn a
				// background build failure into a table-wide write outage.
				IndexBuildPhase::Building | IndexBuildPhase::Error => {
					// A generation installed by this protocol version owns a
					// `!bt` ticket counter, so admission never writes `!bs` and
					// cannot invalidate the builder's in-flight batch. A build
					// whose generation predates the counter has no `!bt` and
					// keeps allocating from `!bs.next_ticket` until its next
					// generation, so upgrading under an in-flight build does not
					// change how its tickets are issued.
					let bt = ikb.new_bt_key(state.generation);
					let counter = tx.get_key(&bt, None).await?;
					let ticket = counter.unwrap_or(state.next_ticket);
					let reservation = IndexBuildReservation {
						node: ctx.node_id(),
						expires_at: Utc::now()
							+ chrono::Duration::seconds(BUILD_RESERVATION_TTL_SECS),
					};
					let br = ikb.new_br_key(state.generation, ticket);
					let release = IndexBuildReservationRelease::new(
						self.tf.clone(),
						ctx.try_get_sequences()?.clone(),
						ctx.node_id(),
						br.encode_key()?,
						reservation.kv_encode_value()?,
					);
					tx.set_key(&br, &reservation).await?;
					// Both allocation paths are compare-and-swaps, so a
					// concurrent generation flip — which rewrites `!bs` and
					// removes `!bt` after reading it — fences this admission on
					// every backend, including the last-writer-wins ones where a
					// blind write would not conflict.
					let res = match counter {
						Some(current) => {
							tx.put_compare_key(&bt, &ticket.saturating_add(1), Some(&current)).await
						}
						None => {
							let mut next = state.clone();
							next.next_ticket = next.next_ticket.saturating_add(1);
							next.updated_at = Utc::now();
							// Freeze legacy fallback state before refreshing
							// `updated_at`; writer admissions must not extend the
							// builder lease.
							next.owner_heartbeat_at =
								state.owner_heartbeat_at.or(Some(state.updated_at));
							tx.put_compare_key(&state_key, &next, Some(&state)).await
						}
					};
					match res {
						Ok(()) => {
							tx.commit().await?;
							return Ok(DurableAdmissionDecision::Admit(DurableAdmission {
								generation: state.generation,
								ticket,
								initial_complete: state.initial_complete,
								release,
							}));
						}
						Err(err) if is_condition_not_met(&err) => {
							let _ = tx.cancel().await;
							continue;
						}
						Err(err) => {
							let _ = tx.cancel().await;
							return Err(err);
						}
					}
				}
			}
		}
	}

	/// Drop an index's entry from the local task map and signal its builder to
	/// abort, returning the build so a caller can wait for the task to exit.
	///
	/// The abort flag is only observed at the builder's next checkpoint, so the
	/// task is still running when this returns.
	async fn take_local_builder(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		ix: IndexId,
	) -> Option<IndexBuilding> {
		let key = IndexKey::new(ns, db, tb, ix);
		let building = self.indexes.write().await.remove(&key);
		if let Some(building) = &building {
			building.abort();
		}
		building
	}

	/// Abort a builder task running in this process.
	///
	/// Schema statements retire durable state separately in their own
	/// transaction. This method only handles the process-local task map.
	pub(crate) async fn remove_index(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		ix: IndexId,
	) -> Result<()> {
		self.take_local_builder(ns, db, tb, ix).await;
		Ok(())
	}

	/// Abort a builder task running in this process and wait for it to stop.
	///
	/// [`Self::remove_index`] only raises the abort flag; the task observes it at
	/// its next checkpoint and keeps writing until then, including the durable
	/// state and index data of the batch it is inside. A caller that deletes
	/// those keys afterwards must order its delete after the task's last write,
	/// so it waits here first: a builder write that lands after the delete
	/// re-creates state for a build that no longer exists, and nothing ever
	/// removes it.
	///
	/// `deadline` is the budget for the whole transaction-close drain, from
	/// [`build_abort_deadline`](super::build_abort_deadline), so aborting several
	/// builders costs one budget rather than one per builder. Once it passes the
	/// caller still proceeds, leaving the compare-and-swap on `!bs` that every
	/// builder write performs as the only fence. The task's map entry is already
	/// gone by then, so a caller that retries its delete does so without a
	/// second wait — the warning below is the only record of that degradation.
	pub(crate) async fn remove_index_and_wait(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		ix: IndexId,
		deadline: Instant,
	) {
		let Some(building) = self.take_local_builder(ns, db, tb, ix).await else {
			return;
		};
		// A drain that has already spent its budget still polls the wait once,
		// which is enough for a builder that has meanwhile finished.
		let remaining = deadline.saturating_duration_since(Instant::now());
		if timeout(remaining, building.wait_finished()).await.is_err() {
			warn!(
				target: "surrealdb::core::kvs::index",
				index = %building.ix.name,
				table = %building.ix.table_name,
				"timed out waiting for an aborted index builder to stop; \
				 its durable build state is being deleted while it may still write"
			);
		}
	}
}
