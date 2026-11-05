//! The doc-ID deferral: what a build over the table's shared doc-ID space does with the mappings
//! a queued delete leaves behind ([`super::kind::Kind::defers_doc_ids`]).
//!
//! A queued delete defers the removal of its record's mapping so replay can still resolve the
//! doc-ID; the last building consumer of the space reclaims the mappings once no sibling still
//! needs them.

use std::borrow::Cow;
use std::collections::HashSet;

use anyhow::Result;

use super::IndexBuildPhase;
use super::builder::Building;
use super::kind::Kind;
use crate::catalog::providers::TableProvider;
use crate::ctx::FrozenContext;
use crate::idx::IndexKeyBase;
use crate::idx::docids::{DocId, TableDocIds};
use crate::key::schema::{
	DocLookupKey, DocLookupPrefix, DocPendingKey, DocPendingPrefix, RecordKey,
};
use crate::key::{KVKeyDecode, KVSubspace, Key};
use crate::kvs::{INDEXING_BATCH_SIZE, Transaction};
use crate::val::{Array, Number, Object, RecordIdKey, RecordIdentity, Value};

/// `id` and every id that differs from it only in the variant of a number
/// nested in it, up to `cap` of them: the ids the index format spells alike.
pub(super) fn numeric_variants(id: &RecordIdKey, cap: usize) -> Vec<RecordIdKey> {
	let rebuild = |v: Value| match v {
		Value::Array(a) => Some(RecordIdKey::Array(a)),
		Value::Object(o) => Some(RecordIdKey::Object(o)),
		Value::None
		| Value::Null
		| Value::Bool(_)
		| Value::Number(_)
		| Value::String(_)
		| Value::Duration(_)
		| Value::Datetime(_)
		| Value::Date(_)
		| Value::Time(_)
		| Value::Timestamp(_)
		| Value::Interval(_)
		| Value::Uuid(_)
		| Value::Set(_)
		| Value::Geometry(_)
		| Value::Bytes(_)
		| Value::Table(_)
		| Value::RecordId(_)
		| Value::File(_)
		| Value::Regex(_)
		| Value::Range(_)
		| Value::Closure(_) => None,
	};
	match id {
		RecordIdKey::Array(a) => {
			value_variants(&Value::Array(a.clone()), cap).into_iter().filter_map(rebuild).collect()
		}
		RecordIdKey::Object(o) => {
			value_variants(&Value::Object(o.clone()), cap).into_iter().filter_map(rebuild).collect()
		}
		other @ (RecordIdKey::Number(_)
		| RecordIdKey::String(_)
		| RecordIdKey::Uuid(_)
		| RecordIdKey::Range(_)) => vec![other.clone()],
	}
}

/// [`numeric_variants`] over a value.
pub(super) fn value_variants(value: &Value, cap: usize) -> Vec<Value> {
	match value {
		Value::Number(n) => {
			number_variants(n.clone()).into_iter().map(Value::Number).take(cap).collect()
		}
		Value::Array(a) => {
			let parts = a.0.iter().map(|v| value_variants(v, cap)).collect();
			capped_product(parts, cap).into_iter().map(|vs| Value::Array(Array(vs))).collect()
		}
		Value::Object(o) => {
			let keys: Vec<_> = o.0.iter().map(|(k, _)| k.clone()).collect();
			let parts = o.0.iter().map(|(_, v)| value_variants(v, cap)).collect();
			capped_product(parts, cap)
				.into_iter()
				.map(|vs| Value::Object(Object(keys.iter().cloned().zip(vs).collect())))
				.collect()
		}
		other @ (Value::None
		| Value::Null
		| Value::Bool(_)
		| Value::String(_)
		| Value::Duration(_)
		| Value::Datetime(_)
		| Value::Date(_)
		| Value::Time(_)
		| Value::Timestamp(_)
		| Value::Interval(_)
		| Value::Uuid(_)
		| Value::Set(_)
		| Value::Geometry(_)
		| Value::Bytes(_)
		| Value::Table(_)
		| Value::RecordId(_)
		| Value::File(_)
		| Value::Regex(_)
		| Value::Range(_)
		| Value::Closure(_)) => vec![other.clone()],
	}
}

/// The integer, float, decimal and bigdecimal a number's value can be written as, and a
/// bigdecimal as it is written: the index format spells a bigdecimal from its coefficient and
/// scale, as it spells a decimal.
pub(super) fn number_variants(n: Number) -> Vec<Number> {
	use rust_decimal::Decimal;
	use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
	let mut written = None;
	let exact = match n {
		Number::Int(i) => Decimal::from(i),
		Number::Float(f) => match Decimal::from_f64(f) {
			Some(d) => d,
			None => return vec![n],
		},
		Number::Decimal(d) => d,
		Number::I8(i) => Decimal::from(i),
		Number::U8(u) => Decimal::from(u),
		Number::I16(i) => Decimal::from(i),
		Number::U16(u) => Decimal::from(u),
		Number::I32(i) => Decimal::from(i),
		Number::U32(u) => Decimal::from(u),
		Number::U64(u) => Decimal::from(u),
		Number::F16(f) => match Decimal::from_f64(f.to_f64()) {
			Some(d) => d,
			None => return vec![n],
		},
		Number::F32(f) => match Decimal::from_f32(f) {
			Some(d) => d,
			None => return vec![n],
		},
		Number::BigDecimal(ref b) => match Decimal::try_from(b.as_ref()) {
			Ok(d) => {
				written = Some(n.clone());
				d
			}
			Err(_) => return vec![n],
		},
	};
	let mut out = Vec::with_capacity(5);
	if exact.is_integer()
		&& let Some(i) = exact.to_i64()
	{
		out.push(Number::Int(i));
	}
	if let Some(f) = exact.to_f64() {
		out.push(Number::Float(f));
	}
	out.push(Number::Decimal(exact));
	out.push(Number::BigDecimal(Box::new(surrealdb_types::BigDecimal::from(exact))));
	out.extend(written);
	out
}

/// Every choice of one value per part, in order, up to `cap` of them.
pub(super) fn capped_product(parts: Vec<Vec<Value>>, cap: usize) -> Vec<Vec<Value>> {
	let mut acc: Vec<Vec<Value>> = vec![Vec::new()];
	for choices in parts {
		let mut next = Vec::new();
		'fill: for prefix in &acc {
			for choice in &choices {
				if next.len() == cap {
					break 'fill;
				}
				let mut combined = prefix.clone();
				combined.push(choice.clone());
				next.push(combined);
			}
		}
		acc = next;
	}
	acc
}

/// Maximum consecutive commit-conflict retries for one deferred doc-ID reclaim
/// sweep chunk (and for its repair pass) before the sweep gives up.
///
/// The sweep runs after the build is durably `Online` and its `!dp` markers are
/// durable, so giving up loses nothing — leftover markers are drained by the
/// next doc-ID index build on the table. The bound exists so a conflict storm
/// cannot wedge the finished build task (and, for a blocking `DEFINE INDEX`,
/// the statement waiting on it).
pub(super) const DOC_ID_RECLAIM_MAX_RETRIES: usize = 10;

/// Most ids a pending-reclaim marker in a 3.3 pre-release's spelling is read as.
///
/// That spelling drops a nested number's variant, so the marker stands for
/// every id that differs only there — three per number, an integer, a float
/// and a decimal of one value. The bound covers an id with three numbers in
/// full and keeps a marker with more from costing a read per combination.
pub(super) const LEGACY_MARKER_MAX_READINGS: usize = 27;

impl Building {
	/// Whether another doc-ID-consuming index on the same table is still building
	/// (durable build phase `Building`/`Closing`), and so may still need the shared
	/// `!dj`/`!dd` mapping to replay its own copy of a queued delete.
	///
	/// Gates the reclaim in [`apply_appending`](Self::apply_appending): while a
	/// sibling is still building the mapping must be kept alive (each build resolves
	/// and removes its own entries against it), so the reclaim is deferred through a
	/// durable `!dp` marker and completed by
	/// [`reclaim_deferred_doc_ids`](Self::reclaim_deferred_doc_ids). The common case
	/// (a single building doc-ID index) finds no sibling and reclaims immediately;
	/// an already-`Online` sibling keeps its `!bs` state but is not counted, since
	/// it processed the delete synchronously.
	///
	/// The check counts a sibling by durable phase alone, deliberately ignoring
	/// owner-lease expiry: a stranded `Building` sibling is resumable, and its
	/// resumed build still needs the mapping to replay its unconsumed deletes, so
	/// keeping the mapping (and the durable marker) is the safe answer either way.
	pub(super) async fn other_doc_id_index_building(&self, ctx: &FrozenContext) -> Result<bool> {
		let tx = ctx.tx();
		let indexes =
			tx.all_tb_indexes(self.ix_key.ns, self.ix_key.db, self.ikb.table(), None).await?;
		for other in indexes.iter() {
			if other.index_id == self.ix.index_id || !Kind::of(other).defers_doc_ids() {
				continue;
			}
			let other_ikb = IndexKeyBase::new(
				self.ix_key.ns,
				self.ix_key.db,
				self.ikb.table().clone(),
				other.index_id,
			);
			if let Some(state) = tx.get_key(&other_ikb.new_bs_key(), None).await?
				&& matches!(state.phase, IndexBuildPhase::Building | IndexBuildPhase::Closing)
			{
				return Ok(true);
			}
		}
		Ok(false)
	}

	/// Records durably — in the same transaction that consumes the delete from
	/// this builder's queue — that the shared doc-ID mapping for `id` could not
	/// be reclaimed yet, because a sibling doc-ID index was still building and
	/// may still need it to replay its own copy of the delete.
	///
	/// The `!dp` marker is table-scoped, so it is visible to *every* doc-ID
	/// index build on the table — including builds that started after the delete
	/// and resumed/taken-over builds — and survives builder errors, aborts and
	/// crashes. It is consumed by
	/// [`reclaim_deferred_doc_ids`](Self::reclaim_deferred_doc_ids).
	pub(super) async fn defer_doc_id_reclaim(
		&self,
		tx: &Transaction,
		id: &RecordIdKey,
	) -> Result<()> {
		let dp = DocPendingKey::new(
			self.ix_key.ns,
			self.ix_key.db,
			Cow::Borrowed(self.ikb.table()),
			RecordIdentity(id.clone()),
		);
		tx.set_key(&dp, &()).await
	}

	/// Every record a pending-reclaim marker in a 3.3 pre-release's spelling can
	/// stand for, up to [`LEGACY_MARKER_MAX_READINGS`]: the id its bytes decode
	/// to under the index format, in each variant of every number nested in it.
	pub(super) fn legacy_marker_readings(&self, marker: &[u8]) -> Result<Vec<RecordIdKey>> {
		let table = Cow::Borrowed(self.ikb.table());
		let pending =
			DocPendingPrefix::new(self.ix_key.ns, self.ix_key.db, table.clone()).encode_bound()?;
		let Some(spelling) = marker.strip_prefix(&*pending) else {
			return Ok(Vec::new());
		};
		let mut erased =
			DocLookupPrefix::new(self.ix_key.ns, self.ix_key.db, table).encode_bound()?.to_vec();
		erased.extend_from_slice(spelling);
		let Ok(di) = DocLookupKey::decode_key(&erased) else {
			return Ok(Vec::new());
		};
		Ok(numeric_variants(&di.id, LEGACY_MARKER_MAX_READINGS))
	}

	/// Drains the table's durable `!dp` pending-reclaim markers, reclaiming the
	/// shared `!dj`/`!dd` mapping of every marked record that is still absent
	/// (see [`apply_appending`](Self::apply_appending)).
	///
	/// Runs best-effort, immediately after this builder publishes `Online`, and
	/// only when no sibling doc-ID index is still `Building`/`Closing` — the
	/// later-to-finish builder re-checks after its own `Online` commit, so it
	/// deterministically observes every earlier finisher as `Online`. If a
	/// sibling is still building, the sweep leaves the durable markers in place:
	/// they are drained by whichever doc-ID index build finishes next with no
	/// building sibling — including a build that started after the deletes, a
	/// resumed/taken-over build, or a later `REBUILD` — and
	/// [`TableDocIds::remove_all`] clears them when the table's last doc-ID
	/// index is dropped. A marker is therefore never lost, only completed later.
	///
	/// The sweep processes markers in [`INDEXING_BATCH_SIZE`] chunks, each in its
	/// own transaction, probing the marked records with one batched read:
	/// - a record that is still absent has its `!dj`/`!dd` mapping reclaimed;
	/// - a record re-created during the build keeps its live mapping;
	/// - the `!dp` marker is consumed in both cases.
	///
	/// A record re-created *concurrently with a sweep chunk* can adopt the old
	/// id from its snapshot (a pure read, which no backend validates at commit)
	/// while the chunk reclaims that same mapping. Each chunk therefore re-probes
	/// the records it reclaimed after committing and restores the mapping of any
	/// that turn out live (see [`TableDocIds::restore`]); the doubly-raced
	/// residual (a re-create overlapping both the chunk and its repair probe)
	/// self-heals on the record's next indexed write. Duplicate sweeps (two
	/// builders finishing in lock-step) are harmless: reclaim and restore are
	/// both idempotent put-if-absent/delete shapes.
	pub(super) async fn reclaim_deferred_doc_ids(&self) -> Result<()> {
		// Only doc-ID-consuming index builds sweep; other index kinds never
		// defer and skip the range probe entirely.
		if !self.kind().defers_doc_ids() {
			return Ok(());
		}
		#[cfg(test)]
		reclaim_sweep_park::wait(self.ctx.node_id()).await;
		let docs = TableDocIds::new(self.ix_key.ns, self.ix_key.db, self.ikb.table().clone());
		let mut retries = 0usize;
		loop {
			// The sweep runs after `Online`, so a `REMOVE INDEX` abort is the
			// only cancellation signal left to honour.
			if self.is_aborted().await {
				return Ok(());
			}
			let ctx = self.new_write_tx_ctx().await?;
			let tx = ctx.tx();
			// A still-building sibling may still need the mappings to replay its
			// queued deletes: leave the durable markers for a later finisher.
			if catch!(tx, self.other_doc_id_index_building(&ctx).await) {
				tx.cancel().await?;
				return Ok(());
			}
			// Fetch the next chunk of pending markers.
			let rng = catch!(
				tx,
				DocPendingPrefix::new(
					self.ix_key.ns,
					self.ix_key.db,
					Cow::Borrowed(self.ikb.table())
				)
				.range()
			);
			let keys = catch!(tx, tx.keys(rng, INDEXING_BATCH_SIZE, 0, None).await);
			if keys.is_empty() {
				tx.cancel().await?;
				return Ok(());
			}
			// A 3.3 pre-release spelled the id in a marker under the index format,
			// which drops a nested number's variant, and which spelling a marker
			// is in cannot be read off its bytes. The two overlap: an id under this
			// release's spelling encodes to the same bytes as one under the
			// pre-release's followed by `NONE`, `NULL` or a bool, because the tag
			// after the number lands where its kind would have been. So a marker
			// that decodes may still have been written for another record, and one
			// that does not decode names a record only the old spelling reaches.
			//
			// Every reading is taken, and none has to be ruled out: the rule this
			// sweep applies — reclaim a mapping only once its record is gone — is
			// the right one for whichever record a reading turns out to be. One
			// that is really a live record keeps its mapping; one that is absent
			// should not be holding a mapping in any case.
			let mut ids: Vec<RecordIdKey> = Vec::with_capacity(keys.len());
			let mut seen = HashSet::with_capacity(keys.len());
			let mut take = |id: RecordIdKey, ids: &mut Vec<RecordIdKey>| {
				if seen.insert(RecordIdentity(id.clone())) {
					ids.push(id);
				}
			};
			for k in &keys {
				if let Ok(dp) = DocPendingKey::decode_key(k) {
					let decoded = dp.id.0;
					// The two spellings differ only over numbers, so an id holding
					// none encodes alike under both and has no second reading.
					let alike = decoded.index_format_spells_alike();
					take(decoded, &mut ids);
					if alike {
						continue;
					}
				}
				for id in catch!(tx, self.legacy_marker_readings(k)) {
					take(id, &mut ids);
				}
			}
			// One batched probe decides every record's fate in this chunk.
			let record_keys: Vec<RecordKey> = ids.iter().map(|id| self.record_key(id)).collect();
			let records = catch!(tx, tx.getm_raw(record_keys, None).await);
			// Records reclaimed in this chunk, remembered for the post-commit
			// re-probe that repairs concurrent re-creates.
			let mut reclaimed: Vec<(RecordIdKey, DocId)> = Vec::new();
			for (id, existing) in ids.iter().zip(records) {
				if existing.is_none()
					&& let Some(doc_id) = catch!(tx, docs.remove(&tx, id).await)
				{
					reclaimed.push((id.clone(), doc_id));
				}
			}
			// The marker is consumed whether the mapping was reclaimed (record
			// absent), kept (record re-created during the build), already gone
			// (reclaimed by an earlier inline pass or sweep), or named a record
			// that could no longer be recovered at all. The key deleted is the one
			// the scan returned: a marker in a 3.3 pre-release's spelling re-encodes to
			// this release's, which is not the key on disk.
			for k in &keys {
				catch!(tx, tx.del(Key::from(k.as_slice())).await);
			}
			if self
				.commit_and_retryable_conflict(
					&tx,
					"transient conflict reclaiming deferred doc-ID mappings, retrying",
				)
				.await?
			{
				// Retry the same chunk: the markers are still durable. Bounded so
				// a conflict storm cannot wedge the (already Online) build task —
				// leftover markers are picked up by the next doc-ID index build.
				retries += 1;
				if retries > DOC_ID_RECLAIM_MAX_RETRIES {
					warn!(
						index = %self.ix.name,
						table = %self.ix.table_name,
						"giving up the deferred doc-ID reclaim sweep after repeated \
						 conflicts; leftover markers will be reclaimed by the next \
						 doc-ID index build on the table"
					);
					return Ok(());
				}
				continue;
			}
			retries = 0;
			// Repair the mappings of records re-created while the chunk committed.
			self.repair_recreated_doc_ids(&docs, &reclaimed).await?;
		}
	}

	/// Restores the `!dj`/`!dd` mapping of any record in `reclaimed` that exists
	/// again — i.e. was re-created concurrently with the sweep chunk that
	/// reclaimed its mapping, adopting the old id from its pre-reclaim snapshot.
	///
	/// [`TableDocIds::restore`] claims the forward mapping with a put-if-absent,
	/// so a record that instead re-acquired a *fresh* id after the reclaim is
	/// left untouched. Best-effort like the sweep itself: on repeated conflicts
	/// the remaining repairs are abandoned with a warning, and the affected
	/// record self-heals on its next indexed write.
	pub(super) async fn repair_recreated_doc_ids(
		&self,
		docs: &TableDocIds,
		reclaimed: &[(RecordIdKey, DocId)],
	) -> Result<()> {
		if reclaimed.is_empty() {
			return Ok(());
		}
		let mut retries = 0usize;
		loop {
			let ctx = self.new_write_tx_ctx().await?;
			let tx = ctx.tx();
			let record_keys: Vec<RecordKey> =
				reclaimed.iter().map(|(id, _)| self.record_key(id)).collect();
			let records = catch!(tx, tx.getm_raw(record_keys, None).await);
			let mut restored = 0usize;
			for ((id, doc_id), existing) in reclaimed.iter().zip(records) {
				if existing.is_some() && catch!(tx, docs.restore(&tx, id, *doc_id).await) {
					restored += 1;
				}
			}
			if restored == 0 {
				tx.cancel().await?;
				return Ok(());
			}
			if !self
				.commit_and_retryable_conflict(
					&tx,
					"transient conflict repairing re-created doc-ID mappings, retrying",
				)
				.await?
			{
				warn!(
					index = %self.ix.name,
					table = %self.ix.table_name,
					restored,
					"restored doc-ID mappings of records re-created concurrently \
					 with the deferred reclaim sweep"
				);
				return Ok(());
			}
			retries += 1;
			if retries > DOC_ID_RECLAIM_MAX_RETRIES {
				warn!(
					index = %self.ix.name,
					table = %self.ix.table_name,
					"giving up doc-ID mapping repair after repeated conflicts; \
					 affected records re-index on their next write"
				);
				return Ok(());
			}
		}
	}
}

/// Holds the deferred doc-ID reclaim sweeps a node starts, for tests that
/// act while a sweep runs.
#[cfg(test)]
pub(super) mod reclaim_sweep_park {
	use std::collections::HashMap;
	use std::sync::{Arc, LazyLock, Mutex};

	use tokio::sync::Notify;
	use tokio_util::sync::CancellationToken;
	use uuid::Uuid;

	struct Park {
		parked: Notify,
		release: CancellationToken,
	}

	static PARKS: LazyLock<Mutex<HashMap<Uuid, Arc<Park>>>> = LazyLock::new(Default::default);

	/// Holds every sweep `node` starts, until the returned guard drops.
	pub(crate) fn park(node: Uuid) -> Guard {
		let park = Arc::new(Park {
			parked: Notify::new(),
			release: CancellationToken::new(),
		});
		PARKS.lock().expect("park lock").insert(node, Arc::clone(&park));
		Guard {
			node,
			park,
		}
	}

	/// Waits here while `node`'s sweeps are held.
	pub(super) async fn wait(node: Uuid) {
		let park = PARKS.lock().expect("park lock").get(&node).map(Arc::clone);
		if let Some(park) = park {
			park.parked.notify_one();
			park.release.cancelled().await;
		}
	}

	pub(crate) struct Guard {
		node: Uuid,
		park: Arc<Park>,
	}

	impl Guard {
		/// Resolves once a sweep is held.
		pub(crate) async fn parked(&self) {
			self.park.parked.notified().await;
		}
	}

	impl Drop for Guard {
		fn drop(&mut self) {
			PARKS.lock().expect("park lock").remove(&self.node);
			self.park.release.cancel();
		}
	}
}
