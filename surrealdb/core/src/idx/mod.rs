pub mod docids;
pub(crate) mod entry;
pub(crate) mod error;
pub(crate) mod ft;
pub(crate) mod index;
pub(crate) mod keys;
pub mod planner;
pub mod trees;

use std::borrow::Cow;
use std::fmt::{Debug, Display};
use std::sync::Arc;

use anyhow::Result;
use uuid::Uuid;

pub(crate) use self::error::{Error, index_exists_record};
use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::err::Error as CoreError;
use crate::idx::docids::DocId;
use crate::idx::trees::hnsw::ElementId;
use crate::idx::trees::vector::SerializedVector;
use crate::key::schema::{
	BuildAppendGenerationPrefix, BuildAppendIxPrefix, BuildAppendKey, BuildAppendTicketPrefix,
	BuildPrimaryGenerationPrefix, BuildPrimaryIxPrefix, BuildPrimaryKey,
	BuildReservationGenerationPrefix, BuildReservationIxPrefix, BuildReservationKey, BuildStateKey,
	BuildTicketIxPrefix, BuildTicketKey, DocCountKey, DocLengthKey, DocStatsDeltaKey, DocStatsKey,
	HnswElementHashedKey, HnswElementKey, HnswGenerationKey, HnswLayerKey, HnswLayerLayerPrefix,
	HnswNodeKey, HnswNodeLayerPrefix, HnswPendingRoot, HnswRecordPendingKey,
	HnswRecordPendingPrefix, HnswStateKey, HnswVectorKey, IndexAppendKey, IndexAppendPrefix,
	IndexCompactionKey, IndexPrimaryKey, IndexVersionKey, TermChangeKey, TermChangeSetKey,
	TermChangesKey, TermDocsKey, TermGenerationKey, TermPostingKey,
};
#[cfg(diskann)]
use crate::key::schema::{
	DiskannElementDocsKey, DiskannElementHashedKey, DiskannElementKey, DiskannGenerationKey,
	DiskannNodeKey, DiskannPendingKey, DiskannPendingLegacyKey, DiskannRecordPendingKey,
	DiskannRecordPendingPrefix, DiskannRecordPendingShardKey, DiskannRecordPendingShardShardPrefix,
	DiskannStateKey,
};
use crate::key::{KVKey, Key, RawRange, TypedRange};
use crate::kvs::index::{
	Appending, AppendingId, BatchId, BuildGeneration, BuildTicket, BuildTicketMutationSeq,
	IndexBuildReservation, PrimaryAppendingTicket,
};
use crate::kvs::{Error as KvsError, Transaction};
use crate::val::{RecordIdKey, TableName};

/// Reads a compaction generation key.
///
/// Missing generation keys are equivalent to generation `0`
pub(in crate::idx) async fn read_compaction_generation<K>(
	tx: &Transaction,
	key: &K,
) -> Result<Option<u64>>
where
	K: KVKey<Value = u64> + Debug,
{
	tx.get_key(key, None).await
}

/// Advances a compaction generation with a conditional write.
///
/// Returns `false` when the stored generation differs from `current`, so the
/// caller can skip a plan built from an older snapshot.
pub(in crate::idx) async fn bump_compaction_generation<K>(
	tx: &Transaction,
	key: &K,
	current: Option<u64>,
) -> Result<bool>
where
	K: KVKey<Value = u64> + Debug,
{
	let next = current.unwrap_or(0).saturating_add(1);
	match tx.put_compare_key(key, &next, current.as_ref()).await {
		Ok(()) => Ok(true),
		Err(e) if is_transaction_condition_not_met(&e) => Ok(false),
		Err(e) => Err(e),
	}
}

/// Identifies the datastore error used for failed conditional writes/deletes.
pub(in crate::idx) fn is_transaction_condition_not_met(e: &anyhow::Error) -> bool {
	if matches!(
		e.downcast_ref::<CoreError>(),
		Some(CoreError::Kvs(KvsError::TransactionConditionNotMet))
	) {
		return true;
	}
	matches!(e.downcast_ref::<KvsError>(), Some(KvsError::TransactionConditionNotMet))
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
#[repr(transparent)]
pub struct IndexKeyBase(Arc<Inner>);

#[derive(Debug, Hash, PartialEq, Eq)]
struct Inner {
	ns: NamespaceId,
	db: DatabaseId,
	tb: TableName,
	ix: IndexId,
}

impl Display for IndexKeyBase {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "NS: {} - DB: {} - TB: {} - IX: {}", self.0.ns, self.0.db, self.0.tb, self.0.ix.0)
	}
}

impl IndexKeyBase {
	pub fn new(ns: NamespaceId, db: DatabaseId, tb: TableName, ix: IndexId) -> Self {
		Self(Arc::new(Inner {
			ns,
			db,
			tb,
			ix,
		}))
	}

	fn new_he_key(&self, element_id: ElementId) -> HnswVectorKey<'_> {
		HnswVectorKey {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
			element_id,
		}
	}

	/// Range covering append-keyed HNSW pending updates.
	fn new_hp_range(&self) -> Result<RawRange> {
		HnswPendingRoot {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
		}
		.range()
	}

	/// Key storing the HNSW pending compaction generation.
	fn new_hg_key(&self) -> HnswGenerationKey<'_> {
		HnswGenerationKey {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
		}
	}

	/// Key storing the pending HNSW update for one record.
	fn new_hr_key<'a>(&'a self, id: &'a RecordIdKey) -> HnswRecordPendingKey<'a> {
		HnswRecordPendingKey {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
			id: Cow::Borrowed(id),
		}
	}

	/// Range covering record-keyed HNSW pending updates.
	fn new_hr_range(&self) -> Result<TypedRange<crate::idx::trees::hnsw::HnswRecordPendingUpdate>> {
		HnswRecordPendingPrefix {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
		}
		.range()
	}

	fn new_hl_key(&self, layer: u16, chunk: u32) -> HnswLayerKey<'_> {
		HnswLayerKey {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
			layer,
			chunk,
		}
	}

	/// Returns a key range covering all legacy `HnswLayerKey` chunk entries for the given HNSW
	/// layer.
	fn new_hl_layer_range(&self, layer: u16) -> Result<TypedRange<Vec<u8>>> {
		HnswLayerLayerPrefix {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
			layer,
		}
		.range()
	}

	/// Creates a per-node `Hn` key for storing a single node's edge list in an HNSW layer.
	fn new_hn_key(&self, layer: u16, node: ElementId) -> HnswNodeKey<'_> {
		HnswNodeKey {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
			layer,
			node,
		}
	}

	/// Returns a key range covering all per-node `Hn` entries for the given HNSW layer.
	fn new_hn_layer_range(&self, layer: u16) -> Result<TypedRange<Vec<u8>>> {
		HnswNodeLayerPrefix {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
			layer,
		}
		.range()
	}

	fn new_hv_key<'a>(&'a self, vec: &'a SerializedVector) -> HnswElementKey<'a> {
		HnswElementKey {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
			vec: Cow::Borrowed(vec),
		}
	}

	fn new_hh_key(&self, hash: [u8; 32]) -> HnswElementHashedKey<'_> {
		HnswElementHashedKey {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
			hash,
		}
	}

	fn new_hs_key(&self) -> HnswStateKey<'_> {
		HnswStateKey {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
		}
	}

	/// Key storing a DiskANN graph element vector/status payload.
	#[cfg(diskann)]
	fn new_de_key(&self, element_id: ElementId) -> DiskannElementKey<'_> {
		DiskannElementKey {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
			element_id,
		}
	}

	/// Range covering all DiskANN graph element payloads.
	#[cfg(diskann)]
	fn new_de_range(&self) -> Result<TypedRange<crate::idx::trees::diskann::DiskAnnElement>> {
		use crate::key::schema::DiskannElementPrefix;

		DiskannElementPrefix {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
		}
		.range()
	}

	/// Key storing the DiskANN pending compaction generation.
	#[cfg(diskann)]
	fn new_dg_key(&self) -> DiskannGenerationKey<'_> {
		DiskannGenerationKey {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
		}
	}

	/// Key mapping a vector hash to DiskANN hashed-vector document mappings.
	#[cfg(diskann)]
	fn new_dh_key(&self, hash: [u8; 32]) -> DiskannElementHashedKey<'_> {
		DiskannElementHashedKey {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
			hash,
		}
	}

	/// Key storing one DiskANN graph adjacency list.
	#[cfg(diskann)]
	fn new_dn_key(&self, element_id: ElementId) -> DiskannNodeKey<'_> {
		DiskannNodeKey {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
			element_id,
		}
	}

	/// Key storing one shard of the legacy DiskANN pending-state guard (tracks `!dr` records).
	///
	/// New code never constructs this — the sharded layout uses [`Self::new_dy_key`], and old nodes
	/// own `!dp`. Retained for tests that simulate a pre-change node and for the legacy on-disk
	/// family; hence `dead_code` in a non-test build.
	#[cfg(diskann)]
	#[allow(dead_code)]
	fn new_dp_key(&self, shard: u16) -> DiskannPendingLegacyKey<'_> {
		DiskannPendingLegacyKey {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
			shard,
		}
	}

	/// Key storing one shard of the sharded DiskANN pending-state guard (tracks `!dw` records).
	///
	/// Separate from `!dp` so a pre-change node's compactor — which only knows `!dp`/`!dr` — cannot
	/// clear the guard for sharded data it can't see during a mixed-version rolling upgrade.
	#[cfg(diskann)]
	fn new_dy_key(&self, shard: u16) -> DiskannPendingKey<'_> {
		DiskannPendingKey {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
			shard,
		}
	}

	/// Key mapping an exact serialized vector to its DiskANN document set.
	#[cfg(diskann)]
	fn new_dq_key<'a>(&'a self, vec: &'a SerializedVector) -> DiskannElementDocsKey<'a> {
		DiskannElementDocsKey {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
			vec: Cow::Borrowed(vec),
		}
	}

	/// Key storing one shard's sharded pending DiskANN update (`!dw{shard}{id}`) for one record.
	///
	/// `shard` is the writer's pending-state shard (see `pending_state_shard`); prefixing it lets
	/// compaction drain — and lookup scan — one shard at a time. New writes always use this layout.
	#[cfg(diskann)]
	fn new_dw_key<'a>(
		&'a self,
		shard: u16,
		id: &'a RecordIdKey,
	) -> DiskannRecordPendingShardKey<'a> {
		DiskannRecordPendingShardKey {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
			shard,
			id: Cow::Borrowed(id),
		}
	}

	/// Range covering the sharded `!dw` pending updates for one shard.
	#[cfg(diskann)]
	fn new_dw_shard_range(
		&self,
		shard: u16,
	) -> Result<TypedRange<crate::idx::trees::diskann::DiskAnnRecordPendingUpdate>> {
		DiskannRecordPendingShardShardPrefix {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
			shard,
		}
		.range()
	}

	/// Key storing the legacy unsharded pending DiskANN update for one record.
	///
	/// New writes use the sharded `!dw` layout; this legacy key is read and deleted by the write
	/// path's dual-read fold, and scanned/range-deleted by lookup and compaction, until the legacy
	/// range drains empty.
	#[cfg(diskann)]
	fn new_dr_key<'a>(&'a self, id: &'a RecordIdKey) -> DiskannRecordPendingKey<'a> {
		DiskannRecordPendingKey {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
			id: Cow::Borrowed(id),
		}
	}

	/// Range covering legacy unsharded record-keyed DiskANN pending updates.
	#[cfg(diskann)]
	fn new_dr_range(
		&self,
	) -> Result<TypedRange<crate::idx::trees::diskann::DiskAnnRecordPendingUpdate>> {
		DiskannRecordPendingPrefix {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
		}
		.range()
	}

	/// Key storing the DiskANN graph state.
	#[cfg(diskann)]
	fn new_ds_key(&self) -> DiskannStateKey<'_> {
		DiskannStateKey {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
		}
	}

	pub(crate) fn new_ig_key(
		&self,
		appending_id: AppendingId,
		batch_id: BatchId,
	) -> IndexAppendKey<'_> {
		IndexAppendKey {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
			appending_id,
			batch_id,
		}
	}

	pub(crate) fn new_ig_range(&self) -> Result<TypedRange<Appending>> {
		IndexAppendPrefix {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
		}
		.range()
	}

	pub(crate) fn new_ip_key(&self, id: RecordIdKey) -> IndexPrimaryKey<'_> {
		IndexPrimaryKey {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
			id,
		}
	}

	/// Key storing durable build state for this table index.
	pub(crate) fn new_bs_key(&self) -> BuildStateKey<'_> {
		BuildStateKey {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
		}
	}

	/// Key storing the writer-admission ticket counter for a build generation.
	pub(crate) fn new_bt_key(&self, generation: BuildGeneration) -> BuildTicketKey<'_> {
		BuildTicketKey {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
			generation,
		}
	}

	/// Range covering the ticket counter of one build generation.
	/// Ticket counters of every generation below `below`.
	///
	/// The bound is on the generation field, so nothing has to reach into a range's
	/// bytes to move its end.
	pub(crate) fn new_bt_range_below(
		&self,
		below: BuildGeneration,
	) -> Result<TypedRange<BuildTicket>> {
		BuildTicketIxPrefix {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
		}
		.range_where(..below)
	}

	/// Range covering the ticket counters of every generation of this index.
	pub(crate) fn new_bt_all_generations_range(&self) -> Result<TypedRange<BuildTicket>> {
		BuildTicketIxPrefix {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
		}
		.range()
	}

	/// Key storing one durable writer reservation for a build generation.
	pub(crate) fn new_br_key(
		&self,
		generation: BuildGeneration,
		ticket: BuildTicket,
	) -> BuildReservationKey<'_> {
		BuildReservationKey {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
			generation,
			ticket,
		}
	}

	/// Range covering writer reservations for one build generation.
	pub(crate) fn new_br_range(
		&self,
		generation: BuildGeneration,
	) -> Result<TypedRange<IndexBuildReservation>> {
		BuildReservationGenerationPrefix {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
			generation,
		}
		.range()
	}

	/// Range covering writer reservations across all generations of this index.
	/// As `new_br_all_generations_range`, but only generations below `below`.
	pub(crate) fn new_br_range_below(
		&self,
		below: BuildGeneration,
	) -> Result<TypedRange<IndexBuildReservation>> {
		BuildReservationIxPrefix {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
		}
		.range_where(..below)
	}

	pub(crate) fn new_br_all_generations_range(&self) -> Result<TypedRange<IndexBuildReservation>> {
		BuildReservationIxPrefix {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
		}
		.range()
	}

	/// Key storing one durable queued mutation for a build generation.
	pub(crate) fn new_bg_key(
		&self,
		generation: BuildGeneration,
		ticket: BuildTicket,
		mutation_seq: BuildTicketMutationSeq,
	) -> BuildAppendKey<'_> {
		BuildAppendKey {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
			generation,
			ticket,
			mutation_seq,
		}
	}

	/// Range covering durable queued mutations for one build generation.
	pub(crate) fn new_bg_range(
		&self,
		generation: BuildGeneration,
	) -> Result<TypedRange<Appending>> {
		BuildAppendGenerationPrefix {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
			generation,
		}
		.range()
	}

	/// Range covering every durable queued mutation that shares one reservation
	/// ticket within a build generation — used by `wait_for_durable_reservations`
	/// to decide whether a writer has committed any of its batched mutations.
	pub(crate) fn new_bg_ticket_range(
		&self,
		generation: BuildGeneration,
		ticket: BuildTicket,
	) -> Result<TypedRange<Appending>> {
		BuildAppendTicketPrefix {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
			generation,
			ticket,
		}
		.range()
	}

	/// Range covering durable queued mutations across all generations of this index.
	/// As `new_bg_all_generations_range`, but only generations below `below`.
	pub(crate) fn new_bg_range_below(
		&self,
		below: BuildGeneration,
	) -> Result<TypedRange<Appending>> {
		BuildAppendIxPrefix {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
		}
		.range_where(..below)
	}

	pub(crate) fn new_bg_all_generations_range(&self) -> Result<TypedRange<Appending>> {
		BuildAppendIxPrefix {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
		}
		.range()
	}

	/// Key mapping a record to its first queued mutation during the initial scan.
	pub(crate) fn new_bp_key<'a>(
		&'a self,
		generation: BuildGeneration,
		id: &'a RecordIdKey,
	) -> BuildPrimaryKey<'a> {
		BuildPrimaryKey {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
			generation,
			id: Cow::Borrowed(id),
		}
	}

	/// Range covering primary-appending markers for one build generation.
	///
	/// Only the build tests read a whole generation's markers; the engine deletes
	/// them by generation span instead, via [`Self::new_bp_range_below`]. Gated to
	/// exactly the configuration that uses it, so it cannot quietly become an
	/// unused bound.
	#[cfg(all(test, feature = "kv-mem"))]
	pub(crate) fn new_bp_range(
		&self,
		generation: BuildGeneration,
	) -> Result<TypedRange<PrimaryAppendingTicket>> {
		use crate::key::schema::BuildPrimaryGenerationPrefix;

		BuildPrimaryGenerationPrefix {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
			generation,
		}
		.range()
	}

	/// Range covering a primary-appending record-id span for one build generation.
	pub(crate) fn new_bp_span_range(
		&self,
		generation: BuildGeneration,
		after: Option<&RecordIdKey>,
		through: Option<&RecordIdKey>,
	) -> Result<TypedRange<PrimaryAppendingTicket>> {
		use std::ops::Bound;

		let start = match after {
			Some(after) => Bound::Excluded(Cow::Borrowed(after)),
			None => Bound::Unbounded,
		};
		let end = match through {
			Some(through) => Bound::Included(Cow::Borrowed(through)),
			None => Bound::Unbounded,
		};
		BuildPrimaryGenerationPrefix {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
			generation,
		}
		.range_where((start, end))
	}

	/// Range covering primary-appending markers across all generations of this index.
	/// As `new_bp_all_generations_range`, but only generations below `below`.
	pub(crate) fn new_bp_range_below(
		&self,
		below: BuildGeneration,
	) -> Result<TypedRange<PrimaryAppendingTicket>> {
		BuildPrimaryIxPrefix {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
		}
		.range_where(..below)
	}

	pub(crate) fn new_bp_all_generations_range(
		&self,
	) -> Result<TypedRange<PrimaryAppendingTicket>> {
		BuildPrimaryIxPrefix {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
		}
		.range()
	}

	pub(crate) fn new_ic_key(&self, nid: Uuid) -> IndexCompactionKey<'_> {
		IndexCompactionKey {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
			nid,
			uid: Uuid::now_v7(),
		}
	}

	fn new_td_root<'a>(&'a self, term: &'a str) -> TermDocsKey<'a> {
		TermDocsKey {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
			term: Cow::Borrowed(term),
		}
	}

	fn new_td<'a>(&'a self, term: &'a str, doc_id: DocId) -> TermPostingKey<'a> {
		TermPostingKey {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
			term: Cow::Borrowed(term),
			id: doc_id,
		}
	}

	fn new_tt<'a>(
		&'a self,
		term: &'a str,
		doc_id: DocId,
		nid: Uuid,
		uid: Uuid,
		add: bool,
	) -> TermChangeKey<'a> {
		TermChangeKey {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
			term: Cow::Borrowed(term),
			doc_id,
			nid,
			uid,
			add,
		}
	}

	fn new_tt_term_range<'a>(&'a self, term: &'a str) -> Result<TypedRange<String>> {
		TermChangeSetKey {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
			term: Cow::Borrowed(term),
		}
		.range()
	}

	fn new_tt_terms_range(&self) -> Result<TypedRange<String>> {
		TermChangesKey {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
		}
		.range()
	}

	/// Generation guard for full-text term-document (`!tt`) compaction.
	fn new_tv_key(&self) -> TermGenerationKey<'_> {
		TermGenerationKey {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
		}
	}

	fn new_dc_with_id(&self, doc_id: DocId, nid: Uuid, uid: Uuid) -> DocStatsDeltaKey<'_> {
		DocStatsDeltaKey {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
			doc_id,
			nid,
			uid,
		}
	}

	fn new_dc_compacted(&self) -> Result<Key<'static>> {
		DocStatsKey {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
		}
		.encode_key()
	}

	/// Generation guard for full-text document-stat (`!dc`) compaction.
	fn new_dv_key(&self) -> DocCountKey<'_> {
		DocCountKey {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
		}
	}

	fn new_dl(&self, doc_id: DocId) -> DocLengthKey<'_> {
		DocLengthKey {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
			id: doc_id,
		}
	}

	/// Generation guard for count-index (`!iu`) compaction.
	pub(crate) fn new_iv_key(&self) -> IndexVersionKey<'_> {
		IndexVersionKey {
			ns: self.0.ns,
			db: self.0.db,
			tb: Cow::Borrowed(&self.0.tb),
			ix: self.0.ix,
		}
	}

	pub(crate) fn ns(&self) -> NamespaceId {
		self.0.ns
	}

	pub(crate) fn db(&self) -> DatabaseId {
		self.0.db
	}

	pub(crate) fn table(&self) -> &TableName {
		&self.0.tb
	}

	pub(crate) fn index(&self) -> IndexId {
		self.0.ix
	}
}
