pub(crate) mod ft;
pub(crate) mod index;
pub mod planner;
pub(super) mod seqdocids;
pub mod trees;

use std::borrow::Cow;
use std::fmt::{Debug, Display};
use std::ops::Range;
use std::sync::Arc;

use anyhow::Result;
use uuid::Uuid;

use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::err::Error;
use crate::idx::seqdocids::DocId;
use crate::idx::trees::hnsw::ElementId;
use crate::idx::trees::vector::SerializedVector;
use crate::key::index::dc::Dc;
#[cfg(diskann)]
use crate::key::index::dd::{Dd, DdRoot};
#[cfg(diskann)]
use crate::key::index::de::De;
#[cfg(diskann)]
use crate::key::index::dg::Dg;
#[cfg(diskann)]
use crate::key::index::dh::Dh;
#[cfg(diskann)]
use crate::key::index::di::Di;
use crate::key::index::dl::Dl;
#[cfg(diskann)]
use crate::key::index::dn::Dn;
#[cfg(diskann)]
use crate::key::index::dp::Dp;
#[cfg(diskann)]
use crate::key::index::dq::Dq;
#[cfg(diskann)]
use crate::key::index::dr::{DiskAnnRecordPending, DiskAnnRecordPendingPrefix};
#[cfg(diskann)]
use crate::key::index::ds::Ds;
use crate::key::index::dv::Dv;
use crate::key::index::hd::{Hd, HdRoot};
use crate::key::index::he::He;
use crate::key::index::hg::Hg;
use crate::key::index::hh::Hh;
use crate::key::index::hi::Hi;
use crate::key::index::hl::Hl;
use crate::key::index::hn::HnswNode;
use crate::key::index::hp::HnswPendingPrefix;
use crate::key::index::hr::{HnswRecordPending, HnswRecordPendingPrefix};
use crate::key::index::hs::Hs;
use crate::key::index::hv::Hv;
use crate::key::index::ib::Ib;
use crate::key::index::id::Id as IdKey;
use crate::key::index::ig::IndexAppending;
use crate::key::index::ii::Ii;
use crate::key::index::ip::Ip;
use crate::key::index::is::Is;
use crate::key::index::iv::Iv;
use crate::key::index::td::{Td, TdRoot};
use crate::key::index::tt::Tt;
use crate::key::index::tv::Tv;
use crate::key::root::ic::IndexCompactionKey;
use crate::key::table::bg::Bg;
use crate::key::table::bp::Bp;
use crate::key::table::br::Br;
use crate::key::table::bs::Bs;
use crate::kvs::index::{
	AppendingId, BatchId, BuildGeneration, BuildTicket, BuildTicketMutationSeq,
};
use crate::kvs::{Error as KvsError, KVKey, Key, Transaction};
use crate::val::{RecordIdKey, TableName};

/// Reads a compaction generation key.
///
/// Missing generation keys are equivalent to generation `0`.
pub(in crate::idx) async fn read_compaction_generation<K>(
	tx: &Transaction,
	key: &K,
) -> Result<Option<u64>>
where
	K: KVKey<ValueType = u64> + Debug,
{
	tx.get(key, None).await
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
	K: KVKey<ValueType = u64> + Debug,
{
	let next = current.unwrap_or(0).saturating_add(1);
	match tx.putc(key, &next, current.as_ref()).await {
		Ok(()) => Ok(true),
		Err(e) if is_transaction_condition_not_met(&e) => Ok(false),
		Err(e) => Err(e),
	}
}

/// Identifies the datastore error used for failed conditional writes/deletes.
pub(in crate::idx) fn is_transaction_condition_not_met(e: &anyhow::Error) -> bool {
	if matches!(e.downcast_ref::<Error>(), Some(Error::Kvs(KvsError::TransactionConditionNotMet))) {
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

	fn new_hd_root_key(&self) -> HdRoot<'_> {
		HdRoot::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix)
	}

	fn new_hd_key(&self, doc_id: DocId) -> Hd<'_> {
		Hd::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, doc_id)
	}

	fn new_he_key(&self, element_id: ElementId) -> He<'_> {
		He::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, element_id)
	}

	fn new_hi_key<'a>(&'a self, id: &'a RecordIdKey) -> Hi<'a> {
		Hi::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, id)
	}

	/// Range covering append-keyed HNSW pending updates.
	fn new_hp_range(&self) -> Result<Range<Key>> {
		HnswPendingPrefix::range(self.0.ns, self.0.db, &self.0.tb, self.0.ix)
	}

	/// Key storing the HNSW pending compaction generation.
	fn new_hg_key(&self) -> Hg<'_> {
		Hg::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix)
	}

	/// Key storing the pending HNSW update for one record.
	fn new_hr_key<'a>(&'a self, id: &'a RecordIdKey) -> HnswRecordPending<'a> {
		HnswRecordPending::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, id)
	}

	/// Range covering record-keyed HNSW pending updates.
	fn new_hr_range(&self) -> Result<Range<Key>> {
		HnswRecordPendingPrefix::range(self.0.ns, self.0.db, &self.0.tb, self.0.ix)
	}

	fn new_hl_key(&self, layer: u16, chunk: u32) -> Hl<'_> {
		Hl::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, layer, chunk)
	}

	/// Returns a key range covering all legacy `Hl` chunk entries for the given HNSW layer.
	fn new_hl_layer_range(&self, layer: u16) -> Result<Range<Key>> {
		Hl::new_layer_range(self.0.ns, self.0.db, &self.0.tb, self.0.ix, layer)
	}

	/// Creates a per-node `Hn` key for storing a single node's edge list in an HNSW layer.
	fn new_hn_key(&self, layer: u16, node: ElementId) -> HnswNode<'_> {
		HnswNode::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, layer, node)
	}

	/// Returns a key range covering all per-node `Hn` entries for the given HNSW layer.
	fn new_hn_layer_range(&self, layer: u16) -> Result<Range<Key>> {
		HnswNode::new_layer_range(self.0.ns, self.0.db, &self.0.tb, self.0.ix, layer)
	}

	fn new_hv_key<'a>(&'a self, vec: &'a SerializedVector) -> Hv<'a> {
		Hv::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, vec)
	}

	fn new_hh_key(&self, hash: [u8; 32]) -> Hh<'_> {
		Hh::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, hash)
	}

	fn new_hs_key(&self) -> Hs<'_> {
		Hs::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix)
	}

	/// Root key storing DiskANN document-id allocator state.
	#[cfg(diskann)]
	fn new_dd_root_key(&self) -> DdRoot<'_> {
		DdRoot::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix)
	}

	/// Key mapping a compact DiskANN document ID to a record key.
	#[cfg(diskann)]
	fn new_dd_key(&self, doc_id: DocId) -> Dd<'_> {
		Dd::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, doc_id)
	}

	/// Key storing a DiskANN graph element vector/status payload.
	#[cfg(diskann)]
	fn new_de_key(&self, element_id: ElementId) -> De<'_> {
		De::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, element_id)
	}

	/// Range covering all DiskANN graph element payloads.
	#[cfg(diskann)]
	fn new_de_range(&self) -> Result<Range<Key>> {
		De::range(self.0.ns, self.0.db, &self.0.tb, self.0.ix)
	}

	/// Key storing the DiskANN pending compaction generation.
	#[cfg(diskann)]
	fn new_dg_key(&self) -> Dg<'_> {
		Dg::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix)
	}

	/// Key mapping a vector hash to DiskANN hashed-vector document mappings.
	#[cfg(diskann)]
	fn new_dh_key(&self, hash: [u8; 32]) -> Dh<'_> {
		Dh::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, hash)
	}

	/// Key mapping a record key to a compact DiskANN document ID.
	#[cfg(diskann)]
	fn new_di_key<'a>(&'a self, id: &'a RecordIdKey) -> Di<'a> {
		Di::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, id)
	}

	/// Key storing one DiskANN graph adjacency list.
	#[cfg(diskann)]
	fn new_dn_key(&self, element_id: ElementId) -> Dn<'_> {
		Dn::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, element_id)
	}

	/// Key storing one shard of the distributed-safe DiskANN pending-state guard.
	#[cfg(diskann)]
	fn new_dp_key(&self, shard: u16) -> Dp<'_> {
		Dp::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, shard)
	}

	/// Key mapping an exact serialized vector to its DiskANN document set.
	#[cfg(diskann)]
	fn new_dq_key<'a>(&'a self, vec: &'a SerializedVector) -> Dq<'a> {
		Dq::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, vec)
	}

	/// Key storing the pending DiskANN update for one record.
	#[cfg(diskann)]
	fn new_dr_key<'a>(&'a self, id: &'a RecordIdKey) -> DiskAnnRecordPending<'a> {
		DiskAnnRecordPending::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, id)
	}

	/// Range covering record-keyed DiskANN pending updates.
	#[cfg(diskann)]
	fn new_dr_range(&self) -> Result<Range<Key>> {
		DiskAnnRecordPendingPrefix::range(self.0.ns, self.0.db, &self.0.tb, self.0.ix)
	}

	/// Key storing the DiskANN graph state.
	#[cfg(diskann)]
	fn new_ds_key(&self) -> Ds<'_> {
		Ds::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix)
	}

	fn new_ii_key(&self, doc_id: DocId) -> Ii<'_> {
		Ii::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, doc_id)
	}

	fn new_id_key(&self, id: RecordIdKey) -> IdKey<'_> {
		IdKey::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, id)
	}

	pub(crate) fn new_ig_key(
		&self,
		appending_id: AppendingId,
		batch_id: BatchId,
	) -> IndexAppending<'_> {
		IndexAppending::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, appending_id, batch_id)
	}

	pub(crate) fn new_ig_range(&self) -> Result<Range<Key>> {
		IndexAppending::new_range(self.0.ns, self.0.db, &self.0.tb, self.0.ix)
	}

	pub(crate) fn new_ip_key(&self, id: RecordIdKey) -> Ip<'_> {
		Ip::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, id)
	}

	/// Key storing durable build state for this table index.
	pub(crate) fn new_bs_key(&self) -> Bs<'_> {
		Bs::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix)
	}

	/// Key storing one durable writer reservation for a build generation.
	pub(crate) fn new_br_key(&self, generation: BuildGeneration, ticket: BuildTicket) -> Br<'_> {
		Br::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, generation, ticket)
	}

	/// Range covering writer reservations for one build generation.
	pub(crate) fn new_br_range(&self, generation: BuildGeneration) -> Result<Range<Key>> {
		Br::range(self.0.ns, self.0.db, &self.0.tb, self.0.ix, generation)
	}

	/// Range covering writer reservations across all generations of this index.
	pub(crate) fn new_br_all_generations_range(&self) -> Result<Range<Key>> {
		Br::all_generations_range(self.0.ns, self.0.db, &self.0.tb, self.0.ix)
	}

	/// Key storing one durable queued mutation for a build generation.
	pub(crate) fn new_bg_key(
		&self,
		generation: BuildGeneration,
		ticket: BuildTicket,
		mutation_seq: BuildTicketMutationSeq,
	) -> Bg<'_> {
		Bg::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, generation, ticket, mutation_seq)
	}

	/// Range covering durable queued mutations for one build generation.
	pub(crate) fn new_bg_range(&self, generation: BuildGeneration) -> Result<Range<Key>> {
		Bg::range(self.0.ns, self.0.db, &self.0.tb, self.0.ix, generation)
	}

	/// Range covering every durable queued mutation that shares one reservation
	/// ticket within a build generation — used by `wait_for_durable_reservations`
	/// to decide whether a writer has committed any of its batched mutations.
	pub(crate) fn new_bg_ticket_range(
		&self,
		generation: BuildGeneration,
		ticket: BuildTicket,
	) -> Result<Range<Key>> {
		Bg::ticket_range(self.0.ns, self.0.db, &self.0.tb, self.0.ix, generation, ticket)
	}

	/// Range covering durable queued mutations across all generations of this index.
	pub(crate) fn new_bg_all_generations_range(&self) -> Result<Range<Key>> {
		Bg::all_generations_range(self.0.ns, self.0.db, &self.0.tb, self.0.ix)
	}

	/// Key mapping a record to its first queued mutation during the initial scan.
	pub(crate) fn new_bp_key(&self, generation: BuildGeneration, id: RecordIdKey) -> Bp<'_> {
		Bp::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, generation, id)
	}

	#[cfg(test)]
	/// Range covering primary-appending markers for one build generation.
	pub(crate) fn new_bp_range(&self, generation: BuildGeneration) -> Result<Range<Key>> {
		Bp::range(self.0.ns, self.0.db, &self.0.tb, self.0.ix, generation)
	}

	/// Range covering a primary-appending record-id span for one build generation.
	pub(crate) fn new_bp_span_range(
		&self,
		generation: BuildGeneration,
		after: Option<&RecordIdKey>,
		through: Option<&RecordIdKey>,
	) -> Result<Range<Key>> {
		Bp::span_range(self.0.ns, self.0.db, &self.0.tb, self.0.ix, generation, after, through)
	}

	/// Range covering primary-appending markers across all generations of this index.
	pub(crate) fn new_bp_all_generations_range(&self) -> Result<Range<Key>> {
		Bp::all_generations_range(self.0.ns, self.0.db, &self.0.tb, self.0.ix)
	}

	pub(crate) fn new_ib_key(&self, start: i64) -> Ib<'_> {
		Ib::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, start)
	}

	pub(crate) fn new_ic_key(&self, nid: Uuid) -> IndexCompactionKey<'_> {
		IndexCompactionKey::new(
			self.0.ns,
			self.0.db,
			Cow::Borrowed(&self.0.tb),
			self.0.ix,
			nid,
			Uuid::now_v7(),
		)
	}

	pub(crate) fn new_ib_range(&self) -> Result<Range<Key>> {
		Ib::new_range(self.0.ns, self.0.db, &self.0.tb, self.0.ix)
	}

	pub(crate) fn new_is_key(&self, nid: Uuid) -> Is<'_> {
		Is::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, nid)
	}

	fn new_td_root<'a>(&'a self, term: &'a str) -> TdRoot<'a> {
		TdRoot::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, term)
	}

	fn new_td<'a>(&'a self, term: &'a str, doc_id: DocId) -> Td<'a> {
		Td::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, term, doc_id)
	}

	fn new_tt<'a>(
		&'a self,
		term: &'a str,
		doc_id: DocId,
		nid: Uuid,
		uid: Uuid,
		add: bool,
	) -> Tt<'a> {
		Tt::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, term, doc_id, nid, uid, add)
	}

	fn new_tt_term_range(&self, term: &str) -> Result<(Key, Key)> {
		Tt::term_range(self.0.ns, self.0.db, &self.0.tb, self.0.ix, term)
	}

	fn new_tt_terms_range(&self) -> Result<(Key, Key)> {
		Tt::terms_range(self.0.ns, self.0.db, &self.0.tb, self.0.ix)
	}

	/// Generation guard for full-text term-document (`!tt`) compaction.
	fn new_tv_key(&self) -> Tv<'_> {
		Tv::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix)
	}

	fn new_dc_with_id(&self, doc_id: DocId, nid: Uuid, uid: Uuid) -> Dc<'_> {
		Dc::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, doc_id, nid, uid)
	}

	fn new_dc_compacted(&self) -> Result<Key> {
		Dc::new_root(self.0.ns, self.0.db, &self.0.tb, self.0.ix)
	}

	fn new_dc_range_with_root(&self) -> Result<(Key, Key)> {
		Dc::range_with_root(self.0.ns, self.0.db, &self.0.tb, self.0.ix)
	}

	/// Generation guard for full-text document-stat (`!dc`) compaction.
	fn new_dv_key(&self) -> Dv<'_> {
		Dv::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix)
	}

	fn new_dl(&self, doc_id: DocId) -> Dl<'_> {
		Dl::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, doc_id)
	}

	/// Generation guard for count-index (`!iu`) compaction.
	pub(crate) fn new_iv_key(&self) -> Iv<'_> {
		Iv::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix)
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
