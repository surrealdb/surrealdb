//! The keyspace schema.
//!
//! One [`keyspace!`](surrealdb_keyspace_macro::keyspace) invocation declares the
//! key hierarchy. Everything else in this module is generated from it: the key
//! types, their encoders and decoders, the scan bounds, the reverse decoder, and
//! [`KEYSPACE_MAP`].
//!
//! Segments are written in encode order. A nested block declares a level whose
//! segments every key beneath it inherits, so a child key cannot be constructed
//! without the data identifying its parents. `@` marks a truncation point and
//! generates the scan bound ending there.
//!
//! The macro rejects, at compile time, any schema in which two keys could claim
//! the same bytes, a tag could be spelled by a field value, or a scan bound has
//! no upper bound. See the macro crate's documentation for the full rule set.
//!
//! Field types are declared in the `types` prelude, which is what lets the
//! checker reason about the bytes each one produces. `'k` in an alias stands for
//! the lifetime of the key bytes and is replaced with the generated one.

use std::borrow::Cow;

use surrealdb_catalog::{DatabaseId, IndexId, NamespaceId, Record};
use surrealdb_expr::expr::dir::Dir;
use surrealdb_expr::val::{IndexFormat, RecordId, RecordIdKey, Value};
use surrealdb_keyspace_macro::keyspace;
// The generated code refers to the key error by this name.
use surrealdb_kvs::key::Error as KeyError;
use surrealdb_kvs::key::{KVKey, KVKeyDecode, KVRange, KVSubspace, RawRange, TypedRange};
use surrealdb_kvs::value::KVValue;
use surrealdb_kvs::{Key, KeyRange};
use surrealdb_strand::TableName;
use uuid::Uuid;

use crate::key::reclaim::{Expunge, ReclaimKind, ReclaimState};
use crate::tasklease::TaskLeaseType;
use crate::values::ids::DocId;
use crate::values::index_build::{
	Appending, AppendingId, BatchId, BuildGeneration, BuildTicket, BuildTicketMutationSeq,
	IndexBuildReservation, IndexBuildState, PrimaryAppendingTicket,
};
use crate::values::vector::{ElementId, SerializedVector};

keyspace! {
	types {
		fixed(1)  bool, Dir;
		fixed(2)  TaskLeaseType, u16;
		fixed(4)  NamespaceId, DatabaseId, IndexId, BuildTicketMutationSeq, u32;
		fixed(4)  AppendingId, BatchId;
		fixed(8)  u64, i64, DocId, BuildGeneration, BuildTicket, ElementId;
		fixed(16) Uuid;
		fixed(32) Hash32 = [u8; 32];
		// Hand-written codecs whose discriminants predate storekey's numbering.
		fixed(1)  ReclaimKind, Expunge;
		// An `Option` is self-delimiting: its tag says whether a payload follows.
		var       OptUuidPair = Option<(Uuid, Uuid)>;
		var       Vector = Cow<'k, SerializedVector>;
		var       Value;
		// One index key stores an owned record id rather than a borrowed one.
		var       RecordIdKey;
		var       Str = Cow<'k, str>;
		var       Table = Cow<'k, TableName>;
		var       Id = Cow<'k, RecordIdKey>;
		var       Bytes = Cow<'k, [u8]>;
	}

	/// Storage format version. Sits outside the `/`-rooted keyspace and so sorts
	/// before all data, which is what lets a version probe read it without
	/// knowing whether anything else exists yet.
	version = ["!v"] => crate::version::MajorVersion (also_range);

	root = ["/"] (format_generic) {
		/// The full semantic version the datastore has been advanced to.
		///
		/// Distinct from the bare `!v` key above, which holds only the major
		/// version and predates this one. Both are written: a node older than
		/// 3.3 reads `!v` and does not know about this key.
		storage_version = ["!vs"] => crate::version::StorageVersion;

		/// One entry in the datastore's version history, written when a node
		/// advances the stamp above.
		///
		/// Ordered by timestamp first so a scan reads the history in the order
		/// it happened. The node id makes the key unique per writer, so two
		/// nodes recording a transition never contend for one key — which is
		/// what backends that resolve concurrent writes by last-writer-wins
		/// rather than by conflict require.
		version_history = ["!vh", @, ts: u64, nd: Uuid]
			=> crate::version::VersionHistoryEntry;

		/// The record of one applied data migration, keyed by its stable id.
		///
		/// The key's presence is what marks the migration as applied, so a
		/// migration runs at most once per datastore however many nodes start
		/// at the same moment.
		migration = ["!mg", @, id: u32] => crate::version::MigrationRecord;

		/// A namespace definition, keyed by name.
		namespace = ["!ns", @, ns: Str] => surrealdb_catalog::NamespaceDefinition;

		/// A cluster node's heartbeat and metadata.
		node = ["!nd", @, nd: Uuid] => surrealdb_catalog::node::Node;

		/// A root-level user definition.
		root_user = ["!us", @, user: Str] => surrealdb_catalog::UserDefinition;

		/// A root-level access method definition.
		root_access_method = ["!ac", @, ac: Str] => surrealdb_catalog::StoredAccessDefinition;

		/// Root-level configuration, keyed by kind.
		root_config = ["!cg", @, ty: Str] => surrealdb_catalog::StoredConfigDefinition;

		/// A lease over one of the singleton background tasks.
		task_lease = ["!tl", @, task_id: TaskLeaseType] => surrealdb_catalog::TaskLease;

		/// A durable session.
		session = ["!se", @, id: Uuid] => crate::values::session::DurableSession;

		/// The namespace id allocator's reserved batch and its state.
		ns_id_batch = ["!nh", @, start: i64] => crate::sequences::BatchValue;
		ns_id_state = ["!ni", @, nid: Uuid] => crate::sequences::SequenceState;

		/// Queued asynchronous events.
		///
		/// The namespace, database and table are spelled out as fields rather than
		/// inherited from a level, so a queued entry survives the deletion of the
		/// subtree it refers to and can still be drained afterwards. The same
		/// applies to the compaction and reclaim queues below.
		event_queue = ["!eq", @, ns: NamespaceId, db: DatabaseId, tb: Table, ev: Str,
					   ts: u64, node_id: Uuid] => crate::values::event_queue::AsyncEventRecord;

		/// Index compaction work items.
		index_compaction = ["!ic", @, ns: NamespaceId, db: DatabaseId, tb: Table,
							@, ix: IndexId, @, nid: Uuid, uid: Uuid] => ();

		/// Resources awaiting reclamation after a drop.
		reclaim = ["!rc", @, kind: ReclaimKind, ns: NamespaceId, db: DatabaseId,
				   tb: Table, ix: IndexId, expunge: Expunge, uid: Uuid] => ReclaimState;

		/// Per-node state. The node's own bytes address nothing; only the keys
		/// beneath them do.
		node_state = ["$", nd: Uuid] {
			node_live_query = ["!lq", @, lq: Uuid] => surrealdb_catalog::NodeLiveQuery;
		}

		/// Grants issued against a root-level access method.
		root_access = ["&", ac: Str] {
			root_grant = ["!gr", @, gr: Str] => surrealdb_catalog::AccessGrant;
		}

		ns = ["*", ns: NamespaceId] (format_generic) {
			/// A database definition, keyed by name within its namespace.
			database = ["!db", @, db: Str] => surrealdb_catalog::DatabaseDefinition;

			/// A namespace-level user definition.
			ns_user = ["!us", @, user: Str] => surrealdb_catalog::UserDefinition;

			/// A namespace-level access method definition.
			ns_access_method = ["!ac", @, ac: Str] => surrealdb_catalog::StoredAccessDefinition;

			/// The database id allocator's reserved batch and its state.
			db_id_batch = ["!dh", @, start: i64] => crate::sequences::BatchValue;
			db_id_state = ["!di", @, nid: Uuid] => crate::sequences::SequenceState;

			/// Grants issued against a namespace-level access method.
			ns_access = ["&", ac: Str] {
				ns_grant = ["!gr", @, gr: Str] => surrealdb_catalog::AccessGrant;
			}

			db = ["*", db: DatabaseId] (format_generic) {
				/// A table definition, keyed by name within its database.
				table = ["!tb", @, tb: Table] => surrealdb_catalog::StoredTableDefinition;

				/// A database-level user definition.
				db_user = ["!us", @, user: Str] => surrealdb_catalog::UserDefinition;

				/// A database-level access method definition.
				db_access_method = ["!ac", @, ac: Str] => surrealdb_catalog::StoredAccessDefinition;

				api = ["!ap", @, ap: Str] => surrealdb_catalog::StoredApiDefinition;
				analyzer = ["!az", @, az: Str] => surrealdb_catalog::AnalyzerDefinition;
				bucket = ["!bu", @, bu: Str] => surrealdb_catalog::StoredBucketDefinition;
				db_config = ["!cg", @, ty: Str] => surrealdb_catalog::StoredConfigDefinition;

				/// A user-defined function. The tag is `!fn`, not `!fc`.
				function = ["!fn", @, fc: Str] => surrealdb_catalog::StoredFunctionDefinition;

				module = ["!md", @, md: Str] => surrealdb_catalog::StoredModuleDefinition;

				/// A machine-learning model, keyed by name and version.
				ml_model = ["!ml", @, ml: Str, vn: Str] => surrealdb_catalog::StoredMlModelDefinition;

				param = ["!pa", @, pa: Str] => surrealdb_catalog::StoredParamDefinition;

				/// The table id allocator's reserved batch and its state.
				tb_id_batch = ["!th", @, start: i64] => crate::sequences::BatchValue;
				tb_id_state = ["!ti", @, nid: Uuid] => crate::sequences::SequenceState;

				/// A sequence definition.
				///
				/// `!sd` rather than `!sq`, which the `seq` level below already
				/// owns: a definition at `!sq{name}` would be a strict prefix of
				/// that level's `!st` and `!ba` keys, so a scan of definitions
				/// would read allocator state instead. Definitions therefore get a
				/// subspace of their own, holding nothing else, and listing them
				/// costs one scan of exactly the definitions.
				sequence = ["!sd", @, sq: Str] => surrealdb_catalog::SequenceDefinition;

				/// Change-feed entries, ordered by timestamp then table.
				///
				/// The two truncation points bound a whole database's changes and
				/// one timestamp's changes respectively; the change-feed reader and
				/// its garbage collector each need one of them.
				change_feed = ["#", @, ts: Bytes, @, "*", tb: Table]
					=> crate::values::changefeed::TableMutations;

				/// Live-query events, ordered by timestamp then table.
				///
				/// Byte-for-byte the change feed's layout with a different section
				/// marker, so the two sections sit side by side without overlapping.
				live_events = ["%", @, ts: Bytes, @, "*", tb: Table]
					=> crate::values::live_query::LiveEvents;

				/// Per-sequence allocator state. The definition lives under `!sd`
				/// while its state lives here under `!sq`; the two are different
				/// subspaces for the same logical entity, kept apart so that
				/// listing definitions does not scan allocator state.
				seq = ["!sq", sq: Str] {
					seq_batch = ["!ba", @, start: i64] => crate::sequences::BatchValue;
					seq_state = ["!st", @, nid: Uuid] => crate::sequences::SequenceState;
				}

				/// Grants issued against a database-level access method.
				db_access = ["&", ac: Str] {
					db_grant = ["!gr", @, gr: Str] => surrealdb_catalog::AccessGrant;
				}

				tbl = ["*", tb: Table] (format_generic) {
					/// A record document. Its identity comes from the key, so the
					/// key supplies the decode context that rebuilds the `id` field
					/// rather than trusting the stored copy.
					record = ["*", @, id: Id] => Record (ctx = |k: &RecordKey| RecordId {
						table: k.tb.as_ref().clone(),
						key: k.id.clone().into_owned(),
					});

					/// Graph edges.
					///
					/// The inner key records one endpoint of an edge; the pointer key
					/// extends it with the edge record's own identity. The inner
					/// layout is a strict prefix of the pointer layout, so one scan
					/// returns both, and the pointer form tolerates trailing bytes so
					/// a later format can append without breaking this reader. The
					/// inner form does not: trailing bytes there mean corruption.
					graph = ["~", @, id: Id, @, dir: Dir, @, foreign_table: Table,
							 @, foreign_key: Id] => ();
					graph_pointer = graph + [target_table: Table, target_key: Id] => ()
						(ignore_trailing);

					/// Reference back-links.
					///
					/// Field order is what the four bounds scan by: every reference to
					/// a record in this table, then to one record, then filtered by
					/// the referring table, then by the referring field.
					reference = ["&", @, id: Id, @, foreign_table: Table,
								 @, foreign_field: Str, @, foreign_key: Id] => ();

					/// Table-scoped schema definitions.
					event = ["!ev", @, ev: Str] => surrealdb_catalog::StoredEventDefinition;
					field = ["!fd", @, fd: Str] => surrealdb_catalog::StoredFieldDefinition;

					/// A table that computes its rows from this one.
					foreign_table = ["!ft", @, ft: Table]
						=> surrealdb_catalog::StoredTableDefinition;

					/// An index definition, plus the reverse lookup from the index's
					/// id back to its name.
					index_name = ["!il", ix: IndexId] => String (derive(+Ord));
					index_def = ["!ix", @, ix: Str]
						=> surrealdb_catalog::StoredIndexDefinition (derive(+Ord));

					/// A live-query subscription against this table.
					subscription = ["!lq", @, lq: Uuid]
						=> surrealdb_catalog::StoredSubscriptionDefinition;

					/// The document id allocator's reserved batch and its state.
					doc_id_batch = ["!dh", @, start: i64] => crate::sequences::BatchValue;
					doc_id_state = ["!ds", @, nid: Uuid] => crate::sequences::SequenceState;

					/// The two directions of the document id mapping: id to record
					/// key, and record key back to id.
					doc_key = ["!dd", @, doc_id: DocId] => RecordIdKey;

					#[format(IndexFormat)]
					doc_lookup = ["!di", @, id: Id] => DocId;

					/// Records whose index entries are still pending.
					#[format(IndexFormat)]
					doc_pending = ["!dp", @, id: Id] => ();

					/// The index id allocator's reserved batch and its state.
					index_id_batch = ["!ih", @, start: i64] => crate::sequences::BatchValue;
					index_id_state = ["!is", @, nid: Uuid] => crate::sequences::SequenceState;

					/// Index build progress.
					///
					/// A build is identified by an index id and a generation; within
					/// a generation, work is handed out as tickets, and each ticket
					/// accumulates mutations in sequence. Each truncation point
					/// bounds one level of that nesting, which is what lets a worker
					/// claim, replay or discard exactly one generation, ticket or
					/// mutation run.
					build_state = ["!bs", @, ix: IndexId] => IndexBuildState;
					build_ticket = ["!bt", ix: IndexId, @, generation: BuildGeneration]
						=> BuildTicket;
					build_reservation = ["!br", ix: IndexId, @, generation: BuildGeneration,
										 @, ticket: BuildTicket] => IndexBuildReservation;
					build_append = ["!bg", ix: IndexId, @, generation: BuildGeneration,
									@, ticket: BuildTicket,
									@, mutation_seq: BuildTicketMutationSeq] => Appending;

					/// Primary-key appends recorded during a build. Encoded under the
					/// index format because the record id is part of the key.
					#[format(IndexFormat)]
					build_primary = ["!bp", ix: IndexId, @, generation: BuildGeneration,
									 @, id: Id] => PrimaryAppendingTicket;

					idx = ["+", ix: IndexId] (format_generic) {
						/// Index entries.
						///
						/// The indexed field values are a list, so the bound after
						/// them comes in two forms: closed, which bounds entries whose
						/// values match exactly, and open, which omits the list
						/// terminator and so bounds every entry whose values merely
						/// start with the given ones. The open form is what a range
						/// query over a leading subset of the indexed fields needs.
						///
						/// The trailing byte distinguishes the two entry shapes and is
						/// the discriminant of a retired `Option` field: `0x03` where
						/// a record id follows, `0x02` where the index is unique and
						/// the values alone identify the row.
						#[format(IndexFormat)]
						entry = ["*", @, fd: [Value..], @open, @, raw 0x03, id: Id]
							=> crate::values::entry::IndexEntryValue (derive(-Eq));

						#[format(IndexFormat)]
						unique = ["*", fd: [Value..], raw 0x02]
							=> crate::values::entry::IndexEntryValue (derive(-Eq));

						/// Full-text term document sets: every document id carrying a
						/// term, compacted into one bitmap.
						term_docs = ["!td", @, term: Str] => roaring::RoaringTreemap;

						/// Per-(term, document) posting, read but never written.
						/// Extends the term's root key with one document id, so a
						/// scan of the root's range returns both and the decoder
						/// tells them apart by whether the document id is present.
						/// Read as the fallback for a document that carries no `!dt`
						/// entry, the absence of which is what selects it.
						term_posting = term_docs + [id: DocId]
							=> crate::values::fulltext::TermDocument;

						/// Per-document totals `!td` could not hold, because a bitmap
						/// carries a document once and a total can be anything.
						///
						/// Written only where a compaction round reached part of a
						/// term's changes, so the total it folded is not yet the
						/// document's final one. Absent for every term whose changes
						/// were folded whole, which is every term once the delta
						/// families are drained.
						term_docs_residual = ["!tr", @, term: Str]
							=> crate::values::fulltext::TermDocsResidual;

						/// One document's postings, keyed by the document rather than
						/// by term.
						///
						/// A posting is only ever addressed as (term, document): the
						/// set of documents carrying a term comes from `!td`, and a
						/// posting is read afterwards to score or highlight a document
						/// already known to match. Nothing enumerates a term's
						/// postings. Keying by document therefore serves every access
						/// this family has, and collapses the ~1 key per (term,
						/// document) that `!td` spent into one key per document —
						/// bounded, since a document's distinct terms are bounded by
						/// the document itself.
						///
						/// Scoring reads the whole entry to take one term's frequency
						/// from it. That is one point read per document scored, where
						/// the per-term shape needed one per (term, document); on an
						/// index declared with `HIGHLIGHTS` the entry also carries
						/// offsets, so those reads move fewer keys but more bytes.
						doc_terms = ["!dt", @, id: DocId]
							=> crate::values::fulltext::DocumentTerms;

						/// Uncompacted full-text term changes: all terms, one term, and
						/// one change. Each level is also a stored key, because
						/// compaction writes its result at the bound itself.
						///
						/// Read but never written. Readers fold this family and `!tx`
						/// together and compaction drains both, so an index carrying
						/// entries in either shape resolves to the same document set.
						term_changes = ["!tt"] => String (also_range);
						term_change_set = term_changes + [term: Str] => String
							(also_range);
						term_change = term_change_set
							+ [doc_id: DocId, nid: Uuid, uid: Uuid, add: bool] => String;

						/// Uncompacted full-text term changes, batched per transaction.
						///
						/// `!tt` spends one key per (term, document) pair. Nothing
						/// requires that: the entry is already tagged with a
						/// per-transaction id, so two transactions never share a key,
						/// and one transaction's whole contribution to a term collapses
						/// into a single bitmap without reintroducing contention. A
						/// statement indexing many records then writes one key per
						/// distinct term rather than one per (term, record).
						///
						/// `add` stays in the key, as in `!tt`, so the two directions
						/// never share a bitmap and a reader needs no signed payload.
						///
						/// The trade is that the value's size follows the writing
						/// transaction rather than being fixed: one entry names every
						/// document that transaction moved for the term. A reader that
						/// bounds work by key count therefore bounds nothing — see how
						/// full-text compaction spends its limit as a document budget.
						term_change_batch = ["!tx", @, term: Str, @, nid: Uuid, uid: Uuid, add: bool]
							=> roaring::RoaringTreemap;

						/// Document length and count, compacted at the bound and
						/// accumulated in deltas beneath it.
						///
						/// Read but never written; drained alongside `!dx`.
						doc_stats = ["!dc"] => crate::values::fulltext::DocLengthAndCount
							(also_range);
						doc_stats_delta = doc_stats + [doc_id: DocId, nid: Uuid, uid: Uuid]
							=> crate::values::fulltext::DocLengthAndCount;

						/// Document length and count, batched per transaction.
						///
						/// The same per-transaction tagging that makes `!tx` safe makes
						/// a per-document key unnecessary here: the fields sum, so one
						/// entry can carry every document the transaction indexed.
						doc_stats_batch = ["!dx", @, nid: Uuid, uid: Uuid]
							=> crate::values::fulltext::DocLengthAndCount;

						doc_length = ["!dl", @, id: DocId] => crate::values::fulltext::DocLength;
						doc_count = ["!dv"] => u64;

						/// Index row count, compacted at the `None` bound and
						/// accumulated as signed deltas beneath it. `None` sorts before
						/// every `Some`, which is what puts the compacted total ahead
						/// of its deltas.
						index_count = ["!iu", @, uid: OptUuidPair, pos: bool, count: u64] => ();

						index_version = ["!iv"] => u64;

						/// Full-text term-document compaction generation. Deliberately
						/// outside the `!tt` delta range, so a compactor can check that
						/// the snapshot it read is still current before applying
						/// exact-key deletes. A missing value reads as generation zero.
						term_generation = ["!tv"] => u64;

						/// Appends queued while an index is building.
						index_append = ["!ig", @, appending_id: AppendingId, batch_id: BatchId]
							=> Appending;

						#[format(IndexFormat)]
						index_primary = ["!ip", id: RecordIdKey]
							=> crate::values::index_build::PrimaryAppending;

						/// HNSW graph storage.
						hnsw_state = ["!hs"] => crate::values::hnsw::HnswState;
						hnsw_generation = ["!hg"] => u64;
						hnsw_vector = ["!he", @, element_id: ElementId]
							=> crate::values::vector::SerializedVector;
						hnsw_element = ["!hv", @, vec: Vector]
							=> crate::values::hnsw::ElementDocs (derive(-Eq, -PartialOrd));
						hnsw_element_hashed = ["!hh", @, hash: Hash32]
							=> crate::values::hnsw::ElementHashedDocs;
						hnsw_layer = ["!hl", layer: u16, @, chunk: u32] => Vec<u8>;
						hnsw_node = ["!hn", layer: u16, @, node: ElementId] => Vec<u8>;

						#[format(IndexFormat)]
						hnsw_record_pending = ["!hr", @, id: Id]
							=> crate::values::hnsw::HnswRecordPendingUpdate;

						/// Append-keyed HNSW pending updates: one entry per queued
						/// change, where `!hr` above holds one coalesced entry per
						/// record. Read and drained, never written, so an index
						/// holding entries under this layout still empties.
						hnsw_pending_legacy = ["!hp", @, appending_id: u64]
							=> crate::values::hnsw::VectorPendingUpdate;

						/// DiskANN graph storage, compiled only where the backend is
						/// available. Conflicts are reported against every key
						/// regardless of configuration, so these cannot collide with
						/// the keys they coexist with when enabled.
						#[cfg(diskann)]
						diskann_state = ["!ds"] => crate::values::diskann::DiskAnnState;
						#[cfg(diskann)]
						diskann_generation = ["!dg"] => u64;
						#[cfg(diskann)]
						diskann_element = ["!de", @, element_id: ElementId]
							=> crate::values::diskann::DiskAnnElement;
						#[cfg(diskann)]
						diskann_node = ["!dn", @, element_id: ElementId]
							=> crate::values::diskann::DiskAnnNode;
						#[cfg(diskann)]
						diskann_element_docs = ["!dq", @, vec: Vector]
							=> crate::values::diskann::DiskAnnElementDocs
							(derive(-Eq, -PartialOrd));
						#[cfg(diskann)]
						diskann_element_hashed = ["!dh", @, hash: Hash32]
							=> crate::values::diskann::DiskAnnElementHashedDocs;
						#[cfg(diskann)]
						diskann_pending_legacy = ["!dp", @, shard: u16]
							=> crate::values::diskann::DiskAnnPendingState;
						#[cfg(diskann)]
						diskann_pending = ["!dy", @, shard: u16]
							=> crate::values::diskann::DiskAnnPendingState;

						/// The sharded successor to the legacy pending layout, under a
						/// distinct tag so the two never overlap during a dual read.
						#[cfg(diskann)]
						#[format(IndexFormat)]
						diskann_record_pending = ["!dr", @, id: Id]
							=> crate::values::diskann::DiskAnnRecordPendingUpdate;
						#[cfg(diskann)]
						#[format(IndexFormat)]
						diskann_record_pending_shard = ["!dw", shard: u16, @, id: Id]
							=> crate::values::diskann::DiskAnnRecordPendingUpdate;
					}
				}
			}
		}
	}
}

impl ReclaimKey<'_> {
	/// A queue entry for a whole namespace.
	///
	/// The queue is one flat band keyed by kind, so the fields a kind does not use
	/// are zeroed rather than absent. Zeroing them here keeps that convention in
	/// one place instead of at each of the callers that enqueue work.
	pub fn namespace(ns: NamespaceId, expunge: bool, uid: Uuid) -> Self {
		ReclaimKey {
			kind: ReclaimKind::Namespace,
			ns,
			db: DatabaseId(0),
			tb: Cow::Owned(TableName::default()),
			ix: IndexId(0),
			expunge: Self::expunge(expunge),
			uid,
		}
	}

	/// A queue entry for a whole database. See [`Self::namespace`] on the unused
	/// fields.
	pub fn database(ns: NamespaceId, db: DatabaseId, expunge: bool, uid: Uuid) -> Self {
		ReclaimKey {
			kind: ReclaimKind::Database,
			ns,
			db,
			tb: Cow::Owned(TableName::default()),
			ix: IndexId(0),
			expunge: Self::expunge(expunge),
			uid,
		}
	}

	fn expunge(expunge: bool) -> Expunge {
		match expunge {
			true => Expunge::Expunge,
			false => Expunge::Keep,
		}
	}
}

/// A graph adjacency key, decoded from either member of the family.
///
/// A pointer key extends an inner key byte for byte, so one scan returns both and
/// a caller that only wants the edge does not have to know which it got.
pub struct DecodedGraph {
	/// The edge the adjacency names, which both layouts carry.
	pub edge: RecordId,
	/// The far vertex, present only in the pointer layout. Having it lets a
	/// traversal reach the other end without reading the edge record.
	pub target: Option<RecordId>,
}

impl DecodedGraph {
	/// Decodes whichever member of the graph family these bytes are.
	///
	/// The longer layout is tried first: a pointer key's bytes begin with a
	/// complete inner key, so trying the inner one first would succeed on a
	/// pointer key while silently discarding its tail.
	///
	/// Bytes after a pointer's tail are ignored, which is the family's
	/// forward-compatibility contract — a field appended after the tail stays
	/// readable here, just unread. Bytes after a bare inner key are not ignored:
	/// new fields are always layered after the tail, so trailing bytes there are
	/// corruption rather than a newer writer.
	pub fn decode(bytes: &[u8]) -> anyhow::Result<Self> {
		if let Ok(pointer) = GraphPointerKey::decode_key(bytes) {
			return Ok(DecodedGraph {
				edge: RecordId {
					table: pointer.foreign_table.into_owned(),
					key: pointer.foreign_key.into_owned(),
				},
				target: Some(RecordId {
					table: pointer.target_table.into_owned(),
					key: pointer.target_key.into_owned(),
				}),
			});
		}
		let inner = GraphKey::decode_key(bytes)?;
		Ok(DecodedGraph {
			edge: RecordId {
				table: inner.foreign_table.into_owned(),
				key: inner.foreign_key.into_owned(),
			},
			target: None,
		})
	}
}

#[cfg(test)]
mod tests {
	use surrealdb_strand::Strand;

	use super::*;

	/// The bytes each generated key produces, against the layout the store
	/// already holds. A mismatch here means the encoder would not read existing
	/// data.
	#[test]
	fn encodings_match_the_stored_layout() {
		let namespace = NamespaceKey::new(Cow::Borrowed("test"));
		assert_eq!(&*namespace.encode_key().unwrap(), b"/!nstest\0");

		let node = NodeKey::new(Uuid::from_u128(0));
		assert_eq!(&*node.encode_key().unwrap(), &b"/!nd\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"[..]);

		let user = RootUserKey::new(Cow::Borrowed("testuser"));
		assert_eq!(&*user.encode_key().unwrap(), b"/!ustestuser\0");

		let access = RootAccessMethodKey::new(Cow::Borrowed("testac"));
		assert_eq!(&*access.encode_key().unwrap(), b"/!actestac\0");

		// `i64` is sign-flipped before the big-endian bytes, so ordering by key
		// matches ordering by value across zero.
		let batch = NsIdBatchKey::new(123);
		assert_eq!(&*batch.encode_key().unwrap(), b"/!nh\x80\0\0\0\0\0\0\x7b");

		let session = SessionKey::new(Uuid::from_u128(1));
		assert_eq!(&*session.encode_key().unwrap(), &b"/!se\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01"[..]);

		let grant = RootGrantKey::new(Cow::Borrowed("testac"), Cow::Borrowed("testgr"));
		assert_eq!(&*grant.encode_key().unwrap(), b"/&testac\0!grtestgr\0");

		let database = DatabaseKey::new(NamespaceId(1), Cow::Borrowed("testdb"));
		assert_eq!(&*database.encode_key().unwrap(), b"/*\x00\x00\x00\x01!dbtestdb\0");

		let tb = TableName::from("testtb");
		let table = TableKey::new(NamespaceId(1), DatabaseId(2), Cow::Borrowed(&tb));
		assert_eq!(
			&*table.encode_key().unwrap(),
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!tbtesttb\0"
		);

		// The tag is `!fn` even though the entry is named `function`.
		let function = FunctionKey::new(NamespaceId(1), DatabaseId(2), Cow::Borrowed("testfn"));
		assert_eq!(
			&*function.encode_key().unwrap(),
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!fntestfn\0"
		);

		let model = MlModelKey::new(
			NamespaceId(1),
			DatabaseId(2),
			Cow::Borrowed("testml"),
			Cow::Borrowed("1.0.0"),
		);
		assert_eq!(
			&*model.encode_key().unwrap(),
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!mltestml\x001.0.0\0"
		);

		let id = RecordIdKey::String(Strand::new_static("testid"));
		let record =
			RecordKey::new(NamespaceId(1), DatabaseId(2), Cow::Borrowed(&tb), Cow::Borrowed(&id));
		assert_eq!(
			&*record.encode_key().unwrap(),
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0*\x03testid\0"
		);
	}

	/// Table-level keys, against their stored layout.
	#[test]
	fn table_level_encodings_match_the_stored_layout() {
		let tb = TableName::from("testtb");
		let of = |k: &[u8]| k.to_vec();

		let event = EventKey::new(
			NamespaceId(1),
			DatabaseId(2),
			Cow::Borrowed(&tb),
			Cow::Borrowed("testev"),
		);
		assert_eq!(
			of(&event.encode_key().unwrap()),
			of(b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0!evtestev\0")
		);

		let field = FieldKey::new(
			NamespaceId(1),
			DatabaseId(2),
			Cow::Borrowed(&tb),
			Cow::Borrowed("testfd"),
		);
		assert_eq!(
			of(&field.encode_key().unwrap()),
			of(b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0!fdtestfd\0")
		);

		let index = IndexDefKey::new(
			NamespaceId(1),
			DatabaseId(2),
			Cow::Borrowed(&tb),
			Cow::Borrowed("testix"),
		);
		assert_eq!(
			of(&index.encode_key().unwrap()),
			of(b"/*\0\0\0\x01*\0\0\0\x02*testtb\0!ixtestix\0")
		);

		let doc = DocKeyKey::new(NamespaceId(1), DatabaseId(2), Cow::Borrowed(&tb), 1);
		assert_eq!(
			of(&doc.encode_key().unwrap()),
			of(b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0!dd\0\0\0\0\0\0\0\x01")
		);

		// The record id is encoded under the index format, which is why this entry
		// pins that format rather than the default.
		let id = RecordIdKey::String(Strand::new_static("id"));
		let lookup = DocLookupKey::new(
			NamespaceId(1),
			DatabaseId(2),
			Cow::Borrowed(&tb),
			Cow::Borrowed(&id),
		);
		assert_eq!(
			of(&lookup.encode_key().unwrap()),
			of(b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0!di\x03id\0")
		);

		// The append-keyed HNSW pending layout is no longer written, so this is the
		// one thing that keeps it readable: the bytes come from the encoder that
		// wrote them, and an index upgraded from before the `!hr` change still has
		// entries under them waiting to be drained.
		let pending = HnswPendingLegacyKey::new(
			NamespaceId(1),
			DatabaseId(2),
			Cow::Borrowed(&tb),
			IndexId(3),
			7,
		);
		assert_eq!(
			of(&pending.encode_key().unwrap()),
			of(b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!hp\0\0\0\0\0\0\0\x07")
		);

		let range =
			EventPrefix::new(NamespaceId(1), DatabaseId(2), Cow::Borrowed(&tb)).range().unwrap();
		assert_eq!(of(range.start()), of(b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0!ev\0"));
		assert_eq!(of(range.end()), of(b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0!ew"));
	}

	/// Owning a key's borrowed fields does not change the bytes it spells.
	///
	/// `into_owned` exists so a key decoded from a scan can outlive the batch that
	/// lent it the bytes, and callers then delete through it. That is only sound
	/// if the owned key addresses the same entry, so the equality is the whole
	/// contract rather than an incidental property.
	#[test]
	fn owning_a_key_does_not_move_it() {
		let tb = TableName::from("testtb");
		let id = RecordIdKey::String(Strand::new_static("id"));

		// One key per shape a borrowed field can take: a borrowed string, a
		// borrowed table name, a borrowed record id under the index format, and a
		// key that borrows nothing beyond its inherited level.
		let namespace = NamespaceKey::new(Cow::Borrowed("test"));
		assert_eq!(
			namespace.encode_key().unwrap(),
			namespace.clone().into_owned().encode_key().unwrap()
		);

		let table = TableKey::new(NamespaceId(1), DatabaseId(2), Cow::Borrowed(&tb));
		assert_eq!(table.encode_key().unwrap(), table.clone().into_owned().encode_key().unwrap());

		let record =
			RecordKey::new(NamespaceId(1), DatabaseId(2), Cow::Borrowed(&tb), Cow::Borrowed(&id));
		assert_eq!(record.encode_key().unwrap(), record.clone().into_owned().encode_key().unwrap());

		let count = IndexCountKey::new(
			NamespaceId(1),
			DatabaseId(2),
			Cow::Borrowed(&tb),
			IndexId(3),
			Some((Uuid::from_u128(4), Uuid::from_u128(5))),
			true,
			9,
		);
		assert_eq!(count.encode_key().unwrap(), count.clone().into_owned().encode_key().unwrap());

		// And the owned key really does outlive the bytes it decoded from.
		let owned = {
			let bytes = table.encode_key().unwrap();
			TableKey::decode_key(&bytes).unwrap().into_owned()
		};
		assert_eq!(owned.encode_key().unwrap(), table.encode_key().unwrap());
	}

	/// Byte strings that exist in stored data.
	///
	/// Every one is a run of bytes an encoder really produced, which is what makes
	/// them usable as an oracle. They are the corpus for
	/// [`the_stored_corpus_round_trips`], the check that the keyspace reads and
	/// writes exactly what is already on disk.
	///
	/// Some are scan bounds rather than whole keys, and those deliberately do not
	/// decode — a bound names a position between keys.
	const STORED_CORPUS: &[&[u8]] = &[
			b"/!ac\0",
			b"/!mg\x00\x00\x00\x01",
			b"/!actestac\x00",
			b"/!ad",
			b"/!cgtestty\0",
			b"/!vh\x00\x00\x00\x00\x00\x00\x00\x07\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0",
			b"/!vs",
			b"/!eq\x00\x00\x00\x01\x00\x00\x00\x02testtb\0testev\0\0\0\0\0\0\0\0\x01\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10",
			b"/!ic\0",
			b"/!id",
			b"/!nd\0",
			b"/!nd\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
			b"/!ne",
			b"/!nh\x80\0\0\0\0\0\0\x7B",
			b"/!ni\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01",
			b"/!nstest\0",
			b"/!rc\x00",
			b"/!rd",
			b"/!se\0",
			b"/!se\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
			b"/!sf",
			b"/!tl\0\x01",
			b"/!us\0",
			b"/!ustestuser\x00",
			b"/!ut",
			b"/$\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10",
			b"/&testac\0",
			b"/&testac\0!gr\0",
			b"/&testac\0!grtestgr\0",
			b"/&testac\0!gs",
			b"/*\0\0\0\x01*\0\0\0\x02*testtb\0!ixtestix\0",
			b"/*\0\0\0\x7B!dh\x80\0\0\0\0\0\0\x2A",
			b"/*\0\0\0\x7B!di\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x0F",
			b"/*\0\0\0\x7B*\0\0\0\xEA!th\x80\0\0\0\0\0\0\x2A",
			b"/*\0\0\0\x7B*\0\0\0\xEA*testtb\0!ii",
			b"/*\x00\x00\x00\x01!actestac\0",
			b"/*\x00\x00\x00\x01!db\0",
			b"/*\x00\x00\x00\x01!dbtest\0",
			b"/*\x00\x00\x00\x01!dc",
			b"/*\x00\x00\x00\x01!us\0",
			b"/*\x00\x00\x00\x01!ustestuser\0",
			b"/*\x00\x00\x00\x01!ut",
			b"/*\x00\x00\x00\x01&testac\0",
			b"/*\x00\x00\x00\x01&testac\0!gr\0",
			b"/*\x00\x00\x00\x01&testac\0!grtestgr\0",
			b"/*\x00\x00\x00\x01&testac\0!gs",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!ac\0",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!actestac\0",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!ad",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!ap",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!aptest\0",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!az",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!aztest\0",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!bu\0",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!butest\0",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!bv",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!cg",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!cgtestty\0",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!fntestfc\0",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!mdtestmd\0",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!ml",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!mltestml\x001.0.0\0",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!patestpa\0",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!sqtestsq\0!ba\x80\0\0\0\0\0\0\x64",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!sqtestsq\0!st\0\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!us\0",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!ustestuser\0",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!ut",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02&testac\0",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02&testac\0!gr\0",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02&testac\0!grtestgr\0",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02&testac\0!gs",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!sd\0",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!sdtest\0",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0!di",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0!di\x03id\0",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0!dp",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0!dp\x03id\0",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0!evtestev\0",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0!ew",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0!fdtestfd\0",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0!fe",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0!fttestft\0",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0!fu",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0!lr",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0*\x03testid\0",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!dq\x01\x01\x01\x01\x03\x01\0\x01\0\x80\x3F\x01\0\x01\0\x01\0\x40\x01\0\x01\0\x40\x40\0",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!dq\x01\x01\x05\x03\x01\x01\x01\0\x02\x01\0\x03\x01\0\0",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!dq\x01\x01\x06\x03\x01\x01\x02\x03\0",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!dq\x01\x01\x07\x03\x01\x01\x02\x03\0",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!hp\0\0\0\0\0\0\0\x07",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!hr\x03testid\0",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!hv\x01\x01\x01\0\x03\x01\0\x01\0\x01\0\x01\0\x01\0\x01\0\xF0\x3F\x01\0\x01\0\x01\0\x01\0\x01\0\x01\0\x01\0\x40\x01\0\x01\0\x01\0\x01\0\x01\0\x01\0\x08\x40\0",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!hv\x01\x01\x01\x01\x03\x01\0\x01\0\x80\x3F\x01\0\x01\0\x01\0\x40\x01\0\x01\0\x40\x40\0",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!hv\x01\x01\x02\x03\x01\x01\x01\0\x01\0\x01\0\x01\0\x01\0\x01\0\x01\0\x02\x01\0\x01\0\x01\0\x01\0\x01\0\x01\0\x01\0\x03\x01\0\x01\0\x01\0\x01\0\x01\0\x01\0\x01\0\0",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!hv\x01\x01\x03\x03\x01\x01\x01\0\x01\0\x01\0\x02\x01\0\x01\0\x01\0\x03\x01\0\x01\0\x01\0\0",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!hv\x01\x01\x04\x03\x01\x01\x01\0\x02\x01\0\x03\x01\0\0",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!hv\x01\x01\x05\x03\x01\x01\x01\0\x02\x01\0\x03\x01\0\0",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!hv\x01\x01\x06\x03\x01\x01\x02\x03\0",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!hv\x01\x01\x07\x03\x01\x01\x02\x03\0",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!ip\x03id\0",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!tdterm\0",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!tdterm\0\0\0\0\0\0\0\0\x81",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!ttterm\0\0\0\0\0\0\0\0\x81\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x02\x03",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\x00+\x00\x00\x00\x03!hh\x00\x1a\x4c\xc3\xa7\xc5\xc7\x39\xdf\x75\x9d\xf2\xc0\x56\x3c\x82\x24\xd5\xca\x89\xbe\x7f\xba\xbd\x99\xcf\x56\x88\xd2\xa0\x49\x15",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\x00+\x00\x00\x00\x03!hh\x26\x28\xed\x7d\x3c\xb9\x18\xf7\x6d\xbd\xd6\xe6\xe6\xb0\x53\x8f\x27\x15\x19\xc4\x99\x7c\xd6\x14\x4a\x69\x93\x9a\xf9\xf6\x84\x5c",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\x00+\x00\x00\x00\x03!hh\x53\x59\xf1\xe1\x6c\xcb\x8b\x69\x45\xd9\xf9\x94\xa8\x81\x90\x29\xce\xf0\x85\xf1\xbf\x0c\xb5\x41\x76\xf7\x6d\x9f\x83\xb8\x1c\x29",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\x00+\x00\x00\x00\x03!hh\x8e\xca\x85\xc9\x29\x2e\x3a\xba\xb7\xa9\x74\xe8\x36\x32\x18\x89\x29\x45\x9d\x08\xe7\x0b\x53\x77\x21\xc4\x91\x9e\x22\xab\x0a\x27",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\x00+\x00\x00\x00\x03!hh\xdf\x07\xf4\x90\x4a\x7c\xcb\x20\x3d\xc9\x35\xda\xe7\xba\xf4\xa4\xc1\xf0\xab\x92\x79\xa8\x63\xa6\x91\x09\xbe\x74\xf2\x60\x32\xef",
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02\0",
			b"/*\x00\x00\x00\x01\0",
			b"/*\x00\x00\x00\x7B*\x00\x00\x00\xEA!ti\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x0F",
			b"/\0",
	];

	/// Every stored key in the corpus decodes to the key it names and re-encodes to
	/// exactly the bytes it came from.
	///
	/// This is the whole byte-compatibility claim in one test, over a corpus written
	/// by the previous encoders rather than transcribed by hand: if a declared
	/// layout drifted by a byte, either the decode fails or the re-encode differs.
	#[test]
	fn the_stored_corpus_round_trips() {
		let mut decoded = 0usize;
		let mut bounds = Vec::new();
		for bytes in STORED_CORPUS {
			let Some(key) = AnyKey::decode(bytes) else {
				bounds.push(describe(bytes).to_string());
				continue;
			};
			assert_eq!(
				String::from_utf8_lossy(&key.encode_key().unwrap()),
				String::from_utf8_lossy(bytes),
				"{:?} did not re-encode to the bytes it decoded from",
				key.kind()
			);
			decoded += 1;
		}

		// Both counts are pinned. A layout change that quietly stops recognising a
		// key shows up as a smaller first number; one that makes a *bound* decode as
		// a key shows up as a larger one, and that is just as wrong — a bound has to
		// sit between keys, not on one.
		assert_eq!(
			(decoded, bounds.len()),
			(77, 36),
			"the corpus split moved; entries read as bounds:\n{}",
			bounds.join("\n")
		);
	}

	/// The index-build family, against its stored layout.
	///
	/// This family carries the build state, ticket counters and queues that an
	/// in-progress index build resumes from, so its bytes have to survive a restart
	/// on a store written by any other build of this layout.
	#[test]
	fn the_index_build_family_matches_the_stored_layout() {
		let tb = TableName::from("testtb");
		let id = RecordIdKey::String(Strand::new_static("id"));
		let of = |b: &[u8]| String::from_utf8_lossy(b).to_string();

		let state =
			BuildStateKey::new(NamespaceId(1), DatabaseId(2), Cow::Borrowed(&tb), IndexId(3));
		assert_eq!(
			of(&state.encode_key().unwrap()),
			of(b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0!bs\x00\x00\x00\x03")
		);

		let ticket =
			BuildTicketKey::new(NamespaceId(1), DatabaseId(2), Cow::Borrowed(&tb), IndexId(3), 7);
		assert_eq!(
			of(&ticket.encode_key().unwrap()),
			of(b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0!bt\x00\x00\x00\x03\
			     \x00\x00\x00\x00\x00\x00\x00\x07")
		);

		let reservation = BuildReservationKey::new(
			NamespaceId(1),
			DatabaseId(2),
			Cow::Borrowed(&tb),
			IndexId(3),
			7,
			11,
		);
		assert_eq!(
			of(&reservation.encode_key().unwrap()),
			of(b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0!br\x00\x00\x00\x03\
			     \x00\x00\x00\x00\x00\x00\x00\x07\x00\x00\x00\x00\x00\x00\x00\x0b")
		);

		let append = BuildAppendKey::new(
			NamespaceId(1),
			DatabaseId(2),
			Cow::Borrowed(&tb),
			IndexId(3),
			7,
			11,
			13,
		);
		assert_eq!(
			of(&append.encode_key().unwrap()),
			of(b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0!bg\x00\x00\x00\x03\
			     \x00\x00\x00\x00\x00\x00\x00\x07\x00\x00\x00\x00\x00\x00\x00\x0b\
			     \x00\x00\x00\x0d")
		);

		// Encoded under the index format, because the record id is part of the key.
		let primary = BuildPrimaryKey::new(
			NamespaceId(1),
			DatabaseId(2),
			Cow::Borrowed(&tb),
			IndexId(3),
			7,
			Cow::Borrowed(&id),
		);
		assert_eq!(
			of(&primary.encode_key().unwrap()),
			of(b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0!bp\x00\x00\x00\x03\
			     \x00\x00\x00\x00\x00\x00\x00\x07\x03id\0")
		);

		// A bound whose entry pins the index format must be encoded under it too, or
		// the bound and the keys it means to cover disagree. The generator inherits
		// the format rather than leaving it to be restated: these bytes are the
		// primary key's own, up to the record id.
		let bound = BuildPrimaryGenerationPrefix::new(
			NamespaceId(1),
			DatabaseId(2),
			Cow::Borrowed(&tb),
			IndexId(3),
			7,
		);
		assert_eq!(
			of(&bound.encode_bound().unwrap()),
			of(b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0!bp\x00\x00\x00\x03\
			     \x00\x00\x00\x00\x00\x00\x00\x07")
		);
	}

	/// Index entries, against their stored layout.
	///
	/// These two are the only keys built from a list segment and a raw
	/// discriminant byte: the list writes each element behind `mark_terminator()`
	/// and closes with the terminator, and the raw segment writes the byte that
	/// tells the two shapes apart.
	#[test]
	fn index_entries_match_the_hand_written_encoders() {
		let tb = TableName::from("testtb");
		let fd: Vec<Value> = vec![Value::from("testfd1"), Value::from("testfd2")];
		let id = RecordIdKey::String(Strand::new_static("testid"));

		let entry = EntryKey::new(
			NamespaceId(1),
			DatabaseId(2),
			Cow::Borrowed(&tb),
			IndexId(3),
			Cow::Borrowed(&fd),
			Cow::Borrowed(&id),
		);
		assert_eq!(
			&*entry.encode_key().unwrap(),
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03*\x06testfd1\0\x06testfd2\0\0\x03\x03testid\0"
		);

		let unique = UniqueKey::new(
			NamespaceId(1),
			DatabaseId(2),
			Cow::Borrowed(&tb),
			IndexId(3),
			Cow::Borrowed(&fd),
		);
		assert_eq!(
			&*unique.encode_key().unwrap(),
			b"/*\0\0\0\x01*\0\0\0\x02*testtb\0+\0\0\0\x03*\x06testfd1\0\x06testfd2\0\0\x02"
		);
	}

	/// The closed and open bounds over the indexed values differ by exactly the
	/// list terminator, which is what makes the open one bound a partial match.
	#[test]
	fn open_and_closed_value_bounds_differ_by_the_terminator() {
		let tb = TableName::from("testtb");
		let fd: Vec<Value> = vec![Value::from("testfd1")];

		let closed = EntryFdPrefix::new(
			NamespaceId(1),
			DatabaseId(2),
			Cow::Borrowed(&tb),
			IndexId(3),
			Cow::Borrowed(&fd),
		)
		.encode_bound()
		.unwrap();
		let open = EntryFdOpenPrefix::new(
			NamespaceId(1),
			DatabaseId(2),
			Cow::Borrowed(&tb),
			IndexId(3),
			Cow::Borrowed(&fd),
		)
		.encode_bound()
		.unwrap();

		assert_eq!(&*closed, [&*open, &[0u8][..]].concat(), "the closed bound adds the terminator");

		// An entry whose values merely start with `fd` is inside the open bound's
		// range but outside the closed one's.
		let longer: Vec<Value> = vec![Value::from("testfd1"), Value::from("extra")];
		let id = RecordIdKey::String(Strand::new_static("id"));
		let key = EntryKey::new(
			NamespaceId(1),
			DatabaseId(2),
			Cow::Borrowed(&tb),
			IndexId(3),
			Cow::Borrowed(&longer),
			Cow::Borrowed(&id),
		)
		.encode_key()
		.unwrap();

		let open_range = EntryFdOpenPrefix::new(
			NamespaceId(1),
			DatabaseId(2),
			Cow::Borrowed(&tb),
			IndexId(3),
			Cow::Borrowed(&fd),
		)
		.range()
		.unwrap();
		assert!(
			*open_range.start() <= key && key < *open_range.end(),
			"a longer value list is inside the open bound"
		);
	}

	/// The full-text families are declared prefix extensions, so a single scan
	/// covers a term's root and its postings and each still decodes to its own
	/// type.
	#[test]
	fn extension_families_share_a_scan_and_stay_distinguishable() {
		let tb = TableName::from("testtb");
		let root = TermDocsKey::new(
			NamespaceId(1),
			DatabaseId(2),
			Cow::Borrowed(&tb),
			IndexId(3),
			Cow::Borrowed("term"),
		);
		let posting = TermPostingKey::new(
			NamespaceId(1),
			DatabaseId(2),
			Cow::Borrowed(&tb),
			IndexId(3),
			Cow::Borrowed("term"),
			42,
		);

		let root_bytes = root.encode_key().unwrap();
		let posting_bytes = posting.encode_key().unwrap();
		assert!(
			posting_bytes.starts_with(&root_bytes),
			"a posting extends its term's bytes, which is what one scan relies on"
		);

		assert_eq!(AnyKey::decode(&root_bytes).map(|k| k.kind()), Some(KeyKind::TermDocs));
		assert_eq!(AnyKey::decode(&posting_bytes).map(|k| k.kind()), Some(KeyKind::TermPosting));

		// The three-deep change family resolves the same way.
		let terms =
			TermChangesKey::new(NamespaceId(1), DatabaseId(2), Cow::Borrowed(&tb), IndexId(3));
		let set = TermChangeSetKey::new(
			NamespaceId(1),
			DatabaseId(2),
			Cow::Borrowed(&tb),
			IndexId(3),
			Cow::Borrowed("term"),
		);
		let terms_bytes = terms.encode_key().unwrap();
		let set_bytes = set.encode_key().unwrap();
		assert!(set_bytes.starts_with(&terms_bytes));
		assert_eq!(AnyKey::decode(&terms_bytes).map(|k| k.kind()), Some(KeyKind::TermChanges));
		assert_eq!(AnyKey::decode(&set_bytes).map(|k| k.kind()), Some(KeyKind::TermChangeSet));
	}

	/// A bounded range contains the keys it bounds.
	///
	/// The bound is built from the truncation point plus one more field value, so
	/// it has to reproduce every byte the encoder writes in between — a separator
	/// dropped there puts the bound outside the run of keys it is meant to
	/// delimit, and the scan comes back empty or, worse, holding a neighbouring
	/// run.
	#[test]
	fn a_bounded_range_contains_the_keys_it_bounds() {
		let tb = TableName::from("testtb");
		let before = TableName::from("aaa");
		let after = TableName::from("zzz");
		let ts: Vec<u8> = ::std::vec![0, 0, 0, 0, 0, 0, 0, 9];

		let key = |tb: &TableName| {
			ChangeFeedKey::new(NamespaceId(1), DatabaseId(2), Cow::Borrowed(&ts), Cow::Borrowed(tb))
				.encode_key()
				.unwrap()
		};

		// A separator (`*`) sits between the timestamp this bound stops at and the
		// table name being bounded.
		let range = ChangeFeedTsPrefix::new(NamespaceId(1), DatabaseId(2), Cow::Borrowed(&ts))
			.range_where(Cow::Borrowed(&tb)..=Cow::Borrowed(&tb))
			.unwrap();
		let target = key(&tb);
		assert!(
			*range.start() <= target && target < *range.end(),
			"the change feed at one timestamp for one table must fall inside its own range"
		);
		assert!(key(&before) < *range.start(), "a table sorting before the bound is excluded");
		assert!(key(&after) >= *range.end(), "a table sorting after the bound is excluded");
	}

	/// An inclusive upper bound clears the keys that extend the bound.
	///
	/// A term's postings extend the term key itself, so the end of a range that
	/// includes the term has to be the successor of the whole run. The immediate
	/// successor would sit between the term and its first posting and silently drop
	/// every one of them.
	#[test]
	fn an_inclusive_bound_covers_the_keys_that_extend_it() {
		let tb = TableName::from("testtb");
		let range =
			TermDocsPrefix::new(NamespaceId(1), DatabaseId(2), Cow::Borrowed(&tb), IndexId(3))
				.range_where(Cow::Borrowed("term")..=Cow::Borrowed("term"))
				.unwrap();

		let root = TermDocsKey::new(
			NamespaceId(1),
			DatabaseId(2),
			Cow::Borrowed(&tb),
			IndexId(3),
			Cow::Borrowed("term"),
		)
		.encode_key()
		.unwrap();
		let posting = TermPostingKey::new(
			NamespaceId(1),
			DatabaseId(2),
			Cow::Borrowed(&tb),
			IndexId(3),
			Cow::Borrowed("term"),
			42,
		)
		.encode_key()
		.unwrap();

		assert!(
			*range.start() <= root && root < *range.end(),
			"the term itself is inside the range"
		);
		assert!(posting < *range.end(), "so is every posting that extends it");
	}

	/// The graph family, against the stored layout and its two contracts.
	///
	/// A pointer key extends an inner key byte for byte. That is what lets one
	/// scan return both, and it is also why an upper bound over inner keys must be
	/// the successor of the whole run rather than the immediate successor of one
	/// key: `skip_extensions` computes it, so no caller appends `0xff` by hand.
	#[test]
	fn the_graph_family_matches_the_stored_layout() {
		use surrealdb_expr::expr::dir::Dir;

		let tb = TableName::from("testtb");
		let ft = TableName::from("other");
		let id = RecordIdKey::String(Strand::new_static("a"));
		let fk = RecordIdKey::String(Strand::new_static("b"));

		let generated = GraphKey::new(
			NamespaceId(1),
			DatabaseId(2),
			Cow::Borrowed(&tb),
			Cow::Borrowed(&id),
			Dir::Out,
			Cow::Borrowed(&ft),
			Cow::Borrowed(&fk),
		);
		let inner = generated.encode_key().unwrap();
		assert_eq!(
			String::from_utf8_lossy(&inner),
			String::from_utf8_lossy(
				b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0~\x03a\0\x03other\0\x03b\0"
			)
		);

		let tt = TableName::from("target");
		let tk = RecordIdKey::String(Strand::new_static("c"));
		let pointer = GraphPointerKey::new(
			NamespaceId(1),
			DatabaseId(2),
			Cow::Borrowed(&tb),
			Cow::Borrowed(&id),
			Dir::Out,
			Cow::Borrowed(&ft),
			Cow::Borrowed(&fk),
			Cow::Borrowed(&tt),
			Cow::Borrowed(&tk),
		);
		let pointer_bytes = pointer.encode_key().unwrap();
		assert!(pointer_bytes.starts_with(&inner), "a pointer key extends its inner key");
		assert!(pointer_bytes.len() > inner.len());

		// The exclusive bound past an inner key and every pointer key built on it.
		// Every extension sorts below it, and the next inner key sorts at or above.
		let past = GraphKey::new(
			NamespaceId(1),
			DatabaseId(2),
			Cow::Borrowed(&tb),
			Cow::Borrowed(&id),
			Dir::Out,
			Cow::Borrowed(&ft),
			Cow::Borrowed(&fk),
		)
		.skip_extensions()
		.unwrap();
		assert!(inner < past && pointer_bytes < past, "the bound clears the whole run");

		let next_edge = GraphKey::new(
			NamespaceId(1),
			DatabaseId(2),
			Cow::Borrowed(&tb),
			Cow::Borrowed(&id),
			Dir::Out,
			Cow::Borrowed(&ft),
			Cow::Owned(RecordIdKey::String(Strand::new_static("c"))),
		)
		.encode_key()
		.unwrap();
		assert!(next_edge >= past, "the bound stops before the next edge");
	}

	/// The two halves of the graph decode contract: a pointer key tolerates bytes
	/// after its tail so a later format can append to it, while an inner key does
	/// not, because there trailing bytes mean corruption.
	#[test]
	fn the_graph_trailing_byte_contract_holds_in_both_directions() {
		use surrealdb_expr::expr::dir::Dir;

		let tb = TableName::from("testtb");
		let ft = TableName::from("other");
		let id = RecordIdKey::String(Strand::new_static("a"));
		let fk = RecordIdKey::String(Strand::new_static("b"));
		let inner = GraphKey::new(
			NamespaceId(1),
			DatabaseId(2),
			Cow::Borrowed(&tb),
			Cow::Borrowed(&id),
			Dir::Out,
			Cow::Borrowed(&ft),
			Cow::Borrowed(&fk),
		)
		.encode_key()
		.unwrap();

		let mut trailing = inner.to_vec();
		trailing.extend_from_slice(b"\x01\x02");
		assert!(GraphKey::decode_key(&trailing).is_err(), "an inner key rejects trailing bytes");

		let tt = TableName::from("target");
		let tk = RecordIdKey::String(Strand::new_static("c"));
		let pointer = GraphPointerKey::new(
			NamespaceId(1),
			DatabaseId(2),
			Cow::Borrowed(&tb),
			Cow::Borrowed(&id),
			Dir::Out,
			Cow::Borrowed(&ft),
			Cow::Borrowed(&fk),
			Cow::Borrowed(&tt),
			Cow::Borrowed(&tk),
		)
		.encode_key()
		.unwrap();
		let mut extended = pointer.to_vec();
		extended.extend_from_slice(b"\x01\x02");
		assert!(
			GraphPointerKey::decode_key(&extended).is_ok(),
			"a pointer key ignores bytes after its tail"
		);

		// Raw bytes still resolve to the more specific layout of the two.
		assert_eq!(AnyKey::decode(&inner).map(|k| k.kind()), Some(KeyKind::Graph));
		assert_eq!(AnyKey::decode(&pointer).map(|k| k.kind()), Some(KeyKind::GraphPointer));
	}

	/// A bound excludes its own bytes and stops before the next sibling subspace,
	/// with no sentinel byte written by hand anywhere.
	#[test]
	fn bounds_match_the_stored_layout() {
		let range = NamespacePrefix::new().range().unwrap();
		assert_eq!(&**range.start(), b"/!ns\0");
		assert_eq!(&**range.end(), b"/!nt");

		let range = RootUserPrefix::new().range().unwrap();
		assert_eq!(&**range.start(), b"/!us\0");
		assert_eq!(&**range.end(), b"/!ut");

		let range = IndexCompactionPrefix::new().range().unwrap();
		assert_eq!(&**range.start(), b"/!ic\0");
		assert_eq!(&**range.end(), b"/!id");

		let feed = ChangeFeedPrefix::new(NamespaceId(1), DatabaseId(2));
		assert_eq!(&*feed.encode_bound().unwrap(), b"/*\x00\x00\x00\x01*\x00\x00\x00\x02#");
	}

	/// A level root bounds its whole subtree, which is what a namespace or table
	/// drop relies on.
	#[test]
	fn a_level_root_bounds_its_subtree() {
		let root = NsRoot::new(NamespaceId(1));
		assert_eq!(&*root.encode_bound().unwrap(), b"/*\x00\x00\x00\x01");

		let range = root.range().unwrap();
		let inside = TableKey::new(NamespaceId(1), DatabaseId(2), Cow::Owned(TableName::from("t")))
			.encode_key()
			.unwrap();
		assert!(
			*range.start() <= inside && inside < *range.end(),
			"a table is inside its namespace"
		);

		let outside =
			TableKey::new(NamespaceId(2), DatabaseId(2), Cow::Owned(TableName::from("t")))
				.encode_key()
				.unwrap();
		assert!(outside >= *range.end(), "another namespace is outside");
	}

	/// The index-compaction queue's per-index bound brackets exactly one index,
	/// which is what lets a worker claim one index's work without touching the
	/// next.
	#[test]
	fn a_queue_bound_brackets_exactly_one_index() {
		let tb = TableName::from("testtb");
		let entry = |ix: u32| {
			IndexCompactionKey::new(
				NamespaceId(1),
				DatabaseId(2),
				Cow::Borrowed(&tb),
				IndexId(ix),
				Uuid::from_u128(0),
				Uuid::from_u128(0),
			)
			.encode_key()
			.unwrap()
		};

		let range = IndexCompactionTbPrefix::new(NamespaceId(1), DatabaseId(2), Cow::Borrowed(&tb))
			.range_where(IndexId(3)..=IndexId(3))
			.unwrap();

		assert!(*range.start() <= entry(3) && entry(3) < *range.end(), "index 3 is in range");
		assert!(entry(4) >= *range.end(), "index 4 is out of range");
	}

	/// Builders are the only way to name a child, so a key cannot be built with
	/// parent identifiers that do not belong to it.
	#[test]
	fn a_parent_names_its_children() {
		let root = DbRoot::new(NamespaceId(1), DatabaseId(2));
		let tb = TableName::from("testtb");
		let built = root.table_key(Cow::Borrowed(&tb));
		let direct = TableKey::new(NamespaceId(1), DatabaseId(2), Cow::Borrowed(&tb));
		assert_eq!(built.encode_key().unwrap(), direct.encode_key().unwrap());
	}

	/// Raw bytes identify themselves, so a scan can interpret what it finds
	/// instead of assuming what it should find.
	#[test]
	fn keys_identify_themselves_from_their_bytes() {
		let tb = TableName::from("testtb");
		let table = TableKey::new(NamespaceId(1), DatabaseId(2), Cow::Borrowed(&tb));
		let bytes = table.encode_key().unwrap();
		let decoded = AnyKey::decode(&bytes).expect("a generated key must decode");
		assert_eq!(decoded.kind(), KeyKind::Table);
		assert_eq!(decoded.kind().route(), "/*{ns}*{db}!tb{tb}");

		let namespace = NamespaceKey::new(Cow::Borrowed("test")).encode_key().unwrap();
		assert_eq!(AnyKey::decode(&namespace).map(|k| k.kind()), Some(KeyKind::Namespace));

		// The same `!us` tag appears at three levels; only the surrounding
		// structure tells them apart, which is what a per-key decoder could not do.
		let root_user = RootUserKey::new(Cow::Borrowed("u")).encode_key().unwrap();
		assert_eq!(AnyKey::decode(&root_user).map(|k| k.kind()), Some(KeyKind::RootUser));
		let ns_user = NsUserKey::new(NamespaceId(1), Cow::Borrowed("u")).encode_key().unwrap();
		assert_eq!(AnyKey::decode(&ns_user).map(|k| k.kind()), Some(KeyKind::NsUser));
		let db_user =
			DbUserKey::new(NamespaceId(1), DatabaseId(2), Cow::Borrowed("u")).encode_key().unwrap();
		assert_eq!(AnyKey::decode(&db_user).map(|k| k.kind()), Some(KeyKind::DbUser));
	}

	/// Every field survives a round trip through the decoder.
	#[test]
	fn decoding_recovers_every_field() {
		let original =
			TableKey::new(NamespaceId(7), DatabaseId(9), Cow::Owned(TableName::from("abc")));
		let bytes = original.encode_key().unwrap();
		assert_eq!(TableKey::decode_key(&bytes).unwrap(), original);

		let grant = RootGrantKey::new(Cow::Borrowed("ac"), Cow::Borrowed("gr"));
		let bytes = grant.encode_key().unwrap();
		assert_eq!(RootGrantKey::decode_key(&bytes).unwrap(), grant);
	}

	/// A table whose name begins with `sq` encodes outside the range that lists
	/// sequence definitions, and so does a sequence's allocator state. Both are
	/// adjacent to the `!sd` tag and neither may fall inside it.
	#[test]
	fn a_table_named_sq_falls_outside_the_sequence_definitions() {
		let range = SequencePrefix::new(NamespaceId(1), DatabaseId(2)).range().unwrap();
		let table = TableName::from("sqfoo");
		let record = RecordKey::new(
			NamespaceId(1),
			DatabaseId(2),
			Cow::Borrowed(&table),
			Cow::Owned(RecordIdKey::String(Strand::new_static("id"))),
		)
		.encode_key()
		.unwrap();

		assert!(
			record < *range.start() || record >= *range.end(),
			"a record of table `sqfoo` still falls inside the sequence definition range"
		);

		// `!sq{name}` is a strict prefix of the allocator state, so a definition
		// tagged `!sq` would sit inside a scan of it; `!sd` keeps them apart.
		let state = SeqStateKey::new(
			NamespaceId(1),
			DatabaseId(2),
			Cow::Borrowed("foo"),
			Uuid::from_u128(0),
		)
		.encode_key()
		.unwrap();
		assert!(state < *range.start() || state >= *range.end());
	}

	/// Bytes belonging to no declared key are reported, not guessed at.
	#[test]
	fn unknown_bytes_are_described_rather_than_misread() {
		assert!(AnyKey::decode(b"/!zz-nonsense").is_none());
		let described = describe(b"/!zz-nonsense").to_string();
		assert!(described.contains("unrecognised key"), "got {described}");

		// A partial match reports how far it got, which is what makes a corrupt
		// key diagnosable rather than merely rejected.
		let described = describe(b"/!ns").to_string();
		assert!(described.contains("matched"), "got {described}");
	}

	/// A description names the routes the bytes actually got closest to.
	///
	/// This is the difference between a usable report and a dump of the keyspace:
	/// the match walks each layout, stepping over the identifiers it cannot read,
	/// rather than comparing the leading tag that nearly every key shares.
	#[test]
	fn a_partial_match_reports_where_it_diverged() {
		let unknown = |bytes: &[u8]| match describe(bytes) {
			KeyDescription::Unknown {
				matched,
				candidates,
				..
			} => (matched, candidates),
			other => panic!("expected undecodable bytes, got {other}"),
		};

		// A root-level tag that does not exist. Only the routes sharing `/!n` are
		// reported, and they are the whole answer.
		let (matched, candidates) = unknown(b"/!nx");
		assert_eq!(matched, 3);
		assert_eq!(candidates, ["/!ns{ns}", "/!nd{nd}", "/!nh{start}", "/!ni{nid}"]);

		// A well-formed table-level prefix with an undeclared tag after it. The
		// namespace and database ids are stepped over rather than compared, so the
		// match reaches the table name.
		let mut bytes = b"/*".to_vec();
		bytes.extend_from_slice(&1u32.to_be_bytes());
		bytes.push(b'*');
		bytes.extend_from_slice(&2u32.to_be_bytes());
		bytes.extend_from_slice(b"*person\0!zz");

		let (matched, candidates) = unknown(&bytes);
		assert_eq!(matched, 12, "the match should reach the start of the table name");
		assert!(
			candidates.iter().all(|route| route.starts_with("/*{ns}*{db}*")),
			"every candidate shares the matched prefix: {candidates:?}"
		);

		// A table name is variable-width, so nothing past it sits at a known
		// offset and every key beneath the table is equally close. The report is
		// still bounded, because it is printed once per unrecognised key.
		let line = describe(&bytes).to_string();
		assert!(line.len() < 500, "a description is one readable line, got {} bytes", line.len());
	}

	/// The map is generated from the same schema as the encoders, so it lists
	/// every route.
	#[test]
	fn the_keyspace_map_documents_every_route() {
		for pattern in PATTERNS {
			assert!(
				KEYSPACE_MAP.contains(pattern.route),
				"{} missing from the keyspace map",
				pattern.route
			);
		}
	}

	/// Every tag that appears in stored data has a declared counterpart.
	///
	/// The list is written out here rather than derived, so it checks coverage in
	/// one direction only: it cannot notice a tag that exists in data but was never
	/// added to this list, and it says nothing about the bound-only regions or level
	/// roots that `PATTERNS` does not carry. What it does catch is a declared tag
	/// being dropped or renamed, which would orphan the data written under it.
	///
	/// The check in the other direction is `kvs::tests::keyspace_test`: it drives
	/// real statements through the engine, scans the whole store and fails on any
	/// key the declared keyspace cannot name.
	///
	/// The check is on tags rather than on names, because the generated names are
	/// derived and deliberately differ.
	#[test]
	fn every_stored_tag_is_declared() {
		// Tag sequences that appear in stored keys, as sigils and tag letters with
		// field values elided.
		#[cfg_attr(not(diskann), allow(unused_mut))]
		let mut stored_tags: Vec<&str> = ::std::vec![
			"!v",
			"/!ns",
			"/!nd",
			"/!us",
			"/!ac",
			"/!cg",
			"/!tl",
			"/!se",
			"/!nh",
			"/!ni",
			"/!eq",
			"/!ic",
			"/!rc",
			"/&!gr",
			"/$!lq",
			"/*!db",
			"/*!us",
			"/*!ac",
			"/*!dh",
			"/*!di",
			"/*&!gr",
			"/**!tb",
			"/**!us",
			"/**!ac",
			"/**!ap",
			"/**!az",
			"/**!bu",
			"/**!cg",
			"/**!fn",
			"/**!md",
			"/**!ml",
			"/**!pa",
			"/**!th",
			"/**!sd",
			"/**!ti",
			"/**#*",
			"/**%*",
			"/**!sq!ba",
			"/**!sq!st",
			"/**&!gr",
			"/***!ev",
			"/***!fd",
			"/***!ft",
			"/***!il",
			"/***!ix",
			"/***!lq",
			"/***!dd",
			"/***!dh",
			"/***!di",
			"/***!dp",
			"/***!ds",
			"/***!ih",
			"/***!is",
			"/***!bs",
			"/***!bt",
			"/***!br",
			"/***!bg",
			"/***!bp",
			"/****",
			"/***~",
			"/***&",
			"/***+!dc",
			"/***+!dl",
			"/***+!dv",
			"/***+!hh",
			"/***+!he",
			"/***+!hg",
			"/***+!hl",
			"/***+!hn",
			"/***+!hr",
			"/***+!hs",
			"/***+!hv",
			"/***+!ig",
			"/***+!ip",
			"/***+!iu",
			"/***+!iv",
			"/***+!td",
			"/***+!tt",
			"/***+!tv",
			// The two index entry shapes share their tag up to the trailing
			// discriminant that tells them apart.
			"/***+*\\x03",
			"/***+*\\x02",
		];

		// `build.rs` turns this on for every 64-bit non-WASM build, so these keys
		// are live in ordinary builds and belong in the checklist under the same
		// condition the declarations carry.
		#[cfg(diskann)]
		stored_tags.extend_from_slice(&[
			"/***+!de", "/***+!dg", "/***+!dh", "/***+!dn", "/***+!dp", "/***+!dq", "/***+!dr",
			"/***+!ds", "/***+!dw", "/***+!dy",
		]);

		// Reduce every declared route the same way: drop field placeholders.
		let declared: Vec<String> = PATTERNS
			.iter()
			.map(|p| {
				let mut out = String::new();
				let mut depth = 0usize;
				for c in p.route.chars() {
					match c {
						'{' => depth += 1,
						'}' => depth = depth.saturating_sub(1),
						_ if depth == 0 => out.push(c),
						_ => {}
					}
				}
				out
			})
			.collect();

		let missing: Vec<&str> =
			stored_tags.iter().copied().filter(|tag| !declared.iter().any(|d| d == tag)).collect();
		assert!(missing.is_empty(), "these stored keys are not declared: {missing:?}");
	}

	/// The map is checked in, so any change to the keyspace shows up in review as
	/// a readable diff of routes rather than only as a diff of the schema.
	///
	/// Regenerate with `RESULT=OVERWRITE cargo test -p surrealdb-datastore keyspace_map`
	/// and read the diff before committing it.
	#[test]
	fn the_keyspace_map_matches_its_snapshot() {
		const SNAPSHOT: &str = include_str!("keyspace.map");
		if std::env::var("RESULT").as_deref() == Ok("OVERWRITE") {
			let path = concat!(env!("CARGO_MANIFEST_DIR"), "/src/key/keyspace.map");
			std::fs::write(path, KEYSPACE_MAP).expect("write the keyspace map snapshot");
			return;
		}
		if KEYSPACE_MAP == SNAPSHOT {
			return;
		}

		// Report only the routes that moved. Printing both copies of a map this
		// size buries the one line that actually changed.
		let mut report = String::from(
			"the keyspace layout changed; rerun with `RESULT=OVERWRITE` and review the diff\n",
		);
		let current: Vec<&str> = KEYSPACE_MAP.lines().collect();
		let snapshot: Vec<&str> = SNAPSHOT.lines().collect();
		for line in &current {
			if !snapshot.contains(line) {
				report.push_str(&format!("+ {}\n", line.trim_end()));
			}
		}
		for line in &snapshot {
			if !current.contains(line) {
				report.push_str(&format!("- {}\n", line.trim_end()));
			}
		}
		panic!("{report}");
	}
}
