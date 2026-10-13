//! The fold: moving per-edge pointer keys into packed adjacency blocks.
//!
//! A fold rewrites one (vertex, direction) scope inside the caller's
//! transaction: it captures the scope's pointer keys — values included —
//! merges them into the packed chunks they overlap, writes the rewritten
//! chunks, and deletes exactly the keys it captured, each conditioned on
//! the captured value so a concurrent overwrite (an edge deleted into a
//! tombstone, or a tombstoned edge re-related) fails the transaction
//! instead of being erased. Correctness never depends on a fold running:
//! the merged reader treats the delta side as authoritative, so a scope
//! folds when it folds.
//!
//! Two kinds of pointer key cannot be packed and stay in the delta layer,
//! served by the merged reader: legacy-format keys (no embedded target — a
//! block entry without a target could not serve the reads pointer keys
//! serve) until a re-RELATE migrates them, and values carrying a section
//! this build does not interpret (block entries hold only the props
//! section, so packing would discard the rest) until a build that
//! interprets the section folds them.
//!
//! The first fold against a vertex table sets the table's `graph_folded`
//! marker in the same transaction, which is what switches edge deletion
//! on that table from key deletion to tombstone writes; see
//! `StoredTableDefinition::graph_folded` for the contract.
//!
//! With numeric compression enabled (`graph_numeric_ids`), rewritten
//! chunks hold table-level doc-IDs instead of record-key bytes — the fold
//! is the assignment point, batch-resolving ids in each record's table
//! space — and every table whose ids a chunk holds gains the
//! `graph_doc_ids` marker in the same transaction, which is what keeps
//! record deletion removing mappings and the doc-ID reclaim gates from
//! wiping the space while blocks still resolve through it. Recording a
//! marker also claims the table's space from any reclaim queued before it
//! (`Transaction::claim_tb_doc_ids_reclaim`), like every other consumer.
//! A chunk is compressed only when the edge table and every target table
//! it names are defined and claimable; otherwise it stays plain. Existing
//! plain chunks migrate on their next fold, and numeric chunks decompress
//! the same way when the knob is turned back off.

use std::borrow::Cow;
use std::collections::{BTreeMap, HashSet};
use std::ops::Bound;
use std::sync::Arc;

use anyhow::{Context as _, Result, ensure};
use surrealdb_datastore::values::graph::{
	ADJACENCY_FLAG_PROPS, AdjacencyBlock, AdjacencyValue, BlockEntries, BlockEntry, NumericEntry,
	ivarint_len, uvarint_len,
};
use surrealdb_datastore::{DocIdsReclaimClaim, Transaction};
use surrealdb_kvs::consts::NORMAL_BATCH_SIZE;
use surrealdb_kvs::{Direction, Key};
use uuid::Uuid;

use super::resolve::resolve_doc_ids;
use crate::catalog::providers::TableProvider;
use crate::catalog::{DatabaseId, NamespaceId, TableDefinition};
use crate::docids::{DocId, TableDocIds};
use crate::env::IndexEnv;
use crate::expr::Dir;
use crate::key::schema::{AdjacencyEdgeTablePrefix, AdjacencyKey, DecodedGraph, GraphDirPrefix};
use crate::key::{KVKey, KVKeyDecode, KVValue};
use crate::val::{RecordId, RecordIdKey, TableName};

/// Split threshold: a rewritten chunk closes once it reaches this many
/// entries.
pub(super) const CHUNK_MAX_ENTRIES: usize = 512;

/// Split threshold: a rewritten chunk closes once its encoded entries
/// reach this many bytes. Comfortably under every backend's value limits
/// and around one storage page.
pub(super) const CHUNK_MAX_BYTES: usize = 8 * 1024;

/// How many existing chunks one fold pass may load and rewrite per edge
/// table. Bounds the pass's transaction size independently of the delta
/// bound: scattered deltas (random record ids) span essentially the whole
/// scope, so without this the rewrite would read and re-write every chunk
/// of an arbitrarily large scope in one transaction. At [`CHUNK_MAX_BYTES`]
/// this caps the rewrite around half a megabyte.
const MAX_CHUNKS_PER_PASS: usize = 64;

/// What one fold pass over a scope did.
#[derive(Debug, Default, Clone, Copy)]
pub struct FoldOutcome {
	/// Pointer keys consumed into blocks (tombstones consumed count too).
	pub folded: usize,
	/// Whether foldable keys remained beyond the pass's capture or
	/// chunk-rewrite bounds; the scope needs another pass (in a fresh
	/// transaction) to drain.
	pub has_more: bool,
}

/// Folds one (vertex, direction) scope inside the environment's
/// transaction.
///
/// The pass's transaction size is bounded twice over: at most `max_deltas`
/// foldable pointer keys are captured, and at most [`MAX_CHUNKS_PER_PASS`]
/// existing chunks are rewritten per edge table — deltas beyond a pass's
/// chunk coverage stay uncaptured. A scope with more work reports
/// `has_more` and is drained by repeated calls, each in its own
/// transaction. The caller owns commit and the retry on a conflicting
/// concurrent write.
///
/// `ns_name` / `db_name` address the catalog writes that set the fold
/// markers.
#[allow(clippy::too_many_arguments)]
pub async fn fold_scope(
	env: &dyn IndexEnv,
	ns: NamespaceId,
	db: DatabaseId,
	ns_name: &str,
	db_name: &str,
	vertex: &RecordId,
	dir: Dir,
	max_deltas: usize,
) -> Result<FoldOutcome> {
	fold_scope_bounded(env, ns, db, ns_name, db_name, vertex, dir, max_deltas, MAX_CHUNKS_PER_PASS)
		.await
}

/// As [`fold_scope`], with the per-edge-table chunk-rewrite bound explicit
/// instead of fixed at [`MAX_CHUNKS_PER_PASS`].
#[allow(clippy::too_many_arguments)]
pub(super) async fn fold_scope_bounded(
	env: &dyn IndexEnv,
	ns: NamespaceId,
	db: DatabaseId,
	ns_name: &str,
	db_name: &str,
	vertex: &RecordId,
	dir: Dir,
	max_deltas: usize,
	max_chunks: usize,
) -> Result<FoldOutcome> {
	let txn = env.tx();
	// Capture the scope's foldable pointer keys: raw key, raw value, and
	// the decoded fields the block entries need. Bounded by `max_deltas`;
	// a zero bound would report `has_more` forever without progressing.
	let max_deltas = max_deltas.max(1);
	let mut captured: Vec<CapturedDelta> = Vec::new();
	let mut has_more = false;
	{
		let range = GraphDirPrefix {
			ns,
			db,
			tb: Cow::Borrowed(&vertex.table),
			id: Cow::Borrowed(&vertex.key),
			dir,
		}
		.range()?;
		let mut cursor = txn.open_vals_cursor_raw(range, Direction::Forward, 0, None).await?;
		'scan: loop {
			let batch = cursor
				.next_batch(NORMAL_BATCH_SIZE)
				.await
				.context("Failed to scan pointer keys for folding")?;
			if batch.is_empty() {
				break;
			}
			for (key, val) in &batch {
				let decoded = DecodedGraph::decode(key)?;
				// Legacy-format keys carry no target and cannot be packed.
				let Some(target) = decoded.target else {
					continue;
				};
				let value = AdjacencyValue::kv_decode_value(val, ())?;
				// A value carrying a section this build does not interpret
				// cannot be packed either: block entries hold only the props
				// section, so folding it would discard the rest. It stays in
				// the delta layer — where the merged reader serves it
				// verbatim — until a build that interprets the bit folds it.
				if value
					.sections
					.iter()
					.any(|s| s.flag_bit != ADJACENCY_FLAG_PROPS.trailing_zeros() as u8)
				{
					continue;
				}
				if captured.len() >= max_deltas {
					has_more = true;
					break 'scan;
				}
				captured.push(CapturedDelta {
					key: key.to_vec(),
					value_bytes: val.to_vec(),
					value,
					edge: decoded.edge,
					target,
				});
			}
		}
	}
	if captured.is_empty() {
		return Ok(FoldOutcome {
			folded: 0,
			has_more,
		});
	}

	// Rewrite the packed chunks each edge table's captured run overlaps.
	// A group whose span covers more chunks than the rewrite bound merges
	// only a prefix of its deltas; the rest stay in the delta layer for a
	// later pass.
	let mut groups: BTreeMap<TableName, Vec<CapturedDelta>> = BTreeMap::new();
	for delta in captured {
		groups.entry(delta.edge.table.clone()).or_default().push(delta);
	}
	let mut folded = 0;
	let mut consumed_counts: Vec<usize> = Vec::with_capacity(groups.len());
	for (edge_table, deltas) in &groups {
		let consumed = fold_edge_table(
			env, &txn, ns, db, ns_name, db_name, vertex, dir, edge_table, deltas, max_chunks,
		)
		.await?;
		if consumed < deltas.len() {
			has_more = true;
		}
		folded += consumed;
		consumed_counts.push(consumed);
	}

	// The first fold against this vertex table flips its `graph_folded`
	// marker, transactionally with the blocks it justifies. `tb` above may
	// come from the transaction's cache rather than a read of the
	// underlying key, so the flip goes through `replace_tb`, whose locked
	// read of the exact table-definition key serializes it against every
	// concurrent writer of that key on every backend: two racing first
	// folds cannot both commit the flip, and a racing catalog refresh
	// (DDL rewriting the definition from a pre-flip snapshot) cannot
	// clobber the marker back to false — whichever commits second
	// conflicts and retries from a fresh read.
	let tb = txn
		.get_tb(ns, db, &vertex.table, None)
		.await?
		.context("A fold targeted a vertex table that is not defined")?;
	if !tb.graph_folded {
		let mut updated = (*tb).clone();
		updated.graph_folded = true;
		updated.cache_tables_ts = Uuid::now_v7();
		txn.replace_tb(ns_name, db_name, &updated).await?;
	}

	// Delete exactly the keys the rewrite consumed (the merged prefix of
	// each group), each conditioned on its captured value: a pointer key
	// concurrently rewritten (delete → tombstone, or re-RELATE over a
	// tombstone) fails the fold instead of losing the newer write.
	for (group, consumed) in groups.values().zip(&consumed_counts) {
		for delta in &group[..*consumed] {
			txn.del_compare(Key::from(delta.key.as_slice()), Some(&delta.value_bytes)).await?;
		}
	}

	Ok(FoldOutcome {
		folded,
		has_more,
	})
}

struct CapturedDelta {
	key: Vec<u8>,
	value_bytes: Vec<u8>,
	value: AdjacencyValue,
	edge: RecordId,
	target: RecordId,
}

/// One merged entry awaiting repacking, in the pointer-key byte domain.
struct ResolvedEntry {
	edge_key: Vec<u8>,
	target_table: TableName,
	target_key: Vec<u8>,
	props: Option<Vec<u8>>,
	/// The doc-IDs carried from the numeric chunk the entry came from;
	/// `None` for plain-sourced entries and captured deltas, which are
	/// batch-resolved before the repack encodes them.
	edge_doc: Option<DocId>,
	target_doc: Option<DocId>,
}

/// Merges a prefix of one edge table's captured deltas into the chunks
/// they overlap and writes the rewritten chunks, loading at most
/// `max_chunks` existing chunks. When the deltas' span covers more chunks
/// than that, only the deltas sorting below the first unloaded chunk's
/// start are merged — unloaded chunk boundaries are never shifted, so the
/// block side stays in global entry order — and the rest are left for a
/// later pass. Returns how many deltas (always a non-empty prefix of
/// `deltas`) were merged.
#[allow(clippy::too_many_arguments)]
async fn fold_edge_table(
	env: &dyn IndexEnv,
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	ns_name: &str,
	db_name: &str,
	vertex: &RecordId,
	dir: Dir,
	edge_table: &TableName,
	deltas: &[CapturedDelta],
	max_chunks: usize,
) -> Result<usize> {
	let prefix = || AdjacencyEdgeTablePrefix {
		ns,
		db,
		tb: Cow::Borrowed(&vertex.table),
		id: Cow::Borrowed(&vertex.key),
		dir,
		edge_table: Cow::Borrowed(edge_table),
	};

	// The captured run's span, in edge-key order (the capture scan emits
	// keys in that order, so first/last bound it).
	let span_low = &deltas.first().expect("a group holds at least one delta").edge.key;
	let span_high = &deltas.last().expect("a group holds at least one delta").edge.key;

	// The chunks that could hold an entry inside the span: from the
	// covering chunk of the low end through the last chunk starting at or
	// before the high end — loaded up to `max_chunks` of them, plus one
	// extra key to learn where the pass's coverage stops. The first
	// unloaded chunk's start is the pass's boundary: the covering chunk of
	// every delta below it is loaded, so tombstones subtract the entries
	// they target and live deltas repack next to their neighbours, while
	// deltas at or beyond it are not merged at all.
	let covering = txn
		.keysr_raw(
			prefix().range_where((Bound::Unbounded, Bound::Included(Cow::Borrowed(span_low))))?,
			1,
			0,
			None,
		)
		.await?;
	let read_from: Bound<Cow<RecordIdKey>> = match covering.into_iter().next() {
		Some(key) => {
			let covering = AdjacencyKey::decode_key(&key)
				.context("Failed to decode an adjacency block key")?;
			Bound::Included(Cow::Owned(covering.chunk_start.into_owned()))
		}
		None => Bound::Included(Cow::Borrowed(span_low)),
	};
	let max_chunks = max_chunks.max(1);
	let mut affected = txn
		.scan(
			prefix().range_where((read_from, Bound::Included(Cow::Borrowed(span_high))))?,
			u32::try_from(max_chunks.saturating_add(1)).unwrap_or(u32::MAX),
			0,
			None,
		)
		.await?;
	let boundary: Option<Vec<u8>> = if affected.len() > max_chunks {
		let (key, _) = affected.pop().expect("a longer-than-bound scan holds an extra chunk");
		let key =
			AdjacencyKey::decode_key(&key).context("Failed to decode an adjacency block key")?;
		Some(storekey::encode_vec(&key.chunk_start).map_err(anyhow::Error::from_boxed)?)
	} else {
		None
	};

	// The consumed deltas: the prefix sorting below the boundary. The
	// first delta always does — its covering chunk is the first one loaded
	// — so every pass makes progress.
	let mut consumed = deltas.len();
	if let Some(boundary) = &boundary {
		consumed = 0;
		for delta in deltas {
			let edge_key =
				storekey::encode_vec(&delta.edge.key).map_err(anyhow::Error::from_boxed)?;
			if edge_key.as_slice() >= boundary.as_slice() {
				break;
			}
			consumed += 1;
		}
	}
	let deltas = &deltas[..consumed];

	// Merge: existing block entries and captured deltas, keyed on the full
	// entry identity — edge key and target — in the same order the pointer
	// keys sort. A live delta replaces or inserts its identity; a tombstone
	// removes exactly the identity it was written over. Identity includes
	// the target because a deleted edge id can be re-related to a different
	// target: its tombstone (old target) and its new live key then coexist
	// in the delta layer, and an edge-only identity would let whichever
	// sorts later erase the other.
	type MergedIdentity = (Vec<u8>, Vec<u8>, Vec<u8>);
	/// One merged entry's payload. Numeric-sourced entries carry the ids
	/// they were decoded from, so the repack reuses them instead of
	/// re-resolving; a delta overwriting such an entry resets them.
	struct MergedEntry {
		target_table: TableName,
		props: Option<Vec<u8>>,
		edge_doc: Option<DocId>,
		target_doc: Option<DocId>,
	}
	let mut merged: BTreeMap<MergedIdentity, MergedEntry> = BTreeMap::new();
	let mut old_keys: Vec<Key<'static>> = Vec::new();
	for (key, block) in &affected {
		old_keys.push(Key::from(key.as_slice()).into_static());
		match &block.entries {
			BlockEntries::Plain(entries) => {
				for entry in entries {
					let target_table = block
						.targets
						.get(entry.target_table as usize)
						.expect("block decode validated every dictionary index")
						.clone();
					let target_table_key =
						storekey::encode_vec(&target_table).map_err(anyhow::Error::from_boxed)?;
					merged.insert(
						(entry.edge_key.clone(), target_table_key, entry.target_key.clone()),
						MergedEntry {
							target_table,
							props: entry.props.clone(),
							edge_doc: None,
							target_doc: None,
						},
					);
				}
			}
			BlockEntries::Numeric(entries) => {
				// Resolve the chunk back into the byte domain the merge
				// works in. An id that no longer resolves marks a deleted
				// record: the fold is where such residue is collected. The
				// resolve cache is keyed per table generation, so each
				// table's id comes from this transaction's snapshot.
				let edge_ids: Vec<DocId> = entries.iter().map(|e| e.edge).collect();
				let cache = Some(env.index_stores().adjacency_resolve());
				let edge_tb_id = txn.get_tb(ns, db, edge_table, None).await?.map(|tb| tb.table_id);
				let edge_keys =
					resolve_doc_ids(txn, ns, db, edge_table, edge_tb_id, &edge_ids, None, cache)
						.await?;
				let mut target_keys: Vec<Option<RecordIdKey>> = vec![None; entries.len()];
				for (dict, table) in block.targets.iter().enumerate() {
					let slots: Vec<usize> = entries
						.iter()
						.enumerate()
						.filter(|(_, e)| e.target_table as usize == dict)
						.map(|(i, _)| i)
						.collect();
					let ids: Vec<DocId> = slots.iter().map(|&i| entries[i].target).collect();
					let table_id = txn.get_tb(ns, db, table, None).await?.map(|tb| tb.table_id);
					let keys =
						resolve_doc_ids(txn, ns, db, table, table_id, &ids, None, cache).await?;
					for (&slot, key) in slots.iter().zip(keys) {
						target_keys[slot] = key;
					}
				}
				for (entry, (edge_key, target_key)) in
					entries.iter().zip(edge_keys.into_iter().zip(target_keys))
				{
					let (Some(edge_key), Some(target_key)) = (edge_key, target_key) else {
						continue;
					};
					let target_table = block
						.targets
						.get(entry.target_table as usize)
						.expect("block decode validated every dictionary index")
						.clone();
					let target_table_key =
						storekey::encode_vec(&target_table).map_err(anyhow::Error::from_boxed)?;
					merged.insert(
						(
							storekey::encode_vec(&edge_key).map_err(anyhow::Error::from_boxed)?,
							target_table_key,
							storekey::encode_vec(&target_key).map_err(anyhow::Error::from_boxed)?,
						),
						MergedEntry {
							target_table,
							props: entry.props.clone(),
							edge_doc: Some(entry.edge),
							target_doc: Some(entry.target),
						},
					);
				}
			}
		}
	}
	for delta in deltas {
		let edge_key = storekey::encode_vec(&delta.edge.key).map_err(anyhow::Error::from_boxed)?;
		let target_table_key =
			storekey::encode_vec(&delta.target.table).map_err(anyhow::Error::from_boxed)?;
		let target_key =
			storekey::encode_vec(&delta.target.key).map_err(anyhow::Error::from_boxed)?;
		let identity = (edge_key, target_table_key, target_key);
		if delta.value.is_tombstone() {
			merged.remove(&identity);
		} else {
			let props = delta
				.value
				.section(ADJACENCY_FLAG_PROPS.trailing_zeros() as u8)
				.map(<[u8]>::to_vec);
			merged.insert(
				identity,
				MergedEntry {
					target_table: delta.target.table.clone(),
					props,
					edge_doc: None,
					target_doc: None,
				},
			);
		}
	}
	let entries: Vec<ResolvedEntry> = merged
		.into_iter()
		.map(|((edge_key, _, target_key), entry)| ResolvedEntry {
			edge_key,
			target_table: entry.target_table,
			target_key,
			props: entry.props,
			edge_doc: entry.edge_doc,
			target_doc: entry.target_doc,
		})
		.collect();

	// Whether the rewritten chunks compress to doc-IDs: the knob must be
	// on, the edge table and every target table must be defined — the
	// markers below are what keep those tables' doc-ID spaces alive, and a
	// marker cannot be recorded on a table that does not exist — and every
	// unmarked table's space must be claimable from any queued reclaim.
	let numeric = if env.config().graph_numeric_ids {
		let mut involved: HashSet<&TableName> = HashSet::new();
		involved.insert(edge_table);
		involved.extend(entries.iter().map(|e| &e.target_table));

		// First pass: look up every involved table without writing anything.
		// Whether the chunk compresses is all-or-nothing across `involved`,
		// so no table's marker may be written before every table's
		// definedness is known — otherwise which tables end up marked would
		// depend on `involved`'s unspecified iteration order instead of on
		// the outcome.
		let mut defined: Vec<(&TableName, Arc<TableDefinition>)> =
			Vec::with_capacity(involved.len());
		let mut compress = true;
		for table in &involved {
			match txn.get_tb(ns, db, table, None).await? {
				Some(tb) => defined.push((*table, tb)),
				None => {
					compress = false;
					break;
				}
			}
		}

		// Second pass: the marker makes each unmarked table's shared doc-ID
		// space load-bearing, so the space must be claimed from any queued
		// reclaim before the marker commits — dropping a table's last
		// doc-ID index while it is unmarked enqueues exactly such a
		// reclaim, and letting it drain would wipe the `!di`/`!dd` mappings
		// the new blocks resolve through, while a part-drained queue could
		// hand the fold a torn mapping to adopt. Probe first, across every
		// table: like definedness, the decision is all-or-nothing, and
		// probing keeps one unclaimable table from leaving another table's
		// queued reclaim half-cancelled in this transaction.
		if compress {
			for (table, tb) in &defined {
				if !tb.graph_doc_ids
					&& txn.probe_tb_doc_ids_reclaim(ns, db, table).await?
						== DocIdsReclaimClaim::InProgress
				{
					compress = false;
					break;
				}
			}
		}

		// Third pass: every table is defined and claimable, so cancel the
		// queued reclaims and record the markers, in the same transaction
		// as the chunks that need them. The claim's conditional deletes
		// turn a race with a concurrent reaper page into a commit conflict,
		// which the fold's caller already retries.
		if compress {
			for (table, tb) in defined {
				if !tb.graph_doc_ids {
					let claim = txn.claim_tb_doc_ids_reclaim(ns, db, table).await?;
					ensure!(
						claim == DocIdsReclaimClaim::Available,
						"a probed doc-ID reclaim became unclaimable within one snapshot"
					);
					// Record the marker through `replace_tb`, whose locked
					// read of the exact table-definition key serializes this
					// read-modify-write against every concurrent writer of
					// that key on every backend: a racing catalog refresh
					// from a pre-marker snapshot conflicts instead of
					// clobbering the marker back to false and leaving the
					// numeric chunks resolving through a reclaimable space.
					let mut updated = (*tb).clone();
					updated.graph_doc_ids = true;
					updated.cache_tables_ts = Uuid::now_v7();
					txn.replace_tb(ns_name, db_name, &updated).await?;
				}
			}
		}
		compress
	} else {
		false
	};

	// The doc-IDs each numeric entry encodes, aligned with `entries`. Ids
	// that came through a numeric chunk were carried by the merge; the
	// gaps — plain-sourced entries and captured deltas — are filled here,
	// batched per table, before the repack encodes them.
	let (edge_docs, target_docs) = if numeric {
		resolve_repack_ids(env, txn, ns, db, edge_table, &entries).await?
	} else {
		(Vec::new(), Vec::new())
	};

	// Repack into chunks, greedily filled to the split thresholds. Edge
	// keys are commonly monotonic, so greedy filling leaves earlier
	// chunks dense and growth appends to the right.
	let mut new_chunks: Vec<(RecordIdKey, AdjacencyBlock)> = Vec::new();
	let mut targets: Vec<TableName> = Vec::new();
	let mut plain: Vec<BlockEntry> = Vec::new();
	let mut numeric_entries: Vec<NumericEntry> = Vec::new();
	let mut chunk_start: Option<RecordIdKey> = None;
	let mut bytes = 0usize;
	// The edge id last appended to the open numeric chunk — the previous
	// link of the encoder's per-chunk delta chain, meaningful only while
	// that chunk is non-empty.
	let mut previous_edge: u64 = 0;
	for (index, entry) in entries.into_iter().enumerate() {
		let count = if numeric {
			numeric_entries.len()
		} else {
			plain.len()
		};
		let mut entry_bytes = if numeric {
			// The exact encoded width: the edge id — absolute for a chunk's
			// first entry, a zigzag delta from the previous one otherwise —
			// one dictionary byte, the target id, and the props segment
			// charged as if the block-level props flag were set (its
			// one-byte zero-length slot for an entry with no props), the
			// safe upper bound since the flag depends on the whole chunk.
			let edge_width = if count == 0 {
				uvarint_len(edge_docs[index])
			} else {
				ivarint_len(edge_docs[index].wrapping_sub(previous_edge) as i64)
			};
			edge_width + 1 + uvarint_len(target_docs[index]) + props_width(entry.props.as_deref())
		} else {
			entry.edge_key.len() + entry.target_key.len() + entry.props.as_ref().map_or(0, Vec::len)
		};
		let dict_full = !targets.contains(&entry.target_table) && targets.len() >= u8::MAX as usize;
		if count > 0
			&& (count >= CHUNK_MAX_ENTRIES || bytes + entry_bytes > CHUNK_MAX_BYTES || dict_full)
		{
			new_chunks.push((
				chunk_start.take().expect("a non-empty chunk recorded its start"),
				AdjacencyBlock {
					targets: std::mem::take(&mut targets),
					entries: if numeric {
						BlockEntries::Numeric(std::mem::take(&mut numeric_entries))
					} else {
						BlockEntries::Plain(std::mem::take(&mut plain))
					},
				},
			));
			bytes = 0;
			if numeric {
				// The entry opens the fresh chunk, so its edge id is
				// encoded absolute rather than as a delta.
				entry_bytes = uvarint_len(edge_docs[index])
					+ 1 + uvarint_len(target_docs[index])
					+ props_width(entry.props.as_deref());
			}
		}
		let edge_key: RecordIdKey = storekey::decode_borrow(&entry.edge_key)
			.context("Failed to decode a merged entry's edge key")?;
		if chunk_start.is_none() {
			chunk_start = Some(edge_key.clone());
		}
		let dict_index = match targets.iter().position(|t| *t == entry.target_table) {
			Some(i) => i,
			None => {
				targets.push(entry.target_table.clone());
				targets.len() - 1
			}
		};
		ensure!(dict_index <= u8::MAX as usize, "chunk dictionary overflow despite split");
		if numeric {
			previous_edge = edge_docs[index];
			numeric_entries.push(NumericEntry {
				edge: edge_docs[index],
				target_table: dict_index as u8,
				target: target_docs[index],
				props: entry.props,
			});
		} else {
			plain.push(BlockEntry {
				edge_key: entry.edge_key,
				target_table: dict_index as u8,
				target_key: entry.target_key,
				props: entry.props,
			});
		}
		bytes += entry_bytes;
	}
	if !plain.is_empty() || !numeric_entries.is_empty() {
		new_chunks.push((
			chunk_start.take().expect("a non-empty chunk recorded its start"),
			AdjacencyBlock {
				targets,
				entries: if numeric {
					BlockEntries::Numeric(numeric_entries)
				} else {
					BlockEntries::Plain(plain)
				},
			},
		));
	}

	// Replace the affected chunks: delete the ones whose key is not
	// rewritten, then write the new set. A same-key rewrite is a plain
	// overwrite.
	let new_keys: Vec<Key<'static>> = new_chunks
		.iter()
		.map(|(start, _)| {
			AdjacencyKey {
				ns,
				db,
				tb: Cow::Borrowed(&vertex.table),
				id: Cow::Borrowed(&vertex.key),
				dir,
				edge_table: Cow::Borrowed(edge_table),
				chunk_start: Cow::Borrowed(start),
			}
			.encode_key()
		})
		.collect::<Result<_>>()?;
	for old in old_keys {
		if !new_keys.contains(&old) {
			txn.del(old).await?;
		}
	}
	for ((_, block), key) in new_chunks.iter().zip(&new_keys) {
		txn.set(key.clone(), block.kv_encode_value()?).await?;
	}

	Ok(consumed)
}

/// The bytes an entry's props segment occupies in a numeric chunk whose
/// block-level props flag is set: the length varint plus the body, or the
/// one-byte zero-length slot for an entry without props.
fn props_width(props: Option<&[u8]>) -> usize {
	match props {
		Some(body) => uvarint_len(body.len() as u64) + body.len(),
		None => 1,
	}
}

/// Resolves the doc-IDs a numeric repack encodes: one column of edge ids
/// and one of target ids, each aligned with `entries`.
///
/// Ids carried through the merge are reused verbatim — refolding an
/// already-numeric scope assigns nothing new. The rest resolve through one
/// batched `!di` lookup per table, and only records with no mapping — new
/// to their table's space — take
/// [`TableDocIds::resolve_or_assign`]'s sequence allocation and
/// conditional claim, inside this same transaction.
async fn resolve_repack_ids(
	env: &dyn IndexEnv,
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	edge_table: &TableName,
	entries: &[ResolvedEntry],
) -> Result<(Vec<DocId>, Vec<DocId>)> {
	let mut edge_docs: Vec<Option<DocId>> = entries.iter().map(|e| e.edge_doc).collect();
	let mut target_docs: Vec<Option<DocId>> = entries.iter().map(|e| e.target_doc).collect();
	let edge_missing: Vec<(usize, &[u8])> = entries
		.iter()
		.enumerate()
		.filter(|(_, e)| e.edge_doc.is_none())
		.map(|(slot, e)| (slot, e.edge_key.as_slice()))
		.collect();
	let edge_ids = TableDocIds::new(ns, db, edge_table.clone());
	fill_missing_doc_ids(env, txn, &edge_ids, &edge_missing, &mut edge_docs).await?;
	let mut by_table: BTreeMap<&TableName, Vec<(usize, &[u8])>> = BTreeMap::new();
	for (slot, entry) in entries.iter().enumerate() {
		if entry.target_doc.is_none() {
			by_table
				.entry(&entry.target_table)
				.or_default()
				.push((slot, entry.target_key.as_slice()));
		}
	}
	for (table, missing) in by_table {
		let doc_ids = TableDocIds::new(ns, db, table.clone());
		fill_missing_doc_ids(env, txn, &doc_ids, &missing, &mut target_docs).await?;
	}
	Ok((
		edge_docs.into_iter().map(|id| id.expect("every edge slot was filled")).collect(),
		target_docs.into_iter().map(|id| id.expect("every target slot was filled")).collect(),
	))
}

/// Fills `out`'s named slots with one table's doc-IDs: one batched lookup
/// of the deduplicated record keys — several entries can name the same
/// record — then the assignment path for keys with no mapping yet.
async fn fill_missing_doc_ids(
	env: &dyn IndexEnv,
	txn: &Transaction,
	doc_ids: &TableDocIds,
	missing: &[(usize, &[u8])],
	out: &mut [Option<DocId>],
) -> Result<()> {
	if missing.is_empty() {
		return Ok(());
	}
	let mut unique: BTreeMap<&[u8], Vec<usize>> = BTreeMap::new();
	for &(slot, key) in missing {
		unique.entry(key).or_default().push(slot);
	}
	let keys: Vec<RecordIdKey> = unique
		.keys()
		.map(|bytes| {
			storekey::decode_borrow(bytes).context("Failed to decode a merged entry's record key")
		})
		.collect::<Result<_>>()?;
	let found = doc_ids.get_doc_ids_batch(txn, &keys).await?;
	for ((slots, key), found) in unique.values().zip(&keys).zip(found) {
		let id = match found {
			Some(id) => id,
			None => doc_ids.resolve_or_assign(env, key).await?,
		};
		for &slot in slots {
			out[slot] = Some(id);
		}
	}
	Ok(())
}
