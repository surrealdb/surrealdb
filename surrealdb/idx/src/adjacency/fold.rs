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

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::ops::Bound;

use anyhow::{Context as _, Result, ensure};
use surrealdb_datastore::Transaction;
use surrealdb_datastore::values::graph::{
	ADJACENCY_FLAG_PROPS, AdjacencyBlock, AdjacencyValue, BlockEntry,
};
use surrealdb_kvs::consts::NORMAL_BATCH_SIZE;
use surrealdb_kvs::{Direction, Key};
use uuid::Uuid;

use crate::catalog::providers::TableProvider;
use crate::catalog::{DatabaseId, NamespaceId};
use crate::expr::Dir;
use crate::key::schema::{AdjacencyEdgeTablePrefix, AdjacencyKey, DecodedGraph, GraphDirPrefix};
use crate::key::{KVKey, KVKeyDecode, KVValue};
use crate::val::{RecordId, RecordIdKey, TableName};

/// Split threshold: a rewritten chunk closes once it reaches this many
/// entries.
const CHUNK_MAX_ENTRIES: usize = 512;

/// Split threshold: a rewritten chunk closes once its encoded entries
/// reach this many bytes. Comfortably under every backend's value limits
/// and around one storage page.
const CHUNK_MAX_BYTES: usize = 8 * 1024;

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

/// Folds one (vertex, direction) scope inside the caller's transaction.
///
/// The pass's transaction size is bounded twice over: at most `max_deltas`
/// foldable pointer keys are captured, and at most [`MAX_CHUNKS_PER_PASS`]
/// existing chunks are rewritten per edge table — deltas beyond a pass's
/// chunk coverage stay uncaptured. A scope with more work reports
/// `has_more` and is drained by repeated calls, each in its own
/// transaction. The caller owns commit and the retry on a conflicting
/// concurrent write.
///
/// `ns_name` / `db_name` address the catalog write that sets the vertex
/// table's `graph_folded` marker on the first fold.
#[allow(clippy::too_many_arguments)]
pub async fn fold_scope(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	ns_name: &str,
	db_name: &str,
	vertex: &RecordId,
	dir: Dir,
	max_deltas: usize,
) -> Result<FoldOutcome> {
	fold_scope_bounded(txn, ns, db, ns_name, db_name, vertex, dir, max_deltas, MAX_CHUNKS_PER_PASS)
		.await
}

/// As [`fold_scope`], with the per-edge-table chunk-rewrite bound explicit
/// instead of fixed at [`MAX_CHUNKS_PER_PASS`].
#[allow(clippy::too_many_arguments)]
pub(super) async fn fold_scope_bounded(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	ns_name: &str,
	db_name: &str,
	vertex: &RecordId,
	dir: Dir,
	max_deltas: usize,
	max_chunks: usize,
) -> Result<FoldOutcome> {
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
		let consumed =
			fold_edge_table(txn, ns, db, vertex, dir, edge_table, deltas, max_chunks).await?;
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
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
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
	type MergedEntry = (TableName, Option<Vec<u8>>);
	let mut merged: BTreeMap<MergedIdentity, MergedEntry> = BTreeMap::new();
	let mut old_keys: Vec<Key<'static>> = Vec::new();
	for (key, block) in &affected {
		old_keys.push(Key::from(key.as_slice()).into_static());
		for entry in &block.entries {
			let target_table = block
				.targets
				.get(entry.target_table as usize)
				.expect("block decode validated every dictionary index")
				.clone();
			let target_table_key =
				storekey::encode_vec(&target_table).map_err(anyhow::Error::from_boxed)?;
			merged.insert(
				(entry.edge_key.clone(), target_table_key, entry.target_key.clone()),
				(target_table, entry.props.clone()),
			);
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
			merged.insert(identity, (delta.target.table.clone(), props));
		}
	}

	// Repack into chunks, greedily filled to the split thresholds. Edge
	// keys are commonly monotonic, so greedy filling leaves earlier
	// chunks dense and growth appends to the right.
	let mut new_chunks: Vec<(RecordIdKey, AdjacencyBlock)> = Vec::new();
	let mut block = AdjacencyBlock::default();
	let mut chunk_start: Option<RecordIdKey> = None;
	let mut bytes = 0usize;
	for ((edge_key, _, target_key), (target_table, props)) in merged {
		let entry_bytes = edge_key.len() + target_key.len() + props.as_ref().map_or(0, Vec::len);
		let dict_full =
			!block.targets.contains(&target_table) && block.targets.len() >= u8::MAX as usize;
		if !block.entries.is_empty()
			&& (block.entries.len() >= CHUNK_MAX_ENTRIES
				|| bytes + entry_bytes > CHUNK_MAX_BYTES
				|| dict_full)
		{
			new_chunks.push((
				chunk_start.take().expect("a non-empty chunk recorded its start"),
				std::mem::take(&mut block),
			));
			bytes = 0;
		}
		if chunk_start.is_none() {
			chunk_start = Some(
				storekey::decode_borrow::<RecordIdKey>(&edge_key)
					.context("Failed to decode a merged entry's edge key")?,
			);
		}
		let dict_index = match block.targets.iter().position(|t| *t == target_table) {
			Some(i) => i,
			None => {
				block.targets.push(target_table);
				block.targets.len() - 1
			}
		};
		ensure!(dict_index <= u8::MAX as usize, "chunk dictionary overflow despite split");
		block.entries.push(BlockEntry {
			edge_key,
			target_table: dict_index as u8,
			target_key,
			props,
		});
		bytes += entry_bytes;
	}
	if !block.entries.is_empty() {
		new_chunks.push((chunk_start.take().expect("a non-empty chunk recorded its start"), block));
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
