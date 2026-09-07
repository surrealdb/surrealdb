//! The merged cursor over packed adjacency blocks and per-edge pointer keys.
//!
//! # Merge domain
//!
//! Both sides are compared, filtered and emitted in the **vertex-side
//! pointer-key byte domain**: a delta item's position is its physical key,
//! and a block entry's position is the pointer key it would have had before
//! folding, synthesized from the chunk's scope and the entry's fields. One
//! byte order therefore drives everything — the two-stream merge, the
//! delta-wins tie break, the window filter that a bounded (`->edge:a..b`)
//! or resumed scan imposes, and the emitted key that callers use as a
//! resume position or forward as raw lookup bytes. It is exactly the order
//! a pre-blocks key scan emitted, which is the order every consumer was
//! built against.
//!
//! # Correctness against the fold
//!
//! The fold moves entries from the delta side to the block side in one
//! transaction, so at any read snapshot the union of the two sides is
//! exactly the live set. A tombstone-valued pointer key marks a folded
//! edge as deleted until the next fold consumes both; the merge subtracts
//! it. A live pointer key for an edge that also has a block entry (an edge
//! re-related after folding) wins over the block entry, so the edge is
//! emitted once, with the delta's payload.

use std::borrow::Cow;
use std::collections::VecDeque;
use std::ops::Bound;

use anyhow::{Context as _, Result, bail};
use surrealdb_datastore::Transaction;
use surrealdb_datastore::tx::{MeteredKeysCursor, MeteredValsCursor};
use surrealdb_datastore::values::graph::{
	ADJACENCY_FLAG_PROPS, AdjacencyBlock, AdjacencyValue, BlockEntry,
};
use surrealdb_kvs::Direction;
use surrealdb_kvs::consts::NORMAL_BATCH_SIZE;

use crate::catalog::{DatabaseId, NamespaceId};
use crate::expr::Dir;
use crate::key::schema::{
	AdjacencyDirPrefix, AdjacencyEdgeTablePrefix, AdjacencyIdPrefix, AdjacencyKey, DecodedGraph,
	GraphForeignTablePrefix,
};
use crate::key::{KVKeyDecode, KVValue, RawRange};
use crate::val::{RecordId, RecordIdKey, TableName};

/// One (vertex, direction, edge-table) slice of adjacency to read.
///
/// `delta_range` is the vertex-side pointer/inner key range the scan would
/// have used before blocks existed — foreign-key bounds and any resume
/// position already applied. The block side derives its own range from the
/// other fields and is filtered to `delta_range`'s window, so the two
/// sides always describe the same slice.
pub struct AdjacencyScope<'a> {
	pub ns: NamespaceId,
	pub db: DatabaseId,
	/// The source vertex whose adjacency is being read.
	pub vertex: &'a RecordId,
	/// The direction to read; `None` spans both directions (the
	/// whole-vertex scan the legacy executor uses for `<->`).
	pub dir: Option<Dir>,
	/// Restricts the scan to one edge table; `None` spans all of them.
	/// Must name the same table any foreign-key bounds in `delta_range`
	/// were cut against.
	pub edge_table: Option<&'a TableName>,
	/// The lower foreign-key bound the delta range was cut on, when there
	/// is one. Lets the block side seek its covering chunk instead of
	/// scanning the scope's blocks from the start; purely an optimisation,
	/// since block entries are window-filtered against `delta_range`
	/// either way.
	pub fk_lower: Option<&'a RecordIdKey>,
	/// The vertex-side key range to scan.
	pub delta_range: RawRange,
}

/// One edge produced by the merge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergedAdjacencyEdge {
	/// The edge's position in the pointer-key byte domain: the physical
	/// key bytes for a delta-sourced edge, the synthesized equivalent for
	/// a block-sourced one. Decodes with [`DecodedGraph`]; valid as a
	/// resume position (`RawRange::resume_after`) and as raw lookup bytes.
	pub key: Vec<u8>,
	/// The edge record the adjacency names.
	pub edge: RecordId,
	/// The far vertex, absent only on legacy-format delta keys that never
	/// embedded one.
	pub target: Option<RecordId>,
	/// The edge's inline-property section body, carried verbatim.
	pub props: Option<Vec<u8>>,
}

/// How many edges each side of the merge produced.
#[derive(Debug, Default, Clone, Copy)]
pub struct MergeStats {
	/// Edges emitted from packed block entries.
	pub block_hits: u64,
	/// Edges emitted from per-edge pointer keys.
	pub delta_hits: u64,
}

/// The delta side's cursor: keys-only while the vertex table has never
/// folded — the exact scan and cost the engine always had — and key+value
/// once it has, because tombstones and per-edge sections live in the
/// values from then on.
enum DeltaCursor<'a> {
	Keys(MeteredKeysCursor<'a>),
	Vals(MeteredValsCursor<'a>),
}

struct DeltaItem {
	key: Vec<u8>,
	value: AdjacencyValue,
}

/// What one cursor emits, fixed by its first batch call: full edges
/// ([`MergedAdjacencyCursor::next_batch`]) or raw pointer-key bytes
/// ([`MergedAdjacencyCursor::next_batch_keys`]). The mode decides how much
/// of each item the fills materialize, so it cannot change mid-scan.
#[derive(Clone, Copy, PartialEq, Eq)]
enum EmitMode {
	Edges,
	Keys,
}

/// A block-sourced item buffered for the merge: always its pointer-key
/// bytes, plus the decoded edge fields unless the cursor emits keys only.
struct BlockItem {
	key: Vec<u8>,
	edge: Option<BlockEdge>,
}

/// The decoded fields of a block entry, materialized only in edges mode.
struct BlockEdge {
	edge: RecordId,
	target: RecordId,
	props: Option<Vec<u8>>,
}

/// A chunk mid-materialization: fetched and decoded once, its entries
/// synthesized on demand as the merge needs them, so a bounded read (an
/// existence peek, a small LIMIT) never pays for the whole chunk.
struct OpenChunk {
	block: AdjacencyBlock,
	/// The pointer-key bytes shared by every entry, through the edge table.
	prefix: Vec<u8>,
	/// The storekey-encoded target-table names, positional with
	/// `block.targets`.
	target_tables: Vec<Vec<u8>>,
	/// The chunk's edge table, owned once for every entry's `RecordId`.
	edge_table: TableName,
	/// The next entry index to synthesize.
	next: usize,
}

pub struct MergedAdjacencyCursor<'a> {
	// Delta side.
	delta: DeltaCursor<'a>,
	delta_buf: VecDeque<DeltaItem>,
	delta_done: bool,
	// The delta fill size for the batch call in flight: it starts at the
	// caller's limit and doubles on each re-fill within one call, up to
	// `NORMAL_BATCH_SIZE`.
	delta_fill: u32,
	// Block side; the cursor is `None` while the vertex table has never
	// folded.
	blocks: Option<MeteredValsCursor<'a>>,
	current_chunk: Option<OpenChunk>,
	block_buf: VecDeque<BlockItem>,
	block_done: bool,
	// What the cursor emits; set by the first batch call.
	mode: Option<EmitMode>,
	// The window every block entry must fall into, in the pointer-key
	// byte domain. Delta items are inside it by construction (their
	// cursor scans exactly this range).
	window_start: Vec<u8>,
	window_end: Vec<u8>,
	stats: MergeStats,
	#[cfg(test)]
	delta_fills: u32,
}

/// One item the two-stream merge produced, before mode-specific shaping.
enum MergedItem {
	Delta(DeltaItem),
	Block(BlockItem),
}

impl<'a> MergedAdjacencyCursor<'a> {
	/// Opens the merged cursor over one adjacency scope.
	///
	/// `folded` is the vertex table's `graph_folded` marker (see
	/// [`super::vertex_table_folded`]); passing `false` for a folded table
	/// loses edges, and the caller owns reading the marker from the same
	/// transaction so the two cannot disagree.
	pub async fn open(
		txn: &'a Transaction,
		scope: AdjacencyScope<'_>,
		folded: bool,
		version: Option<u64>,
	) -> Result<Self> {
		let window_start = scope.delta_range.start().to_vec();
		let window_end = scope.delta_range.end().to_vec();
		let (delta, blocks) = if folded {
			let block_range = block_range(txn, &scope, version).await?;
			let delta = txn
				.open_vals_cursor_raw(scope.delta_range, Direction::Forward, 0, version)
				.await
				.context("Failed to open the graph delta cursor")?;
			let blocks = txn
				.open_vals_cursor_raw(block_range, Direction::Forward, 0, version)
				.await
				.context("Failed to open the adjacency block cursor")?;
			(DeltaCursor::Vals(delta), Some(blocks))
		} else {
			let delta = txn
				.open_keys_cursor_raw(scope.delta_range, Direction::Forward, 0, version)
				.await
				.context("Failed to open the graph cursor")?;
			(DeltaCursor::Keys(delta), None)
		};
		Ok(Self {
			delta,
			delta_buf: VecDeque::new(),
			delta_done: false,
			delta_fill: 1,
			blocks,
			current_chunk: None,
			block_buf: VecDeque::new(),
			block_done: false,
			mode: None,
			window_start,
			window_end,
			stats: MergeStats::default(),
			#[cfg(test)]
			delta_fills: 0,
		})
	}

	/// How many edges each side has produced so far.
	pub fn stats(&self) -> MergeStats {
		self.stats
	}

	/// The next run of merged edges, at most `limit` of them. An empty
	/// result means the scope is exhausted. A cursor emits either edges or
	/// keys ([`Self::next_batch_keys`]), never both.
	pub async fn next_batch(&mut self, limit: u32) -> Result<Vec<MergedAdjacencyEdge>> {
		self.set_mode(EmitMode::Edges)?;
		let limit = limit.max(1) as usize;
		self.delta_fill = (limit as u32).min(NORMAL_BATCH_SIZE);
		let mut out = Vec::with_capacity(limit.min(NORMAL_BATCH_SIZE as usize));
		while out.len() < limit {
			let Some(item) = self.next_merged(limit - out.len()).await? else {
				break;
			};
			out.push(match item {
				MergedItem::Delta(item) => {
					let decoded = DecodedGraph::decode(&item.key)?;
					MergedAdjacencyEdge {
						props: item
							.value
							.section(ADJACENCY_FLAG_PROPS.trailing_zeros() as u8)
							.map(<[u8]>::to_vec),
						key: item.key,
						edge: decoded.edge,
						target: decoded.target,
					}
				}
				MergedItem::Block(item) => {
					let edge = item.edge.expect("edges mode materializes full block entries");
					MergedAdjacencyEdge {
						key: item.key,
						edge: edge.edge,
						target: Some(edge.target),
						props: edge.props,
					}
				}
			});
		}
		Ok(out)
	}

	/// As [`Self::next_batch`], emitting only each edge's pointer-key
	/// bytes: the same two-stream merge, tombstone subtraction and order,
	/// with no key decode, `RecordId` construction or props clone per item.
	/// For consumers that inspect emptiness or forward raw key bytes.
	pub async fn next_batch_keys(&mut self, limit: u32) -> Result<Vec<Vec<u8>>> {
		self.set_mode(EmitMode::Keys)?;
		let limit = limit.max(1) as usize;
		self.delta_fill = (limit as u32).min(NORMAL_BATCH_SIZE);
		let mut out = Vec::with_capacity(limit.min(NORMAL_BATCH_SIZE as usize));
		while out.len() < limit {
			let Some(item) = self.next_merged(limit - out.len()).await? else {
				break;
			};
			out.push(match item {
				MergedItem::Delta(item) => item.key,
				MergedItem::Block(item) => item.key,
			});
		}
		Ok(out)
	}

	fn set_mode(&mut self, mode: EmitMode) -> Result<()> {
		match self.mode {
			None => {
				self.mode = Some(mode);
				Ok(())
			}
			Some(current) if current == mode => Ok(()),
			Some(_) => bail!("a merged adjacency cursor emits edges or keys, never both"),
		}
	}

	/// The next merged item, or `None` once the scope is exhausted. `need`
	/// is how many more items the batch call in flight wants; the block
	/// side materializes no further ahead than that.
	async fn next_merged(&mut self, need: usize) -> Result<Option<MergedItem>> {
		loop {
			if self.delta_buf.is_empty() && !self.delta_done {
				// The first fill of a batch call is at the caller's
				// granularity, so a one-key existence peek costs one key —
				// as the raw scan it replaced did. Each re-fill within the
				// same call doubles the size up to `NORMAL_BATCH_SIZE`: a
				// long run of tombstones then costs a logarithmic number of
				// escalating fills instead of one storage round trip per
				// skipped key.
				let fill = self.delta_fill;
				self.delta_fill = self.delta_fill.saturating_mul(2).min(NORMAL_BATCH_SIZE);
				self.fill_delta(fill).await?;
			}
			if self.block_buf.is_empty() && !self.block_done && self.blocks.is_some() {
				self.fill_blocks(need).await?;
			}
			// Which side owns the next position. Equal keys cannot happen
			// across the sides (a delta key and a synthesized block key for
			// the same edge are byte-identical only when the edge exists on
			// both sides, which the dedupe below resolves before comparing
			// again), so a strict comparison decides.
			let take_delta = match (self.delta_buf.front(), self.block_buf.front()) {
				(None, None) => return Ok(None),
				(Some(_), None) => true,
				(None, Some(_)) => false,
				(Some(d), Some(b)) => d.key <= b.key,
			};
			if take_delta {
				let item =
					self.delta_buf.pop_front().expect("take_delta implies a buffered delta item");
				// Delta wins over a block entry for the same edge: drop the
				// block copy whether the delta is live or a tombstone.
				if self.block_buf.front().is_some_and(|b| b.key == item.key) {
					self.block_buf.pop_front();
				}
				if item.value.is_tombstone() {
					continue;
				}
				self.stats.delta_hits += 1;
				return Ok(Some(MergedItem::Delta(item)));
			}
			let entry =
				self.block_buf.pop_front().expect("!take_delta implies a buffered block entry");
			self.stats.block_hits += 1;
			return Ok(Some(MergedItem::Block(entry)));
		}
	}

	async fn fill_delta(&mut self, fill: u32) -> Result<()> {
		#[cfg(test)]
		{
			self.delta_fills += 1;
		}
		match &mut self.delta {
			DeltaCursor::Keys(cursor) => {
				let batch = cursor.next_batch(fill).await.context("Failed to scan graph edges")?;
				if batch.is_empty() {
					self.delta_done = true;
					return Ok(());
				}
				for key in &batch {
					self.delta_buf.push_back(DeltaItem {
						key: key.to_vec(),
						value: AdjacencyValue::live(),
					});
				}
			}
			DeltaCursor::Vals(cursor) => {
				let batch = cursor.next_batch(fill).await.context("Failed to scan graph edges")?;
				if batch.is_empty() {
					self.delta_done = true;
					return Ok(());
				}
				for (key, val) in &batch {
					self.delta_buf.push_back(DeltaItem {
						key: key.to_vec(),
						value: AdjacencyValue::kv_decode_value(val, ())?,
					});
				}
			}
		}
		Ok(())
	}

	/// Fills `block_buf` until it covers `need`, the window ends, or the
	/// block side is exhausted. Chunks are fetched and decoded one at a
	/// time and stay open across calls: entries are synthesized only as
	/// needed, so a bounded read pays one chunk-value decode plus `need`
	/// entry materializations, not the whole chunk.
	async fn fill_blocks(&mut self, need: usize) -> Result<()> {
		let need = need.max(1);
		let keys_only = self.mode == Some(EmitMode::Keys);
		while self.block_buf.len() < need && !self.block_done {
			if self.current_chunk.is_some() {
				self.materialize_open_chunk(need, keys_only)?;
				continue;
			}
			let Some(cursor) = self.blocks.as_mut() else {
				self.block_done = true;
				return Ok(());
			};
			let batch = cursor.next_batch(1).await.context("Failed to scan adjacency blocks")?;
			let Some((key, val)) = batch.into_iter().next() else {
				self.block_done = true;
				return Ok(());
			};
			let chunk_key =
				AdjacencyKey::decode_key(key).context("Failed to decode an adjacency block key")?;
			let block = AdjacencyBlock::kv_decode_value(val, ())?;
			// The pointer-key bytes shared by every entry in this chunk:
			// everything up to and including the edge table. Entries append
			// their own edge key, target table and target key.
			let prefix = pointer_prefix(&chunk_key)?;
			let target_tables: Vec<Vec<u8>> = block
				.targets
				.iter()
				.map(|t| storekey::encode_vec(t).map_err(anyhow::Error::from_boxed))
				.collect::<Result<_>>()?;
			self.current_chunk = Some(OpenChunk {
				block,
				prefix,
				target_tables,
				edge_table: chunk_key.edge_table.into_owned(),
				next: 0,
			});
		}
		Ok(())
	}

	/// Synthesizes entries from the open chunk into `block_buf` until the
	/// buffer covers `need`, the window ends (which finishes the block
	/// side), or the chunk is exhausted (which closes it, so the next fill
	/// fetches the following chunk). Window-skipped entries do not count
	/// toward `need`.
	fn materialize_open_chunk(&mut self, need: usize, keys_only: bool) -> Result<()> {
		let Some(chunk) = self.current_chunk.as_mut() else {
			return Ok(());
		};
		let mut exhausted = true;
		let mut window_ended = false;
		while let Some(entry) = chunk.block.entries.get(chunk.next) {
			if self.block_buf.len() >= need {
				// The chunk stays open; the next fill resumes at `next`.
				exhausted = false;
				break;
			}
			chunk.next += 1;
			let key = synthesize_pointer_key(&chunk.prefix, &chunk.target_tables, entry)?;
			// A bounded or resumed scan's window, in the same domain.
			if key.as_slice() < self.window_start.as_slice() {
				continue;
			}
			if key.as_slice() >= self.window_end.as_slice() {
				window_ended = true;
				break;
			}
			let edge = if keys_only {
				None
			} else {
				let edge_key: RecordIdKey = storekey::decode_borrow(&entry.edge_key)
					.context("Failed to decode a block entry's edge key")?;
				let target_key: RecordIdKey = storekey::decode_borrow(&entry.target_key)
					.context("Failed to decode a block entry's target key")?;
				let target_table = chunk
					.block
					.targets
					.get(entry.target_table as usize)
					.expect("block decode validated every dictionary index")
					.clone();
				Some(BlockEdge {
					edge: RecordId::new(chunk.edge_table.clone(), edge_key),
					target: RecordId::new(target_table, target_key),
					props: entry.props.clone(),
				})
			};
			self.block_buf.push_back(BlockItem {
				key,
				edge,
			});
		}
		if window_ended {
			self.block_done = true;
		}
		if exhausted || window_ended {
			self.current_chunk = None;
		}
		Ok(())
	}

	/// How many delta fills the cursor has issued.
	#[cfg(test)]
	pub(super) fn delta_fill_count(&self) -> u32 {
		self.delta_fills
	}

	/// How many block items are buffered ahead of the merge.
	#[cfg(test)]
	pub(super) fn buffered_block_entries(&self) -> usize {
		self.block_buf.len()
	}

	/// How many entries of the open chunk are not yet materialized.
	#[cfg(test)]
	pub(super) fn open_chunk_remaining(&self) -> usize {
		self.current_chunk.as_ref().map_or(0, |chunk| chunk.block.entries.len() - chunk.next)
	}
}

/// The block-side range for a scope, seeking the covering chunk when the
/// scope carries a foreign-key lower bound.
async fn block_range(
	txn: &Transaction,
	scope: &AdjacencyScope<'_>,
	version: Option<u64>,
) -> Result<RawRange> {
	let ns = scope.ns;
	let db = scope.db;
	let tb = Cow::Borrowed(&scope.vertex.table);
	let id = Cow::Borrowed(&scope.vertex.key);
	let range = match (scope.dir, scope.edge_table) {
		(None, _) => AdjacencyIdPrefix {
			ns,
			db,
			tb,
			id,
		}
		.range()?
		.into(),
		(Some(dir), None) => AdjacencyDirPrefix {
			ns,
			db,
			tb,
			id,
			dir,
		}
		.range()?
		.into(),
		(Some(dir), Some(edge_table)) => {
			let prefix = || AdjacencyEdgeTablePrefix {
				ns,
				db,
				tb: Cow::Borrowed(&scope.vertex.table),
				id: Cow::Borrowed(&scope.vertex.key),
				dir,
				edge_table: Cow::Borrowed(edge_table),
			};
			match scope.fk_lower {
				None => prefix().range()?.into(),
				Some(fk_lower) => {
					// A chunk whose `chunk_start` precedes the bound can
					// still hold entries at or after it, so the scan starts
					// at the last chunk at or before the bound — or at the
					// bound itself when no such chunk exists.
					let up_to_bound = prefix().range_where((
						Bound::Unbounded,
						Bound::Included(Cow::Borrowed(fk_lower)),
					))?;
					let covering = txn.keysr_raw(up_to_bound, 1, 0, version).await?;
					match covering.into_iter().next() {
						Some(key) => {
							let covering = AdjacencyKey::decode_key(&key)
								.context("Failed to decode an adjacency block key")?;
							let chunk_start = covering.chunk_start.into_owned();
							prefix()
								.range_where((
									Bound::Included(Cow::Owned(chunk_start)),
									Bound::Unbounded,
								))?
								.into()
						}
						None => prefix()
							.range_where((
								Bound::Included(Cow::Borrowed(fk_lower)),
								Bound::Unbounded,
							))?
							.into(),
					}
				}
			}
		}
	};
	Ok(range)
}

/// The pointer-key bytes shared by every entry of one chunk: the vertex's
/// graph subspace, the direction, and the edge table.
fn pointer_prefix(chunk_key: &AdjacencyKey) -> Result<Vec<u8>> {
	let prefix = GraphForeignTablePrefix {
		ns: chunk_key.ns,
		db: chunk_key.db,
		tb: chunk_key.tb.clone(),
		id: chunk_key.id.clone(),
		dir: chunk_key.dir,
		foreign_table: chunk_key.edge_table.clone(),
	}
	.range_subtree()?;
	Ok(prefix.start().to_vec())
}

/// A block entry's pointer-key bytes: the chunk prefix, the edge key, the
/// dictionary-encoded target table, and the target key.
fn synthesize_pointer_key(
	prefix: &[u8],
	target_tables: &[Vec<u8>],
	entry: &BlockEntry,
) -> Result<Vec<u8>> {
	let target_table = target_tables
		.get(entry.target_table as usize)
		.expect("block decode validated every dictionary index");
	let mut key = Vec::with_capacity(
		prefix.len() + entry.edge_key.len() + target_table.len() + entry.target_key.len(),
	);
	key.extend_from_slice(prefix);
	key.extend_from_slice(&entry.edge_key);
	key.extend_from_slice(target_table);
	key.extend_from_slice(&entry.target_key);
	Ok(key)
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::key::KVKey;
	use crate::key::schema::GraphPointerKey;

	/// The synthesized pointer key for a block entry must be byte-identical
	/// to the key the same edge had before folding: the merge, the window
	/// filter, the resume contract and the raw-bytes consumers all assume
	/// the two domains are one.
	#[test]
	fn synthesized_keys_match_real_pointer_keys() {
		let ns = NamespaceId(7);
		let db = DatabaseId(9);
		let vertex = RecordId::new("person".into(), "alice".to_owned());
		let edge = RecordId::new("likes".into(), "e1".to_owned());
		let target = RecordId::new("person".into(), "bob".to_owned());

		let real = GraphPointerKey {
			ns,
			db,
			tb: Cow::Borrowed(&vertex.table),
			id: Cow::Borrowed(&vertex.key),
			dir: Dir::Out,
			foreign_table: Cow::Borrowed(&edge.table),
			foreign_key: Cow::Borrowed(&edge.key),
			target_table: Cow::Borrowed(&target.table),
			target_key: Cow::Borrowed(&target.key),
		}
		.encode_key()
		.unwrap();

		let chunk_key = AdjacencyKey {
			ns,
			db,
			tb: Cow::Borrowed(&vertex.table),
			id: Cow::Borrowed(&vertex.key),
			dir: Dir::Out,
			edge_table: Cow::Borrowed(&edge.table),
			chunk_start: Cow::Borrowed(&edge.key),
		};
		let prefix = pointer_prefix(&chunk_key).unwrap();
		let entry = BlockEntry {
			edge_key: storekey::encode_vec(&edge.key).unwrap(),
			target_table: 0,
			target_key: storekey::encode_vec(&target.key).unwrap(),
			props: None,
		};
		let target_tables = vec![storekey::encode_vec(&target.table).unwrap()];
		let synthesized = synthesize_pointer_key(&prefix, &target_tables, &entry).unwrap();

		assert_eq!(synthesized.as_slice(), real.as_ref());
	}
}
