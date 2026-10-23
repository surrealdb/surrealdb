//! Ingested objects: a large commit's write set in R2, read in place rather than applied into
//! every node's memory.
//!
//! The object is the packed write set the commit made (`n:u32`, then each write as
//! `klen:u32 key vlen:u32 value`, `vlen = u32::MAX` for a delete), sorted by key with one write
//! per key, so every reader that only knows the packed form still reads it whole. After it come
//! the object's metadata: a sparse index (every [`BLOCK_ENTRIES`]th key and its offset), a Bloom
//! filter over its keys, and a fixed footer. A pointer commit names the object and where the
//! metadata sits, so a node applying the commit reads only the metadata (one ranged read) and
//! registers the object as a layer at the commit's position. A read checks the filter, then
//! fetches the one block that can hold the key; a scan reads the blocks in order. The layer is
//! dropped once the tier's sealed position covers it (the seal has the records then).
//!
//! The commits of a rolled run are read the same way, one [`RunDir`] layer per run: a directory
//! of the run's sections (each one's key range, Bloom filter and where it sits), so a read checks
//! the directory and fetches only the section that can hold the key.

use std::sync::Arc;

use anyhow::{Result, bail};

/// Writes per block: one block is one ranged read.
pub const BLOCK_ENTRIES: usize = 64;
/// Bloom filter bits per key, and probes per key (about 1% false positives).
const BITS_PER_KEY: usize = 10;
const PROBES: u64 = 7;
/// The footer: index offset and length, filter offset and length, writes, end of the writes,
/// then the magic.
const FOOTER: usize = 4 * 6 + 8;
const MAGIC: &[u8; 8] = b"SURING01";
const DELETE: u32 = u32::MAX;

/// A key's two independent hashes for the filter's double hashing.
fn hashes(key: &[u8]) -> (u64, u64) {
	let mut h1: u64 = 0xcbf2_9ce4_8422_2325;
	let mut h2: u64 = 0x9e37_79b9_7f4a_7c15;
	for &b in key {
		h1 = (h1 ^ u64::from(b)).wrapping_mul(0x0000_0100_0000_01b3);
		h2 = (h2.rotate_left(5) ^ u64::from(b)).wrapping_mul(0x51_7cc1_b727_220a_95);
	}
	h1 ^= h1 >> 33;
	h1 = h1.wrapping_mul(0xff51_afd7_ed55_8ccd);
	h1 ^= h1 >> 33;
	(h1, h2 | 1)
}

/// The object for a packed write set: the write set, then its index, filter and footer, and the
/// offset and length of that metadata (what a reader fetches).
pub fn build(packed: &[u8]) -> Result<(Vec<u8>, u32, u32)> {
	let writes = crate::log::unpack(packed)?;
	let n = writes.len();
	// Only the write set: a section (what a commit now hands over) has its own meta after the
	// entries, which this object replaces with the format below.
	let entries_len = 4 + writes.iter().map(|(k, v)| 8 + k.len() + v.map_or(0, <[u8]>::len)).sum::<usize>();
	let packed = &packed[..entries_len];
	let mut out = Vec::with_capacity(packed.len() + n / BLOCK_ENTRIES * 64 + n * BITS_PER_KEY / 8 + 64);
	out.extend_from_slice(packed);
	// The packed write set's offsets, walked again: each write's start.
	let mut offsets = Vec::with_capacity(n.div_ceil(BLOCK_ENTRIES));
	let mut off = 4usize;
	let mut prev: Option<&[u8]> = None;
	for (i, (k, v)) in writes.iter().enumerate() {
		if prev.is_some_and(|p| p >= *k) {
			bail!("an ingested write set must be sorted with one write per key");
		}
		prev = Some(k);
		if i % BLOCK_ENTRIES == 0 {
			offsets.push((*k, off as u32));
		}
		off += 4 + k.len() + 4 + v.map_or(0, <[u8]>::len);
	}
	let entries_end = off as u32;
	let meta_start = out.len() as u32;
	// Index: count, then (klen:u32 key off:u32)*.
	let index_off = out.len() as u32;
	out.extend_from_slice(&(offsets.len() as u32).to_le_bytes());
	for (k, o) in &offsets {
		out.extend_from_slice(&(k.len() as u32).to_le_bytes());
		out.extend_from_slice(k);
		out.extend_from_slice(&o.to_le_bytes());
	}
	let index_len = out.len() as u32 - index_off;
	// Filter: u64 words.
	let words = (n * BITS_PER_KEY).div_ceil(64).max(1);
	let mut bits = vec![0u64; words];
	let m = (words * 64) as u64;
	for (k, _) in &writes {
		let (a, b) = hashes(k);
		for p in 0..PROBES {
			let bit = a.wrapping_add(p.wrapping_mul(b)) % m;
			bits[(bit / 64) as usize] |= 1 << (bit % 64);
		}
	}
	let bloom_off = out.len() as u32;
	for w in &bits {
		out.extend_from_slice(&w.to_le_bytes());
	}
	let bloom_len = out.len() as u32 - bloom_off;
	for x in [index_off, index_len, bloom_off, bloom_len, n as u32, entries_end] {
		out.extend_from_slice(&x.to_le_bytes());
	}
	out.extend_from_slice(MAGIC);
	let meta_len = out.len() as u32 - meta_start;
	Ok((out, meta_start, meta_len))
}

/// The `len` bytes of `meta` that the object's offset `off` names, `meta` starting at `meta_start`.
fn region_of(meta: &[u8], meta_start: u32, off: u32, len: u32) -> Option<&[u8]> {
	let start = off.checked_sub(meta_start)? as usize;
	meta.get(start..start.checked_add(len as usize)?)
}

/// The little-endian word of `bytes` at `at`.
fn word_at(bytes: &[u8], at: usize) -> Option<u32> {
	Some(u32::from_le_bytes(bytes.get(at..at.checked_add(4)?)?.try_into().ok()?))
}

/// An ingested object as a reader holds it: where it is and what it covers, its index and its
/// filter. The writes stay in R2.
#[derive(Debug)]
pub struct Ingested {
	/// The log position its writes take.
	pub pos: u64,
	/// Its key in the object store.
	pub key: Arc<str>,
	/// Each block's first key and offset, in key order.
	index: Vec<(Vec<u8>, u32)>,
	/// Where the writes end (the last block's end).
	entries_end: u32,
	bloom: Vec<u64>,
}

impl Ingested {
	/// The object from its metadata: the bytes `build` placed at `meta_start`.
	pub fn parse(pos: u64, key: String, meta_start: u32, meta: &[u8]) -> Result<Self> {
		if meta.len() < FOOTER || &meta[meta.len() - 8..] != MAGIC {
			bail!("ingested object {key}: no footer");
		}
		let f = &meta[meta.len() - FOOTER..];
		let u = |i: usize| u32::from_le_bytes(f[i * 4..i * 4 + 4].try_into().expect("4 bytes"));
		let (index_off, index_len, bloom_off, bloom_len, n, entries_end) = (u(0), u(1), u(2), u(3), u(4), u(5));
		let corrupt = || anyhow::anyhow!("ingested object {key}: corrupt metadata");
		let ix = region_of(meta, meta_start, index_off, index_len).ok_or_else(corrupt)?;
		let count = word_at(ix, 0).ok_or_else(corrupt)? as usize;
		// Every entry holds at least its two length words, so a count the region cannot hold is corrupt.
		if count > ix.len() / 8 {
			return Err(corrupt());
		}
		// One index entry per block: an index that disagrees with the write count would make
		// `may_hold` skip writes the object holds.
		if count != (n as usize).div_ceil(BLOCK_ENTRIES) {
			return Err(corrupt());
		}
		let mut index: Vec<(Vec<u8>, u32)> = Vec::with_capacity(count);
		let mut o = 4usize;
		for _ in 0..count {
			let kl = word_at(ix, o).ok_or_else(corrupt)? as usize;
			let key_start = o.checked_add(4).ok_or_else(corrupt)?;
			let key_end = key_start.checked_add(kl).ok_or_else(corrupt)?;
			let k = ix.get(key_start..key_end).ok_or_else(corrupt)?.to_vec();
			let off = word_at(ix, key_end).ok_or_else(corrupt)?;
			// Keys and offsets strictly rise and stay inside the writes, so `block_of` partitions a
			// sorted index and `block_range` never underflows.
			if off > entries_end
				|| index.last().is_some_and(|(prev_key, prev_off)| off <= *prev_off || k <= *prev_key)
			{
				return Err(corrupt());
			}
			index.push((k, off));
			o = key_end.checked_add(4).ok_or_else(corrupt)?;
		}
		let b = region_of(meta, meta_start, bloom_off, bloom_len).ok_or_else(corrupt)?;
		let bloom: Vec<u64> =
			b.chunks_exact(8).map(|w| u64::from_le_bytes(w.try_into().expect("8 bytes"))).collect();
		// The filter's size follows from the write count: a truncated or oversized one would
		// make `may_hold` answer "absent" for a key the object holds.
		if b.len() % 8 != 0 || bloom.len() != (n as usize * BITS_PER_KEY).div_ceil(64).max(1) {
			return Err(corrupt());
		}
		Ok(Self { pos, key: Arc::from(key), index, entries_end, bloom })
	}

	/// Whether the object may hold `key` (no false negatives).
	pub fn may_hold(&self, key: &[u8]) -> bool {
		let Some((first, _)) = self.index.first() else {
			return false;
		};
		if key < first.as_slice() {
			return false;
		}
		let m = (self.bloom.len() * 64) as u64;
		let (a, b) = hashes(key);
		(0..PROBES).all(|p| {
			let bit = a.wrapping_add(p.wrapping_mul(b)) % m;
			self.bloom[(bit / 64) as usize] & (1 << (bit % 64)) != 0
		})
	}

	/// The number of blocks.
	pub fn blocks(&self) -> usize {
		self.index.len()
	}

	/// Block `i`'s byte range in the object.
	pub fn block_range(&self, i: usize) -> (u64, u64) {
		let start = self.index[i].1;
		let end = self.index.get(i + 1).map_or(self.entries_end, |(_, o)| *o);
		(u64::from(start), u64::from(end - start))
	}

	/// The block that holds `key` if any does: the last whose first key is at or before it.
	pub fn block_of(&self, key: &[u8]) -> Option<usize> {
		self.index.partition_point(|(k, _)| k.as_slice() <= key).checked_sub(1)
	}

	/// The blocks that can hold keys in `[lo, hi)`, in order.
	pub fn blocks_in(&self, lo: &[u8], hi: &[u8]) -> std::ops::Range<usize> {
		let first = self.block_of(lo).unwrap_or(0);
		let end = self.index.partition_point(|(k, _)| k.as_slice() < hi);
		first..end.max(first)
	}

	/// Bytes a reader that keeps this in memory spends on it (the index and the filter).
	pub fn footprint(&self) -> usize {
		64 + self.bloom.len() * 8 + self.index.iter().map(|(k, _)| k.len() + 32).sum::<usize>()
	}
}

/// A block to read: where it is, and the position of the commit whose writes it holds.
#[derive(Debug, Clone)]
pub struct BlockAt {
	pub pos: u64,
	/// The object (an ingested object, or a run) in the object store.
	pub key: Arc<str>,
	pub start: u64,
	pub len: u64,
}

/// What a scan reads of one commit: the blocks that can hold keys in its range, in key order.
#[derive(Debug)]
pub struct ScanSrc {
	pub pos: u64,
	pub key: Arc<str>,
	pub blocks: Vec<(u64, u64)>,
}

/// One layer a read passes through above the sealed tier: an ingested object (one commit), or a
/// rolled run (many). Layers cover disjoint, ascending ranges of positions.
#[derive(Debug)]
pub enum Layer {
	Object(Ingested),
	Run(RunDir),
}

impl Layer {
	/// The lowest position it holds.
	pub fn first(&self) -> u64 {
		match self {
			Layer::Object(o) => o.pos,
			Layer::Run(r) => r.first(),
		}
	}

	/// The highest position it holds.
	pub fn last(&self) -> u64 {
		match self {
			Layer::Object(o) => o.pos,
			Layer::Run(r) => r.last(),
		}
	}

	/// Commits it holds.
	pub fn sections(&self) -> usize {
		match self {
			Layer::Object(_) => 1,
			Layer::Run(r) => r.len(),
		}
	}

	/// Bytes a reader that keeps this in memory spends on it.
	pub fn footprint(&self) -> usize {
		match self {
			Layer::Object(o) => o.footprint(),
			Layer::Run(r) => r.footprint(),
		}
	}

	/// The blocks that may hold `key` (whose [`surrealds_section::hash`] is `h`) in commits above
	/// position `after` and at or below `at`, newest first, added to `out`.
	pub fn probe(&self, key: &[u8], h: u64, after: u64, at: u64, out: &mut Vec<BlockAt>) {
		match self {
			Layer::Object(o) => {
				if o.pos > after && o.pos <= at && o.may_hold(key) {
					if let Some(i) = o.block_of(key) {
						let (start, len) = o.block_range(i);
						out.push(BlockAt { pos: o.pos, key: Arc::clone(&o.key), start, len });
					}
				}
			}
			Layer::Run(r) => r.probe(key, h, after, at, out),
		}
	}

	/// What a scan of `[lo, hi)` reads of each commit at or below `at` that can hold keys in it,
	/// added to `out`, oldest first.
	pub fn scan_srcs(&self, lo: &[u8], hi: &[u8], at: u64, out: &mut Vec<ScanSrc>) {
		match self {
			Layer::Object(o) => {
				if o.pos > at {
					return;
				}
				let r = o.blocks_in(lo, hi);
				if !r.is_empty() {
					out.push(ScanSrc { pos: o.pos, key: Arc::clone(&o.key), blocks: r.map(|i| o.block_range(i)).collect() });
				}
			}
			Layer::Run(r) => r.scan_srcs(lo, hi, at, out),
		}
	}
}

/// The commits of one rolled run, as a reader holds them: for each section, its position, where
/// it sits in the run, its key range and its Bloom filter, kept together (the ranges and filters
/// in two arenas) so that a read checks a run's directory in one pass and fetches only the section
/// that can hold the key. Fences (the section's blocks) are kept only for the sections large
/// enough to have them. The sections' writes stay in R2.
#[derive(Debug, Default)]
pub struct RunDir {
	key: Arc<str>,
	secs: Vec<Sec>,
	/// Every section's smallest key, then its largest unless the two are the same.
	keys: Vec<u8>,
	/// Every section's filter, back to back.
	bloom: Vec<u64>,
	/// Fences by section index, for the sections that have them.
	fences: Vec<(u32, Vec<(Vec<u8>, u32)>)>,
	/// The union of the sections' key ranges.
	lo: Vec<u8>,
	hi: Vec<u8>,
}

#[derive(Debug, Clone, Copy)]
struct Sec {
	pos: u64,
	/// Where the section starts in the run.
	base: u32,
	entries_end: u32,
	min_at: u32,
	min_len: u32,
	/// The largest key's length, `u32::MAX` when it is the smallest too.
	max_len: u32,
	bloom_at: u32,
	log2: u8,
}

impl RunDir {
	/// The directory of run `key`, with room for `sections` sections, to [`Self::push`] to.
	pub fn new(key: Arc<str>, sections: usize) -> Self {
		RunDir { key, secs: Vec::with_capacity(sections), ..RunDir::default() }
	}

	/// Add a section: its position must be above the last one added. One with no write is left out.
	pub fn push(&mut self, pos: u64, base: u32, m: &surrealds_section::Meta) {
		debug_assert!(self.secs.last().is_none_or(|s| s.pos < pos), "sections ascend by position");
		if m.n == 0 {
			return;
		}
		let (min, max) = m.range();
		let (words, log2) = m.bloom();
		let idx = self.secs.len() as u32;
		self.secs.push(Sec {
			pos,
			base,
			entries_end: m.entries_end(),
			min_at: self.keys.len() as u32,
			min_len: min.len() as u32,
			max_len: if min == max { u32::MAX } else { max.len() as u32 },
			bloom_at: self.bloom.len() as u32,
			log2,
		});
		self.keys.extend_from_slice(min);
		if min != max {
			self.keys.extend_from_slice(max);
		}
		self.bloom.extend_from_slice(words);
		if !m.fences().is_empty() {
			self.fences.push((idx, m.fences().to_vec()));
		}
		if self.secs.len() == 1 || min < self.lo.as_slice() {
			self.lo = min.to_vec();
		}
		if self.secs.len() == 1 || max > self.hi.as_slice() {
			self.hi = max.to_vec();
		}
	}

	/// The lowest position of a section it holds.
	pub fn first(&self) -> u64 {
		self.secs.first().map_or(0, |s| s.pos)
	}

	/// The highest position of a section it holds.
	pub fn last(&self) -> u64 {
		self.secs.last().map_or(0, |s| s.pos)
	}

	/// Sections held.
	pub fn len(&self) -> usize {
		self.secs.len()
	}

	pub fn is_empty(&self) -> bool {
		self.secs.is_empty()
	}

	/// Bytes the directory takes.
	pub fn footprint(&self) -> usize {
		96 + self.secs.len() * std::mem::size_of::<Sec>()
			+ self.keys.len()
			+ self.bloom.len() * 8
			+ self.lo.len()
			+ self.hi.len()
			+ self.fences.iter().map(|(_, f)| 24 + f.iter().map(|(k, _)| k.len() + 32).sum::<usize>()).sum::<usize>()
	}

	/// The directory without the sections at or below `through` (the tier holds them now), copied:
	/// `None` when none are left.
	pub fn after(&self, through: u64) -> Option<RunDir> {
		let from = self.secs.partition_point(|s| s.pos <= through);
		if from == self.secs.len() {
			return None;
		}
		let mut dir = RunDir { key: Arc::clone(&self.key), secs: Vec::with_capacity(self.secs.len() - from), ..RunDir::default() };
		for (idx, s) in self.secs.iter().enumerate().skip(from) {
			let at = dir.secs.len() as u32;
			let (min, max) = (self.min(s), self.max(s));
			dir.secs.push(Sec { min_at: dir.keys.len() as u32, bloom_at: dir.bloom.len() as u32, ..*s });
			dir.keys.extend_from_slice(min);
			if s.max_len != u32::MAX {
				dir.keys.extend_from_slice(max);
			}
			dir.bloom.extend_from_slice(self.words(s));
			if let Some(f) = self.fences_of(idx as u32) {
				dir.fences.push((at, f.to_vec()));
			}
			if at == 0 || min < dir.lo.as_slice() {
				dir.lo = min.to_vec();
			}
			if at == 0 || max > dir.hi.as_slice() {
				dir.hi = max.to_vec();
			}
		}
		Some(dir)
	}

	fn min(&self, s: &Sec) -> &[u8] {
		&self.keys[s.min_at as usize..(s.min_at + s.min_len) as usize]
	}

	fn max(&self, s: &Sec) -> &[u8] {
		if s.max_len == u32::MAX {
			self.min(s)
		} else {
			let at = (s.min_at + s.min_len) as usize;
			&self.keys[at..at + s.max_len as usize]
		}
	}

	fn words(&self, s: &Sec) -> &[u64] {
		let at = s.bloom_at as usize;
		&self.bloom[at..at + (1usize << s.log2) / 64]
	}

	fn fences_of(&self, idx: u32) -> Option<&[(Vec<u8>, u32)]> {
		self.fences.binary_search_by_key(&idx, |(i, _)| *i).ok().map(|i| self.fences[i].1.as_slice())
	}

	/// The sections that may hold `key`, newest first, among those in `(after, at]`: the first
	/// check is the run's whole key range, and each section's is its range and then its filter.
	pub fn probe(&self, key: &[u8], h: u64, after: u64, at: u64, out: &mut Vec<BlockAt>) {
		if self.secs.is_empty() || key < self.lo.as_slice() || key > self.hi.as_slice() {
			return;
		}
		let end = self.secs.partition_point(|s| s.pos <= at);
		for idx in (0..end).rev() {
			let s = &self.secs[idx];
			if s.pos <= after {
				break;
			}
			if key < self.min(s) || key > self.max(s) || !surrealds_section::bloom_has(self.words(s), s.log2, h) {
				continue;
			}
			let (start, len) = match self.fences_of(idx as u32) {
				None => (4, s.entries_end - 4),
				Some(f) => {
					let i = f.partition_point(|(k, _)| k.as_slice() <= key).saturating_sub(1);
					let end = f.get(i + 1).map_or(s.entries_end, |(_, o)| *o);
					(f[i].1, end - f[i].1)
				}
			};
			out.push(BlockAt { pos: s.pos, key: Arc::clone(&self.key), start: u64::from(s.base) + u64::from(start), len: u64::from(len) });
		}
	}

	/// What a scan of `[lo, hi)` reads of each section at or below `at` whose range overlaps it.
	pub fn scan_srcs(&self, lo: &[u8], hi: &[u8], at: u64, out: &mut Vec<ScanSrc>) {
		if self.secs.is_empty() || lo > self.hi.as_slice() || hi <= self.lo.as_slice() {
			return;
		}
		let end = self.secs.partition_point(|s| s.pos <= at);
		for (idx, s) in self.secs[..end].iter().enumerate() {
			if lo > self.max(s) || hi <= self.min(s) {
				continue;
			}
			let base = u64::from(s.base);
			let blocks = match self.fences_of(idx as u32) {
				None => vec![(base + 4, u64::from(s.entries_end - 4))],
				Some(f) => {
					let first = f.partition_point(|(k, _)| k.as_slice() <= lo).saturating_sub(1);
					let last = f.partition_point(|(k, _)| k.as_slice() < hi).max(first);
					(first..last)
						.map(|i| {
							let end = f.get(i + 1).map_or(s.entries_end, |(_, o)| *o);
							(base + u64::from(f[i].1), u64::from(end - f[i].1))
						})
						.collect()
				}
			};
			out.push(ScanSrc { pos: s.pos, key: Arc::clone(&self.key), blocks });
		}
	}
}

/// A block's writes: (key, value), `None` for a delete.
pub fn block_writes(block: &[u8]) -> Result<Vec<(&[u8], Option<&[u8]>)>> {
	let corrupt = || anyhow::anyhow!("ingested block: corrupt entry");
	let mut out = Vec::with_capacity(BLOCK_ENTRIES);
	let mut o = 0usize;
	while o < block.len() {
		let kl = word_at(block, o).ok_or_else(corrupt)? as usize;
		let key_start = o.checked_add(4).ok_or_else(corrupt)?;
		let key_end = key_start.checked_add(kl).ok_or_else(corrupt)?;
		let k = block.get(key_start..key_end).ok_or_else(corrupt)?;
		let vl = word_at(block, key_end).ok_or_else(corrupt)?;
		o = key_end.checked_add(4).ok_or_else(corrupt)?;
		if vl == DELETE {
			out.push((k, None));
		} else {
			let end = o.checked_add(vl as usize).ok_or_else(corrupt)?;
			out.push((k, Some(block.get(o..end).ok_or_else(corrupt)?)));
			o = end;
		}
	}
	Ok(out)
}

#[cfg(test)]
mod tests {
	use super::*;

	fn packed(writes: &[(Vec<u8>, Option<Vec<u8>>)]) -> Vec<u8> {
		let mut out = (writes.len() as u32).to_le_bytes().to_vec();
		for (k, v) in writes {
			out.extend_from_slice(&(k.len() as u32).to_le_bytes());
			out.extend_from_slice(k);
			match v {
				Some(v) => {
					out.extend_from_slice(&(v.len() as u32).to_le_bytes());
					out.extend_from_slice(v);
				}
				None => out.extend_from_slice(&DELETE.to_le_bytes()),
			}
		}
		out
	}

	#[test]
	fn corrupt_blocks_and_write_sets_are_errors_not_panics() {
		let writes: Vec<(Vec<u8>, Option<Vec<u8>>)> =
			(0..5u32).map(|i| (format!("key{i}").into_bytes(), Some(vec![1; 4]))).collect();
		let p = packed(&writes);
		let block = &p[4..];
		assert_eq!(block_writes(block).unwrap().len(), 5);
		for cut in [1, 3, 6, block.len() - 1] {
			assert!(block_writes(&block[..cut]).is_err(), "cut at {cut}");
		}
		let mut long = block.to_vec();
		long[..4].copy_from_slice(&u32::MAX.to_le_bytes());
		assert!(block_writes(&long).is_err());
		assert!(crate::log::unpack(&u32::MAX.to_le_bytes()).is_err());
		assert!(crate::log::unpack(&p[..p.len() - 1]).is_err());
		assert_eq!(crate::log::unpack(&p).unwrap().len(), 5);
	}

	#[test]
	fn metadata_that_disagrees_with_its_footer_is_an_error_not_a_panic() {
		let writes: Vec<(Vec<u8>, Option<Vec<u8>>)> =
			(0..50u32).map(|i| (format!("key{i:04}").into_bytes(), Some(vec![i as u8; 5]))).collect();
		let (obj, meta_start, meta_len) = build(&packed(&writes)).unwrap();
		let meta = &obj[meta_start as usize..(meta_start + meta_len) as usize];
		// The footer's offsets are relative to the whole object: a start past them underflows.
		assert!(Ingested::parse(1, "k".into(), meta_start + meta_len, meta).is_err());
		// Metadata cut short, with its footer moved to the end, names regions that are not there.
		let footer = &meta[meta.len() - FOOTER..];
		let mut cut = meta[..8].to_vec();
		cut.extend_from_slice(footer);
		assert!(Ingested::parse(1, "k".into(), meta_start, &cut).is_err());
		// A count the index cannot hold.
		let mut huge = meta.to_vec();
		let index_at = u32::from_le_bytes(footer[0..4].try_into().unwrap()) - meta_start;
		huge[index_at as usize..index_at as usize + 4].copy_from_slice(&u32::MAX.to_le_bytes());
		assert!(Ingested::parse(1, "k".into(), meta_start, &huge).is_err());
	}

	#[test]
	fn reads_every_write_back_through_index_and_filter() {
		let writes: Vec<(Vec<u8>, Option<Vec<u8>>)> = (0..1000u32)
			.map(|i| (format!("key{i:06}").into_bytes(), if i % 7 == 0 { None } else { Some(vec![i as u8; (i % 13) as usize]) }))
			.collect();
		let p = packed(&writes);
		let (obj, meta_start, meta_len) = build(&p).unwrap();
		// Old readers: the packed prefix is unchanged.
		assert_eq!(&obj[..p.len()], p.as_slice());
		let ing = Ingested::parse(5, "k".into(), meta_start, &obj[meta_start as usize..(meta_start + meta_len) as usize]).unwrap();
		for (k, v) in &writes {
			assert!(ing.may_hold(k));
			let (s, l) = ing.block_range(ing.block_of(k).unwrap());
			let got = block_writes(&obj[s as usize..(s + l) as usize]).unwrap();
			let found = got.iter().find(|(gk, _)| gk == &k.as_slice()).expect("in its block");
			assert_eq!(found.1, v.as_deref());
		}
		let absent = (0..1000).filter(|i| ing.may_hold(format!("nope{i:06}").as_bytes())).count();
		assert!(absent < 50, "filter false positives: {absent}");
		assert!(!ing.may_hold(b"aaa"), "before the first key");
		// A range's blocks cover exactly its keys.
		let r = ing.blocks_in(b"key000100", b"key000300");
		let keys: Vec<Vec<u8>> = r
			.flat_map(|i| {
				let (s, l) = ing.block_range(i);
				block_writes(&obj[s as usize..(s + l) as usize]).unwrap().into_iter().map(|(k, _)| k.to_vec()).collect::<Vec<_>>()
			})
			.filter(|k| k.as_slice() >= b"key000100".as_slice() && k.as_slice() < b"key000300".as_slice())
			.collect();
		assert_eq!(keys.len(), 200);
	}

	/// A run of `n` sections (positions from `first`) over keys `k00000..k05000`: mostly a few keys,
	/// every 40th large enough to have fences. The run's bytes, its directory, and each section's
	/// writes by position.
	fn run_fixture(n: u64, first: u64) -> (Vec<u8>, RunDir, Vec<(u64, std::collections::BTreeMap<Vec<u8>, Option<Vec<u8>>>)>) {
		let mut seed = 0x9e37_79b9_7f4a_7c15u64;
		let mut next = move || {
			seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
			seed >> 33
		};
		let mut run = b"header".to_vec();
		let mut dir = RunDir::new(Arc::from("run"), n as usize);
		let mut all = Vec::new();
		for c in 0..n {
			let count = if c % 40 == 39 { 150 } else { 1 + next() % 12 };
			let value = if c % 40 == 39 { 3000 } else { 20 };
			let writes: std::collections::BTreeMap<Vec<u8>, Option<Vec<u8>>> = (0..count)
				.map(|_| {
					let k = format!("k{:05}", next() % 5000).into_bytes();
					let v = if next() % 4 == 0 { None } else { Some(vec![(c % 251) as u8; value]) };
					(k, v)
				})
				.collect();
			let built = surrealds_section::build(writes.iter().map(|(k, v)| (k.as_slice(), v.as_deref()))).expect("sorted");
			let meta = surrealds_section::Meta::parse_section(&built.bytes).expect("meta");
			dir.push(first + c, run.len() as u32, &meta);
			run.extend_from_slice(&built.bytes);
			all.push((first + c, writes));
		}
		(run, dir, all)
	}

	#[test]
	fn a_run_directory_reads_every_key_as_of_every_position_and_trims_by_position() {
		let (run, dir, all) = run_fixture(300, 100);
		assert_eq!((dir.first(), dir.last(), dir.len()), (100, 399, 300));
		assert!(dir.fences.len() >= 7, "the large sections have fences: {}", dir.fences.len());
		let newest = |key: &[u8], at: u64, after: u64| -> Option<(u64, Option<Vec<u8>>)> {
			all.iter().rev().find(|(p, w)| *p <= at && *p > after && w.contains_key(key)).map(|(p, w)| (*p, w[key].clone()))
		};
		let read = |dir: &RunDir, key: &[u8], at: u64, after: u64| -> Option<(u64, Option<Vec<u8>>)> {
			let mut out = Vec::new();
			dir.probe(key, surrealds_section::hash(key), after, at, &mut out);
			assert!(out.windows(2).all(|w| w[0].pos > w[1].pos), "newest first");
			out.iter().find_map(|b| {
				let bytes = &run[b.start as usize..(b.start + b.len) as usize];
				surrealds_section::find_in_block(bytes, key).expect("block").map(|v| (b.pos, v.map(<[u8]>::to_vec)))
			})
		};
		for at in [99, 100, 150, 239, 240, 399, 1_000] {
			for i in (0..5000).step_by(13) {
				let key = format!("k{i:05}").into_bytes();
				assert_eq!(read(&dir, &key, at, 0), newest(&key, at, 0), "key {i} as of {at}");
				assert_eq!(read(&dir, &key, at, 200), newest(&key, at, 200), "key {i} as of {at}, newer than 200");
			}
		}
		// A scan reads exactly the keys of the range, from every section it overlaps.
		let (lo, hi) = (b"k01000".to_vec(), b"k01500".to_vec());
		let mut srcs = Vec::new();
		dir.scan_srcs(&lo, &hi, 300, &mut srcs);
		let mut got: Vec<(u64, Vec<u8>)> = Vec::new();
		for src in &srcs {
			assert!(src.pos <= 300);
			for (start, len) in &src.blocks {
				for (k, _) in block_writes(&run[*start as usize..(*start + *len) as usize]).expect("block") {
					if k >= lo.as_slice() && k < hi.as_slice() {
						got.push((src.pos, k.to_vec()));
					}
				}
			}
		}
		let mut want: Vec<(u64, Vec<u8>)> = all.iter().filter(|(p, _)| *p <= 300).flat_map(|(p, w)| w.keys().filter(|k| **k >= lo && **k < hi).map(|k| (*p, k.clone()))).collect();
		got.sort();
		want.sort();
		assert_eq!(got, want);
		// The directory after a seal ends inside the run: the sections above, and nothing below.
		let trimmed = dir.after(250).expect("sections above");
		assert_eq!((trimmed.first(), trimmed.last(), trimmed.len()), (251, 399, 149));
		for i in (0..5000).step_by(11) {
			let key = format!("k{i:05}").into_bytes();
			assert_eq!(read(&trimmed, &key, 1_000, 0), newest(&key, 1_000, 250), "key {i} after the seal");
		}
		assert!(trimmed.footprint() < dir.footprint());
		assert!(dir.after(399).is_none());
		// What the sequencer counts for a section's directory entry (`SEC_NODE_OVERHEAD`, 64) covers
		// the entry's own bytes.
		assert!(std::mem::size_of::<Sec>() <= 48);
	}

	/// What a point read costs in the layers of rolled runs of one-key commits (the common small
	/// commit), by how many runs and how many commits a run holds: microseconds a read, and
	/// nanoseconds a section probed. Run with
	/// `cargo test --release -- --ignored --nocapture run_directory_probe`.
	#[test]
	#[ignore = "a measurement"]
	fn run_directory_probe_cost() {
		for (runs, per_run) in [(1u64, 1_000u64), (1, 8_000), (1, 80_000), (64, 125), (512, 16), (2_048, 8), (2_048, 1), (2_048, 12)] {
			let mut seed = 7u64;
			let layers: Vec<Layer> = (0..runs)
				.map(|r| {
					let mut dir = RunDir::new(Arc::from(format!("run{r}")), per_run as usize);
					for c in 0..per_run {
						seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
						let k = format!("table:{seed:016x}").into_bytes();
						let built = surrealds_section::build([(k.as_slice(), Some(&b"v"[..]))]).expect("one key");
						dir.push(r * per_run + c + 1, (c * 100) as u32, &surrealds_section::Meta::parse_section(&built.bytes).expect("meta"));
					}
					Layer::Run(dir)
				})
				.collect();
			let keys: Vec<Vec<u8>> = (0..2_000u64).map(|i| format!("table:{:016x}", i.wrapping_mul(0x9e37_79b9_7f4a_7c15)).into_bytes()).collect();
			let mut out = Vec::new();
			let t = std::time::Instant::now();
			for k in &keys {
				let h = surrealds_section::hash(k);
				for l in layers.iter().rev() {
					l.probe(k, h, 0, u64::MAX, &mut out);
				}
				out.clear();
			}
			let per = t.elapsed().as_nanos() as f64 / keys.len() as f64;
			let sections = (runs * per_run) as f64;
			let bytes: usize = layers.iter().map(Layer::footprint).sum();
			println!(
				"{runs} runs of {per_run}: {:.1} us a point read, {:.2} ns a section, {:.0} bytes a section",
				per / 1000.0,
				per / sections,
				bytes as f64 / sections
			);
		}
	}

	#[test]
	fn refuses_unsorted() {
		let p = packed(&[(b"b".to_vec(), None), (b"a".to_vec(), None)]);
		assert!(build(&p).is_err());
	}
}
