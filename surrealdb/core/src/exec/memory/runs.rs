//! The encoding of a spilled run: a header, checksummed blocks of records,
//! and a trailer, written into parts and read back from them.
//!
//! A record is a list of keys and a row, each a value encoded as a stored
//! value is and framed by its length; the record is framed by its key count.
//! A run is laid out, big-endian throughout, as:
//!
//! - a header: the magic, the run format, the build key, the program key, the breaker that wrote
//!   the run (kind, index, key count) and the order its records lie in, and the block size the
//!   writer used;
//! - blocks: a payload length, the CRC-32C of the payload, and the payload, whole records;
//! - a trailer: a zero length in a block's place, the counts of records, blocks and payload bytes
//!   written, and a closing magic.
//!
//! The store sees opaque bytes: parts are cut wherever a part's worth has
//! been written, so a block may cross a part boundary, and a reader holds the
//! part it is reading and, while a block crosses into the next, that one too.
//! A writer seals a block, patching its length and checksum, before any byte
//! of it leaves the writer; a reader hands on no record of a block until the
//! block's checksum matches. Because a part is written only after a block is
//! sealed, a writer holds up to a part and one block, and a reader a part and
//! the partial block crossing into the next: each is bounded by
//! [`buffer_bound`], which the lease's headroom reserves, plus the record
//! being encoded or decoded.
//!
//! Runs are temporary and are never read across builds: the build key, which
//! covers the program ISA version, the run format and the release, is checked
//! before any record is read. A run is opened only as the breaker that wrote
//! it ([`RunSpec`]); a run of another format, build, program or breaker, a
//! damaged block, a missing or disagreeing trailer, and bytes after the
//! trailer are each refused with a [`RunError`], so a run is never read as
//! something it is not and never answers its end silently.
//!
//! A [`Run`] owns its run in the store: dropping it deletes the run, so a
//! query that ends or fails before merging what it spilled leaves nothing
//! behind. A run handed to another process ([`Run::into_ref`]) is that
//! process's to delete ([`Run::adopt`]).
//!
//! A query writes one run at a time: a [`RunSink`] holds the query's writer
//! ([`QueryMemory::writer`]) from the moment it opens until it finishes or is
//! dropped.

// The runtime the IR lowering emits calls into.
#![allow(dead_code)]

use std::sync::Arc;

use anyhow::Context as _;
use revision::{DeserializeRevisioned, SerializeRevisioned};

use super::QueryMemory;
use super::store::{RunRead, RunRef, RunWrite, Sharing, TempStore, Tier};
use crate::exec::vm::program::ISA_VERSION;
use crate::val::Value;

/// The bytes a run begins with.
const MAGIC: [u8; 8] = *b"SRLSPILL";
/// The bytes a run's trailer ends with.
const END: [u8; 8] = *b"SRLSPEND";
/// The run format this build writes and the only one it reads.
pub(crate) const FORMAT: u16 = 1;
/// The largest block payload a writer gathers before it closes the block,
/// where the store's parts are larger.
const BLOCK_SIZE: usize = 64 << 10;
/// The bytes of a header up to and including its order tag.
const HEADER_FIXED: usize = 48;
/// The bytes of a block's length and checksum.
const BLOCK_HEADER: usize = 8;
/// The bytes of a trailer.
const TRAILER: usize = 36;

/// The bytes a run's writer or reader holds for a store whose parts are
/// `part_size` bytes: a part, and the block header and block it holds beyond
/// the part while a block is sealed or crosses a part boundary. A record being
/// encoded or decoded comes on top.
pub(crate) fn buffer_bound(part_size: usize) -> usize {
	part_size.saturating_add(BLOCK_SIZE.min(part_size)).saturating_add(BLOCK_HEADER)
}

pub(crate) const FNV_OFFSET: u128 = 0x6c62272e07bb014262b821756295c58d;
const FNV_PRIME: u128 = 0x0000000001000000000000000000013B;

/// 128-bit FNV-1a over `bytes`, continuing from `hash`.
pub(crate) const fn fnv1a_128(mut hash: u128, bytes: &[u8]) -> u128 {
	let mut at = 0;
	while at < bytes.len() {
		hash ^= bytes[at] as u128;
		hash = hash.wrapping_mul(FNV_PRIME);
		at += 1;
	}
	hash
}

/// A build's key: FNV-1a-128 over the ISA version (u32), the run format
/// (u16) and the bytes of the release, the fixed-width fields first so the
/// input is unambiguous.
pub(crate) const fn build_key(isa: u32, format: u16, release: &str) -> u128 {
	let hash = fnv1a_128(FNV_OFFSET, &isa.to_be_bytes());
	let hash = fnv1a_128(hash, &format.to_be_bytes());
	fnv1a_128(hash, release.as_bytes())
}

/// This build's key: a run written by another build is rejected.
pub(crate) const BUILD_KEY: [u8; 16] =
	build_key(ISA_VERSION, FORMAT, env!("CARGO_PKG_VERSION")).to_be_bytes();

const CRC_TABLE: [u32; 256] = {
	let mut table = [0u32; 256];
	let mut at = 0;
	while at < 256 {
		let mut crc = at as u32;
		let mut bit = 0;
		while bit < 8 {
			crc = if crc & 1 == 1 {
				(crc >> 1) ^ 0x82F6_3B78
			} else {
				crc >> 1
			};
			bit += 1;
		}
		table[at] = crc;
		at += 1;
	}
	table
};

/// The CRC-32C (Castagnoli) of `bytes`.
pub(crate) fn crc32c(bytes: &[u8]) -> u32 {
	let mut crc = u32::MAX;
	for byte in bytes {
		crc = CRC_TABLE[((crc ^ *byte as u32) & 0xFF) as usize] ^ (crc >> 8);
	}
	!crc
}

/// The identity a dispatcher gave a query's program; zero for a run no other
/// process reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub(crate) struct ProgramKey(pub [u8; 16]);

impl ProgramKey {
	/// The key of a run read only by the process that wrote it.
	pub(crate) const LOCAL: Self = Self([0; 16]);
}

/// The breaker, and the part of it, that wrote a run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum BreakerKind {
	Sort = 1,
	Groups = 2,
	DistinctSeen = 3,
	DistinctDeferred = 4,
	DistinctKept = 5,
	JoinBuild = 6,
	JoinProbe = 7,
	JoinOut = 8,
	WindowBuffer = 9,
}

impl BreakerKind {
	#[cfg_attr(not(test), allow(dead_code))]
	fn from_u8(byte: u8) -> Option<Self> {
		Some(match byte {
			1 => Self::Sort,
			2 => Self::Groups,
			3 => Self::DistinctSeen,
			4 => Self::DistinctDeferred,
			5 => Self::DistinctKept,
			6 => Self::JoinBuild,
			7 => Self::JoinProbe,
			8 => Self::JoinOut,
			9 => Self::WindowBuffer,
			_ => return None,
		})
	}
}

/// The order a run's records lie in.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RunOrder {
	/// `Value::cmp` on the key vector.
	Plain,
	/// One byte per sort key (bit 0 ascending, bit 1 collate, bit 2
	/// numeric); keys past the list compare by `Value::cmp`.
	Keys(Vec<u8>),
}

/// What a run was written for: a reader opens it only as the same.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RunSpec {
	pub kind: BreakerKind,
	/// The breaker's index within its program.
	pub index: u16,
	/// The key count every record carries.
	pub arity: u16,
	pub order: RunOrder,
	pub program: ProgramKey,
}

impl RunSpec {
	/// A spec for a run no other process reads.
	pub(crate) fn new(kind: BreakerKind, index: u16, arity: usize, order: RunOrder) -> Self {
		Self {
			kind,
			index,
			arity: u16::try_from(arity).expect("a breaker has fewer than 65536 keys"),
			order,
			program: ProgramKey::LOCAL,
		}
	}
}

/// Why a spilled run cannot be read.
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub(crate) enum RunError {
	#[error("A spilled run does not begin with a run header")]
	NotARun,
	#[error("A spilled run has format {0}, which this build cannot read")]
	Format(u16),
	#[error("A spilled run was written by a different build")]
	OtherBuild,
	#[error("A spilled run was written for a different program")]
	OtherProgram,
	#[error("A spilled run was written for a different breaker")]
	OtherBreaker,
	#[error("A spilled run is corrupt: block {0} failed its checksum")]
	Corrupt(u64),
	#[error("A spilled run is truncated")]
	Truncated,
	#[error("A spilled run holds a malformed record")]
	Malformed,
}

fn refuse(error: RunError) -> anyhow::Error {
	anyhow::Error::new(error)
}

/// A spilled record: its keys, then its row.
pub(crate) type Record = (Vec<Value>, Value);

/// A run in the store, deleted when dropped.
pub(crate) struct Run {
	run: RunRef,
	spec: RunSpec,
	bytes: u64,
	records: u64,
	store: Arc<dyn TempStore>,
	memory: Arc<QueryMemory>,
	/// Whether dropping the run deletes it: not once it is handed on.
	armed: bool,
}

impl std::fmt::Debug for Run {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("Run")
			.field("run", &self.run)
			.field("kind", &self.spec.kind)
			.field("records", &self.records)
			.finish()
	}
}

impl Run {
	/// A reader over the run's records, from the first.
	pub(crate) async fn read(&self) -> anyhow::Result<RunSource> {
		RunSource::open(&self.store, &self.run, &self.spec).await
	}

	/// What the run was written for.
	pub(crate) fn spec(&self) -> &RunSpec {
		&self.spec
	}

	/// The records the run holds.
	pub(crate) fn records(&self) -> u64 {
		self.records
	}

	/// Which processes can open the run.
	#[cfg_attr(not(test), allow(dead_code))]
	pub(crate) fn tier(&self) -> Tier {
		self.run.tier
	}

	/// Hand the run on: it is no longer deleted when dropped, and its bytes no
	/// longer count against this query's quota. Answers its name and its
	/// record count; the receiver owns it from here ([`Run::adopt`]).
	#[cfg_attr(not(test), allow(dead_code))]
	pub(crate) fn into_ref(mut self) -> (RunRef, u64) {
		self.armed = false;
		self.memory.unspilled(self.bytes);
		(self.run.clone(), self.records)
	}

	/// Take ownership of `run`, written for `spec` with `records` records by
	/// another query or process: it is read through `memory`'s store, counted
	/// against `memory`'s quota, and deleted when dropped. A run refused for
	/// the quota is not counted and is not owned: the caller deletes it.
	#[cfg_attr(not(test), allow(dead_code))]
	pub(crate) fn adopt(
		memory: &Arc<QueryMemory>,
		run: RunRef,
		spec: RunSpec,
		records: u64,
	) -> anyhow::Result<Run> {
		let store = memory.store().context("A query adopted a run with no temporary store")?;
		if let Err(error) = memory.spilled(run.bytes) {
			memory.unspilled(run.bytes);
			return Err(error);
		}
		Ok(Run {
			bytes: run.bytes,
			run,
			spec,
			records,
			store,
			memory: Arc::clone(memory),
			armed: true,
		})
	}

	/// The run's name in its store.
	#[cfg(test)]
	pub(crate) fn run_ref(&self) -> &RunRef {
		&self.run
	}
}

impl Drop for Run {
	fn drop(&mut self) {
		if self.armed {
			self.store.delete_run(&self.run);
			self.memory.unspilled(self.bytes);
		}
	}
}

/// The bytes a run being written has counted as spilled, released if the sink
/// is dropped before it finishes into a [`Run`], which then owns the count.
struct Spilled {
	memory: Arc<QueryMemory>,
	bytes: u64,
}

impl Drop for Spilled {
	fn drop(&mut self) {
		self.memory.unspilled(self.bytes);
	}
}

/// A run being written: records gathered into blocks, and blocks into parts
/// written out as they fill.
///
/// The bytes written are exactly the header, sealed blocks and the trailer: a
/// part is written only directly after a block is sealed.
pub(crate) struct RunSink {
	writer: Box<dyn RunWrite>,
	buf: Vec<u8>,
	part_size: usize,
	block_size: usize,
	/// Where in `buf` the open block's length and checksum are.
	block_start: usize,
	spec: RunSpec,
	records: u64,
	blocks: u64,
	payload: u64,
	store: Arc<dyn TempStore>,
	memory: Arc<QueryMemory>,
	spilled: Spilled,
	/// The query's one run writer, held until the run is finished or the sink
	/// is dropped.
	_writing: tokio::sync::OwnedSemaphorePermit,
}

impl RunSink {
	/// Begin a run in `memory`'s store, written as `spec` says and readable as
	/// `sharing` says.
	pub(crate) async fn open(
		memory: &Arc<QueryMemory>,
		spec: RunSpec,
		sharing: Sharing,
	) -> anyhow::Result<Self> {
		let store = memory.store().context("A query spilled with no temporary store")?;
		let writing = memory.writer().await?;
		let part_size = store.part_size();
		let block_size = BLOCK_SIZE.min(part_size);
		let writer = store.write_run(sharing).await?;
		let mut buf = Vec::with_capacity(buffer_bound(part_size));
		write_header(&mut buf, &spec, block_size);
		let block_start = buf.len();
		buf.extend_from_slice(&[0u8; BLOCK_HEADER]);
		Ok(Self {
			writer,
			buf,
			part_size,
			block_size,
			block_start,
			spec,
			records: 0,
			blocks: 0,
			payload: 0,
			store,
			memory: Arc::clone(memory),
			spilled: Spilled {
				memory: Arc::clone(memory),
				bytes: 0,
			},
			_writing: writing,
		})
	}

	/// Append a record. A record that would take the open block past the
	/// block size opens the next block; a block that reaches the block size
	/// is sealed. Sealed blocks are written out once the buffer holds a part,
	/// never an open block.
	pub(crate) async fn push(&mut self, keys: &[Value], row: &Value) -> anyhow::Result<()> {
		anyhow::ensure!(
			keys.len() == self.spec.arity as usize,
			"Internal error: a spilled record has {} keys where its run has {}",
			keys.len(),
			self.spec.arity
		);
		let rec = self.buf.len();
		encode_record(&mut self.buf, keys, row)?;
		// A record that outgrows the spare capacity doubles the buffer; give back
		// what the record did not need so the buffer holds `buffer_bound` plus
		// the record and no more.
		self.buf.shrink_to(buffer_bound(self.part_size).max(self.buf.len()));
		self.records += 1;
		let body = self.block_start + BLOCK_HEADER;
		if rec > body && self.buf.len() - body > self.block_size {
			self.seal(rec)?;
			if rec >= self.part_size {
				// The sealed blocks make a part; the record, which opens the
				// next block, stays behind.
				let record = self.buf.split_off(rec);
				self.write_part().await?;
				self.block_start = 0;
				self.buf.extend_from_slice(&[0u8; BLOCK_HEADER]);
				self.buf.extend_from_slice(&record);
			} else {
				self.buf.splice(rec..rec, [0u8; BLOCK_HEADER]);
				self.block_start = rec;
			}
		}
		if self.buf.len() - (self.block_start + BLOCK_HEADER) >= self.block_size {
			self.seal(self.buf.len())?;
			if self.buf.len() >= self.part_size {
				self.write_part().await?;
			}
			self.block_start = self.buf.len();
			self.buf.extend_from_slice(&[0u8; BLOCK_HEADER]);
		}
		Ok(())
	}

	/// Close the open block at `end`, writing its length and checksum.
	fn seal(&mut self, end: usize) -> anyhow::Result<()> {
		let body = self.block_start + BLOCK_HEADER;
		let len = u32::try_from(end - body).context("A spilled block is too large")?;
		let crc = crc32c(&self.buf[body..end]);
		self.buf[self.block_start..self.block_start + 4].copy_from_slice(&len.to_be_bytes());
		self.buf[self.block_start + 4..body].copy_from_slice(&crc.to_be_bytes());
		self.blocks += 1;
		self.payload += len as u64;
		Ok(())
	}

	async fn write_part(&mut self) -> anyhow::Result<()> {
		let len = self.buf.len() as u64;
		self.spilled.bytes += len;
		self.memory.spilled(len)?;
		let part =
			std::mem::replace(&mut self.buf, Vec::with_capacity(buffer_bound(self.part_size)));
		self.writer.write_part(part).await
	}

	/// Close the run: seal the open block, if it holds a record, and write the
	/// trailer.
	pub(crate) async fn finish(mut self) -> anyhow::Result<Run> {
		if self.buf.len() > self.block_start + BLOCK_HEADER {
			self.seal(self.buf.len())?;
		} else {
			self.buf.truncate(self.block_start);
		}
		self.buf.extend_from_slice(&0u32.to_be_bytes());
		self.buf.extend_from_slice(&self.records.to_be_bytes());
		self.buf.extend_from_slice(&self.blocks.to_be_bytes());
		self.buf.extend_from_slice(&self.payload.to_be_bytes());
		self.buf.extend_from_slice(&END);
		self.write_part().await?;
		let run = self.writer.finish().await?;
		Ok(Run {
			run,
			spec: self.spec,
			bytes: std::mem::take(&mut self.spilled.bytes),
			records: self.records,
			store: self.store,
			memory: self.memory,
			armed: true,
		})
	}
}

fn write_header(out: &mut Vec<u8>, spec: &RunSpec, block_size: usize) {
	out.extend_from_slice(&MAGIC);
	out.extend_from_slice(&FORMAT.to_be_bytes());
	out.extend_from_slice(&BUILD_KEY);
	write_spec(out, spec);
	out.extend_from_slice(&(block_size as u32).to_be_bytes());
}

/// Append what a run was written for: its program, its breaker (kind, index,
/// key count) and the order of its records, as a run's header carries them.
pub(crate) fn write_spec(out: &mut Vec<u8>, spec: &RunSpec) {
	out.extend_from_slice(&spec.program.0);
	out.push(spec.kind as u8);
	out.extend_from_slice(&spec.index.to_be_bytes());
	out.extend_from_slice(&spec.arity.to_be_bytes());
	match &spec.order {
		RunOrder::Plain => out.push(0),
		RunOrder::Keys(keys) => {
			out.push(1);
			out.extend_from_slice(&(keys.len() as u16).to_be_bytes());
			out.extend_from_slice(keys);
		}
	}
}

/// The spec [`write_spec`] wrote at `at` in `bytes`, and where it ends, or
/// `None` when `bytes` ends first or holds a kind or an order tag no breaker
/// writes.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn read_spec(bytes: &[u8], at: usize) -> Option<(RunSpec, usize)> {
	let program: [u8; 16] = bytes.get(at..at.checked_add(16)?)?.try_into().ok()?;
	let fixed = bytes.get(at + 16..at.checked_add(16 + 5)?)?;
	let kind = BreakerKind::from_u8(fixed[0])?;
	let (index, arity) = (be_u16(&fixed[1..3]), be_u16(&fixed[3..5]));
	let mut end = at + 21;
	let order = match *bytes.get(end)? {
		0 => {
			end += 1;
			RunOrder::Plain
		}
		1 => {
			let count = be_u16(bytes.get(end + 1..end.checked_add(3)?)?) as usize;
			let keys = bytes.get(end + 3..end.checked_add(3 + count)?)?.to_vec();
			end += 3 + count;
			RunOrder::Keys(keys)
		}
		_ => return None,
	};
	let spec = RunSpec {
		kind,
		index,
		arity,
		order,
		program: ProgramKey(program),
	};
	Some((spec, end))
}

/// A run's records, read back in the order written, each block checked
/// before any of its records is answered.
pub(crate) struct RunSource {
	reader: Option<Box<dyn RunRead>>,
	buf: Vec<u8>,
	/// The capacity `buf` is grown to when a part is appended: [`buffer_bound`] of
	/// the store's part size.
	bound: usize,
	/// The next byte to parse.
	at: usize,
	/// Where `buf` begins in the run, so `base + at` is the next byte's offset.
	base: u64,
	/// The bytes the run holds, from its [`RunRef`]. A block's length is not covered by
	/// its checksum, so a block that cannot fit in what remains, with the
	/// trailer after it, is refused before any of it is buffered.
	size: u64,
	/// The end of the verified block being decoded; `at` when none is open.
	block_end: usize,
	arity: usize,
	records: u64,
	blocks: u64,
	payload: u64,
	done: bool,
}

fn be_u16(bytes: &[u8]) -> u16 {
	u16::from_be_bytes([bytes[0], bytes[1]])
}

fn be_u32(bytes: &[u8]) -> u32 {
	u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
}

fn be_u64(bytes: &[u8]) -> u64 {
	let mut raw = [0u8; 8];
	raw.copy_from_slice(&bytes[..8]);
	u64::from_be_bytes(raw)
}

impl RunSource {
	/// Open `run` in `store`, reading and checking its header against this
	/// build and `expected` before any record is read. The run's size is
	/// `run.bytes`; a block that, with the trailer, would pass it is refused
	/// as truncated.
	pub(crate) async fn open(
		store: &Arc<dyn TempStore>,
		run: &RunRef,
		expected: &RunSpec,
	) -> anyhow::Result<Self> {
		let mut source = Self {
			reader: Some(store.read_run(run).await?),
			buf: Vec::new(),
			bound: buffer_bound(store.part_size()),
			at: 0,
			base: 0,
			size: run.bytes,
			block_end: 0,
			arity: expected.arity as usize,
			records: 0,
			blocks: 0,
			payload: 0,
			done: false,
		};
		if !source.fill(HEADER_FIXED).await? {
			return Err(refuse(RunError::Truncated));
		}
		let head = &source.buf[source.at..source.at + HEADER_FIXED];
		if head[..8] != MAGIC {
			return Err(refuse(RunError::NotARun));
		}
		let format = be_u16(&head[8..10]);
		if format != FORMAT {
			return Err(refuse(RunError::Format(format)));
		}
		if head[10..26] != BUILD_KEY {
			return Err(refuse(RunError::OtherBuild));
		}
		let mut wanted = Vec::new();
		write_spec(&mut wanted, expected);
		if head[26..42] != wanted[..16] {
			return Err(refuse(RunError::OtherProgram));
		}
		if head[42..47] != wanted[16..21] {
			return Err(refuse(RunError::OtherBreaker));
		}
		// The spec ends with the order, a tag and, for sort keys, their count
		// and bytes, which follow the fixed part of the header.
		let mut spec_len = HEADER_FIXED - 26;
		match head[47] {
			0 => {}
			1 => {
				if !source.fill(HEADER_FIXED + 2).await? {
					return Err(refuse(RunError::Truncated));
				}
				let count = be_u16(&source.buf[source.at + HEADER_FIXED..]) as usize;
				if !source.fill(HEADER_FIXED + 2 + count).await? {
					return Err(refuse(RunError::Truncated));
				}
				spec_len += 2 + count;
			}
			_ => return Err(refuse(RunError::Malformed)),
		}
		if source.buf[source.at + 26..source.at + 26 + spec_len] != wanted[..] {
			return Err(refuse(RunError::OtherBreaker));
		}
		source.advance(26 + spec_len);
		if !source.fill(4).await? {
			return Err(refuse(RunError::Truncated));
		}
		if be_u32(&source.buf[source.at..]) == 0 {
			return Err(refuse(RunError::Malformed));
		}
		source.advance(4);
		Ok(source)
	}

	/// Step past `bytes` of header or block header, with no block open.
	fn advance(&mut self, bytes: usize) {
		self.at += bytes;
		self.block_end = self.at;
	}

	/// Whether `n` bytes are available from `at`, reading parts until they
	/// are. Called only with no block open, so parts already consumed can be
	/// dropped.
	async fn fill(&mut self, n: usize) -> anyhow::Result<bool> {
		debug_assert_eq!(self.at, self.block_end);
		while self.buf.len() - self.at < n {
			let Some(reader) = &mut self.reader else {
				return Ok(false);
			};
			let Some(part) = reader.next_part().await? else {
				self.reader = None;
				return Ok(false);
			};
			self.buf.drain(..self.at);
			self.base += self.at as u64;
			self.at = 0;
			self.block_end = 0;
			if self.buf.is_empty() {
				self.buf = part;
			} else {
				let len = self.buf.len();
				self.buf.reserve_exact(self.bound.max(len + part.len()) - len);
				self.buf.extend_from_slice(&part);
			}
		}
		Ok(true)
	}

	/// The next record, or `None` at the end of the run.
	///
	/// A record is answered only from a block whose checksum has matched. The
	/// end of the run is its trailer, which must agree with what was read; a
	/// run that ends otherwise is an error, never an early `None`.
	pub(crate) async fn next(&mut self) -> anyhow::Result<Option<Record>> {
		loop {
			if self.done {
				return Ok(None);
			}
			if self.at < self.block_end {
				let Some((record, used)) = decode_record(&self.buf[self.at..self.block_end])?
				else {
					return Err(refuse(RunError::Malformed));
				};
				if record.0.len() != self.arity {
					return Err(refuse(RunError::Malformed));
				}
				self.at += used;
				self.records += 1;
				return Ok(Some(record));
			}
			if !self.fill(4).await? {
				return Err(refuse(RunError::Truncated));
			}
			let len = be_u32(&self.buf[self.at..]) as usize;
			if len == 0 {
				self.read_trailer().await?;
				return Ok(None);
			}
			let total = len.checked_add(BLOCK_HEADER).ok_or_else(|| refuse(RunError::Malformed))?;
			let end = (self.base + self.at as u64).saturating_add(total as u64);
			if end.saturating_add(TRAILER as u64) > self.size {
				return Err(refuse(RunError::Truncated));
			}
			if !self.fill(total).await? {
				return Err(refuse(RunError::Truncated));
			}
			let body = self.at + BLOCK_HEADER;
			if crc32c(&self.buf[body..body + len]) != be_u32(&self.buf[self.at + 4..]) {
				return Err(refuse(RunError::Corrupt(self.blocks)));
			}
			self.blocks += 1;
			self.payload += len as u64;
			self.at = body;
			self.block_end = body + len;
		}
	}

	async fn read_trailer(&mut self) -> anyhow::Result<()> {
		if !self.fill(TRAILER).await? {
			return Err(refuse(RunError::Truncated));
		}
		let trailer = &self.buf[self.at..self.at + TRAILER];
		if trailer[28..] != END
			|| be_u64(&trailer[4..]) != self.records
			|| be_u64(&trailer[12..]) != self.blocks
			|| be_u64(&trailer[20..]) != self.payload
		{
			return Err(refuse(RunError::Truncated));
		}
		self.advance(TRAILER);
		if self.buf.len() > self.at {
			return Err(refuse(RunError::Malformed));
		}
		while let Some(reader) = &mut self.reader {
			match reader.next_part().await? {
				Some(part) if !part.is_empty() => return Err(refuse(RunError::Malformed)),
				Some(_) => {}
				None => self.reader = None,
			}
		}
		self.done = true;
		Ok(())
	}
}

fn encode_value(out: &mut Vec<u8>, value: &Value) -> anyhow::Result<()> {
	let at = out.len();
	out.extend_from_slice(&[0u8; 8]);
	crate::val::closure::with_spill_serialization(|| {
		SerializeRevisioned::serialize_revisioned(value, out)
	})?;
	let len = (out.len() - at - 8) as u64;
	out[at..at + 8].copy_from_slice(&len.to_be_bytes());
	Ok(())
}

/// Append the framed encoding of a record to `out`.
pub(crate) fn encode_record(out: &mut Vec<u8>, keys: &[Value], row: &Value) -> anyhow::Result<()> {
	out.extend_from_slice(&(keys.len() as u64).to_be_bytes());
	for key in keys {
		encode_value(out, key)?;
	}
	encode_value(out, row)
}

fn read_u64(bytes: &[u8], at: usize) -> Option<u64> {
	let end = at.checked_add(8)?;
	let raw: [u8; 8] = bytes.get(at..end)?.try_into().ok()?;
	Some(u64::from_be_bytes(raw))
}

/// The value framed at `at`, and where the next frame starts, or `None` when
/// `bytes` ends before the frame does.
fn decode_value(bytes: &[u8], at: usize) -> anyhow::Result<Option<(Value, usize)>> {
	let Some(len) = read_u64(bytes, at) else {
		return Ok(None);
	};
	let start = at + 8;
	let Some(end) = usize::try_from(len).ok().and_then(|len| start.checked_add(len)) else {
		return Err(anyhow::Error::new(RunError::Malformed));
	};
	let Some(mut frame) = bytes.get(start..end) else {
		return Ok(None);
	};
	let value = crate::val::closure::with_spill_serialization(|| {
		DeserializeRevisioned::deserialize_revisioned(&mut frame)
	})?;
	Ok(Some((value, end)))
}

/// The record at the start of `bytes`, and its length, or `None` when `bytes`
/// ends before the record does.
pub(crate) fn decode_record(bytes: &[u8]) -> anyhow::Result<Option<(Record, usize)>> {
	let Some(count) = read_u64(bytes, 0) else {
		return Ok(None);
	};
	let mut at = 8;
	let mut keys = Vec::with_capacity((count as usize).min(64));
	for _ in 0..count {
		let Some((key, next)) = decode_value(bytes, at)? else {
			return Ok(None);
		};
		keys.push(key);
		at = next;
	}
	let Some((row, next)) = decode_value(bytes, at)? else {
		return Ok(None);
	};
	Ok(Some(((keys, row), next)))
}

#[cfg(test)]
mod tests {
	use std::sync::atomic::Ordering;

	use super::super::governor::Unlimited;
	use super::super::store::{MemTempStore, Tier};
	use super::super::{MemoryHost, MemorySettings};
	use super::*;

	fn memory_over(store: Arc<dyn TempStore>, fan_in: usize) -> Arc<QueryMemory> {
		MemoryHost {
			governor: Arc::new(Unlimited),
			store: Some(store),
			settings: MemorySettings {
				floor: 1 << 20,
				fan_in,
				chunk: 4096,
				wait: std::time::Duration::ZERO,
				max: usize::MAX,
			},
		}
		.query()
	}

	fn spec(kind: BreakerKind) -> RunSpec {
		RunSpec::new(kind, 3, 2, RunOrder::Plain)
	}

	fn record(i: usize) -> Record {
		(vec![Value::from(i % 5), Value::from(i)], Value::from(format!("row-{i}")))
	}

	async fn write(
		memory: &Arc<QueryMemory>,
		spec: RunSpec,
		sharing: Sharing,
		records: Vec<Record>,
	) -> Run {
		let mut sink = RunSink::open(memory, spec, sharing).await.expect("a sink");
		for (keys, row) in &records {
			sink.push(keys, row).await.expect("a record");
		}
		sink.finish().await.expect("finished")
	}

	/// The records `source` answers and the error that ends them, if any.
	async fn drain(mut source: RunSource) -> (Vec<Record>, Option<anyhow::Error>) {
		let mut got = Vec::new();
		loop {
			match source.next().await {
				Ok(Some(record)) => got.push(record),
				Ok(None) => return (got, None),
				Err(e) => return (got, Some(e)),
			}
		}
	}

	fn error_of(error: Option<anyhow::Error>) -> RunError {
		let error = error.expect("the run is refused");
		error.downcast_ref::<RunError>().expect("a run error").clone()
	}

	/// A run of 300 records over a store of `part_size`, with a handle to
	/// tamper with it.
	async fn written(part_size: usize, count: usize) -> (Arc<MemTempStore>, Run, Vec<Record>) {
		let store = Arc::new(MemTempStore::new(part_size, None));
		let memory = memory_over(Arc::clone(&store) as Arc<dyn TempStore>, 4);
		let records: Vec<Record> = (0..count).map(record).collect();
		let run = write(&memory, spec(BreakerKind::Sort), Sharing::Private, records.clone()).await;
		(store, run, records)
	}

	async fn open_of(
		store: &Arc<MemTempStore>,
		run: &Run,
		expected: &RunSpec,
	) -> anyhow::Result<RunSource> {
		let store = Arc::clone(store) as Arc<dyn TempStore>;
		RunSource::open(&store, run.run_ref(), expected).await
	}

	#[test]
	fn crc32c_matches_the_castagnoli_check_value() {
		assert_eq!(crc32c(b"123456789"), 0xE306_9283);
		assert_eq!(crc32c(b""), 0);
	}

	#[test]
	fn fnv1a_128_matches_its_published_vectors() {
		assert_eq!(fnv1a_128(FNV_OFFSET, b""), 0x6c62272e07bb014262b821756295c58d);
		assert_eq!(fnv1a_128(FNV_OFFSET, b"a"), 0xd228cb696f1a8caf78912b704e4a8964);
		let release = env!("CARGO_PKG_VERSION");
		assert_eq!(build_key(ISA_VERSION, FORMAT, release).to_be_bytes(), BUILD_KEY);
		assert_ne!(build_key(ISA_VERSION, FORMAT + 1, release).to_be_bytes(), BUILD_KEY);
		assert_ne!(build_key(ISA_VERSION + 1, FORMAT, release).to_be_bytes(), BUILD_KEY);
	}

	#[tokio::test]
	async fn a_run_round_trips_its_header_records_and_trailer() {
		for part_size in [1, 7, 64, 1 << 20] {
			let big = (vec![Value::from(1), Value::from(2)], Value::from("x".repeat(200 << 10)));
			for with_big in [false, true] {
				for count in [0, 1, 300] {
					let store = Arc::new(MemTempStore::new(part_size, None));
					let memory = memory_over(Arc::clone(&store) as Arc<dyn TempStore>, 4);
					let mut records: Vec<Record> = (0..count).map(record).collect();
					if with_big {
						records.push(big.clone());
					}
					let run =
						write(&memory, spec(BreakerKind::Sort), Sharing::Private, records.clone())
							.await;
					assert_eq!(run.records(), records.len() as u64);
					let (got, error) = drain(run.read().await.expect("a reader")).await;
					assert!(error.is_none());
					assert_eq!(got, records);
					let mut bytes = Vec::new();
					store.tamper(run.run_ref(), |b| bytes = b.clone());
					assert_eq!(&bytes[..8], b"SRLSPILL");
					assert_eq!(&bytes[bytes.len() - 8..], b"SRLSPEND");
					assert_eq!(&bytes[42..48], &[BreakerKind::Sort as u8, 0, 3, 0, 2, 0]);
				}
			}
		}
	}

	#[tokio::test]
	async fn a_run_with_another_magic_is_not_a_run() {
		let (store, run, _) = written(1 << 20, 3).await;
		store.tamper(run.run_ref(), |b| b[0] ^= 0xFF);
		let error = open_of(&store, &run, &spec(BreakerKind::Sort)).await.err();
		assert_eq!(error_of(error), RunError::NotARun);
	}

	#[tokio::test]
	async fn a_run_of_another_format_is_rejected() {
		let (store, run, _) = written(1 << 20, 3).await;
		store.tamper(run.run_ref(), |b| b[8..10].copy_from_slice(&(FORMAT + 1).to_be_bytes()));
		let error = open_of(&store, &run, &spec(BreakerKind::Sort)).await.err();
		assert_eq!(error_of(error), RunError::Format(FORMAT + 1));
	}

	#[tokio::test]
	async fn a_run_of_another_build_is_rejected() {
		let (store, run, _) = written(1 << 20, 3).await;
		store.tamper(run.run_ref(), |b| b[10] ^= 0xFF);
		let error = open_of(&store, &run, &spec(BreakerKind::Sort)).await.err();
		assert_eq!(error_of(error), RunError::OtherBuild);
	}

	#[tokio::test]
	async fn a_run_of_another_program_is_rejected() {
		let store = Arc::new(MemTempStore::new(1 << 20, None));
		let memory = memory_over(Arc::clone(&store) as Arc<dyn TempStore>, 4);
		let other = RunSpec {
			program: ProgramKey([1; 16]),
			..spec(BreakerKind::Sort)
		};
		let run = write(&memory, other, Sharing::Private, vec![record(1)]).await;
		let error = open_of(&store, &run, &spec(BreakerKind::Sort)).await.err();
		assert_eq!(error_of(error), RunError::OtherProgram);
	}

	#[tokio::test]
	async fn a_run_of_another_breaker_is_rejected() {
		let (store, run, _) = written(1 << 20, 3).await;
		let base = spec(BreakerKind::Sort);
		let others = [
			spec(BreakerKind::Groups),
			RunSpec {
				index: 4,
				..base.clone()
			},
			RunSpec {
				arity: 3,
				..base.clone()
			},
			RunSpec {
				order: RunOrder::Keys(vec![1]),
				..base.clone()
			},
		];
		for other in others {
			let error = open_of(&store, &run, &other).await.err();
			assert_eq!(error_of(error), RunError::OtherBreaker);
		}
	}

	#[tokio::test]
	async fn a_damaged_block_fails_before_any_of_its_rows() {
		let (store, run, _) = written(1 << 20, 300).await;
		store.tamper(run.run_ref(), |b| b[60] ^= 0xFF);
		let source = open_of(&store, &run, &spec(BreakerKind::Sort)).await.expect("a header");
		let (got, error) = drain(source).await;
		assert!(got.is_empty());
		assert_eq!(error_of(error), RunError::Corrupt(0));

		let (store, run, _) = written(1 << 20, 3000).await;
		store.tamper(run.run_ref(), |b| {
			let at = b.len() - 36 - 1;
			b[at] ^= 0xFF;
		});
		let source = open_of(&store, &run, &spec(BreakerKind::Sort)).await.expect("a header");
		let (got, error) = drain(source).await;
		assert!(!got.is_empty() && got.len() < 3000);
		let RunError::Corrupt(block) = error_of(error) else {
			panic!("a corrupt block");
		};
		assert!(block > 0);
	}

	/// A block length no run could hold is refused as truncation.
	#[tokio::test]
	async fn a_block_longer_than_the_run_is_refused() {
		let (store, run, _) = written(64, 300).await;
		store.tamper(run.run_ref(), |b| b[52..56].copy_from_slice(&0x7FFF_FFFFu32.to_be_bytes()));
		let mut source = open_of(&store, &run, &spec(BreakerKind::Sort)).await.expect("a header");
		let error = source.next().await.expect_err("a refused block");
		assert_eq!(error_of(Some(error)), RunError::Truncated);
	}

	#[tokio::test]
	async fn a_run_without_its_trailer_is_truncated() {
		let (store, run, _) = written(1 << 20, 300).await;
		store.tamper(run.run_ref(), |b| b.truncate(b.len() - 36));
		let source = open_of(&store, &run, &spec(BreakerKind::Sort)).await.expect("a header");
		let (got, error) = drain(source).await;
		assert_eq!(got.len(), 300);
		assert_eq!(error_of(error), RunError::Truncated);
	}

	#[tokio::test]
	async fn a_run_whose_counts_disagree_is_truncated() {
		let (store, run, _) = written(1 << 20, 300).await;
		store.tamper(run.run_ref(), |b| {
			let len = b.len();
			b[len - 32..len - 24].copy_from_slice(&299u64.to_be_bytes());
		});
		let source = open_of(&store, &run, &spec(BreakerKind::Sort)).await.expect("a header");
		let (got, error) = drain(source).await;
		assert_eq!(got.len(), 300);
		assert_eq!(error_of(error), RunError::Truncated);
	}

	#[tokio::test]
	async fn a_run_with_bytes_after_its_trailer_is_malformed() {
		let (store, run, _) = written(1 << 20, 300).await;
		store.tamper(run.run_ref(), |b| b.push(0));
		let source = open_of(&store, &run, &spec(BreakerKind::Sort)).await.expect("a header");
		let (got, error) = drain(source).await;
		assert_eq!(got.len(), 300);
		assert_eq!(error_of(error), RunError::Malformed);
	}

	#[tokio::test]
	async fn a_record_of_another_arity_is_refused_by_the_writer() {
		let store = Arc::new(MemTempStore::new(64, None));
		let memory = memory_over(store as Arc<dyn TempStore>, 4);
		let mut sink = RunSink::open(&memory, spec(BreakerKind::Sort), Sharing::Private)
			.await
			.expect("a sink");
		assert!(sink.push(&[Value::from(1)], &Value::from(1)).await.is_err());
	}

	/// A sink dropped before it finishes gives back what it counted as spilled.
	#[tokio::test]
	async fn a_dropped_sink_releases_its_spilled_bytes() {
		let store = Arc::new(MemTempStore::new(64, None));
		let memory = memory_over(store as Arc<dyn TempStore>, 4);
		let mut sink = RunSink::open(&memory, spec(BreakerKind::Sort), Sharing::Private)
			.await
			.expect("a sink");
		for i in 0..300 {
			let (keys, row) = record(i);
			sink.push(&keys, &row).await.expect("a record");
		}
		assert!(memory.spilled.load(Ordering::Relaxed) > 0);
		drop(sink);
		assert_eq!(memory.spilled.load(Ordering::Relaxed), 0);
	}

	#[tokio::test]
	async fn a_shared_run_opens_through_a_second_handle() {
		let (a, b) = MemTempStore::shared(64, None);
		let (a, b) = (Arc::new(a), Arc::new(b));
		let memory = memory_over(Arc::clone(&a) as Arc<dyn TempStore>, 4);
		let records: Vec<Record> = (0..300).map(record).collect();
		let run = write(&memory, spec(BreakerKind::Sort), Sharing::Shared, records.clone()).await;
		assert_eq!(run.run_ref().tier, Tier::Shared);
		assert_eq!(run.run_ref().session, *a.session());
		let other = Arc::clone(&b) as Arc<dyn TempStore>;
		let source = RunSource::open(&other, run.run_ref(), &spec(BreakerKind::Sort))
			.await
			.expect("a header");
		let (got, error) = drain(source).await;
		assert!(error.is_none());
		assert_eq!(got, records);
		drop(run);
		assert_eq!(b.runs(), 0);
	}

	#[tokio::test]
	async fn a_private_run_is_not_readable_from_another_handle() {
		let (a, b) = MemTempStore::shared(64, None);
		let memory = memory_over(Arc::new(a) as Arc<dyn TempStore>, 4);
		let run = write(&memory, spec(BreakerKind::Sort), Sharing::Private, vec![record(1)]).await;
		assert_eq!(run.run_ref().tier, Tier::Local);
		assert!(b.read_run(run.run_ref()).await.is_err());
	}

	#[tokio::test]
	async fn a_local_store_refuses_to_share() {
		let store = MemTempStore::new(64, None);
		assert!(!store.shares());
		assert!(store.write_run(Sharing::Shared).await.is_err());
	}

	#[tokio::test]
	async fn a_run_round_trips_records_containing_closures() {
		let store = Arc::new(MemTempStore::new(64, None));
		let memory = memory_over(Arc::clone(&store) as Arc<dyn TempStore>, 4);
		let closure = Value::Closure(Box::new(crate::val::Closure::Expr {
			args: vec![(crate::expr::Param::from("x".to_string()), crate::expr::Kind::Any)],
			returns: None,
			body: crate::expr::Expr::Literal(crate::expr::Literal::Integer(42)),
			captures: crate::expr::variables::Variables::new(),
		}));
		let records = vec![
			(vec![Value::from(1), closure.clone()], closure.clone()),
			(vec![Value::from(2), Value::from("k")], Value::Array(vec![closure].into())),
		];
		let run = write(&memory, spec(BreakerKind::Sort), Sharing::Private, records.clone()).await;
		let (got, error) = drain(run.read().await.expect("a reader")).await;
		assert!(error.is_none());
		assert_eq!(got, records);
	}
}
