//! The values stored under graph adjacency keys.
//!
//! [`AdjacencyValue`] is the value of every vertex-side pointer key
//! (`GraphPointerKey`). [`AdjacencyBlock`] is the value of a packed
//! adjacency chunk (`AdjacencyKey`), which holds a sorted run of a
//! (vertex, direction, edge-table) scope's edges folded out of the
//! per-edge pointer keys.

use anyhow::{Result, bail, ensure};
use surrealdb_catalog::{DatabaseId, NamespaceId};
use surrealdb_expr::expr::dir::Dir;
use surrealdb_expr::val::{RecordIdKey, Value};
use surrealdb_kvs::key::Error as KeyError;
use surrealdb_kvs::value::KVValue;
use surrealdb_strand::TableName;

/// One (vertex, direction) adjacency scope named for folding: the unit the
/// fold queue, the read-path observations, and the fold engine all work in.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct GraphFoldScope {
	pub ns: NamespaceId,
	pub db: DatabaseId,
	/// The vertex's table.
	pub tb: TableName,
	/// The vertex's record key.
	pub id: RecordIdKey,
	pub dir: Dir,
}

/// Flag bit marking a pointer key as a tombstone: the edge it names was
/// deleted after being folded into a packed block, and readers must
/// subtract it from the block's entries until the next fold consumes it.
///
/// Bit 0 is the only section-less flag; every other bit gates one
/// length-prefixed section in the value body (see [`AdjacencyValue`]).
pub const ADJACENCY_FLAG_TOMBSTONE: u8 = 1 << 0;

/// Flag bit reserved for the inline edge-property payload section.
pub const ADJACENCY_FLAG_PROPS: u8 = 1 << 1;

/// Flag bit reserved for the target doc-id section.
pub const ADJACENCY_FLAG_TARGET_DOC_ID: u8 = 1 << 2;

/// Props-section header bit: the edge's inline values outgrew the cap and
/// were not written; readers must fall back to the record.
pub const INLINE_PROPS_SPILLED: u8 = 1 << 0;

/// The inline edge-property payload — the body of the
/// [`ADJACENCY_FLAG_PROPS`] section of an [`AdjacencyValue`], and of a
/// block entry's props segment (the fold copies section bodies verbatim).
///
/// # Encoding
///
/// One header byte (bit 0 [`INLINE_PROPS_SPILLED`]; other bits reserved and
/// rejected), the writing table's inline-field **generation** as a
/// little-endian `u32`, and — unless spilled — a value count byte followed
/// by that many revision-encoded [`Value`]s, one per inline field **in the
/// field-name order the generation defines** (`Value::None` for a field
/// the edge does not carry).
///
/// The generation is what makes a payload trustworthy: it identifies the
/// exact inline field set (names, order, and types) the values were
/// written under, and every schema mutation that changes that set bumps
/// it. A reader whose plan-time generation or field count differs treats
/// the payload as stale and falls back to the record — never evaluating
/// values against a different set than wrote them.
#[derive(Clone, Debug, PartialEq)]
pub struct InlineProps {
	/// The writing table's inline-field generation.
	pub generation: u32,
	/// The inline values in generation-defined field order, or `None` when
	/// the payload spilled past the cap.
	pub values: Option<Vec<Value>>,
}

impl InlineProps {
	/// A spilled payload: the generation is still recorded, so a reader can
	/// tell "current but too large" from "written under an older set".
	pub fn spilled(generation: u32) -> Self {
		Self {
			generation,
			values: None,
		}
	}

	pub fn is_spilled(&self) -> bool {
		self.values.is_none()
	}

	/// Encodes the section body.
	pub fn encode(&self) -> Result<Vec<u8>> {
		let mut buf = Vec::new();
		match &self.values {
			None => {
				buf.push(INLINE_PROPS_SPILLED);
				buf.extend_from_slice(&self.generation.to_le_bytes());
			}
			Some(values) => {
				ensure!(
					values.len() <= u8::MAX as usize,
					"an inline props payload cannot carry more than 255 values"
				);
				buf.push(0);
				buf.extend_from_slice(&self.generation.to_le_bytes());
				buf.push(values.len() as u8);
				for value in values {
					revision::SerializeRevisioned::serialize_revisioned(value, &mut buf)?;
				}
			}
		}
		Ok(buf)
	}

	/// Encodes a live section body for `count` values directly from
	/// borrowed values, abandoning the serialisation as soon as the
	/// encoded values would exceed `cap` bytes.
	///
	/// Returns `None` on overflow — the caller writes
	/// [`InlineProps::spilled`] instead — so the work spent on an
	/// over-cap payload is bounded by roughly `cap` bytes rather than
	/// the values' full encoded size. The boundary matches
	/// [`InlineProps::encode`] exactly: values whose encoding totals
	/// `cap` bytes still fit; one byte more spills. A completed
	/// encoding is byte-identical to [`InlineProps::encode`] over the
	/// same values.
	pub fn encode_values_capped<'a>(
		generation: u32,
		values: impl IntoIterator<Item = &'a Value>,
		count: u8,
		cap: usize,
	) -> Result<Option<Vec<u8>>> {
		// Header byte, generation, count: the fixed prelude the cap does
		// not bound.
		const PRELUDE: usize = 6;
		let mut writer = CappedWriter::new(cap.saturating_add(PRELUDE));
		writer.buf.push(0);
		writer.buf.extend_from_slice(&generation.to_le_bytes());
		writer.buf.push(count);
		for value in values {
			match revision::SerializeRevisioned::serialize_revisioned(value, &mut writer) {
				Ok(()) => {}
				Err(_) if writer.overflowed => return Ok(None),
				Err(e) => return Err(e.into()),
			}
		}
		Ok(Some(writer.buf))
	}

	/// Decodes a section body.
	pub fn decode(mut body: &[u8]) -> Result<Self> {
		let corrupted = || KeyError::Corrupted("an inline props payload is malformed");
		let (&header, rest) = body.split_first().ok_or_else(corrupted)?;
		body = rest;
		ensure!(header & !INLINE_PROPS_SPILLED == 0, corrupted());
		let (gen_bytes, rest) = body.split_at_checked(4).ok_or_else(corrupted)?;
		body = rest;
		let generation = u32::from_le_bytes(gen_bytes.try_into().expect("split at four"));
		if header & INLINE_PROPS_SPILLED != 0 {
			ensure!(body.is_empty(), corrupted());
			return Ok(Self::spilled(generation));
		}
		let (&count, rest) = body.split_first().ok_or_else(corrupted)?;
		let mut reader = rest;
		let mut values = Vec::with_capacity(count as usize);
		for _ in 0..count {
			values.push(revision::DeserializeRevisioned::deserialize_revisioned(&mut reader)?);
		}
		ensure!(reader.is_empty(), corrupted());
		Ok(Self {
			generation,
			values: Some(values),
		})
	}
}

/// A `Vec`-backed writer that refuses any write growing the buffer past
/// `max` bytes, flagging the refusal so a caller can tell a cap trip
/// from a genuine serialisation error. A refused write consumes nothing,
/// so the buffer never holds more than `max` bytes.
struct CappedWriter {
	buf: Vec<u8>,
	max: usize,
	overflowed: bool,
}

impl CappedWriter {
	fn new(max: usize) -> Self {
		Self {
			buf: Vec::new(),
			max,
			overflowed: false,
		}
	}
}

impl std::io::Write for CappedWriter {
	fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
		if data.len() > self.max.saturating_sub(self.buf.len()) {
			self.overflowed = true;
			return Err(std::io::ErrorKind::WriteZero.into());
		}
		self.buf.extend_from_slice(data);
		Ok(data.len())
	}

	fn flush(&mut self) -> std::io::Result<()> {
		Ok(())
	}
}

/// The value of a vertex-side graph pointer key.
///
/// # Encoding
///
/// A live pointer with no extra sections encodes to **empty bytes** — the
/// exact bytes every pointer key has carried since the format's
/// introduction, so plain writes and pre-existing data are
/// indistinguishable.
///
/// A non-empty value is one flags byte followed by the flagged sections:
///
/// - bit 0 ([`ADJACENCY_FLAG_TOMBSTONE`]) is section-less — it flags the value itself as a
///   tombstone;
/// - every set bit above 0 contributes one section, in ascending bit order, each encoded as an
///   unsigned-LEB128 byte length followed by the section body.
///
/// Decoders skip the body of any section whose bit they do not
/// interpret, and keep it verbatim so a rewrite preserves it; they error
/// only on malformed lengths. That is the forward-compatibility
/// contract: a build that knows only bit 0 reads — and safely rewrites —
/// values written by a build that also fills bit 1 or 2.
///
/// Current allocation: bit 0 tombstone; bit 1 inline edge properties;
/// bit 2 target doc-id. Writers in this build set bit 0 only, and never
/// combine it with sections; interpreters of an individual section
/// enforce their own validity rules on top of this container.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AdjacencyValue {
	/// Whether this pointer marks a folded edge as deleted.
	pub tombstone: bool,
	/// The flag-gated sections, in ascending bit order, carried verbatim
	/// whether or not this build interprets them.
	pub sections: Vec<AdjacencySection>,
}

/// One flag-gated section of an [`AdjacencyValue`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdjacencySection {
	/// The flag bit (1–7) that gates this section.
	pub flag_bit: u8,
	/// The section body, opaque to builds that do not interpret the bit.
	pub body: Vec<u8>,
}

impl AdjacencyValue {
	/// A live pointer with no sections — encodes to empty bytes.
	pub fn live() -> Self {
		Self::default()
	}

	/// A tombstone over a folded edge.
	pub fn tombstone() -> Self {
		Self {
			tombstone: true,
			sections: Vec::new(),
		}
	}

	/// Whether this value marks its edge as deleted.
	pub fn is_tombstone(&self) -> bool {
		self.tombstone
	}

	/// The body of the section gated by `flag_bit`, if present.
	pub fn section(&self, flag_bit: u8) -> Option<&[u8]> {
		self.sections.iter().find(|s| s.flag_bit == flag_bit).map(|s| s.body.as_slice())
	}
}

impl KVValue for AdjacencyValue {
	type KeyContext = ();

	fn kv_encode_value(&self) -> Result<Vec<u8>> {
		if !self.tombstone && self.sections.is_empty() {
			return Ok(Vec::new());
		}
		let mut flags = if self.tombstone {
			ADJACENCY_FLAG_TOMBSTONE
		} else {
			0
		};
		let mut previous_bit = 0u8;
		for section in &self.sections {
			ensure!(
				(1..8).contains(&section.flag_bit),
				"adjacency section flag bit {} is out of range",
				section.flag_bit
			);
			ensure!(
				section.flag_bit > previous_bit,
				"adjacency sections must be unique and in ascending bit order"
			);
			previous_bit = section.flag_bit;
			flags |= 1 << section.flag_bit;
		}
		let mut buf =
			Vec::with_capacity(1 + self.sections.iter().map(|s| s.body.len() + 2).sum::<usize>());
		buf.push(flags);
		for section in &self.sections {
			write_uvarint(&mut buf, section.body.len() as u64);
			buf.extend_from_slice(&section.body);
		}
		Ok(buf)
	}

	fn kv_decode_value(bytes: &[u8], _: ()) -> Result<Self> {
		let Some((&flags, mut rest)) = bytes.split_first() else {
			return Ok(Self::live());
		};
		let tombstone = flags & ADJACENCY_FLAG_TOMBSTONE != 0;
		let mut sections = Vec::new();
		for flag_bit in 1..8u8 {
			if flags & (1 << flag_bit) == 0 {
				continue;
			}
			let len = read_uvarint(&mut rest)
				.ok_or(KeyError::Corrupted("adjacency section length is malformed"))?;
			let len = usize::try_from(len)
				.map_err(|_| KeyError::Corrupted("adjacency section length is malformed"))?;
			ensure!(
				rest.len() >= len,
				KeyError::Corrupted("adjacency section is shorter than its declared length")
			);
			let (body, tail) = rest.split_at(len);
			sections.push(AdjacencySection {
				flag_bit,
				body: body.to_vec(),
			});
			rest = tail;
		}
		ensure!(
			rest.is_empty(),
			KeyError::Corrupted("adjacency value carries bytes beyond its flagged sections")
		);
		Ok(Self {
			tombstone,
			sections,
		})
	}
}

/// Block header flag: entries are doc-id encoded rather than carrying the
/// storekey bytes of their record ids.
pub const BLOCK_FLAG_NUMERIC: u8 = 1 << 0;

/// Block header flag: each entry carries an inline-property segment.
pub const BLOCK_FLAG_PROPS: u8 = 1 << 1;

const BLOCK_KNOWN_FLAGS: u8 = BLOCK_FLAG_NUMERIC | BLOCK_FLAG_PROPS;

/// One packed adjacency chunk: a sorted run of edges for a single
/// (vertex, direction, edge-table) scope.
///
/// Entries carry the same information as the pointer keys they were
/// folded from — the edge's own record key and the far vertex — so every
/// read that today's pointer keys serve can be served from a block, in
/// one of two encodings:
///
/// * **Plain** entries hold the storekey bytes of their record ids, so comparisons against pointer
///   keys during the block/delta merge, and against the chunk-routing bounds, are exact byte
///   comparisons.
/// * **Numeric** entries hold each record's table-level doc-ID (the shared space every index
///   consumes), delta-compressed, and a reader resolves them back through the `!dd` mapping. The
///   fold only emits this encoding when every involved table is defined and numeric compression is
///   enabled; an id that no longer resolves marks a deleted record, and readers skip it.
///
/// Entries are sorted by, and unique on, their full identity — edge key,
/// then target table, then target key — the same order the pointer keys
/// sort under their `(vertex, dir, edge_table)` prefix, so a block scan
/// emits edges in exactly the order a key scan would. An edge key alone
/// is almost always unique too, but a deleted edge re-related to a new
/// target can transiently pair one edge key with two targets across the
/// block and delta layers, so the identity the format enforces is the
/// full one. Numeric entries keep that order positionally: doc-IDs do
/// not sort like the keys they map, so the writer's ordering is the
/// contract and the codec preserves it byte for byte.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdjacencyBlock {
	/// The target-table dictionary entries reference by index. Targets
	/// concentrate on few tables, so the name is stored once per table
	/// rather than once per edge.
	pub targets: Vec<TableName>,
	/// The edges, in pointer-key order.
	pub entries: BlockEntries,
}

impl Default for AdjacencyBlock {
	fn default() -> Self {
		Self {
			targets: Vec::new(),
			entries: BlockEntries::Plain(Vec::new()),
		}
	}
}

impl AdjacencyBlock {
	/// How many edges the chunk holds.
	pub fn len(&self) -> usize {
		match &self.entries {
			BlockEntries::Plain(entries) => entries.len(),
			BlockEntries::Numeric(entries) => entries.len(),
		}
	}

	/// The header of an encoded chunk — the flags byte and the entry count —
	/// without decoding the dictionary or the entries. Applies the same
	/// validation as [`KVValue::kv_decode_value`]: an unknown header flag is
	/// a build too old for the data, a loud error.
	pub fn kv_decode_header(bytes: &[u8]) -> Result<(u8, u64)> {
		let Some((&flags, mut rest)) = bytes.split_first() else {
			bail!(KeyError::Corrupted("adjacency block is empty"));
		};
		ensure!(
			flags & !BLOCK_KNOWN_FLAGS == 0,
			KeyError::Corrupted("adjacency block header carries unknown flags")
		);
		let count =
			read_uvarint(&mut rest).ok_or(KeyError::Corrupted("adjacency block is truncated"))?;
		Ok((flags, count))
	}

	/// The entry count alone, read from the leading header bytes of an
	/// encoded chunk — degree counting sums these without decoding the
	/// dictionary or the entries.
	pub fn kv_decode_len(bytes: &[u8]) -> Result<u64> {
		Ok(Self::kv_decode_header(bytes)?.1)
	}

	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}
}

/// The chunk's entries, in one of the two encodings.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlockEntries {
	Plain(Vec<BlockEntry>),
	Numeric(Vec<NumericEntry>),
}

/// One edge held in a plain-encoded [`AdjacencyBlock`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockEntry {
	/// The storekey bytes of the edge record's `RecordIdKey`.
	pub edge_key: Vec<u8>,
	/// Index into the block's target-table dictionary.
	pub target_table: u8,
	/// The storekey bytes of the target vertex's `RecordIdKey`.
	pub target_key: Vec<u8>,
	/// The edge's inline-property section body, carried verbatim from
	/// the pointer value it was folded from.
	pub props: Option<Vec<u8>>,
}

/// One edge held in a numeric-encoded [`AdjacencyBlock`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NumericEntry {
	/// The edge record's doc-ID in its table's shared space.
	pub edge: u64,
	/// Index into the block's target-table dictionary.
	pub target_table: u8,
	/// The target record's doc-ID in its table's shared space.
	pub target: u64,
	/// The edge's inline-property section body, carried verbatim.
	pub props: Option<Vec<u8>>,
}

impl KVValue for AdjacencyBlock {
	type KeyContext = ();

	fn kv_encode_value(&self) -> Result<Vec<u8>> {
		// The dictionary length is stored in one byte, so 255 entries is the
		// hard ceiling — the writer splits a chunk before exceeding it.
		ensure!(
			self.targets.len() <= u8::MAX as usize,
			"adjacency block target dictionary exceeds {} tables",
			u8::MAX
		);
		let (numeric, props) = match &self.entries {
			BlockEntries::Plain(entries) => (false, entries.iter().any(|e| e.props.is_some())),
			BlockEntries::Numeric(entries) => (true, entries.iter().any(|e| e.props.is_some())),
		};
		let mut flags = 0;
		if numeric {
			flags |= BLOCK_FLAG_NUMERIC;
		}
		if props {
			flags |= BLOCK_FLAG_PROPS;
		}
		let mut buf = Vec::new();
		buf.push(flags);
		write_uvarint(&mut buf, self.len() as u64);
		buf.push(self.targets.len() as u8);
		for target in &self.targets {
			let name = target.as_str().as_bytes();
			write_uvarint(&mut buf, name.len() as u64);
			buf.extend_from_slice(name);
		}
		match &self.entries {
			BlockEntries::Plain(entries) => {
				let mut previous: Option<(&[u8], &TableName, &[u8])> = None;
				for entry in entries {
					let target_name = self.targets.get(entry.target_table as usize);
					let Some(target_name) = target_name else {
						bail!("adjacency block entry names a target table outside the dictionary");
					};
					let identity =
						(entry.edge_key.as_slice(), target_name, entry.target_key.as_slice());
					ensure!(
						previous.is_none_or(|p| p < identity),
						"adjacency block entries must be unique and sorted by identity"
					);
					previous = Some(identity);
					write_uvarint(&mut buf, entry.edge_key.len() as u64);
					buf.extend_from_slice(&entry.edge_key);
					buf.push(entry.target_table);
					write_uvarint(&mut buf, entry.target_key.len() as u64);
					buf.extend_from_slice(&entry.target_key);
					if props {
						match &entry.props {
							Some(body) => {
								write_uvarint(&mut buf, body.len() as u64);
								buf.extend_from_slice(body);
							}
							None => write_uvarint(&mut buf, 0),
						}
					}
				}
			}
			BlockEntries::Numeric(entries) => {
				// The edge ids first, delta-compressed. Entries sit in
				// pointer-key order, which doc-IDs do not follow, so the
				// deltas are signed (zigzag). The remaining per-entry fields
				// follow positionally.
				let mut previous: u64 = 0;
				for (i, entry) in entries.iter().enumerate() {
					if i == 0 {
						write_uvarint(&mut buf, entry.edge);
					} else {
						write_ivarint(&mut buf, entry.edge.wrapping_sub(previous) as i64);
					}
					previous = entry.edge;
				}
				for entry in entries {
					ensure!(
						(entry.target_table as usize) < self.targets.len(),
						"adjacency block entry names a target table outside the dictionary"
					);
					buf.push(entry.target_table);
					write_uvarint(&mut buf, entry.target);
					if props {
						match &entry.props {
							Some(body) => {
								write_uvarint(&mut buf, body.len() as u64);
								buf.extend_from_slice(body);
							}
							None => write_uvarint(&mut buf, 0),
						}
					}
				}
			}
		}
		Ok(buf)
	}

	fn kv_decode_value(bytes: &[u8], _: ()) -> Result<Self> {
		let Some((&flags, mut rest)) = bytes.split_first() else {
			bail!(KeyError::Corrupted("adjacency block is empty"));
		};
		// Blocks are written and read only by folding-aware builds, so an
		// unknown header flag is a build too old for the data — a loud
		// error, unlike the skippable pointer-value sections.
		ensure!(
			flags & !BLOCK_KNOWN_FLAGS == 0,
			KeyError::Corrupted("adjacency block header carries unknown flags")
		);
		let numeric = flags & BLOCK_FLAG_NUMERIC != 0;
		let props = flags & BLOCK_FLAG_PROPS != 0;
		let corrupted = || KeyError::Corrupted("adjacency block is truncated");
		let count = read_uvarint(&mut rest).ok_or_else(corrupted)?;
		let count = usize::try_from(count).map_err(|_| corrupted())?;
		let (&dict_len, tail) = rest.split_first().ok_or_else(corrupted)?;
		rest = tail;
		let mut targets = Vec::with_capacity(dict_len as usize);
		for _ in 0..dict_len {
			let name = read_segment(&mut rest).ok_or_else(corrupted)?;
			let name = std::str::from_utf8(name)
				.map_err(|_| KeyError::Corrupted("adjacency block table name is not utf-8"))?;
			targets.push(TableName::from(name));
		}
		let entries = if numeric {
			let mut edges = Vec::with_capacity(count.min(4096));
			let mut previous: u64 = 0;
			for i in 0..count {
				let edge = if i == 0 {
					read_uvarint(&mut rest).ok_or_else(corrupted)?
				} else {
					previous.wrapping_add(read_ivarint(&mut rest).ok_or_else(corrupted)? as u64)
				};
				previous = edge;
				edges.push(edge);
			}
			let mut entries = Vec::with_capacity(count.min(4096));
			for edge in edges {
				let (&target_table, tail) = rest.split_first().ok_or_else(corrupted)?;
				rest = tail;
				ensure!(
					(target_table as usize) < targets.len(),
					KeyError::Corrupted(
						"adjacency block entry names a target table outside the dictionary"
					)
				);
				let target = read_uvarint(&mut rest).ok_or_else(corrupted)?;
				let props = if props {
					let body = read_segment(&mut rest).ok_or_else(corrupted)?;
					if body.is_empty() {
						None
					} else {
						Some(body.to_vec())
					}
				} else {
					None
				};
				entries.push(NumericEntry {
					edge,
					target_table,
					target,
					props,
				});
			}
			BlockEntries::Numeric(entries)
		} else {
			let mut entries = Vec::with_capacity(count.min(4096));
			for _ in 0..count {
				let edge_key = read_segment(&mut rest).ok_or_else(corrupted)?.to_vec();
				let (&target_table, tail) = rest.split_first().ok_or_else(corrupted)?;
				rest = tail;
				ensure!(
					(target_table as usize) < targets.len(),
					KeyError::Corrupted(
						"adjacency block entry names a target table outside the dictionary"
					)
				);
				let target_key = read_segment(&mut rest).ok_or_else(corrupted)?.to_vec();
				let props = if props {
					let body = read_segment(&mut rest).ok_or_else(corrupted)?;
					if body.is_empty() {
						None
					} else {
						Some(body.to_vec())
					}
				} else {
					None
				};
				entries.push(BlockEntry {
					edge_key,
					target_table,
					target_key,
					props,
				});
			}
			BlockEntries::Plain(entries)
		};
		ensure!(
			rest.is_empty(),
			KeyError::Corrupted("adjacency block carries bytes beyond its entries")
		);
		Ok(Self {
			targets,
			entries,
		})
	}
}

/// Appends `value` as unsigned LEB128.
fn write_uvarint(buf: &mut Vec<u8>, mut value: u64) {
	loop {
		let byte = (value & 0x7f) as u8;
		value >>= 7;
		if value == 0 {
			buf.push(byte);
			return;
		}
		buf.push(byte | 0x80);
	}
}

/// Appends `value` as zigzag-then-unsigned LEB128, for signed deltas.
fn write_ivarint(buf: &mut Vec<u8>, value: i64) {
	write_uvarint(buf, ((value << 1) ^ (value >> 63)) as u64);
}

/// The byte width [`write_uvarint`] encodes `value` in, so a writer can
/// size an encoding without producing it.
pub fn uvarint_len(value: u64) -> usize {
	(64 - (value | 1).leading_zeros() as usize).div_ceil(7)
}

/// The byte width [`write_ivarint`] encodes `value` in.
pub fn ivarint_len(value: i64) -> usize {
	uvarint_len(((value << 1) ^ (value >> 63)) as u64)
}

/// Reads an unsigned LEB128 value, advancing `bytes` past it. Returns
/// `None` when the input ends mid-varint or the value overflows a `u64`.
fn read_uvarint(bytes: &mut &[u8]) -> Option<u64> {
	let mut value = 0u64;
	let mut shift = 0u32;
	loop {
		let (&byte, rest) = bytes.split_first()?;
		*bytes = rest;
		if shift == 63 && byte > 1 {
			return None;
		}
		value |= u64::from(byte & 0x7f) << shift;
		if byte & 0x80 == 0 {
			return Some(value);
		}
		shift += 7;
		if shift > 63 {
			return None;
		}
	}
}

/// Reads a zigzag-encoded signed value, advancing `bytes` past it.
fn read_ivarint(bytes: &mut &[u8]) -> Option<i64> {
	let raw = read_uvarint(bytes)?;
	Some(((raw >> 1) as i64) ^ -((raw & 1) as i64))
}

/// Reads a length-prefixed byte segment, advancing `bytes` past it.
fn read_segment<'a>(bytes: &mut &'a [u8]) -> Option<&'a [u8]> {
	let len = read_uvarint(bytes)?;
	let len = usize::try_from(len).ok()?;
	if bytes.len() < len {
		return None;
	}
	let (segment, rest) = bytes.split_at(len);
	*bytes = rest;
	Some(segment)
}

#[cfg(test)]
mod tests {
	use super::*;

	/// A live pointer with no sections must encode to empty bytes — the
	/// exact value every pointer key has always carried — and empty bytes
	/// must decode back to it.
	#[test]
	fn live_is_byte_identical_to_the_legacy_empty_value() {
		assert_eq!(AdjacencyValue::live().kv_encode_value().unwrap(), Vec::<u8>::new());
		assert_eq!(AdjacencyValue::kv_decode_value(&[], ()).unwrap(), AdjacencyValue::live());
	}

	#[test]
	fn tombstone_round_trips() {
		let value = AdjacencyValue::tombstone();
		let bytes = value.kv_encode_value().unwrap();
		assert_eq!(bytes, vec![ADJACENCY_FLAG_TOMBSTONE]);
		assert_eq!(AdjacencyValue::kv_decode_value(&bytes, ()).unwrap(), value);
	}

	#[test]
	fn sections_round_trip_and_stay_verbatim() {
		let value = AdjacencyValue {
			tombstone: false,
			sections: vec![
				AdjacencySection {
					flag_bit: 1,
					body: vec![9, 8, 7],
				},
				AdjacencySection {
					flag_bit: 2,
					body: Vec::new(),
				},
			],
		};
		let bytes = value.kv_encode_value().unwrap();
		let decoded = AdjacencyValue::kv_decode_value(&bytes, ()).unwrap();
		assert_eq!(decoded, value);
		assert_eq!(decoded.section(1), Some([9, 8, 7].as_slice()));
		assert_eq!(decoded.section(2), Some([].as_slice()));
		assert_eq!(decoded.section(3), None);
	}

	/// A decoder that does not interpret a section must still read past
	/// it: an unknown flag bit is data from a newer build, not corruption.
	#[test]
	fn unknown_section_bits_are_skipped_not_rejected() {
		// Flags declare bit 7, which no build interprets yet.
		let bytes = vec![1 << 7, 2, 0xaa, 0xbb];
		let decoded = AdjacencyValue::kv_decode_value(&bytes, ()).unwrap();
		assert_eq!(decoded.section(7), Some([0xaa, 0xbb].as_slice()));
		// Re-encoding preserves the uninterpreted section byte-for-byte.
		assert_eq!(decoded.kv_encode_value().unwrap(), bytes);
	}

	#[test]
	fn malformed_section_lengths_are_rejected() {
		// Section length says 200 bytes; only two follow.
		assert!(AdjacencyValue::kv_decode_value(&[1 << 1, 200, 0xaa, 0xbb], ()).is_err());
		// Truncated mid-varint.
		assert!(AdjacencyValue::kv_decode_value(&[1 << 1, 0x80], ()).is_err());
		// Bytes beyond the flagged sections.
		assert!(AdjacencyValue::kv_decode_value(&[1 << 1, 1, 0xaa, 0xbb], ()).is_err());
	}

	#[test]
	fn sections_must_be_unique_and_ascending_on_encode() {
		let out_of_order = AdjacencyValue {
			tombstone: false,
			sections: vec![
				AdjacencySection {
					flag_bit: 2,
					body: Vec::new(),
				},
				AdjacencySection {
					flag_bit: 1,
					body: Vec::new(),
				},
			],
		};
		assert!(out_of_order.kv_encode_value().is_err());
	}

	fn entry(edge: &[u8], table: u8, target: &[u8]) -> BlockEntry {
		BlockEntry {
			edge_key: edge.to_vec(),
			target_table: table,
			target_key: target.to_vec(),
			props: None,
		}
	}

	#[test]
	fn block_round_trips() {
		let block = AdjacencyBlock {
			targets: vec![TableName::from("person"), TableName::from("org")],
			entries: BlockEntries::Plain(vec![
				entry(b"\x03a\x00", 0, b"\x03x\x00"),
				entry(b"\x03b\x00", 1, b"\x03y\x00"),
			]),
		};
		let bytes = block.kv_encode_value().unwrap();
		assert_eq!(AdjacencyBlock::kv_decode_value(&bytes, ()).unwrap(), block);
	}

	#[test]
	fn block_props_round_trip() {
		let mut with_props = entry(b"\x03a\x00", 0, b"\x03x\x00");
		with_props.props = Some(vec![1, 2, 3]);
		let block = AdjacencyBlock {
			targets: vec![TableName::from("person")],
			entries: BlockEntries::Plain(vec![with_props, entry(b"\x03b\x00", 0, b"\x03y\x00")]),
		};
		let bytes = block.kv_encode_value().unwrap();
		assert_eq!(AdjacencyBlock::kv_decode_value(&bytes, ()).unwrap(), block);
	}

	#[test]
	fn block_rejects_unsorted_entries_and_bad_dictionary_indexes() {
		let unsorted = AdjacencyBlock {
			targets: vec![TableName::from("person")],
			entries: BlockEntries::Plain(vec![
				entry(b"\x03b\x00", 0, b"\x03y\x00"),
				entry(b"\x03a\x00", 0, b"\x03x\x00"),
			]),
		};
		assert!(unsorted.kv_encode_value().is_err());
		let out_of_dict = AdjacencyBlock {
			targets: vec![TableName::from("person")],
			entries: BlockEntries::Plain(vec![entry(b"\x03a\x00", 1, b"\x03x\x00")]),
		};
		assert!(out_of_dict.kv_encode_value().is_err());
	}

	#[test]
	fn block_rejects_unknown_flags_and_truncation() {
		// Unknown header flag.
		assert!(AdjacencyBlock::kv_decode_value(&[1 << 5, 0, 0], ()).is_err());
		// Truncated mid-entry.
		let block = AdjacencyBlock {
			targets: vec![TableName::from("person")],
			entries: BlockEntries::Plain(vec![entry(b"\x03a\x00", 0, b"\x03x\x00")]),
		};
		let bytes = block.kv_encode_value().unwrap();
		assert!(AdjacencyBlock::kv_decode_value(&bytes[..bytes.len() - 1], ()).is_err());
		// Trailing garbage.
		let mut padded = bytes;
		padded.push(0);
		assert!(AdjacencyBlock::kv_decode_value(&padded, ()).is_err());
	}

	/// The header-only reads agree with the full decode — the entry count
	/// matches for both encodings — and reject an unknown header flag
	/// exactly as `kv_decode_value` does.
	#[test]
	fn block_header_reads_match_the_full_decode_and_validate_flags() {
		let plain = AdjacencyBlock {
			targets: vec![TableName::from("person")],
			entries: BlockEntries::Plain(vec![
				entry(b"\x03a\x00", 0, b"\x03x\x00"),
				entry(b"\x03b\x00", 0, b"\x03y\x00"),
			]),
		};
		let numeric = AdjacencyBlock {
			targets: vec![TableName::from("person")],
			entries: BlockEntries::Numeric(vec![
				NumericEntry {
					edge: 42,
					target_table: 0,
					target: 7,
					props: Some(vec![1, 2]),
				},
				NumericEntry {
					edge: 3,
					target_table: 0,
					target: 900,
					props: None,
				},
				NumericEntry {
					edge: 8,
					target_table: 0,
					target: 1,
					props: None,
				},
			]),
		};
		for (block, numeric_flag) in [(plain, false), (numeric, true)] {
			let bytes = block.kv_encode_value().unwrap();
			assert_eq!(AdjacencyBlock::kv_decode_len(&bytes).unwrap(), block.len() as u64);
			let (flags, count) = AdjacencyBlock::kv_decode_header(&bytes).unwrap();
			assert_eq!(count, block.len() as u64);
			assert_eq!(flags & BLOCK_FLAG_NUMERIC != 0, numeric_flag);
			// An unknown header flag must be rejected, not counted.
			let mut unknown = bytes;
			unknown[0] |= 1 << 7;
			assert!(AdjacencyBlock::kv_decode_len(&unknown).is_err());
			assert!(AdjacencyBlock::kv_decode_header(&unknown).is_err());
		}
		assert!(AdjacencyBlock::kv_decode_len(&[]).is_err());
	}

	/// Numeric entries keep positional order and survive non-monotonic,
	/// wrapping deltas — doc-IDs do not sort like the keys they map.
	#[test]
	fn numeric_block_round_trips() {
		let block = AdjacencyBlock {
			targets: vec![TableName::from("person")],
			entries: BlockEntries::Numeric(vec![
				NumericEntry {
					edge: 42,
					target_table: 0,
					target: 7,
					props: None,
				},
				NumericEntry {
					edge: 3,
					target_table: 0,
					target: 900,
					props: Some(vec![1, 2]),
				},
				NumericEntry {
					edge: u64::MAX,
					target_table: 0,
					target: 0,
					props: None,
				},
			]),
		};
		let bytes = block.kv_encode_value().unwrap();
		assert_eq!(AdjacencyBlock::kv_decode_value(&bytes, ()).unwrap(), block);
	}

	/// The width helpers must agree byte-for-byte with the encoders they
	/// mirror, across every varint length class.
	#[test]
	fn varint_widths_match_the_encoders() {
		for value in [0u64, 1, 127, 128, 300, 16383, 16384, u32::MAX as u64, u64::MAX] {
			let mut buf = Vec::new();
			write_uvarint(&mut buf, value);
			assert_eq!(uvarint_len(value), buf.len(), "uvarint width for {value}");
		}
		for value in [0i64, 1, -1, 63, 64, -64, -65, 1 << 20, -(1 << 20), i64::MAX, i64::MIN] {
			let mut buf = Vec::new();
			write_ivarint(&mut buf, value);
			assert_eq!(ivarint_len(value), buf.len(), "ivarint width for {value}");
		}
	}

	#[test]
	fn uvarint_round_trips() {
		for value in [0u64, 1, 127, 128, 300, u32::MAX as u64, u64::MAX] {
			let mut buf = Vec::new();
			write_uvarint(&mut buf, value);
			let mut slice = buf.as_slice();
			assert_eq!(read_uvarint(&mut slice), Some(value));
			assert!(slice.is_empty());
		}
		// Overflowing a u64 must be rejected, not wrapped.
		let mut overlong = &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02][..];
		assert_eq!(read_uvarint(&mut overlong), None);
	}

	/// The dictionary count is one byte: 255 tables round-trip, and the
	/// encoder refuses 256 rather than truncating the count to zero and
	/// committing an undecodable chunk.
	#[test]
	fn block_dictionary_is_bounded_by_its_count_byte() {
		let entry = |dict: u8| BlockEntry {
			edge_key: vec![dict],
			target_table: dict,
			target_key: vec![1],
			props: None,
		};
		let block = |tables: usize| AdjacencyBlock {
			targets: (0..tables).map(|n| TableName::from(format!("t{n:03}"))).collect(),
			entries: BlockEntries::Plain(vec![entry(0)]),
		};
		let ok = block(255);
		let bytes = ok.kv_encode_value().unwrap();
		assert_eq!(AdjacencyBlock::kv_decode_value(&bytes, ()).unwrap(), ok);
		assert!(block(256).kv_encode_value().is_err());
	}

	/// The inline-props payload round-trips through its section body, in
	/// both live and spilled forms, and rejects what it cannot interpret.
	#[test]
	fn inline_props_round_trip() {
		for props in [
			InlineProps {
				generation: 0,
				values: Some(Vec::new()),
			},
			InlineProps {
				generation: 7,
				values: Some(vec![
					Value::from(42),
					Value::None,
					Value::from("text"),
					Value::Bool(true),
				]),
			},
			InlineProps::spilled(u32::MAX),
		] {
			let body = props.encode().unwrap();
			assert_eq!(InlineProps::decode(&body).unwrap(), props);
		}
		// Unknown header bits are rejected: they would change the layout.
		assert!(InlineProps::decode(&[0x02, 0, 0, 0, 0, 0]).is_err());
		// A spilled payload carries no values.
		assert!(InlineProps::decode(&[0x01, 0, 0, 0, 0, 9]).is_err());
		// A truncated value list is rejected.
		let body = InlineProps {
			generation: 1,
			values: Some(vec![Value::from(1)]),
		}
		.encode()
		.unwrap();
		assert!(InlineProps::decode(&body[..body.len() - 1]).is_err());
		// Trailing bytes beyond the declared count are rejected.
		let mut long = body;
		long.push(0);
		assert!(InlineProps::decode(&long).is_err());
	}

	/// The capped encoder is byte-identical to `encode` while the values
	/// fit, honours the exact spill boundary (`cap` bytes of encoded
	/// values still fit; one fewer spills), and reports overflow as
	/// `None` rather than an error.
	#[test]
	fn capped_encode_matches_encode_and_its_boundary() {
		let props = InlineProps {
			generation: 7,
			values: Some(vec![Value::from(42), Value::None, Value::from("text")]),
		};
		let full = props.encode().unwrap();
		let values = props.values.as_ref().unwrap();
		let capped = |cap: usize| {
			InlineProps::encode_values_capped(7, values.iter(), values.len() as u8, cap).unwrap()
		};
		assert_eq!(capped(64), Some(full.clone()));
		// The prelude (header, generation, count) is exempt from the cap.
		let exact = full.len() - 6;
		assert_eq!(capped(exact), Some(full));
		assert_eq!(capped(exact - 1), None);
	}

	/// The capped writer refuses — without buffering — any write that
	/// would grow it past its bound, so an over-cap value costs at most
	/// the cap, not its own encoded size.
	#[test]
	fn capped_writer_bounds_the_buffer() {
		use std::io::Write;
		let mut writer = CappedWriter::new(70);
		writer.write_all(&[0u8; 70]).unwrap();
		assert!(!writer.overflowed);
		assert!(writer.write_all(&[1u8]).is_err());
		assert!(writer.overflowed);
		assert_eq!(writer.buf.len(), 70);
		// A megabyte strand against a small cap is refused, not accumulated.
		let big = Value::from("x".repeat(1024 * 1024));
		assert_eq!(InlineProps::encode_values_capped(1, [&big], 1, 64).unwrap(), None);
	}

	/// A live pointer value carrying only a props section keeps the
	/// section container contract: skippable by builds that do not
	/// interpret bit 1, and preserved verbatim on rewrite.
	#[test]
	fn props_section_rides_the_container() {
		let body = InlineProps {
			generation: 3,
			values: Some(vec![Value::from(1)]),
		}
		.encode()
		.unwrap();
		let value = AdjacencyValue {
			tombstone: false,
			sections: vec![AdjacencySection {
				flag_bit: ADJACENCY_FLAG_PROPS.trailing_zeros() as u8,
				body: body.clone(),
			}],
		};
		let bytes = value.kv_encode_value().unwrap();
		let decoded = AdjacencyValue::kv_decode_value(&bytes, ()).unwrap();
		assert_eq!(decoded, value);
		assert_eq!(
			decoded.section(ADJACENCY_FLAG_PROPS.trailing_zeros() as u8),
			Some(body.as_slice())
		);
	}
}
