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
use surrealdb_expr::val::RecordIdKey;
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
/// read that today's pointer keys serve can be served from a block.
/// Record keys are held as their storekey-encoded bytes: comparisons
/// against pointer keys during the block/delta merge, and against the
/// chunk-routing bounds, are then exact byte comparisons.
///
/// Entries are sorted by, and unique on, their full identity — edge key,
/// then target table, then target key — the same order the pointer keys
/// sort under their `(vertex, dir, edge_table)` prefix, so a block scan
/// emits edges in exactly the order a key scan would. An edge key alone
/// is almost always unique too, but a deleted edge re-related to a new
/// target can transiently pair one edge key with two targets across the
/// block and delta layers, so the identity the format enforces is the
/// full one.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AdjacencyBlock {
	/// The target-table dictionary entries reference by index. Targets
	/// concentrate on few tables, so the name is stored once per table
	/// rather than once per edge.
	pub targets: Vec<TableName>,
	/// The edges, sorted by `edge_key` bytes.
	pub entries: Vec<BlockEntry>,
}

/// One edge held in an [`AdjacencyBlock`].
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
		let props = self.entries.iter().any(|e| e.props.is_some());
		let flags = if props {
			BLOCK_FLAG_PROPS
		} else {
			0
		};
		let mut buf = Vec::new();
		buf.push(flags);
		write_uvarint(&mut buf, self.entries.len() as u64);
		buf.push(self.targets.len() as u8);
		for target in &self.targets {
			let name = target.as_str().as_bytes();
			write_uvarint(&mut buf, name.len() as u64);
			buf.extend_from_slice(name);
		}
		let mut previous: Option<(&[u8], &TableName, &[u8])> = None;
		for entry in &self.entries {
			let target_name = self.targets.get(entry.target_table as usize);
			let Some(target_name) = target_name else {
				bail!("adjacency block entry names a target table outside the dictionary");
			};
			let identity = (entry.edge_key.as_slice(), target_name, entry.target_key.as_slice());
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
		ensure!(
			flags & BLOCK_FLAG_NUMERIC == 0,
			KeyError::Corrupted("numeric adjacency block encoding is not supported by this build")
		);
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
			entries: vec![
				entry(b"\x03a\x00", 0, b"\x03x\x00"),
				entry(b"\x03b\x00", 1, b"\x03y\x00"),
			],
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
			entries: vec![with_props, entry(b"\x03b\x00", 0, b"\x03y\x00")],
		};
		let bytes = block.kv_encode_value().unwrap();
		assert_eq!(AdjacencyBlock::kv_decode_value(&bytes, ()).unwrap(), block);
	}

	#[test]
	fn block_rejects_unsorted_entries_and_bad_dictionary_indexes() {
		let unsorted = AdjacencyBlock {
			targets: vec![TableName::from("person")],
			entries: vec![
				entry(b"\x03b\x00", 0, b"\x03y\x00"),
				entry(b"\x03a\x00", 0, b"\x03x\x00"),
			],
		};
		assert!(unsorted.kv_encode_value().is_err());
		let out_of_dict = AdjacencyBlock {
			targets: vec![TableName::from("person")],
			entries: vec![entry(b"\x03a\x00", 1, b"\x03x\x00")],
		};
		assert!(out_of_dict.kv_encode_value().is_err());
	}

	#[test]
	fn block_rejects_unknown_flags_and_truncation() {
		// Unknown header flag.
		assert!(AdjacencyBlock::kv_decode_value(&[1 << 5, 0, 0], ()).is_err());
		// Numeric encoding is a later format.
		assert!(AdjacencyBlock::kv_decode_value(&[BLOCK_FLAG_NUMERIC, 0, 0], ()).is_err());
		// Truncated mid-entry.
		let block = AdjacencyBlock {
			targets: vec![TableName::from("person")],
			entries: vec![entry(b"\x03a\x00", 0, b"\x03x\x00")],
		};
		let bytes = block.kv_encode_value().unwrap();
		assert!(AdjacencyBlock::kv_decode_value(&bytes[..bytes.len() - 1], ()).is_err());
		// Trailing garbage.
		let mut padded = bytes;
		padded.push(0);
		assert!(AdjacencyBlock::kv_decode_value(&padded, ()).is_err());
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
			entries: vec![entry(0)],
		};
		let ok = block(255);
		let bytes = ok.kv_encode_value().unwrap();
		assert_eq!(AdjacencyBlock::kv_decode_value(&bytes, ()).unwrap(), ok);
		assert!(block(256).kv_encode_value().is_err());
	}
}
