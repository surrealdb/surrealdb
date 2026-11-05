//! Record module for SurrealDB
//!
//! This module provides the `Record` type which represents a database record with metadata,
//! and the envelope its bytes are stored in. Records can contain both data and metadata about
//! the record type (e.g., whether it's an edge).
//!
//! # The stored record
//!
//! A record is stored in one of two encodings. The first byte tells them apart.
//!
//! * The **v1 record envelope** ([`StoredRecord`]) starts with `0xFF`. It holds the record's data
//!   in the stored-value grammar (`surrealdb_expr::val::stored`), which is read in place without
//!   decoding.
//! * The **`revision` encoding** is the derived encoding of [`Record`]. It starts with the struct
//!   revision, 1 or 2, never `0xFF`. Every release before the v1 envelope wrote it, and a table
//!   whose rows are written in [`RecordFormat::Revision`], or whose record the v1 envelope cannot
//!   hold, writes it still.
//!
//! ## The v1 envelope
//!
//! A v1 record is `FF`, a header byte, the generation and the body. [`StoredRecord`] holds the
//! normative byte-level layout: the header bits, the layouts, and what each refusal means.
//!
//! The generation is canonical LEB128: one byte below 128, never a final zero group after the
//! first byte, never more than 10 bytes, never more than 64 bits. A reader refuses any other
//! encoding rather than wrapping or truncating it, so one number has one byte string.
//!
//! ### Generations
//!
//! The generation names the layout descriptor of the table's rows. Generation 0 is readable
//! without a descriptor: its rows are documents. Every other generation is read only against its
//! table's layouts ([`TableLayouts`]), and a generation the layouts do not list is refused; a
//! reader holding no layouts refuses every generation but 0. The check happens before the body is
//! read: [`Record::decode_stored`] and [`Record::decode_as_written`] make it, and a reader that
//! has admitted the generation itself, once per row, decodes with [`Record::decode_admitted`] and
//! [`Record::decode_admitted_as_written`]. A `revision` record has no generation and is never
//! checked.
//!
//! A record whose first byte is `0xFF` and whose envelope does not parse is corrupt: it is never
//! passed to the `revision` reader. A header with a reserved bit set, a layout this build does not
//! read, a malformed generation, a packed layout at generation 0, a document with no body, or a
//! KEYED body that is not an object is refused with an [`EnvelopeError`].
//!
//! ### Packed rows
//!
//! A row in a packed layout is read only through the packed descriptor of its generation
//! ([`crate::packed`]): a reader holding no layouts refuses it, and so does one whose descriptor
//! for the generation lays rows out as documents. A packed row always leaves out its `"id"`, which
//! every reader restores from the record's key, so a decode that is given no key yields a row with
//! no `"id"`. A table whose current generation is packed also holds document rows at that
//! generation: a record the descriptor cannot hold is written in the document layout under the same
//! stamp.
//!
//! ### Edges
//!
//! EDGE says the record is an edge, per row: a table of type `ANY` holds edges made by `RELATE`
//! and plain records that merely have `in` and `out` fields, and a table's definition can change
//! after its rows are written. No variant bytes follow it: the adjacency layout comes from
//! [`edge_layout`], which a generation's descriptor answers. A row whose generation has no
//! descriptor naming a layout, generation 0 above all, keeps [`V1_EDGE_VARIANT`] for good: the bits
//! never change meaning. Reading a v1 EDGE row yields
//! `Metadata { record_type: Edge { variant: V1_EDGE_VARIANT }, aggregation_stats: [] }`; a v1 row
//! without EDGE has no metadata.
//!
//! ### Keyed documents
//!
//! A document is KEYED when its top-level `"id"` is the record's key and is left out of the bytes.
//! The writer sets KEYED only when the id is identical to the key, not merely equal, and the key
//! reads back from its storage-key encoding as exactly that id. A KEYED body never holds an
//! `"id"` entry; a full decode refuses one, since the reader would overwrite it.
//!
//! ## Routing
//!
//! The caller of [`encode_record`] and [`encode_whole_record`] chooses a [`RecordFormat`]. In
//! [`RecordFormat::Document`] a record is written in the v1 envelope, stamped with the format's
//! generation, when its metadata is none, or an edge on [`V1_EDGE_VARIANT`] with no aggregation
//! stats. Every other record is written in the `revision` encoding: records with aggregation stats
//! (view rows that keep their stats inline), edges on an earlier adjacency layout, and a table
//! record with metadata. Their metadata is kept exactly as it is. [`RecordFormat::Revision`]
//! writes every record in the `revision` encoding. Reading takes the first byte: `0xFF` is the v1
//! envelope and anything else is a `revision` record, so a store may hold both.
//!
//! A store keeps a group's stats inline only until it switches to keeping them under their own key
//! ([`ViewState`](crate::ViewState)); from then on the view maintenance writes the group's row as a
//! record with no metadata, so the v1 envelope never holds stats. A `revision` row read after the
//! switch is converted by the write that next touches it.
//!
//! [`RecordFormat::Packed`] writes a record whose metadata the envelope holds as a packed row of
//! the generation's descriptor when the record's `"id"` is its key and every value fits its
//! column, and as a document stamped with the same generation otherwise. Packing needs the
//! descriptor, which the table's layouts hold, so a packed format is written through a
//! [`RecordWriter`] or [`encode_record_with`]; [`encode_record`] and [`encode_whole_record`] refuse
//! it ([`RecordLayoutError::WriteWithoutLayouts`]).

use std::borrow::Cow;
use std::io::Read;
use std::sync::Arc;

use anyhow::{Result, ensure};
use revision::{DeserializeRevisioned, Error as RevisionError, SkipCheckRevisioned, revisioned};
use surrealdb_collections::bounded;
use surrealdb_kvs::value::KVValue;
use surrealdb_strand::Strand;

use crate::Generation;
use crate::catalog::aggregation::AggregationStat;
use crate::layout::{PackedDescriptor, RecordLayoutError, TableLayouts};
use crate::packed::{CheckedPackedRow, PackedError, PackedRow, Packing, pack};
use crate::val::stored::varint::{self, VarintError};
use crate::val::{Object, RecordId, TableName, Value, check_value_depth, same_record_id, stored};
#[cfg(debug_assertions)]
use crate::val::{same_object_except, same_value};

/// Represents a record stored in the database
///
/// A `Record` contains both the actual data and optional metadata about the record.
/// The metadata can include information such as the record type (e.g., Edge for graph edges).
///
/// # Examples
///
/// ```no_compile
/// use surrealdb_catalog::Record;
/// use surrealdb_expr::val::{Object, Value};
///
/// // Create a new record with data
/// let record = Record::new(Value::Object(Object::default()));
///
/// // Check if it's an edge record
/// assert!(!record.is_edge());
/// ```
///
/// # Storage
///
/// A record is written in the encoding its [`RecordFormat`] names: the v1 record envelope
/// ([`StoredRecord`]) when the format is [`RecordFormat::Document`] and its metadata is none or an
/// edge on [`V1_EDGE_VARIANT`] with no aggregation stats, and the `revision` encoding otherwise. A
/// record written by an earlier release is `revision`-encoded and is read as it was written; the
/// first byte tells the encodings apart. The `revision` derive writes that encoding
/// and also encodes a record nested in another `revision`-encoded value, such as a queued async
/// event's cursor record. Its reader is hand-written and allocates only as bytes arrive.
///
/// The storage key carries the record's canonical id. A read through
/// [`Record::kv_decode_value`] sets the data's top-level `"id"` from the key,
/// whatever the stored bytes held, and checks a v1 record's generation against the table's
/// layouts the [`RecordContext`] carries.
#[revisioned(
	revision(1),
	revision(2, optimised, indexed_struct),
	deserialize = false,
	skip = true,
	walk = true
)]
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Record {
	/// Optional metadata about the record (e.g., record type)
	pub metadata: Option<Metadata>,
	/// The actual data stored in the record
	// TODO (DB-655): Switch to `Object`.
	pub data: Value,
}

/// What a record is written as.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecordFormat {
	/// The `revision` encoding, which every release reads.
	Revision,
	/// The v1 envelope in the document layout, stamped with `generation`, for a record the
	/// envelope holds; the `revision` encoding for any other.
	Document {
		/// The generation the record is stamped with. `None` is the stamp 0 of a record written
		/// before its table had generations, which needs no descriptor.
		generation: Option<Generation>,
	},
	/// The v1 envelope in the packed layout of `generation`'s descriptor, for a record the
	/// envelope holds whose `"id"` is its key and whose values all fit their columns; the document
	/// layout stamped with `generation` for any other record the envelope holds; the `revision`
	/// encoding otherwise. Packing needs the table's layouts ([`encode_record_with`],
	/// [`RecordWriter`]).
	Packed {
		/// The generation the record is stamped with, and whose descriptor lays the row out.
		generation: Generation,
	},
}

/// The format the rows of one table are written in, with the table's layouts: the ones a packed
/// format reads its descriptor from, and the ones a write that names its record is checked against
/// ([`RecordLayoutError::ForeignLayouts`]).
///
/// Every writer of a table's rows takes its writer from the table's layouts
/// (`Transaction::record_writer`) or from a write context's slot, and passes the record's id to
/// [`encode`](Self::encode) whenever it has it: a write without a key never packs and cannot be
/// checked against its table.
#[derive(Clone, Debug)]
pub struct RecordWriter {
	format: RecordFormat,
	layouts: Option<Arc<TableLayouts>>,
}

impl RecordWriter {
	/// A writer of `format`. `layouts` are kept for a format of a nonzero generation
	/// ([`RecordFormat::Document`] from generation 1, and [`RecordFormat::Packed`]) and dropped for
	/// [`RecordFormat::Revision`] and generation 0, which need none. A packed format without
	/// layouts is refused when it encodes ([`RecordLayoutError::WriteWithoutLayouts`]).
	pub fn new(format: RecordFormat, layouts: Option<Arc<TableLayouts>>) -> Self {
		let keeps = match format {
			RecordFormat::Revision => false,
			RecordFormat::Document {
				generation,
			} => generation.is_some(),
			RecordFormat::Packed {
				..
			} => true,
		};
		Self {
			format,
			layouts: if keeps {
				layouts
			} else {
				None
			},
		}
	}

	/// The format the writer writes in.
	#[inline]
	pub fn format(&self) -> RecordFormat {
		self.format
	}

	/// The layouts the writer holds.
	#[inline]
	pub fn layouts(&self) -> Option<&Arc<TableLayouts>> {
		self.layouts.as_ref()
	}

	/// [`encode_record_with`] with this writer's format and layouts.
	pub fn encode(
		&self,
		metadata: Option<&Metadata>,
		data: &Value,
		key: Option<&RecordId>,
	) -> Result<Vec<u8>> {
		encode_record_with(self.format, self.layouts.as_deref(), metadata, data, key)
	}

	/// [`encode_whole_record_with`] with this writer's format and layouts.
	pub fn encode_whole(&self, record: &Record, key: Option<&RecordId>) -> Result<Vec<u8>> {
		encode_whole_record_with(self.format, self.layouts.as_deref(), record, key)
	}
}

/// The `u32` offset table that opens a revision-2 `Record` payload, one entry per field.
const RECORD_OFFSET_TABLE: usize = 2 * size_of::<u32>();

/// Reads exactly what the derived serializer writes. Revision 1 is the two fields in order.
/// Revision 2 is a `u32` payload length and the payload: an offset table, then the two fields.
/// The payload is read through [`bounded::read_payload`], decoded from the slice and refused if
/// bytes are left over.
impl DeserializeRevisioned for Record {
	fn deserialize_revisioned<R: Read>(reader: &mut R) -> Result<Self, RevisionError> {
		match bounded::read_revision(reader, "Record", 2)? {
			1 => {
				let metadata = Option::<Metadata>::deserialize_revisioned(reader)?;
				let data = Value::deserialize_revisioned(reader)?;
				Ok(Self {
					metadata,
					data,
				})
			}
			_ => {
				let payload = bounded::read_payload(reader)?;
				let mut body = payload
					.get(RECORD_OFFSET_TABLE..)
					.ok_or(RevisionError::OptimisedSubReaderOverrun)?;
				let metadata = Option::<Metadata>::deserialize_revisioned(&mut body)?;
				let data = Value::deserialize_revisioned(&mut body)?;
				bounded::finish(body)?;
				Ok(Self {
					metadata,
					data,
				})
			}
		}
	}
}

impl SkipCheckRevisioned for Record {
	fn skip_check_revisioned<R: Read>(reader: &mut R) -> Result<(), RevisionError> {
		Self::deserialize_revisioned(reader).map(drop)
	}
}

/// Strand value for the `"id"` field name. Used by the post-decode id
/// normaliser so we don't allocate a fresh `Strand` per record.
const ID_KEY: Strand = Strand::new_static("id");

/// What a stored record is decoded against: its id, from its key, and its table's layouts.
///
/// The layouts are what a v1 record's generation is checked against. A context without them reads
/// `revision` records and v1 records of generation 0, which need no descriptor; any other
/// generation is refused.
#[derive(Clone, Debug)]
pub struct RecordContext {
	/// The record's id, rebuilt from its storage key.
	pub id: RecordId,
	/// The layouts of the id's table, as the reader's snapshot lists them.
	pub layouts: Option<Arc<TableLayouts>>,
}

impl RecordContext {
	/// A context that reads `revision` records and v1 records of generation 0 only.
	pub fn without_layouts(id: RecordId) -> Self {
		Self {
			id,
			layouts: None,
		}
	}

	/// A context that reads every generation `layouts` lists.
	pub fn with_layouts(id: RecordId, layouts: Arc<TableLayouts>) -> Self {
		Self {
			id,
			layouts: Some(layouts),
		}
	}
}

impl KVValue for Record {
	/// The storage key carries the canonical `RecordId`, and the table's layouts say what each
	/// generation holds. The decoder uses the id to set `data`'s `"id"` field: the storage key is
	/// the single source of truth for a record's identity.
	type KeyContext = RecordContext;

	/// Encode a `Record` in the `revision` encoding, which every release reads. It holds all
	/// metadata and keeps the top-level `"id"`: without the key there is nothing to tell that it
	/// is the key's.
	///
	/// This is never the encoding of a row written into a table: a table that has a generation
	/// is written in v1 at every feature level, and a packed table's rows need its layouts, which
	/// this method has no access to. A row writer takes its writer from
	/// `Transaction::record_writer` (or a write context's slot) and encodes with
	/// [`RecordWriter::encode_whole`] or [`encode_whole_record_with`].
	///
	/// The nesting depth is checked first. The SQL formatter used by export
	/// and `INFO` and the public-value conversion every query result passes
	/// through recurse over nested collections with no depth guard of their
	/// own — a value deep enough to overflow a thread stack in any of them
	/// aborts the process, and once stored it does so again on every
	/// subsequent read. Rejecting the write is what keeps such a value from
	/// existing: the parser's caps are per-parse, so a read-modify-write loop
	/// adding one level per request never approaches them.
	fn kv_encode_value(&self) -> Result<Vec<u8>> {
		encode_whole_record(RecordFormat::Revision, self, None)
	}

	/// Decode a `Record` in either encoding, as [`Record::decode_stored`] does, and force its
	/// `data`'s top-level `"id"` to the canonical id reconstructed from the storage key.
	///
	/// The storage key — not the stored bytes — is the source of truth
	/// for a record's identity, so we **overwrite** any `"id"` carried in
	/// the payload rather than preserving it. This matters for records
	/// whose stored `"id"` differs from their key: a materialised view
	/// such as `DEFINE TABLE v AS SELECT id, .. FROM src` projects the
	/// *source* row's id into the view record's data, but the view
	/// record's identity is its own key (`v:..`), which is what callers
	/// (and the pre-decode filter, which derives `id` from the key) must
	/// see. Overwriting also restores an id the encoding left out.
	fn kv_decode_value(bytes: &[u8], ctx: RecordContext) -> Result<Record> {
		Self::decode_stored(bytes, ctx.layouts.as_deref(), ctx.id)
	}
}

/// Encodes a record's metadata and data in `format`, without the table's layouts: a
/// [`RecordFormat::Packed`] is refused ([`RecordLayoutError::WriteWithoutLayouts`]), and every
/// other format is written as [`encode_record_with`] writes it with no layouts.
pub fn encode_record(
	format: RecordFormat,
	metadata: Option<&Metadata>,
	data: &Value,
	key: Option<&RecordId>,
) -> Result<Vec<u8>> {
	encode_record_with(format, None, metadata, data, key)
}

/// Encodes a record's metadata and data in `format`.
///
/// [`RecordFormat::Document`] writes the v1 envelope ([`StoredRecord`]) stamped with the format's
/// generation when `metadata` is `None`, or an edge on [`V1_EDGE_VARIANT`] with no aggregation
/// stats. [`RecordFormat::Packed`] writes the same records as a packed row of the generation's
/// descriptor when the record's `"id"` is `key` and every value fits its column, and as a document
/// otherwise. Any other metadata, and every record of [`RecordFormat::Revision`], is written in the
/// `revision` encoding, which holds all of it. That encoding is written from the borrowed
/// arguments, byte for byte as the derived encoding of a [`Record`] holding them, without cloning;
/// the output is the same whether the caller holds a `Record` or its parts.
///
/// `key` is the record's id, when the caller knows it. In the v1 envelope a top-level `"id"`
/// identical to it, whose key reads back from its storage-key encoding as exactly itself, is left
/// out of the bytes (a document marks it [`StoredRecord::KEYED`]; a packed row always leaves it
/// out); a read restores it from the key. Any other `"id"` — a view row's projected source id — is
/// stored as it is, in a document. The `revision` encoding keeps every `"id"`.
///
/// `layouts` are the table's layouts. A packed format reads its descriptor from them, and a write
/// that names its record (`key`) is refused when the layouts are another table's, in every format
/// ([`RecordLayoutError::ForeignLayouts`]). A write with no key cannot be checked against its
/// table: its caller owes that `layouts` are the layouts of the table it writes. A packed format
/// without layouts is [`RecordLayoutError::WriteWithoutLayouts`].
///
/// The generation is written as it is given. The caller owes it a descriptor in the table's
/// layouts, unless it is `None` (the stamp 0 of a record written before its table had generations):
/// a row of any other generation is read only against a descriptor for it.
///
/// Fails on data nested past the value depth cap, and on data holding a
/// closure, which has no stored form. Every build checks that the envelope it wrote parses back
/// to the header, generation and body length it was written with, and that a packed row passes
/// [`check_packed`](crate::packed::check_packed); a debug build also decodes the whole row and
/// compares it with its input.
pub fn encode_record_with(
	format: RecordFormat,
	layouts: Option<&TableLayouts>,
	metadata: Option<&Metadata>,
	data: &Value,
	key: Option<&RecordId>,
) -> Result<Vec<u8>> {
	if let (Some(layouts), Some(key)) = (layouts, key)
		&& *layouts.table() != key.table
	{
		return Err(layout_error(RecordLayoutError::ForeignLayouts {
			layouts: layouts.table().clone(),
			table: key.table.clone(),
		}));
	}
	check_value_depth(data)?;
	match format {
		RecordFormat::Document {
			generation,
		} => match written_in_envelope(metadata) {
			Some(edge) => encode_document(generation, edge, data, key),
			None => encode_revision(metadata, data),
		},
		RecordFormat::Packed {
			generation,
		} => match written_in_envelope(metadata) {
			Some(edge) => encode_packed(generation, RowFlags::edge(edge), data, key, layouts),
			None => encode_revision(metadata, data),
		},
		RecordFormat::Revision => encode_revision(metadata, data),
	}
}

/// The generation the bytes [`encode_record_with`] writes for `format` and `metadata` carry: `None`
/// (stamp 0) for [`RecordFormat::Revision`], and for a [`RecordFormat::Document`] or
/// [`RecordFormat::Packed`] whose metadata the v1 envelope does not hold, which is written in
/// `revision`; the format's generation otherwise. A packed format falls back to a document at the
/// same generation, so the bytes carry the generation either way.
///
/// Every [`RecordFormat`] states its own arm, so a variant added later cannot be stamped by
/// default.
pub fn written_generation(format: RecordFormat, metadata: Option<&Metadata>) -> Option<Generation> {
	written_in_envelope(metadata)?;
	match format {
		RecordFormat::Revision => None,
		RecordFormat::Document {
			generation,
		} => generation,
		RecordFormat::Packed {
			generation,
		} => Some(generation),
	}
}

/// Encodes `record` as [`encode_record`] encodes its parts.
pub fn encode_whole_record(
	format: RecordFormat,
	record: &Record,
	key: Option<&RecordId>,
) -> Result<Vec<u8>> {
	encode_record(format, record.metadata.as_ref(), &record.data, key)
}

/// Encodes `record` as [`encode_record_with`] encodes its parts.
pub fn encode_whole_record_with(
	format: RecordFormat,
	layouts: Option<&TableLayouts>,
	record: &Record,
	key: Option<&RecordId>,
) -> Result<Vec<u8>> {
	encode_record_with(format, layouts, record.metadata.as_ref(), &record.data, key)
}

/// How `metadata` is written. `Some(edge)` is the v1 envelope, with the EDGE bit set when `edge`;
/// `None` is the `revision` encoding.
///
/// The v1 envelope holds exactly two metadata values: none, and an edge on [`V1_EDGE_VARIANT`]
/// with no aggregation stats.
fn written_in_envelope(metadata: Option<&Metadata>) -> Option<bool> {
	match metadata {
		None => Some(false),
		Some(Metadata {
			record_type: RecordType::Edge {
				variant: V1_EDGE_VARIANT,
			},
			aggregation_stats,
		}) if aggregation_stats.is_empty() => Some(true),
		Some(_) => None,
	}
}

/// The object `data` is, when its `"id"` is the record's key and the envelope may leave it out.
///
/// The ids must be identical, not merely equal: `t:[-0.0]` equals `t:[0.0]`, and `t:[1.0]` equals
/// `t:[1]`. The key must also read back from its storage-key encoding as the same id, and that
/// encoding normalises floats and decimals. The exactness walk runs only for a key already
/// identical to the data's id, which the depth check has bounded.
fn keyed_object<'v>(data: &'v Value, key: Option<&RecordId>) -> Option<&'v Object> {
	let (Value::Object(obj), Some(key)) = (data, key) else {
		return None;
	};
	match obj.0.get("id") {
		Some(Value::RecordId(id)) if same_record_id(id, key) && key.key.key_encoding_is_exact() => {
			Some(obj)
		}
		_ => None,
	}
}

/// Writes a v1 document envelope around `data`, stamped with `generation` (stamp 0 for `None`).
fn encode_document(
	generation: Option<Generation>,
	edge: bool,
	data: &Value,
	key: Option<&RecordId>,
) -> Result<Vec<u8>> {
	let stamp = generation.map_or(StoredRecord::NO_GENERATION, Generation::stamp);
	let keyed = keyed_object(data, key);
	let mut header = StoredRecord::DOCUMENT;
	if edge {
		header |= StoredRecord::EDGE;
	}
	if keyed.is_some() {
		header |= StoredRecord::KEYED;
	}
	let (bytes, data_len) =
		stored::with_encoder(|enc| -> Result<(Vec<u8>, usize), stored::Error> {
			match keyed {
				Some(obj) => enc.object_without(obj, "id")?,
				None => enc.value(data)?,
			}
			let data_len = enc.written();
			enc.put_varint_u64(stamp);
			enc.put_u8(header);
			enc.put_u8(StoredRecord::MARKER);
			Ok((enc.as_bytes().to_vec(), data_len))
		})
		.map_err(stored_write_error)?;
	check_envelope(&bytes, header, stamp, data_len)?;
	#[cfg(debug_assertions)]
	check_reads_back(&bytes, None, edge.then(edge_metadata).as_ref(), data, keyed.is_some())?;
	Ok(bytes)
}

/// The error a write returns for a stored-grammar failure. A closure is refused with the error its
/// `revision` encoding raises, so a write storing one fails alike whichever encoding it would be
/// written in.
#[cold]
fn stored_write_error(error: stored::Error) -> anyhow::Error {
	if let stored::Error::Closure = error {
		anyhow::Error::new(RevisionError::Conversion(
			"Closures cannot be stored on disk".to_owned(),
		))
	} else {
		anyhow::Error::new(error)
	}
}

/// Writes a record in the packed layout of `generation`'s descriptor, or in the document layout at
/// the same generation when the layout cannot hold it.
///
/// `stamp` is the packed generation. Refuses, before any byte is written, a write without
/// `layouts` ([`RecordLayoutError::WriteWithoutLayouts`]), a generation the layouts do not admit,
/// and a descriptor that lays rows out as documents ([`RecordLayoutError::LayoutMismatch`]).
/// A record whose `"id"` is not its key is a document. The packed row has passed
/// [`check_packed`](crate::packed::check_packed), and a debug build decodes it too.
fn encode_packed(
	stamp: Generation,
	flags: RowFlags,
	data: &Value,
	key: Option<&RecordId>,
	layouts: Option<&TableLayouts>,
) -> Result<Vec<u8>> {
	let Some(layouts) = layouts else {
		return Err(layout_error(RecordLayoutError::WriteWithoutLayouts {
			generation: stamp,
		}));
	};
	let packed = layouts.admit_packed(stamp).map_err(layout_error)?;
	let Some(object) = keyed_object(data, key) else {
		return encode_document(Some(stamp), flags.is_edge(), data, key);
	};
	match pack(packed, stamp, flags, object).map_err(stored_write_error)? {
		Packing::Misfit => encode_document(Some(stamp), flags.is_edge(), data, key),
		Packing::Packed(bytes) => {
			#[cfg(debug_assertions)]
			check_reads_back(
				&bytes,
				Some(layouts),
				flags.is_edge().then(edge_metadata).as_ref(),
				data,
				true,
			)?;
			Ok(bytes)
		}
	}
}

/// Writes a record of `metadata` and `data` in the `revision` encoding, borrowing both.
///
/// The bytes are exactly those the derived serializer of [`Record`] writes at revision 2:
///
/// ```text
/// u16 revision (2), u32 LE payload length, payload
/// payload = u32 LE offset of each field from the payload's start (2), metadata, data
/// ```
///
/// A payload past `u32::MAX` bytes is refused.
fn encode_revision(metadata: Option<&Metadata>, data: &Value) -> Result<Vec<u8>> {
	use revision::SerializeRevisioned;

	const LATEST_REVISION: u16 = 2;
	const LEN: usize = size_of::<u32>();
	let mut out = Vec::with_capacity(64);
	LATEST_REVISION.serialize_revisioned(&mut out)?;
	let length_at = out.len();
	let payload_at = length_at + LEN;
	out.resize(payload_at + RECORD_OFFSET_TABLE, 0);
	let metadata_offset = u32::try_from(RECORD_OFFSET_TABLE)?;
	metadata.map(Cow::Borrowed).serialize_revisioned(&mut out)?;
	let data_offset = u32::try_from(out.len() - payload_at)?;
	data.serialize_revisioned(&mut out)?;
	let length = u32::try_from(out.len() - payload_at)
		.map_err(|_| RevisionError::Serialize("a record payload exceeds u32::MAX bytes".into()))?;
	let patches =
		[(length_at, length), (payload_at, metadata_offset), (payload_at + LEN, data_offset)];
	for (at, value) in patches {
		out.get_mut(at..at + LEN)
			.ok_or(RevisionError::OptimisedSubReaderOverrun)?
			.copy_from_slice(&value.to_le_bytes());
	}
	ensure!(
		out.first() != Some(&StoredRecord::MARKER),
		"a revision-encoded record starts with the v1 envelope marker"
	);
	#[cfg(debug_assertions)]
	check_reads_back(&out, None, metadata, data, false)?;
	Ok(out)
}

/// Checks that the stamp of a v1 record of `table` is one the table's layouts read.
///
/// Stamp 0 needs no descriptor and is admitted with or without layouts. Any other stamp is read
/// only against its table's layouts: none given is [`RecordLayoutError::NoLayouts`], layouts of
/// another table than `table` are [`RecordLayoutError::ForeignLayouts`], and a generation the
/// layouts do not list, or list as refused, is the refusal [`TableLayouts::admit`] names. A stamp
/// that names no generation is the refusal [`TableLayouts::stamp_refusal`] names.
#[inline]
fn admit_generation(stamp: u64, layouts: Option<&TableLayouts>, table: &TableName) -> Result<()> {
	if stamp == StoredRecord::NO_GENERATION {
		return Ok(());
	}
	let Some(layouts) = layouts else {
		return Err(layout_error(RecordLayoutError::NoLayouts {
			generation: stamp,
		}));
	};
	if layouts.table() != table {
		return Err(layout_error(RecordLayoutError::ForeignLayouts {
			layouts: layouts.table().clone(),
			table: table.clone(),
		}));
	}
	match Generation::of_stamp(stamp) {
		Ok(Some(generation)) => layouts.admit(generation).map(drop).map_err(layout_error),
		// The stamp is nonzero here, and every nonzero stamp in range names a generation.
		Ok(None) => Ok(()),
		Err(stamp) => Err(layout_error(layouts.stamp_refusal(stamp))),
	}
}

/// A refusal to read a record because of its table's layouts, as the error a read returns.
#[cold]
fn layout_error(error: RecordLayoutError) -> anyhow::Error {
	anyhow::Error::new(crate::Error::Layout(error))
}

/// A malformed packed row of `table`'s generation `generation`, as the error a read returns.
#[cold]
fn packed_error(table: &TableName, generation: Generation, cause: PackedError) -> anyhow::Error {
	layout_error(RecordLayoutError::MalformedRow {
		table: table.clone(),
		generation,
		cause,
	})
}

/// Reads the packed row `stored` of the record `id` through its generation's descriptor in
/// `layouts`, merging `id` into the data as the row is decoded.
///
/// The refusals, all before any of the body is read except the last:
/// - no layouts: [`RecordLayoutError::NoLayouts`];
/// - layouts of another table than `id`'s: [`RecordLayoutError::ForeignLayouts`]. A row's
///   generation names a descriptor of one table only, so a descriptor of another table could decode
///   the body to plausible wrong values; the check is one table-name compare per packed row;
/// - a generation the layouts do not list, or list as refused: the refusal [`TableLayouts::admit`]
///   names;
/// - a descriptor that lays rows out as documents: [`RecordLayoutError::LayoutMismatch`];
/// - a body that is not a row of the descriptor: [`RecordLayoutError::MalformedRow`].
fn read_packed(
	stored: &StoredRecord<'_>,
	layouts: Option<&TableLayouts>,
	id: RecordId,
) -> Result<Record> {
	let Some(layouts) = layouts else {
		return Err(layout_error(RecordLayoutError::NoLayouts {
			generation: stored.stamp(),
		}));
	};
	if *layouts.table() != id.table {
		return Err(layout_error(RecordLayoutError::ForeignLayouts {
			layouts: layouts.table().clone(),
			table: id.table,
		}));
	}
	let generation = packed_generation(stored, layouts)?;
	let packed = layouts.admit_packed(generation).map_err(layout_error)?;
	stored
		.decode_packed(packed, Some(id))
		.map_err(|cause| packed_error(layouts.table(), generation, cause))
}

/// The generation a packed row is read through. [`StoredRecord::parse`] refuses a packed row at
/// stamp 0, so the stamp of a packed row is a generation or a stamp above the highest one.
fn packed_generation(stored: &StoredRecord<'_>, layouts: &TableLayouts) -> Result<Generation> {
	match stored.generation() {
		Ok(Some(generation)) => Ok(generation),
		Ok(None) => Err(anyhow::Error::new(EnvelopeError::PackedWithoutGeneration)),
		Err(stamp) => Err(layout_error(layouts.stamp_refusal(stamp))),
	}
}

/// Decodes the envelope `stored` as it was written, restoring the `"id"` a keyed record left out
/// from `rid`, which a keyed record is given.
fn decode_envelope_as_written(stored: &StoredRecord<'_>, rid: Option<RecordId>) -> Result<Record> {
	let mut record = stored.decode_document()?;
	if stored.is_keyed()
		&& let Some(rid) = rid
		&& let Value::Object(obj) = &mut record.data
	{
		obj.0.insert(ID_KEY, Value::RecordId(rid));
	}
	Ok(record)
}

/// The metadata a v1 EDGE row stands for.
fn edge_metadata() -> Metadata {
	Metadata {
		record_type: RecordType::Edge {
			variant: V1_EDGE_VARIANT,
		},
		aggregation_stats: Vec::new(),
	}
}

/// Checks that a v1 envelope just written parses to the header, generation and body length it was
/// written with. This runs on every write in every build and reads no data: it is what stops an
/// encoder fault from persisting a row no reader accepts.
fn check_envelope(bytes: &[u8], header: u8, generation: u64, data_len: usize) -> Result<()> {
	let parsed = StoredRecord::parse(bytes);
	ensure!(
		matches!(
			&parsed,
			Ok(Some(stored)) if stored.header == header
				&& stored.generation == generation
				&& stored.body.len() == data_len
		),
		"an encoded record does not read back as the envelope it was written with"
	);
	Ok(())
}

/// Decodes `bytes`, just written for `metadata` and `data`, and checks that the row reads back as
/// what was written, in whichever encoding it is in. `keyed` says the body of a v1 row leaves out
/// the data's `"id"`. A packed row is read through the descriptor `layouts` give for the
/// generation it names, as every reader looks it up.
///
/// Debug builds run this on every write. Neither error names the data.
#[cfg(debug_assertions)]
fn check_reads_back(
	bytes: &[u8],
	layouts: Option<&TableLayouts>,
	metadata: Option<&Metadata>,
	data: &Value,
	keyed: bool,
) -> Result<()> {
	let read = match StoredRecord::parse(bytes)? {
		Some(stored) if stored.is_packed() => {
			let Some(layouts) = layouts else {
				anyhow::bail!(
					"an encoded packed record has no packed descriptor to read back with"
				);
			};
			let packed =
				layouts.admit_packed(packed_generation(&stored, layouts)?).map_err(layout_error)?;
			stored.decode_packed(packed, None)?
		}
		Some(stored) => stored.decode_document()?,
		None => revision::from_slice::<Record>(bytes)?,
	};
	// Metadata is compared by its `revision` bytes: they are exact, where `PartialEq` on the
	// values an aggregation stat holds is not reflexive for a geometry with a NaN coordinate.
	ensure!(
		revision::to_vec(&read.metadata)? == revision::to_vec(&metadata.cloned())?,
		"an encoded record reads back with other metadata"
	);
	let same = match (&read.data, data) {
		(Value::Object(read), Value::Object(written)) if keyed => {
			same_object_except(read, written, "id")
		}
		(read, written) => !keyed && same_value(read, written),
	};
	ensure!(same, "an encoded record reads back with other data");
	Ok(())
}

/// Why the envelope of a stored record was refused.
///
/// Each refusal is a record the writer never produces, or one that a build this one does not
/// know produced; the bytes are never skipped or guessed at.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum EnvelopeError {
	#[error("a stored record ends before its header")]
	NoHeader,
	#[error("a stored record has the reserved header bits {0:#04x} set")]
	ReservedBits(u8),
	#[error("a stored record in a packed layout has no generation")]
	PackedWithoutGeneration,
	#[error("a stored record is in the unknown layout {0}")]
	UnknownLayout(u8),
	#[error("a stored record's generation is malformed: {0}")]
	Generation(VarintError),
	#[error("a stored record has no data")]
	NoData,
	#[error("a stored record keyed by its id holds data that is not an object")]
	KeyedNotObject,
}

/// The flag bits of a v1 record's header that a writer sets, apart from the layout and the bit a
/// packed layout gives its own meaning: EDGE, bit 3.
///
/// A packed row is written with the flags of its record, so the header a writer composes from a
/// layout and its flags is the one a check of the written bytes compares.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RowFlags(u8);

impl RowFlags {
	/// No flag: a record that is not an edge.
	pub const NONE: Self = Self(0);
	/// The record is an edge on [`V1_EDGE_VARIANT`].
	pub const EDGE: Self = Self(StoredRecord::<'static>::EDGE);

	/// [`EDGE`](Self::EDGE) when `edge`, and [`NONE`](Self::NONE) otherwise.
	#[inline]
	pub const fn edge(edge: bool) -> Self {
		if edge {
			Self::EDGE
		} else {
			Self::NONE
		}
	}

	/// The header bits the flags set.
	#[inline]
	pub const fn bits(self) -> u8 {
		self.0
	}

	/// Whether the flags say the record is an edge.
	#[inline]
	pub const fn is_edge(self) -> bool {
		self.0 & StoredRecord::<'static>::EDGE != 0
	}
}

/// A record in the v1 envelope, split into its parts without decoding the data.
///
/// ```text
/// record     = FF, header, generation, body
/// header     = bits 0-2  layout   0 DOCUMENT  the body is one stored value
///                                 1, 2, 3     packed layouts (1-, 2- and 4-byte end offsets):
///                                             the body is a packed row of the generation's descriptor
///                                 4-7         refused: UnknownLayout
///              bit 3     EDGE     the record is an edge on V1_EDGE_VARIANT; no variant bytes follow
///              bits 4, 5 reserved, zero
///              bit 6     KEYED    (DOCUMENT) the body is an object whose "id", the record's
///                                 key, was left out; the body holds no "id" entry
///                        ESCAPES  (packed layouts) the body ends with a trailer
///              bit 7     reserved, zero
/// generation = canonical unsigned LEB128 u64, 1 to 10 bytes; at least 1 in a packed layout
/// body       = DOCUMENT: one stored value, to the end of the record
///              packed:   a packed row, to the end of the record; empty for a descriptor with no columns
/// ```
///
/// A `revision`-encoded record starts with its struct revision, 1 or 2, never `FF`.
/// [`parse`](Self::parse) takes every step of the envelope check in constant time, so an in-place
/// reader pays for it on every row: it refuses a header with a reserved bit set, a layout this
/// build does not read, a generation that is truncated, longer than its value needs, or more than
/// 64 bits, a packed layout at generation 0 (a descriptor-less generation has no packed row), and,
/// for a document, an empty body and a KEYED body whose first tag is not an object's. It does not
/// read the body. [`decode_document`](Self::decode_document) refuses a malformed document body, and
/// a KEYED object that holds an `"id"`; [`decode_packed`](Self::decode_packed) reads a packed body
/// through its descriptor.
///
/// A packed row has no `"id"` of its own: the record's key is its id. Only a document holds its
/// body as one stored value, so [`document`](Self::document) answers a packed row with `None`, and
/// an in-place reader of document bytes declines a packed row.
///
/// A row's metadata is a function of its header: EDGE says the record is an edge on
/// [`V1_EDGE_VARIANT`], and nothing else is recorded. See the module documentation for which
/// records are written in this envelope.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StoredRecord<'a> {
	header: u8,
	generation: u64,
	body: &'a [u8],
}

impl<'a> StoredRecord<'a> {
	/// The first byte of a record in this envelope.
	pub const MARKER: u8 = 0xFF;
	/// The header bits naming the layout of the body.
	pub const LAYOUT: u8 = 0x07;
	/// The layout in which the body is one stored value.
	pub const DOCUMENT: u8 = 0;
	/// A packed layout with 1-byte end offsets.
	pub const PACKED_U8: u8 = 1;
	/// A packed layout with 2-byte end offsets.
	pub const PACKED_U16: u8 = 2;
	/// A packed layout with 4-byte end offsets.
	pub const PACKED_U32: u8 = 3;
	/// The record is an edge on [`V1_EDGE_VARIANT`].
	pub const EDGE: u8 = 0x08;
	/// A document's body is an object whose `"id"`, the record's key, was left out.
	pub const KEYED: u8 = 0x40;
	/// A packed row's body ends with a trailer; the same bit as [`KEYED`](Self::KEYED), which only
	/// a document gives its meaning.
	pub const ESCAPES: u8 = 0x40;
	/// Header bits that must be zero.
	pub const RESERVED: u8 = 0xB0;
	/// The generation of a record written before its table had one.
	pub const NO_GENERATION: u64 = 0;

	/// Splits `bytes`, or returns `None` when they are a `revision`-encoded
	/// record: their first byte is not [`MARKER`](Self::MARKER).
	#[inline]
	pub fn parse(bytes: &'a [u8]) -> Result<Option<Self>, EnvelopeError> {
		let Some((&Self::MARKER, rest)) = bytes.split_first() else {
			return Ok(None);
		};
		let (&header, rest) = rest.split_first().ok_or(EnvelopeError::NoHeader)?;
		let reserved = header & Self::RESERVED;
		if reserved != 0 {
			return Err(EnvelopeError::ReservedBits(reserved));
		}
		let layout = header & Self::LAYOUT;
		let packed = match layout {
			Self::DOCUMENT => false,
			Self::PACKED_U8..=Self::PACKED_U32 => true,
			layout => return Err(EnvelopeError::UnknownLayout(layout)),
		};
		let (generation, used) = varint::read_u64(rest).map_err(EnvelopeError::Generation)?;
		let body = rest.get(used..).ok_or(EnvelopeError::Generation(VarintError::Truncated))?;
		if packed {
			if generation == Self::NO_GENERATION {
				return Err(EnvelopeError::PackedWithoutGeneration);
			}
		} else {
			if body.is_empty() {
				return Err(EnvelopeError::NoData);
			}
			if header & Self::KEYED != 0
				&& !matches!(
					body.first().map(|&tag| stored::Kind::of(tag)),
					Some(stored::Kind::Object)
				) {
				return Err(EnvelopeError::KeyedNotObject);
			}
		}
		Ok(Some(Self {
			header,
			generation,
			body,
		}))
	}

	/// The stamp the record was written under: [`NO_GENERATION`](Self::NO_GENERATION) for a record
	/// written before its table had one.
	#[inline]
	pub const fn stamp(&self) -> u64 {
		self.generation
	}

	/// The generation the record was written under: `None` for a record written before its table
	/// had one, and `Err(stamp)` for a stamp that names no generation.
	#[inline]
	pub const fn generation(&self) -> Result<Option<Generation>, u64> {
		Generation::of_stamp(self.generation)
	}

	/// Whether the record is an edge.
	#[inline]
	pub const fn is_edge(&self) -> bool {
		self.header & Self::EDGE != 0
	}

	/// The adjacency-key variant of an edge, which [`edge_layout`] names for the row's generation.
	#[inline]
	pub const fn edge_variant(&self) -> Option<u16> {
		if self.is_edge() {
			Some(edge_layout(self.generation))
		} else {
			None
		}
	}

	/// The layout value of the header: [`DOCUMENT`](Self::DOCUMENT), or one of the packed layouts.
	#[inline]
	pub const fn layout(&self) -> u8 {
		self.header & Self::LAYOUT
	}

	/// Whether the body is a packed row.
	#[inline]
	pub const fn is_packed(&self) -> bool {
		matches!(self.header & Self::LAYOUT, Self::PACKED_U8..=Self::PACKED_U32)
	}

	/// Whether the data is a document object whose `"id"`, the record's key, was left out. False
	/// for every packed row, which leaves its id out by its layout
	/// ([`id_left_out`](Self::id_left_out)).
	#[inline]
	pub const fn is_keyed(&self) -> bool {
		self.header & Self::LAYOUT == Self::DOCUMENT && self.header & Self::KEYED != 0
	}

	/// Whether a packed body ends with a trailer. A document has none, whatever its header says of
	/// the bit it shares with KEYED.
	#[inline]
	pub const fn has_escapes(&self) -> bool {
		self.is_packed() && self.header & Self::ESCAPES != 0
	}

	/// Whether the stored data leaves out the record's `"id"`, which a reader restores from the
	/// record's key: a keyed document, and every packed row.
	#[inline]
	pub const fn id_left_out(&self) -> bool {
		self.is_keyed() || self.is_packed()
	}

	/// Assembles a record from its parts without reading or checking any of them. The caller owes
	/// that `header` is a header the envelope allows, that `generation` is the generation `body`'s
	/// record names, and that `body` is the bytes after it.
	#[inline]
	pub(crate) const fn from_parts(header: u8, generation: u64, body: &'a [u8]) -> Self {
		Self {
			header,
			generation,
			body,
		}
	}

	/// The bytes after the generation, in whichever layout the header names. Crate-private: only
	/// the packed reader splits a packed body, so no reader outside this crate can take it for a
	/// stored value.
	#[inline]
	pub(crate) const fn body(&self) -> &'a [u8] {
		self.body
	}

	/// The data's stored bytes, unchecked, when the record is a document; `None` for a packed row,
	/// whose body is not a value of the stored grammar. Crate-private: outside this crate a
	/// document is taken only as a [`CheckedDocument`] from
	/// [`checked_document`](Self::checked_document).
	#[inline]
	pub(crate) const fn document(&self) -> Option<&'a [u8]> {
		if self.is_packed() {
			None
		} else {
			Some(self.body)
		}
	}

	/// The record's document, checked as [`CheckedDocument`] describes: its body is one value of
	/// the stored grammar and, when the record is keyed, an object holding no `"id"`. `Ok(None)`
	/// for a packed row, which has no document.
	///
	/// Refuses exactly what [`decode_document`](Self::decode_document) refuses, so a reader that
	/// takes its fields from the checked document refuses every record the whole decode refuses,
	/// whichever fields it reads.
	pub fn checked_document(&self) -> Result<Option<CheckedDocument<'a>>, stored::Error> {
		if self.is_packed() {
			return Ok(None);
		}
		stored::validate(self.body)?;
		if self.is_keyed()
			&& let Some(root) = stored::ValueRef::new(self.body)?.as_ordered_object()?
			&& root.get("id")?.is_some()
		{
			return Err(stored::Error::Malformed(
				"a record keyed by its id that also stores an id",
			));
		}
		Ok(Some(CheckedDocument {
			body: self.body,
			keyed: self.is_keyed(),
			verified: false,
		}))
	}

	/// The record's document as an in-place reader takes it, admitted against `layouts`, the
	/// layouts of its table that admitted its generation. `Ok(None)` for a packed row, which has
	/// no document.
	///
	/// This is where an in-place read of a document decides whether to walk it. A document whose
	/// generation `layouts` vouch for ([`TableLayouts::vouches_for`]) is admitted as it is: every
	/// row of a verified generation is one the whole decode accepts, so it answers what
	/// [`checked_document`](Self::checked_document) answers without the O(bytes) walk. Every other
	/// document, including every one read without layouts, is checked by
	/// [`checked_document`](Self::checked_document).
	#[inline]
	pub fn admit_document(
		&self,
		layouts: Option<&TableLayouts>,
	) -> Result<Option<CheckedDocument<'a>>, stored::Error> {
		if self.is_packed() {
			return Ok(None);
		}
		if layouts.is_some_and(|layouts| layouts.vouches_for(self.generation)) {
			return Ok(Some(CheckedDocument {
				body: self.body,
				keyed: self.is_keyed(),
				verified: true,
			}));
		}
		self.checked_document()
	}

	/// Whether `bytes` are a v1 record in a packed layout: two byte loads, which prove nothing
	/// else.
	#[inline]
	pub fn is_packed_bytes(bytes: &[u8]) -> bool {
		bytes.first() == Some(&Self::MARKER)
			&& matches!(bytes.get(1).map(|header| header & Self::LAYOUT), Some(1..=3))
	}

	/// The record's metadata: an edge on [`V1_EDGE_VARIANT`] with no aggregation stats when EDGE is
	/// set, and none otherwise.
	pub fn metadata(&self) -> Option<Metadata> {
		self.is_edge().then(edge_metadata)
	}

	/// The record of a document, its data as stored: a keyed record's data has no `"id"`.
	///
	/// Refuses a malformed body, a keyed object that holds an `"id"` entry (the writer never stores
	/// one, and the reader would overwrite it), and a packed row, which only
	/// [`decode_packed`](Self::decode_packed) reads.
	pub fn decode_document(&self) -> Result<Record, stored::Error> {
		let Some(body) = self.document() else {
			return Err(stored::Error::Malformed("a packed record read as a document"));
		};
		let data = stored::decode(body)?;
		if self.is_keyed() && matches!(&data, Value::Object(obj) if obj.0.contains_key("id")) {
			return Err(stored::Error::Malformed(
				"a record keyed by its id that also stores an id",
			));
		}
		Ok(Record {
			metadata: self.metadata(),
			data,
		})
	}

	/// The record of a packed row, read through `descriptor`, the packed descriptor of the row's
	/// generation: its data from the descriptor's columns, with `id` merged into the root object at
	/// its place in key order when given, and its metadata from the header.
	///
	/// Refuses a record that is not packed and a body that is not a row of `descriptor`
	/// ([`PackedError`]); it checks every column ([`PackedRow::decode`]).
	pub fn decode_packed(
		&self,
		descriptor: &PackedDescriptor,
		id: Option<RecordId>,
	) -> Result<Record, PackedError> {
		let row = PackedRow::new(self, descriptor)?;
		let data = row.decode(id.map(Value::RecordId))?;
		Ok(Record {
			metadata: self.metadata(),
			data: Value::Object(data),
		})
	}

	/// The record of a packed row, checked: its body is a row of `descriptor` and the whole decode
	/// of it, run to build nothing, accepts it, with `id` at its place in key order. The check is
	/// the decode's own walk ([`PackedRow::decode`]), so it refuses exactly the rows
	/// [`decode_packed`](Self::decode_packed) refuses, with the same error, whichever fields a
	/// reader then takes from the [`CheckedPackedRow`].
	///
	/// A reader takes a packed row in place only as the row this returns.
	pub fn checked_packed(
		&self,
		descriptor: &'a PackedDescriptor,
	) -> Result<CheckedPackedRow<'a>, PackedError> {
		PackedRow::new(self, descriptor)?.checked()
	}

	/// The record's packed row as an in-place reader takes it, admitted against `layouts`, the
	/// layouts of its table that admitted its generation, and read through `descriptor`, its
	/// generation's packed descriptor there.
	///
	/// This is where an in-place read of a packed row decides whether to walk it, as
	/// [`admit_document`](Self::admit_document) decides for a document. A row of a generation
	/// `layouts` vouch for ([`TableLayouts::vouches_for`]) is admitted once its structure parses
	/// ([`PackedRow::new`]), without the check walk: every row of a verified generation is one the
	/// whole decode accepts. Every other row is checked by
	/// [`checked_packed`](Self::checked_packed).
	#[inline]
	pub fn admit_packed(
		&self,
		descriptor: &'a PackedDescriptor,
		layouts: &TableLayouts,
	) -> Result<CheckedPackedRow<'a>, PackedError> {
		let row = PackedRow::new(self, descriptor)?;
		if layouts.vouches_for(self.generation) {
			return Ok(row.vouched());
		}
		row.checked()
	}
}

/// A record's document, checked: its body is one value of the stored grammar and, when the record
/// is keyed, an object holding no `"id"`. These are the bytes [`StoredRecord::decode_document`]
/// accepts, so an in-place reader that takes a document only as this refuses every record the
/// whole decode refuses.
///
/// The proof is one of two. [`StoredRecord::checked_document`] walks the body with
/// [`stored::validate`]. [`StoredRecord::admit_document`] makes one unwalked for a document whose
/// generation its table's layouts vouch for: every row of a verified generation is one the whole
/// decode accepts ([`crate::layout::LayoutDescriptor::verified`]).
///
/// Only those two make one.
#[derive(Clone, Copy, Debug)]
pub struct CheckedDocument<'a> {
	body: &'a [u8],
	keyed: bool,
	/// Whether the document was admitted unwalked, because its generation is verified.
	verified: bool,
}

impl<'a> CheckedDocument<'a> {
	/// The document's stored bytes: one value of the stored grammar, to the end of the record.
	#[inline]
	pub fn bytes(&self) -> &'a [u8] {
		self.body
	}

	/// Whether the record is keyed: the document is an object whose `"id"`, the record's key, is
	/// left out of the bytes.
	#[inline]
	pub fn is_keyed(&self) -> bool {
		self.keyed
	}

	/// Whether the document was admitted without a walk because its generation is verified
	/// ([`StoredRecord::admit_document`]), rather than checked.
	#[inline]
	pub fn is_verified(&self) -> bool {
		self.verified
	}

	/// The document's root value. The check already proved the bytes are one value, so the error
	/// is never produced for a document this type was made from; the result stays fallible so
	/// that no caller needs an unwrap.
	#[inline]
	pub fn value(&self) -> Result<stored::ValueRef<'a>, stored::Error> {
		stored::ValueRef::new(self.body)
	}
}

impl Record {
	/// Creates a new record with the given data and no metadata
	pub fn new(data: Value) -> Self {
		Self {
			metadata: None,
			data,
		}
	}

	/// Decodes a stored record of the table `layouts` describes, in either encoding, and sets the
	/// data's top-level `"id"` to `rid`, the record's id, whatever the stored bytes held.
	///
	/// A v1 record's generation is checked against `layouts` before its body is read; see
	/// [`RecordLayoutError`] for the refusals. A packed row is read through its generation's
	/// descriptor, which it needs `layouts` for, and its `"id"` is merged in as it is decoded. A
	/// `revision` record has no generation and never reads `layouts`.
	pub fn decode_stored(
		bytes: &[u8],
		layouts: Option<&TableLayouts>,
		rid: RecordId,
	) -> Result<Record> {
		let mut record = match StoredRecord::parse(bytes)? {
			Some(stored) if stored.is_packed() => return read_packed(&stored, layouts, rid),
			Some(stored) => {
				admit_generation(stored.stamp(), layouts, &rid.table)?;
				stored.decode_document()?
			}
			None => revision::from_slice(bytes)?,
		};
		if let Value::Object(obj) = &mut record.data {
			obj.0.insert(ID_KEY, Value::RecordId(rid));
		}
		Ok(record)
	}

	/// Decodes a stored record in either encoding as it was written: an `"id"`
	/// the encoding left out is restored from `rid`, the record's id, and any
	/// other is left as stored — a view row holds none, or a projected one.
	///
	/// A v1 record's generation is checked against `layouts` as [`Record::decode_stored`]
	/// checks it. `rid` is called at most once, and only for a record that leaves its `"id"` out
	/// (a keyed document or a packed row) or a record of a nonzero generation.
	pub fn decode_as_written(
		bytes: &[u8],
		layouts: Option<&TableLayouts>,
		rid: impl FnOnce() -> RecordId,
	) -> Result<Record> {
		let Some(stored) = StoredRecord::parse(bytes)? else {
			return Ok(revision::from_slice(bytes)?);
		};
		if stored.is_packed() {
			return read_packed(&stored, layouts, rid());
		}
		let stamp = stored.stamp();
		let rid = if stamp != StoredRecord::NO_GENERATION {
			let rid = rid();
			admit_generation(stamp, layouts, &rid.table)?;
			Some(rid)
		} else {
			stored.is_keyed().then(rid)
		};
		decode_envelope_as_written(&stored, rid)
	}

	/// Decodes bytes whose generation the caller has already admitted against the table's layouts
	/// (`TableLayouts::admit`, or a gate built on it), and sets the data's top-level `"id"` to
	/// `rid`.
	///
	/// This is [`Record::decode_stored`] without the generation check of a document: the generation
	/// of a document is not read. A packed row is read through its descriptor, so `layouts` must be
	/// the layouts of `rid`'s table, in the namespace and database the record lives in, that the
	/// row's reader admitted it against. The decode refuses layouts of another table
	/// ([`RecordLayoutError::ForeignLayouts`]) but cannot tell another namespace's or database's
	/// layouts of the same table apart, because a record id names neither. An envelope this build
	/// does not read is still refused.
	pub fn decode_admitted(
		bytes: &[u8],
		layouts: Option<&TableLayouts>,
		rid: RecordId,
	) -> Result<Record> {
		let mut record = match StoredRecord::parse(bytes)? {
			Some(stored) if stored.is_packed() => return read_packed(&stored, layouts, rid),
			Some(stored) => stored.decode_document()?,
			None => revision::from_slice(bytes)?,
		};
		if let Value::Object(obj) = &mut record.data {
			obj.0.insert(ID_KEY, Value::RecordId(rid));
		}
		Ok(record)
	}

	/// Decodes bytes whose generation the caller has already admitted, as
	/// [`Record::decode_as_written`] decodes them, without the generation check of a document.
	/// `rid` is called at most once, and only for a keyed document or a packed row. The duty of
	/// `layouts` is the one [`Record::decode_admitted`] states.
	pub fn decode_admitted_as_written(
		bytes: &[u8],
		layouts: Option<&TableLayouts>,
		rid: impl FnOnce() -> RecordId,
	) -> Result<Record> {
		match StoredRecord::parse(bytes)? {
			Some(stored) if stored.is_packed() => read_packed(&stored, layouts, rid()),
			Some(stored) => decode_envelope_as_written(&stored, stored.is_keyed().then(rid)),
			None => Ok(revision::from_slice(bytes)?),
		}
	}

	/// Checks if this record represents an edge in a graph
	pub const fn is_edge(&self) -> bool {
		matches!(
			&self.metadata,
			Some(Metadata {
				record_type: RecordType::Edge { .. },
				..
			})
		)
	}

	/// Returns the adjacency-key format variant of this edge record, if
	/// any. Returns `None` for non-edge records. Callers that need to
	/// preserve an existing edge's variant on UPDATE (rather than
	/// downgrading it to a different write-time default) should source
	/// the variant from this helper on the prior document.
	pub const fn edge_variant(&self) -> Option<u16> {
		match &self.metadata {
			Some(Metadata {
				record_type: RecordType::Edge {
					variant,
				},
				..
			}) => Some(*variant),
			_ => None,
		}
	}

	/// Wraps this record in an `Arc` for shared ownership.
	pub fn into_read_only(self) -> Arc<Self> {
		Arc::new(self)
	}

	/// Sets the record type in the metadata
	pub fn set_record_type(&mut self, rtype: RecordType) {
		match &mut self.metadata {
			Some(metadata) => {
				metadata.record_type = rtype;
			}
			metadata => {
				*metadata = Some(Metadata {
					record_type: rtype,
					aggregation_stats: Vec::new(),
				});
			}
		}
	}
}

/// The adjacency-key layout generation used when writing brand-new edges.
///
/// Existing edges keep whatever variant they were originally stamped with
/// so their on-disk keys stay valid — only first-write picks this value.
///
/// The layout a variant names is dispatched in
/// `core/src/exec/vm/write_storage.rs` (`StorageFact::LegacyLayout` and
/// `StorageFact::CurrentLayout` choose which layout a write targets,
/// `StorageInstr::WriteEdgeKeys` writes the adjacency keys, and
/// `purge_pointers` deletes the keys of the layout an edge was written with)
/// and lowered in `core/src/exec/vm/lower_write_storage.rs`. Bump this when
/// the on-disk adjacency-key layout for new edges advances, and add the
/// matching arm to each of those.
///
/// A new edge on the current layout is stored under the v1 record envelope's
/// EDGE bit, which names [`V1_EDGE_VARIANT`] and carries no variant bytes.
/// Bumping this value without deciding how that bit's meaning follows is a
/// compile error, from the assertion below [`V1_EDGE_VARIANT`].
pub const LATEST_EDGE_VARIANT: u16 = 2;

/// The adjacency-key layout the v1 record envelope's EDGE bit stands for.
///
/// A generation-0 v1 row, and a v1 row whose generation's descriptor names no
/// adjacency layout, is an edge on this layout for good: the same bytes mean
/// the same layout in every release, so this value never changes. A
/// generation's descriptor may name a layout for the rows written under it;
/// this value stays the layout of the rows that have none.
pub const V1_EDGE_VARIANT: u16 = 2;

const _: () = assert!(
	LATEST_EDGE_VARIANT == V1_EDGE_VARIANT,
	"LATEST_EDGE_VARIANT differs from V1_EDGE_VARIANT, which the record envelope's EDGE bit names \
	 for good; decide how the envelope writes the new layout before changing it"
);

/// The adjacency-key layout the descriptor of `generation` names for a v1 EDGE row.
///
/// A generation whose descriptor names no layout, generation 0 above all, is on
/// [`V1_EDGE_VARIANT`] for good. No descriptor names one yet, so every generation answers that
/// layout; a descriptor that does answers its own here, and nothing else about how an edge is
/// stored or purged changes. The answer is a function of the generation alone, so a row never
/// carries a variant of its own.
#[inline]
pub const fn edge_layout(generation: u64) -> u16 {
	let _ = generation;
	V1_EDGE_VARIANT
}

/// Types of records that can be stored in the database
///
/// This enum defines the different types of records that can be stored.
/// Edge records carry a `variant` discriminator so format-aware paths
/// (adjacency-key writes, edge purge) know which on-disk layout the
/// record was created against and can dispatch accordingly.
#[revisioned(revision = 2)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, PartialOrd, Hash)]
pub enum RecordType {
	/// Represents a normal table record
	/// From 3.0.0
	#[default]
	Table,
	/// Revision-1 unit form of `Edge`. Records persisted before the
	/// variant marker existed deserialize through this variant and are
	/// upgraded to `Edge { variant: 1 }`, which corresponds to the
	/// original adjacency-key layout (no embedded target vertex).
	/// From v3.0.0 to 3.1.0
	#[revision(end = 2, convert_fn = "upgrade_edge_v1", fields_name = "EdgeV1")]
	Edge,
	/// Represents an edge in a graph. `variant` identifies which
	/// adjacency-key layout was used when the edge was written, so the
	/// purge path knows exactly which keys to delete without probing
	/// multiple formats. New edges get the current format generation;
	/// older edges keep whatever variant they were written with.
	/// From v3.1.0
	#[revision(start = 2)]
	Edge {
		variant: u16,
	},
}

impl RecordType {
	/// Legacy unit `Edge` records predate the variant marker and were
	/// always written with the original adjacency-key layout. Stamp
	/// them as variant 1 on decode so format-aware code paths can treat
	/// them uniformly with explicitly-stamped records going forward.
	fn upgrade_edge_v1(_fields: EdgeV1, _revision: u16) -> Result<Self, revision::Error> {
		Ok(Self::Edge {
			variant: 1,
		})
	}
}

/// Metadata associated with a record
///
/// This struct contains optional metadata about a record, such as its type and
/// aggregation statistics for materialized view records.
/// The metadata is revisioned to ensure compatibility across different versions
/// of the database.
#[revisioned(revision = 1, deserialize = false, skip = true, walk = true)]
#[derive(Clone, Debug, PartialEq)]
pub struct Metadata {
	/// The type of the record (e.g., Edge for graph edges)
	pub record_type: RecordType,
	/// Statistics related to running aggregations for this record.
	/// These do not directly correspond to a field but must be used in conjunction with the table
	/// definition to calculate the final value for this record.
	pub aggregation_stats: Vec<AggregationStat>,
}

/// Reads exactly what the derived serializer writes: the record type, then a length-prefixed list
/// of aggregation stats, which is read through [`bounded::read_vec`] so a claimed length costs
/// nothing beyond the stats the input holds.
impl DeserializeRevisioned for Metadata {
	fn deserialize_revisioned<R: Read>(reader: &mut R) -> Result<Self, RevisionError> {
		bounded::read_revision(reader, "Metadata", 1)?;
		let record_type = RecordType::deserialize_revisioned(reader)?;
		let aggregation_stats = bounded::read_vec::<AggregationStat, _>(reader)?;
		Ok(Self {
			record_type,
			aggregation_stats,
		})
	}
}

impl SkipCheckRevisioned for Metadata {
	fn skip_check_revisioned<R: Read>(reader: &mut R) -> Result<(), RevisionError> {
		Self::deserialize_revisioned(reader).map(drop)
	}
}

#[cfg(test)]
mod tests {
	use surrealdb_strand::Strand;

	use super::*;
	use crate::val::{Array, Datetime, Number, RecordIdKey, TableName};

	fn make_rid(table: &str, key: &str) -> RecordId {
		RecordId {
			table: TableName::new(table),
			key: RecordIdKey::String(Strand::new(key)),
		}
	}

	/// The generation-0 document format.
	const DOC0: RecordFormat = RecordFormat::Document {
		generation: None,
	};

	/// What the generation-0 document format writes for `record`, with no key to leave out.
	fn v1(record: &Record) -> Vec<u8> {
		encode_whole_record(DOC0, record, None).unwrap()
	}

	/// A read of `bytes` that holds no layouts.
	fn read(bytes: &[u8], rid: RecordId) -> Result<Record> {
		Record::kv_decode_value(bytes, RecordContext::without_layouts(rid))
	}

	#[test]
	fn legacy_unit_edge_decodes_to_variant_one() {
		// Records persisted before the variant marker existed wrote
		// `RecordType::Edge` as a rev-1 unit variant. The rev-1 → rev-2
		// upgrade path must map them to `Edge { variant: 1 }` so the
		// purge layer (which dispatches on the variant) treats them as
		// the original adjacency-key layout.
		use revision::{DeserializeRevisioned, SerializeRevisioned, revisioned};

		#[revisioned(revision = 1)]
		#[derive(Debug, PartialEq)]
		enum LegacyRecordType {
			Table,
			Edge,
		}

		let mut bytes = Vec::new();
		LegacyRecordType::Edge.serialize_revisioned(&mut bytes).unwrap();

		let decoded =
			<RecordType as DeserializeRevisioned>::deserialize_revisioned(&mut bytes.as_slice())
				.unwrap();
		assert_eq!(
			decoded,
			RecordType::Edge {
				variant: 1,
			}
		);
	}

	#[test]
	fn current_edge_round_trips_through_metadata() {
		// A `Metadata` containing `Edge { variant: N }` must round-trip
		// through the revisioned encoder/decoder without losing the
		// variant marker. This guards against accidental regressions
		// where Metadata's encoding inadvertently strips struct-variant
		// fields on the RecordType field.
		use revision::{DeserializeRevisioned, SerializeRevisioned};

		let original = Metadata {
			record_type: RecordType::Edge {
				variant: 7,
			},
			aggregation_stats: Vec::new(),
		};
		let mut bytes = Vec::new();
		original.serialize_revisioned(&mut bytes).unwrap();
		let decoded =
			<Metadata as DeserializeRevisioned>::deserialize_revisioned(&mut bytes.as_slice())
				.unwrap();
		assert_eq!(decoded, original);
	}

	#[test]
	fn a_record_encodes_to_its_documented_bytes() {
		// The marker, a header naming the document layout and nothing else, the
		// generation (0: written before the table had one), then the data as a
		// stored value.
		assert_eq!(v1(&Record::new(Value::Bool(true))), [0xFF, 0x00, 0x00, 0x03]);
		assert_eq!(v1(&Record::default()), [0xFF, 0x00, 0x00, 0x00]);
		// An edge on the current adjacency layout sets the EDGE bit and carries no
		// variant byte.
		let edge = Record {
			metadata: Some(Metadata {
				record_type: RecordType::Edge {
					variant: V1_EDGE_VARIANT,
				},
				aggregation_stats: Vec::new(),
			}),
			data: Value::Bool(true),
		};
		assert_eq!(v1(&edge), [0xFF, 0x08, 0x00, 0x03]);
		// Metadata the envelope cannot hold is written in `revision`.
		let table = Record {
			metadata: Some(Metadata {
				record_type: RecordType::Table,
				aggregation_stats: Vec::new(),
			}),
			data: Value::Bool(true),
		};
		assert_eq!(table.kv_encode_value().unwrap(), revision::to_vec(&table).unwrap());
	}

	fn every_stat() -> Vec<AggregationStat> {
		vec![
			AggregationStat::Count {
				count: 3,
			},
			AggregationStat::CountValue {
				arg: 1,
				count: -4,
			},
			AggregationStat::NumberMax {
				arg: 2,
				max: f64::NEG_INFINITY.into(),
			},
			AggregationStat::NumberMin {
				arg: 3,
				min: Number::Decimal("1.50".parse().unwrap()),
			},
			AggregationStat::Sum {
				arg: 4,
				sum: 7.into(),
			},
			AggregationStat::Mean {
				arg: 5,
				sum: 2.5.into(),
				count: 2,
			},
			AggregationStat::StdDev {
				arg: 6,
				shift: 1.0.into(),
				sum: 3.into(),
				sum_of_squares: 9.0.into(),
				count: 3,
			},
			AggregationStat::Variance {
				arg: 7,
				shift: 0.0.into(),
				sum: 0.0.into(),
				sum_of_squares: 0.0.into(),
				count: 0,
			},
			AggregationStat::TimeMax {
				arg: 8,
				max: Datetime::MIN_UTC,
			},
			AggregationStat::TimeMin {
				arg: 9,
				min: Datetime::MAX_UTC,
			},
			AggregationStat::Accumulate {
				arg: 10,
				values: vec![Value::from(1i64), Value::String(Strand::new("x")), Value::None],
			},
			AggregationStat::TemporalMax {
				arg: 11,
				max: Value::Date(crate::val::Date(19_000)),
			},
			AggregationStat::TemporalMin {
				arg: 12,
				min: Value::Timestamp(crate::val::Date(-3).at_midnight()),
			},
		]
	}

	#[test]
	fn metadata_round_trips() {
		let rid = make_rid("view", "a");
		let mut data = Object::default();
		data.0.insert(Strand::new("total"), Value::from(10i64));
		let metadata = [
			None,
			Some(Metadata {
				record_type: RecordType::Table,
				aggregation_stats: Vec::new(),
			}),
			Some(Metadata {
				record_type: RecordType::Edge {
					variant: 1,
				},
				aggregation_stats: Vec::new(),
			}),
			Some(Metadata {
				record_type: RecordType::Edge {
					variant: LATEST_EDGE_VARIANT,
				},
				aggregation_stats: Vec::new(),
			}),
			Some(Metadata {
				record_type: RecordType::Table,
				aggregation_stats: every_stat(),
			}),
			Some(Metadata {
				record_type: RecordType::Edge {
					variant: u16::MAX,
				},
				aggregation_stats: every_stat(),
			}),
		];
		for metadata in metadata {
			for data in [Value::Object(data.clone()), Value::None, Value::from(5i64)] {
				let record = Record {
					metadata: metadata.clone(),
					data,
				};
				let bytes = v1(&record);
				// The envelope holds only metadata its header can state; every other record is
				// written in `revision`, and both read back the same.
				let in_envelope = written_in_envelope(record.metadata.as_ref()).is_some();
				match StoredRecord::parse(&bytes).unwrap() {
					Some(stored) => {
						assert!(in_envelope, "{:?} is written in revision", record.metadata);
						assert_eq!(stored.is_edge(), record.is_edge());
						assert_eq!(stored.edge_variant(), record.edge_variant());
						assert_eq!(stored.metadata(), record.metadata);
					}
					None => {
						assert!(!in_envelope, "{:?} is written in the envelope", record.metadata)
					}
				}
				let mut expected = record.clone();
				if let Value::Object(obj) = &mut expected.data {
					obj.0.insert(Strand::new("id"), Value::RecordId(rid.clone()));
				}
				assert_eq!(read(&bytes, rid.clone()).unwrap(), expected);
			}
		}
	}

	#[test]
	fn the_key_id_is_left_out_and_restored() {
		let rid = make_rid("user", "alice");
		let mut obj = Object::default();
		obj.0.insert(Strand::new("id"), Value::RecordId(rid.clone()));
		obj.0.insert(Strand::new("name"), Value::String(Strand::new("Alice")));
		let record = Record::new(Value::Object(obj.clone()));

		let keyed = encode_record(DOC0, None, &record.data, Some(&rid)).unwrap();
		let unkeyed = v1(&record);
		assert!(keyed.len() < unkeyed.len());
		let stored = StoredRecord::parse(&keyed).unwrap().unwrap();
		assert!(stored.is_keyed());
		assert!(!StoredRecord::parse(&unkeyed).unwrap().unwrap().is_keyed());

		// The bytes hold no id.
		let mut without = obj.clone();
		without.0.remove("id");
		assert_eq!(stored.decode_document().unwrap(), Record::new(Value::Object(without)));
		// Every read through the key restores it.
		assert_eq!(read(&keyed, rid.clone()).unwrap(), record);
		assert_eq!(Record::decode_as_written(&keyed, None, || rid.clone()).unwrap(), record);
		assert_eq!(Record::decode_as_written(&unkeyed, None, || unreachable!()).unwrap(), record);
	}

	#[test]
	fn a_foreign_id_is_stored() {
		// A view row whose data projects its source row's id: the id is not
		// the key's, so it stays in the bytes, and a read as written sees it.
		let view_key = make_rid("high_scores", "2");
		let source_id = make_rid("src", "2");
		let mut obj = Object::default();
		obj.0.insert(Strand::new("id"), Value::RecordId(source_id));
		obj.0.insert(Strand::new("score"), Value::from(20i64));
		let record = Record::new(Value::Object(obj));
		let bytes = encode_record(DOC0, None, &record.data, Some(&view_key)).unwrap();
		assert!(!StoredRecord::parse(&bytes).unwrap().unwrap().is_keyed());
		assert_eq!(Record::decode_as_written(&bytes, None, || unreachable!()).unwrap(), record);
		let read = read(&bytes, view_key.clone()).unwrap();
		let Value::Object(o) = &read.data else {
			panic!("expected Value::Object");
		};
		assert_eq!(o.0.get("id"), Some(&Value::RecordId(view_key)));
		// A view row with no id at all is read with none.
		let bare = Record::new(Value::Object(Object::default()));
		let bytes = encode_record(DOC0, None, &bare.data, Some(&make_rid("v", "1"))).unwrap();
		assert_eq!(Record::decode_as_written(&bytes, None, || unreachable!()).unwrap(), bare);
	}

	#[test]
	fn what_this_build_does_not_know_is_refused() {
		let rid = make_rid("t", "1");
		let cases: Vec<(&str, Vec<u8>)> = vec![
			("a packed layout", vec![0xFF, 0x01, 0x00]),
			("the reserved bit", vec![0xFF, 0x80, 0x00]),
			(
				"a keyed record whose data is not an object",
				vec![0xFF, StoredRecord::KEYED, 0x00, 0x03],
			),
			("no header", vec![0xFF]),
			("no data", vec![0xFF, 0x00, 0x00]),
		];
		for (what, bytes) in cases {
			assert!(read(&bytes, rid.clone()).is_err(), "{what}: {bytes:02x?}");
		}
	}

	#[test]
	fn revision_and_stored_records_decode_alike() {
		let rid = make_rid("user", "alice");
		let mut obj = Object::default();
		obj.0.insert(Strand::new("id"), Value::RecordId(rid.clone()));
		obj.0.insert(Strand::new("tags"), Value::Array(Array(vec![Value::from(1i64)])));
		for metadata in [
			None,
			Some(Metadata {
				record_type: RecordType::Edge {
					variant: 2,
				},
				aggregation_stats: Vec::new(),
			}),
			Some(Metadata {
				record_type: RecordType::Table,
				aggregation_stats: every_stat(),
			}),
		] {
			let record = Record {
				metadata,
				data: Value::Object(obj.clone()),
			};
			let legacy = revision::to_vec(&record).unwrap();
			let stored =
				encode_record(DOC0, record.metadata.as_ref(), &record.data, Some(&rid)).unwrap();
			assert_eq!(StoredRecord::parse(&legacy).unwrap(), None);
			assert_eq!(read(&legacy, rid.clone()).unwrap(), read(&stored, rid.clone()).unwrap());
			assert_eq!(
				Record::decode_as_written(&legacy, None, || rid.clone()).unwrap(),
				Record::decode_as_written(&stored, None, || rid.clone()).unwrap()
			);
		}
	}

	/// Build the on-disk bytes a *legacy* writer would have produced: a
	/// `Record` whose stored `Object` has had its top-level `"id"`
	/// removed (the pre-3.2 encoder stripped it). Used to exercise the
	/// `kv_decode_value` splice path.
	fn legacy_stripped_bytes(obj_without_id: Object) -> Vec<u8> {
		revision::to_vec(&Record::new(Value::Object(obj_without_id))).unwrap()
	}

	#[test]
	fn encode_decode_round_trips_with_id() {
		// Current writers store `id` inline. Encode → decode must return
		// the record unchanged, with `id` present at the top level.
		let rid = make_rid("user", "alice");
		let mut obj = Object::default();
		obj.0.insert(Strand::new("id"), Value::RecordId(rid.clone()));
		obj.0.insert(Strand::new("name"), Value::String(Strand::new("Alice")));
		let original = Record::new(Value::Object(obj));

		let bytes = v1(&original);
		let decoded = read(&bytes, rid.clone()).unwrap();
		assert_eq!(decoded, original);
		let Value::Object(o) = &decoded.data else {
			panic!("expected Value::Object");
		};
		assert_eq!(o.0.get("id"), Some(&Value::RecordId(rid)));
	}

	#[test]
	fn decode_splices_id_for_legacy_stripped_data() {
		// Legacy data lacks the top-level `id`. Decode must synthesise it
		// from the storage key at the correct sorted position. Three
		// bracket cases: id in the middle, at the end, at the front.
		let rid = make_rid("user", "alice");

		// "age" < "id" < "name" → id lands in the middle.
		let mut middle = Object::default();
		middle.0.insert(Strand::new("age"), Value::from(30i64));
		middle.0.insert(Strand::new("name"), Value::String(Strand::new("Alice")));
		let decoded = read(&legacy_stripped_bytes(middle), rid.clone()).unwrap();
		let Value::Object(o) = &decoded.data else {
			panic!("expected Value::Object");
		};
		let keys: Vec<&str> = o.0.iter().map(|(k, _)| k.as_str()).collect();
		assert_eq!(keys, vec!["age", "id", "name"]);
		assert_eq!(o.0.get("id"), Some(&Value::RecordId(rid.clone())));

		// All keys sort before "id" → appended at the end.
		let mut tail = Object::default();
		tail.0.insert(Strand::new("address"), Value::String(Strand::new("123 main")));
		tail.0.insert(Strand::new("age"), Value::from(30i64));
		let decoded = read(&legacy_stripped_bytes(tail), rid.clone()).unwrap();
		let Value::Object(o) = &decoded.data else {
			panic!("expected Value::Object");
		};
		let keys: Vec<&str> = o.0.iter().map(|(k, _)| k.as_str()).collect();
		assert_eq!(keys, vec!["address", "age", "id"]);

		// All keys sort after "id" → prepended at the front.
		let mut head = Object::default();
		head.0.insert(Strand::new("name"), Value::String(Strand::new("Alice")));
		head.0.insert(Strand::new("zip"), Value::from(12345i64));
		let decoded = read(&legacy_stripped_bytes(head), rid).unwrap();
		let Value::Object(o) = &decoded.data else {
			panic!("expected Value::Object");
		};
		let keys: Vec<&str> = o.0.iter().map(|(k, _)| k.as_str()).collect();
		assert_eq!(keys, vec!["id", "name", "zip"]);
	}

	#[test]
	fn decode_round_trips_matching_id() {
		// When the stored `id` matches the storage key (the normal case),
		// decode returns the record unchanged.
		let rid = make_rid("user", "alice");
		let mut obj = Object::default();
		obj.0.insert(Strand::new("id"), Value::RecordId(rid.clone()));
		obj.0.insert(Strand::new("name"), Value::String(Strand::new("Alice")));
		let record = Record::new(Value::Object(obj));

		let bytes = revision::to_vec(&record).unwrap();
		let decoded = read(&bytes, rid).unwrap();
		assert_eq!(decoded, record);
	}

	#[test]
	fn decode_overwrites_id_with_storage_key() {
		// The storage key is the source of truth for identity. When the
		// stored `id` differs from the key — as for a materialised view
		// `DEFINE TABLE v AS SELECT id, .. FROM src`, which projects the
		// *source* row's id into the view record stored under `v:..` — the
		// decoded `id` must be the key, not the stored (foreign) id.
		let view_key = make_rid("high_scores", "2");
		let source_id = make_rid("src", "2");
		let mut obj = Object::default();
		// Stored data carries the projected source id, as the view writer
		// persists it.
		obj.0.insert(Strand::new("id"), Value::RecordId(source_id));
		obj.0.insert(Strand::new("name"), Value::String(Strand::new("b")));
		obj.0.insert(Strand::new("score"), Value::from(20i64));
		let stored = Record::new(Value::Object(obj));

		let bytes = revision::to_vec(&stored).unwrap();
		let decoded = read(&bytes, view_key.clone()).unwrap();
		let Value::Object(o) = &decoded.data else {
			panic!("expected Value::Object");
		};
		assert_eq!(o.0.get("id"), Some(&Value::RecordId(view_key)));
	}

	#[test]
	fn nested_id_is_preserved() {
		// Nested objects with their own `id` field must round-trip intact;
		// the top-level splice only touches the outermost object.
		let rid = make_rid("user", "alice");

		let mut profile = Object::default();
		profile.0.insert(Strand::new("id"), Value::String(Strand::new("profile:7")));
		profile.0.insert(Strand::new("bio"), Value::String(Strand::new("hello")));

		let mut friend1 = Object::default();
		friend1.0.insert(Strand::new("id"), Value::String(Strand::new("user:bob")));
		let mut friend2 = Object::default();
		friend2.0.insert(Strand::new("id"), Value::String(Strand::new("user:carol")));

		let friends = Array(vec![Value::Object(friend1.clone()), Value::Object(friend2.clone())]);

		let mut top = Object::default();
		top.0.insert(Strand::new("id"), Value::RecordId(rid.clone()));
		top.0.insert(Strand::new("profile"), Value::Object(profile.clone()));
		top.0.insert(Strand::new("friends"), Value::Array(friends));

		let record = Record::new(Value::Object(top));

		let bytes = v1(&record);
		let decoded = read(&bytes, rid.clone()).unwrap();

		let Value::Object(o) = &decoded.data else {
			panic!("expected Value::Object");
		};
		assert_eq!(o.0.get("id"), Some(&Value::RecordId(rid)));
		let Some(Value::Object(p)) = o.0.get("profile") else {
			panic!("expected nested profile object");
		};
		assert_eq!(p.0.get("id"), Some(&Value::String(Strand::new("profile:7"))));

		let Some(Value::Array(arr)) = o.0.get("friends") else {
			panic!("expected friends array");
		};
		let v: Vec<_> = arr.iter().collect();
		let Value::Object(f0) = v[0] else {
			panic!("expected nested friend object");
		};
		assert_eq!(f0.0.get("id"), Some(&Value::String(Strand::new("user:bob"))));
		let Value::Object(f1) = v[1] else {
			panic!("expected nested friend object");
		};
		assert_eq!(f1.0.get("id"), Some(&Value::String(Strand::new("user:carol"))));
	}

	#[test]
	fn encode_round_trips_with_divergent_key_orderings() {
		// Keys `"name"` (varint length 4) and `"zip"` (varint length 3)
		// have opposite K::Ord and wire-byte orderings: K::Ord puts
		// `"name"` before `"zip"`, while the rev-2 indexed-map body sorts
		// by wire bytes (length-prefixed), putting `"zip"` first. The
		// deserialiser re-sorts by K::Ord on read, so the round-trip
		// preserves the record's logical content and key order.
		let rid = make_rid("user", "alice");
		let mut obj = Object::default();
		obj.0.insert(Strand::new("id"), Value::RecordId(rid.clone()));
		obj.0.insert(Strand::new("name"), Value::String(Strand::new("Alice")));
		obj.0.insert(Strand::new("zip"), Value::from(12345i64));
		let original = Record::new(Value::Object(obj));

		let bytes = v1(&original);
		let decoded = read(&bytes, rid).unwrap();
		assert_eq!(decoded, original);
		let Value::Object(o) = decoded.data else {
			panic!("expected Value::Object");
		};
		let keys: Vec<&str> = o.0.iter().map(|(k, _)| k.as_str()).collect();
		assert_eq!(keys, vec!["id", "name", "zip"]);
	}
}
