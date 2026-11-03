//! Output frames: result rows encoded straight into a host-supplied buffer.
//!
//! A [`RowFrame`] holds rows as concatenated CBOR items with an offsets table,
//! so a host can route, split or concatenate a frame without decoding it.
//! Each row's bytes are exactly what the RPC layer's CBOR encoder
//! (`rpc::format::cbor::encode`) writes for the public value the row converts
//! to (`convert_value_to_public_value`): the same tags, the same shortest
//! integer and float forms, object keys in the same order.
//!
//! Rows reach a frame two ways, neither of which builds the public value:
//!
//! - [`RowFrame::push_value`] encodes an in-memory [`Value`];
//! - [`RowFrame::push_stored`] transcodes a stored record — its key and its stored value, in the
//!   stored-value encoding or in `revision` — reading the stored bytes in place. Objects, arrays,
//!   strings, integers and the other leaves are copied from where they lie; a value of a kind the
//!   transcoder does not read in place is decoded alone and encoded as an in-memory value. A packed
//!   row is read column by column through its generation's descriptor, its `id` from the key.
//!
//! A frame's buffers are the host's: [`RowFrame::clear`] keeps their capacity,
//! so a frame reused across batches encodes in steady state without asking
//! the global allocator for anything. The transcoder's scratch (the order in
//! which an object's stored entries are written) lives in the frame too, for
//! the same reason.

use std::fmt::Write as _;
#[cfg(feature = "shaped")]
use std::sync::Arc;

use revision::optimised::IndexedSeqWalker;
use revision::optimised::tag::{SizeClass, Tag};
use surrealdb_rpc::{EncodedRows, RowFormat};
#[cfg(feature = "shaped")]
use surrealdb_types::shaped::{Shape, ValueShaper};

use super::scalar::{Held, Scalar};
use crate::catalog::packed::{FieldRef, PackedRow, PackedVisitor};
use crate::catalog::{StoredRecord, TableLayouts};
use crate::exec::object_extract::indexed_map_walker_from_object_bytes;
use crate::exec::object_extract::wire_skip::{rev2_optimised_payload_unchecked, skip_value_wire};
use crate::exec::pre_decode_filter::strand_payload_str;
use crate::key::KVKeyDecode as _;
use crate::val::key_read::KeyIdRef;
use crate::val::stored::{Kind, ObjectRef, OrderedObjectRef, ValueRef};
use crate::val::{Number, RecordIdKey, Value};

// CBOR tags the RPC layer's encoder writes (`surrealdb_types::cbor`).
const TAG_SPEC_UUID: u64 = 37;
const TAG_NONE: u64 = 6;
const TAG_TABLE: u64 = 7;
const TAG_RECORDID: u64 = 8;
const TAG_STRING_DECIMAL: u64 = 10;
const TAG_CUSTOM_DATETIME: u64 = 12;
const TAG_CUSTOM_DURATION: u64 = 14;

// Sized number tags (IANA unassigned 180..=188)
const TAG_I8: u64 = 180;
const TAG_U8: u64 = 181;
const TAG_I16: u64 = 182;
const TAG_U16: u64 = 183;
const TAG_I32: u64 = 184;
const TAG_U32: u64 = 185;
const TAG_U64: u64 = 186;
const TAG_F16: u64 = 187;
const TAG_F32: u64 = 188;
const TAG_BIGDECIMAL: u64 = 192;

// CBOR major types.
const MAJOR_POSITIVE: u8 = 0;
const MAJOR_NEGATIVE: u8 = 1;
const MAJOR_BYTES: u8 = 2;
const MAJOR_TEXT: u8 = 3;
const MAJOR_ARRAY: u8 = 4;
const MAJOR_MAP: u8 = 5;
const MAJOR_TAG: u8 = 6;

/// rev-2 [`Value`] variant ids, in the declaration order of `Value`.
mod variant {
	pub(super) const STRING: u8 = 4;
	pub(super) const ARRAY: u8 = 8;
	pub(super) const OBJECT: u8 = 10;
	pub(super) const RECORD_ID: u8 = 14;
}

#[cfg(test)]
thread_local! {
	/// How many stored records [`RowFrame::push_stored`] decoded whole rather
	/// than transcoding in place.
	static WHOLE_DECODES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
	/// How many stored values the transcoder decoded whole into a [`Value`].
	static VALUE_DECODES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
	/// How many packed rows [`RowFrame::push_stored`] transcoded by visiting them, neither checked
	/// whole nor decoded.
	static PACKED_VISITS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// What [`RowFrame::push_stored`] did with the stored records it was given on this thread.
#[cfg(test)]
pub(super) mod observed {
	/// The packed rows transcoded by visiting them, neither checked whole nor decoded.
	pub(in crate::exec::vm) fn packed_visits() -> usize {
		super::PACKED_VISITS.with(std::cell::Cell::get)
	}

	/// The stored records decoded whole rather than transcoded in place.
	pub(in crate::exec::vm) fn whole_decodes() -> usize {
		super::WHOLE_DECODES.with(std::cell::Cell::get)
	}
}

/// Why a shaped row is refused by a build without shaped frames, which never offers them.
#[cfg(not(feature = "shaped"))]
const SHAPED_NOT_BUILT: &str = "this build does not serve shaped frames";

/// The record field a stored record's key supplies.
const ID_KEY: &str = "id";

/// The most a frame reserves ahead for the rows it expects ([`RowFrame::expecting`]); past it, a
/// frame grows as its rows arrive.
const MAX_EXPECTED_RESERVE: usize = 16 << 20;

/// Rows encoded as CBOR items back to back, or as JSON texts joined by `,`,
/// with where each ends.
#[derive(Debug)]
pub(crate) struct RowFrame {
	format: RowFormat,
	/// `head` bytes of headroom, then the rows.
	data: Vec<u8>,
	/// How many bytes in front of the rows are free: for CBOR rows, room for the response
	/// written around them ([`EncodedRows::wrap`]); none for other rows.
	head: usize,
	/// Where each row ends, past the first row's start.
	ends: Vec<u32>,
	/// How many rows the frame is expected to hold before they are taken: once the first row is
	/// written, the frame reserves room for this many rows of its size, so that a frame filled
	/// row by row is not grown, and copied, a doubling at a time.
	expected_rows: usize,
	/// The capacities of `data` and `ends` when their rows were last taken, which the next row
	/// reserves: rows taken in batches are written into buffers of the size the last batch
	/// needed, and a frame taken for the last time leaves no buffer behind.
	taken_capacity: (usize, usize),
	/// A reused buffer for the text renderings JSON rows hold.
	text: String,
	/// An object's stored entries, as offsets into the stored value being
	/// transcoded, in the order they are written. Each nested object uses
	/// the tail past its parent's entries.
	scratch: Vec<Entry>,
	/// A reused buffer for a record key's string id that holds an escaped byte, unescaped.
	id_text: Vec<u8>,
	/// For shaped rows, the shapes they are written in.
	#[cfg(feature = "shaped")]
	shaped: Option<Box<Shaped>>,
}

#[cfg(feature = "shaped")]
/// The shaped rows of a frame: which shape each is written in, the shaper value rows go through,
/// and the copy plans packed rows run.
#[derive(Debug, Default)]
struct Shaped {
	shaper: ValueShaper,
	shapes: Vec<Arc<Shape>>,
	row_shapes: Vec<u16>,
	/// The copy plan of each (table, generation) a packed row of this frame has been written in.
	plans: Vec<(String, crate::catalog::Generation, Arc<crate::exec::shaped::CopyPlan>)>,
	/// Which ops of a copy plan wrote a present column, for the row being written.
	present: Vec<bool>,
	/// The index in `plans` of the plan the last packed row ran.
	last_plan: usize,
	/// The layouts (by address) and generation of the last packed row, with its plan's index: a
	/// row of the same layouts and generation runs that plan, its generation already admitted.
	run: Option<(usize, crate::catalog::Generation, usize)>,
	/// The shape documents are written in: their `id`, and every other entry in the root's
	/// remainder.
	document: Option<Arc<Shape>>,
	/// The statement's seed shape, whose declared arrays the copy plans hold natively.
	seed: Option<Arc<Shape>>,
	/// The table of the packed rows last written and the bytes its record keys share before the
	/// id: a key that starts with them is of that table, and only its id is read.
	key_prefix: (String, Vec<u8>),
}

#[cfg(feature = "shaped")]
impl Shaped {
	/// Records that the row just written is in `shape`.
	fn note(&mut self, shape: &Arc<Shape>) -> anyhow::Result<()> {
		note_in(&mut self.shapes, &mut self.row_shapes, shape)
	}

	/// The index in [`Self::plans`] of the copy plan of generation `generation` of `table`, whose
	/// descriptor is `descriptor`, built the first time it is asked for.
	fn plan(
		&mut self,
		table: &str,
		generation: crate::catalog::Generation,
		descriptor: &crate::catalog::layout::PackedDescriptor,
	) -> anyhow::Result<usize> {
		let matches =
			|(t, g, _): &(String, crate::catalog::Generation, _)| *g == generation && t == table;
		if self.plans.get(self.last_plan).is_some_and(matches) {
			return Ok(self.last_plan);
		}
		if let Some(at) = self.plans.iter().position(matches) {
			self.last_plan = at;
			return Ok(at);
		}
		let plan =
			Arc::new(crate::exec::shaped::copy_plan(descriptor, table, self.seed.as_deref())?);
		self.plans.push((table.to_owned(), generation, plan));
		self.last_plan = self.plans.len() - 1;
		Ok(self.last_plan)
	}
}

#[cfg(feature = "shaped")]
/// Records in `row_shapes` that the row just written is in `shape`, adding it to `shapes`.
fn note_in(
	shapes: &mut Vec<Arc<Shape>>,
	row_shapes: &mut Vec<u16>,
	shape: &Arc<Shape>,
) -> anyhow::Result<()> {
	let at = match shapes.iter().rposition(|s| Arc::ptr_eq(s, shape)) {
		Some(at) => at,
		None => {
			shapes.push(Arc::clone(shape));
			shapes.len() - 1
		}
	};
	row_shapes.push(u16::try_from(at)?);
	Ok(())
}

/// One stored object entry: where its key's UTF-8 and its value wire lie,
/// relative to the start of the stored value being transcoded.
#[derive(Debug, Clone, Copy)]
pub(super) struct Entry {
	key: (u32, u32),
	value: (u32, u32),
}

/// The byte range of a `(start, len)` span, summed in `usize` so it cannot wrap.
fn span_range((start, len): (u32, u32)) -> std::ops::Range<usize> {
	let start = start as usize;
	start..start.saturating_add(len as usize)
}

impl Default for RowFrame {
	fn default() -> Self {
		Self::with_format(RowFormat::Cbor)
	}
}

impl RowFrame {
	pub(crate) fn new() -> Self {
		Self::default()
	}

	/// A frame whose rows are encoded in `format`.
	pub(crate) fn with_format(format: RowFormat) -> Self {
		Self {
			format,
			data: Vec::new(),
			head: 0,
			ends: Vec::new(),
			expected_rows: 0,
			taken_capacity: (0, 0),
			text: String::new(),
			scratch: Vec::new(),
			id_text: Vec::new(),
			#[cfg(feature = "shaped")]
			shaped: (format == RowFormat::Shaped).then(Box::default),
		}
	}

	/// The frame, expecting `rows` rows before they are taken: once its first row is written it
	/// reserves room for that many rows of the size.
	pub(crate) fn expecting(mut self, rows: usize) -> Self {
		self.expected_rows = rows;
		self
	}

	#[cfg(feature = "shaped")]
	/// For shaped rows, the seed shape value rows whose `id` names `table` start from.
	pub(crate) fn seed(&mut self, table: &str, seed: Arc<Shape>) {
		if let Some(shaped) = &mut self.shaped {
			shaped.shaper = ValueShaper::with_seed(table.to_owned(), Arc::clone(&seed));
			shaped.seed = Some(seed);
		}
	}

	/// The frame's rows, handed on as [`EncodedRows`]; the frame is left
	/// empty, and its next row reserves buffers of the capacity these had.
	pub(crate) fn take_encoded(&mut self) -> EncodedRows {
		let (data_capacity, ends_capacity) = (self.data.capacity(), self.ends.capacity());
		self.taken_capacity = (data_capacity, ends_capacity);
		let bytes = std::mem::take(&mut self.data);
		let head = std::mem::take(&mut self.head);
		let ends = std::mem::take(&mut self.ends);
		#[cfg(feature = "shaped")]
		if let Some(shaped) = &mut self.shaped {
			let shapes = std::mem::take(&mut shaped.shapes);
			let row_shapes =
				std::mem::replace(&mut shaped.row_shapes, Vec::with_capacity(ends_capacity));
			return EncodedRows::shaped(
				bytes,
				ends,
				shapes,
				row_shapes,
				crate::rpc::format::decode_rows,
			);
		}
		EncodedRows::with_headroom(self.format, bytes, head, ends, crate::rpc::format::decode_rows)
	}

	/// How many rows the frame holds.
	pub(crate) fn len(&self) -> usize {
		self.ends.len()
	}

	/// Row `at`'s CBOR bytes.
	#[cfg(test)]
	pub(crate) fn row(&self, at: usize) -> &[u8] {
		let start = match at {
			0 => 0,
			_ if self.format == RowFormat::Json => self.ends[at - 1] as usize + 1,
			_ => self.ends[at - 1] as usize,
		};
		&self.data()[start..self.ends[at] as usize]
	}

	/// Every row's bytes, back to back.
	pub(crate) fn data(&self) -> &[u8] {
		&self.data[self.head..]
	}

	/// Drop every row, keeping the buffers' capacity.
	pub(crate) fn clear(&mut self) {
		self.data.clear();
		self.head = 0;
		self.ends.clear();
		#[cfg(feature = "shaped")]
		if let Some(shaped) = &mut self.shaped {
			shaped.shapes.clear();
			shaped.row_shapes.clear();
		}
	}

	/// Reserve room for the rows the frame expects, each taken to be `row` bytes long, within
	/// [`MAX_EXPECTED_RESERVE`].
	fn reserve_expected(&mut self, row: usize) {
		let want = row.saturating_mul(self.expected_rows).min(MAX_EXPECTED_RESERVE);
		let free = self.data.capacity() - self.data.len();
		if want > free {
			self.data.reserve(want);
		}
		self.ends.reserve(self.expected_rows.saturating_sub(self.ends.len()));
	}

	/// Run `write` as the next row, which it leaves whole or, on an error,
	/// not at all.
	fn row_with(
		&mut self,
		write: impl FnOnce(&mut Self) -> anyhow::Result<()>,
	) -> anyhow::Result<()> {
		if self.data.is_empty() {
			if self.data.capacity() == 0 {
				let (data, ends) = std::mem::take(&mut self.taken_capacity);
				self.data.reserve(data);
				self.ends.reserve(ends);
			}
			if matches!(self.format, RowFormat::Cbor | RowFormat::Cbor2) {
				self.head = crate::rpc::format::encoded::ROW_HEADROOM;
				self.data.resize(self.head, 0);
			}
		}
		let start = self.data.len();
		if self.format == RowFormat::Json && !self.ends.is_empty() {
			self.data.push(b',');
		}
		match write(self) {
			Ok(()) => {
				let end = u32::try_from(self.data.len() - self.head)
					.map_err(|_| anyhow::anyhow!("an output frame larger than 4 GiB"))?;
				self.ends.push(end);
				if self.ends.len() == 1 && self.expected_rows > 1 {
					self.reserve_expected(self.data.len() - start);
				}
				Ok(())
			}
			Err(e) => {
				self.data.truncate(start);
				#[cfg(feature = "shaped")]
				if let Some(shaped) = &mut self.shaped {
					shaped.row_shapes.truncate(self.ends.len());
				}
				Err(e)
			}
		}
	}

	/// Append `value` as a row.
	pub(crate) fn push_value(&mut self, value: &Value) -> anyhow::Result<()> {
		self.row_with(|frame| match frame.format {
			RowFormat::Cbor => encode_value(&mut frame.data, value, 1, false),
			RowFormat::Cbor2 => encode_value(&mut frame.data, value, 1, true),
			RowFormat::Json => {
				super::row_json::encode_json(&mut frame.data, value, 1, &mut frame.text)
			}
			#[cfg(feature = "shaped")]
			RowFormat::Shaped => frame.shaped_value(super::row_shaped::CoreShaped::Value(value)),
			#[cfg(not(feature = "shaped"))]
			RowFormat::Shaped => anyhow::bail!(SHAPED_NOT_BUILT),
		})
	}

	#[cfg(feature = "shaped")]
	/// Write `value` as a shaped row.
	fn shaped_value(&mut self, value: super::row_shaped::CoreShaped<'_>) -> anyhow::Result<()> {
		let Some(shaped) = &mut self.shaped else {
			anyhow::bail!("a shaped row in a frame of another format");
		};
		let shape = shaped.shaper.write(value, &mut self.data)?;
		shaped.note(&shape)
	}

	/// Append the object of `fields` as a row: what [`Self::push_value`]
	/// appends for the object holding each value under its key. `fields`
	/// holds each key once, in ascending order of its text.
	pub(crate) fn push_fields(&mut self, fields: &[(&str, Scalar<'_>)]) -> anyhow::Result<()> {
		debug_assert!(fields.windows(2).all(|w| w[0].0 < w[1].0), "fields in key order");
		#[cfg(feature = "shaped")]
		if self.format == RowFormat::Shaped {
			return self.row_with(|frame| {
				frame.shaped_value(super::row_shaped::CoreShaped::Fields(fields))
			});
		}
		self.row_with(|frame| {
			let RowFrame {
				format,
				data,
				text: text_buf,
				..
			} = frame;
			match format {
				RowFormat::Shaped => anyhow::bail!("shaped fields are written as a value"),
				RowFormat::Cbor => {
					head(data, MAJOR_MAP, fields.len() as u64);
					for (key, value) in fields {
						text(data, key);
						encode_scalar(data, None, value, 2, false)?;
					}
				}
				RowFormat::Cbor2 => {
					head(data, MAJOR_MAP, fields.len() as u64);
					for (key, value) in fields {
						text(data, key);
						encode_scalar(data, None, value, 2, true)?;
					}
				}
				RowFormat::Json => {
					data.push(b'{');
					for (at, (key, value)) in fields.iter().enumerate() {
						if at > 0 {
							data.push(b',');
						}
						super::row_json::json_string(data, key)?;
						data.push(b':');
						encode_scalar(data, Some(text_buf), value, 2, false)?;
					}
					data.push(b'}');
				}
			}
			Ok(())
		})
	}

	/// Append the stored record `key` names, whose stored value is `value`, as
	/// a row: what [`Self::push_value`] appends for the record decoded, with
	/// its `id` taken from the key.
	///
	/// The caller has admitted `value`'s generation against its table's layouts
	/// ([`crate::kvs::RowGate`]) and passes the layouts that admitted it. A
	/// document row is checked whole and transcoded in place. A packed row is
	/// visited through its generation's descriptor in `layouts`
	/// ([`PackedRow::visit`]): its columns are copied from where they lie and its
	/// `id` from the key, and it is not checked whole, its writer having checked
	/// it. A packed row the visit refuses, or one given without layouts, is
	/// decoded whole, which raises the error a client sees. An envelope this build
	/// does not read fails with its typed error.
	pub(crate) fn push_stored(
		&mut self,
		key: &[u8],
		value: &[u8],
		layouts: Option<&TableLayouts>,
	) -> anyhow::Result<()> {
		#[cfg(feature = "shaped")]
		if self.format == RowFormat::Shaped {
			return self.row_with(|frame| frame.shaped_stored(key, value, layouts));
		}
		#[cfg(not(feature = "shaped"))]
		if self.format == RowFormat::Shaped {
			anyhow::bail!(SHAPED_NOT_BUILT);
		}
		self.row_with(|frame| {
			let RowFrame {
				format,
				data,
				scratch,
				text,
				id_text,
				..
			} = frame;
			let mut stored = Stored {
				base: value,
				out: data,
				scratch,
				id_text,
				json: (*format == RowFormat::Json).then_some(text),
				v2: *format == RowFormat::Cbor2,
			};
			let record = StoredRecord::parse(value)?;
			if let Some(record) = &record
				&& record.is_packed()
				&& let Some(layouts) = layouts
			{
				let start = stored.out.len();
				if stored.packed(key, record, layouts).is_ok() {
					#[cfg(test)]
					PACKED_VISITS.with(|n| n.set(n.get() + 1));
					return Ok(());
				}
				stored.out.truncate(start);
			}
			// A document the whole decode refuses falls through to that decode, which reports it.
			if let Some(record) = record
				&& let Ok(Some(document)) = record.admit_document(layouts)
				&& let Some(object) = document.value()?.as_object()?.map(ObjectRef::validated)
			{
				return stored.stored_object(object, Some(key), 1);
			}
			if let Some(data) = crate::exec::object_extract::rev2_record_data_bytes(value)
				&& let Some(object) = varlen_body(data, variant::OBJECT)
				&& stored.object(object, Some(key), 1)?
			{
				return Ok(());
			}
			#[cfg(test)]
			WHOLE_DECODES.with(|n| n.set(n.get() + 1));
			let decoded =
				crate::exec::operators::scan::pipeline::decode_record_value(key, value, layouts)?;
			stored.encoded(&decoded, 1)
		})
	}
}

#[cfg(feature = "shaped")]
impl RowFrame {
	/// [`Self::push_stored`] for shaped rows: a packed row runs its generation's copy plan; any
	/// other record, and a packed row the plan refuses, is decoded whole and written as a value.
	fn shaped_stored(
		&mut self,
		key: &[u8],
		value: &[u8],
		layouts: Option<&TableLayouts>,
	) -> anyhow::Result<()> {
		let RowFrame {
			data,
			scratch,
			id_text,
			shaped,
			..
		} = self;
		let Some(shaped) = shaped else {
			anyhow::bail!("a shaped row in a frame of another format");
		};
		let record = StoredRecord::parse(value)?;
		if let Some(record) = &record
			&& record.is_packed()
			&& let Some(layouts) = layouts
		{
			let start = data.len();
			let written = (|| -> anyhow::Result<usize> {
				let Ok(Some(generation)) = record.generation() else {
					anyhow::bail!("a packed row whose stamp names no generation");
				};
				let table = layouts.table().as_str();
				let (prefix_table, prefix) = &shaped.key_prefix;
				let id = if prefix_table == table && !prefix.is_empty() && key.starts_with(prefix) {
					super::row_packed::read_key_id_at(key, prefix.len())
						.ok_or_else(|| anyhow::anyhow!("a record key that does not decode"))?
				} else {
					let Some((key_table, id)) = super::row_packed::read_key_id(key) else {
						anyhow::bail!("a record key that does not decode");
					};
					if !key_table.iter().eq(table.as_bytes()) {
						anyhow::bail!("a record read through another table's layouts");
					}
					if let Some(at) = super::row_packed::key_id_start(key) {
						shaped.key_prefix = (table.to_owned(), key[..at].to_vec());
					}
					id
				};
				let address = std::ptr::from_ref(layouts) as usize;
				let at = match shaped.run {
					Some((held, held_generation, at))
						if held == address && held_generation == generation =>
					{
						at
					}
					_ => {
						let descriptor = layouts.admit_packed(generation)?;
						let at = shaped.plan(table, generation, descriptor)?;
						shaped.run = Some((address, generation, at));
						at
					}
				};
				let row = PackedRow::new(record, &shaped.plans[at].2.descriptor)?;
				super::row_shaped::write_packed(
					&shaped.plans[at].2,
					&row,
					key,
					&id,
					value,
					data,
					scratch,
					&mut shaped.present,
					id_text,
				)?;
				Ok(at)
			})();
			match written {
				Ok(at) => {
					#[cfg(test)]
					PACKED_VISITS.with(|n| n.set(n.get() + 1));
					let shape = &shaped.plans[at].2.shape;
					shaped.shaper.set_current(shape);
					return note_in(&mut shaped.shapes, &mut shaped.row_shapes, shape);
				}
				Err(_) => data.truncate(start),
			}
		}
		// A document is written in the document shape, its entries transcoded in place; one the
		// whole decode refuses falls through to that decode, which reports it.
		if let Some(record) = record
			&& let Ok(Some(document)) = record.admit_document(layouts)
			&& let Some(object) = document.value()?.as_object()?.map(ObjectRef::validated)
		{
			let shape = match &shaped.document {
				Some(shape) => Arc::clone(shape),
				None => {
					let shape = super::row_shaped::document_shape()?;
					shaped.document = Some(Arc::clone(&shape));
					shape
				}
			};
			let mut stored = Stored {
				base: value,
				out: data,
				scratch,
				id_text,
				json: None,
				v2: true,
			};
			super::row_shaped::write_document(&shape, key, object, &mut stored)?;
			return shaped.note(&shape);
		}
		#[cfg(test)]
		WHOLE_DECODES.with(|n| n.set(n.get() + 1));
		let decoded =
			crate::exec::operators::scan::pipeline::decode_record_value(key, value, layouts)?;
		let shape = shaped.shaper.write(super::row_shaped::CoreShaped::Value(&decoded), data)?;
		shaped.note(&shape)
	}
}

// --------------------------------------------------
// CBOR primitives
// --------------------------------------------------

/// A CBOR head: major type `major` with argument `n`, in its shortest form.
#[inline]
pub(super) fn head(out: &mut Vec<u8>, major: u8, n: u64) {
	let m = major << 5;
	if n <= 23 {
		out.push(m | n as u8);
	} else if n <= u8::MAX as u64 {
		out.extend_from_slice(&[m | 24, n as u8]);
	} else if n <= u16::MAX as u64 {
		out.push(m | 25);
		out.extend_from_slice(&(n as u16).to_be_bytes());
	} else if n <= u32::MAX as u64 {
		out.push(m | 26);
		out.extend_from_slice(&(n as u32).to_be_bytes());
	} else {
		out.push(m | 27);
		out.extend_from_slice(&n.to_be_bytes());
	}
}

#[inline]
fn int(out: &mut Vec<u8>, v: i64) {
	if v < 0 {
		head(out, MAJOR_NEGATIVE, v as u64 ^ !0);
	} else {
		head(out, MAJOR_POSITIVE, v as u64);
	}
}

#[inline]
pub(super) fn text(out: &mut Vec<u8>, s: &str) {
	head(out, MAJOR_TEXT, s.len() as u64);
	out.extend_from_slice(s.as_bytes());
}

#[inline]
fn bytes(out: &mut Vec<u8>, b: &[u8]) {
	head(out, MAJOR_BYTES, b.len() as u64);
	out.extend_from_slice(b);
}

#[inline]
fn tag(out: &mut Vec<u8>, t: u64) {
	head(out, MAJOR_TAG, t);
}

/// A float in the shortest of half, single and double precision that holds
/// it bit for bit, as the RPC layer's encoder writes it.
fn float(out: &mut Vec<u8>, v: f64) {
	if let Some(half) = f16_bits(v) {
		out.push(0xf9);
		out.extend_from_slice(&half.to_be_bytes());
		return;
	}
	let single = v as f32;
	if f64::from(single).to_bits() == v.to_bits() {
		out.push(0xfa);
		out.extend_from_slice(&single.to_be_bytes());
		return;
	}
	out.push(0xfb);
	out.extend_from_slice(&v.to_be_bytes());
}

/// The half-precision bits of `v` when converting it to half precision and
/// back gives the same bits.
fn f16_bits(v: f64) -> Option<u16> {
	let bits = v.to_bits();
	let sign = ((bits >> 48) & 0x8000) as u16;
	let exp = ((bits >> 52) & 0x7ff) as i32;
	let man = bits & 0x000f_ffff_ffff_ffff;
	if exp == 0x7ff {
		if man == 0 {
			return Some(sign | 0x7c00);
		}
		// A NaN round-trips only when it is quiet and its payload lies in the
		// mantissa bits a half keeps.
		if man & (1 << 51) == 0 || man & ((1 << 42) - 1) != 0 {
			return None;
		}
		return Some(sign | 0x7c00 | ((man >> 42) as u16 & 0x3ff));
	}
	if exp == 0 && man == 0 {
		return Some(sign);
	}
	if exp == 0 {
		// A double subnormal is far below a half's smallest subnormal.
		return None;
	}
	let unbiased = exp - 1023;
	if unbiased > 15 {
		return None;
	}
	if unbiased >= -14 {
		// A normal half keeps 10 of the 52 mantissa bits.
		if man & ((1 << 42) - 1) != 0 {
			return None;
		}
		return Some(sign | (((unbiased + 15) as u16) << 10) | (man >> 42) as u16);
	}
	// A subnormal half: `value = m * 2^-24` for an integer `m` in `1..1024`.
	let shift = -14 - unbiased; // 1 ..
	if shift > 10 {
		return None;
	}
	let full = (1u64 << 52) | man; // the significand with its implicit bit
	let drop = 42 + shift as u32;
	if full & ((1u64 << drop) - 1) != 0 {
		return None;
	}
	Some(sign | (full >> drop) as u16)
}

/// A `fmt::Write` into a fixed buffer on the stack.
struct StackText {
	buf: [u8; 64],
	len: usize,
}

impl std::fmt::Write for StackText {
	fn write_str(&mut self, s: &str) -> std::fmt::Result {
		let end = self.len + s.len();
		let dst = self.buf.get_mut(self.len..end).ok_or(std::fmt::Error)?;
		dst.copy_from_slice(s.as_bytes());
		self.len = end;
		Ok(())
	}
}

/// Write a decimal as CBOR text, formatted on the stack.
fn decimal_text(out: &mut Vec<u8>, d: rust_decimal::Decimal) {
	let mut stack = StackText {
		buf: [0; 64],
		len: 0,
	};
	if write!(stack, "{d}").is_ok() {
		head(out, MAJOR_TEXT, stack.len as u64);
		out.extend_from_slice(&stack.buf[..stack.len]);
	} else {
		text(out, &d.to_string());
	}
}

fn too_deep(depth: usize) -> anyhow::Result<()> {
	if depth > crate::val::MAX_VALUE_DEPTH {
		anyhow::bail!(crate::expr::Error::ValueTooDeep {
			max: crate::val::MAX_VALUE_DEPTH,
		});
	}
	Ok(())
}

// --------------------------------------------------
// In-memory values
// --------------------------------------------------

/// Encode `value`, at nesting depth `depth`, as the RPC layer encodes the
/// public value it converts to.
pub(super) fn encode_value(
	out: &mut Vec<u8>,
	value: &Value,
	depth: usize,
	v2: bool,
) -> anyhow::Result<()> {
	too_deep(depth)?;
	match value {
		Value::None => {
			tag(out, TAG_NONE);
			out.push(0xf6);
		}
		Value::Null => out.push(0xf6),
		Value::Bool(v) => out.push(if *v {
			0xf5
		} else {
			0xf4
		}),
		Value::Number(n) => number(out, n.clone(), v2)?,
		Value::String(s) => text(out, s.as_str()),
		Value::Duration(d) => duration(out, d.0.as_secs(), d.0.subsec_nanos()),
		Value::Datetime(d) => datetime(out, d),
		Value::Uuid(u) => uuid(out, u.0.as_bytes()),
		Value::Array(a) => {
			head(out, MAJOR_ARRAY, a.len() as u64);
			for v in a.iter() {
				encode_value(out, v, depth + 1, v2)?;
			}
		}
		Value::Object(o) => {
			head(out, MAJOR_MAP, o.len() as u64);
			for (k, v) in o.iter() {
				text(out, k.as_str());
				encode_value(out, v, depth + 1, v2)?;
			}
		}
		Value::Bytes(b) => bytes(out, &b.0),
		Value::Table(t) => {
			tag(out, TAG_TABLE);
			text(out, t.as_str());
		}
		Value::RecordId(rid) => {
			encode_record_id(out, rid.table.as_str(), &rid.key, depth, v2, || value.clone())?
		}
		// Kinds rare in result rows go through the public conversion.
		Value::Set(_)
		| Value::Date(_)
		| Value::Time(_)
		| Value::Timestamp(_)
		| Value::Interval(_)
		| Value::Geometry(_)
		| Value::File(_)
		| Value::Regex(_)
		| Value::Range(_)
		| Value::Closure(_) => public(out, value, depth, v2)?,
	}
	Ok(())
}

/// Encode `value` through the public conversion and the RPC layer's encoder:
/// the definition the direct encoding answers to, for the kinds it does not
/// write itself.
#[cold]
fn public(out: &mut Vec<u8>, value: &Value, depth: usize, v2: bool) -> anyhow::Result<()> {
	too_deep(depth)?;
	let converted = crate::val::convert_value_to_public_value(value.clone())?;
	let encoded = if v2 {
		crate::rpc::format::cbor::encode_v2(converted)?
	} else {
		crate::rpc::format::cbor::encode(converted)?
	};
	out.extend_from_slice(&encoded);
	Ok(())
}

#[allow(
	clippy::needless_pass_by_value,
	reason = "Number is not Copy; this takes its operand by value like the arithmetic it feeds"
)]
/// A bigdecimal as `rpc::format::cbor` writes it: under `cbor.2` always tag 192 over its text,
/// so its kind and scale survive; without it tag 10 when it fits a decimal exactly, else plain
/// text.
fn big_decimal(out: &mut Vec<u8>, v: &surrealdb_types::BigDecimal, v2: bool) {
	if v2 {
		tag(out, TAG_BIGDECIMAL);
		text(out, &v.to_string());
		return;
	}
	match rust_decimal::Decimal::try_from(v) {
		Ok(d) => {
			tag(out, TAG_STRING_DECIMAL);
			decimal_text(out, d);
		}
		Err(_) => text(out, &v.to_string()),
	}
}

fn number(out: &mut Vec<u8>, n: Number, v2: bool) -> anyhow::Result<()> {
	match n {
		Number::BigDecimal(v) => big_decimal(out, &v, v2),
		Number::Int(v) => int(out, v),
		Number::Float(v) => float(out, v),
		Number::Decimal(v) => {
			tag(out, TAG_STRING_DECIMAL);
			decimal_text(out, v);
		}
		Number::I8(v) => {
			if v2 {
				tag(out, TAG_I8);
			}
			int(out, i64::from(v));
		}
		Number::U8(v) => {
			if v2 {
				tag(out, TAG_U8);
			}
			int(out, i64::from(v));
		}
		Number::I16(v) => {
			if v2 {
				tag(out, TAG_I16);
			}
			int(out, i64::from(v));
		}
		Number::U16(v) => {
			if v2 {
				tag(out, TAG_U16);
			}
			int(out, i64::from(v));
		}
		Number::I32(v) => {
			if v2 {
				tag(out, TAG_I32);
			}
			int(out, i64::from(v));
		}
		Number::U32(v) => {
			if v2 {
				tag(out, TAG_U32);
			}
			int(out, i64::from(v));
		}
		Number::U64(v) => {
			if v2 {
				tag(out, TAG_U64);
				head(out, MAJOR_POSITIVE, v);
			} else if v <= i64::MAX as u64 {
				head(out, MAJOR_POSITIVE, v);
			} else {
				tag(out, TAG_STRING_DECIMAL);
				decimal_text(out, rust_decimal::Decimal::from(v));
			}
		}
		Number::F16(v) => {
			if v2 {
				tag(out, TAG_F16);
			}
			float(out, v.to_f64());
		}
		Number::F32(v) => {
			if v2 {
				tag(out, TAG_F32);
			}
			float(out, f64::from(v));
		}
	}
	Ok(())
}

fn duration(out: &mut Vec<u8>, secs: u64, nanos: u32) {
	tag(out, TAG_CUSTOM_DURATION);
	match (secs, nanos) {
		(0, 0) => head(out, MAJOR_ARRAY, 0),
		(_, 0) => {
			head(out, MAJOR_ARRAY, 1);
			head(out, MAJOR_POSITIVE, secs);
		}
		_ => {
			head(out, MAJOR_ARRAY, 2);
			head(out, MAJOR_POSITIVE, secs);
			head(out, MAJOR_POSITIVE, nanos as u64);
		}
	}
}

fn datetime(out: &mut Vec<u8>, d: &crate::val::Datetime) {
	tag(out, TAG_CUSTOM_DATETIME);
	head(out, MAJOR_ARRAY, 2);
	int(out, d.0.timestamp());
	int(out, d.0.timestamp_subsec_nanos() as i64);
}

fn uuid(out: &mut Vec<u8>, b: &[u8; 16]) {
	tag(out, TAG_SPEC_UUID);
	bytes(out, b);
}

/// Encode the record id of table `table` and key `key` at depth `depth`;
/// `whole` makes it as a value for the public conversion of a range key.
fn encode_record_id(
	out: &mut Vec<u8>,
	table: &str,
	key: &RecordIdKey,
	depth: usize,
	v2: bool,
	whole: impl FnOnce() -> Value,
) -> anyhow::Result<()> {
	let key_depth = depth + 1;
	too_deep(key_depth)?;
	match key {
		RecordIdKey::Number(n) => record_id(out, table, |out| {
			int(out, *n);
			Ok(())
		}),
		RecordIdKey::String(s) => record_id(out, table, |out| {
			text(out, s.as_str());
			Ok(())
		}),
		RecordIdKey::Uuid(u) => record_id(out, table, |out| {
			uuid(out, u.0.as_bytes());
			Ok(())
		}),
		RecordIdKey::Array(a) => record_id(out, table, |out| {
			head(out, MAJOR_ARRAY, a.len() as u64);
			for v in a.iter() {
				encode_value(out, v, key_depth + 1, v2)?;
			}
			Ok(())
		}),
		RecordIdKey::Object(o) => record_id(out, table, |out| {
			head(out, MAJOR_MAP, o.len() as u64);
			for (k, v) in o.iter() {
				text(out, k.as_str());
				encode_value(out, v, key_depth + 1, v2)?;
			}
			Ok(())
		}),
		RecordIdKey::Range(_) => public(out, &whole(), depth, v2),
	}
}

fn record_id(
	out: &mut Vec<u8>,
	table: &str,
	key: impl FnOnce(&mut Vec<u8>) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
	tag(out, TAG_RECORDID);
	head(out, MAJOR_ARRAY, 2);
	text(out, table);
	key(out)
}

/// Encode `scalar` at depth `depth`: as JSON when given the scratch text
/// buffer JSON renderings use, as CBOR otherwise.
pub(super) fn encode_scalar(
	out: &mut Vec<u8>,
	json: Option<&mut String>,
	scalar: &Scalar<'_>,
	depth: usize,
	v2: bool,
) -> anyhow::Result<()> {
	if let Some(text_buf) = json {
		return super::row_json::encode_json_scalar(out, scalar, depth, text_buf);
	}
	match scalar {
		Scalar::Value(value) => return encode_value(out, value, depth, v2),
		Scalar::None => {
			tag(out, TAG_NONE);
			out.push(0xf6);
		}
		Scalar::Null => out.push(0xf6),
		Scalar::Bool(v) => out.push(if *v {
			0xf5
		} else {
			0xf4
		}),
		Scalar::Number(n) => number(out, n.clone(), v2)?,
		Scalar::Str(s) => text(out, s),
		Scalar::Bytes(b) => bytes(out, b),
		Scalar::Duration(d) => duration(out, d.0.as_secs(), d.0.subsec_nanos()),
		Scalar::Datetime(d) => datetime(out, d),
		Scalar::Uuid(u) => uuid(out, u.0.as_bytes()),
	}
	Ok(())
}

// --------------------------------------------------
// Stored values
// --------------------------------------------------

/// The transcoder over one stored value.
pub(super) struct Stored<'b, 'o> {
	/// The stored value every [`Entry`] offset is relative to.
	pub(super) base: &'b [u8],
	pub(super) out: &'o mut Vec<u8>,
	pub(super) scratch: &'o mut Vec<Entry>,
	/// The scratch an escaped string id is unescaped into.
	pub(super) id_text: &'o mut Vec<u8>,
	/// For JSON rows, the scratch text renderings are written into; `None`
	/// for CBOR.
	pub(super) json: Option<&'o mut String>,
	pub(super) v2: bool,
}

/// The payload of a rev-2 `Value` wire of variant `want`, past its rev, tag
/// and length: the inner type's own wire.
fn varlen_body(wire: &[u8], want: u8) -> Option<&[u8]> {
	let [rev, t, rest @ ..] = wire else {
		return None;
	};
	let t = Tag(*t);
	if *rev != 2 || t.variant_id() != want || !matches!(t.size_class().ok()?, SizeClass::Varlen) {
		return None;
	}
	let len = u32::from_le_bytes(rest.get(..4)?.try_into().ok()?) as usize;
	rest.get(4..4usize.checked_add(len)?)
}

impl<'b> Stored<'b, '_> {
	/// Where `part` lies within the stored value, or `None` when it does not lie within it or
	/// the value is too large for the offsets to hold.
	fn offset(&self, part: &[u8]) -> Option<(u32, u32)> {
		let start = (part.as_ptr() as usize).checked_sub(self.base.as_ptr() as usize)?;
		let end = start.checked_add(part.len())?;
		if end > self.base.len() {
			return None;
		}
		Some((u32::try_from(start).ok()?, u32::try_from(part.len()).ok()?))
	}

	/// The entry whose key UTF-8 is `key` and whose value wire is `value`, or `None` when either
	/// has no [`Self::offset`].
	fn entry(&self, key: &[u8], value: &[u8]) -> Option<Entry> {
		Some(Entry {
			key: self.offset(key)?,
			value: self.offset(value)?,
		})
	}

	/// The bytes an [`Self::offset`] names. An offset is only ever made from a part of `base`, so
	/// its range is within it and its end cannot overflow.
	fn at(&self, span: (u32, u32)) -> &'b [u8] {
		&self.base[span_range(span)]
	}

	/// Write `value`, decoded, at depth `depth`.
	fn encoded(&mut self, value: &Value, depth: usize) -> anyhow::Result<()> {
		match &mut self.json {
			Some(text) => super::row_json::encode_json(self.out, value, depth, text),
			None => encode_value(self.out, value, depth, self.v2),
		}
	}

	/// Write `scalar` at depth `depth`.
	fn scalar(&mut self, scalar: &Scalar<'_>, depth: usize) -> anyhow::Result<()> {
		encode_scalar(self.out, self.json.as_deref_mut(), scalar, depth, self.v2)
	}

	/// Open a map of `count` entries.
	fn open_map(&mut self, count: usize) {
		match self.json {
			Some(_) => self.out.push(b'{'),
			None => head(self.out, MAJOR_MAP, count as u64),
		}
	}

	/// Write the key of the map's `at`th entry.
	fn map_key(&mut self, at: usize, key: &str) -> anyhow::Result<()> {
		match self.json {
			Some(_) => {
				if at > 0 {
					self.out.push(b',');
				}
				super::row_json::json_string(self.out, key)?;
				self.out.push(b':');
			}
			None => text(self.out, key),
		}
		Ok(())
	}

	fn close_map(&mut self) {
		if self.json.is_some() {
			self.out.push(b'}');
		}
	}

	/// Open a list of `len` elements.
	fn open_array(&mut self, len: usize) {
		match self.json {
			Some(_) => self.out.push(b'['),
			None => head(self.out, MAJOR_ARRAY, len as u64),
		}
	}

	/// Separate the list's `at`th element from the one before it.
	fn element(&mut self, at: usize) {
		if self.json.is_some() && at > 0 {
			self.out.push(b',');
		}
	}

	fn close_array(&mut self) {
		if self.json.is_some() {
			self.out.push(b']');
		}
	}

	/// Transcode the value wire `wire` at depth `depth`.
	fn value(&mut self, wire: &'b [u8], depth: usize) -> anyhow::Result<()> {
		too_deep(depth)?;
		if let [2, t, ..] = wire {
			let t = Tag(*t);
			match t.variant_id() {
				variant::STRING => {
					if let Some(s) = strand_payload_str(wire) {
						return self.scalar(&Scalar::Str(s), depth);
					}
				}
				variant::OBJECT => {
					if let Some(object) = varlen_body(wire, variant::OBJECT)
						&& self.object(object, None, depth)?
					{
						return Ok(());
					}
				}
				variant::ARRAY => {
					if let Some(array) = varlen_body(wire, variant::ARRAY)
						&& self.array(array, depth)?
					{
						return Ok(());
					}
				}
				variant::RECORD_ID => {
					if let Some(body) = varlen_body(wire, variant::RECORD_ID)
						&& let Some((table, key)) = stored_record_id(body)
					{
						too_deep(depth + 1)?;
						self.record_id_in_place(table, key)?;
						return Ok(());
					}
				}
				_ => {}
			}
		}
		// A scalar kind is read from its payload in place.
		let scalar = super::row::scalar_from_wire(wire)?;
		#[cfg(test)]
		if scalar.as_whole().is_some() {
			VALUE_DECODES.with(|n| n.set(n.get() + 1));
		}
		self.scalar(&scalar, depth)
	}

	/// Transcode the stored object `object` (an `Object` wire), with `id`
	/// taken from the record key `key` when given. `false`, having written
	/// nothing, for an object the walker cannot read.
	fn object(
		&mut self,
		object: &'b [u8],
		key: Option<&[u8]>,
		depth: usize,
	) -> anyhow::Result<bool> {
		too_deep(depth)?;
		let Ok(walker) = indexed_map_walker_from_object_bytes(object) else {
			return Ok(false);
		};
		let mark = self.scratch.len();
		if let Some(entries) = walker.entries() {
			for (k, v) in entries {
				let Some(k) = strand_utf8(k) else {
					self.scratch.truncate(mark);
					return Ok(false);
				};
				let Some(entry) = self.entry(k, v) else {
					self.scratch.truncate(mark);
					return Ok(false);
				};
				self.scratch.push(entry);
			}
		} else {
			let Some(body) = walker.legacy_body() else {
				return Ok(false);
			};
			let mut reader = body;
			for _ in 0..walker.len() {
				let Ok(len) =
					<usize as revision::DeserializeRevisioned>::deserialize_revisioned(&mut reader)
				else {
					self.scratch.truncate(mark);
					return Ok(false);
				};
				let (Some(k), Some(rest)) = (reader.get(..len), reader.get(len..)) else {
					self.scratch.truncate(mark);
					return Ok(false);
				};
				let mut probe = rest;
				if skip_value_wire(&mut probe).is_err() {
					self.scratch.truncate(mark);
					return Ok(false);
				}
				let v = &rest[..rest.len() - probe.len()];
				reader = probe;
				let Some(entry) = self.entry(k, v) else {
					self.scratch.truncate(mark);
					return Ok(false);
				};
				self.scratch.push(entry);
			}
		}
		// A record's `id` is its key, whatever the stored value holds.
		if key.is_some() {
			let base = self.base;
			let mut kept = mark;
			for at in mark..self.scratch.len() {
				let e = self.scratch[at];
				if &base[span_range(e.key)] != ID_KEY.as_bytes() {
					self.scratch[kept] = e;
					kept += 1;
				}
			}
			self.scratch.truncate(kept);
		}
		// Stored keys are ordered by their encoding, length first; the public
		// object orders them by their text.
		let base = self.base;
		let utf8 = |e: &Entry| &base[span_range(e.key)];
		self.scratch[mark..].sort_unstable_by(|a, b| utf8(a).cmp(utf8(b)));
		let count = self.scratch.len() - mark + usize::from(key.is_some());
		self.open_map(count);
		let mut id_written = key.is_none();
		let mut written = 0;
		let mut at = mark;
		while at < self.scratch.len() {
			let entry = self.scratch[at];
			let k = self.at(entry.key);
			if !id_written && ID_KEY.as_bytes() < k {
				self.record_key(written, key.unwrap_or_default(), depth + 1)?;
				written += 1;
				id_written = true;
			}
			let k = std::str::from_utf8(k)
				.map_err(|_| anyhow::anyhow!("an object key that is not UTF-8"))?;
			self.map_key(written, k)?;
			written += 1;
			self.value(self.at(entry.value), depth + 1)?;
			at += 1;
		}
		if !id_written {
			self.record_key(written, key.unwrap_or_default(), depth + 1)?;
		}
		self.close_map();
		self.scratch.truncate(mark);
		Ok(true)
	}

	/// Write the `id` entry, the map's `at`th, for the record key `key`.
	fn record_key(&mut self, at: usize, key: &[u8], depth: usize) -> anyhow::Result<()> {
		if let Some((table, id)) = super::row_packed::read_key_id(key) {
			let raw = table.as_bytes();
			let raw = raw.get(..raw.len().saturating_sub(1)).unwrap_or_default();
			if !raw.contains(&1)
				&& let Ok(table) = std::str::from_utf8(raw)
			{
				return self.key_id(at, table, &id, key, depth);
			}
		}
		self.decoded_record_key(at, key, depth)
	}

	/// Write the `id` entry, the map's `at`th, for `table` and `id` read from `key` in place.
	fn key_id(
		&mut self,
		at: usize,
		table: &str,
		id: &KeyIdRef<'_>,
		key: &[u8],
		depth: usize,
	) -> anyhow::Result<()> {
		match id {
			KeyIdRef::Number(n) => {
				self.map_key(at, ID_KEY)?;
				too_deep(depth + 1)?;
				self.record_id_in_place(table, StoredKey::Number(*n))
			}
			KeyIdRef::String(escaped) => {
				self.map_key(at, ID_KEY)?;
				too_deep(depth + 1)?;
				let raw = escaped.as_bytes();
				let raw = raw.get(..raw.len().saturating_sub(1)).unwrap_or_default();
				// The key escapes a byte with a 1 before it; most strings hold neither.
				let utf8 = if raw.contains(&1) {
					self.id_text.clear();
					self.id_text.extend(escaped.iter());
					&self.id_text[..]
				} else {
					raw
				};
				// The key read proved the id UTF-8; CBOR copies its bytes, JSON escapes its text.
				match &mut self.json {
					Some(text_buf) => {
						let s = std::str::from_utf8(utf8)
							.map_err(|_| anyhow::anyhow!("a record key id that is not UTF-8"))?;
						super::row_json::record_id_text_str(self.out, table, s, text_buf)
					}
					None => {
						let out = &mut *self.out;
						tag(out, TAG_RECORDID);
						head(out, MAJOR_ARRAY, 2);
						text(out, table);
						head(out, MAJOR_TEXT, utf8.len() as u64);
						out.extend_from_slice(utf8);
						Ok(())
					}
				}
			}
			KeyIdRef::Uuid(u) => {
				self.map_key(at, ID_KEY)?;
				too_deep(depth + 1)?;
				self.record_id_in_place(table, StoredKey::Uuid(u.0))
			}
			KeyIdRef::Other => self.decoded_record_key(at, key, depth),
		}
	}

	/// Write the `id` entry, the map's `at`th, by decoding the record key `key`.
	fn decoded_record_key(&mut self, at: usize, key: &[u8], depth: usize) -> anyhow::Result<()> {
		self.map_key(at, ID_KEY)?;
		let decoded = crate::key::schema::RecordKey::decode_key(key)
			.map_err(|e| anyhow::anyhow!("Failed to decode record key: {e}"))?;
		let whole = || {
			Value::RecordId(crate::val::RecordId {
				table: decoded.tb.as_ref().clone(),
				key: decoded.id.as_ref().clone(),
			})
		};
		match &mut self.json {
			Some(text_buf) => {
				too_deep(depth + 1)?;
				match decoded.id.as_ref() {
					RecordIdKey::Number(_) | RecordIdKey::String(_) | RecordIdKey::Uuid(_) => {
						super::row_json::record_id_text(
							self.out,
							decoded.tb.as_str(),
							decoded.id.as_ref(),
							text_buf,
						)
					}
					RecordIdKey::Array(_) | RecordIdKey::Object(_) | RecordIdKey::Range(_) => {
						super::row_json::encode_json(self.out, &whole(), depth, text_buf)
					}
				}
			}
			None => {
				encode_record_id(self.out, decoded.tb.as_str(), &decoded.id, depth, self.v2, whole)
			}
		}
	}

	/// Transcode the stored-value object `object`, with `id` taken from the
	/// record key `key` when given. Its keys ascend in byte order, which is the
	/// order the public object holds them in.
	fn stored_object(
		&mut self,
		object: OrderedObjectRef<'b>,
		key: Option<&[u8]>,
		depth: usize,
	) -> anyhow::Result<()> {
		too_deep(depth)?;
		// The entry count is declared before the entries, from a lookup of `id`.
		// A record's `id` is its key, whatever the stored value holds.
		let replaced = key.is_some() && object.get(ID_KEY)?.is_some();
		self.open_map(object.len() - usize::from(replaced) + usize::from(key.is_some()));
		let mut pending = key;
		let mut written = 0;
		for entry in object.iter() {
			let (k, v) = entry?;
			if key.is_some() && k == ID_KEY {
				continue;
			}
			if let Some(record) = pending
				&& ID_KEY < k
			{
				self.record_key(written, record, depth + 1)?;
				written += 1;
				pending = None;
			}
			self.map_key(written, k)?;
			written += 1;
			self.stored_value(v, depth + 1)?;
		}
		if let Some(record) = pending {
			self.record_key(written, record, depth + 1)?;
		}
		self.close_map();
		Ok(())
	}

	#[cfg(feature = "shaped")]
	/// Transcode the stored-value object `object`, every entry but `id`, as one CBOR map; nothing
	/// when it holds no other entry.
	pub(super) fn stored_object_rest(
		&mut self,
		object: OrderedObjectRef<'b>,
		depth: usize,
	) -> anyhow::Result<()> {
		too_deep(depth)?;
		let count = object.len() - usize::from(object.get(ID_KEY)?.is_some());
		if count == 0 {
			return Ok(());
		}
		head(self.out, MAJOR_MAP, count as u64);
		for entry in object.iter() {
			let (k, v) = entry?;
			if k == ID_KEY {
				continue;
			}
			text(self.out, k);
			self.stored_value(v, depth + 1)?;
		}
		Ok(())
	}

	/// Transcode the stored-value `value` at depth `depth`.
	pub(super) fn stored_value(&mut self, value: ValueRef<'b>, depth: usize) -> anyhow::Result<()> {
		too_deep(depth)?;
		match value.kind() {
			// A string's payload is its UTF-8, checked when it was written: CBOR copies it as it
			// lies and forms no `str` from it.
			Kind::String if self.json.is_none() => {
				head(self.out, MAJOR_TEXT, value.payload().len() as u64);
				self.out.extend_from_slice(value.payload());
				return Ok(());
			}
			Kind::Object => {
				if let Some(object) = value.as_ordered_object()? {
					return self.stored_object(object, None, depth);
				}
			}
			Kind::Array => {
				if let Some(array) = value.as_array()? {
					if !array.is_empty() {
						too_deep(depth + 1)?;
					}
					if let Some(ints) = array.ints() {
						self.open_array(ints.len());
						for (at, v) in ints.enumerate() {
							self.element(at);
							match &mut self.json {
								Some(_) => serde_json::to_writer(&mut *self.out, &v)?,
								None => int(self.out, v),
							}
						}
						self.close_array();
						return Ok(());
					}
					if let Some(floats) = array.floats() {
						self.open_array(floats.len());
						for (at, v) in floats.enumerate() {
							self.element(at);
							match &mut self.json {
								Some(_) if v.is_finite() => {
									serde_json::to_writer(&mut *self.out, &v)?
								}
								Some(_) => self.out.extend_from_slice(b"null"),
								None => float(self.out, v),
							}
						}
						self.close_array();
						return Ok(());
					}
					self.open_array(array.len());
					for (at, element) in array.iter().enumerate() {
						self.element(at);
						self.stored_value(element?, depth + 1)?;
					}
					self.close_array();
					return Ok(());
				}
			}
			Kind::RecordId => {
				if let Some((table, key)) = value.as_record_id()? {
					let key = match key.kind() {
						Kind::Int => key.as_int()?.map(StoredKey::Number),
						Kind::String => key.as_str()?.map(StoredKey::String),
						Kind::Uuid => key.as_uuid()?.map(StoredKey::Uuid),
						Kind::None
						| Kind::Null
						| Kind::Bool
						| Kind::Float
						| Kind::Decimal
						| Kind::BigDecimal
						| Kind::Duration
						| Kind::Datetime
						| Kind::Date
						| Kind::Time
						| Kind::Timestamp
						| Kind::Interval
						| Kind::Bytes
						| Kind::Table
						| Kind::RecordId
						| Kind::File
						| Kind::Regex
						| Kind::Range
						| Kind::Geometry
						| Kind::Array
						| Kind::Set
						| Kind::Object
						| Kind::I8
						| Kind::U8
						| Kind::I16
						| Kind::U16
						| Kind::I32
						| Kind::U32
						| Kind::U64
						| Kind::F16
						| Kind::F32
						| Kind::Unknown => None,
					};
					if let Some(key) = key {
						too_deep(depth + 1)?;
						self.record_id_in_place(table, key)?;
						return Ok(());
					}
				}
			}
			Kind::None
			| Kind::Null
			| Kind::Bool
			| Kind::Int
			| Kind::Float
			| Kind::Decimal
			| Kind::BigDecimal
			| Kind::I8
			| Kind::U8
			| Kind::I16
			| Kind::U16
			| Kind::I32
			| Kind::U32
			| Kind::U64
			| Kind::F16
			| Kind::F32
			| Kind::String
			| Kind::Duration
			| Kind::Datetime
			| Kind::Date
			| Kind::Time
			| Kind::Timestamp
			| Kind::Interval
			| Kind::Uuid
			| Kind::Bytes
			| Kind::Table
			| Kind::File
			| Kind::Regex
			| Kind::Range
			| Kind::Geometry
			| Kind::Set
			| Kind::Unknown => {}
		}
		let scalar = super::row::scalar_from_stored(value)?;
		#[cfg(test)]
		if scalar.as_whole().is_some() {
			VALUE_DECODES.with(|n| n.set(n.get() + 1));
		}
		self.scalar(&scalar, depth)
	}

	/// Write a record id whose key is a number, a string or a uuid, read in place.
	fn record_id_in_place(&mut self, table: &str, key: StoredKey<'_>) -> anyhow::Result<()> {
		match &mut self.json {
			Some(text_buf) => match key {
				StoredKey::Number(n) => {
					super::row_json::record_id_text_int(self.out, table, n, text_buf)
				}
				StoredKey::String(s) => {
					super::row_json::record_id_text_str(self.out, table, s, text_buf)
				}
				StoredKey::Uuid(u) => {
					super::row_json::record_id_text_uuid(self.out, table, u, text_buf)
				}
			},
			None => {
				tag(self.out, TAG_RECORDID);
				head(self.out, MAJOR_ARRAY, 2);
				text(self.out, table);
				match key {
					StoredKey::Number(n) => int(self.out, n),
					StoredKey::String(s) => text(self.out, s),
					StoredKey::Uuid(u) => uuid(self.out, u.as_bytes()),
				}
				Ok(())
			}
		}
	}

	/// Transcode the stored array `array` (an `Array` wire). `false`, having
	/// written nothing, for an array the walker cannot read.
	fn array(&mut self, array: &'b [u8], depth: usize) -> anyhow::Result<bool> {
		let Ok(payload) = rev2_optimised_payload_unchecked(array) else {
			return Ok(false);
		};
		let Ok(walker) = IndexedSeqWalker::<'_, Value>::from_payload_unvalidated(payload) else {
			return Ok(false);
		};
		// Find every element before writing any, so a wire that does not read
		// leaves nothing written.
		let len = walker.len();
		if walker.is_indexed() {
			for i in 0..len {
				if walker.element_bytes(i).is_err() {
					return Ok(false);
				}
			}
			self.open_array(len);
			for i in 0..len {
				let element = walker.element_bytes(i)?;
				self.element(i);
				self.value(element, depth + 1)?;
			}
			self.close_array();
		} else {
			let body = walker.body();
			let mut probe = body;
			for _ in 0..len {
				if skip_value_wire(&mut probe).is_err() {
					return Ok(false);
				}
			}
			self.open_array(len);
			let mut reader = body;
			for i in 0..len {
				let mut next = reader;
				skip_value_wire(&mut next)?;
				let element = &reader[..reader.len() - next.len()];
				reader = next;
				self.element(i);
				self.value(element, depth + 1)?;
			}
			self.close_array();
		}
		Ok(true)
	}
}

impl Stored<'_, '_> {
	/// Transcode the packed row `record`, of the record `key` names, through its generation's
	/// descriptor in `layouts`, with its `id` from the key ([`PackedRow::visit`]). On an error some
	/// of the row may have been written; the caller drops it.
	fn packed(
		&mut self,
		key: &[u8],
		record: &StoredRecord<'_>,
		layouts: &TableLayouts,
	) -> anyhow::Result<()> {
		let Ok(Some(generation)) = record.generation() else {
			anyhow::bail!("a packed row whose stamp names no generation");
		};
		let descriptor = layouts.admit_packed(generation)?;
		let row = PackedRow::new(record, descriptor)?;
		let table = layouts.table().as_str();
		let Some((key_table, id)) = super::row_packed::read_key_id(key) else {
			anyhow::bail!("a record key that does not decode");
		};
		if !key_table.iter().eq(table.as_bytes()) {
			anyhow::bail!("a record read through another table's layouts");
		}
		let mut sink = PackedSink {
			stored: Stored {
				base: self.base,
				out: &mut *self.out,
				scratch: &mut *self.scratch,
				id_text: &mut *self.id_text,
				json: self.json.as_deref_mut(),
				v2: self.v2,
			},
			key,
			table,
			id,
			depth: 0,
		};
		row.visit(&mut sink)
	}
}

/// A packed row's record as [`PackedRow::visit`] hands it, written to a frame.
struct PackedSink<'a, 'o> {
	stored: Stored<'a, 'o>,
	/// The record's key, for an id read only by decoding it.
	key: &'a [u8],
	/// The record's table, which the key names.
	table: &'a str,
	/// The record's id, read from the key in place.
	id: KeyIdRef<'a>,
	/// The depth of the open object: 1 for the record.
	depth: usize,
}

impl<'a> PackedVisitor<'a> for PackedSink<'a, '_> {
	type Error = anyhow::Error;

	fn open(&mut self) -> anyhow::Result<usize> {
		self.depth += 1;
		too_deep(self.depth)?;
		let mark = self.stored.out.len();
		match self.stored.json {
			Some(_) => self.stored.out.push(b'{'),
			// The head of a map of up to 23 entries, which `close` rewrites with the count.
			None => self.stored.out.push(MAJOR_MAP << 5),
		}
		Ok(mark)
	}

	fn key(&mut self, at: usize, key: &str) -> anyhow::Result<()> {
		self.stored.map_key(at, key)
	}

	fn id(&mut self, at: usize) -> anyhow::Result<()> {
		self.stored.key_id(at, self.table, &self.id, self.key, self.depth + 1)
	}

	/// CBOR text is copied from storage as written: a string is checked to be UTF-8 when it is
	/// written, and this read forms no `str` from the bytes. JSON reads the string as `str`, which
	/// it checks, since it escapes the text.
	fn raw_text(&self) -> bool {
		self.stored.json.is_none()
	}

	fn text(&mut self, bytes: &'a [u8]) -> anyhow::Result<()> {
		head(self.stored.out, MAJOR_TEXT, bytes.len() as u64);
		self.stored.out.extend_from_slice(bytes);
		Ok(())
	}

	fn field(&mut self, field: FieldRef<'a>) -> anyhow::Result<()> {
		let depth = self.depth + 1;
		// The common leaves, straight to CBOR.
		if self.stored.json.is_none() {
			let out = &mut *self.stored.out;
			match field {
				FieldRef::Null => out.push(0xf6),
				FieldRef::Bool(v) => out.push(if v {
					0xf5
				} else {
					0xf4
				}),
				FieldRef::Int(v) => int(out, v),
				FieldRef::Float(v) => float(out, v),
				FieldRef::String(v) => text(out, v),
				FieldRef::Datetime(v) => {
					tag(out, TAG_CUSTOM_DATETIME);
					head(out, MAJOR_ARRAY, 2);
					int(out, v.timestamp());
					int(out, v.timestamp_subsec_nanos() as i64);
				}
				FieldRef::I8(_)
				| FieldRef::I16(_)
				| FieldRef::I32(_)
				| FieldRef::U8(_)
				| FieldRef::U16(_)
				| FieldRef::U32(_)
				| FieldRef::U64(_)
				| FieldRef::F16(_)
				| FieldRef::F32(_)
				| FieldRef::Absent
				| FieldRef::Object
				| FieldRef::Decimal(_)
				| FieldRef::Uuid(_)
				| FieldRef::Duration(_)
				| FieldRef::Point(..)
				| FieldRef::Bytes(_)
				| FieldRef::Literal(_)
				| FieldRef::RecordId(..)
				| FieldRef::Stored(_) => return self.field_value(field, depth),
			}
			return Ok(());
		}
		self.field_value(field, depth)
	}

	fn close(&mut self, mark: usize, len: usize) -> anyhow::Result<()> {
		self.depth -= 1;
		if self.stored.json.is_some() {
			self.stored.out.push(b'}');
			return Ok(());
		}
		// The map's head was written for up to 23 entries; a longer one moves its entries along
		// by the bytes its head takes beyond one.
		let n = len as u64;
		let m = MAJOR_MAP << 5;
		let mut wide = [0u8; 9];
		let used = if n <= 23 {
			wide[0] = m | n as u8;
			1
		} else if n <= u64::from(u8::MAX) {
			wide[..2].copy_from_slice(&[m | 24, n as u8]);
			2
		} else if n <= u64::from(u16::MAX) {
			wide[0] = m | 25;
			wide[1..3].copy_from_slice(&(n as u16).to_be_bytes());
			3
		} else if n <= u64::from(u32::MAX) {
			wide[0] = m | 26;
			wide[1..5].copy_from_slice(&(n as u32).to_be_bytes());
			5
		} else {
			wide[0] = m | 27;
			wide[1..9].copy_from_slice(&n.to_be_bytes());
			9
		};
		self.stored.out.splice(mark..mark + 1, wide[..used].iter().copied());
		Ok(())
	}
}

impl<'a> PackedSink<'a, '_> {
	/// Write `field`, at depth `depth`, through the scalar or stored-value encoders.
	fn field_value(&mut self, field: FieldRef<'a>, depth: usize) -> anyhow::Result<()> {
		let scalar = match field {
			FieldRef::Absent | FieldRef::Object => {
				anyhow::bail!("a packed row visit handed an absent field or an object node")
			}
			FieldRef::Stored(value) => return self.stored.stored_value(value, depth),
			FieldRef::RecordId(table, key) => {
				let link = ValueRef::new_nested(key, self.depth + 1)?;
				let in_place = match link.kind() {
					Kind::Int => link.as_int()?.map(StoredKey::Number),
					Kind::String => link.as_str()?.map(StoredKey::String),
					Kind::Uuid => link.as_uuid()?.map(StoredKey::Uuid),
					Kind::None
					| Kind::Null
					| Kind::Bool
					| Kind::Float
					| Kind::Decimal
					| Kind::BigDecimal
					| Kind::I8
					| Kind::U8
					| Kind::I16
					| Kind::U16
					| Kind::I32
					| Kind::U32
					| Kind::U64
					| Kind::F16
					| Kind::F32
					| Kind::Duration
					| Kind::Datetime
					| Kind::Date
					| Kind::Time
					| Kind::Timestamp
					| Kind::Interval
					| Kind::Bytes
					| Kind::Table
					| Kind::RecordId
					| Kind::File
					| Kind::Regex
					| Kind::Range
					| Kind::Geometry
					| Kind::Array
					| Kind::Set
					| Kind::Object
					| Kind::Unknown => None,
				};
				if let Some(link) = in_place {
					too_deep(depth + 1)?;
					return self.stored.record_id_in_place(table.as_str(), link);
				}
				let key = crate::val::stored::decode_key_nested(key, self.depth + 1)?;
				let link = Value::RecordId(crate::val::RecordId {
					table: table.clone(),
					key,
				});
				return self.stored.encoded(&link, depth);
			}
			FieldRef::Null => Scalar::Null,
			FieldRef::Bool(v) => Scalar::Bool(v),
			FieldRef::I8(v) => Scalar::Number(Number::I8(v)),
			FieldRef::I16(v) => Scalar::Number(Number::I16(v)),
			FieldRef::I32(v) => Scalar::Number(Number::I32(v)),
			FieldRef::Int(v) => Scalar::Number(Number::Int(v)),
			FieldRef::U8(v) => Scalar::Number(Number::U8(v)),
			FieldRef::U16(v) => Scalar::Number(Number::U16(v)),
			FieldRef::U32(v) => Scalar::Number(Number::U32(v)),
			FieldRef::U64(v) => Scalar::Number(Number::U64(v)),
			FieldRef::F16(v) => Scalar::Number(Number::F16(v)),
			FieldRef::F32(v) => Scalar::Number(Number::F32(v)),
			FieldRef::Float(v) => Scalar::Number(Number::Float(v)),
			FieldRef::Decimal(v) => Scalar::Number(Number::Decimal(v)),
			FieldRef::Uuid(v) => Scalar::Uuid(crate::val::Uuid(v)),
			FieldRef::Datetime(v) => Scalar::Datetime(crate::val::Datetime(v)),
			FieldRef::Duration(v) => Scalar::Duration(crate::val::Duration(v)),
			FieldRef::String(v) => Scalar::Str(v),
			FieldRef::Bytes(v) => Scalar::Bytes(v),
			FieldRef::Literal(v) => Scalar::of(v),
			FieldRef::Point(x, y) => Scalar::Value(Held::Owned(Value::Geometry(
				crate::val::Geometry::Point((x, y).into()),
			))),
		};
		self.stored.scalar(&scalar, depth)
	}
}

/// A record id key read in place.
#[derive(Clone, Copy)]
enum StoredKey<'b> {
	Number(i64),
	String(&'b str),
	Uuid(uuid::Uuid),
}

/// The table and key of a stored `RecordId` wire (revision 1: `rev, table
/// strand, key`), for a key that is a number, a string or a uuid.
fn stored_record_id(wire: &[u8]) -> Option<(&str, StoredKey<'_>)> {
	use revision::DeserializeRevisioned as _;
	let mut r = wire;
	if u16::deserialize_revisioned(&mut r).ok()? != 1 {
		return None;
	}
	let table = strand_str(&mut r)?;
	if u16::deserialize_revisioned(&mut r).ok()? != 1 {
		return None;
	}
	let key = match u32::deserialize_revisioned(&mut r).ok()? {
		0 => StoredKey::Number(i64::deserialize_revisioned(&mut r).ok()?),
		1 => StoredKey::String(strand_str(&mut r)?),
		2 => StoredKey::Uuid(crate::val::Uuid::deserialize_revisioned(&mut r).ok()?.0),
		_ => return None,
	};
	r.is_empty().then_some((table, key))
}

/// Read a strand (`varint len || utf8`) from the front of `r`.
fn strand_str<'b>(r: &mut &'b [u8]) -> Option<&'b str> {
	let len = <usize as revision::DeserializeRevisioned>::deserialize_revisioned(r).ok()?;
	let (utf8, rest) = (r.get(..len)?, r.get(len..)?);
	*r = rest;
	std::str::from_utf8(utf8).ok()
}

/// The UTF-8 of a stored strand wire (`varint len || utf8`).
fn strand_utf8(wire: &[u8]) -> Option<&[u8]> {
	let mut r = wire;
	let len = <usize as revision::DeserializeRevisioned>::deserialize_revisioned(&mut r).ok()?;
	(r.len() == len).then_some(r)
}

#[cfg(test)]
mod tests {
	use super::*;

	/// The format the generation-0 samples are written in.
	const DOCUMENT: crate::catalog::RecordFormat = crate::catalog::RecordFormat::Document {
		generation: None,
	};

	/// What the RPC layer writes for `value`.
	fn expected(value: &Value) -> Vec<u8> {
		expected_in(RowFormat::Cbor, value)
	}

	/// What the RPC layer writes for `value` in `format`.
	fn expected_in(format: RowFormat, value: &Value) -> Vec<u8> {
		let public = crate::val::convert_value_to_public_value(value.clone()).expect("converts");
		match format {
			RowFormat::Cbor => crate::rpc::format::cbor::encode(public).expect("encodes"),
			RowFormat::Cbor2 => crate::rpc::format::cbor::encode_v2(public).expect("encodes"),
			RowFormat::Json => {
				crate::rpc::format::json::encode_str(public).expect("encodes").into_bytes()
			}
			RowFormat::Shaped => unreachable!("shaped rows are compared decoded"),
		}
	}

	#[test]
	fn a_float_takes_the_shortest_exact_form() {
		for bits in (0u64..=u16::MAX as u64).step_by(7) {
			// Every half widened, and its neighbours either side.
			let half_bits = bits as u16;
			let exp = (half_bits >> 10) & 0x1f;
			let wide = if exp == 0x1f {
				continue;
			} else {
				let sign = if half_bits & 0x8000 != 0 {
					-1.0
				} else {
					1.0
				};
				let man = (half_bits & 0x3ff) as f64;
				sign * if exp == 0 {
					man * 2f64.powi(-24)
				} else {
					(1.0 + man / 1024.0) * 2f64.powi(exp as i32 - 15)
				}
			};
			for v in [wide, f64::from_bits(wide.to_bits() + 1)] {
				let value = Value::Number(Number::Float(v));
				let mut frame = RowFrame::new();
				frame.push_value(&value).expect("encodes");
				assert_eq!(frame.row(0), expected(&value).as_slice(), "{v:e}");
			}
		}
	}

	/// A datetime is written as the RPC layer writes it, in JSON and in CBOR: every count of
	/// fractional digits, the edges of four-digit years, years beyond them and a leap second.
	#[test]
	fn a_datetime_encodes_as_the_rpc_layer_encodes_it() {
		use chrono::{TimeZone as _, Utc};
		let mut datetimes = Vec::new();
		for (secs, nanos) in [
			(0, 0),
			(1_700_000_000, 0),
			(1_700_000_000, 6_000_000),
			(1_700_000_000, 120_000),
			(1_700_000_000, 7),
			(1_700_000_000, 999_999_999),
			(-62_135_596_800, 0),
			(-62_135_596_801, 5),
			(253_402_300_799, 1_000),
			(253_402_300_800, 0),
			(-1, 500_000_000),
		] {
			datetimes.push(Utc.timestamp_opt(secs, nanos).single().expect("a datetime"));
		}
		datetimes.push(
			chrono::NaiveDate::from_ymd_opt(2016, 12, 31)
				.and_then(|d| d.and_hms_nano_opt(23, 59, 59, 1_500_000_000))
				.expect("a leap second")
				.and_utc(),
		);
		for d in datetimes {
			let value = Value::Datetime(crate::val::Datetime(d));
			for format in [RowFormat::Cbor, RowFormat::Json] {
				let mut frame = RowFrame::with_format(format);
				frame.push_value(&value).expect("encodes");
				assert_eq!(
					String::from_utf8_lossy(frame.row(0)),
					String::from_utf8_lossy(&expected_in(format, &value)),
					"{format:?} {d:?}"
				);
			}
		}
	}

	/// A bigdecimal is written as the RPC layer writes it in each format: as tag 192 under
	/// `cbor.2`; under `cbor` as a decimal when it fits one exactly and otherwise as plain text;
	/// and as text in JSON. Values at and beyond the decimal's 28 digits and scale do not fit.
	#[test]
	fn a_bigdecimal_encodes_as_the_rpc_layer_encodes_it() {
		for text in [
			"1.50",
			"-0",
			"79228162514264337593543950335",
			"1234567890123456789012345678901234567890",
			"-0.0000000000000000000000000000001",
			"0.00000000000000000000000000000000000000000000000000000000000000001",
			"1.5000000000000000000000000000000000000000000000000000000000000000000000000001",
			"123456789012345678901234567890.5",
		] {
			let value =
				Value::Number(Number::BigDecimal(Box::new(text.parse().expect("a bigdecimal"))));
			for format in [RowFormat::Cbor, RowFormat::Cbor2, RowFormat::Json] {
				let mut frame = RowFrame::with_format(format);
				frame.push_value(&value).expect("encodes");
				assert_eq!(
					frame.row(0),
					expected_in(format, &value).as_slice(),
					"{format:?} {text}"
				);
			}
		}
	}

	/// `src`, a SurrealQL value literal, as a value.
	fn val(src: &str) -> Value {
		Value::from(crate::syn::value(src).expect("parses"))
	}

	/// A stored record, in either encoding, transcodes to the bytes its decoded
	/// form encodes to.
	#[test]
	fn a_stored_record_transcodes_as_its_decoded_form_encodes() {
		use crate::key::KVKey as _;

		let mut datas = Vec::new();
		for src in [
			"{}",
			"{ a: 1 }",
			"{ id: 'ignored', b: 'x' }",
			"{ f: t:1, g: t:abc, h: other:-5, u: t:u'018f9b8e-0000-7000-8000-000000000000', \
			   i: [t:2, t:u'018f9b8e-0000-7000-8000-000000000000'] }",
			"{ a: t:⟨with space⟩, b: t:⟨123⟩, c: `select`:x, d: t:⟨a\"b⟩, e: 'q\"\\\\x\\té', \
			   f: 1h30m, g: d'2024-01-02T03:04:05.006Z', h: u'018f9b8e-0000-7000-8000-000000000000', \
			   i: 1.5f, j: 2.25dec, l: NONE, m: NULL, n: false }",
			"{ zz: 1, a: [1, 2, { q: 'r', p: NONE }], m: { k: true, kk: 1.5f, kkk: 2dec } }",
			"{ i8s: [-128, 0, 127], i16s: [-1000, 1000], i32s: [-100000, 100000], \
			   i64s: [-5000000000, 5000000000], f32s: [0.0f, -0.0f, 1.5f, Infinity], \
			   f64s: [0.1f, 1.5f, -Infinity, NaN] }",
		] {
			datas.push(val(src));
		}
		// Wide enough for the indexed map and seq forms.
		let mut wide = crate::val::Object::default();
		for i in 0..40 {
			wide.insert(format!("f{}", 40 - i), Value::Array((0..i).map(Value::from).collect()));
		}
		datas.push(Value::Object(wide));
		for (i, data) in datas.into_iter().enumerate() {
			let keys = [
				RecordIdKey::Number(i as i64),
				RecordIdKey::String(format!("k_{i}").into()),
				RecordIdKey::String(format!("a\0b\u{1}c_{i}").into()),
				RecordIdKey::Uuid(crate::val::Uuid(uuid::Uuid::from_u128(i as u128 + 1))),
			];
			for rid_key in keys {
				let rid = crate::val::RecordId {
					table: "t".into(),
					key: rid_key,
				};
				let key = crate::key::schema::RecordKey::new(
					crate::catalog::NamespaceId(1),
					crate::catalog::DatabaseId(2),
					std::borrow::Cow::Borrowed(&rid.table),
					std::borrow::Cow::Borrowed(&rid.key),
				)
				.encode_key()
				.expect("encodes");
				// The record as a writer holds it: its `id` the key's, unless the
				// data names another.
				let mut held = data.clone();
				if let Value::Object(object) = &mut held
					&& !object.contains_key("id")
				{
					object.insert("id".to_owned(), Value::RecordId(rid.clone()));
				}
				let legacy =
					revision::to_vec(&crate::catalog::Record::new(data.clone())).expect("encodes");
				let decoded =
					crate::exec::operators::scan::pipeline::decode_record(&key, &legacy, None)
						.expect("decodes");
				let keyed = crate::catalog::encode_record(DOCUMENT, None, &held, Some(&rid))
					.expect("encodes");
				let unkeyed =
					crate::catalog::encode_record(DOCUMENT, None, &held, None).expect("encodes");
				for value in [&legacy, &keyed, &unkeyed] {
					assert_eq!(
						crate::exec::operators::scan::pipeline::decode_record(&key, value, None)
							.expect("decodes"),
						decoded,
						"{i}: every encoding decodes alike"
					);
					for format in [RowFormat::Cbor, RowFormat::Json] {
						let mut frame = RowFrame::with_format(format);
						let before = WHOLE_DECODES.with(|n| n.get());
						let values_before = VALUE_DECODES.with(|n| n.get());
						frame.push_stored(&key, value, None).expect("transcodes");
						assert_eq!(WHOLE_DECODES.with(|n| n.get()), before, "transcoded in place");
						assert_eq!(
							VALUE_DECODES.with(|n| n.get()),
							values_before,
							"{i}: leaves in place"
						);
						assert_eq!(
							String::from_utf8_lossy(frame.row(0)),
							String::from_utf8_lossy(&expected_in(format, &decoded)),
							"{format:?} {decoded:?}"
						);
					}
				}
			}
		}
	}

	/// A stored-value record whose object keys are out of order is refused, as
	/// a whole decode refuses it, rather than transcoded to a map whose
	/// declared length its entries do not fill.
	#[test]
	fn a_stored_object_with_keys_out_of_order_is_refused() {
		use crate::key::KVKey as _;

		let rid = crate::val::RecordId {
			table: "t".into(),
			key: RecordIdKey::Number(1),
		};
		let key = crate::key::schema::RecordKey::new(
			crate::catalog::NamespaceId(1),
			crate::catalog::DatabaseId(2),
			std::borrow::Cow::Borrowed(&rid.table),
			std::borrow::Cow::Borrowed(&rid.key),
		)
		.encode_key()
		.expect("encodes");
		let mut value =
			crate::catalog::encode_record(DOCUMENT, None, &val("{ aa: 1, bb: 2 }"), Some(&rid))
				.expect("encodes");
		let at = |v: &[u8], needle: &[u8]| {
			v.windows(needle.len()).position(|w| w == needle).expect("the key is stored")
		};
		let (a, b) = (at(&value, b"aa"), at(&value, b"bb"));
		value[a..a + 2].copy_from_slice(b"bb");
		value[b..b + 2].copy_from_slice(b"aa");
		assert!(
			crate::exec::operators::scan::pipeline::decode_record(&key, &value, None).is_err(),
			"a whole decode refuses it"
		);
		for format in [RowFormat::Cbor, RowFormat::Json] {
			let mut frame = RowFrame::with_format(format);
			assert!(frame.push_stored(&key, &value, None).is_err(), "{format:?}");
		}
	}

	/// A stored-value record whose data is no object is decoded whole, and
	/// one that does not parse fails the row and writes nothing.
	#[test]
	fn a_stored_value_record_not_transcoded_in_place() {
		use crate::key::KVKey as _;

		let rid = crate::val::RecordId {
			table: "t".into(),
			key: RecordIdKey::Number(1),
		};
		let key = crate::key::schema::RecordKey::new(
			crate::catalog::NamespaceId(1),
			crate::catalog::DatabaseId(2),
			std::borrow::Cow::Borrowed(&rid.table),
			std::borrow::Cow::Borrowed(&rid.key),
		)
		.encode_key()
		.expect("encodes");
		let value =
			crate::catalog::encode_record(DOCUMENT, None, &Value::from(5), None).expect("encodes");
		let mut frame = RowFrame::new();
		let before = WHOLE_DECODES.with(|n| n.get());
		frame.push_stored(&key, &value, None).expect("transcodes");
		assert_eq!(WHOLE_DECODES.with(|n| n.get()), before + 1, "decoded whole");
		assert_eq!(frame.row(0), expected(&Value::from(5)).as_slice());
		let broken = [crate::catalog::StoredRecord::MARKER, 0x80, 0x00];
		assert!(frame.push_stored(&key, &broken, None).is_err());
		assert_eq!(frame.len(), 1, "the failed row is not kept");
	}

	/// A packed row with an `F32` column holding a NaN, signaling or quiet, is transcoded without
	/// a whole decode into every row frame. A shaped frame copies the slot, so it preserves the
	/// exact `f32` bit pattern. A CBOR frame carries the value as a float, which CBOR v2 tags as
	/// an `f32`: it keeps a NaN of the same sign, and the payload only as far as the platform's
	/// conversion between `f32` and `f64` keeps it.
	#[cfg(feature = "shaped")]
	#[test]
	fn a_packed_f32_nan_keeps_its_bits_in_a_shaped_frame_and_its_sign_in_cbor() {
		use std::sync::Arc;

		use crate::catalog::{
			Column, ColumnRole, DatabaseId, Generation, GenerationLayout, GenerationRing,
			LayoutDescriptor, Location, NamespaceId, PackedDescriptor, PhysicalKind, Presence,
			RecordFormat, RowLayout, StoredRecord, TableLayouts, encode_record_with,
		};
		use crate::key::KVKey as _;
		use crate::val::{Object, TableName};

		let table = TableName::new("t");
		let generation = Generation::FIRST;
		let packed_descriptor = PackedDescriptor {
			columns: vec![Column {
				path: vec!["f".to_owned()],
				kind: PhysicalKind::F32,
				presence: Presence::Required,
				location: Location::Fixed(0),
				role: ColumnRole::Field,
				literals: Vec::new(),
				tables: Vec::new(),
			}],
			presence_bits: 0,
			bool_bits: 0,
			fixed_size: 4,
			var_count: 0,
		};
		let layout_descriptor = Arc::new(LayoutDescriptor {
			table: table.clone(),
			layout: RowLayout::Packed(packed_descriptor),
			migration: None,
			migrated: None,
			verified: None,
		});
		let ring = GenerationRing::new(generation, generation, None).expect("ring");
		let layouts = TableLayouts::new(
			NamespaceId(1),
			DatabaseId(2),
			table,
			ring,
			vec![(generation, GenerationLayout::Readable(layout_descriptor))],
			0,
		)
		.expect("layouts");

		let rid = crate::val::RecordId {
			table: "t".into(),
			key: RecordIdKey::Number(1),
		};
		let key = crate::key::schema::RecordKey::new(
			NamespaceId(1),
			DatabaseId(2),
			std::borrow::Cow::Borrowed(&rid.table),
			std::borrow::Cow::Borrowed(&rid.key),
		)
		.encode_key()
		.expect("encodes");

		for expected_bits in [0x7f80_0001u32, 0xff80_0001u32, 0x7fc0_0001u32] {
			let mut obj = Object::default();
			obj.insert("id".to_owned(), Value::RecordId(rid.clone()));
			obj.insert("f".to_owned(), Value::Number(Number::F32(f32::from_bits(expected_bits))));
			let packed_bytes = encode_record_with(
				RecordFormat::Packed {
					generation,
				},
				Some(&layouts),
				None,
				&Value::Object(obj),
				Some(&rid),
			)
			.expect("encodes packed");
			assert!(
				StoredRecord::parse(&packed_bytes).expect("parses").expect("v1").is_packed(),
				"expected an f32 NaN to fit in PhysicalKind::F32"
			);

			let whole = crate::exec::operators::scan::pipeline::decode_record_value(
				&key,
				&packed_bytes,
				Some(&layouts),
			)
			.expect("decodes whole");
			let Value::Object(whole_obj) = &whole else {
				panic!("expected object, got {whole:?}");
			};
			let Some(Value::Number(Number::F32(whole_f))) = whole_obj.get("f") else {
				panic!("expected f32 field f in {whole_obj:?}");
			};
			assert_eq!(whole_f.to_bits(), expected_bits, "whole decode preserves NaN bits");

			for format in [RowFormat::Cbor, RowFormat::Cbor2, RowFormat::Shaped] {
				let mut frame = RowFrame::with_format(format);
				let whole_before = WHOLE_DECODES.with(|n| n.get());
				let visits_before = PACKED_VISITS.with(|n| n.get());
				frame.push_stored(&key, &packed_bytes, Some(&layouts)).expect("transcodes");
				assert_eq!(WHOLE_DECODES.with(|n| n.get()), whole_before, "{format:?}");
				assert_eq!(PACKED_VISITS.with(|n| n.get()), visits_before + 1, "{format:?}");

				let values = frame.take_encoded().to_values().expect("decodes frame");
				assert_eq!(values.len(), 1);
				let surrealdb_types::Value::Object(decoded_obj) = &values[0] else {
					panic!("expected object in {format:?}, got {:?}", values[0]);
				};
				let negative = expected_bits >> 31 == 1;
				match (format, decoded_obj.get("f")) {
					(
						RowFormat::Shaped,
						Some(surrealdb_types::Value::Number(surrealdb_types::Number::F32(f))),
					) => assert_eq!(
						f.to_bits(),
						expected_bits,
						"{format:?} preserves NaN bits {expected_bits:#010x}"
					),
					(
						RowFormat::Cbor2,
						Some(surrealdb_types::Value::Number(surrealdb_types::Number::F32(f))),
					) => {
						assert!(f.is_nan() && f.is_sign_negative() == negative, "{format:?}: {f}");
					}
					(
						RowFormat::Cbor,
						Some(surrealdb_types::Value::Number(surrealdb_types::Number::Float(f))),
					) => {
						assert!(f.is_nan() && f.is_sign_negative() == negative, "{format:?}: {f}");
					}
					(_, other) => panic!("unexpected field f in {format:?}: {other:?}"),
				}
			}
		}
	}
}
