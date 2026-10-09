//! Reading a stored record's fields where they lie.
//!
//! A [`RowReader`] answers a field of the row as a [`Scalar`] that borrows
//! from where the row lies: the stored bytes, or a value in memory. The VM
//! reads rows only through it, so the storage encoding is this module's
//! concern alone.
//!
//! [`RevisionRow`] reads the `revision` encoding. A field of a record stored
//! in the current revision is found by a lookup over the encoded object and
//! decoded alone: a string is borrowed, a number, boolean, duration, datetime
//! or uuid is decoded inline, and any other kind is decoded into an owned
//! [`Value`]. A record the lookup cannot walk is decoded whole, once, and read
//! from that.

use std::borrow::Cow;
use std::cell::OnceCell;

use revision::optimised::IndexedMapWalker;
use revision::optimised::tag::{SizeClass, Tag};
use surrealdb_strand::Strand;

use super::scalar::Scalar;
use crate::exec::object_extract::{
	PathSegment, indexed_map_walker_from_object_bytes, lookup_value_bytes_in_map,
	rev2_record_data_bytes,
};
use crate::exec::operators::scan::pipeline::decode_record;
use crate::exec::pre_decode_filter::{int_on_wire, strand_payload_str_unchecked};
use crate::val::Value;

/// The fields of one row, as the caller may see them.
pub(super) trait RowReader {
	/// Field `name` of the row, `NONE` when the row has no such field.
	fn field(&self, name: &FieldName) -> anyhow::Result<Scalar<'_>>;

	/// Field `name` of the row when it holds an integer, `None` when it holds
	/// anything else or is absent. A typed instruction's guard: a caller that
	/// gets `None` reads the field through [`Self::field`].
	fn field_int(&self, name: &FieldName) -> anyhow::Result<Option<i64>> {
		Ok(match self.field(name)? {
			Scalar::Number(crate::val::Number::Int(v)) => Some(v),
			_ => None,
		})
	}

	/// The whole row.
	fn record(&self) -> anyhow::Result<Value>;

	/// The whole row, borrowed, when the reader holds it as one value: what
	/// [`Self::record`] answers without a copy. `None` for a reader that
	/// assembles the row on demand.
	fn borrowed(&self) -> Option<&Value> {
		None
	}

	/// Whether the row is a record id rather than a record.
	fn is_record_id(&self) -> bool {
		false
	}

	/// Whether the row holds a key under any of `names`, whatever its value.
	fn has_any_field(&self, names: &[surrealdb_strand::Strand]) -> anyhow::Result<bool> {
		Ok(match self.record()? {
			Value::Object(object) => names.iter().any(|name| object.contains_key(name.as_str())),
			_ => false,
		})
	}
}

/// A top-level field name, encoded once for the lookups that find it.
///
/// `slot` is where the field sat among the keys of the last stored record it
/// was found in, `NO_SLOT` before it is found. Records of one shape keep a
/// field at one slot, so a lookup probes that slot first; the probe is checked
/// against the key, so a slot another record left is only a slower lookup.
///
/// The slot is a per-run lookup hint with no semantic identity: equality
/// compares the name alone, and a copy of the name rebuilt from its text (as
/// after any encoding and decoding of a program) starts at `NO_SLOT`. Nothing
/// may read the slot as a fact about the name.
#[derive(Debug)]
pub(super) struct FieldName {
	segment: PathSegment,
	slot: std::sync::atomic::AtomicU32,
}

impl PartialEq for FieldName {
	fn eq(&self, other: &Self) -> bool {
		self.segment.as_str() == other.segment.as_str()
	}
}

impl Eq for FieldName {}

/// [`FieldName::slot`] before the field is found.
const NO_SLOT: u32 = u32::MAX;

impl Clone for FieldName {
	fn clone(&self) -> Self {
		Self {
			segment: self.segment.clone(),
			slot: std::sync::atomic::AtomicU32::new(
				self.slot.load(std::sync::atomic::Ordering::Relaxed),
			),
		}
	}
}

impl FieldName {
	pub(super) fn new(name: &str) -> Self {
		Self {
			segment: PathSegment::new(Strand::from(name)),
			slot: std::sync::atomic::AtomicU32::new(NO_SLOT),
		}
	}

	pub(super) fn as_str(&self) -> &str {
		self.segment.as_str()
	}
}

/// A record as the storage cursor hands it over: its key and its encoded
/// value, borrowed for the visit.
pub(super) struct RevisionRow<'a> {
	key: &'a [u8],
	value: &'a [u8],
	/// The record's `data` field, where the current revision can be read in
	/// place.
	data: Option<&'a [u8]>,
	/// The record decoded whole, for a record `data` does not reach and for
	/// the fields read from the key.
	decoded: OnceCell<Value>,
	/// The walker over `data`'s fields, built at the first field read and
	/// shared by every later one; `None` when `data` is no object it walks.
	fields: OnceCell<Option<IndexedMapWalker<'a, Strand, Value>>>,
}

impl<'a> RevisionRow<'a> {
	pub(super) fn new(key: &'a [u8], value: &'a [u8]) -> Self {
		Self {
			key,
			value,
			data: rev2_record_data_bytes(value),
			decoded: OnceCell::new(),
			fields: OnceCell::new(),
		}
	}

	/// The encoded value of field `name` of the record's `data`, as
	/// [`Self::field_bytes`] finds it, walking `data` once per row.
	fn stored_field(&self, name: &FieldName) -> Option<Option<&'a [u8]>> {
		let walker = self
			.fields
			.get_or_init(|| indexed_map_walker_from_object_bytes(object_wire(self.data?)?).ok())
			.as_ref()?;
		if !walker.is_indexed() {
			return lookup_value_bytes_in_map(walker, &name.segment).ok();
		}
		use std::sync::atomic::Ordering::Relaxed;
		let hint = match name.slot.load(Relaxed) {
			NO_SLOT => None,
			slot => Some(slot),
		};
		let needle = name.segment.wire();
		let found = walker.find_value_bytes_with_hint(hint, |key: &[u8]| key.cmp(needle)).ok()?;
		if let Some(slot) = found.hint
			&& Some(slot) != hint
		{
			name.slot.store(slot, Relaxed);
		}
		Some(found.value)
	}

	fn decoded(&self) -> anyhow::Result<&Value> {
		if let Some(value) = self.decoded.get() {
			return Ok(value);
		}
		let value = decode_record(self.key, self.value).map_err(|e| anyhow::anyhow!("{e:?}"))?;
		Ok(self.decoded.get_or_init(|| value))
	}

	fn decoded_field(&self, name: &str) -> anyhow::Result<Scalar<'_>> {
		Ok(match self.decoded()? {
			Value::Object(object) => object.get(name).map(Scalar::of).unwrap_or(Scalar::None),
			_ => Scalar::None,
		})
	}

	/// The encoded value of field `name` of `data`, when `data` is an
	/// object the lookup can walk: `Some(None)` when it has no such field.
	#[cfg(test)]
	pub(super) fn field_bytes(data: &'a [u8], name: &FieldName) -> Option<Option<&'a [u8]>> {
		let map = indexed_map_walker_from_object_bytes(object_wire(data)?).ok()?;
		lookup_value_bytes_in_map(&map, &name.segment).ok()
	}
}

/// The variant id of [`Value::Object`] in a rev-2 [`Value`] wire: its position
/// among the variants.
const OBJECT_VARIANT: u8 = 10;

/// The wire of the [`Object`](crate::val::Object) a rev-2 `Value::Object`
/// wire holds: `rev = 2 || tag || u32_le length || object`.
fn object_wire(value: &[u8]) -> Option<&[u8]> {
	let [rev, tag, rest @ ..] = value else {
		return None;
	};
	let tag = Tag(*tag);
	if *rev != 2
		|| tag.variant_id() != OBJECT_VARIANT
		|| !matches!(tag.size_class().ok()?, SizeClass::Varlen)
	{
		return None;
	}
	let len = u32::from_le_bytes(rest.get(..4)?.try_into().ok()?) as usize;
	rest.get(4..4 + len)
}

impl RowReader for RevisionRow<'_> {
	fn field(&self, name: &FieldName) -> anyhow::Result<Scalar<'_>> {
		// A record's `id` is its key; the stored value does not hold it.
		if name.as_str() == "id" {
			return self.decoded_field("id");
		}
		if self.data.is_none() {
			return self.decoded_field(name.as_str());
		}
		match self.stored_field(name) {
			Some(Some(bytes)) => scalar_from_wire(bytes),
			Some(None) => Ok(Scalar::None),
			None => self.decoded_field(name.as_str()),
		}
	}

	fn field_int(&self, name: &FieldName) -> anyhow::Result<Option<i64>> {
		if name.as_str() == "id" {
			return Ok(None);
		}
		match self.stored_field(name) {
			Some(Some(bytes)) => Ok(int_on_wire(bytes)),
			Some(None) => Ok(None),
			None => Ok(match self.field(name)? {
				Scalar::Number(crate::val::Number::Int(v)) => Some(v),
				_ => None,
			}),
		}
	}

	fn record(&self) -> anyhow::Result<Value> {
		match self.decoded.get() {
			Some(value) => Ok(value.clone()),
			None => decode_record(self.key, self.value).map_err(|e| anyhow::anyhow!("{e:?}")),
		}
	}
}

/// A row held in memory: a record decoded by the scan, a row a sort ordered, a
/// group an aggregate finished.
pub(super) struct ValueRow<'a>(pub(super) &'a Value);

impl RowReader for ValueRow<'_> {
	fn field(&self, name: &FieldName) -> anyhow::Result<Scalar<'_>> {
		match self.0 {
			Value::Object(object) => {
				Ok(object.get(name.as_str()).map(Scalar::of).unwrap_or(Scalar::None))
			}
			_ => anyhow::bail!("the VM read a field from a row that is not an object"),
		}
	}

	fn record(&self) -> anyhow::Result<Value> {
		Ok(self.0.clone())
	}

	fn borrowed(&self) -> Option<&Value> {
		Some(self.0)
	}

	fn is_record_id(&self) -> bool {
		matches!(self.0, Value::RecordId(_))
	}

	fn has_any_field(&self, names: &[surrealdb_strand::Strand]) -> anyhow::Result<bool> {
		Ok(match self.0 {
			Value::Object(object) => names.iter().any(|name| object.contains_key(name.as_str())),
			_ => false,
		})
	}
}

/// The scalar an encoded [`Value`] denotes, borrowing a string from `bytes`.
pub(super) fn scalar_from_wire(bytes: &[u8]) -> anyhow::Result<Scalar<'_>> {
	if let Some(s) = strand_payload_str_unchecked(bytes) {
		return Ok(Scalar::Str(s));
	}
	if let Some(v) = int_on_wire(bytes) {
		return Ok(Scalar::Number(crate::val::Number::Int(v)));
	}
	// `NONE`, `NULL` and booleans are whole in their tag and the byte after.
	if let [2, tag, rest @ ..] = bytes {
		let tag = Tag(*tag);
		match (tag.variant_id(), tag.size_class()) {
			(0, Ok(SizeClass::Inline)) => return Ok(Scalar::None),
			(1, Ok(SizeClass::Inline)) => return Ok(Scalar::Null),
			(2, Ok(SizeClass::Fixed)) => {
				if let Some(b) = rest.first() {
					return Ok(Scalar::Bool(*b != 0));
				}
			}
			// A number, duration, datetime or uuid is decoded from its payload
			// by its own type, which reads the slice in place.
			(variant @ (3 | 5 | 6 | 7), Ok(SizeClass::Varlen)) => {
				if let Some(len) = rest.get(..4) {
					let len = u32::from_le_bytes(len.try_into().expect("four bytes")) as usize;
					if let Some(mut payload) = rest.get(4..4 + len) {
						use revision::DeserializeRevisioned as D;
						let scalar = match variant {
							3 => <crate::val::Number as D>::deserialize_revisioned(&mut payload)
								.map(Scalar::Number),
							5 => <crate::val::Duration as D>::deserialize_revisioned(&mut payload)
								.map(Scalar::Duration),
							6 => <crate::val::Datetime as D>::deserialize_revisioned(&mut payload)
								.map(Scalar::Datetime),
							_ => <crate::val::Uuid as D>::deserialize_revisioned(&mut payload)
								.map(Scalar::Uuid),
						};
						if let Ok(scalar) = scalar
							&& payload.is_empty()
						{
							return Ok(scalar);
						}
					}
				}
			}
			_ => {}
		}
	}
	let value: Value = revision::from_slice(bytes)?;
	Ok(match value {
		Value::None => Scalar::None,
		Value::Null => Scalar::Null,
		Value::Bool(v) => Scalar::Bool(v),
		Value::Number(v) => Scalar::Number(v),
		Value::Duration(v) => Scalar::Duration(v),
		Value::Datetime(v) => Scalar::Datetime(v),
		Value::Uuid(v) => Scalar::Uuid(v),
		other => Scalar::Value(Cow::Owned(other)),
	})
}
