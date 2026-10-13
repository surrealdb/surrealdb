//! Reading a stored record's fields where they lie.
//!
//! A [`RowReader`] answers a field of the row as a [`Scalar`] that borrows
//! from where the row lies: the stored bytes, or a value in memory. The VM
//! reads rows only through it, so the storage encoding is this module's
//! concern alone.
//!
//! [`StoredRow`] reads a record in either of its encodings. A field is found
//! by a lookup over the stored object and decoded alone: a string or bytes is
//! borrowed, a number, boolean, duration, datetime or uuid is decoded inline,
//! and any other kind is decoded into an owned [`Value`]. In the stored-value
//! encoding the lookup is a search of the object's sorted keys that probes
//! first the slot the field held in the last record it was found in; a record
//! in the current `revision` revision is walked through its encoded map. A
//! record neither reaches — data that is not an object, an older revision — is
//! decoded whole, once, and read from that.

// Runtime support that lowered programs call into.
#![allow(dead_code)]

use std::borrow::Cow;
use std::cell::OnceCell;

use revision::optimised::IndexedMapWalker;
use revision::optimised::tag::{SizeClass, Tag};
use surrealdb_strand::Strand;

use super::scalar::Scalar;
use crate::catalog::StoredRecord;
use crate::exec::object_extract::{
	PathSegment, indexed_map_walker_from_object_bytes, lookup_value_bytes_in_map,
	rev2_record_data_bytes,
};
use crate::exec::operators::scan::pipeline::decode_record_value;
use crate::exec::pre_decode_filter::{int_on_wire, strand_payload_str};
use crate::key::KVKeyDecode as _;
use crate::val::stored::{Kind, ObjectRef, OrderedObjectRef, ValueRef};
use crate::val::{Datetime, Duration, Number, Uuid, Value};

/// The fields of one row, as the caller may see them.
pub(super) trait RowReader {
	/// Field `name` of the row, `NONE` when the row has no such field.
	fn field(&self, name: &FieldName) -> anyhow::Result<Scalar<'_>>;

	/// Field `name` of the row when it holds an integer, `None` when it holds
	/// anything else or is absent. A typed instruction's guard: a caller that
	/// gets `None` reads the field through [`Self::field`].
	fn field_int(&self, name: &FieldName) -> anyhow::Result<Option<i64>> {
		Ok(match self.field(name)? {
			Scalar::Number(Number::Int(v)) => Some(v),
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

	/// The refusal a read of this row raises where it is a KNN query vector's
	/// row being scanned ([`ForbiddenRowReader`]), for a step that takes
	/// another value where the pipeline has no row; `None` for any other row.
	fn forbids(&self) -> Option<anyhow::Error> {
		None
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
///
/// The caller has admitted the record's generation against its table's layouts
/// ([`crate::kvs::RowGate`], `Transaction::decode_record`) before it builds the
/// row: nothing here checks a generation. A row whose envelope this build does
/// not read parses as no layout, is decoded whole, and is refused there with
/// the typed envelope error.
///
/// A row never holds a packed record: the host decodes a packed record through
/// its table's layouts before a row is built, and runs it as a value row. One
/// that reaches a `StoredRow` has no layouts to be read through, is decoded
/// whole, and is refused there as a record read without its table's layouts.
pub(super) struct StoredRow<'a> {
	key: &'a [u8],
	value: &'a [u8],
	/// Where the record's fields can be read in place.
	layout: Layout<'a>,
	/// The record decoded whole, for a record the layout does not reach and
	/// for the fields read from the key.
	decoded: OnceCell<Value>,
	/// The record's `id`, read from its key alone, for a record whose data is
	/// an object read in place.
	id: OnceCell<Value>,
	/// The object read in place with its key order confirmed, set at the first
	/// field read.
	ordered: OnceCell<OrderedObjectRef<'a>>,
}

/// How a record's fields are read in place.
enum Layout<'a> {
	/// The stored-value encoding, whose data is an object.
	Stored(ObjectRef<'a>),
	/// The `data` field of a record in the current `revision` revision.
	Revision {
		data: &'a [u8],
		/// The walker over `data`'s fields, built at the first field read and
		/// shared by every later one; `None` when `data` is no object it walks.
		fields: OnceCell<Option<IndexedMapWalker<'a, Strand, Value>>>,
	},
	/// Nothing is read in place: the record is decoded whole.
	Whole,
}

impl<'a> Layout<'a> {
	/// How the fields of the record `value` are read in place, dispatched on its
	/// first bytes: a document of the stored-value grammar, a `revision`
	/// record, or neither.
	///
	/// `value` is admitted by the caller ([`StoredRow`]): the generation of a
	/// document is not read here.
	fn of(value: &'a [u8]) -> Self {
		if value.first() == Some(&StoredRecord::MARKER) {
			// A record that does not parse, or whose data is not an object, is
			// decoded whole, which reports what is wrong with it.
			// A packed record has no document, and a document a whole decode refuses is
			// not read in place: either way it is decoded whole.
			return match StoredRecord::parse(value) {
				Ok(Some(record)) => match record.checked_document() {
					Ok(Some(document)) => match document.value().and_then(|v| v.as_object()) {
						Ok(Some(object)) => Self::Stored(object),
						_ => Self::Whole,
					},
					_ => Self::Whole,
				},
				_ => Self::Whole,
			};
		}
		match rev2_record_data_bytes(value) {
			Some(data) => Self::Revision {
				data,
				fields: OnceCell::new(),
			},
			None => Self::Whole,
		}
	}
}

impl<'a> StoredRow<'a> {
	pub(super) fn new(key: &'a [u8], value: &'a [u8]) -> Self {
		Self {
			key,
			value,
			layout: Layout::of(value),
			decoded: OnceCell::new(),
			id: OnceCell::new(),
			ordered: OnceCell::new(),
		}
	}

	/// `object`, the row's [`Layout::Stored`] object, with its key order
	/// confirmed once per row. An object whose keys do not ascend is refused,
	/// as a whole decode refuses it.
	fn ordered(&self, object: &ObjectRef<'a>) -> anyhow::Result<OrderedObjectRef<'a>> {
		if let Some(ordered) = self.ordered.get() {
			return Ok(*ordered);
		}
		let ordered = object.ordered()?;
		Ok(*self.ordered.get_or_init(|| ordered))
	}

	/// The `id` of a record whose data is an object: its key, decoded alone.
	fn key_id(&self) -> anyhow::Result<&Value> {
		if let Some(id) = self.id.get() {
			return Ok(id);
		}
		let key = crate::key::schema::RecordKey::decode_key(self.key)?;
		let id = Value::RecordId(crate::val::RecordId {
			table: key.tb.into_owned(),
			key: key.id.into_owned(),
		});
		Ok(self.id.get_or_init(|| id))
	}

	/// The encoded value of field `name` of a `revision` record's `data`, as
	/// [`Self::field_bytes`] finds it, walking `data` once per row: `None` when
	/// the record is not one the walker reads.
	fn revision_field(&self, name: &FieldName) -> Option<Option<&'a [u8]>> {
		let Layout::Revision {
			data,
			fields,
		} = &self.layout
		else {
			return None;
		};
		let walker = fields
			.get_or_init(|| indexed_map_walker_from_object_bytes(object_wire(data)?).ok())
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
		let value = decode_record_value(self.key, self.value, None)?;
		Ok(self.decoded.get_or_init(|| value))
	}

	fn decoded_field(&self, name: &str) -> anyhow::Result<Scalar<'_>> {
		Ok(match self.decoded()? {
			Value::Object(object) => object.get(name).map(Scalar::of).unwrap_or(Scalar::None),
			_ => Scalar::None,
		})
	}

	/// The encoded value of field `name` of `data`, when `data` is a
	/// `revision` object the lookup can walk: `Some(None)` when it has no such
	/// field.
	#[cfg(test)]
	pub(super) fn field_bytes(data: &'a [u8], name: &FieldName) -> Option<Option<&'a [u8]>> {
		let map = indexed_map_walker_from_object_bytes(object_wire(data)?).ok()?;
		lookup_value_bytes_in_map(&map, &name.segment).ok()
	}
}

/// Field `name` of a stored-value object, probing the slot the field held in
/// the last record it was found in before searching, and remembering where it
/// was found.
#[inline]
fn stored_field<'a>(
	object: &OrderedObjectRef<'a>,
	name: &FieldName,
) -> anyhow::Result<Option<ValueRef<'a>>> {
	use std::sync::atomic::Ordering::Relaxed;
	let hint = name.slot.load(Relaxed);
	let mut slot = hint;
	let found = object.get_hinted(name.as_str(), &mut slot)?;
	if slot != hint {
		name.slot.store(slot, Relaxed);
	}
	Ok(found)
}

/// The scalar a stored value denotes, borrowing a string or bytes from where
/// it lies.
pub(super) fn scalar_from_stored(v: ValueRef<'_>) -> anyhow::Result<Scalar<'_>> {
	fn some<T>(v: Option<T>) -> anyhow::Result<T> {
		v.ok_or_else(|| anyhow::anyhow!("a stored value whose kind and tag disagree"))
	}
	Ok(match v.kind() {
		Kind::None => Scalar::None,
		Kind::Null => Scalar::Null,
		Kind::Bool => Scalar::Bool(some(v.as_bool())?),
		Kind::Int => Scalar::Number(Number::Int(some(v.as_int()?)?)),
		Kind::Float => Scalar::Number(Number::Float(some(v.as_float()?)?)),
		Kind::Decimal => Scalar::Number(Number::Decimal(some(v.as_decimal()?)?)),
		Kind::String => Scalar::Str(some(v.as_str()?)?),
		Kind::Duration => Scalar::Duration(Duration(some(v.as_duration()?)?)),
		Kind::Datetime => Scalar::Datetime(Datetime(some(v.as_datetime()?)?)),
		Kind::Uuid => Scalar::Uuid(Uuid(some(v.as_uuid()?)?)),
		Kind::Bytes => Scalar::Bytes(some(v.as_bytes())?),
		_ => Scalar::Value(Cow::Owned(v.decode()?)),
	})
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
	rest.get(4..4usize.checked_add(len)?)
}

impl RowReader for StoredRow<'_> {
	fn field(&self, name: &FieldName) -> anyhow::Result<Scalar<'_>> {
		// A record's `id` is its key, whatever the stored value holds.
		if name.as_str() == "id" {
			if let Layout::Stored(object) = &self.layout {
				self.ordered(object)?;
				return Ok(Scalar::of(self.key_id()?));
			}
			return self.decoded_field("id");
		}
		match &self.layout {
			Layout::Stored(object) => match stored_field(&self.ordered(object)?, name)? {
				Some(v) => scalar_from_stored(v),
				None => Ok(Scalar::None),
			},
			Layout::Revision {
				..
			} => match self.revision_field(name) {
				Some(Some(bytes)) => scalar_from_wire(bytes),
				Some(None) => Ok(Scalar::None),
				None => self.decoded_field(name.as_str()),
			},
			Layout::Whole => self.decoded_field(name.as_str()),
		}
	}

	fn field_int(&self, name: &FieldName) -> anyhow::Result<Option<i64>> {
		// A record's `id` is a record id, never an integer; the read still refuses a record the
		// whole decode refuses.
		if name.as_str() == "id" {
			match &self.layout {
				Layout::Stored(object) => {
					self.ordered(object)?;
				}
				Layout::Whole => {
					self.decoded()?;
				}
				Layout::Revision {
					..
				} => {}
			}
			return Ok(None);
		}
		if let Layout::Stored(object) = &self.layout {
			return Ok(match stored_field(&self.ordered(object)?, name)? {
				Some(v) => v.as_int()?,
				None => None,
			});
		}
		match self.revision_field(name) {
			Some(Some(bytes)) => Ok(int_on_wire(bytes)),
			Some(None) => Ok(None),
			None => Ok(match self.field(name)? {
				Scalar::Number(Number::Int(v)) => Some(v),
				_ => None,
			}),
		}
	}

	fn record(&self) -> anyhow::Result<Value> {
		match self.decoded.get() {
			Some(value) => Ok(value.clone()),
			None => decode_record_value(self.key, self.value, None),
		}
	}

	fn has_any_field(&self, names: &[Strand]) -> anyhow::Result<bool> {
		if let Layout::Stored(object) = &self.layout {
			let object = self.ordered(object)?;
			// The record is an object, so a read gives it the key's `id`.
			for name in names {
				if name.as_str() == "id" || object.get(name.as_str())?.is_some() {
					return Ok(true);
				}
			}
			return Ok(false);
		}
		Ok(match self.decoded()? {
			Value::Object(object) => names.iter().any(|name| object.contains_key(name.as_str())),
			_ => false,
		})
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

/// The row of a pipeline that runs over none where a per-row evaluation would
/// have run over the row being scanned: a KNN query vector's
/// ([`ForbiddenRow`](crate::exec::context::ForbiddenRow)). Every read of it
/// fails with the vector's refusal.
pub(super) struct ForbiddenRowReader<'a>(pub(super) &'a crate::exec::context::ForbiddenRow);

impl RowReader for ForbiddenRowReader<'_> {
	fn field(&self, _name: &FieldName) -> anyhow::Result<Scalar<'_>> {
		Err(self.0.refusal())
	}

	fn field_int(&self, _name: &FieldName) -> anyhow::Result<Option<i64>> {
		Err(self.0.refusal())
	}

	fn record(&self) -> anyhow::Result<Value> {
		Err(self.0.refusal())
	}

	fn has_any_field(&self, _names: &[surrealdb_strand::Strand]) -> anyhow::Result<bool> {
		Err(self.0.refusal())
	}

	fn forbids(&self) -> Option<anyhow::Error> {
		Some(self.0.refusal())
	}
}

/// The scalar an encoded [`Value`] denotes, borrowing a string from `bytes`.
pub(super) fn scalar_from_wire(bytes: &[u8]) -> anyhow::Result<Scalar<'_>> {
	if let Some(s) = strand_payload_str(bytes) {
		return Ok(Scalar::Str(s));
	}
	if let Some(v) = int_on_wire(bytes) {
		return Ok(Scalar::Number(Number::Int(v)));
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
				if let Some(len) = rest.first_chunk::<4>() {
					let len = u32::from_le_bytes(*len) as usize;
					if let Some(mut payload) =
						4usize.checked_add(len).and_then(|end| rest.get(4..end))
					{
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

#[cfg(test)]
mod tests {
	use super::*;
	use crate::val::stored;

	/// The format the generation-0 samples are written in.
	const DOCUMENT: crate::catalog::RecordFormat = crate::catalog::RecordFormat::Document {
		generation: None,
	};

	/// `src`, a SurrealQL value literal, as a value.
	fn val(src: &str) -> Value {
		Value::from(crate::syn::value(src).expect("parses"))
	}

	/// Every kind a field can hold reads back as the value that was stored,
	/// from the stored bytes, and a field the record lacks reads as `NONE`.
	#[test]
	fn a_field_reads_back_as_stored() {
		let data = val("{ s: 'text', i: 7, f: 1.5f, d: 2dec, b: true, n: NULL, dur: 1s, \
			 dt: d'2024-01-01T00:00:00Z', u: u'018f9b8e-0000-7000-8000-000000000000', \
			 arr: [1, 'a'], obj: { x: 1 }, bytes: b\"6162\" }");
		let record = crate::catalog::Record::new(data.clone());
		let value = revision::to_vec(&record).expect("encodes");
		let data_wire = rev2_record_data_bytes(&value).expect("a current record");
		assert!(object_wire(data_wire).is_some(), "the data is an object the reader walks");
		let Value::Object(fields) = &data else {
			unreachable!()
		};
		for (name, stored) in fields.iter() {
			let bytes = StoredRow::field_bytes(data_wire, &FieldName::new(name))
				.expect("walkable")
				.expect("present");
			let scalar = scalar_from_wire(bytes).expect("decodes");
			assert_eq!(scalar.to_value(), *stored, "{name}");
			// A scalar kind is read in place, not decoded into a value.
			if !matches!(stored, Value::Array(_) | Value::Object(_) | Value::Bytes(_)) {
				assert!(scalar.as_whole().is_none(), "{name} read in place");
			}
		}
		assert_eq!(StoredRow::field_bytes(data_wire, &FieldName::new("absent")), Some(None));
		let Some(Some(s)) = StoredRow::field_bytes(data_wire, &FieldName::new("s")) else {
			unreachable!()
		};
		assert!(matches!(scalar_from_wire(s), Ok(Scalar::Str("text"))), "a string is borrowed");
	}

	/// A value whose tag is the whole value is refused when bytes follow the
	/// tag, before any read of it, as a whole decode refuses it; without them
	/// it reads as its tag.
	#[test]
	fn a_payload_on_a_whole_tag_is_refused() {
		for tag in [stored::tag::SMALL_INT, stored::tag::SMALL_INT + 5, u8::MAX - 1] {
			let clean = [tag];
			let v = ValueRef::new(&clean).expect("a tag");
			assert!(scalar_from_stored(v).is_ok(), "{tag:#x}");
			let stray = [tag, 0x01];
			assert!(ValueRef::new(&stray).is_err(), "{tag:#x}");
			assert!(stored::decode(&stray).is_err(), "{tag:#x}: a whole decode refuses it too");
		}
	}

	/// The key of record `rid` in namespace and database 1.
	fn key_of(rid: &crate::val::RecordId) -> Vec<u8> {
		use surrealdb_kvs::key::KVKey;
		crate::key::schema::RecordKey {
			ns: crate::catalog::NamespaceId(1),
			db: crate::catalog::DatabaseId(1),
			tb: Cow::Borrowed(&rid.table),
			id: Cow::Borrowed(&rid.key),
		}
		.encode_key()
		.expect("encodes")
		.to_vec()
	}

	/// In the stored-value encoding, every kind a field can hold reads back in
	/// place as the value that was stored, `id` reads as the key whatever the
	/// bytes hold, and a field the record lacks reads as `NONE`.
	#[test]
	fn a_stored_value_field_reads_back_in_place() {
		let rid = crate::val::RecordId::new("t".into(), crate::val::RecordIdKey::Number(1));
		let data =
			val("{ s: 'text', i: 7, big: 1000000000000, f: 1.5f, d: 2dec, b: true, n: NULL, \
			 dur: 1s, dt: d'2024-01-01T00:00:00Z', u: u'018f9b8e-0000-7000-8000-000000000000', \
			 arr: [1, 'a'], floats: [1.5f, 2.5f], obj: { x: 1 }, bytes: b\"6162\", \
			 none: NONE }");
		let Value::Object(mut fields) = data else {
			unreachable!()
		};
		fields.insert("id".to_owned(), Value::RecordId(rid.clone()));
		let data = Value::Object(fields.clone());
		let key = key_of(&rid);
		for keyed in [Some(&rid), None] {
			let value =
				crate::catalog::encode_record(DOCUMENT, None, &data, keyed).expect("encodes");
			let row = StoredRow::new(&key, &value);
			assert!(matches!(row.layout, Layout::Stored(_)), "read in place");
			for (name, stored) in fields.iter() {
				let scalar = row.field(&FieldName::new(name)).expect("reads");
				assert_eq!(scalar.to_value(), *stored, "{name}");
				if !matches!(stored, Value::Array(_) | Value::Object(_) | Value::RecordId(_)) {
					assert!(scalar.as_whole().is_none(), "{name} read in place");
				}
				let int = match stored {
					Value::Number(crate::val::Number::Int(i)) => Some(*i),
					_ => None,
				};
				if name != "id" {
					assert_eq!(row.field_int(&FieldName::new(name)).expect("reads"), int, "{name}");
				}
			}
			assert!(matches!(row.field(&FieldName::new("absent")), Ok(Scalar::None)));
			assert!(matches!(row.field(&FieldName::new("s")), Ok(Scalar::Str("text"))));
			assert!(row.decoded.get().is_none(), "no field read decoded the record");
			assert_eq!(row.record().expect("decodes"), data);
			let names = |n: &[&str]| n.iter().map(|n| Strand::from(*n)).collect::<Vec<_>>();
			assert!(row.has_any_field(&names(&["absent", "dt"])).expect("reads"));
			assert!(row.has_any_field(&names(&["id"])).expect("reads"));
			assert!(!row.has_any_field(&names(&["absent", "missing"])).expect("reads"));
		}
	}

	/// A record whose object keys are out of order is refused by a field read,
	/// as a whole decode refuses it, whether the read finds its field or not.
	#[test]
	fn a_read_over_keys_out_of_order_is_refused() {
		let rid = crate::val::RecordId::new("t".into(), crate::val::RecordIdKey::Number(1));
		let key = key_of(&rid);
		let data = val("{ aa: 1, bb: 2 }");
		let mut value =
			crate::catalog::encode_record(DOCUMENT, None, &data, Some(&rid)).expect("encodes");
		let at = |v: &[u8], needle: &[u8]| {
			v.windows(needle.len()).position(|w| w == needle).expect("the key is stored")
		};
		let (a, b) = (at(&value, b"aa"), at(&value, b"bb"));
		value[a..a + 2].copy_from_slice(b"bb");
		value[b..b + 2].copy_from_slice(b"aa");
		assert!(decode_record_value(&key, &value, None).is_err(), "a whole decode refuses it");
		let row = StoredRow::new(&key, &value);
		assert!(matches!(row.layout, Layout::Whole), "a refused document is decoded whole");
		for name in ["aa", "cc", "id"] {
			let row = StoredRow::new(&key, &value);
			assert!(row.field(&FieldName::new(name)).is_err(), "{name}");
			let row = StoredRow::new(&key, &value);
			assert!(row.field_int(&FieldName::new(name)).is_err(), "{name}");
			let row = StoredRow::new(&key, &value);
			assert!(row.has_any_field(&[Strand::from(name)]).is_err(), "{name}");
		}
	}

	/// A field's slot is remembered from one record to the next, and a slot
	/// another shape of record left is only a slower lookup.
	#[test]
	fn a_stored_value_field_keeps_its_slot() {
		let name = FieldName::new("m");
		let rid = crate::val::RecordId::new("t".into(), crate::val::RecordIdKey::Number(1));
		let key = key_of(&rid);
		for (record, at) in
			[("{ a: 1, m: 2, z: 3 }", 1), ("{ m: 2 }", 0), ("{ a: 1, b: 1, m: 2 }", 2)]
		{
			let value =
				crate::catalog::encode_record(DOCUMENT, None, &val(record), None).expect("encodes");
			let row = StoredRow::new(&key, &value);
			assert_eq!(row.field_int(&name).expect("reads"), Some(2), "{record}");
			assert_eq!(name.slot.load(std::sync::atomic::Ordering::Relaxed), at, "{record}");
		}
	}

	/// A stored-value record whose data is no object, and one that does not
	/// parse, are decoded whole, and the decode reports what is wrong.
	#[test]
	fn a_stored_value_record_not_read_in_place_is_decoded() {
		let rid = crate::val::RecordId::new("t".into(), crate::val::RecordIdKey::Number(1));
		let key = key_of(&rid);
		let value =
			crate::catalog::encode_record(DOCUMENT, None, &Value::from(5), None).expect("encodes");
		let row = StoredRow::new(&key, &value);
		assert!(matches!(row.layout, Layout::Whole));
		assert!(matches!(row.field(&FieldName::new("a")), Ok(Scalar::None)));
		assert_eq!(row.record().expect("decodes"), Value::from(5));
		let broken = [StoredRecord::MARKER, 0x80, 0x00];
		let row = StoredRow::new(&key, &broken);
		assert!(matches!(row.layout, Layout::Whole));
		assert!(row.field(&FieldName::new("a")).is_err());
	}

	/// A keyed document that stores an `"id"` is not read in place, so a field
	/// read refuses it as a whole decode does, whichever field is read.
	#[test]
	fn a_keyed_document_that_stores_an_id_is_refused_in_place() {
		let rid = crate::val::RecordId::new("t".into(), crate::val::RecordIdKey::Number(1));
		let key = key_of(&rid);
		let Value::Object(mut fields) = val("{ a: 1 }") else {
			unreachable!()
		};
		fields.insert("id".to_owned(), Value::RecordId(rid));
		// Written unkeyed, the id is stored; marking the record keyed sets the header bit
		// the reader takes as "the id was left out".
		let mut value = crate::catalog::encode_record(DOCUMENT, None, &Value::Object(fields), None)
			.expect("encodes");
		assert_eq!(value[0], StoredRecord::MARKER);
		value[1] |= StoredRecord::KEYED;
		assert!(decode_record_value(&key, &value, None).is_err(), "a whole decode refuses it");
		let row = StoredRow::new(&key, &value);
		assert!(matches!(row.layout, Layout::Whole), "not read in place");
		assert!(row.field(&FieldName::new("a")).is_err());
		let row = StoredRow::new(&key, &value);
		assert!(row.field(&FieldName::new("id")).is_err());
	}

	/// A malformed field the read does not ask for refuses the read, as the
	/// whole decode refuses the record.
	#[test]
	fn a_malformed_field_the_read_does_not_ask_for_is_refused() {
		let rid = crate::val::RecordId::new("t".into(), crate::val::RecordIdKey::Number(1));
		let key = key_of(&rid);
		let mut value =
			crate::catalog::encode_record(DOCUMENT, None, &val("{ a: 1, b: 'xy' }"), None)
				.expect("encodes");
		let at = value.windows(2).position(|w| w == b"xy").expect("the string is stored");
		// 0xFF is never valid UTF-8, so the string does not decode.
		value[at] = 0xFF;
		assert!(decode_record_value(&key, &value, None).is_err(), "a whole decode refuses it");
		let row = StoredRow::new(&key, &value);
		assert!(matches!(row.layout, Layout::Whole), "not read in place");
		assert!(row.field(&FieldName::new("a")).is_err(), "the read of a refuses too");
	}
}
