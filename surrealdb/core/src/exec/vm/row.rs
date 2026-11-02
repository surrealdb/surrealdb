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

use std::cell::OnceCell;

use revision::optimised::IndexedMapWalker;
use revision::optimised::tag::{SizeClass, Tag};
use surrealdb_strand::Strand;

use super::scalar::{Held, Scalar};
use crate::catalog::{StoredRecord, TableLayouts};
use crate::exec::object_extract::{
	PathSegment, indexed_map_walker_from_object_bytes, lookup_value_bytes_in_map,
	rev2_record_data_bytes,
};
use crate::exec::operators::extract_vector;
use crate::exec::operators::scan::pipeline::decode_record_value;
use crate::exec::pre_decode_filter::{int_on_wire, strand_payload_str};
use crate::key::KVKeyDecode as _;
use crate::val::field_path::{FieldPath, FieldPathPart};
use crate::val::stored::{ArrayRef, Kind, OrderedObjectRef, ValueRef};
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
			Scalar::Number(Number::I8(v)) => Some(v as i64),
			Scalar::Number(Number::U8(v)) => Some(v as i64),
			Scalar::Number(Number::I16(v)) => Some(v as i64),
			Scalar::Number(Number::U16(v)) => Some(v as i64),
			Scalar::Number(Number::I32(v)) => Some(v as i64),
			Scalar::Number(Number::U32(v)) => Some(v as i64),
			Scalar::Number(Number::U64(v)) => i64::try_from(v).ok(),
			Scalar::Number(
				Number::Float(_)
				| Number::Decimal(_)
				| Number::F16(_)
				| Number::F32(_)
				| Number::BigDecimal(_),
			) => None,
			Scalar::None
			| Scalar::Null
			| Scalar::Bool(_)
			| Scalar::Str(_)
			| Scalar::Duration(_)
			| Scalar::Datetime(_)
			| Scalar::Uuid(_)
			| Scalar::Bytes(_)
			| Scalar::Value(_) => None,
		})
	}

	/// The whole row.
	fn record(&self) -> anyhow::Result<Value>;

	/// Reads the vector at `path` of the row into `out`, replacing its contents, as
	/// [`extract_vector`] reads it from the whole row, and answers whether there is one of at most
	/// `limit` numbers. `path` starts at field `name`, and `rest` is the rest of it.
	///
	/// A reader that holds the row whole reads the path from it; any other reads field `name` and
	/// the rest of the path from that field's value. A reader that reads its row where it lies
	/// reads the vector's numbers from there instead, with the same answer.
	fn knn_vector(
		&self,
		path: &FieldPath,
		name: &FieldName,
		rest: &FieldPath,
		out: &mut Vec<Number>,
		limit: usize,
	) -> anyhow::Result<bool> {
		Ok(match self.borrowed() {
			Some(whole) => extract_vector(whole, path, out, limit),
			None => extract_vector(&self.field(name)?.as_value(), rest, out, limit),
		})
	}

	/// The whole row, borrowed, when the reader holds it as one value: what
	/// [`Self::record`] answers without a copy. `None` for a reader that
	/// assembles the row on demand.
	fn borrowed(&self) -> Option<&Value> {
		None
	}

	/// Reads the numbers of field `name` of the row into `out`, replacing its contents, where the
	/// field holds an array of numbers only, and answers whether it does: the vector a two-argument
	/// `vector::*` builtin reads from the field as its argument. `false` for any other value,
	/// nested arrays included, which the builtin reads decoded.
	fn vector_field(&self, name: &FieldName, out: &mut Vec<Number>) -> anyhow::Result<bool> {
		Ok(scalar_numbers(&self.field(name)?, out))
	}

	/// Reads the `(lon, lat)` coordinates of field `name` of the row where the field holds a
	/// `Geometry::Point`, and answers `None` for any other value or when the field is absent.
	fn point_field(&self, name: &FieldName) -> anyhow::Result<Option<(f64, f64)>> {
		Ok(match self.field(name)? {
			Scalar::Value(v) => {
				if let Value::Geometry(crate::val::Geometry::Point(pt)) = v.as_ref() {
					Some((pt.x(), pt.y()))
				} else {
					None
				}
			}
			Scalar::None
			| Scalar::Null
			| Scalar::Bool(_)
			| Scalar::Number(_)
			| Scalar::Str(_)
			| Scalar::Duration(_)
			| Scalar::Datetime(_)
			| Scalar::Uuid(_)
			| Scalar::Bytes(_) => None,
		})
	}

	/// Whether the row is a record id rather than a record.
	fn is_record_id(&self) -> bool {
		false
	}

	/// Whether the row holds a key under any of `names`, whatever its value.
	fn has_any_field(&self, names: &[surrealdb_strand::Strand]) -> anyhow::Result<bool> {
		Ok(match self.record()?.as_object() {
			Some(object) => names.iter().any(|name| object.contains_key(name.as_str())),
			None => false,
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
///
/// `column` is the same kind of hint for a packed row: the index the name's
/// column held in the descriptor of the last packed row it was found in,
/// `NO_SLOT` before it is found. A packed row verifies it against its own
/// descriptor on every read ([`CheckedPackedRow::root_column`]), so a hint
/// another generation's descriptor left is only a slower lookup.
///
/// [`CheckedPackedRow::root_column`]: crate::catalog::packed::CheckedPackedRow::root_column
#[derive(Debug)]
pub(super) struct FieldName {
	segment: PathSegment,
	slot: std::sync::atomic::AtomicU32,
	column: std::sync::atomic::AtomicU32,
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
			column: std::sync::atomic::AtomicU32::new(
				self.column.load(std::sync::atomic::Ordering::Relaxed),
			),
		}
	}
}

impl FieldName {
	pub(super) fn new(name: &str) -> Self {
		Self {
			segment: PathSegment::new(Strand::from(name)),
			slot: std::sync::atomic::AtomicU32::new(NO_SLOT),
			column: std::sync::atomic::AtomicU32::new(NO_SLOT),
		}
	}

	pub(super) fn as_str(&self) -> &str {
		self.segment.as_str()
	}

	/// The packed-column hint of the name.
	pub(super) fn column_hint(&self) -> &std::sync::atomic::AtomicU32 {
		&self.column
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
/// A `StoredRow` never holds a packed record: the host reads a packed record
/// through its table's layouts, in place as a [`PackedRowReader`] or decoded
/// whole and run as a value row, before a `StoredRow` is built. One that reaches
/// a `StoredRow` has no layouts to be read through, is decoded whole, and is
/// refused there as a record read without its table's layouts.
///
/// [`PackedRowReader`]: super::row_packed::PackedRowReader
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
}

/// How a record's fields are read in place.
enum Layout<'a> {
	/// The stored-value encoding, whose data is a validated object.
	Stored(OrderedObjectRef<'a>),
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
	/// document is not read here. `layouts` are the layouts that admitted it: a
	/// document of a generation they vouch for is read in place unwalked
	/// ([`StoredRecord::admit_document`]), and any other document is walked first.
	fn of(value: &'a [u8], layouts: Option<&TableLayouts>) -> Self {
		if value.first() == Some(&StoredRecord::MARKER) {
			// A record that does not parse, or whose data is not an object, is
			// decoded whole, which reports what is wrong with it.
			// A packed record has no document, and a document a whole decode refuses is
			// not read in place: either way it is decoded whole.
			return match StoredRecord::parse(value) {
				Ok(Some(record)) => match record.admit_document(layouts) {
					Ok(Some(document)) => match document.value().and_then(|v| v.as_object()) {
						Ok(Some(object)) => Self::Stored(object.validated()),
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
		Self::admitted(key, value, None)
	}

	/// The row of the record `key` names, whose generation `layouts` admitted:
	/// a document of a generation they vouch for is read in place without a
	/// walk of its bytes.
	pub(super) fn admitted(key: &'a [u8], value: &'a [u8], layouts: Option<&TableLayouts>) -> Self {
		Self {
			key,
			value,
			layout: Layout::of(value, layouts),
			decoded: OnceCell::new(),
			id: OnceCell::new(),
		}
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
		Ok(match self.decoded()?.as_object() {
			Some(object) => object.get(name).map(Scalar::of).unwrap_or(Scalar::None),
			None => Scalar::None,
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

/// Reads the vector at `rest` of the stored value `value` into `out`, replacing its contents, as
/// [`extract_vector`] reads the vector at that path of the value `value` decodes to, and answers
/// whether there is one. `None` where the path crosses a value this read does not walk, for the
/// caller to read decoded: a step other than a field, or an array that holds values rather than
/// numbers, whose field a step projects from each element.
pub(super) fn stored_path_vector(
	mut value: ValueRef<'_>,
	rest: &FieldPath,
	out: &mut Vec<Number>,
	limit: usize,
) -> anyhow::Result<Option<bool>> {
	for (at, part) in rest.0.iter().enumerate() {
		let FieldPathPart::Field(key) = part else {
			return Ok(None);
		};
		if let Some(object) = value.as_ordered_object()? {
			match object.get(key)? {
				Some(found) => value = found,
				None => {
					out.clear();
					return Ok(Some(false));
				}
			}
			continue;
		}
		// A field step over anything but an object reaches a vector only through an array holding
		// objects, which projects the field of each. Over any other value (a scalar, a set, an
		// empty array, a typed array of numbers) the field steps left reach `NONE`, a set or an
		// array with no number, and none holds a vector.
		let projects = value.as_array()?.is_some_and(|array| {
			!array.is_empty() && array.floats().is_none() && array.ints().is_none()
		});
		if projects || rest.0[at..].iter().any(|p| !matches!(p, FieldPathPart::Field(_))) {
			return Ok(None);
		}
		out.clear();
		return Ok(Some(false));
	}
	stored_vector(value, out, limit).map(Some)
}

/// Reads the numbers of `scalar` into `out`, replacing its contents, where it is an array of
/// numbers only, and answers whether it is.
pub(super) fn scalar_numbers(scalar: &Scalar<'_>, out: &mut Vec<Number>) -> bool {
	out.clear();
	match scalar {
		Scalar::Value(value) => match value.as_ref().as_array() {
			Some(array) => array.iter().all(|value| match value.as_number() {
				Some(number) => {
					out.push(number.clone());
					true
				}
				None => false,
			}),
			None => false,
		},
		Scalar::None
		| Scalar::Null
		| Scalar::Bool(_)
		| Scalar::Number(_)
		| Scalar::Str(_)
		| Scalar::Duration(_)
		| Scalar::Datetime(_)
		| Scalar::Uuid(_)
		| Scalar::Bytes(_) => false,
	}
}

/// Reads the numbers of the stored value `value` into `out`, replacing its contents, where it is
/// an array of numbers only, each the number its decode holds, and answers whether it is.
pub(super) fn stored_numbers(value: ValueRef<'_>, out: &mut Vec<Number>) -> anyhow::Result<bool> {
	out.clear();
	let Some(array) = value.as_array()? else {
		return Ok(false);
	};
	if let Some(floats) = array.floats() {
		out.extend(floats.map(Number::Float));
		return Ok(true);
	}
	if let Some(ints) = array.ints() {
		out.extend(ints.map(Number::Int));
		return Ok(true);
	}
	for element in array.iter() {
		let element = element?;
		let number = match element.kind() {
			Kind::Int => element.as_int()?.map(Number::Int),
			Kind::Float => element.as_float()?.map(Number::Float),
			Kind::Decimal => element.as_decimal()?.map(Number::Decimal),
			Kind::BigDecimal => {
				element.as_bigdecimal()?.map(|big| Number::BigDecimal(Box::new(big)))
			}
			Kind::I8
			| Kind::U8
			| Kind::I16
			| Kind::U16
			| Kind::I32
			| Kind::U32
			| Kind::U64
			| Kind::F16
			| Kind::F32 => {
				if let Value::Number(n) = element.decode()? {
					Some(n)
				} else {
					None
				}
			}
			Kind::None
			| Kind::Null
			| Kind::Bool
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
			| Kind::RecordId
			| Kind::File
			| Kind::Regex
			| Kind::Range
			| Kind::Geometry
			| Kind::Array
			| Kind::Set
			| Kind::Object
			| Kind::Unknown => return Ok(false),
		};
		let Some(number) = number else {
			return Err(anyhow::anyhow!("a stored value whose kind and tag disagree"));
		};
		out.push(number);
	}
	Ok(true)
}

/// Reads the stored value `value` into `out`, replacing its contents, as [`extract_vector`] reads
/// the value it decodes to: whether it is an array of numbers and nested arrays of them, at most
/// `limit` numbers in all, flattened in order.
fn stored_vector(value: ValueRef<'_>, out: &mut Vec<Number>, limit: usize) -> anyhow::Result<bool> {
	out.clear();
	let Some(array) = value.as_array()? else {
		// Only an array holds a vector; a set, like any other value, holds none.
		return Ok(false);
	};
	Ok(flatten_stored(array, out, limit)? && !out.is_empty())
}

/// Appends the numbers of `array` to `out`, descending into nested arrays in order, as
/// [`extract_vector`] flattens a decoded array: `false` on an element that is neither a number nor
/// an array, and as soon as `out` would hold more than `limit` numbers.
fn flatten_stored(
	array: ArrayRef<'_>,
	out: &mut Vec<Number>,
	limit: usize,
) -> anyhow::Result<bool> {
	// A typed array holds numbers only, each the number a whole decode builds from it.
	if let Some(floats) = array.floats() {
		if floats.len() > limit - out.len().min(limit) {
			return Ok(false);
		}
		out.extend(floats.map(Number::Float));
		return Ok(true);
	}
	if let Some(ints) = array.ints() {
		if ints.len() > limit - out.len().min(limit) {
			return Ok(false);
		}
		out.extend(ints.map(Number::Int));
		return Ok(true);
	}
	for element in array.iter() {
		let element = element?;
		let number = match element.kind() {
			Kind::Int => element.as_int()?.map(Number::Int),
			Kind::Float => element.as_float()?.map(Number::Float),
			Kind::Decimal => element.as_decimal()?.map(Number::Decimal),
			Kind::BigDecimal => {
				element.as_bigdecimal()?.map(|big| Number::BigDecimal(Box::new(big)))
			}
			Kind::I8
			| Kind::U8
			| Kind::I16
			| Kind::U16
			| Kind::I32
			| Kind::U32
			| Kind::U64
			| Kind::F16
			| Kind::F32 => {
				if let Value::Number(n) = element.decode()? {
					Some(n)
				} else {
					None
				}
			}
			Kind::Array => match element.as_array()? {
				Some(inner) => {
					if !flatten_stored(inner, out, limit)? {
						return Ok(false);
					}
					continue;
				}
				None => None,
			},
			Kind::None
			| Kind::Null
			| Kind::Bool
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
			| Kind::RecordId
			| Kind::File
			| Kind::Regex
			| Kind::Range
			| Kind::Geometry
			| Kind::Set
			| Kind::Object
			| Kind::Unknown => return Ok(false),
		};
		let Some(number) = number else {
			return Err(anyhow::anyhow!("a stored value whose kind and tag disagree"));
		};
		if out.len() >= limit {
			return Ok(false);
		}
		out.push(number);
	}
	Ok(true)
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
		Kind::BigDecimal => Scalar::Number(Number::BigDecimal(Box::new(some(v.as_bigdecimal()?)?))),
		Kind::I8
		| Kind::U8
		| Kind::I16
		| Kind::U16
		| Kind::I32
		| Kind::U32
		| Kind::U64
		| Kind::F16
		| Kind::F32 => {
			if let Value::Number(n) = v.decode()? {
				Scalar::Number(n)
			} else {
				return Err(anyhow::anyhow!("a stored value whose kind and tag disagree"));
			}
		}
		Kind::String => Scalar::Str(some(v.as_str()?)?),
		Kind::Duration => Scalar::Duration(Duration(some(v.as_duration()?)?)),
		Kind::Datetime => Scalar::Datetime(Datetime(some(v.as_datetime()?)?)),
		Kind::Uuid => Scalar::Uuid(Uuid(some(v.as_uuid()?)?)),
		Kind::Bytes => Scalar::Bytes(some(v.as_bytes())?),
		Kind::Table
		| Kind::RecordId
		| Kind::File
		| Kind::Regex
		| Kind::Range
		| Kind::Geometry
		| Kind::Array
		| Kind::Set
		| Kind::Date
		| Kind::Time
		| Kind::Timestamp
		| Kind::Interval
		| Kind::Object
		| Kind::Unknown => Scalar::Value(Held::Owned(v.decode()?)),
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
			if let Layout::Stored(_) = &self.layout {
				return Ok(Scalar::of(self.key_id()?));
			}
			return self.decoded_field("id");
		}
		match &self.layout {
			Layout::Stored(object) => match stored_field(object, name)? {
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
			if let Layout::Whole = &self.layout {
				self.decoded()?;
			}
			return Ok(None);
		}
		if let Layout::Stored(object) = &self.layout {
			return Ok(match stored_field(object, name)? {
				Some(v) => v.as_int()?,
				None => None,
			});
		}
		match self.revision_field(name) {
			Some(Some(bytes)) => Ok(int_on_wire(bytes)),
			Some(None) => Ok(None),
			None => Ok(match self.field(name)? {
				Scalar::Number(Number::Int(v)) => Some(v),
				Scalar::Number(Number::I8(v)) => Some(v as i64),
				Scalar::Number(Number::U8(v)) => Some(v as i64),
				Scalar::Number(Number::I16(v)) => Some(v as i64),
				Scalar::Number(Number::U16(v)) => Some(v as i64),
				Scalar::Number(Number::I32(v)) => Some(v as i64),
				Scalar::Number(Number::U32(v)) => Some(v as i64),
				Scalar::Number(Number::U64(v)) => i64::try_from(v).ok(),
				Scalar::Number(
					Number::Float(_)
					| Number::Decimal(_)
					| Number::F16(_)
					| Number::F32(_)
					| Number::BigDecimal(_),
				) => None,
				Scalar::None
				| Scalar::Null
				| Scalar::Bool(_)
				| Scalar::Str(_)
				| Scalar::Duration(_)
				| Scalar::Datetime(_)
				| Scalar::Uuid(_)
				| Scalar::Bytes(_)
				| Scalar::Value(_) => None,
			}),
		}
	}

	fn record(&self) -> anyhow::Result<Value> {
		match self.decoded.get() {
			Some(value) => Ok(value.clone()),
			None => decode_record_value(self.key, self.value, None),
		}
	}

	fn vector_field(&self, name: &FieldName, out: &mut Vec<Number>) -> anyhow::Result<bool> {
		// A record decoded already, or one no layout reads in place, is read from its decoded
		// value; a record's `id` is its key, never an array.
		if self.decoded.get().is_none()
			&& name.as_str() != "id"
			&& let Layout::Stored(object) = &self.layout
		{
			return match stored_field(object, name)? {
				Some(value) => stored_numbers(value, out),
				None => {
					out.clear();
					Ok(false)
				}
			};
		}
		Ok(scalar_numbers(&self.field(name)?, out))
	}

	fn knn_vector(
		&self,
		path: &FieldPath,
		name: &FieldName,
		rest: &FieldPath,
		out: &mut Vec<Number>,
		limit: usize,
	) -> anyhow::Result<bool> {
		// A record decoded already, or one no layout reads in place, is read from its decoded
		// value, with no copy of the field.
		if let Some(whole) = self.decoded.get() {
			return Ok(extract_vector(whole, path, out, limit));
		}
		if let Layout::Whole = self.layout {
			return Ok(extract_vector(self.decoded()?, path, out, limit));
		}
		// A record's `id` is its key, never a vector of numbers; it is read as any other reader
		// reads it.
		if let Layout::Stored(object) = &self.layout
			&& name.as_str() != "id"
		{
			let Some(value) = stored_field(object, name)? else {
				out.clear();
				return Ok(false);
			};
			if let Some(read) = stored_path_vector(value, rest, out, limit)? {
				return Ok(read);
			}
		}
		Ok(extract_vector(&self.field(name)?.as_value(), rest, out, limit))
	}

	fn has_any_field(&self, names: &[Strand]) -> anyhow::Result<bool> {
		if let Layout::Stored(object) = &self.layout {
			// The record is an object, so a read gives it the key's `id`.
			for name in names {
				if name.as_str() == "id" || object.get(name.as_str())?.is_some() {
					return Ok(true);
				}
			}
			return Ok(false);
		}
		Ok(match self.decoded()?.as_object() {
			Some(object) => names.iter().any(|name| object.contains_key(name.as_str())),
			None => false,
		})
	}
}

/// A row held in memory: a record decoded by the scan, a row a sort ordered, a
/// group an aggregate finished.
pub(super) struct ValueRow<'a>(pub(super) &'a Value);

impl RowReader for ValueRow<'_> {
	fn field(&self, name: &FieldName) -> anyhow::Result<Scalar<'_>> {
		match self.0.as_object() {
			Some(object) => Ok(object.get(name.as_str()).map(Scalar::of).unwrap_or(Scalar::None)),
			// A record whose content is not an object has no field, as a field of
			// any non-object value is `NONE`.
			None => Ok(Scalar::None),
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
		Ok(match self.0.as_object() {
			Some(object) => names.iter().any(|name| object.contains_key(name.as_str())),
			None => false,
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
		other @ (Value::String(_)
		| Value::Array(_)
		| Value::Date(_)
		| Value::Time(_)
		| Value::Timestamp(_)
		| Value::Interval(_)
		| Value::Set(_)
		| Value::Object(_)
		| Value::Geometry(_)
		| Value::Bytes(_)
		| Value::Table(_)
		| Value::RecordId(_)
		| Value::File(_)
		| Value::Regex(_)
		| Value::Range(_)
		| Value::Closure(_)) => Scalar::Value(Held::Owned(other)),
	})
}

#[cfg(test)]
mod tests {
	use std::borrow::Cow;

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
					Value::Number(crate::val::Number::I8(i)) => Some(*i as i64),
					Value::Number(crate::val::Number::U8(i)) => Some(*i as i64),
					Value::Number(crate::val::Number::I16(i)) => Some(*i as i64),
					Value::Number(crate::val::Number::U16(i)) => Some(*i as i64),
					Value::Number(crate::val::Number::I32(i)) => Some(*i as i64),
					Value::Number(crate::val::Number::U32(i)) => Some(*i as i64),
					Value::Number(crate::val::Number::U64(i)) => i64::try_from(*i).ok(),
					Value::Number(
						crate::val::Number::F16(_)
						| crate::val::Number::F32(_)
						| crate::val::Number::Float(_)
						| crate::val::Number::Decimal(_)
						| Number::BigDecimal(_),
					)
					| Value::None
					| Value::Null
					| Value::Bool(_)
					| Value::String(_)
					| Value::Duration(_)
					| Value::Datetime(_)
					| Value::Date(_)
					| Value::Time(_)
					| Value::Timestamp(_)
					| Value::Interval(_)
					| Value::Uuid(_)
					| Value::Array(_)
					| Value::Set(_)
					| Value::Object(_)
					| Value::Geometry(_)
					| Value::Bytes(_)
					| Value::Table(_)
					| Value::RecordId(_)
					| Value::File(_)
					| Value::Regex(_)
					| Value::Range(_)
					| Value::Closure(_) => None,
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

	/// A ranking's vector read from a stored record in place is the vector
	/// `extract_vector` reads from the record decoded whole, number for number
	/// and kind for kind, whatever the array's encoding, its nesting, the path
	/// into it and the bound on its length; and the record is not decoded.
	#[test]
	fn a_knn_vector_reads_in_place_as_from_the_decoded_record() {
		use crate::val::field_path::{FieldPath, FieldPathPart};
		let rid = crate::val::RecordId::new("t".into(), crate::val::RecordIdKey::Number(1));
		let key = key_of(&rid);
		let mut data = val("{ f32: [1f, 0f, 0f], f64: [0.1f, 0.2f, 0.3f], ints: [2, 0, 0], \
			 big: [1000000000000, 0, 1], mix: [1, 0.5f, 0], dec: [0dec, 1dec, 0dec], \
			 nest: [[0f], [0f, 3f]], typed: [[1f, 2f], [3]], wide: [1f, 1f, 1f, 1f], \
			 short: [1f], str: [1f, 'x', 0f], empty: [], empties: [[], []], scalar: 3, \
			 obj: { v: [1f, 2f, 3f], deep: { v: [4, 5, 6] } }, arrobj: [{ v: [1f, 2f, 3f] }], \
			 none: NONE, null: NULL }");
		if let Value::Object(fields) = &mut data {
			let set = |items: Vec<Value>| Value::Set(crate::val::Set::from(items));
			let floats =
				|| Value::from(vec![Value::from(1f64), Value::from(2f64), Value::from(3f64)]);
			fields.insert("set".to_owned(), set(vec![Value::from(1f64), Value::from(2f64)]));
			let mut inner = crate::val::Object::default();
			inner.insert("v".to_owned(), floats());
			fields.insert("setobj".to_owned(), set(vec![Value::Object(inner)]));
		}
		let Value::Object(fields) = &data else {
			unreachable!()
		};
		let mut paths: Vec<Vec<&str>> = fields.keys().map(|name| vec![name.as_str()]).collect();
		paths.extend([
			vec!["obj", "v"],
			vec!["obj", "deep", "v"],
			vec!["obj", "missing"],
			vec!["obj", "v", "x"],
			vec!["arrobj", "v"],
			vec!["arrobj", "v", "w"],
			vec!["scalar", "v"],
			vec!["scalar", "v", "w"],
			vec!["f32", "v"],
			vec!["ints", "v"],
			vec!["set", "v"],
			vec!["setobj", "v"],
			vec!["none", "v"],
			vec!["absent"],
			vec!["absent", "v"],
			vec!["id"],
		]);
		let path = |parts: &[&str]| {
			FieldPath(parts.iter().map(|p| FieldPathPart::Field((*p).to_owned())).collect())
		};
		let value =
			crate::catalog::encode_record(DOCUMENT, None, &data, Some(&rid)).expect("encodes");
		let decoded = decode_record_value(&key, &value, None).expect("decodes");
		let row = StoredRow::new(&key, &value);
		assert!(matches!(row.layout, Layout::Stored(_)), "read in place");
		let (mut got, mut want) = (vec![Number::Int(-1)], vec![Number::Int(-1)]);
		let mut compare = |row: &StoredRow<'_>| {
			for parts in &paths {
				let (full, rest) = (path(parts), path(&parts[1..]));
				for limit in [0, 1, 2, 3, 4, 100] {
					let read = row
						.knn_vector(&full, &FieldName::new(parts[0]), &rest, &mut got, limit)
						.expect("reads");
					let expected = extract_vector(&decoded, &full, &mut want, limit);
					assert_eq!(read, expected, "{parts:?} at most {limit}");
					if expected {
						assert_eq!(
							format!("{got:?}"),
							format!("{want:?}"),
							"{parts:?} at most {limit}"
						);
					}
				}
			}
		};
		compare(&row);
		assert!(row.decoded.get().is_none(), "no ranking decoded the record");
		// A row decoded already is read from its decoded value, with the same answers.
		row.decoded().expect("decodes");
		compare(&row);
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

	/// `RowReader::point_field` returns `Some((lon, lat))` only for a `Geometry::Point` field and
	/// `None` for non-point geometries, scalars, `id` and absent fields across stored, revision
	/// and in-memory rows.
	#[test]
	fn point_field_reads_only_points() {
		let rid = crate::val::RecordId::new("t".into(), crate::val::RecordIdKey::Number(1));
		let key = key_of(&rid);
		let data = val(
			"{ pt: (12.5, -34.25), line: { type: 'LineString', coordinates: [[0, 0], [1, 1]] }, num: 42, str: 'x', null_f: NULL }",
		);
		let stored_bytes =
			crate::catalog::encode_record(DOCUMENT, None, &data, Some(&rid)).expect("encodes");
		let rev_bytes =
			revision::to_vec(&crate::catalog::Record::new(data.clone())).expect("encodes");

		let check = |reader: &dyn RowReader| {
			assert_eq!(
				reader.point_field(&FieldName::new("pt")).expect("reads"),
				Some((12.5, -34.25))
			);
			for non_pt in ["line", "num", "str", "null_f", "absent", "id"] {
				assert_eq!(
					reader.point_field(&FieldName::new(non_pt)).expect("reads"),
					None,
					"{non_pt}"
				);
			}
		};

		let stored_row = StoredRow::new(&key, &stored_bytes);
		assert!(matches!(stored_row.layout, Layout::Stored(_)));
		check(&stored_row);
		assert!(stored_row.decoded.get().is_none(), "read in place without whole decode");

		let rev_row = StoredRow::new(&key, &rev_bytes);
		assert!(matches!(rev_row.layout, Layout::Revision { .. }));
		check(&rev_row);

		let value_row = ValueRow(&data);
		check(&value_row);
	}
}
