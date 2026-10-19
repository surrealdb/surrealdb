//! Reading a packed row's fields where they lie.
//!
//! A packed row ([`StoredRecord`] in a packed layout) holds its columns at the offsets its
//! generation's descriptor names, so a field is read by that offset: a fixed slot is one load, a
//! string or bytes is borrowed from the variable region, and a number, boolean, duration,
//! datetime or uuid is decoded inline. The row is read only once it has been checked as a whole
//! ([`StoredRecord::checked_packed`]): the check is the whole decode run to build nothing, so a
//! row it refuses is a row the whole decode refuses, whichever fields the body goes on to read.
//!
//! [`PackedRowReader`] is that row as a [`RowReader`]. It never holds a value of the record
//! until one is asked for: the key is validated whole when the row is opened, its table read from
//! the key's bytes and its id read without being built, and the record id is built from the key
//! only when `id` is read or the row is decoded. The row is decoded whole at most once, when the
//! body leaves it pending to be settled or asks for it.

use std::borrow::Cow;
use std::cell::OnceCell;

use anyhow::anyhow;
use storekey::{BorrowDecode, BorrowReader, EscapedSlice};

use super::row::{FieldName, RowReader, scalar_from_stored};
use super::scalar::Scalar;
use crate::catalog::packed::{CheckedPackedRow, FieldRef};
use crate::catalog::{StoredRecord, TableLayouts};
use crate::exec::operators::scan::pipeline::decode_record_value;
use crate::key::KVKeyDecode as _;
use crate::key::schema::{RecordKey, TblPrefix};
use crate::val::key_read::{KeyIdRead, KeyIdRef, KeyText};
use crate::val::{Datetime, Duration, Geometry, Number, RecordId, Uuid, Value, stored};

/// A packed row read in place, as a [`RowReader`].
pub(super) struct PackedRowReader<'a> {
	key: &'a [u8],
	row: CheckedPackedRow<'a>,
	/// The record decoded whole, for [`RowReader::record`], the settle of a row the body left
	/// pending, and the row's borrowed whole once it has been decoded.
	decoded: OnceCell<Value>,
	/// The record's `id`, built from its key when it is first read.
	id: OnceCell<Value>,
}

impl<'a> PackedRowReader<'a> {
	/// The packed row `value` of the record `key` names, read in place through the descriptor of
	/// its generation in `layouts`, which admitted it.
	///
	/// Refuses the row the whole decode refuses, before any field is read, with the error
	/// [`decode_record_value`] raises for the same bytes: the key, the layouts and the row are
	/// checked in the order the whole decode checks them. The key is validated whole here, so a key
	/// the whole decode refuses is refused here with that decode's error; its id is built when it
	/// is first read.
	pub(super) fn new(
		key: &'a [u8],
		value: &'a [u8],
		layouts: Option<&'a TableLayouts>,
	) -> anyhow::Result<Self> {
		match admit(key, value, layouts) {
			Some(row) => Ok(Self::of(key, row)),
			None => Err(refusal(key, value, layouts)),
		}
	}

	/// The packed row `row` of the record `key` names, read in place.
	pub(super) fn of(key: &'a [u8], row: CheckedPackedRow<'a>) -> Self {
		#[cfg(test)]
		observed::note_in_place();
		Self {
			key,
			row,
			decoded: OnceCell::new(),
			id: OnceCell::new(),
		}
	}

	/// The record's `id`: its key, decoded once. The key was validated when the row was opened, so
	/// a key that does not decode here is a disagreement of the two reads, raised as the whole
	/// decode raises it.
	fn key_id(&self) -> anyhow::Result<&Value> {
		if let Some(id) = self.id.get() {
			return Ok(id);
		}
		let id = record_id(self.key)?;
		Ok(self.id.get_or_init(|| id))
	}

	/// The record decoded whole, once: the value [`decode_record_value`] answers for the same
	/// bytes.
	fn decoded(&self) -> anyhow::Result<&Value> {
		if let Some(value) = self.decoded.get() {
			return Ok(value);
		}
		let object = self.row.decode(Some(self.key_id()?.clone()))?;
		Ok(self.decoded.get_or_init(|| Value::Object(object)))
	}

	/// The record, whole, ending the reader: what [`decode_record_value`] answers for the same
	/// bytes.
	pub(super) fn into_value(mut self) -> anyhow::Result<Value> {
		if let Some(value) = self.decoded.take() {
			return Ok(value);
		}
		let id = self.key_id()?.clone();
		Ok(Value::Object(self.row.decode(Some(id))?))
	}

	/// Root field `name` read from column `index`, or from the root's remainder when it has no
	/// column. A name with no column costs the one search that finds none.
	fn read(&self, name: &FieldName) -> anyhow::Result<(Option<usize>, FieldRef<'a>)> {
		Ok(match self.row.root_column(name.as_str(), name.column_hint()) {
			Some(index) => (Some(index), self.row.read_column(index)?),
			None => (None, self.row.read_root_remainder(name.as_str())?),
		})
	}
}

/// A packed row run through a body that reads none of its fields ([`Pipeline::reads_row`] clear):
/// the record is decoded whole the first time the body asks for any of it, and never otherwise, so
/// a row the body hands on untouched can be written out from its bytes
/// ([`RowFrame::push_stored`](super::row_frame::RowFrame::push_stored)).
///
/// [`Pipeline::reads_row`]: super::program::Pipeline::reads_row
pub(super) struct LazyPackedRow<'a> {
	key: &'a [u8],
	value: &'a [u8],
	layouts: Option<&'a TableLayouts>,
	admitted: &'a crate::exec::packed_view::AdmittedRow<'a>,
	decoded: OnceCell<Value>,
}

impl<'a> LazyPackedRow<'a> {
	/// The packed row `value` of the record `key` names, as `admitted` admitted it through
	/// `layouts`.
	pub(super) fn new(
		key: &'a [u8],
		value: &'a [u8],
		layouts: Option<&'a TableLayouts>,
		admitted: &'a crate::exec::packed_view::AdmittedRow<'a>,
	) -> Self {
		Self {
			key,
			value,
			layouts,
			admitted,
			decoded: OnceCell::new(),
		}
	}

	/// The record decoded whole, once: the value [`decode_record_value`] answers for the same
	/// bytes, or its error.
	fn decoded(&self) -> anyhow::Result<&Value> {
		if let Some(value) = self.decoded.get() {
			return Ok(value);
		}
		#[cfg(test)]
		observed::note_whole();
		let value = self.admitted.decode(self.key, self.value, self.layouts)?;
		Ok(self.decoded.get_or_init(|| value))
	}

	/// The record, when the body asked for it and so it was decoded.
	pub(super) fn into_decoded(self) -> Option<Value> {
		self.decoded.into_inner()
	}
}

impl RowReader for LazyPackedRow<'_> {
	fn field(&self, name: &FieldName) -> anyhow::Result<Scalar<'_>> {
		match self.decoded()? {
			Value::Object(object) => {
				Ok(object.get(name.as_str()).map(Scalar::of).unwrap_or(Scalar::None))
			}
			_ => anyhow::bail!("the VM read a field from a row that is not an object"),
		}
	}

	fn record(&self) -> anyhow::Result<Value> {
		Ok(self.decoded()?.clone())
	}

	fn borrowed(&self) -> Option<&Value> {
		self.decoded.get()
	}

	fn has_any_field(&self, names: &[surrealdb_strand::Strand]) -> anyhow::Result<bool> {
		super::row::ValueRow(self.decoded()?).has_any_field(names)
	}
}

/// The record id the record key `key` names, decoded as the whole decode decodes it.
fn record_id(key: &[u8]) -> anyhow::Result<Value> {
	let key = RecordKey::decode_key(key).map_err(|e| e.context("Failed to decode record key"))?;
	Ok(Value::RecordId(RecordId {
		table: key.tb.into_owned(),
		key: key.id.into_owned(),
	}))
}

/// The checked packed row `row` of the record `key` names, decoded whole: the value
/// [`decode_record_value`] answers for the same bytes.
pub(in crate::exec) fn decode_checked(
	key: &[u8],
	row: CheckedPackedRow<'_>,
) -> anyhow::Result<Value> {
	Ok(Value::Object(row.decode(Some(record_id(key)?))?))
}

/// The table a record key names, as the key's own bytes. The key is read whole as
/// `RecordKey::decode_key` reads it, building neither the table name nor the id, so the id is
/// not built unless `id` is read: `None` exactly when the key decode refuses the bytes, a key
/// that does not begin as a record key does, whose id does not decode, or that has bytes after
/// its id.
fn read_key(key: &[u8]) -> Option<&EscapedSlice> {
	let mut reader = BorrowReader::new(key);
	<TblPrefix as BorrowDecode<'_, ()>>::borrow_decode(&mut reader).ok()?;
	let table = KeyText::borrow_decode(&mut reader).ok()?.escaped();
	let separator: u8 = BorrowDecode::<()>::borrow_decode(&mut reader).ok()?;
	if separator != b'*' {
		return None;
	}
	KeyIdRead::borrow_decode(&mut reader).ok()?;
	reader.is_empty().then_some(table)
}

/// The table a record key names, as the key's own bytes, and its id, read as [`read_key`] reads
/// them: `None` exactly where [`read_key`] answers `None`. A number or string id is borrowed from
/// the key; any other is validated and held as [`KeyIdRef::Other`].
pub(super) fn read_key_id(key: &[u8]) -> Option<(&EscapedSlice, KeyIdRef<'_>)> {
	let mut reader = BorrowReader::new(key);
	<TblPrefix as BorrowDecode<'_, ()>>::borrow_decode(&mut reader).ok()?;
	let table = KeyText::borrow_decode(&mut reader).ok()?.escaped();
	let separator: u8 = BorrowDecode::<()>::borrow_decode(&mut reader).ok()?;
	if separator != b'*' {
		return None;
	}
	let id = KeyIdRef::borrow_decode(&mut reader).ok()?;
	reader.is_empty().then_some((table, id))
}

/// Where the id of the record key `key` starts, past the prefix and table [`read_key_id`] reads:
/// the keys of one table share the bytes before it.
#[cfg(feature = "shaped")]
pub(super) fn key_id_start(key: &[u8]) -> Option<usize> {
	let (table, _) = read_key_id(key)?;
	let start = (table.as_bytes().as_ptr() as usize).checked_sub(key.as_ptr() as usize)?;
	let at = start.checked_add(table.as_bytes().len())?.checked_add(1)?;
	(key.get(at - 1) == Some(&b'*')).then_some(at)
}

/// The id of the record key `key` whose id starts at `at` ([`key_id_start`]), read as
/// [`read_key_id`] reads it.
#[cfg(feature = "shaped")]
pub(super) fn read_key_id_at(key: &[u8], at: usize) -> Option<KeyIdRef<'_>> {
	let mut reader = BorrowReader::new(key.get(at..)?);
	let id = KeyIdRef::borrow_decode(&mut reader).ok()?;
	reader.is_empty().then_some(id)
}

/// The packed row `value` of the record `key` names, checked, or `None` when any step of the whole
/// decode refuses it.
///
/// The steps are those of the whole decode, in its order: the key, read whole as
/// `RecordKey::decode_key` reads it, then the envelope, the layouts' table against the key's, the
/// row's generation as `layouts` lists it, and the row itself
/// ([`StoredRecord::checked_packed`]). A row the whole decode accepts is accepted here, and one it
/// refuses is refused, whichever fields are read afterwards.
pub(in crate::exec) fn admit<'a>(
	key: &'a [u8],
	value: &'a [u8],
	layouts: Option<&'a TableLayouts>,
) -> Option<CheckedPackedRow<'a>> {
	let table = read_key(key)?;
	let record = StoredRecord::parse(value).ok()??;
	let layouts = layouts?;
	if !table.iter().eq(layouts.table().as_str().as_bytes()) {
		return None;
	}
	let Ok(Some(generation)) = record.generation() else {
		return None;
	};
	let descriptor = layouts.admit_packed(generation).ok()?;
	record.checked_packed(descriptor).ok()
}

/// What the whole decode of `key`'s record at `value` refuses, which a reader that refused the
/// row has not made. The decode refuses the same bytes: should it accept them, the check and the
/// decode disagree.
pub(in crate::exec) fn refusal(
	key: &[u8],
	value: &[u8],
	layouts: Option<&TableLayouts>,
) -> anyhow::Error {
	match decode_record_value(key, value, layouts) {
		Err(error) => error,
		Ok(_) => anyhow!("a packed row refused by its check but accepted by its decode"),
	}
}

impl RowReader for PackedRowReader<'_> {
	fn field(&self, name: &FieldName) -> anyhow::Result<Scalar<'_>> {
		// A record's `id` is its key, whatever the row holds.
		if name.as_str() == "id" {
			return Ok(Scalar::of(self.key_id()?));
		}
		let (column, field) = self.read(name)?;
		Ok(match field {
			FieldRef::Absent => Scalar::None,
			FieldRef::Null => Scalar::Null,
			FieldRef::Object => {
				let Some(column) = column else {
					anyhow::bail!("a packed row's object node read without its column");
				};
				Scalar::Value(Cow::Owned(Value::Object(self.row.decode_node(column)?)))
			}
			FieldRef::Bool(v) => Scalar::Bool(v),
			FieldRef::Int(v) => Scalar::Number(Number::Int(v)),
			FieldRef::Float(v) => Scalar::Number(Number::Float(v)),
			FieldRef::Decimal(v) => Scalar::Number(Number::Decimal(v)),
			FieldRef::Uuid(v) => Scalar::Uuid(Uuid(v)),
			FieldRef::Datetime(v) => Scalar::Datetime(Datetime(v)),
			FieldRef::Duration(v) => Scalar::Duration(Duration(v)),
			FieldRef::Point(x, y) => {
				Scalar::Value(Cow::Owned(Value::Geometry(Geometry::Point((x, y).into()))))
			}
			FieldRef::String(v) => Scalar::Str(v),
			FieldRef::Bytes(v) => Scalar::Bytes(v),
			FieldRef::Literal(v) => Scalar::of(v),
			// A root field sits one level below the record id's.
			FieldRef::RecordId(table, key) => {
				Scalar::Value(Cow::Owned(Value::RecordId(RecordId {
					table: table.clone(),
					key: stored::decode_key_nested(key, 2)?,
				})))
			}
			FieldRef::Stored(v) => scalar_from_stored(v)?,
		})
	}

	fn field_int(&self, name: &FieldName) -> anyhow::Result<Option<i64>> {
		// A record's `id` is a record id, never an integer.
		if name.as_str() == "id" {
			return Ok(None);
		}
		Ok(match self.read(name)?.1 {
			FieldRef::Int(v) => Some(v),
			// An integer a `Value` column holds, or a literal union's literal, is an integer.
			FieldRef::Stored(v) => v.as_int()?,
			FieldRef::Literal(Value::Number(Number::Int(v))) => Some(*v),
			_ => None,
		})
	}

	fn record(&self) -> anyhow::Result<Value> {
		Ok(self.decoded()?.clone())
	}

	/// The record once it has been decoded; the reader never decodes it to answer.
	fn borrowed(&self) -> Option<&Value> {
		self.decoded.get()
	}

	fn has_any_field(&self, names: &[surrealdb_strand::Strand]) -> anyhow::Result<bool> {
		for name in names {
			// The record is an object, so a read gives it the key's `id`.
			if name.as_str() == "id"
				|| !matches!(self.row.read_root(name.as_str())?, FieldRef::Absent)
			{
				return Ok(true);
			}
		}
		Ok(false)
	}
}

/// The packed rows a test run read in place, decoded whole, or rejected from their columns by the
/// filter or the probe, and the pipelines of the programs it ran, on this thread.
#[cfg(test)]
pub(in crate::exec) mod observed {
	use std::cell::{Cell, RefCell};

	use super::super::program::{Pipeline, Source};

	thread_local! {
		static IN_PLACE: Cell<usize> = const { Cell::new(0) };
		static WHOLE: Cell<usize> = const { Cell::new(0) };
		static FILTERED: Cell<usize> = const { Cell::new(0) };
		static PROBED: Cell<usize> = const { Cell::new(0) };
		static PROBED_BYTES: Cell<usize> = const { Cell::new(0) };
		static PIPELINES: RefCell<Vec<Shape>> = const { RefCell::new(Vec::new()) };
	}

	/// What a compiled pipeline decides about the rows it reads.
	#[derive(Clone, Copy, Debug, PartialEq, Eq)]
	pub(in crate::exec) struct Shape {
		/// The source is a scan of a table.
		pub(in crate::exec) scan: bool,
		/// The source is counted without reading a record.
		pub(in crate::exec) count: bool,
		/// The body runs on the stored row ([`Pipeline::in_place`]).
		pub(in crate::exec) in_place: bool,
		/// The body reads fields of the row and takes it whole nowhere ([`Pipeline::reads_row`]).
		pub(in crate::exec) reads_row: bool,
		/// The rows are put through an enforcing step before the body runs.
		pub(in crate::exec) enforced: bool,
		/// The scan judges a record's stored bytes before decoding it ([`Source::prefilter`]).
		pub(in crate::exec) prefilter: bool,
		/// The scan's prefilter judges the packed rows it reads
		/// ([`PreDecodeShape::judges_packed_rows`](crate::exec::pre_decode_filter::PreDecodeShape::judges_packed_rows)).
		pub(in crate::exec) judges_packed: bool,
		/// The scan holds a top-k key, and whether it may reject a packed row from its columns
		/// (`TopKScan::packed_rows`).
		pub(in crate::exec) topk_packed: Option<bool>,
	}

	/// Forgets what was observed.
	pub(in crate::exec) fn reset() {
		IN_PLACE.with(|n| n.set(0));
		WHOLE.with(|n| n.set(0));
		FILTERED.with(|n| n.set(0));
		PROBED.with(|n| n.set(0));
		PROBED_BYTES.with(|n| n.set(0));
		PIPELINES.with(|p| p.borrow_mut().clear());
	}

	/// The pipelines run since [`reset`], in the order they started.
	pub(in crate::exec) fn pipelines() -> Vec<Shape> {
		PIPELINES.with(|p| p.borrow().clone())
	}

	pub(in crate::exec::vm) fn note_pipeline(pipeline: &Pipeline) {
		let shape = Shape {
			scan: matches!(pipeline.source, Source::Scan { .. }),
			count: matches!(pipeline.source, Source::Count { .. }),
			in_place: pipeline.in_place,
			reads_row: pipeline.reads_row,
			enforced: pipeline.enforce.is_some(),
			prefilter: pipeline.source.prefilter().is_some(),
			judges_packed: pipeline
				.source
				.prefilter()
				.is_some_and(crate::exec::pre_decode_filter::PreDecodeShape::judges_packed_rows),
			topk_packed: pipeline.source.topk().map(|key| key.packed_rows),
		};
		PIPELINES.with(|p| p.borrow_mut().push(shape));
	}

	/// The packed rows read in place since [`reset`].
	pub(in crate::exec) fn in_place() -> usize {
		IN_PLACE.with(Cell::get)
	}

	/// The packed rows a body took decoded whole since [`reset`].
	pub(in crate::exec) fn whole() -> usize {
		WHOLE.with(Cell::get)
	}

	/// The packed rows the pre-decode filter rejected from their columns since [`reset`].
	pub(in crate::exec) fn filtered() -> usize {
		FILTERED.with(Cell::get)
	}

	/// The packed rows the top-k probe rejected from their columns since [`reset`].
	pub(in crate::exec) fn probed() -> usize {
		PROBED.with(Cell::get)
	}

	pub(in crate::exec) fn note_filtered() {
		FILTERED.with(|n| n.set(n.get() + 1));
	}

	/// The records stored as documents the top-k probe rejected from their bytes since [`reset`].
	pub(in crate::exec) fn probed_bytes() -> usize {
		PROBED_BYTES.with(Cell::get)
	}

	pub(in crate::exec) fn note_probed(packed: bool) {
		let count = if packed {
			&PROBED
		} else {
			&PROBED_BYTES
		};
		count.with(|n| n.set(n.get() + 1));
	}

	pub(in crate::exec::vm) fn note_in_place() {
		IN_PLACE.with(|n| n.set(n.get() + 1));
	}

	pub(in crate::exec::vm) fn note_whole() {
		WHOLE.with(|n| n.set(n.get() + 1));
	}
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeSet;
	use std::ops::Bound;

	use rust_decimal::Decimal;
	use surrealdb_kvs::key::KVKey;

	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::val::{
		Array, Bytes, File, Object, Range, RecordIdKey, RecordIdKeyRange, Set, TableName,
	};

	/// The tag byte the key encoding writes for `value`'s variant: its index in `Value`, and two.
	/// A new variant of `Value` must be given its place here and in the mirror of `Value` in
	/// [`crate::val::key_read`].
	fn tag(value: &Value) -> u8 {
		match value {
			Value::None => 2,
			Value::Null => 3,
			Value::Bool(_) => 4,
			Value::Number(_) => 5,
			Value::String(_) => 6,
			Value::Duration(_) => 7,
			Value::Datetime(_) => 8,
			Value::Uuid(_) => 9,
			Value::Array(_) => 10,
			Value::Set(_) => 11,
			Value::Object(_) => 12,
			Value::Geometry(_) => 13,
			Value::Bytes(_) => 14,
			Value::Table(_) => 15,
			Value::RecordId(_) => 16,
			Value::File(_) => 17,
			Value::Regex(_) => 18,
			Value::Range(_) => 19,
			Value::Closure(_) => 20,
		}
	}

	fn int(value: i64) -> Value {
		Value::Number(Number::Int(value))
	}

	fn object_of(entries: Vec<(&str, Value)>) -> Object {
		let mut object = Object::default();
		for (key, value) in entries {
			object.insert(key, value);
		}
		object
	}

	/// Numbers of every kind, among them those whose decimal buffer holds a byte the key encoding
	/// escapes, the extremes of each kind and the values that are no integer.
	fn numbers() -> Vec<Number> {
		let mut numbers = vec![
			Number::Int(0),
			Number::Int(1),
			Number::Int(-1),
			Number::Int(i64::MAX),
			Number::Int(i64::MIN),
			Number::Float(1.5),
			Number::Float(-0.0),
			Number::Float(f64::MAX),
			Number::Float(f64::MIN_POSITIVE),
			Number::Float(f64::NAN),
			Number::Float(f64::INFINITY),
			Number::Float(f64::NEG_INFINITY),
			Number::Decimal(1.into()),
			Number::Decimal(Decimal::MAX),
			Number::Decimal(Decimal::MIN),
			Number::Decimal(Decimal::new(15, 3)),
		];
		let escaped = |number: &Number| number.as_decimal_buf().iter().any(|&byte| byte < 2);
		let mut found = 0;
		for value in 0..20_000i64 {
			for number in [Number::Int(value), Number::Float(value as f64 / 8.0)] {
				if found < 8 && escaped(&number) {
					found += 1;
					numbers.push(number);
				}
			}
		}
		assert!(found > 0, "no number holds an escaped byte");
		numbers
	}

	/// A value of every variant of `Value` that can be written in a key, with the shapes of the
	/// variants that nest: strings the strand holds inline and not, an escaped byte, and values
	/// within arrays, objects, sets and ranges.
	fn values() -> Vec<Value> {
		let datetime = "2021-01-01T00:00:00Z".parse().expect("a datetime");
		let point = Geometry::Point((1.0, 2.0).into());
		let mut values: Vec<Value> = numbers().into_iter().map(Value::Number).collect();
		values.extend([
			Value::String("x".into()),
			Value::String("a string longer than a strand holds inline".into()),
			Value::String("nul\0and\u{1}inside".into()),
			Value::Duration(Duration::new(3, 4)),
			Value::Datetime(datetime),
			Value::Uuid(Uuid::default()),
			Value::Array(Array::from(vec![int(1), Value::String("x".into())])),
			Value::Array(Array::from(vec![Value::Array(Array::from(vec![Value::None]))])),
			Value::Set(Set::from(BTreeSet::from([int(1), int(2)]))),
			Value::Object(object_of(vec![("a", int(1)), ("b", Value::String("x".into()))])),
			Value::Object(Object::default()),
			Value::Geometry(point.clone()),
			Value::Geometry(Geometry::Collection(vec![point])),
			Value::Bytes(Bytes::from(vec![1, 2, 3])),
			Value::Table(TableName::new("t")),
			Value::RecordId(RecordId {
				table: TableName::new("t"),
				key: RecordIdKey::Number(1),
			}),
			Value::File(File::new("bucket".into(), "/key".into())),
			Value::Range(Box::new(Range {
				start: Bound::Included(int(1)),
				end: Bound::Unbounded,
			})),
			Value::Range(Box::new(Range {
				start: Bound::Excluded(Value::Array(Array::from(vec![int(1)]))),
				end: Bound::Included(int(9)),
			})),
		]);
		values
	}

	/// The ids a key holds, of every kind and, for the kinds that nest, around every value of
	/// [`values`].
	fn ids() -> Vec<RecordIdKey> {
		let mut ids = vec![
			RecordIdKey::Number(1),
			RecordIdKey::Number(i64::MIN),
			RecordIdKey::String("a key longer than a strand holds inline".into()),
			RecordIdKey::String("nul\0and\u{1}inside".into()),
			RecordIdKey::String(format!("{0}\0{0}\u{1}{0}", "\u{e9}".repeat(40)).into()),
			RecordIdKey::String(format!("a{0}\0{0}\u{1}{0}", "\u{20ac}".repeat(30)).into()),
			RecordIdKey::String(format!("ab{0}\0{0}\u{1}{0}", "\u{1f600}".repeat(20)).into()),
			RecordIdKey::Uuid(Uuid::default()),
			RecordIdKey::Array(Array::from(values())),
			RecordIdKey::Object(Object::default()),
			RecordIdKey::Range(Box::new(RecordIdKeyRange {
				start: Bound::Included(RecordIdKey::Number(1)),
				end: Bound::Unbounded,
			})),
			RecordIdKey::Range(Box::new(RecordIdKeyRange {
				start: Bound::Excluded(RecordIdKey::Array(Array::from(values()))),
				end: Bound::Included(RecordIdKey::Object(object_of(vec![("k", int(1))]))),
			})),
		];
		for value in values() {
			ids.push(RecordIdKey::Array(Array::from(vec![value.clone()])));
			ids.push(RecordIdKey::Object(object_of(vec![("k", value)])));
		}
		// A new kind of id must be listed above and in the mirror of `RecordIdKey` in
		// `crate::val::key_read`.
		for id in &ids {
			match id {
				RecordIdKey::Number(_)
				| RecordIdKey::String(_)
				| RecordIdKey::Uuid(_)
				| RecordIdKey::Array(_)
				| RecordIdKey::Object(_)
				| RecordIdKey::Range(_) => {}
			}
		}
		ids
	}

	const LONG: &str = "a_table_whose_name_is_longer_than_twenty_three_bytes";

	fn key_of(name: &str, id: &RecordIdKey) -> Vec<u8> {
		RecordKey::new(
			NamespaceId(7),
			DatabaseId(9),
			Cow::Borrowed(&TableName::new(name)),
			Cow::Borrowed(id),
		)
		.encode_key()
		.expect("a record key")
		.to_vec()
	}

	/// A record key of every kind of id, for tables of every kind of name.
	fn keys() -> Vec<(&'static str, Vec<u8>)> {
		let mut keys = Vec::new();
		for name in ["t", LONG, "with space", "n\u{e9}e", "nul\0inside"] {
			for id in ids() {
				keys.push((name, key_of(name, &id)));
			}
		}
		keys
	}

	/// The bytes `read_key` and the key decode answer for `bytes` agree: both refuse them, or both
	/// accept them and name the same table.
	fn agree(bytes: &[u8]) {
		let decoded = RecordKey::decode_key(bytes);
		let read = read_key(bytes);
		assert_eq!(read.is_some(), decoded.is_ok(), "{bytes:02x?}: {decoded:?}");
		if let (Some(table), Ok(decoded)) = (read, decoded) {
			assert!(table.iter().eq(decoded.tb.as_str().as_bytes()), "{bytes:02x?}");
		}
	}

	/// A key is read as the key decode reads it: the table it names is the table it decodes to,
	/// for every kind of id.
	#[test]
	fn a_key_reads_as_the_key_decode_reads_it() {
		for (name, key) in keys() {
			let table = read_key(&key).unwrap_or_else(|| panic!("{name}: {key:02x?}"));
			assert!(table.iter().eq(name.as_bytes()), "{key:02x?}");
			agree(&key);
		}
		assert!(read_key(&[]).is_none());
		assert!(read_key(&[0xFF, 0xFF]).is_none());
	}

	/// The key read accepts exactly the bytes the key decode accepts: every flip of every bit of
	/// each key, every truncation of it and a byte appended to it.
	#[test]
	fn the_key_read_accepts_what_the_key_decode_accepts() {
		for (_, key) in keys() {
			agree(&key);
			for at in 0..key.len() {
				for bit in 0..8 {
					let mut mutant = key.clone();
					mutant[at] ^= 1 << bit;
					agree(&mutant);
				}
				agree(&key[..at]);
			}
			let mut longer = key.clone();
			longer.push(0);
			agree(&longer);
		}
	}

	/// Every variant of `Value`, and every byte that could name one, is read within an array, an
	/// object and a range as the value decode reads it. The tag of each value that can be written
	/// is the tag the mirror of `Value` in [`crate::val::key_read`] reads it by, and every tag
	/// byte, those of no variant and those of the regex and the closure, which are never
	/// decoded, is refused or accepted as the key decode does.
	#[test]
	fn a_value_in_an_id_is_read_as_the_value_decode_reads_it() {
		let at = |id: RecordIdKey, other: RecordIdKey| {
			let (a, b) = (key_of("t", &id), key_of("t", &other));
			a.iter().zip(&b).position(|(a, b)| a != b).expect("keys that differ")
		};
		let slots = [
			(
				at(
					RecordIdKey::Array(Array::from(vec![Value::None])),
					RecordIdKey::Array(Array::from(vec![Value::Null])),
				),
				RecordIdKey::Array(Array::from(vec![Value::None])),
			),
			(
				at(
					RecordIdKey::Object(object_of(vec![("k", Value::None)])),
					RecordIdKey::Object(object_of(vec![("k", Value::Null)])),
				),
				RecordIdKey::Object(object_of(vec![("k", Value::None)])),
			),
		];
		for (slot, id) in slots {
			let key = key_of("t", &id);
			for byte in 0..=u8::MAX {
				let mut mutant = key.clone();
				mutant[slot] = byte;
				agree(&mutant);
			}
			for value in values() {
				let with = match id {
					RecordIdKey::Array(_) => RecordIdKey::Array(Array::from(vec![value.clone()])),
					_ => RecordIdKey::Object(object_of(vec![("k", value.clone())])),
				};
				let key = key_of("t", &with);
				assert_eq!(key[slot], tag(&value), "{value:?}");
				agree(&key);
			}
		}
	}

	/// The elements of a set are read as the set decode reads them: in any order, equal elements
	/// merged, and by the set's own order, which only the set decode applies.
	#[test]
	fn the_elements_of_an_id_set_are_read_as_the_set_decode_reads_them() {
		let wrap = |value: Value| RecordIdKey::Array(Array::from(vec![value]));
		let list = |a: Value, b: Value| Value::Array(Array::from(vec![a, b]));
		let sets = key_of("t", &wrap(Value::Set(Set::from(BTreeSet::from([int(1)])))));
		let lists = key_of("t", &wrap(Value::Array(Array::from(vec![int(1)]))));
		let slot = sets.iter().zip(&lists).position(|(a, b)| a != b).expect("keys that differ");
		let nan = || Value::Number(Number::Float(f64::NAN));
		let infinity = || Value::Number(Number::Float(f64::INFINITY));
		for (a, b) in [
			(int(1), int(2)),
			(int(2), int(1)),
			(int(1), int(1)),
			(nan(), nan()),
			(nan(), int(1)),
			(int(1), nan()),
			(infinity(), nan()),
			(Value::None, Value::Null),
			(Value::Null, Value::None),
		] {
			let mut bytes = key_of("t", &wrap(list(a.clone(), b.clone())));
			bytes[slot] = tag(&Value::Set(Set::new()));
			agree(&bytes);
		}
	}

	/// The entries of an object are read as the object decode reads them: keys that do not ascend
	/// or repeat are refused.
	#[test]
	fn the_entries_of_an_id_object_must_ascend() {
		let object = RecordIdKey::Object(object_of(vec![("a", int(1)), ("b", int(2))]));
		let key = key_of("t", &object);
		let first = key.windows(2).position(|pair| pair == b"a\0").expect("the first key");
		let second = key.windows(2).position(|pair| pair == b"b\0").expect("the second key");
		for (one, other) in [(b'a', b'a'), (b'b', b'a'), (b'a', b'b'), (b'a', b'c'), (b'c', b'b')] {
			let mut bytes = key.clone();
			bytes[first] = one;
			bytes[second] = other;
			agree(&bytes);
		}
	}
}
