//! Shaped rows in an output frame: values written through a [`ValueShaper`], and packed rows
//! written by running their generation's copy plan ([`crate::exec::shaped`]).

use surrealdb_types::shaped::{
	KeyLeaf, Leaf, RowWriter, ShapedError, ShapedValue, cbor_head, decimal_text,
};

use super::row_frame::{Entry, Stored, encode_scalar, encode_value};
use super::scalar::{Held, Scalar};
use crate::catalog::packed::{FieldRef, PackedRow};
use crate::exec::shaped::{CopyPlan, Fill, Source};
use crate::key::KVKeyDecode as _;
use crate::val::key_read::KeyIdRef;
use crate::val::stored::{Kind as StoredKind, ValueRef};
use crate::val::{Number, RecordIdKey, Value};

/// A value a frame writes as a shaped row: an in-memory value, a scalar, or an object of
/// top-level fields in key order.
#[derive(Clone, Copy)]
pub(super) enum CoreShaped<'v> {
	Value(&'v Value),
	Scalar(&'v Scalar<'v>),
	Fields(&'v [(&'v str, Scalar<'v>)]),
}

fn dynamic_error(e: &anyhow::Error) -> ShapedError {
	ShapedError(e.to_string())
}

impl<'v> CoreShaped<'v> {
	fn value_leaf(value: &'v Value) -> Leaf<'v> {
		match value {
			Value::None => Leaf::None,
			Value::Null => Leaf::Null,
			Value::Bool(v) => Leaf::Bool(*v),
			Value::Number(Number::I8(v)) => Leaf::I8(*v),
			Value::Number(Number::U8(v)) => Leaf::U8(*v),
			Value::Number(Number::I16(v)) => Leaf::I16(*v),
			Value::Number(Number::U16(v)) => Leaf::U16(*v),
			Value::Number(Number::I32(v)) => Leaf::I32(*v),
			Value::Number(Number::U32(v)) => Leaf::U32(*v),
			Value::Number(Number::Int(v)) => Leaf::Int(*v),
			Value::Number(Number::U64(v)) => Leaf::U64(*v),
			Value::Number(Number::F16(v)) => Leaf::F16(*v),
			Value::Number(Number::F32(v)) => Leaf::F32(*v),
			Value::Number(Number::Float(v)) => Leaf::Float(*v),
			Value::Number(Number::Decimal(v)) => Leaf::Decimal(*v),
			// A bigdecimal that fits a decimal is a decimal leaf. Any other value has no
			// fixed-width leaf and is written through the dynamic path.
			Value::Number(Number::BigDecimal(v)) => {
				match rust_decimal::Decimal::try_from(v.as_ref()) {
					Ok(d) => Leaf::Decimal(d),
					Err(_) => Leaf::Other,
				}
			}
			Value::String(v) => Leaf::String(v.as_str()),
			Value::Bytes(v) => Leaf::Bytes(&v.0),
			Value::Datetime(v) => Leaf::Datetime(v.0.timestamp(), v.0.timestamp_subsec_nanos()),
			Value::Duration(v) => Leaf::Duration(v.0.as_secs(), v.0.subsec_nanos()),
			Value::Uuid(v) => Leaf::Uuid(*v.0.as_bytes()),
			Value::RecordId(r) => Leaf::Record(
				r.table.as_str(),
				match &r.key {
					RecordIdKey::Number(n) => KeyLeaf::Int(*n),
					RecordIdKey::String(s) => KeyLeaf::String(s.as_str()),
					RecordIdKey::Uuid(u) => KeyLeaf::Uuid(*u.0.as_bytes()),
					RecordIdKey::Array(_) | RecordIdKey::Object(_) | RecordIdKey::Range(_) => {
						KeyLeaf::Other
					}
				},
			),
			Value::Object(_) => Leaf::Object,
			Value::Array(_) => Leaf::Array,
			Value::Set(_)
			| Value::Date(_)
			| Value::Time(_)
			| Value::Timestamp(_)
			| Value::Interval(_)
			| Value::Geometry(_)
			| Value::Table(_)
			| Value::File(_)
			| Value::Regex(_)
			| Value::Range(_)
			| Value::Closure(_) => Leaf::Other,
		}
	}
}

impl<'v> ShapedValue<'v> for CoreShaped<'v> {
	fn leaf(self) -> Leaf<'v> {
		match self {
			CoreShaped::Value(value) => Self::value_leaf(value),
			CoreShaped::Fields(_) => Leaf::Object,
			CoreShaped::Scalar(scalar) => match scalar {
				Scalar::None => Leaf::None,
				Scalar::Null => Leaf::Null,
				Scalar::Bool(v) => Leaf::Bool(*v),
				Scalar::Number(Number::I8(v)) => Leaf::I8(*v),
				Scalar::Number(Number::U8(v)) => Leaf::U8(*v),
				Scalar::Number(Number::I16(v)) => Leaf::I16(*v),
				Scalar::Number(Number::U16(v)) => Leaf::U16(*v),
				Scalar::Number(Number::I32(v)) => Leaf::I32(*v),
				Scalar::Number(Number::U32(v)) => Leaf::U32(*v),
				Scalar::Number(Number::Int(v)) => Leaf::Int(*v),
				Scalar::Number(Number::U64(v)) => Leaf::U64(*v),
				Scalar::Number(Number::F16(v)) => Leaf::F16(*v),
				Scalar::Number(Number::F32(v)) => Leaf::F32(*v),
				Scalar::Number(Number::Float(v)) => Leaf::Float(*v),
				Scalar::Number(Number::Decimal(v)) => Leaf::Decimal(*v),
				Scalar::Number(Number::BigDecimal(v)) => {
					match rust_decimal::Decimal::try_from(v.as_ref()) {
						Ok(d) => Leaf::Decimal(d),
						Err(_) => Leaf::Other,
					}
				}
				Scalar::Str(v) => Leaf::String(v),
				Scalar::Bytes(v) => Leaf::Bytes(v),
				Scalar::Duration(v) => Leaf::Duration(v.0.as_secs(), v.0.subsec_nanos()),
				Scalar::Datetime(v) => {
					Leaf::Datetime(v.0.timestamp(), v.0.timestamp_subsec_nanos())
				}
				Scalar::Uuid(v) => Leaf::Uuid(*v.0.as_bytes()),
				Scalar::Value(value) => Self::value_leaf(value.as_ref()),
			},
		}
	}

	fn get(self, key: &str) -> Option<Self> {
		match self {
			CoreShaped::Fields(fields) => fields
				.binary_search_by(|(k, _)| (*k).cmp(key))
				.ok()
				.map(|at| CoreShaped::Scalar(&fields[at].1)),
			CoreShaped::Value(Value::Object(o)) => o.get(key).map(CoreShaped::Value),
			CoreShaped::Scalar(Scalar::Value(value)) => {
				value.as_ref().as_object().and_then(|o| o.get(key)).map(CoreShaped::Value)
			}
			CoreShaped::Value(_) | CoreShaped::Scalar(_) => None,
		}
	}

	fn entries(self) -> impl Iterator<Item = (&'v str, Self)> {
		let object = match self {
			CoreShaped::Value(Value::Object(o)) => Some(o),
			CoreShaped::Scalar(Scalar::Value(value)) => value.as_ref().as_object(),
			CoreShaped::Value(_) | CoreShaped::Scalar(_) | CoreShaped::Fields(_) => None,
		};
		let fields = match self {
			CoreShaped::Fields(fields) => Some(fields),
			CoreShaped::Value(_) | CoreShaped::Scalar(_) => None,
		};
		object
			.into_iter()
			.flat_map(|o| o.iter().map(|(k, v)| (k.as_str(), CoreShaped::Value(v))))
			.chain(
				fields.into_iter().flat_map(|f| f.iter().map(|(k, v)| (*k, CoreShaped::Scalar(v)))),
			)
	}

	fn elements(self) -> impl Iterator<Item = Self> {
		let array = match self {
			CoreShaped::Value(Value::Array(a)) => Some(a),
			CoreShaped::Scalar(Scalar::Value(value)) => value.as_ref().as_array(),
			CoreShaped::Value(_) | CoreShaped::Scalar(_) | CoreShaped::Fields(_) => None,
		};
		array.into_iter().flat_map(|a| a.iter().map(CoreShaped::Value))
	}

	fn write_dynamic(self, out: &mut Vec<u8>) -> Result<(), ShapedError> {
		match self {
			CoreShaped::Value(value) => {
				encode_value(out, value, 1, true).map_err(|e| dynamic_error(&e))
			}
			CoreShaped::Scalar(scalar) => {
				encode_scalar(out, None, scalar, 1, true).map_err(|e| dynamic_error(&e))
			}
			CoreShaped::Fields(fields) => {
				cbor_head(out, 5, fields.len() as u64);
				for (key, value) in fields {
					cbor_head(out, 3, key.len() as u64);
					out.extend_from_slice(key.as_bytes());
					encode_scalar(out, None, value, 2, true).map_err(|e| dynamic_error(&e))?;
				}
				Ok(())
			}
		}
	}
}

/// Writes the packed row `row`, of the record `key` names (whose id the key reads as `id`), at the
/// end of `out` by running `plan`: the stored presence and fixed regions copied as one block, then
/// every slot and bit of a column the row does not hold cleared, and the columns the wire holds in
/// another form written. `present` and `id_text` are the frame's scratch. On an error some of the
/// row may have been written; the caller drops it.
#[allow(clippy::too_many_arguments)]
pub(super) fn write_packed(
	plan: &CopyPlan,
	row: &PackedRow<'_>,
	key: &[u8],
	id: &KeyIdRef<'_>,
	base: &[u8],
	out: &mut Vec<u8>,
	scratch: &mut Vec<Entry>,
	present: &mut Vec<bool>,
	id_text: &mut Vec<u8>,
) -> anyhow::Result<()> {
	let stored = row.fixed_bytes(0, plan.fixed_len as usize)?;
	let mut w = RowWriter::begin_with(out, &plan.shape, stored)?;
	let start = w.start();
	let fixed_at = w.fixed_at();
	for &(offset, len) in &plan.junk {
		let at = fixed_at + offset as usize;
		w.out()[at..at + len as usize].fill(0);
	}
	if let Some((index, slot)) = plan.root_rest {
		w.open_var(slot)?;
		remainder(row, index, 0, true, base, w.out(), scratch, id_text)?;
		w.close_var()?;
	}
	// Escapes are read in column order, so one cursor takes each trailer entry once.
	let mut trailer = 0usize;
	if present.len() == plan.ops.len() {
		present.fill(false);
	} else {
		present.clear();
		present.resize(plan.ops.len(), false);
	}
	for (at, op) in plan.ops.iter().enumerate() {
		let enclosed = op.parent.is_none_or(|p| present[p as usize]);
		// The state the copied bits give the column, as the stored row holds it.
		let bits = w.out();
		let bit = |b: u32| bits[start + (b / 8) as usize] & (1 << (b % 8)) != 0;
		let held = match (enclosed, op.width) {
			(false, _) => false,
			(true, 0) => true,
			(true, 1) => bit(op.presence),
			(true, _) => match (bit(op.presence), bit(op.presence + 1)) {
				(true, false) => true,
				(false, _) => false,
				(true, true) => anyhow::bail!("a presence of both bits set"),
			},
		};
		present[at] = held;
		if let Some(run) = op.run {
			write_run(&mut w, row, &run, present)?;
		}
		if !held {
			// Under a node the row does not hold, the column's bits are cleared too: nothing but
			// what the row holds reaches the wire.
			if !enclosed && op.width > 0 {
				w.set_bits(op.presence, op.width, 0);
			}
			if op.bit {
				w.fixed_bit(op.slot, false);
			} else if op.zero > 0 {
				let at = fixed_at + op.slot as usize;
				w.out()[at..at + op.zero as usize].fill(0);
			}
			continue;
		}
		let depth = 2;
		match op.fill {
			Fill::Copied | Fill::InRun => {}
			Fill::Id => {
				w.open_var(op.slot)?;
				write_key_id(key, id, None, w.out(), id_text)?;
				w.close_var()?;
			}
			Fill::String | Fill::RawBytes => {
				let Source::Column(index) = op.source else {
					anyhow::bail!("a string column without a source");
				};
				let bytes = row.column_bytes(index)?;
				w.open_var(op.slot)?;
				w.out().extend_from_slice(bytes);
				w.close_var()?;
			}
			Fill::Object {
				rest,
				depth,
			} => {
				if let Some(index) = rest {
					w.open_var(op.rest_slot)?;
					remainder(row, index, depth, false, base, w.out(), scratch, id_text)?;
					w.close_var()?;
				}
			}
			Fill::DatetimeNs | Fill::DurationNs => {
				let at = fixed_at + op.slot as usize;
				let raw = u64::from_le_bytes(w.out()[at..at + 8].try_into()?);
				let escaped = match op.fill {
					Fill::DatetimeNs => raw as i64 == i64::MIN || raw as i64 == i64::MAX,
					Fill::Id
					| Fill::Copied
					| Fill::DurationNs
					| Fill::String
					| Fill::RawBytes
					| Fill::Bytes
					| Fill::InRun
					| Fill::Decimal
					| Fill::Record
					| Fill::Dynamic
					| Fill::Array(_)
					| Fill::Object {
						..
					} => raw == u64::MAX,
				};
				if escaped {
					let Source::Column(index) = op.source else {
						anyhow::bail!("an escaped column without a source");
					};
					let (secs, nanos) = match row.column_value(index, &mut trailer)? {
						FieldRef::Datetime(v) => {
							(v.timestamp().to_le_bytes(), v.timestamp_subsec_nanos())
						}
						FieldRef::Duration(v) => (v.as_secs().to_le_bytes(), v.subsec_nanos()),
						FieldRef::Absent
						| FieldRef::Null
						| FieldRef::Object
						| FieldRef::Bool(_)
						| FieldRef::Int(_)
						| FieldRef::Float(_)
						| FieldRef::Decimal(_)
						| FieldRef::I8(_)
						| FieldRef::I16(_)
						| FieldRef::I32(_)
						| FieldRef::U8(_)
						| FieldRef::U16(_)
						| FieldRef::U32(_)
						| FieldRef::U64(_)
						| FieldRef::F16(_)
						| FieldRef::F32(_)
						| FieldRef::Uuid(_)
						| FieldRef::Point(..)
						| FieldRef::String(_)
						| FieldRef::Bytes(_)
						| FieldRef::Literal(_)
						| FieldRef::RecordId(..)
						| FieldRef::Stored(_) => {
							anyhow::bail!("an escaped slot of another kind")
						}
					};
					w.open_var(op.rest_slot)?;
					w.out().extend_from_slice(&secs);
					w.out().extend_from_slice(&nanos.to_le_bytes());
					w.close_var()?;
				}
			}
			fill
			@ (Fill::Bytes | Fill::Decimal | Fill::Record | Fill::Dynamic | Fill::Array(_)) => {
				let Source::Column(index) = op.source else {
					anyhow::bail!("a column without a source");
				};
				let value = row.column_value(index, &mut trailer)?;
				w.open_var(op.slot)?;
				match (fill, value) {
					(Fill::Bytes, FieldRef::Bytes(v)) => w.out().extend_from_slice(v),
					(Fill::Decimal, FieldRef::Decimal(v)) => decimal_text(w.out(), &v),
					(Fill::Record, FieldRef::RecordId(table, key)) => {
						let column_table = &plan.shape.columns()[op.column as usize].table;
						write_link(w.out(), column_table, table.as_str(), key, depth)?;
					}
					(Fill::Dynamic, FieldRef::Null) => w.out().push(0xf6),
					(Fill::Array(element), FieldRef::Stored(value)) => {
						native_array(w.out(), element, value)?
					}
					(Fill::Dynamic, field) => {
						dynamic(field, base, w.out(), scratch, id_text, depth)?
					}
					_ => anyhow::bail!("a packed column read as another kind than its plan's"),
				}
				w.close_var()?;
			}
		}
	}
	w.finish()?;
	Ok(())
}

/// Writes the run `run` of stored variable slices as its consecutive wire slots, with one copy. A
/// member the row does not hold (`present` false) must have an empty slice, as its writer leaves
/// it: one that holds bytes fails the row, so no stale byte reaches the wire.
fn write_run(
	w: &mut RowWriter<'_>,
	row: &PackedRow<'_>,
	run: &crate::exec::shaped::Run,
	present: &[bool],
) -> anyhow::Result<()> {
	let first = run.first_var as usize;
	let count = run.count as usize;
	let start = match first.checked_sub(1) {
		Some(before) => row.var_end(before)?,
		None => 0,
	};
	if count > 32 {
		anyhow::bail!("a run longer than its scratch");
	}
	let mut ends = [0u32; 32];
	let mut previous = start;
	for (k, slot) in ends.iter_mut().take(count).enumerate() {
		let end = row.var_end(first + k)?;
		if end < previous {
			anyhow::bail!("variable slices out of order");
		}
		if !present[run.first_op as usize + k] && end != previous {
			anyhow::bail!("an absent column with a stored slice");
		}
		*slot = u32::try_from(end - start)?;
		previous = end;
	}
	let bytes = row.var_bytes(start, previous)?;
	w.var_run(run.first_slot, bytes, ends[..count].iter().copied())?;
	Ok(())
}

/// Writes the stored array `value` as an `array` slice of `element`s; an array that is not one, or
/// holds another kind of element, fails the row.
fn native_array(
	out: &mut Vec<u8>,
	element: surrealdb_types::shaped::Kind,
	value: ValueRef<'_>,
) -> anyhow::Result<()> {
	use surrealdb_types::shaped::Kind as Wire;
	let Some(array) = value.as_array()? else {
		anyhow::bail!("a declared array that is not one");
	};
	let count = array.len();
	out.extend_from_slice(&u32::try_from(count)?.to_le_bytes());
	if element.is_var() {
		let ends = out.len();
		out.resize(ends + 4 * count, 0);
		let base = out.len();
		for (at, item) in array.iter().enumerate() {
			let item = item?;
			match (element, item.kind()) {
				// A string is UTF-8 as written, copied as it lies.
				(Wire::String, StoredKind::String) => out.extend_from_slice(item.payload()),
				(Wire::Bytes, _) => match item.as_bytes() {
					Some(bytes) => out.extend_from_slice(bytes),
					None => anyhow::bail!("an array element of another kind"),
				},
				(Wire::Decimal, _) => match item.as_decimal()? {
					Some(v) => decimal_text(out, &v),
					None => anyhow::bail!("an array element of another kind"),
				},
				_ => anyhow::bail!("an array element of another kind"),
			}
			let end = u32::try_from(out.len() - base)?;
			out[ends + 4 * at..ends + 4 * at + 4].copy_from_slice(&end.to_le_bytes());
		}
		return Ok(());
	}
	for item in array.iter() {
		let item = item?;
		match element {
			Wire::Int => match (item.kind(), item.as_int()?) {
				(StoredKind::Int, Some(v)) => out.extend_from_slice(&v.to_le_bytes()),
				_ => anyhow::bail!("an array element of another kind"),
			},
			Wire::I8 => match (item.kind(), item.as_int()?) {
				(StoredKind::I8, Some(v)) => out.extend_from_slice(&(v as i8).to_le_bytes()),
				_ => anyhow::bail!("an array element of another kind"),
			},
			Wire::I16 => match (item.kind(), item.as_int()?) {
				(StoredKind::I16, Some(v)) => out.extend_from_slice(&(v as i16).to_le_bytes()),
				_ => anyhow::bail!("an array element of another kind"),
			},
			Wire::I32 => match (item.kind(), item.as_int()?) {
				(StoredKind::I32, Some(v)) => out.extend_from_slice(&(v as i32).to_le_bytes()),
				_ => anyhow::bail!("an array element of another kind"),
			},
			Wire::U8 => match (item.kind(), item.as_int()?) {
				(StoredKind::U8, Some(v)) => out.extend_from_slice(&(v as u8).to_le_bytes()),
				_ => anyhow::bail!("an array element of another kind"),
			},
			Wire::U16 => match (item.kind(), item.as_int()?) {
				(StoredKind::U16, Some(v)) => out.extend_from_slice(&(v as u16).to_le_bytes()),
				_ => anyhow::bail!("an array element of another kind"),
			},
			Wire::U32 => match (item.kind(), item.as_int()?) {
				(StoredKind::U32, Some(v)) => out.extend_from_slice(&(v as u32).to_le_bytes()),
				_ => anyhow::bail!("an array element of another kind"),
			},
			Wire::U64 => match (item.kind(), item.decode()?) {
				(StoredKind::U64, Value::Number(Number::U64(v))) => {
					out.extend_from_slice(&v.to_le_bytes())
				}
				_ => anyhow::bail!("an array element of another kind"),
			},
			Wire::Float => match (item.kind(), item.as_float()?) {
				(StoredKind::Float, Some(v)) => out.extend_from_slice(&v.to_bits().to_le_bytes()),
				_ => anyhow::bail!("an array element of another kind"),
			},
			Wire::F16 => match (item.kind(), item.decode()?) {
				(StoredKind::F16, Value::Number(Number::F16(v))) => {
					out.extend_from_slice(&v.to_bits().to_le_bytes())
				}
				_ => anyhow::bail!("an array element of another kind"),
			},
			Wire::F32 => match (item.kind(), item.decode()?) {
				(StoredKind::F32, Value::Number(Number::F32(v))) => {
					out.extend_from_slice(&v.to_bits().to_le_bytes())
				}
				_ => anyhow::bail!("an array element of another kind"),
			},
			Wire::Bool => match item.as_bool() {
				Some(v) => out.push(u8::from(v)),
				None => anyhow::bail!("an array element of another kind"),
			},
			Wire::Uuid => match item.as_uuid()? {
				Some(v) => out.extend_from_slice(v.as_bytes()),
				None => anyhow::bail!("an array element of another kind"),
			},
			Wire::Datetime => match item.as_datetime()? {
				Some(v) => {
					out.extend_from_slice(&v.timestamp().to_le_bytes());
					out.extend_from_slice(&v.timestamp_subsec_nanos().to_le_bytes());
				}
				None => anyhow::bail!("an array element of another kind"),
			},
			Wire::Duration => match item.as_duration()? {
				Some(v) => {
					out.extend_from_slice(&v.as_secs().to_le_bytes());
					out.extend_from_slice(&v.subsec_nanos().to_le_bytes());
				}
				None => anyhow::bail!("an array element of another kind"),
			},
			Wire::String
			| Wire::Bytes
			| Wire::Decimal
			| Wire::Record
			| Wire::Dynamic
			| Wire::Object
			| Wire::Bit
			| Wire::Array
			| Wire::DatetimeNs
			| Wire::DurationNs => anyhow::bail!("an array element kind not written natively"),
		}
	}
	Ok(())
}

/// Writes the remainder of an open node, descriptor column `index` at depth `depth`: nothing when
/// the node holds no key beyond its columns, else one CBOR map.
#[allow(clippy::too_many_arguments)]
fn remainder(
	row: &PackedRow<'_>,
	index: usize,
	depth: usize,
	root: bool,
	base: &[u8],
	out: &mut Vec<u8>,
	scratch: &mut Vec<Entry>,
	id_text: &mut Vec<u8>,
) -> anyhow::Result<()> {
	let Some(object) = row.column_remainder(index, depth)? else {
		return Ok(());
	};
	if object.is_empty() {
		return Ok(());
	}
	cbor_head(out, 5, object.len() as u64);
	for at in 0..object.len() {
		let (key, value) = object.entry(at)?;
		anyhow::ensure!(!(root && key == "id"), "a root remainder holding `id`");
		cbor_head(out, 3, key.len() as u64);
		out.extend_from_slice(key.as_bytes());
		Stored {
			base,
			out: &mut *out,
			scratch: &mut *scratch,
			id_text: &mut *id_text,
			json: None,
			v2: true,
		}
		.stored_value(value, depth + 2)?;
	}
	Ok(())
}

/// Writes a dynamic column's value as one CBOR item.
fn dynamic(
	field: FieldRef<'_>,
	base: &[u8],
	out: &mut Vec<u8>,
	scratch: &mut Vec<Entry>,
	id_text: &mut Vec<u8>,
	depth: usize,
) -> anyhow::Result<()> {
	let scalar = match field {
		FieldRef::Stored(value) => {
			return Stored {
				base,
				out,
				scratch,
				id_text,
				json: None,
				v2: true,
			}
			.stored_value(value, depth);
		}
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
		FieldRef::Bool(v) => Scalar::Bool(v),
		FieldRef::String(v) => Scalar::Str(v),
		FieldRef::Bytes(v) => Scalar::Bytes(v),
		FieldRef::Uuid(v) => Scalar::Uuid(crate::val::Uuid(v)),
		FieldRef::Datetime(v) => Scalar::Datetime(crate::val::Datetime(v)),
		FieldRef::Duration(v) => Scalar::Duration(crate::val::Duration(v)),
		FieldRef::Literal(v) => Scalar::of(v),
		FieldRef::Point(x, y) => {
			Scalar::Value(Held::Owned(Value::Geometry(crate::val::Geometry::Point((x, y).into()))))
		}
		FieldRef::RecordId(table, key) => {
			let key = crate::val::stored::decode_key_nested(key, depth)?;
			Scalar::Value(Held::Owned(Value::RecordId(crate::val::RecordId {
				table: table.clone(),
				key,
			})))
		}
		FieldRef::Null | FieldRef::Absent | FieldRef::Object => {
			anyhow::bail!("a dynamic column handed a field with no value")
		}
	};
	encode_scalar(out, None, &scalar, depth, true)
}

/// Writes a record link column's slice for the record id of `table` and the stored key `key`.
fn write_link(
	out: &mut Vec<u8>,
	column_table: &str,
	table: &str,
	key: &[u8],
	depth: usize,
) -> anyhow::Result<()> {
	let link = ValueRef::new_nested(key, depth)?;
	let leaf = match link.kind() {
		StoredKind::Int => link.as_int()?.map(KeyLeaf::Int),
		StoredKind::String => link.as_str()?.map(KeyLeaf::String),
		StoredKind::None
		| StoredKind::Null
		| StoredKind::Bool
		| StoredKind::Float
		| StoredKind::Decimal
		| StoredKind::BigDecimal
		| StoredKind::I8
		| StoredKind::U8
		| StoredKind::I16
		| StoredKind::U16
		| StoredKind::I32
		| StoredKind::U32
		| StoredKind::U64
		| StoredKind::F16
		| StoredKind::F32
		| StoredKind::Duration
		| StoredKind::Datetime
		| StoredKind::Date
		| StoredKind::Time
		| StoredKind::Timestamp
		| StoredKind::Interval
		| StoredKind::Uuid
		| StoredKind::Bytes
		| StoredKind::Table
		| StoredKind::RecordId
		| StoredKind::File
		| StoredKind::Regex
		| StoredKind::Range
		| StoredKind::Geometry
		| StoredKind::Array
		| StoredKind::Set
		| StoredKind::Object
		| StoredKind::Unknown => None,
	};
	// A table too long for the slice's `u16` length is carried in a whole record id.
	let carried_too_long = column_table.is_empty() && table.len() > usize::from(u16::MAX);
	match leaf {
		Some(leaf) if !carried_too_long => write_record_slice(out, column_table, table, leaf),
		_ => {
			let key = crate::val::stored::decode_key_nested(key, depth)?;
			let whole = Value::RecordId(crate::val::RecordId {
				table: table.into(),
				key,
			});
			out.push(3);
			encode_value(out, &whole, depth, true)
		}
	}
}

/// Writes a `record` slice whose key is a number, a string or a uuid.
fn write_record_slice(
	out: &mut Vec<u8>,
	column_table: &str,
	table: &str,
	key: KeyLeaf<'_>,
) -> anyhow::Result<()> {
	out.push(match key {
		KeyLeaf::Int(_) => 0,
		KeyLeaf::String(_) => 1,
		KeyLeaf::Uuid(_) => 2,
		KeyLeaf::Other => anyhow::bail!("a record key held whole"),
	});
	if column_table.is_empty() {
		let len = u16::try_from(table.len())?;
		out.extend_from_slice(&len.to_le_bytes());
		out.extend_from_slice(table.as_bytes());
	} else {
		anyhow::ensure!(column_table == table, "a record of another table than its column's");
	}
	match key {
		KeyLeaf::Int(n) => out.extend_from_slice(&n.to_le_bytes()),
		KeyLeaf::String(s) => out.extend_from_slice(s.as_bytes()),
		KeyLeaf::Uuid(u) => out.extend_from_slice(&u),
		KeyLeaf::Other => {}
	}
	Ok(())
}

/// The shape documents are written in: an `id` column whose values carry their table, and an
/// open root holding every other entry.
pub(super) fn document_shape() -> anyhow::Result<std::sync::Arc<surrealdb_types::shaped::Shape>> {
	use surrealdb_types::shaped::{ColumnDef, Kind, Shape, ShapeDef};
	let id = ColumnDef {
		name: "id".to_owned(),
		kind: Kind::Record,
		element: None,
		flags: 0,
		table: String::new(),
		open: false,
		children: Vec::new(),
	};
	Ok(std::sync::Arc::new(Shape::new(ShapeDef::object(true, &[id]))?))
}

/// Writes the document `object` of the record `key` names as a row of `shape`
/// ([`document_shape`]): its entries but `id` transcoded in place into the root's remainder, its
/// `id` from the key.
pub(super) fn write_document(
	shape: &surrealdb_types::shaped::Shape,
	key: &[u8],
	object: crate::val::stored::OrderedObjectRef<'_>,
	stored: &mut Stored<'_, '_>,
) -> anyhow::Result<()> {
	let rest = shape.def().root_rest;
	let id = shape.columns()[0].slot;
	let base = stored.base;
	let mut w = RowWriter::begin(stored.out, shape);
	w.open_var(rest)?;
	Stored {
		base,
		out: w.out(),
		scratch: &mut *stored.scratch,
		id_text: &mut *stored.id_text,
		json: None,
		v2: true,
	}
	.stored_object_rest(object, 1)?;
	w.close_var()?;
	let Some((table, key_id)) = super::row_packed::read_key_id(key) else {
		anyhow::bail!("a record key that does not decode");
	};
	w.open_var(id)?;
	write_key_id(key, &key_id, Some(table), w.out(), stored.id_text)?;
	w.close_var()?;
	w.finish()?;
	Ok(())
}

/// Writes the record id slice of the record `key` names, whose id the key reads as `id`: with the
/// key's `table` when given, else in a column that names the table.
fn write_key_id(
	key: &[u8],
	id: &KeyIdRef<'_>,
	table: Option<&storekey::EscapedSlice>,
	out: &mut Vec<u8>,
	id_text: &mut Vec<u8>,
) -> anyhow::Result<()> {
	// A table too long for the slice's `u16` length is carried in a whole record id.
	let limit = usize::from(u16::MAX);
	let carried_too_long =
		table.is_some_and(|t| t.as_bytes().len() > limit + 1 && t.iter().count() > limit);
	if carried_too_long || matches!(id, KeyIdRef::Other) {
		let decoded = crate::key::schema::RecordKey::decode_key(key)
			.map_err(|e| anyhow::anyhow!("Failed to decode record key: {e}"))?;
		let whole = Value::RecordId(crate::val::RecordId {
			table: decoded.tb.as_ref().clone(),
			key: decoded.id.as_ref().clone(),
		});
		out.push(3);
		return encode_value(out, &whole, 2, true);
	}
	out.push(match id {
		KeyIdRef::Number(_) => 0,
		KeyIdRef::String(_) => 1,
		KeyIdRef::Uuid(_) | KeyIdRef::Other => 2,
	});
	if let Some(table) = table {
		let at = out.len();
		out.extend_from_slice(&[0, 0]);
		out.extend(table.iter());
		let len = u16::try_from(out.len() - at - 2)?;
		out[at..at + 2].copy_from_slice(&len.to_le_bytes());
	}
	match id {
		KeyIdRef::Number(n) => out.extend_from_slice(&n.to_le_bytes()),
		KeyIdRef::String(escaped) => {
			let raw = escaped.as_bytes();
			let raw = raw.get(..raw.len().saturating_sub(1)).unwrap_or_default();
			// The key escapes a byte with a 1 before it; most strings hold neither.
			if raw.contains(&1) {
				id_text.clear();
				id_text.extend(escaped.iter());
				out.extend_from_slice(id_text);
			} else {
				out.extend_from_slice(raw);
			}
		}
		KeyIdRef::Uuid(u) => out.extend_from_slice(u.0.as_bytes()),
		KeyIdRef::Other => {}
	}
	Ok(())
}
