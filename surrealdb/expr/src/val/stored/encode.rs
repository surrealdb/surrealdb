use std::cell::RefCell;
use std::ops::Bound;

use super::tag::*;
use super::{DEPTH_LIMIT, Error, geometry, int_width, varint};
use crate::val::{Number, Object, RecordId, RecordIdKey, Value};

/// Writes stored encodings back to front.
///
/// What has been written is `buf[head..]`; each write prepends. A container's
/// entries are therefore complete before its count and offset table are
/// written, so the table takes the narrowest width that holds it without
/// moving the entries. Values are prepended last entry first.
///
/// One encoder is reused across values ([`with_encoder`]), so encoding a record
/// allocates nothing beyond the bytes it returns.
#[derive(Default)]
pub struct Encoder {
	buf: Vec<u8>,
	head: usize,
	/// The entry starts of the containers being written, innermost last, as
	/// [`Encoder::written`] values.
	starts: Vec<u32>,
}

/// A container being written with [`Encoder::open`].
///
/// The encoder writes back to front, so a container's entries are prepended last first and the
/// container is closed after the first entry has been prepended.
#[must_use = "an opened container must be closed"]
#[derive(Debug)]
pub struct Open {
	/// Where the container's entries end: `written()` when it was opened.
	end: usize,
	/// The length of the encoder's entry-start stack when it was opened.
	mark: usize,
	/// `written()` after the entry or element ended last.
	at: usize,
	/// The number of the map entry ended last.
	last: Option<u32>,
}

impl Open {
	/// Consumes the handle: where the container's entries end, and the entry-start stack length
	/// when it was opened.
	fn into_span(self) -> (usize, usize) {
		(self.end, self.mark)
	}
}

/// Buffers grown past this are dropped rather than kept for the next value, so
/// one outsized record does not pin its buffer to the thread.
const RETAIN_LIMIT: usize = 1 << 20;

thread_local! {
	static ENCODER: RefCell<Encoder> = RefCell::new(Encoder::default());
}

/// Runs `f` with this thread's reusable encoder, cleared, or with a fresh one
/// when the thread's is already in use.
pub fn with_encoder<R>(f: impl FnOnce(&mut Encoder) -> R) -> R {
	ENCODER.with(|cell| match cell.try_borrow_mut() {
		Ok(mut enc) => {
			enc.clear();
			let r = f(&mut enc);
			if enc.buf.len() > RETAIN_LIMIT {
				*enc = Encoder::default();
			}
			r
		}
		Err(_) => f(&mut Encoder::default()),
	})
}

/// The stored encoding of `v`.
pub fn encode(v: &Value) -> Result<Vec<u8>, Error> {
	with_encoder(|enc| {
		enc.value(v)?;
		Ok(enc.as_bytes().to_vec())
	})
}

/// The head byte of a DECIMAL payload, the little-endian magnitude of the mantissa, and the
/// number of leading bytes of it that hold the magnitude: the fewest that do, none for zero.
#[inline]
fn decimal_parts(d: &rust_decimal::Decimal) -> (u8, [u8; 16], usize) {
	let m = d.mantissa().unsigned_abs();
	let len = (128 - m.leading_zeros() as usize).div_ceil(8);
	(((d.is_sign_negative() as u8) << 7) | d.scale() as u8, m.to_le_bytes(), len)
}

/// Appends the payload of the DECIMAL `d` to `out`: the head byte, then the magnitude of the
/// mantissa in the fewest bytes, little-endian. It is the payload [`Encoder::decimal`] writes
/// after the tag, and the only one [`decimal_payload`](super::decimal_payload) accepts.
pub fn put_decimal_payload(d: &rust_decimal::Decimal, out: &mut Vec<u8>) {
	let (head, magnitude, len) = decimal_parts(d);
	out.push(head);
	out.extend_from_slice(&magnitude[..len]);
}

impl Encoder {
	/// Discards everything written.
	pub fn clear(&mut self) {
		self.head = self.buf.len();
		self.starts.clear();
	}

	/// The number of bytes written.
	#[inline(always)]
	pub fn written(&self) -> usize {
		self.buf.len() - self.head
	}

	/// Everything written, first byte first.
	#[inline(always)]
	pub fn as_bytes(&self) -> &[u8] {
		&self.buf[self.head..]
	}

	#[cold]
	#[inline(never)]
	fn grow(&mut self, need: usize) {
		let used = self.written();
		let len = (self.buf.len() * 2).max(used + need).max(256);
		let mut buf = vec![0u8; len];
		buf[len - used..].copy_from_slice(&self.buf[self.head..]);
		self.head = len - used;
		self.buf = buf;
	}

	/// Prepends `n` bytes for the caller to fill.
	#[inline(always)]
	pub(super) fn front(&mut self, n: usize) -> &mut [u8] {
		if self.head < n {
			self.grow(n);
		}
		self.head -= n;
		&mut self.buf[self.head..self.head + n]
	}

	/// Prepends `b`.
	#[inline(always)]
	pub fn put(&mut self, b: &[u8]) {
		self.front(b.len()).copy_from_slice(b);
	}

	/// Prepends one byte.
	#[inline(always)]
	pub fn put_u8(&mut self, b: u8) {
		self.front(1)[0] = b;
	}

	/// Prepends `v` as a canonical unsigned LEB128 varint.
	#[inline]
	pub fn put_varint_u64(&mut self, v: u64) {
		let mut buf = [0u8; varint::MAX_LEN];
		self.put(varint::write_u64(v, &mut buf));
	}

	/// Prepends a `clen` length prefix.
	#[inline(always)]
	pub fn put_clen(&mut self, n: usize) -> Result<(), Error> {
		if n < 0xFF {
			self.put_u8(n as u8);
		} else {
			let n =
				u32::try_from(n).map_err(|_| Error::Unstorable("a string longer than 4 GiB"))?;
			self.put(&n.to_le_bytes());
			self.put_u8(0xFF);
		}
		Ok(())
	}

	/// Prepends a `clen`-prefixed byte string.
	#[inline(always)]
	fn put_clen_bytes(&mut self, b: &[u8]) -> Result<(), Error> {
		self.put(b);
		self.put_clen(b.len())
	}

	#[inline(always)]
	fn mark(&mut self) -> Result<(), Error> {
		let at = u32::try_from(self.written())
			.map_err(|_| Error::Unstorable("a value larger than 4 GiB"))?;
		self.starts.push(at);
		Ok(())
	}

	/// Prepends the tag, count and offset table of the entries written since `end`, whose starts
	/// were pushed after `mark`, last entry first. The entry count is the number of starts pushed
	/// since `mark`, so the table always describes exactly the entries recorded.
	fn finish(&mut self, base: u8, end: usize, mark: usize) -> Result<(), Error> {
		const MISORDERED: Error = Error::Unstorable("a container closed out of order");
		let n = self.starts.len().checked_sub(mark).ok_or(MISORDERED)?;
		let start = self.written();
		let len = start.checked_sub(end).ok_or(MISORDERED)?;
		// The starts are non-decreasing, so the first and last bound all of them: every entry
		// begins inside the span being closed, which keeps each offset within `len`.
		let spans = self.starts.get(mark..).ok_or(MISORDERED)?;
		if let (Some(&first), Some(&last)) = (spans.first(), spans.last())
			&& ((first as usize) < end || (last as usize) > start)
		{
			return Err(MISORDERED);
		}
		let (w, tag) = if len <= 0xFF && n <= 0xFF {
			(1, base)
		} else if len <= 0xFFFF && n <= 0xFFFF {
			(2, base + 1)
		} else if u32::try_from(len).is_ok() && u32::try_from(n).is_ok() {
			(4, base + 2)
		} else {
			return Err(Error::Unstorable("a value larger than 4 GiB"));
		};
		let total = n
			.checked_mul(w)
			.and_then(|t| t.checked_add(1 + w))
			.ok_or(Error::Unstorable("a value larger than 4 GiB"))?;
		if self.head < total {
			self.grow(total);
		}
		self.head -= total;
		let h = self.head;
		let out = &mut self.buf[h..h + total];
		let starts = &self.starts[mark..];
		out[0] = tag;
		match w {
			1 => {
				out[1] = n as u8;
				for (slot, &s) in out[2..].iter_mut().zip(starts.iter().rev()) {
					*slot = (start - s as usize) as u8;
				}
			}
			2 => {
				out[1..3].copy_from_slice(&(n as u16).to_le_bytes());
				for (slot, &s) in
					out[3..].as_chunks_mut::<2>().0.iter_mut().zip(starts.iter().rev())
				{
					*slot = ((start - s as usize) as u16).to_le_bytes();
				}
			}
			_ => {
				out[1..5].copy_from_slice(&(n as u32).to_le_bytes());
				for (slot, &s) in
					out[5..].as_chunks_mut::<4>().0.iter_mut().zip(starts.iter().rev())
				{
					*slot = ((start - s as usize) as u32).to_le_bytes();
				}
			}
		}
		self.starts.truncate(mark);
		Ok(())
	}

	/// Prepends `v`.
	pub fn value(&mut self, v: &Value) -> Result<(), Error> {
		self.value_at(v, 0)
	}

	/// Prepends `v` as a value nested `depth` containers deep: the depth limit counts the levels
	/// above it, exactly as [`decode_nested`](super::decode_nested) does at the same depth.
	pub fn value_nested(&mut self, v: &Value, depth: usize) -> Result<(), Error> {
		self.value_at(v, depth)
	}

	/// Prepends the record id key `k` alone, nested `depth` containers deep. A record id at depth
	/// `d` holds its key at `d + 1`.
	pub fn record_key_nested(&mut self, k: &RecordIdKey, depth: usize) -> Result<(), Error> {
		self.key(k, depth)
	}

	/// [`Encoder::object_entries`] for an OBJECT nested `depth` containers deep.
	pub fn object_entries_nested<'v, I>(
		&mut self,
		rev: I,
		n: usize,
		depth: usize,
	) -> Result<(), Error>
	where
		I: Iterator<Item = (&'v str, &'v Value)>,
	{
		self.object(rev, n, depth)
	}

	/// Prepends `obj` without its entry for `skip`, if it has one.
	pub fn object_without(&mut self, obj: &Object, skip: &str) -> Result<(), Error> {
		let n = obj.0.len() - usize::from(obj.0.contains_key(skip));
		self.object(
			obj.0.iter().rev().filter(|(k, _)| k.as_str() != skip).map(|(k, v)| (k.as_str(), v)),
			n,
			0,
		)
	}

	/// An OBJECT of `n` entries; `rev` yields them last first, so a key order ascending bytewise
	/// reads ascending when reversed. The caller supplies the order and the count: a count that is
	/// not the number of entries yielded is refused, and keys are not checked.
	pub fn object_entries<'v, I>(&mut self, rev: I, n: usize) -> Result<(), Error>
	where
		I: Iterator<Item = (&'v str, &'v Value)>,
	{
		self.object(rev, n, 0)
	}

	/// `rev` yields the entries last first.
	fn object<'v, I>(&mut self, rev: I, n: usize, depth: usize) -> Result<(), Error>
	where
		I: Iterator<Item = (&'v str, &'v Value)>,
	{
		let end = self.written();
		let mark = self.starts.len();
		for (k, v) in rev {
			self.value_at(v, depth + 1)?;
			self.put_clen_bytes(k.as_bytes())?;
			self.mark()?;
		}
		self.expect_entries(mark, n)?;
		self.finish(OBJECT, end, mark)
	}

	/// `rev` yields the elements last first.
	fn items<'v, I>(&mut self, base: u8, rev: I, n: usize, depth: usize) -> Result<(), Error>
	where
		I: Iterator<Item = &'v Value>,
	{
		let end = self.written();
		let mark = self.starts.len();
		for v in rev {
			self.value_at(v, depth + 1)?;
			self.mark()?;
		}
		self.expect_entries(mark, n)?;
		self.finish(base, end, mark)
	}

	/// Checks that exactly `n` entries have been recorded since `mark`.
	#[inline(always)]
	fn expect_entries(&self, mark: usize, n: usize) -> Result<(), Error> {
		match self.starts.len().checked_sub(mark) {
			Some(got) if got == n => Ok(()),
			_ => Err(Error::Unstorable("a container whose entry count is not the entries written")),
		}
	}

	/// Prepends NONE.
	#[inline(always)]
	pub fn none(&mut self) {
		self.put_u8(NONE);
	}

	/// Prepends FALSE or TRUE.
	#[inline(always)]
	pub fn bool(&mut self, b: bool) {
		self.put_u8(if b {
			TRUE
		} else {
			FALSE
		});
	}

	/// Prepends the integer `v`: a one-byte small int in `0..=126`, else INT in the fewest bytes
	/// that hold it.
	#[inline]
	pub fn int(&mut self, v: i64) {
		if (0..=SMALL_INT_MAX).contains(&v) {
			self.put_u8(SMALL_INT + v as u8);
		} else {
			self.put(&v.to_le_bytes()[..int_width(v)]);
			self.put_u8(INT);
		}
	}

	/// Prepends the float `f`: FLOAT in its 4-byte form whenever an `f32` holds it exactly (NaN
	/// excepted), else in its 8-byte form.
	#[inline]
	pub fn float(&mut self, f: f64) {
		let g = f as f32;
		if g as f64 == f && !f.is_nan() {
			self.put(&g.to_le_bytes());
		} else {
			self.put(&f.to_le_bytes());
		}
		self.put_u8(FLOAT);
	}

	/// Prepends `f` as FLOAT in its 4-byte form, holding the bits of `f` itself.
	#[inline]
	pub fn f32_bits(&mut self, f: f32) {
		self.put(&f.to_le_bytes());
		self.put_u8(FLOAT);
	}

	/// Prepends the decimal `d`.
	#[inline]
	pub fn decimal(&mut self, d: &rust_decimal::Decimal) {
		let (head, magnitude, len) = decimal_parts(d);
		self.put(&magnitude[..len]);
		self.put_u8(head);
		self.put_u8(DECIMAL);
	}

	/// Prepends `n` as INT, FLOAT or DECIMAL.
	#[inline]
	pub fn number(&mut self, n: &Number) {
		match n {
			Number::Int(i) => self.int(*i),
			Number::Float(f) => self.float(*f),
			Number::Decimal(d) => self.decimal(d),
		}
	}

	/// Prepends the STRING `s`.
	#[inline]
	pub fn string(&mut self, s: &str) {
		self.put(s.as_bytes());
		self.put_u8(STRING);
	}

	/// Prepends the BYTES `b`.
	#[inline]
	pub fn bytes(&mut self, b: &[u8]) {
		self.put(b);
		self.put_u8(BYTES);
	}

	/// Prepends the UUID `u`.
	#[inline]
	pub fn uuid(&mut self, u: &uuid::Uuid) {
		self.put(u.as_bytes());
		self.put_u8(UUID);
	}

	/// Prepends the DURATION `d`: nanoseconds as a `u64` when they fit, else seconds and
	/// sub-second nanoseconds.
	#[inline]
	pub fn duration(&mut self, d: std::time::Duration) {
		match u64::try_from(d.as_nanos()) {
			Ok(ns) => self.put(&ns.to_le_bytes()),
			Err(_) => {
				self.put(&d.subsec_nanos().to_le_bytes());
				self.put(&d.as_secs().to_le_bytes());
			}
		}
		self.put_u8(DURATION);
	}

	/// Prepends the DATETIME `d`: nanoseconds from the Unix epoch when they fit, else seconds and
	/// sub-second nanoseconds.
	#[inline]
	pub fn datetime(&mut self, d: &chrono::DateTime<chrono::Utc>) {
		// A leap second's sub-second part is a second or more, which a nanosecond count cannot
		// carry.
		let subsec = d.timestamp_subsec_nanos();
		match d.timestamp_nanos_opt().filter(|_| subsec < 1_000_000_000) {
			Some(ns) => self.put(&ns.to_le_bytes()),
			None => {
				self.put(&subsec.to_le_bytes());
				self.put(&d.timestamp().to_le_bytes());
			}
		}
		self.put_u8(DATETIME);
	}

	/// Prepends the RECORD ID `r`.
	pub fn record_id(&mut self, r: &RecordId) -> Result<(), Error> {
		self.record_id_at(r, 0)
	}

	fn record_id_at(&mut self, r: &RecordId, depth: usize) -> Result<(), Error> {
		self.key(&r.key, depth + 1)?;
		self.put_clen_bytes(r.table.as_str().as_bytes())?;
		self.put_u8(RECORD_ID);
		Ok(())
	}

	/// Prepends the canonical array of `v`: a typed integer or float array where one applies, else
	/// an ARRAY of its elements.
	pub fn values(&mut self, v: &[Value]) -> Result<(), Error> {
		self.values_at(v, 0)
	}

	/// Opens a container whose entries are prepended last first. Close it with
	/// [`Encoder::close_map`] or [`Encoder::close_list`] after ending every entry.
	pub fn open(&mut self) -> Result<Open, Error> {
		let written = self.written();
		u32::try_from(written).map_err(|_| Error::Unstorable("a value larger than 4 GiB"))?;
		Ok(Open {
			end: written,
			mark: self.starts.len(),
			at: written,
			last: None,
		})
	}

	/// Ends a map entry whose item was just prepended: prepends `number` and records the entry.
	///
	/// Entries are prepended last first, so each `number` is smaller than the one before it, and
	/// is at least 1. An item is at least one byte. A violation is refused, because it would write
	/// a map the reader refuses.
	pub fn end_entry(&mut self, open: &mut Open, number: u32) -> Result<(), Error> {
		if number == 0 {
			return Err(Error::Unstorable("a map entry number of zero"));
		}
		if open.last.is_some_and(|last| number >= last) {
			return Err(Error::Unstorable("map entry numbers are not strictly ascending"));
		}
		if self.written() <= open.at {
			return Err(Error::Unstorable("a map entry with no item"));
		}
		self.put_varint_u64(u64::from(number));
		self.mark()?;
		open.at = self.written();
		open.last = Some(number);
		Ok(())
	}

	/// Prepends `item`, already encoded, as a map entry under `number`. Refuses an empty `item`.
	///
	/// The bytes are copied as they are: the caller has validated them.
	pub fn raw_entry(&mut self, open: &mut Open, number: u32, item: &[u8]) -> Result<(), Error> {
		if item.is_empty() {
			return Err(Error::Unstorable("a map entry with no item"));
		}
		self.put(item);
		self.end_entry(open, number)
	}

	/// Ends a list element whose item was just prepended. An item is at least one byte.
	pub fn end_item(&mut self, open: &mut Open) -> Result<(), Error> {
		if self.written() <= open.at {
			return Err(Error::Unstorable("a list element with no item"));
		}
		self.mark()?;
		open.at = self.written();
		Ok(())
	}

	/// Closes the map of the entries ended since `open`. The entry count is the number of entries
	/// ended, so the count and the offset table always agree with the entries.
	pub fn close_map(&mut self, open: Open) -> Result<(), Error> {
		let (end, mark) = open.into_span();
		self.finish(MAP, end, mark)
	}

	/// Closes the list of the elements ended since `open`, as an ARRAY container.
	pub fn close_list(&mut self, open: Open) -> Result<(), Error> {
		let (end, mark) = open.into_span();
		self.finish(ARRAY, end, mark)
	}

	fn value_at(&mut self, v: &Value, depth: usize) -> Result<(), Error> {
		if depth > DEPTH_LIMIT {
			return Err(Error::TooDeep(DEPTH_LIMIT));
		}
		match v {
			Value::None => self.none(),
			Value::Null => self.put_u8(NULL),
			Value::Bool(b) => self.bool(*b),
			Value::Number(n) => self.number(n),
			Value::String(s) => self.string(s.as_str()),
			Value::Duration(d) => self.duration(d.0),
			Value::Datetime(d) => self.datetime(&d.0),
			Value::Uuid(u) => self.uuid(&u.0),
			Value::Array(a) => self.values_at(&a.0, depth)?,
			Value::Set(s) => self.items(SET, s.0.iter().rev(), s.0.len(), depth)?,
			Value::Object(o) => {
				self.object(o.0.iter().rev().map(|(k, v)| (k.as_str(), v)), o.0.len(), depth)?
			}
			Value::Geometry(g) => {
				geometry::encode(self, g, depth)?;
				self.put_u8(GEOMETRY);
			}
			Value::Bytes(b) => self.bytes(&b.0),
			Value::Table(t) => {
				self.put(t.as_str().as_bytes());
				self.put_u8(TABLE);
			}
			Value::RecordId(rid) => self.record_id_at(rid, depth)?,
			Value::File(f) => {
				self.put(f.key.as_bytes());
				self.put_clen_bytes(f.bucket.as_bytes())?;
				self.put_u8(FILE);
			}
			Value::Regex(r) => {
				self.put(r.0.as_str().as_bytes());
				self.put_u8(REGEX);
			}
			Value::Range(r) => {
				self.end_bound(&r.end, |e, v| e.value_at(v, depth + 1))?;
				self.start_bound(&r.start, |e, v| e.value_at(v, depth + 1))?;
				self.put_u8(RANGE);
			}
			Value::Closure(_) => return Err(Error::Closure),
		}
		Ok(())
	}

	/// Prepends the integers `v` as `W`-byte little-endian elements, last element first in the
	/// buffer. Every element of `v` is an integer that fits `W` bytes, which the caller has
	/// established with [`narrowest_int_array_width`].
	#[inline]
	fn put_ints<const W: usize>(&mut self, v: &[Value]) {
		let (out, _) = self.front(v.len() * W).as_chunks_mut::<W>();
		debug_assert_eq!(out.len(), v.len());
		for (slot, e) in out.iter_mut().zip(v) {
			if let Value::Number(Number::Int(i)) = e
				&& let Some(b) = i.to_le_bytes().first_chunk::<W>()
			{
				*slot = *b;
			} else {
				debug_assert!(false, "an element that is not an integer of the array's width");
			}
		}
	}

	fn values_at(&mut self, v: &[Value], depth: usize) -> Result<(), Error> {
		if let Some(w) = narrowest_int_array_width(v) {
			match w {
				1 => self.put_ints::<1>(v),
				2 => self.put_ints::<2>(v),
				4 => self.put_ints::<4>(v),
				_ => self.put_ints::<8>(v),
			}
			self.put_u8(I8_ARRAY + w.trailing_zeros() as u8);
			return Ok(());
		}
		if !v.is_empty() && v.iter().all(|v| matches!(v, Value::Number(Number::Float(_)))) {
			let floats = v.iter().map(|v| match v {
				Value::Number(Number::Float(f)) => *f,
				_ => unreachable!("every element is a float"),
			});
			if floats.clone().all(|f| (f as f32) as f64 == f && !f.is_nan()) {
				let (out, _) = self.front(v.len() * 4).as_chunks_mut::<4>();
				for (slot, f) in out.iter_mut().zip(floats) {
					*slot = (f as f32).to_le_bytes();
				}
				self.put_u8(F32_ARRAY);
			} else {
				let (out, _) = self.front(v.len() * 8).as_chunks_mut::<8>();
				for (slot, f) in out.iter_mut().zip(floats) {
					*slot = f.to_le_bytes();
				}
				self.put_u8(F64_ARRAY);
			}
			return Ok(());
		}
		self.items(ARRAY, v.iter().rev(), v.len(), depth)
	}

	fn key(&mut self, k: &RecordIdKey, depth: usize) -> Result<(), Error> {
		if depth > DEPTH_LIMIT {
			return Err(Error::TooDeep(DEPTH_LIMIT));
		}
		match k {
			RecordIdKey::Number(n) => self.int(*n),
			RecordIdKey::String(s) => {
				self.put(s.as_str().as_bytes());
				self.put_u8(STRING);
			}
			RecordIdKey::Uuid(u) => {
				self.put(u.0.as_bytes());
				self.put_u8(UUID);
			}
			RecordIdKey::Array(a) => self.values_at(&a.0, depth)?,
			RecordIdKey::Object(o) => {
				self.object(o.0.iter().rev().map(|(k, v)| (k.as_str(), v)), o.0.len(), depth)?
			}
			RecordIdKey::Range(r) => {
				self.end_bound(&r.end, |e, k| e.key(k, depth + 1))?;
				self.start_bound(&r.start, |e, k| e.key(k, depth + 1))?;
				self.put_u8(RANGE);
			}
		}
		Ok(())
	}

	/// A start bound: its kind, then for a bound its `clen`-prefixed value.
	fn start_bound<T>(
		&mut self,
		b: &Bound<T>,
		f: impl FnOnce(&mut Self, &T) -> Result<(), Error>,
	) -> Result<(), Error> {
		match b {
			Bound::Unbounded => self.put_u8(UNBOUNDED),
			Bound::Included(v) | Bound::Excluded(v) => {
				let before = self.written();
				f(self, v)?;
				self.put_clen(self.written() - before)?;
				self.put_u8(if matches!(b, Bound::Included(_)) {
					INCLUDED
				} else {
					EXCLUDED
				});
			}
		}
		Ok(())
	}

	/// An end bound: its kind, then for a bound its value, running to the end.
	fn end_bound<T>(
		&mut self,
		b: &Bound<T>,
		f: impl FnOnce(&mut Self, &T) -> Result<(), Error>,
	) -> Result<(), Error> {
		match b {
			Bound::Unbounded => self.put_u8(UNBOUNDED),
			Bound::Included(v) | Bound::Excluded(v) => {
				f(self, v)?;
				self.put_u8(if matches!(b, Bound::Included(_)) {
					INCLUDED
				} else {
					EXCLUDED
				});
			}
		}
		Ok(())
	}
}

/// The element width in bytes (1, 2, 4 or 8) of the typed integer array that holds `v`: the
/// narrowest that holds every element. `None` when `v` is empty or any element is not an
/// integer, which leaves `v` a generic array. Stops at the first element that is not an integer.
#[inline]
fn narrowest_int_array_width(v: &[Value]) -> Option<usize> {
	let mut w = 1;
	for e in v {
		match e {
			Value::Number(Number::Int(i)) => w = w.max(int_width(*i)),
			_ => return None,
		}
	}
	// An empty array has no width: the first element decides whether the form applies at all.
	(!v.is_empty()).then(|| w.next_power_of_two())
}
