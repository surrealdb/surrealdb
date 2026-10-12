use std::cmp::Ordering;

use super::decode::{self, fixed, int_array_element_width};
use super::tag::*;
use super::{DEPTH_LIMIT, Error, read_int, read_width, split_clen, utf8};
use crate::val::Value;

/// The kind of a stored value, from its tag.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
	None,
	Null,
	Bool,
	Int,
	Float,
	Decimal,
	String,
	Duration,
	Datetime,
	Uuid,
	Bytes,
	Table,
	RecordId,
	File,
	Regex,
	Range,
	Geometry,
	Array,
	Set,
	Object,
	/// A tag this build does not know: a kind added after it.
	Unknown,
}

impl Kind {
	#[inline(always)]
	pub const fn of(tag: u8) -> Self {
		match tag {
			NONE => Self::None,
			NULL => Self::Null,
			FALSE | TRUE => Self::Bool,
			INT | 0x80..=0xFE => Self::Int,
			FLOAT => Self::Float,
			DECIMAL => Self::Decimal,
			STRING => Self::String,
			DURATION => Self::Duration,
			DATETIME => Self::Datetime,
			UUID => Self::Uuid,
			BYTES => Self::Bytes,
			TABLE => Self::Table,
			RECORD_ID => Self::RecordId,
			FILE => Self::File,
			REGEX => Self::Regex,
			RANGE => Self::Range,
			GEOMETRY => Self::Geometry,
			F64_ARRAY | F32_ARRAY | I8_ARRAY..=I64_ARRAY | 0x14..=0x16 => Self::Array,
			0x17..=0x19 => Self::Set,
			0x1A..=0x1C => Self::Object,
			_ => Self::Unknown,
		}
	}
}

/// A stored value read in place: its tag, its payload and the number of containers above it.
///
/// A value whose tag is the whole value — NONE, NULL, FALSE, TRUE and the small
/// ints — has an empty payload: [`new`](Self::new) refuses one that carries a
/// payload, as a whole decode does, so no accessor reads such bytes as a value.
///
/// An element of a typed float array has no tag of its own in the bytes; it is
/// read as a [`FLOAT`] whose payload is its 4 or 8 bytes. An element of a typed
/// integer array is likewise read as an [`INT`] whose payload is its 1, 2, 4 or
/// 8 bytes at the array's width. Those payloads are not the canonical encoding
/// of the element, so `==` and [`write_to`](Self::write_to) compare and copy
/// bytes, not values: compare values through the typed accessors or
/// [`decode`](Self::decode).
///
/// The depth is the nesting the value sits at, counted as a whole decode counts it, so a
/// lookup refuses a value nested past the grammar's depth limit exactly where a decode of the
/// whole value refuses it.
#[derive(Clone, Copy, Debug)]
pub struct ValueRef<'a> {
	tag: u8,
	payload: &'a [u8],
	depth: usize,
}

/// Equal when their tags and payloads are: the depth is where a value sits, not part of its
/// bytes.
impl PartialEq for ValueRef<'_> {
	#[inline(always)]
	fn eq(&self, other: &Self) -> bool {
		self.tag == other.tag && self.payload == other.payload
	}
}

impl Eq for ValueRef<'_> {}

impl<'a> ValueRef<'a> {
	/// The value whose stored encoding is exactly `bytes`, at depth 0.
	///
	/// Refuses a value whose tag is the whole value but which carries a
	/// payload, as a whole decode of it does.
	#[inline(always)]
	pub fn new(bytes: &'a [u8]) -> Result<Self, Error> {
		Self::new_nested(bytes, 0)
	}

	/// The value whose stored encoding is exactly `bytes`, nested `depth` containers deep, so
	/// the grammar's depth limit counts the levels above it.
	///
	/// Refuses what [`new`](Self::new) refuses, and a value nested past the depth limit, as a
	/// decode of the whole value does.
	#[inline(always)]
	pub fn new_nested(bytes: &'a [u8], depth: usize) -> Result<Self, Error> {
		let (&tag, payload) = bytes.split_first().ok_or(Error::Truncated)?;
		if depth > DEPTH_LIMIT {
			return Err(Error::TooDeep(DEPTH_LIMIT));
		}
		if matches!(tag, NONE..=TRUE | SMALL_INT..=0xFE) && !payload.is_empty() {
			return Err(Error::Malformed("a payload on a value that has none"));
		}
		Ok(Self {
			tag,
			payload,
			depth,
		})
	}

	#[inline(always)]
	pub const fn tag(&self) -> u8 {
		self.tag
	}

	/// The bytes after the tag.
	#[inline(always)]
	pub const fn payload(&self) -> &'a [u8] {
		self.payload
	}

	#[inline(always)]
	pub const fn kind(&self) -> Kind {
		Kind::of(self.tag)
	}

	/// Appends this value's stored encoding to `out`.
	#[inline]
	pub fn write_to(&self, out: &mut Vec<u8>) {
		out.push(self.tag);
		out.extend_from_slice(self.payload);
	}

	/// Decodes this value, at its depth.
	#[inline]
	pub fn decode(&self) -> Result<Value, Error> {
		decode::parts::<decode::Build>(self.tag, self.payload, self.depth)
	}

	#[inline(always)]
	pub fn as_bool(&self) -> Option<bool> {
		match self.tag {
			FALSE => Some(false),
			TRUE => Some(true),
			_ => None,
		}
	}

	/// The integer, if this is one.
	#[inline(always)]
	pub fn as_int(&self) -> Result<Option<i64>, Error> {
		match self.tag {
			0x80..=0xFE => Ok(Some((self.tag - SMALL_INT) as i64)),
			INT => read_int(self.payload).map(Some),
			_ => Ok(None),
		}
	}

	/// The float, if this is one.
	#[inline(always)]
	pub fn as_float(&self) -> Result<Option<f64>, Error> {
		match self.tag {
			FLOAT => decode::float(self.payload).map(Some),
			_ => Ok(None),
		}
	}

	/// The decimal, if this is one.
	#[inline]
	pub fn as_decimal(&self) -> Result<Option<rust_decimal::Decimal>, Error> {
		match self.tag {
			DECIMAL => decode::decimal(self.payload).map(Some),
			_ => Ok(None),
		}
	}

	/// The string, if this is one.
	#[inline(always)]
	pub fn as_str(&self) -> Result<Option<&'a str>, Error> {
		match self.tag {
			STRING => utf8(self.payload).map(Some),
			_ => Ok(None),
		}
	}

	/// The bytes, if this is a byte string.
	#[inline(always)]
	pub fn as_bytes(&self) -> Option<&'a [u8]> {
		match self.tag {
			BYTES => Some(self.payload),
			_ => None,
		}
	}

	#[inline]
	pub fn as_duration(&self) -> Result<Option<std::time::Duration>, Error> {
		match self.tag {
			DURATION => decode::duration(self.payload).map(Some),
			_ => Ok(None),
		}
	}

	#[inline]
	pub fn as_datetime(&self) -> Result<Option<chrono::DateTime<chrono::Utc>>, Error> {
		match self.tag {
			DATETIME => decode::datetime(self.payload).map(Some),
			_ => Ok(None),
		}
	}

	#[inline]
	pub fn as_uuid(&self) -> Result<Option<uuid::Uuid>, Error> {
		match self.tag {
			UUID => Ok(Some(uuid::Uuid::from_bytes(fixed(self.payload)?))),
			_ => Ok(None),
		}
	}

	/// The table name, if this is a table.
	#[inline]
	pub fn as_table(&self) -> Result<Option<&'a str>, Error> {
		match self.tag {
			TABLE => utf8(self.payload).map(Some),
			_ => Ok(None),
		}
	}

	/// A record id's table name and key, if this is one.
	#[inline]
	pub fn as_record_id(&self) -> Result<Option<(&'a str, ValueRef<'a>)>, Error> {
		match self.tag {
			RECORD_ID => {
				let (table, key) = split_clen(self.payload)?;
				Ok(Some((utf8(table)?, ValueRef::new_nested(key, self.depth + 1)?)))
			}
			_ => Ok(None),
		}
	}

	/// The object, if this is one.
	#[inline(always)]
	pub fn as_object(&self) -> Result<Option<ObjectRef<'a>>, Error> {
		match self.tag {
			0x1A..=0x1C => Ok(Some(ObjectRef {
				c: Container::split(self.payload, width(self.tag, OBJECT))?,
				depth: self.depth,
			})),
			_ => Ok(None),
		}
	}

	/// The object, if this is one, with its key order confirmed so that its
	/// fields can be looked up: see [`ObjectRef::ordered`].
	#[inline]
	pub fn as_ordered_object(&self) -> Result<Option<OrderedObjectRef<'a>>, Error> {
		self.as_object()?.map(ObjectRef::ordered).transpose()
	}

	/// The array, if this is one.
	#[inline(always)]
	pub fn as_array(&self) -> Result<Option<ArrayRef<'a>>, Error> {
		ArrayRef::of(self.tag, self.payload, ARRAY, self.depth)
	}

	/// The set, if this is one.
	#[inline(always)]
	pub fn as_set(&self) -> Result<Option<ArrayRef<'a>>, Error> {
		ArrayRef::of(self.tag, self.payload, SET, self.depth)
	}
}

/// A container's offset width, entry count, offset table and entries.
#[derive(Clone, Copy, Debug)]
pub(super) struct Container<'a> {
	pub w: usize,
	pub n: usize,
	pub table: &'a [u8],
	pub data: &'a [u8],
}

impl<'a> Container<'a> {
	#[inline(always)]
	pub fn split(p: &'a [u8], w: usize) -> Result<Self, Error> {
		let n = read_width(p, 0, w).ok_or(Error::Truncated)?;
		let end = n
			.checked_mul(w)
			.and_then(|t| t.checked_add(w))
			.ok_or(Error::Malformed("an impossible entry count"))?;
		let table = p.get(w..end).ok_or(Error::Truncated)?;
		let data = p.get(end..).ok_or(Error::Truncated)?;
		if n == 0 && !data.is_empty() {
			return Err(Error::Malformed("bytes after the end of an empty container"));
		}
		Ok(Self {
			w,
			n,
			table,
			data,
		})
	}

	#[inline(always)]
	fn offset(&self, i: usize) -> Result<usize, Error> {
		let at = i.checked_mul(self.w).ok_or(Error::Malformed("an entry outside its container"))?;
		read_width(self.table, at, self.w).ok_or(Error::Truncated)
	}

	/// Entry `i`'s bytes, for `i < n`.
	#[inline(always)]
	pub fn entry(&self, i: usize) -> Result<&'a [u8], Error> {
		if i >= self.n {
			return Err(Error::Malformed("an entry outside its container"));
		}
		let start = self.offset(i)?;
		let end = if i + 1 < self.n {
			self.offset(i + 1)?
		} else {
			self.data.len()
		};
		if i == 0 && start != 0 {
			return Err(Error::Malformed("bytes before a container's first entry"));
		}
		match self.data.get(start..end) {
			Some(e) if !e.is_empty() => Ok(e),
			_ => Err(Error::Malformed("an entry outside its container")),
		}
	}

	/// Entry `i`'s key and value bytes, for an object's container. The key is
	/// read within its entry, so a length prefix that runs past the entry is
	/// refused, and a lookup reads only the entries it compares.
	#[inline(always)]
	fn split_entry(&self, i: usize) -> Result<(&'a [u8], &'a [u8]), Error> {
		split_clen(self.entry(i)?)
	}
}

/// A stored object read in place.
#[derive(Clone, Copy, Debug)]
pub struct ObjectRef<'a> {
	c: Container<'a>,
	/// The depth of the object itself; its entries sit one level below.
	depth: usize,
}

impl<'a> ObjectRef<'a> {
	#[inline(always)]
	pub fn len(&self) -> usize {
		self.c.n
	}

	#[inline(always)]
	pub fn is_empty(&self) -> bool {
		self.c.n == 0
	}

	/// Entry `i`: its key and value, for `i < len()`.
	#[inline]
	pub fn entry(&self, i: usize) -> Result<(&'a str, ValueRef<'a>), Error> {
		let (k, v) = split_clen(self.c.entry(i)?)?;
		Ok((utf8(k)?, ValueRef::new_nested(v, self.depth + 1)?))
	}

	/// The value of entry `i`, for `i < len()`.
	#[inline(always)]
	pub fn value_at(&self, i: usize) -> Result<ValueRef<'a>, Error> {
		ValueRef::new_nested(split_clen(self.c.entry(i)?)?.1, self.depth + 1)
	}

	/// This object with its keys confirmed: refuses an object whose keys do
	/// not strictly ascend or are not UTF-8, as a whole decode of it does.
	/// Reads the keys alone.
	///
	/// Only an [`OrderedObjectRef`] looks a field up: the lookup searches the
	/// keys by halves, so a key it misses is proved absent only over keys that
	/// ascend.
	pub fn ordered(self) -> Result<OrderedObjectRef<'a>, Error> {
		let mut prev: Option<&[u8]> = None;
		for i in 0..self.c.n {
			let (k, _) = self.c.split_entry(i)?;
			if prev.is_some_and(|prev| prev >= k) {
				return Err(Error::Malformed("object keys out of order"));
			}
			utf8(k)?;
			prev = Some(k);
		}
		Ok(OrderedObjectRef {
			obj: self,
		})
	}

	/// The entries in key order.
	#[inline]
	pub fn iter(&self) -> ObjectIter<'a> {
		ObjectIter {
			obj: *self,
			i: 0,
		}
	}
}

/// A stored object whose keys are confirmed to strictly ascend, read in place:
/// the only form of an object whose fields are looked up by key. Made by
/// [`ObjectRef::ordered`]. A nested object reached through a lookup is an
/// [`ObjectRef`] again, and is confirmed in its turn before it is looked into.
#[derive(Clone, Copy, Debug)]
pub struct OrderedObjectRef<'a> {
	obj: ObjectRef<'a>,
}

impl<'a> std::ops::Deref for OrderedObjectRef<'a> {
	type Target = ObjectRef<'a>;

	#[inline(always)]
	fn deref(&self) -> &ObjectRef<'a> {
		&self.obj
	}
}

impl<'a> OrderedObjectRef<'a> {
	/// The value of field `key`.
	#[inline]
	pub fn get(&self, key: &str) -> Result<Option<ValueRef<'a>>, Error> {
		let mut hint = u32::MAX;
		self.get_hinted(key, &mut hint)
	}

	/// The value of field `key`, trying entry `*hint` before searching.
	///
	/// `*hint` is set to the entry the key was found at, and left alone when
	/// the key is absent. Rows of one table usually hold a field at the same
	/// position, so a hint carried from one row to the next usually finds it
	/// with a single comparison.
	#[inline]
	pub fn get_hinted(&self, key: &str, hint: &mut u32) -> Result<Option<ValueRef<'a>>, Error> {
		match self.find(key.as_bytes(), hint)? {
			Some(value) => ValueRef::new_nested(value, self.obj.depth + 1).map(Some),
			None => Ok(None),
		}
	}

	/// The value bytes of the entry whose key is `needle`. Each entry the
	/// search compares is read once, and the match hands back the value it
	/// already split off.
	fn find(&self, needle: &[u8], hint: &mut u32) -> Result<Option<&'a [u8]>, Error> {
		let n = self.obj.c.n;
		let (mut lo, mut hi) = (0, n);
		let h = *hint as usize;
		if h < n {
			let (k, v) = self.obj.c.split_entry(h)?;
			match k.cmp(needle) {
				Ordering::Equal => return Ok(Some(v)),
				Ordering::Less => {
					lo = h + 1;
					// The next entry is the likeliest place for it.
					if lo < n {
						let (k, v) = self.obj.c.split_entry(lo)?;
						match k.cmp(needle) {
							Ordering::Equal => {
								*hint = lo as u32;
								return Ok(Some(v));
							}
							Ordering::Less => lo += 1,
							Ordering::Greater => return Ok(None),
						}
					}
				}
				Ordering::Greater => hi = h,
			}
		}
		while lo < hi {
			let mid = lo + (hi - lo) / 2;
			let (k, v) = self.obj.c.split_entry(mid)?;
			match k.cmp(needle) {
				Ordering::Less => lo = mid + 1,
				Ordering::Greater => hi = mid,
				Ordering::Equal => {
					*hint = mid as u32;
					return Ok(Some(v));
				}
			}
		}
		Ok(None)
	}
}

/// The entries of an [`ObjectRef`], in key order.
pub struct ObjectIter<'a> {
	obj: ObjectRef<'a>,
	i: usize,
}

impl<'a> Iterator for ObjectIter<'a> {
	type Item = Result<(&'a str, ValueRef<'a>), Error>;

	#[inline]
	fn next(&mut self) -> Option<Self::Item> {
		if self.i >= self.obj.len() {
			return None;
		}
		let r = self.obj.entry(self.i);
		self.i += 1;
		Some(r)
	}

	#[inline]
	fn size_hint(&self) -> (usize, Option<usize>) {
		let n = self.obj.len() - self.i;
		(n, Some(n))
	}
}

#[derive(Clone, Copy, Debug)]
enum ArrayRepr<'a> {
	Values(Container<'a>),
	F64(&'a [u8]),
	F32(&'a [u8]),
	/// An integer array: the payload and its element width in bytes.
	Ints {
		p: &'a [u8],
		w: usize,
	},
}

/// A stored array or set read in place.
#[derive(Clone, Copy, Debug)]
pub struct ArrayRef<'a> {
	repr: ArrayRepr<'a>,
	/// The depth of the array itself; its elements sit one level below.
	depth: usize,
}

impl<'a> ArrayRef<'a> {
	#[inline(always)]
	fn of(tag: u8, payload: &'a [u8], base: u8, depth: usize) -> Result<Option<Self>, Error> {
		let repr = match tag {
			t if (base..base + 3).contains(&t) => {
				ArrayRepr::Values(Container::split(payload, width(t, base))?)
			}
			F64_ARRAY if base == ARRAY => {
				if payload.is_empty() || !payload.len().is_multiple_of(8) {
					return Err(Error::Malformed("a float array of the wrong width"));
				}
				ArrayRepr::F64(payload)
			}
			F32_ARRAY if base == ARRAY => {
				if payload.is_empty() || !payload.len().is_multiple_of(4) {
					return Err(Error::Malformed("a float array of the wrong width"));
				}
				ArrayRepr::F32(payload)
			}
			I8_ARRAY..=I64_ARRAY if base == ARRAY => ArrayRepr::Ints {
				p: payload,
				w: int_array_element_width(tag, payload)?,
			},
			_ => return Ok(None),
		};
		Ok(Some(Self {
			repr,
			depth,
		}))
	}

	#[inline(always)]
	pub fn len(&self) -> usize {
		match self.repr {
			ArrayRepr::Values(c) => c.n,
			ArrayRepr::F64(p) => p.len() / 8,
			ArrayRepr::F32(p) => p.len() / 4,
			ArrayRepr::Ints {
				p,
				w,
			} => p.len() / w,
		}
	}

	#[inline(always)]
	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}

	/// Element `i`, if there is one.
	#[inline]
	pub fn get(&self, i: usize) -> Result<Option<ValueRef<'a>>, Error> {
		if i >= self.len() {
			return Ok(None);
		}
		// An element of a typed array is a scalar that the whole decode builds without a depth
		// check, so it carries the array's own depth, which the value holding the array already
		// admitted. An element of a `Values` array is a value one level below the array.
		Ok(Some(match self.repr {
			ArrayRepr::Values(c) => ValueRef::new_nested(c.entry(i)?, self.depth + 1)?,
			ArrayRepr::F64(p) => ValueRef {
				tag: FLOAT,
				payload: p
					.as_chunks::<8>()
					.0
					.get(i)
					.ok_or(Error::Malformed("an element outside its array"))?,
				depth: self.depth,
			},
			ArrayRepr::F32(p) => ValueRef {
				tag: FLOAT,
				payload: p
					.as_chunks::<4>()
					.0
					.get(i)
					.ok_or(Error::Malformed("an element outside its array"))?,
				depth: self.depth,
			},
			ArrayRepr::Ints {
				p,
				w,
			} => {
				let start =
					i.checked_mul(w).ok_or(Error::Malformed("an element outside its array"))?;
				let end =
					start.checked_add(w).ok_or(Error::Malformed("an element outside its array"))?;
				ValueRef {
					tag: INT,
					payload: p
						.get(start..end)
						.ok_or(Error::Malformed("an element outside its array"))?,
					depth: self.depth,
				}
			}
		}))
	}

	/// The elements, if this is an array of integers held as a typed integer array: each as an
	/// `i64`. An untyped array of integers has no contiguous form and gives `None`.
	#[inline]
	pub fn ints(&self) -> Option<impl ExactSizeIterator<Item = i64> + 'a> {
		match self.repr {
			ArrayRepr::Ints {
				p,
				w,
			} => Some(match w {
				1 => IntIter::I8(p.as_chunks::<1>().0.iter()),
				2 => IntIter::I16(p.as_chunks::<2>().0.iter()),
				4 => IntIter::I32(p.as_chunks::<4>().0.iter()),
				_ => IntIter::I64(p.as_chunks::<8>().0.iter()),
			}),
			_ => None,
		}
	}

	/// The elements, if this is an array of floats: each as an `f64`.
	#[inline]
	pub fn floats(&self) -> Option<impl ExactSizeIterator<Item = f64> + 'a> {
		match self.repr {
			ArrayRepr::F64(p) => Some(FloatIter::F64(p.as_chunks::<8>().0.iter())),
			ArrayRepr::F32(p) => Some(FloatIter::F32(p.as_chunks::<4>().0.iter())),
			ArrayRepr::Values(_)
			| ArrayRepr::Ints {
				..
			} => None,
		}
	}

	/// The elements in order.
	#[inline]
	pub fn iter(&self) -> ArrayIter<'a> {
		ArrayIter {
			arr: *self,
			i: 0,
		}
	}
}

enum FloatIter<'a> {
	F64(std::slice::Iter<'a, [u8; 8]>),
	F32(std::slice::Iter<'a, [u8; 4]>),
}

impl Iterator for FloatIter<'_> {
	type Item = f64;

	#[inline]
	fn next(&mut self) -> Option<f64> {
		match self {
			Self::F64(i) => i.next().map(|b| f64::from_le_bytes(*b)),
			Self::F32(i) => i.next().map(|b| f32::from_le_bytes(*b) as f64),
		}
	}

	#[inline]
	fn size_hint(&self) -> (usize, Option<usize>) {
		match self {
			Self::F64(i) => i.size_hint(),
			Self::F32(i) => i.size_hint(),
		}
	}
}

impl ExactSizeIterator for FloatIter<'_> {}

enum IntIter<'a> {
	I8(std::slice::Iter<'a, [u8; 1]>),
	I16(std::slice::Iter<'a, [u8; 2]>),
	I32(std::slice::Iter<'a, [u8; 4]>),
	I64(std::slice::Iter<'a, [u8; 8]>),
}

impl Iterator for IntIter<'_> {
	type Item = i64;

	#[inline]
	fn next(&mut self) -> Option<i64> {
		match self {
			Self::I8(i) => i.next().map(|b| i8::from_le_bytes(*b) as i64),
			Self::I16(i) => i.next().map(|b| i16::from_le_bytes(*b) as i64),
			Self::I32(i) => i.next().map(|b| i32::from_le_bytes(*b) as i64),
			Self::I64(i) => i.next().map(|b| i64::from_le_bytes(*b)),
		}
	}

	#[inline]
	fn size_hint(&self) -> (usize, Option<usize>) {
		match self {
			Self::I8(i) => i.size_hint(),
			Self::I16(i) => i.size_hint(),
			Self::I32(i) => i.size_hint(),
			Self::I64(i) => i.size_hint(),
		}
	}
}

impl ExactSizeIterator for IntIter<'_> {}

/// The elements of an [`ArrayRef`], in order.
pub struct ArrayIter<'a> {
	arr: ArrayRef<'a>,
	i: usize,
}

impl<'a> Iterator for ArrayIter<'a> {
	type Item = Result<ValueRef<'a>, Error>;

	#[inline]
	fn next(&mut self) -> Option<Self::Item> {
		let r = self.arr.get(self.i).transpose()?;
		self.i += 1;
		Some(r)
	}

	#[inline]
	fn size_hint(&self) -> (usize, Option<usize>) {
		let n = self.arr.len().saturating_sub(self.i);
		(n, Some(n))
	}
}

#[cfg(test)]
mod depth_tests {
	use super::ValueRef;
	use crate::val::stored::tag::{OBJECT, SMALL_INT};
	use crate::val::stored::{DEPTH_LIMIT, Error, decode};

	/// The encoding of the object `{ k: inner }`, with a width-1 container.
	fn wrap_in_object(inner: &[u8]) -> Vec<u8> {
		[&[OBJECT, 0x01, 0x00, 0x01, b'k'][..], inner].concat()
	}

	/// The integer 7, nested `levels` objects deep: the only field `k` of each object holds the
	/// next level.
	fn nested_objects(levels: usize) -> Vec<u8> {
		let mut b = vec![SMALL_INT + 7];
		for _ in 0..levels {
			b = wrap_in_object(&b);
		}
		b
	}

	/// Reads `levels` fields `k` down from `bytes` by lookups, and returns the value reached.
	fn descend(bytes: &[u8], levels: usize) -> Result<ValueRef<'_>, Error> {
		let mut v = ValueRef::new(bytes)?;
		for _ in 0..levels {
			let obj = v.as_ordered_object()?.expect("an object");
			v = obj.get("k")?.expect("a field k");
		}
		Ok(v)
	}

	#[test]
	fn a_value_at_the_depth_limit_is_accepted_by_lookups_and_decode() {
		let bytes = nested_objects(DEPTH_LIMIT);
		assert!(decode(&bytes).is_ok());
		let leaf = descend(&bytes, DEPTH_LIMIT).unwrap();
		assert_eq!(leaf.as_int().unwrap(), Some(7));
		assert!(leaf.decode().is_ok());
	}

	#[test]
	fn a_value_past_the_depth_limit_is_refused_by_lookups_as_by_decode() {
		let bytes = nested_objects(DEPTH_LIMIT + 1);
		assert_eq!(decode(&bytes).err(), Some(Error::TooDeep(DEPTH_LIMIT)));
		let reached = descend(&bytes, DEPTH_LIMIT + 1).and_then(|v| v.decode());
		assert_eq!(reached.err(), Some(Error::TooDeep(DEPTH_LIMIT)));
	}

	#[test]
	fn new_nested_refuses_a_depth_past_the_limit() {
		assert!(ValueRef::new_nested(&[SMALL_INT], DEPTH_LIMIT).is_ok());
		assert_eq!(
			ValueRef::new_nested(&[SMALL_INT], DEPTH_LIMIT + 1).err(),
			Some(Error::TooDeep(DEPTH_LIMIT))
		);
	}
}
