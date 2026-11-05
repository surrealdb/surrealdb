//! Query results whose rows are already encoded for the wire.
//!
//! A transport that answers in CBOR or JSON can ask the executor for rows in
//! that encoding ([`RowFormat`]), so the server never builds the public
//! [`Value`] of a row: the executor writes each row straight into a buffer and
//! the transport splices the buffers into its response. The bytes of every row
//! are exactly what the transport's encoder writes for the public value the
//! row converts to, so the response is byte-identical to one built from
//! values.
//!
//! A caller that needs owned values after all — a format that has no encoded
//! form, or code that inspects a result — decodes them with
//! [`EncodedRows::to_values`], through the decoder the producer attached.

use std::sync::Arc;
use std::time::Duration;

use surrealdb_types::shaped::Shape;
use surrealdb_types::{Array, Error, Value};

use crate::{QueryResult, QueryType};

/// An encoding the executor can write result rows in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RowFormat {
	/// Each row is one CBOR data item; rows are concatenated.
	Cbor,
	/// Each row is one CBOR v2 data item; rows are concatenated.
	Cbor2,
	/// Each row is one JSON text; rows are joined by `,`.
	Json,
	/// Each row is one shaped row (`surrealdb_types::shaped`) of the shape the rows name for it;
	/// rows are concatenated. Everything around the rows is CBOR v2.
	Shaped,
}

impl RowFormat {
	/// The format everything but the rows is written in: CBOR v2 for a shaped response.
	pub fn envelope(self) -> RowFormat {
		match self {
			RowFormat::Shaped => RowFormat::Cbor2,
			RowFormat::Cbor => RowFormat::Cbor,
			RowFormat::Cbor2 => RowFormat::Cbor2,
			RowFormat::Json => RowFormat::Json,
		}
	}
}

/// Decodes encoded rows back into public values.
pub type RowDecoder = fn(&EncodedRows) -> Result<Vec<Value>, Error>;

/// Rows encoded in one [`RowFormat`], back to back, with where each ends.
///
/// `ends[i]` is the offset just past row `i`. In JSON the `,` that separates
/// two rows follows a row's end, so row `i + 1` starts one byte past
/// `ends[i]`; in CBOR and shaped rows it starts at `ends[i]`.
///
/// Shaped rows also say which shape each row was written in: row `i` is in
/// `shapes[row_shapes[i]]`. Rows of other formats have neither.
#[derive(Clone)]
pub struct EncodedRows {
	format: RowFormat,
	/// `head` bytes of headroom, then the rows.
	bytes: Vec<u8>,
	/// How many bytes in front of the rows are free, for what is written ahead of them
	/// ([`Self::wrap`]).
	head: usize,
	ends: Vec<u32>,
	/// For shaped rows, the shapes they are written in; boxed, so rows of
	/// other formats carry one pointer for it.
	shaped: Option<Box<ShapedRows>>,
	decode: RowDecoder,
}

/// The shapes of shaped rows: row `i` is in `shapes[row_shapes[i]]`.
#[derive(Clone, Default)]
struct ShapedRows {
	shapes: Vec<Arc<Shape>>,
	row_shapes: Vec<u16>,
}

impl std::fmt::Debug for EncodedRows {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("EncodedRows")
			.field("format", &self.format)
			.field("count", &self.ends.len())
			.field("bytes", &self.bytes().len())
			.finish()
	}
}

impl PartialEq for EncodedRows {
	/// Rows are equal when they are the same bytes in the same format; the
	/// decoder is not compared.
	fn eq(&self, other: &Self) -> bool {
		self.format == other.format
			&& self.ends == other.ends
			&& self.bytes() == other.bytes()
			&& self.row_shapes().len() == other.row_shapes().len()
			&& self
				.row_shapes()
				.iter()
				.zip(other.row_shapes())
				.all(|(a, b)| self.shapes()[*a as usize] == other.shapes()[*b as usize])
	}
}

impl EncodedRows {
	/// The rows in `bytes`, row `i` ending at `ends[i]`, decodable by
	/// `decode`. `ends` ascends and its last entry is `bytes.len()`.
	pub fn new(format: RowFormat, bytes: Vec<u8>, ends: Vec<u32>, decode: RowDecoder) -> Self {
		Self::with_headroom(format, bytes, 0, ends, decode)
	}

	/// The rows in `bytes` past its first `head` bytes, which are free for what is written ahead
	/// of the rows ([`Self::wrap`]); row `i` ends `ends[i]` bytes past the first row's start.
	pub fn with_headroom(
		format: RowFormat,
		bytes: Vec<u8>,
		head: usize,
		ends: Vec<u32>,
		decode: RowDecoder,
	) -> Self {
		debug_assert!(head <= bytes.len());
		debug_assert!(ends.last().is_none_or(|end| *end as usize == bytes.len() - head));
		debug_assert!(format != RowFormat::Shaped || ends.is_empty(), "shaped rows name shapes");
		Self {
			format,
			bytes,
			head,
			ends,
			shaped: None,
			decode,
		}
	}

	/// The rows' bytes with `prefix` written ahead of them and `suffix` after them, and where
	/// in the buffer they start: the rows are not copied. `Err` gives the rows back unchanged
	/// where their headroom is shorter than `prefix`.
	pub fn wrap(mut self, prefix: &[u8], suffix: &[u8]) -> Result<(Vec<u8>, usize), Self> {
		let Some(start) = self.head.checked_sub(prefix.len()) else {
			return Err(self);
		};
		self.bytes[start..self.head].copy_from_slice(prefix);
		self.bytes.extend_from_slice(suffix);
		Ok((self.bytes, start))
	}

	/// Shaped rows in `bytes`, row `i` ending at `ends[i]` and written in
	/// `shapes[row_shapes[i]]`.
	pub fn shaped(
		bytes: Vec<u8>,
		ends: Vec<u32>,
		shapes: Vec<Arc<Shape>>,
		row_shapes: Vec<u16>,
		decode: RowDecoder,
	) -> Self {
		debug_assert!(ends.last().is_none_or(|end| *end as usize == bytes.len()));
		debug_assert_eq!(ends.len(), row_shapes.len());
		debug_assert!(row_shapes.iter().all(|s| (*s as usize) < shapes.len()));
		Self {
			format: RowFormat::Shaped,
			bytes,
			head: 0,
			ends,
			shaped: Some(Box::new(ShapedRows {
				shapes,
				row_shapes,
			})),
			decode,
		}
	}

	/// No rows.
	pub fn empty(format: RowFormat, decode: RowDecoder) -> Self {
		Self::new(format, Vec::new(), Vec::new(), decode)
	}

	/// No rows, in the format and with the decoder of these.
	pub fn empty_like(&self) -> Self {
		Self::empty(self.format, self.decode)
	}

	pub fn format(&self) -> RowFormat {
		self.format
	}

	/// How many rows there are.
	pub fn len(&self) -> usize {
		self.ends.len()
	}

	pub fn is_empty(&self) -> bool {
		self.ends.is_empty()
	}

	/// The rows' encoded bytes: concatenated CBOR items or shaped rows, or
	/// `,`-joined JSON texts.
	pub fn bytes(&self) -> &[u8] {
		&self.bytes[self.head..]
	}

	/// Where each row ends.
	pub fn ends(&self) -> &[u32] {
		&self.ends
	}

	/// The shapes shaped rows are written in.
	pub fn shapes(&self) -> &[Arc<Shape>] {
		self.shaped.as_ref().map_or(&[], |s| &s.shapes)
	}

	/// For each shaped row, its shape's index in [`Self::shapes`].
	pub fn row_shapes(&self) -> &[u16] {
		self.shaped.as_ref().map_or(&[], |s| &s.row_shapes)
	}

	/// The shaped rows as runs of one shape each: the shape, the row count and
	/// the rows' bytes, in order. Empty for other formats.
	pub fn runs(&self) -> impl Iterator<Item = (&Arc<Shape>, u32, &[u8])> {
		let mut at = 0usize;
		let (shapes, row_shapes) = (self.shapes(), self.row_shapes());
		std::iter::from_fn(move || {
			let shape = *row_shapes.get(at)?;
			let from = at;
			while row_shapes.get(at) == Some(&shape) {
				at += 1;
			}
			let bytes = &self.bytes()[self.start(from)..self.ends[at - 1] as usize];
			Some((&shapes[shape as usize], (at - from) as u32, bytes))
		})
	}

	/// Where row `at` starts.
	fn start(&self, at: usize) -> usize {
		match at {
			0 => 0,
			_ if self.format == RowFormat::Json => self.ends[at - 1] as usize + 1,
			_ => self.ends[at - 1] as usize,
		}
	}

	/// `parts`' rows, in order, as one set of rows in `format`: the rows extending an empty set
	/// by each part answers, with the bytes copied at most once. Rows copied from several parts
	/// keep `head` bytes of headroom, and as many free behind them ([`Self::wrap`]); one part is
	/// taken as it is. Every part must be in `format`.
	pub fn concat(
		format: RowFormat,
		decode: RowDecoder,
		mut parts: Vec<EncodedRows>,
		head: usize,
	) -> Result<Self, Error> {
		// Shaped rows are extended part by part: extending names each shape once, in the order
		// the rows first use it, which is what the block announces.
		if format == RowFormat::Shaped {
			let mut rows = Self::empty(format, decode);
			for part in &parts {
				rows.extend(part)?;
			}
			return Ok(rows);
		}
		if parts.len() == 1 {
			let only = parts.pop().expect("one part");
			debug_assert_eq!(only.format, format, "rows of one statement share a format");
			return Ok(only);
		}
		let separators = match format {
			RowFormat::Json => parts.len(),
			RowFormat::Cbor | RowFormat::Cbor2 | RowFormat::Shaped => 0,
		};
		let len = parts.iter().map(|p| p.bytes().len()).sum::<usize>() + separators;
		// As much room again behind the rows, for what is written after them.
		let mut bytes = Vec::with_capacity(head + len + head);
		bytes.resize(head, 0);
		let ends = Vec::with_capacity(parts.iter().map(EncodedRows::len).sum());
		let mut rows = Self::with_headroom(format, bytes, head, ends, decode);
		for part in &parts {
			rows.extend(part)?;
		}
		Ok(rows)
	}

	/// Append `other`'s rows, which must be in the same format.
	pub fn extend(&mut self, other: &EncodedRows) -> Result<(), Error> {
		self.extend_range(other, 0, other.len())
	}

	/// Append rows `from..to` of `other`, which must be in the same format.
	pub fn extend_range(
		&mut self,
		other: &EncodedRows,
		from: usize,
		to: usize,
	) -> Result<(), Error> {
		debug_assert_eq!(self.format, other.format, "rows of one statement share a format");
		debug_assert!(from <= to && to <= other.len());
		if from == to {
			return Ok(());
		}
		let too_large = || Error::internal("Encoded rows larger than 4 GiB".to_owned());
		if self.format == RowFormat::Json && !self.ends.is_empty() {
			self.bytes.push(b',');
		}
		let start = other.start(from);
		// Each row of `other` moves by the same amount: from `start` to the
		// current end of these bytes.
		let base = u32::try_from(self.bytes().len()).map_err(|_| too_large())?;
		let shift = |end: u32| base as u64 + end as u64 - start as u64;
		if shift(other.ends[to - 1]) > u32::MAX as u64 {
			return Err(too_large());
		}
		if self.format == RowFormat::Shaped {
			self.extend_shapes(other, from, to)?;
		}
		self.bytes.extend_from_slice(&other.bytes()[start..other.ends[to - 1] as usize]);
		self.ends.extend(other.ends[from..to].iter().map(|end| shift(*end) as u32));
		Ok(())
	}

	/// Append the shapes of `other`'s rows `from..to` to [`Self::row_shapes`],
	/// adding each shape these rows do not name yet.
	fn extend_shapes(&mut self, other: &EncodedRows, from: usize, to: usize) -> Result<(), Error> {
		let (other_shapes, other_rows) = (other.shapes(), other.row_shapes());
		let mine = self.shaped.get_or_insert_with(Box::default);
		let mut map: Vec<Option<u16>> = vec![None; other_shapes.len()];
		let mut added: Vec<Arc<Shape>> = Vec::new();
		// Resolve every shape these rows name before touching `mine`, so a refusal leaves it as it
		// was.
		for &at in &other_rows[from..to] {
			let slot = &mut map[at as usize];
			if slot.is_some() {
				continue;
			}
			let shape = &other_shapes[at as usize];
			let find = |s: &Arc<Shape>| Arc::ptr_eq(s, shape) || **s == **shape;
			let index = match mine.shapes.iter().position(find) {
				Some(index) => index,
				None => match added.iter().position(find) {
					Some(index) => mine.shapes.len() + index,
					None => {
						added.push(Arc::clone(shape));
						mine.shapes.len() + added.len() - 1
					}
				},
			};
			*slot = Some(u16::try_from(index).map_err(|_| {
				Error::internal("More than 65536 shapes in one set of rows".to_owned())
			})?);
		}
		mine.shapes.extend(added);
		mine.row_shapes
			.extend(other_rows[from..to].iter().map(|&at| map[at as usize].unwrap_or_default()));
		Ok(())
	}

	/// The rows as public values.
	pub fn to_values(&self) -> Result<Vec<Value>, Error> {
		(self.decode)(self)
	}
}

/// What a statement answered, with its rows encoded.
#[derive(Debug, Clone)]
pub enum EncodedPayload {
	/// The statement's value is the list of these rows.
	Rows(EncodedRows),
	/// The statement's value is one value that is not a list of rows.
	Value(Value),
}

/// A statement's result with its rows encoded: the encoded counterpart of
/// [`QueryResult`].
#[derive(Debug, Clone)]
pub struct EncodedQueryResult {
	pub time: Duration,
	pub result: Result<EncodedPayload, Error>,
	pub query_type: QueryType,
}

impl EncodedQueryResult {
	/// How many bytes its encoded rows take; none for a result that is not rows.
	pub fn encoded_len(&self) -> usize {
		match &self.result {
			Ok(EncodedPayload::Rows(rows)) => rows.bytes().len(),
			_ => 0,
		}
	}

	/// The result with its rows decoded into public values.
	pub fn into_query_result(self) -> QueryResult {
		let result = match self.result {
			Ok(EncodedPayload::Rows(rows)) => {
				rows.to_values().map(|values| Value::Array(Array::from(values)))
			}
			Ok(EncodedPayload::Value(value)) => Ok(value),
			Err(error) => Err(error),
		};
		QueryResult {
			time: self.time,
			result,
			query_type: self.query_type,
		}
	}
}

impl serde::Serialize for EncodedQueryResult {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		self.clone().into_query_result().serialize(serializer)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn no_decode(_: &EncodedRows) -> Result<Vec<Value>, Error> {
		Ok(Vec::new())
	}

	/// Rows `items` of `format`, each one byte string, with `head` bytes of headroom.
	fn rows(format: RowFormat, items: &[&[u8]], head: usize) -> EncodedRows {
		let mut bytes = vec![0; head];
		let mut ends = Vec::new();
		for (i, item) in items.iter().enumerate() {
			if format == RowFormat::Json && i > 0 {
				bytes.push(b',');
			}
			bytes.extend_from_slice(item);
			ends.push((bytes.len() - head) as u32);
		}
		EncodedRows::with_headroom(format, bytes, head, ends, no_decode)
	}

	#[test]
	fn joined_parts_are_the_parts_extended_in_turn() {
		for format in [RowFormat::Cbor, RowFormat::Json] {
			let parts = vec![
				rows(format, &[b"1", b"22"], 4),
				rows(format, &[], 0),
				rows(format, &[b"333"], 0),
			];
			let mut extended = EncodedRows::empty(format, no_decode);
			for part in &parts {
				extended.extend(part).unwrap();
			}
			let joined = EncodedRows::concat(format, no_decode, parts, 8).unwrap();
			assert!(joined == extended, "{format:?}");
			assert_eq!(joined.bytes(), extended.bytes());
			assert_eq!(joined.ends(), extended.ends());
		}
	}

	#[test]
	fn one_part_is_taken_as_it_is() {
		let part = rows(RowFormat::Cbor, &[b"ab"], 3);
		let joined = EncodedRows::concat(RowFormat::Cbor, no_decode, vec![part], 8).unwrap();
		assert_eq!(joined.bytes(), b"ab");
		assert_eq!(
			joined.wrap(b"xyz", b"!").map(|(b, at)| b[at..].to_vec()).ok(),
			Some(b"xyzab!".to_vec())
		);
	}

	#[test]
	fn rows_are_wrapped_in_their_headroom_or_given_back() {
		let wrapped = rows(RowFormat::Cbor, &[b"1", b"2"], 4).wrap(b"<<", b">>").unwrap();
		assert_eq!(&wrapped.0[wrapped.1..], b"<<12>>");
		let refused = rows(RowFormat::Cbor, &[b"1", b"2"], 1).wrap(b"<<", b">>").unwrap_err();
		assert_eq!(refused.bytes(), b"12");
		assert_eq!(refused.ends(), &[1, 2]);
	}
}
