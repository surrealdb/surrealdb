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

use std::time::Duration;

use surrealdb_types::{Array, Error, Value};

use crate::{QueryResult, QueryType};

/// An encoding the executor can write result rows in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RowFormat {
	/// Each row is one CBOR data item; rows are concatenated.
	Cbor,
	/// Each row is one JSON text; rows are joined by `,`.
	Json,
}

/// Decodes `count` rows encoded in `format` back into public values.
pub type RowDecoder = fn(RowFormat, &[u8], usize) -> Result<Vec<Value>, Error>;

/// Rows encoded in one [`RowFormat`], back to back, with where each ends.
///
/// `ends[i]` is the offset just past row `i`. In JSON the `,` that separates
/// two rows follows a row's end, so row `i + 1` starts one byte past
/// `ends[i]`; in CBOR it starts at `ends[i]`.
#[derive(Clone)]
pub struct EncodedRows {
	format: RowFormat,
	bytes: Vec<u8>,
	ends: Vec<u32>,
	decode: RowDecoder,
}

impl std::fmt::Debug for EncodedRows {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("EncodedRows")
			.field("format", &self.format)
			.field("count", &self.ends.len())
			.field("bytes", &self.bytes.len())
			.finish()
	}
}

impl PartialEq for EncodedRows {
	/// Rows are equal when they are the same bytes in the same format; the
	/// decoder is not compared.
	fn eq(&self, other: &Self) -> bool {
		self.format == other.format && self.ends == other.ends && self.bytes == other.bytes
	}
}

impl EncodedRows {
	/// The rows in `bytes`, row `i` ending at `ends[i]`, decodable by
	/// `decode`. `ends` ascends and its last entry is `bytes.len()`.
	pub fn new(format: RowFormat, bytes: Vec<u8>, ends: Vec<u32>, decode: RowDecoder) -> Self {
		debug_assert!(ends.last().is_none_or(|end| *end as usize == bytes.len()));
		Self {
			format,
			bytes,
			ends,
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

	/// The rows' encoded bytes: concatenated CBOR items, or `,`-joined JSON
	/// texts.
	pub fn bytes(&self) -> &[u8] {
		&self.bytes
	}

	/// Where row `at` starts.
	fn start(&self, at: usize) -> usize {
		match at {
			0 => 0,
			_ if self.format == RowFormat::Json => self.ends[at - 1] as usize + 1,
			_ => self.ends[at - 1] as usize,
		}
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
		let base = u32::try_from(self.bytes.len()).map_err(|_| too_large())?;
		let shift = |end: u32| base as u64 + end as u64 - start as u64;
		if shift(other.ends[to - 1]) > u32::MAX as u64 {
			return Err(too_large());
		}
		self.bytes.extend_from_slice(&other.bytes[start..other.ends[to - 1] as usize]);
		self.ends.extend(other.ends[from..to].iter().map(|end| shift(*end) as u32));
		Ok(())
	}

	/// The rows as public values.
	pub fn to_values(&self) -> Result<Vec<Value>, Error> {
		(self.decode)(self.format, &self.bytes, self.ends.len())
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
