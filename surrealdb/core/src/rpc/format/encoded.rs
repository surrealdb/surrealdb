//! Responses assembled from rows already encoded for the wire.
//!
//! A transport that answers in CBOR or JSON runs its query with rows encoded
//! in that format ([`RowFormat`]), collects the stream's items per statement
//! ([`QueryCollector`]) and writes the response around the encoded rows
//! ([`encode_results`], [`encode_response`]). Everything but the rows is
//! encoded by the same encoder a response built from values goes through, and
//! the parts are written in the order that encoder writes an object's keys
//! (the public object is ordered by key), so the bytes are identical to
//! `encode(response.into_value())`.

use surrealdb_rpc::{
	DbResponse, DbResult, EncodedPayload, EncodedQueryResult, EncodedRows, QueryResult,
	QueryStreamItem, RowFormat,
};
use surrealdb_types::shaped::ShapeScope;
#[cfg(feature = "shaped")]
use surrealdb_types::shaped::{PublicShaped, ShapedError, ValueShaper};
use surrealdb_types::{Error as TypesError, SurrealValue, Value};

/// Why shaped rows are refused by a build without shaped frames, which never offers them.
#[cfg(not(feature = "shaped"))]
const SHAPED_NOT_BUILT: &str = "this build does not serve shaped frames";

/// The recursion limit for decoding rows this server encoded: deep enough for
/// any value the storage depth guard admits, with its CBOR tags.
const DECODE_RECURSION_LIMIT: usize = 4 * crate::val::MAX_VALUE_DEPTH + 16;

/// Decode `rows` into public values: the
/// [`RowDecoder`](surrealdb_rpc::RowDecoder) the executor attaches to the rows
/// it encodes.
pub fn decode_rows(rows: &EncodedRows) -> Result<Vec<Value>, TypesError> {
	let (bytes, count) = (rows.bytes(), rows.len());
	let fail = |e: String| TypesError::internal(format!("Failed to decode encoded rows: {e}"));
	match rows.format() {
		#[cfg(not(feature = "shaped"))]
		RowFormat::Shaped => Err(fail(SHAPED_NOT_BUILT.to_owned())),
		#[cfg(feature = "shaped")]
		RowFormat::Shaped => {
			let mut values = Vec::with_capacity(count);
			let mut start = 0usize;
			for (end, shape) in rows.ends().iter().zip(rows.row_shapes()) {
				let shape = &rows.shapes()[*shape as usize];
				let row = &bytes[start..*end as usize];
				let (value, used) =
					surrealdb_types::shaped::decode_row(shape, row, &mut decode_dynamic)
						.map_err(|e| fail(e.to_string()))?;
				if used != row.len() {
					return Err(fail("bytes past a shaped row".to_owned()));
				}
				values.push(value);
				start = *end as usize;
			}
			Ok(values)
		}
		RowFormat::Cbor => {
			let mut reader = bytes;
			let mut values = Vec::with_capacity(count);
			for _ in 0..count {
				let item: ciborium::Value = ciborium::de::from_reader_with_recursion_limit(
					&mut reader,
					DECODE_RECURSION_LIMIT,
				)
				.map_err(|e| fail(e.to_string()))?;
				values.push(super::cbor::to_value(item).map_err(|e| fail(e.to_string()))?);
			}
			Ok(values)
		}
		RowFormat::Cbor2 => {
			let mut reader = bytes;
			let mut values = Vec::with_capacity(count);
			for _ in 0..count {
				let item: ciborium::Value = ciborium::de::from_reader_with_recursion_limit(
					&mut reader,
					DECODE_RECURSION_LIMIT,
				)
				.map_err(|e| fail(e.to_string()))?;
				values.push(super::cbor::to_value_v2(item).map_err(|e| fail(e.to_string()))?);
			}
			Ok(values)
		}
		RowFormat::Json => {
			let mut text = Vec::with_capacity(bytes.len() + 2);
			text.push(b'[');
			text.extend_from_slice(bytes);
			text.push(b']');
			let json: serde_json::Value =
				serde_json::from_slice(&text).map_err(|e| fail(e.to_string()))?;
			if let Value::Array(rows) = super::json::json_to_value(json) {
				Ok(rows.into_inner())
			} else {
				Err(fail("not a list".to_owned()))
			}
		}
	}
}

#[cfg(feature = "shaped")]
/// A dynamic slice of a shaped row: one CBOR item, read as the CBOR format
/// reads one.
pub fn decode_dynamic(bytes: &[u8]) -> Result<Value, String> {
	super::cbor::decode_exact_v2(bytes, DECODE_RECURSION_LIMIT).map_err(|e| e.to_string())
}

#[cfg(feature = "shaped")]
/// Write `value` as a dynamic slice of a shaped row: one CBOR item, as the
/// CBOR format encodes it.
pub fn encode_dynamic(value: &Value, out: &mut Vec<u8>) -> Result<(), ShapedError> {
	let bytes = super::cbor::encode_v2(value.clone()).map_err(|e| ShapedError(e.to_string()))?;
	out.extend_from_slice(&bytes);
	Ok(())
}

#[cfg(feature = "shaped")]
/// A shaped response or frame read back into the value its CBOR form would
/// decode to: the CBOR item with every block (tag
/// [`CBOR_TAG`](surrealdb_types::shaped::CBOR_TAG) around a byte string)
/// replaced by the array of its rows, read by `reader`, whose shape scope
/// carries across the responses of one stream or live query.
pub fn decode_shaped(
	bytes: &[u8],
	reader: &mut surrealdb_types::shaped::BlockReader,
) -> anyhow::Result<Value> {
	let item: ciborium::Value =
		ciborium::de::from_reader_with_recursion_limit(bytes, DECODE_RECURSION_LIMIT)
			.map_err(|e| anyhow::anyhow!(e.to_string()))?;
	shaped_item(item, reader)
}

#[cfg(feature = "shaped")]
fn shaped_item(
	item: ciborium::Value,
	reader: &mut surrealdb_types::shaped::BlockReader,
) -> anyhow::Result<Value> {
	match item {
		ciborium::Value::Tag(tag, inner) => {
			if tag == surrealdb_types::shaped::CBOR_TAG {
				let ciborium::Value::Bytes(contents) = *inner else {
					anyhow::bail!("a shaped block that is not a byte string");
				};
				let rows = reader
					.read_block(&contents, &mut decode_dynamic)
					.map_err(|e| anyhow::anyhow!(e))?;
				Ok(Value::Array(rows.into()))
			} else {
				super::cbor::to_value_v2(ciborium::Value::Tag(tag, inner))
			}
		}
		ciborium::Value::Map(entries) => {
			let mut object = std::collections::BTreeMap::new();
			for (key, value) in entries {
				let ciborium::Value::Text(key) = key else {
					anyhow::bail!("a map key that is not text");
				};
				object.insert(key, shaped_item(value, reader)?);
			}
			Ok(Value::Object(object.into()))
		}
		ciborium::Value::Array(items) => Ok(Value::Array(
			items
				.into_iter()
				.map(|i| shaped_item(i, reader))
				.collect::<anyhow::Result<Vec<_>>>()?
				.into(),
		)),
		ciborium::Value::Integer(v) => super::cbor::to_value_v2(ciborium::Value::Integer(v)),
		ciborium::Value::Bytes(v) => super::cbor::to_value_v2(ciborium::Value::Bytes(v)),
		ciborium::Value::Float(v) => super::cbor::to_value_v2(ciborium::Value::Float(v)),
		ciborium::Value::Text(v) => super::cbor::to_value_v2(ciborium::Value::Text(v)),
		ciborium::Value::Bool(v) => super::cbor::to_value_v2(ciborium::Value::Bool(v)),
		ciborium::Value::Null => super::cbor::to_value_v2(ciborium::Value::Null),
		_ => anyhow::bail!("Encountered an unknown CBOR data type"),
	}
}

/// Encode a public value in `format`, as the response encoder does.
fn encode_public(format: RowFormat, value: Value, out: &mut Vec<u8>) -> anyhow::Result<()> {
	match format.envelope() {
		RowFormat::Json => out.extend_from_slice(super::json::encode_str(value)?.as_bytes()),
		RowFormat::Cbor => out.extend_from_slice(&super::cbor::encode(value)?),
		RowFormat::Cbor2 => out.extend_from_slice(&super::cbor::encode_v2(value)?),
		RowFormat::Shaped => unreachable!("envelope is never shaped"),
	}
	Ok(())
}

#[cfg(feature = "shaped")]
/// Shaped rows of public values, each written by `shaper`.
pub fn shaped_rows_of_values(
	shaper: &mut ValueShaper,
	values: &[Value],
) -> anyhow::Result<EncodedRows> {
	let mut bytes = Vec::new();
	let mut ends = Vec::with_capacity(values.len());
	let mut shapes: Vec<std::sync::Arc<surrealdb_types::shaped::Shape>> = Vec::new();
	let mut row_shapes = Vec::with_capacity(values.len());
	for value in values {
		let shape = shaper
			.write(PublicShaped::new(value, encode_dynamic), &mut bytes)
			.map_err(|e| anyhow::anyhow!(e))?;
		let at = match shapes.iter().position(|s| std::sync::Arc::ptr_eq(s, &shape)) {
			Some(at) => at,
			None => {
				shapes.push(shape);
				shapes.len() - 1
			}
		};
		row_shapes.push(u16::try_from(at)?);
		ends.push(
			u32::try_from(bytes.len())
				.map_err(|_| anyhow::anyhow!("Encoded rows larger than 4 GiB"))?,
		);
	}
	Ok(EncodedRows::shaped(bytes, ends, shapes, row_shapes, decode_rows))
}

/// The rows a list of public values encodes to, for a statement whose rows
/// arrived as values.
fn rows_of_values(format: RowFormat, values: Vec<Value>) -> anyhow::Result<EncodedRows> {
	if format == RowFormat::Shaped {
		#[cfg(feature = "shaped")]
		return shaped_rows_of_values(&mut ValueShaper::new(), &values);
		#[cfg(not(feature = "shaped"))]
		anyhow::bail!(SHAPED_NOT_BUILT);
	}
	let mut bytes = Vec::new();
	let mut ends = Vec::with_capacity(values.len());
	for (i, value) in values.into_iter().enumerate() {
		if format == RowFormat::Json && i > 0 {
			bytes.push(b',');
		}
		encode_public(format, value, &mut bytes)?;
		ends.push(
			u32::try_from(bytes.len())
				.map_err(|_| anyhow::anyhow!("Encoded rows larger than 4 GiB"))?,
		);
	}
	Ok(EncodedRows::new(format, bytes, ends, decode_rows))
}

/// Collects a streaming execution's items into the results a buffered
/// execution answers, with every statement's rows encoded in one format.
///
/// A statement's rows are provisional until its `Finished` item: one that
/// carries an error drops them, as a buffered result holds the error alone.
#[derive(Debug)]
pub struct QueryCollector {
	format: RowFormat,
	/// For shaped rows that arrive as values, the shaper they are written by.
	#[cfg(feature = "shaped")]
	shaper: ValueShaper,
	/// Statements with a payload but no `Finished` yet, by index.
	pending: Vec<(usize, Pending)>,
	results: Vec<EncodedQueryResult>,
}

/// What a statement has answered before its `Finished` item.
#[derive(Debug)]
enum Pending {
	/// Its rows, in the parts they arrived in, joined once the statement finishes.
	Rows(Vec<EncodedRows>),
	/// One value that is not a list of rows.
	Value(Value),
}

impl QueryCollector {
	pub fn new(format: RowFormat) -> Self {
		Self {
			format,
			#[cfg(feature = "shaped")]
			shaper: ValueShaper::new(),
			pending: Vec::new(),
			results: Vec::new(),
		}
	}

	fn rows_for(&mut self, index: usize) -> &mut Vec<EncodedRows> {
		let at = match self.pending.iter().position(|(i, _)| *i == index) {
			// A statement contributes rows or one value, never both.
			Some(at) => {
				if matches!(self.pending[at].1, Pending::Value(_)) {
					self.pending[at].1 = Pending::Rows(Vec::new());
				}
				at
			}
			None => {
				self.pending.push((index, Pending::Rows(Vec::new())));
				self.pending.len() - 1
			}
		};
		match &mut self.pending[at].1 {
			Pending::Rows(rows) => rows,
			Pending::Value(_) => unreachable!("a statement's payload was just made rows"),
		}
	}

	/// Fold one item in.
	pub fn absorb(&mut self, item: QueryStreamItem) -> anyhow::Result<()> {
		match item {
			QueryStreamItem::Frame {
				index,
				rows,
			} => {
				let rows = match rows.format() == self.format {
					true => rows,
					false => self.rows_of(rows.to_values()?)?,
				};
				let parts = self.rows_for(index);
				if !rows.is_empty() {
					parts.push(rows);
				}
			}
			QueryStreamItem::Rows {
				index,
				values,
			} => {
				let rows = self.rows_of(values)?;
				let parts = self.rows_for(index);
				if !rows.is_empty() {
					parts.push(rows);
				}
			}
			QueryStreamItem::Value {
				index,
				value,
			} => {
				self.pending.retain(|(i, _)| *i != index);
				self.pending.push((index, Pending::Value(value)));
			}
			QueryStreamItem::Finished {
				index,
				time,
				query_type,
				error,
			} => {
				let payload = self
					.pending
					.iter()
					.position(|(i, _)| *i == index)
					.map(|at| self.pending.swap_remove(at).1);
				let result = match error {
					Some(error) => Err(error),
					None => Ok(match payload {
						Some(Pending::Value(value)) => EncodedPayload::Value(value),
						Some(Pending::Rows(parts)) => {
							let head = match self.format {
								RowFormat::Cbor | RowFormat::Cbor2 => ROW_HEADROOM,
								RowFormat::Json | RowFormat::Shaped => 0,
							};
							EncodedPayload::Rows(EncodedRows::concat(
								self.format,
								decode_rows,
								parts,
								head,
							)?)
						}
						// A statement that streamed no rows answers an empty list.
						None => EncodedPayload::Rows(EncodedRows::empty(self.format, decode_rows)),
					}),
				};
				self.results.push(EncodedQueryResult {
					time,
					result,
					query_type,
				});
			}
			// A buffered answer carries no trace: only a streaming execution
			// is asked for one.
			QueryStreamItem::Trace {
				..
			} => {}
		}
		Ok(())
	}

	/// The rows `values` encode to in the collector's format.
	fn rows_of(&mut self, values: Vec<Value>) -> anyhow::Result<EncodedRows> {
		match self.format {
			#[cfg(feature = "shaped")]
			RowFormat::Shaped => shaped_rows_of_values(&mut self.shaper, &values),
			format => rows_of_values(format, values),
		}
	}

	/// The results, in the order their statements finished.
	pub fn finish(self) -> Vec<EncodedQueryResult> {
		self.results
	}
}

/// Write `rows` as the list they are: for shaped rows, a block whose shapes
/// are announced in `scope`.
///
/// With `hole`, CBOR rows are not written: the list's head is, and `hole` is set to where the
/// rows' bytes belong.
fn write_rows(
	format: RowFormat,
	rows: &EncodedRows,
	scope: &mut ShapeScope,
	out: &mut Vec<u8>,
	hole: Option<&mut Option<usize>>,
) -> anyhow::Result<()> {
	match format {
		RowFormat::Cbor | RowFormat::Cbor2 => {
			cbor_head(out, 4, rows.len() as u64);
			match hole {
				Some(hole) => *hole = Some(out.len()),
				None => out.extend_from_slice(rows.bytes()),
			}
		}
		RowFormat::Json => {
			out.push(b'[');
			out.extend_from_slice(rows.bytes());
			out.push(b']');
		}
		#[cfg(not(feature = "shaped"))]
		RowFormat::Shaped => {
			let _ = scope;
			anyhow::bail!(SHAPED_NOT_BUILT);
		}
		#[cfg(feature = "shaped")]
		RowFormat::Shaped => {
			surrealdb_types::shaped::write_block(out, scope, rows.runs())
				.map_err(|e| anyhow::anyhow!(e))?;
		}
	}
	Ok(())
}

fn cbor_head(out: &mut Vec<u8>, major: u8, n: u64) {
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

/// An object writer that writes keys in the order given, which the callers
/// give in key order.
struct ObjectWriter<'o> {
	format: RowFormat,
	out: &'o mut Vec<u8>,
	written: usize,
}

impl<'o> ObjectWriter<'o> {
	fn open(format: RowFormat, out: &'o mut Vec<u8>, entries: usize) -> Self {
		let format = format.envelope();
		if format == RowFormat::Json {
			out.push(b'{');
		} else {
			cbor_head(out, 5, entries as u64);
		}
		Self {
			format,
			out,
			written: 0,
		}
	}

	/// Start the entry `key`; its value is written next.
	fn key(&mut self, key: &str) -> anyhow::Result<()> {
		if self.format == RowFormat::Json {
			if self.written > 0 {
				self.out.push(b',');
			}
			serde_json::to_writer(&mut *self.out, key)?;
			self.out.push(b':');
		} else {
			cbor_head(self.out, 3, key.len() as u64);
			self.out.extend_from_slice(key.as_bytes());
		}
		self.written += 1;
		Ok(())
	}

	fn value(&mut self, value: Value) -> anyhow::Result<()> {
		encode_public(self.format, value, self.out)
	}

	fn close(self) {
		if self.format == RowFormat::Json {
			self.out.push(b'}');
		}
	}
}

/// Write one statement's result as `QueryResult::into_value` encodes.
fn write_result(
	format: RowFormat,
	result: &EncodedQueryResult,
	out: &mut Vec<u8>,
	hole: Option<&mut Option<usize>>,
) -> anyhow::Result<()> {
	let payload = match &result.result {
		Ok(payload) => payload,
		// An error carries no rows: it is encoded whole.
		Err(_) => {
			return encode_public(format, result.clone().into_query_result().into_value(), out);
		}
	};
	// `{ result, status, time, type }`, in key order.
	let mut object = ObjectWriter::open(format, out, 4);
	object.key("result")?;
	match payload {
		// A buffered result's block is its own shape scope.
		EncodedPayload::Rows(rows) => {
			write_rows(format, rows, &mut ShapeScope::new(), object.out, hole)?
		}
		EncodedPayload::Value(value) => object.value(value.clone())?,
	}
	object.key("status")?;
	object.value(surrealdb_rpc::Status::Ok.into_value())?;
	object.key("time")?;
	object.value(Value::String(format!("{:?}", result.time)))?;
	object.key("type")?;
	object.value(result.query_type.into_value())?;
	object.close();
	Ok(())
}

/// Write `results` as the list of statement results a query answers.
pub fn encode_results(
	format: RowFormat,
	results: &[EncodedQueryResult],
	out: &mut Vec<u8>,
) -> anyhow::Result<()> {
	write_results(format, results, out, None)
}

/// [`encode_results`], leaving the rows of a single result out where `hole` asks it to
/// ([`write_rows`]).
fn write_results(
	format: RowFormat,
	results: &[EncodedQueryResult],
	out: &mut Vec<u8>,
	mut hole: Option<&mut Option<usize>>,
) -> anyhow::Result<()> {
	debug_assert!(hole.is_none() || results.len() == 1, "a hole is left for one result's rows");
	if format.envelope() == RowFormat::Json {
		out.push(b'[');
	} else {
		cbor_head(out, 4, results.len() as u64);
	}
	for (i, result) in results.iter().enumerate() {
		if format == RowFormat::Json && i > 0 {
			out.push(b',');
		}
		write_result(format, result, out, hole.as_deref_mut())?;
	}
	if format == RowFormat::Json {
		out.push(b']');
	}
	Ok(())
}

/// Write the response envelope `{ id, result, session }`, in key order with
/// absent keys left out, with `result` written by `result`.
///
/// `capacity` is what the response is expected to take, so that the rows are copied in once.
fn write_envelope(
	format: RowFormat,
	id: Option<Value>,
	session_id: Option<uuid::Uuid>,
	capacity: usize,
	result: impl FnOnce(&mut Vec<u8>) -> anyhow::Result<()>,
) -> anyhow::Result<Vec<u8>> {
	let entries = 1 + usize::from(id.is_some()) + usize::from(session_id.is_some());
	let mut out = Vec::with_capacity(capacity);
	let mut object = ObjectWriter::open(format, &mut out, entries);
	if let Some(id) = id {
		object.key("id")?;
		object.value(id)?;
	}
	object.key("result")?;
	result(object.out)?;
	if let Some(session_id) = session_id {
		object.key(surrealdb_rpc::request::SESSION_ID)?;
		object.value(Value::Uuid(session_id.into()))?;
	}
	object.close();
	Ok(out)
}

/// Encode `response` in `format`: byte-identical to encoding
/// `response.into_value()`, without building the rows of a
/// [`DbResult::Encoded`] as values.
pub fn encode_response(format: RowFormat, response: DbResponse) -> anyhow::Result<Vec<u8>> {
	let DbResponse {
		id,
		session_id,
		result,
	} = response;
	let results = match result {
		Ok(DbResult::Encoded(results)) => results,
		other => {
			let response = DbResponse {
				id,
				session_id,
				result: other,
			};
			let mut out = Vec::new();
			encode_public(format, response.into_value(), &mut out)?;
			return Ok(out);
		}
	};
	let capacity =
		ENVELOPE_SLACK + results.iter().map(EncodedQueryResult::encoded_len).sum::<usize>();
	write_envelope(format, id, session_id, capacity, |out| encode_results(format, &results, out))
}

/// What a response takes beyond its rows' bytes, for its envelope and each result's other fields,
/// in most cases.
const ENVELOPE_SLACK: usize = 256;

/// The bytes left free ahead of CBOR rows as they are encoded, so that the response written
/// around the rows of a single statement fits in front of them ([`encode_response_bytes`]).
pub(crate) const ROW_HEADROOM: usize = 128;

/// [`encode_response`] as shared bytes. The response to a query of one statement whose rows are
/// CBOR is written around the rows where they lie, which copies none of them when the headroom
/// ahead of them holds what comes first ([`EncodedRows::wrap`]).
pub fn encode_response_bytes(
	format: RowFormat,
	response: DbResponse,
) -> anyhow::Result<bytes::Bytes> {
	let single_rows = matches!(format, RowFormat::Cbor | RowFormat::Cbor2)
		&& matches!(&response.result, Ok(DbResult::Encoded(results))
			if results.len() == 1 && matches!(results[0].result, Ok(EncodedPayload::Rows(_))));
	if !single_rows {
		return encode_response(format, response).map(bytes::Bytes::from);
	}
	let DbResponse {
		id,
		session_id,
		result,
	} = response;
	let Ok(DbResult::Encoded(mut results)) = result else {
		unreachable!("a single result of rows was just matched");
	};
	let mut hole = None;
	let around = write_envelope(format, id, session_id, ENVELOPE_SLACK, |out| {
		write_results(format, &results, out, Some(&mut hole))
	})?;
	let hole = hole.expect("the rows' place is marked");
	let (prefix, suffix) = around.split_at(hole);
	let Some(Ok(EncodedPayload::Rows(rows))) = results.pop().map(|r| r.result) else {
		unreachable!("a single result of rows was just matched");
	};
	match rows.wrap(prefix, suffix) {
		Ok((bytes, start)) => Ok(bytes::Bytes::from(bytes).slice(start..)),
		Err(rows) => {
			let mut out = Vec::with_capacity(around.len() + rows.bytes().len());
			out.extend_from_slice(prefix);
			out.extend_from_slice(rows.bytes());
			out.extend_from_slice(suffix);
			Ok(out.into())
		}
	}
}

#[cfg(feature = "shaped")]
/// A `query_stream` frame as a shaped stream sends it: rows that arrive as
/// values, or encoded in another format, are written as shaped rows by
/// `shaper`, the stream's, so every `rows` frame of the stream is a block.
/// Any other frame is returned as it is.
pub fn shaped_outgoing(
	frame: surrealdb_rpc::OutgoingFrame,
	shaper: &mut ValueShaper,
) -> Result<surrealdb_rpc::OutgoingFrame, TypesError> {
	use surrealdb_rpc::{OutgoingFrame, QueryStreamFrame};
	let shaped = |values: &[Value], shaper: &mut ValueShaper| {
		shaped_rows_of_values(shaper, values).map_err(|e| TypesError::internal(e.to_string()))
	};
	Ok(match frame {
		OutgoingFrame::Frame(QueryStreamFrame::Rows {
			index,
			values,
			trace,
		}) => OutgoingFrame::Rows {
			index,
			rows: shaped(&values, shaper)?,
			trace,
		},
		OutgoingFrame::Rows {
			index,
			rows,
			trace,
		} if rows.format() != RowFormat::Shaped => OutgoingFrame::Rows {
			index,
			rows: shaped(&rows.to_values()?, shaper)?,
			trace,
		},
		frame @ (OutgoingFrame::Frame(_)
		| OutgoingFrame::Rows {
			..
		}) => frame,
	})
}

#[cfg(feature = "shaped")]
/// Encode the live notification `notification`, as the response to request
/// `id`, on a shaped connection: the CBOR its response encodes to, with the
/// notification's `result` written as a block of one row, in the live query's
/// shape scope `scope`, by its shaper `shaper`.
pub fn encode_shaped_notification(
	id: Option<Value>,
	session_id: Option<uuid::Uuid>,
	notification: surrealdb_types::Notification,
	shaper: &mut ValueShaper,
	scope: &mut ShapeScope,
) -> anyhow::Result<Vec<u8>> {
	let Value::Object(object) = DbResult::Live(notification).into_value() else {
		anyhow::bail!("a notification that is not an object");
	};
	let object = object.into_inner();
	write_envelope(RowFormat::Cbor2, id, session_id, ENVELOPE_SLACK, |out| {
		let mut writer = ObjectWriter::open(RowFormat::Cbor2, out, object.len());
		for (key, value) in object {
			writer.key(&key)?;
			if key == "result" {
				let rows = shaped_rows_of_values(shaper, std::slice::from_ref(&value))?;
				write_rows(RowFormat::Shaped, &rows, scope, writer.out, None)?;
			} else {
				writer.value(value)?;
			}
		}
		writer.close();
		Ok(())
	})
}

/// Encode the `query_stream` `rows` frame of statement `index` carrying
/// `rows`, as the successful response to request `id`: byte-identical to
/// encoding that response with the frame's value
/// ([`QueryStreamFrame::into_value`](surrealdb_rpc::QueryStreamFrame::into_value))
/// built from the values the rows encode. Shaped rows are one block, its
/// shapes announced in `scope`, the stream's.
pub fn encode_stream_rows(
	format: RowFormat,
	id: Option<Value>,
	session_id: Option<uuid::Uuid>,
	index: usize,
	rows: &EncodedRows,
	trace: Option<Box<surrealdb_rpc::Trace>>,
	scope: &mut ShapeScope,
) -> anyhow::Result<Vec<u8>> {
	anyhow::ensure!(rows.format() == format, "rows encoded in another format");
	let index = i64::try_from(index)?;
	let trace = trace.filter(|trace| !trace.is_empty()).map(|trace| trace.into_value());
	write_envelope(format, id, session_id, ENVELOPE_SLACK + rows.bytes().len(), |out| {
		// `{ index, stream, trace, values }`, in key order, `trace` only when
		// there is one.
		let mut object = ObjectWriter::open(format, out, 3 + usize::from(trace.is_some()));
		object.key("index")?;
		object.value(Value::from_t(index))?;
		object.key("stream")?;
		object.value(Value::String("rows".to_owned()))?;
		if let Some(trace) = trace {
			object.key("trace")?;
			object.value(trace)?;
		}
		object.key("values")?;
		write_rows(format, rows, scope, object.out, None)?;
		object.close();
		Ok(())
	})
}

/// Trips a query's own cancel handle when the future collecting its results
/// is dropped before the execution finished.
struct StopOnDrop {
	cancel: crate::ctx::CancelHandle,
	detached: std::sync::Arc<std::sync::atomic::AtomicBool>,
	armed: bool,
}

impl Drop for StopOnDrop {
	fn drop(&mut self) {
		if self.armed {
			self.detached.store(true, std::sync::atomic::Ordering::SeqCst);
			self.cancel.trip();
		}
	}
}

/// Run `request` on the streaming executor with rows encoded in `format`,
/// collecting its items: the results [`Datastore::run`](crate::kvs::Datastore::run)
/// answers, with no public value built for any row, alongside the executor's
/// own results (whose row values are empty).
///
/// The execution owns an open transaction, so it is never dropped
/// mid-flight. It runs as its own task under a cancel handle of its own,
/// tripped when `outer` (a connection's handle) is. If this future is dropped
/// first — a caller's wall-clock guard, a client gone — the handle is tripped
/// and the item channel closed, and the task polls the execution until it
/// stops and finalises its transaction; any `LIVE SELECT` it committed is
/// then deleted, since nobody is left to register it.
pub(crate) async fn collect_encoded(
	kvs: &std::sync::Arc<crate::kvs::Datastore>,
	request: crate::kvs::QueryRequest<'_>,
	outer: Option<crate::ctx::CancelHandle>,
	format: RowFormat,
) -> Result<(Vec<QueryResult>, Vec<EncodedQueryResult>), TypesError> {
	let cancel = crate::ctx::CancelHandle::new();
	let (items, received) = async_channel::bounded(surrealdb_rpc::QUERY_STREAM_BUFFER);
	let job = kvs.run_streaming(
		request.with_optional_cancel(Some(cancel.clone())).with_row_format(Some(format)),
		items,
	)?;
	let detached = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
	let mut guard = StopOnDrop {
		cancel: cancel.clone(),
		detached: std::sync::Arc::clone(&detached),
		armed: true,
	};
	let execution = {
		let kvs = std::sync::Arc::clone(kvs);
		let mut run = job.run;
		async move {
			let result = match outer {
				Some(outer) => {
					let token = outer.token();
					tokio::select! {
						biased;
						result = &mut run => Some(result),
						_ = token.cancelled() => None,
					}
				}
				None => Some((&mut run).await),
			};
			// The connection was cancelled: this query stops with it, and is
			// still polled until it has.
			let result = match result {
				Some(result) => result,
				None => {
					cancel.trip();
					run.await
				}
			};
			if detached.load(std::sync::atomic::Ordering::SeqCst) {
				crate::rpc::protocol::settle_detached_live_queries(&kvs, result).await;
				return None;
			}
			Some(result)
		}
	};
	#[cfg(not(target_family = "wasm"))]
	let execution = {
		let task = tokio::spawn(execution);
		async move {
			task.await.map_err(|e| TypesError::internal(format!("The query task failed: {e}")))
		}
	};
	#[cfg(target_family = "wasm")]
	let execution = async move { Ok::<_, TypesError>(execution.await) };
	// The execution and the drain run together: the channel is bounded.
	let mut collector = QueryCollector::new(format);
	let drain = async {
		let mut failed = None;
		while let Ok(item) = received.recv().await {
			if failed.is_none()
				&& let Err(e) = collector.absorb(item)
			{
				failed = Some(e);
			}
		}
		failed
	};
	let (ran, failed) = futures::join!(execution, drain);
	guard.armed = false;
	let results = match ran? {
		Some(results) => results?,
		None => return Err(TypesError::internal("The query was detached".to_owned())),
	};
	if let Some(e) = failed {
		return Err(TypesError::internal(e.to_string()));
	}
	Ok((results, collector.finish()))
}

/// Run `sql` on the streaming executor with rows encoded in `format`: the
/// results [`Datastore::execute`](crate::kvs::Datastore::execute) answers,
/// with no public value built for any row. See [`collect_encoded`] for how
/// the execution is stopped if this future is dropped.
pub async fn execute_encoded(
	ds: &std::sync::Arc<crate::kvs::Datastore>,
	sql: &str,
	session: &crate::dbs::Session,
	vars: Option<surrealdb_types::Variables>,
	format: RowFormat,
	read_only: Option<crate::kvs::ReadOnlyOptions>,
) -> Result<Vec<EncodedQueryResult>, TypesError> {
	let request =
		crate::kvs::QueryRequest::new(sql, session).with_variables(vars).with_read_only(read_only);
	Ok(collect_encoded(ds, request, None, format).await?.1)
}

/// The buffered results `results` decode to, for a caller that needs values.
pub fn into_query_results(results: Vec<EncodedQueryResult>) -> Vec<QueryResult> {
	results.into_iter().map(EncodedQueryResult::into_query_result).collect()
}

#[cfg(test)]
pub(crate) mod tests {
	use std::sync::Arc;
	use std::time::Duration;

	use surrealdb_rpc::{DbResponse, DbResult, QUERY_STREAM_BUFFER, RowFormat};
	use surrealdb_types::Value;

	#[cfg(feature = "shaped")]
	use super::decode_dynamic;
	use super::{QueryCollector, encode_response, encode_response_bytes, encode_results};
	use crate::catalog::providers::CatalogProvider;
	use crate::dbs::Session;
	use crate::kvs::{Datastore, QueryRequest, TransactionType};

	/// A dynamic slice is one CBOR item: a second item after it is refused.
	#[cfg(feature = "shaped")]
	#[test]
	fn a_dynamic_slice_with_a_trailing_item_is_refused() {
		assert!(decode_dynamic(&[0x01]).is_ok());
		assert!(decode_dynamic(&[0x01, 0x02]).is_err());
	}

	pub(crate) async fn datastore() -> Arc<Datastore> {
		let ds = Datastore::new("memory").await.expect("a memory datastore");
		let tx = ds.transaction(TransactionType::Write).await.expect("a write transaction");
		tx.ensure_ns_db(None, "test", "test").await.expect("the test namespace and database");
		tx.commit().await.expect("committing the namespace");
		let seed = "
			CREATE t:1 SET s = 'a', n = 1, f = 1.5f, d = 1.25dec, b = true, z = NULL,
				dur = 1h30m, dt = d'2024-01-02T03:04:05.006Z', u = u'018f9b8e-0000-7000-8000-000000000000',
				arr = [1, 'x', [2]], obj = { k: 'v', q: { r: 1 } }, link = t:2, by = <bytes> 'ab',
				long = 'a string longer than twenty-three bytes, with \"quotes\" and \\\\ and é';
			CREATE t:2 SET s = 'b', n = 2, tags = <set> [3, 1], geo = (1.5, 2.5), nan = <float> 'NaN', inf = math::inf,
				rid = t:['a', 1], ridobj = t:{ a: 1 }, ridstr = t:⟨with space⟩, rng = 1..5, deep = { a: { b: { c: [{ d: 1 }] } } };
			CREATE t:3 SET s = 'c', n = -3, big = 9223372036854775807, small = -9223372036854775808,
				dts = d'1970-01-01T00:00:00Z', dtn = d'2024-01-02T03:04:05.123456789Z', durz = 0ns;
			CREATE t:4 SET tbl = type::table('x');
			CREATE t:5 SET s = 'e', n = 5, nones = [NONE, NULL, 1], nested = { a: NONE, b: NULL, c: [NONE] },
				dneg = -0.000123dec, dbig = 123456789012345678901234567.89dec, dexp = 1.5e10dec, dzero = 0dec,
				dtpre = d'1900-06-15T12:00:00Z', dtfar = d'9999-12-31T23:59:59.999999999Z',
				bempty = <bytes> '', bbin = encoding::base64::decode('AP8K'),
				fneg0 = -0.0f, fbig = 1e300f, fsmall = 5e-324f, ctrl = 'tab\tnl\nend', emoji = '😀',
				durbig = 52w3d4h5m6s7ms8µs9ns,
				line = <geometry<line>> { type: 'LineString', coordinates: [[0, 0], [1, 1]] },
				poly = <geometry<polygon>> { type: 'Polygon', coordinates: [[[0, 0], [1, 0], [1, 1], [0, 0]]] },
				mpoint = <geometry<multipoint>> { type: 'MultiPoint', coordinates: [[0, 0], [1.5, -1]] },
				gcoll = <geometry<collection>> { type: 'GeometryCollection', geometries: [{ type: 'Point', coordinates: [1, 2] }] },
				riduuid = t:u'018f9b8e-0000-7000-8000-000000000001', ridarr = [t:1, t:['a', 1]];
			CREATE t:u'018f9b8e-0000-7000-8000-000000000002' SET s = 'uuid key', n = 6;
			CREATE t:['k', 2] SET s = 'array key', n = 7;
			CREATE t:{ k: 'obj' } SET s = 'object key', n = 8;
			CREATE t:⟨str key⟩ SET s = 'string key', n = 9;
		";
		for result in ds.execute(seed, &session(), None).await.expect("the seed runs") {
			result.result.expect("every seed statement succeeds");
		}
		ds
	}

	pub(crate) fn session() -> Session {
		Session::owner().with_ns("test").with_db("test")
	}

	/// The queries the identity is checked over: rows of every kind, a single
	/// value, a list value, an empty list, errors, a failed transaction, and
	/// statements that answer nothing.
	pub(crate) const QUERIES: &[&str] = &[
		"SELECT * FROM t",
		"SELECT s, n FROM t WHERE n > 1",
		"SELECT n AS z, s AS a, dt, u, d, f, link, arr, obj FROM t",
		"SELECT s, obj.k AS x, obj.q.r AS y FROM t",
		"SELECT n AS a, s AS a FROM t",
		"SELECT `select`, s AS `weird key` FROM t",
		"SELECT * FROM t ORDER BY n DESC LIMIT 2",
		"SELECT * FROM t ORDER BY n",
		"SELECT s, n FROM t ORDER BY s DESC",
		"SELECT * FROM t ORDER BY n START 1 LIMIT 2",
		"SELECT * FROM ONLY t:1",
		"SELECT VALUE s FROM t",
		"SELECT count() FROM t GROUP ALL",
		"SELECT * FROM t WHERE n > 100",
		"SELECT * FROM t:5",
		"SELECT nones, nested, missing, nested.a AS na FROM t:5",
		"SELECT * FROM t WHERE n >= 5 ORDER BY n DESC",
		"SELECT * FROM t ORDER BY s",
		"SELECT id FROM t ORDER BY id",
		"SELECT VALUE id FROM t",
		"SELECT s, count() AS c FROM t GROUP BY s",
		"SELECT * OMIT s FROM t",
		"SELECT * FROM t:1 FETCH link",
		"SELECT * FROM t SPLIT arr",
		"SELECT NONE AS x, s FROM t",
		"SELECT s, NONE AS x, { a: NONE, b: 1 } AS o FROM t ORDER BY s",
		"RETURN [1, 2, 3]",
		"RETURN []",
		"RETURN 1 + 2",
		"LET $x = 1; RETURN $x",
		"SELECT * FROM fn::missing()",
		"THROW 'boom'",
		"BEGIN; CREATE u:1; THROW 'stop'; COMMIT;",
		"SELECT * FROM t:1; SELECT * FROM t:2; RETURN NONE",
		"INFO FOR DB",
		"CREATE c:1 SET a = 1; UPDATE c:1 SET a = 2; DELETE c:1 RETURN BEFORE",
	];

	/// `results`' times zeroed, since two runs take different times.
	fn untimed(mut results: Vec<surrealdb_rpc::QueryResult>) -> Vec<surrealdb_rpc::QueryResult> {
		for result in &mut results {
			result.time = Duration::ZERO;
		}
		results
	}

	pub(crate) async fn encoded(
		ds: &Arc<Datastore>,
		sql: &str,
		format: RowFormat,
	) -> (Vec<surrealdb_rpc::EncodedQueryResult>, usize) {
		let session = session();
		let (items, received) = async_channel::bounded(QUERY_STREAM_BUFFER);
		let job = ds
			.run_streaming(QueryRequest::new(sql, &session).with_row_format(Some(format)), items)
			.expect("the query starts");
		let mut collector = QueryCollector::new(format);
		let mut frames = 0;
		let drain = async {
			while let Ok(item) = received.recv().await {
				frames += usize::from(matches!(item, surrealdb_rpc::QueryStreamItem::Frame { .. }));
				collector.absorb(item).expect("absorbs");
			}
		};
		let (ran, ()) = futures::join!(job.run, drain);
		ran.expect("the query runs");
		let mut results = collector.finish();
		for result in &mut results {
			result.time = Duration::ZERO;
		}
		(results, frames)
	}

	fn encode_values(format: RowFormat, value: Value) -> Vec<u8> {
		match format {
			RowFormat::Cbor => super::super::cbor::encode(value).expect("encodes"),
			RowFormat::Cbor2 => super::super::cbor::encode_v2(value).expect("encodes"),
			RowFormat::Json => super::super::json::encode_str(value).expect("encodes").into_bytes(),
			RowFormat::Shaped => unreachable!("shaped rows are compared decoded"),
		}
	}

	/// Every query's response, assembled around encoded rows, is the bytes
	/// the response built from values encodes to, in both formats, bare (as
	/// `/sql` answers) and in the RPC envelope.
	#[tokio::test]
	async fn an_encoded_response_is_byte_identical_to_one_built_from_values() {
		let ds = datastore().await;
		for format in [RowFormat::Cbor, RowFormat::Cbor2, RowFormat::Json] {
			for sql in QUERIES {
				let values =
					untimed(ds.execute(sql, &session(), None).await.expect("the query runs"));
				let (rows, frames) = encoded(&ds, sql, format).await;
				if *sql == "SELECT * FROM t" || sql.contains("ORDER BY") {
					assert!(frames > 0, "`{sql}` streams its rows as frames");
				}

				let mut bare = Vec::new();
				encode_results(format, &rows, &mut bare).expect("encodes");
				let expected = encode_values(
					format,
					Value::Array(
						values
							.iter()
							.cloned()
							.map(surrealdb_types::SurrealValue::into_value)
							.collect::<Vec<_>>()
							.into(),
					),
				);
				assert_eq!(
					String::from_utf8_lossy(&bare),
					String::from_utf8_lossy(&expected),
					"{format:?} `{sql}`"
				);
				assert_eq!(bare, expected, "{format:?} `{sql}`");

				assert_bytes_match(format, &rows);
				let id = Some(Value::from_t(7i64));
				let session_id = Some(uuid::Uuid::nil());
				let enveloped = encode_response(
					format,
					DbResponse::success(id.clone(), session_id, DbResult::Encoded(rows)),
				)
				.expect("encodes");
				let expected = encode_values(
					format,
					surrealdb_types::SurrealValue::into_value(DbResponse::success(
						id,
						session_id,
						DbResult::Query(values),
					)),
				);
				assert_eq!(enveloped, expected, "{format:?} enveloped `{sql}`");
			}
		}
	}

	/// [`encode_response_bytes`] answers what [`encode_response`] does for `rows`, whatever the
	/// request id (one too long for the rows' headroom among them) and session.
	fn assert_bytes_match(format: RowFormat, rows: &[surrealdb_rpc::EncodedQueryResult]) {
		let long = "an id longer than the headroom kept ahead of the rows".repeat(4);
		for id in [None, Some(Value::from_t(7i64)), Some(Value::String(long))] {
			for session_id in [None, Some(uuid::Uuid::nil())] {
				let response = || {
					DbResponse::success(id.clone(), session_id, DbResult::Encoded(rows.to_vec()))
				};
				let expected = encode_response(format, response()).expect("encodes");
				let bytes = encode_response_bytes(format, response()).expect("encodes");
				assert_eq!(
					&bytes[..],
					&expected[..],
					"{format:?} id {id:?} session {session_id:?}"
				);
			}
		}
	}

	#[tokio::test]
	async fn rows_of_many_frames_are_written_around_where_they_lie() {
		let ds = datastore().await;
		let seed = "FOR $i IN 0..2500 { CREATE many SET n = $i, s = 'row ' + <string> $i };";
		for result in ds.execute(seed, &session(), None).await.expect("the seed runs") {
			result.result.expect("the seed succeeds");
		}
		let sql = "SELECT * FROM many ORDER BY n";
		let values = untimed(ds.execute(sql, &session(), None).await.expect("the query runs"));
		for format in [RowFormat::Cbor, RowFormat::Cbor2, RowFormat::Json] {
			let (rows, frames) = encoded(&ds, sql, format).await;
			assert!(frames > 1, "{format:?}: the rows arrive in several frames");
			assert_bytes_match(format, &rows);
			let id = Some(Value::from_t(1i64));
			let bytes = encode_response_bytes(
				format,
				DbResponse::success(id.clone(), None, DbResult::Encoded(rows)),
			)
			.expect("encodes");
			let expected = encode_values(
				format,
				surrealdb_types::SurrealValue::into_value(DbResponse::success(
					id,
					None,
					DbResult::Query(values.clone()),
				)),
			);
			assert_eq!(&bytes[..], &expected[..], "{format:?}");
		}
	}

	/// A streaming execution's items, each `Finished` item's time zeroed.
	pub(crate) async fn items(
		ds: &Arc<Datastore>,
		sql: &str,
		format: Option<RowFormat>,
	) -> Vec<surrealdb_rpc::QueryStreamItem> {
		let session = session();
		let (sender, received) = async_channel::bounded(QUERY_STREAM_BUFFER);
		let job = ds
			.run_streaming(QueryRequest::new(sql, &session).with_row_format(format), sender)
			.expect("the query starts");
		let drain = async {
			let mut items = Vec::new();
			while let Ok(mut item) = received.recv().await {
				if let surrealdb_rpc::QueryStreamItem::Finished {
					time,
					..
				} = &mut item
				{
					*time = Duration::ZERO;
				}
				items.push(item);
			}
			items
		};
		let (ran, items) = futures::join!(job.run, drain);
		ran.expect("the query runs");
		items
	}

	/// Every `query_stream` frame, rows written around their encoded bytes,
	/// is the message the frame built from values encodes to: the same frames,
	/// the same rows in each, byte for byte, in both formats.
	#[tokio::test]
	async fn encoded_stream_frames_are_byte_identical_to_frames_built_from_values() {
		use surrealdb_rpc::{OutgoingFrame, StreamFrames};
		let ds = datastore().await;
		let id = Some(Value::from_t(7i64));
		let session_id = Some(uuid::Uuid::nil());
		let envelope = |format, frame: surrealdb_rpc::QueryStreamFrame| {
			encode_response(
				format,
				DbResponse::success(id.clone(), session_id, DbResult::Other(frame.into_value())),
			)
			.expect("encodes")
		};
		for format in [RowFormat::Cbor, RowFormat::Cbor2, RowFormat::Json] {
			for sql in QUERIES {
				let mut by_value = StreamFrames::new();
				for item in items(&ds, sql, None).await {
					by_value.absorb(item);
				}
				let expected: Vec<Vec<u8>> =
					std::iter::from_fn(|| by_value.pop()).map(|f| envelope(format, f)).collect();

				let mut by_bytes = StreamFrames::keeping_encoded_rows();
				for item in items(&ds, sql, Some(format)).await {
					by_bytes.absorb(item);
				}
				let mut encoded_frames = 0;
				let actual: Vec<Vec<u8>> = std::iter::from_fn(|| by_bytes.pop_outgoing())
					.map(|frame| match frame {
						OutgoingFrame::Frame(frame) => envelope(format, frame),
						OutgoingFrame::Rows {
							index,
							rows,
							trace,
						} => {
							encoded_frames += 1;
							super::encode_stream_rows(
								format,
								id.clone(),
								session_id,
								index,
								&rows,
								trace,
								&mut surrealdb_types::shaped::ShapeScope::new(),
							)
							.expect("encodes")
						}
					})
					.collect();
				if *sql == "SELECT * FROM t" || sql.contains("ORDER BY") {
					assert!(encoded_frames > 0, "`{sql}` streams encoded rows frames");
				}
				assert_eq!(actual.len(), expected.len(), "{format:?} `{sql}` frame count");
				for (a, e) in actual.iter().zip(&expected) {
					assert_eq!(
						String::from_utf8_lossy(a),
						String::from_utf8_lossy(e),
						"{format:?} `{sql}`"
					);
					assert_eq!(a, e, "{format:?} `{sql}`");
				}
			}
		}
	}

	/// Rows read back from frames (`EncodedRows::to_values`, the decoder a
	/// value consumer such as gRPC or the embedded SDK uses) are exactly the
	/// values the old path built: the same kinds, NONE kept apart from NULL,
	/// record ids, datetimes, decimals, geometries and bytes intact, in order.
	#[tokio::test]
	async fn rows_decoded_from_cbor_frames_are_the_values_the_old_path_built() {
		let ds = datastore().await;
		for sql in QUERIES {
			let values = untimed(ds.execute(sql, &session(), None).await.expect("the query runs"));
			let (rows, _) = encoded(&ds, sql, RowFormat::Cbor).await;
			let decoded = untimed(super::into_query_results(rows));
			assert_eq!(decoded.len(), values.len(), "`{sql}`");
			for (d, v) in decoded.iter().zip(&values) {
				assert_eq!(d.result, v.result, "`{sql}`");
				assert_eq!(d.query_type, v.query_type, "`{sql}`");
			}
		}
		// The kinds above, checked as kinds, so a lossy decode that happened to
		// match on both sides cannot pass.
		let (rows, _) = encoded(&ds, "SELECT * FROM t:5", RowFormat::Cbor).await;
		let Value::Array(rows) = super::into_query_results(rows).remove(0).result.expect("a row")
		else {
			panic!("not rows");
		};
		let row = rows.into_inner().remove(0);
		let Value::Object(row) = row else {
			panic!("not an object: {row:?}")
		};
		assert!(matches!(row.get("id"), Some(Value::RecordId(_))));
		assert!(matches!(
			row.get("dbig"),
			Some(Value::Number(surrealdb_types::Number::Decimal(_)))
		));
		assert!(matches!(row.get("dtfar"), Some(Value::Datetime(_))));
		assert!(matches!(row.get("bbin"), Some(Value::Bytes(_))));
		assert!(matches!(row.get("poly"), Some(Value::Geometry(_))));
		assert!(matches!(row.get("durbig"), Some(Value::Duration(_))));
		match row.get("nones") {
			Some(Value::Array(a)) => {
				assert_eq!(
					a.iter().collect::<Vec<_>>(),
					[&Value::None, &Value::Null, &Value::from_t(1i64)]
				)
			}
			other => panic!("nones: {other:?}"),
		}
	}

	/// A build without shaped frames does not offer them: the subprotocol is not served, a
	/// request naming it is answered as for any unsupported format, and a caller that asks the
	/// executor for shaped rows anyway gets an error for each statement of rows, never a panic.
	#[cfg(not(feature = "shaped"))]
	#[tokio::test]
	async fn a_build_without_shaped_frames_refuses_them_cleanly() {
		use super::super::{Format, PROTOCOLS};
		assert_eq!(Format::from("shaped"), Format::Unsupported);
		assert_eq!(Format::shaped(), Format::Unsupported);
		assert!(!PROTOCOLS.contains(&"shaped"));
		assert_eq!(Format::Shaped.row_format(), None);
		let ds = Datastore::new("memory").await.expect("a memory datastore");
		let tx = ds.transaction(TransactionType::Write).await.expect("a write transaction");
		tx.ensure_ns_db(None, "test", "test").await.expect("the test namespace and database");
		tx.commit().await.expect("committing the namespace");
		ds.execute("CREATE t:1 SET s = 'a'; CREATE t:2 SET s = 'b'", &session(), None)
			.await
			.expect("the seed runs");
		let (results, _) =
			encoded(&ds, "SELECT * FROM t; SELECT s FROM t; RETURN 1", RowFormat::Shaped).await;
		for result in &results[..2] {
			let error = result.result.as_ref().expect_err("a statement of rows is refused");
			assert!(error.to_string().contains("does not serve shaped frames"), "{error}");
		}
		assert!(results[2].result.is_ok(), "a statement of no rows answers");
		let mut out = Vec::new();
		encode_results(RowFormat::Shaped, &results, &mut out).expect("errors encode as CBOR");
	}

	/// A query that runs until it is stopped, writing to the table `e`.
	const ENDLESS: &str = "FOR $i IN 0..1000000000 { CREATE e SET n = $i }";

	/// A collecting future dropped mid-flight does not drop its execution:
	/// the execution is stopped and polled to completion on its own task, and
	/// its writes are not committed.
	#[tokio::test]
	async fn a_dropped_collection_stops_its_execution() {
		let ds = datastore().await;
		let session = session();
		ds.execute("DEFINE TABLE e", &session, None).await.expect("defines the table");
		let alive = || tokio::runtime::Handle::current().metrics().num_alive_tasks();
		let before = alive();
		let run = super::execute_encoded(&ds, ENDLESS, &session, None, RowFormat::Cbor, None);
		let dropped = tokio::time::timeout(Duration::from_millis(200), run).await;
		assert!(dropped.is_err(), "the query was still running when it was dropped");
		let stopped = async {
			while alive() > before {
				tokio::time::sleep(Duration::from_millis(10)).await;
			}
		};
		tokio::time::timeout(Duration::from_secs(30), stopped)
			.await
			.expect("the execution stops after its collector is dropped");
		let committed = ds
			.execute("SELECT VALUE id FROM e", &session, None)
			.await
			.expect("the count runs")
			.remove(0)
			.result
			.expect("the count succeeds");
		assert_eq!(committed, Value::Array(Vec::<Value>::new().into()), "nothing was committed");
	}

	/// A tripped connection handle stops a query collected under it.
	#[tokio::test]
	async fn a_cancelled_connection_stops_its_queries() {
		let ds = datastore().await;
		let session = session();
		let outer = crate::ctx::CancelHandle::new();
		let run = super::collect_encoded(
			&ds,
			QueryRequest::new(ENDLESS, &session),
			Some(outer.clone()),
			RowFormat::Json,
		);
		let trip = async {
			tokio::time::sleep(Duration::from_millis(200)).await;
			outer.trip();
		};
		let (ran, ()) =
			tokio::time::timeout(Duration::from_secs(30), async { futures::join!(run, trip) })
				.await
				.expect("the query stops when its connection is cancelled");
		// What the client is answered: the statement failed.
		let failed = match ran {
			Ok((_, answered)) => answered.iter().any(|r| r.result.is_err()),
			Err(_) => true,
		};
		assert!(failed, "a cancelled query does not answer as if it completed");
	}
}
