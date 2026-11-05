//! The items a streaming query execution produces.
//!
//! A query answered all at once is one [`QueryResult`] per statement. A query
//! answered as it runs is a sequence of these instead, so a transport can put
//! rows on the wire before the statement that produced them has finished.
//!
//! # Rows are provisional until their statement finishes
//!
//! Rows are emitted as they are produced, which is before the statement's
//! outcome is known: it may still fail on a later row, and inside a
//! `BEGIN … COMMIT` block the transaction may still roll back. A statement's
//! [`Finished`](QueryStreamItem::Finished) item is what makes its rows valid,
//! and one carrying an error retracts every row that preceded it.
//!
//! A consumer that buffers until `Finished` never observes this. One that
//! forwards rows as they arrive must be able to retract them.

use std::time::Duration;

use surrealdb_types::{
	Array, Error, Number, Object, SerializationError, SurrealValue, Value, object,
};

use crate::encoded::EncodedRows;
use crate::trace::{Trace, TraceKinds};
use crate::{QueryResult, QueryType};

/// The channel depth a consumer should give a streaming execution.
///
/// Each item is a whole batch of rows, so a small number is already a
/// substantial buffer. Keeping it small is what makes the consumer's read speed
/// the producer's rate limit: a client that stops reading stops the scan,
/// rather than letting it run to completion into memory nobody is draining.
pub const QUERY_STREAM_BUFFER: usize = 2;

/// One item of a streaming query execution.
///
/// A statement contributes either [`Rows`](Self::Rows) items or a single
/// [`Value`](Self::Value) item, never both, and always exactly one
/// [`Finished`](Self::Finished) which terminates it.
#[derive(Debug)]
pub enum QueryStreamItem {
	/// Rows produced by statement `index`; more may follow. The statement's
	/// value is every row it emitted, as an array.
	Rows {
		index: usize,
		values: Vec<Value>,
	},
	/// Rows produced by statement `index`, already encoded for the wire; more
	/// may follow. Produced only for an execution that asked for a
	/// [`RowFormat`](crate::RowFormat), in place of [`Rows`](Self::Rows), and
	/// counts as rows for every other purpose.
	Frame {
		index: usize,
		rows: EncodedRows,
	},
	/// Statement `index` produced a single value that is not a list of rows —
	/// `SELECT ONLY`, `RETURN 1 + 2`, a block.
	Value {
		index: usize,
		value: Value,
	},
	/// Statement `index` is final. Terminal for that index: no further item
	/// carries it. `error` is set when the statement failed, in which case any
	/// rows already emitted for it must be discarded.
	Finished {
		index: usize,
		time: Duration,
		query_type: QueryType,
		error: Option<Error>,
	},
	/// Trace entries for statement `index`, for an execution that asked for a
	/// trace (see [`crate::trace`]). Sent beside the rows items it describes
	/// and before the statement's `Finished`, and retracted with them when
	/// that `Finished` carries an error. A transport carries it inside the
	/// statement's next frame rather than as a message of its own.
	Trace {
		index: usize,
		trace: Box<Trace>,
	},
}

impl QueryStreamItem {
	/// The statement this item belongs to.
	pub fn index(&self) -> usize {
		match self {
			Self::Rows {
				index,
				..
			}
			| Self::Frame {
				index,
				..
			}
			| Self::Value {
				index,
				..
			}
			| Self::Finished {
				index,
				..
			}
			| Self::Trace {
				index,
				..
			} => *index,
		}
	}

	/// The item with encoded rows decoded into values: a
	/// [`Frame`](Self::Frame) becomes the [`Rows`](Self::Rows) it encodes, and
	/// any other item is returned as it is. For a consumer that reads values.
	pub fn decode_frame(self) -> Result<Self, Error> {
		match self {
			Self::Frame {
				index,
				rows,
			} => Ok(Self::Rows {
				index,
				values: rows.to_values()?,
			}),
			other @ Self::Rows {
				..
			}
			| other @ Self::Value {
				..
			}
			| other @ Self::Finished {
				..
			}
			| other @ Self::Trace {
				..
			} => Ok(other),
		}
	}
}

/// Renders a finished [`QueryResult`] as the items a consumer would have seen
/// had it been streamed.
///
/// This is what lets a buffered execution answer a streaming caller: a
/// statement whose rows were never streamed contributes the same items, just
/// all at once. The list-versus-single distinction follows the value's own
/// shape, which is the distinction a consumer rebuilds the result from.
pub fn items_for_result(index: usize, result: QueryResult) -> Vec<QueryStreamItem> {
	let QueryResult {
		time,
		result,
		query_type,
	} = result;
	let (payload, error) = match result {
		Ok(Value::Array(rows)) => (
			Some(QueryStreamItem::Rows {
				index,
				values: rows.into_inner(),
			}),
			None,
		),
		Ok(value) => (
			Some(QueryStreamItem::Value {
				index,
				value,
			}),
			None,
		),
		Err(error) => (None, Some(error)),
	};
	let finished = QueryStreamItem::Finished {
		index,
		time,
		query_type,
		error,
	};
	match payload {
		Some(payload) => vec![payload, finished],
		None => vec![finished],
	}
}

/// The most bytes of trace entries [`Collected`] keeps across all statements,
/// by estimate of the record ids they hold: an answer given all at once
/// carries its whole trace in one reply, so it is bounded where a stream's is
/// not. Past it, a kind stops collecting and is listed as truncated on the
/// statement that reached it.
pub const COLLECTED_TRACE_BYTES: usize = 1 << 20;

/// A streaming execution's items gathered back into the answer a buffered
/// execution gives: one [`QueryResult`] per statement that finished, in order,
/// each with the trace its statement sent, if any.
///
/// The stream's contract is applied as a consumer applies it: a statement's
/// rows and trace stand only once its `Finished` arrives, and one carrying an
/// error retracts them. A statement that sent no `Finished` — one control flow
/// skipped — contributes nothing.
#[derive(Default)]
pub struct Collected {
	statements: Vec<Option<CollectedStatement>>,
	entries: usize,
}

#[derive(Default)]
struct CollectedStatement {
	rows: Vec<Value>,
	single: Option<Value>,
	trace: Option<Trace>,
	/// The bytes of trace entries this statement holds of
	/// [`COLLECTED_TRACE_BYTES`], given back when it fails.
	entries: usize,
	finished: Option<(Duration, QueryType, Option<Error>)>,
}

impl CollectedStatement {
	fn failed(&self) -> bool {
		matches!(self.finished, Some((_, _, Some(_))))
	}
}

impl Collected {
	fn statement(&mut self, index: usize) -> &mut CollectedStatement {
		if index >= self.statements.len() {
			self.statements.resize_with(index + 1, || None);
		}
		self.statements[index].get_or_insert_with(CollectedStatement::default)
	}

	/// Statement `index` failed with `error`: what it produced is retracted, and
	/// the trace entries it held are free for the statements after it.
	fn fail(&mut self, index: usize, time: Duration, query_type: QueryType, error: Error) {
		let statement = self.statement(index);
		statement.finished = Some((time, query_type, Some(error)));
		statement.rows.clear();
		statement.single = None;
		statement.trace = None;
		let released = std::mem::take(&mut statement.entries);
		self.entries -= released;
	}

	/// Fold one item in. Encoded rows are read as the values they encode, and
	/// rows that cannot be read fail their statement.
	pub fn absorb(&mut self, item: QueryStreamItem) {
		match item {
			QueryStreamItem::Rows {
				index,
				values,
			} => self.statement(index).rows.extend(values),
			QueryStreamItem::Frame {
				index,
				rows,
			} => match rows.to_values() {
				Ok(values) => self.statement(index).rows.extend(values),
				Err(error) => self.fail(index, Duration::ZERO, QueryType::Other, error),
			},
			QueryStreamItem::Value {
				index,
				value,
			} => self.statement(index).single = Some(value),
			QueryStreamItem::Trace {
				index,
				trace,
			} => {
				// A statement already failed takes nothing further.
				if self.statement(index).failed() {
					return;
				}
				let mut trace = *trace;
				let room = COLLECTED_TRACE_BYTES.saturating_sub(self.entries);
				let mut truncated = TraceKinds::NONE;
				// Keep entries in order while their bytes fit what is left.
				fn cut<T>(entries: &mut Vec<T>, left: &mut usize, bytes: fn(&T) -> usize) -> bool {
					let mut kept = 0;
					for entry in entries.iter() {
						let size = bytes(entry);
						if size > *left {
							break;
						}
						*left -= size;
						kept += 1;
					}
					let cut = kept < entries.len();
					entries.truncate(kept);
					cut
				}
				let mut left = room;
				if cut(&mut trace.relations, &mut left, Trace::relation_bytes) {
					truncated = truncated.union(TraceKinds::RELATIONS);
				}
				if cut(&mut trace.references, &mut left, Trace::reference_bytes) {
					truncated = truncated.union(TraceKinds::REFERENCES);
				}
				if cut(&mut trace.links, &mut left, Trace::link_bytes) {
					truncated = truncated.union(TraceKinds::LINKS);
				}
				// A variable that does not fit is kept by name, as the statement's
				// own cap keeps one, so the client still sees it was bound.
				let mut kept = 0;
				for variable in &trace.variables {
					let size = Trace::variable_bytes(variable);
					if size > left {
						break;
					}
					left -= size;
					kept += 1;
				}
				if kept < trace.variables.len() {
					for (_, value) in &mut trace.variables[kept..] {
						*value = Value::None;
					}
					truncated = truncated.union(TraceKinds::VARIABLES);
				}
				trace.truncated = trace.truncated.union(truncated);
				self.entries += room - left;
				let statement = self.statement(index);
				statement.entries += room - left;
				match &mut statement.trace {
					Some(held) => held.absorb(trace),
					held @ None => *held = Some(trace),
				}
			}
			QueryStreamItem::Finished {
				index,
				time,
				query_type,
				error,
			} => {
				// Rows that could not be read already failed the statement.
				if self.statement(index).finished.is_some() {
					return;
				}
				match error {
					Some(error) => self.fail(index, time, query_type, error),
					None => self.statement(index).finished = Some((time, query_type, None)),
				}
			}
		}
	}

	/// Each finished statement's result, in order, with its trace.
	pub fn finish(self) -> Vec<(QueryResult, Option<Trace>)> {
		self.statements
			.into_iter()
			.flatten()
			.filter_map(|statement| {
				let (time, query_type, error) = statement.finished?;
				Some(match error {
					Some(error) => (
						QueryResult {
							time,
							result: Err(error),
							query_type,
						},
						None,
					),
					None => (
						QueryResult {
							time,
							result: Ok(statement
								.single
								.unwrap_or_else(|| Value::Array(Array::from(statement.rows)))),
							query_type,
						},
						statement.trace.filter(|trace| !trace.is_empty()),
					),
				})
			})
			.collect()
	}
}

/// The key that tags a WebSocket stream frame object, holding the frame's
/// kind (`"begin"`, `"rows"`, `"value"`, `"finished"` or `"end"`).
///
/// Frames are sent only in answer to a `query_stream` request and every frame
/// carries that request's id, so a client decodes them by looking the id up in
/// its own pending-request state — never by inspecting the shape of an
/// arbitrary response payload, which user data could imitate.
pub const STREAM_FRAME_KEY: &str = "stream";

/// The revision of the value-encoded frame protocol this build speaks, carried
/// on the [`Begin`](QueryStreamFrame::Begin) frame as `version`.
///
/// Scoped to the transports that carry frames as values — WebSocket and the
/// embedded engines. gRPC frames are protobuf messages with their own schema
/// evolution and carry no equivalent field.
///
/// It exists so a client can tell one *shape* of the protocol from another
/// without inspecting frames, and it changes only when a change is not
/// backwards compatible. What is guaranteed within a revision:
///
/// - A `stream` tag is never repurposed. `begin`, `rows`, `value`, `finished` and `end` keep the
///   meanings documented on [`QueryStreamFrame`] for as long as this number does not change.
/// - New tags and new fields may be added. A client must ignore a tag it does not know and a field
///   it does not read, rather than treating either as an error — that is what makes adding one
///   backwards compatible.
/// - Every `finished` frame carries `single`. It is what distinguishes a statement whose value is
///   one bare value from one whose value is a list, and a statement that emitted no payload frame
///   has nothing else to tell them apart, so it is always written — never omitted as a default.
///
/// A client reading frames as objects — as the JavaScript SDK does — compares
/// this when it requires an exact revision; one that only needs the guarantees
/// above can ignore it, since a `begin` frame without a `version` predates the
/// field and speaks revision 1. [`QueryStreamFrame::from_value`] does not carry
/// it into [`Begin`](QueryStreamFrame::Begin), so a client decoding through that
/// type reads it off the object before parsing, or not at all.
pub const QUERY_STREAM_VERSION: i64 = 1;

/// One frame of a streaming query answer on the WebSocket protocol.
///
/// A `query_stream` request is answered by a sequence of these instead of a
/// single response: one [`Begin`](Self::Begin), the per-statement payload and
/// [`Finished`](Self::Finished) frames as execution produces them, and exactly
/// one terminal [`End`](Self::End). Every frame is wrapped in an ordinary
/// response envelope carrying the request's id and session, so it reaches the
/// pending call that asked for it.
///
/// The item contract carries over from [`QueryStreamItem`]: a statement
/// contributes [`Rows`](Self::Rows) frames or one [`Value`](Self::Value)
/// frame, never both; its rows are provisional until its `Finished` frame
/// arrives; and a `Finished` carrying an error retracts every row that
/// preceded it for that statement. A failure that belongs to no single
/// statement is reported on the `End` frame, which retracts every statement
/// that has not already finished.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryStreamFrame {
	/// The stream is open. Sent before execution begins. `statements` is an
	/// upper bound on the statements that will finish: control flow such as a
	/// `RETURN` inside a `BEGIN` block can skip the tail, so count results by
	/// `Finished` frames, never by this.
	///
	/// On the wire this frame also carries [`QUERY_STREAM_VERSION`] as
	/// `version`, which is the whole of the protocol's negotiation: it is not
	/// parsed back here, because a server only ever produces its own revision.
	Begin {
		statements: usize,
	},
	/// Rows produced by statement `index`; more may follow. The statement's
	/// value is every row it emitted, in order, as an array.
	Rows {
		index: usize,
		values: Vec<Value>,
		/// Trace entries of the statement; see [`crate::trace`].
		trace: Option<Box<Trace>>,
	},
	/// Statement `index` produced a single value that is not a list of rows —
	/// `SELECT ONLY`, `RETURN 1 + 2`, a block.
	Value {
		index: usize,
		value: Value,
		/// Trace entries of the statement; see [`crate::trace`].
		trace: Option<Box<Trace>>,
	},
	/// Statement `index` is final. Terminal for that index: no further frame
	/// carries it. `error` set means the statement failed and any rows already
	/// delivered for it must be discarded. `single` distinguishes a statement
	/// whose value is one bare value from one whose value is a list — a
	/// statement that emitted no payload frames is an empty list unless
	/// `single` says otherwise.
	Finished {
		index: usize,
		time: Duration,
		query_type: QueryType,
		single: bool,
		error: Option<Error>,
		/// The statement-level trace, and any entries no earlier frame took;
		/// see [`crate::trace`].
		trace: Option<Box<Trace>>,
	},
	/// The stream is complete; nothing follows. `results` is the number of
	/// statements whose `Finished` frame was delivered, so it agrees with what
	/// the consumer counted.
	///
	/// `error` set means one of two things, and a consumer that acts on
	/// streamed results has to handle both:
	///
	/// - Execution stopped for a reason belonging to no single statement — a timeout, a
	///   cancellation, a transaction that could not commit. Every statement *without* a `Finished`
	///   frame is retracted; the ones that finished stand.
	/// - Something a finished statement produced could not be kept after the fact. A statement that
	///   already finished cannot be retracted — the consumer has its outcome — so the error names
	///   what was lost instead. A `LIVE SELECT` whose subscription was discarded because its
	///   session ended is reported this way, naming the live-query ids that will never deliver.
	End {
		results: usize,
		time: Duration,
		error: Option<Error>,
	},
}

impl QueryStreamFrame {
	/// Renders this frame as the wire value carried in the response envelope.
	///
	/// The shape is an object tagged by [`STREAM_FRAME_KEY`]; durations use
	/// the same human-readable rendering as [`QueryResult`]'s `time` field and
	/// errors use the full wire error object, so every serialization format
	/// the WebSocket protocol supports carries frames with no format-specific
	/// handling.
	pub fn into_value(self) -> Value {
		match self {
			Self::Begin {
				statements,
			} => Value::Object(object! {
				stream: "begin",
				version: QUERY_STREAM_VERSION,
				statements: statements as i64,
			}),
			Self::Rows {
				index,
				values,
				trace,
			} => with_trace(
				object! {
					stream: "rows",
					index: index as i64,
					values: Value::Array(Array::from(values)),
				},
				trace,
			),
			Self::Value {
				index,
				value,
				trace,
			} => with_trace(
				object! {
					stream: "value",
					index: index as i64,
					value: value,
				},
				trace,
			),
			Self::Finished {
				index,
				time,
				query_type,
				single,
				error,
				trace,
			} => {
				let mut map = object! {
					stream: "finished",
					index: index as i64,
					time: format!("{time:?}"),
					type: query_type.into_value(),
					single: single,
				};
				if let Some(error) = error {
					map.insert("error", SurrealValue::into_value(error));
				}
				with_trace(map, trace)
			}
			Self::End {
				results,
				time,
				error,
			} => {
				let mut map = object! {
					stream: "end",
					results: results as i64,
					time: format!("{time:?}"),
				};
				if let Some(error) = error {
					map.insert("error", SurrealValue::into_value(error));
				}
				Value::Object(map)
			}
		}
	}

	/// Parses a frame from its wire value. The inverse of
	/// [`into_value`](Self::into_value).
	pub fn from_value(value: Value) -> Result<Self, Error> {
		let Value::Object(mut map) = value else {
			return Err(frame_error("Expected object for stream frame"));
		};
		let Some(Value::String(tag)) = map.remove(STREAM_FRAME_KEY) else {
			return Err(frame_error("Expected stream tag for stream frame"));
		};
		match tag.as_str() {
			"begin" => Ok(Self::Begin {
				statements: take_index(&mut map, "statements")?,
			}),
			"rows" => {
				let Some(Value::Array(values)) = map.remove("values") else {
					return Err(frame_error("Expected values array for rows frame"));
				};
				Ok(Self::Rows {
					index: take_index(&mut map, "index")?,
					values: values.into_inner(),
					trace: take_trace(&mut map)?,
				})
			}
			"value" => Ok(Self::Value {
				index: take_index(&mut map, "index")?,
				value: map.remove("value").unwrap_or(Value::None),
				trace: take_trace(&mut map)?,
			}),
			"finished" => Ok(Self::Finished {
				index: take_index(&mut map, "index")?,
				time: take_time(&mut map)?,
				query_type: match map.remove("type") {
					Some(value) => QueryType::from_value(value)?,
					None => QueryType::Other,
				},
				single: matches!(map.remove("single"), Some(Value::Bool(true))),
				error: map.remove("error").map(Error::from_value).transpose()?,
				trace: take_trace(&mut map)?,
			}),
			"end" => Ok(Self::End {
				results: take_index(&mut map, "results")?,
				time: take_time(&mut map)?,
				error: map.remove("error").map(Error::from_value).transpose()?,
			}),
			_ => Err(frame_error("Unknown stream frame tag")),
		}
	}
}

/// `map` with `trace` under its key, when there is one to send.
fn with_trace(mut map: Object, trace: Option<Box<Trace>>) -> Value {
	if let Some(trace) = trace.filter(|trace| !trace.is_empty()) {
		map.insert("trace", trace.into_value());
	}
	Value::Object(map)
}

fn take_trace(map: &mut Object) -> Result<Option<Box<Trace>>, Error> {
	map.remove("trace").map(|value| Trace::from_value(value).map(Box::new)).transpose()
}

fn frame_error(msg: &str) -> Error {
	Error::serialization(msg.to_string(), SerializationError::Deserialization)
}

fn take_index(map: &mut Object, key: &str) -> Result<usize, Error> {
	match map.remove(key) {
		Some(Value::Number(Number::Int(n))) if n >= 0 => Ok(n as usize),
		_ => Err(frame_error("Expected a non-negative integer for stream frame field")),
	}
}

fn take_time(map: &mut Object) -> Result<Duration, Error> {
	let Some(Value::String(time)) = map.remove("time") else {
		return Err(frame_error("Expected time string for stream frame"));
	};
	humantime::parse_duration(&time)
		.map_err(|e| Error::serialization(e.to_string(), SerializationError::Deserialization))
}

#[cfg(test)]
mod tests {
	use surrealdb_types::{Array, Number};

	use super::*;

	fn int(n: i64) -> Value {
		Value::Number(Number::Int(n))
	}

	fn ok(value: Value) -> QueryResult {
		QueryResult {
			time: Duration::from_millis(1),
			result: Ok(value),
			query_type: QueryType::Other,
		}
	}

	/// A list result becomes rows, so a consumer rebuilds it as an array; any
	/// other value becomes a single value and is not wrapped.
	#[test]
	fn a_results_shape_decides_its_items() {
		let rows = items_for_result(0, ok(Value::Array(Array::from(vec![int(1)]))));
		assert!(matches!(rows[0], QueryStreamItem::Rows { .. }), "a list becomes rows");

		let single = items_for_result(0, ok(int(1)));
		assert!(matches!(single[0], QueryStreamItem::Value { .. }), "a scalar stays single");
	}

	/// Every statement ends with exactly one `Finished`, and a failed one
	/// carries no payload at all.
	#[test]
	fn every_statement_ends_once() {
		let items = items_for_result(3, ok(int(1)));
		assert_eq!(items.len(), 2);
		assert!(matches!(
			items[1],
			QueryStreamItem::Finished {
				index: 3,
				error: None,
				..
			}
		));

		let failed = items_for_result(
			3,
			QueryResult {
				time: Duration::ZERO,
				result: Err(Error::internal("nope".to_string())),
				query_type: QueryType::Other,
			},
		);
		assert_eq!(failed.len(), 1, "a failed statement has no payload item");
		assert!(matches!(
			failed[0],
			QueryStreamItem::Finished {
				error: Some(_),
				..
			}
		));
	}

	/// Gathered items answer what a buffered execution would, with the trace of
	/// each statement that finished well and none of one that failed.
	#[test]
	fn collected_items_answer_the_buffered_results() {
		let trace = |n: i64| QueryStreamItem::Trace {
			index: 0,
			trace: Box::new(Trace {
				variables: vec![(format!("v{n}"), int(n))],
				..Trace::default()
			}),
		};
		let mut collected = Collected::default();
		collected.absorb(trace(1));
		collected.absorb(QueryStreamItem::Rows {
			index: 0,
			values: vec![int(1), int(2)],
		});
		collected.absorb(QueryStreamItem::Finished {
			index: 0,
			time: Duration::ZERO,
			query_type: QueryType::Other,
			error: None,
		});
		collected.absorb(QueryStreamItem::Trace {
			index: 1,
			trace: Box::new(Trace {
				partial: true,
				..Trace::default()
			}),
		});
		collected.absorb(QueryStreamItem::Finished {
			index: 1,
			time: Duration::ZERO,
			query_type: QueryType::Other,
			error: Some(Error::internal("no".to_string())),
		});
		let results = collected.finish();
		assert_eq!(results.len(), 2);
		assert_eq!(
			results[0].0.result.as_ref().unwrap(),
			&Value::Array(Array::from(vec![int(1), int(2)]))
		);
		assert_eq!(results[0].1.as_ref().unwrap().variables.len(), 1);
		assert!(results[1].0.result.is_err());
		assert!(results[1].1.is_none(), "a failed statement's trace is retracted");
	}

	/// `n` relations whose ids all weigh the same, and how many of them fill
	/// [`COLLECTED_TRACE_BYTES`].
	fn even_relations(n: usize) -> (Vec<crate::trace::Relation>, usize) {
		let relation = |n: usize| crate::trace::Relation {
			r#in: None,
			edge: surrealdb_types::RecordId::new("e", format!("{n:08}")),
			out: None,
		};
		let fill = COLLECTED_TRACE_BYTES / Trace::relation_bytes(&relation(0));
		((0..n).map(relation).collect(), fill)
	}

	/// A statement that fails gives back the trace bytes it held, so the
	/// statements after it are not cut for a trace the answer does not carry.
	#[test]
	fn a_failed_statement_frees_its_share_of_the_bound() {
		let (_, fill) = even_relations(0);
		let full = || {
			Box::new(Trace {
				relations: even_relations(fill).0,
				..Trace::default()
			})
		};
		let mut collected = Collected::default();
		collected.absorb(QueryStreamItem::Trace {
			index: 0,
			trace: full(),
		});
		collected.absorb(QueryStreamItem::Finished {
			index: 0,
			time: Duration::ZERO,
			query_type: QueryType::Other,
			error: Some(Error::internal("no".to_string())),
		});
		collected.absorb(QueryStreamItem::Trace {
			index: 1,
			trace: full(),
		});
		collected.absorb(QueryStreamItem::Finished {
			index: 1,
			time: Duration::ZERO,
			query_type: QueryType::Other,
			error: None,
		});
		let results = collected.finish();
		let trace = results[1].1.as_ref().expect("the second statement keeps its trace");
		assert_eq!(trace.relations.len(), fill);
		assert!(trace.truncated.is_empty());
	}

	/// Past the bound on an answer given at once, entries are cut and their kind
	/// is listed as truncated.
	#[test]
	fn a_collected_trace_is_bounded() {
		let (_, fill) = even_relations(0);
		let (relations, _) = even_relations(fill + 5);
		let mut collected = Collected::default();
		collected.absorb(QueryStreamItem::Trace {
			index: 0,
			trace: Box::new(Trace {
				relations,
				..Trace::default()
			}),
		});
		collected.absorb(QueryStreamItem::Finished {
			index: 0,
			time: Duration::ZERO,
			query_type: QueryType::Other,
			error: None,
		});
		let trace = collected.finish().remove(0).1.unwrap();
		assert_eq!(trace.relations.len(), fill);
		assert!(trace.truncated.contains(TraceKinds::RELATIONS));
	}

	/// Bound variables count toward the bound: past it a variable is kept by
	/// name with `NONE`, and `variables` is truncated.
	#[test]
	fn variables_count_toward_the_collected_bound() {
		let big = Value::String("x".repeat(COLLECTED_TRACE_BYTES / 3));
		let mut collected = Collected::default();
		for index in 0..5 {
			collected.absorb(QueryStreamItem::Trace {
				index,
				trace: Box::new(Trace {
					variables: vec![("v".to_owned(), big.clone())],
					..Trace::default()
				}),
			});
			collected.absorb(QueryStreamItem::Finished {
				index,
				time: Duration::ZERO,
				query_type: QueryType::Other,
				error: None,
			});
		}
		let results = collected.finish();
		let kept =
			results.iter().filter(|(_, t)| t.as_ref().unwrap().variables[0].1 != Value::None);
		assert!(kept.count() < 5, "the bound stops the copies");
		let last = results[4].1.as_ref().unwrap();
		assert_eq!(last.variables, vec![("v".to_owned(), Value::None)]);
		assert!(last.truncated.contains(TraceKinds::VARIABLES));
	}

	/// The bound is in bytes: relations whose ids hold long keys reach it after
	/// a few entries.
	#[test]
	fn long_keys_reach_the_collected_bound_sooner() {
		let relations = (0..20)
			.map(|n| crate::trace::Relation {
				r#in: None,
				edge: surrealdb_types::RecordId::new("e", format!("{n}{}", "x".repeat(100_000))),
				out: None,
			})
			.collect();
		let mut collected = Collected::default();
		collected.absorb(QueryStreamItem::Trace {
			index: 0,
			trace: Box::new(Trace {
				relations,
				..Trace::default()
			}),
		});
		collected.absorb(QueryStreamItem::Finished {
			index: 0,
			time: Duration::ZERO,
			query_type: QueryType::Other,
			error: None,
		});
		let trace = collected.finish().remove(0).1.unwrap();
		assert!(trace.relations.len() < 20, "kept {}", trace.relations.len());
		assert!(trace.truncated.contains(TraceKinds::RELATIONS));
	}

	/// Every frame kind survives the trip through its wire value, including
	/// the optional error and the duration rendering.
	#[test]
	fn every_frame_kind_round_trips() {
		let frames = vec![
			QueryStreamFrame::Begin {
				statements: 3,
			},
			QueryStreamFrame::Rows {
				index: 0,
				values: vec![int(1), int(2)],
				trace: None,
			},
			QueryStreamFrame::Rows {
				index: 1,
				values: vec![],
				trace: None,
			},
			QueryStreamFrame::Value {
				index: 1,
				value: Value::None,
				trace: None,
			},
			QueryStreamFrame::Finished {
				index: 0,
				time: Duration::from_micros(1500),
				query_type: QueryType::Other,
				single: false,
				error: None,
				trace: None,
			},
			QueryStreamFrame::Finished {
				index: 1,
				time: Duration::from_secs(2),
				query_type: QueryType::Live,
				single: true,
				error: Some(Error::internal("boom".to_string())),
				trace: None,
			},
			QueryStreamFrame::End {
				results: 2,
				time: Duration::from_millis(7),
				error: None,
			},
			QueryStreamFrame::End {
				results: 0,
				time: Duration::ZERO,
				error: Some(Error::internal("gone".to_string())),
			},
		];
		for frame in frames {
			let reparsed = QueryStreamFrame::from_value(frame.clone().into_value())
				.expect("frame should round-trip");
			match (&frame, &reparsed) {
				(
					QueryStreamFrame::Finished {
						error: Some(a),
						..
					},
					QueryStreamFrame::Finished {
						error: Some(b),
						..
					},
				)
				| (
					QueryStreamFrame::End {
						error: Some(a),
						..
					},
					QueryStreamFrame::End {
						error: Some(b),
						..
					},
				) => {
					assert_eq!(a.message(), b.message());
				}
				_ => assert_eq!(frame, reparsed),
			}
		}
	}

	/// The frame's wire object must never look like a live notification to a
	/// structural sniffer: notifications are detected by the presence of both
	/// `id` and `action` keys, so no frame may carry that pair.
	#[test]
	fn frames_never_imitate_a_live_notification() {
		let frames = vec![
			QueryStreamFrame::Begin {
				statements: 1,
			},
			QueryStreamFrame::Rows {
				index: 0,
				values: vec![int(1)],
				trace: None,
			},
			QueryStreamFrame::Value {
				index: 0,
				value: int(1),
				trace: None,
			},
			QueryStreamFrame::Finished {
				index: 0,
				time: Duration::ZERO,
				query_type: QueryType::Other,
				single: false,
				error: None,
				trace: None,
			},
			QueryStreamFrame::End {
				results: 1,
				time: Duration::ZERO,
				error: None,
			},
		];
		for frame in frames {
			let Value::Object(map) = frame.into_value() else {
				panic!("frames are objects");
			};
			assert!(
				!(map.contains_key("id") && map.contains_key("action")),
				"a frame carrying both id and action would misparse as a notification"
			);
			assert!(map.contains_key(STREAM_FRAME_KEY), "every frame is tagged");
		}
	}

	/// The two things a client decodes structurally rather than negotiating:
	/// the revision on the opening frame, and `single` on every `finished` one.
	/// A `single` omitted as a default would make a single-valued statement
	/// that emitted no payload frame read as an empty list.
	#[test]
	fn the_wire_carries_the_version_and_always_carries_single() {
		let Value::Object(begin) = (QueryStreamFrame::Begin {
			statements: 1,
		})
		.into_value() else {
			panic!("frames are objects");
		};
		assert_eq!(begin.get("version"), Some(&int(QUERY_STREAM_VERSION)));

		for single in [false, true] {
			let Value::Object(finished) = (QueryStreamFrame::Finished {
				index: 0,
				time: Duration::ZERO,
				query_type: QueryType::Other,
				single,
				error: None,
				trace: None,
			})
			.into_value() else {
				panic!("frames are objects");
			};
			assert_eq!(finished.get("single"), Some(&Value::Bool(single)));
		}
	}
}
