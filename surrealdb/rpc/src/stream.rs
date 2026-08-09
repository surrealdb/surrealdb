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
}

impl QueryStreamItem {
	/// The statement this item belongs to.
	pub fn index(&self) -> usize {
		match self {
			Self::Rows {
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
			} => *index,
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

/// The key that tags a WebSocket stream frame object, holding the frame's
/// kind (`"begin"`, `"rows"`, `"value"`, `"finished"` or `"end"`).
///
/// Frames are sent only in answer to a `query_stream` request and every frame
/// carries that request's id, so a client decodes them by looking the id up in
/// its own pending-request state — never by inspecting the shape of an
/// arbitrary response payload, which user data could imitate.
pub const STREAM_FRAME_KEY: &str = "stream";

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
	Begin {
		statements: usize,
	},
	/// Rows produced by statement `index`; more may follow. The statement's
	/// value is every row it emitted, in order, as an array.
	Rows {
		index: usize,
		values: Vec<Value>,
	},
	/// Statement `index` produced a single value that is not a list of rows —
	/// `SELECT ONLY`, `RETURN 1 + 2`, a block.
	Value {
		index: usize,
		value: Value,
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
				statements: statements as i64,
			}),
			Self::Rows {
				index,
				values,
			} => Value::Object(object! {
				stream: "rows",
				index: index as i64,
				values: Value::Array(Array::from(values)),
			}),
			Self::Value {
				index,
				value,
			} => Value::Object(object! {
				stream: "value",
				index: index as i64,
				value: value,
			}),
			Self::Finished {
				index,
				time,
				query_type,
				single,
				error,
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
				Value::Object(map)
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
				})
			}
			"value" => Ok(Self::Value {
				index: take_index(&mut map, "index")?,
				value: map.remove("value").unwrap_or(Value::None),
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
			},
			QueryStreamFrame::Rows {
				index: 1,
				values: vec![],
			},
			QueryStreamFrame::Value {
				index: 1,
				value: Value::None,
			},
			QueryStreamFrame::Finished {
				index: 0,
				time: Duration::from_micros(1500),
				query_type: QueryType::Other,
				single: false,
				error: None,
			},
			QueryStreamFrame::Finished {
				index: 1,
				time: Duration::from_secs(2),
				query_type: QueryType::Live,
				single: true,
				error: Some(Error::internal("boom".to_string())),
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
			},
			QueryStreamFrame::Value {
				index: 0,
				value: int(1),
			},
			QueryStreamFrame::Finished {
				index: 0,
				time: Duration::ZERO,
				query_type: QueryType::Other,
				single: false,
				error: None,
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
}
