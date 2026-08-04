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

use surrealdb_types::{Error, Value};

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
}
