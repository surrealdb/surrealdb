use std::ops::Bound;

use js::JsLifetime;
use js::class::Trace;
use js::function::Opt;
use surrealdb_types::ToSql;

use crate::val::{self, Value};

/// A SurrealQL range as seen from a scripting function.
///
/// `start` and `end` are `undefined` for an unbounded side; `startInclusive`
/// and `endInclusive` say whether each present side is part of the range. The
/// constructor's optional third and fourth arguments set them; they default to
/// an inclusive start and an exclusive end, like `start..end`.
#[derive(Clone, Trace, JsLifetime)]
#[js::class]
pub(crate) struct Range {
	#[qjs(skip_trace)]
	pub(crate) value: val::Range,
}

fn bound_value(bound: &Bound<Value>) -> Value {
	match bound {
		Bound::Included(v) | Bound::Excluded(v) => v.clone(),
		Bound::Unbounded => Value::None,
	}
}

#[js::methods]
impl Range {
	// The binding layer hands optional arguments over by value.
	#[allow(clippy::needless_pass_by_value)]
	#[qjs(constructor)]
	pub fn new(
		start: Value,
		end: Value,
		start_inclusive: Opt<bool>,
		end_inclusive: Opt<bool>,
	) -> Self {
		let bound = |v: Value, inclusive: bool| match v {
			Value::None => Bound::Unbounded,
			v if inclusive => Bound::Included(v),
			v => Bound::Excluded(v),
		};
		Self {
			value: val::Range {
				start: bound(start, start_inclusive.0.unwrap_or(true)),
				end: bound(end, end_inclusive.0.unwrap_or(false)),
			},
		}
	}

	#[qjs(get)]
	pub fn start(&self) -> Value {
		bound_value(&self.value.start)
	}

	#[qjs(get)]
	pub fn end(&self) -> Value {
		bound_value(&self.value.end)
	}

	#[qjs(get, rename = "startInclusive")]
	pub fn start_inclusive(&self) -> bool {
		matches!(self.value.start, Bound::Included(_))
	}

	#[qjs(get, rename = "endInclusive")]
	pub fn end_inclusive(&self) -> bool {
		matches!(self.value.end, Bound::Included(_))
	}

	/// Compare two Range instances
	pub fn is(a: &Range, b: &Range) -> bool {
		a.value == b.value
	}

	/// Convert the object to a string
	#[qjs(rename = "toString")]
	pub fn js_to_string(&self) -> String {
		self.value.to_sql()
	}

	/// Convert the object to JSON
	#[qjs(rename = "toJSON")]
	pub fn to_json(&self) -> String {
		self.value.to_sql()
	}
}
