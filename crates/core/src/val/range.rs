use crate::val::Value;
use std::ops::Bound;

pub struct Range {
	pub start: Bound<Value>,
	pub end: Bound<Value>,
}
