use crate::val::Value;
use std::ops::Bound;

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Range {
	pub start: Bound<Value>,
	pub end: Bound<Value>,
}
