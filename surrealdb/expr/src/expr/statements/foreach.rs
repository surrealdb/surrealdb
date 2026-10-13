use common::range::IntegerRangeIter;

use crate::expr::{Block, Expr, Param, Value};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ForeachStatement {
	pub param: Param,
	pub range: Expr,
	pub block: Block,
}

pub enum ForeachIter {
	Array(std::vec::IntoIter<Value>),
	Range(std::iter::Map<IntegerRangeIter, fn(i64) -> Value>),
}

impl Iterator for ForeachIter {
	type Item = Value;

	fn next(&mut self) -> Option<Self::Item> {
		match self {
			ForeachIter::Array(iter) => iter.next(),
			ForeachIter::Range(iter) => iter.next(),
		}
	}
}

impl ForeachStatement {
	/// Check if we require a writeable transaction
	pub fn read_only(&self) -> bool {
		self.range.read_only() && self.block.read_only()
	}
}
