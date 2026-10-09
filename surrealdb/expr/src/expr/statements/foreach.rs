use common::range::IntegerRangeIter;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::expr::{Block, CoverStmts, Expr, Param, Value};

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

impl ToSql for ForeachStatement {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		write_sql!(f, sql_fmt, "FOR {} IN {} {}", self.param, CoverStmts(&self.range), self.block)
	}
}
