use crate::sql::range::TypedRange;
use crate::sql::{Block, Param, SqlValue};

use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct ForeachStatement {
	pub param: Param,
	pub range: SqlValue,
	pub block: Block,
}

enum ForeachIter {
	Array(std::vec::IntoIter<SqlValue>),
	Range(std::iter::Map<TypedRange<i64>, fn(i64) -> SqlValue>),
}

impl Iterator for ForeachIter {
	type Item = SqlValue;

	fn next(&mut self) -> Option<Self::Item> {
		match self {
			ForeachIter::Array(iter) => iter.next(),
			ForeachIter::Range(iter) => iter.next(),
		}
	}
}

impl Display for ForeachStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "FOR {} IN {} {}", self.param, self.range, self.block)
	}
}

impl From<ForeachStatement> for crate::expr::statements::ForeachStatement {
	fn from(v: ForeachStatement) -> Self {
		Self {
			param: v.param.into(),
			range: v.range.into(),
			block: v.block.into(),
		}
	}
}

impl From<crate::expr::statements::ForeachStatement> for ForeachStatement {
	fn from(v: crate::expr::statements::ForeachStatement) -> Self {
		Self {
			param: v.param.into(),
			range: v.range.into(),
			block: v.block.into(),
		}
	}
}
