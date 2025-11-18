use std::fmt::{self, Display};
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::sql::{Block, Expr, Param};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct ForeachStatement {
	pub param: Param,
	pub range: Expr,
	pub block: Block,
}

impl Display for ForeachStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "FOR {} IN {} {}", self.param, self.range, self.block)
	}
}

impl ToSql for ForeachStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, "FOR {} IN ", self.param);
		self.range.fmt_sql(f, fmt);
		f.push(' ');
		self.block.fmt_sql(f, fmt);
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
