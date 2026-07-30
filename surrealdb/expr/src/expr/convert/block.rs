//! `sql` -> `expr` conversions for [`crate::sql::block`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::block::*;

impl From<Block> for crate::expr::Block {
	fn from(v: Block) -> Self {
		crate::expr::Block(v.0.into_iter().map(Into::into).collect())
	}
}

impl From<crate::expr::Block> for Block {
	fn from(v: crate::expr::Block) -> Self {
		Block(v.0.into_iter().map(Into::into).collect())
	}
}
