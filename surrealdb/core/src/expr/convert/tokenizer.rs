//! `sql` -> `expr` conversions for [`crate::sql::tokenizer`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::tokenizer::*;

impl From<Tokenizer> for crate::expr::Tokenizer {
	fn from(v: Tokenizer) -> Self {
		match v {
			Tokenizer::Blank => Self::Blank,
			Tokenizer::Camel => Self::Camel,
			Tokenizer::Class => Self::Class,
			Tokenizer::Punct => Self::Punct,
		}
	}
}

impl From<crate::expr::Tokenizer> for Tokenizer {
	fn from(v: crate::expr::Tokenizer) -> Self {
		match v {
			crate::expr::Tokenizer::Blank => Self::Blank,
			crate::expr::Tokenizer::Camel => Self::Camel,
			crate::expr::Tokenizer::Class => Self::Class,
			crate::expr::Tokenizer::Punct => Self::Punct,
		}
	}
}
