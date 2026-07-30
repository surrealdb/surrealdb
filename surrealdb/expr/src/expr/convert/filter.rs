//! `sql` -> `expr` conversions for [`crate::sql::filter`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::filter::*;

impl From<Filter> for crate::expr::Filter {
	fn from(v: Filter) -> Self {
		match v {
			Filter::Ascii => Self::Ascii,
			Filter::EdgeNgram(min, max) => Self::EdgeNgram(min, max),
			Filter::Lowercase => Self::Lowercase,
			Filter::Ngram(min, max) => Self::Ngram(min, max),
			Filter::Snowball(lang) => Self::Snowball(lang.into()),
			Filter::Uppercase => Self::Uppercase,
			Filter::Mapper(path) => Self::Mapper(path),
		}
	}
}

impl From<crate::expr::Filter> for Filter {
	fn from(v: crate::expr::Filter) -> Self {
		match v {
			crate::expr::Filter::Ascii => Self::Ascii,
			crate::expr::Filter::EdgeNgram(min, max) => Self::EdgeNgram(min, max),
			crate::expr::Filter::Lowercase => Self::Lowercase,
			crate::expr::Filter::Ngram(min, max) => Self::Ngram(min, max),
			crate::expr::Filter::Snowball(lang) => Self::Snowball(lang.into()),
			crate::expr::Filter::Uppercase => Self::Uppercase,
			crate::expr::Filter::Mapper(path) => Self::Mapper(path),
		}
	}
}
