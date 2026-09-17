//! `sql` -> `expr` conversions for [`crate::sql::tokenizer`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::tokenizer::*;

impl From<SegmentLanguage> for crate::expr::SegmentLanguage {
	fn from(v: SegmentLanguage) -> Self {
		match v {
			SegmentLanguage::Chinese => Self::Chinese,
			SegmentLanguage::Japanese => Self::Japanese,
			SegmentLanguage::Korean => Self::Korean,
		}
	}
}

impl From<crate::expr::SegmentLanguage> for SegmentLanguage {
	fn from(v: crate::expr::SegmentLanguage) -> Self {
		match v {
			crate::expr::SegmentLanguage::Chinese => Self::Chinese,
			crate::expr::SegmentLanguage::Japanese => Self::Japanese,
			crate::expr::SegmentLanguage::Korean => Self::Korean,
		}
	}
}

impl From<Tokenizer> for crate::expr::Tokenizer {
	fn from(v: Tokenizer) -> Self {
		match v {
			Tokenizer::Blank => Self::Blank,
			Tokenizer::Camel => Self::Camel,
			Tokenizer::Class => Self::Class,
			Tokenizer::Punct => Self::Punct,
			Tokenizer::Segment(l) => Self::Segment(l.into()),
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
			crate::expr::Tokenizer::Segment(l) => Self::Segment(l.into()),
		}
	}
}
