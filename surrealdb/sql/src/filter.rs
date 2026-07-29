use common::fmt::QuoteStr;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::language::Language;

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Filter {
	Ascii,
	EdgeNgram(u16, u16),
	Lowercase,
	Ngram(u16, u16),
	Snowball(Language),
	Uppercase,
	Mapper(String),
}

impl ToSql for Filter {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			Self::Ascii => f.push_str("ASCII"),
			Self::EdgeNgram(min, max) => write_sql!(f, fmt, "EDGENGRAM({min},{max})"),
			Self::Lowercase => f.push_str("LOWERCASE"),
			Self::Ngram(min, max) => write_sql!(f, fmt, "NGRAM({min},{max})"),
			Self::Snowball(lang) => write_sql!(f, fmt, "SNOWBALL({lang})"),
			Self::Uppercase => f.push_str("UPPERCASE"),
			Self::Mapper(path) => write_sql!(f, fmt, "MAPPER({})", QuoteStr(path)),
		}
	}
}
