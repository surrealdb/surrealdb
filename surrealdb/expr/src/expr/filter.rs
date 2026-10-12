use common::fmt::QuoteStr;
use revision::revisioned;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::expr::language::Language;
use crate::val::stored::schema::Stored;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash, Stored)]
#[stored(since = "4.0")]
pub enum Filter {
	#[stored(n = 1)]
	Ascii,
	#[stored(n = 2)]
	EdgeNgram(u16, u16),
	#[stored(n = 3)]
	Lowercase,
	#[stored(n = 4)]
	Ngram(u16, u16),
	#[stored(n = 5)]
	Snowball(Language),
	#[stored(n = 6)]
	Uppercase,
	#[stored(n = 7)]
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
