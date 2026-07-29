use std::fmt;
use std::fmt::Display;

use surrealdb_types::{SqlFormat, ToSql, write_sql};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Tokenizer {
	Blank,
	Camel,
	Class,
	Punct,
}

impl Display for Tokenizer {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str(match self {
			Self::Blank => "BLANK",
			Self::Camel => "CAMEL",
			Self::Class => "CLASS",
			Self::Punct => "PUNCT",
		})
	}
}

impl ToSql for Tokenizer {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		write_sql!(
			f,
			sql_fmt,
			"{}",
			match self {
				Self::Blank => "BLANK",
				Self::Camel => "CAMEL",
				Self::Class => "CLASS",
				Self::Punct => "PUNCT",
			}
		)
	}
}

/// Writes tokenizer keywords separated by commas (no spaces), as in `TOKENIZERS BLANK,CAMEL`.
pub fn write_tokenizers_sql<I>(f: &mut String, sql_fmt: SqlFormat, tokenizers: I)
where
	I: IntoIterator<Item = Tokenizer>,
{
	for (i, t) in tokenizers.into_iter().enumerate() {
		if i > 0 {
			f.push(',');
		}
		t.fmt_sql(f, sql_fmt);
	}
}
