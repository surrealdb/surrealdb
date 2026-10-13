use std::fmt;
use std::fmt::Display;

use surrealdb_types::{SqlFormat, ToSql, write_sql};

/// The language a [`Tokenizer::Segment`] segments with.
///
/// Separate from [`crate::language::Language`], which selects a Snowball
/// stemming algorithm: the two vocabularies do not overlap. Snowball strips
/// suffixes and has no CJK algorithms, while these languages are segmented by
/// dictionary-driven morphological analysis and have no stemmer. Keeping them
/// apart makes `SNOWBALL(KOREAN)` and `SEGMENT(ENGLISH)` unrepresentable
/// rather than merely rejected.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum SegmentLanguage {
	Chinese,
	Japanese,
	Korean,
}

impl SegmentLanguage {
	pub fn as_str(self) -> &'static str {
		match self {
			Self::Chinese => "CHINESE",
			Self::Japanese => "JAPANESE",
			Self::Korean => "KOREAN",
		}
	}
}

impl Display for SegmentLanguage {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str(self.as_str())
	}
}

impl ToSql for SegmentLanguage {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str(self.as_str())
	}
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Tokenizer {
	Blank,
	Camel,
	Class,
	Punct,
	/// Splits a span into words using a morphological dictionary.
	///
	/// Unlike the character-class tokenizers, which decide a boundary from the
	/// pair of characters around it, this one needs the whole span at once: the
	/// boundaries come from a lattice search over dictionary entries. It
	/// therefore runs after the character-class tokenizers have produced spans,
	/// segmenting each one further.
	Segment(SegmentLanguage),
}

impl Display for Tokenizer {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Blank => f.write_str("BLANK"),
			Self::Camel => f.write_str("CAMEL"),
			Self::Class => f.write_str("CLASS"),
			Self::Punct => f.write_str("PUNCT"),
			Self::Segment(l) => write!(f, "SEGMENT({l})"),
		}
	}
}

impl ToSql for Tokenizer {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		match self {
			Self::Blank => f.push_str("BLANK"),
			Self::Camel => f.push_str("CAMEL"),
			Self::Class => f.push_str("CLASS"),
			Self::Punct => f.push_str("PUNCT"),
			Self::Segment(l) => write_sql!(f, sql_fmt, "SEGMENT({})", l),
		}
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
