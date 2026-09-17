use std::fmt;
use std::fmt::Display;

use revision::revisioned;

/// The language a [`Tokenizer::Segment`] segments with.
///
/// Separate from [`crate::expr::Language`], which selects a Snowball stemming
/// algorithm: the two vocabularies do not overlap. Snowball strips suffixes and
/// has no CJK algorithms, while these languages are segmented by
/// dictionary-driven morphological analysis and have no stemmer. Keeping them
/// apart makes `SNOWBALL(KOREAN)` and `SEGMENT(ENGLISH)` unrepresentable rather
/// than merely rejected.
#[revisioned(revision = 1)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
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

#[revisioned(revision = 2)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
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
	#[revision(start = 2)]
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
