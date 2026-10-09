//! The segmenter of a build without the `cjk-tokenizer` feature.
//!
//! It keeps the API of the lindera-backed segmenter so the analyzer and the
//! segmenter cache compile unchanged, but no value of it can exist: every
//! attempt to build one fails with an error naming the missing feature. A
//! `SEGMENT(<language>)` clause still parses; building the analyzer that holds
//! it fails.

use std::convert::Infallible;
use std::path::Path;

use anyhow::{Result, bail};

use crate::Error;
use crate::expr::tokenizer::SegmentLanguage;

/// One word found inside a span, and the byte range it occupied there.
pub(in crate::ft) struct Word {
	pub(in crate::ft) start: usize,
	pub(in crate::ft) end: usize,
}

/// A segmenter that cannot be constructed.
#[derive(Clone)]
pub(crate) struct Segmenter(Infallible);

impl Segmenter {
	/// Always fails: this build has no segmentation support.
	pub(crate) fn new(language: SegmentLanguage, _dictionary_path: Option<&Path>) -> Result<Self> {
		bail!(Error::AnalyzerError(format!(
			"Cannot segment {language} text: this build was compiled without the `cjk-tokenizer` \
			 feature, which `TOKENIZERS SEGMENT(...)` requires."
		)))
	}

	/// Unreachable, as no value of this type exists.
	pub(in crate::ft) fn segment(&self, _span: &str) -> Result<Vec<Word>> {
		match self.0 {}
	}
}
