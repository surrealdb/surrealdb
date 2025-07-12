pub(crate) mod analyzer;
pub(crate) mod fulltext;
pub(crate) mod highlighter;
mod offset;
pub(crate) mod search;

pub(super) type Position = u32;
pub(super) type DocLength = u64;
pub(super) type TermFrequency = u64;
