pub(crate) mod analyzer;
pub(crate) mod fulltext;
pub(crate) mod highlighter;
pub(crate) mod offset;
pub(crate) mod search;

pub(crate) type Position = u32;
pub(crate) type DocLength = u64;
pub(crate) type TermFrequency = u64;
pub(crate) type Score = f32;
