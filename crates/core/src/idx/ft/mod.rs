use serde::{Deserialize, Serialize};

pub(crate) mod analyzer;
pub(crate) mod fulltext;
pub(crate) mod highlighter;
mod offset;
pub(crate) mod search;

pub(super) type Position = u32;
pub(super) type DocLength = u64;
pub(super) type TermFrequency = u64;
pub(super) type Score = f32;

pub(crate) type MatchRef = u8;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
pub(crate) enum SearchOperator {
	AND,
	OR,
}
