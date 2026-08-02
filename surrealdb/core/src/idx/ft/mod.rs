pub(crate) mod analyzer;
pub(crate) mod fulltext;
pub(crate) mod highlighter;
pub(crate) mod offset;

pub(crate) use surrealdb_datastore::values::fulltext::{DocLength, Position, TermFrequency};

pub(super) type Score = f32;

pub(crate) use crate::expr::operator::MatchRef;
