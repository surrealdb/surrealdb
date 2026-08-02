pub mod analyzer;
pub mod fulltext;
pub mod highlighter;
pub mod offset;

use anyhow::Result;
use surrealdb_datastore::Transaction;
pub(crate) use surrealdb_datastore::values::fulltext::{DocLength, Position, TermFrequency};

use crate::docids::DocId;
use crate::val::RecordId;

pub(super) type Score = f32;

pub use crate::expr::operator::MatchRef;

// The iterator is always driven by the crate above through a concrete type, so
// the future never has to be named and no auto-trait bound on it is needed.
#[allow(async_fn_in_trait)]
// `len` is the hit count the scorer needs, not a container size; emptiness is
// never asked, so there is no `is_empty` to pair it with.
#[allow(clippy::len_without_is_empty)]
pub trait MatchesHitsIterator {
	fn len(&self) -> usize;
	async fn next(&mut self, tx: &Transaction) -> Result<Option<(RecordId, DocId)>>;
}
