//! Index iterators for streaming record retrieval.
//!
//! This module provides iterators that efficiently retrieve records from
//! various index types. Each iterator produces batches of records to
//! support the streaming executor model.

pub mod btree;
pub mod count;
pub mod fulltext;
pub mod hnsw;

pub use btree::{
	IndexEqualIterator, IndexRangeIterator, IndexUnionIterator, UniqueEqualIterator,
	UniqueRangeIterator,
};
pub use count::CountIterator;
pub use fulltext::FullTextIterator;
pub use hnsw::KnnIterator;
