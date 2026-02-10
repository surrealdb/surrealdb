//! Index iterators for streaming record retrieval.
//!
//! This module provides iterators that efficiently retrieve records from
//! various index types. Each iterator produces batches of records to
//! support the streaming executor model.

pub mod btree;

pub use btree::{IndexEqualIterator, IndexRangeIterator, UniqueEqualIterator, UniqueRangeIterator};
