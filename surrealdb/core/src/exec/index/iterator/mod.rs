//! Index iterators for streaming record retrieval.
//!
//! This module provides iterators that efficiently retrieve records from
//! various index types. Each iterator produces batches of records to
//! support the engine model.

pub mod btree;
