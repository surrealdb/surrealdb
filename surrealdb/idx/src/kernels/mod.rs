//! Kernels: the pure computations an index read or write is made of.
//!
//! A kernel is a function over values and bytes. It never opens a
//! transaction, reads a key or holds a cursor: whatever it needs from storage
//! is read by its caller and handed to it, and whatever it produces — a key, a
//! range of keys, a decoded entry — is handed back for the caller to read or
//! write. That split is what lets an executor see every key range a request
//! reads, meter each step, and split a range across workers, while the
//! computation itself stays in one place.
//!
//! The same kernel serves the read path and the write path. An index entry is
//! written at the key [`btree::IndexKeyspace::stored_key`] encodes, and a scan
//! for the values that entry holds reads the range
//! [`btree::IndexKeyspace::equality`] encodes from the same values, so the two
//! agree byte for byte by construction rather than by keeping two encoders in
//! step.
//!
//! [`btree`] holds the B-tree and `UNIQUE` key kernels, [`docids`] the keys of
//! the reverse doc-ID mapping and the record keys they name, [`fulltext`] the
//! resolution of a query's terms into document sets, their combination and
//! BM25, [`postings`] a term's compacted postings as posting blocks hold them
//! and the edits compaction makes to those blocks, and [`vector`] the exact tier's query conversion
//! and scoring and the distance functions.
//!
//! Every kernel is named in the [`registry`], so a description of what a
//! program may run (a capability manifest) can name the kernels it calls.

pub mod btree;
pub mod docids;
pub mod fulltext;
pub mod postings;
pub mod registry;
pub mod vector;

pub use registry::Kernel;
