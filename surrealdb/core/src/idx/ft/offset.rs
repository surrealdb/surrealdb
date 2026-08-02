//! Where a term's position inside a document is declared.
//!
//! The offset is stored inside a full-text posting, so its shape is part of the
//! keyspace and is declared below this layer. The analyzer that produces offsets
//! and the highlighter that consumes them stay here and read it downward.

pub(crate) use surrealdb_datastore::values::fulltext::Offset;
