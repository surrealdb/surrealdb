//! The value stored under a b-tree index entry.
//!
//! Its shape is part of the keyspace and is declared below this layer. The index
//! operations that write and read entries stay here.

pub(crate) use surrealdb_datastore::values::entry::IndexEntryValue;
