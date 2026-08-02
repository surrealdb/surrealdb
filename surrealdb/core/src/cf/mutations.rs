//! Re-export of the change feed's stored shapes.
//!
//! What a change feed entry holds is part of the keyspace, so it is declared below
//! this layer; the reader, the garbage collector and the document write path all
//! read it from there.

pub use surrealdb_datastore::values::changefeed::{ChangeSet, DatabaseMutation, TableMutations};
