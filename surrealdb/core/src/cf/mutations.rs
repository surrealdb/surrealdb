//! The change feed's stored shapes, bound to core's own `crate::cf::…` paths.
//!
//! What a change feed entry holds is part of the keyspace, so it is declared below
//! this layer, in [`surrealdb_datastore`]; the reader, the garbage collector and
//! the document write path all read it from there.

pub(crate) use surrealdb_datastore::values::changefeed::{
	ChangeSet, DatabaseMutation, TableMutations,
};
