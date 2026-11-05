//! Re-export of the captured live-query event shapes.
//!
//! What a live-query capture persists is part of the keyspace, so it is declared
//! below this layer; the router and the subscriber-side matching read it from
//! there.

pub(crate) use surrealdb_datastore::values::live_query::{LiveAction, LiveEvent, LiveEvents};
