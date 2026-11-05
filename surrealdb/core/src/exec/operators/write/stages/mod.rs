//! The per-record helpers the VM's write lowering calls, one module per concern.
//!
//! A module answers for one concern of a write: data clauses, guards, index
//! maintenance, live-query delivery, events and change feeds, reference keys,
//! cascading purges, field schema, record storage and view maintenance.

pub(crate) mod data;
pub(crate) mod guard;
pub(crate) mod index;
pub(crate) mod lives;
pub(crate) mod observe;
pub(crate) mod purge;
pub(crate) mod refs;
pub(crate) mod schema;
pub(crate) mod store;
pub(crate) mod view;
