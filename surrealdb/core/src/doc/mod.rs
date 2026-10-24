//! The record-shaped types the engine shares: the cursor over a record being
//! evaluated, the catalog scopes a write resolves, the failures a record write
//! can raise, and the drain of the async event queue.

pub(crate) use self::document::*;
pub(crate) use self::error::Error;
pub(crate) use self::event::RunningEvents;
#[cfg(test)]
pub(crate) use self::event::process_events_batch;
pub use self::event::{EventBatch, FailedBatch, process_next_events_batch};

mod document; // The record cursor and the catalog scopes a write resolves
mod error; // The failures raised while validating and writing a document

pub(crate) mod alter; // The shape of an evaluated data clause
mod event; // Drains the queue of ASYNC table events
mod inline; // Keeps inline edge-property payloads in step with the record
pub(crate) use inline::{encode_inline_section, inline_fields};

#[cfg(test)]
mod tests; // Datastore-backed tests for the document processing pipeline

/// A type of action that triggers an event or view update
#[derive(Clone, Debug, Eq, PartialEq, Hash, Copy)]
pub(crate) enum Action {
	Create,
	Update,
	Delete,
}
