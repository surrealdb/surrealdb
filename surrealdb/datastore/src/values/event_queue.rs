//! What a queued asynchronous event carries.
//!
//! `DEFINE EVENT ... ASYNC` does not run its body on the write path. The write
//! enqueues everything the body will need under `!eq` - the document that changed,
//! the context values, the authentication in force, and the event definition as it
//! stood - and a background task drains the queue later, possibly on another node
//! and possibly after a restart. So the payload has to be self-contained, and
//! being stored, it belongs to the keyspace.
//!
//! Nothing here has behaviour: rebuilding a context, an options frame and a cursor
//! document from this payload, and the retry policy that decides whether a failed
//! attempt is requeued, all need the engine and stay with it.

use std::collections::HashMap;
use std::sync::Arc;

use revision::revisioned;
use surrealdb_catalog::{Record, StoredEventDefinition};
use surrealdb_expr::val::{RecordId, Value};
use surrealdb_iam::Auth;
use surrealdb_kvs::impl_kv_value_revisioned;
use surrealdb_strand::Strand;

/// Persisted payload for processing DEFINE EVENT ... ASYNC.
#[revisioned(revision = 1)]
#[derive(Clone, Debug)]
pub struct AsyncEventRecord {
	/// Number of processing attempts already recorded; incremented when a failed
	/// run is requeued and compared against the event retry limit.
	pub attempt: u16,
	/// Async event nesting depth for this record (0 for top-level); used to enforce max_depth.
	pub event_depth: u16,
	/// Record id of the cursor document, if one exists.
	pub rid: Option<Arc<RecordId>>,
	/// Read-only snapshot of the cursor record captured at enqueue time.
	pub cursor_record: Arc<Record>,
	/// Whether computed fields were already evaluated in the snapshot.
	pub fields_computed: bool,
	/// Namespace name captured at enqueue time; re-resolved to validate the queue key.
	pub ns: Arc<str>,
	/// Database name captured at enqueue time; re-resolved to validate the queue key.
	pub db: Arc<str>,
	/// Whether permission checks should run when processing the event.
	pub perms: bool,
	/// Whether authentication is enabled for this event execution.
	pub auth_enabled: bool,
	/// Captured context values (session variables and event inputs like event, value, before,
	/// after, and input) restored for processing.
	pub values: HashMap<Strand, Arc<Value>>,
	/// Auth context with any event-specific limits applied.
	pub auth_with_limit: Arc<Auth>,
	/// Snapshot of the event definition used for execution and retry policy.
	pub event_definition: StoredEventDefinition,
}

impl_kv_value_revisioned!(AsyncEventRecord);
