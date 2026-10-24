//! What a queued asynchronous event carries.
//!
//! `DEFINE EVENT ... ASYNC` does not run its body on the write path. The write
//! enqueues everything the body will need under `!eq` - the document that changed,
//! the context values, the authentication in force, and the event definition as it
//! stood - and a background task drains the queue later, possibly on another node
//! and possibly after a restart. So the payload has to be self-contained, and
//! being stored, it belongs to the keyspace.
//!
//! A processor claims an entry — records itself on it — before it runs the
//! body, so the claim is part of the payload too.
//!
//! Nothing here has behaviour: rebuilding a context, an options frame and a cursor
//! document from this payload, the rule for how long a claim holds, and the retry
//! policy that decides whether a failed attempt is requeued, all need the engine
//! and stay with it.

use std::collections::HashMap;
use std::sync::Arc;

use revision::revisioned;
use surrealdb_catalog::stored::EmbeddedEntry;
use surrealdb_catalog::{Record, StoredEventDefinition};
use surrealdb_expr::val::{RecordId, Value};
use surrealdb_iam::Auth;
use surrealdb_strand::Strand;
use uuid::Uuid;

/// The processor holding a queued event to run it.
///
/// Written onto the entry before its body is evaluated, so that a second
/// processor fetching the same entry can tell it is held. A claim is honoured
/// for a bounded time, after which its holder is presumed to have died with
/// it; the bound and the rule live with the drain.
#[revisioned(revision = 1)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EventClaim {
	/// The node that took the entry.
	pub node: Uuid,
	/// When it took the entry: unix milliseconds on that node's clock.
	pub at: u64,
}

/// Persisted payload for processing DEFINE EVENT ... ASYNC.
#[revisioned(revision = 2)]
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
	///
	/// Held through [`EmbeddedEntry`] so that a definition `revision` cannot hold travels in the
	/// stored grammar.
	pub event_definition: EmbeddedEntry<StoredEventDefinition>,
	/// The processor holding the entry to run it, if one does: `None` on
	/// enqueue and after a requeue, which releases the entry for its next
	/// attempt.
	#[revision(start = 2)]
	pub claim: Option<EventClaim>,
}

impl surrealdb_kvs::value::KVValue for AsyncEventRecord {
	type KeyContext = ();

	/// Encodes an unclaimed record at revision 1 so that a build predating
	/// [`EventClaim`] can decode any unclaimed entry (including one requeued,
	/// released, or handed back to the root queue during a rollback), and
	/// encodes a claimed record at revision 2.
	#[inline]
	fn kv_encode_value(&self) -> ::anyhow::Result<Vec<u8>> {
		let mut bytes = ::revision::to_vec(self)?;
		if self.claim.is_none() && bytes.first() == Some(&2) && bytes.last() == Some(&0) {
			bytes[0] = 1;
			bytes.pop();
		}
		Ok(bytes)
	}

	#[inline]
	fn kv_decode_value(bytes: &[u8], _: ()) -> ::anyhow::Result<Self> {
		::revision::from_slice(bytes).map_err(|error| {
			let error = ::anyhow::Error::from(error);
			match error.chain().find_map(surrealdb_catalog::stored::EntryError::of_cause) {
				Some(entry) => surrealdb_catalog::Error::Entry(entry).into(),
				None => error,
			}
		})
	}
}

#[cfg(test)]
mod tests {
	use surrealdb_catalog::auth::AuthLimit;
	use surrealdb_catalog::{EventKind, ExprText};
	use surrealdb_iam::Role;

	use super::*;

	fn record() -> AsyncEventRecord {
		AsyncEventRecord {
			attempt: 2,
			event_depth: 1,
			rid: Some(Arc::new(RecordId::new("t".into(), 7i64))),
			cursor_record: Arc::new(Record {
				metadata: None,
				data: Value::Bool(true),
			}),
			fields_computed: true,
			ns: Arc::from("ns"),
			db: Arc::from("db"),
			perms: true,
			auth_enabled: false,
			values: HashMap::from([(Strand::new("k"), Arc::new(Value::Bool(false)))]),
			auth_with_limit: Arc::new(Auth::for_root(Role::Viewer)),
			event_definition: EmbeddedEntry::from(StoredEventDefinition {
				name: Strand::new("ev"),
				target_table: Strand::new("t"),
				when: ExprText::from_raw("$event != NONE"),
				then: vec![ExprText::from_raw("$value.a")],
				comment: None,
				auth_limit: AuthLimit::new_no_limit(),
				kind: EventKind::Async {
					retry: 3,
					max_depth: 4,
				},
			}),
			claim: None,
		}
	}

	fn hex(bytes: &[u8]) -> String {
		bytes.iter().map(|byte| format!("{byte:02x}")).collect()
	}

	/// The payload's bytes are part of the keyspace: a queued record written by one build is
	/// drained by another.
	#[test]
	fn async_event_record_bytes_are_frozen() {
		const FROZEN: &str = "0202010101017401000e020c00000008000000090000000002220101026e73026462010001016b0222000101010b73797374656d5f61757468051401010101000302657601740e246576656e7420213d204e4f4e4501082476616c75652e610001010101054f776e65720101030400";
		let rec = record();
		let bytes = revision::to_vec(&rec).unwrap();
		assert_eq!(hex(&bytes), FROZEN);

		let back: AsyncEventRecord = revision::from_slice(&bytes).unwrap();
		assert_eq!(back.attempt, rec.attempt);
		assert_eq!(back.event_depth, rec.event_depth);
		assert_eq!(back.rid, rec.rid);
		assert_eq!(back.cursor_record, rec.cursor_record);
		assert_eq!(back.fields_computed, rec.fields_computed);
		assert_eq!(back.ns, rec.ns);
		assert_eq!(back.db, rec.db);
		assert_eq!(back.perms, rec.perms);
		assert_eq!(back.auth_enabled, rec.auth_enabled);
		assert_eq!(back.values, rec.values);
		assert_eq!(back.auth_with_limit, rec.auth_with_limit);
		assert_eq!(back.event_definition, rec.event_definition);
		assert_eq!(back.claim, rec.claim);
		assert_eq!(revision::to_vec(&back).unwrap(), bytes);
	}

	/// A record queued at revision 1, before an entry could carry a claim, is drained by a build
	/// that writes revision 2: it decodes unclaimed, with every other field as it was written.
	#[test]
	fn an_async_event_record_from_revision_1_decodes_unclaimed() {
		const REVISION_1: &str = "0102010101017401000e020c00000008000000090000000002220101026e73026462010001016b0222000101010b73797374656d5f61757468051401010101000302657601740e246576656e7420213d204e4f4e4501082476616c75652e610001010101054f776e657201010304";
		let bytes: Vec<u8> = (0..REVISION_1.len())
			.step_by(2)
			.map(|i| u8::from_str_radix(&REVISION_1[i..i + 2], 16).unwrap())
			.collect();
		let back: AsyncEventRecord = revision::from_slice(&bytes).unwrap();
		let rec = record();
		assert_eq!(back.claim, None);
		assert_eq!(back.attempt, rec.attempt);
		assert_eq!(back.rid, rec.rid);
		assert_eq!(back.values, rec.values);
		assert_eq!(back.auth_with_limit, rec.auth_with_limit);
		assert_eq!(back.event_definition, rec.event_definition);
		assert_eq!(hex(&revision::to_vec(&back).unwrap()), hex(&revision::to_vec(&rec).unwrap()));
	}
}
