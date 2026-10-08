//! Backwards-compatibility coverage for [`DurableSession`]'s revision bump.
//!
//! `DurableSession` gained `data` (the access method's `CONTEXT` payload) at
//! revision 2. Every `/!se{id}` entry an older node wrote is revision 1, and a
//! rolling upgrade decodes those with the new code: if the added field's
//! encoding were wrong in any way, `kv_decode_value` would fail for every
//! attached session on every node — and `revision.lock` would not catch it,
//! because it records that the shape changed, not that old bytes still decode.
//!
//! The round-trip test beside the type encodes and decodes revision-2 bytes
//! with revision-2 code, which by construction cannot see that break. This
//! module holds frozen revision-1 and revision-2 bytes instead.
//!
//! Revision 2 is also the revision a node writes today, and it is the layout
//! every earlier release that reads `DurableSession` decodes. The type keeps
//! its planner-strategy slot, written as the default and ignored on read, so a
//! session written after an upgrade is still readable on rollback: the frozen
//! revision-2 bytes below were produced by the previous release's code, and the
//! current encoder must reproduce them byte for byte.
//!
//! This lives outside the `fixtures.rs`/`vX_Y_Z.rs` machinery next door because
//! that machinery snapshots a whole catalog at a released version, and no
//! released snapshot contains a `DurableSession` at all. Regenerate the constant
//! with the ignored test at the bottom if — and only if — the revision-1 *shape*
//! is ever restated; the bytes themselves must never be edited by hand.

use revision::revisioned;
use surrealdb_datastore::values::session::{DurableSession, NewPlannerStrategy};
use surrealdb_expr::val::{Object, Value};
use surrealdb_iam::Auth;
use uuid::Uuid;

use crate::key::KVValue;

/// `DurableSession` exactly as revision 1 declared it: the current struct minus
/// the `data` field that `#[revision(start = 2)]` added.
///
/// Kept so the frozen bytes below can be regenerated from a readable value
/// rather than transcribed. It is never used outside this module's tests.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, PartialEq)]
struct DurableSessionV1 {
	expires_at: u64,
	au: Auth,
	rt: bool,
	ip: Option<String>,
	or: Option<String>,
	id: Option<Uuid>,
	ns: Option<String>,
	db: Option<String>,
	ac: Option<String>,
	tk: Option<Value>,
	rd: Option<Value>,
	exp: Option<i64>,
	variables: Object,
	new_planner_strategy: NewPlannerStrategy,
	redact_volatile_explain_attrs: bool,
}

/// The value the frozen bytes were captured from: a record-access session with
/// every optional field populated, so a decode break in any of them shows up.
fn durable_session_v1() -> DurableSessionV1 {
	let mut variables = Object::default();
	variables.insert("greeting", Value::from("hello"));

	let mut token = Object::default();
	token.insert("iss", Value::from("surrealdb"));

	DurableSessionV1 {
		expires_at: 123_456_789,
		au: Auth::for_record("person:tobie".to_owned(), "app", "app", "account"),
		rt: true,
		ip: Some("10.0.0.1".to_owned()),
		or: Some("example.com".to_owned()),
		id: Some(Uuid::from_u128(7)),
		ns: Some("app".to_owned()),
		db: Some("app".to_owned()),
		ac: Some("account".to_owned()),
		tk: Some(Value::Object(token)),
		rd: Some(Value::RecordId(surrealdb_expr::val::RecordId::new(
			"person".into(),
			surrealdb_strand::Strand::new("tobie"),
		))),
		exp: Some(1_700_000_000),
		variables,
		new_planner_strategy: NewPlannerStrategy::default(),
		redact_volatile_explain_attrs: true,
	}
}

/// FROZEN. Revision-1 `DurableSession` bytes, as an older node wrote them under
/// `/!se{id}`. Never edit these by hand and never regenerate them to make a
/// failing test pass: a failure here means the current code can no longer read
/// what a previous version wrote.
#[rustfmt::skip]
const DURABLE_SESSION_V1: &[u8] = &[
	1, 252, 21, 205, 91, 7, 1, 1, 1, 12, 112, 101, 114, 115, 111, 110,
	58, 116, 111, 98, 105, 101, 5, 20, 1, 4, 3, 97, 112, 112, 3, 97,
	112, 112, 7, 97, 99, 99, 111, 117, 110, 116, 0, 1, 1, 8, 49, 48,
	46, 48, 46, 48, 46, 49, 1, 11, 101, 120, 97, 109, 112, 108, 101, 46,
	99, 111, 109, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 7, 1, 3, 97, 112, 112, 1, 3, 97, 112, 112, 1, 7,
	97, 99, 99, 111, 117, 110, 116, 1, 2, 74, 27, 0, 0, 0, 2, 22,
	0, 0, 0, 0, 1, 3, 105, 115, 115, 2, 68, 10, 0, 0, 0, 9,
	115, 117, 114, 114, 101, 97, 108, 100, 98, 1, 2, 78, 16, 0, 0, 0,
	1, 6, 112, 101, 114, 115, 111, 110, 1, 1, 5, 116, 111, 98, 105, 101,
	1, 252, 0, 226, 167, 202, 2, 23, 0, 0, 0, 0, 1, 8, 103, 114,
	101, 101, 116, 105, 110, 103, 2, 68, 6, 0, 0, 0, 5, 104, 101, 108,
	108, 111, 1, 0, 1,
];

/// The revision-2 session the frozen bytes below were captured from: the
/// revision-1 session with a `CONTEXT` payload, built as a [`DurableSession`].
fn durable_session_v2() -> DurableSession {
	let v1 = durable_session_v1();
	DurableSession {
		expires_at: v1.expires_at,
		au: v1.au,
		rt: v1.rt,
		ip: v1.ip,
		or: v1.or,
		id: v1.id,
		ns: v1.ns,
		db: v1.db,
		ac: v1.ac,
		tk: v1.tk,
		rd: v1.rd,
		data: Some(Value::from("context")),
		exp: v1.exp,
		variables: v1.variables,
		new_planner_strategy: v1.new_planner_strategy,
		redact_volatile_explain_attrs: v1.redact_volatile_explain_attrs,
	}
}

/// FROZEN. Revision-2 `DurableSession` bytes, written by the release that
/// declared `DurableSession` at revision 2 with its own planner-strategy type.
/// Never edit these by hand and never regenerate them to make a failing test
/// pass: a failure here means the current code can no longer read, or no longer
/// write, the layout that release reads.
#[rustfmt::skip]
const DURABLE_SESSION_V2: &[u8] = &[
	2, 252, 21, 205, 91, 7, 1, 1, 1, 12, 112, 101, 114, 115, 111, 110,
	58, 116, 111, 98, 105, 101, 5, 20, 1, 4, 3, 97, 112, 112, 3, 97,
	112, 112, 7, 97, 99, 99, 111, 117, 110, 116, 0, 1, 1, 8, 49, 48,
	46, 48, 46, 48, 46, 49, 1, 11, 101, 120, 97, 109, 112, 108, 101, 46,
	99, 111, 109, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 7, 1, 3, 97, 112, 112, 1, 3, 97, 112, 112, 1, 7,
	97, 99, 99, 111, 117, 110, 116, 1, 2, 74, 27, 0, 0, 0, 2, 22,
	0, 0, 0, 0, 1, 3, 105, 115, 115, 2, 68, 10, 0, 0, 0, 9,
	115, 117, 114, 114, 101, 97, 108, 100, 98, 1, 2, 78, 16, 0, 0, 0,
	1, 6, 112, 101, 114, 115, 111, 110, 1, 1, 5, 116, 111, 98, 105, 101,
	1, 2, 68, 8, 0, 0, 0, 7, 99, 111, 110, 116, 101, 120, 116, 1,
	252, 0, 226, 167, 202, 2, 23, 0, 0, 0, 0, 1, 8, 103, 114, 101,
	101, 116, 105, 110, 103, 2, 68, 6, 0, 0, 0, 5, 104, 101, 108, 108,
	111, 1, 0, 1,
];

/// Revision-1 bytes must still decode, and the field `#[revision(start = 2)]`
/// added must default to `None` — a session that predates `CONTEXT` clauses.
#[test]
fn v1_durable_session_decodes_with_no_context_payload() {
	let decoded = DurableSession::kv_decode_value(DURABLE_SESSION_V1, ()).unwrap_or_else(|e| {
		panic!(
			"BACKWARDS COMPATIBILITY BROKEN: revision-1 DurableSession bytes no longer decode.\n\
			 Error: {e}\n\n\
			 Every `/!se{{id}}` entry written by a previous version is revision 1. A node that \
			 cannot decode them drops or errors every attached session on upgrade."
		)
	});

	let expected = durable_session_v1();
	assert_eq!(decoded.data, None, "the field added at revision 2 must default to None");
	assert_eq!(decoded.expires_at, expected.expires_at);
	assert_eq!(decoded.au, expected.au);
	assert_eq!(decoded.rt, expected.rt);
	assert_eq!(decoded.ip, expected.ip);
	assert_eq!(decoded.or, expected.or);
	assert_eq!(decoded.id, expected.id);
	assert_eq!(decoded.ns, expected.ns);
	assert_eq!(decoded.db, expected.db);
	assert_eq!(decoded.ac, expected.ac);
	assert_eq!(decoded.tk, expected.tk);
	assert_eq!(decoded.rd, expected.rd);
	assert_eq!(decoded.exp, expected.exp);
	assert_eq!(decoded.variables, expected.variables);
	assert_eq!(decoded.new_planner_strategy, expected.new_planner_strategy);
	assert_eq!(decoded.redact_volatile_explain_attrs, expected.redact_volatile_explain_attrs);
}

/// Decoding revision-1 bytes and writing them back produces revision-2 bytes,
/// with the payload slot encoded in place — mid-struct, exactly where
/// `#[revision(start = 2)]` declares it — and decoding those again is lossless.
///
/// The revision byte moving 1 -> 2 is the normal, intended cost of the bump: a
/// node still on the previous version cannot read what an upgraded node writes.
/// It is asserted rather than assumed so that a change which silently kept
/// writing revision 1 (and therefore silently dropped every payload) fails here.
#[test]
fn re_encoding_an_upgraded_session_writes_revision_2_losslessly() {
	let decoded = DurableSession::kv_decode_value(DURABLE_SESSION_V1, ()).expect("decode");
	let re_encoded = decoded.kv_encode_value().expect("encode");

	assert_eq!(DURABLE_SESSION_V1.first(), Some(&1), "the frozen fixture must be revision 1");
	assert_eq!(re_encoded.first(), Some(&2), "an upgraded node must write revision 2");
	assert_eq!(
		re_encoded.len(),
		DURABLE_SESSION_V1.len() + 1,
		"the only added byte is the absent payload's `None` marker"
	);

	let round_tripped = DurableSession::kv_decode_value(&re_encoded, ()).expect("re-decode");
	assert_eq!(round_tripped, decoded);
}

/// Revision-2 bytes written by the previous release decode, payload included,
/// and the planner-strategy slot they carry is consumed without shifting the
/// fields after it.
#[test]
fn v2_durable_session_decodes() {
	let decoded = DurableSession::kv_decode_value(DURABLE_SESSION_V2, ()).unwrap_or_else(|e| {
		panic!(
			"BACKWARDS COMPATIBILITY BROKEN: revision-2 DurableSession bytes no longer decode.\n\
			 Error: {e}\n\n\
			 Every `/!se{{id}}` entry written by the previous release is revision 2. A node that \
			 cannot decode them drops or errors every attached session on upgrade."
		)
	});

	assert_eq!(DURABLE_SESSION_V2.first(), Some(&2), "the frozen fixture must be revision 2");
	assert_eq!(decoded, durable_session_v2());
}

/// A session written now is revision-2 bytes identical to what the previous
/// release wrote for the same session, so a node rolled back to that release
/// reads it. A revision bump, a dropped or reordered field, or a changed
/// planner-strategy encoding fails here.
#[test]
fn a_session_written_now_is_the_layout_the_previous_release_reads() {
	let encoded = durable_session_v2().kv_encode_value().expect("encode");
	assert_eq!(
		encoded, DURABLE_SESSION_V2,
		"the current encoding of a session must equal the previous release's bytes"
	);
}

/// The planner-strategy slot is written as the default whatever the entry it
/// was restored from carried, and the live session captured into a durable one
/// stays at revision 2.
#[test]
fn a_captured_session_is_written_at_revision_2_with_the_default_strategy() {
	let session = crate::dbs::Session::for_level(crate::iam::Level::Root, crate::iam::Role::Owner);
	let durable = crate::dbs::durable_session(&session, 123_456_789);
	assert_eq!(durable.new_planner_strategy, NewPlannerStrategy::default());

	let encoded = durable.kv_encode_value().expect("encode");
	assert_eq!(encoded.first(), Some(&2), "a node must keep writing revision 2");
	let decoded = DurableSession::kv_decode_value(&encoded, ()).expect("decode");
	assert_eq!(decoded, durable);
}

/// Regenerate [`DURABLE_SESSION_V1`]. Ignored so it never runs in CI:
///
/// ```text
/// cargo test -p surrealdb-core --lib kvs::compat::durable_session::generate -- --ignored --nocapture
/// ```
#[test]
#[ignore = "prints the frozen fixture; run by hand when the revision-1 shape is restated"]
fn generate() {
	let bytes = revision::to_vec(&durable_session_v1()).expect("encode");
	let body = bytes
		.chunks(16)
		.map(|row| {
			let cells: Vec<String> = row.iter().map(|b| b.to_string()).collect();
			format!("\t{},", cells.join(", "))
		})
		.collect::<Vec<_>>()
		.join("\n");
	println!("const DURABLE_SESSION_V1: &[u8] = &[\n{body}\n];");
}

/// Regenerate [`DURABLE_SESSION_V2`] from the type as the previous release
/// declared it. Ignored so it never runs in CI; run it only on a checkout of
/// that release, never on one that has changed the layout:
///
/// ```text
/// cargo test -p surrealdb-core --lib kvs::compat::durable_session::generate_v2 -- --ignored --nocapture
/// ```
#[test]
#[ignore = "prints the frozen fixture; run by hand on the release that wrote revision 2"]
fn generate_v2() {
	let bytes = revision::to_vec(&durable_session_v2()).expect("encode");
	let body = bytes
		.chunks(16)
		.map(|row| {
			let cells: Vec<String> = row.iter().map(|b| b.to_string()).collect();
			format!("\t{},", cells.join(", "))
		})
		.collect::<Vec<_>>()
		.join("\n");
	println!("const DURABLE_SESSION_V2: &[u8] = &[\n{body}\n];");
}
