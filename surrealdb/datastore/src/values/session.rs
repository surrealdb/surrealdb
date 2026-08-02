//! What an attached client session persists.
//!
//! An RPC session outlives the process that attached it: it is stored under
//! `/!se{id}` so any node sharing the datastore can serve a reconnecting client.
//! That makes its stored form part of the keyspace, while the live session it
//! mirrors belongs to the layer that talks to clients.

use revision::revisioned;
use surrealdb_expr::val::{Object, Value};
use surrealdb_iam::Auth;
use surrealdb_kvs::impl_kv_value_revisioned;
use surrealdb_rpc::capabilities::NewPlannerStrategy;
use uuid::Uuid;

/// The durable form of a client-attached RPC session, stored under `/!se{id}` so
/// the session survives the process that attached it and is reachable from any
/// cluster node sharing the datastore.
///
/// The live session is serde-only and holds public value types, while every
/// stored KV value is `revision`-encoded — so this mirror carries the same fields
/// converted to their internal revisioned forms, plus the absolute expiry of the
/// durable copy. The conversion in both directions lives with the live session.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, PartialEq)]
pub struct DurableSession {
	/// When the durable copy expires, in milliseconds since the UNIX epoch.
	/// Enforced lazily on load and by the periodic purge task. Unrelated to
	/// the authentication expiry in `exp`, which stays enforced at query time.
	pub expires_at: u64,
	/// The session [`Auth`] information
	pub au: Auth,
	/// Whether realtime queries are supported
	pub rt: bool,
	/// The connection IP address
	pub ip: Option<String>,
	/// The connection origin
	pub or: Option<String>,
	/// The session ID
	pub id: Option<Uuid>,
	/// The selected namespace
	pub ns: Option<String>,
	/// The selected database
	pub db: Option<String>,
	/// The access method
	pub ac: Option<String>,
	/// The authentication token
	pub tk: Option<Value>,
	/// The record authentication data
	pub rd: Option<Value>,
	/// The expiration time of the session authentication
	pub exp: Option<i64>,
	/// The variables set on the session
	pub variables: Object,
	/// Strategy for the new streaming planner/executor
	pub new_planner_strategy: NewPlannerStrategy,
	/// When true, EXPLAIN ANALYZE output omits elapsed durations
	pub redact_volatile_explain_attrs: bool,
}

impl_kv_value_revisioned!(DurableSession);
