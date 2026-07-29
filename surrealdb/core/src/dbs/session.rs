use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::Result;
use chrono::Utc;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use surrealdb_types::ToSql;
use uuid::Uuid;

use crate::iam::{Auth, Level, Role};
use crate::key::impl_kv_value_revisioned;
use crate::types::{PublicValue, PublicVariables};
use crate::val::{Object, Value};

/// Caller-supplied session input for one WebSocket connection or one HTTP/RPC request.
///
/// **Lifetime:** shared by many queries on that connection or request.
/// **Source of truth:** JWT/basic auth, RPC headers, `USE` namespace/database, variables.
///
/// At the start of work, [`crate::kvs::Datastore::setup_options`] derives the stack-local
/// [`crate::dbs::Options`] frame; [`crate::ctx::Context::attach_session`] copies tenant identity
/// and realtime capability into ambient [`crate::ctx::Context`].
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Session {
	/// The current session [`Auth`] information
	pub au: Arc<Auth>,
	/// Whether realtime queries are supported
	pub rt: bool,
	/// The current connection IP address
	pub ip: Option<String>,
	/// The current connection origin
	pub or: Option<String>,
	/// The current session ID
	pub id: Option<Uuid>,
	/// The currently selected namespace
	pub ns: Option<String>,
	/// The currently selected database
	pub db: Option<String>,
	/// The current access method
	pub ac: Option<String>,
	/// The current authentication token
	pub tk: Option<PublicValue>,
	/// The current record authentication data
	pub rd: Option<PublicValue>,
	/// The current expiration time of the session
	pub exp: Option<i64>,
	/// The variables set
	pub variables: PublicVariables,
	/// Strategy for the new streaming planner/executor.
	pub new_planner_strategy: NewPlannerStrategy,
	/// When true, EXPLAIN ANALYZE output omits elapsed durations, making
	/// output deterministic for testing.
	pub redact_volatile_explain_attrs: bool,
}

#[revisioned(revision = 1)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum NewPlannerStrategy {
	/// Try the new planner for read-only statements, fall back to compute on Unimplemented.
	#[default]
	BestEffortReadOnlyStatements,
	/// Skip the new planner entirely; always use the compute executor.
	ComputeOnly,
	/// Require the new planner for all read-only statements.
	/// Promotes `exec::Error::PlannerUnimplemented` to `exec::Error::Query` (hard error)
	/// instead of falling back.
	AllReadOnlyStatements,
}

impl fmt::Display for NewPlannerStrategy {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::BestEffortReadOnlyStatements => f.write_str("best-effort"),
			Self::ComputeOnly => f.write_str("compute-only"),
			Self::AllReadOnlyStatements => f.write_str("all-read-only"),
		}
	}
}

impl FromStr for NewPlannerStrategy {
	type Err = String;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"best-effort" => Ok(Self::BestEffortReadOnlyStatements),
			"compute-only" => Ok(Self::ComputeOnly),
			"all-read-only" => Ok(Self::AllReadOnlyStatements),
			_ => Err(format!(
				"unknown planner strategy: '{s}' (expected 'best-effort', 'compute-only', or 'all-read-only')"
			)),
		}
	}
}

impl Session {
	/// Set the selected namespace for the session
	pub fn with_ns(mut self, ns: &str) -> Session {
		self.ns = Some(ns.to_owned());
		self
	}

	/// Set the selected database for the session
	pub fn with_db(mut self, db: &str) -> Session {
		self.db = Some(db.to_owned());
		self
	}

	/// Set the selected access method for the session
	pub fn with_ac(mut self, ac: &str) -> Session {
		self.ac = Some(ac.to_owned());
		self
	}

	// Set the realtime functionality of the session
	pub fn with_rt(mut self, rt: bool) -> Session {
		self.rt = rt;
		self
	}

	/// Set the new planner strategy for the session
	pub fn new_planner_strategy(mut self, strategy: NewPlannerStrategy) -> Session {
		self.new_planner_strategy = strategy;
		self
	}

	/// Retrieves the selected namespace
	pub(crate) fn ns(&self) -> Option<Arc<str>> {
		self.ns.as_deref().map(Into::into)
	}

	/// Retrieves the selected database
	pub(crate) fn db(&self) -> Option<Arc<str>> {
		self.db.as_deref().map(Into::into)
	}

	/// Checks if live queries are allowed
	pub(crate) fn live(&self) -> bool {
		self.rt
	}

	/// Checks if the session has expired
	pub(crate) fn expired(&self) -> bool {
		match self.exp {
			Some(exp) => Utc::now().timestamp() > exp,
			// It is currently possible to have sessions without expiration.
			None => false,
		}
	}

	pub(crate) fn values(&self) -> Vec<(&'static str, Value)> {
		use crate::val::convert_public::convert_public_value_to_internal;

		let access = self.ac.as_deref().map(Value::from).unwrap_or(Value::None);
		let auth = self.rd.clone().map(convert_public_value_to_internal).unwrap_or(Value::None);
		let token = self.tk.clone().map(convert_public_value_to_internal).unwrap_or(Value::None);
		let session = Value::from(map! {
			"ac" => access.clone(),
			"exp" => self.exp.map(Value::from).unwrap_or(Value::None),
			"db" => self.db.as_deref().map(Value::from).unwrap_or(Value::None),
			"id" => self.id.map(Value::from).unwrap_or(Value::None),
			"ip" => self.ip.as_deref().map(Value::from).unwrap_or(Value::None),
			"ns" => self.ns.as_deref().map(Value::from).unwrap_or(Value::None),
			"or" => self.or.as_deref().map(Value::from).unwrap_or(Value::None),
			"rd" => auth.clone(),
			"tk" => token.clone(),
		});

		vec![("access", access), ("auth", auth), ("token", token), ("session", session)]
	}

	/// Create a system session for a given level and role
	pub fn for_level(level: Level, role: Role) -> Session {
		// Create a new session
		let mut sess = Session::default();
		// Set the session details
		match level {
			Level::Root => {
				sess.au = Arc::new(Auth::for_root(role));
			}
			Level::Namespace(ns) => {
				sess.au = Arc::new(Auth::for_ns(role, &ns));
				sess.ns = Some(ns);
			}
			Level::Database(ns, db) => {
				sess.au = Arc::new(Auth::for_db(role, &ns, &db));
				sess.ns = Some(ns);
				sess.db = Some(db);
			}
			_ => {}
		}
		sess
	}

	/// Create a record user session for a given NS and DB
	pub fn for_record(ns: &str, db: &str, ac: &str, rid: PublicValue) -> Session {
		Session {
			ac: Some(ac.to_owned()),
			au: Arc::new(Auth::for_record(rid.to_sql(), ns, db, ac)),
			rt: false,
			ip: None,
			or: None,
			id: None,
			ns: Some(ns.to_owned()),
			db: Some(db.to_owned()),
			tk: None,
			rd: Some(rid),
			exp: None,
			variables: Default::default(),
			new_planner_strategy: NewPlannerStrategy::default(),
			redact_volatile_explain_attrs: false,
		}
	}

	/// Create a system session for the root level with Owner role
	pub fn owner() -> Session {
		Session::for_level(Level::Root, Role::Owner)
	}

	/// Create a system session for the root level with Editor role
	pub fn editor() -> Session {
		Session::for_level(Level::Root, Role::Editor)
	}

	/// Create a system session for the root level with Viewer role
	pub fn viewer() -> Session {
		Session::for_level(Level::Root, Role::Viewer)
	}
}

/// The durable form of a client-attached RPC [`Session`], stored under
/// [`crate::key::root::se::Se`] (`/!se{id}`) so the session survives the
/// process that attached it and is reachable from any cluster node sharing
/// the datastore.
///
/// [`Session`] itself is serde-only and holds public value types, while every
/// stored KV value in this crate is `revision`-encoded — so this mirror
/// carries the same fields converted to their internal revisioned forms, plus
/// the absolute expiry of the durable copy.
///
/// This is an internal storage representation, scoped to the crate like the
/// `revision`-encoded value types it holds ([`Value`], [`Object`]); callers
/// go through the public [`Session`] via [`from_session`](Self::from_session)
/// and [`into_session`](Self::into_session).
#[revisioned(revision = 1)]
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct DurableSession {
	/// When the durable copy expires, in milliseconds since the UNIX epoch.
	/// Enforced lazily on load and by the periodic purge task. Unrelated to
	/// the authentication expiry in `exp`, which stays enforced at query time
	/// via [`Session::expired`].
	pub(crate) expires_at: u64,
	/// The session [`Auth`] information
	pub(crate) au: Auth,
	/// Whether realtime queries are supported
	pub(crate) rt: bool,
	/// The connection IP address
	pub(crate) ip: Option<String>,
	/// The connection origin
	pub(crate) or: Option<String>,
	/// The session ID
	pub(crate) id: Option<Uuid>,
	/// The selected namespace
	pub(crate) ns: Option<String>,
	/// The selected database
	pub(crate) db: Option<String>,
	/// The access method
	pub(crate) ac: Option<String>,
	/// The authentication token
	pub(crate) tk: Option<Value>,
	/// The record authentication data
	pub(crate) rd: Option<Value>,
	/// The expiration time of the session authentication
	pub(crate) exp: Option<i64>,
	/// The variables set on the session
	pub(crate) variables: Object,
	/// Strategy for the new streaming planner/executor
	pub(crate) new_planner_strategy: NewPlannerStrategy,
	/// When true, EXPLAIN ANALYZE output omits elapsed durations
	pub(crate) redact_volatile_explain_attrs: bool,
}

impl_kv_value_revisioned!(DurableSession);

impl DurableSession {
	/// Capture the durable form of a session, expiring at `expires_at`
	/// (milliseconds since the UNIX epoch).
	pub(crate) fn from_session(session: &Session, expires_at: u64) -> Self {
		use crate::val::convert_public::convert_public_value_to_internal;
		Self {
			expires_at,
			au: (*session.au).clone(),
			rt: session.rt,
			ip: session.ip.clone(),
			or: session.or.clone(),
			id: session.id,
			ns: session.ns.clone(),
			db: session.db.clone(),
			ac: session.ac.clone(),
			tk: session.tk.clone().map(convert_public_value_to_internal),
			rd: session.rd.clone().map(convert_public_value_to_internal),
			exp: session.exp,
			variables: session
				.variables
				.clone()
				.into_iter()
				.map(|(k, v)| (k, convert_public_value_to_internal(v)))
				.collect(),
			new_planner_strategy: session.new_planner_strategy,
			redact_volatile_explain_attrs: session.redact_volatile_explain_attrs,
		}
	}

	/// Restore the in-memory session this durable copy was captured from.
	pub(crate) fn into_session(self) -> Result<Session> {
		use crate::val::convert_value_to_public_value;
		Ok(Session {
			au: Arc::new(self.au),
			rt: self.rt,
			ip: self.ip,
			or: self.or,
			id: self.id,
			ns: self.ns,
			db: self.db,
			ac: self.ac,
			tk: self.tk.map(convert_value_to_public_value).transpose()?,
			rd: self.rd.map(convert_value_to_public_value).transpose()?,
			exp: self.exp,
			variables: self
				.variables
				.into_iter()
				.map(|(k, v)| Ok((k.into_string(), convert_value_to_public_value(v)?)))
				.collect::<Result<PublicVariables>>()?,
			new_planner_strategy: self.new_planner_strategy,
			redact_volatile_explain_attrs: self.redact_volatile_explain_attrs,
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn json_round_trip_preserves_auth_and_context() {
		// A root-Owner session with selected ns/db and an expiry — the shape
		// a persisting `RpcProtocol` writes to durable storage and restores.
		let original = Session {
			id: Some(Uuid::from_u128(1)),
			exp: Some(1_700_000_000),
			..Session::owner().with_ns("app").with_db("app")
		};

		let json = serde_json::to_string(&original).expect("serialize");
		let restored: Session = serde_json::from_str(&json).expect("deserialize");

		// The whole struct round-trips, including the `Arc<Auth>` that the
		// `arc_auth` helper serializes as its inner `Auth`.
		assert_eq!(original, restored);
		assert!(restored.au.is_root());
		assert_eq!(restored.ns.as_deref(), Some("app"));
		assert_eq!(restored.db.as_deref(), Some("app"));
	}

	/// A record-access session carrying every convertible field — auth
	/// principal, token and record-auth values, and session variables with
	/// non-JSON types (record ids, datetimes, decimals, nested objects) —
	/// survives the Session -> DurableSession -> revision bytes -> Session
	/// journey unchanged.
	#[test]
	fn durable_session_round_trip_preserves_all_fields() {
		use std::collections::BTreeMap;

		use surrealdb_types::{Number, Value as PV};

		let mut variables = PublicVariables::default();
		variables.insert("str", PV::String("hello".to_owned()));
		variables.insert("dec", PV::Number(Number::Decimal("1.5".parse().unwrap())));
		variables.insert("rid", PV::RecordId(surrealdb_types::RecordId::new("person", "tobie")));
		variables.insert("dt", PV::Datetime(surrealdb_types::Datetime::now()));
		variables.insert(
			"obj",
			PV::Object(surrealdb_types::Object::from(BTreeMap::from([("nested", PV::Bool(true))]))),
		);

		let original = Session {
			rt: true,
			ip: Some("10.0.0.1".to_owned()),
			or: Some("example.com".to_owned()),
			id: Some(Uuid::from_u128(7)),
			exp: Some(1_700_000_000),
			tk: Some(PV::Object(surrealdb_types::Object::from(BTreeMap::from([(
				"iss",
				PV::String("surrealdb".to_owned()),
			)])))),
			rd: Some(PV::RecordId(surrealdb_types::RecordId::new("person", "tobie"))),
			variables,
			redact_volatile_explain_attrs: true,
			..Session::for_record(
				"app",
				"app",
				"account",
				PublicValue::RecordId(surrealdb_types::RecordId::new("person", "tobie")),
			)
		};

		let durable = DurableSession::from_session(&original, 123_456_789);
		assert_eq!(durable.expires_at, 123_456_789);

		// The stored form must survive the actual KV encoding.
		let bytes = revision::to_vec(&durable).expect("revision encode");
		let decoded: DurableSession = revision::from_slice(&bytes).expect("revision decode");
		assert_eq!(durable, decoded);

		let restored = decoded.into_session().expect("convert back");
		assert_eq!(original, restored);
	}
}
