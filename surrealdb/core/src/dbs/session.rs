use std::sync::Arc;

use anyhow::Result;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use surrealdb_types::ToSql;
use uuid::Uuid;

use crate::dbs::{DurableSession, NewPlannerStrategy};
use crate::iam::{Auth, Level, Role};
use crate::types::{PublicValue, PublicVariables};
use crate::val::Value;

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
	///
	/// Public because the expiry rule has to be applied identically wherever a
	/// session is accepted, including the embedded engine outside this crate.
	/// Reimplementing it against the public `exp` field is what lets the two
	/// drift.
	pub fn expired(&self) -> bool {
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

/// The identity a session is acting as, captured so a change can be detected.
///
/// SECURITY: LIVE queries capture the session's auth principal at registration
/// time. When an auth-lifecycle operation changes the principal on the same
/// connection, those captured snapshots would continue to dispatch
/// notifications under the prior context, bypassing the access controls that
/// should now apply (GHSA-2xrp-m9c6-75rj). Callers snapshot the principal
/// before the operation and tear LIVE subscriptions down when it changes.
/// Token refresh against the same identity leaves the principal unchanged and
/// preserves the subscriptions.
///
/// Lives here rather than beside any one caller because every transport that
/// accepts an auth-lifecycle operation has to apply the same rule; a second
/// copy of "what counts as a change" is what lets two transports drift apart.
#[derive(Clone, Debug)]
pub struct AuthPrincipalSnapshot {
	id: String,
	level: Level,
}

impl AuthPrincipalSnapshot {
	/// Capture the principal `session` is currently acting as.
	pub fn capture(session: &Session) -> Self {
		Self {
			id: session.au.id().to_string(),
			level: session.au.level().clone(),
		}
	}

	/// Whether `session` is now acting as a different principal.
	pub fn differs_from(&self, session: &Session) -> bool {
		session.au.id() != self.id || session.au.level() != &self.level
	}
}

/// Capture the durable form of `session`, expiring at `expires_at`
/// (milliseconds since the UNIX epoch).
///
/// A free function rather than a constructor on [`DurableSession`]: the stored
/// shape belongs to the keyspace, one layer down, and knows nothing about the
/// public value types a live session carries.
pub(crate) fn durable_session(session: &Session, expires_at: u64) -> DurableSession {
	use crate::val::convert_public::convert_public_value_to_internal;
	DurableSession {
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

/// Restore the in-memory session `durable` was captured from.
///
/// See [`durable_session`] for why this is not a method.
pub(crate) fn restore_session(durable: DurableSession) -> Result<Session> {
	use crate::val::convert_value_to_public_value;
	Ok(Session {
		au: Arc::new(durable.au),
		rt: durable.rt,
		ip: durable.ip,
		or: durable.or,
		id: durable.id,
		ns: durable.ns,
		db: durable.db,
		ac: durable.ac,
		tk: durable.tk.map(convert_value_to_public_value).transpose()?,
		rd: durable.rd.map(convert_value_to_public_value).transpose()?,
		exp: durable.exp,
		variables: durable
			.variables
			.into_iter()
			.map(|(k, v)| Ok((k.into_string(), convert_value_to_public_value(v)?)))
			.collect::<Result<PublicVariables>>()?,
		new_planner_strategy: durable.new_planner_strategy,
		redact_volatile_explain_attrs: durable.redact_volatile_explain_attrs,
	})
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

		let durable = durable_session(&original, 123_456_789);
		assert_eq!(durable.expires_at, 123_456_789);

		// The stored form must survive the actual KV encoding.
		let bytes = revision::to_vec(&durable).expect("revision encode");
		let decoded: DurableSession = revision::from_slice(&bytes).expect("revision decode");
		assert_eq!(durable, decoded);

		let restored = restore_session(decoded).expect("convert back");
		assert_eq!(original, restored);
	}
}
