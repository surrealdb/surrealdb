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

/// The whole result of an access method's `CONTEXT` clause.
///
/// Carries both representations of the same payload: the public form, which is
/// what the public `Session` API hands out and what serialises, and the
/// internal form, which is what a query context binds. Both are behind an
/// `Arc` and are converted once, at authentication time.
///
/// Storing the internal form is what makes cloning a `Session` a pointer copy
/// and removes the *conversion* from the query path: holding only the public
/// form would make [`Session::values`] re-run `convert_public_value_to_internal`
/// over the whole payload — a deep traversal allocating a fresh map per object
/// and a fresh vector per array — on every query.
///
/// PERFORMANCE, stated plainly because the `Arc` invites the wrong conclusion:
/// the `Arc` removes the conversion and nothing else. `Session::values` still
/// deep-clones the payload into the assembled `$session` object once per query,
/// and `values()` is reached from `Context::attach_session` at
/// every `attach_session` site in `kvs/ds.rs` — so every `execute`, `process`,
/// `process_stream`, `evaluate` and API request pays it, *including* queries
/// that never mention `$session.data`. `Value::Object`/`Value::Array` are inline
/// (`VecMap`/`Vec`), so for the payload sizes `AccessDefinition::context`
/// contemplates the cost is O(nodes) allocations and megabytes of memcpy per
/// query, and it is the dominant term — not the conversion this type removed.
///
/// Removing it needs the assembled `$session` object memoised as one
/// `Arc<Value>` and handed out by `Arc::clone`. Two things block that here, and
/// neither is a small change: `Session`'s fields are all `pub`, so `ns`/`db`/
/// `ac`/`rd`/`tk`/`exp` mutate with no chance to invalidate a cache, and a
/// private cache field would make every `Session { .. }` literal outside this
/// module fail to compile. `Value` has no `Arc`-backed variant either, so the
/// payload cannot be shared into the object without a clone. Until one of those
/// changes, treat a large `CONTEXT` payload as a per-query cost on every query
/// the session runs.
///
/// Serialisation is defined on the public form, and equality on a digest of it
/// (see the [`PartialEq`] impl); the internal form and the digest are both
/// derived from the public form and are never independently set.
#[derive(Clone, Debug)]
pub struct SessionData {
	public: Arc<PublicValue>,
	internal: Arc<Value>,
	digest: PayloadDigest,
}

/// A collision-resistant summary of a `CONTEXT` payload, computed once when the
/// payload is built and compared in place of the payload itself.
type PayloadDigest = [u8; 32];

/// Digest `value` so that two payloads carrying the same content compare equal
/// regardless of the order a scan happened to return array elements in.
///
/// Array and set elements are digested individually and their digests sorted
/// before being folded in, so `['acme','globex']` and `['globex','acme']`
/// produce the same digest. Object keys are already canonically ordered by the
/// `BTreeMap` behind [`surrealdb_types::Object`]. Everything else is folded in
/// as its SQL rendering, which distinguishes types (`'1'` from `1`) and keeps
/// order where order is semantic — inside a record id key, say.
///
/// Sorting per-element digests rather than combining them commutatively is
/// deliberate: a commutative combiner (xor, wrapping add) has trivial
/// collisions, and this digest decides whether a LIVE subscription survives.
///
/// Two payloads that are `==` but rendered differently — `1` against `1.0`,
/// which [`surrealdb_types::Number`] compares equal across representations —
/// digest differently and so count as a change. That errs towards tearing a
/// subscription down, which is the safe direction.
fn payload_digest(value: &PublicValue) -> PayloadDigest {
	use sha2::{Digest, Sha256};

	fn fold_unordered<'a>(
		hasher: &mut sha2::Sha256,
		tag: u8,
		items: impl ExactSizeIterator<Item = &'a PublicValue>,
	) {
		hasher.update([tag]);
		hasher.update((items.len() as u64).to_be_bytes());
		let mut digests: Vec<PayloadDigest> = items.map(payload_digest).collect();
		digests.sort_unstable();
		for digest in digests {
			hasher.update(digest);
		}
	}

	let mut hasher = Sha256::new();
	match value {
		PublicValue::Array(items) => fold_unordered(&mut hasher, b'A', items.iter()),
		PublicValue::Set(items) => fold_unordered(&mut hasher, b'S', items.iter()),
		PublicValue::Object(fields) => {
			hasher.update([b'O']);
			hasher.update((fields.len() as u64).to_be_bytes());
			for (key, field) in fields.iter() {
				hasher.update((key.len() as u64).to_be_bytes());
				hasher.update(key.as_bytes());
				hasher.update(payload_digest(field));
			}
		}
		scalar => {
			hasher.update([b'V']);
			let rendered = scalar.to_sql();
			hasher.update((rendered.len() as u64).to_be_bytes());
			hasher.update(rendered.as_bytes());
		}
	}
	hasher.finalize().into()
}

impl SessionData {
	/// Build the session data from the value a `CONTEXT` clause returned.
	pub fn new(value: PublicValue) -> Self {
		use crate::val::convert_public::convert_public_value_to_internal;
		let internal = Arc::new(convert_public_value_to_internal(value.clone()));
		let digest = payload_digest(&value);
		Self {
			public: Arc::new(value),
			internal,
			digest,
		}
	}

	/// The payload in its public form.
	pub fn public(&self) -> &PublicValue {
		&self.public
	}

	/// The payload in the internal form a query context binds.
	pub(crate) fn internal(&self) -> &Value {
		&self.internal
	}
}

/// Equality is "the same payload", not "the same `Value`".
///
/// The canonical `CONTEXT` clause is a scan — `SELECT VALUE org FROM grant
/// WHERE owner = $auth` — whose element order is not stable across index
/// selection, compaction or split ranges. A structural compare would therefore
/// report an *unchanged* grant set as changed whenever a rescan reordered it,
/// and the callers of this comparison treat a changed payload as a changed
/// principal: `AuthPrincipalSnapshot::differs_from` tears LIVE subscriptions
/// down, and the MCP handshake fingerprint rejects the request. An ordinary
/// refresh-token rotation would silently drop a subscriber's LIVE queries.
///
/// So equality is defined on `payload_digest`, which is order-insensitive for
/// arrays and sets — the shapes a grant list actually takes — and O(1) rather
/// than O(payload) per signin/signup/authenticate.
///
/// The cost of that choice, stated so it is not discovered later: a permission
/// that reads the payload *positionally* (`$session.data[0]`) cannot see a pure
/// reorder as a principal change. Payloads used as sets — every `IN`/`CONTAINS`
/// shape this feature is built for — are unaffected.
impl PartialEq for SessionData {
	fn eq(&self, other: &Self) -> bool {
		self.digest == other.digest
	}
}

impl Eq for SessionData {}

impl Serialize for SessionData {
	fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
		self.public.serialize(serializer)
	}
}

impl<'de> Deserialize<'de> for SessionData {
	fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
		Ok(Self::new(PublicValue::deserialize(deserializer)?))
	}
}

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
	/// The whole result of the access method's `CONTEXT` clause, computed
	/// once at authentication time and read back as `$session.data`.
	///
	/// Server-computed: nothing a client sends populates it. `session` is in
	/// [`surrealdb_cnf::PROTECTED_PARAM_NAMES`], so `LET`, `UNSET`, `db.set`
	/// and the RPC `set`/`unset` methods all refuse to bind the name.
	///
	/// SECURITY: that is not the same as unforgeable, and this field must not
	/// be documented as if it were. Only those statement forms consult the
	/// protected list. The constructs that bind a *user-chosen* parameter name
	/// — `FOR $session IN [...]`, a closure parameter, a user-defined function
	/// argument — bind it with no check at all, and
	/// [`crate::ctx::Context::value`] prefers a key inserted locally over the
	/// parent chain, so a query can shadow `$session` for a body it controls.
	///
	/// The identical routes shadow `$auth`, which every record permission
	/// written before this field already depends on. So the honest boundary is
	/// this: `$session.data` is exactly as trustworthy as `$auth` — no more, no
	/// less — and a permission that reads it is no weaker than one that reads
	/// `$auth`. Closing the hole is an engine-wide change to how parameters are
	/// bound, not a property this field can hold on its own.
	pub data: Option<SessionData>,
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

	/// The ambient parameters this session contributes to a query's context.
	///
	/// Returns `Arc<Value>` rather than `Value` because
	/// [`crate::ctx::Context::add_values`] stores `Arc<Value>`: handing it one
	/// already built saves re-wrapping every entry on every query.
	///
	/// The `Arc` wraps the *assembled* object, which is rebuilt here on every
	/// query — so a `CONTEXT` payload is deep-cloned into it every time, whether
	/// or not the query reads `$session.data`. See the PERFORMANCE note on
	/// [`SessionData`] for why that clone cannot be memoised away yet.
	pub(crate) fn values(&self) -> Vec<(&'static str, Arc<Value>)> {
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
			// Present only when the access method defined a `CONTEXT` clause,
			// so the `$session` object a session without one sees is exactly
			// what it saw before the clause existed.
			"data", if let Some(v) = self.data.as_ref() => v.internal().clone(),
		});

		vec![
			("access", Arc::new(access)),
			("auth", Arc::new(auth)),
			("token", Arc::new(token)),
			("session", Arc::new(session)),
		]
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
			data: None,
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
	data: Option<SessionData>,
}

impl AuthPrincipalSnapshot {
	/// Capture the principal `session` is currently acting as.
	pub fn capture(session: &Session) -> Self {
		Self {
			id: session.au.id().to_string(),
			level: session.au.level().clone(),
			data: session.data.clone(),
		}
	}

	/// Whether `session` is now acting as a different principal.
	///
	/// The `CONTEXT` payload counts as part of the principal. Permissions read
	/// it as `$session.data`, and a LIVE subscription evaluates them against
	/// the session snapshot frozen at registration — so if a re-authentication
	/// recomputes a *different* payload (a grant revoked in the table, seen on
	/// the next refresh-token rotation), subscriptions registered under the old
	/// one would keep dispatching under it indefinitely, since refresh tokens
	/// rotate without bound. Treating a changed payload as a changed principal
	/// tears those down, which is the same rule GHSA-2xrp-m9c6-75rj applied to
	/// the auth principal itself. An unchanged payload compares equal and
	/// preserves the subscriptions, so an ordinary refresh is unaffected.
	///
	/// "Unchanged" here means [`SessionData`]'s own equality, which is
	/// order-insensitive for arrays and sets: a rescan that returns the same
	/// grants in a different order is not a change, and must not cost the
	/// subscriber their LIVE queries. See the note on [`SessionData`]'s
	/// [`PartialEq`] impl for what that trades away.
	pub fn differs_from(&self, session: &Session) -> bool {
		session.au.id() != self.id || session.au.level() != &self.level || session.data != self.data
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
		data: session.data.as_ref().map(|d| d.internal().clone()),
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
		data: durable.data.map(convert_value_to_public_value).transpose()?.map(SessionData::new),
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
			data: Some(SessionData::new(PV::Object(surrealdb_types::Object::from(
				BTreeMap::from([(
					"orgs",
					PV::Array(surrealdb_types::Array::from(vec![
						PV::String("acme".to_owned()),
						PV::String("globex".to_owned()),
					])),
				)]),
			)))),
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

	/// The `$session` object a session without a `CONTEXT` clause sees is
	/// exactly what it saw before the clause existed — the `data` key appears
	/// only once one is populated. That is what makes the feature free for
	/// every deployment that does not use it.
	#[test]
	fn session_values_only_carry_data_when_a_context_clause_populated_it() {
		use surrealdb_types::Value as PV;

		fn session_keys(session: &Session) -> Vec<String> {
			let values = session.values();
			let (_, session_value) =
				values.iter().find(|(k, _)| *k == "session").expect("a `session` entry");
			let obj = match session_value.as_ref() {
				Value::Object(o) => o,
				other => panic!("expected an object, got {other:?}"),
			};
			let mut keys: Vec<String> = obj.keys().map(|k| k.as_str().to_owned()).collect();
			keys.sort();
			keys
		}

		let mut session = Session::for_record(
			"app",
			"app",
			"account",
			PublicValue::RecordId(surrealdb_types::RecordId::new("person", "tobie")),
		);
		assert_eq!(
			session_keys(&session),
			vec!["ac", "db", "exp", "id", "ip", "ns", "or", "rd", "tk"]
		);

		session.data = Some(SessionData::new(PV::String("payload".to_owned())));
		assert_eq!(
			session_keys(&session),
			vec!["ac", "data", "db", "exp", "id", "ip", "ns", "or", "rd", "tk"]
		);

		let values = session.values();
		let (_, session_value) = values.iter().find(|(k, _)| *k == "session").unwrap();
		let Value::Object(obj) = session_value.as_ref() else {
			panic!("expected an object")
		};
		assert_eq!(obj.get("data"), Some(&Value::from("payload")));
	}

	/// A rescan that returns the same grants in a different order is not a
	/// change of principal. `SELECT VALUE org FROM grant WHERE owner = $auth`
	/// has no guaranteed element order, so a structural compare would tear
	/// down a subscriber's LIVE queries on an ordinary refresh-token rotation.
	#[test]
	fn a_reordered_payload_is_the_same_payload() {
		use surrealdb_types::{Array, Object, Value as PV};

		fn array(items: &[&str]) -> PV {
			PV::Array(Array::from(
				items.iter().map(|s| PV::String((*s).to_owned())).collect::<Vec<_>>(),
			))
		}

		// Top level.
		assert_eq!(
			SessionData::new(array(&["acme", "globex"])),
			SessionData::new(array(&["globex", "acme"]))
		);

		// Nested under an object key, which is where a `CONTEXT` clause
		// returning several lists puts them.
		let nested = |orgs: PV| {
			let mut obj = Object::default();
			obj.insert("orgs", orgs);
			SessionData::new(PV::Object(obj))
		};
		assert_eq!(nested(array(&["acme", "globex"])), nested(array(&["globex", "acme"])));

		// Content changes are still changes.
		assert_ne!(
			SessionData::new(array(&["acme", "globex"])),
			SessionData::new(array(&["acme", "initech"]))
		);
		assert_ne!(SessionData::new(array(&["acme"])), SessionData::new(array(&["acme", "acme"])));
		assert_ne!(
			SessionData::new(array(&["acme"])),
			SessionData::new(PV::String("acme".to_owned()))
		);

		// A string is not the array containing it, and types are not conflated.
		assert_ne!(
			SessionData::new(PV::String("1".to_owned())),
			SessionData::new(PV::Number(surrealdb_types::Number::Int(1)))
		);
	}

	/// A changed `CONTEXT` payload counts as a changed principal, so LIVE
	/// subscriptions registered under a revoked grant are torn down rather than
	/// left dispatching under it. An unchanged payload preserves them.
	#[test]
	fn auth_principal_snapshot_tracks_the_context_payload() {
		use surrealdb_types::Value as PV;

		let mut session = Session::for_record(
			"app",
			"app",
			"account",
			PublicValue::RecordId(surrealdb_types::RecordId::new("person", "tobie")),
		);
		session.data = Some(SessionData::new(PV::String("acme".to_owned())));
		let snapshot = AuthPrincipalSnapshot::capture(&session);
		assert!(!snapshot.differs_from(&session));

		// Re-authenticating to the same identity with the same payload keeps
		// the subscriptions.
		session.data = Some(SessionData::new(PV::String("acme".to_owned())));
		assert!(!snapshot.differs_from(&session));

		// A revoked grant does not.
		session.data = Some(SessionData::new(PV::String("globex".to_owned())));
		assert!(snapshot.differs_from(&session));

		session.data = None;
		assert!(snapshot.differs_from(&session));
	}
}
