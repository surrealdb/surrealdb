//! This SDK can be used as a client to connect to SurrealDB servers.
//!
//! # Example
//!
//! ```no_run
//! use std::borrow::Cow;
//! use serde::{Serialize, Deserialize};
//! use serde_json::json;
//! use surrealdb::{Error, Surreal};
//! use surrealdb::opt::auth::Root;
//! use surrealdb::engine::remote::ws::Ws;
//!
//! #[derive(Serialize, Deserialize)]
//! struct Person {
//!     title: String,
//!     name: Name,
//!     marketing: bool,
//! }
//!
//! // Pro tip: Replace String with Cow<'static, str> to
//! // avoid unnecessary heap allocations when inserting
//!
//! #[derive(Serialize, Deserialize)]
//! struct Name {
//!     first: Cow<'static, str>,
//!     last: Cow<'static, str>,
//! }
//!
//! // Install at https://surrealdb.com/install
//! // and use `surreal start --user root --pass root`
//! // to start a working database to take the following queries
//!
//! // See the results via `surreal sql --ns namespace --db database --pretty`
//! // or https://surrealist.app/
//! // followed by the query `SELECT * FROM person;`
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Error> {
//!     let db = Surreal::new::<Ws>("localhost:8000").await?;
//!
//!     // Signin as a namespace, database, or root user
//!     db.signin(Root {
//!         username: "root",
//!         password: "root",
//!     }).await?;
//!
//!     // Select a specific namespace / database
//!     db.use_ns("namespace").use_db("database").await?;
//!
//!     // Create a new person with a random ID
//!     let created: Option<Person> = db.create("person")
//!         .content(Person {
//!             title: "Founder & CEO".into(),
//!             name: Name {
//!                 first: "Tobie".into(),
//!                 last: "Morgan Hitchcock".into(),
//!             },
//!             marketing: true,
//!         })
//!         .await?;
//!
//!     // Create a new person with a specific ID
//!     let created: Option<Person> = db.create(("person", "jaime"))
//!         .content(Person {
//!             title: "Founder & COO".into(),
//!             name: Name {
//!                 first: "Jaime".into(),
//!                 last: "Morgan Hitchcock".into(),
//!             },
//!             marketing: false,
//!         })
//!         .await?;
//!
//!     // Update a person record with a specific ID
//!     let updated: Option<Person> = db.update(("person", "jaime"))
//!         .merge(json!({"marketing": true}))
//!         .await?;
//!
//!     // Select all people records
//!     let people: Vec<Person> = db.select("person").await?;
//!
//!     // Perform a custom advanced query
//!     let query = r#"
//!         SELECT marketing, count()
//!         FROM type::table($table)
//!         GROUP BY marketing
//!     "#;
//!
//!     let groups = db.query(query)
//!         .bind(("table", "person"))
//!         .await?;
//!
//!     Ok(())
//! }
//! ```

#[cfg(feature = "protocol-http")]
#[cfg_attr(docsrs, doc(cfg(feature = "protocol-http")))]
pub mod http;

#[cfg(feature = "protocol-ws")]
#[cfg_attr(docsrs, doc(cfg(feature = "protocol-ws")))]
pub mod ws;

use surrealdb_core::iam::token::Token;
use uuid::Uuid;

use crate::conn::cmd::Command;
use crate::types::{Array, SurrealValue, Value};

/// A struct which will be serialized as a map to behave like the previously
/// used BTreeMap.
///
/// This struct serializes as if it is a crate::types::Value::Object.
#[derive(Clone, Debug, SurrealValue)]
#[surreal(crate = "crate::types")]
pub(crate) struct RouterRequest {
	pub(crate) id: Option<i64>,
	pub(crate) method: &'static str,
	pub(crate) params: Option<Value>,
	pub(crate) txn: Option<Uuid>,
	#[surreal(rename = "session")]
	pub(crate) session_id: Option<Uuid>,
}

impl Command {
	fn into_router_request(
		self,
		id: Option<i64>,
		session_id: Option<Uuid>,
	) -> Option<RouterRequest> {
		use crate::types::Uuid;

		let res = match self {
			Command::Use {
				namespace,
				database,
			} => {
				let namespace = namespace.map(Value::String).unwrap_or(Value::None);
				let database = database.map(Value::String).unwrap_or(Value::None);
				RouterRequest {
					id,
					method: "use",
					params: Some(Value::Array(Array::from(vec![namespace, database]))),
					txn: None,
					session_id,
				}
			}
			Command::Signup {
				credentials,
			} => RouterRequest {
				id,
				method: "signup",
				params: Some(Value::Array(Array::from(vec![Value::from_t(credentials)]))),
				txn: None,
				session_id,
			},
			Command::Signin {
				credentials,
			} => RouterRequest {
				id,
				method: "signin",
				params: Some(Value::Array(Array::from(vec![Value::from_t(credentials)]))),
				txn: None,
				session_id,
			},
			Command::Authenticate {
				token,
			} => RouterRequest {
				id,
				method: "authenticate",
				// Extract only the access token for authentication.
				// If the token has a refresh component, we ignore it here
				// as authentication only needs the access token.
				params: Some(Value::Array(Array::from(vec![match token {
					Token::Access(access) => access.into_value(),
					Token::WithRefresh {
						access,
						..
					} => access.into_value(),
				}]))),
				txn: None,
				session_id,
			},
			Command::Refresh {
				token,
			} => RouterRequest {
				id,
				method: "refresh",
				// Send the entire token structure (both access and refresh tokens)
				// to the server for the refresh operation.
				params: Some(Value::Array(Array::from(vec![Value::from_t(token)]))),
				txn: None,
				session_id,
			},
			Command::Invalidate => RouterRequest {
				id,
				method: "invalidate",
				params: None,
				txn: None,
				session_id,
			},
			Command::Begin => RouterRequest {
				id,
				method: "begin",
				params: None,
				txn: None,
				session_id,
			},
			Command::Commit {
				txn,
			} => RouterRequest {
				id,
				method: "commit",
				params: Some(Value::Array(Array::from(vec![Value::Uuid(Uuid::from(txn))]))),
				txn: None,
				session_id,
			},
			Command::Rollback {
				txn,
			} => RouterRequest {
				id,
				method: "cancel",
				params: Some(Value::Array(Array::from(vec![Value::Uuid(Uuid::from(txn))]))),
				txn: None,
				session_id,
			},
			Command::Revoke {
				token,
			} => RouterRequest {
				id,
				method: "revoke",
				params: Some(Value::Array(Array::from(vec![token.into_value()]))),
				txn: None,
				session_id,
			},
			Command::Query {
				txn,
				query,
				variables,
			} => {
				let params: Vec<Value> =
					vec![Value::String(query.into_owned()), Value::Object(variables.into())];
				RouterRequest {
					id,
					method: "query",
					params: Some(Value::Array(Array::from(params))),
					txn,
					session_id,
				}
			}
			Command::ExportFile {
				..
			}
			| Command::ExportBytes {
				..
			}
			| Command::ImportFile {
				..
			}
			| Command::ExportBytesMl {
				..
			}
			| Command::ExportMl {
				..
			}
			| Command::ImportMl {
				..
			} => return None,
			Command::Health => RouterRequest {
				id,
				method: "ping",
				params: None,
				txn: None,
				session_id,
			},
			Command::Version => RouterRequest {
				id,
				method: "version",
				params: None,
				txn: None,
				session_id,
			},
			Command::Set {
				key,
				value,
			} => RouterRequest {
				id,
				method: "let",
				params: Some(Value::from_t(vec![Value::from_t(key), value])),
				txn: None,
				session_id,
			},
			Command::Unset {
				key,
			} => RouterRequest {
				id,
				method: "unset",
				params: Some(Value::from_t(vec![Value::from_t(key)])),
				txn: None,
				session_id,
			},
			Command::SubscribeLive {
				..
			} => return None,
			Command::Kill {
				uuid,
			} => RouterRequest {
				id,
				method: "kill",
				params: Some(Value::from_t(vec![Value::Uuid(Uuid::from(uuid))])),
				txn: None,
				session_id,
			},
			Command::Attach {
				session_id,
			} => RouterRequest {
				id,
				method: "attach",
				params: None,
				txn: None,
				session_id: Some(session_id),
			},
			Command::Detach {
				session_id,
			} => RouterRequest {
				id,
				method: "detach",
				params: None,
				txn: None,
				session_id: Some(session_id),
			},
			Command::Run {
				name,
				version,
				args,
			} => {
				let version = version.map(Value::String).unwrap_or(Value::None);
				RouterRequest {
					id,
					method: "run",
					params: Some(Value::Array(Array::from(vec![
						Value::String(name),
						version,
						Value::Array(args),
					]))),
					txn: None,
					session_id,
				}
			}
		};
		Some(res)
	}

	fn replayable(&self) -> bool {
		matches!(
			self,
			Command::Signup { .. }
				| Command::Signin { .. }
				| Command::Authenticate { .. }
				| Command::Invalidate
				| Command::Use { .. }
				| Command::Set { .. }
				| Command::Unset { .. }
		)
	}

	/// Whether `self` would be a no-op (replay-wise) if appended directly after
	/// `prev` at the tail of the replay log. Used to coalesce idempotent
	/// command runs (e.g. a `useDb` loop) so the replay log doesn't grow O(N).
	///
	/// Load-bearing invariant for the `Use(None, None)` -> `true` branch:
	/// `Command::Use` carries `Option<String>` for each field, and
	/// [`Command::into_router_request`] encodes `None` as `Value::None` (never
	/// `Value::Null`). The server-side `yuse` handler treats `(None, None)` as
	/// a no-op when `session.ns` is already set — only the `Value::Null` form
	/// (unreachable from the SDK) clears the session. If a future change adds
	/// a clearing variant to `Command::Use` or maps `None` to `Value::Null` on
	/// the wire, revisit this method and the corresponding tests.
	#[cfg(feature = "protocol-ws")]
	fn is_replay_noop_after(&self, prev: &Command) -> bool {
		match (prev, self) {
			(
				Command::Use {
					namespace: pn,
					database: pd,
				},
				Command::Use {
					namespace: nn,
					database: nd,
				},
			) => {
				// Use commands carry `None` for "leave unchanged" and `Some(_)`
				// for "set to this". Replaying `prev` already left the session
				// in some (ns', db') state; `self` is a no-op only if every
				// field it sets matches what `prev` already set.
				let ns_noop = nn.is_none() || nn == pn;
				let db_noop = nd.is_none() || nd == pd;
				ns_noop && db_noop
			}
			_ => false,
		}
	}
}

/// Append `command` to the replay log unless it's a no-op against the current
/// tail entry. Caller has already verified the command is replayable.
///
/// Concurrency invariant: the tail read and the push are not atomic with each
/// other. Only call this from contexts where replay recording is serialized
/// for a given session (e.g. the WS engine's single-threaded response loop).
/// The HTTP engine handles each request in a spawned task and must keep using
/// a plain `replay.push(command)`.
#[cfg(feature = "protocol-ws")]
fn record_replayable(replay: &boxcar::Vec<Command>, command: Command) {
	debug_assert!(
		command.replayable(),
		"record_replayable called with non-replayable command: {command:?}",
	);
	let n = replay.count();
	if n > 0
		&& let Some(prev) = replay.get(n - 1)
		&& command.is_replay_noop_after(prev)
	{
		return;
	}
	replay.push(command);
}

#[cfg(test)]
mod test {
	use uuid::Uuid;

	use super::RouterRequest;
	use crate::types::{Array, Number, SurrealValue, Value};

	fn assert_converts<S, D, I>(req: &RouterRequest, s: S, d: D)
	where
		S: FnOnce(&Value) -> I,
		D: FnOnce(I) -> Value,
	{
		let v = req.clone().into_value();
		let ser = s(&v);
		let val = d(ser);
		let Value::Object(obj) = val else {
			panic!("not an object");
		};
		assert_eq!(
			obj.get("id").cloned().and_then(|x| if let Value::Number(Number::Int(x)) = x {
				Some(x)
			} else {
				None
			}),
			req.id
		);
		let Some(Value::String(x)) = obj.get("method") else {
			panic!("invalid method field: {obj:?}")
		};
		assert_eq!(x.as_str(), req.method);

		assert_eq!(obj.get("params").cloned(), req.params);
	}

	#[test]
	fn router_request_value_conversion() {
		let request = RouterRequest {
			id: Some(1234),
			method: "request",
			params: Some(Value::Array(Array::from(vec![
				Value::Number(Number::Int(1234i64)),
				Value::String("request".to_string()),
			]))),
			txn: Some(Uuid::new_v4()),
			session_id: Some(Uuid::new_v4()),
		};

		assert_converts(
			&request,
			|i| surrealdb_core::rpc::format::flatbuffers::encode(i).unwrap(),
			|b| surrealdb_core::rpc::format::flatbuffers::decode(&b).unwrap(),
		);
	}
}

// Replay coalescing is only used by the WS engine, so gate these tests on the
// same feature that compiles `record_replayable` / `is_replay_noop_after`.
#[cfg(all(test, feature = "protocol-ws"))]
mod replay_test {
	use super::{Command, record_replayable};

	fn use_cmd(ns: Option<&str>, db: Option<&str>) -> Command {
		Command::Use {
			namespace: ns.map(String::from),
			database: db.map(String::from),
		}
	}

	#[test]
	fn record_replayable_dedups_identical_consecutive_use() {
		let replay = boxcar::Vec::new();
		record_replayable(&replay, use_cmd(Some("ns1"), None));
		for _ in 0..500 {
			record_replayable(&replay, use_cmd(None, Some("db1")));
		}
		// Two Use entries: one for ns, one for db. The 499 repeats are coalesced.
		assert_eq!(replay.count(), 2);
	}

	#[test]
	fn record_replayable_keeps_distinct_use_entries() {
		let replay = boxcar::Vec::new();
		record_replayable(&replay, use_cmd(Some("ns1"), None));
		record_replayable(&replay, use_cmd(None, Some("db1")));
		record_replayable(&replay, use_cmd(Some("ns2"), None));
		record_replayable(&replay, use_cmd(None, Some("db2")));
		assert_eq!(replay.count(), 4);
	}

	#[test]
	fn record_replayable_does_not_dedup_other_commands() {
		let replay = boxcar::Vec::new();
		record_replayable(&replay, Command::Invalidate);
		record_replayable(&replay, Command::Invalidate);
		assert_eq!(replay.count(), 2);
	}

	#[test]
	fn record_replayable_does_not_skip_use_after_non_use_tail() {
		let replay = boxcar::Vec::new();
		record_replayable(&replay, use_cmd(Some("ns1"), Some("db1")));
		record_replayable(&replay, Command::Invalidate);
		// Even though this Use matches an earlier Use, the tail isn't Use,
		// so we still record it.
		record_replayable(&replay, use_cmd(Some("ns1"), Some("db1")));
		assert_eq!(replay.count(), 3);
	}

	#[test]
	fn is_replay_noop_after_partial_use_against_full_tail() {
		// Tail already sets both ns and db. A follow-up that only sets db to
		// the same value should be dropped; a follow-up that changes db should
		// not.
		let full = use_cmd(Some("ns1"), Some("db1"));
		assert!(use_cmd(None, Some("db1")).is_replay_noop_after(&full));
		assert!(!use_cmd(None, Some("db2")).is_replay_noop_after(&full));
	}

	#[test]
	fn use_defaults_subsumed_by_prior_full_use_is_safe_to_drop() {
		// `Surreal::use_defaults()` sends `Use { namespace: None, database: None }`.
		// When the replay log tail already targets a specific (ns, db), replaying
		// that Use sets `session.ns` first, so the server's `yuse` handler treats
		// the subsequent `Use(None, None)` as a no-op (the defaults branch is
		// gated on `session.ns.is_none()`). The SDK cannot send the wire-level
		// `Value::Null` that would clear the session — `Command::Use` is typed
		// as `Option<String>`, and `None` is encoded as `Value::None`. So
		// dropping the trailing `Use(None, None)` preserves end-state under
		// replay against any fresh session.
		let full = use_cmd(Some("ns1"), Some("db1"));
		assert!(use_cmd(None, None).is_replay_noop_after(&full));

		let replay = boxcar::Vec::new();
		record_replayable(&replay, full);
		record_replayable(&replay, use_cmd(None, None));
		assert_eq!(replay.count(), 1);
	}
}
