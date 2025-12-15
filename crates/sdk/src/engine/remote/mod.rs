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
}

#[cfg(test)]
mod test {
	use uuid::Uuid;

	use super::RouterRequest;
	use crate::types::{Array, Number, SurrealValue, Value};

	fn assert_converts<S, D, I>(req: RouterRequest, s: S, d: D)
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
			request,
			|i| surrealdb_core::rpc::format::flatbuffers::encode(i).unwrap(),
			|b| surrealdb_core::rpc::format::flatbuffers::decode(&b).unwrap(),
		);
	}
}
