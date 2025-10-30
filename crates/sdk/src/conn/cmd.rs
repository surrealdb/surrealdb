use std::borrow::Cow;
use std::path::PathBuf;

use async_channel::Sender;
use surrealdb_core::iam::token::Token;
use surrealdb_core::kvs::export::Config as DbExportConfig;
#[cfg(any(feature = "protocol-ws", feature = "protocol-http"))]
use surrealdb_types::SurrealValue;
use surrealdb_types::{Array, Notification, Object, Value, Variables};
use uuid::Uuid;

use super::MlExportConfig;
use crate::Result;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) enum Command {
	Use {
		namespace: Option<String>,
		database: Option<String>,
	},
	Signup {
		credentials: Object,
	},
	Signin {
		credentials: Object,
	},
	Authenticate {
		token: Token,
	},
	Refresh {
		token: Token,
	},
	Invalidate,
	Revoke {
		token: Token,
	},
	RawQuery {
		txn: Option<Uuid>,
		query: Cow<'static, str>,
		variables: Variables,
	},
	ExportFile {
		path: PathBuf,
		config: Option<DbExportConfig>,
	},
	ExportMl {
		path: PathBuf,
		config: MlExportConfig,
	},
	ExportBytes {
		bytes: Sender<Result<Vec<u8>>>,
		config: Option<DbExportConfig>,
	},
	ExportBytesMl {
		bytes: Sender<Result<Vec<u8>>>,
		config: MlExportConfig,
	},
	ImportFile {
		path: PathBuf,
	},
	ImportMl {
		path: PathBuf,
	},
	Health,
	Version,
	Set {
		key: String,
		value: Value,
	},
	Unset {
		key: String,
	},
	SubscribeLive {
		uuid: Uuid,
		notification_sender: Sender<Result<Notification>>,
	},
	Kill {
		uuid: Uuid,
	},
	Run {
		name: String,
		version: Option<String>,
		args: Array,
	},
}

impl Command {
	#[cfg(any(feature = "protocol-ws", feature = "protocol-http"))]
	pub(crate) fn into_router_request(self, id: Option<i64>) -> Option<RouterRequest> {
		use surrealdb_types::Uuid;

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
					transaction: None,
				}
			}
			Command::Signup {
				credentials,
			} => RouterRequest {
				id,
				method: "signup",
				params: Some(Value::Array(Array::from(vec![Value::from_t(credentials)]))),
				transaction: None,
			},
			Command::Signin {
				credentials,
			} => RouterRequest {
				id,
				method: "signin",
				params: Some(Value::Array(Array::from(vec![Value::from_t(credentials)]))),
				transaction: None,
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
				transaction: None,
			},
			Command::Refresh {
				token,
			} => RouterRequest {
				id,
				method: "refresh",
				// Send the entire token structure (both access and refresh tokens)
				// to the server for the refresh operation.
				params: Some(Value::Array(Array::from(vec![Value::from_t(token)]))),
				transaction: None,
			},
			Command::Invalidate => RouterRequest {
				id,
				method: "invalidate",
				params: None,
				transaction: None,
			},
			Command::Revoke {
				token,
			} => RouterRequest {
				id,
				method: "revoke",
				params: Some(Value::Array(Array::from(vec![token.into_value()]))),
				transaction: None,
			},
			Command::RawQuery {
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
					transaction: txn,
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
				transaction: None,
			},
			Command::Version => RouterRequest {
				id,
				method: "version",
				params: None,
				transaction: None,
			},
			Command::Set {
				key,
				value,
			} => RouterRequest {
				id,
				method: "let",
				params: Some(Value::from_t(vec![Value::from_t(key), value])),
				transaction: None,
			},
			Command::Unset {
				key,
			} => RouterRequest {
				id,
				method: "unset",
				params: Some(Value::from_t(vec![Value::from_t(key)])),
				transaction: None,
			},
			Command::SubscribeLive {
				..
			} => return None,
			Command::Kill {
				uuid,
			} => RouterRequest {
				id,
				method: "kill",
				params: Some(Value::from_t(vec![Value::Uuid(Uuid(uuid))])),
				transaction: None,
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
					transaction: None,
				}
			}
		};
		Some(res)
	}
}

/// A struct which will be serialized as a map to behave like the previously
/// used BTreeMap.
///
/// This struct serializes as if it is a surrealdb_types::Value::Object.
#[derive(Clone, Debug, SurrealValue)]
#[cfg(any(feature = "protocol-ws", feature = "protocol-http"))]
pub(crate) struct RouterRequest {
	pub(crate) id: Option<i64>,
	pub(crate) method: &'static str,
	pub(crate) params: Option<Value>,
	#[allow(dead_code)]
	pub(crate) transaction: Option<Uuid>,
}

#[cfg(test)]
mod test {
	use surrealdb_types::{Array, Number, SurrealValue, Value};
	use uuid::Uuid;

	use super::RouterRequest;

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
			transaction: Some(Uuid::new_v4()),
		};

		assert_converts(
			request,
			|i| surrealdb_core::rpc::format::flatbuffers::encode(i).unwrap(),
			|b| surrealdb_core::rpc::format::flatbuffers::decode(&b).unwrap(),
		);
	}
}
