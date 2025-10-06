use std::borrow::Cow;
use std::io::Read;
use std::path::PathBuf;

use async_channel::Sender;
use bincode::Options;
use revision::{DeserializeRevisioned, Revisioned, SerializeRevisioned};
use serde::Serialize;
use surrealdb_core::kvs::export::Config as DbExportConfig;
use surrealdb_types::{Array, Notification, Object, SurrealValue, Value, Variables};
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
		token: String,
	},
	Invalidate,
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
				params: Some(Value::Array(Array::from(vec![Value::from_t(token)]))),
				transaction: None,
			},
			Command::Invalidate => RouterRequest {
				id,
				method: "invalidate",
				params: None,
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
pub(crate) struct RouterRequest {
	id: Option<i64>,
	method: &'static str,
	params: Option<Value>,
	#[allow(dead_code)]
	transaction: Option<Uuid>,
}

impl Serialize for RouterRequest {
	fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		let value = self.clone().into_value();
		value.serialize(serializer)
	}
}

impl Revisioned for RouterRequest {
	fn revision() -> u16 {
		1
	}
}

impl SerializeRevisioned for RouterRequest {
	fn serialize_revisioned<W: std::io::Write>(
		&self,
		w: &mut W,
	) -> std::result::Result<(), revision::Error> {
		let value = self.clone().into_value();
		value.serialize_revisioned(w)
	}
}

impl DeserializeRevisioned for RouterRequest {
	fn deserialize_revisioned<R: Read>(_: &mut R) -> std::result::Result<Self, revision::Error>
	where
		Self: Sized,
	{
		let value = Value::deserialize_revisioned(r)?;
		println!("de: value: {:?}", value);
		Self::from_value(value).map_err(|err| revision::Error::Conversion(err.to_string()))
	}
}

#[cfg(test)]
mod test {
	use std::io::Cursor;

	use revision::{DeserializeRevisioned, SerializeRevisioned};
	use surrealdb_types::{Array, Number, Value};
	use uuid::Uuid;

	use super::RouterRequest;

	fn assert_converts<S, D, I>(req: &RouterRequest, s: S, d: D)
	where
		S: FnOnce(&RouterRequest) -> I,
		D: FnOnce(I) -> Value,
	{
		let ser = s(req);
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

		println!("test convert bincode");

		assert_converts(
			&request,
			|i| surrealdb_core::rpc::format::bincode::encode(i).unwrap(),
			|b| surrealdb_core::rpc::format::bincode::decode(&b).unwrap(),
		);

		println!("test convert revisioned");

		assert_converts(
			&request,
			|i| revision::to_vec(i).unwrap(),
			|b| revision::from_slice(&b).unwrap(),
		);

		println!("done");
	}
}
