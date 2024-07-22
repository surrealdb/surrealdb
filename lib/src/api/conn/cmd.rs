use crate::Result;
use bincode::Options;
use channel::Sender;
use revision::Revisioned;
use serde::{ser::SerializeMap as _, Serialize};
use std::{collections::BTreeMap, io::Read, path::PathBuf};
use surrealdb_core::{
	dbs::Notification,
	sql::{Object, Query, Value},
};
use uuid::Uuid;

use super::MlExportConfig;

#[derive(Debug, Clone)]
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
	Create {
		what: Value,
		data: Option<Value>,
	},
	Upsert {
		one: bool,
		what: Value,
		data: Option<Value>,
	},
	Update {
		one: bool,
		what: Value,
		data: Option<Value>,
	},
	Insert {
		what: Option<Value>,
		data: Value,
	},
	Patch {
		one: bool,
		what: Value,
		data: Option<Value>,
	},
	Merge {
		one: bool,
		what: Value,
		data: Option<Value>,
	},
	Select {
		one: bool,
		what: Value,
	},
	Delete {
		one: bool,
		what: Value,
	},
	Query {
		query: Query,
		variables: BTreeMap<String, Value>,
	},
	#[cfg(not(target_arch = "wasm32"))]
	ExportFile {
		path: PathBuf,
	},
	#[cfg(all(not(target_arch = "wasm32"), feature = "ml"))]
	ExportMl {
		path: PathBuf,
		config: MlExportConfig,
	},
	#[cfg(not(target_arch = "wasm32"))]
	ExportBytes {
		bytes: Sender<Result<Vec<u8>>>,
	},
	#[cfg(all(not(target_arch = "wasm32"), feature = "ml"))]
	ExportBytesMl {
		bytes: Sender<Result<Vec<u8>>>,
		config: MlExportConfig,
	},
	#[cfg(not(target_arch = "wasm32"))]
	ImportFile {
		path: PathBuf,
	},
	#[cfg(all(not(target_arch = "wasm32"), feature = "ml"))]
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
		notification_sender: Sender<Notification>,
	},
	Kill {
		uuid: Uuid,
	},
}

impl Command {
	pub(crate) fn into_router_request(self, id: Option<i64>) -> Option<RouterRequest> {
		let id = id.map(Value::from);
		let res = match self {
			Command::Use {
				namespace,
				database,
			} => RouterRequest {
				id,
				method: Value::from("use"),
				params: Some(vec![Value::from(namespace), Value::from(database)].into()),
			},
			Command::Signup {
				credentials,
			} => RouterRequest {
				id,
				method: "signup".into(),
				params: Some(vec![Value::from(credentials)].into()),
			},
			Command::Signin {
				credentials,
			} => RouterRequest {
				id,
				method: "signin".into(),
				params: Some(vec![Value::from(credentials)].into()),
			},
			Command::Authenticate {
				token,
			} => RouterRequest {
				id,
				method: "signin".into(),
				params: Some(vec![Value::from(token)].into()),
			},
			Command::Invalidate => RouterRequest {
				id,
				method: "invalidate".into(),
				params: None,
			},
			Command::Create {
				what,
				data,
			} => {
				let mut params = vec![what];
				if let Some(data) = data {
					params.push(data);
				}

				RouterRequest {
					id,
					method: "create".into(),
					params: Some(params.into()),
				}
			}
			Command::Upsert {
				what,
				data,
				..
			} => {
				let mut params = vec![what];
				if let Some(data) = data {
					params.push(data);
				}

				RouterRequest {
					id,
					method: "upsert".into(),
					params: Some(params.into()),
				}
			}
			Command::Update {
				what,
				data,
				..
			} => {
				let mut params = vec![what];

				if let Some(data) = data {
					params.push(data);
				}

				RouterRequest {
					id,
					method: "update".into(),
					params: Some(params.into()),
				}
			}
			Command::Insert {
				what,
				data,
			} => {
				let mut params = if let Some(w) = what {
					vec![w]
				} else {
					vec![Value::None]
				};

				params.push(data);

				RouterRequest {
					id,
					method: "insert".into(),
					params: Some(params.into()),
				}
			}
			Command::Patch {
				what,
				data,
				..
			} => {
				let mut params = vec![what];
				if let Some(data) = data {
					params.push(data);
				}

				RouterRequest {
					id,
					method: "patch".into(),
					params: Some(params.into()),
				}
			}
			Command::Merge {
				what,
				data,
				..
			} => {
				let mut params = vec![what];
				if let Some(data) = data {
					params.push(data);
				}

				RouterRequest {
					id,
					method: "merge".into(),
					params: Some(params.into()),
				}
			}
			Command::Select {
				what,
				..
			} => RouterRequest {
				id,
				method: "select".into(),
				params: Some(vec![what].into()),
			},
			Command::Delete {
				what,
				..
			} => RouterRequest {
				id,
				method: "delete".into(),
				params: Some(vec![what].into()),
			},
			Command::Query {
				query,
				variables,
			} => {
				let params: Vec<Value> = vec![query.into(), variables.into()];
				RouterRequest {
					id,
					method: "query".into(),
					params: Some(params.into()),
				}
			}
			#[cfg(not(target_arch = "wasm32"))]
			Command::ExportFile {
				..
			}
			| Command::ExportBytes {
				..
			}
			| Command::ExportBytesMl {
				..
			}
			| Command::ImportFile {
				..
			} => return None,
			#[cfg(all(not(target_arch = "wasm32"), feature = "ml"))]
			Command::ExportMl {
				..
			}
			| Command::ImportMl {
				..
			} => return None,
			Command::Health => RouterRequest {
				id,
				method: "ping".into(),
				params: None,
			},
			Command::Version => RouterRequest {
				id,
				method: "version".into(),
				params: None,
			},
			Command::Set {
				key,
				value,
			} => RouterRequest {
				id,
				method: "let".into(),
				params: Some(vec![Value::from(key), value].into()),
			},
			Command::Unset {
				key,
			} => RouterRequest {
				id,
				method: "unset".into(),
				params: Some(vec![Value::from(key)].into()),
			},
			Command::SubscribeLive {
				..
			} => return None,
			Command::Kill {
				uuid,
			} => RouterRequest {
				id,
				method: "kill".into(),
				params: Some(vec![Value::from(uuid)].into()),
			},
		};
		Some(res)
	}
}

/// A struct which will be serialized as a map to behave like the previously used BTreeMap.
///
/// This struct serializes as if it is a surrealdb_core::sql::Value::Object.
#[derive(Debug)]
pub(crate) struct RouterRequest {
	id: Option<Value>,
	method: Value,
	params: Option<Value>,
}

impl Serialize for RouterRequest {
	fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		struct InnerRequest<'a>(&'a RouterRequest);

		impl Serialize for InnerRequest<'_> {
			fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
			where
				S: serde::Serializer,
			{
				let size = 1 + self.0.id.is_some() as usize + self.0.params.is_some() as usize;
				let mut map = serializer.serialize_map(Some(size))?;
				if let Some(id) = self.0.id.as_ref() {
					map.serialize_entry("id", id)?;
				}
				map.serialize_entry("method", &self.0.method)?;
				if let Some(params) = self.0.params.as_ref() {
					map.serialize_entry("params", params)?;
				}
				map.end()
			}
		}

		serializer.serialize_newtype_variant("Value", 9, "Object", &InnerRequest(self))
	}
}

impl Revisioned for RouterRequest {
	fn revision() -> u16 {
		1
	}

	fn serialize_revisioned<W: std::io::Write>(
		&self,
		w: &mut W,
	) -> std::result::Result<(), revision::Error> {
		// version
		Revisioned::serialize_revisioned(&1u32, w)?;
		// object variant
		Revisioned::serialize_revisioned(&9u32, w)?;
		// object wrapper version
		Revisioned::serialize_revisioned(&1u32, w)?;

		let size = 1 + self.id.is_some() as usize + self.params.is_some() as usize;
		size.serialize_revisioned(w)?;

		let serializer = bincode::options()
			.with_no_limit()
			.with_little_endian()
			.with_varint_encoding()
			.reject_trailing_bytes();

		if let Some(x) = self.id.as_ref() {
			serializer
				.serialize_into(&mut *w, "id")
				.map_err(|err| revision::Error::Serialize(err.to_string()))?;
			x.serialize_revisioned(w)?;
		}
		serializer
			.serialize_into(&mut *w, "method")
			.map_err(|err| revision::Error::Serialize(err.to_string()))?;
		self.method.serialize_revisioned(w)?;

		if let Some(x) = self.params.as_ref() {
			serializer
				.serialize_into(&mut *w, "params")
				.map_err(|err| revision::Error::Serialize(err.to_string()))?;
			x.serialize_revisioned(w)?;
		}

		Ok(())
	}

	fn deserialize_revisioned<R: Read>(_: &mut R) -> std::result::Result<Self, revision::Error>
	where
		Self: Sized,
	{
		panic!("deliberately unimplemented");
	}
}

#[cfg(test)]
mod test {
	use std::io::Cursor;

	use revision::Revisioned;
	use surrealdb_core::sql::Value;

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
		assert_eq!(obj.get("id").cloned(), req.id);
		assert_eq!(obj.get("method").unwrap().clone(), req.method);
		assert_eq!(obj.get("params").cloned(), req.params);
	}

	#[test]
	fn router_request_value_conversion() {
		let request = RouterRequest {
			id: Some(Value::from(1234i64)),
			method: Value::from("request"),
			params: Some(vec![Value::from(1234i64), Value::from("request")].into()),
		};

		println!("test convert bincode");

		assert_converts(
			&request,
			|i| bincode::serialize(i).unwrap(),
			|b| bincode::deserialize(&b).unwrap(),
		);

		println!("test convert json");

		assert_converts(
			&request,
			|i| serde_json::to_string(i).unwrap(),
			|b| serde_json::from_str(&b).unwrap(),
		);

		println!("test convert revisioned");

		assert_converts(
			&request,
			|i| {
				let mut buf = Vec::new();
				i.serialize_revisioned(&mut Cursor::new(&mut buf)).unwrap();
				buf
			},
			|b| Value::deserialize_revisioned(&mut Cursor::new(b)).unwrap(),
		);

		println!("done");
	}
}
