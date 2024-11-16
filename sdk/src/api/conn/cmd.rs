use super::MlExportConfig;
use crate::{opt::Resource, value::Notification, Result};
use bincode::Options;
use channel::Sender;
use revision::Revisioned;
use serde::{ser::SerializeMap as _, Serialize};
use std::io::Read;
use std::path::PathBuf;
use surrealdb_core::sql::{Array as CoreArray, Object as CoreObject, Query, Value as CoreValue};
use uuid::Uuid;
use surrealdb_core::kvs::export::Config as DbExportConfig;

#[cfg(any(feature = "protocol-ws", feature = "protocol-http"))]
use surrealdb_core::sql::Table as CoreTable;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) enum Command {
	Use {
		namespace: Option<String>,
		database: Option<String>,
	},
	Signup {
		credentials: CoreObject,
	},
	Signin {
		credentials: CoreObject,
	},
	Authenticate {
		token: String,
	},
	Invalidate,
	Create {
		what: Resource,
		data: Option<CoreValue>,
	},
	Upsert {
		what: Resource,
		data: Option<CoreValue>,
	},
	Update {
		what: Resource,
		data: Option<CoreValue>,
	},
	Insert {
		// inserts can only be on a table.
		what: Option<String>,
		data: CoreValue,
	},
	InsertRelation {
		what: Option<String>,
		data: CoreValue,
	},
	Patch {
		what: Resource,
		data: Option<CoreValue>,
	},
	Merge {
		what: Resource,
		data: Option<CoreValue>,
	},
	Select {
		what: Resource,
	},
	Delete {
		what: Resource,
	},
	Query {
		query: Query,
		variables: CoreObject,
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
		value: CoreValue,
	},
	Unset {
		key: String,
	},
	SubscribeLive {
		uuid: Uuid,
		notification_sender: Sender<Notification<CoreValue>>,
	},
	Kill {
		uuid: Uuid,
	},
	Run {
		name: String,
		version: Option<String>,
		args: CoreArray,
	},
}

impl Command {
	#[cfg(any(feature = "protocol-ws", feature = "protocol-http"))]
	pub(crate) fn into_router_request(self, id: Option<i64>) -> Option<RouterRequest> {
		let res = match self {
			Command::Use {
				namespace,
				database,
			} => RouterRequest {
				id,
				method: "use",
				params: Some(vec![CoreValue::from(namespace), CoreValue::from(database)].into()),
			},
			Command::Signup {
				credentials,
			} => RouterRequest {
				id,
				method: "signup",
				params: Some(vec![CoreValue::from(credentials)].into()),
			},
			Command::Signin {
				credentials,
			} => RouterRequest {
				id,
				method: "signin",
				params: Some(vec![CoreValue::from(credentials)].into()),
			},
			Command::Authenticate {
				token,
			} => RouterRequest {
				id,
				method: "authenticate",
				params: Some(vec![CoreValue::from(token)].into()),
			},
			Command::Invalidate => RouterRequest {
				id,
				method: "invalidate",
				params: None,
			},
			Command::Create {
				what,
				data,
			} => {
				let mut params = vec![what.into_core_value()];
				if let Some(data) = data {
					params.push(data);
				}

				RouterRequest {
					id,
					method: "create",
					params: Some(params.into()),
				}
			}
			Command::Upsert {
				what,
				data,
				..
			} => {
				let mut params = vec![what.into_core_value()];
				if let Some(data) = data {
					params.push(data);
				}

				RouterRequest {
					id,
					method: "upsert",
					params: Some(params.into()),
				}
			}
			Command::Update {
				what,
				data,
				..
			} => {
				let mut params = vec![what.into_core_value()];

				if let Some(data) = data {
					params.push(data);
				}

				RouterRequest {
					id,
					method: "update",
					params: Some(params.into()),
				}
			}
			Command::Insert {
				what,
				data,
			} => {
				let table = match what {
					Some(w) => {
						let mut table = CoreTable::default();
						table.0.clone_from(&w);
						CoreValue::from(table)
					}
					None => CoreValue::None,
				};

				let params = vec![table, data];

				RouterRequest {
					id,
					method: "insert",
					params: Some(params.into()),
				}
			}
			Command::InsertRelation {
				what,
				data,
			} => {
				let table = match what {
					Some(w) => {
						let mut tmp = CoreTable::default();
						tmp.0 = w.clone();
						CoreValue::from(tmp)
					}
					None => CoreValue::None,
				};
				let params = vec![table, data];

				RouterRequest {
					id,
					method: "insert_relation",
					params: Some(params.into()),
				}
			}
			Command::Patch {
				what,
				data,
				..
			} => {
				let mut params = vec![what.into_core_value()];

				if let Some(data) = data {
					params.push(data);
				}

				RouterRequest {
					id,
					method: "patch",
					params: Some(params.into()),
				}
			}
			Command::Merge {
				what,
				data,
				..
			} => {
				let mut params = vec![what.into_core_value()];
				if let Some(data) = data {
					params.push(data)
				}

				RouterRequest {
					id,
					method: "merge",
					params: Some(params.into()),
				}
			}
			Command::Select {
				what,
				..
			} => RouterRequest {
				id,
				method: "select",
				params: Some(CoreValue::Array(vec![what.into_core_value()].into())),
			},
			Command::Delete {
				what,
				..
			} => RouterRequest {
				id,
				method: "delete",
				params: Some(CoreValue::Array(vec![what.into_core_value()].into())),
			},
			Command::Query {
				query,
				variables,
			} => {
				let params: Vec<CoreValue> = vec![query.into(), variables.into()];
				RouterRequest {
					id,
					method: "query",
					params: Some(params.into()),
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
			},
			Command::Version => RouterRequest {
				id,
				method: "version",
				params: None,
			},
			Command::Set {
				key,
				value,
			} => RouterRequest {
				id,
				method: "let",
				params: Some(CoreValue::from(vec![CoreValue::from(key), value])),
			},
			Command::Unset {
				key,
			} => RouterRequest {
				id,
				method: "unset",
				params: Some(CoreValue::from(vec![CoreValue::from(key)])),
			},
			Command::SubscribeLive {
				..
			} => return None,
			Command::Kill {
				uuid,
			} => RouterRequest {
				id,
				method: "kill",
				params: Some(CoreValue::from(vec![CoreValue::from(uuid)])),
			},
			Command::Run {
				name,
				version,
				args,
			} => RouterRequest {
				id,
				method: "run",
				params: Some(
					vec![CoreValue::from(name), CoreValue::from(version), CoreValue::Array(args)]
						.into(),
				),
			},
		};
		Some(res)
	}

	#[cfg(feature = "protocol-http")]
	pub(crate) fn needs_flatten(&self) -> bool {
		match self {
			Command::Upsert {
				what,
				..
			}
			| Command::Update {
				what,
				..
			}
			| Command::Patch {
				what,
				..
			}
			| Command::Merge {
				what,
				..
			}
			| Command::Select {
				what,
			}
			| Command::Delete {
				what,
			} => matches!(what, Resource::RecordId(_)),
			Command::Insert {
				data,
				..
			} => !data.is_array(),
			_ => false,
		}
	}
}

/// A struct which will be serialized as a map to behave like the previously used BTreeMap.
///
/// This struct serializes as if it is a surrealdb_core::sql::Value::Object.
#[derive(Debug)]
pub(crate) struct RouterRequest {
	id: Option<i64>,
	method: &'static str,
	params: Option<CoreValue>,
}

impl Serialize for RouterRequest {
	fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		struct InnerRequest<'a>(&'a RouterRequest);
		struct InnerNumberVariant(i64);
		struct InnerNumber(i64);
		struct InnerMethod(&'static str);
		struct InnerStrand(&'static str);
		struct InnerObject<'a>(&'a RouterRequest);

		impl Serialize for InnerNumberVariant {
			fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
			where
				S: serde::Serializer,
			{
				serializer.serialize_newtype_variant("Value", 3, "Number", &InnerNumber(self.0))
			}
		}

		impl Serialize for InnerNumber {
			fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
			where
				S: serde::Serializer,
			{
				serializer.serialize_newtype_variant("Number", 0, "Int", &self.0)
			}
		}

		impl Serialize for InnerMethod {
			fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
			where
				S: serde::Serializer,
			{
				serializer.serialize_newtype_variant("Value", 4, "Strand", &InnerStrand(self.0))
			}
		}

		impl Serialize for InnerStrand {
			fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
			where
				S: serde::Serializer,
			{
				serializer.serialize_newtype_struct("$surrealdb::private::sql::Strand", self.0)
			}
		}

		impl Serialize for InnerRequest<'_> {
			fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
			where
				S: serde::Serializer,
			{
				let size = 1 + self.0.id.is_some() as usize + self.0.params.is_some() as usize;
				let mut map = serializer.serialize_map(Some(size))?;
				if let Some(id) = self.0.id.as_ref() {
					map.serialize_entry("id", &InnerNumberVariant(*id))?;
				}
				map.serialize_entry("method", &InnerMethod(self.0.method))?;
				if let Some(params) = self.0.params.as_ref() {
					map.serialize_entry("params", params)?;
				}
				map.end()
			}
		}

		impl Serialize for InnerObject<'_> {
			fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
			where
				S: serde::Serializer,
			{
				serializer.serialize_newtype_struct("Object", &InnerRequest(self.0))
			}
		}

		serializer.serialize_newtype_variant(
			"$surrealdb::private::sql::Value",
			9,
			"Object",
			&InnerObject(self),
		)
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

			// the Value version
			1u16.serialize_revisioned(w)?;

			// the Value::Number variant
			3u16.serialize_revisioned(w)?;

			// the Number version
			1u16.serialize_revisioned(w)?;

			// the Number::Int variant
			0u16.serialize_revisioned(w)?;

			x.serialize_revisioned(w)?;
		}

		serializer
			.serialize_into(&mut *w, "method")
			.map_err(|err| revision::Error::Serialize(err.to_string()))?;

		// the Value version
		1u16.serialize_revisioned(w)?;

		// the Value::Strand variant
		4u16.serialize_revisioned(w)?;

		// the Strand version
		1u16.serialize_revisioned(w)?;

		serializer
			.serialize_into(&mut *w, self.method)
			.map_err(|e| revision::Error::Serialize(format!("{:?}", e)))?;

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
	use surrealdb_core::sql::{Number, Value};

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
		let Some(Value::Strand(x)) = obj.get("method") else {
			panic!("invalid method field: {}", obj)
		};
		assert_eq!(x.0, req.method);

		assert_eq!(obj.get("params").cloned(), req.params);
	}

	#[test]
	fn router_request_value_conversion() {
		let request = RouterRequest {
			id: Some(1234),
			method: "request",
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
