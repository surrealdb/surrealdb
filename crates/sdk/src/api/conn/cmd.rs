use super::MlExportConfig;
use crate::Result;
use async_channel::Sender;
use bincode::Options;
use revision::Revisioned;
use serde::{Serialize, ser::SerializeMap as _};
use std::io::Read;
use std::path::PathBuf;
use surrealdb_core::dbs::Notification;
use surrealdb_core::dbs::Variables;
#[allow(unused_imports)]
use surrealdb_core::expr::{Array, Object, Query, Value};
use surrealdb_core::expr::{Data, Fields, Values};
use surrealdb_core::iam::{SigninParams, SignupParams};
use surrealdb_core::kvs::export::Config as DbExportConfig;
#[allow(unused_imports)]
use surrealdb_core::sql::{Object as SqlObject, Query as SqlQuery, SqlValue};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub(crate) struct Request {
	pub(crate) id: String,
	pub(crate) command: Command,
}

impl Request {
	pub(crate) fn new(command: Command) -> Self {
		Self {
			id: Uuid::new_v4().to_string(),
			command,
		}
	}

	pub(crate) fn new_with_id(id: String, command: Command) -> Self {
		Self {
			id,
			command,
		}
	}

	pub(crate) fn with_id(mut self, id: String) -> Self {
		self.id = id;
		self
	}
}

#[derive(Debug, Clone)]
pub struct LiveQueryParams {
	pub txn: Option<Uuid>,
	pub what: Value,
	pub cond: Option<Value>,
	pub fields: Fields,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) enum Command {
	Use {
		namespace: Option<String>,
		database: Option<String>,
	},
	Signup(SignupParams),
	Signin(SigninParams),
	Authenticate {
		token: String,
	},
	Invalidate,
	Create {
		txn: Option<Uuid>,
		what: Values,
		data: Option<Value>,
	},
	Upsert {
		txn: Option<Uuid>,
		what: Values,
		data: Option<Data>,
	},
	Update {
		txn: Option<Uuid>,
		what: Values,
		data: Option<Data>,
	},
	Insert {
		txn: Option<Uuid>,
		// inserts can only be on a table.
		what: Option<String>,
		data: Value,
	},
	Select {
		txn: Option<Uuid>,
		what: Values,
	},
	Delete {
		txn: Option<Uuid>,
		what: Values,
	},
	Query {
		txn: Option<Uuid>,
		query: String,
		variables: Variables,
	},
	MultiQuery {
		txn: Option<Uuid>,
		queries: Vec<String>,
		variables: Variables,
	},
	LiveQuery(LiveQueryParams),
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
		notification_sender: Sender<Notification>,
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
			| Command::Select {
				what,
				..
			}
			| Command::Delete {
				what,
				..
			} => {
				for value in what.iter() {
					if matches!(value, Value::Thing(_)) {
						return true;
					}
				}

				false
			}
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
/// This struct serializes as if it is a surrealdb_core::expr::Value::Object.
#[derive(Debug)]
pub(crate) struct RouterRequest {
	id: Option<i64>,
	method: &'static str,
	params: Option<Value>,
	#[allow(dead_code)]
	transaction: Option<Uuid>,
}

#[cfg(feature = "protocol-ws")]
fn stringify_queries(value: Value) -> Value {
	match value {
		Value::Query(query) => Value::Strand(query.to_string().into()),
		Value::Array(array) => Value::Array(Array::from(
			array.0.into_iter().map(stringify_queries).collect::<Vec<_>>(),
		)),
		_ => value,
	}
}

impl RouterRequest {
	#[cfg(feature = "protocol-ws")]
	pub(crate) fn stringify_queries(self) -> Self {
		Self {
			params: self.params.map(stringify_queries),
			..self
		}
	}
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
		struct InnerTransaction<'a>(&'a Uuid);
		struct InnerUuid<'a>(&'a Uuid);
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

		impl Serialize for InnerTransaction<'_> {
			fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
			where
				S: serde::Serializer,
			{
				serializer.serialize_newtype_variant("Value", 7, "Uuid", &InnerUuid(self.0))
			}
		}

		impl Serialize for InnerUuid<'_> {
			fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
			where
				S: serde::Serializer,
			{
				serializer.serialize_newtype_struct("$surrealdb::private::sql::Uuid", self.0)
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
				if let Some(txn) = self.0.transaction.as_ref() {
					map.serialize_entry("transaction", &InnerTransaction(txn))?;
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
		2
	}

	fn serialize_revisioned<W: std::io::Write>(
		&self,
		w: &mut W,
	) -> std::result::Result<(), revision::Error> {
		// version
		Revisioned::serialize_revisioned(&2u32, w)?;
		// object variant
		Revisioned::serialize_revisioned(&9u32, w)?;
		// object wrapper version
		Revisioned::serialize_revisioned(&1u32, w)?;

		let size = 1
			+ self.id.is_some() as usize
			+ self.params.is_some() as usize
			+ self.transaction.is_some() as usize;
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
			2u16.serialize_revisioned(w)?;

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
		2u16.serialize_revisioned(w)?;

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

		if let Some(x) = self.transaction.as_ref() {
			serializer
				.serialize_into(&mut *w, "transaction")
				.map_err(|err| revision::Error::Serialize(err.to_string()))?;

			// the Value version
			2u16.serialize_revisioned(w)?;

			// the Value::Uuid variant
			7u16.serialize_revisioned(w)?;

			// the Uuid version
			1u16.serialize_revisioned(w)?;

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
	use surrealdb_core::expr::{Number, Value};
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
			transaction: Some(Uuid::new_v4()),
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
