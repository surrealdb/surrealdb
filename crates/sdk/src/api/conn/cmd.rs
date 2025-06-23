use super::MlExportConfig;
use crate::{Result, opt::Resource};
use async_channel::Sender;
use bincode::Options;
use revision::Revisioned;
use semver::Op;
use serde::{Serialize, ser::SerializeMap as _};
use surrealdb_core::dbs::Variables;
use surrealdb_core::expr::{Data, Fields, Values};
use surrealdb_core::iam::{SigninParams, SignupParams};
use surrealdb_core::protocol::{FromFlatbuffers, ToFlatbuffers};
use std::borrow::Cow;
use std::io::Read;
use std::path::PathBuf;
#[allow(unused_imports)]
use surrealdb_core::expr::{
	Array as Array, Object as Object, Query as Query, Value as Value,
};
use surrealdb_core::kvs::export::Config as DbExportConfig;
#[allow(unused_imports)]
use surrealdb_core::sql::{
	Object as SqlObject, Query as SqlQuery, SqlValue as SqlValue,
};
use surrealdb_core::{
	dbs::Notification,
};
use surrealdb_core::protocol::flatbuffers::surreal_db::protocol::rpc as rpc_fb;
use uuid::Uuid;

#[cfg(any(feature = "protocol-ws", feature = "protocol-http"))]
use surrealdb_core::expr::Table as Table;

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
		Self { id, command }
	}

	pub(crate) fn with_id(mut self, id: String) -> Self {
		self.id = id;
		self
	}
}

impl ToFlatbuffers for Request {
	type Output<'a> = flatbuffers::WIPOffset<rpc_fb::Request<'a>>;
	fn to_fb<'a>(&self, fbb: &mut flatbuffers::FlatBufferBuilder<'a>) -> Self::Output<'a> {
		let id = fbb.create_string(&self.id);
		let command = self.command.to_fb(fbb);

		rpc_fb::Request::create(
			fbb,
			&rpc_fb::RequestArgs {
				id: Some(id),
				command: Some(command),
			},
		)
	}
}

#[derive(Debug, Clone)]
pub struct LiveQueryParams {
	pub what: Value,
	pub cond: Option<Value>,
	pub fields: Fields,
}

// impl ToFlatbuffers for LiveQueryParams {
// 	type Output<'a> = flatbuffers::WIPOffset<rpc_fb::LiveQueryParams<'a>>;
// 	fn to_fb<'a>(&self, fbb: &mut flatbuffers::FlatBufferBuilder<'a>) -> Self::Output<'a> {
// 		let what = self.what.to_fb(fbb);
// 		let cond = self.cond.as_ref().map(|c| c.to_fb(fbb));

// 		rpc_fb::LiveQueryParams::create(
// 			fbb,
// 			&rpc_fb::LiveQueryParamsArgs {
// 				what: Some(what),
// 				cond,
// 			},
// 		)
// 	}
// }

// impl FromFlatbuffers for LiveQueryParams {
// 	type Input<'a> = rpc_fb::LiveQueryParams<'a>;

// 	fn from_fb(input: &Self::Input<'_>) -> Self {
// 		let what = Values::from_fb(input.what().unwrap());
// 		let cond = input.cond().map(Value::from_fb);

// 		Self { what, cond }
// 	}
// }

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
		what: Values,
		data: Option<Value>,
	},
	Upsert {
		what: Values,
		data: Option<Data>,
	},
	Update {
		what: Values,
		data: Option<Data>,
	},
	Insert {
		// inserts can only be on a table.
		what: Option<String>,
		data: Value,
	},
	Select {
		what: Values,
	},
	Delete {
		what: Values,
	},
	Query {
		query: Cow<'static, str>,
		variables: Variables,
	},
	MultiQuery {
		queries: Vec<Cow<'static, str>>,
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
			}
			| Command::Delete {
				what,
			} => matches!(what, Value::Thing(_)),
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
	use surrealdb_core::expr::{Number, Value};

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
