use std::borrow::Cow;
use std::io::Read;
use std::path::PathBuf;

use async_channel::Sender;
use bincode::Options;
use revision::Revisioned;
use serde::Serialize;
use serde::ser::SerializeMap as _;
use uuid::Uuid;

use super::MlExportConfig;
use crate::Result;
use crate::core::dbs::Notification;
use crate::core::expr::LogicalPlan;
use crate::core::kvs::export::Config as DbExportConfig;
#[cfg(any(feature = "protocol-ws", feature = "protocol-http"))]
use crate::core::val::Table as CoreTable;
#[allow(unused_imports)]
use crate::core::val::{Array as CoreArray, Object as CoreObject, Value as CoreValue};
use crate::opt::Resource;

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
		txn: Option<Uuid>,
		what: Resource,
		data: Option<CoreValue>,
	},
	Upsert {
		txn: Option<Uuid>,
		what: Resource,
		data: Option<CoreValue>,
	},
	Update {
		txn: Option<Uuid>,
		what: Resource,
		data: Option<CoreValue>,
	},
	Insert {
		txn: Option<Uuid>,
		// inserts can only be on a table.
		what: Option<String>,
		data: CoreValue,
	},
	InsertRelation {
		txn: Option<Uuid>,
		what: Option<String>,
		data: CoreValue,
	},
	Patch {
		txn: Option<Uuid>,
		what: Resource,
		data: Option<CoreValue>,
		upsert: bool,
	},
	Merge {
		txn: Option<Uuid>,
		what: Resource,
		data: Option<CoreValue>,
		upsert: bool,
	},
	Select {
		txn: Option<Uuid>,
		what: Resource,
	},
	Delete {
		txn: Option<Uuid>,
		what: Resource,
	},
	Query {
		txn: Option<Uuid>,
		query: LogicalPlan,
		variables: CoreObject,
	},
	RawQuery {
		txn: Option<Uuid>,
		query: Cow<'static, str>,
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
		notification_sender: Sender<Notification>,
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
		use crate::core::expr::{Data, Output, UpdateStatement, UpsertStatement};
		use crate::core::val::{self, Strand};
		use crate::engine::resource_to_exprs;

		let res = match self {
			Command::Use {
				namespace,
				database,
			} => {
				// TODO: Null byte validity
				let namespace = namespace
					.map(|n| unsafe { Strand::new_unchecked(n) }.into())
					.unwrap_or(CoreValue::None);
				let database = database
					.map(|d| unsafe { Strand::new_unchecked(d) }.into())
					.unwrap_or(CoreValue::None);
				RouterRequest {
					id,
					method: "use",
					params: Some(vec![namespace, database].into()),
					transaction: None,
				}
			}
			Command::Signup {
				credentials,
			} => RouterRequest {
				id,
				method: "signup",
				params: Some(vec![CoreValue::from(credentials)].into()),
				transaction: None,
			},
			Command::Signin {
				credentials,
			} => RouterRequest {
				id,
				method: "signin",
				params: Some(vec![CoreValue::from(credentials)].into()),
				transaction: None,
			},
			Command::Authenticate {
				token,
			} => RouterRequest {
				id,
				method: "authenticate",
				params: Some(vec![CoreValue::from(token)].into()),
				transaction: None,
			},
			Command::Invalidate => RouterRequest {
				id,
				method: "invalidate",
				params: None,
				transaction: None,
			},
			Command::Create {
				txn,
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
					transaction: txn,
				}
			}
			Command::Upsert {
				txn,
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
					transaction: txn,
				}
			}
			Command::Update {
				txn,
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
					transaction: txn,
				}
			}
			Command::Insert {
				txn,
				what,
				data,
			} => {
				let table = match what {
					Some(w) => {
						// TODO: Null byte validity
						let table = unsafe { CoreTable::new_unchecked(w) };
						CoreValue::from(table)
					}
					None => CoreValue::None,
				};

				let params = vec![table, data];

				RouterRequest {
					id,
					method: "insert",
					params: Some(params.into()),
					transaction: txn,
				}
			}
			Command::InsertRelation {
				txn,
				what,
				data,
			} => {
				let table = match what {
					Some(w) => {
						// TODO: Null byte validity
						let table = unsafe { CoreTable::new_unchecked(w) };
						CoreValue::from(table)
					}
					None => CoreValue::None,
				};
				let params = vec![table, data];

				RouterRequest {
					id,
					method: "insert_relation",
					params: Some(params.into()),
					transaction: txn,
				}
			}
			Command::Patch {
				txn,
				what,
				data,
				upsert,
				..
			} => {
				let query = if upsert {
					let expr = UpsertStatement {
						only: false,
						what: resource_to_exprs(what),
						with: None,
						data: data.map(|x| Data::PatchExpression(x.into_literal())),
						cond: None,
						output: Some(Output::After),
						timeout: None,
						parallel: false,
						explain: None,
					};
					expr.to_string()
				} else {
					let expr = UpdateStatement {
						only: false,
						what: resource_to_exprs(what),
						with: None,
						data: data.map(|x| Data::PatchExpression(x.into_literal())),
						cond: None,
						output: Some(Output::After),
						timeout: None,
						parallel: false,
						explain: None,
					};
					expr.to_string()
				};
				//TODO: Null byte validity
				let query = unsafe { Strand::new_unchecked(query) };

				let variables = val::Object::default();
				let params: Vec<CoreValue> = vec![query.into(), variables.into()];

				RouterRequest {
					id,
					method: "query",
					params: Some(params.into()),
					transaction: txn,
				}
			}
			Command::Merge {
				txn,
				what,
				data,
				upsert,
				..
			} => {
				let query = if upsert {
					let expr = UpsertStatement {
						only: false,
						what: resource_to_exprs(what),
						with: None,
						data: data.map(|x| Data::MergeExpression(x.into_literal())),
						cond: None,
						output: Some(Output::After),
						timeout: None,
						parallel: false,
						explain: None,
					};
					expr.to_string()
				} else {
					let expr = UpdateStatement {
						only: false,
						what: resource_to_exprs(what),
						with: None,
						data: data.map(|x| Data::MergeExpression(x.into_literal())),
						cond: None,
						output: Some(Output::After),
						timeout: None,
						parallel: false,
						explain: None,
					};
					expr.to_string()
				};
				//TODO: Null byte validity
				let query = unsafe { Strand::new_unchecked(query) };

				let variables = val::Object::default();
				let params: Vec<CoreValue> = vec![query.into(), variables.into()];

				RouterRequest {
					id,
					method: "query",
					params: Some(params.into()),
					transaction: txn,
				}
			}
			Command::Select {
				txn,
				what,
				..
			} => RouterRequest {
				id,
				method: "select",
				params: Some(CoreValue::Array(vec![what.into_core_value()].into())),
				transaction: txn,
			},
			Command::Delete {
				txn,
				what,
				..
			} => RouterRequest {
				id,
				method: "delete",
				params: Some(CoreValue::Array(vec![what.into_core_value()].into())),
				transaction: txn,
			},
			Command::Query {
				txn,
				query,
				variables,
			} => {
				// TODO: Null byte validity
				let query = unsafe { Strand::new_unchecked(query.to_string()) };
				let params: Vec<CoreValue> = vec![query.into(), variables.into()];
				RouterRequest {
					id,
					method: "query",
					params: Some(params.into()),
					transaction: txn,
				}
			}
			Command::RawQuery {
				txn,
				query,
				variables,
			} => {
				let params: Vec<CoreValue> = vec![query.into_owned().into(), variables.into()];
				RouterRequest {
					id,
					method: "query",
					params: Some(params.into()),
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
				params: Some(CoreValue::from(vec![CoreValue::from(key), value])),
				transaction: None,
			},
			Command::Unset {
				key,
			} => RouterRequest {
				id,
				method: "unset",
				params: Some(CoreValue::from(vec![CoreValue::from(key)])),
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
				params: Some(CoreValue::from(vec![CoreValue::from(val::Uuid(uuid))])),
				transaction: None,
			},
			Command::Run {
				name,
				version,
				args,
			} => {
				// TODO: Null byte validity
				let version = version
					.map(|x| unsafe { Strand::new_unchecked(x) }.into())
					.unwrap_or(CoreValue::None);
				RouterRequest {
					id,
					method: "run",
					params: Some(
						vec![CoreValue::from(name), version, CoreValue::Array(args)].into(),
					),
					transaction: None,
				}
			}
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
				..
			}
			| Command::Delete {
				what,
				..
			} => matches!(what, Resource::RecordId(_)),
			Command::Insert {
				data,
				..
			} => !data.is_array(),
			_ => false,
		}
	}
}

/// A struct which will be serialized as a map to behave like the previously
/// used BTreeMap.
///
/// This struct serializes as if it is a crate::core::expr::Value::Object.
#[derive(Debug)]
pub(crate) struct RouterRequest {
	id: Option<i64>,
	method: &'static str,
	params: Option<CoreValue>,
	#[allow(dead_code)]
	transaction: Option<Uuid>,
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

		if let Some(x) = self.transaction.as_ref() {
			serializer
				.serialize_into(&mut *w, "transaction")
				.map_err(|err| revision::Error::Serialize(err.to_string()))?;

			// the Value version
			1u16.serialize_revisioned(w)?;

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
	use uuid::Uuid;

	use super::RouterRequest;
	use crate::core::val::{Number, Value};

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
		assert_eq!(x.as_str(), req.method);

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
			|i| crate::core::rpc::format::bincode::encode(i).unwrap(),
			|b| crate::core::rpc::format::bincode::decode(&b).unwrap(),
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
