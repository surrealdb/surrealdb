use crate::dbs::Variables;
use crate::expr::Duration;
use crate::expr::{Data, Fetchs, Fields, TryFromValue, Value};
use crate::iam::AccessMethod;
use crate::iam::{SigninParams, SignupParams};
use crate::protocol::{FromFlatbuffers, ToFlatbuffers};
use crate::rpc::format::cbor::Cbor;
use crate::rpc::protocol::v1::types::{V1Array, V1Number, V1Uuid, V1Value};
use crate::rpc::{Method, RpcError};
use serde::{Deserialize, Serialize};
use surrealdb_protocol::proto::rpc::v1 as rpc_proto;

use uuid::Uuid;
pub static ID: &str = "id";
pub static METHOD: &str = "method";
pub static PARAMS: &str = "params";
pub static VERSION: &str = "version";
pub static TXN: &str = "txn";

#[derive(Debug)]
pub struct V1Request {
	pub id: Option<V1Value>,
	pub version: Option<u8>,
	pub txn: Option<Uuid>,
	pub method: Method,
	pub params: V1Array,
}

impl TryFrom<Cbor> for V1Request {
	type Error = RpcError;
	fn try_from(val: Cbor) -> Result<Self, RpcError> {
		V1Value::try_from(val).map_err(|err| RpcError::InvalidRequest(err.to_string()))?.try_into()
	}
}

impl TryFrom<V1Value> for V1Request {
	type Error = RpcError;
	fn try_from(val: V1Value) -> Result<Self, RpcError> {
		// Fetch the 'id' argument
		let id = match val.get_field_value("id") {
			V1Value::None => None,
			V1Value::Null => Some(V1Value::Null),
			V1Value::Uuid(v) => Some(V1Value::Uuid(v)),
			V1Value::Number(v) => Some(V1Value::Number(v)),
			V1Value::Strand(v) => Some(V1Value::Strand(v)),
			V1Value::Datetime(v) => Some(V1Value::Datetime(v)),
			unexpected => {
				return Err(RpcError::InvalidRequest(format!("Unexpected id: {:?}", unexpected)));
			}
		};

		// Fetch the 'version' argument
		let version = match val.get_field_value(VERSION) {
			V1Value::None => None,
			V1Value::Null => None,
			V1Value::Number(v) => match v {
				V1Number::Int(1) => Some(1),
				V1Number::Int(2) => Some(2),
				unexpected => {
					return Err(RpcError::InvalidRequest(format!(
						"Unexpected version: {:?}",
						unexpected
					)));
				}
			},
			unexpected => {
				return Err(RpcError::InvalidRequest(format!(
					"Unexpected version: {:?}",
					unexpected
				)));
			}
		};
		// Fetch the 'txn' argument
		let txn = match val.get_field_value(TXN) {
			V1Value::None => None,
			V1Value::Null => None,
			V1Value::Uuid(x) => Some(x.0),
			V1Value::Strand(x) => Some(
				Uuid::try_parse(&x.0).map_err(|err| RpcError::InvalidRequest(err.to_string()))?,
			),
			unexpected => {
				return Err(RpcError::InvalidRequest(format!("Unexpected txn: {:?}", unexpected)));
			}
		};
		// Fetch the 'method' argument
		let method = match val.get_field_value(METHOD) {
			V1Value::Strand(v) => v.0,
			unexpected => {
				return Err(RpcError::InvalidRequest(format!(
					"Unexpected method: {:?}",
					unexpected
				)));
			}
		};
		// Fetch the 'params' argument
		let params = match val.get_field_value(PARAMS) {
			V1Value::Array(v) => v,
			_ => V1Array::default(),
		};
		// Parse the specified method
		let method = Method::parse_case_sensitive(method);
		// Return the parsed request
		Ok(V1Request {
			id,
			method,
			params,
			version,
			txn,
		})
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthParams {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionParams {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PingParams {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InfoParams {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UseParams {
	pub namespace: Option<String>,
	pub database: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthenticateParams {
	pub token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvalidateParams {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateParams {
	pub only: Option<bool>,
	pub what: Value,
	pub data: Option<Value>,
	pub output: Option<Value>,
	pub timeout: Option<Duration>,
	pub parallel: Option<bool>,
	pub version: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResetParams {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KillParams {
	pub live_uuid: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveParams {
	pub what: Value,
	pub expr: Fields,
	pub cond: Option<Value>,
	pub fetch: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetParams {
	pub key: String,
	pub value: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnsetParams {
	pub key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelectParams {
	pub expr: Fields,
	pub omit: Option<Value>,
	pub only: Option<bool>,
	pub what: Value,
	pub with: Option<Value>,
	pub cond: Option<Value>,
	pub split: Option<Value>,
	pub group: Option<Value>,
	pub order: Option<Value>,
	pub start: Option<u64>,
	pub limit: Option<u64>,
	pub fetch: Option<Fetchs>,
	pub version: Option<Value>,
	pub timeout: Option<Duration>,
	pub parallel: Option<bool>,
	pub explain: Option<Value>,
	pub tempfiles: Option<bool>,
	pub variables: Variables,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InsertParams {
	pub into: Value,
	pub data: Value,
	pub ignore: Option<bool>,
	pub update: Option<Value>,
	pub output: Option<Value>,
	pub timeout: Option<Duration>,
	pub parallel: Option<bool>,
	pub relation: Option<bool>,
	pub version: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpsertParams {
	pub only: Option<bool>,
	pub what: Value,
	pub with: Option<Value>,
	pub data: Option<Data>,
	pub cond: Option<Value>,
	pub output: Option<Value>,
	pub timeout: Option<Duration>,
	pub parallel: Option<bool>,
	pub explain: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateParams {
	pub only: Option<bool>,
	pub what: Value,
	pub with: Option<Value>,
	pub data: Option<Data>,
	pub cond: Option<Value>,
	pub output: Option<Value>,
	pub timeout: Option<Duration>,
	pub parallel: Option<bool>,
	pub explain: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteParams {
	pub only: Option<bool>,
	pub what: Value,
	pub with: Option<Value>,
	pub cond: Option<Value>,
	pub output: Option<Value>,
	pub timeout: Option<Duration>,
	pub parallel: Option<bool>,
	pub explain: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryParams {
	pub query: String,
	pub variables: Variables,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelateParams {
	pub only: Option<bool>,
	pub kind: Value,
	pub from: Value,
	pub with: Option<Value>,
	pub uniq: Option<bool>,
	pub data: Option<Value>,
	pub output: Option<Value>,
	pub timeout: Option<Duration>,
	pub parallel: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunParams {
	pub name: String,
	pub version: Option<String>,
	pub args: Vec<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphQlParams {
	pub query: String,
	pub variables: Variables,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Command {
	Health(HealthParams),
	Version(VersionParams),
	Ping(PingParams),
	Info(InfoParams),
	Use(UseParams),
	Signup(SignupParams),
	Signin(SigninParams),
	Authenticate(AuthenticateParams),
	Invalidate(InvalidateParams),
	Create(CreateParams),
	Reset(ResetParams),
	Kill(KillParams),
	Live(LiveParams),
	Set(SetParams),
	Unset(UnsetParams),
	Select(SelectParams),
	Insert(InsertParams),
	Upsert(UpsertParams),
	Update(UpdateParams),
	Delete(DeleteParams),
	Query(QueryParams),
	Relate(RelateParams),
	Run(RunParams),
	GraphQl(GraphQlParams),
}
