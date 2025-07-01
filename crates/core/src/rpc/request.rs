use crate::dbs::Variables;
use crate::expr::Duration;
use crate::expr::{Data, Fetchs, Fields, TryFromValue, Value};
use crate::iam::AccessMethod;
use crate::iam::{SigninParams, SignupParams};
use surrealdb_protocol::proto::rpc::v1 as rpc_proto;
use crate::protocol::{FromFlatbuffers, ToFlatbuffers};
use serde::{Deserialize, Serialize};

use uuid::Uuid;

pub static ID: &str = "id";
pub static METHOD: &str = "method";
pub static PARAMS: &str = "params";
pub static VERSION: &str = "version";
pub static TXN: &str = "txn";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Request {
	pub id: Option<String>,
	pub command: Command,
}

impl Request {
	pub fn method(&self) -> String {
		match &self.command {
			Command::Health(_) => "health".to_string(),
			Command::Version(_) => "version".to_string(),
			Command::Ping(_) => "ping".to_string(),
			Command::Info(_) => "info".to_string(),
			Command::Use(_) => "use".to_string(),
			Command::Signup(_) => "signup".to_string(),
			Command::Signin(_) => "signin".to_string(),
			Command::Authenticate(_) => "authenticate".to_string(),
			Command::Invalidate(_) => "invalidate".to_string(),
			Command::Create(_) => "create".to_string(),
			Command::Reset(_) => "reset".to_string(),
			Command::Kill(_) => "kill".to_string(),
			Command::Live(_) => "live".to_string(),
			Command::Set(_) => "set".to_string(),
			Command::Unset(_) => "unset".to_string(),
			Command::Select(_) => "select".to_string(),
			Command::Insert(_) => "insert".to_string(),
			Command::Upsert(_) => "upsert".to_string(),
			Command::Update(_) => "update".to_string(),
			Command::Delete(_) => "delete".to_string(),
			Command::Query(_) => "query".to_string(),
			Command::Relate(_) => "relate".to_string(),
			Command::Run(_) => "run".to_string(),
			Command::GraphQl(_) => "graphql".to_string(),
		}
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
