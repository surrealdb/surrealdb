use crate::dbs::Variables;
use crate::expr::Duration;
use crate::expr::{Data, Fetchs, Fields, TryFromValue, Value};
use crate::iam::AccessMethod;
use crate::iam::{SigninParams, SignupParams};
use crate::protocol::flatbuffers::surreal_db::protocol::rpc;
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

use crate::protocol::flatbuffers::surreal_db::protocol::rpc as rpc_fb;

impl ToFlatbuffers for Request {
	type Output<'bldr> = flatbuffers::WIPOffset<rpc_fb::Request<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let id = self.id.as_ref().map(|s| builder.create_string(s));

		let (command_type, command) = match &self.command {
			Command::Health(_) => (
				rpc_fb::Command::Health,
				rpc_fb::HealthParams::create(builder, &rpc_fb::HealthParamsArgs {})
					.as_union_value(),
			),
			Command::Version(_) => (
				rpc_fb::Command::Version,
				rpc_fb::VersionParams::create(builder, &rpc_fb::VersionParamsArgs {})
					.as_union_value(),
			),
			Command::Ping(_) => (
				rpc_fb::Command::Ping,
				rpc_fb::PingParams::create(builder, &rpc_fb::PingParamsArgs {}).as_union_value(),
			),
			Command::Info(_) => (
				rpc_fb::Command::Info,
				rpc_fb::InfoParams::create(builder, &rpc_fb::InfoParamsArgs {}).as_union_value(),
			),
			Command::Use(params) => (rpc_fb::Command::Use, params.to_fb(builder).as_union_value()),
			Command::Signup(params) => {
				(rpc_fb::Command::Signup, params.to_fb(builder).as_union_value())
			}
			Command::Signin(params) => {
				(rpc_fb::Command::Signin, params.to_fb(builder).as_union_value())
			}
			Command::Authenticate(params) => {
				(rpc_fb::Command::Authenticate, params.to_fb(builder).as_union_value())
			}
			Command::Invalidate(params) => (
				rpc_fb::Command::Invalidate,
				rpc_fb::InvalidateParams::create(builder, &rpc_fb::InvalidateParamsArgs {})
					.as_union_value(),
			),
			Command::Create(params) => {
				(rpc_fb::Command::Create, params.to_fb(builder).as_union_value())
			}
			Command::Reset(params) => (
				rpc_fb::Command::Reset,
				rpc_fb::ResetParams::create(builder, &rpc_fb::ResetParamsArgs {}).as_union_value(),
			),
			Command::Kill(params) => {
				(rpc_fb::Command::Kill, params.to_fb(builder).as_union_value())
			}
			Command::Live(params) => {
				(rpc_fb::Command::Live, params.to_fb(builder).as_union_value())
			}
			Command::Set(params) => (rpc_fb::Command::Set, params.to_fb(builder).as_union_value()),
			Command::Unset(params) => {
				let key = builder.create_string(&params.key);
				(
					rpc_fb::Command::Unset,
					rpc_fb::UnsetParams::create(
						builder,
						&rpc_fb::UnsetParamsArgs {
							key: Some(key),
						},
					)
					.as_union_value(),
				)
			}
			Command::Select(params) => {
				(rpc_fb::Command::Select, params.to_fb(builder).as_union_value())
			}
			Command::Insert(params) => {
				(rpc_fb::Command::Insert, params.to_fb(builder).as_union_value())
			}
			Command::Upsert(params) => {
				(rpc_fb::Command::Upsert, params.to_fb(builder).as_union_value())
			}
			Command::Update(params) => {
				(rpc_fb::Command::Update, params.to_fb(builder).as_union_value())
			}
			Command::Delete(params) => {
				(rpc_fb::Command::Delete, params.to_fb(builder).as_union_value())
			}
			Command::Query(params) => {
				(rpc_fb::Command::Query, params.to_fb(builder).as_union_value())
			}
			Command::Relate(params) => {
				(rpc_fb::Command::Relate, params.to_fb(builder).as_union_value())
			}
			Command::Run(params) => (rpc_fb::Command::Run, params.to_fb(builder).as_union_value()),
			Command::GraphQl(params) => {
				(rpc_fb::Command::GraphQl, params.to_fb(builder).as_union_value())
			}
		};

		let request = rpc_fb::RequestArgs {
			id,
			command_type,
			command: Some(command),
		};
		rpc_fb::Request::create(builder, &request)
	}
}

impl FromFlatbuffers for Request {
	type Input<'a> = rpc_fb::Request<'a>;

	#[inline]
	fn from_fb(reader: Self::Input<'_>) -> anyhow::Result<Self> {
		let id = reader.id().map(|s| s.to_string());
		let command = match reader.command_type() {
			rpc_fb::Command::Health => Command::Health(HealthParams {}),
			rpc_fb::Command::Version => Command::Version(VersionParams {}),
			rpc_fb::Command::Ping => Command::Ping(PingParams {}),
			rpc_fb::Command::Info => Command::Info(InfoParams {}),
			rpc_fb::Command::Use => {
				let params = reader.command_as_use().expect("Command is Use");
				Command::Use(UseParams::from_fb(params)?)
			}
			rpc_fb::Command::Signup => {
				let params = reader.command_as_signup().expect("Command is Signup");
				Command::Signup(SignupParams::from_fb(params)?)
			}
			rpc_fb::Command::Signin => {
				let params = reader.command_as_signin().expect("Command is Signin");
				Command::Signin(SigninParams::from_fb(params)?)
			}
			rpc_fb::Command::Authenticate => {
				let params = reader.command_as_authenticate().expect("Command is Authenticate");
				Command::Authenticate(AuthenticateParams::from_fb(params)?)
			}
			rpc_fb::Command::Invalidate => Command::Invalidate(InvalidateParams {}),
			rpc_fb::Command::Create => {
				let params = reader.command_as_create().expect("Command is Create");
				Command::Create(CreateParams::from_fb(params)?)
			}
			rpc_fb::Command::Reset => Command::Reset(ResetParams {}),
			rpc_fb::Command::Kill => {
				let params = reader.command_as_kill().expect("Command is Kill");
				Command::Kill(KillParams::from_fb(params)?)
			}
			rpc_fb::Command::Live => {
				let params = reader.command_as_live().expect("Command is Live");
				Command::Live(LiveParams::from_fb(params)?)
			}
			rpc_fb::Command::Set => {
				let params = reader.command_as_set().expect("Command is Set");
				Command::Set(SetParams::from_fb(params)?)
			}
			rpc_fb::Command::Unset => {
				let params = reader.command_as_unset().expect("Command is Unset");
				Command::Unset(UnsetParams::from_fb(params)?)
			}
			rpc_fb::Command::Select => {
				let params = reader.command_as_select().expect("Command is Select");
				Command::Select(SelectParams::from_fb(params)?)
			}
			rpc_fb::Command::Insert => {
				let params = reader.command_as_insert().expect("Command is Insert");
				Command::Insert(InsertParams::from_fb(params)?)
			}
			rpc_fb::Command::Upsert => {
				let params = reader.command_as_upsert().expect("Command is Upsert");
				Command::Upsert(UpsertParams::from_fb(params)?)
			}
			rpc_fb::Command::Update => {
				let params = reader.command_as_update().expect("Command is Update");
				Command::Update(UpdateParams::from_fb(params)?)
			}
			rpc_fb::Command::Delete => {
				let params = reader.command_as_delete().expect("Command is Delete");
				Command::Delete(DeleteParams::from_fb(params)?)
			}
			rpc_fb::Command::Query => {
				let params = reader.command_as_query().expect("Command is Query");
				Command::Query(QueryParams::from_fb(params)?)
			}
			rpc_fb::Command::Relate => {
				let params = reader.command_as_relate().expect("Command is Relate");
				Command::Relate(RelateParams::from_fb(params)?)
			}
			rpc_fb::Command::Run => {
				let params = reader.command_as_run().expect("Command is Run");
				Command::Run(RunParams::from_fb(params)?)
			}
			rpc_fb::Command::GraphQl => {
				let params = reader.command_as_graph_ql().expect("Command is GraphQL");
				Command::GraphQl(GraphQlParams::from_fb(params)?)
			}
			unexpected => {
				return Err(anyhow::anyhow!("Unexpected command: {:?}", unexpected));
			}
		};

		Ok(Request {
			id,
			command,
		})
	}
}
