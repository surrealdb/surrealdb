use anyhow::Context;
use anyhow::anyhow;
use std::str::FromStr;
use std::{collections::BTreeMap, fmt::Display};
use uuid::Uuid;

use crate::dbs::Variables;
use crate::expr::Data;
use crate::expr::Duration;
use crate::expr::Fetch;
use crate::expr::Fetchs;
use crate::expr::Fields;
use crate::expr::Value;
use crate::iam::AccessMethod;
use crate::iam::SigninParams;
use crate::iam::SignupParams;
use surrealdb_protocol::proto::rpc::v1 as rpc_proto;
use crate::rpc::request::{
	AuthenticateParams, CreateParams, DeleteParams, GraphQlParams, HealthParams, InfoParams,
	InsertParams, InvalidateParams, KillParams, LiveParams, PingParams, QueryParams, RelateParams,
	ResetParams, RunParams, SelectParams, SetParams, UnsetParams, UpdateParams, UpsertParams,
	UseParams, VersionParams,
};
use crate::{
	expr::access,
	protocol::{FromFlatbuffers, ToFlatbuffers},
};

// impl ToFlatbuffers for SignupParams {
// 	type Output<'a> = flatbuffers::WIPOffset<rpc_fb::SignupParams<'a>>;

// 	#[inline]
// 	fn to_fb<'a>(&self, fbb: &mut flatbuffers::FlatBufferBuilder<'a>) -> Self::Output<'a> {
// 		let namespace = fbb.create_string(&self.namespace);
// 		let database = fbb.create_string(&self.database);
// 		let access_name = fbb.create_string(&self.access_name);
// 		let variables = self.variables.to_fb(fbb);

// 		rpc_fb::SignupParams::create(
// 			fbb,
// 			&rpc_fb::SignupParamsArgs {
// 				namespace: Some(namespace),
// 				database: Some(database),
// 				access_name: Some(access_name),
// 				variables: Some(variables),
// 			},
// 		)
// 	}
// }

// impl FromFlatbuffers for SignupParams {
// 	type Input<'a> = rpc_fb::SignupParams<'a>;

// 	#[inline]
// 	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self>
// 	where
// 		Self: Sized,
// 	{
// 		let namespace = input.namespace().context("Missing namespace")?.to_string();
// 		let database = input.database().context("Missing database")?.to_string();
// 		let access_name = input.access_name().context("Missing access name")?.to_string();
// 		let variables = input.variables().context("Failed to get variables")?;
// 		let variables = Variables::from_fb(variables)?;

// 		Ok(SignupParams {
// 			namespace,
// 			database,
// 			access_name,
// 			variables,
// 		})
// 	}
// }

// impl ToFlatbuffers for SigninParams {
// 	type Output<'a> = flatbuffers::WIPOffset<rpc_fb::SigninParams<'a>>;

// 	#[inline]
// 	fn to_fb<'a>(&self, fbb: &mut flatbuffers::FlatBufferBuilder<'a>) -> Self::Output<'a> {
// 		let (access_method_type, access_method) = match &self.access_method {
// 			AccessMethod::RootUser {
// 				username,
// 				password,
// 			} => {
// 				let username = fbb.create_string(username.as_str());
// 				let password = fbb.create_string(password.as_str());
// 				(
// 					rpc_fb::AccessMethod::Root,
// 					rpc_fb::RootUserCredentials::create(
// 						fbb,
// 						&rpc_fb::RootUserCredentialsArgs {
// 							username: Some(username),
// 							password: Some(password),
// 						},
// 					)
// 					.as_union_value(),
// 				)
// 			}
// 			AccessMethod::NamespaceAccess {
// 				namespace,
// 				access_name,
// 				key,
// 			} => {
// 				let namespace = fbb.create_string(&namespace);
// 				let access_name = fbb.create_string(&access_name);
// 				let key = fbb.create_string(&key);
// 				(
// 					rpc_fb::AccessMethod::Namespace,
// 					rpc_fb::NamespaceAccessCredentials::create(
// 						fbb,
// 						&rpc_fb::NamespaceAccessCredentialsArgs {
// 							namespace: Some(namespace),
// 							access: Some(access_name),
// 							key: Some(key),
// 						},
// 					)
// 					.as_union_value(),
// 				)
// 			}
// 			AccessMethod::DatabaseAccess {
// 				namespace,
// 				database,
// 				access_name,
// 				key,
// 				refresh_token,
// 			} => {
// 				let namespace = fbb.create_string(&namespace);
// 				let database = fbb.create_string(&database);
// 				let access_name = fbb.create_string(&access_name);
// 				let key = fbb.create_string(&key);
// 				let refresh_token = refresh_token.as_ref().map(|s| fbb.create_string(s));
// 				(
// 					rpc_fb::AccessMethod::Database,
// 					rpc_fb::DatabaseAccessCredentials::create(
// 						fbb,
// 						&rpc_fb::DatabaseAccessCredentialsArgs {
// 							namespace: Some(namespace),
// 							database: Some(database),
// 							access: Some(access_name),
// 							key: Some(key),
// 							refresh: refresh_token,
// 						},
// 					)
// 					.as_union_value(),
// 				)
// 			}
// 			AccessMethod::NamespaceUser {
// 				namespace,
// 				username,
// 				password,
// 			} => {
// 				let namespace = fbb.create_string(&namespace);
// 				let username = fbb.create_string(&username);
// 				let password = fbb.create_string(&password);
// 				(
// 					rpc_fb::AccessMethod::NamespaceUser,
// 					rpc_fb::NamespaceUserCredentials::create(
// 						fbb,
// 						&rpc_fb::NamespaceUserCredentialsArgs {
// 							namespace: Some(namespace),
// 							username: Some(username),
// 							password: Some(password),
// 						},
// 					)
// 					.as_union_value(),
// 				)
// 			}
// 			AccessMethod::DatabaseUser {
// 				namespace,
// 				database,
// 				username,
// 				password,
// 			} => {
// 				let namespace = fbb.create_string(&namespace);
// 				let database = fbb.create_string(&database);
// 				let username = fbb.create_string(&username);
// 				let password = fbb.create_string(&password);
// 				(
// 					rpc_fb::AccessMethod::DatabaseUser,
// 					rpc_fb::DatabaseUserCredentials::create(
// 						fbb,
// 						&rpc_fb::DatabaseUserCredentialsArgs {
// 							namespace: Some(namespace),
// 							database: Some(database),
// 							username: Some(username),
// 							password: Some(password),
// 						},
// 					)
// 					.as_union_value(),
// 				)
// 			}
// 			unexpected => {
// 				panic!("Unexpected access method: {:?}", unexpected);
// 			}
// 		};

// 		rpc_fb::SigninParams::create(
// 			fbb,
// 			&rpc_fb::SigninParamsArgs {
// 				access_method_type,
// 				access_method: Some(access_method),
// 			},
// 		)
// 	}
// }

// impl FromFlatbuffers for SigninParams {
// 	type Input<'a> = rpc_fb::SigninParams<'a>;

// 	#[inline]
// 	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self>
// 	where
// 		Self: Sized,
// 	{
// 		let access_method = match input.access_method_type() {
// 			rpc_fb::AccessMethod::Root => {
// 				let root_user =
// 					input.access_method_as_root().context("Missing root user credentials")?;
// 				let username = root_user.username().context("Missing username")?.to_string();
// 				let password = root_user.password().context("Missing password")?.to_string();
// 				AccessMethod::RootUser {
// 					username,
// 					password,
// 				}
// 			}
// 			rpc_fb::AccessMethod::Namespace => {
// 				let namespace_access = input
// 					.access_method_as_namespace()
// 					.context("Missing namespace access credentials")?;
// 				let namespace =
// 					namespace_access.namespace().context("Missing namespace")?.to_string();
// 				let access_name =
// 					namespace_access.access().context("Missing access name")?.to_string();
// 				let key = namespace_access.key().context("Missing key")?.to_string();
// 				AccessMethod::NamespaceAccess {
// 					namespace,
// 					access_name,
// 					key,
// 				}
// 			}
// 			rpc_fb::AccessMethod::Database => {
// 				let database_access = input
// 					.access_method_as_database()
// 					.context("Missing database access credentials")?;
// 				let namespace =
// 					database_access.namespace().context("Missing namespace")?.to_string();
// 				let database = database_access.database().context("Missing database")?.to_string();
// 				let access_name =
// 					database_access.access().context("Missing access name")?.to_string();
// 				let key = database_access.key().context("Missing key")?.to_string();
// 				let refresh_token = database_access.refresh().map(|s| s.to_string());
// 				AccessMethod::DatabaseAccess {
// 					namespace,
// 					database,
// 					access_name,
// 					key,
// 					refresh_token,
// 				}
// 			}
// 			rpc_fb::AccessMethod::NamespaceUser => {
// 				let namespace_user = input
// 					.access_method_as_namespace_user()
// 					.context("Missing namespace user credentials")?;
// 				let namespace =
// 					namespace_user.namespace().context("Missing namespace")?.to_string();
// 				let username = namespace_user.username().context("Missing username")?.to_string();
// 				let password = namespace_user.password().context("Missing password")?.to_string();
// 				AccessMethod::NamespaceUser {
// 					namespace,
// 					username,
// 					password,
// 				}
// 			}
// 			rpc_fb::AccessMethod::DatabaseUser => {
// 				let database_user = input
// 					.access_method_as_database_user()
// 					.context("Missing database user credentials")?;
// 				let namespace = database_user.namespace().context("Missing namespace")?.to_string();
// 				let database = database_user.database().context("Missing database")?.to_string();
// 				let username = database_user.username().context("Missing username")?.to_string();
// 				let password = database_user.password().context("Missing password")?.to_string();
// 				AccessMethod::DatabaseUser {
// 					namespace,
// 					database,
// 					username,
// 					password,
// 				}
// 			}
// 			rpc_fb::AccessMethod::AccessToken => {
// 				let access_token = input
// 					.access_method_as_access_token()
// 					.context("Missing access token credentials")?;
// 				let token = access_token.token().context("Missing token")?.to_string();
// 				AccessMethod::AccessToken {
// 					token,
// 				}
// 			}
// 			unexpected => {
// 				return Err(anyhow!("Unexpected access method: {:?}", unexpected));
// 			}
// 		};

// 		Ok(SigninParams {
// 			access_method,
// 		})
// 	}
// }

// impl ToFlatbuffers for UseParams {
// 	type Output<'a> = flatbuffers::WIPOffset<rpc_fb::UseParams<'a>>;

// 	#[inline]
// 	fn to_fb<'a>(&self, fbb: &mut flatbuffers::FlatBufferBuilder<'a>) -> Self::Output<'a> {
// 		let namespace = self.namespace.as_ref().map(|s| fbb.create_string(s));
// 		let database = self.database.as_ref().map(|s| fbb.create_string(s));

// 		rpc_fb::UseParams::create(
// 			fbb,
// 			&rpc_fb::UseParamsArgs {
// 				namespace,
// 				database,
// 			},
// 		)
// 	}
// }

// impl FromFlatbuffers for UseParams {
// 	type Input<'a> = rpc_fb::UseParams<'a>;

// 	#[inline]
// 	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self>
// 	where
// 		Self: Sized,
// 	{
// 		let namespace = input.namespace().map(|s| s.to_string());
// 		let database = input.database().map(|s| s.to_string());

// 		Ok(UseParams {
// 			namespace,
// 			database,
// 		})
// 	}
// }

// impl ToFlatbuffers for AuthenticateParams {
// 	type Output<'a> = flatbuffers::WIPOffset<rpc_fb::AuthenticateParams<'a>>;

// 	#[inline]
// 	fn to_fb<'a>(&self, fbb: &mut flatbuffers::FlatBufferBuilder<'a>) -> Self::Output<'a> {
// 		let token = fbb.create_string(&self.token);

// 		rpc_fb::AuthenticateParams::create(
// 			fbb,
// 			&rpc_fb::AuthenticateParamsArgs {
// 				token: Some(token),
// 			},
// 		)
// 	}
// }

// impl FromFlatbuffers for AuthenticateParams {
// 	type Input<'a> = rpc_fb::AuthenticateParams<'a>;

// 	#[inline]
// 	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self>
// 	where
// 		Self: Sized,
// 	{
// 		let token = input.token().context("Missing token")?.to_string();

// 		Ok(AuthenticateParams {
// 			token,
// 		})
// 	}
// }

// impl ToFlatbuffers for CreateParams {
// 	type Output<'a> = flatbuffers::WIPOffset<rpc_fb::CreateParams<'a>>;

// 	#[inline]
// 	fn to_fb<'a>(&self, fbb: &mut flatbuffers::FlatBufferBuilder<'a>) -> Self::Output<'a> {
// 		let only = self.only;
// 		let what = self.what.to_fb(fbb);
// 		let data = self.data.as_ref().map(|v| v.to_fb(fbb));
// 		let output = self.output.as_ref().map(|v| v.to_fb(fbb));
// 		let timeout = self.timeout.map(|t| t.to_fb(fbb));
// 		let parallel = self.parallel;
// 		let version = self.version.as_ref().map(|v| v.to_fb(fbb));
// 		rpc_fb::CreateParams::create(
// 			fbb,
// 			&rpc_fb::CreateParamsArgs {
// 				only,
// 				what: Some(what),
// 				data,
// 				output,
// 				timeout,
// 				parallel,
// 				version,
// 			},
// 		)
// 	}
// }

// impl FromFlatbuffers for CreateParams {
// 	type Input<'a> = rpc_fb::CreateParams<'a>;

// 	#[inline]
// 	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self>
// 	where
// 		Self: Sized,
// 	{
// 		let only = input.only();
// 		let what = Value::from_fb(input.what().context("Missing what")?)?;
// 		let data = input.data().map(|v| Value::from_fb(v)).transpose()?;
// 		let output = input.output().map(|v| Value::from_fb(v)).transpose()?;
// 		let timeout = input.timeout().map(|d| Duration::from_fb(d)).transpose()?;
// 		let parallel = input.parallel();
// 		let version = input.version().map(|v| Value::from_fb(v)).transpose()?;

// 		Ok(CreateParams {
// 			only,
// 			what,
// 			data,
// 			output,
// 			timeout,
// 			parallel,
// 			version,
// 		})
// 	}
// }

// impl ToFlatbuffers for LiveParams {
// 	type Output<'a> = flatbuffers::WIPOffset<rpc_fb::LiveParams<'a>>;

// 	#[inline]
// 	fn to_fb<'a>(&self, fbb: &mut flatbuffers::FlatBufferBuilder<'a>) -> Self::Output<'a> {
// 		let what = self.what.to_fb(fbb);
// 		let expr = self.expr.to_fb(fbb);
// 		let cond = self.cond.as_ref().map(|v| v.to_fb(fbb));
// 		let fetch = self.fetch.as_ref().map(|v| v.to_fb(fbb));

// 		rpc_fb::LiveParams::create(
// 			fbb,
// 			&rpc_fb::LiveParamsArgs {
// 				what: Some(what),
// 				expr: Some(expr),
// 				cond,
// 				fetch,
// 			},
// 		)
// 	}
// }

// impl FromFlatbuffers for LiveParams {
// 	type Input<'a> = rpc_fb::LiveParams<'a>;

// 	#[inline]
// 	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self>
// 	where
// 		Self: Sized,
// 	{
// 		let what = Value::from_fb(input.what().context("Missing what")?)?;
// 		let expr = Fields::from_fb(input.expr().context("Missing expr")?)?;
// 		let cond = input.cond().map(|v| Value::from_fb(v)).transpose()?;
// 		let fetch = input.fetch().map(|v| Value::from_fb(v)).transpose()?;

// 		Ok(LiveParams {
// 			what,
// 			expr,
// 			cond,
// 			fetch,
// 		})
// 	}
// }

// impl ToFlatbuffers for KillParams {
// 	type Output<'a> = flatbuffers::WIPOffset<rpc_fb::KillParams<'a>>;

// 	#[inline]
// 	fn to_fb<'a>(&self, fbb: &mut flatbuffers::FlatBufferBuilder<'a>) -> Self::Output<'a> {
// 		let live_uuid = fbb.create_string(&self.live_uuid.to_string());

// 		rpc_fb::KillParams::create(
// 			fbb,
// 			&rpc_fb::KillParamsArgs {
// 				live_uuid: Some(live_uuid),
// 			},
// 		)
// 	}
// }

// impl FromFlatbuffers for KillParams {
// 	type Input<'a> = rpc_fb::KillParams<'a>;

// 	#[inline]
// 	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self>
// 	where
// 		Self: Sized,
// 	{
// 		let live_uuid = Uuid::from_str(input.live_uuid().context("Missing live UUID")?)?;

// 		Ok(KillParams {
// 			live_uuid,
// 		})
// 	}
// }

// impl ToFlatbuffers for SetParams {
// 	type Output<'a> = flatbuffers::WIPOffset<rpc_fb::SetParams<'a>>;

// 	#[inline]
// 	fn to_fb<'a>(&self, fbb: &mut flatbuffers::FlatBufferBuilder<'a>) -> Self::Output<'a> {
// 		let key = fbb.create_string(&self.key);
// 		let value = self.value.to_fb(fbb);

// 		rpc_fb::SetParams::create(
// 			fbb,
// 			&rpc_fb::SetParamsArgs {
// 				key: Some(key),
// 				value: Some(value),
// 			},
// 		)
// 	}
// }

// impl FromFlatbuffers for SetParams {
// 	type Input<'a> = rpc_fb::SetParams<'a>;

// 	#[inline]
// 	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self>
// 	where
// 		Self: Sized,
// 	{
// 		let key = input.key().context("Missing key")?.to_string();
// 		let value = Value::from_fb(input.value().context("Missing value")?)?;

// 		Ok(SetParams {
// 			key,
// 			value,
// 		})
// 	}
// }

// impl ToFlatbuffers for UnsetParams {
// 	type Output<'a> = flatbuffers::WIPOffset<rpc_fb::UnsetParams<'a>>;

// 	#[inline]
// 	fn to_fb<'a>(&self, fbb: &mut flatbuffers::FlatBufferBuilder<'a>) -> Self::Output<'a> {
// 		let key = fbb.create_string(&self.key);

// 		rpc_fb::UnsetParams::create(
// 			fbb,
// 			&rpc_fb::UnsetParamsArgs {
// 				key: Some(key),
// 			},
// 		)
// 	}
// }

// impl FromFlatbuffers for UnsetParams {
// 	type Input<'a> = rpc_fb::UnsetParams<'a>;

// 	#[inline]
// 	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self>
// 	where
// 		Self: Sized,
// 	{
// 		let key = input.key().context("Missing key")?.to_string();

// 		Ok(UnsetParams {
// 			key,
// 		})
// 	}
// }

// impl ToFlatbuffers for SelectParams {
// 	type Output<'a> = flatbuffers::WIPOffset<rpc_fb::SelectParams<'a>>;

// 	#[inline]
// 	fn to_fb<'a>(&self, fbb: &mut flatbuffers::FlatBufferBuilder<'a>) -> Self::Output<'a> {
// 		let expr = self.expr.to_fb(fbb);
// 		let omit = self.omit.as_ref().map(|v| v.to_fb(fbb));
// 		let only = self.only;
// 		let what = self.what.to_fb(fbb);
// 		let with = self.with.as_ref().map(|v| v.to_fb(fbb));
// 		let cond = self.cond.as_ref().map(|v| v.to_fb(fbb));
// 		let split = self.split.as_ref().map(|v| v.to_fb(fbb));
// 		let group = self.group.as_ref().map(|v| v.to_fb(fbb));
// 		let order = self.order.as_ref().map(|v| v.to_fb(fbb));
// 		let start = self.start;
// 		let limit = self.limit;
// 		let fetch = self.fetch.as_ref().map(|v| v.to_fb(fbb));
// 		let version = self.version.as_ref().map(|v| v.to_fb(fbb));
// 		let timeout = self.timeout.map(|d| d.to_fb(fbb));
// 		let parallel = self.parallel;
// 		let explain = self.explain.as_ref().map(|v| v.to_fb(fbb));
// 		let tempfiles = self.tempfiles;
// 		let variables = self.variables.to_fb(fbb);

// 		rpc_fb::SelectParams::create(
// 			fbb,
// 			&rpc_fb::SelectParamsArgs {
// 				expr: Some(expr),
// 				omit,
// 				only,
// 				what: Some(what),
// 				with,
// 				cond,
// 				split,
// 				group,
// 				order,
// 				start,
// 				limit,
// 				fetch,
// 				version,
// 				timeout,
// 				parallel,
// 				explain,
// 				tempfiles,
// 				variables: Some(variables),
// 			},
// 		)
// 	}
// }

// impl FromFlatbuffers for SelectParams {
// 	type Input<'a> = rpc_fb::SelectParams<'a>;

// 	#[inline]
// 	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self>
// 	where
// 		Self: Sized,
// 	{
// 		let expr = Fields::from_fb(input.expr().context("Missing expr")?)?;
// 		let omit = input.omit().map(|v| Value::from_fb(v)).transpose()?;
// 		let only = input.only();
// 		let what = Value::from_fb(input.what().context("Missing what")?)?;
// 		let with = input.with().map(|v| Value::from_fb(v)).transpose()?;
// 		let cond = input.cond().map(|v| Value::from_fb(v)).transpose()?;
// 		let split = input.split().map(|v| Value::from_fb(v)).transpose()?;
// 		let group = input.group().map(|v| Value::from_fb(v)).transpose()?;
// 		let order = input.order().map(|v| Value::from_fb(v)).transpose()?;
// 		let start = input.start();
// 		let limit = input.limit();
// 		let fetch = input.fetch().map(|v| Fetchs::from_fb(v)).transpose()?;
// 		let version = input.version().map(|v| Value::from_fb(v)).transpose()?;
// 		let timeout = input.timeout().map(|d| Duration::from_fb(d)).transpose()?;
// 		let parallel = input.parallel();
// 		let explain = input.explain().map(|v| Value::from_fb(v)).transpose()?;
// 		let tempfiles = input.tempfiles();
// 		let variables = Variables::from_fb(input.variables().context("Missing variables")?)?;

// 		Ok(SelectParams {
// 			expr,
// 			omit,
// 			only,
// 			what,
// 			with,
// 			cond,
// 			split,
// 			group,
// 			order,
// 			start,
// 			limit,
// 			fetch,
// 			version,
// 			timeout,
// 			parallel,
// 			explain,
// 			tempfiles,
// 			variables,
// 		})
// 	}
// }

// impl ToFlatbuffers for InsertParams {
// 	type Output<'a> = flatbuffers::WIPOffset<rpc_fb::InsertParams<'a>>;

// 	#[inline]
// 	fn to_fb<'a>(&self, fbb: &mut flatbuffers::FlatBufferBuilder<'a>) -> Self::Output<'a> {
// 		let into = self.into.to_fb(fbb);
// 		let data = self.data.to_fb(fbb);
// 		let ignore = self.ignore;
// 		let update = self.update.as_ref().map(|v| v.to_fb(fbb));
// 		let output = self.output.as_ref().map(|v| v.to_fb(fbb));
// 		let timeout = self.timeout.map(|d| d.to_fb(fbb));
// 		let parallel = self.parallel;
// 		let relation = self.relation;
// 		let version = self.version.as_ref().map(|v| v.to_fb(fbb));

// 		rpc_fb::InsertParams::create(
// 			fbb,
// 			&rpc_fb::InsertParamsArgs {
// 				into: Some(into),
// 				data: Some(data),
// 				ignore,
// 				update,
// 				output,
// 				timeout,
// 				parallel,
// 				relation,
// 				version,
// 			},
// 		)
// 	}
// }

// impl FromFlatbuffers for InsertParams {
// 	type Input<'a> = rpc_fb::InsertParams<'a>;

// 	#[inline]
// 	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self>
// 	where
// 		Self: Sized,
// 	{
// 		let into = Value::from_fb(rpc_fb::InsertParams::into(&input).context("Missing into")?)?;
// 		let data = Value::from_fb(input.data().context("Missing data")?)?;
// 		let ignore = input.ignore();
// 		let update = input.update().map(|v| Value::from_fb(v)).transpose()?;
// 		let output = input.output().map(|v| Value::from_fb(v)).transpose()?;
// 		let timeout = input.timeout().map(|d| Duration::from_fb(d)).transpose()?;
// 		let parallel = input.parallel();
// 		let relation = input.relation();
// 		let version = input.version().map(|v| Value::from_fb(v)).transpose()?;

// 		Ok(InsertParams {
// 			into,
// 			data,
// 			ignore,
// 			update,
// 			output,
// 			timeout,
// 			parallel,
// 			relation,
// 			version,
// 		})
// 	}
// }

// impl ToFlatbuffers for UpdateParams {
// 	type Output<'a> = flatbuffers::WIPOffset<rpc_fb::UpdateParams<'a>>;

// 	#[inline]
// 	fn to_fb<'a>(&self, fbb: &mut flatbuffers::FlatBufferBuilder<'a>) -> Self::Output<'a> {
// 		let only = self.only;
// 		let what = self.what.to_fb(fbb);
// 		let with = self.with.as_ref().map(|v| v.to_fb(fbb));
// 		let data = self.data.as_ref().map(|v| v.to_fb(fbb));
// 		let cond = self.cond.as_ref().map(|v| v.to_fb(fbb));
// 		let output = self.output.as_ref().map(|v| v.to_fb(fbb));
// 		let timeout = self.timeout.map(|d| d.to_fb(fbb));
// 		let parallel = self.parallel;
// 		let explain = self.explain.as_ref().map(|v| v.to_fb(fbb));

// 		rpc_fb::UpdateParams::create(
// 			fbb,
// 			&rpc_fb::UpdateParamsArgs {
// 				only,
// 				what: Some(what),
// 				with,
// 				data,
// 				cond,
// 				output,
// 				timeout,
// 				parallel,
// 				explain,
// 			},
// 		)
// 	}
// }

// impl FromFlatbuffers for UpdateParams {
// 	type Input<'a> = rpc_fb::UpdateParams<'a>;

// 	#[inline]
// 	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self>
// 	where
// 		Self: Sized,
// 	{
// 		let only = input.only();
// 		let what = Value::from_fb(input.what().context("Missing what")?)?;
// 		let with = input.with().map(|v| Value::from_fb(v)).transpose()?;
// 		let data = input.data().map(|v| Data::from_fb(v)).transpose()?;
// 		let cond = input.cond().map(|v| Value::from_fb(v)).transpose()?;
// 		let output = input.output().map(|v| Value::from_fb(v)).transpose()?;
// 		let timeout = input.timeout().map(|d| Duration::from_fb(d)).transpose()?;
// 		let parallel = input.parallel();
// 		let explain = input.explain().map(|v| Value::from_fb(v)).transpose()?;

// 		Ok(UpdateParams {
// 			only,
// 			what,
// 			with,
// 			data,
// 			cond,
// 			output,
// 			timeout,
// 			parallel,
// 			explain,
// 		})
// 	}
// }

// impl ToFlatbuffers for UpsertParams {
// 	type Output<'a> = flatbuffers::WIPOffset<rpc_fb::UpsertParams<'a>>;

// 	#[inline]
// 	fn to_fb<'a>(&self, fbb: &mut flatbuffers::FlatBufferBuilder<'a>) -> Self::Output<'a> {
// 		let only = self.only;
// 		let what = self.what.to_fb(fbb);
// 		let with = self.with.as_ref().map(|v| v.to_fb(fbb));
// 		let data = self.data.as_ref().map(|v| v.to_fb(fbb));
// 		let cond = self.cond.as_ref().map(|v| v.to_fb(fbb));
// 		let output = self.output.as_ref().map(|v| v.to_fb(fbb));
// 		let timeout = self.timeout.map(|d| d.to_fb(fbb));
// 		let parallel = self.parallel;
// 		let explain = self.explain.as_ref().map(|v| v.to_fb(fbb));

// 		rpc_fb::UpsertParams::create(
// 			fbb,
// 			&rpc_fb::UpsertParamsArgs {
// 				only,
// 				what: Some(what),
// 				with,
// 				data,
// 				cond,
// 				output,
// 				timeout,
// 				parallel,
// 				explain,
// 			},
// 		)
// 	}
// }

// impl FromFlatbuffers for UpsertParams {
// 	type Input<'a> = rpc_fb::UpsertParams<'a>;

// 	#[inline]
// 	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self>
// 	where
// 		Self: Sized,
// 	{
// 		let only = input.only();
// 		let what = Value::from_fb(input.what().context("Missing what")?)?;
// 		let with = input.with().map(|v| Value::from_fb(v)).transpose()?;
// 		let data = input.data().map(|v| Data::from_fb(v)).transpose()?;
// 		let cond = input.cond().map(|v| Value::from_fb(v)).transpose()?;
// 		let output = input.output().map(|v| Value::from_fb(v)).transpose()?;
// 		let timeout = input.timeout().map(|d| Duration::from_fb(d)).transpose()?;
// 		let parallel = input.parallel();
// 		let explain = input.explain().map(|v| Value::from_fb(v)).transpose()?;

// 		Ok(UpsertParams {
// 			only,
// 			what,
// 			with,
// 			data,
// 			cond,
// 			output,
// 			timeout,
// 			parallel,
// 			explain,
// 		})
// 	}
// }

// impl ToFlatbuffers for DeleteParams {
// 	type Output<'a> = flatbuffers::WIPOffset<rpc_fb::DeleteParams<'a>>;

// 	#[inline]
// 	fn to_fb<'a>(&self, fbb: &mut flatbuffers::FlatBufferBuilder<'a>) -> Self::Output<'a> {
// 		let only = self.only;
// 		let what = self.what.to_fb(fbb);
// 		let with = self.with.as_ref().map(|v| v.to_fb(fbb));
// 		let cond = self.cond.as_ref().map(|v| v.to_fb(fbb));
// 		let output = self.output.as_ref().map(|v| v.to_fb(fbb));
// 		let timeout = self.timeout.map(|d| d.to_fb(fbb));
// 		let parallel = self.parallel;
// 		let explain = self.explain.as_ref().map(|v| v.to_fb(fbb));

// 		rpc_fb::DeleteParams::create(
// 			fbb,
// 			&rpc_fb::DeleteParamsArgs {
// 				only,
// 				what: Some(what),
// 				with,
// 				cond,
// 				output,
// 				timeout,
// 				parallel,
// 				explain,
// 			},
// 		)
// 	}
// }

// impl FromFlatbuffers for DeleteParams {
// 	type Input<'a> = rpc_fb::DeleteParams<'a>;

// 	#[inline]
// 	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self>
// 	where
// 		Self: Sized,
// 	{
// 		let only = input.only();
// 		let what = Value::from_fb(input.what().context("Missing what")?)?;
// 		let with = input.with().map(|v| Value::from_fb(v)).transpose()?;
// 		let cond = input.cond().map(|v| Value::from_fb(v)).transpose()?;
// 		let output = input.output().map(|v| Value::from_fb(v)).transpose()?;
// 		let timeout = input.timeout().map(|d| Duration::from_fb(d)).transpose()?;
// 		let parallel = input.parallel();
// 		let explain = input.explain().map(|v| Value::from_fb(v)).transpose()?;

// 		Ok(DeleteParams {
// 			only,
// 			what,
// 			with,
// 			cond,
// 			output,
// 			timeout,
// 			parallel,
// 			explain,
// 		})
// 	}
// }

// impl ToFlatbuffers for QueryParams {
// 	type Output<'a> = flatbuffers::WIPOffset<rpc_fb::QueryParams<'a>>;

// 	#[inline]
// 	fn to_fb<'a>(&self, fbb: &mut flatbuffers::FlatBufferBuilder<'a>) -> Self::Output<'a> {
// 		let query = fbb.create_string(&self.query);
// 		let variables = self.variables.to_fb(fbb);

// 		rpc_fb::QueryParams::create(
// 			fbb,
// 			&rpc_fb::QueryParamsArgs {
// 				query: Some(query),
// 				variables: Some(variables),
// 			},
// 		)
// 	}
// }

// impl FromFlatbuffers for QueryParams {
// 	type Input<'a> = rpc_fb::QueryParams<'a>;

// 	#[inline]
// 	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self>
// 	where
// 		Self: Sized,
// 	{
// 		let query = input.query().context("Missing query")?.to_string();
// 		let variables = Variables::from_fb(input.variables().context("Missing variables")?)?;

// 		Ok(QueryParams {
// 			query,
// 			variables,
// 		})
// 	}
// }

// impl ToFlatbuffers for RelateParams {
// 	type Output<'a> = flatbuffers::WIPOffset<rpc_fb::RelateParams<'a>>;

// 	#[inline]
// 	fn to_fb<'a>(&self, fbb: &mut flatbuffers::FlatBufferBuilder<'a>) -> Self::Output<'a> {
// 		let only = self.only;
// 		let kind = self.kind.to_fb(fbb);
// 		let from = self.from.to_fb(fbb);
// 		let with = self.with.as_ref().map(|v| v.to_fb(fbb));
// 		let uniq = self.uniq;
// 		let data = self.data.as_ref().map(|v| v.to_fb(fbb));
// 		let output = self.output.as_ref().map(|v| v.to_fb(fbb));
// 		let timeout = self.timeout.map(|d| d.to_fb(fbb));
// 		let parallel = self.parallel;

// 		rpc_fb::RelateParams::create(
// 			fbb,
// 			&rpc_fb::RelateParamsArgs {
// 				only,
// 				kind: Some(kind),
// 				from: Some(from),
// 				with,
// 				uniq,
// 				data,
// 				output,
// 				timeout,
// 				parallel,
// 			},
// 		)
// 	}
// }

// impl FromFlatbuffers for RelateParams {
// 	type Input<'a> = rpc_fb::RelateParams<'a>;

// 	#[inline]
// 	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self>
// 	where
// 		Self: Sized,
// 	{
// 		let only = input.only();
// 		let kind = Value::from_fb(input.kind().context("Missing kind")?)?;
// 		let from = Value::from_fb(input.from().context("Missing from")?)?;
// 		let with = input.with().map(|v| Value::from_fb(v)).transpose()?;
// 		let uniq = input.uniq();
// 		let data = input.data().map(|v| Value::from_fb(v)).transpose()?;
// 		let output = input.output().map(|v| Value::from_fb(v)).transpose()?;
// 		let timeout = input.timeout().map(|d| Duration::from_fb(d)).transpose()?;
// 		let parallel = input.parallel();

// 		Ok(RelateParams {
// 			only,
// 			kind,
// 			from,
// 			with,
// 			uniq,
// 			data,
// 			output,
// 			timeout,
// 			parallel,
// 		})
// 	}
// }

// impl ToFlatbuffers for RunParams {
// 	type Output<'a> = flatbuffers::WIPOffset<rpc_fb::RunParams<'a>>;

// 	#[inline]
// 	fn to_fb<'a>(&self, fbb: &mut flatbuffers::FlatBufferBuilder<'a>) -> Self::Output<'a> {
// 		let name = fbb.create_string(&self.name);
// 		let version = self.version.as_ref().map(|s| fbb.create_string(s));
// 		let args = self.args.iter().map(|v| v.to_fb(fbb)).collect::<Vec<_>>();
// 		let args = fbb.create_vector(&args);

// 		rpc_fb::RunParams::create(
// 			fbb,
// 			&rpc_fb::RunParamsArgs {
// 				name: Some(name),
// 				version,
// 				args: Some(args),
// 			},
// 		)
// 	}
// }

// impl FromFlatbuffers for RunParams {
// 	type Input<'a> = rpc_fb::RunParams<'a>;

// 	#[inline]
// 	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self>
// 	where
// 		Self: Sized,
// 	{
// 		let name = input.name().context("Missing name")?.to_string();
// 		let version = input.version().map(|s| s.to_string());
// 		let args = match input.args() {
// 			Some(args) => {
// 				args.iter().map(|v| Value::from_fb(v)).collect::<anyhow::Result<Vec<_>>>()?
// 			}
// 			None => vec![],
// 		};

// 		Ok(RunParams {
// 			name,
// 			version,
// 			args,
// 		})
// 	}
// }

// impl ToFlatbuffers for GraphQlParams {
// 	type Output<'a> = flatbuffers::WIPOffset<rpc_fb::GraphQlParams<'a>>;

// 	#[inline]
// 	fn to_fb<'a>(&self, fbb: &mut flatbuffers::FlatBufferBuilder<'a>) -> Self::Output<'a> {
// 		let query = fbb.create_string(&self.query);
// 		let variables = self.variables.to_fb(fbb);

// 		rpc_fb::GraphQlParams::create(
// 			fbb,
// 			&rpc_fb::GraphQlParamsArgs {
// 				query: Some(query),
// 				variables: Some(variables),
// 			},
// 		)
// 	}
// }

// impl FromFlatbuffers for GraphQlParams {
// 	type Input<'a> = rpc_fb::GraphQlParams<'a>;

// 	#[inline]
// 	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self>
// 	where
// 		Self: Sized,
// 	{
// 		let query = input.query().context("Missing query")?.to_string();
// 		let variables = Variables::from_fb(input.variables().context("Missing variables")?)?;

// 		Ok(GraphQlParams {
// 			query,
// 			variables,
// 		})
// 	}
// }


impl TryFrom<AccessMethod> for rpc_proto::AccessMethod {
	type Error = anyhow::Error;

	fn try_from(value: AccessMethod) -> Result<Self, Self::Error> {
		todo!("STUB: TryFrom<AccessMethod> for rpc_proto::AccessMethod");
	}
}

impl TryFrom<rpc_proto::AccessMethod> for AccessMethod {
	type Error = anyhow::Error;

	fn try_from(value: rpc_proto::AccessMethod) -> Result<Self, Self::Error> {
		todo!("STUB: TryFrom<rpc_proto::AccessMethod> for AccessMethod");
	}
}
