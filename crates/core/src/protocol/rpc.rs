use anyhow::Context;
use anyhow::anyhow;
use std::{collections::BTreeMap, fmt::Display};
use uuid::Uuid;

use crate::dbs::Variables;
use crate::iam::AccessMethod;
use crate::iam::SigninParams;
use crate::iam::SignupParams;
use crate::{
	expr::access,
	protocol::{FromFlatbuffers, ToFlatbuffers},
};
use crate::protocol::flatbuffers::surreal_db::protocol::rpc as rpc_fb;
use crate::protocol::flatbuffers::surreal_db::protocol::expr as expr_fb;


impl ToFlatbuffers for SignupParams {
	type Output<'a> = flatbuffers::WIPOffset<rpc_fb::SignupParams<'a>>;
	
	fn to_fb<'a>(&self, fbb: &mut flatbuffers::FlatBufferBuilder<'a>) -> Self::Output<'a> {
		let namespace = fbb.create_string(&self.namespace);
		let database = fbb.create_string(&self.database);
		let access_name = fbb.create_string(&self.access_name);
		let variables = self.variables.to_fb(fbb);

		rpc_fb::SignupParams::create(
			fbb,
			&rpc_fb::SignupParamsArgs {
				namespace: Some(namespace),
				database: Some(database),
				access_name: Some(access_name),
				variables: Some(variables),
			},
		)
	}
}

impl FromFlatbuffers for SignupParams {
	type Input<'a> = rpc_fb::SignupParams<'a>;

	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self>
	where
		Self: Sized,
	{
		let namespace = input.namespace().context("Missing namespace")?
			.to_string();
		let database = input.database().context("Missing database")?
			.to_string();
		let access_name = input.access_name().context("Missing access name")?
			.to_string();
		let variables = input.variables().context("Failed to get variables")?;
		let variables = Variables::from_fb(variables)?;

		Ok(SignupParams {
			namespace,
			database,
			access_name,
			variables,
		})
	}
}

impl ToFlatbuffers for SigninParams {
	type Output<'a> = flatbuffers::WIPOffset<rpc_fb::SigninParams<'a>>;

	fn to_fb<'a>(&self, fbb: &mut flatbuffers::FlatBufferBuilder<'a>) -> Self::Output<'a> {
		let args = match &self.access_method {
			AccessMethod::RootUser { username, password } => {
				let username = fbb.create_string(username);
				let password = fbb.create_string(password);
				let root_user = rpc_fb::RootUserCredentials::create(
					fbb,
					&rpc_fb::RootUserCredentialsArgs {
						username: Some(username),
						password: Some(password),
					},
				);
				rpc_fb::SigninParamsArgs {
					access_method_type: rpc_fb::AccessMethod::Root,
					access_method: Some(root_user.as_union_value()),
				}
			},
			_ => todo!("STU: Implement other access methods"),
		};

		rpc_fb::SigninParams::create(
			fbb,
			&args,
		)
	}
}

impl FromFlatbuffers for SigninParams {
	type Input<'a> = rpc_fb::SigninParams<'a>;

	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self>
	where
		Self: Sized,
	{
		let access_method = match input.access_method_type() {
			rpc_fb::AccessMethod::Root => {
				let root_user = input.access_method_as_root().context("Missing root user credentials")?;
				let username = root_user.username().context("Missing username")?.to_string();
				let password = root_user.password().context("Missing password")?.to_string();
				AccessMethod::RootUser {
					username,
					password,
				}
			},
			_ => todo!("STU: Implement other access methods"),
		};

		Ok(SigninParams { access_method })
	}
}
