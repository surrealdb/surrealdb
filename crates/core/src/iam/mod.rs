use anyhow::Context;
pub use entities::Level;
use thiserror::Error;

pub mod access;
pub mod auth;
pub mod base;
pub mod check;
pub mod clear;
pub mod entities;
pub(crate) mod file;
pub mod issue;
#[cfg(feature = "jwks")]
pub mod jwks;
pub mod reset;
pub mod signin;
pub mod signup;
pub mod token;
pub mod verify;

use crate::dbs::Variables;

pub use self::auth::*;
pub use self::entities::*;
use serde::{Deserialize, Serialize};

use surrealdb_protocol::proto::rpc::v1 as rpc_proto;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum Error {
	#[error("Invalid role '{0}'")]
	InvalidRole(String),

	#[error("Not enough permissions to perform this action")]
	NotAllowed {
		actor: String,
		action: String,
		resource: String,
	},
}

pub fn is_allowed_check(actor: &Actor, action: &Action, resource: &Resource) -> bool {
	match action {
		Action::View => resource.level().sublevel_of(actor.level()),
		Action::Edit => {
			if actor.has_role(Role::Owner) {
				resource.level().sublevel_of(actor.level())
			} else if actor.has_role(Role::Editor) {
				matches!(
					resource.kind(),
					ResourceKind::Namespace
						| ResourceKind::Database
						| ResourceKind::Record
						| ResourceKind::Table
						| ResourceKind::Document
						| ResourceKind::Option
						| ResourceKind::Function
						| ResourceKind::Analyzer
						| ResourceKind::Parameter
						| ResourceKind::Event
						| ResourceKind::Field
						| ResourceKind::Index
				) && resource.level().sublevel_of(actor.level())
			} else {
				false
			}
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignupParams {
	pub namespace: String,
	pub database: String,
	pub access_name: String,
	pub variables: Variables,
}

impl TryFrom<SignupParams> for rpc_proto::SignupRequest {
	type Error = anyhow::Error;

	fn try_from(value: SignupParams) -> Result<Self, Self::Error> {
		Ok(Self {
			namespace: value.namespace,
			database: value.database,
			access_name: value.access_name,
			variables: Some(value.variables.try_into()?),
		})
	}
}

impl TryFrom<rpc_proto::SignupRequest> for SignupParams {
	type Error = anyhow::Error;

	fn try_from(value: rpc_proto::SignupRequest) -> Result<Self, Self::Error> {
		let variables = value.variables.context("variables are required")?;
		Ok(Self {
			namespace: value.namespace,
			database: value.database,
			access_name: value.access_name,
			variables: variables.try_into()?,
		})
	}
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SigninParams {
	pub access_method: AccessMethod,
}

impl TryFrom<SigninParams> for rpc_proto::SigninRequest {
	type Error = anyhow::Error;

	fn try_from(value: SigninParams) -> Result<Self, Self::Error> {
		Ok(Self {
			access_method: Some(value.access_method.try_into()?),
		})
	}
}

impl TryFrom<rpc_proto::SigninRequest> for SigninParams {
	type Error = anyhow::Error;

	fn try_from(value: rpc_proto::SigninRequest) -> Result<Self, Self::Error> {
		let access_method = value.access_method.context("access_method is required")?;
		Ok(Self {
			access_method: access_method.try_into()?,
		})
	}
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AccessMethod {
	RootUser {
		username: String,
		password: String,
	},
	NamespaceAccess {
		namespace: String,
		access_name: String,
		key: String,
	},
	DatabaseAccess {
		namespace: String,
		database: String,
		access_name: String,
		key: String,
		refresh_token: Option<String>,
	},
	NamespaceUser {
		namespace: String,
		username: String,
		password: String,
	},
	DatabaseUser {
		namespace: String,
		database: String,
		username: String,
		password: String,
	},
	AccessToken {
		token: String,
	},
}

pub fn is_allowed(actor: &Actor, action: &Action, resource: &Resource) -> Result<(), Error> {
	if !is_allowed_check(actor, action, resource) {
		let err = Error::NotAllowed {
			actor: actor.to_string(),
			action: action.to_string(),
			resource: format!("{}", resource),
		};

		trace!("{}", err);
		return Err(err);
	}

	Ok(())
}
