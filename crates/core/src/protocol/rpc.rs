use crate::iam::AccessMethod;
use surrealdb_protocol::proto::rpc::v1 as rpc_proto;

impl TryFrom<AccessMethod> for rpc_proto::AccessMethod {
	type Error = anyhow::Error;

	fn try_from(value: AccessMethod) -> Result<Self, Self::Error> {
		use rpc_proto::access_method::Method as MethodEnum;
		match value {
			AccessMethod::RootUser {
				username,
				password,
			} => Ok(rpc_proto::AccessMethod {
				method: Some(MethodEnum::Root(rpc_proto::RootUserCredentials {
					username: username.into(),
					password: password.into(),
				})),
			}),
			AccessMethod::NamespaceUser {
				namespace,
				username,
				password,
			} => Ok(rpc_proto::AccessMethod {
				method: Some(MethodEnum::NamespaceUser(rpc_proto::NamespaceUserCredentials {
					namespace: namespace.into(),
					username: username.into(),
					password: password.into(),
				})),
			}),
			AccessMethod::DatabaseUser {
				namespace,
				database,
				username,
				password,
			} => Ok(rpc_proto::AccessMethod {
				method: Some(MethodEnum::DatabaseUser(rpc_proto::DatabaseUserCredentials {
					namespace: namespace.into(),
					database: database.into(),
					username: username.into(),
					password: password.into(),
				})),
			}),
			AccessMethod::NamespaceAccess {
				namespace,
				access_name,
				key,
			} => Ok(rpc_proto::AccessMethod {
				method: Some(MethodEnum::Namespace(rpc_proto::NamespaceAccessCredentials {
					namespace: namespace.into(),
					access: access_name.into(),
					key: key.into(),
				})),
			}),
			AccessMethod::DatabaseAccess {
				namespace,
				database,
				access_name,
				key,
				refresh_token,
			} => Ok(rpc_proto::AccessMethod {
				method: Some(MethodEnum::Database(rpc_proto::DatabaseAccessCredentials {
					namespace: namespace.into(),
					database: database.into(),
					access: access_name.into(),
					key: key.into(),
					refresh: refresh_token.unwrap_or_default(),
				})),
			}),
			AccessMethod::AccessToken {
				token,
			} => Ok(rpc_proto::AccessMethod {
				method: Some(MethodEnum::AccessToken(rpc_proto::AccessToken {
					token: token.into(),
				})),
			}),
		}
	}
}

impl TryFrom<rpc_proto::AccessMethod> for AccessMethod {
	type Error = anyhow::Error;

	fn try_from(value: rpc_proto::AccessMethod) -> Result<Self, Self::Error> {
		use rpc_proto::access_method::Method as MethodEnum;
		let Some(method) = value.method else {
			return Err(anyhow::anyhow!("Missing method"));
		};
		match method {
			MethodEnum::Root(credentials) => Ok(AccessMethod::RootUser {
				username: credentials.username.into(),
				password: credentials.password.into(),
			}),
			MethodEnum::NamespaceUser(credentials) => Ok(AccessMethod::NamespaceUser {
				namespace: credentials.namespace.into(),
				username: credentials.username.into(),
				password: credentials.password.into(),
			}),
			MethodEnum::DatabaseUser(credentials) => Ok(AccessMethod::DatabaseUser {
				namespace: credentials.namespace.into(),
				database: credentials.database.into(),
				username: credentials.username.into(),
				password: credentials.password.into(),
			}),
			MethodEnum::Namespace(credentials) => Ok(AccessMethod::NamespaceAccess {
				namespace: credentials.namespace.into(),
				access_name: credentials.access.into(),
				key: credentials.key.into(),
			}),
			MethodEnum::Database(credentials) => Ok(AccessMethod::DatabaseAccess {
				namespace: credentials.namespace.into(),
				database: credentials.database.into(),
				access_name: credentials.access.into(),
				key: credentials.key.into(),
				refresh_token: credentials.refresh.into(),
			}),
			MethodEnum::AccessToken(credentials) => Ok(AccessMethod::AccessToken {
				token: credentials.token.into(),
			}),
		}
	}
}
