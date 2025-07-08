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
					username,
					password,
				})),
			}),
			AccessMethod::NamespaceUser {
				namespace,
				username,
				password,
			} => Ok(rpc_proto::AccessMethod {
				method: Some(MethodEnum::NamespaceUser(rpc_proto::NamespaceUserCredentials {
					namespace,
					username,
					password,
				})),
			}),
			AccessMethod::DatabaseUser {
				namespace,
				database,
				username,
				password,
			} => Ok(rpc_proto::AccessMethod {
				method: Some(MethodEnum::DatabaseUser(rpc_proto::DatabaseUserCredentials {
					namespace,
					database,
					username,
					password,
				})),
			}),
			AccessMethod::NamespaceAccess {
				namespace,
				access_name,
				key,
			} => Ok(rpc_proto::AccessMethod {
				method: Some(MethodEnum::Namespace(rpc_proto::NamespaceAccessCredentials {
					namespace,
					access: access_name,
					key,
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
					namespace,
					database,
					access: access_name,
					key,
					refresh: refresh_token.unwrap_or_default(),
				})),
			}),
			AccessMethod::AccessToken {
				token,
			} => Ok(rpc_proto::AccessMethod {
				method: Some(MethodEnum::AccessToken(rpc_proto::AccessToken {
					token,
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
				username: credentials.username,
				password: credentials.password,
			}),
			MethodEnum::NamespaceUser(credentials) => Ok(AccessMethod::NamespaceUser {
				namespace: credentials.namespace,
				username: credentials.username,
				password: credentials.password,
			}),
			MethodEnum::DatabaseUser(credentials) => Ok(AccessMethod::DatabaseUser {
				namespace: credentials.namespace,
				database: credentials.database,
				username: credentials.username,
				password: credentials.password,
			}),
			MethodEnum::Namespace(credentials) => Ok(AccessMethod::NamespaceAccess {
				namespace: credentials.namespace,
				access_name: credentials.access,
				key: credentials.key,
			}),
			MethodEnum::Database(credentials) => Ok(AccessMethod::DatabaseAccess {
				namespace: credentials.namespace,
				database: credentials.database,
				access_name: credentials.access,
				key: credentials.key,
				refresh_token: Some(credentials.refresh),
			}),
			MethodEnum::AccessToken(credentials) => Ok(AccessMethod::AccessToken {
				token: credentials.token,
			}),
		}
	}
}
