use std::{collections::BTreeMap, fmt::Display};
use anyhow::anyhow;
use uuid::Uuid;

use crate::expr::access;


impl super::surrealdb::rpc::Request {
    pub fn new(command: super::surrealdb::rpc::request::Command) -> Self {
        super::surrealdb::rpc::Request {
            id: Uuid::new_v4().to_string(),
            rpc_version: Some(3),
            command: Some(command),
        }
    }
}

impl super::surrealdb::rpc::Response {
    pub fn into_results(self) -> impl Iterator<Item = anyhow::Result<super::surrealdb::value::Value>> {

        self.results.into_iter().filter_map(|result| {
            let result = result.result?;
            Some(match result {
                super::surrealdb::rpc::query_result::Result::Error(err) => Err(anyhow!("{:?}", err)),
                super::surrealdb::rpc::query_result::Result::Value(value) => Ok(value),
            })
        })
    }
}        

// impl From<super::surrealdb::rpc::Response> for super::surrealdb::value::Value {}

impl super::surrealdb::rpc::QueryParams {
    pub fn extend_vars(&mut self, vars: &BTreeMap<String, super::surrealdb::value::Value>) {
        for (k, v) in vars {
            self.variables.insert(k.clone(), v.clone());
        }
    }
}

impl super::surrealdb::rpc::Request {
    pub fn method(&self) -> &str {
        use crate::protocol::surrealdb::rpc::request::Command;

        match &self.command {
            Some(Command::Health(_)) => "health",
            Some(Command::Version(_)) => "version",
            Some(Command::Info(_)) => "info",
            Some(Command::Use(_)) => "use",
            Some(Command::Signup(_)) => "signup",
            Some(Command::Signin(_)) => "signin",
            Some(Command::Authenticate(_)) => "authenticate",
            Some(Command::Invalidate(_)) => "invalidate",
            Some(Command::Reset(_)) => "reset",
            Some(Command::Kill(_)) => "kill",
            Some(Command::Live(_)) => "live",
            Some(Command::Set(_)) => "set",
            Some(Command::Unset(_)) => "unset",
            Some(Command::Select(_)) => "select",
            Some(Command::Insert(_)) => "insert",
            Some(Command::Create(_)) => "create",
            Some(Command::Upsert(_)) => "upsert",
            Some(Command::Update(_)) => "update",
            Some(Command::Merge(_)) => "merge",
            Some(Command::Patch(_)) => "patch",
            Some(Command::Delete(_)) => "delete",
            Some(Command::Query(_)) => "query",
            Some(Command::RawQuery(_)) => "raw_query",
            Some(Command::Relate(_)) => "relate",
            Some(Command::Run(_)) => "run",
            Some(Command::Graphql(_)) => "graphql",
            Some(Command::InsertRelation(_)) => "insert_relation",
            None => "unknown",
        }
    }
}

impl Display for super::surrealdb::rpc::Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Error {}: {}",
            self.code,
            self.message,
        )
    }
}


// pub trait RpcCommand {
//     type Response;

//     fn into_command(self) -> super::surrealdb::rpc::request::Command;

//     fn response_from_proto(response: super::surrealdb::rpc::Response) -> anyhow::Result<Self::Response>;
// }

// impl RpcCommand for super::surrealdb::rpc::SigninParams {
//     type Response = super::surrealdb::rpc::SigninResponse;

//     fn into_command(self) -> super::surrealdb::rpc::request::Command {
//         super::surrealdb::rpc::request::Command::Signin(self)
//     }

//     fn response_from_proto(response: super::surrealdb::rpc::Response) -> anyhow::Result<Self::Response> {
        
//     }
    
// }