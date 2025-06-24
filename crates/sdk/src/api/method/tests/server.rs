use super::types::User;
use crate::api::QueryResults as QueryResponse;
use crate::api::conn::{Command, Route};
use crate::opt::Resource;
use async_channel::Receiver;
use surrealdb_core::dbs::{QueryResult, QueryResultData};
use surrealdb_core::expr::{Value as Value, to_value};
use surrealdb_core::protocol::flatbuffers::surreal_db::protocol::rpc::{
	CreateParams, DeleteParams, InsertParams, SelectParams, UpdateParams,
	UpsertParams,
};

pub(super) fn mock(route_rx: Receiver<Route>) {
	tokio::spawn(async move {
		while let Ok(Route {
			request,
			response,
		}) = route_rx.recv().await
		{
			let cmd = request.command;

			let result = match cmd {
				Command::Invalidate | Command::Health => Ok(QueryResultData::new_from_value(Value::None)),
				Command::Authenticate {
					..
				}
				| Command::Kill {
					..
				}
				| Command::Unset {
					..
				} => Ok(QueryResultData::new_from_value(Value::None)),
				Command::SubscribeLive {
					..
				} => Ok(QueryResultData::new_from_value("c6c0e36c-e2cf-42cb-b2d5-75415249b261".to_owned().into())),
				Command::Version => Ok(QueryResultData::new_from_value("1.0.0".into())),
				Command::Use {
					..
				} => Ok(QueryResultData::new_from_value(Value::None)),
				Command::Signup {
					..
				}
				| Command::Signin {
					..
				} => Ok(QueryResultData::new_from_value("jwt".to_owned().into())),
				Command::Set {
					..
				} => Ok(QueryResultData::new_from_value(Value::None)),
				Command::Query {
					..
				} => Ok(QueryResultData::Results(Vec::new())),
				Command::Create {
					data,
					..
				} => match data {
					None => Ok(QueryResultData::new_from_value(to_value(User::default()).unwrap())),
					Some(user) => Ok(QueryResultData::new_from_value(user.clone())),
				},
				Command::Select {
					what,
					..
				}
				| Command::Delete {
					what,
					..
				} => {
					let mut results = Vec::new();
					for what in what.iter() {
						match what {
							Value::Table(..) | Value::Array(..) | Value::Range(_) => {
								results.push(QueryResult::default());
							}
							Value::Thing(..) => {
								results.push(QueryResult::new_from_value(to_value(User::default()).unwrap()));
							}
							_ => unreachable!(),
						}
					}
					Ok(QueryResultData::Results(results))
				},
				Command::Upsert {
					what,
					..
				}
				| Command::Update {
					what,
					..
				} => {
					let mut results = Vec::new();
					for what in what.iter() {
						match what {
							Value::Table(..) | Value::Array(..) | Value::Range(_) => {
								results.push(QueryResult::default());
							}
							Value::Thing(..) => {
								results.push(QueryResult::new_from_value(to_value(User::default()).unwrap()));
							}
							_ => unreachable!(),
						}
					}
					Ok(QueryResultData::Results(results))
				},
				Command::Insert {
					data,
					..
				} => match data {
					Value::Array(..) => {
						Ok(QueryResultData::new_from_value(Value::Array(Default::default())))
					}
					_ => Ok(QueryResultData::new_from_value(to_value(User::default()).unwrap())),
				},
				Command::Run {
					..
				} => Ok(QueryResultData::new_from_value(Value::None)),
				Command::ExportMl {
					..
				}
				| Command::ExportBytesMl {
					..
				}
				| Command::ExportFile {
					..
				}
				| Command::ExportBytes {
					..
				}
				| Command::ImportMl {
					..
				}
				| Command::ImportFile {
					..
				} => Ok(QueryResultData::new_from_value(Value::None)),
			};

			if let Err(message) = response.send(result).await {
				panic!("message dropped; {message:?}");
			}
		}
	});
}
