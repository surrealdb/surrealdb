use async_channel::Receiver;
use surrealdb_types::SurrealValue;

use super::types::User;
use crate::api::conn::{Command, DbResponse, Route};
use crate::api::{self, IndexedResults as QueryResponse};
use crate::opt::Resource;

pub(super) fn mock(route_rx: Receiver<Route>) {
	tokio::spawn(async move {
		while let Ok(Route {
			request,
			response,
		}) = route_rx.recv().await
		{
			let cmd = request.command;

			eprintln!("cmd: {cmd:?}");

			let result = match cmd {
				Command::Invalidate | Command::Health => Ok(DbResponse::Other(Value::None)),
				Command::Authenticate {
					..
				}
				| Command::Kill {
					..
				}
				| Command::Unset {
					..
				} => Ok(DbResponse::Other(Value::None)),
				Command::SubscribeLive {
					..
				} => Ok(DbResponse::Other("c6c0e36c-e2cf-42cb-b2d5-75415249b261".to_owned().into())),
				Command::Version => Ok(DbResponse::Other("1.0.0".into())),
				Command::Use {
					..
				} => Ok(DbResponse::Other(Value::None)),
				Command::Signup {
					..
				}
				| Command::Signin {
					..
				} => Ok(DbResponse::Other("jwt".to_owned().into())),
				Command::Set {
					..
				} => Ok(DbResponse::Other(Value::None)),
				Command::Query {
					..
				}
				| Command::RawQuery {
					..
				}
				| Command::Patch {
					..
				}
				| Command::Merge {
					..
				} => Ok(DbResponse::Query(QueryResponse::new())),
				Command::Create {
					data,
					..
				} => match data {
					None => Ok(DbResponse::Other(User::default().into_value())),
					Some(user) => Ok(DbResponse::Other(user.clone())),
				},
				Command::Select {
					what,
					..
				}
				| Command::Delete {
					what,
					..
				} => match what {
					Resource::Table(..) | Resource::Array(..) | Resource::Range(_) => {
						Ok(DbResponse::Other(Value::Array(Default::default())))
					}
					Resource::RecordId(..) => Ok(DbResponse::Other(User::default().into_value())),
					_ => unreachable!(),
				},
				Command::Upsert {
					what,
					..
				}
				| Command::Update {
					what,
					..
				} => match what {
					Resource::Table(..) | Resource::Array(..) | Resource::Range(..) => {
						Ok(DbResponse::Other(Value::Array(Default::default())))
					}
					Resource::RecordId(..) => Ok(DbResponse::Other(User::default().into_value())),
					_ => unreachable!(),
				},
				Command::Insert {
					data,
					..
				} => match data {
					Value::Array(..) => Ok(DbResponse::Other(Value::Array(Default::default()))),
					_ => Ok(DbResponse::Other(User::default().into_value())),
				},
				Command::InsertRelation {
					data,
					..
				} => match data {
					Value::Array(..) => Ok(DbResponse::Other(Value::Array(Default::default()))),
					_ => Ok(DbResponse::Other(User::default().into_value())),
				},
				Command::Run {
					..
				} => Ok(DbResponse::Other(Value::None)),
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
				} => Ok(DbResponse::Other(Value::None)),
			};

			if let Err(message) = response.send(result).await {
				panic!("message dropped; {message:?}");
			}
		}
	});
}
