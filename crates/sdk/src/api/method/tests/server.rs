use async_channel::Receiver;
use surrealdb_core::rpc::DbResult;

use super::types::User;
use crate::api::conn::{Command, IndexedDbResults, Route};
use crate::opt::Resource;
use crate::types::{SurrealValue, Value};

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
				Command::Invalidate | Command::Health => DbResult::Other(Value::None),
				Command::Authenticate {
					..
				}
				| Command::Kill {
					..
				}
				| Command::Unset {
					..
				} => DbResult::Other(Value::None),
				Command::SubscribeLive {
					..
				} => DbResult::Other(Value::String(
					"c6c0e36c-e2cf-42cb-b2d5-75415249b261".to_string(),
				)),
				Command::Version => DbResult::Other(Value::String("1.0.0".to_string())),
				Command::Use {
					..
				} => DbResult::Other(Value::None),
				Command::Signup {
					..
				}
				| Command::Signin {
					..
				} => DbResult::Other(Value::String("jwt".to_string())),
				Command::Set {
					..
				} => DbResult::Other(Value::None),
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
				} => DbResult::Query(Vec::new()),
				Command::Create {
					data,
					..
				} => match data {
					None => DbResult::Other(User::default().into_value()),
					Some(user) => DbResult::Other(user.clone()),
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
						DbResult::Other(Value::Array(Default::default()))
					}
					Resource::RecordId(..) => DbResult::Other(User::default().into_value()),
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
						DbResult::Other(Value::Array(Default::default()))
					}
					Resource::RecordId(..) => DbResult::Other(User::default().into_value()),
					_ => unreachable!(),
				},
				Command::Insert {
					data,
					..
				} => match data {
					Value::Array(..) => DbResult::Other(Value::Array(Default::default())),
					_ => DbResult::Other(User::default().into_value()),
				},
				Command::InsertRelation {
					data,
					..
				} => match data {
					Value::Array(..) => DbResult::Other(Value::Array(Default::default())),
					_ => DbResult::Other(User::default().into_value()),
				},
				Command::Run {
					..
				} => DbResult::Other(Value::None),
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
				} => DbResult::Other(Value::None),
			};

			let result = IndexedDbResults::from_server_result(result).unwrap();

			if let Err(message) = response.send(Ok(result)).await {
				panic!("message dropped; {message:?}");
			}
		}
	});
}
