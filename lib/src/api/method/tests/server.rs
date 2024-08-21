use super::types::User;
use crate::api::conn::{Command, DbResponse, Route};
use crate::api::Response as QueryResponse;
use crate::opt::Resource;
use channel::Receiver;
use surrealdb_core::sql::{to_value as to_core_value, Value as CoreValue};

pub(super) fn mock(route_rx: Receiver<Route>) {
	tokio::spawn(async move {
		while let Ok(Route {
			request,
			response,
		}) = route_rx.recv().await
		{
			let cmd = request.command;

			let result = match cmd {
				Command::Invalidate | Command::Health => Ok(DbResponse::Other(CoreValue::None)),
				Command::Authenticate {
					..
				}
				| Command::Kill {
					..
				}
				| Command::Unset {
					..
				} => Ok(DbResponse::Other(CoreValue::None)),
				Command::SubscribeLive {
					..
				} => Ok(DbResponse::Other("c6c0e36c-e2cf-42cb-b2d5-75415249b261".to_owned().into())),
				Command::Version => Ok(DbResponse::Other("1.0.0".into())),
				Command::Use {
					..
				} => Ok(DbResponse::Other(CoreValue::None)),
				Command::Signup {
					..
				}
				| Command::Signin {
					..
				} => Ok(DbResponse::Other("jwt".to_owned().into())),
				Command::Set {
					..
				} => Ok(DbResponse::Other(CoreValue::None)),
				Command::Query {
					..
				} => Ok(DbResponse::Query(QueryResponse::new())),
				Command::Create {
					data,
					..
				} => match data {
					None => Ok(DbResponse::Other(to_core_value(User::default()).unwrap())),
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
						Ok(DbResponse::Other(CoreValue::Array(Default::default())))
					}
					Resource::RecordId(..) => {
						Ok(DbResponse::Other(to_core_value(User::default()).unwrap()))
					}
					_ => unreachable!(),
				},
				Command::Upsert {
					what,
					..
				}
				| Command::Update {
					what,
					..
				}
				| Command::Merge {
					what,
					..
				}
				| Command::Patch {
					what,
					..
				} => match what {
					Resource::Table(..) | Resource::Array(..) | Resource::Range(..) => {
						Ok(DbResponse::Other(CoreValue::Array(Default::default())))
					}
					Resource::RecordId(..) => {
						Ok(DbResponse::Other(to_core_value(User::default()).unwrap()))
					}
					_ => unreachable!(),
				},
				Command::Insert {
					data,
					..
				} => match data {
					CoreValue::Array(..) => {
						Ok(DbResponse::Other(CoreValue::Array(Default::default())))
					}
					_ => Ok(DbResponse::Other(to_core_value(User::default()).unwrap())),
				},
				Command::Run {
					..
				} => Ok(DbResponse::Other(CoreValue::None)),
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
				} => Ok(DbResponse::Other(CoreValue::None)),
			};

			if let Err(message) = response.send(result).await {
				panic!("message dropped; {message:?}");
			}
		}
	});
}
